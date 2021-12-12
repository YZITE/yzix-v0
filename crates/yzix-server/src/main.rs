#![forbid(
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion,
    unsafe_code,
    unused_must_use
)]

use async_channel::{unbounded, bounded, Sender};
use reqwest::Client as FetchClient;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::store::{Dump, Flags as DumpFlags, Hash as StoreHash};
use yzix_core::tracing::{error, info};
use yzix_core::{OutputError, OutputName, Response, ResponseKind, Utf8PathBuf};
use yzix_pool::Pool;

mod clients;
use clients::{handle_client_io, handle_clients_initial};
mod fetch;
use fetch::mangle_result as mangle_fetch_result;
mod in2_helpers;
use in2_helpers::{create_in2_symlinks_bunch, resolve_in2_from_target};
mod utils;
use utils::*;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
    store_path: Utf8PathBuf,
    container_runner: String,
    socket_bind: String,
    bearer_tokens: HashSet<String>,
    auto_repair: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Output {
    NotStarted,
    Scheduled,
    Failed(OutputError),
    /// NOTE: thanks to `VerificationOk`, the hashmap may be empty
    Success(HashMap<OutputName, StoreHash>),
}

#[derive(Clone, Debug)]
pub enum LogFwdMessage {
    Response(Arc<Response>),
    Subscribe(Sender<Arc<Response>>),
}

#[derive(Debug)]
pub struct NodeMeta {
    output: Output,
    log: Vec<Sender<LogFwdMessage>>,
    reschedule: bool,
}

impl Default for NodeMeta {
    fn default() -> Self {
        Self {
            output: Output::NotStarted,
            log: vec![],
            reschedule: false,
        }
    }
}

impl build_graph::ReadOutHash for NodeMeta {
    fn read_out_hash(&self, output: &str) -> Option<StoreHash> {
        if let Output::Success(x) = &self.output {
            x.get(output).copied()
        } else {
            None
        }
    }
}

pub enum MainMessage {
    ClientConn {
        conn: tokio::net::TcpStream,
        opts: yzix_core::ClientOpts,
    },
    Schedule {
        graph: build_graph::Graph<()>,
        attach_logs: Option<AttachLogsKind>,
    },
    Shutdown,
    Done {
        nid: NodeIndex,
        det: Result<Option<BuiltItem>, OutputError>,
    },
}

pub enum AttachLogsKind {
    // attach logs via bearer
    Bearer(Arc<str>),
    // dup existing logs
    Dup(Vec<Sender<LogFwdMessage>>),
}

// this structure is used to avoid switching up argument order
pub struct WorkItemRun {
    inhash: StoreHash,
    args: Vec<String>,
    envs: HashMap<String, String>,
    new_root: Option<StoreHash>,
    outputs: HashSet<OutputName>,
    need_store_mount: bool,
}

pub struct BuiltItem {
    inhash: Option<StoreHash>,
    outputs: HashMap<OutputName, (Option<Arc<Dump>>, StoreHash)>,
}

impl BuiltItem {
    pub fn with_single(
        inhash: Option<StoreHash>,
        dump: Option<Arc<Dump>>,
        outhash: StoreHash,
    ) -> Self {
        let mut outputs = HashMap::new();
        outputs.insert(OutputName::default(), (dump, outhash));
        Self { inhash, outputs }
    }
}

const INPUT_REALISATION_DIR_POSTFIX: &str = ".in2";

async fn schedule(
    config: Arc<ServerConfig>,
    mains: Sender<MainMessage>,
    jobsem: Arc<tokio::sync::Semaphore>,
    connpool: Pool<FetchClient>,
    containerpool: Pool<String>,
    graph: &mut build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) {
    {
        let mo = &mut graph.0[nid].rest.output;
        if *mo != Output::NotStarted {
            // already started
            return;
        }
        *mo = Output::Scheduled;
    }
    let det = match graph.eval_node(nid, &config.store_path) {
        Ok(x) => {
            use build_graph::WorkItem as WI;
            let inhash = x.inputs_hash();
            if let Some(inhash) = inhash {
                use std::path::Path;
                let realis_path = config
                    .store_path
                    .join(format!("{}{}", inhash, INPUT_REALISATION_DIR_POSTFIX))
                    .into_std_path_buf();

                let outputs: HashMap<_, _> = tokio::task::block_in_place(|| {
                    if let Ok(rp) = std::fs::read_link(&realis_path) {
                        resolve_in2_from_target(rp, Path::new(""))
                            .map(|outhash| (OutputName::default(), (None, outhash)))
                            .into_iter()
                            .collect()
                    } else if let Ok(realis_iter) = std::fs::read_dir(&realis_path) {
                        // determine list of necessary outputs
                        // NOTE: we rely on the fact that no scheduling happens in parallel,
                        // so we can assume no one observes that the output is already set
                        // to scheduled, so that no additional graph is merged with the main one,
                        // so that no additionally required outputs appear while we determine
                        // the set of necessary outputs
                        let super_dir = Path::new("..");
                        // don't pessimize, read all available outputs
                        realis_iter
                            .filter_map(|entry| {
                                let entry = entry.ok()?;
                                let name = entry
                                    .file_name()
                                    .to_str()
                                    .and_then(|filn| OutputName::new(filn.to_string()))?;
                                let rp = std::fs::read_link(entry.path()).ok()?;
                                Some((name, rp))
                            })
                            .filter_map(|(filn, rp)| {
                                resolve_in2_from_target(rp, super_dir)
                                    .map(|outhash| (filn, (None, outhash)))
                            })
                            .collect()
                    } else {
                        HashMap::new()
                    }
                });
                if !outputs.is_empty() {
                    if !graph.0[nid].rest.reschedule {
                        // make sure that we don't end up in an endless loop
                        graph.0[nid].rest.reschedule = true;
                        // lucky case: we have found a valid input->output realisation/shortcut!
                        // do not submit the inhash to main, it would only try
                        // to rewrite it, we don't want that.
                        // if we don't have all necessary outputs, then
                        // we will get rescheduled and thanks to the check above
                        // this won't lead to an endless loop
                        mains
                            .send(MainMessage::Done {
                                nid,
                                det: Ok(Some(BuiltItem {
                                    inhash: None,
                                    outputs,
                                })),
                            })
                            .await
                            .unwrap();
                        return;
                    } else {
                        info!(%inhash, "cached entry rejected (rescheduled)");
                    }
                }
            }
            match x {
                WI::Run {
                    args,
                    envs,
                    new_root,
                    outputs,
                    uses_placeholders,
                } => {
                    let config = config.clone();
                    let jobsem = jobsem.clone();
                    let containerpool = containerpool.clone();
                    let mains = mains.clone();
                    tokio::spawn(async move {
                        let _job = jobsem.acquire().await;
                        let container_name = containerpool.pop().await;
                        let det = handle_process(
                            &config,
                            &container_name,
                            WorkItemRun {
                                inhash: inhash.unwrap(),
                                args,
                                envs,
                                new_root,
                                outputs,
                                need_store_mount: uses_placeholders,
                            },
                        )
                        .await
                        .map(Some);
                        let _ = tokio::join!(
                            containerpool.push(container_name),
                            mains.send(MainMessage::Done { nid, det }),
                        );
                    });
                    return;
                }
                WI::UnDump { dat } => {
                    let outhash = StoreHash::hash_complex::<Dump>(&*dat);
                    Ok(Some(BuiltItem::with_single(inhash, Some(dat), outhash)))
                }
                WI::Require(outhash) => Ok(Some(BuiltItem::with_single(None, None, outhash))),
                WI::Fetch { url, expect_hash } => {
                    if let Some(outhash) = expect_hash.and_then(|expect_hash| {
                        if std::path::Path::new(&config.store_path.join(expect_hash.to_string()))
                            .is_file()
                        {
                            Some(expect_hash)
                        } else {
                            None
                        }
                    }) {
                        Ok(Some(BuiltItem::with_single(None, None, outhash)))
                    } else {
                        let connpool = connpool.clone();
                        tokio::spawn(async move {
                            println!("\rfetch {}", url);
                            let client = connpool.pop().await;
                            let resp = client.get(url.clone()).send().await;
                            connpool.push(client).await;
                            let _ = mains
                                .send(MainMessage::Done {
                                    nid,
                                    det: mangle_fetch_result(url, resp, expect_hash)
                                        .await
                                        .map(Some),
                                })
                                .await;
                        });
                        return;
                    }
                }
                WI::Eval(outhash) => {
                    let log = graph.0[nid].rest.log.clone();
                    let store_path = config.store_path.clone();
                    let mains = mains.clone();
                    tokio::spawn(async move {
                        let det = match read_graph_from_store(&store_path, outhash).await {
                            Ok(graph) => {
                                let _ = mains
                                    .send(MainMessage::Schedule {
                                        graph,
                                        attach_logs: if log.is_empty() {
                                            None
                                        } else {
                                            Some(AttachLogsKind::Dup(log))
                                        },
                                    })
                                    .await;
                                // for the graph itself, as a fallback, this node is kinda transparent
                                Ok(Some(BuiltItem::with_single(None, None, outhash)))
                            }
                            Err(e) => Err(e),
                        };
                        let _ = mains.send(MainMessage::Done { nid, det }).await;
                    });
                    return;
                }
                WI::Dump(hsh) => {
                    match hsh
                        .into_iter()
                        .map(|(k, v)| {
                            Dump::read_from_path(
                                config.store_path.join(v.to_string()).as_std_path(),
                            )
                            .map(|vd| (k, vd))
                        })
                        .collect()
                    {
                        Ok(dat) => {
                            let dump = Arc::new(Dump::Directory(dat));
                            let outhash = StoreHash::hash_complex::<Dump>(&*dump);
                            push_response(&mut graph.0[nid], ResponseKind::Dump(dump.clone()))
                                .await;
                            Ok(Some(BuiltItem::with_single(
                                inhash,
                                Some(dump),
                                outhash,
                            )))
                        }
                        Err(e) => Err(e.into()),
                    }
                }
                WI::VerificationOk => Ok(None),
            }
        }

        Err(build_graph::EvalError::InputWithoutHash(je)) => {
            if let Output::Failed(_) = graph.0[graph.0.edge_endpoints(je).unwrap().1].rest.output {
                // dependency failed
                Err(OutputError::InputFailed(graph.0[je].kind.clone()))
            } else {
                // inputs missing
                // NOTE: do not delete the next line, it prevents a deadlock
                // of graph processing, because otherwise we would just stop
                // processing this input without scheduling it ever again.
                graph.0[nid].rest.output = Output::NotStarted;
                return;
            }
        }

        Err(build_graph::EvalError::Final(e)) => Err(e),
    };

    // this is necessary to properly update the progress bar
    // and serialization to local store
    mains.send(MainMessage::Done { nid, det }).await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // set the time zone to avoid funky locale stuff
    std::env::set_var("TZ", "UTC");

    let config: ServerConfig = {
        let mut args = std::env::args().skip(1);
        let arg = if let Some(x) = args.next() {
            x
        } else {
            eprintln!(
                "yzix-server: ERROR: invalid invocation (supply a config file as only argument)"
            );
            std::process::exit(1);
        };
        if args.next().is_some() || arg == "--help" {
            eprintln!(
                "yzix-server: ERROR: invalid invocation (supply a config file as only argument)"
            );
            std::process::exit(1);
        }

        toml::de::from_slice(&std::fs::read(arg).expect("unable to read supplied config file"))
            .expect("unable to parse supplied config file")
    };
    let config = Arc::new(config);

    // install global subscriber configured based on RUST_LOG envvar.
    tracing_subscriber::fmt::init();

    std::fs::create_dir_all(&config.store_path).expect("unable to create store dir");

    let cpucnt = num_cpus::get();
    let jobsem = Arc::new(tokio::sync::Semaphore::new(cpucnt));

    // setup pools
    let connpool = Pool::<FetchClient>::default();
    let containerpool = Pool::<String>::default();

    {
        use std::time::Duration;
        let cto = Duration::from_secs(30);
        let tow = Duration::from_secs(600);
        for _ in 0..cpucnt {
            let a = connpool.push(
                FetchClient::builder()
                    .user_agent("Yzix 0.1 server")
                    .connect_timeout(cto)
                    .timeout(tow)
                    .build()
                    .expect("unable to setup HTTP client"),
            );
            let b = containerpool.push(format!("yzix-{}", random_name()));
            tokio::join!(a, b);
        }
    }

    let listener = tokio::net::TcpListener::bind(&config.socket_bind)
        .await
        .unwrap();

    // we don't need workers. we just spawn tasks instead
    // this means that it is impossible to fix or modify
    // running or older tasks. (invariant)
    // we manage the graph in the main thread, it is !Sync+!Send

    // this queue needs to be unbounded, otherwise the main thread could block itself...
    let (mains, mainr) = unbounded();
    let mut graph = build_graph::Graph::default();

    // install Ctrl+C handler
    let mains2 = mains.clone();
    tokio::spawn(async move {
        // wait for shutdown signal
        tokio::signal::ctrl_c().await.unwrap();
        let _ = mains2.send(MainMessage::Shutdown).await;
    });

    // handle log forwarding
    let mut logwbearer = HashMap::<String, Sender<LogFwdMessage>>::new();
    for i in &config.bearer_tokens {
        let (logs, logr) = bounded(1000);
        logwbearer.insert(i.clone(), logs);
        tokio::spawn(async move {
            let mut subs = Vec::new();
            while let Ok(x) = logr.recv().await {
                use LogFwdMessage as LFM;
                match x {
                    LFM::Response(r) => {
                        log_to_bunch(&mut subs, r).await;
                    }
                    LFM::Subscribe(s) => {
                        subs.push(s);
                    }
                }
            }
        });
    }
    let logwbearer = logwbearer;

    tokio::spawn(handle_clients_initial(
        mains.clone(),
        listener,
        config.bearer_tokens.clone(),
    ));

    use MainMessage as MM;
    while let Ok(x) = mainr.recv().await {
        match x {
            MM::Shutdown => break,
            MM::ClientConn { conn, opts } => {
                let bearer_auth: Arc<str> = opts.bearer_auth.into();
                tokio::spawn(handle_client_io(
                    mains.clone(),
                    conn,
                    // this is necessary to associate a schedule/subgraph
                    // to a log; even if the current connection doesn't
                    // want to receive logs, the user can have multiple
                    // clients, and maybe another one wants to tail-f
                    // the logs (this is also the recommended approach).
                    bearer_auth.clone(),
                    if opts.attach_to_logs {
                        let (logs, logr) = bounded(1000);
                        logwbearer[&*bearer_auth]
                            .send(LogFwdMessage::Subscribe(logs))
                            .await
                            .ok()
                            .map(|_| logr)
                    } else {
                        None
                    },
                ));
            }
            MM::Schedule {
                graph: graph2,
                attach_logs,
            } => {
                let trt = if let Some(attach_logs) = attach_logs {
                    let log = match attach_logs {
                        AttachLogsKind::Bearer(bearer) => {
                            vec![logwbearer[&*bearer].clone()]
                        }
                        AttachLogsKind::Dup(log) => log,
                    };
                    graph.take_and_merge(
                        graph2,
                        |()| NodeMeta {
                            log: log.clone(),
                            ..NodeMeta::default()
                        },
                        |noder| noder.log.extend(log.clone()),
                    )
                } else {
                    graph.take_and_merge(graph2, |()| NodeMeta::default(), |_| {})
                };
                for (_, nid) in trt {
                    schedule(
                        config.clone(),
                        mains.clone(),
                        jobsem.clone(),
                        connpool.clone(),
                        containerpool.clone(),
                        &mut graph,
                        nid,
                    )
                    .await;
                }
            }
            MM::Done { nid, det } => {
                use yzix_core::tracing::{span, Level};
                let mut node = &mut graph.0[nid];
                let span =
                    span!(Level::INFO, "Done", ?nid, bldname = %node.name, tag = %node.logtag);
                let _guard = span.enter();
                match det {
                    Ok(Some(BuiltItem { inhash, outputs })) => {
                        let mut success = true;
                        let mut syms = HashMap::new();
                        for (outname, (dump, outhash)) in &outputs {
                            let span = span!(Level::ERROR, "output", %outhash);
                            let _guard = span.enter();
                            let ohs = outhash.to_string();
                            let dstpath = config.store_path.join(&ohs).into_std_path_buf();
                            let mut err_output = None;
                            if let Some(dump) = dump {
                                let dump = Arc::clone(dump);
                                let mut do_write = true;
                                if dstpath.exists() {
                                    if config.auto_repair {
                                        match Dump::read_from_path(&dstpath) {
                                            Ok(on_disk_dump) => {
                                                let on_disk_hash =
                                                    StoreHash::hash_complex::<Dump>(&on_disk_dump);
                                                if on_disk_hash != *outhash {
                                                    error!("detected data corruption");
                                                } else if on_disk_dump != *dump {
                                                    error!("detected hash collision");
                                                    err_output = Some(OutputError::HashCollision(
                                                        on_disk_hash,
                                                    ));
                                                    do_write = false;
                                                } else {
                                                    do_write = false;
                                                }
                                            }
                                            Err(e) => {
                                                error!("while dumping: {}", e);
                                            }
                                        }
                                    } else {
                                        do_write = false;
                                    }
                                }
                                if do_write {
                                    if let Err(e) = dump.write_to_path(
                                        &dstpath,
                                        DumpFlags {
                                            force: true,
                                            make_readonly: true,
                                        },
                                    ) {
                                        error!("{}", e);
                                        err_output = Some(e.into());
                                    }
                                }

                                if err_output.is_none() {
                                    syms.insert(outname.clone().into(), outhash.to_string());
                                }
                            }
                            if err_output.is_none() && !dstpath.exists() {
                                err_output = Some(OutputError::Unavailable);
                            }
                            if let Some(e) = err_output {
                                // TODO: should we really give up here, and not try to dump the other outputs?
                                node.rest.output = Output::Failed(e.clone());
                                push_response(node, ResponseKind::OutputNotify(Err(e))).await;
                                success = false;
                                // do not yet give up here
                                //break;
                            }
                        }

                        if let Some(inhash) = inhash {
                            let inpath = config
                                .store_path
                                .join(format!("{}{}", inhash, INPUT_REALISATION_DIR_POSTFIX));
                            // FIXME: how to deal with conflicting hashes?

                            // optimization, because the amount of subdirectories per directory
                            // is really limited (e.g. ~64000 for ext4)
                            // see also: https://ext4.wiki.kernel.org/index.php/Ext4_Howto#Sub_directory_scalability
                            // so we omit creating a subdirectory if it would only contain
                            // the 'out' (default output path) symlink

                            // this is caching, if it fails,
                            //    it's non-fatal for the node, but a big error for the server

                            let span = span!(Level::ERROR, "realisation write", %inpath, ?syms);
                            let _guard = span.enter();

                            let mut create_bunch = false;
                            if syms.len() == 1 {
                                if let Some(target) = syms.remove(&*OutputName::default()) {
                                    // check if the link target stays the same
                                    match std::fs::read_link(inpath.as_std_path()) {
                                        Ok(oldtrg) if oldtrg == std::path::Path::new(&target) => {}
                                        Err(erl) if erl.kind() == std::io::ErrorKind::NotFound => {
                                            if let Err(e) = std::os::unix::fs::symlink(
                                                &target,
                                                inpath.as_std_path(),
                                            ) {
                                                error!("{}", e);
                                            }
                                        }
                                        Ok(orig_target) => {
                                            error!(?orig_target, "outname differs");
                                        }
                                        Err(e) => {
                                            error!("blocked: {}", e);
                                        }
                                    }
                                    // usually, you can't mark a symlink read-only
                                } else {
                                    create_bunch = true;
                                }
                            } else {
                                create_bunch = true;
                            }

                            if create_bunch {
                                if let Err(e) =
                                    create_in2_symlinks_bunch(inpath.as_std_path(), &syms)
                                {
                                    error!("{}", e);
                                }
                            }
                        }

                        if success {
                            let realisation = outputs
                                .iter()
                                .map(|(name, &(_, outhash))| (name.clone(), outhash))
                                .collect::<HashMap<OutputName, StoreHash>>();
                            node.rest.output = Output::Success(realisation.clone());
                            push_response(node, ResponseKind::OutputNotify(Ok(realisation))).await;

                            // schedule now available jobs
                            let outputs: HashSet<_> = outputs.into_iter().map(|(i, _)| i).collect();
                            let reschedule_allowed = graph.0[nid].rest.reschedule;
                            let mut reschedule = false;
                            let mut per_nid_necouts = HashMap::<_, HashSet<_>>::new();
                            for edge in graph.0.edges_directed(nid, Direction::Incoming) {
                                per_nid_necouts
                                    .entry(edge.source())
                                    .or_default()
                                    .insert(edge.weight().sel_output.clone());
                            }
                            for (nid2, necouts) in per_nid_necouts {
                                if necouts.is_subset(&outputs) || !reschedule_allowed {
                                    // if no reschedule is allowed,
                                    // then this marks nid2 as failed
                                    // (because the input is unavailable)
                                    schedule(
                                        config.clone(),
                                        mains.clone(),
                                        jobsem.clone(),
                                        connpool.clone(),
                                        containerpool.clone(),
                                        &mut graph,
                                        nid2,
                                    )
                                    .await;
                                } else {
                                    // this requires an output which is not yet available,
                                    // e.g. because the input node was merged, and the
                                    // nid2 was added to the main graph after scheduling
                                    reschedule = true;
                                }
                            }
                            if reschedule {
                                assert!(reschedule_allowed);
                                graph.0[nid].rest.output = Output::NotStarted;
                                schedule(
                                    config.clone(),
                                    mains.clone(),
                                    jobsem.clone(),
                                    connpool.clone(),
                                    containerpool.clone(),
                                    &mut graph,
                                    nid,
                                )
                                .await;
                            }
                        }
                    }
                    Ok(None) => {
                        node.rest.output = Output::Success(HashMap::new());
                    }
                    Err(oe) => {
                        error!("{}", oe);
                        node.rest.output = Output::Failed(oe.clone());
                        push_response(node, ResponseKind::OutputNotify(Err(oe))).await;
                    }
                }
            }
        }

        // garbage collection, only necessary if the graph takes up too much space
        // we clean up either if the graph contains more than 1000 nodes or uses more
        // than 1 MiB memory.
        if graph.0.node_count() == 0 {
            continue;
        } else if graph.0.node_count() < 1000 {
            // FIXME: if necessary, only run the garbage collection on
            // small build graphs if the yzix-server process uses too much memory.
            // it is necessary to check if this has any positive effect,
            // we need to benchmark this.
            //if mw < 0x100000 {
            //    continue;
            //}
        }

        // propagate failures
        for i in graph
            .0
            .node_indices()
            .filter(|&i| matches!(graph.0[i].rest.output, Output::Failed(_)))
            .collect::<Vec<_>>()
        {
            let mut ineigh = graph.0.neighbors_directed(i, Direction::Incoming).detach();
            while let Some((je, js)) = ineigh.next(&graph.0) {
                let mo = &mut graph.0[js].rest.output;
                if *mo == Output::NotStarted {
                    *mo = Output::Scheduled;
                    // keep scheduler in sync
                    let (mains2, jek) = (mains.clone(), graph.0[je].kind.clone());
                    tokio::spawn(async move {
                        let _ = mains2
                            .send(MainMessage::Done {
                                nid: js,
                                det: Err(OutputError::InputFailed(jek)),
                            })
                            .await;
                    });
                }
            }
        }

        // search unnecessary nodes
        let cnt = graph
            .0
            .node_indices()
            .filter(|&i| {
                matches!(
                    graph.0[i].rest.output,
                    Output::Success(_) | Output::Failed(_)
                )
            })
            .filter(|&i| {
                graph
                    .0
                    .edges_directed(i, Direction::Incoming)
                    .all(|j| graph.0[j.source()].rest.output != Output::NotStarted)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|i| graph.0.remove_node(i))
            .count();
        if cnt > 0 && graph.0.node_count() == 0 {
            // reset to reclaim memory
            info!("reset to reclaim memory");
            graph.0 = Default::default();
        }
    }
}
