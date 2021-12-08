#![forbid(
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion,
    unsafe_code,
    unused_must_use
)]

use async_channel::{unbounded, Sender};
use async_lock::Semaphore;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yz_server_executor::Executor;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::proto::{self, OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{Utf8Path, Utf8PathBuf};

mod clients;
use clients::{handle_client_io, handle_clients_initial};
mod utils;
use utils::*;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
    store_path: Utf8PathBuf,
    container_runner: String,
    socket_bind: String,
    bearer_tokens: HashSet<String>,
    auto_repair: bool,
    worker_uid: u32,
    worker_gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Output {
    NotStarted,
    Scheduled,
    Failed(OutputError),
    Success(StoreHash),
}

#[derive(Clone, Debug)]
pub enum LogFwdMessage {
    Response(Arc<Response>),
    Subscribe(Sender<Arc<Response>>),
}

#[derive(Debug)]
pub struct NodeMeta {
    output: Output,
    log: smallvec::SmallVec<[Sender<LogFwdMessage>; 1]>,
}

impl build_graph::ReadOutHash for NodeMeta {
    fn read_out_hash(&self) -> Option<StoreHash> {
        if let Output::Success(x) = &self.output {
            Some(*x)
        } else {
            None
        }
    }
}

pub enum MainMessage {
    ClientConn {
        conn: async_net::TcpStream,
        opts: proto::ClientOpts,
    },
    Schedule {
        graph: build_graph::Graph<()>,
        attach_logs_bearer_token: Option<Arc<str>>,
    },
    Log(String),
    Shutdown,
    Done {
        nid: NodeIndex,
        det: Result<BuiltItem, OutputError>,
    },
}

struct WorkItem {
    expect_hash: Option<StoreHash>,
    kind: WorkItemKind,
}

pub struct WorkItemRun {
    args: Vec<String>,
    envs: HashMap<String, String>,
    new_root: Option<StoreHash>,
}

enum WorkItemKind {
    Run(WorkItemRun),
    UnDump(Arc<Dump>),
    Require(StoreHash),
    Dump(HashMap<String, StoreHash>),
}

pub struct BuiltItem {
    inhash: Option<StoreHash>,
    dump: Option<Arc<Dump>>,
    outhash: StoreHash,
}

const INPUT_REALISATION_EXT: &str = "in";

fn try_make_work_intern(
    store_path: &Utf8Path,
    g_: &build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) -> Option<Result<WorkItem, OutputError>> {
    let pnw = &g_.0[nid];
    if pnw.rest.output != Output::NotStarted {
        return None;
    }

    // resolve placeholders

    let mut rphs = HashMap::new();
    let mut expect_hash = None;
    let mut new_root = None;

    for e in g_.0.edges(nid) {
        let (ew, et) = (e.weight(), e.target());
        if let Output::Success(h) = g_.0[et].rest.output {
            use build_graph::Edge;
            match ew {
                Edge::AssertEqual => {
                    if let Some(x) = expect_hash {
                        if x != h {
                            return Some(Err(OutputError::HashMismatch(x, h)));
                        }
                    } else {
                        expect_hash = Some(h);
                    }
                }
                Edge::Placeholder(iname) => {
                    use std::collections::hash_map::Entry;
                    match rphs.entry(iname.to_string()) {
                        Entry::Occupied(_) => {
                            return Some(Err(OutputError::InputDup(iname.to_string())))
                        }
                        Entry::Vacant(v) => v.insert(h),
                    };
                }
                Edge::Root => {
                    if new_root.is_some() {
                        return Some(Err(OutputError::InputDup("{root}".to_string())));
                    }
                    new_root = Some(h);
                }
            }
        } else {
            return None;
        }
    }

    // evaluate command and envs
    use build_graph::NodeKind as NK;

    let kind = match &pnw.kind {
        NK::Run { command, envs } => {
            let args = match command
                .iter()
                .map(|i| eval_pat(store_path, &rphs, i))
                .collect::<Result<Vec<_>, _>>()
            {
                Err(e) => return Some(Err(OutputError::InputNotFound(e))),
                Ok(x) if x.is_empty() => return Some(Err(OutputError::EmptyCommand)),
                Ok(x) => x,
            };

            let envs = match envs
                .iter()
                .map(|(k, i)| eval_pat(store_path, &rphs, i).map(|j| (k.to_string(), j)))
                .collect::<Result<HashMap<_, _>, _>>()
            {
                Ok(x) => x,
                Err(e) => return Some(Err(OutputError::InputNotFound(e))),
            };

            WorkItemKind::Run(WorkItemRun {
                args,
                envs,
                new_root,
            })
        }
        NK::UnDump { dat } => WorkItemKind::UnDump(dat.clone()),
        NK::Require { hash } => WorkItemKind::Require(*hash),
        NK::Dump => WorkItemKind::Dump(rphs),
    };

    Some(Ok(WorkItem { kind, expect_hash }))
}

async fn schedule(
    config: &Arc<ServerConfig>,
    ex: &Executor<'static>,
    mains: &Sender<MainMessage>,
    jobsem: &Arc<Semaphore>,
    graph: &mut build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) {
    let ret = match try_make_work_intern(&config.store_path, graph, nid) {
        None => return, // inputs missing
        Some(x) => x,
    };

    let inhash = graph.hash_node_inputs(nid, config.store_path.as_str().as_bytes());

    if let Some(inhash) = inhash {
        if let Ok(real_path) = std::fs::read_link(
            config
                .store_path
                .join(format!("{}.{}", inhash, INPUT_REALISATION_EXT)),
        ) {
            if real_path.is_relative() && real_path.parent() == Some(std::path::Path::new("")) {
                if let Some(real_path) = real_path.file_name().and_then(|rp| rp.to_str()) {
                    if let Ok(cahash) = real_path.parse::<StoreHash>() {
                        // lucky case: we have found a valid shortcut!
                        // do not submit the inhash to main, it would only try
                        // to rewrite it, we don't want that.
                        mains
                            .send(MainMessage::Done {
                                nid,
                                det: Ok(BuiltItem {
                                    inhash: None,
                                    outhash: cahash,
                                    dump: None,
                                }),
                            })
                            .await
                            .unwrap();
                        return;
                    }
                }
            }
        }
    }

    graph.0[nid].rest.output = Output::Scheduled;

    let det = match ret {
        Ok(WorkItem {
            kind: WorkItemKind::Run(wir),
            expect_hash,
        }) => {
            let ni = &graph.0[nid];
            let logtag = ni.logtag;
            let config = config.clone();
            let name = ni.name.clone();
            let log = ni.rest.log.clone();
            let jobsem = jobsem.clone();
            let mains = mains.clone();
            ex.spawn(async move {
                let _job = jobsem.acquire().await;
                let det =
                    handle_process(&config, logtag, inhash, name, wir, expect_hash, log).await;
                let _ = mains.send(MainMessage::Done { nid, det }).await;
            })
            .detach();
            return;
        }
        Ok(WorkItem {
            kind: WorkItemKind::UnDump(dump),
            expect_hash,
        }) => {
            let outhash = StoreHash::hash_complex(&dump);
            let mut det = Ok(BuiltItem {
                inhash,
                dump: Some(dump),
                outhash,
            });
            if let Some(expect_hash) = expect_hash {
                if expect_hash != outhash {
                    det = Err(OutputError::HashMismatch(expect_hash, outhash));
                }
            }
            det
        }
        Ok(WorkItem {
            kind: WorkItemKind::Require(outhash),
            expect_hash,
        }) => {
            let mut det = Ok(BuiltItem {
                inhash: None,
                dump: None,
                outhash,
            });
            if let Some(expect_hash) = expect_hash {
                if expect_hash != outhash {
                    det = Err(OutputError::HashMismatch(expect_hash, outhash));
                }
            }
            det
        }
        Ok(WorkItem {
            kind: WorkItemKind::Dump(hsh),
            expect_hash,
        }) => {
            match hsh
                .into_iter()
                .map(|(k, v)| {
                    Dump::read_from_path(config.store_path.join(v.to_string()).as_std_path())
                        .map(|vd| (k, vd))
                })
                .collect()
            {
                Ok(dat) => {
                    let dump = Dump::Directory(dat);
                    let outhash = StoreHash::hash_complex(&dump);
                    let mut success = true;
                    let mut det = None;
                    if let Some(expect_hash) = expect_hash {
                        if expect_hash != outhash {
                            det = Some(Err(OutputError::HashMismatch(expect_hash, outhash)));
                            success = false;
                        }
                    }
                    if success {
                        let dump = Arc::new(dump);
                        push_response(graph, nid, ResponseKind::Dump(dump.clone())).await;
                        Ok(BuiltItem {
                            inhash,
                            dump: Some(dump),
                            outhash,
                        })
                    } else {
                        det.unwrap()
                    }
                }
                Err(e) => Err(e.into()),
            }
        }
        Err(oe) => Err(oe),
    };
    // this is necessary to properly update the progress bar
    // and serialization to local store
    mains.send(MainMessage::Done { nid, det }).await.unwrap();
}

fn main() {
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

    std::fs::create_dir_all(&config.store_path).expect("unable to create store dir");

    let cpucnt = num_cpus::get();
    let ex = Arc::new(yz_server_executor::ServerExecutor::with_threads(cpucnt));
    let jobsem = Arc::new(Semaphore::new(cpucnt));

    ex.block_on(move |ex| async move {
        let listener = async_net::TcpListener::bind(&config.socket_bind)
            .await
            .unwrap();

        // we don't need workers. we just spawn tasks instead
        // this means that it is impossible to fix or modify
        // running or older tasks. (invariant)
        // we manage the graph in the main thread, it is !Sync+!Send

        let (mains, mainr) = unbounded();
        let mut graph = build_graph::Graph::default();

        // install Ctrl+C handler
        let mains2 = mains.clone();
        ex.spawn(async move {
            // wait for shutdown signal
            async_ctrlc::CtrlC::new().unwrap().await;
            let _ = mains2.send(MainMessage::Shutdown).await;
        })
        .detach();

        // handle log forwarding
        let mut logwbearer = HashMap::<String, Sender<LogFwdMessage>>::new();
        for i in &config.bearer_tokens {
            let (logs, logr) = unbounded();
            logwbearer.insert(i.clone(), logs);
            ex.spawn(async move {
                let mut subs = smallvec::SmallVec::new();
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
            })
            .detach();
        }
        let logwbearer = logwbearer;

        ex.spawn(handle_clients_initial(
            mains.clone(),
            listener,
            config.bearer_tokens.clone(),
        ))
        .detach();

        use MainMessage as MM;
        while let Ok(x) = mainr.recv().await {
            match x {
                MM::Shutdown => break,
                MM::Log(logline) => println!("{}", logline),
                MM::ClientConn { conn, opts } => {
                    ex.spawn(handle_client_io(
                        &mains,
                        conn,
                        if opts.attach_to_logs {
                            let (logs, logr) = unbounded();
                            let _ = logwbearer[&opts.bearer_auth]
                                .send(LogFwdMessage::Subscribe(logs))
                                .await;
                            Some((opts.bearer_auth.into(), logr))
                        } else {
                            None
                        },
                    ))
                    .detach();
                }
                MM::Schedule {
                    graph: graph2,
                    attach_logs_bearer_token,
                } => {
                    let trt = if let Some(bearer) = attach_logs_bearer_token {
                        let log = &logwbearer[&*bearer];
                        graph.take_and_merge(
                            graph2,
                            |()| NodeMeta {
                                output: Output::NotStarted,
                                log: smallvec::smallvec![log.clone()],
                            },
                            |noder| noder.log.push(log.clone()),
                        )
                    } else {
                        graph.take_and_merge(
                            graph2,
                            |()| NodeMeta {
                                output: Output::NotStarted,
                                log: smallvec::smallvec![],
                            },
                            |_| {},
                        )
                    };
                    for (_, nid) in trt {
                        schedule(&config, ex, &mains, &jobsem, &mut graph, nid).await;
                    }
                }
                MM::Done { nid, det } => {
                    let mut node = &mut graph.0[nid];
                    match det {
                        Ok(BuiltItem {
                            inhash,
                            dump,
                            outhash,
                        }) => {
                            let ohs = outhash.to_string();
                            let dstpath = config.store_path.join(&ohs).into_std_path_buf();
                            let mut err_output = None;
                            if let Some(dump) = dump {
                                let mut do_write = true;
                                if dstpath.exists() {
                                    if config.auto_repair {
                                        match Dump::read_from_path(&dstpath) {
                                            Ok(on_disk_dump) => {
                                                let on_disk_hash =
                                                    StoreHash::hash_complex(&on_disk_dump);
                                                if on_disk_hash != outhash {
                                                    println!(
                                                        "WARNING: detected data corruption @ {}",
                                                        outhash
                                                    );
                                                } else if on_disk_dump != *dump {
                                                    println!(
                                                        "ERROR: detected hash collision @ {}",
                                                        outhash
                                                    );
                                                    err_output =
                                                        Some(OutputError::HashCollision(outhash));
                                                    do_write = false;
                                                } else {
                                                    do_write = false;
                                                }
                                            }
                                            Err(e) => {
                                                println!(
                                                    "WARNING: error while dumping @ {}: {}",
                                                    outhash, e
                                                );
                                            }
                                        }
                                    } else {
                                        do_write = false;
                                    }
                                }
                                if do_write {
                                    if let Err(e) = dump.write_to_path(&dstpath, true) {
                                        println!("ERROR: {}", e);
                                        err_output = Some(e.into());
                                    }
                                }
                                if err_output.is_none() {
                                    if let Some(inhash) = inhash {
                                        let inpath = config
                                            .store_path
                                            .join(format!("{}.{}", inhash, INPUT_REALISATION_EXT));
                                        let target = format!("{}", outhash).into();
                                        let to_dir = match &*dump {
                                            Dump::Directory(_) => true,
                                            Dump::Regular { .. } => false,
                                            Dump::SymLink { to_dir, .. } => *to_dir,
                                        };
                                        if let Err(e) = (Dump::SymLink { target, to_dir })
                                            .write_to_path(inpath.as_std_path(), true)
                                        {
                                            // this is just caching, non-fatal
                                            println!("ERROR: {}", e);
                                        }
                                    }
                                }
                            }
                            if err_output.is_none() && !dstpath.exists() {
                                err_output = Some(OutputError::Unavailable);
                            }
                            if let Some(e) = err_output {
                                node.rest.output = Output::Failed(e.clone());
                                push_response(&mut graph, nid, ResponseKind::OutputNotify(Err(e)))
                                    .await;
                            } else {
                                node.rest.output = Output::Success(outhash);
                                push_response(
                                    &mut graph,
                                    nid,
                                    ResponseKind::OutputNotify(Ok(outhash)),
                                )
                                .await;
                                // schedule now available jobs
                                let mut next_nodes = graph
                                    .0
                                    .neighbors_directed(nid, Direction::Incoming)
                                    .detach();
                                while let Some(nid2) = next_nodes.next_node(&graph.0) {
                                    schedule(&config, ex, &mains, &jobsem, &mut graph, nid2).await;
                                }
                            }
                        }
                        Err(oe) => {
                            let rk = ResponseKind::LogLine {
                                bldname: node.name.clone(),
                                content: format!("ERROR: {}", oe),
                            };
                            println!("{}: ERROR: {}", node.name, oe);
                            node.rest.output = Output::Failed(oe);
                            push_response(&mut graph, nid, rk).await;
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
                        mains
                            .send(MainMessage::Done {
                                nid: js,
                                det: Err(OutputError::InputFailed(graph.0[je].clone())),
                            })
                            .await
                            .unwrap();
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
            if cnt > 0 {
                // DEBUG
                println!("pruned {} node(s)", cnt);
                if graph.0.node_count() == 0 {
                    // reset to reclaim memory
                    println!("reset to reclaim memory");
                    graph.0 = Default::default();
                }
            }
        }
    });
}
