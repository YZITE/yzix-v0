#![forbid(
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion,
    unsafe_code,
    unused_must_use
)]

use async_channel::{unbounded, Sender};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::proto::{self, OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{Utf8Path, Utf8PathBuf};

mod clients;
use clients::{handle_client_io, handle_clients_initial};
mod fetch;
use fetch::{mangle_result as mangle_fetch_result, ConnPool as FetchConnPool};
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
        conn: tokio::net::TcpStream,
        opts: proto::ClientOpts,
    },
    Schedule {
        graph: build_graph::Graph<()>,
        attach_logs: Option<AttachLogsKind>,
    },
    Log(String),
    Shutdown,
    Done {
        nid: NodeIndex,
        det: Result<BuiltItem, OutputError>,
    },
}

pub enum AttachLogsKind {
    // attach logs via bearer
    Bearer(Arc<str>),
    // dup existing logs
    Dup(smallvec::SmallVec<[Sender<LogFwdMessage>; 1]>),
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
    Fetch(yzix_core::Url),
    Eval(StoreHash),
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
    use build_graph::{Edge, NodeKind as NK};

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
            match ew {
                Edge::AssertEqual => {
                    if let Some(x) = expect_hash {
                        if x != h {
                            return Some(Err(OutputError::HashMismatch {
                                expected: x,
                                got: h,
                            }));
                        }
                    } else {
                        expect_hash = Some(h);
                    }
                }
                Edge::Placeholder(iname) => {
                    use std::collections::hash_map::Entry;
                    match rphs.entry(iname.to_string()) {
                        Entry::Occupied(_) => return Some(Err(OutputError::InputDup(ew.clone()))),
                        Entry::Vacant(v) => v.insert(h),
                    };
                }
                Edge::Root => {
                    if new_root.is_some() {
                        return Some(Err(OutputError::InputDup(ew.clone())));
                    }
                    new_root = Some(h);
                }
            }
        } else {
            return None;
        }
    }

    // evaluate command and envs

    let kind = match &pnw.kind {
        NK::Run { command, envs } => {
            let args = match command
                .iter()
                .map(|i| eval_pat(store_path, &rphs, i))
                .collect::<Result<Vec<_>, _>>()
            {
                Err(e) => return Some(Err(OutputError::InputNotFound(Edge::Placeholder(e)))),
                Ok(x) if x.is_empty() => return Some(Err(OutputError::EmptyCommand)),
                Ok(x) => x,
            };

            let envs = match envs
                .iter()
                .map(|(k, i)| eval_pat(store_path, &rphs, i).map(|j| (k.to_string(), j)))
                .collect::<Result<HashMap<_, _>, _>>()
            {
                Ok(x) => x,
                Err(e) => return Some(Err(OutputError::InputNotFound(Edge::Placeholder(e)))),
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

        NK::Fetch { url, hash } => {
            if let Some(x) = expect_hash {
                if let &Some(y) = hash {
                    if x != y {
                        return Some(Err(OutputError::HashMismatch {
                            expected: x,
                            got: y,
                        }));
                    }
                }
            } else {
                expect_hash = hash.as_ref().copied();
            }
            // this is also what's checked by reqwest::IntoUrl
            if !url.has_host() {
                return Some(Err(OutputError::InvalidUrl(url.clone())));
            }
            WorkItemKind::Fetch(url.clone())
        }

        // NOTE: we deliberately don't care about expect_hash in Eval nodes for now
        NK::Eval => match new_root {
            Some(hash) => WorkItemKind::Eval(hash),
            None => return Some(Err(OutputError::InputNotFound(Edge::Root))),
        },
    };

    Some(Ok(WorkItem { kind, expect_hash }))
}

async fn schedule(
    config: Arc<ServerConfig>,
    mains: Sender<MainMessage>,
    jobsem: Arc<tokio::sync::Semaphore>,
    connpool: FetchConnPool,
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
            tokio::spawn(async move {
                let _job = jobsem.acquire().await;
                let det =
                    handle_process(&config, logtag, inhash, name, wir, expect_hash, log).await;
                let _ = mains.send(MainMessage::Done { nid, det }).await;
            });
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
                    det = Err(OutputError::HashMismatch {
                        expected: expect_hash,
                        got: outhash,
                    });
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
                    det = Err(OutputError::HashMismatch {
                        expected: expect_hash,
                        got: outhash,
                    });
                }
            }
            det
        }
        Ok(WorkItem {
            kind: WorkItemKind::Fetch(url),
            expect_hash,
        }) => {
            let mut det = None;

            if let Some(expect_hash) = expect_hash {
                if std::path::Path::new(&config.store_path.join(expect_hash.to_string())).is_file()
                {
                    det = Some(Ok(BuiltItem {
                        inhash: None,
                        dump: None,
                        outhash: expect_hash,
                    }));
                }
            }

            if let Some(det) = det {
                det
            } else {
                let connpool = connpool.clone();
                tokio::spawn(async move {
                    let client = connpool.pop().await;
                    let resp = client.get(url.clone()).send().await;
                    let _ = connpool.push(client).await;
                    let _ = mains
                        .send(MainMessage::Done {
                            nid,
                            det: mangle_fetch_result(url, resp, expect_hash).await,
                        })
                        .await;
                });
                return;
            }
        }
        Ok(WorkItem {
            kind: WorkItemKind::Eval(outhash),
            ..
        }) => {
            let log = graph.0[nid].rest.log.clone();
            let store_path = config.store_path.clone();
            let mains = mains.clone();
            tokio::spawn(async move {
                let det = match read_graph_from_store(&store_path, outhash) {
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
                        Ok(BuiltItem {
                            inhash: None,
                            dump: None,
                            outhash,
                        })
                    }
                    Err(e) => Err(e),
                };
                let _ = mains.send(MainMessage::Done { nid, det }).await;
            });
            return;
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
                            det = Some(Err(OutputError::HashMismatch {
                                expected: expect_hash,
                                got: outhash,
                            }));
                            success = false;
                        }
                    }
                    if success {
                        push_response(graph, nid, ResponseKind::Dump(dump.clone())).await;
                        Ok(BuiltItem {
                            inhash,
                            dump: Some(Arc::new(dump)),
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

    std::fs::create_dir_all(&config.store_path).expect("unable to create store dir");

    let cpucnt = num_cpus::get();
    let jobsem = Arc::new(tokio::sync::Semaphore::new(cpucnt));

    // setup fetch connection pool
    let connpool = FetchConnPool::default();

    {
        use std::time::Duration;
        let cto = Duration::from_secs(30);
        let tow = Duration::from_secs(600);
        for _ in 0..cpucnt {
            connpool
                .push(
                    reqwest::Client::builder()
                        .user_agent("Yzix 0.1 server")
                        .connect_timeout(cto)
                        .timeout(tow)
                        .build()
                        .expect("unable to setup HTTP client"),
                )
                .await;
        }
    }

    let listener = tokio::net::TcpListener::bind(&config.socket_bind)
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
    tokio::spawn(async move {
        // wait for shutdown signal
        tokio::signal::ctrl_c().await.unwrap();
        let _ = mains2.send(MainMessage::Shutdown).await;
    });

    // handle log forwarding
    let mut logwbearer = HashMap::<String, Sender<LogFwdMessage>>::new();
    for i in &config.bearer_tokens {
        let (logs, logr) = unbounded();
        logwbearer.insert(i.clone(), logs);
        tokio::spawn(async move {
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
            MM::Log(logline) => println!("{}", logline),
            MM::ClientConn { conn, opts } => {
                tokio::spawn(handle_client_io(
                    mains.clone(),
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
                ));
            }
            MM::Schedule {
                graph: graph2,
                attach_logs,
            } => {
                let trt = if let Some(attach_logs) = attach_logs {
                    let log = match attach_logs {
                        AttachLogsKind::Bearer(bearer) => {
                            smallvec::smallvec![logwbearer[&*bearer].clone()]
                        }
                        AttachLogsKind::Dup(log) => log,
                    };
                    graph.take_and_merge(
                        graph2,
                        |()| NodeMeta {
                            output: Output::NotStarted,
                            log: log.clone(),
                        },
                        |noder| noder.log.extend(log.clone()),
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
                    schedule(
                        config.clone(),
                        mains.clone(),
                        jobsem.clone(),
                        connpool.clone(),
                        &mut graph,
                        nid,
                    )
                    .await;
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
                            push_response(&mut graph, nid, ResponseKind::OutputNotify(Ok(outhash)))
                                .await;
                            // schedule now available jobs
                            let mut next_nodes = graph
                                .0
                                .neighbors_directed(nid, Direction::Incoming)
                                .detach();
                            while let Some(nid2) = next_nodes.next_node(&graph.0) {
                                schedule(
                                    config.clone(),
                                    mains.clone(),
                                    jobsem.clone(),
                                    connpool.clone(),
                                    &mut graph,
                                    nid2,
                                )
                                .await;
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
}
