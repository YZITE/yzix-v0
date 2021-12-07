#![forbid(
    unsafe_code,
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion
)]

use async_channel::{unbounded, Receiver, Sender};
use async_ctrlc::CtrlC;
use async_lock::Semaphore;
use async_net::TcpListener;
use async_process::{Command, ExitStatus};
use futures_lite::io::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yz_server_executor::Executor;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::proto::{self, OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{ciborium, Utf8Path, Utf8PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Output {
    NotStarted,
    Scheduled,
    Failed(OutputError),
    Success(StoreHash),
}

#[derive(Debug)]
struct NodeMeta {
    output: Output,
    log: Vec<Sender<Response>>,
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

enum MainMessage {
    Schedule {
        graph: build_graph::Graph<()>,
        log: Sender<Response>,
    },
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

struct WorkItemRun {
    cmd: String,
    args: Vec<String>,
    envs: HashMap<String, String>,
}

enum WorkItemKind {
    Run(WorkItemRun),
    UnDump(Dump),
    Dump(HashMap<String, StoreHash>),
}

struct BuiltItem {
    inhash: Option<StoreHash>,
    dump: Option<Dump>,
    outhash: StoreHash,
}

async fn handle_logging<T: futures_lite::io::AsyncRead + std::marker::Unpin>(
    tag: u64,
    bldname: String,
    mut log: Vec<Sender<Response>>,
    pipe: T,
) {
    use futures_lite::{io::AsyncBufReadExt, stream::StreamExt};
    let mut lbs = bit_set::BitSet::with_capacity(log.len());
    let mut stream = futures_lite::io::BufReader::new(pipe).lines();
    while let Some(Ok(content)) = stream.next().await {
        for (lidx, l) in log.iter().enumerate() {
            if l.send(Response {
                tag,
                kind: ResponseKind::LogLine {
                    bldname: bldname.to_string(),
                    content: content.clone(),
                },
            })
            .await
            .is_err()
            {
                lbs.insert(lidx);
            }
        }
        let _ = content;
        let mut cnt = 0;
        log.retain(|_| {
            let mark = lbs.contains(cnt);
            cnt += 1;
            !mark
        });
        lbs.clear();
        if log.is_empty() {
            break;
        }
    }
}

async fn handle_process(
    tag: u64,
    inhash: Option<StoreHash>,
    bldname: String,
    WorkItemRun { cmd, args, envs }: WorkItemRun,
    expect_hash: Option<StoreHash>,
    log: Vec<Sender<Response>>,
) -> Result<BuiltItem, OutputError> {
    let workdir = tempfile::tempdir()?;

    // FIXME: wrap that into a crun invocation
    use async_process::Stdio;
    use futures_lite::future::zip;
    let mut ch = Command::new(cmd)
        // TODO: insert .env_clear()
        .args(args)
        .envs(envs)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(workdir.path())
        .spawn()?;
    let a = handle_logging(
        tag,
        bldname.to_string(),
        log.clone(),
        ch.stdout.take().unwrap(),
    );
    let b = handle_logging(tag, bldname, log, ch.stderr.take().unwrap());
    let exs = zip(zip(a, b), ch.status()).await.1?;
    if exs.success() {
        let dump = Dump::read_from_path(&workdir.path().join("out"))?;
        let hash = StoreHash::hash_complex(&dump);
        if let Some(expect_hash) = expect_hash {
            if hash != expect_hash {
                return Err(OutputError::HashMismatch(expect_hash, hash));
            }
        }
        Ok(BuiltItem {
            inhash,
            dump: Some(dump),
            outhash: hash,
        })
    } else if let Some(x) = exs.code() {
        Err(OutputError::Exit(x))
    } else {
        #[cfg(unix)]
        if let Some(x) = std::os::unix::process::ExitStatusExt::signal(&exs) {
            return Err(OutputError::Killed(x));
        }

        Err(OutputError::Unknown(exs.to_string()))
    }
}

async fn push_response(g_: &build_graph::Graph<NodeMeta>, nid: NodeIndex, kind: ResponseKind) {
    use futures_lite::stream::StreamExt;
    let n = &g_.g[nid];
    let resp = Response {
        tag: n.logtag,
        kind: kind.clone(),
    };
    let mut cont: futures_util::stream::FuturesUnordered<_> =
        n.rest.log.iter().map(|li| li.send(resp.clone())).collect();
    while let Some(_) = cont.next().await {}
}

fn eval_pat(
    store_path: &Utf8Path,
    rphs: &HashMap<String, StoreHash>,
    xs: &[build_graph::CmdArgSnip],
) -> Result<String, String> {
    // NOTE: do not use an iterator chain here,
    // as looping lets us handle string and returning more efficiently
    let mut ret = String::new();
    for x in xs {
        use build_graph::CmdArgSnip as CArgS;
        match x {
            CArgS::String(s) => ret += s,
            CArgS::Placeholder(iname) => {
                if let Some(y) = rphs.get(iname) {
                    ret += store_path.join(y.to_string()).as_str();
                } else {
                    return Err(iname.to_string());
                }
            }
        }
    }
    Ok(ret)
}

fn try_make_work_intern(
    store_path: &Utf8Path,
    g_: &build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) -> Option<Result<WorkItem, OutputError>> {
    let pnw = &g_.g[nid];
    if pnw.rest.output != Output::NotStarted {
        return None;
    }

    // resolve placeholders

    let mut rphs = HashMap::new();
    let mut expect_hash = None;

    for e in g_.g.edges(nid) {
        let (ew, et) = (e.weight(), e.target());
        if let Output::Success(h) = g_.g[et].rest.output {
            match ew {
                build_graph::Edge::AssertEqual => {
                    if let Some(x) = expect_hash {
                        if x != h {
                            return Some(Err(OutputError::HashMismatch(x, h)));
                        }
                    } else {
                        expect_hash = Some(h);
                    }
                }
                build_graph::Edge::Placeholder(iname) => {
                    use std::collections::hash_map::Entry;
                    match rphs.entry(iname.to_string()) {
                        Entry::Occupied(_) => {
                            return Some(Err(OutputError::InputDup(iname.to_string())))
                        }
                        Entry::Vacant(v) => v.insert(h),
                    };
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
            let (cmd, args) = match command
                .iter()
                .map(|i| eval_pat(store_path, &rphs, i))
                .collect::<Result<Vec<_>, _>>()
            {
                Err(e) => return Some(Err(OutputError::InputNotFound(e))),
                Ok(x) if x.is_empty() => return Some(Err(OutputError::EmptyCommand)),
                Ok(mut command) => {
                    let cmd = command.remove(0);
                    (cmd, command)
                }
            };

            let envs = match envs
                .iter()
                .map(|(k, i)| eval_pat(store_path, &rphs, i).map(|j| (k.to_string(), j)))
                .collect::<Result<HashMap<_, _>, _>>()
            {
                Ok(x) => x,
                Err(e) => return Some(Err(OutputError::InputNotFound(e))),
            };

            WorkItemKind::Run(WorkItemRun { cmd, args, envs })
        }
        NK::UnDump { dat } => WorkItemKind::UnDump(dat.clone()),
        NK::Dump => WorkItemKind::Dump(rphs),
    };

    Some(Ok(WorkItem { kind, expect_hash }))
}

async fn schedule(
    store_path: &Utf8Path,
    ex: &Executor<'static>,
    mains: &Sender<MainMessage>,
    jobsem: &Arc<Semaphore>,
    graph: &mut build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) {
    let ret = match try_make_work_intern(store_path, graph, nid) {
        None => return, // inputs missing
        Some(x) => x,
    };

    let inhash = graph.hash_node_inputs(nid, store_path.as_str().as_bytes());

    if let Some(inhash) = inhash {
        if let Ok(real_path) = std::fs::read_link(store_path.join(format!("{}.inp", inhash))) {
            if real_path.is_relative() && real_path.parent().is_none() {
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
                            .await;
                        return;
                    }
                }
            }
        }
    }

    graph.g[nid].rest.output = Output::Scheduled;

    let det = match ret {
        Ok(WorkItem {
            kind: WorkItemKind::Run(wir),
            expect_hash,
        }) => {
            let ni = &graph.g[nid];
            let logtag = ni.logtag;
            let name = ni.name.clone();
            let log = ni.rest.log.clone();
            let jobsem = jobsem.clone();
            let mains = mains.clone();
            ex.spawn(async move {
                let _job = jobsem.acquire().await;
                let det = handle_process(logtag, inhash, name, wir, expect_hash, log).await;
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
            let mut success = true;
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
            kind: WorkItemKind::Dump(hsh),
            expect_hash,
        }) => {
            match hsh
                .into_iter()
                .map(|(k, v)| {
                    Dump::read_from_path(store_path.join(v.to_string()).as_std_path())
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
                        push_response(&graph, nid, ResponseKind::Dump(dump.clone())).await;
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
        Err(oe) => {
            // the output is already set to failed, we just need
            // to send the appropriate log message
            push_response(
                &graph,
                nid,
                ResponseKind::LogLine {
                    bldname: graph.g[nid].name.clone(),
                    content: format!("ERROR: {}", oe),
                },
            )
            .await;
            Err(oe)
        }
    };
    // this is necessary to properly update the progress bar
    mains.send(MainMessage::Done { nid, det }).await;
}

fn main() {
    // set the time zone to avoid funky locale stuff
    std::env::set_var("TZ", "UTC");

    let matches = {
        use clap::{App, Arg};
        App::new("yzix-server")
            .arg(
                Arg::with_name("store")
                    .long("store")
                    .value_name("STORE")
                    .takes_value(true),
            )
            .arg(
                Arg::with_name("bind")
                    .long("bind")
                    .value_name("ADDR:PORT")
                    .takes_value(true),
            )
            .arg(Arg::with_name("auto-repair").long("auto-repair"))
            .get_matches()
    };

    let store_path = Utf8PathBuf::from(matches.value_of("store").unwrap());
    let auto_repair = matches.is_present("auto-repair");

    let cpucnt = num_cpus::get();
    let pbar = indicatif::ProgressBar::new(0);
    let ex = Arc::new(yz_server_executor::ServerExecutor::with_threads(cpucnt));
    let jobsem = Arc::new(Semaphore::new(cpucnt));

    ex.block_on(move |ex| async move {
        let listener = TcpListener::bind(matches.value_of("bind").unwrap())
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
        });

        let mains2 = mains.clone();
        let pbar2 = pbar.clone();
        ex.spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                use futures_lite::{future::zip, AsyncReadExt, AsyncWriteExt};
                // TODO: auth
                let mut stream2 = stream.clone();
                let (logs, logr) = unbounded();
                let mains3 = mains2.clone();
                let pbar3 = pbar2.clone();
                // handle input
                zip(
                    async move {
                        let mut stream = futures_lite::io::BufReader::new(stream);
                        let mut lenbuf = [0u8; 4];
                        while stream.read_exact(&mut lenbuf).await.is_ok() {
                            let len = u32::from_le_bytes(lenbuf);
                            // TODO: make sure that the length isn't too big
                            let mut buf: Vec<u8> = Vec::new();
                            buf.resize(len.try_into().unwrap(), 0);
                            if !stream.read_exact(&mut buf[..]).await.is_ok() {
                                break;
                            }
                            use proto::ControlCommand as C;
                            let cmd: C = match ciborium::de::from_reader(&buf[..]) {
                                Ok(x) => x,
                                Err(_) => break,
                            };
                            if mains3
                                .send(match cmd {
                                    C::Schedule(graph) => MainMessage::Schedule {
                                        graph,
                                        log: logs.clone(),
                                    },
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    // handle output
                    async move {
                        let mut buf: Vec<u8> = Vec::new();
                        while let Ok(x) = logr.recv().await {
                            buf.clear();
                            if let Err(e) = ciborium::ser::into_writer(&x, &mut buf) {
                                pbar3.println(format!("ERROR: {}", e));
                                // TODO: handle error
                            } else {
                                if stream2
                                    .write_all(&u32::to_le_bytes(buf.len().try_into().unwrap()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                if stream2.write_all(&buf[..]).await.is_err() {
                                    break;
                                }
                                if stream2.flush().await.is_err() {
                                    break;
                                }
                            }
                        }
                    },
                )
                .await;
            }
        });

        use MainMessage as MM;
        while let Ok(x) = mainr.recv().await {
            match x {
                MM::Shutdown => break,
                MM::Schedule { graph: graph2, log } => {
                    let trt = graph.take_and_merge(
                        graph2,
                        |()| {
                            pbar.inc_length(1);
                            NodeMeta {
                                output: Output::NotStarted,
                                log: vec![log.clone()],
                            }
                        },
                        |noder| noder.log.push(log.clone()),
                    );
                    for (_, nid) in trt {
                        schedule(&store_path, ex, &mains, &jobsem, &mut graph, nid).await;
                    }
                }
                MM::Done { nid, det } => {
                    let mut node = &mut graph.g[nid];
                    match det {
                        Ok(BuiltItem {
                            inhash,
                            dump,
                            outhash,
                        }) => {
                            pbar.inc(1);
                            if let Some(dump) = dump {
                                let ohs = outhash.to_string();
                                let dstpath = store_path.join(&ohs).into_std_path_buf();
                                let mut do_write = true;
                                if dstpath.exists() {
                                    if auto_repair {
                                        let on_disk_dump = match Dump::read_from_path(&dstpath) {
                                            Ok(on_disk_dump) => {
                                                let on_disk_hash =
                                                    StoreHash::hash_complex(&on_disk_dump);
                                                if on_disk_hash != outhash {
                                                    pbar.println(format!(
                                                        "WARNING: detected data corruption @ {}",
                                                        outhash
                                                    ));
                                                } else if on_disk_dump != dump {
                                                    pbar.println(format!(
                                                        "ERROR: detected hash collision @ {}",
                                                        outhash
                                                    ));
                                                    node.rest.output = Output::Failed(
                                                        OutputError::HashCollision(outhash),
                                                    );
                                                    continue;
                                                } else {
                                                    do_write = false;
                                                }
                                            }
                                            Err(e) => {
                                                pbar.println(format!(
                                                    "WARNING: error while dumping @ {}: {}",
                                                    outhash, e
                                                ));
                                            }
                                        };
                                    }
                                }
                                if do_write {
                                    if let Err(e) = dump.write_to_path(&dstpath, true) {
                                        pbar.println(format!("ERROR: {}", e));
                                        node.rest.output = Output::Failed(e.into());
                                        continue;
                                    }
                                }
                                if let Some(inhash) = inhash {
                                    let inpath = store_path.join(format!("{}.in", inhash));
                                    let target = format!("{}", outhash).into();
                                    let to_dir = match dump {
                                        Dump::Directory(_) => true,
                                        Dump::Regular { .. } => false,
                                        Dump::SymLink { to_dir, .. } => to_dir,
                                    };
                                    if let Err(e) = (Dump::SymLink { target, to_dir })
                                        .write_to_path(inpath.as_std_path(), true)
                                    {
                                        // this is just caching, non-fatal
                                        pbar.println(format!("ERROR: {}", e));
                                    }
                                }
                            }
                            node.rest.output = Output::Success(outhash);
                            // schedule now available jobs
                            let mut next_nodes = graph
                                .g
                                .neighbors_directed(nid, Direction::Incoming)
                                .detach();
                            while let Some(nid2) = next_nodes.next_node(&graph.g) {
                                schedule(&store_path, ex, &mains, &jobsem, &mut graph, nid2).await;
                            }
                        }
                        Err(oe) => {
                            let rk = ResponseKind::LogLine {
                                bldname: node.name.clone(),
                                content: format!("ERROR: {}", oe),
                            };
                            pbar.println(format!("{}: ERROR: {}", node.name, oe));
                            node.rest.output = Output::Failed(oe);
                            push_response(&graph, nid, rk).await;
                        }
                    }
                }
            }

            // garbage collection
            if graph.g.node_count() != 0 {
                // search unnecessary nodes
                let gc: Vec<_> = graph
                    .g
                    .node_indices()
                    .filter(|&i| {
                        matches!(
                            graph.g[i].rest.output,
                            Output::Success(_) | Output::Failed(_)
                        )
                    })
                    .filter(|&i| {
                        graph.g.edges_directed(i, Direction::Incoming).all(|j| {
                            matches!(
                                graph.g[j.source()].rest.output,
                                Output::Success(_) | Output::Failed(_)
                            )
                        })
                    })
                    .collect();
                let cnt = gc.into_iter().map(|i| graph.g.remove_node(i)).count();
                if cnt > 0 {
                    // DEBUG
                    pbar.println(format!("pruned {} node(s)", cnt));
                    if graph.g.node_count() == 0 {
                        // reset to reclaim memory
                        graph.g = Default::default();
                    }
                }
            }
        }
    });
}
