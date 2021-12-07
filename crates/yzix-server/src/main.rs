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
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yz_server_executor::Executor;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::proto::{self, OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{ciborium, Utf8PathBuf};

static STORE_PATH: OnceCell<Utf8PathBuf> = OnceCell::new();

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

impl From<()> for NodeMeta {
    fn from(_: ()) -> NodeMeta {
        NodeMeta {
            output: Output::NotStarted,
            log: Vec::new(),
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
        det: Result<(Dump, StoreHash), OutputError>,
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
    bldname: String,
    WorkItemRun { cmd, args, envs }: WorkItemRun,
    expect_hash: Option<StoreHash>,
    log: Vec<Sender<Response>>,
) -> Result<(Dump, StoreHash), OutputError> {
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
        .spawn()?;
    let a = handle_logging(
        tag,
        bldname.to_string(),
        log.clone(),
        ch.stdout.take().unwrap(),
    );
    let b = handle_logging(tag, bldname, log, ch.stderr.take().unwrap());
    let (((), ()), exs) = zip(zip(a, b), ch.status()).await;
    let exs = exs?;
    if exs.success() {
        // FIXME: serialize the output
        let dump = Dump::Directory(Default::default());
        let hash = StoreHash::hash_complex(&dump);
        if let Some(expect_hash) = expect_hash {
            if hash != expect_hash {
                return Err(OutputError::HashMismatch(expect_hash, hash));
            }
        }
        Ok((dump, hash))
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
                    ret += STORE_PATH.get().unwrap().join(y.to_string()).as_str();
                } else {
                    return Err(iname.to_string());
                }
            }
        }
    }
    Ok(ret)
}

fn try_make_work_intern(
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
                .map(|i| eval_pat(&rphs, i))
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
                .map(|(k, i)| eval_pat(&rphs, i).map(|j| (k.to_string(), j)))
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
    ex: &Executor<'static>,
    mains: &Sender<MainMessage>,
    jobsem: &Arc<Semaphore>,
    graph: &mut build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
) {
    let ret = match try_make_work_intern(graph, nid) {
        None => return, // inputs missing
        Some(x) => x,
    };
    let output: Output = match &ret {
        Err(e) => Output::Failed(e.clone()),
        Ok(_) => Output::Scheduled,
    };
    graph.g[nid].rest.output = output;

    match ret {
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
                let det = handle_process(logtag, name, wir, expect_hash, log).await;
                let _ = mains.send(MainMessage::Done { nid, det }).await;
            })
            .detach();
        }
        Ok(WorkItem {
            kind: WorkItemKind::UnDump(dat),
            expect_hash,
        }) => {
            let hash = StoreHash::hash_complex(&dat);
            let mut success = true;
            let mut det = Ok((dat, hash));
            if let Some(expect_hash) = expect_hash {
                if expect_hash != hash {
                    det = Err(OutputError::HashMismatch(expect_hash, hash));
                }
            }
            mains.send(MainMessage::Done { nid, det }).await;
        }
        Ok(WorkItem {
            kind: WorkItemKind::Dump(hsh),
            expect_hash,
        }) => {
            // TODO: read files from store
            let dat = Dump::Directory(Default::default());
            let hash = StoreHash::hash_complex(&dat);
            let mut success = true;
            let mut det = Ok((dat, hash));
            if let Some(expect_hash) = expect_hash {
                if expect_hash != hash {
                    det = Err(OutputError::HashMismatch(expect_hash, hash));
                }
            }
            mains.send(MainMessage::Done { nid, det }).await;
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
        }
    }
}

fn main() {
    // set the time zone to avoid funky locale stuff
    std::env::set_var("TZ", "UTC");
    STORE_PATH.set("/tmp/yzix-store".into()).unwrap();

    let cpucnt = num_cpus::get();
    let pbar = indicatif::ProgressBar::new(0);
    let ex = Arc::new(yz_server_executor::ServerExecutor::with_threads(cpucnt));
    let jobsem = Arc::new(Semaphore::new(cpucnt));

    ex.block_on(move |ex| async move {
        let listener = TcpListener::bind("127.0.0.1:3669").await.unwrap();

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
        ex.spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                use futures_lite::{future::zip, AsyncReadExt, AsyncWriteExt};
                // TODO: auth
                let mut stream2 = stream.clone();
                let (logs, logr) = unbounded();
                let mains3 = mains2.clone();
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
                        schedule(ex, &mains, &jobsem, &mut graph, nid).await;
                    }
                }
                MM::Done { nid, det } => {
                    let mut node = &mut graph.g[nid];
                    match det {
                        Ok((dump, outphash)) => {
                            // TODO: insert the result into the store
                            node.rest.output = Output::Success(outphash);
                            pbar.inc(1);
                            // schedule now available jobs
                            let mut next_nodes = graph
                                .g
                                .neighbors_directed(nid, Direction::Incoming)
                                .detach();
                            while let Some(nid2) = next_nodes.next_node(&graph.g) {
                                schedule(ex, &mains, &jobsem, &mut graph, nid2).await;
                            }
                        }
                        Err(oe) => {
                            let rk = ResponseKind::LogLine {
                                bldname: node.name.clone(),
                                content: format!("ERROR: {}", oe),
                            };
                            node.rest.output = Output::Failed(oe);

                            push_response(&graph, nid, rk).await;
                        }
                    }
                }
            }
        }
    });
}
