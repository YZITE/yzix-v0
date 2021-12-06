use async_channel::{unbounded, Receiver, Sender};
use async_ctrlc::CtrlC;
use yz_server_executor::Executor;
use async_lock::Semaphore;
use async_net::tcp::TcpListener;
use async_process::{Command, ExitStatus};
use futures_lite::io::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use yzix_core::build_graph::{self, NodeIndex};
use yzix_core::proto::{self, Response, ResponseKind, OutputError};
use yzix_core::{store::{Dump, Hash as StoreHash}, Direction, ciborium, Utf8PathBuf};
use once_cell::sync::OnceCell;

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
            output: Output::NotDone,
            log: Vec::new(),
        }
    }
}

enum MainMessage {
    Schedule {
        graph: build_graph::Graph<()>,
        log: Sender<Response>,
    },
    Control {
        inner: ControlCommand,
    },
    Shutdown,
    Done {
        nid: NodeIndex,
        det: Result<(Dump, StoreHash), OutputError>,
    },
}

struct WorkItem {
    cmd: String,
    args: Vec<String>,
    envs: HashMap<String, String>,
    expect_hash: Option<StoreHash>,
}

async fn handle_logging<T: futures_lite::io::AsyncRead>(
    tag: u64,
    bldname: String,
    mut log: Vec<Sender<Response>>,
    pipe: T,
) -> std::io::Result<()> {
    use futures_lite::io::AsyncBufReadExt;
    let mut lbs = bit_set::with_capacity(log.len());
    for i in futures_lite::io::BufReader::new(pipe).lines() {
        let content = i.await?;
        for (lidx, l= in log.iter().enumerate() {
            if l.send(Response {
                tag,
                kind: ResponseKind::LogLine { bldname: bldname.to_string(), content: content.clone() },
            }).await.is_err() {
                lbs.insert(lidx);
            }
        }
        let _ = content;
        let mut cnt = 0;
        log.retain(|_| {
            let mark = lbs.contains(cnt);
            cnt += 1;
            !mark
        })
        lbs.clear();
        if log.is_empty() {
            break;
        }
    }
}

async fn handle_process(
    ex: &Executor<'static>,
    tag: u64,
    bldname: &str,
    WorkItem { cmd, args, envs, expect_hash }: WorkItem,
    log: Vec<Sender<Response>>,
) -> Result<(Dump, Hash), OutputError> {
    // FIXME: wrap that into a crun invocation
    use async_process::Stdio;
    let ch = Command::new(cmd)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    ex.spawn(handle_logging(tag, bldname.to_string(), ch.stdout.take().unwrap(), log.clone()))
        .detach();
    ex.spawn(handle_logging(tag, bldname.to_string(), ch.stderr.take().unwrap(), log))
        .detach();
    let exs = ch.status().await?;
    if exs.success() {
        // FIXME: serialize the output
        let dump = Dump::Directory(Default::default());
        let hash = StoreHash::hash_complex(&dump);
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

async fn push_response(g_: &build_graph::Graph<NodeMeta>, i: NodeIndex, kind: ResponseKind) {
    let n = &g_.g[i];
    let _ = n.rest.log.send(Response { tag: *n.logtag, kind }).await;
}

trait GraphExt {
    /// check if a node should be scheduled
    ///
    /// this requires that:
    /// * the node isn't started yet
    /// * all dependencies succeeded
    fn chk_should_schedule(&self, i: NodeIndex) -> bool;

    /// if this returns an error, the caller should log it
    fn try_make_work(&mut self, i: NodeIndex) -> Option<Result<WorkItem, OutputError>>;
}

impl GraphExt for build_graph::Graph<NodeMeta> {
    fn chk_should_schedule(&self, i: NodeIndex) -> bool {
        self.g[x].rest.output == Output::NotStarted && self.g.neighbors(x).all(|i| if let Output::Success(_) = self.g[i].rest.output { true } else { false })
    }

    fn try_make_work(&mut self, i: NodeIndex) -> Option<Result<WorkItem, OutputError>> {
        fn eval_pat(rphs: &HashMap<String, StoreHash>, xs: &[build_graph::CmdArgSnip]) -> Result<String, String> {
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
                    },
                }
            }
            Ok(ret)
        }

        fn intern(g_: &build_graph::Graph<NodeMeta>, i: NodeIndex) -> Option<Result<WorkItem, OutputError>> {
            let pnw = &g_.g[x];
            if pnw.rest.output != Output::NotStarted {
                return None;
            }

            // resolve placeholders

            let mut rphs = HashMap::new();
            let mut expect_hash = None;

            for e in self.g.edges(x) {
                let (ew, et) = (e.weight(), e.target());
                if let Output::Success(h) = self.g[et].rest.output {
                    match ew {
                        build_graph::Edge::AssertEqual => {
                            if let Some(x) = expect_hash {
                                if x != h {
                                    return Some(Err(OutputError::HashMismatch(x, h)));
                                }
                            } else {
                                expect_hash = Some(h);
                            }
                        },
                        build_graph::Edge::Placeholder(iname) => {
                            use std::collections::hash_map::Entry;
                            match rphs.entry(iname.to_string()) {
                                Entry::Occupied(_) => return Some(Err(OutputError::InputDup(iname.to_string()))),
                                Entry::Vacant(v) => v.insert(h),
                            };
                        },
                    }
                } else {
                    return None;
                }
            }

            // evaluate command and envs

            let mut command: Vec<_> = pnw.command.iter().map(|i| eval_pat(&rphs, i)).collect::<Result<_, _>>().map_err(OutputError::InvalidInput)?;

            if command.is_empty() {
                return Some(Err(OutputError::EmptyCommand));
            }
            let cmd = command.remove(0);
            let args = command;

            let envs: HashMap<_, _> = pnw.envs.iter().map(|(k, i)| eval_pat(&rphs, i).map(|j| (k, j))).collect::<Result<_, _>>().map_err(OutputError::InvalidInput)?;

            Some(Ok(WorkItem {
                cmd,
                args,
                envs,
                expect_hash,
            }))
        }

        let (new_out, ret) = match intern(self, i) {
            Err(e) => (Output::Failed(e.clone()), Err(e)),
            x => (Output::Scheduled, x),
        };
        self.g[x].rest.output = new_out;
        Some(ret)
    }
}

async fn schedule(ex: &Executor<'static>, mains: &Sender<MainMessage>, jobsem: &Arc<Semaphore>, graph: &mut build_graph::Graph<NodeMeta>, nid: NodeIndex) {
                                match graph.try_make_work(nid) {
                                    None => {}, // inputs missing
                                    Some(Ok(wi)) => {
                                        let ni = &graph.g[nid];
                                        let logtag = *ni.logtag;
                                        let name = ni.name.clone();
                                        let log = ni.log.rest.clone();
                                        let jobsem = jobsem.clone();
                                        let mains = mains.clone();
                                        ex.spawn(async move {
                                            let _job = jobsem.acquire().await;
                                            let det = handle_process(ex, logtag, name, wi, log).await;
                                            let _ = mains.send(MainMessage::Done { nid, det }).await;
                                        }).detach();
                                    },
                                    Some(Err(oe)) => {
                                        // the output is already set to failed, we just need
                                        // to send the appropriate log message
                                        push_response(&graph, nid, ResponseKind::LogLine { bldname: graph.g[nid].name.clone(), content: format!("ERROR: {}", oe) }).await;
                                    },
                                }
}

fn main() {
    // set the time zone to avoid funky locale stuff
    std::env::set_var("TZ", "UTC");
    STORE_PATH.set("/tmp/yzix-store".into()).unwrap();

    let cpucnt = num_cpus::get();
    let pbar = indicatif::ProgressBar::new();
    let ex = yz_server_executor::ServerExecutor::with_threads(cpucnt);
    let listener = TcpListener::bind("127.0.0.1:3669").unwrap();
    let jobsem = Arc::new(Semaphore::new(cpucnt));

    ex.block_on(move |ex| {
        // we don't need workers. we just spawn tasks instead
        // this means that it is impossible to fix or modify
        // running or older tasks. (invariant)
        // we manage the graph in the main thread, it is !Sync+!Send

        let (rcts, rctr) = unbounded();
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
                // TODO: auth
                let stream2 = stream.clone();
                let (logs, logr) = unbounded();
                // handle input
                let mains3 = mains2.clone();
                ex.spawn(async move {
                    let mut lenbuf = [0u8; 4];
                    while stream.read_exact(&mut lenbuf).await.is_ok() {
                        let len = u32::from_le_bytes(lenbuf);
                        // TODO: make sure that the length isn't too big
                        let mut buf: Vec<u8> = Vec::new();
                        buf.resize(len, 0);
                        if !stream.read_exact(&mut buf[..]).await.is_ok() {
                            break;
                        }
                        use ControlCommand as C;
                        let cmd: C = match ciborium::de::from_reader(&mut buf) {
                            Ok(x) => x,
                            Err(_) => break,
                        };
                        if mains3.send(match cmd {
                            C::Schedule(graph) => MainCommand::Schedule {
                                graph,
                                log: logs.clone(),
                            },
                        }).await.is_err() {
                            break;
                        }
                    }
                });
                // handle output
                ex.spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    while let Ok(x) = logr.recv().await {
                        buf.clear();
                        if let Err(e) = ciborium::ser::to_writer(&mut buf) {
                            // TODO: handle error
                        } else {
                            if stream2.write_all(&u32::to_le_bytes(buf.len())).await.is_err() {
                                break;
                            }
                            if stream2.write_all(&buf[..]).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        });

        use MainMessage as MM;
        while let Ok(x) = mains.recv().await {
            match x {
                MM::Shutdown => break,
                MM::Control { inner } => {
                    use ControlCommand as CM;
                    match inner {
                        CM::Schedule(graph2) => {
                            let trt = graph.take_and_merge(graph2, |()| {
                                pbar.inc_length(1);
                            NodeMeta {
                                output: Output::NotDone,
                                log: vec![log.clone()],
                            }
                            }, |noder| noder.log.push(log.clone()));
                            for (_, nid) in trt {
                                schedule(ex, &mains, &jobsem, &mut graph, nid).await;
                            }
                        }
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
                            for e in graph.edges_directed(Direction::Incoming) {
                                schedule(ex, &mains, &jobsem, &mut graph, e.source()).await;
                            }
                        }
                        Err(oe) => {
                            push_response(&graph, nid, ResponseKind::LogLine { bldname: graph.g[nid].name.clone(), content: format!("ERROR: {}", oe) }).await;
                            node.rest.output = Output::Failed(oe);
                        }
                    }
                }
            }
        }
    });
}
