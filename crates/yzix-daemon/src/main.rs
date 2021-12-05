use async_channel::{unbounded, Receiver, Sender};
use async_ctrlc::CtrlC;
use async_executor::Executor;
use async_net::unix::UnixListener;
use async_process::{Command, ExitStatus};
use futures_lite::io::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use yzix_core::build_graph::{self, NodeIndex};
use yzix_core::{proto::ControlCommand, store::Dump, StoreHash, StoreName, StorePath};

#[derive(Clone, Debug)]
enum Output {
    NotDone,
    Failed,
    Success(StorePath),
}

#[derive(Debug)]
struct NodeMeta {
    output: Output,
    log: Sender<String>,
}

impl From<()> for NodeMeta {
    fn from(_: ()) -> NodeMeta {
        NodeMeta {
            output: Output::NotDone,
            log: unbounded().0,
        }
    }
}

struct WorkItem {
    name: StoreName,
    command: (String, Vec<String>),
    log: Sender<String>,
    nid: NodeIndex,
}

enum DoneDetMessage {
    Ok(Dump),
    ExitErr(ExitStatus),
    IoErr(std::io::Error),
}

enum MainMessage {
    Control { inner: ControlCommand },
    Shutdown,
    Done { nid: NodeIndex, det: DoneDetMessage },
}

async fn handle_logging<T: futures_lite::io::AsyncRead>(
    log: Sender<String>,
    pipe: T,
) -> std::io::Result<()> {
    use futures_lite::io::AsyncBufReadExt;
    for i in futures_lite::io::BufReader::new(pipe).lines() {
        if log.send(i.await?).await.is_err() {
            break;
        }
    }
}

fn main() {
    let pbar = indicatif::ProgressBar::new();
    let base = yzix_core::StoreBase::Local("/tmp/yzix-store".into());
    let ex = yz_server_executor::ServerExecutor::new();
    let listener = UnixListener::bind("/tmp/yzix-srv").unwrap();

    ex.block_on(move |ex| {
        // we don't need workers. we just spawn tasks instead
        // this means that it is impossible to fix or modify
        // running or older tasks. (invariant)
        // we manage the graph in the main thread, it is !Sync+!Send

        let (rcts, rctr) = unbounded();
        let (mains, mainr) = unbounded();
        let (ws, wr) = unbounded();

        let mut graph = build_graph::Graph::default();

        // install Ctrl+C handler
        let mains2 = mains.clone();
        ex.spawn(async move {
            // wait for shutdown signal
            async_ctrlc::CtrlC::new().unwrap().await;
            let _ = mains2.send(MainMessage::Shutdown).await;
        });

        // start workers
        for i in 0..num_cpus::get() {
            let wr = wr.clone();
            let mains = mains.clone();
            ex.spawn(async move {
                while let Ok(WorkItem {
                    name,
                    command: (cmd, args),
                    log,
                    nid,
                }) = wr.recv().await
                {
                    // FIXME: wrap that into a crun invocation
                    use async_process::Stdio;
                    let det = match Command::new(cmd)
                        .args(args)
                        .stdin(Stdio::null())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                    {
                        Err(e) => DoneDetMessage::IoErr(e),
                        Ok(ch) => {
                            ex.spawn(handle_logging(ch.stdout.take().unwrap())).detach();
                            ex.spawn(handle_logging(ch.stderr.take().unwrap())).detach();
                            match ch.status() {
                                Err(e) => DoneDetMessage::IoErr(e),
                                Ok(x) if x.success() => {
                                    // FIXME: serialize the output
                                    DoneDetMessage::Ok(Dump::Directory(Default::default()))
                                }
                                Ok(x) => DoneDetMessage::ExitErr(x),
                            }
                        }
                    };
                    if mains.send(MainMessage::Done { nid, det }).await.is_err() {
                        break;
                    }
                }
            });
        }

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
                        let cmd = match ciborium::de::from_reader(buf) {
                            Ok(x) => x,
                            Err(_) => break,
                        };
                        if mains3.send(cmd).await.is_err() {
                            break;
                        }
                    }
                });
                // handle output
                ex.spawn(async move {
                    while let Ok(x) = logr.recv().await {
                        if stream2.write(&x).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        use MainMessage as MM;
        while let Ok(x) = mains.recv() {
            match x {
                MM::Shutdown => break,
                MM::Control { inner } => {
                    use ControlCommand as CM;
                    match inner {
                        CM::Schedule(graph2) => {
                            runner.schedule();
                            let existing_nodes: HashSet<_> = graph.g.node_indices().collect();
                            let trt = graph.take_and_merge(graph2);
                            for i in graph
                                .g
                                .node_indices()
                                .collect::<HashSet<_>>()
                                .differece(&existing_nodes)
                            {
                                // setup log stuff
                                // TODO: maybe we should map the initial graph instead?
                                graph.g[i].rest.log = log;
                            }
                        }
                    }
                }
                MM::Done { nid, det } => {
                    let mut node = &mut graph.g[nid];
                    use DoneDetMessage as DD;
                    node.rest.output = match det {
                        DD::Ok(x) => {
                            let outph = StoreHash::hash_complex(&x);
                            // TODO: insert the result into the store
                            Output::Success()
                        }
                        DD::ExitErr(es) => {
                            // TODO: log failure into associated logger
                            Output::Failed
                        }
                        DD::IoErr(ioe) => {
                            // TODO: log failure into associated logger
                            Output::Failed
                        }
                    }
                }
            }
        }
    });
}
