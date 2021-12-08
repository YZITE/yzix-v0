#![forbid(
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion,
    unsafe_code,
    unused_must_use
)]

use async_channel::{unbounded, Receiver, Sender};
use async_lock::Semaphore;
use async_process::Command;
use futures_lite::{future::zip, io as flio, AsyncReadExt, AsyncWriteExt};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::{future::Future, marker::Unpin, sync::Arc};
use yz_server_executor::Executor;
use yzix_core::build_graph::{self, Direction, EdgeRef, NodeIndex};
use yzix_core::proto::{self, OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{ciborium, Utf8Path, Utf8PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct ServerConfig {
    store_path: Utf8PathBuf,
    socket_bind: String,
    bearer_tokens: HashSet<String>,
    auto_repair: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Output {
    NotStarted,
    Scheduled,
    Failed(OutputError),
    Success(StoreHash),
}

#[derive(Clone, Debug)]
enum LogFwdMessage {
    Response(Arc<Response>),
    Subscribe(Sender<Arc<Response>>),
}

#[derive(Debug)]
struct NodeMeta {
    output: Output,
    log: Vec<Sender<LogFwdMessage>>,
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

async fn log_to_bunch<T: Clone>(subs: &mut Vec<Sender<T>>, msg: T) {
    let mut cnt = 0usize;
    let mut bs = bit_set::BitSet::with_capacity(subs.len());
    {
        use futures_lite::stream::StreamExt;
        let mut cont: futures_util::stream::FuturesOrdered<_> =
            subs.iter().map(|li| li.send(msg.clone())).collect();
        while let Some(suc) = cont.next().await {
            if suc.is_ok() {
                bs.insert(cnt);
            }
            cnt += 1;
        }
    }
    cnt = 0;
    subs.retain(|_| {
        let prev_cnt = cnt;
        cnt += 1;
        bs.contains(prev_cnt)
    });
}

fn push_response(
    g_: &mut build_graph::Graph<NodeMeta>,
    nid: NodeIndex,
    kind: ResponseKind,
) -> impl Future<Output = ()> + '_ {
    let n = &mut g_.0[nid];
    log_to_bunch(
        &mut n.rest.log,
        LogFwdMessage::Response(Arc::new(Response {
            tag: n.logtag,
            kind,
        })),
    )
}

async fn handle_logging<T: flio::AsyncRead + Unpin, U: flio::AsyncRead + Unpin>(
    tag: u64,
    bldname: String,
    mut log: Vec<Sender<LogFwdMessage>>,
    pipe1: T,
    pipe2: U,
) {
    use futures_lite::{io::AsyncBufReadExt, stream::StreamExt};
    let mut stream1 = flio::BufReader::new(pipe1).lines();
    let mut stream2 = flio::BufReader::new(pipe2).lines();
    while let Some(content) = futures_lite::future::or(stream1.next(), stream2.next()).await {
        let content = match content {
            Ok(x) => x,
            // TODO: give the client a proper error serialization
            Err(e) => format!("I/O error: {}", e),
        };
        log_to_bunch(
            &mut log,
            LogFwdMessage::Response(Arc::new(Response {
                tag,
                kind: ResponseKind::LogLine {
                    bldname: bldname.to_string(),
                    content: content.clone(),
                },
            })),
        )
        .await;
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
    log: Vec<Sender<LogFwdMessage>>,
) -> Result<BuiltItem, OutputError> {
    let workdir = tempfile::tempdir()?;

    // FIXME: wrap that into a crun invocation
    use async_process::Stdio;
    let mut ch = Command::new(cmd)
        // TODO: insert .env_clear()
        .args(args)
        .envs(envs)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(workdir.path())
        .spawn()?;
    let x = handle_logging(
        tag,
        bldname,
        log,
        ch.stdout.take().unwrap(),
        ch.stderr.take().unwrap(),
    );
    let exs = zip(x, ch.status()).await.1?;
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

fn handle_client_io(
    mains: &Sender<MainMessage>,
    stream: async_net::TcpStream,
    attach_logs: Option<(Arc<str>, Receiver<Arc<Response>>)>,
) -> impl Future<Output = ((), ())> {
    let mainsi = mains.clone();
    let mainso = mains.clone();
    let mut stream2 = stream.clone();
    let (attach_logs_bearer_token, logr) = match attach_logs {
        Some((x, y)) => (Some(x), Some(y)),
        None => (None, None),
    };

    // handle input
    zip(
        async move {
            let mut lenbuf = [0u8; 4];
            let mut buf: Vec<u8> = Vec::new();
            let mut stream = flio::BufReader::new(stream);
            while stream.read_exact(&mut lenbuf).await.is_ok() {
                buf.clear();
                let len = u32::from_le_bytes(lenbuf);
                // TODO: make sure that the length isn't too big
                buf.resize(len.try_into().unwrap(), 0);
                if stream.read_exact(&mut buf[..]).await.is_err() {
                    break;
                }
                use proto::ControlCommand as C;
                let cmd: C = match ciborium::de::from_reader(&buf[..]) {
                    Ok(x) => x,
                    Err(e) => {
                        // TODO: report error to client, maybe?
                        if mainsi
                            .send(MainMessage::Log(format!("CBOR ERROR: {}", e)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if mainsi
                            .send(MainMessage::Log(format!("CBOR ERROR DEBUG: {:?}", buf)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        let val: ciborium::value::Value = match ciborium::de::from_reader(&buf[..]) {
                            Err(_) => break,
                            Ok(x) => x,
                        };
                        if mainsi
                            .send(MainMessage::Log(format!("CBOR ERROR DEBUG: {:#?}", val)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if mainsi
                            .send(MainMessage::Log(format!("CBOR ERROR DEBUG: {:#?}", val.deserialized::<C>())))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        break;
                    },
                };
                if mainsi
                    .send(match cmd {
                        C::Schedule {
                            graph,
                            attach_to_logs,
                        } => MainMessage::Schedule {
                            graph,
                            attach_logs_bearer_token: if attach_to_logs {
                                attach_logs_bearer_token.clone()
                            } else {
                                None
                            },
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
            if let Some(logr) = logr {
                let mut buf: Vec<u8> = Vec::new();
                while let Ok(x) = logr.recv().await {
                    buf.clear();
                    if let Err(e) = ciborium::ser::into_writer(&*x, &mut buf) {
                        // TODO: handle error
                        if mainso
                            .send(MainMessage::Log(format!("CBOR ERROR: {}", e)))
                            .await
                            .is_err()
                        {
                            break;
                        }
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
            }
        },
    )
}

async fn handle_clients_initial(
    mains: Sender<MainMessage>,
    listener: async_net::TcpListener,
    valid_bearer_tokens: HashSet<String>,
) {
    while let Ok((mut stream, _)) = listener.accept().await {
        // auth + options
        let mut lenbuf = [0u8; 4];
        if stream.read_exact(&mut lenbuf).await.is_err() {
            continue;
        }
        let len = u32::from_le_bytes(lenbuf);
        if len >= 0x400 {
            continue;
        }
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(len.try_into().unwrap(), 0);
        if stream.read_exact(&mut buf[..]).await.is_err() {
            continue;
        }
        let opts: proto::ClientOpts = match ciborium::de::from_reader(&buf[..]) {
            Ok(x) => x,
            Err(_) => continue,
        };
        if !valid_bearer_tokens.contains(&opts.bearer_auth) {
            continue;
        }
        if mains
            .send(MainMessage::ClientConn { conn: stream, opts })
            .await
            .is_err()
        {
            break;
        }
    }
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
    let pnw = &g_.0[nid];
    if pnw.rest.output != Output::NotStarted {
        return None;
    }

    // resolve placeholders

    let mut rphs = HashMap::new();
    let mut expect_hash = None;

    for e in g_.0.edges(nid) {
        let (ew, et) = (e.weight(), e.target());
        if let Output::Success(h) = g_.0[et].rest.output {
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
        Err(oe) => {
            // the output is already set to failed, we just need
            // to send the appropriate log message
            push_response(
                graph,
                nid,
                ResponseKind::LogLine {
                    bldname: graph.0[nid].name.clone(),
                    content: format!("ERROR: {}", oe),
                },
            )
            .await;
            Err(oe)
        }
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

    std::fs::create_dir_all(&config.store_path).expect("unable to create store dir");

    let cpucnt = num_cpus::get();
    let pbar = indicatif::ProgressBar::new(0);
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
                MM::Log(logline) => pbar.println(logline),
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
                            |()| {
                                pbar.inc_length(1);
                                NodeMeta {
                                    output: Output::NotStarted,
                                    log: vec![log.clone()],
                                }
                            },
                            |noder| noder.log.push(log.clone()),
                        )
                    } else {
                        graph.take_and_merge(
                            graph2,
                            |()| {
                                pbar.inc_length(1);
                                NodeMeta {
                                    output: Output::NotStarted,
                                    log: vec![],
                                }
                            },
                            |_| {},
                        )
                    };
                    for (_, nid) in trt {
                        schedule(&config.store_path, ex, &mains, &jobsem, &mut graph, nid).await;
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
                            pbar.inc(1);
                            if let Some(dump) = dump {
                                let ohs = outhash.to_string();
                                let dstpath = config.store_path.join(&ohs).into_std_path_buf();
                                let mut do_write = true;
                                if dstpath.exists() {
                                    if config.auto_repair {
                                        match Dump::read_from_path(&dstpath) {
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
                                        }
                                    } else {
                                        do_write = false;
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
                                    let inpath = config.store_path.join(format!("{}.in", inhash));
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
                                .0
                                .neighbors_directed(nid, Direction::Incoming)
                                .detach();
                            while let Some(nid2) = next_nodes.next_node(&graph.0) {
                                schedule(&config.store_path, ex, &mains, &jobsem, &mut graph, nid2)
                                    .await;
                            }
                        }
                        Err(oe) => {
                            let rk = ResponseKind::LogLine {
                                bldname: node.name.clone(),
                                content: format!("ERROR: {}", oe),
                            };
                            pbar.println(format!("{}: ERROR: {}", node.name, oe));
                            node.rest.output = Output::Failed(oe);
                            push_response(&mut graph, nid, rk).await;
                        }
                    }
                }
            }

            // garbage collection
            if graph.0.node_count() == 0 {
                continue;
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
                        // keep pbar and such in sync
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
                .inspect(|i| pbar.println(format!("PRUNED: {:?}", i)))
                .count();
            if cnt > 0 {
                // DEBUG
                pbar.println(format!("pruned {} node(s)", cnt));
                if graph.0.node_count() == 0 {
                    // reset to reclaim memory
                    graph.0 = Default::default();
                }
            }
        }
    });
}
