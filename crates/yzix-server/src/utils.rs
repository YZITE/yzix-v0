use async_channel::{Sender};
use async_process::Command;
use futures_lite::{future::zip, io as flio};
use std::collections::{HashMap};
use std::{future::Future, marker::Unpin, sync::Arc};
use yzix_core::build_graph::{self, NodeIndex};
use yzix_core::proto::{OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::{Utf8Path};
use crate::{NodeMeta, LogFwdMessage, WorkItemRun, BuiltItem};

pub async fn log_to_bunch<T: Clone>(subs: &mut Vec<Sender<T>>, msg: T) {
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

pub fn push_response(
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

pub async fn handle_process(
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

pub fn eval_pat(
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
