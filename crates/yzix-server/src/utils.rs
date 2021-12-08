use crate::{BuiltItem, LogFwdMessage, NodeMeta, WorkItemRun};
use async_channel::Sender;
use async_process::Command;
use futures_lite::{future::zip, io as flio};
use std::collections::{HashMap, HashSet};
use std::{future::Future, marker::Unpin, path::Path, sync::Arc};
use yzix_core::build_graph::{self, NodeIndex};
use yzix_core::proto::{OutputError, Response, ResponseKind};
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::Utf8Path;

pub async fn log_to_bunch<T: Clone>(subs: &mut smallvec::SmallVec<[Sender<T>; 1]>, msg: T) {
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
    mut log: smallvec::SmallVec<[Sender<LogFwdMessage>; 1]>,
    pipe1: T,
    pipe2: U,
) {
    use futures_lite::{io::AsyncBufReadExt, stream::StreamExt};
    let mut stream1 = flio::BufReader::new(pipe1).lines();
    let mut stream2 = flio::BufReader::new(pipe2).lines();
    // FIXME: refactor into channel-forwarding, because `or` sometimes looses data
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

fn write_linux_ocirt_spec(
    config: &crate::ServerConfig,
    rootdir: &Path,
    args: Vec<String>,
    env: Vec<String>,
    specpath: &Path,
) -> std::io::Result<()> {
    // NOTE: windows support is harder, because we need to use a hyperv container there...
    use oci_spec::runtime as osr;
    let mut mounts = osr::get_default_mounts();
    mounts.push(
        osr::MountBuilder::default()
            .destination(&config.store_path)
            .source(&config.store_path)
            .typ("none")
            .options(vec!["bind".to_string()])
            .build()
            .unwrap(),
    );
    let mut caps = HashSet::new();
    caps.insert(osr::Capability::BlockSuspend);
    let mut ropaths = osr::get_default_readonly_paths();
    ropaths.push(config.store_path.to_string());
    let spec = osr::SpecBuilder::default()
        .version("1.0.0")
        .root(
            osr::RootBuilder::default()
                .path(rootdir)
                .readonly(false)
                .build()
                .unwrap(),
        )
        .hostname("yzix")
        .mounts(mounts)
        .process(
            osr::ProcessBuilder::default()
                .terminal(false)
                .user(
                    osr::UserBuilder::default()
                        .uid(0u32)
                        .gid(0u32)
                        .build()
                        .unwrap(),
                )
                .args(args)
                .env(env)
                .cwd("/")
                .capabilities(
                    osr::LinuxCapabilitiesBuilder::default()
                        .bounding(caps.clone())
                        .effective(caps.clone())
                        .inheritable(caps.clone())
                        .permitted(caps.clone())
                        .ambient(caps)
                        .build()
                        .unwrap(),
                )
                .rlimits(vec![osr::LinuxRlimitBuilder::default()
                    .typ(osr::LinuxRlimitType::RlimitNofile)
                    .hard(4096u64)
                    .soft(1024u64)
                    .build()
                    .unwrap()])
                .no_new_privileges(true)
                .build()
                .unwrap(),
        )
        .linux(
            osr::LinuxBuilder::default()
                .uid_mappings(vec![osr::LinuxIdMappingBuilder::default()
                    .host_id(config.worker_uid)
                    .container_id(0u32)
                    .size(1u32)
                    .build()
                    .unwrap()])
                .gid_mappings(vec![osr::LinuxIdMappingBuilder::default()
                    .host_id(config.worker_gid)
                    .container_id(0u32)
                    .size(1u32)
                    .build()
                    .unwrap()])
                .namespaces(osr::get_default_namespaces())
                .masked_paths(osr::get_default_maskedpaths())
                .readonly_paths(ropaths)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    std::fs::write(
        specpath,
        serde_json::to_string(&spec).expect("unable to serialize OCI spec"),
    )
}

fn random_name() -> String {
    use rand::prelude::*;
    let mut rng = rand::thread_rng();
    std::iter::repeat(())
        .take(20)
        .map(|()| char::from_u32(rng.gen_range(b'a'..=b'z').into()).unwrap())
        .collect::<String>()
}

pub async fn handle_process(
    config: &crate::ServerConfig,
    tag: u64,
    inhash: Option<StoreHash>,
    bldname: String,
    WorkItemRun {
        args,
        envs,
        new_root,
    }: WorkItemRun,
    expect_hash: Option<StoreHash>,
    log: smallvec::SmallVec<[Sender<LogFwdMessage>; 1]>,
) -> Result<BuiltItem, OutputError> {
    let workdir = tempfile::tempdir()?;
    let rootdir = workdir.path().join("rootfs");

    if let Some(new_root) = new_root {
        // to work-around read-only stuff and such, copy the tree...
        Dump::read_from_path(
            &config
                .store_path
                .join(new_root.to_string())
                .into_std_path_buf(),
        )?
        .write_to_path(&rootdir, true)?;
        let mut perms = std::fs::metadata(&rootdir)?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&rootdir, perms)?;
    } else {
        std::fs::create_dir_all(&rootdir)?;
    }

    // generate spec
    write_linux_ocirt_spec(
        config,
        &rootdir,
        args,
        envs.into_iter()
            .map(|(i, j)| format!("{}={}", i, j))
            .collect(),
        &workdir.path().join("config.json"),
    )?;

    use async_process::Stdio;
    let mut ch = Command::new(&config.container_runner)
        .args(vec!["run".to_string(), format!("yzix-{}", random_name())])
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
        let dump = Dump::read_from_path(&rootdir.join("out"))?;
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

pub fn memory_usage() -> usize {
    // check memory usage
    let stats = super::GLOBAL.stats();
    let mut mw = stats.bytes_allocated - stats.bytes_deallocated;
    let realloc_abs: usize = stats.bytes_reallocated.abs().try_into().unwrap();
    if stats.bytes_reallocated > 0 {
        mw += realloc_abs;
    } else {
        mw -= realloc_abs;
    }
    eprintln!("\n{:#?} -> {}\n", stats, mw);
    mw
}
