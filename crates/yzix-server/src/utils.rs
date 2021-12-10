use crate::{BuiltItem, LogFwdMessage, NodeMeta, WorkItemRun};
use async_channel::Sender;
use async_process::Command;
use futures_util::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    stream::StreamExt,
};
use std::collections::HashSet;
use std::{future::Future, marker::Unpin, path::Path, sync::Arc};
use yzix_core::build_graph;
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::Utf8Path;
use yzix_core::{OutputError, Response, ResponseKind};

pub async fn log_to_bunch<T: Clone>(subs: &mut smallvec::SmallVec<[Sender<T>; 1]>, msg: T) {
    let mut cnt = 0usize;
    let mut bs = bit_set::BitSet::with_capacity(subs.len());
    {
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
    n: &mut build_graph::Node<NodeMeta>,
    kind: ResponseKind,
) -> impl Future<Output = ()> + '_ {
    log_to_bunch(
        &mut n.rest.log,
        LogFwdMessage::Response(Arc::new(Response {
            tag: n.logtag,
            kind,
        })),
    )
}

async fn handle_logging<T: AsyncRead + Unpin>(
    tag: u64,
    bldname: &str,
    mut log: smallvec::SmallVec<[Sender<LogFwdMessage>; 1]>,
    pipe: T,
) {
    let mut stream = BufReader::new(pipe).lines();
    while let Some(content) = stream.next().await {
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
                    content,
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
    need_store_mount: bool,
) -> std::io::Result<()> {
    // NOTE: windows support is harder, because we need to use a hyperv container there...
    use oci_spec::runtime as osr;
    let mut mounts = osr::get_default_mounts();
    // rootless: filter 'gid=' options in mount args
    mounts
        .iter_mut()
        .filter(|i| {
            i.options()
                .as_ref()
                .map(|i2| i2.iter().any(|j: &String| j.starts_with("gid=")))
                .unwrap_or(false)
        })
        .for_each(|i| {
            let newopts: Vec<_> = i
                .options()
                .as_ref()
                .unwrap()
                .iter()
                .filter(|j| j.starts_with("gid="))
                .cloned()
                .collect();
            *i = osr::MountBuilder::default()
                .destination(i.destination())
                .typ(i.typ().as_ref().unwrap())
                .source(i.source().as_ref().unwrap())
                .options(newopts)
                .build()
                .unwrap();
        });
    if need_store_mount {
        mounts.push(
            osr::MountBuilder::default()
                .destination(&config.store_path)
                .source(&config.store_path)
                .typ("none")
                .options(vec!["bind".to_string()])
                .build()
                .unwrap(),
        );
    }
    let mut caps = HashSet::new();
    caps.insert(osr::Capability::BlockSuspend);
    let mut ropaths = osr::get_default_readonly_paths();
    ropaths.push(config.store_path.to_string());
    let mut namespaces = osr::get_default_namespaces();
    namespaces.push(
        osr::LinuxNamespaceBuilder::default()
            .typ(osr::LinuxNamespaceType::User)
            .build()
            .unwrap(),
    );
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
                .namespaces(namespaces)
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

pub fn random_name() -> String {
    use rand::prelude::*;
    let mut rng = rand::thread_rng();
    std::iter::repeat(())
        .take(20)
        .map(|()| char::from_u32(rng.gen_range(b'a'..=b'z').into()).unwrap())
        .collect::<String>()
}

pub async fn handle_process(
    config: &crate::ServerConfig,
    container_name: &str,
    WorkItemRun {
        inhash,
        bldname,
        args,
        envs,
        new_root,
        outputs,
        log,
        logtag,
        need_store_mount,
    }: WorkItemRun,
) -> Result<BuiltItem, OutputError> {
    let workdir = tempfile::tempdir()?;
    let rootdir = workdir.path().join("rootfs");

    let extra_env = if let Some(new_root) = new_root {
        // to work-around read-only stuff and such, copy the tree...
        let rootfs = config.store_path.join(new_root.to_string());

        Dump::read_from_path(rootfs.as_std_path())?.write_to_path(&rootdir, true)?;
        let mut perms = std::fs::metadata(&rootdir)?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&rootdir, perms)?;
        Some(format!("ROOTFS={}", rootfs))
    } else {
        std::fs::create_dir_all(&rootdir)?;
        None
    };

    // generate spec
    write_linux_ocirt_spec(
        config,
        &rootdir,
        args,
        envs.into_iter()
            .map(|(i, j)| format!("{}={}", i, j))
            .chain(extra_env)
            .collect(),
        &workdir.path().join("config.json"),
        need_store_mount,
    )?;

    use async_process::Stdio;
    let mut ch = Command::new(&config.container_runner)
        .args(vec!["run".to_string(), container_name.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(workdir.path())
        .spawn()?;
    let x = handle_logging(logtag, &bldname, log.clone(), ch.stdout.take().unwrap());
    let y = handle_logging(logtag, &bldname, log, ch.stderr.take().unwrap());

    let exs = tokio::join!(x, y, ch.status()).2?;
    if exs.success() {
        Ok(BuiltItem {
            inhash,
            outputs: outputs
                .into_iter()
                .map(|i| {
                    let dump = Dump::read_from_path(&rootdir.join("out"))?;
                    let outhash = StoreHash::hash_complex(&dump);
                    Ok((i, (Some(std::sync::Arc::new(dump)), outhash)))
                })
                .collect::<Result<_, yzix_core::store::Error>>()?,
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

pub fn read_graph_from_store(
    store_path: &Utf8Path,
    outhash: StoreHash,
) -> Result<build_graph::Graph<()>, OutputError> {
    let real_path = store_path.join(outhash.to_string()).into_std_path_buf();
    Ok(serde_json::from_str(
        &std::fs::read_to_string(&real_path).map_err(|e| yzix_core::store::Error {
            real_path,
            kind: e.into(),
        })?,
    )?)
}
