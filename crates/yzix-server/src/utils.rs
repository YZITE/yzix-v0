use crate::{BuiltItem, LogFwdMessage, NodeMeta, WorkItemRun};
use async_channel::{Receiver, Sender};
use futures_util::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::{future::Future, marker::Unpin, path::Path, sync::Arc};
use yzix_core::build_graph;
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::Utf8Path;
use yzix_core::{OutputError, Response, ResponseKind};

pub async fn log_to_bunch<T: Clone>(subs: &mut Vec<Sender<T>>, msg: T) {
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

pub async fn log_to_bunch_hm<T: Clone>(
    subs: &mut std::collections::HashMap<Arc<str>, Sender<T>>,
    msg: T,
) {
    let mut hs = HashSet::new();
    {
        let mut cont: futures_util::stream::FuturesOrdered<_> = subs
            .iter()
            .map(|(name, li)| li.send(msg.clone()).map(|suc| (Arc::clone(name), suc)))
            .collect();
        while let Some((name, suc)) = cont.next().await {
            if suc.is_ok() {
                hs.insert(name);
            }
        }
    }
    subs.retain(|name, _| hs.contains(name));
}

pub fn push_response(
    n: &mut build_graph::Node<NodeMeta>,
    kind: ResponseKind,
) -> impl Future<Output = ()> + '_ {
    log_to_bunch_hm(
        &mut n.rest.log,
        LogFwdMessage::Response(Arc::new(Response {
            tag: n.logtag,
            kind,
        })),
    )
}

async fn handle_logging_to_intermed<T: tokio::io::AsyncRead + Unpin>(
    log: Sender<String>,
    pipe: T,
) -> std::io::Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut stream = BufReader::new(pipe).lines();
    while let Some(content) = stream.next_line().await? {
        if log.send(content).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn handle_logging_to_file(mut linp: Receiver<String>, loutp: &Path) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut fout = async_compression::tokio::write::ZstdEncoder::with_quality(
        tokio::fs::File::create(loutp).await?,
        async_compression::Level::Best,
    );
    while let Some(mut content) = linp.next().await {
        content.push('\n');
        fout.write_all(content.as_bytes()).await?;
    }
    fout.flush().await?;
    fout.shutdown().await?;
    Ok(())
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
                .filter(|j| !j.starts_with("gid="))
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

fn dfl_env_var(envs: &mut HashMap<String, String>, key: &str, value: &str) {
    envs.entry(key.to_string())
        .or_insert_with(|| value.to_string());
}

pub async fn handle_process(
    config: &crate::ServerConfig,
    container_name: &str,
    WorkItemRun {
        inhash,
        args,
        mut envs,
        new_root,
        outputs,
        need_store_mount,
    }: WorkItemRun,
) -> Result<BuiltItem, OutputError> {
    let workdir = tempfile::tempdir()?;
    let rootdir = workdir.path().join("rootfs");
    let logoutput = config.store_path.join(format!("{}.log.zst", inhash));

    if let Some(new_root) = new_root {
        tokio::task::block_in_place(|| {
            // to work-around read-only stuff and such, copy the tree...
            let rootfs = config.store_path.join(new_root.to_string());

            // this can take extremely long
            Dump::read_from_path(rootfs.as_std_path())?.write_to_path(
                &rootdir,
                yzix_core::store::Flags {
                    force: true,
                    make_readonly: false,
                },
            )?;
            let mut perms = std::fs::metadata(&rootdir)?.permissions();
            perms.set_readonly(false);
            std::fs::set_permissions(&rootdir, perms)?;
            envs.insert("ROOTFS".to_string(), rootfs.to_string());
            Ok::<_, OutputError>(())
        })?;
    } else {
        std::fs::create_dir_all(&rootdir)?;
    }

    dfl_env_var(&mut envs, "LC_ALL", "C.UTF-8");
    dfl_env_var(&mut envs, "TZ", "UTC");

    // generate spec
    write_linux_ocirt_spec(
        config,
        &rootdir,
        args,
        envs.into_iter()
            .map(|(i, j)| format!("{}={}", i, j))
            .collect(),
        &workdir.path().join("config.json"),
        need_store_mount,
    )?;

    use std::process::Stdio;
    let mut ch = tokio::process::Command::new(&config.container_runner)
        .args(vec![
            "--root".to_string(),
            config.store_path.join(".runc").into_string(),
            "run".to_string(),
            container_name.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(workdir.path())
        .spawn()?;
    let (logfwds, logfwdr) = async_channel::bounded(1000);
    let x = handle_logging_to_intermed(logfwds.clone(), ch.stdout.take().unwrap());
    let y = handle_logging_to_intermed(logfwds, ch.stderr.take().unwrap());
    let z = handle_logging_to_file(logfwdr, logoutput.as_std_path());

    let (_, _, z, exs) = tokio::join!(x, y, z, ch.wait());
    let (_, exs) = (z?, exs?);
    if exs.success() {
        Ok(BuiltItem {
            inhash: Some(inhash),
            outputs: outputs
                .into_iter()
                .map(|i| {
                    tokio::task::block_in_place(|| {
                        let dump = Dump::read_from_path(&rootdir.join(&*i))?;
                        let outhash = StoreHash::hash_complex::<Dump>(&dump);
                        Ok::<_, yzix_core::store::Error>((
                            i,
                            (Some(std::sync::Arc::new(dump)), outhash),
                        ))
                    })
                })
                .collect::<Result<_, _>>()?,
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

pub async fn read_graph_from_store(
    store_path: &Utf8Path,
    outhash: StoreHash,
) -> Result<build_graph::Graph<()>, OutputError> {
    let real_path = store_path.join(outhash.to_string()).into_std_path_buf();
    Ok(serde_json::from_str(
        &tokio::fs::read_to_string(&real_path)
            .await
            .map_err(|e| yzix_core::store::Error {
                real_path,
                kind: e.into(),
            })?,
    )?)
}
