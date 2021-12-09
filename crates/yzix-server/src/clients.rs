use crate::{AttachLogsKind, MainMessage};
use async_channel::{Receiver, Sender};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use yzix_core::ciborium;
use yzix_core::{ControlCommand, Length as ProtoLength, Response};

pub async fn handle_client_io(
    mains: Sender<MainMessage>,
    mut stream: TcpStream,
    attach_logs: Option<(Arc<str>, Receiver<Arc<Response>>)>,
) {
    let mainsi = mains.clone();
    let mainso = mains;
    let (attach_logs_bearer_token, logr) = match attach_logs {
        Some((x, y)) => (Some(x), Some(y)),
        None => (None, None),
    };

    let (stream, mut stream2) = stream.split();

    // handle input
    tokio::join!(
        async move {
            let mut lenbuf = [0u8; std::mem::size_of::<ProtoLength>()];
            let mut buf: Vec<u8> = Vec::new();
            let mut stream = tokio::io::BufReader::new(stream);
            while stream.read_exact(&mut lenbuf).await.is_ok() {
                buf.clear();
                let len = ProtoLength::from_le_bytes(lenbuf);
                // TODO: make sure that the length isn't too big
                buf.resize(len.try_into().unwrap(), 0);
                if stream.read_exact(&mut buf[..]).await.is_err() {
                    break;
                }
                use ControlCommand as C;
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
                        let val: ciborium::value::Value = match ciborium::de::from_reader(&buf[..])
                        {
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
                        break;
                    }
                };
                if mainsi
                    .send(match cmd {
                        C::Schedule {
                            graph,
                            attach_to_logs,
                        } => MainMessage::Schedule {
                            graph,
                            attach_logs: if attach_to_logs {
                                attach_logs_bearer_token.clone().map(AttachLogsKind::Bearer)
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
                            .write_all(&ProtoLength::to_le_bytes(buf.len().try_into().unwrap()))
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
    );
}

pub async fn handle_clients_initial(
    mains: Sender<MainMessage>,
    listener: TcpListener,
    valid_bearer_tokens: HashSet<String>,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok((x, _)) => x,
            Err(e) => {
                eprintln!("clients listener error: {}", e);
                continue;
            }
        };

        // auth + options
        let mut lenbuf = [0u8; std::mem::size_of::<ProtoLength>()];
        if stream.read_exact(&mut lenbuf).await.is_err() {
            continue;
        }
        let len = ProtoLength::from_le_bytes(lenbuf);
        if len >= 0x400 {
            continue;
        }
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(len.try_into().unwrap(), 0);
        if stream.read_exact(&mut buf[..]).await.is_err() {
            continue;
        }
        let opts: yzix_core::ClientOpts = match ciborium::de::from_reader(&buf[..]) {
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
