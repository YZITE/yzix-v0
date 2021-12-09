use std::io::{Read, Write};
use yzix_core::{ciborium, ControlCommand, Response, ResponseKind as RK};

fn establish_connection(
    server: &str,
    attach_to_logs: bool,
) -> std::io::Result<std::net::TcpStream> {
    let bearer_auth = if let Ok(x) = std::env::var("YZIX_BEARER_TOKEN") {
        x
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "no bearer token found in env var YZIX_BEARER_TOKEN",
        ));
    };

    let mut stream = std::io::BufWriter::new(std::net::TcpStream::connect(server)?);

    {
        let opts = yzix_core::ClientOpts {
            bearer_auth,
            attach_to_logs,
        };
        let mut opts_ser = Vec::new();
        ciborium::ser::into_writer(&opts, &mut opts_ser)
            .expect("unable to serialize client options to CBOR");

        stream
            .write_all(
                &yzix_core::Length::try_from(opts_ser.len())
                    .unwrap()
                    .to_le_bytes(),
            )
            .expect("unable to login");
        stream.write_all(&opts_ser[..]).expect("unable to login");
    }

    stream.into_inner().map_err(|e| e.into_error())
}

fn main() {
    let matches = {
        use clap::{App, Arg, SubCommand};
        App::new("yzix-client")
            .arg(
                Arg::with_name("SERVER")
                    .short("s")
                    .long("server")
                    .help("yzix server address:port")
                    .takes_value(true)
                    .required(true),
            )
            .subcommand(
                SubCommand::with_name("build")
                    .about("submit a build graph and print all returned logs")
                    .arg(
                        Arg::with_name("GRAPH")
                            .long("graph")
                            .help("build graph to submit")
                            .takes_value(true)
                            .required(true),
                    )
                    .arg(
                        Arg::with_name("no-attach-to-logs-per-bearer")
                            .long("no-attach-to-logs-per-bearer")
                            .help("don't attach to logging output (per bearer)"),
                    )
                    .arg(
                        Arg::with_name("no-attach-to-logs-for-graph")
                            .long("no-attach-to-logs-for-graph")
                            .help("don't attach to logging output (for graph)"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("upload")
                    .about("submit a build graph consisting of dumps of specified files")
                    .arg(
                        Arg::with_name("SOURCES")
                            .long("sources")
                            .help("each specified path gets included as a dump")
                            .takes_value(true)
                            .multiple(true)
                            .required(true),
                    ),
            )
            .get_matches()
    };

    if let Some(scmd) = matches.subcommand_matches("build") {
        let graph: yzix_core::build_graph::Graph<()> =
            serde_json::from_reader(std::io::BufReader::new(
                std::fs::File::open(scmd.value_of("GRAPH").unwrap()).expect("unable to open graph"),
            ))
            .expect("unable to parse graph from file");

        let schedule_cmd = ControlCommand::Schedule {
            graph,
            attach_to_logs: !scmd.is_present("no-attach-to-logs-for-graph"),
        };
        let mut cmd_ser = Vec::new();
        ciborium::ser::into_writer(&schedule_cmd, &mut cmd_ser)
            .expect("unable to serialize graph to CBOR");

        let mut stream = establish_connection(
            matches.value_of("SERVER").unwrap(),
            !scmd.is_present("no-attach-to-logs-per-bearer"),
        )
        .expect("unable to establish connection to yzix server");

        stream
            .write_all(
                &yzix_core::Length::try_from(cmd_ser.len())
                    .expect("unable to serialize command length (graph too big?)")
                    .to_le_bytes(),
            )
            .expect("unable to push schedule to server (maybe bearer token is incorrect?)");
        stream
            .write_all(&cmd_ser[..])
            .expect("unable to push schedule to server");

        if !scmd.is_present("no-attach-to-logs-per-bearer")
            && !scmd.is_present("no-attach-to-logs-for-graph")
        {
            let mut buf = [0u8; std::mem::size_of::<yzix_core::Length>()];
            let mut dat = Vec::new();
            let mut stream = std::io::BufReader::new(stream);
            while stream.read_exact(&mut buf).is_ok() {
                let len: usize = yzix_core::Length::from_le_bytes(buf)
                    .try_into()
                    .expect("unable to deserialize response length");
                dat.resize(len, 0);
                stream.read_exact(&mut dat).expect("read failed");
                let resp: Response =
                    ciborium::de::from_reader(&dat[..]).expect("unable to deserialize response");

                let tag = resp.tag;
                match resp.kind {
                    RK::LogLine { bldname, content } => {
                        if tag != 0 {
                            print!("{}:", tag);
                        }
                        println!("{}> {}", bldname, content);
                    }
                    RK::Dump(dump) => {
                        println!("{}:[DUMP] {:?}", tag, dump);
                    }
                    RK::OutputNotify(Ok(outputs)) => {
                        println!("{}:=>", tag);
                        for (key, outhash) in outputs {
                            println!("\t{}->{} {:?}", key, outhash, outhash.0);
                        }
                    }
                    RK::OutputNotify(Err(oe)) => {
                        println!("{}:[ERROR] {:?}", tag, oe);
                    }
                }
            }
        }
    } else if let Some(scmd) = matches.subcommand_matches("upload") {
        use yzix_core::build_graph as bg;
        let mut graph = bg::Graph::default();

        let startval = 0xbeef;

        for (tag, i) in scmd.values_of("SOURCES").unwrap().enumerate() {
            let p = std::path::Path::new(i);
            graph.0.add_node(bg::Node {
                name: p.file_name().unwrap().to_str().unwrap().to_string(),
                kind: bg::NodeKind::UnDump {
                    dat: std::sync::Arc::new(
                        yzix_core::store::Dump::read_from_path(p)
                            .unwrap_or_else(|e| panic!("{}: unable to read source: {}", i, e)),
                    ),
                },
                logtag: (startval + tag).try_into().unwrap(),
                rest: (),
            });
        }

        let schedule_cmd = ControlCommand::Schedule {
            graph,
            attach_to_logs: !scmd.is_present("no-attach-to-logs-for-graph"),
        };
        let mut cmd_ser = Vec::new();
        ciborium::ser::into_writer(&schedule_cmd, &mut cmd_ser)
            .expect("unable to serialize graph to CBOR");

        let mut stream = establish_connection(matches.value_of("SERVER").unwrap(), true)
            .expect("unable to establish connection to yzix server");

        stream
            .write_all(
                &yzix_core::Length::try_from(cmd_ser.len())
                    .expect("unable to serialize command length (graph too big?)")
                    .to_le_bytes(),
            )
            .expect("unable to push schedule to server (maybe bearer token is incorrect?)");
        stream
            .write_all(&cmd_ser[..])
            .expect("unable to push schedule to server");

        let mut buf = [0u8; std::mem::size_of::<yzix_core::Length>()];
        let mut dat = Vec::new();
        let mut stream = std::io::BufReader::new(stream);
        while stream.read_exact(&mut buf).is_ok() {
            let len: usize = yzix_core::Length::from_le_bytes(buf)
                .try_into()
                .expect("unable to deserialize response length");
            dat.resize(len, 0);
            stream.read_exact(&mut dat).expect("read failed");
            let resp: Response =
                ciborium::de::from_reader(&dat[..]).expect("unable to deserialize response");

            let tag = resp.tag;
            match resp.kind {
                RK::LogLine { bldname, content } => {
                    if tag != 0 {
                        print!("{}:", tag);
                    }
                    println!("{}> {}", bldname, content);
                }
                RK::Dump(dump) => {
                    println!("{}:[DUMP] {:?}", tag, dump);
                }
                RK::OutputNotify(Ok(outputs)) => {
                    println!("{}:=>", tag);
                    for (key, outhash) in outputs {
                        println!("\t{}->{} {:?}", key, outhash, outhash.0);
                    }
                }
                RK::OutputNotify(Err(oe)) => {
                    println!("{}:[ERROR] {:?}", tag, oe);
                }
            }
        }
    }
}
