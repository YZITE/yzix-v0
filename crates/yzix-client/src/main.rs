use std::io::Read;
use yzix_core::{ciborium, proto};

fn main() {
    let matches = {
        use clap::{App, Arg, SubCommand};
        App::new("yzix-client")
            .arg(
                Arg::with_name("SERVER")
                    .help("yzix server address:port")
                    .takes_value(true)
                    .required(true)
                    .index(1),
            )
            .subcommand(
                SubCommand::with_name("build")
                    .about("submit a build graph and print all returned logs")
                    .arg(
                        Arg::with_name("GRAPH")
                            .help("build graph to submit")
                            .takes_value(true)
                            .required(true)
                            .index(1),
                    ),
            )
            .get_matches()
    };

    println!("{:?}", matches.value_of("SERVER"));

    if let Some(scmd) = matches.subcommand_matches("build") {
        let graph: yzix_core::build_graph::Graph<()> =
            serde_json::from_reader(std::io::BufReader::new(
                std::fs::File::open(scmd.value_of("GRAPH").unwrap()).expect("unable to open graph"),
            ))
            .expect("unable to parse graph from file");
        println!("graph = {:?}", graph);
        let schedule_cmd = proto::ControlCommand::Schedule(graph);
        let mut cmd_ser = Vec::new();
        ciborium::ser::into_writer(&schedule_cmd, &mut cmd_ser)
            .expect("unable to serialize graph to CBOR");

        use std::io::Write;
        let mut stream = std::net::TcpStream::connect(matches.value_of("SERVER").unwrap())
            .expect("unable to establish connection to yzix server");
        stream
            .write_all(
                &u32::try_from(cmd_ser.len())
                    .expect("unable to serialize command length (graph too big?)")
                    .to_le_bytes(),
            )
            .expect("unable to push schedule to server");
        stream
            .write_all(&cmd_ser[..])
            .expect("unable to push schedule to server");

        let mut buf = [0u8; 4];
        let mut dat = Vec::new();
        let mut stream = std::io::BufReader::new(stream);
        while stream.read_exact(&mut buf).is_ok() {
            let len: usize = u32::from_le_bytes(buf)
                .try_into()
                .expect("unable to deserialize response length");
            dat.resize(len, 0);
            stream.read_exact(&mut dat).expect("read failed");
            let resp: proto::Response =
                ciborium::de::from_reader(&dat[..]).expect("unable to deserialize response");

            use proto::ResponseKind as RK;
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
                RK::OutputNotify(Ok(outhash)) => {
                    println!("{}:=>{}", tag, outhash);
                }
                RK::OutputNotify(Err(oe)) => {
                    println!("{}:[ERROR] {:?}", tag, oe);
                }
            }
        }
    } else {
        // primt some example
        use yzix_core::{build_graph as bg, store::Dump};
        let mut graph = yzix_core::build_graph::Graph::<()>::default();
        let a = graph.0.add_node(bg::Node {
            name: "hi".to_string(),
            kind: bg::NodeKind::UnDump {
                dat: Dump::Regular {
                    executable: true,
                    contents: "echo Hi".to_string().into(),
                },
            },
            logtag: 1,
            rest: (),
        });
        let b = graph.0.add_node(bg::Node {
            name: "use hi".to_string(),
            kind: bg::NodeKind::Run {
                command: vec![
                    vec![bg::CmdArgSnip::String("bash".to_string())],
                    vec![bg::CmdArgSnip::Placeholder("hiinp".to_string())],
                ],
                envs: Default::default(),
            },
            logtag: 2,
            rest: (),
        });
        graph
            .0
            .add_edge(b, a, bg::Edge::Placeholder("hiinp".to_string()));
        serde_json::ser::to_writer_pretty(std::io::stdout(), &graph).unwrap();
    }
}
