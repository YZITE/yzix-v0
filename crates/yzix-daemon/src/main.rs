use yzix_core::{build_graph as bg, StoreHash, StoreName, StorePath};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use async_channel::{Sender, Receiver};
use async_process::{Command, ExitStatus};
use async_net::unix::UnixListener;

// NOTE: no explicit "done" command, just close the channel,
// the runner will continue building until done
#[derive(Debug)]
pub enum ControlCommand {
    /// wait for the workers to finish the currently running command,
    /// then return
    /// TODO: add some way to kill current jobs
    GracefulCancel,

    /// add a (sub-) graph to the main graph
    Schedule {
        graph: bg::Graph,
        trgs: Vec<sgraph::NodeIndex>,
    },
}

// NOTE: we use the nid as a kind of "request id" for server-worker communication

struct WorkItem {
    name: StoreName,
    command: Vec<String>,
    nid: sgraph::NodeIndex,
}

struct DoneItem {
    nid: sgraph::NodeIndex,
    ret: ExitStatus,
    output: Vec<u8>,
}

// this is a separate struct to allow finer splitting into functions
// to avoid putting all relevant code into one big chan::select! stmt
struct Runner {
    graph: bg::Graph,
    trgs: HashSet<sgraph::NodeIndex>,
    ws: Option<chan::Sender<WorkItem>>,
}

impl Runner {
    fn schedule(&mut self, g: bg::Graph, trgs: Vec<sgraph::NodeIndex>) {
        if let Some(ws) = self.ws {
            let trt = self.graph.take_and_merge(inp);
            // FIXME: notify the user if the insertion of any node fails
            self.trgs.extend(trgs.into_iter().flat_map(|i| trt.get(&i)).copied());
            // FIXME: add next runnable jobs to ws
        }
    }
}

fn worker(wr: chan::Receiver<WorkItem>, ds: chan::Sender<DoneItem>) {
    while let Ok(WorkItem { name, command, nid }) = wr.recv() {
        Command::new
        ds.send(DoneItem {
            nid,
            ret,
            output,
        }).unwrap();
    }
}

fn spawn_w_net(ex: &, ctrls: Sender<ControlCommand>) {
    let listener = UnixListener::bind("/tmp/yzix-srv").unwrap();
    let mut incoming = listener.incoming();

    while let Some
}

fn main() {
    let pbar = indicatif::ProgressBar::new();

    let mut runner = Runner {
        graph: Graph::default(),
        trgs: HashSet::new(),
    };

    let ex = yz_server_executor::ServerExecutor::new();
    let sem = async_lock::Semaphore::new(num_cpus::get());
    ex.block_on(move |ex| {
        // we don't need workers. we just spawn tasks instead
        // this means that it is impossible to fix or modify
        // running or older tasks. (invariant)
        // we manage the graph in the main thread, it is !Sync+!Send

        
    });

    // main loop
    let ws = Some(ws);
    loop {
        chan::select! {
            recv(ctrl) -> cmsg => {
                use ControlCommand as C;
                match cmsg {
                    C::GracefulCancel => {
                        runner.trgs.clear();
                        ws = None;
                    },
                    C::Schedule { graph, trgs } => runner.schedule(graph, trgs),
                }
            },
            recv(dnit) -> dmsg => {
                // FIXME: handle output, etc.
                pbar.inc(1);
            },
        }
    }
}
