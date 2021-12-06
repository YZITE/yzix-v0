use crate::{
    build_graph::Graph,
    store::{Dump, Hash as StoreHash},
};
use serde::{Deserialize, Serialize};

/// NOTE: closing a connection will cause you to permanently lose access
/// to log output of your currently submitted build graphs
/// (e.g. you can't reattach to a running build)
/// TODO: find a way to allow safe and secure reattachment
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ControlCommand {
    /// schedule a bunch of commands
    Schedule(Graph<()>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response {
    tag: u64,
    kind: ResponseKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ResponseKind {
    LogLine { bldname: String, content: String },
    Dump(Dump),
    OutputNotify(Result<StoreHash, OutputError>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OutputError {
    /// command returned with exit code
    Exit(core::num::NonZeroI32),
    /// command was killed
    Killed(i32),
    /// server-side I/O error
    Io(i32),
    /// something else, idk.
    Unknown,
}
