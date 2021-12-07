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
    pub tag: u64,
    pub kind: ResponseKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ResponseKind {
    LogLine { bldname: String, content: String },
    Dump(Dump),
    OutputNotify(Result<StoreHash, OutputError>),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, thiserror::Error)]
pub enum OutputError {
    #[error("command returned with exit code {0}")]
    Exit(i32),

    #[error("command was killed with signal {0}")]
    Killed(i32),

    #[error("server-side I/O error with errno {0}")]
    Io(i32),

    #[error("mismatch against AssertEqual ({0} != {1})")]
    HashMismatch(StoreHash, StoreHash),

    #[error("input edges '{0:?}' failed")]
    InputFailed(crate::build_graph::Edge),

    #[error("missing input edge '{0}'")]
    InputNotFound(String),

    #[error("multiple input edges used the same placeholder name '{0}'")]
    InputDup(String),

    #[error("given command is empty")]
    EmptyCommand,

    #[error("dump failed: {0}")]
    DumpFailed(String),

    #[error("hash collision at {0}")]
    HashCollision(StoreHash),

    #[error("an underspecified error happened: {0}")]
    Unknown(String),
}

impl From<std::io::Error> for OutputError {
    fn from(e: std::io::Error) -> OutputError {
        if let Some(x) = e.raw_os_error() {
            OutputError::Io(x)
        } else if let Some(x) = e.get_ref() {
            if let Some(y) = x.downcast_ref::<crate::store::Error>() {
                OutputError::DumpFailed(y.to_string())
            } else {
                OutputError::Unknown(e.to_string())
            }
        } else {
            OutputError::Unknown(e.to_string())
        }
    }
}
