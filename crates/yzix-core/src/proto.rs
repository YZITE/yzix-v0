use crate::{
    build_graph::Graph,
    store::{Dump, Hash as StoreHash},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ControlCommand {
    /// schedule a bunch of commands
    Schedule {
        graph: Graph<()>,

        /// if set to false, no logs will be sent or kept
        /// about your submitted build graph
        attach_to_logs: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClientOpts {
    pub bearer_auth: String,

    /// if set to true, all logs for this bearer
    /// token will be sent to this client.
    ///
    /// if set to false, no logs will be sent
    /// to this client.
    ///
    /// if you really need separate log streams,
    /// use multiple different bearer tokens.
    pub attach_to_logs: bool,
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

    #[error("hash collision at {0}")]
    HashCollision(StoreHash),

    #[error("expected file to be available, but expectation wasn't met")]
    Unavailable,

    #[error("store error: {0}")]
    Store(#[from] crate::store::Error),

    #[error("an underspecified error happened: {0}")]
    Unknown(String),
}

impl From<std::io::Error> for OutputError {
    fn from(e: std::io::Error) -> OutputError {
        if let Some(x) = e.raw_os_error() {
            OutputError::Io(x)
        } else {
            OutputError::Unknown(e.to_string())
        }
    }
}
