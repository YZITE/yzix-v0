#![forbid(
    clippy::cast_ptr_alignment,
    trivial_casts,
    unconditional_recursion,
    unsafe_code,
    unused_must_use
)]

pub mod build_graph;
pub mod store;
mod strwrappers;
pub use crate::strwrappers::OutputName;

pub use camino::{Utf8Path, Utf8PathBuf};
pub use ciborium;
pub use url::Url;

use crate::{
    build_graph::{EdgeKind, Graph},
    store::{Dump, Hash as StoreHash},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Length = u64;

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
    OutputNotify(Result<HashMap<OutputName, StoreHash>, OutputError>),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, thiserror::Error)]
pub enum OutputError {
    #[error("command returned with exit code {0}")]
    Exit(i32),

    #[error("command was killed with signal {0}")]
    Killed(i32),

    #[error("server-side I/O error with errno {0}")]
    Io(i32),

    #[error("hash mismatch (expected {expected}, got {got})")]
    HashMismatch { expected: StoreHash, got: StoreHash },

    #[error("input edge {0:?} failed")]
    InputFailed(EdgeKind),

    #[error("missing input edge {0:?}")]
    InputNotFound(EdgeKind),

    #[error("multiple input edges are equal, but not allowed: {0:?}")]
    InputDup(EdgeKind),

    #[error("invalid URL for fetch: {0}")]
    InvalidUrl(Url),

    #[error("fetch of {url:?} failed with {msg}")]
    FetchFailed {
        url: Option<Url>,
        status: Option<u16>,
        msg: String,
    },

    #[error("given command is empty")]
    EmptyCommand,

    #[error("hash collision at {0}")]
    HashCollision(StoreHash),

    /// this is used when a hash was set as `required`, but wasn't available
    #[error("expected file to be available, but expectation wasn't met")]
    Unavailable,

    #[error("store error: {0}")]
    Store(#[from] crate::store::Error),

    #[error("number narrowing failed")]
    NumNarrowFailed,

    /// NOTE: line and column are one-based, but `serde_json` reserves
    /// the right to also set them to null for some bound errors...
    #[error("JSON deserialization error of type {typ} at {line}:{column}")]
    JsonDeserialize { line: u64, column: u32, typ: String },

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

impl From<std::num::TryFromIntError> for OutputError {
    fn from(_: std::num::TryFromIntError) -> OutputError {
        OutputError::NumNarrowFailed
    }
}

#[cfg(unix)]
impl From<nix::errno::Errno> for OutputError {
    fn from(e: nix::errno::Errno) -> OutputError {
        OutputError::Io(e as i32)
    }
}

#[cfg(feature = "reqwest")]
impl From<reqwest::Error> for OutputError {
    fn from(e: reqwest::Error) -> OutputError {
        OutputError::FetchFailed {
            url: e.url().map(Clone::clone),
            status: e.status().map(|x| x.as_u16()),
            msg: e.to_string(),
        }
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for OutputError {
    fn from(e: serde_json::Error) -> OutputError {
        macro_rules! try_convert {
            ($x:expr) => {
                if let Ok(x) = $x.try_into() {
                    x
                } else {
                    return OutputError::NumNarrowFailed;
                }
            };
        }
        OutputError::JsonDeserialize {
            line: try_convert!(e.line()),
            column: try_convert!(e.column()),
            typ: format!("{:?}", e.classify()),
        }
    }
}
