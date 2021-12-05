use crate::build_graph as bg;
use serde::{Deserialize, Serialize};

/// NOTE: closing a connection will cause you to permanently lose access
/// to log output of your currently submitted build graphs
/// (e.g. you can't reattach to a running build)
/// TODO: find a way to allow safe and secure reattachment
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ControlCommand {
    /// schedule a bunch of commands
    Schedule(bg::Graph<()>),
}
