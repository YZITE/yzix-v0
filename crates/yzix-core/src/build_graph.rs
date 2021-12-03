use crate::{StoreHash, StoreName};
use serde::{Deserialize, Serialize};
use string_interner::symbol::SymbolU32;

type Symbol = SymbolU32;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputName(String);

impl InputName {
    pub fn new(inp: String) -> Option<Self> {
        if inp.contains(|i| !i.is_alphanumeric() && i != '_') || inp.starts_with(|i| i.is_digit()) {
            None
        } else {
            Some(Self(inp))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CmdArgSnip {
    String(String),
    Placeholder(InputName),
}

type CmdArg = Vec<CmdArgSnip>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Node<T> {
    Command {
        /// every node has a name
        name: StoreName,

        command: Vec<CmdArg>,

        /// to support FODs
        expect_hash: Option<StoreHash>,

        /// to be able to reuse this data-structure, e.g. to unfold
        /// cycles, we clone the graph and populate the rest with
        /// default values.
        rest: T,
    },

    CycleBreak,
}

/// this stores the `rest` data for nodes in the "work graph"
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NodeWork {
    /// when this node is already realized into the local store,
    /// we then cache the resulting (content-addressed) hash part
    /// of the store path here.
    out_hash: Option<yzix_store_core::StoreHash>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Edge {
    kind: EdgeKind,

    /// describes how many cycle iterations this edge will persist
    /// when any edge drops to ttl==0, then the build of anything
    /// which depends on it will fail
    ttl: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum EdgeKind {
    Boot,
    PostBoot,
    Placeholder(InputName),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Graph {}
