use crate::{store::Hash as StoreHash, OutputName};
pub use petgraph::stable_graph::{NodeIndex, StableGraph as RawGraph};
pub use petgraph::{visit::EdgeRef, Direction};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

mod eval;
pub use eval::*;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum CmdArgSnip {
    String(String),
    Placeholder(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node<T> {
    pub name: String,
    pub kind: NodeKind,
    pub logtag: u64,

    /// to support additional data
    /// (e.g. used by the server to add execution metadata)
    pub rest: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeKind {
    Run {
        /// the first level of Vec represents argv[],
        /// the second level contains components which will be
        /// concatenated before invocation,
        /// to make it possible to properly use placeholders
        command: Vec<Vec<CmdArgSnip>>,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        envs: HashMap<String, Vec<CmdArgSnip>>,

        /// default is just the output "out", equivalent to an empty set
        #[serde(default, skip_serializing_if = "HashSet::is_empty")]
        outputs: HashSet<OutputName>,
    },

    /// this is necessary to just let the client download stuff,
    /// and use it as a source;
    /// additionally, it also might allow us to later extend this
    /// to suppot some distcc-like workflow
    UnDump { dat: Arc<crate::store::Dump> },

    /// a kind of potential fixed-output derivation, used to avoid
    /// additional round-trips between client and server
    Fetch {
        url: url::Url,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        hash: Option<StoreHash>,
    },

    /// to avoid the need to always upload huge amount of data,
    /// use this to require a store path to be already present.
    Require { hash: StoreHash },

    /// requires a root edge as input, and evaluates it as a
    /// json build graph. use carefully
    Eval,

    /// when this node is reached, send a combined dump of all
    /// inputs (with each input represented as an entry in the
    /// top-level, which is a directory
    // TODO: maybe add a node kind to dump stuff into a S3 bucket.
    Dump,

    /// instead of allowing loops, we instead insert this node
    /// which describes basically an "dynamic expect_hash",
    /// e.g. make sure that the output hash of all inputs is equal.
    AssertEqual,
}

impl<T, U> std::cmp::PartialEq<Node<U>> for Node<T> {
    fn eq(&self, rhs: &Node<U>) -> bool {
        self.name == rhs.name && self.kind == rhs.kind
    }
}

impl<T> Node<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Node<U> {
        let Node {
            name,
            kind,
            logtag,
            rest,
        } = self;
        Node {
            name,
            kind,
            logtag,
            rest: f(rest),
        }
    }

    pub fn map_ref<U>(&self, f: impl FnOnce(&T) -> U) -> Node<U> {
        let Node {
            name,
            kind,
            logtag,
            rest,
        } = self;
        Node {
            name: name.clone(),
            kind: kind.clone(),
            logtag: *logtag,
            rest: f(rest),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Edge {
    pub kind: EdgeKind,

    /// denotes which output (if the input node has multiple outputs)
    /// should be referenced.
    /// NOTE: this makes it necessary to use a graph struct which is
    /// capable of representing multiple edges between the same two nodes.
    #[serde(default, skip_serializing_if = "crate::strwrappers::is_default_output")]
    pub sel_output: OutputName,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum EdgeKind {
    Placeholder(String),

    /// specify an alternative root directory.
    /// this changes the build environment to make / read-only,
    /// and cwd from / to /tmp (the expected output stays ./out)
    Root,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Graph<T>(pub RawGraph<Node<T>, Edge>);

impl<T> Default for Graph<T> {
    #[inline]
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> Graph<T> {
    /// replace all references to a node with another one;
    /// used e.g. to merge identical nodes
    pub fn replace_node(&mut self, from: NodeIndex, to: NodeIndex) -> Option<Node<T>> {
        // ignore input edges, transfer output edges
        let mut oedges: HashMap<_, _> = self
            .0
            .edges_directed(from, Direction::Incoming)
            .map(|i| (i.target(), i.id()))
            .collect::<Vec<_>>()
            // break reference to `self.0`
            .into_iter()
            .map(|(trg, i)| (trg, self.0.remove_edge(i).unwrap()))
            .filter(|&(trg, _)| trg != to)
            .collect();
        // remove old node, if this yields then oedges is empty
        let ret = self.0.remove_node(from)?;
        // prune unnecessary edges
        for i in self.0.edges_directed(to, Direction::Incoming) {
            if let Some(x) = oedges.remove(&i.source()) {
                if &x == i.weight() {
                    // edge identical, drop it
                    continue;
                }
                // ignore it
                // TODO: insert some logging here
                continue;
            }
        }
        // transfer remaining edges
        for (trg, e) in oedges {
            self.0.add_edge(trg, to, e);
        }
        // do not reset input hash cache, it only depends on the inputs,
        // which we just leave as-is
        Some(ret)
    }

    /// @returns mapping from rhs nodeids to self nodeids;
    /// our main job is to deduplicate identical nodes
    /// if the return value contains lesser entries than rhs contains nodes,
    /// then some nodes failed the conversion (e.g. the graph contained a cycle)
    pub fn take_and_merge<U, MF, AF>(
        &mut self,
        rhs: Graph<U>,
        mut mapf: MF,
        attachf: AF,
    ) -> HashMap<NodeIndex, NodeIndex>
    where
        MF: FnMut(&U) -> T,
        AF: Fn(&mut T),
    {
        // handle the initial scheduling faster
        // disable that for now, we really want node deduplication
        // and cycle detection
        /*
        if self.0.node_count() == 0 {
            *self = rhs;
            return self.node_indices().map(|i| (i, i)).collect();
        }
        */

        // idx with base rhs -> idx with base self
        let mut ret = HashMap::new();

        // handle non-cyclic parts
        loop {
            let ret_elems = ret.len();

            // handle all nodes with no unresolved inputs
            'l_noinp: for i in rhs.0.node_indices() {
                if ret.contains_key(&i) {
                    // already handled
                    continue;
                }

                // contains outgoing half-edges
                let res_inps = rhs.0.edges(i);
                let res_inps_cnt = res_inps.clone().count();
                let res_inps: HashMap<_, _> = res_inps
                    .flat_map(|ie| ret.get(&ie.target()).map(|&x| (x, ie.weight())))
                    .collect();
                if res_inps.len() != res_inps_cnt {
                    // contains unresolved inputs
                    continue;
                }

                let ni = rhs.0.node_weight(i).unwrap();

                // check against all existing nodes
                // we can't really cache that, because we modify it all the time...
                for j in self.0.node_indices() {
                    // make sure that j also has the same inputs
                    if ni == self.0.node_weight(j).unwrap()
                        && self
                            .0
                            .edges(j)
                            .map(|je| (je.target(), je.weight()))
                            .collect::<HashMap<_, _>>()
                            == res_inps
                    {
                        // we have found an identical node, merge
                        ret.insert(i, j);
                        attachf(&mut self.0[j].rest);
                        continue 'l_noinp;
                    }
                }

                // move it
                let j = self.0.add_node(ni.map_ref(&mut mapf));
                ret.insert(i, j);

                // copy outgoing edges
                for (src, wei) in res_inps {
                    self.0.add_edge(j, src, wei.clone());
                }
            }

            if ret.len() == ret_elems {
                // no modifications happened...
                break;
            }
        }
        ret
    }
}
