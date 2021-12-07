use crate::store::Hash as StoreHash;
pub use petgraph::stable_graph::{NodeIndex, StableGraph as RawGraph};
pub use petgraph::{visit::EdgeRef, Direction};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

// we don't support FOD's for now,
// I don't think they are strictly necessary, and if that
// is proved wrong, we may always add them later.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum NodeKind {
    Run {
        /// the first level of Vec represents argv[],
        /// the second level contains components which will be
        /// concatenated before invocation,
        /// to make it possible to properly use placeholders
        command: Vec<Vec<CmdArgSnip>>,

        envs: HashMap<String, Vec<CmdArgSnip>>,
    },

    /// this is necessary to just let the client download stuff,
    /// and use it as a source;
    /// additionally, it also might allow us to later extend this
    /// to suppot some distcc-like workflow
    UnDump { dat: crate::store::Dump },

    /// when this node is reached, send a combined dump of all
    /// inputs (with each input represented as an entry in the
    /// top-level, which is a directory
    Dump,
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
pub enum Edge {
    Placeholder(String),

    /// instead of allowing loops, we instead insert this edge
    /// which describes basically an "dynamic expect_hash",
    /// e.g. make sure that the output hash is equal to the
    /// output hash of another derivation/node
    AssertEqual,
}

pub trait ReadOutHash {
    fn read_out_hash(&self) -> Option<StoreHash>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Graph<T> {
    pub g: RawGraph<Node<T>, Edge>,
}

impl<T> Default for Graph<T> {
    fn default() -> Self {
        Self {
            g: Default::default(),
        }
    }
}

impl<T> Graph<T> {
    pub fn hash_node_inputs(&self, nid: NodeIndex, seed: &[u8]) -> Option<StoreHash>
    where
        T: ReadOutHash,
    {
        // NOTE: we intentionally don't hash the node name
        use blake2::digest::Update;
        use ciborium::ser::into_writer as cbor_write;
        let node = self.g.node_weight(nid)?;
        let mut hasher = StoreHash::get_hasher();
        let mut tmp_ser = Vec::new();
        hasher.update(seed);
        match &node.kind {
            NodeKind::Run { command, envs } => {
                hasher.update(b"run\0");
                cbor_write(command, &mut tmp_ser).unwrap();
                hasher.update(&tmp_ser);
                hasher.update(b"\0envs\0");
                cbor_write(envs, &mut tmp_ser).unwrap();
                hasher.update(&tmp_ser);
                hasher.update([0]);
            }
            NodeKind::UnDump { dat } => {
                hasher.update(b"undump\0");
                cbor_write(dat, &mut tmp_ser).unwrap();
                hasher.update(&tmp_ser);
                hasher.update([0]);
            }
            // always build notify nodes
            NodeKind::Dump { .. } => {
                return None;
            }
        }
        let _ = tmp_ser;

        for i in self.g.edges(nid) {
            if let Edge::Placeholder(plh) = &i.weight() {
                hasher.update(&*plh);
                hasher.update([0]);
                hasher.update(
                    self.g
                        .node_weight(i.target())
                        .unwrap()
                        .rest
                        .read_out_hash()?,
                );
                hasher.update([0]);
            }
        }

        Some(StoreHash::finalize_hasher(hasher))
    }

    /// replace all references to a node with another one;
    /// used e.g. to merge identical nodes
    pub fn replace_node(&mut self, from: NodeIndex, to: NodeIndex) -> Option<Node<T>> {
        // ignore input edges, transfer output edges
        let mut oedges: HashMap<_, _> = self
            .g
            .edges_directed(from, Direction::Incoming)
            .map(|i| (i.target(), i.id()))
            .collect::<Vec<_>>()
            // break reference to `self.g`
            .into_iter()
            .map(|(trg, i)| (trg, self.g.remove_edge(i).unwrap()))
            .filter(|&(trg, _)| trg != to)
            .collect();
        // remove old node, if this yields then oedges is empty
        let ret = self.g.remove_node(from)?;
        // prune unnecessary edges
        for i in self.g.edges_directed(to, Direction::Incoming) {
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
            self.g.add_edge(trg, to, e);
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
        if self.g.node_count() == 0 {
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
            'l_noinp: for i in rhs.g.node_indices() {
                if ret.contains_key(&i) {
                    // already handled
                    continue;
                }

                // contains outgoing half-edges
                let res_inps = rhs.g.edges(i);
                let res_inps_cnt = res_inps.clone().count();
                let res_inps: HashMap<_, _> = res_inps
                    .flat_map(|ie| ret.get(&ie.target()).map(|&x| (x, ie.weight())))
                    .collect();
                if res_inps.len() != res_inps_cnt {
                    // contains unresolved inputs
                    continue;
                }

                let ni = rhs.g.node_weight(i).unwrap();

                // check against all existing nodes
                // we can't really cache that, because we modify it all the time...
                for j in self.g.node_indices() {
                    // make sure that j also has the same inputs
                    if ni == self.g.node_weight(j).unwrap()
                        && self
                            .g
                            .edges(j)
                            .map(|je| (je.target(), je.weight()))
                            .collect::<HashMap<_, _>>()
                            == res_inps
                    {
                        // we have found an identical node, merge
                        ret.insert(i, j);
                        attachf(&mut self.g[j].rest);
                        continue 'l_noinp;
                    }
                }

                // move it
                let j = self.g.add_node(ni.map_ref(&mut mapf));
                ret.insert(i, j);

                // copy outgoing edges
                for (src, wei) in res_inps {
                    self.g.add_edge(j, src, wei.clone());
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
