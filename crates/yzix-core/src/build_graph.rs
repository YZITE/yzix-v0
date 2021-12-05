use crate::{StoreHash, StoreName, InputName};
pub use petgraph::stable_graph::{NodeIndex, StableGraph as RawGraph};
use petgraph::{visit::EdgeRef, Direction};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum CmdArgSnip {
    String(String),
    Placeholder(InputName),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node<T> {
    pub name: StoreName,

    /// the first level of Vec represents argv[],
    /// the second level contains components which will be
    /// concatenated before invocation,
    /// to make it possible to properly use placeholders
    pub command: Vec<Vec<CmdArgSnip>>,

    /// to support FODs
    pub expect_hash: Option<StoreHash>,

    /// to support additional data
    /// (e.g. used by the server to add execution metadata)
    pub rest: T,
}

impl<T, U> std::cmp::PartialEq<Node<U>> for Node<T> {
    fn eq(&self, rhs: &Node<U>) -> bool {
        self.name == rhs.name && self.command == rhs.command && self.expect_hash == rhs.expect_hash
    }
}

impl<T> Node<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Node<U> {
        let Node {
            name,
            command,
            expect_hash,
            rest,
        } = self;
        Node {
            name,
            command,
            expect_hash,
            rest: f(rest),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Edge {
    Placeholder(InputName),

    /// instead of allowing loops, we instead insert this edge
    /// which describes basically an "dynamic expect_hash",
    /// e.g. make sure that the output hash is equal to the
    /// output hash of another derivation/node
    AssertEqual,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Graph<T> {
    pub g: RawGraph<Node<T>, Edge>,
}

impl<T> Graph<T> {
    pub fn hash_node_inputs(&self, nid: NodeIndex) -> Option<StoreHash> {
        let node = self.g.node_weight(nid)?;

        use blake2::digest::Update;

        // for FOD's we only care about the expected hash
        if let Some(x) = node.expect_hash {
            return Some(x);
        }

        let incoming = self.g.edges_directed(nid, Direction::Incoming);
        let mut hasher = StoreHash::get_hasher();
        // gather input data
        hasher.update(&*node.name);
        hasher.update([0]);
        let mut ser_cmd = Vec::new();
        ciborium::ser::into_writer(&node.command, &mut ser_cmd).unwrap();
        hasher.update(ser_cmd);
        hasher.update([0]);

        for i in incoming {
            if let Edge::Placeholder(plh) = &i.weight() {
                hasher.update(&*plh);
                hasher.update([0]);
                hasher.update(&self.hash_node_inputs(i.source())?.0);
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
            .edges(from)
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
        for i in self.g.edges(to) {
            if let Some(x) = oedges.remove(&i.target()) {
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
            self.g.add_edge(to, trg, e);
        }
        // do not reset input hash cache, it only depends on the inputs,
        // which we just leave as-is
        Some(ret)
    }

    /// @returns mapping from rhs nodeids to self nodeids;
    /// our main job is to deduplicate identical nodes
    /// if the return value contains lesser entries than rhs contains nodes,
    /// then some nodes failed the conversion (e.g. the graph contained a cycle)
    pub fn take_and_merge<U>(&mut self, rhs: Graph<U>) -> HashMap<NodeIndex, NodeIndex>
    where
        U: Clone + Into<T>,
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

                // contains incoming half-edges
                let res_inps: HashMap<_, _> = rhs
                    .g
                    .edges_directed(i, Direction::Incoming)
                    .flat_map(|ie| ret.get(&ie.source()).map(|&x| (x, ie.weight())))
                    .collect();

                let ni = rhs.g.node_weight(i).unwrap();

                // check against all existing nodes
                // we can't really cache that, because we modify it all the time...
                for j in self.g.node_indices() {
                    // make sure that j also has the same inputs
                    if ni == self.g.node_weight(j).unwrap()
                        && self
                            .g
                            .edges_directed(i, Direction::Incoming)
                            .map(|je| (je.source(), je.weight()))
                            .collect::<HashMap<_, _>>()
                            == res_inps
                    {
                        // we have found an identical node, merge
                        ret.insert(i, j);
                        continue 'l_noinp;
                    }
                }

                // copy it
                let j = self.g.add_node(ni.clone().map(Into::into));
                ret.insert(i, j);

                // copy ingoing edges
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
