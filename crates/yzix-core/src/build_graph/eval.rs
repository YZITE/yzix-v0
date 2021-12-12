use super::*;
use crate::{store::Dump, OutputError, OutputName, Utf8Path};

pub trait ReadOutHash {
    fn read_out_hash(&self, output: &str) -> Option<StoreHash>;
}

#[cfg(test)]
impl ReadOutHash for () {
    fn read_out_hash(&self, _: &str) -> Option<StoreHash> {
        None
    }
}

fn eval_pattern(
    store_path: &Utf8Path,
    rphs: &HashMap<String, StoreHash>,
    xs: &[CmdArgSnip],
) -> Result<(String, bool), String> {
    // NOTE: do not use an iterator chain here,
    // as looping lets us handle string and returning more efficiently
    let mut ret = String::new();
    let mut uses_placeholders = false;
    for x in xs {
        use CmdArgSnip as CArgS;
        match x {
            CArgS::String(s) => ret += s,
            CArgS::Placeholder(iname) => {
                if let Some(y) = rphs.get(iname) {
                    ret += store_path.join(y.to_string()).as_str();
                    uses_placeholders = true;
                } else {
                    return Err(iname.to_string());
                }
            }
        }
    }
    Ok((ret, uses_placeholders))
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum WorkItem {
    /// node name is intentionally not present here,
    /// as it is irrelevant for execution and hashing
    Run {
        args: Vec<String>,
        envs: HashMap<String, String>,
        new_root: Option<StoreHash>,
        /// invariant: `!outputs.is_empty()`
        outputs: HashSet<OutputName>,
        uses_placeholders: bool,
    },
    UnDump {
        dat: Arc<Dump>,
    },
    Require(StoreHash),
    Fetch {
        url: url::Url,
        expect_hash: Option<StoreHash>,
    },
    Eval(StoreHash),
    Dump(HashMap<String, StoreHash>),

    /// used when nothing else is to be done, e.g. on `AssertEqual` nodes
    VerificationOk,
}

impl WorkItem {
    #[inline]
    pub fn inputs_hash(&self) -> Option<StoreHash> {
        use WorkItem as WI;
        match self {
            // these are dynamic;
            // e.g. used to reduce the size of the transferred graph from client to server,
            // and because they are shortcuts, they are fast to check/execute on the server
            // so we don't want to check input-addressing for them.
            WI::Require(_) | WI::Eval(_) | WI::Dump { .. } | WI::UnDump { .. } => None,
            _ => Some(StoreHash::hash_complex::<Self>(self)),
        }
    }

    pub fn outputs_hash(&self) -> Option<StoreHash> {
        use WorkItem as WI;
        match self {
            WI::Fetch { expect_hash, .. } => expect_hash.as_ref().copied(),
            WI::Require(hash) => Some(*hash),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum EvalError {
    /// This error is returned when [`read_out_hash`](ReadOutHash::read_out_hash) returns `None`.
    /// It allows consuming code to find out which input isn't available,
    /// to let it differentiate between "try again later" and "propagate failure";
    /// e.g. this error might be temporary
    #[error("input hash is unavailable (edge {0:?})")]
    InputWithoutHash(petgraph::graph::EdgeIndex),

    /// permanent error
    #[error(transparent)]
    Final(#[from] OutputError),
}

impl<T: ReadOutHash> Graph<T> {
    pub fn eval_node(&self, nid: NodeIndex, store_path: &Utf8Path) -> Result<WorkItem, EvalError> {
        // resolve placeholders
        let mut rphs = HashMap::new();
        let mut new_root = None;

        for e in self.0.edges(nid) {
            let (ew, et) = (e.weight(), e.target());
            if let Some(h) = self.0[et].rest.read_out_hash(&ew.sel_output) {
                match &ew.kind {
                    EdgeKind::Placeholder(iname) => {
                        use std::collections::hash_map::Entry;
                        match rphs.entry(iname.to_string()) {
                            Entry::Occupied(_) => {
                                return Err(OutputError::InputDup(ew.kind.clone()).into())
                            }
                            Entry::Vacant(v) => v.insert(h),
                        };
                    }
                    EdgeKind::Root => {
                        if new_root.is_some() {
                            return Err(OutputError::InputDup(ew.kind.clone()).into());
                        }
                        new_root = Some(h);
                    }
                }
            } else {
                return Err(EvalError::InputWithoutHash(e.id()));
            }
        }

        // evaluate command and envs
        use NodeKind as NK;

        Ok(match &self.0[nid].kind {
            NK::Run {
                command,
                envs,
                outputs,
            } => {
                let mut uses_placeholders = false;

                let args = match command
                    .iter()
                    .map(|i| {
                        eval_pattern(store_path, &rphs, i).map(|(j, upl)| {
                            uses_placeholders |= upl;
                            j
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Err(e) => {
                        return Err(OutputError::InputNotFound(EdgeKind::Placeholder(e)).into())
                    }
                    Ok(x) if x.is_empty() => return Err(OutputError::EmptyCommand.into()),
                    Ok(x) => x,
                };

                let envs = match envs
                    .iter()
                    .map(|(k, i)| {
                        eval_pattern(store_path, &rphs, i).map(|(j, upl)| {
                            uses_placeholders |= upl;
                            (k.to_string(), j)
                        })
                    })
                    .collect::<Result<HashMap<_, _>, _>>()
                {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(OutputError::InputNotFound(EdgeKind::Placeholder(e)).into())
                    }
                };

                let outputs = if outputs.is_empty() {
                    let mut tmp = HashSet::default();
                    tmp.insert(OutputName::default());
                    tmp
                } else {
                    outputs.clone()
                };

                WorkItem::Run {
                    args,
                    envs,
                    new_root,
                    outputs,
                    uses_placeholders,
                }
            }
            NK::UnDump { dat } => WorkItem::UnDump { dat: dat.clone() },
            NK::Require { hash } => WorkItem::Require(*hash),
            NK::Dump => WorkItem::Dump(rphs),

            NK::Fetch { url, hash } => {
                // this is also what's checked by reqwest::IntoUrl
                let url = url.clone();
                if !url.has_host() {
                    return Err(OutputError::InvalidUrl(url).into());
                }
                WorkItem::Fetch {
                    url,
                    expect_hash: hash.as_ref().copied(),
                }
            }

            // NOTE: we deliberately don't care about expect_hash in Eval nodes for now
            NK::Eval => match new_root {
                Some(hash) => WorkItem::Eval(hash),
                None => return Err(OutputError::InputNotFound(EdgeKind::Root).into()),
            },

            NK::AssertEqual => {
                let mut rphsv = rphs.into_iter().map(|(_, x)| x);
                if let Some(expected) = rphsv.next() {
                    for hash in rphsv {
                        if hash != expected {
                            return Err(OutputError::HashMismatch {
                                expected,
                                got: hash,
                            }
                            .into());
                        }
                    }
                }
                WorkItem::VerificationOk
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_inputs_normalize_outputs() {
        let mut g = Graph::default();
        let a = g.0.add_node(Node {
            name: "std".to_string(),
            logtag: 0,
            rest: (),
            kind: NodeKind::Run {
                command: vec![vec![CmdArgSnip::String("echo".to_string())]],
                envs: HashMap::default(),
                outputs: Default::default(),
            },
        });
        let mut outputs = HashSet::new();
        outputs.insert(OutputName::default());
        let b = g.0.add_node(Node {
            name: "std".to_string(),
            logtag: 0,
            rest: (),
            kind: NodeKind::Run {
                command: vec![vec![CmdArgSnip::String("echo".to_string())]],
                envs: HashMap::default(),
                outputs,
            },
        });
        let a_ = g.eval_node(a, Utf8Path::new(""));
        assert_eq!(a_, g.eval_node(b, Utf8Path::new("")));
        assert_eq!(
            a_.unwrap().inputs_hash(),
            Some(StoreHash([
                240, 40, 148, 62, 109, 115, 165, 5, 250, 100, 153, 29, 194, 107, 31, 111, 190, 172,
                242, 64, 220, 165, 128, 123, 50, 201, 218, 132, 248, 78, 242, 17
            ]))
        );
    }

    #[test]
    fn basic_eval_pattern() {
        let store_path = "/yzix/is/not/nix";

        let mut rphs = HashMap::new();
        rphs.insert(
            "alpine".to_string(),
            "m84OFxOfkVnnF7om15va9o1mgFcWD1TGH26ZhTLPuyg"
                .parse()
                .unwrap(),
        );
        let xs: Vec<_> = crate::pattern!(I "/bin/busybox"; I "sh"; I "echo")
            .into_iter()
            .map(|i| eval_pattern(Utf8Path::new(store_path), &rphs, &i).unwrap())
            .map(|(a, b)| {
                assert!(!b);
                a
            })
            .collect();
        assert_eq!(
            xs,
            ["/bin/busybox", "sh", "echo"]
                .into_iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
        );

        let xs: Vec<_> = crate::pattern!(I "/bin/busybox"; I "$$", P "alpine")
            .into_iter()
            .map(|i| eval_pattern(Utf8Path::new(store_path), &rphs, &i).unwrap())
            .collect();
        assert_eq!(
            xs,
            vec![
                ("/bin/busybox".to_string(), false),
                (
                    "$$/yzix/is/not/nix/m84OFxOfkVnnF7om15va9o1mgFcWD1TGH26ZhTLPuyg".to_string(),
                    true
                )
            ]
        );
    }
}
