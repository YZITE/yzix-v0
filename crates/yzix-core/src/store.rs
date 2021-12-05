use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Base {
    Local { path: Utf8PathBuf, writable: bool },
}

impl fmt::Display for Base {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Base::Local { path, writable } => {
                write!(f, "{}{}", path, if *writable { "" } else { "[ro]" })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Hash(pub [u8; 24]);

const B64_CFG: base64::Config = base64::URL_SAFE_NO_PAD;

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&base64::encode_config(self.0, B64_CFG))
    }
}

impl std::str::FromStr for Hash {
    type Err = base64::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            base64::decode_config(s, B64_CFG)?
                .try_into()
                .map_err(|_| base64::DecodeError::InvalidLength)?,
        ))
    }
}

impl Hash {
    #[inline]
    pub fn get_hasher() -> blake2::VarBlake2b {
        use blake2::digest::VariableOutput;
        blake2::VarBlake2b::new(24).unwrap()
    }

    pub fn finalize_hasher(x: blake2::VarBlake2b) -> Self {
        use blake2::digest::VariableOutput;
        let mut hash = Self([0u8; 24]);
        x.finalize_variable(|res| hash.0.copy_from_slice(res));
        hash
    }

    pub fn hash_complex<T: serde::Serialize>(x: &T) -> Self {
        use blake2::digest::Update;
        let mut hasher = Self::get_hasher();
        let mut ser = Vec::new();
        ciborium::ser::into_writer(x, &mut ser).unwrap();
        hasher.update(ser);
        Self::finalize_hasher(hasher)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct Name(String);

impl Name {
    pub fn new(inp: String) -> Option<Self> {
        if inp.contains(|i: char| !i.is_ascii_graphic() || i == '/') {
            None
        } else {
            Some(Self(inp))
        }
    }
}

impl TryFrom<String> for Name {
    type Error = &'static str;

    fn try_from(x: String) -> Result<Name, &'static str> {
        Name::new(x).ok_or("invalid store name")
    }
}

impl From<Name> for String {
    fn from(x: Name) -> String {
        x.0
    }
}

impl std::ops::Deref for Name {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        &*self.0
    }
}

impl fmt::Display for Name {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&*self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Path {
    base: Arc<Base>,
    hash: Hash,
    name: Name,
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}-{}", self.base, self.hash, self.name)
    }
}

/// sort-of emulation of NAR using CBOR
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Dump {
    Regular { executable: bool, contents: Vec<u8> },
    SymLink { target: Utf8PathBuf },
    Directory(std::collections::BTreeMap<String, Dump>),
}
