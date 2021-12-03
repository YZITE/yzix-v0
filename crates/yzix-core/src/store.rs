use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

#[derive(Debug, Deserialize, Serialize)]
pub enum Base {
    Local { path: Utf8PathBuf, writable: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Hash(pub [u8; 24]);

const B64_CFG: base64::Config = base64::URL_SAFE_NO_PAD;

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use fmt::Write;
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Name(String);

impl Name {
    pub fn new(inp: String) -> Option<Self> {
        if inp.contains(|i| !i.is_ascii_graphic() || i == '/') {
            None
        } else {
            Some(Self(inp))
        }
    }
}

impl std::ops::Deref for Name {
    type Target = str;
    fn deref(&self) -> &str {
        &*self.0
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
        use fmt::Write;
        write!(f, "{}/{}-{}", self.base, self.hash, self.name)
    }
}
