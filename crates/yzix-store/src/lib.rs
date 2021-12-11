use base64::engine::fast_portable::{FastPortable, FastPortableConfig};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{convert, fmt};

mod dump;
// TODO: maybe rename Flags to something better...
pub use dump::{Dump, Flags};

// we can't use > 32 because serde (1.0.130) doesn't derive
// Deserialize... etc. for array lengths > 32.
const HASH_LEN: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Hash(pub [u8; HASH_LEN]);

static B64_ALPHABET: Lazy<base64::alphabet::Alphabet> = Lazy::new(|| {
    // base64, like URL_SAFE, but '-' is replaced with '+' for better interaction
    // with shell programs, which don't like file names which start with '-'
    base64::alphabet::Alphabet::from_str(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+_",
    )
    .unwrap()
});

static B64_ENGINE: Lazy<FastPortable> = Lazy::new(|| {
    FastPortable::from(
        &*B64_ALPHABET,
        FastPortableConfig::new().with_encode_padding(false),
    )
});

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&base64::encode_engine(self.0, &*B64_ENGINE))
    }
}

impl std::str::FromStr for Hash {
    type Err = base64::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            base64::decode_engine(s, &*B64_ENGINE)?
                .try_into()
                .map_err(|_| base64::DecodeError::InvalidLength)?,
        ))
    }
}

impl convert::AsRef<[u8]> for Hash {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Hash {
    #[inline]
    pub fn get_hasher() -> blake2::VarBlake2b {
        use blake2::digest::VariableOutput;
        blake2::VarBlake2b::new(HASH_LEN).unwrap()
    }

    pub fn finalize_hasher(x: blake2::VarBlake2b) -> Self {
        use blake2::digest::VariableOutput;
        let mut hash = Self([0u8; HASH_LEN]);
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

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, thiserror::Error)]
#[error("{real_path}: {kind}")]
pub struct Error {
    pub real_path: std::path::PathBuf,
    #[source]
    pub kind: ErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, thiserror::Error)]
pub enum ErrorKind {
    #[error("unable to convert symlink destination to UTF-8")]
    NonUtf8SymlinkTarget,

    #[error("unable to convert file name to UTF-8")]
    NonUtf8Basename,

    #[error("got unknown file type {0}")]
    UnknownFileType(String),

    #[error("directory entries with empty names are invalid")]
    EmptyBasename,

    #[error("symlinks are unsupported on this system")]
    #[cfg(not(any(unix, windows)))]
    SymlinksUnsupported,

    #[error("store dump declined to overwrite file")]
    /// NOTE: this obviously gets attached to the directory name
    OverwriteDeclined,

    #[error("I/O error: {desc}")]
    // NOTE: the `desc` already contains the os error code, don't print it 2 times.
    IoMisc { errno: Option<i32>, desc: String },
}

impl From<std::io::Error> for ErrorKind {
    fn from(e: std::io::Error) -> ErrorKind {
        ErrorKind::IoMisc {
            errno: e.raw_os_error(),
            desc: e.to_string(),
        }
    }
}
