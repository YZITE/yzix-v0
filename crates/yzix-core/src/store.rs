pub use crate::StoreName as Name;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::{convert, fmt, fs, sync::Arc};

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

#[derive(Clone, Debug)]
pub struct Path {
    pub base: Arc<Base>,
    pub hash: Hash,
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.hash)
    }
}

/// sort-of emulation of NAR using CBOR
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum Dump {
    Regular {
        executable: bool,
        contents: Vec<u8>,
    },
    SymLink {
        target: Utf8PathBuf,
        /// marker, if the symlink points to a directory,
        /// to correctly restore symlinks on windows
        to_dir: bool,
    },
    Directory(std::collections::BTreeMap<String, Dump>),
}

#[allow(unused_variables)]
impl Dump {
    pub fn read_from_path(x: &std::path::Path) -> std::io::Result<Self> {
        let meta = fs::symlink_metadata(x)?;
        let ty = meta.file_type();
        Ok(if ty.is_symlink() {
            let target = fs::read_link(x)?.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unable to convert symlink destination to UTF-8",
                )
            })?;
            let to_dir = if let Ok(x) = std::fs::metadata(x) {
                x.file_type().is_dir()
            } else {
                false
            };
            Dump::SymLink { target, to_dir }
        } else if ty.is_file() {
            Dump::Regular {
                executable: {
                    #[cfg(unix)]
                    let y =
                        std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o111 != 0;

                    #[cfg(not(unix))]
                    let y = false;

                    y
                },
                contents: std::fs::read(x)?,
            }
        } else if ty.is_dir() {
            Dump::Directory(
                std::fs::read_dir(x)?
                    .map(|entry| {
                        let entry = entry?;
                        let name = entry.file_name().into_string().map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unable to convert file name to UTF-8",
                            )
                        })?;
                        let val = Dump::read_from_path(&entry.path())?;
                        Ok::<_, std::io::Error>((name, val))
                    })
                    .collect::<Result<_, _>>()?,
            )
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unable to decode file type of '{}'", x.display()),
            ));
        })
    }

    /// we require that the parent directory already exists,
    /// and will override the target path `x` if it already exists.
    pub fn write_to_path(&self, x: &std::path::Path) -> std::io::Result<()> {
        match self {
            Dump::SymLink { target, to_dir } => {
                #[cfg(windows)]
                {
                    use std::os::windows::fs as wfs;
                    if to_dir {
                        wfs::symlink_dir(&target, x)
                    } else {
                        wfs::symlink_file(&target, x)
                    }
                }

                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&target, x)
                }

                #[cfg(not(any(windows, unix)))]
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "symlinks aren't supported on this platform",
                ))
            }
            Dump::Regular {
                executable,
                contents,
            } => {
                std::fs::write(x, contents)?;

                #[cfg(unix)]
                std::fs::set_permissions(
                    x,
                    std::os::unix::fs::PermissionsExt::from_mode(if *executable {
                        0o555
                    } else {
                        0o444
                    }),
                )
            }
            Dump::Directory(contents) => {
                std::fs::create_dir(&x)?;
                let mut xs = x.to_path_buf();
                for (name, val) in contents {
                    if name.is_empty() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "empty file names are invalid",
                        ));
                    }
                    xs.push(name);
                    Dump::write_to_path(val, &xs)?;
                    xs.pop();
                }
                Ok(())
            }
        }
    }
}
