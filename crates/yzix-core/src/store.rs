use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::{convert, fmt, fs};

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

#[derive(Debug, thiserror::Error)]
#[error("{real_path}: {kind}")]
pub struct Error {
    pub real_path: std::path::PathBuf,
    #[source]
    pub kind: ErrorKind,
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("unable to convert symlink destination to UTF-8")]
    NonUtf8SymlinkTarget,

    #[error("unable to convert file name to UTF-8")]
    NonUtf8Basename,

    #[error("got unknown file type {0:?}")]
    UnknownFileType(std::fs::FileType),

    #[error("directory entries with empty names are invalid")]
    EmptyBasename,

    #[error("symlinks are unsupported on this system")]
    #[cfg(not(any(unix, windows)))]
    SymlinksUnsupported,

    #[error("store dump declined to overwrite file")]
    /// NOTE: this obviously gets attached to the directory name
    OverwriteDeclined,
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
                    Error {
                        real_path: x.to_path_buf(),
                        kind: ErrorKind::NonUtf8SymlinkTarget,
                    },
                )
            })?;
            let to_dir = std::fs::metadata(x)
                .map(|y| y.file_type().is_dir())
                .unwrap_or(false);
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
                                Error {
                                    real_path: entry.path(),
                                    kind: ErrorKind::NonUtf8Basename,
                                },
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
                Error {
                    real_path: x.to_path_buf(),
                    kind: ErrorKind::UnknownFileType(ty),
                },
            ));
        })
    }

    /// we require that the parent directory already exists,
    /// and will override the target path `x` if it already exists.
    pub fn write_to_path(&self, x: &std::path::Path, force: bool) -> std::io::Result<()> {
        // one second past epoch, necessary for e.g. GNU make to recognize
        // the dumped files as "oldest"
        // TODO: when https://github.com/alexcrichton/filetime/pull/75 is merged,
        //   upgrade `filetime` and make this a global `const` binding.
        let reftime = filetime::FileTime::from_unix_time(1, 0);

        use std::io::{Error as IoError, ErrorKind as IoErrorKind};

        if let Ok(y) = fs::symlink_metadata(x) {
            if !y.is_dir() {
                if force {
                    // TODO: optimize for the case when the file is exactly the same
                    fs::remove_file(x)?;
                } else {
                    return Err(IoError::new(
                        IoErrorKind::AlreadyExists,
                        Error {
                            real_path: x.to_path_buf(),
                            kind: ErrorKind::OverwriteDeclined,
                        },
                    ));
                }
            } else if let Dump::Directory(_) = self {
                // passthrough
            } else if force {
                fs::remove_dir_all(x)?;
            } else {
                return Err(IoError::new(
                    IoErrorKind::AlreadyExists,
                    Error {
                        real_path: x.to_path_buf(),
                        kind: ErrorKind::OverwriteDeclined,
                    },
                ));
            }
        }

        match self {
            Dump::SymLink { target, to_dir } => {
                #[cfg(windows)]
                {
                    use std::os::windows::fs as wfs;
                    if to_dir {
                        wfs::symlink_dir(&target, x)?;
                    } else {
                        wfs::symlink_file(&target, x)?;
                    }
                }

                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&target, x)?;
                }

                #[cfg(not(any(windows, unix)))]
                return Err(IoError::new(
                    IoErrorKind::InvalidInput,
                    "symlinks aren't supported on this platform",
                ));
            }
            Dump::Regular {
                executable,
                contents,
            } => {
                fs::write(x, contents)?;

                #[cfg(unix)]
                fs::set_permissions(
                    x,
                    std::os::unix::fs::PermissionsExt::from_mode(if *executable {
                        0o555
                    } else {
                        0o444
                    }),
                )?;
            }
            Dump::Directory(contents) => {
                if let Err(e) = fs::create_dir(&x) {
                    if e.kind() == IoErrorKind::AlreadyExists {
                        // the check at the start of the function should have taken
                        // care of the annoying edge cases.
                        // x is thus already a directory
                        for entry in fs::read_dir(x)? {
                            let entry = entry?;
                            if entry
                                .file_name()
                                .into_string()
                                .ok()
                                .map(|x| contents.contains_key(&x))
                                != Some(true)
                            {
                                // file does not exist in the expected contents...
                                let real_path = entry.path();
                                if !force {
                                    return Err(IoError::new(
                                        IoErrorKind::AlreadyExists,
                                        Error {
                                            real_path,
                                            kind: ErrorKind::OverwriteDeclined,
                                        },
                                    ));
                                } else if entry.file_type()?.is_dir() {
                                    fs::remove_dir_all(real_path)
                                } else {
                                    fs::remove_file(real_path)
                                }?;
                            }
                        }
                    }
                }
                let mut xs = x.to_path_buf();
                for (name, val) in contents {
                    if name.is_empty() {
                        return Err(IoError::new(
                            IoErrorKind::InvalidInput,
                            Error {
                                real_path: x.to_path_buf(),
                                kind: ErrorKind::EmptyBasename,
                            },
                        ));
                    }
                    xs.push(name);
                    // this call also deals with cases where the file already exists
                    Dump::write_to_path(val, &xs, force)?;
                    xs.pop();
                }
            }
        }
        filetime::set_symlink_file_times(x, reftime, reftime)
    }
}
