use super::{Error, ErrorKind};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

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
    pub fn read_from_path(x: &Path) -> Result<Self, Error> {
        let mapef = |e: std::io::Error| Error {
            real_path: x.to_path_buf(),
            kind: e.into(),
        };
        let meta = fs::symlink_metadata(x).map_err(&mapef)?;
        let ty = meta.file_type();
        Ok(if ty.is_symlink() {
            let target = fs::read_link(x)
                .map_err(&mapef)?
                .try_into()
                .map_err(|_| Error {
                    real_path: x.to_path_buf(),
                    kind: ErrorKind::NonUtf8SymlinkTarget,
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
                contents: std::fs::read(x).map_err(&mapef)?,
            }
        } else if ty.is_dir() {
            Dump::Directory(
                std::fs::read_dir(x)
                    .map_err(&mapef)?
                    .map(|entry| {
                        let entry = entry.map_err(&mapef)?;
                        let name = entry.file_name().into_string().map_err(|_| Error {
                            real_path: entry.path(),
                            kind: ErrorKind::NonUtf8Basename,
                        })?;
                        let val = Dump::read_from_path(&entry.path())?;
                        Ok((name, val))
                    })
                    .collect::<Result<_, Error>>()?,
            )
        } else {
            return Err(Error {
                real_path: x.to_path_buf(),
                kind: ErrorKind::UnknownFileType(format!("{:?}", ty)),
            });
        })
    }

    /// we require that the parent directory already exists,
    /// and will override the target path `x` if it already exists.
    pub fn write_to_path(&self, x: &Path, force: bool) -> Result<(), Error> {
        // one second past epoch, necessary for e.g. GNU make to recognize
        // the dumped files as "oldest"
        // TODO: when https://github.com/alexcrichton/filetime/pull/75 is merged,
        //   upgrade `filetime` and make this a global `const` binding.
        let reftime = filetime::FileTime::from_unix_time(1, 0);

        use std::io::ErrorKind as IoErrorKind;

        let mapef = |e: std::io::Error| Error {
            real_path: x.to_path_buf(),
            kind: e.into(),
        };
        let mut skip_write = false;

        if let Ok(y) = fs::symlink_metadata(x) {
            if !y.is_dir() {
                if force {
                    let clrro = || {
                        #[cfg(windows)]
                        {
                            // clear read-only attribute on windows, because
                            // otherwise it would prevent deletion
                            let mut perms = y.permissions();
                            if perms.readonly() {
                                perms.set_readonly(false);
                                fs::set_permissions(x, perms).map_err(&mapef)?;
                            }
                        }
                    };

                    if let Dump::Regular {
                        contents,
                        executable,
                    } = self
                    {
                        let cur_contents = fs::read(x).map_err(&mapef)?;
                        if &cur_contents == contents {
                            skip_write = true;
                        } else {
                            clrro();
                            // omit file deletion here because
                            // we overwrite it anyways
                        }
                    } else {
                        clrro();
                        fs::remove_file(x).map_err(&mapef)?;
                    }
                } else {
                    return Err(Error {
                        real_path: x.to_path_buf(),
                        kind: ErrorKind::OverwriteDeclined,
                    });
                }
            } else if let Dump::Directory(_) = self {
                // passthrough
            } else if force {
                fs::remove_dir_all(x).map_err(&mapef)?;
            } else {
                return Err(Error {
                    real_path: x.to_path_buf(),
                    kind: ErrorKind::OverwriteDeclined,
                });
            }
        }

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
                    .map_err(&mapef)?;
                }

                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&target, x).map_err(&mapef)?;
                }

                #[cfg(not(any(windows, unix)))]
                return Err(Error {
                    real_path: x.to_path_buf(),
                    kind: ErrorKind::SymlinksUnsupported,
                });
            }
            Dump::Regular {
                executable,
                contents,
            } => {
                if !skip_write {
                    fs::write(x, contents).map_err(&mapef)?;
                }

                #[cfg(windows)]
                {
                    let mut perms = fs::metadata(x).map_err(&mapef)?.permissions();
                    if !perms.readonly() {
                        perms.set_readonly(true);
                        fs::set_permissions(x, perms).map_err(&mapef)?;
                    }
                }

                #[cfg(unix)]
                fs::set_permissions(
                    x,
                    std::os::unix::fs::PermissionsExt::from_mode(if *executable {
                        0o555
                    } else {
                        0o444
                    }),
                )
                .map_err(&mapef)?;
            }
            Dump::Directory(contents) => {
                if let Err(e) = fs::create_dir(&x) {
                    if e.kind() == IoErrorKind::AlreadyExists {
                        // the check at the start of the function should have taken
                        // care of the annoying edge cases.
                        // x is thus already a directory
                        for entry in fs::read_dir(x).map_err(&mapef)? {
                            let entry = entry.map_err(&mapef)?;
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
                                    return Err(Error {
                                        real_path,
                                        kind: ErrorKind::OverwriteDeclined,
                                    });
                                } else if entry.file_type().map_err(&mapef)?.is_dir() {
                                    fs::remove_dir_all(real_path)
                                } else {
                                    fs::remove_file(real_path)
                                }
                                .map_err(&mapef)?;
                            }
                        }
                    }
                }
                let mut xs = x.to_path_buf();
                for (name, val) in contents {
                    if name.is_empty() {
                        return Err(Error {
                            real_path: x.to_path_buf(),
                            kind: ErrorKind::EmptyBasename,
                        });
                    }
                    xs.push(name);
                    // this call also deals with cases where the file already exists
                    Dump::write_to_path(val, &xs, force)?;
                    xs.pop();
                }

                #[cfg(unix)]
                fs::set_permissions(x, std::os::unix::fs::PermissionsExt::from_mode(0o555))
                    .map_err(&mapef)?;
            }
        }
        filetime::set_symlink_file_times(x, reftime, reftime).map_err(&mapef)
    }
}
