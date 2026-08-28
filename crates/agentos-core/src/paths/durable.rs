//! Replacing a state file without a window where it is missing, truncated, or
//! readable by everyone (M8 / `GW-001`, deliverable 4).
//!
//! `std::fs::write` is three separate things: create-or-truncate, one or more
//! `write`s, and close. A crash after the truncate leaves an empty file where
//! a paused run used to be; a crash mid-write leaves half a JSON document that
//! parses as nothing; and the mode is `0o666 & !umask`, so on a permissive
//! umask the approval record for a run — which names the tool call about to
//! fire and the conversation that asked for it — is world-readable.
//!
//! Every one of those is a real outcome for the files this module is for:
//! the paused-run record, task state, cron bookkeeping, and the gateway's own
//! control file. `spill/mod.rs` already got this right with `O_EXCL` and
//! `0o600`, but spill files are *created once and never replaced*, which is
//! the easy half. Replacement is what needs the temp-and-rename.
//!
//! # What "durable" means here
//!
//! [`write_private_atomic`] returns only after:
//!
//! 1. the bytes are in a fresh temporary file in the destination's own
//!    directory (`O_EXCL`, mode `0o600`, so neither an existing file nor a
//!    planted symlink is a target),
//! 2. that file's data has reached the disk (`fsync`),
//! 3. it has been `rename`d onto the destination — atomic within a
//!    filesystem, which is why the temporary lives in the same directory
//!    rather than in `/tmp`, and
//! 4. the *directory* has been `fsync`ed, without which the rename itself can
//!    be lost even though the data was not.
//!
//! Step 4 is the one that is usually skipped. It is the difference between "the
//! new contents survive a crash" and "the file still points at them".
//!
//! A reader therefore sees either the whole previous version or the whole new
//! one, never a partial write and never an absence.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Mode for a state file: readable and writable by the owning user only.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Mode for a directory holding state files. `0o700` rather than `0o755`
/// because a directory an attacker can list is a directory whose file *names*
/// leak — conversation ids, task ids, run ids — even when every file in it is
/// unreadable.
#[cfg(unix)]
pub const PRIVATE_DIR_MODE: u32 = 0o700;

#[derive(Debug, Error)]
pub enum DurableWriteError {
    #[error("failed to prepare {path}: {source}")]
    Prepare {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to replace {path}: {source}")]
    Replace {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl DurableWriteError {
    /// The path the failure was about, for a caller whose own error type is
    /// keyed by path.
    pub fn path(&self) -> &Path {
        match self {
            Self::Prepare { path, .. } | Self::Write { path, .. } | Self::Replace { path, .. } => {
                path
            }
        }
    }

    /// The underlying I/O failure, for a caller whose own error type carries
    /// an `io::Error` rather than this one.
    pub fn into_io(self) -> std::io::Error {
        match self {
            Self::Prepare { source, .. }
            | Self::Write { source, .. }
            | Self::Replace { source, .. } => source,
        }
    }
}

/// Create `dir` and every missing parent, private to this user.
///
/// Note that the mode applies to directories this call creates. One that
/// already exists keeps the mode it has — tightening a directory somebody else
/// created is not this function's decision to make.
pub fn create_private_dir(dir: &Path) -> Result<(), DurableWriteError> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(PRIVATE_DIR_MODE);
    }
    builder.create(dir).map_err(|source| {
        // `create_dir_all` is happy when the directory already exists; only a
        // real failure reaches here.
        DurableWriteError::Prepare {
            path: dir.to_path_buf(),
            source,
        }
    })
}

/// Write `bytes` to `path`, atomically and privately. See the module docs for
/// what each step buys.
pub fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<(), DurableWriteError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    create_private_dir(parent)?;

    let temp = temp_path_beside(path);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(PRIVATE_FILE_MODE);
    }
    let mut file = options
        .open(&temp)
        .map_err(|source| DurableWriteError::Prepare {
            path: temp.clone(),
            source,
        })?;

    // From here on a failure leaves the temporary behind, so every arm removes
    // it. The destination is untouched until the rename, which is the point.
    if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = std::fs::remove_file(&temp);
        return Err(DurableWriteError::Write { path: temp, source });
    }
    drop(file);

    if let Err(source) = std::fs::rename(&temp, path) {
        let _ = std::fs::remove_file(&temp);
        return Err(DurableWriteError::Replace {
            path: path.to_path_buf(),
            source,
        });
    }

    // The rename is durable only once the directory entry is. A failure here
    // means the new bytes may be visible but their crash durability is unknown,
    // so success would be a false claim and must not be returned.
    let dir = File::open(parent).map_err(|source| DurableWriteError::Replace {
        path: path.to_path_buf(),
        source,
    })?;
    dir.sync_all()
        .map_err(|source| DurableWriteError::Replace {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

/// A sibling name nothing else will pick: same directory (so the rename stays
/// within one filesystem), same user, and distinct per process *and* per
/// thread, because two shards persisting two runs into one directory must not
/// collide on the temporary.
fn temp_path_beside(path: &Path) -> PathBuf {
    let stem = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_owned());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    let sibling = format!(
        ".{stem}.tmp-{}-{:?}-{nanos}",
        std::process::id(),
        std::thread::current().id()
    );
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .join(sibling)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentos-durable-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_write_creates_the_parent_and_the_file() {
        let root = temp_root("create");
        let path = root.join("nested/state.json");
        write_private_atomic(&path, b"{}").expect("the write succeeds");
        assert_eq!(std::fs::read(&path).expect("readable"), b"{}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_second_write_replaces_the_first_without_leaving_a_temporary() {
        let root = temp_root("replace");
        let path = root.join("state.json");
        write_private_atomic(&path, b"first").expect("the first write succeeds");
        write_private_atomic(&path, b"second").expect("the second write succeeds");
        assert_eq!(std::fs::read(&path).expect("readable"), b"second");

        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .expect("the directory lists")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name != "state.json")
            .collect();
        assert!(
            leftovers.is_empty(),
            "a completed write must clean up after itself, found {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The reason for the module: the same bytes through `fs::write` land at
    /// `0o666 & !umask`, which on the usual `022` is world-readable.
    ///
    /// This asserts against the *ambient* umask rather than setting one —
    /// `umask(2)` is process-global and this binary runs its tests in
    /// parallel. The permissive-umask case the acceptance criterion names is
    /// `tests/state_durability.rs`, which has a process to itself.
    #[cfg(unix)]
    #[test]
    fn a_state_file_is_private_where_a_plain_write_would_not_be() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("modes");
        let path = root.join("run/state.json");
        write_private_atomic(&path, b"secret").expect("the write succeeds");

        let mode = |path: &Path| {
            std::fs::metadata(path)
                .expect("the path exists")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode(&path), 0o600, "state must be owner-only");
        assert_eq!(
            mode(&root.join("run")),
            0o700,
            "a listable directory leaks the names even when the files are private"
        );

        let control = root.join("run/control.json");
        std::fs::write(&control, b"secret").expect("the control write succeeds");
        assert_ne!(
            mode(&control),
            0o600,
            "if the ambient umask already yields 0o600 this test proves nothing"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A symlink planted at the destination is followed by `fs::write` and
    /// would let anything that can write the state directory redirect the
    /// write. The rename replaces the *link*, so the target is untouched.
    #[cfg(unix)]
    #[test]
    fn a_symlink_at_the_destination_is_replaced_rather_than_followed() {
        let root = temp_root("symlink");
        std::fs::create_dir_all(&root).expect("the root is creatable");
        let victim = root.join("victim.txt");
        std::fs::write(&victim, b"do not touch").expect("the victim is writable");
        let path = root.join("state.json");
        std::os::unix::fs::symlink(&victim, &path).expect("the link is creatable");

        write_private_atomic(&path, b"new state").expect("the write succeeds");
        assert_eq!(
            std::fs::read(&victim).expect("readable"),
            b"do not touch",
            "the write followed the symlink"
        );
        assert_eq!(std::fs::read(&path).expect("readable"), b"new state");
        assert!(
            !std::fs::symlink_metadata(&path)
                .expect("the path exists")
                .file_type()
                .is_symlink(),
            "the link should have been replaced by a real file"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
