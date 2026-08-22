//! Who is running this gateway, established by a held lock rather than by a
//! number in a file (M8 / `GW-001`, deliverable 5).
//!
//! # What was wrong
//!
//! `start` wrote a pid into a file; `stop` read it back and shelled out to
//! `kill(1)`. Neither step establishes what it is taken to establish.
//!
//! - `kill -0 <pid>` succeeding means *a* process with that pid exists. Pids
//!   are recycled, and a stale file plus a busy box is all it takes for the
//!   number to name something else entirely — at which point `stop` sends
//!   `SIGTERM`, and then `SIGKILL`, to a stranger.
//! - A pid file left by a process that crashed is indistinguishable from one
//!   left by a process that is running. The code guessed with `kill -0`, which
//!   is the same guess.
//! - `Command::new("kill")` puts the operator's `PATH` between the gateway and
//!   its own lifecycle.
//!
//! # What a lock establishes
//!
//! `flock(LOCK_EX | LOCK_NB)` on the control file, held open for the life of
//! the serving process. The kernel releases it when that process ends, however
//! it ends, so:
//!
//! - **The file is locked** ⟹ the process that wrote the record is alive.
//!   Not "some process with that pid" — *that* process, because the lock is
//!   held by an open file description belonging to it.
//! - **The lock can be taken** ⟹ nobody is serving, whatever the file says.
//!   No liveness heuristic, no stale-file guess.
//!
//! # Why this file is not written atomically
//!
//! Everything else the runtime replaces goes through
//! [`paths::write_private_atomic`](crate::paths::write_private_atomic). This
//! one cannot: `rename` replaces the *directory entry*, and the new file
//! carries none of the old one's locks — so an atomic replacement would
//! silently drop the very thing that makes the record trustworthy. It is
//! written in place, under the lock, and a reader takes the lock before
//! believing what it reads.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("control file {path} is held by pid {}", .record.pid)]
    Held {
        path: PathBuf,
        record: ControlRecord,
    },
    #[error("control file {path} failed: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("control file {path} is not readable as a record: {reason}")]
    Malformed { path: PathBuf, reason: Arc<str> },
}

/// What a serving gateway writes about itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRecord {
    pub pid: u32,
    /// Distinguishes two runs of the gateway that happen to reuse a pid, for
    /// a caller that has a token from a specific start.
    pub token: Arc<str>,
    /// Unix seconds, for the operator reading `status`.
    pub started_at: u64,
}

impl ControlRecord {
    pub fn new(pid: u32, token: impl Into<Arc<str>>, started_at: u64) -> Self {
        Self {
            pid,
            token: token.into(),
            started_at,
        }
    }

    fn render(&self) -> String {
        format!("{} {} {}\n", self.pid, self.token, self.started_at)
    }

    fn parse(text: &str, path: &Path) -> Result<Self, ControlError> {
        let mut parts = text.split_whitespace();
        let malformed = |reason: &str| ControlError::Malformed {
            path: path.to_path_buf(),
            reason: Arc::from(reason),
        };
        let pid = parts
            .next()
            .ok_or_else(|| malformed("empty"))?
            .parse::<u32>()
            .map_err(|_| malformed("first field is not a pid"))?;
        // Older records carried the pid alone, or a pid and a token. Both
        // still parse: an upgrade must not need the operator to delete a file
        // before the gateway will start.
        let token = parts.next().unwrap_or("").to_owned();
        let started_at = parts.next().and_then(|at| at.parse().ok()).unwrap_or(0);
        Ok(Self {
            pid,
            token: Arc::from(token.as_str()),
            started_at,
        })
    }
}

/// An acquired control file. Holds the lock until dropped, which is the whole
/// mechanism: the process's liveness *is* the lock.
#[derive(Debug)]
pub struct ControlFile {
    path: PathBuf,
    /// Never read after construction; it exists to keep the file description —
    /// and therefore the lock — open.
    _file: File,
}

impl ControlFile {
    /// Take the control file, writing `record` into it.
    ///
    /// [`ControlError::Held`] names whoever has it, which is what `start`
    /// prints instead of "already running" with no detail.
    pub fn acquire(path: &Path, record: &ControlRecord) -> Result<Self, ControlError> {
        if let Some(parent) = path.parent() {
            crate::paths::create_private_dir(parent).map_err(|err| ControlError::Io {
                path: err.path().to_path_buf(),
                source: err.into_io(),
            })?;
        }
        let io = |source| ControlError::Io {
            path: path.to_path_buf(),
            source,
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(io)?;

        if !try_lock(&file).map_err(io)? {
            let record = read_locked(&mut file, path)?;
            return Err(ControlError::Held {
                path: path.to_path_buf(),
                record,
            });
        }

        // In place, under the lock. See the module docs for why this is not a
        // temp-and-rename like every other state file.
        file.set_len(0).map_err(io)?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        file.write_all(record.render().as_bytes()).map_err(io)?;
        file.sync_all().map_err(io)?;
        Ok(Self {
            path: path.to_path_buf(),
            _file: file,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Release the lock and remove the file.
    ///
    /// Best effort by design: a gateway that is exiting must not fail because
    /// its own control file could not be removed, and the lock is released by
    /// the kernel whether or not this runs.
    pub fn release(self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Who is serving, according to the lock.
///
/// `Ok(None)` means nobody: either there is no control file, or there is one
/// and its lock is free — which is a crashed process's leftovers, not a
/// running gateway.
pub fn holder(path: &Path) -> Result<Option<ControlRecord>, ControlError> {
    let io = |source| ControlError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io(err)),
    };
    if try_lock(&file).map_err(io)? {
        // Taking it proves nobody was holding it. Give it straight back — this
        // is a query, and leaving it locked would make the next `start` think
        // this reader was the gateway.
        unlock(&file).map_err(io)?;
        return Ok(None);
    }
    read_locked(&mut file, path).map(Some)
}

/// The record as written, whether or not anybody holds the lock. For a caller
/// reporting on a stale file rather than deciding anything from it.
pub fn read_record(path: &Path) -> Result<Option<ControlRecord>, ControlError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ControlError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    read_locked(&mut file, path).map(Some)
}

fn read_locked(file: &mut File, path: &Path) -> Result<ControlRecord, ControlError> {
    let io = |source| ControlError::Io {
        path: path.to_path_buf(),
        source,
    };
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let mut text = String::new();
    file.read_to_string(&mut text).map_err(io)?;
    ControlRecord::parse(&text, path)
}

#[cfg(unix)]
fn try_lock(file: &File) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `flock` takes a file descriptor and a flag word; the descriptor
    // is valid for the borrow of `file`.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    // `EWOULDBLOCK` (== `EAGAIN` on Linux and macOS) is the answer "somebody
    // else has it", not a failure to ask.
    match err.raw_os_error() {
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => Ok(false),
        _ => Err(err),
    }
}

#[cfg(unix)]
fn unlock(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: as above.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        return Ok(());
    }
    Err(std::io::Error::last_os_error())
}

#[cfg(not(unix))]
fn try_lock(_file: &File) -> std::io::Result<bool> {
    // Without an advisory lock there is nothing to establish liveness with,
    // and claiming otherwise would be worse than saying so: a caller that
    // treats "acquired" as "nobody is serving" would start a second gateway.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "control-file locking is only implemented for unix",
    ))
}

#[cfg(not(unix))]
fn unlock(_file: &File) -> std::io::Result<()> {
    Ok(())
}

/// Ask the process holding the control file to stop, by pid, without a shell.
///
/// Verified against the lock first: a pid read out of a file names whatever
/// currently has that pid, and signalling it on the strength of the file alone
/// is how a stale record turns into a `SIGKILL` for an unrelated process.
#[cfg(unix)]
pub fn signal_holder(path: &Path, signal: i32) -> Result<Option<ControlRecord>, ControlError> {
    let Some(record) = holder(path)? else {
        return Ok(None);
    };
    // SAFETY: `kill` with a pid and a signal number has no memory-safety
    // precondition. The pid is the one currently holding the lock, so it names
    // a live process.
    let sent = unsafe { libc::kill(record.pid as libc::pid_t, signal) };
    if sent != 0 {
        return Err(ControlError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::last_os_error(),
        });
    }
    Ok(Some(record))
}

/// Ask the running gateway to drain and exit.
#[cfg(unix)]
pub fn terminate_holder(path: &Path) -> Result<Option<ControlRecord>, ControlError> {
    signal_holder(path, libc::SIGTERM)
}

/// End the running gateway without a drain, for a caller whose own deadline
/// has passed. Whatever was in flight is abandoned; the ingress ledger is what
/// says which conversations those were.
#[cfg(unix)]
pub fn kill_holder(path: &Path) -> Result<Option<ControlRecord>, ControlError> {
    signal_holder(path, libc::SIGKILL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentos-control-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn record() -> ControlRecord {
        ControlRecord::new(std::process::id(), "tok-1", 1_700_000_000)
    }

    #[test]
    fn acquiring_writes_the_record_and_reports_the_holder() {
        let dir = temp_dir("acquire");
        let path = dir.join("gateway.pid");
        assert_eq!(holder(&path).expect("the query runs"), None);

        let held = ControlFile::acquire(&path, &record()).expect("the file is acquirable");
        assert_eq!(
            holder(&path).expect("the query runs"),
            Some(record()),
            "a locked file names its holder"
        );
        held.release();
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The point of the lock: a second gateway cannot start, and finds out who
    /// stopped it.
    #[test]
    fn a_second_acquisition_is_refused_and_names_the_first() {
        let dir = temp_dir("contend");
        let path = dir.join("gateway.pid");
        let _held = ControlFile::acquire(&path, &record()).expect("the first acquires");
        let err = ControlFile::acquire(&path, &ControlRecord::new(999_999, "tok-2", 0))
            .expect_err("the second must be refused");
        match err {
            ControlError::Held { record: found, .. } => assert_eq!(found, record()),
            other => panic!("expected Held, got {other}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What `kill -0` could not tell apart. The record survives, the lock does
    /// not, and only the lock decides.
    #[test]
    fn a_released_lock_leaves_a_readable_record_that_names_nobody() {
        let dir = temp_dir("stale");
        let path = dir.join("gateway.pid");
        let held = ControlFile::acquire(&path, &record()).expect("the file is acquirable");
        // Drop without `release`, which is what a crash looks like: the file
        // stays, the kernel drops the lock.
        drop(held);

        assert_eq!(
            read_record(&path).expect("the record still parses"),
            Some(record()),
            "the file is still there, pid and all"
        );
        assert_eq!(
            holder(&path).expect("the query runs"),
            None,
            "nothing is serving, whatever the file says"
        );
        // And the next gateway starts straight into it.
        ControlFile::acquire(&path, &ControlRecord::new(4242, "tok-3", 1))
            .expect("a stale file is not an obstacle");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Reading must not become holding, or `status` would lock out `start`.
    #[test]
    fn querying_an_unheld_file_does_not_take_it() {
        let dir = temp_dir("query");
        let path = dir.join("gateway.pid");
        std::fs::create_dir_all(&dir).expect("the directory is creatable");
        std::fs::write(&path, "4242 tok 0\n").expect("the file is writable");
        for _ in 0..3 {
            assert_eq!(holder(&path).expect("the query runs"), None);
        }
        ControlFile::acquire(&path, &record()).expect("the file is still acquirable");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A file written by an older release carried the pid alone. An upgrade
    /// must not need the operator to delete anything.
    #[test]
    fn an_older_record_shape_still_parses() {
        let dir = temp_dir("legacy");
        let path = dir.join("gateway.pid");
        std::fs::create_dir_all(&dir).expect("the directory is creatable");
        std::fs::write(&path, "4242\n").expect("the file is writable");
        assert_eq!(
            read_record(&path).expect("the record parses"),
            Some(ControlRecord::new(4242, "", 0))
        );
        std::fs::write(&path, "4242 tok-old\n").expect("the file is writable");
        assert_eq!(
            read_record(&path).expect("the record parses"),
            Some(ControlRecord::new(4242, "tok-old", 0))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn the_control_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir("modes");
        let path = dir.join("gateway.pid");
        let _held = ControlFile::acquire(&path, &record()).expect("the file is acquirable");
        let mode = std::fs::metadata(&path)
            .expect("the file exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Signalling goes to the lock holder or to nobody. A stale record names a
    /// pid the kernel may since have handed to something else.
    #[cfg(unix)]
    #[test]
    fn signalling_a_stale_record_signals_nothing() {
        let dir = temp_dir("signal");
        let path = dir.join("gateway.pid");
        std::fs::create_dir_all(&dir).expect("the directory is creatable");
        // A pid that is almost certainly not ours, in an unlocked file.
        std::fs::write(&path, "999999 tok 0\n").expect("the file is writable");
        assert_eq!(
            signal_holder(&path, 0).expect("the query runs"),
            None,
            "an unlocked record must not be signalled"
        );

        // With the lock held, signal 0 reaches this very process.
        let _held = ControlFile::acquire(&path, &record()).expect("the file is acquirable");
        assert_eq!(
            signal_holder(&path, 0)
                .expect("the signal is sent")
                .map(|record| record.pid),
            Some(std::process::id())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
