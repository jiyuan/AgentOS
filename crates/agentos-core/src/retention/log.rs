//! Size-based rotation for the gateway's own log file.
//!
//! The gateway appends one line per notable event to a single file and never
//! looked at how big it had become. On a busy deployment that is the fastest
//! growing thing the runtime writes, and it is the one an operator is least
//! likely to be watching, because a log is background noise until the volume
//! it lives on fills up.
//!
//! Rotation rather than truncation: what makes a log worth keeping is the run
//! of lines before the thing went wrong, and truncating at a ceiling deletes
//! exactly that.

use std::path::{Path, PathBuf};

/// What one rotation did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RotationCount {
    /// Whether the live log was rotated out this pass.
    pub(crate) rotated: bool,
    /// Rotated files deleted for being past `keep`.
    pub(crate) discarded: usize,
}

/// Rotate `path` if it has reached `max_bytes`, keeping `keep` older files.
///
/// `path.1` is the most recent rotation, `path.2` the one before it, and so
/// on; `path.{keep}` is the oldest kept and anything past it is deleted.
///
/// The live file is *renamed*, not copied and truncated. A writer that had it
/// open across the rename keeps appending to the rotated file rather than to a
/// hole — the gateway opens and closes per line, so at most a line or two lands
/// one file further back than a reader would expect, which is the cheapest of
/// the available wrong answers.
///
/// Best effort: a rename or unlink that fails is reported as "did not rotate"
/// and tried again next sweep. The one thing it must never do is fail the
/// gateway, which is still serving.
pub(crate) async fn rotate(path: &Path, max_bytes: u64, keep: usize) -> RotationCount {
    let mut count = RotationCount::default();
    if max_bytes == 0 || keep == 0 {
        return count;
    }
    let Ok(meta) = tokio::fs::metadata(path).await else {
        // No log yet, or not a file we can stat. Nothing to rotate.
        return count;
    };
    if meta.len() < max_bytes {
        return count;
    }

    // Drop what is already past the ceiling before shifting, so `keep = 3`
    // means three files afterwards rather than three plus the one this pass
    // is about to create.
    let mut index = keep;
    while tokio::fs::remove_file(numbered(path, index)).await.is_ok() {
        count.discarded += 1;
        index += 1;
        // A directory somebody filled with `gateway.log.N` by hand should not
        // turn a sweep into an unbounded loop.
        if index > keep + MAX_EXTRA_ROTATIONS {
            break;
        }
    }
    for index in (1..keep).rev() {
        let from = numbered(path, index);
        if tokio::fs::metadata(&from).await.is_ok() {
            let _ = tokio::fs::rename(&from, numbered(path, index + 1)).await;
        }
    }
    count.rotated = tokio::fs::rename(path, numbered(path, 1)).await.is_ok();
    count
}

/// How far past `keep` a single pass will clean up. Bounded because the loop
/// is driven by what is on disk rather than by anything this runtime wrote.
const MAX_EXTRA_ROTATIONS: usize = 64;

fn numbered(path: &Path, index: usize) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agentos-logrotate-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temp dir");
        root
    }

    #[tokio::test]
    async fn a_small_log_is_left_alone() {
        let dir = temp_dir("small");
        let log = dir.join("gateway.log");
        std::fs::write(&log, "one line\n").expect("write");
        assert_eq!(rotate(&log, 1024, 3).await, RotationCount::default());
        assert!(log.exists());
        assert!(!numbered(&log, 1).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_full_log_moves_aside_and_the_live_path_is_free() {
        let dir = temp_dir("full");
        let log = dir.join("gateway.log");
        std::fs::write(&log, vec![b'x'; 2048]).expect("write");
        let count = rotate(&log, 1024, 3).await;
        assert!(count.rotated);
        assert!(!log.exists(), "the live path is free for the next line");
        assert_eq!(
            std::fs::read(numbered(&log, 1)).expect("rotated").len(),
            2048
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `keep = 2` means two rotated files after the pass, and the oldest is
    /// the one that goes.
    #[tokio::test]
    async fn rotations_shift_along_and_the_oldest_is_discarded() {
        let dir = temp_dir("shift");
        let log = dir.join("gateway.log");
        std::fs::write(numbered(&log, 1), "first rotation").expect("write");
        std::fs::write(numbered(&log, 2), "oldest").expect("write");
        std::fs::write(&log, vec![b'x'; 2048]).expect("write");

        let count = rotate(&log, 1024, 2).await;
        assert!(count.rotated);
        assert_eq!(count.discarded, 1);
        assert_eq!(
            std::fs::read_to_string(numbered(&log, 2)).expect("shifted"),
            "first rotation"
        );
        assert!(!numbered(&log, 3).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_missing_log_is_not_a_failure() {
        let dir = temp_dir("missing");
        assert_eq!(
            rotate(&dir.join("nothing.log"), 1024, 3).await,
            RotationCount::default()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `max_bytes = 0` is the operator saying "one growing file", not "rotate
    /// on every line".
    #[tokio::test]
    async fn rotation_off_never_rotates() {
        let dir = temp_dir("off");
        let log = dir.join("gateway.log");
        std::fs::write(&log, vec![b'x'; 4096]).expect("write");
        assert_eq!(rotate(&log, 0, 3).await, RotationCount::default());
        assert!(log.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
