//! Age and byte ceilings over a directory the runtime owns outright.
//!
//! Three stores have the same shape — traces, inbound attachments, and spill —
//! and differ only in how deep the thing being kept sits and whether it is a
//! file or a directory. So the walk is written once here and each caller says
//! what a *unit* is: the thing that is kept or removed whole.
//!
//! Whole units, never parts of one. A trace with its last thousand lines
//! removed is a run whose outcome cannot be read; a message with two of its
//! three attachments left is a message the transcript describes wrongly. Both
//! are worse than the unit being gone.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// How deep a recursive size walk will go before giving up on a unit.
///
/// These are directories this runtime created, so the real depth is one or
/// two. The cap is there because the walk runs over whatever is actually on
/// disk, and an unbounded descent driven by the filesystem is the kind of loop
/// that only ever runs once.
const MAX_WALK_DEPTH: usize = 8;

/// One thing that is kept or removed whole.
#[derive(Clone, Debug)]
pub(crate) struct Unit {
    pub(crate) path: PathBuf,
    /// The newest write anywhere inside it.
    ///
    /// Newest rather than the directory's own mtime: a conversation that
    /// received an attachment yesterday and another today is as recent as
    /// today, and directory mtimes are not consistent across filesystems.
    pub(crate) newest: SystemTime,
    pub(crate) bytes: u64,
}

/// What one sweep did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SweepCount {
    pub(crate) removed: usize,
    pub(crate) bytes: u64,
}

/// Expire units older than `max_age`, then evict oldest-first until the total
/// fits `max_bytes`.
///
/// In that order, and it matters: age is what the operator reasoned about, and
/// applying the quota first would evict something they had said to keep while
/// something already expired sat beside it.
///
/// Best effort throughout. A unit that cannot be read or removed is skipped
/// rather than failing the sweep — this is maintenance, and a store that
/// cannot be tidied must not be a gateway that stops answering.
pub(crate) async fn sweep(
    root: &Path,
    unit_depth: usize,
    max_age: Option<Duration>,
    max_bytes: Option<u64>,
) -> SweepCount {
    if max_age.is_none() && max_bytes.is_none() {
        return SweepCount::default();
    }
    let mut units = collect_units(root, unit_depth).await;
    let mut count = SweepCount::default();

    if let Some(max_age) = max_age {
        let cutoff = SystemTime::now() - max_age;
        let mut kept = Vec::with_capacity(units.len());
        for unit in units {
            if unit.newest >= cutoff {
                kept.push(unit);
                continue;
            }
            if remove(&unit.path).await {
                count.removed += 1;
                count.bytes += unit.bytes;
            }
        }
        units = kept;
    }

    let Some(max_bytes) = max_bytes else {
        return count;
    };
    let mut total: u64 = units.iter().map(|unit| unit.bytes).sum();
    if total <= max_bytes {
        return count;
    }
    // Oldest first: what a quota evicts should be the same thing an age
    // ceiling would have, so a deployment that sets both does not get two
    // different notions of "expendable".
    units.sort_by_key(|unit| unit.newest);
    for unit in units {
        if total <= max_bytes {
            break;
        }
        if remove(&unit.path).await {
            total = total.saturating_sub(unit.bytes);
            count.removed += 1;
            count.bytes += unit.bytes;
        }
    }
    count
}

/// Every unit under `root`, where a unit is an entry `unit_depth` levels down.
///
/// Symlinks are neither followed nor removed, at any level. These directories
/// are the runtime's own, so a link in one is anomalous; descending it would
/// let something outside the store decide what the sweep deletes, and removing
/// it would delete something the store never wrote.
async fn collect_units(root: &Path, unit_depth: usize) -> Vec<Unit> {
    let mut level = vec![root.to_path_buf()];
    // Descend to the level *above* the units, then everything in it is one.
    for _ in 1..unit_depth {
        let mut next = Vec::new();
        for dir in &level {
            let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
                continue;
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
                    next.push(entry.path());
                }
            }
        }
        level = next;
    }

    let mut units = Vec::new();
    for dir in &level {
        let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Ok(kind) = entry.file_type().await else {
                continue;
            };
            if kind.is_symlink() {
                continue;
            }
            let path = entry.path();
            let Some((newest, bytes)) = measure(&path, kind.is_dir()).await else {
                continue;
            };
            units.push(Unit {
                path,
                newest,
                bytes,
            });
        }
    }
    units
}

/// The newest write inside `path` and how many bytes it holds.
async fn measure(path: &Path, is_dir: bool) -> Option<(SystemTime, u64)> {
    if !is_dir {
        let meta = tokio::fs::metadata(path).await.ok()?;
        return Some((meta.modified().ok()?, meta.len()));
    }
    let mut newest: Option<SystemTime> = None;
    let mut bytes = 0u64;
    let mut stack = vec![(path.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        if depth >= MAX_WALK_DEPTH {
            continue;
        }
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Ok(kind) = entry.file_type().await else {
                continue;
            };
            if kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                stack.push((entry.path(), depth + 1));
                continue;
            }
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            bytes += meta.len();
            if let Ok(modified) = meta.modified() {
                newest = Some(newest.map_or(modified, |seen: SystemTime| seen.max(modified)));
            }
        }
    }
    // An empty directory has no newest write. Treated as ancient rather than
    // skipped, so a unit whose files were removed by hand is still cleaned up
    // instead of staying forever as a directory the sweep cannot classify.
    Some((newest.unwrap_or(SystemTime::UNIX_EPOCH), bytes))
}

async fn remove(path: &Path) -> bool {
    let Ok(meta) = tokio::fs::symlink_metadata(path).await else {
        return false;
    };
    if meta.is_dir() {
        tokio::fs::remove_dir_all(path).await.is_ok()
    } else {
        tokio::fs::remove_file(path).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agentos-retention-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temp root");
        root
    }

    /// Backdate a file so age can be tested without waiting for it.
    fn backdate(path: &Path, secs_ago: u64) {
        let when = SystemTime::now() - Duration::from_secs(secs_ago);
        let times = std::fs::FileTimes::new().set_modified(when);
        let file = std::fs::File::options()
            .write(true)
            .open(path)
            .expect("open for utimes");
        file.set_times(times).expect("backdate");
    }

    #[tokio::test]
    async fn an_age_ceiling_removes_only_the_old_files() {
        let root = temp_root("age");
        std::fs::write(root.join("old.jsonl"), "old").expect("write");
        std::fs::write(root.join("new.jsonl"), "new").expect("write");
        backdate(&root.join("old.jsonl"), 3 * 24 * 60 * 60);

        let count = sweep(&root, 1, Some(Duration::from_secs(24 * 60 * 60)), None).await;
        assert_eq!(count.removed, 1);
        assert!(!root.join("old.jsonl").exists());
        assert!(root.join("new.jsonl").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A quota evicts oldest-first and stops as soon as the total fits, rather
    /// than emptying the store.
    #[tokio::test]
    async fn a_quota_evicts_oldest_first_and_then_stops() {
        let root = temp_root("quota");
        for (name, age) in [("a", 300u64), ("b", 200), ("c", 100)] {
            std::fs::write(root.join(name), vec![b'x'; 100]).expect("write");
            backdate(&root.join(name), age);
        }
        // 300 bytes on disk, 150 allowed: the two oldest go, the newest stays.
        let count = sweep(&root, 1, None, Some(150)).await;
        assert_eq!(count.removed, 2);
        assert_eq!(count.bytes, 200);
        assert!(!root.join("a").exists());
        assert!(!root.join("b").exists());
        assert!(root.join("c").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Attachments nest `<channel>/<conversation>/<message>/<file>`, and the
    /// message directory is the unit: all of it or none of it.
    #[tokio::test]
    async fn a_nested_unit_is_removed_whole() {
        let root = temp_root("nested");
        let message = root.join("telegram").join("conv-1").join("msg-1");
        std::fs::create_dir_all(&message).expect("dirs");
        std::fs::write(message.join("one.png"), vec![b'x'; 10]).expect("write");
        std::fs::write(message.join("two.png"), vec![b'x'; 10]).expect("write");
        backdate(&message.join("one.png"), 10 * 24 * 60 * 60);
        backdate(&message.join("two.png"), 10 * 24 * 60 * 60);

        let count = sweep(&root, 3, Some(Duration::from_secs(24 * 60 * 60)), None).await;
        assert_eq!(count.removed, 1);
        assert!(!message.exists());
        assert!(root.join("telegram").join("conv-1").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The newest file in a unit decides its age, so a conversation still
    /// receiving attachments is not swept because of its first one.
    #[tokio::test]
    async fn a_unit_is_as_old_as_its_newest_write() {
        let root = temp_root("newest");
        let unit = root.join("run-1");
        std::fs::create_dir_all(&unit).expect("dirs");
        std::fs::write(unit.join("old.txt"), "old").expect("write");
        std::fs::write(unit.join("new.txt"), "new").expect("write");
        backdate(&unit.join("old.txt"), 30 * 24 * 60 * 60);

        let count = sweep(&root, 1, Some(Duration::from_secs(24 * 60 * 60)), None).await;
        assert_eq!(count.removed, 0);
        assert!(unit.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A link planted in the store must not let something outside it decide
    /// what gets deleted.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_is_neither_followed_nor_removed() {
        let root = temp_root("symlink");
        let outside = temp_root("symlink-target");
        std::fs::write(outside.join("precious.txt"), "keep me").expect("write");
        std::os::unix::fs::symlink(&outside, root.join("link")).expect("symlink");

        let count = sweep(&root, 1, Some(Duration::from_secs(1)), None).await;
        assert_eq!(count.removed, 0, "the link itself is not a unit");
        assert!(outside.join("precious.txt").exists());
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[tokio::test]
    async fn a_missing_root_is_not_a_failure() {
        let root = temp_root("missing").join("never-created");
        assert_eq!(
            sweep(&root, 1, Some(Duration::from_secs(1)), Some(1024)).await,
            SweepCount::default()
        );
    }

    /// Both ceilings off is not "sweep with no limit", it is "do not sweep".
    #[tokio::test]
    async fn no_ceiling_removes_nothing() {
        let root = temp_root("none");
        std::fs::write(root.join("ancient"), "x").expect("write");
        backdate(&root.join("ancient"), 365 * 24 * 60 * 60);
        assert_eq!(sweep(&root, 1, None, None).await.removed, 0);
        assert!(root.join("ancient").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A directory whose files someone removed by hand is still cleaned up.
    #[tokio::test]
    async fn an_empty_unit_counts_as_ancient() {
        let root = temp_root("empty");
        std::fs::create_dir_all(root.join("hollow")).expect("dirs");
        let count = sweep(&root, 1, Some(Duration::from_secs(1)), None).await;
        assert_eq!(count.removed, 1);
        assert!(!root.join("hollow").exists());
        assert_eq!(UNIX_EPOCH, SystemTime::UNIX_EPOCH);
        let _ = std::fs::remove_dir_all(&root);
    }
}
