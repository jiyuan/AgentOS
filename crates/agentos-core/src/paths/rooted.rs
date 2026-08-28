//! Filesystem access that cannot leave a directory, checked by the kernel.
//!
//! M4 / `FS-001`. Containment used to be lexical: [`safe_workspace_path`]
//! rejected absolute paths and `..` components and then handed back
//! `root.join(requested)` for an ordinary `std::fs` call. That is sound about
//! the *string* and says nothing about the *filesystem*. A symlink at
//! `workspace/notes` pointing at `/etc` makes `notes/passwd` a perfectly
//! traversal-free relative path that reads outside the root, and nothing in
//! the string check can see it.
//!
//! It is also racy even when it is right. Between the check and the `open`,
//! anything with write access to a directory on the path — including the
//! agent's own previous tool call — can replace a component with a symlink.
//! The check passed; the open lands somewhere else.
//!
//! [`RootDir`] closes both. It holds an open descriptor for the root and walks
//! to a target one component at a time with `openat(..., O_NOFOLLOW)`, so:
//!
//! - every intermediate component must be a real directory, not a symlink to
//!   one — a planted link fails with `ELOOP` at the syscall rather than being
//!   resolved;
//! - the final `openat` happens against the descriptor of the parent that was
//!   just verified, so there is no window in which the parent can be swapped
//!   for something else. The path is never re-resolved from the root string.
//!
//! # Symlinks are refused, not resolved
//!
//! Not even a symlink whose target *is* inside the root. Deciding that safely
//! means resolving the target and re-proving containment, which reintroduces
//! the race this module exists to remove, and a link that resolves inside the
//! root today can be repointed outside it before the next call. The error says
//! plainly that a symlink was in the way, so an operator who put one there on
//! purpose can see why their path stopped working.
//!
//! [`safe_workspace_path`]: crate::tools::safe_workspace_path

use super::segment::{path_segment, PathSegmentError};
use std::fs::File;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContainmentError {
    #[error("{0}")]
    Segment(#[from] PathSegmentError),
    #[error("path {path} is absolute; only paths relative to {root} are accepted")]
    Absolute { root: PathBuf, path: PathBuf },
    #[error("path {path} walks outside {root}")]
    Traversal { root: PathBuf, path: PathBuf },
    /// A component of the path is a symbolic link. Refused rather than
    /// followed — see the module docs.
    #[error("{component} in {path} is a symbolic link, and paths beneath {root} are not followed")]
    Symlink {
        root: PathBuf,
        path: PathBuf,
        component: String,
    },
    #[error("cannot open {root}: {source}")]
    Root {
        root: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{path} (beneath {root}): {source}")]
    Io {
        root: PathBuf,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// No no-follow filesystem primitive on this platform, so containment
    /// cannot be enforced and access is refused rather than attempted.
    #[error("directory-relative filesystem access is not available on this platform")]
    Unsupported,
}

/// A directory you can descend from and not out of.
///
/// Cheap to clone conceptually but not `Clone`: it owns a descriptor, and two
/// handles to the same directory buy nothing.
#[derive(Debug)]
pub struct RootDir {
    #[cfg(unix)]
    pub(super) fd: std::os::fd::OwnedFd,
    /// The path the root was opened from, for error messages only. Never
    /// re-resolved: every operation goes through [`Self::fd`].
    pub(super) root: PathBuf,
}

impl RootDir {
    /// Open `root` as a containment boundary.
    ///
    /// The root itself may be reached through symlinks — it is the operator's
    /// own configuration, not model-supplied input. Only paths *beneath* it
    /// are held to the no-follow rule.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ContainmentError> {
        let root = root.into();
        #[cfg(unix)]
        {
            let dir = File::open(&root).map_err(|source| ContainmentError::Root {
                root: root.clone(),
                source,
            })?;
            Ok(Self {
                fd: dir.into(),
                root,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = root;
            Err(ContainmentError::Unsupported)
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Split a caller-supplied relative path into validated components.
    ///
    /// Lexical, and deliberately still strict: `..` is refused outright rather
    /// than resolved, because a `..` that cancels out lexically does not
    /// cancel out on a filesystem where an intermediate component is a
    /// symlink. The syscall walk below is what makes the containment real; this
    /// is what makes the error message useful.
    fn components<'a>(&self, path: &'a Path) -> Result<Vec<&'a str>, ContainmentError> {
        if path.is_absolute() {
            return Err(ContainmentError::Absolute {
                root: self.root.clone(),
                path: path.to_owned(),
            });
        }
        let mut names = Vec::new();
        for component in path.components() {
            match component {
                Component::CurDir => {}
                Component::Normal(name) => {
                    let name = name.to_str().ok_or_else(|| ContainmentError::Traversal {
                        root: self.root.clone(),
                        path: path.to_owned(),
                    })?;
                    names.push(path_segment("path component", name)?);
                }
                Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                    return Err(ContainmentError::Traversal {
                        root: self.root.clone(),
                        path: path.to_owned(),
                    })
                }
            }
        }
        if names.is_empty() {
            return Err(ContainmentError::Traversal {
                root: self.root.clone(),
                path: path.to_owned(),
            });
        }
        Ok(names)
    }

    /// Open a file beneath the root for reading.
    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<File, ContainmentError> {
        let path = path.as_ref();
        self.open_leaf(path, LeafMode::Read)
    }

    /// Create or truncate a file beneath the root, creating the directories
    /// leading to it.
    ///
    /// The directories are created through the same no-follow walk, so a
    /// planted symlink in the middle of a not-yet-existing path fails rather
    /// than deciding where the new file lands.
    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<File, ContainmentError> {
        let path = path.as_ref();
        let names = self.components(path)?;
        let (leaf, parents) = names
            .split_last()
            .expect("components() rejects the empty path");
        let parent = self.descend(path, parents, Descend::Create)?;
        open_leaf_at(&parent, leaf, LeafMode::Write)
            .map_err(|failure| self.leaf_error(path, leaf, failure))
    }

    /// Create a new private file, refusing any pre-existing leaf.
    pub fn create_new_file(&self, path: impl AsRef<Path>) -> Result<File, ContainmentError> {
        let path = path.as_ref();
        let names = self.components(path)?;
        let (leaf, parents) = names
            .split_last()
            .expect("components() rejects the empty path");
        let parent = self.descend(path, parents, Descend::Create)?;
        open_leaf_at(&parent, leaf, LeafMode::CreateNew)
            .map_err(|failure| self.leaf_error(path, leaf, failure))
    }

    /// Open a private file for append, creating its descriptor-walked parents.
    ///
    /// The returned descriptor carries `O_APPEND`, so concurrent writers do
    /// not race a userspace seek with their write. A pre-existing symlink at
    /// the leaf is refused exactly like one in an intermediate component.
    pub fn append_file(&self, path: impl AsRef<Path>) -> Result<File, ContainmentError> {
        let path = path.as_ref();
        let names = self.components(path)?;
        let (leaf, parents) = names
            .split_last()
            .expect("components() rejects the empty path");
        let parent = self.descend(path, parents, Descend::Create)?;
        open_leaf_at(&parent, leaf, LeafMode::Append)
            .map_err(|failure| self.leaf_error(path, leaf, failure))
    }

    /// Atomically publish `bytes` beneath this root and make that publication
    /// durable before reporting success.
    ///
    /// The temporary file, rename, cleanup and parent-directory `fsync` are all
    /// descriptor-relative. No model-selected component is resolved again by
    /// host path after the no-follow walk.
    pub fn write_file_atomic(
        &self,
        path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<(), ContainmentError> {
        self.write_file_atomic_using(path.as_ref(), bytes, sync_dir)
    }

    fn write_file_atomic_using(
        &self,
        path: &Path,
        bytes: &[u8],
        sync_parent: impl FnOnce(&DirFd) -> io::Result<()>,
    ) -> Result<(), ContainmentError> {
        let names = self.components(path)?;
        let (leaf, parents) = names
            .split_last()
            .expect("components() rejects the empty path");
        let parent = self.descend(path, parents, Descend::Create)?;
        let temporary = temporary_leaf();
        let mut file = open_leaf_at(&parent, &temporary, LeafMode::CreateNew)
            .map_err(|failure| self.leaf_error(path, &temporary, failure))?;
        if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = remove_leaf_at(&parent, &temporary);
            return Err(ContainmentError::Io {
                root: self.root.clone(),
                path: path.to_owned(),
                source,
            });
        }
        drop(file);
        if let Err(source) = rename_leaf_at(&parent, &temporary, leaf) {
            let _ = remove_leaf_at(&parent, &temporary);
            return Err(ContainmentError::Io {
                root: self.root.clone(),
                path: path.to_owned(),
                source,
            });
        }
        sync_parent(&parent).map_err(|source| ContainmentError::Io {
            root: self.root.clone(),
            path: path.to_owned(),
            source,
        })
    }

    /// Create a directory beneath the root, and every directory leading to it.
    pub fn create_dir_all(&self, path: impl AsRef<Path>) -> Result<(), ContainmentError> {
        let path = path.as_ref();
        let names = self.components(path)?;
        self.descend(path, &names, Descend::Create).map(|_| ())
    }

    /// What is directly inside a directory beneath the root, unsorted.
    ///
    /// `.` and `..` are omitted. Entries carry names rather than paths: a
    /// caller that wants to act on one must come back through this root, which
    /// is the point. Each entry's facts come from an `fstatat` against the
    /// directory's own descriptor, so a listing describes what the walk
    /// reached rather than whatever the path resolves to a moment later.
    pub fn read_dir(&self, path: impl AsRef<Path>) -> Result<Vec<DirEntry>, ContainmentError> {
        let path = path.as_ref();
        let names = self.components(path)?;
        let dir = self.descend(path, &names, Descend::Existing)?;
        list_dir(dir).map_err(|source| ContainmentError::Io {
            root: self.root.clone(),
            path: path.to_owned(),
            source,
        })
    }

    /// What is directly inside the retained root descriptor.
    ///
    /// This is separate from [`Self::read_dir`] because an empty relative path
    /// is intentionally not a valid model-selected target. Inventory code
    /// still needs to enumerate a configured root without resolving its host
    /// path a second time.
    pub fn read_root_dir(&self) -> Result<Vec<DirEntry>, ContainmentError> {
        #[cfg(unix)]
        {
            let dir = self
                .fd
                .try_clone()
                .map_err(|source| ContainmentError::Root {
                    root: self.root.clone(),
                    source,
                })?;
            list_dir(dir).map_err(|source| ContainmentError::Root {
                root: self.root.clone(),
                source,
            })
        }
        #[cfg(not(unix))]
        {
            Err(ContainmentError::Unsupported)
        }
    }

    /// Whether a path beneath the root is a directory.
    ///
    /// `false` for a symlink, whatever it points at, and `false` for anything
    /// that is not there — a caller deciding between "list it" and "read it"
    /// wants the same answer either way.
    pub fn is_dir(&self, path: impl AsRef<Path>) -> bool {
        let path = path.as_ref();
        let Ok(names) = self.components(path) else {
            return false;
        };
        self.descend(path, &names, Descend::Existing).is_ok()
    }

    fn open_leaf(&self, path: &Path, mode: LeafMode) -> Result<File, ContainmentError> {
        let names = self.components(path)?;
        let (leaf, parents) = names
            .split_last()
            .expect("components() rejects the empty path");
        let parent = self.descend(path, parents, Descend::Existing)?;
        open_leaf_at(&parent, leaf, mode).map_err(|failure| self.leaf_error(path, leaf, failure))
    }

    fn leaf_error(&self, path: &Path, leaf: &str, failure: LeafFailure) -> ContainmentError {
        match failure {
            LeafFailure::Symlink => ContainmentError::Symlink {
                root: self.root.clone(),
                path: path.to_owned(),
                component: leaf.to_owned(),
            },
            LeafFailure::Io(source) => ContainmentError::Io {
                root: self.root.clone(),
                path: path.to_owned(),
                source,
            },
        }
    }
}

/// What went wrong opening the last component, already classified: only the
/// platform layer can tell a symlink refusal from an ordinary I/O failure,
/// because only it knows what the kernel reports for one.
pub(crate) enum LeafFailure {
    Symlink,
    Io(io::Error),
}

/// One name inside a directory, described without leaving the root.
///
/// `is_dir` is false for a symlink whatever it points at, and `is_symlink`
/// says so separately — a caller listing a directory should be able to show
/// that an entry exists and is not reachable through this root, rather than
/// either hiding it or describing it as its target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub len: u64,
    pub modified: Option<std::time::SystemTime>,
}

/// Whether an intermediate directory may be created if it is missing.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Descend {
    Existing,
    Create,
}

/// What to do with the last component.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum LeafMode {
    Read,
    Write,
    Append,
    CreateNew,
}

fn temporary_leaf() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        ".agentos-tmp-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

use super::nofollow::{list_dir, open_leaf_at, remove_leaf_at, rename_leaf_at, sync_dir, DirFd};

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn scratch(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentos-rooted-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir is creatable");
        dir
    }

    #[test]
    fn an_ordinary_relative_path_reads_and_writes() {
        let dir = scratch("ordinary");
        let root = RootDir::open(&dir).expect("root opens");

        let mut file = root.create_file("notes/today.md").expect("write");
        file.write_all(b"hello").expect("written");
        drop(file);

        let mut back = String::new();
        root.open_file("notes/today.md")
            .expect("read")
            .read_to_string(&mut back)
            .expect("readable");
        assert_eq!(back, "hello");
        assert!(root.is_dir("notes"));
        let listed = root.read_dir("notes").expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "today.md");
        assert_eq!(listed[0].len, 5);
        assert!(!listed[0].is_dir && !listed[0].is_symlink);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn traversal_never_reaches_the_filesystem() {
        let dir = scratch("traversal");
        let root = RootDir::open(&dir).expect("root opens");

        for path in ["../outside", "notes/../../outside", "/etc/passwd"] {
            let error = root.open_file(path).expect_err("must be refused");
            assert!(
                matches!(
                    error,
                    ContainmentError::Traversal { .. } | ContainmentError::Absolute { .. }
                ),
                "{path}: {error:?}"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The case lexical checking cannot see: no `..` anywhere, and the path
    /// still leaves the root.
    #[test]
    fn a_planted_symlink_directory_is_refused_rather_than_followed() {
        let dir = scratch("planted-dir");
        let outside = scratch("planted-target");
        std::fs::write(outside.join("secret.txt"), b"not yours").expect("target file");
        std::os::unix::fs::symlink(&outside, dir.join("escape")).expect("symlink");

        let root = RootDir::open(&dir).expect("root opens");
        let error = root
            .open_file("escape/secret.txt")
            .expect_err("a link out of the root is not a path inside it");
        assert!(
            matches!(error, ContainmentError::Symlink { ref component, .. } if component == "escape"),
            "{error:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// A link as the *last* component, which the walk reaches through a
    /// different code path than an intermediate one.
    #[test]
    fn a_symlinked_file_is_refused_too() {
        let dir = scratch("planted-file");
        let outside = scratch("planted-file-target");
        let target = outside.join("secret.txt");
        std::fs::write(&target, b"not yours").expect("target file");
        std::os::unix::fs::symlink(&target, dir.join("shortcut.txt")).expect("symlink");

        let root = RootDir::open(&dir).expect("root opens");
        let error = root.open_file("shortcut.txt").expect_err("refused");
        assert!(
            matches!(error, ContainmentError::Symlink { .. }),
            "{error:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// A write through a symlinked directory would create the file at the
    /// link's target, which is the more damaging half of the same defect.
    #[test]
    fn a_write_cannot_land_outside_the_root_through_a_link() {
        let dir = scratch("write-escape");
        let outside = scratch("write-escape-target");
        std::os::unix::fs::symlink(&outside, dir.join("escape")).expect("symlink");

        let root = RootDir::open(&dir).expect("root opens");
        let error = root.create_file("escape/planted.txt").expect_err("refused");
        assert!(
            matches!(error, ContainmentError::Symlink { .. }),
            "{error:?}"
        );
        assert!(
            !outside.join("planted.txt").exists(),
            "nothing may be created outside the root"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn append_and_atomic_publication_refuse_every_symlink_position() {
        let dir = scratch("rooted-write-links");
        let outside = scratch("rooted-write-links-target");
        let canary = outside.join("canary.txt");
        std::fs::write(&canary, b"untouched").expect("canary");
        std::os::unix::fs::symlink(&outside, dir.join("linked-dir")).expect("directory link");
        std::os::unix::fs::symlink(&canary, dir.join("linked-file")).expect("file link");
        let root = RootDir::open(&dir).expect("root opens");

        assert!(matches!(
            root.append_file("linked-dir/append.txt"),
            Err(ContainmentError::Symlink { .. })
        ));
        assert!(matches!(
            root.append_file("linked-file"),
            Err(ContainmentError::Symlink { .. })
        ));
        assert!(matches!(
            root.write_file_atomic("linked-dir/state.txt", b"outside"),
            Err(ContainmentError::Symlink { .. })
        ));
        root.write_file_atomic("linked-file", b"inside")
            .expect("atomic publication replaces a leaf link without following it");
        assert_eq!(
            std::fs::read(&canary).expect("canary readable"),
            b"untouched"
        );
        assert_eq!(
            std::fs::read(dir.join("linked-file")).expect("replacement readable"),
            b"inside"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn fsync_failure_aborts_state_commit() {
        let dir = scratch("sync-failure");
        let root = RootDir::open(&dir).expect("root opens");
        let error = root
            .write_file_atomic_using("state.json".as_ref(), b"new", |_| {
                Err(io::Error::other("injected directory fsync failure"))
            })
            .expect_err("unknown directory durability must not report success");
        assert!(
            matches!(error, ContainmentError::Io { ref source, .. } if source.to_string().contains("injected directory fsync failure")),
            "{error:?}"
        );
        assert!(
            std::fs::read_dir(&dir)
                .expect("root lists")
                .all(|entry| !entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".agentos-tmp-")),
            "an fsync failure must not leak the temporary file"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_symlink_swaps_cannot_redirect_atomic_publication() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = scratch("atomic-race");
        let outside = scratch("atomic-race-target");
        let canary = outside.join("state.txt");
        std::fs::write(&canary, b"outside-canary").expect("canary");
        std::fs::create_dir(dir.join("live")).expect("live directory");
        let stop = Arc::new(AtomicBool::new(false));
        let swapper = {
            let live = dir.join("live");
            let outside = outside.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = std::fs::remove_dir_all(&live);
                    let _ = std::os::unix::fs::symlink(&outside, &live);
                    let _ = std::fs::remove_file(&live);
                    let _ = std::fs::create_dir(&live);
                }
            })
        };
        let root = RootDir::open(&dir).expect("root opens");
        for _ in 0..2_000 {
            let _ = root.write_file_atomic("live/state.txt", b"inside");
        }
        stop.store(true, Ordering::Relaxed);
        swapper.join().expect("swapper finishes");
        assert_eq!(
            std::fs::read(&canary).expect("canary readable"),
            b"outside-canary",
            "descriptor-relative publication escaped through a symlink swap"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// The race the lexical check could never win.
    ///
    /// A thread swaps a legitimate directory for a symlink out of the root
    /// while another repeatedly opens a path through it. With a check-then-open
    /// design, every iteration that lands in the window opens the attacker's
    /// target. Here the open is `openat(O_NOFOLLOW)` against a descriptor for
    /// the parent that was just verified, so the swap can only ever make the
    /// call *fail*.
    #[test]
    fn swapping_a_directory_for_a_link_mid_walk_can_only_cause_a_failure() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = scratch("race");
        let outside = scratch("race-target");
        std::fs::write(outside.join("secret.txt"), b"not yours").expect("target file");
        std::fs::create_dir_all(dir.join("notes")).expect("real directory");
        std::fs::write(dir.join("notes/secret.txt"), b"mine").expect("real file");

        let stop = Arc::new(AtomicBool::new(false));
        let swapper = {
            let dir = dir.clone();
            let outside = outside.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let live = dir.join("notes");
                while !stop.load(Ordering::Relaxed) {
                    // Not atomic, and it does not need to be: the window in
                    // which `notes` is a link pointing out of the root is the
                    // whole attack, and an attacker with write access to the
                    // parent directory has exactly this much control.
                    let _ = std::fs::remove_dir_all(&live);
                    let _ = std::os::unix::fs::symlink(&outside, &live);
                    let _ = std::fs::remove_file(&live);
                    let _ = std::fs::create_dir(&live);
                    let _ = std::fs::write(live.join("secret.txt"), b"mine");
                }
            })
        };

        // The control runs beside the real thing so this test cannot pass
        // vacuously. `std::fs::File::open` on the joined path is exactly what
        // the lexical check handed back, and it *does* escape — if it never
        // did, the swapper would not be winning any races and the assertion
        // below would be proving nothing.
        let joined = dir.join("notes/secret.txt");
        let root = RootDir::open(&dir).expect("root opens");
        let mut escapes = 0;
        let mut lexical_escapes = 0;
        for _ in 0..5_000 {
            if let Ok(mut file) = root.open_file("notes/secret.txt") {
                let mut content = String::new();
                if file.read_to_string(&mut content).is_ok() && content == "not yours" {
                    escapes += 1;
                }
            }
            if let Ok(content) = std::fs::read_to_string(&joined) {
                if content == "not yours" {
                    lexical_escapes += 1;
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        swapper.join().expect("swapper finishes");

        assert_eq!(
            escapes, 0,
            "a directory swapped for a link mid-walk must never yield its target's contents"
        );
        assert!(
            lexical_escapes > 0,
            "the swapper never won a race, so this run proved nothing; \
             path-based access escaped {lexical_escapes} times"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// Containment is about paths beneath the root, not about how the root
    /// itself was reached — that is the operator's own configuration.
    #[test]
    fn the_root_may_itself_be_reached_through_a_link() {
        let real = scratch("root-real");
        let link = scratch("root-link").join("via");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        std::fs::write(real.join("inside.txt"), b"fine").expect("file");

        let root = RootDir::open(&link).expect("a symlinked root still opens");
        assert!(root.open_file("inside.txt").is_ok());

        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_dir_all(link.parent().expect("has a parent"));
    }

    #[test]
    fn a_missing_file_is_an_io_error_not_a_containment_one() {
        let dir = scratch("missing");
        let root = RootDir::open(&dir).expect("root opens");
        let error = root.open_file("absent.txt").expect_err("not there");
        assert!(matches!(error, ContainmentError::Io { .. }), "{error:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
