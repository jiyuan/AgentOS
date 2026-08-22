//! Persisting oversized tool output instead of destroying it.
//!
//! Roadmap item C2 in `docs/TRANSFER_ROADMAP.md`, review finding F6. A tool
//! result over the inline cap used to be cut at a character boundary and the
//! remainder thrown away, with the model told to "request a smaller range" —
//! which for a non-deterministic command means running it again, and for a
//! one-shot command means the output is simply gone.
//!
//! A spill store writes the full text to a file and hands back an opaque
//! locator. The inline result becomes a bounded preview plus that locator, so
//! the model can read or grep the rest on demand and an operator can still see
//! what a tool actually produced.
//!
//! # What the locator is, and is not
//!
//! `spill:<run>/<artifact>`. It names a place inside *this store* and nothing
//! else. It used to be the absolute host path, rendered with
//! `to_string_lossy()` and handed to the model with an instruction to open it
//! with the `file` tool (M7 / `SPILL-001`). Three things were wrong with that:
//! the host's directory layout landed in a durable transcript that a provider
//! sees on every later turn; the locator was a *path*, so anything that could
//! read a path could read any path; and a spill root outside the workspace was
//! unreadable, because the `file` tool is contained to the workspace.
//!
//! Retrieval is now the `spill_read` tool, which resolves a locator against
//! this store through `paths::RootDir` — so the two segments cannot escape it
//! even if something upstream let a hostile one through — and refuses a
//! locator the calling run's own transcript does not mention. That last check
//! is the authorization: a model may re-read what it was told about, and
//! nothing else. It spans turns for free, because the transcript does.
//!
//! # Storage safety
//!
//! Spilled output is whatever a tool read or fetched, so it is treated as
//! potentially sensitive and potentially hostile:
//!
//! - the root and per-run directories are created `0700`;
//! - files are created with `O_EXCL` and mode `0600`, so a symlink planted at
//!   the target path causes a rejected write rather than a followed one;
//! - every path segment is built by *replacing* unsafe characters rather than
//!   rejecting them, so a runtime-supplied run id or a model-supplied
//!   call id can never escape the root through `..` or a separator. The plan
//!   called for hashing the conversation id; sanitizing gives the same
//!   escape-proof guarantee by construction with no new dependency, keeps
//!   directory names legible to an operator reading the tree, and its only
//!   cost is that two ids differing solely in punctuation share a directory —
//!   harmless, since artifacts are named per tool call inside it.
//!
//! # Why runs, not conversations
//!
//! The plan grouped artifacts by conversation, but `RunState` carries no
//! conversation id and adding one is a breaking `agentos-interfaces` change for
//! no gain here: a spill is produced by one tool call in one run, a run
//! directory is self-contained for retention sweeps, and the run segment in a
//! locator is a *name*, not the thing that authorizes retrieval — so grouping
//! does not decide who can read what.

use crate::paths::{ContainmentError, RootDir};
use agentos_proto::{RunId, ToolCallId};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("spill storage failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// The scheme every locator carries, so a string that is not one is obvious
/// at a glance to a reader and to the parser.
pub const SPILL_SCHEME: &str = "spill:";

/// An opaque handle to one spilled artifact: `spill:<run>/<artifact>`.
///
/// Two sanitized segments and nothing else. Not a path — a locator says where
/// something is *in this store*, and a store is free to be a directory today
/// and an object bucket later without the transcripts written in between
/// meaning something different.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillLocator {
    run: Arc<str>,
    artifact: Arc<str>,
    rendered: Arc<str>,
}

impl SpillLocator {
    fn new(run: String, artifact: String) -> Self {
        let rendered = Arc::from(format!("{SPILL_SCHEME}{run}/{artifact}"));
        Self {
            run: Arc::from(run),
            artifact: Arc::from(artifact),
            rendered,
        }
    }

    /// Read a locator back out of a transcript or a tool argument.
    ///
    /// Rejects anything that is not exactly two segments under the scheme.
    /// This is a parse, not a validation pass over a path: there is no input
    /// it accepts that names something outside a store, so the containment in
    /// `paths::RootDir` beneath it is a second answer to the same question
    /// rather than the only one.
    pub fn parse(raw: &str) -> Option<Self> {
        let body = raw.strip_prefix(SPILL_SCHEME)?;
        let (run, artifact) = body.split_once('/')?;
        if run.is_empty() || artifact.is_empty() || artifact.contains('/') {
            return None;
        }
        // The segments a store writes are already `[A-Za-z0-9_.-]`; anything
        // else came from somewhere that had no business minting a locator.
        let acceptable = |segment: &str| {
            segment.len() <= MAX_SEGMENT_CHARS
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
                && segment != "."
                && segment != ".."
        };
        if !acceptable(run) || !acceptable(artifact) {
            return None;
        }
        Some(Self::new(run.to_owned(), artifact.to_owned()))
    }

    /// The run directory this artifact lives in.
    pub fn run(&self) -> &str {
        &self.run
    }

    /// The artifact's file name within that directory.
    pub fn artifact(&self) -> &str {
        &self.artifact
    }

    pub fn as_str(&self) -> &str {
        &self.rendered
    }

    /// Build a locator without touching the disk. Test-only: production
    /// locators are minted by [`SpillStore::save_text`], which is what makes
    /// them meaningful.
    #[cfg(test)]
    pub fn for_tests(run: &str, artifact: &str) -> Self {
        Self::new(run.to_owned(), artifact.to_owned())
    }
}

impl fmt::Display for SpillLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.rendered)
    }
}

/// Message-metadata key carrying the [`SpillLocator`] of a result whose full
/// text reached the store.
///
/// Written by `loop::items` when a result is spilled and read by
/// `prompt::prune`, which cites it in the elision marker. Shared here so the
/// writer and the reader cannot drift apart.
pub const SPILL_LOCATOR_KEY: &str = "content_spill_locator";

/// One saved artifact: where it is, how big it was, and how to read it back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillRef {
    pub locator: SpillLocator,
    pub bytes: u64,
    /// Model-facing sentence telling it how to retrieve the content. Supplied
    /// by the store because only the store knows what its locator means.
    pub retrieval_hint: Arc<str>,
}

/// Where one spill came from. Descriptive only — used for a readable filename
/// and for operator inspection, never for access control.
#[derive(Clone, Debug)]
pub struct SpillSource<'a> {
    pub run_id: &'a RunId,
    pub tool_name: &'a str,
    pub call_id: &'a ToolCallId,
}

/// How much tool output stays inline, and where the rest goes.
///
/// One struct rather than two dependency fields so the many `RunnerDeps` and
/// `LoopDeps` construction sites can write `Default::default()` and get
/// today's behavior exactly.
#[derive(Clone, Copy, Debug)]
pub struct ContentLimits<'a> {
    /// Bytes of a tool result kept inline before the rest is spilled.
    pub tool_result_inline_bytes: usize,
    /// Where oversized output is persisted. `None` truncates and discards the
    /// remainder, which is the pre-C2 behavior.
    pub spill: Option<&'a SpillStore>,
}

/// Inline cap when a deployment states none. `[limits].tool_result_inline_bytes`
/// overrides it; this constant only names the historical default so an
/// unconfigured runtime behaves as it always did.
pub const DEFAULT_TOOL_RESULT_INLINE_BYTES: usize = 64 * 1024;

impl Default for ContentLimits<'_> {
    fn default() -> Self {
        Self {
            tool_result_inline_bytes: DEFAULT_TOOL_RESULT_INLINE_BYTES,
            spill: None,
        }
    }
}

/// Local filesystem spill storage rooted at one directory.
#[derive(Clone, Debug)]
pub struct SpillStore {
    root: PathBuf,
}

impl SpillStore {
    /// A store rooted at `root`. The directory is created on first write, not
    /// here, so constructing a store never touches the disk.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Persist `content` and return its locator.
    ///
    /// The write is exclusive: if the target path already exists — including as
    /// a symlink someone planted — this fails rather than following it.
    pub async fn save_text(
        &self,
        source: &SpillSource<'_>,
        content: &str,
    ) -> Result<SpillRef, SpillError> {
        let run = safe_segment(source.run_id.as_str());
        let artifact = spill_file_name(source);
        let dir = self.root.join(&run);
        create_private_dir(&dir).await?;
        write_private_file(&dir.join(&artifact), content).await?;

        let locator = SpillLocator::new(run, artifact);
        Ok(SpillRef {
            bytes: content.len() as u64,
            retrieval_hint: Arc::from(format!(
                "The full output was saved as {locator}. Read it with the `spill_read` tool \
                 (use `offset`/`tail` for a slice) instead of re-running the command."
            )),
            locator,
        })
    }

    /// Open the artifact a locator names, contained to this store.
    ///
    /// The walk is `paths::RootDir`'s, one component at a time with
    /// `openat(O_NOFOLLOW)` — so a symlink planted inside the store between
    /// the write and the read is refused rather than followed, and the two
    /// segments cannot name anything above the root even if something
    /// upstream minted a locator this store did not write.
    ///
    /// Authorization is *not* here. This answers "where is it"; whether the
    /// caller may have it is the `spill_read` tool's question, and it asks the
    /// transcript.
    pub fn open(&self, locator: &SpillLocator) -> Result<std::fs::File, ContainmentError> {
        RootDir::open(&self.root)?.open_file(Path::new(locator.run()).join(locator.artifact()))
    }

    /// Delete run directories whose newest artifact is older than `max_age`.
    ///
    /// Returns how many were removed. Best effort throughout: a directory that
    /// cannot be read or removed is skipped rather than failing the sweep, and
    /// the sweep is maintenance — a run that cannot clean up must not be a run
    /// that fails.
    ///
    /// Whole run directories, never individual files: a run's artifacts are
    /// referenced together by the transcript that produced them, so removing
    /// half of one leaves a conversation with locators that resolve and
    /// locators that do not, which is worse than removing all of it.
    pub async fn sweep_older_than(&self, max_age: Duration) -> usize {
        let Ok(mut entries) = tokio::fs::read_dir(&self.root).await else {
            // No root yet means nothing has spilled, which is not a failure.
            return 0;
        };
        let cutoff = SystemTime::now() - max_age;
        let mut removed = 0;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            if newest_write(&path)
                .await
                .is_some_and(|newest| newest >= cutoff)
            {
                continue;
            }
            if tokio::fs::remove_dir_all(&path).await.is_ok() {
                removed += 1;
            }
        }
        removed
    }
}

/// When anything in `dir` was last written, or `None` when it is empty or
/// unreadable.
///
/// The *newest* artifact rather than the directory's own mtime: a run that
/// spilled twice, hours apart, is as recent as its last write, and directory
/// mtimes are not consistent across filesystems.
async fn newest_write(dir: &Path) -> Option<SystemTime> {
    let mut entries = tokio::fs::read_dir(dir).await.ok()?;
    let mut newest = None;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(modified) = entry.metadata().await.and_then(|meta| meta.modified()) {
            newest = Some(newest.map_or(modified, |seen: SystemTime| seen.max(modified)));
        }
    }
    newest
}

/// The cap `safe_segment` applies and [`SpillLocator::parse`] enforces. Well
/// under any filesystem name limit, and short enough that a locator stays
/// readable in a transcript.
const MAX_SEGMENT_CHARS: usize = 64;

/// One path segment built from untrusted text: every character outside
/// `[A-Za-z0-9_]` becomes `_`, and the result is capped well under any
/// filesystem name limit. `..`, `/`, and `\\` therefore cannot survive, so a
/// segment can only ever name a child of its parent.
fn safe_segment(raw: &str) -> String {
    let mut segment: String = raw
        .chars()
        .take(MAX_SEGMENT_CHARS)
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    if segment.is_empty() {
        segment.push('_');
    }
    segment
}

/// `<tool>-<call id>.txt`. Both parts are model- or channel-supplied, so both
/// are sanitized: they name the file but cannot shape the path.
fn spill_file_name(source: &SpillSource<'_>) -> String {
    format!(
        "{}-{}.txt",
        safe_segment(source.tool_name),
        safe_segment(source.call_id.as_str())
    )
}

#[cfg(unix)]
async fn create_private_dir(dir: &Path) -> Result<(), SpillError> {
    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(dir).await.map_err(|source| SpillError::Io {
        path: dir.to_path_buf(),
        source,
    })
}

#[cfg(not(unix))]
async fn create_private_dir(dir: &Path) -> Result<(), SpillError> {
    // No mode bits to set; the parent directory's ACL governs access.
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|source| SpillError::Io {
            path: dir.to_path_buf(),
            source,
        })
}

/// Create the file exclusively and write it. `create_new` is `O_EXCL`, so an
/// existing file *or symlink* at the path is an error rather than a target.
async fn write_private_file(path: &Path, content: &str) -> Result<(), SpillError> {
    use tokio::io::AsyncWriteExt;

    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    // `mode` is tokio's own unix-only inherent method; no std extension trait.
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).await.map_err(|source| SpillError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(content.as_bytes())
        .await
        .map_err(|source| SpillError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.flush().await.map_err(|source| SpillError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agentos-spill-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    struct Cleanup(PathBuf);

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn source<'a>(run: &'a RunId, call: &'a ToolCallId) -> SpillSource<'a> {
        SpillSource {
            run_id: run,
            tool_name: "shell",
            call_id: call,
        }
    }

    #[tokio::test]
    async fn a_large_result_is_recoverable_in_full() {
        let root = temp_root("recover");
        let _cleanup = Cleanup(root.clone());
        let store = SpillStore::new(&root);
        let run = RunId::new("run-1");
        let call = ToolCallId::new("call-1");

        // 1 MiB, well past any inline cap.
        let content = "abcdefgh".repeat(128 * 1024);
        let saved = store
            .save_text(&source(&run, &call), &content)
            .await
            .expect("saving succeeds");

        assert_eq!(saved.bytes, content.len() as u64);
        // Read back through the store, because a locator is not a path any
        // caller can open — which is the point of M7 / `SPILL-001`.
        let mut read = String::new();
        std::io::Read::read_to_string(
            &mut store.open(&saved.locator).expect("the artifact opens"),
            &mut read,
        )
        .expect("the artifact reads");
        assert_eq!(read, content, "the artifact must be byte-identical");
        assert!(saved.retrieval_hint.contains(saved.locator.as_str()));
        assert!(
            !saved.retrieval_hint.contains(&*root.to_string_lossy()),
            "the hint must not put the host spill directory in the transcript"
        );
    }

    #[tokio::test]
    async fn a_planted_symlink_is_rejected_not_followed() {
        let root = temp_root("symlink");
        let _cleanup = Cleanup(root.clone());
        let store = SpillStore::new(&root);
        let run = RunId::new("run-1");
        let call = ToolCallId::new("call-1");
        let spill_source = source(&run, &call);

        // Learn the exact path the store will use, then plant a symlink there
        // pointing at a file we must not clobber.
        let dir = root.join(safe_segment(run.as_str()));
        create_private_dir(&dir).await.expect("dir is creatable");
        let target = root.join("victim.txt");
        std::fs::write(&target, "original").expect("victim is writable");
        let planted = dir.join(spill_file_name(&spill_source));
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &planted).expect("symlink is creatable");
        #[cfg(not(unix))]
        std::fs::write(&planted, "occupied").expect("placeholder is writable");

        let error = store
            .save_text(&spill_source, "attacker-controlled")
            .await
            .expect_err("an occupied path must be rejected");
        assert!(matches!(error, SpillError::Io { .. }));
        assert_eq!(
            std::fs::read_to_string(&target).expect("victim still reads"),
            "original",
            "the write must not have followed the symlink"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn artifacts_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp_root("modes");
        let _cleanup = Cleanup(root.clone());
        let store = SpillStore::new(&root);
        let run = RunId::new("run-1");
        let call = ToolCallId::new("call-1");

        let saved = store
            .save_text(&source(&run, &call), "secret")
            .await
            .expect("saving succeeds");

        let file = store
            .open(&saved.locator)
            .expect("the artifact opens")
            .metadata()
            .expect("artifact metadata");
        assert_eq!(file.permissions().mode() & 0o777, 0o600);
        let dir =
            std::fs::metadata(root.join(safe_segment(run.as_str()))).expect("directory metadata");
        assert_eq!(dir.permissions().mode() & 0o777, 0o700);
    }

    #[tokio::test]
    async fn a_hostile_id_cannot_escape_the_root() {
        let root = temp_root("escape");
        let _cleanup = Cleanup(root.clone());
        let store = SpillStore::new(&root);
        let run = RunId::new("../../etc/passwd");
        let call = ToolCallId::new("../../../evil");

        let saved = store
            .save_text(&source(&run, &call), "content")
            .await
            .expect("saving succeeds");

        // The segments were sanitized, so the locator names a child of the
        // root and nothing above it — and the store can still open it.
        assert!(!saved.locator.run().contains(".."));
        assert!(!saved.locator.artifact().contains(".."));
        assert!(!saved.locator.run().contains('/'));
        assert!(store.open(&saved.locator).is_ok());
        assert!(root
            .join(saved.locator.run())
            .join(saved.locator.artifact())
            .exists());
    }
    /// Retention removes a whole run's artifacts, not some of them: a
    /// conversation with half its locators resolving is worse than one with
    /// none of them.
    #[tokio::test]
    async fn a_sweep_removes_expired_runs_and_keeps_fresh_ones() {
        let root = temp_root("sweep");
        let _ = std::fs::remove_dir_all(&root);
        let store = SpillStore::new(&root);

        for run in ["stale", "fresh"] {
            store
                .save_text(
                    &SpillSource {
                        run_id: &RunId::new(run),
                        call_id: &ToolCallId::new("call-1"),
                        tool_name: "shell",
                    },
                    "output",
                )
                .await
                .expect("the artifact writes");
        }
        // Age one run past the window by moving its file's mtime back. The
        // sweep reads the newest write in each directory, so this is the same
        // state a week-old run would leave.
        let stale = root.join("stale");
        let stale_file = std::fs::read_dir(&stale)
            .expect("the stale run has a directory")
            .next()
            .expect("and an artifact in it")
            .expect("readable")
            .path();
        let old = SystemTime::now() - Duration::from_secs(30 * 24 * 60 * 60);
        filetime_set(&stale_file, old);

        let removed = store
            .sweep_older_than(Duration::from_secs(24 * 60 * 60))
            .await;
        assert_eq!(removed, 1);
        assert!(!stale.exists(), "an expired run is removed whole");
        assert!(root.join("fresh").exists(), "a fresh run is kept");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Nothing to sweep is not a failure: a runtime that has never spilled has
    /// no root, and maintenance must not report an error for that.
    #[tokio::test]
    async fn sweeping_a_store_that_never_wrote_is_a_no_op() {
        let root = temp_root("sweep-empty");
        let _ = std::fs::remove_dir_all(&root);
        let store = SpillStore::new(&root);
        assert_eq!(store.sweep_older_than(Duration::from_secs(60)).await, 0);
    }

    /// Set a file's mtime without pulling in a crate for it.
    ///
    /// `File::set_modified` rather than `touch -d @<epoch>`: the `-d @seconds`
    /// form is a GNU extension, so the previous version of this helper failed
    /// on macOS and every BSD — on a platform AgentOS supports (M2 / `CI-001`).
    fn filetime_set(path: &Path, when: SystemTime) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("the spill artifact is open-able for writing")
            .set_modified(when)
            .expect("the filesystem supports setting mtime");
    }
}
