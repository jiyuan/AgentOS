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
//! no gain here: a spill is produced by one tool call in one run, the locator
//! is absolute so retrieval is unaffected either way, and a run directory is
//! self-contained for retention sweeps.

use agentos_proto::{RunId, ToolCallId};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("spill storage failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// An opaque handle to one spilled artifact.
///
/// The local store renders it as a filesystem path; a future remote store
/// could render a URI or key. Consumers pass it through with the store's
/// [`SpillRef::retrieval_hint`] rather than assuming a path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillLocator(Arc<str>);

impl SpillLocator {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Build a locator without touching the disk. Test-only: production
    /// locators are minted by [`SpillStore::save_text`], which is what makes
    /// them meaningful.
    #[cfg(test)]
    pub fn for_tests(path: &str) -> Self {
        Self(Arc::from(path))
    }
}

impl fmt::Display for SpillLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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
        let dir = self.root.join(safe_segment(source.run_id.as_str()));
        create_private_dir(&dir).await?;

        let path = dir.join(spill_file_name(source));
        write_private_file(&path, content).await?;

        let locator = SpillLocator(Arc::from(path.to_string_lossy().into_owned()));
        Ok(SpillRef {
            bytes: content.len() as u64,
            retrieval_hint: Arc::from(format!(
                "The full output was saved to {locator}. Read it with the `file` tool \
                 (use `offset`/`tail` for a slice) instead of re-running the command."
            )),
            locator,
        })
    }
}

/// One path segment built from untrusted text: every character outside
/// `[A-Za-z0-9_]` becomes `_`, and the result is capped well under any
/// filesystem name limit. `..`, `/`, and `\\` therefore cannot survive, so a
/// segment can only ever name a child of its parent.
fn safe_segment(raw: &str) -> String {
    let mut segment: String = raw
        .chars()
        .take(64)
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
        let read = std::fs::read_to_string(saved.locator.as_str()).expect("the artifact reads");
        assert_eq!(read, content, "the artifact must be byte-identical");
        assert!(saved.retrieval_hint.contains(saved.locator.as_str()));
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

        let file = std::fs::metadata(saved.locator.as_str()).expect("artifact metadata");
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

        let path = PathBuf::from(saved.locator.as_str());
        assert!(
            path.starts_with(&root),
            "{} escaped {}",
            path.display(),
            root.display()
        );
        assert!(!saved.locator.as_str().contains(".."));
    }
}
