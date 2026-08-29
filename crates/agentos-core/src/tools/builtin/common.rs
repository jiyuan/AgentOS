//! Helpers shared across the built-in tool implementations.
//!
//! Two flavours live here:
//!
//! 1. **Production helpers**: `workspace_root`, `safe_workspace_path`,
//!    `elapsed_ms`, `result_metadata`, `default_cron_dir`, `default_skills_dir`.
//!    All path helpers derive from `AGENTOS_HOME` via
//!    `agentos_interfaces::agentos_home(None)`. The runtime pins `AGENTOS_HOME`
//!    once at startup; the fallbacks here are only for standalone tool use.
//!
//! 2. **Test plumbing**: `TEST_CRON_DIR`, `TEST_SKILLS_DIR`, and the matching
//!    RAII guards. Production code never sets these — `cron_root_for_tests`
//!    and `skills_root_for_tests` short-circuit to `None` outside `cfg(test)`.

use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Resolve the workspace root for model-supplied path checks. Derived from
/// `AGENTOS_HOME` (set by the runtime at startup, or by the user shell);
/// standalone tool use falls back to the current directory.
pub(crate) fn workspace_root() -> PathBuf {
    agentos_interfaces::agentos_home(None)
}

/// On-disk root for skill bundles, used by both the `skill_validate` tool and
/// the skill-bundle write boundary guardrail so they resolve the same
/// directory. Honors the thread-local test override; otherwise derives from
/// `AGENTOS_HOME` joined with `workspace/skills`.
pub(crate) fn skills_dir() -> PathBuf {
    if let Some(dir) = skills_root_for_tests() {
        return dir;
    }
    workspace_root().join("workspace").join("skills")
}

/// Validate that `requested` stays inside `root`. Reject:
/// - absolute paths (the model should always pass workspace-relative paths),
/// - any `..` component, even if it cancels out — denial is simpler than
///   resolving relative-path arithmetic safely,
/// - the empty path.
///
/// Returns the path resolved against `root` (so callers get a usable
/// absolute path for I/O). This is intentionally stricter than
/// `Path::canonicalize`, which requires the target to exist and would fail
/// for "write to a new file."
///
/// **This is not the containment boundary.** It answers for the *string*: no
/// `..`, no root prefix. It cannot see a symlink, and it cannot see anything
/// that changes between this call and the caller's `open`. Since M4 /
/// `FS-001` the enforcement is [`crate::paths::RootDir`], which walks to the
/// target with `openat(O_NOFOLLOW)` and operates on the resulting descriptor.
/// What remains here is *classification* — deciding which tree a requested
/// path belongs to, as the skill-bundle guardrail does — and building a path
/// to hand to an API that takes one.
pub(crate) fn safe_workspace_path(root: &Path, requested: &Path) -> Result<PathBuf, String> {
    if requested.as_os_str().is_empty() {
        return Err("empty path".to_owned());
    }
    if requested.is_absolute() {
        return Err(format!(
            "path {} is absolute; tools accept workspace-relative paths only",
            requested.display()
        ));
    }
    for component in requested.components() {
        use std::path::Component;
        match component {
            Component::ParentDir => {
                return Err(format!(
                    "path {} contains a '..' component; tools refuse paths that walk outside the workspace",
                    requested.display()
                ));
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(format!(
                    "path {} has a root prefix; tools accept relative paths only",
                    requested.display()
                ));
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    Ok(root.join(requested))
}

pub(super) fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

pub(super) fn result_metadata(duration_ms: u64, bytes_out: u64) -> BTreeMap<Arc<str>, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("duration_ms"), Value::from(duration_ms));
    metadata.insert(Arc::from("bytes_out"), Value::from(bytes_out));
    metadata
}

/// Resolve the on-disk root for cron task files. Derived from `AGENTOS_HOME`
/// joined with `workspace/crons`.
pub(super) fn default_cron_dir() -> PathBuf {
    workspace_root().join("workspace").join("crons")
}

/// Resolve the on-disk root for workspace skills. Derived from `AGENTOS_HOME`
/// joined with `workspace/skills`.
pub(super) fn default_skills_dir() -> PathBuf {
    workspace_root().join("workspace").join("skills")
}

/// A tool result the model should read and correct, not an error that ends the
/// run: an unknown job id, a spill locator this run never cited, a registry
/// refusal. Carries no metadata — a caller with fields to attach (an egress
/// refusal, a timeout, a tripped guardrail) builds the result itself.
pub(crate) fn failed_result(call: &ToolCall, message: &str) -> ToolResult {
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Failed,
        content: Arc::from(message),
        metadata: BTreeMap::new(),
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static TEST_CRON_DIR: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
    pub(super) static TEST_SKILLS_DIR: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn cron_root_for_tests() -> Option<PathBuf> {
    TEST_CRON_DIR.with(|cell| cell.borrow().clone())
}

#[cfg(not(test))]
pub(super) fn cron_root_for_tests() -> Option<PathBuf> {
    None
}

#[cfg(test)]
pub(super) fn skills_root_for_tests() -> Option<PathBuf> {
    TEST_SKILLS_DIR.with(|cell| cell.borrow().clone())
}

#[cfg(not(test))]
pub(super) fn skills_root_for_tests() -> Option<PathBuf> {
    None
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use agentos_proto::{ToolCall, ToolCallId};
    use serde_json::value::RawValue;

    pub(in crate::tools::builtin) fn unique_tmp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// Construct a stub `ToolCall` for unit tests. The `name` is the LLM-side
    /// tool name; in tests it doesn't have to match the receiving tool, but
    /// pass the right one when round-tripping through `ToolRegistry`.
    pub(in crate::tools::builtin) fn tool_call(name: &str, id: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: Arc::from(name),
            args: RawValue::from_string("{}".to_owned()).unwrap(),
        }
    }

    /// RAII guard installing a thread-local cron directory override for the
    /// duration of a single test, then restoring the prior value and cleaning
    /// up the temp dir.
    pub(in crate::tools::builtin) struct CronDirGuard {
        prior: Option<PathBuf>,
        pub(in crate::tools::builtin) dir: PathBuf,
    }

    impl CronDirGuard {
        pub(in crate::tools::builtin) fn new(prefix: &str) -> Self {
            let dir = unique_tmp_dir(prefix);
            let prior = TEST_CRON_DIR.with(|cell| cell.borrow_mut().replace(dir.clone()));
            Self { prior, dir }
        }
    }

    impl Drop for CronDirGuard {
        fn drop(&mut self) {
            TEST_CRON_DIR.with(|cell| *cell.borrow_mut() = self.prior.clone());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Same as `CronDirGuard` but for skills.
    pub(in crate::tools::builtin) struct SkillsDirGuard {
        prior: Option<PathBuf>,
        pub(in crate::tools::builtin) dir: PathBuf,
    }

    impl SkillsDirGuard {
        pub(in crate::tools::builtin) fn new(prefix: &str) -> Self {
            let dir = unique_tmp_dir(prefix);
            let prior = TEST_SKILLS_DIR.with(|cell| cell.borrow_mut().replace(dir.clone()));
            Self { prior, dir }
        }
    }

    impl Drop for SkillsDirGuard {
        fn drop(&mut self) {
            TEST_SKILLS_DIR.with(|cell| *cell.borrow_mut() = self.prior.clone());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_workspace_path_accepts_relative() {
        let root = PathBuf::from("/tmp/ws");
        let p = safe_workspace_path(&root, Path::new("skills/foo/SKILL.md")).unwrap();
        assert!(p.ends_with("skills/foo/SKILL.md"));
    }

    #[test]
    fn safe_workspace_path_rejects_absolute() {
        let root = PathBuf::from("/tmp/ws");
        let err = safe_workspace_path(&root, Path::new("/etc/passwd")).unwrap_err();
        assert!(err.contains("absolute"));
    }

    #[test]
    fn safe_workspace_path_rejects_parent_traversal() {
        let root = PathBuf::from("/tmp/ws");
        let err = safe_workspace_path(&root, Path::new("../etc/passwd")).unwrap_err();
        assert!(err.contains(".."));
    }

    #[test]
    fn safe_workspace_path_rejects_inner_traversal() {
        let root = PathBuf::from("/tmp/ws");
        // Even cancelling-out `..` is rejected — denial is simpler than
        // proving the canonical form stays inside the root.
        let err = safe_workspace_path(&root, Path::new("foo/../bar")).unwrap_err();
        assert!(err.contains(".."));
    }

    #[test]
    fn safe_workspace_path_rejects_empty() {
        let root = PathBuf::from("/tmp/ws");
        assert!(safe_workspace_path(&root, Path::new("")).is_err());
    }
}
