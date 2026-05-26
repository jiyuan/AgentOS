//! Workspace path resolution. The single source of truth for `AGENTOS_HOME`.
//!
//! Every workspace path in the system (agent config, traces, sessions, skills,
//! crons, attachments, logs, PID files) is derived from one anchor:
//! `AGENTOS_HOME`. There are no per-path env overrides — this function is the
//! only knob.
//!
//! Lives in `agentos-interfaces` because both `agentos-core` (tool builtins)
//! and `agentos-llm` (env loading) need to call it, and `core` must not depend
//! on `llm`. Zero-dep (std::env + std::path only), no trait, no async.

use std::env;
use std::path::{Path, PathBuf};

/// Resolve `AGENTOS_HOME` using the documented three-tier cascade:
///
/// 1. Process env var `AGENTOS_HOME` (set by parent shell or by `.env` load).
/// 2. Parent directory of the `.env` file that was loaded at startup, when
///    `loaded_env_path` is `Some`.
/// 3. `env::current_dir()` as last resort. Falls back to `"."` if even that
///    fails (e.g., the CWD was unlinked).
///
/// Callers that have no access to the loaded `.env` path (for example,
/// tool implementations running long after startup) pass `None`; they then
/// rely on the env var or CWD.
pub fn agentos_home(loaded_env_path: Option<&Path>) -> PathBuf {
    if let Some(val) = env::var_os("AGENTOS_HOME") {
        return PathBuf::from(val);
    }
    if let Some(env_path) = loaded_env_path {
        if let Some(parent) = env_path.parent() {
            return parent.to_path_buf();
        }
    }
    env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // env::set_var is process-global; serialize the env-touching tests so
    // they don't race with each other.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env_unset<F: FnOnce()>(key: &str, f: F) {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let prev = env::var_os(key);
        env::remove_var(key);
        f();
        match prev {
            Some(v) => env::set_var(key, v),
            None => env::remove_var(key),
        }
    }

    fn with_env_set<F: FnOnce()>(key: &str, value: &str, f: F) {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let prev = env::var_os(key);
        env::set_var(key, value);
        f();
        match prev {
            Some(v) => env::set_var(key, v),
            None => env::remove_var(key),
        }
    }

    #[test]
    fn env_var_wins_over_loaded_env_path_and_cwd() {
        with_env_set("AGENTOS_HOME", "/explicit/from/env", || {
            let loaded = PathBuf::from("/some/other/place/.env");
            assert_eq!(
                agentos_home(Some(&loaded)),
                PathBuf::from("/explicit/from/env")
            );
            assert_eq!(agentos_home(None), PathBuf::from("/explicit/from/env"));
        });
    }

    #[test]
    fn loaded_env_parent_used_when_var_unset() {
        with_env_unset("AGENTOS_HOME", || {
            let loaded = PathBuf::from("/repo/root/.env");
            assert_eq!(agentos_home(Some(&loaded)), PathBuf::from("/repo/root"));
        });
    }

    #[test]
    fn cwd_fallback_when_no_var_and_no_loaded_path() {
        with_env_unset("AGENTOS_HOME", || {
            let resolved = agentos_home(None);
            // Should equal CWD, which is at minimum non-empty.
            assert!(resolved.as_os_str().len() > 0);
        });
    }
}
