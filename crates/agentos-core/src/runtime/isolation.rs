//! Where the isolated executor comes from, and what happens when it cannot do
//! the job.

use crate::config::WorkspaceConfig;
use crate::tools::ToolRegistry;
use std::env;
use std::path::PathBuf;

/// The isolated executor this deployment should use, most specific first: the
/// well-known environment variable, then whichever variable `agent.toml`
/// nominates, then the literal path in `agent.toml`.
pub fn isolation_worker_path(config: &WorkspaceConfig) -> Option<PathBuf> {
    env::var_os("AGENTOS_TOOL_WORKER_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            config
                .isolation
                .worker_path_env
                .as_deref()
                .and_then(env::var_os)
                .map(PathBuf::from)
        })
        .or_else(|| config.isolation.worker_path.clone())
}

/// Refuse to start rather than accept a tool whose declared sandbox mode
/// nothing in this runtime can enforce.
///
/// M4 / `SBX-001`, the registration half of
/// `docs/adr/0002-FAIL_CLOSED_ISOLATION.md`. The registry checks the same thing
/// again on every call — this pass exists so the operator hears about a
/// containment gap while they are still looking at the terminal, rather than
/// three turns into someone's conversation.
///
/// Silent for the shipped configuration: every built-in but `shell` declares
/// `full_access`, and `shell` hardens its own child.
pub(super) async fn refuse_unenforceable_isolation(tools: &ToolRegistry) -> Result<(), String> {
    let problems = tools.isolation_problems().await;
    if problems.is_empty() {
        return Ok(());
    }
    Err(format!("refusing to start: {}", problems.join("; ")))
}
