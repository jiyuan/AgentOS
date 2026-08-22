//! What an isolated executor can actually do, asked rather than assumed.
//!
//! M4 / `SBX-001`, closing `docs/adr/0002-FAIL_CLOSED_ISOLATION.md`. The
//! registry used to dispatch a sandboxed tool to
//! `crates/agentos-core/src/bin/agentos-tool-worker.rs` as an opaque
//! stdin/stdout JSON subprocess, with no way to discover before dispatching
//! that the worker rejects every tool but `shell`, or that the machine it runs
//! on has no sandbox backend at all. Both discoveries arrived, if at all, as a
//! non-zero exit and a line of stderr.
//!
//! The worker now answers [`CAPABILITIES_FLAG`] with an
//! [`ExecutorCapabilities`] document: the protocol it speaks, the tools it can
//! execute, and the sandbox modes it observed its own process being able to
//! enforce. [`probe`] asks once; [`ExecutorCapabilities::can_run`] decides.
//! Every way that decision can go wrong is a distinct, named condition, because
//! "the executor is missing", "the executor cannot run this tool", and "the
//! executor cannot enforce this mode" want three different fixes from whoever
//! reads the error.
//!
//! # Why the worker reports enforcement rather than the registry
//!
//! `crate::sandbox::availability()` answers for the process that calls it. The
//! process that matters is the worker's — it may be a different binary, built
//! from a different tree, running under a different kernel policy than the
//! gateway that spawns it. Asking the gateway what the worker can enforce is
//! the same assumption this module exists to remove.

use crate::sandbox::{self, Sandbox};
use crate::tools::exec::{self, Exec};
use agentos_interfaces::tool::SandboxMode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// The handshake this build speaks. Bumped when the request or response shape
/// changes in a way an older peer would misread.
pub const EXECUTOR_PROTOCOL_VERSION: u32 = 1;

/// Argument that asks an executor to describe itself instead of running a call.
pub const CAPABILITIES_FLAG: &str = "--capabilities";

/// The executor ran the tool and the tool itself returned an error.
///
/// Recoverable: the model reads the failure and replans, exactly as it would
/// for an in-process tool error.
pub const EXIT_TOOL_FAILED: i32 = 1;

/// The executor could not run the tool at all — malformed request, unknown
/// tool, sandbox refused.
///
/// Not recoverable by replanning: the deployment is wrong, and the run aborts
/// rather than letting the model retry into the same wall.
pub const EXIT_WORKER_FAILED: i32 = 2;

/// How long the handshake may take. Generous for a process that only prints
/// JSON, short enough that a wedged executor does not stall the first
/// sandboxed call for a minute.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The handshake is a fixed-shape document; anything larger is a broken peer.
const PROBE_MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// What an isolated executor says it can do.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutorCapabilities {
    /// The handshake version the executor speaks.
    pub protocol: u32,
    /// The executor binary's own name, for traces and error messages.
    pub worker: String,
    /// Tools this executor can execute, by name. A tool absent from this set
    /// is refused before the call is dispatched rather than after.
    pub tools: BTreeSet<String>,
    /// The kernel mechanism the executor observed on its own machine
    /// (`landlock`, `seatbelt`), or `None` when it observed none.
    #[serde(default)]
    pub sandbox_mechanism: Option<String>,
    /// Modes the executor can actually enforce. Empty when it can enforce
    /// nothing, which makes every sandboxed dispatch to it an error rather
    /// than a silent full-access run.
    pub sandbox_modes: BTreeSet<SandboxMode>,
}

impl ExecutorCapabilities {
    /// What this build's worker can do, answered from the worker's own
    /// process.
    ///
    /// Lives here rather than in the binary so the shape is defined once and
    /// the registry's expectations and the worker's answer cannot drift.
    pub fn for_this_worker(worker: &str, tools: impl IntoIterator<Item = &'static str>) -> Self {
        let (sandbox_mechanism, sandbox_modes) = match sandbox::availability() {
            sandbox::Availability::Enforced(mechanism) => (
                Some(mechanism.to_owned()),
                BTreeSet::from([SandboxMode::ReadOnly, SandboxMode::WorkspaceWrite]),
            ),
            sandbox::Availability::Unavailable(_) => (None, BTreeSet::new()),
        };
        Self {
            protocol: EXECUTOR_PROTOCOL_VERSION,
            worker: worker.to_owned(),
            tools: tools.into_iter().map(str::to_owned).collect(),
            sandbox_mechanism,
            sandbox_modes,
        }
    }

    /// Whether this executor can run `tool` under `mode`, and if not, which of
    /// the three distinct reasons applies.
    pub fn can_run(&self, tool: &str, mode: SandboxMode) -> Result<(), Incompatible> {
        if self.protocol != EXECUTOR_PROTOCOL_VERSION {
            return Err(Incompatible::Protocol {
                theirs: self.protocol,
            });
        }
        if !self.tools.contains(tool) {
            return Err(Incompatible::ToolUnsupported);
        }
        if !self.sandbox_modes.contains(&mode) {
            return Err(Incompatible::ModeUnenforceable);
        }
        Ok(())
    }
}

/// Why an executor cannot take a particular call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Incompatible {
    /// The executor speaks a handshake this build does not. Almost always a
    /// worker binary left behind by an older install.
    Protocol { theirs: u32 },
    /// The executor has no implementation for this tool. The shipped worker
    /// carries the built-ins it was compiled with and nothing else, so an MCP
    /// tool configured with a sandbox mode lands here.
    ToolUnsupported,
    /// The executor runs, but on a machine where the kernel cannot enforce the
    /// mode the tool declared.
    ModeUnenforceable,
}

impl Incompatible {
    /// A phrase that completes "the isolated executor ...".
    pub fn describe(self) -> String {
        match self {
            Self::Protocol { theirs } => format!(
                "speaks handshake version {theirs}, but this build speaks \
                 {EXECUTOR_PROTOCOL_VERSION}"
            ),
            Self::ToolUnsupported => "has no implementation for this tool".to_owned(),
            Self::ModeUnenforceable => {
                "cannot enforce this sandbox mode on the machine it runs on".to_owned()
            }
        }
    }
}

/// Ask an executor what it can do.
///
/// Runs the executor unsandboxed: the handshake is the step that establishes
/// whether a sandbox is available at all, so requiring one first would be
/// circular. It reads no input, writes one JSON document, and is bounded by
/// [`PROBE_TIMEOUT`] and [`PROBE_MAX_OUTPUT_BYTES`].
pub async fn probe(runner: &Path, extra_env: &[String]) -> Result<ExecutorCapabilities, Arc<str>> {
    let program = runner.to_string_lossy().into_owned();
    let args = vec![CAPABILITIES_FLAG.to_owned()];
    let unrestricted = Sandbox::unrestricted();
    let output = exec::run(Exec {
        program: &program,
        args: &args,
        cwd: None,
        stdin: None,
        timeout: PROBE_TIMEOUT,
        max_output_bytes: PROBE_MAX_OUTPUT_BYTES,
        sandbox: &unrestricted,
        extra_env,
    })
    .await
    .map_err(|err| Arc::from(err.to_string()))?;

    if !output.success {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        return Err(Arc::from(if detail.is_empty() {
            format!("{program} refused the {CAPABILITIES_FLAG} handshake")
        } else {
            format!("{program} refused the {CAPABILITIES_FLAG} handshake: {detail}")
        }));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|err| Arc::from(format!("{program} returned an unreadable handshake: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities(tools: &[&str], modes: &[SandboxMode]) -> ExecutorCapabilities {
        ExecutorCapabilities {
            protocol: EXECUTOR_PROTOCOL_VERSION,
            worker: "agentos-tool-worker".to_owned(),
            tools: tools.iter().map(|tool| (*tool).to_owned()).collect(),
            sandbox_mechanism: Some("landlock".to_owned()),
            sandbox_modes: modes.iter().copied().collect(),
        }
    }

    #[test]
    fn a_supported_tool_under_an_enforceable_mode_is_compatible() {
        let caps = capabilities(&["shell"], &[SandboxMode::WorkspaceWrite]);
        assert_eq!(caps.can_run("shell", SandboxMode::WorkspaceWrite), Ok(()));
    }

    /// The (ii) exposure in ADR-0002: an MCP tool configured with a sandbox
    /// mode is routed to a worker that has never heard of it.
    #[test]
    fn a_tool_the_executor_never_heard_of_is_named_as_such() {
        let caps = capabilities(&["shell"], &[SandboxMode::ReadOnly]);
        assert_eq!(
            caps.can_run("scrape_page", SandboxMode::ReadOnly),
            Err(Incompatible::ToolUnsupported)
        );
    }

    /// Distinct from `ToolUnsupported` on purpose: the executor is right, the
    /// machine is wrong, and the fix is a different one.
    #[test]
    fn an_executor_with_no_backend_cannot_enforce_any_mode() {
        let caps = ExecutorCapabilities {
            sandbox_mechanism: None,
            sandbox_modes: BTreeSet::new(),
            ..capabilities(&["shell"], &[])
        };
        assert_eq!(
            caps.can_run("shell", SandboxMode::WorkspaceWrite),
            Err(Incompatible::ModeUnenforceable)
        );
    }

    #[test]
    fn a_worker_from_another_build_is_refused_before_anything_else_is_checked() {
        // Refused even though it lists the tool and the mode: a peer that
        // speaks a different handshake may mean something else by both.
        let caps = ExecutorCapabilities {
            protocol: EXECUTOR_PROTOCOL_VERSION + 1,
            ..capabilities(&["shell"], &[SandboxMode::WorkspaceWrite])
        };
        assert_eq!(
            caps.can_run("shell", SandboxMode::WorkspaceWrite),
            Err(Incompatible::Protocol {
                theirs: EXECUTOR_PROTOCOL_VERSION + 1
            })
        );
    }

    #[test]
    fn the_handshake_round_trips_through_json() {
        let caps = capabilities(&["shell"], &[SandboxMode::ReadOnly]);
        let encoded = serde_json::to_vec(&caps).expect("serialisable");
        let decoded: ExecutorCapabilities = serde_json::from_slice(&encoded).expect("readable");
        assert_eq!(decoded, caps);
    }

    #[tokio::test]
    async fn an_executor_that_is_not_there_fails_the_handshake_rather_than_hanging() {
        let error = probe(Path::new("/nonexistent/agentos-tool-worker"), &[])
            .await
            .expect_err("a missing executor cannot describe itself");
        assert!(error.contains("agentos-tool-worker"), "{error}");
    }

    /// A binary that exists and answers something else is as unusable as one
    /// that is absent, and says so rather than being parsed hopefully.
    #[tokio::test]
    async fn an_executor_that_answers_with_nonsense_is_refused() {
        let error = probe(Path::new("/bin/echo"), &[])
            .await
            .expect_err("echo does not speak the handshake");
        assert!(error.contains("unreadable handshake"), "{error}");
    }
}
