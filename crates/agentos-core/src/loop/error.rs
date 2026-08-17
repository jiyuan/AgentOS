//! The run loop's error vocabulary.
//!
//! Split out of `loop/mod.rs` to keep it under the module ceiling. Every
//! variant is a way a run can end badly; the loop turns some of them
//! (cancellation, the turn budget) into terminal answers rather than letting
//! them escape.

use crate::subagents::SubAgentError;
use crate::task_workspace::TaskWorkspaceError;
use crate::tools::ToolRegistryError;
use agentos_interfaces::guardrail::GuardrailError;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("run has already finished")]
    AlreadyDone,
    #[error("paused run must be resumed through the approval path")]
    NotResumable,
    #[error("maximum turn count exceeded")]
    MaxTurnsExceeded,
    #[error("orchestrator failed: {0}")]
    Orchestrator(#[from] agentos_interfaces::orchestrator::OrchestratorError),
    #[error("guardrail backend failed: {0}")]
    Guardrail(#[from] GuardrailError),
    #[error("guardrail '{guardrail}' tripped: {reason}")]
    GuardrailTripped {
        guardrail: Arc<str>,
        reason: Arc<str>,
    },
    #[error("tool execution failed: {0}")]
    Tool(#[from] ToolRegistryError),
    #[error("sub-agent execution failed: {0}")]
    SubAgent(#[from] SubAgentError),
    #[error("task workspace failed: {0}")]
    TaskWorkspace(#[from] TaskWorkspaceError),
    #[error("approval denied: {reason}")]
    ApprovalDenied { reason: Arc<str> },
    /// The approval ended without anyone deciding it — the prompt expired, or
    /// the run had no interactive user (roadmap item G2). Fails the run closed
    /// exactly like a denial, and is a separate variant so the audit trail and
    /// the reply to the user can tell the two apart.
    #[error("approval went unanswered: {reason}")]
    ApprovalUnanswered { reason: Arc<str> },
    #[error("approval cannot pause this action yet: {reason}")]
    ApprovalUnsupported { reason: Arc<str> },
    /// The run's cancellation token fired mid-call.
    ///
    /// Internal to the loop: `act` converts it into a terminal stop notice, so
    /// it never reaches a caller of `run_envelope`. It exists because the tool
    /// path is several frames deep and needs a typed way back up.
    #[error("run cancelled")]
    Cancelled,
}
