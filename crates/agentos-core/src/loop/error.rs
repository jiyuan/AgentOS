//! The run loop's error vocabulary.
//!
//! Split out of `loop/mod.rs` to keep it under the module ceiling. Every
//! variant is a way a run can end badly; the loop turns some of them
//! (cancellation, the turn budget) into terminal answers rather than letting
//! them escape.

use agentos_interfaces::run_state::RunState;

use crate::audit::{SafetyEventKind, SafetyLogError};
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
    /// A required durable action/delivery transition failed. The action must
    /// not start, or the run must stop at the ambiguous boundary.
    #[error("durable run state failed: {reason}")]
    DurableState { reason: Arc<str> },
    /// Required evidence for a safety-boundary decision could not be made
    /// durable. The protected action must not proceed after this error.
    #[error("required safety event '{event}' could not be persisted: {source}")]
    SafetyEvidence {
        event: &'static str,
        #[source]
        source: SafetyLogError,
    },
    /// A human was asked and said no.
    ///
    /// Distinct from [`Self::StructuralDenial`] because they are different
    /// facts about who refused, and the reply a user should see differs: one
    /// is their own decision coming back, the other is the deployment's
    /// configuration. Both fail the run closed. The same argument already
    /// separates [`Self::ApprovalUnanswered`] from this one.
    #[error("approval denied: {reason}")]
    ApprovalDenied { reason: Arc<str> },
    /// The policy engine refused an action nobody can retry around: a handoff,
    /// a delegation, an escalation, or a plan that reached `Act` without a
    /// live authorization.
    ///
    /// The *recoverable* counterpart has no variant here on purpose. A denied
    /// tool call comes back as a `Denied` `ToolResult` the model reads and
    /// replans around, so it never becomes a `RunError` at all — which is the
    /// type-level statement that the two are not the same kind of denial
    /// (M6 / `STATE-001`, deliverable 4).
    #[error("policy denied {subject}: {reason}")]
    StructuralDenial { subject: Arc<str>, reason: Arc<str> },
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

impl RunError {
    pub(crate) fn safety_evidence(kind: SafetyEventKind, source: SafetyLogError) -> Self {
        Self::SafetyEvidence {
            event: kind.as_str(),
            source,
        }
    }
}

/// A step that failed, carrying the state the run had reached when it did.
///
/// `RunLoopState::step` consumes `self`, so before this existed a failing step
/// dropped the state on the way out and the runner had nothing left to
/// persist: a run that ended badly left no trace at all, which made the runs
/// most worth reconstructing the ones that recorded the least (M6 / `AUD-001`,
/// [ADR-0005](../../../../docs/adr/0005-SAFETY_EVENTS.md)).
///
/// The state is not optional. Every path out of every state owns its
/// `RunState` at the point it fails, so "we lost it" is not a case the type
/// needs to represent — and making it representable is how it would come back.
pub struct StepFailure {
    /// Boxed so `Result<RunLoopState, StepFailure>` does not carry a whole
    /// `RunState` in its error variant on every ordinary step. One allocation
    /// on a path that is already ending the run.
    state: Box<RunState>,
    error: RunError,
}

impl StepFailure {
    pub fn new(state: RunState, error: impl Into<RunError>) -> Self {
        Self {
            state: Box::new(state),
            error: error.into(),
        }
    }

    pub fn error(&self) -> &RunError {
        &self.error
    }

    /// The state to persist and the error to report.
    pub fn into_parts(self) -> (RunState, RunError) {
        (*self.state, self.error)
    }
}

/// Renders the error and the run it belongs to, not the whole transcript: a
/// `.expect()` on a failed step should print something a person can read.
impl std::fmt::Debug for StepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StepFailure")
            .field("run_id", &self.state.run_id.as_str())
            .field("error", &self.error)
            .finish()
    }
}

impl std::fmt::Display for StepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl From<StepFailure> for RunError {
    fn from(failure: StepFailure) -> Self {
        failure.error
    }
}
