mod commands;
mod max;
mod min;
mod routing;
mod streaming;

pub use max::{MaxOrchestrator, MemoryHydrationSettings};
pub use min::{EchoOrchestrator, MinOrchestrator};

use agentos_interfaces::orchestrator::OrchestratorError;
use agentos_llm::LlmError;
use std::sync::Arc;

/// Project a provider failure into the orchestrator's error, keeping the one
/// class the run loop acts on.
///
/// Every orchestrator that reaches a model routes its errors through here.
/// Collapsing a context-length rejection into `Backend` would cost the run its
/// only chance to recover from one (roadmap item C4), and the compiler cannot
/// catch that omission — hence one shared conversion rather than a `map_err`
/// closure per call site.
pub(crate) fn planning_error(error: LlmError) -> OrchestratorError {
    match error {
        LlmError::ContextLengthExceeded(detail) => OrchestratorError::ContextLengthExceeded(detail),
        other => OrchestratorError::Backend(Arc::from(other.to_string())),
    }
}
