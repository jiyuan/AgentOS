use crate::ids::{AgentId, Namespace, RunId};
use crate::Usage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Durable lifecycle record for one physical provider invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestAttempt {
    /// Stable across transport retries of the same logical request.
    pub logical_request_id: Arc<str>,
    /// One-based physical invocation number within the logical request.
    pub attempt: u32,
    pub run_id: RunId,
    pub active_agent: AgentId,
    pub header: RequestHeader,
    pub status: RequestAttemptStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    /// Bounded classification/detail for failed or cancelled attempts. Never
    /// contains request content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestAttemptStatus {
    Started,
    Succeeded,
    Failed,
    Cancelled,
}

/// What one assembled provider request was made of.
///
/// Recorded once per LLM round-trip so a past request can be rebuilt from the
/// run's trace plus the code and data that produced it. It deliberately carries
/// **provenance, not content**: which skills contributed, which memory records
/// were selected, how much each section weighed. Copying the bodies in would
/// put user memory into every trace file, which
/// [`ARCHITECTURE.md` §14](../../../docs/ARCHITECTURE.md) forbids, and would
/// duplicate data the workspace and the memory store already own.
///
/// The reconstruction standard is therefore "log + code": given this header,
/// the session transcript, the workspace skill files, and the memory records it
/// names, the exact message list is derivable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestHeader {
    /// What sort of request this was — the turn itself, the routing
    /// classifier, the compaction summarizer (M5 / `REQ-001`).
    ///
    /// A header without one cannot be told apart in a replay, and the section
    /// sets differ by kind: the classifier legitimately carries no transcript,
    /// which would otherwise read as a request that lost its conversation. See
    /// `agentos_core::prompt::RequestKind` for the vocabulary and
    /// [`ADR-0004`](../../../docs/adr/0004-REQUEST_KINDS.md) for why a kind
    /// rather than an exception.
    ///
    /// Defaulted for deserialization so a trace written before this field
    /// existed still reads; the default names itself as unknown rather than
    /// guessing `turn`.
    #[serde(default = "unknown_kind")]
    pub kind: Arc<str>,
    /// The sections that contributed, in assembly order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sections: Vec<RequestSection>,
    /// Messages sent, across every section.
    pub total_messages: usize,
    /// Characters sent, across every section. A size proxy alongside the
    /// token estimate, and the one figure that involves no heuristic.
    pub total_chars: usize,
    /// Estimated tokens for the whole request, including
    /// [`Self::tool_tokens`]. Heuristic, and deliberately biased high — see
    /// `agentos_core::prompt::tokens`.
    #[serde(default)]
    pub total_tokens: usize,
    /// Estimated tokens for the tool schemas sent alongside the messages.
    /// Separate because they are not part of any section and are easy to
    /// forget when reading a header.
    #[serde(default)]
    pub tool_tokens: usize,
    /// The model's context window, when the provider could resolve one.
    /// `total_tokens` against this is the pressure the request is under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_budget_tokens: Option<usize>,
    /// Tool results whose middle was elided to fit the window.
    ///
    /// Elision is deterministic in the visible transcript and the window, so
    /// the reconstruction standard above still holds without recording which
    /// messages were cut — but a reader of a trace should not have to re-run
    /// the pruner to learn that it fired.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub elided_messages: usize,
    /// Characters those elisions removed. The gap between this and
    /// [`Self::total_chars`] is what the session log holds and the model did
    /// not see.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub elided_chars: usize,
}

impl Default for RequestHeader {
    /// Hand-written rather than derived so the default `kind` is
    /// [`unknown_kind`] and not the empty string. A header whose kind renders
    /// as `""` in a trace looks like a bug in the writer; one that says
    /// `unknown` says what it means.
    fn default() -> Self {
        Self {
            kind: unknown_kind(),
            sections: Vec::new(),
            total_messages: 0,
            total_chars: 0,
            total_tokens: 0,
            tool_tokens: 0,
            context_budget_tokens: None,
            elided_messages: 0,
            elided_chars: 0,
        }
    }
}

/// What a header read from an older trace reports.
///
/// Not `"turn"`: a header from before kinds existed *was* most likely a turn,
/// and saying so would turn a gap in the record into a claim about it.
fn unknown_kind() -> Arc<str> {
    Arc::from("unknown")
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

/// One section's contribution to a request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestSection {
    /// Stable section identifier (`skill_prelude`, `memory`, `transcript`).
    pub id: Arc<str>,
    /// Messages this section contributed.
    pub messages: usize,
    /// Characters this section contributed.
    pub chars: usize,
    /// Estimated tokens this section contributed.
    #[serde(default)]
    pub tokens: usize,
    /// What the section's content can be re-derived from. Empty when the
    /// section *is* run state the trace already carries, as the transcript is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<RequestSource>,
}

/// One re-derivable input to a section.
///
/// Each variant names where the content lives, never the content itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RequestSource {
    /// A workspace skill, by name. Its body is the enabled `SKILL.md`.
    Skill(Arc<str>),
    /// A memory record selected by hydration. The record id is absent for
    /// backends that do not mint one, in which case the namespace plus the
    /// run's hydration parameters are the only handle on it.
    Memory {
        namespace: Namespace,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        record_id: Option<Arc<str>>,
    },
}
