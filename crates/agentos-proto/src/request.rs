use crate::ids::Namespace;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestHeader {
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
