//! The single authority over what a provider request contains.
//!
//! Roadmap item P1 in `docs/TRANSFER_ROADMAP.md`. Before this module every
//! LLM-backed orchestrator built its own message vector at the call site, and a
//! contribution could be computed and then quietly left out: hydrated memory
//! was written to [`RunContext::memory_fragments`], counted in telemetry, and
//! never reached a request (review finding F1).
//!
//! [`assemble`] is now the only path from a [`RunContext`] to the messages a
//! provider sees. Every contribution is a named [`SectionId`], and every call
//! returns a [`PromptManifest`] recording what actually went in — so "what did
//! the model see" is answerable from the trace rather than by re-reading the
//! orchestrator.
//!
//! One LLM call is deliberately **not** assembled here: the routing
//! classifier's domain-selection round-trip in `orchestrator/routing.rs`. It is
//! a fixed two-message prompt that classifies one input, not a turn in the
//! conversation — routing it through this module would spend the skill prelude
//! and recalled memory on a question that has no use for either, and would let
//! a stored fact steer routing. Keep it separate.

mod sections;

pub use sections::SectionId;

use agentos_interfaces::orchestrator::RunContext;
use agentos_proto::Message;
use tracing::info;

/// One assembled provider request, plus the record of what went into it.
#[derive(Clone, Debug)]
pub struct Prompt {
    /// The messages to send, in order.
    pub messages: Vec<Message>,
    /// What each section contributed.
    pub manifest: PromptManifest,
}

/// What one section contributed to a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SectionEntry {
    pub id: SectionId,
    /// Messages this section added.
    pub messages: usize,
    /// Characters this section added, as a size proxy. Token estimation is
    /// roadmap item C1 and lands on this same manifest.
    pub chars: usize,
}

/// The sections that contributed to one request, in assembly order.
///
/// Sections that contributed nothing are absent, so an empty manifest and a
/// manifest of empty sections are not confusable.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PromptManifest {
    pub sections: Vec<SectionEntry>,
}

impl PromptManifest {
    pub fn total_messages(&self) -> usize {
        self.sections.iter().map(|section| section.messages).sum()
    }

    pub fn total_chars(&self) -> usize {
        self.sections.iter().map(|section| section.chars).sum()
    }

    /// Messages contributed by one section, or 0 when it contributed nothing.
    pub fn messages_in(&self, id: SectionId) -> usize {
        self.sections
            .iter()
            .find(|section| section.id == id)
            .map_or(0, |section| section.messages)
    }
}

/// Assemble the messages for one provider request.
///
/// `skill_prelude` is the caller's precomputed workspace-skill message — the
/// orchestrator owns the catalog, so it renders that section and hands it in.
/// Everything else comes from `ctx`: hydrated memory first, then the
/// conversation, so the request still ends on the latest turn.
pub fn assemble(ctx: &RunContext<'_>, skill_prelude: Option<&Message>) -> Prompt {
    let mut messages = Vec::with_capacity(ctx.state.transcript.items.len().saturating_add(2));
    let mut manifest = PromptManifest::default();

    if let Some(prelude) = skill_prelude {
        push_section(
            &mut messages,
            &mut manifest,
            SectionId::SkillPrelude,
            [prelude.clone()],
        );
    }

    if let Some(memory) = sections::memory_message(&ctx.memory_fragments) {
        push_section(&mut messages, &mut manifest, SectionId::Memory, [memory]);
    }

    push_section(
        &mut messages,
        &mut manifest,
        SectionId::Transcript,
        ctx.state
            .transcript
            .items
            .iter()
            .map(|item| item.message.clone()),
    );

    info!(
        operation = "prompt_assembly",
        run_id = ctx.state.run_id.as_str(),
        active_agent = ctx.state.active_agent.as_str(),
        skill_prelude_messages = manifest.messages_in(SectionId::SkillPrelude),
        memory_messages = manifest.messages_in(SectionId::Memory),
        memory_fragments = ctx.memory_fragments.len(),
        transcript_messages = manifest.messages_in(SectionId::Transcript),
        total_messages = manifest.total_messages(),
        total_chars = manifest.total_chars(),
        "prompt assembled"
    );

    Prompt { messages, manifest }
}

/// Append one section's messages and record what it contributed. A section
/// that yields nothing is not recorded, so the manifest lists contributions
/// rather than attempts.
fn push_section(
    messages: &mut Vec<Message>,
    manifest: &mut PromptManifest,
    id: SectionId,
    rendered: impl IntoIterator<Item = Message>,
) {
    let mut count = 0;
    let mut chars = 0;
    for message in rendered {
        chars += message.content.len();
        count += 1;
        messages.push(message);
    }
    if count > 0 {
        manifest.sections.push(SectionEntry {
            id,
            messages: count,
            chars,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::orchestrator::MemoryFragment;
    use agentos_interfaces::run_state::RunState;
    use agentos_interfaces::session::Item;
    use agentos_proto::{AgentId, MessageRole, Namespace, RunId};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn state_with(items: Vec<&str>) -> RunState {
        let mut state = RunState::new(RunId::new("prompt-test"), AgentId::new("agent"));
        state.transcript.items = items
            .into_iter()
            .map(|content| Item {
                message: Message::text(MessageRole::User, content),
                metadata: BTreeMap::new(),
            })
            .collect();
        state
    }

    fn fragment(fact: &str) -> MemoryFragment {
        MemoryFragment {
            id: None,
            namespace: Namespace::new("private/conversation/c/semantic/general"),
            body: json!({ "fact": fact }),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn transcript_only_is_unchanged_from_the_pre_p1_shape() {
        // The default path must stay byte-identical to what orchestrators built
        // by hand before this module existed.
        let state = state_with(vec!["one", "two"]);
        let ctx = RunContext::from_state(&state);
        let prompt = assemble(&ctx, None);

        assert_eq!(prompt.messages.len(), 2);
        assert_eq!(prompt.messages[0].content.as_ref(), "one");
        assert_eq!(prompt.messages[1].content.as_ref(), "two");
        assert_eq!(prompt.manifest.messages_in(SectionId::Transcript), 2);
        assert_eq!(prompt.manifest.messages_in(SectionId::Memory), 0);
    }

    #[test]
    fn hydrated_fragments_reach_the_request() {
        // The F1 regression, at the unit level: fragments on the context must
        // appear in the assembled messages.
        let state = state_with(vec!["how often do keys rotate?"]);
        let mut ctx = RunContext::from_state(&state);
        ctx.memory_fragments
            .push(fragment("keys rotate every 90 days"));

        let prompt = assemble(&ctx, None);

        assert_eq!(prompt.manifest.messages_in(SectionId::Memory), 1);
        assert!(prompt
            .messages
            .iter()
            .any(|message| message.content.contains("keys rotate every 90 days")));
    }

    #[test]
    fn sections_are_ordered_prelude_memory_then_transcript() {
        let state = state_with(vec!["the user turn"]);
        let mut ctx = RunContext::from_state(&state);
        ctx.memory_fragments.push(fragment("a recalled fact"));
        let prelude = Message::text(MessageRole::System, "the prelude");

        let prompt = assemble(&ctx, Some(&prelude));

        let ids: Vec<SectionId> = prompt
            .manifest
            .sections
            .iter()
            .map(|section| section.id)
            .collect();
        assert_eq!(
            ids,
            vec![
                SectionId::SkillPrelude,
                SectionId::Memory,
                SectionId::Transcript
            ]
        );
        assert_eq!(prompt.messages[0].content.as_ref(), "the prelude");
        assert!(prompt.messages[1].content.contains("a recalled fact"));
        // The request still ends on the conversation, not on injected context.
        assert_eq!(
            prompt
                .messages
                .last()
                .expect("a non-empty prompt")
                .content
                .as_ref(),
            "the user turn"
        );
    }

    #[test]
    fn manifest_totals_cover_every_contributed_message() {
        let state = state_with(vec!["a", "bb"]);
        let mut ctx = RunContext::from_state(&state);
        ctx.memory_fragments.push(fragment("f"));
        let prelude = Message::text(MessageRole::System, "p");

        let prompt = assemble(&ctx, Some(&prelude));

        assert_eq!(prompt.manifest.total_messages(), prompt.messages.len());
        let chars: usize = prompt
            .messages
            .iter()
            .map(|message| message.content.len())
            .sum();
        assert_eq!(prompt.manifest.total_chars(), chars);
    }
}
