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
//! # Kinds, and why not every request is a turn
//!
//! M5 / `REQ-001` (`docs/adr/0004-REQUEST_KINDS.md`). Two production calls are
//! deliberately **not** assembled from the transcript:
//!
//! - the **routing classifier**, a fixed two-message prompt that classifies
//!   one input. Assembling it would spend the skill prelude and recalled
//!   memory on a question that has no use for either, and — the part that
//!   matters — would let a stored fact steer routing, so a memory-poisoning
//!   attack could choose which orchestrator handles the next turn.
//! - the **compaction summarizer**, which rewrites the oldest span of a
//!   conversation and has no `RunContext` to be assembled from.
//!
//! Both used to be unrecorded, because "assembled here" and "recorded here"
//! were the same thing. They are now separate: a [`RequestKind`] names the
//! section set, [`RequestBuilder`] is the single path from sections to
//! messages *and* manifest for every kind, and [`gateway`] is the single path
//! from a [`Request`] to a provider. The invariant is **every provider call
//! records what it was made of**, not *every provider call derives from the
//! transcript* — and `invariants::request_derives_from_state` enforces the
//! difference, refusing a non-turn request that carries turn context.

pub mod calibration;
mod compact;
mod gateway;
mod projection;
mod prune;
mod sections;
mod tokens;

pub use compact::{compact, compact_now, is_checkpoint, select_span, Compacted, Compaction, Span};
pub use gateway::{call_detached, call_with_context, Exchange};
pub use projection::{checkpoint, visible, visible_positions, TRANSCRIPT_SHADOW_KEY};
pub use prune::{Elision, ELIDED_BYTES_KEY, PRUNE_TRIGGER_RATIO};
pub use sections::{RequestKind, SectionId, SkillPrelude};
pub use tokens::{estimate_message, estimate_text, estimate_tool_specs};

use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Message, RequestHeader, RequestSection, RequestSource};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

/// One built provider request, plus the record of what went into it.
///
/// Every kind of request is one of these (M5 / `REQ-001`), so
/// [`gateway::call_with_context`] and [`gateway::call_detached`] can take the
/// request rather than a bare message vector — which is what makes "record a
/// manifest" something the gateway does rather than something each call site
/// remembers to do.
#[derive(Clone, Debug)]
pub struct Request {
    /// Stable identity shared by every physical retry of this request.
    pub logical_request_id: Arc<str>,
    /// What sort of request this is, and therefore which sections it may
    /// carry.
    pub kind: RequestKind,
    /// The messages to send, in order.
    pub messages: Vec<Message>,
    /// What each section contributed.
    pub manifest: PromptManifest,
}

/// What one section contributed to a request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SectionEntry {
    pub id: SectionId,
    /// Messages this section added.
    pub messages: usize,
    /// Characters this section added — the one figure with no heuristic in it.
    pub chars: usize,
    /// Estimated tokens this section added.
    pub tokens: usize,
    /// Where this section's content can be re-derived from. Empty for the
    /// transcript, which the run state already carries.
    pub sources: Vec<RequestSource>,
}

/// The sections that contributed to one request, in assembly order.
///
/// Sections that contributed nothing are absent, so an empty manifest and a
/// manifest of empty sections are not confusable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptManifest {
    /// The kind of request this manifest describes.
    pub kind: RequestKind,
    pub sections: Vec<SectionEntry>,
    /// Estimated tokens for the tool schemas sent with the request. Not a
    /// section — they carry no messages — but they occupy the same window.
    pub tool_tokens: usize,
    /// The model's context window, when the provider resolved one.
    pub context_budget_tokens: Option<usize>,
    /// What elision removed from the transcript to fit that window. Zero on
    /// every request that was already under the trigger ratio.
    pub elided: Elision,
}

impl Default for PromptManifest {
    fn default() -> Self {
        Self {
            kind: RequestKind::Turn,
            sections: Vec::new(),
            tool_tokens: 0,
            context_budget_tokens: None,
            elided: Elision::default(),
        }
    }
}

impl PromptManifest {
    pub fn total_messages(&self) -> usize {
        self.sections.iter().map(|section| section.messages).sum()
    }

    pub fn total_chars(&self) -> usize {
        self.sections.iter().map(|section| section.chars).sum()
    }

    /// Estimated tokens for the whole request, tool schemas included.
    pub fn total_tokens(&self) -> usize {
        self.sections
            .iter()
            .map(|section| section.tokens)
            .sum::<usize>()
            + self.tool_tokens
    }

    /// Share of the model's context window this request occupies, as a
    /// fraction. `None` when the provider could not resolve a budget.
    pub fn pressure(&self) -> Option<f64> {
        let budget = self.context_budget_tokens.filter(|budget| *budget > 0)?;
        Some(self.total_tokens() as f64 / budget as f64)
    }

    /// Messages contributed by one section, or 0 when it contributed nothing.
    pub fn messages_in(&self, id: SectionId) -> usize {
        self.sections
            .iter()
            .find(|section| section.id == id)
            .map_or(0, |section| section.messages)
    }

    /// Project this manifest into the durable header the loop traces.
    pub fn header(&self) -> RequestHeader {
        RequestHeader {
            kind: Arc::from(self.kind.as_str()),
            sections: self
                .sections
                .iter()
                .map(|section| RequestSection {
                    id: Arc::from(section.id.as_str()),
                    messages: section.messages,
                    chars: section.chars,
                    tokens: section.tokens,
                    sources: section.sources.clone(),
                })
                .collect(),
            total_messages: self.total_messages(),
            total_chars: self.total_chars(),
            total_tokens: self.total_tokens(),
            tool_tokens: self.tool_tokens,
            context_budget_tokens: self.context_budget_tokens,
            elided_messages: self.elided.messages,
            elided_chars: self.elided.chars,
        }
    }
}

/// What the caller contributes to an assembly, beyond the run context.
///
/// The orchestrator owns its skill catalog, its tool set, and its provider, so
/// it supplies those three; everything else comes from `ctx`.
#[derive(Default)]
pub struct Assembly<'a> {
    /// The precomputed workspace-skill message and the skills behind it.
    pub skill_prelude: Option<&'a SkillPrelude>,
    /// Tool schemas sent alongside the messages. They carry no messages but
    /// occupy the same context window, so they are estimated with them.
    pub tools: &'a [ToolSpec],
    /// The model's context window, when the provider can resolve one.
    pub context_budget_tokens: Option<usize>,
}

/// Accumulates one request's messages and the record of what produced them.
///
/// The single path from sections to a [`Request`] (M5 / `REQ-001`). Every
/// kind builds through this, so a manifest is written by the same code that
/// appends the messages and cannot describe content that was not sent —
/// which is the property `invariants::request_derives_from_state` checks and
/// the property a hand-written manifest would quietly lose.
pub struct RequestBuilder {
    kind: RequestKind,
    messages: Vec<Message>,
    manifest: PromptManifest,
}

impl RequestBuilder {
    pub fn new(kind: RequestKind) -> Self {
        Self {
            kind,
            messages: Vec::new(),
            manifest: PromptManifest {
                kind,
                ..Default::default()
            },
        }
    }

    /// Pre-size the message vector for a caller that knows roughly how many
    /// it will add. Purely an allocation hint.
    fn with_capacity(mut self, capacity: usize) -> Self {
        self.messages.reserve(capacity);
        self
    }

    /// Append one section's messages and record what it contributed.
    ///
    /// A section that yields nothing is not recorded, so the manifest lists
    /// contributions rather than attempts.
    pub fn section(
        &mut self,
        id: SectionId,
        sources: Vec<RequestSource>,
        rendered: impl IntoIterator<Item = Message>,
    ) -> &mut Self {
        let mut count = 0;
        let mut chars = 0;
        let mut tokens = 0;
        for message in rendered {
            chars += message.content.len();
            tokens += tokens::estimate_message(&message);
            count += 1;
            self.messages.push(message);
        }
        if count > 0 {
            self.manifest.sections.push(SectionEntry {
                id,
                messages: count,
                chars,
                tokens,
                sources,
            });
        }
        self
    }

    /// Record the tool schemas sent alongside the messages. They carry no
    /// messages but occupy the same window.
    pub fn tools(&mut self, tools: &[ToolSpec]) -> &mut Self {
        self.manifest.tool_tokens = tokens::estimate_tool_specs(tools);
        self
    }

    pub fn context_budget(&mut self, budget: Option<usize>) -> &mut Self {
        self.manifest.context_budget_tokens = budget;
        self
    }

    /// Tokens recorded so far, for a caller sizing what it is about to add
    /// against the remaining window.
    fn reserved_tokens(&self) -> usize {
        self.manifest.total_tokens()
    }

    fn record_elision(&mut self, elided: Elision) -> &mut Self {
        self.manifest.elided = elided;
        self
    }

    pub fn finish(self) -> Request {
        static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
        Request {
            logical_request_id: Arc::from(format!(
                "request-{}",
                NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
            )),
            kind: self.kind,
            messages: self.messages,
            manifest: self.manifest,
        }
    }
}

/// Assemble the turn's request from the run context.
///
/// Hydrated memory comes first, then the projected conversation, so the request
/// still ends on the latest turn.
///
/// Records its header on the context immediately. The other two kinds do not:
/// [`routing_request`] is built by a caller that holds a context and lets the
/// gateway record it, and [`summary_request`] is built where no context exists
/// at all. Assembly keeps its own push because a turn's header must survive
/// even when the provider call that follows it *fails* — a request rejected
/// for length is exactly the one whose header a reader wants.
pub fn assemble(ctx: &RunContext<'_>, input: &Assembly<'_>) -> Request {
    let mut builder = RequestBuilder::new(RequestKind::Turn)
        .with_capacity(ctx.state.transcript.items.len().saturating_add(2));

    if let Some(prelude) = input.skill_prelude {
        builder.section(
            SectionId::SkillPrelude,
            prelude.sources(),
            [prelude.message.clone()],
        );
    }

    if let Some(memory) = sections::memory_message(&ctx.memory_fragments) {
        builder.section(
            SectionId::Memory,
            sections::memory_sources(&ctx.memory_fragments),
            [memory],
        );
    }

    builder
        .tools(input.tools)
        .context_budget(input.context_budget_tokens);

    // The projected view, not the raw log: a checkpoint written by compaction
    // hides the span it summarizes without anything having been deleted.
    let mut transcript: Vec<Message> = projection::visible(&ctx.state.transcript)
        .into_iter()
        .map(|item| item.message.clone())
        .collect();

    // Elision runs before the section is measured, so the manifest describes
    // what the provider was actually sent rather than what the log holds. It
    // is a property of this request alone — recomputed every turn, never
    // written back (see `prune`).
    let elided = prune::to_fit(
        &mut transcript,
        &prune::Budget {
            context_budget_tokens: input.context_budget_tokens,
            reserved_tokens: builder.reserved_tokens(),
        },
    );
    builder.record_elision(elided);

    builder.section(SectionId::Transcript, Vec::new(), transcript);
    let request = builder.finish();

    // X5: the request the provider is about to see must derive from the run
    // state, and the manifest must describe it exactly. Checked here because
    // this is the only place both sides exist at once — the loop records the
    // header but never sees the messages.
    #[cfg(debug_assertions)]
    crate::invariants::request_derives_from_state(ctx.state, &request);

    // Durable record of what this request was made of, drained by the loop
    // into a `request_header` trace event after `plan()` returns.
    ctx.push_request_header(request.manifest.header());

    let manifest = &request.manifest;
    info!(
        operation = "prompt_assembly",
        run_id = ctx.state.run_id.as_str(),
        active_agent = ctx.state.active_agent.as_str(),
        kind = manifest.kind.as_str(),
        skill_prelude_messages = manifest.messages_in(SectionId::SkillPrelude),
        memory_messages = manifest.messages_in(SectionId::Memory),
        memory_fragments = ctx.memory_fragments.len(),
        transcript_messages = manifest.messages_in(SectionId::Transcript),
        total_messages = manifest.total_messages(),
        total_chars = manifest.total_chars(),
        total_tokens = manifest.total_tokens(),
        tool_tokens = manifest.tool_tokens,
        elided_messages = manifest.elided.messages,
        elided_chars = manifest.elided.chars,
        "prompt assembled"
    );

    request
}

/// Build the routing classifier's request.
///
/// Two fixed messages: the instruction plus the domain table, and the one
/// input being classified. **Deliberately not [`assemble`]** — see
/// [`RequestKind`] and ADR-0004. Going through the builder anyway is what
/// makes that isolation a recorded, checkable fact rather than an absence: the
/// manifest names exactly two sections, neither of which is turn context, and
/// `invariants::request_derives_from_state` refuses a routing request that
/// carries any.
pub fn routing_request(instruction: Message, input: Message) -> Request {
    let mut builder = RequestBuilder::new(RequestKind::Routing).with_capacity(2);
    builder.section(SectionId::RoutingInstruction, Vec::new(), [instruction]);
    builder.section(SectionId::RoutingInput, Vec::new(), [input]);
    builder.finish()
}

/// Build the compaction summarizer's request.
///
/// The instruction and the rendered span. Carries no live transcript section:
/// the span is text this module rendered *from* the transcript, not the
/// projected conversation, and recording it as `transcript` would make a
/// summarization look like a turn in a replay.
pub fn summary_request(instruction: Message, span: Message) -> Request {
    let mut builder = RequestBuilder::new(RequestKind::Compaction).with_capacity(2);
    builder.section(SectionId::SummaryInstruction, Vec::new(), [instruction]);
    builder.section(SectionId::SummarySpan, Vec::new(), [span]);
    builder.finish()
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
        let prompt = assemble(&ctx, &Assembly::default());

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

        let prompt = assemble(&ctx, &Assembly::default());

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
        let prelude = SkillPrelude {
            message: Message::text(MessageRole::System, "the prelude"),
            skills: vec![Arc::from("deploy-notes")],
        };

        let prompt = assemble(
            &ctx,
            &Assembly {
                skill_prelude: Some(&prelude),
                ..Default::default()
            },
        );

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
    fn assembly_pushes_a_header_naming_its_sources() {
        // P3: every assembled request leaves a record the loop can trace, and
        // that record names where each section came from rather than copying
        // it — no memory body reaches the header.
        let state = state_with(vec!["the user turn"]);
        let mut ctx = RunContext::from_state(&state);
        ctx.memory_fragments.push(MemoryFragment {
            id: Some(agentos_proto::RecordId::new("rec-7")),
            namespace: Namespace::new("private/conversation/c/semantic/general"),
            body: json!({ "fact": "a recalled fact" }),
            metadata: BTreeMap::new(),
        });
        let prelude = SkillPrelude {
            message: Message::text(MessageRole::System, "the prelude"),
            skills: vec![Arc::from("deploy-notes")],
        };

        assemble(
            &ctx,
            &Assembly {
                skill_prelude: Some(&prelude),
                ..Default::default()
            },
        );

        let headers = std::mem::take(
            &mut *ctx
                .request_sink
                .lock()
                .expect("the sink is never poisoned in tests"),
        );
        let [header] = headers.as_slice() else {
            panic!("one assembly pushes exactly one header, got {headers:?}");
        };
        assert_eq!(header.total_messages, 3);
        assert_eq!(
            header.sections[0].sources,
            vec![RequestSource::Skill(Arc::from("deploy-notes"))]
        );
        assert_eq!(
            header.sections[1].sources,
            vec![RequestSource::Memory {
                namespace: Namespace::new("private/conversation/c/semantic/general"),
                record_id: Some(Arc::from("rec-7")),
            }]
        );
        assert!(header.sections[2].sources.is_empty());
        // The fact itself is in the request, never in the header.
        let rendered = serde_json::to_string(header).expect("headers serialize");
        assert!(!rendered.contains("a recalled fact"));
    }

    #[test]
    fn manifest_totals_cover_every_contributed_message() {
        let state = state_with(vec!["a", "bb"]);
        let mut ctx = RunContext::from_state(&state);
        ctx.memory_fragments.push(fragment("f"));
        let prelude = SkillPrelude {
            message: Message::text(MessageRole::System, "p"),
            skills: vec![Arc::from("s")],
        };

        let prompt = assemble(
            &ctx,
            &Assembly {
                skill_prelude: Some(&prelude),
                ..Default::default()
            },
        );

        assert_eq!(prompt.manifest.total_messages(), prompt.messages.len());
        let chars: usize = prompt
            .messages
            .iter()
            .map(|message| message.content.len())
            .sum();
        assert_eq!(prompt.manifest.total_chars(), chars);
    }
}
