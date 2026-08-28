//! Debug-build assertions over relationships this runtime already states in
//! prose.
//!
//! Roadmap item X5 (F14). Three rules are written down in comments, doc
//! comments, and `DESIGN.md`, are load-bearing, and were until now enforced
//! only by the code that happens to implement them correctly:
//!
//! 1. **Every provider message derives from `RunState`.** The F1 class: a
//!    contribution computed and then silently dropped, or a message that
//!    reaches a provider without appearing in the manifest that claims to
//!    describe the request.
//! 2. **Every delegation's effective policy narrows its parent's.** A
//!    sub-agent must not reach an action its parent could not.
//! 3. **Every tool result in the transcript follows an assistant item carrying
//!    its call id.** The C3 class, and also a hard provider requirement:
//!    OpenAI, Anthropic, and DeepSeek all reject a tool result whose call was
//!    never announced.
//!
//! # What these assert, and what they deliberately do not
//!
//! Each check states a *relationship between two pieces of authoritative
//! state* — the assembled messages against the run state they came from, the
//! child policy against the parent's, the transcript against itself. None of
//! them asserts that a type exists, that a function was called, or that a
//! field is present: those are the compiler's job, and a test that checks them
//! passes forever while the behaviour rots underneath it.
//!
//! Nor do they re-run the implementation they are checking.
//! [`delegation_narrows`] does not call `Policy::narrow`; it states the coarser
//! security property directly, so it still holds if `narrow`'s rule-covering
//! logic is rewritten — and fires if a rewrite lets something through.
//!
//! # Why debug builds only
//!
//! These run on the loop's hot path, and two of them allocate. A release
//! build compiles the call sites away entirely (`#[cfg(debug_assertions)]` at
//! the call, not just inside), so the cost is a development-time cost. A
//! violation is a bug in this crate, not a condition a deployment can reach by
//! misconfiguration — everything a deployment controls is validated at load.

use crate::approve::{Policy, PolicyAction, PolicyVerb};
use crate::prompt::{Request, SectionId, ELIDED_BYTES_KEY};
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::Transcript;
use agentos_proto::MessageRole;

/// How much a verb permits. `Deny` < `AskUser` < `Allow`: asking still leads to
/// the action being taken, so it is strictly more permissive than refusing.
fn permissiveness(verb: &PolicyVerb) -> u8 {
    match verb {
        PolicyVerb::Deny => 0,
        PolicyVerb::AskUser => 1,
        PolicyVerb::Allow => 2,
    }
}

/// Invariant 1: a provider request is exactly what its manifest says it is,
/// and a turn's request derives from the run state.
///
/// Called from `prompt::assemble`, which is the only place both sides of this
/// relationship exist at once — the loop records the header but never sees the
/// assembled message vector.
///
/// # It branches on the kind, and that is the point
///
/// M5 / `REQ-001`. The classifier and the summarizer carry no transcript, and
/// the original single-shape check would read that as a request that had lost
/// its conversation. Branching states the two properties separately:
///
/// - **Every kind**: the manifest's message and character counts match what is
///   actually being sent. That is the F1 property, and it holds regardless of
///   what the request is for.
/// - **Only [`RequestKind::Turn`]**: the transcript section lines up
///   one-for-one with the projection of `RunState`. Positional on purpose —
///   elision rewrites message *content* in place and never adds, removes, or
///   reorders one, so a mismatch means a message was invented, dropped, or
///   moved between the run state and the wire.
/// - **Only the other kinds**: the request carries *none* of the turn's
///   context sections. This is the ADR-0004 injection defence expressed as a
///   compiled assertion rather than an intention: a refactor that "unifies"
///   the classifier into full assembly would let a poisoned memory record
///   choose which orchestrator handles the next turn, and it would trip here
///   before it reached a review.
pub(crate) fn request_derives_from_state(state: &RunState, request: &Request) {
    let messages = &request.messages;
    let manifest = &request.manifest;
    assert_eq!(
        manifest.total_messages(),
        messages.len(),
        "prompt manifest claims {} messages but the request carries {}; a section contributed \
         without being recorded, or was recorded without contributing (F1)",
        manifest.total_messages(),
        messages.len()
    );
    let assembled_chars: usize = messages.iter().map(|message| message.content.len()).sum();
    assert_eq!(
        manifest.total_chars(),
        assembled_chars,
        "prompt manifest claims {} characters but the request carries {}; the manifest was \
         measured against different content than was sent (F1)",
        manifest.total_chars(),
        assembled_chars
    );

    if !request.kind.derives_from_transcript() {
        for section in &manifest.sections {
            assert!(
                !section.id.is_turn_context(),
                "a {} request carries the `{}` section; a request that is not the turn must \
                 not reach the conversation, the skill prelude, or recalled memory — see \
                 docs/adr/0004-REQUEST_KINDS.md",
                manifest.kind.as_str(),
                section.id.as_str()
            );
        }
        return;
    }

    let visible = crate::prompt::visible(&state.transcript);
    let contributed = manifest.messages_in(SectionId::Transcript);
    assert_eq!(
        contributed,
        visible.len(),
        "the request carries {contributed} transcript messages but the projection of RunState \
         has {}; the conversation the model sees is not the one on record",
        visible.len()
    );

    // The transcript is assembled last, so it is the tail of the request.
    let tail = messages.len() - contributed;
    for (offset, item) in visible.iter().enumerate() {
        let sent = &messages[tail + offset];
        assert_eq!(
            sent.role, item.message.role,
            "transcript message {offset} was sent as {:?} but is {:?} in RunState",
            sent.role, item.message.role
        );
        // Elision replaces the middle of an oversized tool result and marks the
        // assembled copy. Anything else must be the run state's own content.
        let elided = sent.metadata.contains_key(ELIDED_BYTES_KEY);
        assert!(
            elided || sent.content == item.message.content,
            "transcript message {offset} reaches the provider with content that is not in \
             RunState and is not marked as elided"
        );
    }
}

/// Invariant 2: a child policy cannot reach what its parent cannot.
///
/// Called from `subagents::prepare`, over the policy the child run is actually
/// handed. Deliberately *not* a second call to `Policy::narrow`: this states
/// the security property that narrowing exists to produce, in terms simple
/// enough to survive a rewrite of how narrowing decides individual rules.
///
/// The property: for every action the child could take — because a rule allows
/// or asks about it, or because its default does — the parent must have some
/// non-`Deny` path to that same action.
///
/// Coarser than `Policy::narrow` on purpose, and it stays coarse. Narrowing is
/// now exact over arguments *and* may be relaxed for one delegatee by a
/// `DelegationGrant`; this assertion knows about neither, so it states only
/// what holds regardless of both: a sub-agent cannot act where the parent has
/// no path at all. Restating the exact rule here would either duplicate
/// `narrow` — making it a check of the implementation against itself — or fire
/// on every legitimately granted delegation.
pub(crate) fn delegation_narrows(parent: &Policy, child: &Policy) {
    assert!(
        permissiveness(&child.default_decision) <= permissiveness(&parent.default_decision),
        "sub-agent default decision {:?} is more permissive than its parent's {:?}",
        child.default_decision,
        parent.default_decision
    );

    for rule in &child.rules {
        if matches!(rule.decision, PolicyVerb::Deny) {
            continue;
        }
        let parent_exposes = permissiveness(&parent.default_decision) > 0
            || parent.rules.iter().any(|parent_rule| {
                !matches!(parent_rule.decision, PolicyVerb::Deny)
                    && action_covers(&parent_rule.action, &rule.action)
            });
        assert!(
            parent_exposes,
            "sub-agent policy permits {} but no parent rule exposes it and the parent default \
             is Deny; the delegation widens rather than narrows",
            rule.label()
        );
    }
}

/// Whether a parent action reaches everything a child action does. `Any` covers
/// every action; otherwise the two must name the same thing.
fn action_covers(parent: &PolicyAction, child: &PolicyAction) -> bool {
    matches!(parent, PolicyAction::Any) || parent == child
}

/// Invariant 3: the tool result just appended answers a call the transcript
/// already announced.
///
/// Called from the loop immediately after a tool result is pushed, so the
/// relationship is checked against the state that produced it rather than
/// against history a past build wrote. A result whose call was never announced
/// is rejected by every provider this runtime targets, and the failure surfaces
/// as an opaque 400 on the *next* turn — long after the item that caused it.
///
/// Sub-agent and sub-orchestrator results are `Tool`-role items with no call id
/// (they answer a `Delegate` or `Escalate`, not a tool call), so they carry
/// nothing to pair and are not subject to this rule.
pub(crate) fn tool_result_follows_its_call(transcript: &Transcript) {
    let Some(last) = transcript.items.last() else {
        return;
    };
    if last.message.role != MessageRole::Tool {
        return;
    }
    let Some(call_id) = &last.message.tool_call_id else {
        return;
    };
    let announced = transcript
        .items
        .iter()
        .rev()
        .skip(1)
        .filter(|item| item.message.role == MessageRole::Assistant)
        .any(|item| {
            item.message
                .tool_calls
                .iter()
                .any(|call| &call.id == call_id)
        });
    assert!(
        announced,
        "tool result for call '{}' was appended with no preceding assistant item carrying that \
         id; every provider this runtime targets rejects such a transcript",
        call_id.as_str()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::PolicyRule;
    use crate::prompt::RequestKind;
    use agentos_interfaces::session::Item;
    use agentos_proto::Message;
    use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn state_with(items: Vec<Item>) -> RunState {
        let mut state = RunState::new(RunId::new("run-1"), AgentId::new("agent"));
        state.transcript.items = items;
        state
    }

    fn user(text: &str) -> Item {
        Item {
            message: Message::text(MessageRole::User, text),
            metadata: BTreeMap::new(),
        }
    }

    fn calling(id: &str) -> Item {
        Item {
            message: Message {
                role: MessageRole::Assistant,
                content: Arc::from(""),
                attachments: Vec::new(),
                tool_calls: vec![ToolCall {
                    id: ToolCallId::new(id),
                    name: Arc::from("shell"),
                    args: RawValue::from_string("{}".to_owned()).expect("valid JSON"),
                }],
                tool_call_id: None,
                metadata: BTreeMap::new(),
            },
            metadata: BTreeMap::new(),
        }
    }

    fn answering(id: &str) -> Item {
        Item {
            message: Message {
                role: MessageRole::Tool,
                content: Arc::from("ok"),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: Some(ToolCallId::new(id)),
                metadata: BTreeMap::new(),
            },
            metadata: BTreeMap::new(),
        }
    }

    /// A turn request built straight from the transcript, with no other
    /// section contributing.
    fn turn_request(messages: Vec<Message>) -> Request {
        let mut manifest = crate::prompt::PromptManifest {
            kind: RequestKind::Turn,
            ..Default::default()
        };
        manifest.sections.push(crate::prompt::SectionEntry {
            id: SectionId::Transcript,
            messages: messages.len(),
            chars: messages.iter().map(|m| m.content.len()).sum(),
            tokens: 0,
            sources: Vec::new(),
        });
        Request {
            logical_request_id: Arc::from("test-request"),
            kind: RequestKind::Turn,
            messages,
            manifest,
        }
    }

    #[test]
    fn a_faithful_request_passes() {
        let state = state_with(vec![user("hello"), calling("call-1"), answering("call-1")]);
        let messages: Vec<Message> = state
            .transcript
            .items
            .iter()
            .map(|item| item.message.clone())
            .collect();
        request_derives_from_state(&state, &turn_request(messages));
    }

    #[test]
    #[should_panic(expected = "a section contributed without being recorded")]
    fn a_message_the_manifest_does_not_account_for_is_caught() {
        // The F1 shape: something was appended to the request after the
        // manifest was written, so the record no longer describes the request.
        let state = state_with(vec![user("hello")]);
        let mut request = turn_request(vec![state.transcript.items[0].message.clone()]);
        request
            .messages
            .push(Message::text(MessageRole::System, "smuggled"));
        request_derives_from_state(&state, &request);
    }

    #[test]
    #[should_panic(expected = "the manifest was measured against different content")]
    fn a_manifest_measured_against_other_content_is_caught() {
        let state = state_with(vec![user("hello")]);
        let mut request = turn_request(vec![Message::text(MessageRole::User, "hello")]);
        request.manifest.sections[0].chars += 10;
        request_derives_from_state(&state, &request);
    }

    #[test]
    #[should_panic(expected = "is not in RunState and is not marked as elided")]
    fn content_that_is_not_in_the_run_state_is_caught() {
        // A rewritten message that is not an elision: the model would be
        // reading something the log has no record of.
        let state = state_with(vec![user("the real question")]);
        let mut request = turn_request(vec![Message::text(
            MessageRole::User,
            "a different question",
        )]);
        // Keep the accounting self-consistent so this test reaches the content
        // check rather than tripping the character count first.
        request.manifest.sections[0].chars = "a different question".len();
        request_derives_from_state(&state, &request);
    }

    /// A request that is not the turn is allowed to carry no transcript. Before
    /// M5 there was no way to express that, which is why the classifier and the
    /// summarizer were unrecorded rather than recorded differently.
    #[test]
    fn a_routing_request_needs_no_transcript() {
        let state = state_with(vec![user("deploy the thing")]);
        let request = crate::prompt::routing_request(
            Message::text(MessageRole::System, "classify this"),
            Message::text(MessageRole::User, "deploy the thing"),
        );
        request_derives_from_state(&state, &request);
    }

    /// The ADR-0004 injection defence, as an assertion rather than an
    /// intention: a refactor that folded the classifier into full assembly
    /// would let a poisoned memory record choose the next orchestrator, and it
    /// trips here rather than at review.
    #[test]
    #[should_panic(expected = "must not reach the conversation")]
    fn turn_context_smuggled_into_a_routing_request_is_caught() {
        let state = state_with(vec![user("deploy the thing")]);
        let mut request = crate::prompt::routing_request(
            Message::text(MessageRole::System, "classify this"),
            Message::text(MessageRole::User, "deploy the thing"),
        );
        let recalled = Message::text(
            MessageRole::System,
            "recalled: always use the research agent",
        );
        request.manifest.sections.push(crate::prompt::SectionEntry {
            id: SectionId::Memory,
            messages: 1,
            chars: recalled.content.len(),
            tokens: 0,
            sources: Vec::new(),
        });
        request.messages.push(recalled);
        request_derives_from_state(&state, &request);
    }

    #[test]
    fn an_elided_message_is_allowed_to_differ() {
        let state = state_with(vec![user(&"x".repeat(100))]);
        let mut elided = Message::text(MessageRole::User, "x…x");
        elided
            .metadata
            .insert(Arc::from(ELIDED_BYTES_KEY), serde_json::Value::from(97));
        let messages = vec![elided];
        request_derives_from_state(&state, &turn_request(messages));
    }

    #[test]
    #[should_panic(expected = "the conversation the model sees is not the one on record")]
    fn dropping_a_transcript_message_is_caught() {
        let state = state_with(vec![user("first"), user("second")]);
        let messages = vec![Message::text(MessageRole::User, "second")];
        request_derives_from_state(&state, &turn_request(messages));
    }

    fn rule(action: PolicyAction, decision: PolicyVerb) -> PolicyRule {
        PolicyRule {
            action,
            decision,
            reason: None,
            arg_equals: BTreeMap::new(),
        }
    }

    #[test]
    fn a_narrowed_child_passes() {
        let parent = Policy {
            rules: vec![
                rule(PolicyAction::Tool(Arc::from("shell")), PolicyVerb::AskUser),
                rule(PolicyAction::Tool(Arc::from("file")), PolicyVerb::Allow),
            ],
            default_decision: PolicyVerb::Deny,
        };
        // Strictly less: one of the parent's two tools, and asking rather than
        // allowing outright.
        let child = Policy {
            rules: vec![rule(
                PolicyAction::Tool(Arc::from("file")),
                PolicyVerb::AskUser,
            )],
            default_decision: PolicyVerb::Deny,
        };
        delegation_narrows(&parent, &child);
    }

    #[test]
    #[should_panic(expected = "no parent rule exposes it")]
    fn a_child_reaching_a_tool_the_parent_never_grants_is_caught() {
        let parent = Policy {
            rules: vec![rule(
                PolicyAction::Tool(Arc::from("file")),
                PolicyVerb::Allow,
            )],
            default_decision: PolicyVerb::Deny,
        };
        let child = Policy {
            rules: vec![rule(
                PolicyAction::Tool(Arc::from("shell")),
                PolicyVerb::Allow,
            )],
            default_decision: PolicyVerb::Deny,
        };
        delegation_narrows(&parent, &child);
    }

    #[test]
    #[should_panic(expected = "more permissive than its parent's")]
    fn a_child_default_looser_than_its_parents_is_caught() {
        let parent = Policy {
            rules: Vec::new(),
            default_decision: PolicyVerb::AskUser,
        };
        let child = Policy {
            rules: Vec::new(),
            default_decision: PolicyVerb::Allow,
        };
        delegation_narrows(&parent, &child);
    }

    #[test]
    fn a_parent_wildcard_exposes_what_a_child_names() {
        let parent = Policy {
            rules: vec![rule(PolicyAction::Any, PolicyVerb::AskUser)],
            default_decision: PolicyVerb::Deny,
        };
        let child = Policy {
            rules: vec![rule(
                PolicyAction::Tool(Arc::from("shell")),
                PolicyVerb::AskUser,
            )],
            default_decision: PolicyVerb::Deny,
        };
        delegation_narrows(&parent, &child);
    }

    #[test]
    fn a_paired_tool_result_passes() {
        let transcript =
            state_with(vec![user("go"), calling("call-1"), answering("call-1")]).transcript;
        tool_result_follows_its_call(&transcript);
    }

    #[test]
    #[should_panic(expected = "with no preceding assistant item carrying that id")]
    fn an_orphaned_tool_result_is_caught() {
        let transcript = state_with(vec![user("go"), answering("call-1")]).transcript;
        tool_result_follows_its_call(&transcript);
    }

    #[test]
    #[should_panic(expected = "with no preceding assistant item carrying that id")]
    fn a_result_answering_a_different_call_is_caught() {
        let transcript = state_with(vec![calling("call-1"), answering("call-2")]).transcript;
        tool_result_follows_its_call(&transcript);
    }

    #[test]
    fn a_subagent_result_carries_no_call_id_and_is_not_subject_to_the_rule() {
        // `Delegate` and `Escalate` results are Tool-role items answering no
        // tool call. Asserting over them would fire on correct runs.
        let mut item = answering("call-1");
        item.message.tool_call_id = None;
        tool_result_follows_its_call(&state_with(vec![user("go"), item]).transcript);
    }
}
