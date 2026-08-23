//! Roadmap T1: golden transcripts for assembled runs.
//!
//! Each scenario drives a complete `run_envelope` against the real orchestrator
//! and a [`ScriptedLlm`] that records what it was asked, then pins three things
//! as one golden artifact:
//!
//! - **requests** — the messages and tool names the orchestrator assembled,
//! - **session_items** — the transcript the run produced,
//! - **outcome** — the terminal reply, or the approvals a paused run is waiting on.
//!
//! Unit tests already cover each stage in isolation; these cover the assembled
//! product, which is the layer where a contribution can be computed and then
//! silently not used. See `tests/support/mod.rs` for the harness and the
//! re-record command.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::orchestrator::MaxOrchestrator;
use agentos_core::r#loop::{route, ApprovalBinding, ApprovalTicket};
use agentos_core::runner::{
    approval_prompt_envelope, resume_run, run_envelope, PausedRun, ResumeDecision, RunOutcome,
    SESSION_SCOPE_EPHEMERAL, SESSION_SCOPE_KEY,
};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry};
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_interfaces::session::{Item, Session};
use agentos_interfaces::tool::Tool;
use agentos_proto::{AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use support::{
    assistant, echo_registry, max_with, runner_deps, scenario, scenario_golden, tool_call_response,
    tool_policy, user_envelope, EchoTool, ScriptedLlm, CHANNEL, CONVERSATION, ECHO_TOOL,
};

// ---------------------------------------------------------------------------
// 1. Plain reply
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_plain_reply() {
    let llm = Arc::new(ScriptedLlm::new([assistant(
        "Paris is the capital of France.",
    )]));
    let orchestrator = max_with(llm.clone(), Vec::new());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let outcome = run_envelope(
        user_envelope("What is the capital of France?"),
        RunId::new("golden-reply"),
        &deps,
    )
    .await
    .expect("a plain reply run finishes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    scenario_golden("plain_reply", &llm, &transcript, &outcome);
}

// ---------------------------------------------------------------------------
// 2. Skill prelude
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_skill_prelude() {
    // The enabled skills' SKILL.md bodies are prepended as a system message —
    // the only contribution besides the transcript that reaches a request
    // today. Pinning it is what makes the suite able to detect prompt
    // assembly silently dropping a section.
    let (catalog, _tree) = support::skill_catalog(
        "prelude",
        "deploy-notes",
        "Notes on how deployments are run here.",
        "Always confirm the target environment before deploying.",
    );
    let llm = Arc::new(ScriptedLlm::new([assistant("Understood.")]));
    let orchestrator = MaxOrchestrator::new()
        .with_llm(llm.clone())
        .with_skill_catalog(catalog);
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let outcome = run_envelope(
        user_envelope("What should I check before a release?"),
        RunId::new("golden-prelude"),
        &deps,
    )
    .await
    .expect("a run with a populated catalog finishes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    scenario_golden("skill_prelude", &llm, &transcript, &outcome);
}

// ---------------------------------------------------------------------------
// 3. Tool call through Approve::Allow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_tool_call_allowed() {
    let llm = Arc::new(ScriptedLlm::new([
        tool_call_response("call-1", ECHO_TOOL, r#"{"text":"pong"}"#),
        assistant("The tool said: pong"),
    ]));
    let tools = echo_registry();
    let orchestrator = max_with(llm.clone(), vec![EchoTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[ECHO_TOOL], PolicyVerb::Allow);
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let outcome = run_envelope(
        user_envelope("Echo pong for me."),
        RunId::new("golden-tool"),
        &deps,
    )
    .await
    .expect("an allowed tool call finishes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    scenario_golden("tool_call_allowed", &llm, &transcript, &outcome);
}

// ---------------------------------------------------------------------------
// 3b. A batch of tool calls
// ---------------------------------------------------------------------------

/// Roadmap X1. The interesting part is not that three calls run — it is what
/// the provider is sent afterwards: the batch is replayed as three *separate*
/// assistant turns, each carrying one call and followed by its own result.
/// That is what keeps every provider's "answer every call you were given"
/// requirement satisfied while the loop approves and runs them one at a time.
#[tokio::test]
async fn golden_tool_call_batch() {
    let llm = Arc::new(ScriptedLlm::new([
        support::tool_batch_response(&[
            ("call-a", ECHO_TOOL, r#"{"text":"one"}"#),
            ("call-b", ECHO_TOOL, r#"{"text":"two"}"#),
            ("call-c", ECHO_TOOL, r#"{"text":"three"}"#),
        ]),
        assistant("Ran all three."),
    ]));
    let tools = echo_registry();
    let orchestrator = max_with(llm.clone(), vec![EchoTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[ECHO_TOOL], PolicyVerb::Allow);
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let outcome = run_envelope(
        user_envelope("Echo one, two, and three."),
        RunId::new("golden-batch"),
        &deps,
    )
    .await
    .expect("a batch runs cleanly");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    support::assert_golden("tool_call_batch", &scenario(&llm, &transcript, &outcome));
}

// ---------------------------------------------------------------------------
// 4. ask_user pause and resume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_approval_pause_and_resume() {
    let llm = Arc::new(ScriptedLlm::new([
        tool_call_response("call-gated", ECHO_TOOL, r#"{"text":"approved"}"#),
        assistant("Ran it after you approved."),
    ]));
    let tools = echo_registry();
    let orchestrator = max_with(llm.clone(), vec![EchoTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[ECHO_TOOL], PolicyVerb::AskUser);
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let paused_outcome = run_envelope(
        user_envelope("Echo approved for me."),
        RunId::new("golden-approval"),
        &deps,
    )
    .await
    .expect("a gated tool call pauses cleanly");

    let RunOutcome::Paused(state) = paused_outcome else {
        panic!("an ask_user tool call must pause, not finish");
    };
    let paused_view = support::normalize_approvals(&state);

    // G2: pin the prompt the gateway sends. It is what a user has to answer,
    // so the metadata a channel reads (ticket, the interruption it gates, when
    // it stops counting) and the `/approve <ticket>` instruction in the body
    // are part of the contract. The ticket and expiry are fixed here rather
    // than minted so the golden is stable.
    let ticket = ApprovalTicket::parse("g2fixture").expect("a well-formed fixture ticket");
    let prompt = approval_prompt_envelope(
        &PausedRun {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            state: state.clone(),
        },
        Arc::from("golden-agent"),
        &ticket,
        Some(1_700_000_000),
    )
    .expect("a paused run has a prompt");
    let prompt_view = json!({
        "content": prompt.message.content.as_ref(),
        "metadata": prompt.metadata,
    });

    // An unrelated message must not decide it, however affirmative it sounds.
    // Bound to the sender `user_envelope` stamps, so these route against a
    // prompt that really was put to them and the answers below fail for the
    // reason under test rather than for being someone else's.
    let binding = ApprovalBinding::new(ticket.clone(), support::SENDER);
    let undecided: Vec<Value> = ["y", "yes, go ahead", "approve", "/approve"]
        .into_iter()
        .map(|text| {
            let answer = user_envelope(text);
            json!({
                "input": text,
                "routed": format!("{:?}", route(Some(&binding), &answer)),
            })
        })
        .collect();
    // Resume the way the gateway does: by the id the paused run is actually
    // waiting on, not a guessed one.
    let approval_id = state
        .pending_approval()
        .map(|approval| approval.id.clone())
        .expect("a paused run carries the approval it is waiting on");

    let resumed = resume_run(
        PausedRun {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            state,
        },
        &approval_id,
        ResumeDecision::Approve,
        &deps,
    )
    .await
    .expect("an approved run resumes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    support::assert_golden(
        "approval_pause_resume",
        &json!({
            "paused_approvals": paused_view,
            "prompt": prompt_view,
            "unrelated_input_does_not_decide": undecided,
            "after_resume": scenario(&llm, &transcript, &resumed),
        }),
    );
}

// ---------------------------------------------------------------------------
// 5. Delegation to a sub-agent
// ---------------------------------------------------------------------------

/// Delegates the first user turn, then reports whatever the child returned.
/// Hand-written rather than scripted: what this scenario pins is the *child's*
/// assembled request, so the parent stays a fixed, uninteresting driver.
struct DelegatingParent;

#[async_trait]
impl Orchestrator for DelegatingParent {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let Some(item) = ctx.state.transcript.items.last() else {
            return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
        };
        match item.message.role {
            MessageRole::User => Ok(Plan::Delegate(SubAgentSpec {
                agent_id: AgentId::new("golden-child"),
                policy_id: Arc::from("child-policy"),
                metadata: BTreeMap::new(),
            })),
            _ => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                format!("child said: {}", item.message.content),
            ))),
        }
    }
}

#[tokio::test]
async fn golden_subagent_delegation() {
    let child_llm = Arc::new(ScriptedLlm::new([assistant("Child handled the request.")]));
    let child = MaxOrchestrator::new().with_llm(child_llm.clone());
    let session = Arc::new(InMemorySession::default());

    let mut subagents = SubAgentRegistry::new().with_session(session.clone());
    subagents.register(
        SubAgentDefinition::new(
            AgentId::new("golden-child"),
            "child-policy",
            Arc::new(child),
            Policy::default(),
        )
        .with_max_turns(4),
    );

    let parent = DelegatingParent;
    let policy = Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Delegate,
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    };
    let deps = runner_deps(&parent, session.as_ref(), &policy, None, Some(&subagents));

    let outcome = run_envelope(
        user_envelope("Please hand this to the child."),
        RunId::new("golden-delegate"),
        &deps,
    )
    .await
    .expect("delegation finishes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    support::assert_golden(
        "subagent_delegation",
        &json!({
            "child_requests": support::normalize_requests(&child_llm.requests()),
            "parent_session_items": support::normalize_transcript(&transcript),
            "outcome": support::normalize_outcome(&outcome),
        }),
    );
}

// ---------------------------------------------------------------------------
// 6. Cron ephemeral session scope
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_cron_ephemeral_scope() {
    // A conversation that already carries chat history. A cron tick must not
    // replay it into the model request, and must not append its own items back
    // — the guarantee `SESSION_SCOPE_EPHEMERAL` exists to provide (commit
    // 7b52e7d). The golden pins both halves.
    let session = InMemorySession::default();
    let conversation = support::golden_participant();
    session
        .append(
            &conversation,
            vec![
                Item {
                    message: Message::text(MessageRole::User, "earlier chat message"),
                    metadata: BTreeMap::new(),
                },
                Item {
                    message: Message::text(MessageRole::Assistant, "earlier assistant reply"),
                    metadata: BTreeMap::new(),
                },
            ],
        )
        .await
        .expect("seeding the session succeeds");

    let llm = Arc::new(ScriptedLlm::new([assistant("Digest complete.")]));
    let orchestrator = max_with(llm.clone(), Vec::new());
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from(SESSION_SCOPE_KEY),
        Value::String(SESSION_SCOPE_EPHEMERAL.to_owned()),
    );
    let envelope = Envelope {
        channel_id: ChannelId::new(CHANNEL),
        conversation_id: conversation.conversation.clone(),
        sender: Arc::from("cron:digest"),
        message: Message::text(MessageRole::User, "Run the scheduled digest."),
        metadata,
    };

    let outcome = run_envelope(envelope, RunId::new("golden-cron"), &deps)
        .await
        .expect("an ephemeral run finishes");

    let transcript = session.load(&conversation).await.expect("session loads");
    scenario_golden("cron_ephemeral_scope", &llm, &transcript, &outcome);
}

// ---------------------------------------------------------------------------
// 7. Compaction checkpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_compaction_checkpoint() {
    // The P2 split, end to end: the session log keeps every item, while the
    // request carries only the projected view. A checkpoint summarizing the
    // first two turns hides them from the model without anything having been
    // deleted — the shape compaction (C3) will write.
    let session = InMemorySession::default();
    let conversation = support::golden_participant();
    session
        .append(
            &conversation,
            vec![
                Item {
                    message: Message::text(MessageRole::User, "first question"),
                    metadata: BTreeMap::new(),
                },
                Item {
                    message: Message::text(MessageRole::Assistant, "first answer"),
                    metadata: BTreeMap::new(),
                },
                agentos_core::prompt::checkpoint(
                    Message::text(MessageRole::User, "[summary: we discussed the first topic]"),
                    0,
                    1,
                ),
            ],
        )
        .await
        .expect("seeding the session succeeds");

    let llm = Arc::new(ScriptedLlm::new([assistant("Following up on that.")]));
    let orchestrator = max_with(llm.clone(), Vec::new());
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let outcome = run_envelope(
        user_envelope("and what about the second topic?"),
        RunId::new("golden-checkpoint"),
        &deps,
    )
    .await
    .expect("a checkpointed run finishes");

    let transcript = session.load(&conversation).await.expect("session loads");
    scenario_golden("compaction_checkpoint", &llm, &transcript, &outcome);
}
