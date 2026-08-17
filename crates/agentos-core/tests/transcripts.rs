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
use agentos_core::memory::{
    InMemorySession, MemoryManager, MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility,
    RetrievalStrategy, SqliteStore,
};
use agentos_core::orchestrator::{MaxOrchestrator, MemoryHydrationSettings};
use agentos_core::r#loop::{route, ApprovalTicket};
use agentos_core::runner::{
    approval_prompt_envelope, resume_run, run_envelope, PausedRun, ResumeDecision, RunOutcome,
    SESSION_SCOPE_EPHEMERAL, SESSION_SCOPE_KEY,
};
use agentos_core::spill::{ContentLimits, SpillStore, SPILL_LOCATOR_KEY};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_interfaces::session::{Item, Session};
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall,
    ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use support::{
    assistant, runner_deps, scenario, tool_call_response, tool_policy, user_envelope, ScriptedLlm,
    CHANNEL, CONVERSATION,
};

const ECHO_TOOL: &str = "echo";

/// A deterministic tool: the golden pins its output, so it must not depend on
/// the clock, the filesystem, or the environment.
struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(ECHO_TOOL),
            description: Arc::from("Echo the `text` argument back verbatim."),
            input_schema: json!({
                "type": "object",
                "required": ["text"],
                "properties": { "text": { "type": "string" } }
            }),
            requires_isolation: false,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let text = serde_json::from_str::<Value>(args.get())
            .ok()
            .and_then(|value| value.get("text").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default();
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(text),
            metadata: BTreeMap::new(),
        })
    }
}

const BULK_TOOL: &str = "bulk";

/// Deterministic numbered lines, so a spilled artifact is byte-identical run
/// to run and the golden can pin the preview it produced.
fn bulk_output(lines: usize) -> String {
    (0..lines)
        .map(|index| format!("line {index:04}: the quick brown fox jumps over the lazy dog\n"))
        .collect()
}

/// A tool whose output exceeds any sane inline cap — the case C2 exists for.
struct BulkTool;

#[async_trait]
impl Tool for BulkTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(BULK_TOOL),
            description: Arc::from("Emit `lines` numbered lines of filler."),
            input_schema: json!({
                "type": "object",
                "required": ["lines"],
                "properties": { "lines": { "type": "integer" } }
            }),
            requires_isolation: false,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let lines = serde_json::from_str::<Value>(args.get())
            .ok()
            .and_then(|value| value.get("lines").and_then(Value::as_u64))
            .unwrap_or(0) as usize;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(bulk_output(lines)),
            metadata: BTreeMap::new(),
        })
    }
}

fn echo_registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(EchoTool);
    tools
}

fn max_with(llm: Arc<ScriptedLlm>, tools: Vec<ToolSpec>) -> MaxOrchestrator {
    MaxOrchestrator::with_tools(tools).with_llm(llm)
}

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
        .load(&ConversationId::new(CONVERSATION))
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
        .load(&ConversationId::new(CONVERSATION))
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
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");
    scenario_golden("tool_call_allowed", &llm, &transcript, &outcome);
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
    let undecided: Vec<Value> = ["y", "yes, go ahead", "approve", "/approve"]
        .into_iter()
        .map(|text| {
            let answer = user_envelope(text);
            json!({
                "input": text,
                "routed": format!("{:?}", route(Some(&ticket), &answer)),
            })
        })
        .collect();
    // Resume the way the gateway does: by the id the paused run is actually
    // waiting on, not a guessed one.
    let approval_id = state
        .pending_approvals
        .first()
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
        .load(&ConversationId::new(CONVERSATION))
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
        .load(&ConversationId::new(CONVERSATION))
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
    let conversation = ConversationId::new(CONVERSATION);
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
        conversation_id: conversation.clone(),
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
    let conversation = ConversationId::new(CONVERSATION);
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

// ---------------------------------------------------------------------------
// 8. Hydrated memory
// ---------------------------------------------------------------------------

/// Build a manager holding one semantic fact in the run's conversation scope.
async fn manager_with_fact(fact: &str) -> Arc<MemoryManager> {
    let store = Arc::new(SqliteStore::open_in_memory().expect("sqlite opens in memory"));
    let manager = Arc::new(MemoryManager::new_sqlite(store));
    let caller = agentos_core::memory::MemoryCaller {
        agent_id: AgentId::new("golden-agent"),
        task_id: agentos_proto::TaskId::new("golden-task"),
        conversation_id: ConversationId::new(CONVERSATION),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    };
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Conversation(ConversationId::new(CONVERSATION)),
        MemoryVisibility::Private,
        None,
    );
    manager
        .write_scoped(&caller, scope, json!({ "fact": fact }), BTreeMap::new())
        .await
        .expect("seeding a semantic fact succeeds");
    manager
}

fn hydration_settings() -> MemoryHydrationSettings {
    MemoryHydrationSettings {
        enabled: true,
        max_fragments: 5,
        max_estimated_tokens: 1200,
        // Recency, not Hybrid: this scenario asserts whether hydrated fragments
        // reach the request, not how well retrieval ranks them.
        stores: vec![MemoryStore::Semantic],
        strategy: RetrievalStrategy::Recency,
        allowed_shared_domains: Vec::new(),
    }
}

/// Pins today's behavior: hydration runs, selects the fact, and the assembled
/// request does **not** contain it (roadmap finding F1). The golden is expected
/// to change when roadmap item P1 lands a prompt-assembly step — that diff is
/// the point, not a regression.
#[tokio::test]
async fn golden_memory_hydration() {
    let manager = manager_with_fact("The deploy key rotates every 90 days.").await;
    let llm = Arc::new(ScriptedLlm::new([assistant("Noted.")]));
    let orchestrator = MaxOrchestrator::new()
        .with_llm(llm.clone())
        .with_memory_hydrator(manager, hydration_settings());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let outcome = run_envelope(
        user_envelope("How often does the deploy key rotate?"),
        RunId::new("golden-memory"),
        &deps,
    )
    .await
    .expect("a hydrated run finishes");

    let transcript = session
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");
    scenario_golden("memory_hydration", &llm, &transcript, &outcome);
}

/// The F1 acceptance test, landed by roadmap item P1: hydrated fragments must
/// appear in the assembled request. Before P1 they were written to
/// `RunContext::memory_fragments`, counted in telemetry, and dropped before the
/// request was built.
#[tokio::test]
async fn hydrated_memory_reaches_the_model() {
    let fact = "The deploy key rotates every 90 days.";
    let manager = manager_with_fact(fact).await;
    let llm = Arc::new(ScriptedLlm::new([assistant("Noted.")]));
    let orchestrator = MaxOrchestrator::new()
        .with_llm(llm.clone())
        .with_memory_hydrator(manager, hydration_settings());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    run_envelope(
        user_envelope("How often does the deploy key rotate?"),
        RunId::new("p1-target"),
        &deps,
    )
    .await
    .expect("a hydrated run finishes");

    let requests = llm.requests();
    let request = requests.first().expect("the orchestrator called the model");
    let carried = request
        .messages
        .iter()
        .any(|message| message.content.contains("90 days"));
    assert!(
        carried,
        "the hydrated fact must appear in the assembled request; got: {:#?}",
        request
            .messages
            .iter()
            .map(|message| message.content.as_ref())
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// 9. Oversized tool result: spilled, not destroyed
// ---------------------------------------------------------------------------

/// Roadmap item C2. Before it, output past the inline cap was cut and the
/// remainder destroyed. Now the full text goes to the spill store and the
/// transcript keeps a preview plus a locator, so the golden pins both what the
/// model was told and — by reading the locator back — that nothing was lost.
#[tokio::test]
async fn golden_tool_result_spilled() {
    let llm = Arc::new(ScriptedLlm::new([
        tool_call_response("call-1", BULK_TOOL, r#"{"lines":128}"#),
        assistant("Read the first lines; the rest is on disk."),
    ]));
    let mut tools = ToolRegistry::new();
    tools.register(BulkTool);
    let orchestrator = max_with(llm.clone(), vec![BulkTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[BULK_TOOL], PolicyVerb::Allow);

    let spill_root = support::temp_tree("spill");
    let store = SpillStore::new(spill_root.path());
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    // A cap far below the default, so the scenario spills without a golden
    // carrying 64 KiB of filler.
    deps.content_limits = ContentLimits {
        tool_result_inline_bytes: 512,
        spill: Some(&store),
    };

    let outcome = run_envelope(
        user_envelope("Dump the log for me."),
        RunId::new("golden-spill"),
        &deps,
    )
    .await
    .expect("an oversized tool result finishes the run");

    let transcript = session
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");

    // The exit condition, end to end: the locator the model was handed names a
    // file holding the output in full, not the preview it saw.
    let locator = transcript
        .items
        .iter()
        .find_map(|item| item.message.metadata.get(SPILL_LOCATOR_KEY))
        .and_then(Value::as_str)
        .expect("a spilled result records its locator");
    let recovered = std::fs::read_to_string(locator).expect("the locator names a readable file");
    assert_eq!(recovered, bulk_output(128));

    // The locator is an absolute temp path, so it is redacted before pinning;
    // the file *name* stays, because that is the part C2 derives.
    let document = serde_json::to_string(&scenario(&llm, &transcript, &outcome))
        .expect("scenario documents serialize")
        .replace(&spill_root.path().to_string_lossy().to_string(), "<spill>");
    support::assert_golden(
        "tool_result_spilled",
        &serde_json::from_str(&document).expect("redaction preserves valid JSON"),
    );
}

/// The other half of C2, end to end: once a run is near its context window,
/// the middle of an already-recorded tool result is elided from the *request*
/// while the session log keeps what it always held.
///
/// Not a golden — pinning it would mean storing kilobytes of filler to assert
/// one marker. `prompt::prune`'s unit tests cover the elision rules; this
/// covers that they reach a provider.
#[tokio::test]
async fn elision_reaches_the_model_but_not_the_log() {
    let llm = Arc::new(
        ScriptedLlm::new([
            tool_call_response("call-1", BULK_TOOL, r#"{"lines":128}"#),
            assistant("Summarised from the head and tail."),
        ])
        // Small enough that one spilled result puts the run over the trigger.
        .with_context_budget(1_024),
    );
    let mut tools = ToolRegistry::new();
    tools.register(BulkTool);
    let orchestrator = max_with(llm.clone(), vec![BulkTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[BULK_TOOL], PolicyVerb::Allow);

    let spill_root = support::temp_tree("elision");
    let store = SpillStore::new(spill_root.path());
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.content_limits = ContentLimits {
        tool_result_inline_bytes: 4_096,
        spill: Some(&store),
    };

    run_envelope(
        user_envelope("Dump the log for me."),
        RunId::new("c2-elision"),
        &deps,
    )
    .await
    .expect("a run under pressure still finishes");

    let requests = llm.requests();
    let second = requests.get(1).expect("the tool result is planned on");
    let tool_message = second
        .messages
        .iter()
        .find(|message| message.role == MessageRole::Tool)
        .expect("the request carries the tool result");
    assert!(
        tool_message
            .content
            .contains("bytes elided from the middle"),
        "the request should have been elided; got {} bytes",
        tool_message.content.len()
    );
    // Elided, not destroyed: the marker names the file holding the rest.
    assert!(tool_message.content.contains("bulk-call_1.txt"));

    // The log is untouched. Elision is a view over it, recomputed each turn,
    // so a later compaction could still show these bytes in full.
    let transcript = session
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");
    let logged = transcript
        .items
        .iter()
        .find(|item| item.message.role == MessageRole::Tool)
        .expect("the log carries the tool result");
    assert!(!logged
        .message
        .content
        .contains("bytes elided from the middle"));
    assert!(logged.message.content.len() > tool_message.content.len());
}

// ---------------------------------------------------------------------------

fn scenario_golden(
    name: &str,
    llm: &ScriptedLlm,
    transcript: &agentos_interfaces::session::Transcript,
    outcome: &RunOutcome,
) {
    support::assert_golden(name, &scenario(llm, transcript, outcome));
}
