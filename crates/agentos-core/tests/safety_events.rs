//! M6 / `AUD-001`: a safety-boundary decision leaves a durable record.
//!
//! [ADR-0005](../../../docs/adr/0005-SAFETY_EVENTS.md). Every test here drives
//! a real run against a real on-disk store and then **reopens the file** before
//! reading, because the property is not "the event was written" — it is that a
//! later reader, in a later process, can reconstruct what the runtime was
//! allowed to do.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::audit::{SafetyEventKind, SafetyLog, SafetyOutcome, StoredSafetyEvent};
use agentos_core::memory::SqliteStore;
use agentos_core::r#loop::ToolGuardrailEntry;
use agentos_core::runner::{
    resume_run, run_envelope, JsonlTraceSink, PausedRun, ResumeDecision, RunOutcome, RunnerDeps,
    TraceSink,
};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::guardrail::{GuardrailError, GuardrailOutcome, ToolGuardrail};
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::run_state::ApprovalStatus;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

const TOOL: &str = "secretive";
const SENDER: &str = "operator";
const AGENT: &str = "audited-agent";
const CHANNEL: &str = "audit-channel";
const CONVERSATION: &str = "audit-conversation";

/// Planted in the tool arguments of every run below. If this string is ever
/// findable in a stored event, the redaction contract is broken.
const CANARY: &str = "correct-horse-battery-staple";

/// SHA-256 of `{"token":"correct-horse-battery-staple"}`, written out rather
/// than recomputed with the same code the runtime uses — a test that hashes
/// with the implementation agrees with it by construction.
const CANARY_ARGS_DIGEST: &str = "d6679f2b38976ecf3b7952b51429faed222e726661f4311ccdd1a0084daef793";

fn call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("audited-call"),
        name: Arc::from(TOOL),
        args: serde_json::value::RawValue::from_string(
            serde_json::json!({ "token": CANARY }).to_string(),
        )
        .expect("the arguments are valid JSON"),
    }
}

/// Calls the tool once, then answers with whatever came back.
struct CallOnce;

#[async_trait]
impl Orchestrator for CallOnce {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let saw_result = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| matches!(item.message.role, MessageRole::Tool));
        if saw_result {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        } else {
            Ok(Plan::CallTool(call()))
        }
    }
}

struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(TOOL),
            description: Arc::from("does nothing of consequence"),
            input_schema: serde_json::json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(
        &self,
        call: &ToolCall,
        _args: &serde_json::value::RawValue,
    ) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ok"),
            metadata: BTreeMap::new(),
        })
    }
}

/// Refuses every call it is shown, before the call runs.
struct RefuseEverything;

#[async_trait]
impl ToolGuardrail for RefuseEverything {
    async fn check_call(
        &self,
        _call: &ToolCall,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Tripped(Arc::from(
            "this deployment refuses that",
        )))
    }

    async fn check_result(
        &self,
        _result: &ToolResult,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Passed)
    }
}

fn envelope() -> Envelope {
    Envelope {
        channel_id: ChannelId::new(CHANNEL),
        conversation_id: ConversationId::new(CONVERSATION),
        sender: Arc::from(SENDER),
        message: Message::text(MessageRole::User, "go"),
        metadata: BTreeMap::new(),
    }
}

fn policy(decision: PolicyVerb) -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(TOOL)),
            decision,
            reason: Some(Arc::from("the operator wrote a rule about this")),
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    }
}

fn store_at(path: &Path) -> Arc<SqliteStore> {
    Arc::new(SqliteStore::open(path).expect("the store opens"))
}

/// Reopen the file and read everything back, so no test can pass on state that
/// only ever lived in the writer's memory.
fn reread(path: &Path) -> Vec<StoredSafetyEvent> {
    store_at(path)
        .recent(64)
        .expect("the safety log is readable")
}

fn kinds(events: &[StoredSafetyEvent]) -> Vec<(SafetyEventKind, SafetyOutcome)> {
    events
        .iter()
        .map(|stored| (stored.event.kind, stored.event.outcome))
        .collect()
}

fn deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    session: &'a SqliteStore,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
    safety_log: &'a dyn SafetyLog,
    tool_guardrails: &'a [ToolGuardrailEntry<'a>],
) -> RunnerDeps<'a> {
    deps_with_trace(
        orchestrator,
        session,
        policy,
        tools,
        safety_log,
        tool_guardrails,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn deps_with_trace<'a>(
    orchestrator: &'a dyn Orchestrator,
    session: &'a SqliteStore,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
    safety_log: &'a dyn SafetyLog,
    tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    trace_sink: Option<&'a dyn TraceSink>,
) -> RunnerDeps<'a> {
    RunnerDeps {
        orchestrator,
        session,
        memory_manager: None,
        hooks: None,
        max_turns: 8,
        active_agent: AgentId::new(AGENT),
        tools: Some(tools),
        trace_sink,
        task_workspace: None,
        policy,
        subagents: None,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails,
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel: Default::default(),
        steering: None,
        safety_log: Some(safety_log),
        granted_authority: &[],
    }
}

fn registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(EchoTool);
    tools
}

#[tokio::test]
async fn an_approval_leaves_a_question_and_an_answer_that_outlive_the_process() {
    let tree = support::temp_tree("safety-approve");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = CallOnce;
    let policy = policy(PolicyVerb::AskUser);
    let tools = registry();
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
    );

    let RunOutcome::Paused(paused_state) =
        run_envelope(envelope(), RunId::new("run-approve"), &deps)
            .await
            .expect("the gated call pauses")
    else {
        panic!("an ask_user policy must pause");
    };
    let approval_id = paused_state
        .pending_approval()
        .expect("the run is waiting on something")
        .id
        .clone();

    resume_run(
        PausedRun {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            state: paused_state,
        },
        &approval_id,
        ResumeDecision::Approve,
        &deps,
    )
    .await
    .expect("the approved run finishes");

    // Oldest first reads like the story it is.
    let mut events = reread(&path);
    events.reverse();
    assert_eq!(
        kinds(&events),
        vec![
            (SafetyEventKind::ApprovalRequested, SafetyOutcome::Requested),
            (SafetyEventKind::ApprovalResolved, SafetyOutcome::Approved),
        ],
        "a successful approval is a question and an answer, not the absence of a row"
    );

    let requested = &events[0].event;
    assert_eq!(requested.subject.as_ref(), TOOL);
    assert_eq!(requested.interruption_id.as_ref(), Some(&approval_id));
    assert_eq!(events[1].event.interruption_id.as_ref(), Some(&approval_id));

    // The principal is what makes the row attributable, and it round-trips
    // through storage rather than being reconstructed by the reader.
    let principal = requested
        .principal
        .as_ref()
        .expect("a run-scoped event names its principal");
    assert_eq!(principal.agent.as_str(), AGENT);
    assert_eq!(principal.channel.as_str(), CHANNEL);
    assert_eq!(principal.conversation.as_str(), CONVERSATION);
    assert_eq!(principal.sender.as_deref(), Some(SENDER));
}

#[tokio::test]
async fn a_successful_approval_is_retained_in_the_state_rather_than_deleted() {
    let tree = support::temp_tree("safety-retain");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = CallOnce;
    let policy = policy(PolicyVerb::AskUser);
    let tools = registry();
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
    );

    let RunOutcome::Paused(paused_state) =
        run_envelope(envelope(), RunId::new("run-retain"), &deps)
            .await
            .expect("the gated call pauses")
    else {
        panic!("an ask_user policy must pause");
    };
    let approval_id = paused_state
        .pending_approval()
        .expect("the run is waiting on something")
        .id
        .clone();

    let RunOutcome::Finished { state, .. } = resume_run(
        PausedRun {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            state: paused_state,
        },
        &approval_id,
        ResumeDecision::Approve,
        &deps,
    )
    .await
    .expect("the approved run finishes") else {
        panic!("an approved run finishes");
    };

    // Before M6 this vector was empty here: `take_approved_action` removed the
    // interruption, and its absence was the only evidence the approval had
    // succeeded.
    assert_eq!(state.approvals.len(), 1);
    assert!(state.pending_approval().is_none());
    assert_eq!(state.approvals[0].status, ApprovalStatus::Approved);
    assert!(
        state.approvals[0].consumed,
        "the action was taken, and the record says so instead of vanishing"
    );
}

#[tokio::test]
async fn a_refusal_and_a_question_nobody_answered_are_different_rows() {
    let tree = support::temp_tree("safety-reject");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = CallOnce;
    let policy = policy(PolicyVerb::AskUser);
    let tools = registry();
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
    );

    for (run, decision, expected) in [
        (
            "run-rejected",
            ResumeDecision::Reject {
                reason: Arc::from("no"),
            },
            SafetyOutcome::Rejected,
        ),
        (
            "run-unanswered",
            ResumeDecision::Cancel {
                reason: Arc::from("nobody was there"),
            },
            SafetyOutcome::Unanswered,
        ),
    ] {
        let RunOutcome::Paused(paused_state) = run_envelope(envelope(), RunId::new(run), &deps)
            .await
            .expect("the gated call pauses")
        else {
            panic!("an ask_user policy must pause");
        };
        let approval_id = paused_state
            .pending_approval()
            .expect("the run is waiting on something")
            .id
            .clone();
        // Both fail the run closed, which is the point: the record has to
        // survive the error path, not only the happy one.
        resume_run(
            PausedRun {
                channel_id: ChannelId::new(CHANNEL),
                conversation_id: ConversationId::new(CONVERSATION),
                state: paused_state,
            },
            &approval_id,
            decision,
            &deps,
        )
        .await
        .expect_err("a refused approval fails the run");

        let events = reread(&path);
        let resolved = events
            .iter()
            .find(|stored| stored.event.kind == SafetyEventKind::ApprovalResolved)
            .expect("the resolution is recorded");
        assert_eq!(resolved.event.outcome, expected);
        assert_eq!(resolved.event.interruption_id.as_ref(), Some(&approval_id));
    }
}

#[tokio::test]
async fn a_policy_denial_records_the_call_as_a_digest_and_never_as_arguments() {
    let tree = support::temp_tree("safety-deny");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = CallOnce;
    let policy = policy(PolicyVerb::Deny);
    let tools = registry();
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
    );

    run_envelope(envelope(), RunId::new("run-deny"), &deps)
        .await
        .expect("a denied tool call is recoverable, so the run still finishes");

    let events = reread(&path);
    let denial = events
        .iter()
        .find(|stored| stored.event.kind == SafetyEventKind::PolicyDenial)
        .expect("the denial is recorded");
    assert_eq!(denial.event.outcome, SafetyOutcome::Denied);
    assert_eq!(denial.event.subject.as_ref(), TOOL);
    assert_eq!(
        denial.event.argument_digest.as_ref().map(|d| d.as_str()),
        Some(CANARY_ARGS_DIGEST),
        "the digest names the exact call the decision applied to"
    );

    assert_no_canary(&events);
}

#[tokio::test]
async fn a_tool_guardrail_that_stops_a_call_records_a_refusal() {
    let tree = support::temp_tree("safety-guardrail");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = CallOnce;
    let policy = policy(PolicyVerb::Allow);
    let tools = registry();
    let guardrails = [ToolGuardrailEntry {
        name: Arc::from("RefuseEverything"),
        guardrail: &RefuseEverything,
    }];
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &guardrails,
    );

    run_envelope(envelope(), RunId::new("run-guardrail"), &deps)
        .await
        .expect("a blocked call comes back as a result the model can read");

    let events = reread(&path);
    let trip = events
        .iter()
        .find(|stored| stored.event.kind == SafetyEventKind::ToolGuardrailTrip)
        .expect("the trip is recorded");
    // `Refused`, because the call never ran. A post-result trip is `Tripped`.
    assert_eq!(trip.event.outcome, SafetyOutcome::Refused);
    assert_eq!(trip.event.subject.as_ref(), "RefuseEverything");
    assert_eq!(
        trip.event.detail.as_deref(),
        Some("this deployment refuses that")
    );

    assert_no_canary(&events);
}

/// Plans a handoff, which has no tool-call id to attach a denial to and so
/// ends the run rather than being something the model can replan around.
struct HandOff;

#[async_trait]
impl Orchestrator for HandOff {
    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Handoff(AgentId::new("somewhere-else"), None))
    }
}

#[tokio::test]
async fn a_structural_denial_records_both_the_decision_and_the_run_ending() {
    let tree = support::temp_tree("safety-terminal");
    let path = tree.path().join("store.sqlite");
    let store = store_at(&path);
    let orchestrator = HandOff;
    // No rule covers a handoff, so the `deny` default decides it — and a
    // denied handoff is fatal, unlike a denied tool call.
    let policy = policy(PolicyVerb::Allow);
    let tools = registry();
    let deps = deps(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
    );

    run_envelope(envelope(), RunId::new("run-terminal"), &deps)
        .await
        .expect_err("a denied handoff ends the run");

    let mut events = reread(&path);
    events.reverse();
    assert_eq!(
        kinds(&events),
        vec![
            (SafetyEventKind::PolicyDenial, SafetyOutcome::Denied),
            (SafetyEventKind::TerminalError, SafetyOutcome::Failed),
        ],
        "the decision and the run ending are two facts, and a reader asking \
         why there is no answer needs the second one"
    );
    assert_eq!(events[0].event.subject.as_ref(), "somewhere-else");
    assert_eq!(events[1].event.subject.as_ref(), "approval_denied");
}

/// The redaction contract, over every field of every stored event rather than
/// over the one a reader would think to check. `Debug` renders the whole
/// struct, so a field added later that forwarded arguments would fail here
/// without anyone remembering to extend this.
///
/// Not checked against the raw file: the same SQLite file holds the session
/// log, and the transcript is *supposed* to contain the call.
fn assert_no_canary(events: &[StoredSafetyEvent]) {
    let rendered = format!("{events:?}");
    assert!(
        !rendered.contains(CANARY),
        "a tool argument reached the safety log: {rendered}"
    );
}

#[tokio::test]
async fn a_failed_run_still_persists_the_trace_it_had_built() {
    // Deliverable 3 of M6. `RunLoopState::step` consumes the state, so before
    // `StepFailure` the runner's error path had nothing to write and a run
    // that ended badly left no trace file at all — the runs most worth
    // reconstructing recording the least.
    let tree = support::temp_tree("safety-trace");
    let path = tree.path().join("store.sqlite");
    let traces = tree.path().join("traces");
    let store = store_at(&path);
    let sink = JsonlTraceSink::new(&traces);
    let orchestrator = HandOff;
    let policy = policy(PolicyVerb::Allow);
    let tools = registry();
    let deps = deps_with_trace(
        &orchestrator,
        store.as_ref(),
        &policy,
        &tools,
        store.as_ref(),
        &[],
        Some(&sink),
    );

    run_envelope(envelope(), RunId::new("run-traced"), &deps)
        .await
        .expect_err("a denied handoff ends the run");

    let file = traces.join("run-traced.jsonl");
    let written = std::fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("a failed run writes {}: {err}", file.display()));
    assert!(
        written.contains("\"phase\":\"failed\""),
        "the records are marked as coming from a failed run: {written}"
    );
    assert!(
        written.contains("plan_started") && written.contains("plan_finished"),
        "the turn the run got through is in the file: {written}"
    );
}
