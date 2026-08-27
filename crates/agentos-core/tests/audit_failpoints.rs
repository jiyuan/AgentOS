//! `AUD-002`: required safety evidence is part of the transition, not logging
//! performed after the fact.

mod support;

use agentos_core::approve::{
    DelegationGrantTemplate, Policy, PolicyAction, PolicyRule, PolicyVerb,
};
use agentos_core::audit::SafetyEventKind;
use agentos_core::memory::SqliteStore;
use agentos_core::r#loop::{
    route, ApprovalBinding, ApprovalOutcome, ApprovalTicket, Routed, ToolGuardrailEntry,
};
use agentos_core::runner::{resume_run, run_envelope, PausedRun, RunOutcome, RunnerDeps};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::guardrail::{GuardrailError, GuardrailOutcome, ToolGuardrail};
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use rusqlite::Connection;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const TOOL: &str = "protected";
const AGENT: &str = "audit-failpoint-agent";
const CHANNEL: &str = "audit-failpoint-channel";
const CONVERSATION: &str = "audit-failpoint-conversation";

fn call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("protected-call"),
        name: Arc::from(TOOL),
        args: RawValue::from_string("{}".to_owned()).expect("static arguments are JSON"),
    }
}

fn envelope() -> Envelope {
    Envelope {
        channel_id: ChannelId::new(CHANNEL),
        conversation_id: ConversationId::new(CONVERSATION),
        sender: Arc::from("operator"),
        message: Message::text(MessageRole::User, "run it"),
        metadata: BTreeMap::new(),
    }
}

struct CallOnce;

#[async_trait]
impl Orchestrator for CallOnce {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        if ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| item.message.role == MessageRole::Tool)
        {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        } else {
            Ok(Plan::CallTool(call()))
        }
    }
}

struct FailPlanning;

#[async_trait]
impl Orchestrator for FailPlanning {
    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Err(OrchestratorError::Backend(Arc::from(
            "injected planning failure",
        )))
    }
}

struct DelegateOnce;

#[async_trait]
impl Orchestrator for DelegateOnce {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        if ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| item.message.role == MessageRole::Tool)
        {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        } else {
            Ok(Plan::Delegate(SubAgentSpec {
                agent_id: AgentId::new("child"),
                policy_id: Arc::from("child-policy"),
                metadata: BTreeMap::new(),
            }))
        }
    }
}

struct CountingTool {
    calls: Arc<AtomicUsize>,
    sandbox: SandboxMode,
}

#[async_trait]
impl Tool for CountingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(TOOL),
            description: Arc::from("counts protected executions"),
            input_schema: serde_json::json!({"type": "object"}),
            safety: Default::default(),
            sandbox: self.sandbox,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ok"),
            metadata: BTreeMap::new(),
        })
    }
}

struct RefuseCall;

#[async_trait]
impl ToolGuardrail for RefuseCall {
    async fn check_call(
        &self,
        _call: &ToolCall,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Tripped(Arc::from("injected refusal")))
    }

    async fn check_result(
        &self,
        _result: &ToolResult,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Passed)
    }
}

fn policy(tool: PolicyVerb) -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(TOOL)),
            decision: tool,
            reason: Some(Arc::from("test policy")),
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    }
}

fn delegation_policy() -> Policy {
    Policy {
        rules: vec![
            PolicyRule {
                action: PolicyAction::Delegate,
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::new(),
            },
            PolicyRule {
                action: PolicyAction::Tool(Arc::from(TOOL)),
                decision: PolicyVerb::AskUser,
                reason: Some(Arc::from("parent requires approval")),
                arg_equals: BTreeMap::new(),
            },
        ],
        default_decision: PolicyVerb::Deny,
    }
}

#[allow(clippy::too_many_arguments)]
fn deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    store: &'a SqliteStore,
    policy: &'a Policy,
    tools: Option<&'a ToolRegistry>,
    subagents: Option<&'a SubAgentRegistry>,
    guardrails: &'a [ToolGuardrailEntry<'a>],
    cancel: CancellationToken,
) -> RunnerDeps<'a> {
    RunnerDeps {
        orchestrator,
        session: store,
        memory_manager: None,
        hooks: None,
        max_turns: 8,
        active_agent: AgentId::new(AGENT),
        tools,
        trace_sink: None,
        task_workspace: None,
        policy,
        subagents,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: guardrails,
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel,
        steering: None,
        run_journal: None,
        safety_log: Some(store),
        delegated_authority: None,
    }
}

fn install_failpoint(path: &Path, kind: SafetyEventKind) {
    let conn = Connection::open(path).expect("failpoint connection opens");
    conn.execute_batch("DROP TRIGGER IF EXISTS fail_required_safety_event;")
        .expect("old failpoint drops");
    conn.execute_batch(&format!(
        "CREATE TRIGGER fail_required_safety_event BEFORE INSERT ON safety_events \
         WHEN NEW.kind = '{}' BEGIN SELECT RAISE(FAIL, 'injected safety append failure'); END;",
        kind.as_str()
    ))
    .expect("failpoint installs");
}

fn remove_failpoint(path: &Path) {
    Connection::open(path)
        .expect("failpoint connection opens")
        .execute_batch("DROP TRIGGER IF EXISTS fail_required_safety_event;")
        .expect("failpoint drops");
}

fn assert_evidence_failure(error: impl std::fmt::Display, kind: SafetyEventKind) {
    let rendered = error.to_string();
    assert!(
        rendered.contains("required safety event") && rendered.contains(kind.as_str()),
        "failure must name the missing required evidence, got: {rendered}"
    );
}

fn registry(calls: &Arc<AtomicUsize>, sandbox: SandboxMode) -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(CountingTool {
        calls: Arc::clone(calls),
        sandbox,
    });
    tools
}

fn approval_witness(state: &agentos_interfaces::RunState) -> agentos_core::r#loop::ResumeWitness {
    let pending = state.pending_approval().expect("approval is pending");
    let ticket = ApprovalTicket::parse(&pending.approval_ticket).expect("ticket parses");
    let binding = ApprovalBinding::new(
        pending.approval_instance_id.clone(),
        ticket.clone(),
        pending.id.clone(),
        pending.prompting_principal.clone(),
        None,
    )
    .expect("binding is valid");
    let Routed::Decides { witness } = route(
        Some(&binding),
        &Envelope {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            sender: Arc::from("operator"),
            message: Message::text(MessageRole::User, format!("/approve {ticket}")),
            metadata: BTreeMap::new(),
        },
    ) else {
        panic!("approval answer routes");
    };
    assert_eq!(witness.outcome(), ApprovalOutcome::Approved);
    witness
}

fn granted_subagents(store: Arc<SqliteStore>, calls: Arc<AtomicUsize>) -> SubAgentRegistry {
    let child_tools = Arc::new(registry(&calls, SandboxMode::FullAccess));
    let child = SubAgentDefinition::new(
        AgentId::new("child"),
        "child-policy",
        Arc::new(CallOnce),
        Policy::allow_tools([TOOL]),
    )
    .with_tools(child_tools)
    .with_max_turns(4)
    .with_delegation_grants(vec![DelegationGrantTemplate {
        action: PolicyAction::Tool(Arc::from(TOOL)),
        decision: PolicyVerb::Allow,
        arg_equals: BTreeMap::new(),
        reason: Arc::from("bounded unattended child action"),
        lifetime_secs: 60,
    }]);
    let mut registry = SubAgentRegistry::new()
        .with_session(store.clone())
        .with_safety_log(Some(store));
    registry.register(child);
    registry
}

fn fresh_store(label: &str) -> (support::TempTree, Arc<SqliteStore>, std::path::PathBuf) {
    let tree = support::temp_tree(label);
    let path = tree.path().join("store.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    (tree, store, path)
}

/// AF-016: every required event is either durable or the protected transition
/// stops with a typed operational failure.
#[tokio::test]
async fn required_safety_event_failure_aborts_the_action() {
    // Approval request: no prompt is exposed and no tool executes.
    let (_tree, store, path) = fresh_store("audit-request-fail");
    install_failpoint(&path, SafetyEventKind::ApprovalRequested);
    let calls = Arc::new(AtomicUsize::new(0));
    let tools = registry(&calls, SandboxMode::FullAccess);
    let ask = policy(PolicyVerb::AskUser);
    let error = run_envelope(
        envelope(),
        RunId::new("request-failure"),
        &deps(
            &CallOnce,
            &store,
            &ask,
            Some(&tools),
            None,
            &[],
            Default::default(),
        ),
    )
    .await
    .expect_err("an unrecorded prompt must not be exposed");
    assert_evidence_failure(error, SafetyEventKind::ApprovalRequested);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Approval resolution: the approved action remains blocked.
    let (_tree, store, path) = fresh_store("audit-resolution-fail");
    let calls = Arc::new(AtomicUsize::new(0));
    let tools = registry(&calls, SandboxMode::FullAccess);
    let ask = policy(PolicyVerb::AskUser);
    let runner = deps(
        &CallOnce,
        &store,
        &ask,
        Some(&tools),
        None,
        &[],
        Default::default(),
    );
    let RunOutcome::Paused(state) =
        run_envelope(envelope(), RunId::new("resolution-failure"), &runner)
            .await
            .expect("the request is recorded before the failpoint")
    else {
        panic!("ask_user pauses");
    };
    let witness = approval_witness(&state);
    install_failpoint(&path, SafetyEventKind::ApprovalResolved);
    let error = resume_run(
        PausedRun {
            channel_id: ChannelId::new(CHANNEL),
            conversation_id: ConversationId::new(CONVERSATION),
            state,
        },
        witness,
        &runner,
    )
    .await
    .expect_err("an unrecorded approval must not execute");
    assert_evidence_failure(error, SafetyEventKind::ApprovalResolved);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Policy and guardrail refusals remain refusals; the run cannot continue
    // to another model turn after their evidence fails.
    for (label, kind, verb, guarded) in [
        (
            "denial",
            SafetyEventKind::PolicyDenial,
            PolicyVerb::Deny,
            false,
        ),
        (
            "guardrail",
            SafetyEventKind::ToolGuardrailTrip,
            PolicyVerb::Allow,
            true,
        ),
    ] {
        let (_tree, store, path) = fresh_store(label);
        install_failpoint(&path, kind);
        let calls = Arc::new(AtomicUsize::new(0));
        let tools = registry(&calls, SandboxMode::FullAccess);
        let selected_policy = policy(verb);
        let guardrails = [ToolGuardrailEntry {
            name: Arc::from("RefuseCall"),
            guardrail: &RefuseCall,
        }];
        let selected_guardrails = if guarded { &guardrails[..] } else { &[] };
        let error = run_envelope(
            envelope(),
            RunId::new(format!("{label}-failure")),
            &deps(
                &CallOnce,
                &store,
                &selected_policy,
                Some(&tools),
                None,
                selected_guardrails,
                Default::default(),
            ),
        )
        .await
        .expect_err("a refusal without evidence stops the run");
        assert_evidence_failure(error, kind);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    // Sandbox refusal: the in-process body is still unreachable and the
    // missing refusal event becomes the surfaced failure.
    let (_tree, store, path) = fresh_store("audit-sandbox-fail");
    install_failpoint(&path, SafetyEventKind::SandboxRefusal);
    let calls = Arc::new(AtomicUsize::new(0));
    let tools = registry(&calls, SandboxMode::ReadOnly);
    let allow = policy(PolicyVerb::Allow);
    let error = run_envelope(
        envelope(),
        RunId::new("sandbox-failure"),
        &deps(
            &CallOnce,
            &store,
            &allow,
            Some(&tools),
            None,
            &[],
            Default::default(),
        ),
    )
    .await
    .expect_err("sandbox refusal evidence is required");
    assert_evidence_failure(error, SafetyEventKind::SandboxRefusal);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Cancellation is already the safer outcome; an append failure converts
    // it to an operational error rather than resuming or emitting success.
    let (_tree, store, path) = fresh_store("audit-cancel-fail");
    install_failpoint(&path, SafetyEventKind::Cancellation);
    let calls = Arc::new(AtomicUsize::new(0));
    let tools = registry(&calls, SandboxMode::FullAccess);
    let allow = policy(PolicyVerb::Allow);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = run_envelope(
        envelope(),
        RunId::new("cancellation-failure"),
        &deps(&CallOnce, &store, &allow, Some(&tools), None, &[], cancel),
    )
    .await
    .expect_err("unrecorded cancellation is an operational failure");
    assert_evidence_failure(error, SafetyEventKind::Cancellation);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Grant issuance and use both precede the authority they evidence.
    for kind in [
        SafetyEventKind::DelegationGrantIssued,
        SafetyEventKind::DelegationGrantUsed,
    ] {
        let (_tree, store, path) = fresh_store(kind.as_str());
        install_failpoint(&path, kind);
        let calls = Arc::new(AtomicUsize::new(0));
        let subagents = granted_subagents(store.clone(), Arc::clone(&calls));
        let parent_policy = delegation_policy();
        let error = run_envelope(
            envelope(),
            RunId::new(format!("{}-failure", kind.as_str())),
            &deps(
                &DelegateOnce,
                &store,
                &parent_policy,
                None,
                Some(&subagents),
                &[],
                Default::default(),
            ),
        )
        .await
        .expect_err("delegated authority requires durable evidence");
        assert_evidence_failure(error, kind);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    // A terminal error whose own marker fails is surfaced as the audit
    // operational failure, without recursive best-effort appends.
    let (_tree, store, path) = fresh_store("audit-terminal-fail");
    install_failpoint(&path, SafetyEventKind::TerminalError);
    let deny = policy(PolicyVerb::Deny);
    let error = run_envelope(
        envelope(),
        RunId::new("terminal-failure"),
        &deps(
            &FailPlanning,
            &store,
            &deny,
            None,
            None,
            &[],
            Default::default(),
        ),
    )
    .await
    .expect_err("terminal evidence failure is surfaced");
    assert_evidence_failure(error, SafetyEventKind::TerminalError);

    remove_failpoint(&path);
}
