//! M6 / `STATE-001`, deliverable 8: the run loop's transition table.
//!
//! `RunLoopState` is a public enum, so the compiler checks that `step` handles
//! every *variant*. It does not check the *ordering* — that `Act` is only ever
//! reached through `Approve`. Until M6 nothing did: `ActCtx` had all-public
//! fields, so a caller could build one carrying a tool call and hand it
//! straight to `step`, and the tool ran with no policy decision behind it.
//!
//! Two tests here, doing different jobs:
//!
//! - the transition table pins which state each state may move to, so a new
//!   edge has to be added here before it can be added to the loop; and
//! - the fabrication test states the security property the table cannot: that
//!   the only way into `Act` is through `Approve`.
//!
//! The second one has to be read alongside the compile-fail comment in it. A
//! test cannot demonstrate code that does not compile, so the demonstration is
//! the code that *does*: reaching `Act` requires driving the loop.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::audit::SafetyJournal;
use agentos_core::r#loop::{
    resume_approved, route, ApprovalBinding, ApprovalTicket, LoopDeps, Routed, RunError,
    RunLoopState, StartCtx, ToolGuardrailEntry,
};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Arc;

const TOOL: &str = "transition-tool";

/// The name each state reports, so the table reads as a table.
fn name(state: &RunLoopState) -> &'static str {
    match state {
        RunLoopState::Start(_) => "Start",
        RunLoopState::Plan(_) => "Plan",
        RunLoopState::Approve(_) => "Approve",
        RunLoopState::Act(_) => "Act",
        RunLoopState::Observe(_) => "Observe",
        RunLoopState::Paused(_) => "Paused",
        RunLoopState::Finish(_) => "Finish",
    }
}

fn call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("transition-call"),
        name: Arc::from(TOOL),
        args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
    }
}

/// Calls the tool once, then replies.
struct CallThenReply;

#[async_trait]
impl Orchestrator for CallThenReply {
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

struct NoopTool;

#[async_trait]
impl Tool for NoopTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(TOOL),
            description: Arc::from("transition table fixture"),
            input_schema: serde_json::json!({"type": "object"}),
            safety: Default::default(),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ok"),
            metadata: BTreeMap::new(),
        })
    }
}

fn user_state() -> RunState {
    let mut state = RunState::new(RunId::new("transitions"), AgentId::new("agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, "use the tool"),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

fn deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
    tool_guardrails: &'a [ToolGuardrailEntry<'a>],
) -> LoopDeps<'a> {
    LoopDeps {
        orchestrator,
        max_turns: 8,
        hooks: None,
        tools: Some(tools),
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
        run_journal: None,
        audit: SafetyJournal::detached().for_run(
            Principal::conversation(
                AgentId::new("agent"),
                ChannelId::new("test"),
                ConversationId::new("conv"),
            )
            .with_sender("user"),
            RunId::new("run"),
        ),
        delegated_authority: None,
    }
}

fn registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(NoopTool);
    tools
}

/// Drive from `Start` and record the state each step produced.
async fn walk(deps: &LoopDeps<'_>, steps: usize) -> Vec<&'static str> {
    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    let mut seen = vec![name(&current)];
    for _ in 0..steps {
        if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
            break;
        }
        current = current.step(deps).await.expect("the walk does not fail");
        seen.push(name(&current));
    }
    seen
}

#[tokio::test]
async fn the_allowed_path_visits_every_state_in_order() {
    // The full table for a run that calls one tool and then answers. Written
    // out rather than asserted piecewise: adding a transition to the loop
    // changes this literal, which is the point — the ordering is not
    // compiler-enforced, so it is enforced here.
    let orchestrator = CallThenReply;
    let policy = Policy::allow_tools([TOOL]);
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    assert_eq!(
        walk(&deps, 16).await,
        vec!["Start", "Plan", "Approve", "Act", "Observe", "Plan", "Finish",]
    );
}

#[tokio::test]
async fn an_ask_user_policy_stops_at_paused_and_resumes_into_act() {
    let orchestrator = CallThenReply;
    let policy = Policy::ask_user_tools([TOOL]);
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    assert_eq!(
        walk(&deps, 16).await,
        vec!["Start", "Plan", "Approve", "Paused"]
    );

    // And the pause resumes into `Act`, not back into `Approve`: the decision
    // has been made, and re-deciding it would ask twice.
    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    let paused = loop {
        current = current.step(&deps).await.expect("stepping to the pause");
        if let RunLoopState::Paused(state) = current {
            break state;
        }
    };
    let approval = paused.pending_approval().expect("pending approval");
    let ticket = ApprovalTicket::parse(&approval.approval_ticket).expect("ticket parses");
    let binding = ApprovalBinding::new(
        approval.approval_instance_id.clone(),
        ticket.clone(),
        approval.id.clone(),
        approval.prompting_principal.clone(),
        None,
    )
    .expect("instance matches ticket");
    let answer = Envelope {
        channel_id: ChannelId::new("test"),
        conversation_id: ConversationId::new("conv"),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, format!("/approve {ticket}")),
        metadata: BTreeMap::new(),
    };
    let Routed::Decides { witness } = route(Some(&binding), &answer) else {
        panic!("answer creates witness");
    };
    let resumed = resume_approved(paused, witness).expect("an approved pause resumes");
    assert_eq!(name(&resumed), "Act");
}

#[tokio::test]
async fn a_denied_tool_call_still_reaches_act_and_observe() {
    // Deliverable 4: a policy `deny` on a *tool call* is recoverable. It goes
    // through `Act` — which records a denied `ToolResult` rather than running
    // anything — so the model reads the refusal and replans. It is not an
    // error, and the run does not end.
    let orchestrator = CallThenReply;
    let policy = Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(TOOL)),
            decision: PolicyVerb::Deny,
            reason: Some(Arc::from("not in this deployment")),
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    };
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    assert_eq!(
        walk(&deps, 16).await,
        vec!["Start", "Plan", "Approve", "Act", "Observe", "Plan", "Finish",]
    );
}

#[tokio::test]
async fn a_denied_handoff_ends_the_run_as_a_structural_denial() {
    // The other half of deliverable 4. A handoff has no tool-call id to attach
    // a result to and is not something the model can retry around, so the
    // denial is fatal — and it is a *different* error from a human saying no.
    struct HandOff;

    #[async_trait]
    impl Orchestrator for HandOff {
        async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            Ok(Plan::Handoff(AgentId::new("elsewhere"), None))
        }
    }

    let orchestrator = HandOff;
    let policy = Policy::allow_tools([TOOL]);
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    let failure = loop {
        match current.step(&deps).await {
            Ok(next) => current = next,
            Err(failure) => break failure,
        }
    };
    assert!(
        matches!(
            failure.error(),
            RunError::StructuralDenial { subject, .. } if subject.as_ref() == "elsewhere"
        ),
        "expected a structural denial naming the handoff target, got {:?}",
        failure.error()
    );
}

#[tokio::test]
async fn act_cannot_be_reached_without_approve() {
    // The security property the table cannot state.
    //
    // `ActCtx`'s rustdoc carries the executable `compile_fail` half: its plan,
    // witness, and denied/executable disposition live in one private value, so
    // an external literal cannot fabricate or alter them. What remains
    // reachable is the loop itself, so this asserts the positive: an `Act`
    // obtained by driving the loop does execute, which is what makes that
    // closed door an ordering guarantee rather than a broken state.
    let orchestrator = CallThenReply;
    let policy = Policy::allow_tools([TOOL]);
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    loop {
        current = current.step(&deps).await.expect("the run succeeds");
        if let RunLoopState::Observe(ctx) = &current {
            assert!(
                ctx.state
                    .transcript
                    .items
                    .iter()
                    .any(|item| matches!(item.message.role, MessageRole::Tool)),
                "the tool ran, so the door reached through Approve is open"
            );
            return;
        }
    }
}

#[tokio::test]
async fn a_live_denial_after_approve_still_stops_act() {
    let orchestrator = CallThenReply;
    let allowing = Policy::allow_tools([TOOL]);
    let tools = registry();
    let allowing_deps = deps(&orchestrator, &allowing, &tools, &[]);
    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    let act = loop {
        current = current
            .step(&allowing_deps)
            .await
            .expect("the allowing policy reaches Act");
        if let RunLoopState::Act(ctx) = current {
            break ctx;
        }
    };

    let denying = Policy {
        rules: Vec::new(),
        default_decision: PolicyVerb::Deny,
    };
    let denying_deps = deps(&orchestrator, &denying, &tools, &[]);
    let failure = RunLoopState::Act(act)
        .step(&denying_deps)
        .await
        .expect_err("the live denial must stop the previously allowed plan");
    assert!(
        matches!(
            failure.error(),
            RunError::StructuralDenial { subject, .. } if subject.as_ref() == TOOL
        ),
        "expected a live-policy denial naming the tool, got {:?}",
        failure.error()
    );
}
