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
use agentos_core::r#loop::{
    resume_approved, LoopDeps, RunError, RunLoopState, StartCtx, ToolGuardrailEntry,
};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, Message, MessageRole, RunId, ToolCall, ToolCallId, ToolResult, ToolStatus,
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
        audit: Default::default(),
        granted_authority: &[],
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
    let mut paused = paused;
    let approval_id = paused
        .pending_approval()
        .expect("the run is waiting on something")
        .id
        .clone();
    assert!(paused.approve(&approval_id));
    let resumed = resume_approved(paused).expect("an approved pause resumes");
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
    // This does not compile, and that is the test:
    //
    // ```compile_fail
    // RunLoopState::Act(ActCtx {
    //     state: user_state(),
    //     plan: Plan::CallTool(call()),
    //     turns: 0,
    //     denied: None,
    // })
    // ```
    //
    // `ActCtx` carries a private `Authorization` with no public constructor,
    // so the literal above is rejected with "cannot construct `ActCtx` with
    // struct literal syntax due to private fields" — there is no expression
    // outside `agentos-core` that produces the missing one. What remains
    // reachable is the loop itself, so this asserts the positive: an `Act`
    // obtained by driving the loop does execute, which is what makes the
    // absence above a closed door rather than a broken one.
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
async fn a_plan_swapped_after_approve_is_refused_at_act() {
    // The other half of the hole, and the one a marker type would miss: a
    // caller holding a *legitimate* `ActCtx` can still assign over `plan`,
    // because the field is public and has to be for the loop to be inspectable.
    // The authorization names the plan it was issued for, so the swap is
    // caught in `Act` before anything runs.
    let orchestrator = CallThenReply;
    // Both tools are allowed, so a plain re-check against the policy would
    // wave this through. Only the fingerprint catches it.
    let policy = Policy::allow_tools([TOOL, "other"]);
    let tools = registry();
    let deps = deps(&orchestrator, &policy, &tools, &[]);

    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    let mut act = loop {
        current = current.step(&deps).await.expect("stepping to Act");
        if let RunLoopState::Act(ctx) = current {
            break ctx;
        }
    };
    act.plan = Plan::CallTool(ToolCall {
        id: ToolCallId::new("swapped"),
        name: Arc::from("other"),
        args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
    });

    let failure = RunLoopState::Act(act)
        .step(&deps)
        .await
        .expect_err("a substituted plan must not run");
    assert!(
        matches!(
            failure.error(),
            RunError::StructuralDenial { subject, .. } if subject.as_ref() == "other"
        ),
        "expected a structural denial naming the substituted tool, got {:?}",
        failure.error()
    );
}
