//! A5 invariants for the run loop, exercised through the public API:
//!
//! - Adversarial max-turns: an orchestrator that plans `Plan::CallTool` on
//!   every turn must terminate through the budget-exhausted path after
//!   exactly `max_turns` planning turns — never loop forever, never abort
//!   with an opaque error.
//! - Paused `RunState` JSON round-trip: pausing at `ask_user`, persisting
//!   the state to JSON, restoring it in a "new process", approving, and
//!   resuming must finish the run with full trace continuity (this is also
//!   the regression gate for any future wire-format change to `RunState`).

use agentos_core::approve::Policy;
use agentos_core::r#loop::{resume_approved, LoopDeps, RunLoopState, StartCtx};
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const TOOL_NAME: &str = "mock";

fn user_state(content: &str) -> RunState {
    let mut state = RunState::new(RunId::new("invariant-run"), AgentId::new("invariant-agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, content),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

fn tool_call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("invariant-call"),
        name: Arc::from(TOOL_NAME),
        args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
    }
}

/// Plans `CallTool` on every single turn, forever.
struct AdversarialOrchestrator {
    plan_calls: AtomicUsize,
}

#[async_trait]
impl Orchestrator for AdversarialOrchestrator {
    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        self.plan_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Plan::CallTool(tool_call()))
    }
}

/// Plans one `CallTool`, then replies once the tool result is visible.
struct OneToolOrchestrator;

#[async_trait]
impl Orchestrator for OneToolOrchestrator {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let tool_result_seen = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| matches!(item.message.role, MessageRole::Tool));
        if tool_result_seen {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        } else {
            Ok(Plan::CallTool(tool_call()))
        }
    }
}

struct CountingTool {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for CountingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(TOOL_NAME),
            description: Arc::from("invariant test tool"),
            input_schema: serde_json::json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ok"),
            metadata: BTreeMap::new(),
        })
    }
}

fn deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
    max_turns: usize,
) -> LoopDeps<'a> {
    LoopDeps {
        orchestrator,
        max_turns,
        hooks: None,
        tools: Some(tools),
        task_workspace: None,
        policy,
        subagents: None,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: &[],
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel: Default::default(),
        steering: None,
    }
}

/// Step until a terminal state, bounding the number of `step()` calls so a
/// broken budget check fails the test instead of hanging it.
async fn drive_bounded(deps: &LoopDeps<'_>, state: RunState, max_steps: usize) -> RunLoopState {
    let mut current = RunLoopState::Start(StartCtx { state });
    for _ in 0..max_steps {
        current = current.step(deps).await.expect("loop step must not error");
        if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
            return current;
        }
    }
    panic!("loop did not reach a terminal state within {max_steps} steps");
}

#[tokio::test]
async fn adversarial_orchestrator_is_stopped_by_turn_budget() {
    const MAX_TURNS: usize = 3;
    let orchestrator = AdversarialOrchestrator {
        plan_calls: AtomicUsize::new(0),
    };
    let policy = Policy::allow_tools([TOOL_NAME]);
    let mut tools = ToolRegistry::new();
    let tool_calls = Arc::new(AtomicUsize::new(0));
    tools.register(CountingTool {
        calls: Arc::clone(&tool_calls),
    });

    let deps = deps(&orchestrator, &policy, &tools, MAX_TURNS);
    // Each turn is at most Plan → Approve → Act → Observe (4 steps) plus
    // Start and the terminal budget transition.
    let terminal = drive_bounded(&deps, user_state("go forever"), MAX_TURNS * 4 + 4).await;

    let RunLoopState::Finish(output) = terminal else {
        panic!("expected budget-exhausted finish, got {terminal:?}");
    };
    assert!(
        output.message.content.contains("step budget"),
        "terminal message must carry the truncation notice, got: {}",
        output.message.content
    );
    assert_eq!(
        output.message.metadata.get("run_truncated"),
        Some(&serde_json::Value::Bool(true)),
        "terminal message must be marked truncated"
    );
    // The budget is exact: the orchestrator planned once per allowed turn and
    // the tool ran once per allowed turn — never more.
    assert_eq!(orchestrator.plan_calls.load(Ordering::Relaxed), MAX_TURNS);
    assert_eq!(tool_calls.load(Ordering::Relaxed), MAX_TURNS);
}

#[tokio::test]
async fn paused_run_state_json_round_trips_and_resumes_with_trace_continuity() {
    let orchestrator = OneToolOrchestrator;
    let policy = Policy::ask_user_tools([TOOL_NAME]);
    let mut tools = ToolRegistry::new();
    tools.register(CountingTool {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let deps = deps(&orchestrator, &policy, &tools, 8);

    // Pause at the ask_user approval.
    let terminal = drive_bounded(&deps, user_state("please run the tool"), 16).await;
    let RunLoopState::Paused(paused) = terminal else {
        panic!("expected paused run, got {terminal:?}");
    };
    assert_eq!(paused.pending_approvals.len(), 1);

    // Persist to JSON and restore, as a gateway process restart would.
    let json = serde_json::to_string(&paused).expect("paused RunState serializes");
    let mut restored: RunState = serde_json::from_str(&json).expect("paused RunState deserializes");

    // Round-trip fidelity: the restored state is JSON-identical to the
    // paused one (full-struct comparison; RunState does not derive PartialEq).
    assert_eq!(
        serde_json::to_value(&paused).expect("paused state to value"),
        serde_json::to_value(&restored).expect("restored state to value"),
        "RunState must survive a JSON round-trip unchanged"
    );

    let pre_pause_spans = paused.trace_spans.clone();
    let pre_pause_events = paused.trace_events.clone();
    assert!(
        !pre_pause_spans.is_empty() && !pre_pause_events.is_empty(),
        "a paused run must already carry trace records"
    );

    // Approve and resume the restored state to completion.
    let approval_id = restored.pending_approvals[0].id.clone();
    assert!(restored.approve(&approval_id));
    let mut current = resume_approved(restored).expect("approved state resumes");
    let finished = loop {
        current = current
            .step(&deps)
            .await
            .expect("resume step must not error");
        if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
            break current;
        }
    };
    let RunLoopState::Finish(output) = finished else {
        panic!("resumed run must finish, got {finished:?}");
    };
    assert_eq!(output.message.content.as_ref(), "done");

    // Trace continuity: everything recorded before the pause is still
    // present, in order, at the front of the resumed trace...
    assert_eq!(
        &output.state.trace_spans[..pre_pause_spans.len()],
        &pre_pause_spans[..],
        "pre-pause spans must survive resume unchanged"
    );
    assert_eq!(
        &output.state.trace_events[..pre_pause_events.len()],
        &pre_pause_events[..],
        "pre-pause events must survive resume unchanged"
    );
    // ...the resumed run appended to the same trace...
    assert!(
        output.state.trace_events.len() > pre_pause_events.len(),
        "resume must append new trace events"
    );
    // ...and every event (old and new) resolves to a recorded span.
    for event in &output.state.trace_events {
        assert!(
            output
                .state
                .trace_spans
                .iter()
                .any(|span| span.id == event.span_id),
            "event '{}' references unknown span {:?}",
            event.name,
            event.span_id
        );
    }
    // Identity is preserved across the round-trip.
    assert_eq!(output.state.run_id.as_str(), "invariant-run");
    assert_eq!(output.state.active_agent.as_str(), "invariant-agent");
    assert!(output.state.pending_approvals.is_empty());
}
