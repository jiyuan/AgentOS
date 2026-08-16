//! Roadmap D1: stopping a run in flight.
//!
//! The unit tests in `loop::cancel` cover the race primitive and the parent /
//! child token contract. These cover what only shows up in a whole run: a
//! cancelled run stops at the next boundary, keeps the work it had already
//! done, and takes its sub-agents down with it.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_interfaces::session::Session;
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ConversationId, Message, MessageRole, RunId, ToolCall, ToolCallId, ToolResult,
    ToolStatus,
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use support::{runner_deps, user_envelope, CONVERSATION};

const SLOW_TOOL: &str = "slow";

/// An orchestrator that keeps calling the tool, so a run only ends when
/// something stops it.
struct ToolLoop;

#[async_trait]
impl Orchestrator for ToolLoop {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let calls = ctx
            .state
            .transcript
            .items
            .iter()
            .filter(|item| item.message.role == MessageRole::Tool)
            .count();
        if calls >= 3 {
            return Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                "done on my own",
            )));
        }
        Ok(Plan::CallTool(ToolCall {
            id: ToolCallId::new(format!("call-{calls}")),
            name: Arc::from(SLOW_TOOL),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        }))
    }
}

/// A tool that awaits rather than blocking, and cancels the run on the call
/// the test names. Awaiting is what makes it interruptible at all — see
/// `loop::cancel`'s module docs.
struct SlowTool {
    calls: Arc<AtomicUsize>,
    cancel_on_call: usize,
    cancel: CancellationToken,
}

#[async_trait]
impl Tool for SlowTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(SLOW_TOOL),
            description: Arc::from("A tool that takes its time."),
            input_schema: serde_json::json!({"type": "object"}),
            requires_isolation: false,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        let index = self.calls.fetch_add(1, Ordering::Relaxed);
        if index == self.cancel_on_call {
            // Cancel from inside the call, then yield: the loop is awaiting
            // this future, so it can only observe the token once we suspend.
            self.cancel.cancel();
            tokio::task::yield_now().await;
            std::future::pending::<()>().await;
        }
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("tool output"),
            metadata: BTreeMap::new(),
        })
    }
}

fn allow_all() -> Policy {
    Policy {
        rules: vec![
            PolicyRule {
                action: PolicyAction::Tool(Arc::from(SLOW_TOOL)),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::new(),
            },
            PolicyRule {
                action: PolicyAction::Delegate,
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::new(),
            },
        ],
        default_decision: PolicyVerb::Deny,
    }
}

/// The D1 exit case: a run cancelled mid-tool stops, answers, and keeps what
/// it had already produced.
#[tokio::test]
async fn cancelling_mid_tool_stops_the_run_and_keeps_its_work() {
    let cancel = CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        calls: calls.clone(),
        // Let the first call complete so there is real work to preserve, then
        // cancel inside the second.
        cancel_on_call: 1,
        cancel: cancel.clone(),
    });
    let orchestrator = ToolLoop;
    let session = InMemorySession::default();
    let policy = allow_all();
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.cancel = cancel.clone();

    let outcome = run_envelope(
        user_envelope("Work through this."),
        RunId::new("d1-mid-tool"),
        &deps,
    )
    .await
    .expect("a cancelled run finishes rather than failing");

    let RunOutcome::Finished { state, output } = outcome else {
        panic!("a cancelled run should finish");
    };
    assert!(output.message.content.starts_with("Stopped."));
    assert_ne!(output.message.content.as_ref(), "done on my own");
    assert_eq!(calls.load(Ordering::Relaxed), 2, "no third tool call");
    assert!(state
        .trace_events
        .iter()
        .any(|event| event.name.as_ref() == "run_cancelled"));

    // The first tool's result survived: the run finished rather than erroring,
    // so the runner appended its new items to the session.
    let transcript = session
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");
    assert!(
        transcript
            .items
            .iter()
            .any(|item| item.message.content.contains("tool output")),
        "work done before the stop must be kept: {:?}",
        transcript
            .items
            .iter()
            .map(|item| item.message.content.as_ref())
            .collect::<Vec<_>>()
    );
}

/// Cancelling before the run starts costs nothing: no plan, no tool call.
#[tokio::test]
async fn a_run_cancelled_before_it_starts_does_no_work() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        calls: calls.clone(),
        cancel_on_call: usize::MAX,
        cancel: cancel.clone(),
    });
    let orchestrator = ToolLoop;
    let session = InMemorySession::default();
    let policy = allow_all();
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.cancel = cancel;

    let outcome = run_envelope(
        user_envelope("Work through this."),
        RunId::new("d1-pre-cancelled"),
        &deps,
    )
    .await
    .expect("an already-cancelled run finishes immediately");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("a cancelled run should finish");
    };
    assert!(output.message.content.starts_with("Stopped."));
    assert_eq!(calls.load(Ordering::Relaxed), 0, "no tool ran");
}

/// A default token never fires, so a caller that ignores cancellation sees
/// exactly the pre-D1 behaviour.
#[tokio::test]
async fn a_run_with_the_default_token_is_unaffected() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        calls: calls.clone(),
        cancel_on_call: usize::MAX,
        cancel: CancellationToken::new(),
    });
    let orchestrator = ToolLoop;
    let session = InMemorySession::default();
    let policy = allow_all();
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let outcome = run_envelope(
        user_envelope("Work through this."),
        RunId::new("d1-uncancelled"),
        &deps,
    )
    .await
    .expect("an uncancelled run finishes normally");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("expected a normal finish");
    };
    assert_eq!(output.message.content.as_ref(), "done on my own");
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

// ---------------------------------------------------------------------------
// Delegation
// ---------------------------------------------------------------------------

/// Parent: delegates once, then reports what came back.
struct DelegatingParent;

#[async_trait]
impl Orchestrator for DelegatingParent {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let delegated = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| item.message.role == MessageRole::Tool);
        if delegated {
            return Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                "child came back",
            )));
        }
        Ok(Plan::Delegate(SubAgentSpec {
            agent_id: AgentId::new("child"),
            policy_id: Arc::from("child-policy"),
            metadata: BTreeMap::new(),
        }))
    }
}

/// The delegation contract: cancelling a parent stops its sub-agent tree.
#[tokio::test]
async fn cancelling_a_parent_cancels_its_subagent() {
    let cancel = CancellationToken::new();
    let child_calls = Arc::new(AtomicUsize::new(0));

    let mut child_tools = ToolRegistry::new();
    child_tools.register(SlowTool {
        calls: child_calls.clone(),
        // The child's very first tool call cancels the *parent's* token.
        cancel_on_call: 0,
        cancel: cancel.clone(),
    });
    let child_session = Arc::new(InMemorySession::default());
    let mut subagents = SubAgentRegistry::new().with_session(child_session);
    subagents.register(
        SubAgentDefinition::new(
            AgentId::new("child"),
            "child-policy",
            Arc::new(ToolLoop),
            allow_all(),
        )
        .with_tools(Arc::new(child_tools))
        .with_max_turns(6),
    );

    let orchestrator = DelegatingParent;
    let session = InMemorySession::default();
    let policy = allow_all();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, Some(&subagents));
    deps.cancel = cancel.clone();

    let outcome = run_envelope(
        user_envelope("Delegate this."),
        RunId::new("d1-delegation"),
        &deps,
    )
    .await
    .expect("a cancelled delegation finishes rather than failing");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("expected a finish");
    };
    // The child stopped where the parent's token reached it, and the parent
    // stopped rather than continuing on a truncated child result.
    assert_eq!(
        child_calls.load(Ordering::Relaxed),
        1,
        "the child must not keep working after the parent was cancelled"
    );
    assert!(output.message.content.starts_with("Stopped."));
}
