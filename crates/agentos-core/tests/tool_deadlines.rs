//! Roadmap D2: a tool cannot occupy a worker thread indefinitely.
//!
//! The unit tests in `tools::exec` and `tools::registry` cover the executor and
//! the deadline resolution. This covers the consequence that only shows up in a
//! whole run: an overrunning tool produces a *failed result the model reads and
//! replans around*, not an error that kills the run and every parent above it.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{Message, MessageRole, RunId, ToolCall, ToolCallId, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde_json::{json, value::RawValue};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use support::{runner_deps, user_envelope};

const HANG_TOOL: &str = "hang";

/// Calls the tool once, then reports whatever came back — so the reply proves
/// the model saw the tool's result rather than the run dying on it.
struct CallThenReport;

#[async_trait]
impl Orchestrator for CallThenReport {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let last = ctx.state.transcript.items.last();
        match last.map(|item| item.message.role.clone()) {
            Some(MessageRole::Tool) => {
                let seen = last.expect("matched above").message.content.clone();
                Ok(Plan::Reply(Message::text(
                    MessageRole::Assistant,
                    format!("the tool said: {seen}"),
                )))
            }
            _ => Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("call-1"),
                name: Arc::from(HANG_TOOL),
                args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
            })),
        }
    }
}

/// A tool that never returns. Only a deadline can end a call to it.
struct HangTool {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for HangTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(HANG_TOOL),
            description: Arc::from("Never returns."),
            input_schema: json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::future::pending().await
    }
}

fn allow_hang() -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(HANG_TOOL)),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
        delegation_grants: Vec::new(),
    }
}

/// The D2 exit condition, end to end.
#[tokio::test]
async fn an_overrunning_tool_fails_its_call_rather_than_the_run() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(HangTool {
        calls: calls.clone(),
    });
    let tools = tools.with_timeouts(Duration::from_millis(150), BTreeMap::new());
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow_hang();
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let started = Instant::now();
    let outcome = run_envelope(
        user_envelope("Use the tool."),
        RunId::new("d2-deadline"),
        &deps,
    )
    .await
    .expect("an overrunning tool must not fail the run");

    let RunOutcome::Finished { state, output } = outcome else {
        panic!("the run should finish");
    };

    // The run was bounded by the deadline, not by the tool's willingness to
    // return — which is never.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the run took {:?}",
        started.elapsed()
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    // The model saw the failure and replanned around it, which is the whole
    // reason a deadline is a failed result and not an error.
    assert!(
        output.message.content.contains("did not finish"),
        "the model should have read the timeout: {}",
        output.message.content
    );

    // And the transcript records it as a failed tool result the next turn can
    // reason about, not as a missing turn.
    let timed_out = state
        .transcript
        .items
        .iter()
        .find(|item| item.message.role == MessageRole::Tool)
        .expect("the run recorded a tool result");
    assert_eq!(
        timed_out.message.metadata.get("tool_status"),
        Some(&serde_json::Value::String("failed".to_owned()))
    );
}

/// A tool that finishes inside its deadline is untouched — the pre-D2 path.
#[tokio::test]
async fn a_prompt_tool_is_unaffected_by_the_deadline() {
    struct Prompt;

    #[async_trait]
    impl Tool for Prompt {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: Arc::from(HANG_TOOL),
                description: Arc::from("Returns at once."),
                input_schema: json!({"type": "object"}),
                sandbox: SandboxMode::FullAccess,
                timeout_ms: None,
            }
        }

        async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Succeeded,
                content: Arc::from("prompt output"),
                metadata: BTreeMap::new(),
            })
        }
    }

    let mut tools = ToolRegistry::new();
    tools.register(Prompt);
    let tools = tools.with_timeouts(Duration::from_millis(5_000), BTreeMap::new());
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow_hang();
    let deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let outcome = run_envelope(
        user_envelope("Use the tool."),
        RunId::new("d2-prompt"),
        &deps,
    )
    .await
    .expect("a prompt tool finishes normally");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(
        output.message.content.as_ref(),
        "the tool said: prompt output"
    );
}
