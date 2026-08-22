//! Roadmap X1: a response asking for several tools loses none of them.
//!
//! `orchestrator/max.rs` used to take `tool_calls.first()` and drop the rest,
//! while the Anthropic and DeepSeek adapters parsed the whole vector — so those
//! calls were paid for and discarded. These pin the replacement: every call
//! runs, in order, and each one crosses `Approve` on its own.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::Session;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall, ToolCallId,
    ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const RECORDER: &str = "recorder";
const CONVERSATION: &str = "batch-conv";

fn envelope(text: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new("batches"),
        conversation_id: ConversationId::new(CONVERSATION),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

fn call(id: &str, arg: &str) -> ToolCall {
    ToolCall {
        id: ToolCallId::new(id),
        name: Arc::from(RECORDER),
        args: RawValue::from_string(json!({ "arg": arg }).to_string())
            .expect("test args are valid JSON"),
    }
}

/// Plans one batch, then answers with everything it saw come back.
struct BatchThenReport {
    batch: Mutex<Option<Vec<ToolCall>>>,
}

impl BatchThenReport {
    fn new(calls: Vec<ToolCall>) -> Self {
        Self {
            batch: Mutex::new(Some(calls)),
        }
    }
}

#[async_trait]
impl Orchestrator for BatchThenReport {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        if let Some(calls) = self.batch.lock().expect("not poisoned").take() {
            return Ok(Plan::CallTools(calls));
        }
        let seen: Vec<&str> = ctx
            .state
            .transcript
            .items
            .iter()
            .filter(|item| item.message.role == MessageRole::Tool)
            .map(|item| item.message.content.as_ref())
            .collect();
        Ok(Plan::Reply(Message::text(
            MessageRole::Assistant,
            seen.join(","),
        )))
    }
}

/// Records the order it was called in, so a test can assert results append in
/// the order the model asked for them.
struct Recorder {
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Tool for Recorder {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(RECORDER),
            description: Arc::from("Records that it ran."),
            input_schema: json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let arg = serde_json::from_str::<Value>(args.get())
            .ok()
            .and_then(|value| value.get("arg").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default();
        self.calls.lock().expect("not poisoned").push(arg.clone());
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(arg),
            metadata: BTreeMap::new(),
        })
    }
}

fn policy(verb: PolicyVerb) -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(RECORDER)),
            decision: verb,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    }
}

/// The item's exit condition: extra tool calls are never silently dropped.
#[tokio::test]
async fn every_call_in_a_batch_runs_in_order() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![
        call("c1", "first"),
        call("c2", "second"),
        call("c3", "third"),
    ]);
    let session = InMemorySession::default();
    let allow = policy(PolicyVerb::Allow);
    let deps = support::runner_deps(&orchestrator, &session, &allow, Some(&tools), None);

    let outcome = run_envelope(envelope("do three things"), RunId::new("x1-batch"), &deps)
        .await
        .expect("a batch runs cleanly");

    let RunOutcome::Finished { output, state } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(
        ran.lock().expect("not poisoned").as_slice(),
        ["first", "second", "third"],
        "every call must run, in the order the model asked for them"
    );
    // And the model sees all three results, in the same order.
    assert_eq!(output.message.content.as_ref(), "first,second,third");
    assert!(
        state.queued_tool_calls.is_empty(),
        "a drained batch leaves nothing queued"
    );
}

/// Each queued call is recorded as its own assistant turn followed by its own
/// result. That is not cosmetic: every provider rejects a tool result whose
/// call has no preceding assistant turn, and it means no provider ever sees a
/// half-answered batch.
#[tokio::test]
async fn a_batch_is_written_back_as_paired_turns() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![call("c1", "first"), call("c2", "second")]);
    let session = InMemorySession::default();
    let allow = policy(PolicyVerb::Allow);
    let deps = support::runner_deps(&orchestrator, &session, &allow, Some(&tools), None);

    run_envelope(envelope("do two things"), RunId::new("x1-pairs"), &deps)
        .await
        .expect("a batch runs cleanly");

    let transcript = session
        .load(&ConversationId::new(CONVERSATION))
        .await
        .expect("session loads");
    let shape: Vec<(String, usize)> = transcript
        .items
        .iter()
        .map(|item| {
            (
                format!("{:?}", item.message.role).to_lowercase(),
                item.message.tool_calls.len(),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![
            ("user".to_owned(), 0),
            ("assistant".to_owned(), 1),
            ("tool".to_owned(), 0),
            ("assistant".to_owned(), 1),
            ("tool".to_owned(), 0),
            ("assistant".to_owned(), 0),
        ],
        "each call gets its own assistant turn and its own result"
    );
}

/// Every call crosses the policy engine on its own. A blanket denial denies all
/// of them rather than one — and the run survives, because a denial is a result
/// the model can read.
#[tokio::test]
async fn every_call_in_a_batch_crosses_approve() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![call("c1", "first"), call("c2", "second")]);
    let session = InMemorySession::default();
    let deny = Policy {
        rules: Vec::new(),
        default_decision: PolicyVerb::Deny,
    };
    let deps = support::runner_deps(&orchestrator, &session, &deny, Some(&tools), None);

    let outcome = run_envelope(envelope("do two things"), RunId::new("x1-deny"), &deps)
        .await
        .expect("denials are results, not run failures");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    assert!(
        ran.lock().expect("not poisoned").is_empty(),
        "a denied batch must run nothing"
    );
    // Two denials came back, one per call — not one denial for the batch.
    assert_eq!(output.message.content.matches("not allowed").count(), 2);
}

/// A batch containing a gated call pauses on that call, and what is left of the
/// batch survives being serialized with the paused run.
#[tokio::test]
async fn a_batch_pausing_for_approval_keeps_the_rest_queued() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![call("c1", "first"), call("c2", "second")]);
    let session = InMemorySession::default();
    let ask = policy(PolicyVerb::AskUser);
    let deps = support::runner_deps(&orchestrator, &session, &ask, Some(&tools), None);

    let outcome = run_envelope(envelope("do two things"), RunId::new("x1-pause"), &deps)
        .await
        .expect("a gated batch pauses cleanly");

    let RunOutcome::Paused(state) = outcome else {
        panic!("a gated call must pause");
    };
    assert_eq!(
        state.approvals.iter().filter(|a| a.is_pending()).count(),
        1,
        "one call is asked about at a time"
    );
    assert_eq!(
        state
            .queued_tool_calls
            .iter()
            .map(|call| call.id.as_str())
            .collect::<Vec<_>>(),
        vec!["c2"],
        "the rest of the batch waits on the paused run"
    );
    // The queue is part of the state, so it survives the trip to disk a paused
    // run makes.
    let json = serde_json::to_string(&state).expect("a paused run serializes");
    let restored: agentos_interfaces::RunState =
        serde_json::from_str(&json).expect("and deserializes");
    assert_eq!(restored.queued_tool_calls.len(), 1);
    assert!(ran.lock().expect("not poisoned").is_empty());
}

/// A single-call response still plans as an ordinary `CallTool`, so nothing
/// about the common path changed.
#[tokio::test]
async fn one_call_is_still_a_plain_tool_call() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![call("c1", "only")]);
    let session = InMemorySession::default();
    let allow = policy(PolicyVerb::Allow);
    let deps = support::runner_deps(&orchestrator, &session, &allow, Some(&tools), None);

    let outcome = run_envelope(envelope("do one thing"), RunId::new("x1-single"), &deps)
        .await
        .expect("a single call runs cleanly");

    let RunOutcome::Finished { output, state } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(output.message.content.as_ref(), "only");
    assert!(state.queued_tool_calls.is_empty());
    // No batch was ever recorded, because there was no batch.
    assert!(!state
        .trace_events
        .iter()
        .any(|event| event.name.as_ref() == "tool_batch_planned"));
}

/// The trace says a batch happened and how much of it is left, so a run that
/// stops mid-batch is readable after the fact.
#[tokio::test]
async fn the_trace_records_the_batch_and_what_remains() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let mut tools = ToolRegistry::new();
    tools.register(Recorder { calls: ran.clone() });
    let orchestrator = BatchThenReport::new(vec![
        call("c1", "first"),
        call("c2", "second"),
        call("c3", "third"),
    ]);
    let session = InMemorySession::default();
    let allow = policy(PolicyVerb::Allow);
    let deps = support::runner_deps(&orchestrator, &session, &allow, Some(&tools), None);

    let outcome = run_envelope(envelope("do three things"), RunId::new("x1-trace"), &deps)
        .await
        .expect("a batch runs cleanly");
    let RunOutcome::Finished { state, .. } = outcome else {
        panic!("the run should finish");
    };

    let planned: Vec<&_> = state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref() == "tool_batch_planned")
        .collect();
    assert_eq!(planned.len(), 1);
    assert_eq!(
        planned[0].fields.get("tool_calls").and_then(Value::as_u64),
        Some(3)
    );

    let remaining: Vec<Option<u64>> = state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref() == "tool_batch_claimed")
        .map(|event| event.fields.get("remaining").and_then(Value::as_u64))
        .collect();
    assert_eq!(
        remaining,
        vec![Some(1), Some(0)],
        "one claim per queued call, counting down"
    );
}
