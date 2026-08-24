//! Roadmap G1: conversations are actors — concurrent across, serial within.
//!
//! `gateway::shard` and `gateway::inbox` unit-test their own contracts against
//! a fake handler. These drive the real runner through them, which is where the
//! two claims actually have to hold: a message arriving mid-run reaches the
//! model at its next `Plan`, and a conversation stuck in a slow tool does not
//! delay anyone else's turn.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::gateway::{run_shard, shard_set, ShardConfig, Turn, TurnHandler};
use agentos_core::memory::InMemorySession;
use agentos_core::r#loop::Steering;
use agentos_core::runner::{run_envelope, RunOutcome, RunnerDeps};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall, ToolCallId,
    ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const SLOW_TOOL: &str = "slow";

/// Input that answers without calling the slow tool.
const NO_TOOL: &str = "hello";

fn envelope_in(conversation: &str, text: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new("shard-channel"),
        conversation_id: ConversationId::new(conversation),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// Calls the tool once, then answers with the newest user message it can see.
///
/// That last part is the whole assertion vehicle: if steering works, the newest
/// user message on the second turn is the one that arrived *during* the tool
/// call, not the one that started the run.
struct AnswerNewestUserMessage;

#[async_trait]
impl Orchestrator for AnswerNewestUserMessage {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let called = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| item.message.role == MessageRole::Tool);
        // `NO_TOOL` opts a conversation out of the tool call, so a test can put
        // a fast conversation and a slow one on the same shard.
        let wants_tool = ctx
            .state
            .transcript
            .items
            .iter()
            .find(|item| item.message.role == MessageRole::User)
            .is_some_and(|item| item.message.content.as_ref() != NO_TOOL);
        if wants_tool && !called {
            return Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("call-1"),
                name: Arc::from(SLOW_TOOL),
                args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
            }));
        }
        let newest = ctx
            .state
            .transcript
            .items
            .iter()
            .rev()
            .find(|item| item.message.role == MessageRole::User)
            .map(|item| item.message.content.as_ref().to_owned())
            .unwrap_or_default();
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, newest)))
    }
}

/// Sleeps, and optionally drops a message into a steering queue first — which
/// is exactly what the gateway's router does when a user types again while a
/// tool is running.
struct SlowTool {
    steer: Option<(Steering, String)>,
    delay: Duration,
}

#[async_trait]
impl Tool for SlowTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(SLOW_TOOL),
            description: Arc::from("Sleeps."),
            input_schema: json!({"type": "object"}),
            safety: Default::default(),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        if let Some((steering, text)) = &self.steer {
            steering.push(Message::text(MessageRole::User, text.as_str()));
        }
        tokio::time::sleep(self.delay).await;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("tool done"),
            metadata: BTreeMap::new(),
        })
    }
}

fn allow(tools: &[&str]) -> Policy {
    Policy {
        rules: tools
            .iter()
            .map(|name| PolicyRule {
                action: PolicyAction::Tool(Arc::from(*name)),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::new(),
            })
            .collect(),
        default_decision: PolicyVerb::Deny,
    }
}

/// The item's verification criterion, through a real run: a second message
/// during a run is claimed at the next `Plan` rather than starting a second run.
#[tokio::test]
async fn mid_run_input_is_claimed_at_the_next_plan() {
    let steering = Steering::default();
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        steer: Some((steering.clone(), "actually, do the other thing".to_owned())),
        delay: Duration::from_millis(1),
    });

    let orchestrator = AnswerNewestUserMessage;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.steering = Some(steering.clone());

    let outcome = run_envelope(
        envelope_in("conv-a", "do the first thing"),
        RunId::new("g1-steer"),
        &deps,
    )
    .await
    .expect("the run finishes");

    let RunOutcome::Finished { output, state } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(
        output.message.content.as_ref(),
        "actually, do the other thing",
        "the second turn must plan against the message that arrived mid-run"
    );
    assert!(
        steering.is_empty(),
        "a claimed message must not be claimed twice"
    );
    // It is a real turn, not a side channel: it is in the transcript the run
    // writes back, so the next run sees it too.
    assert!(state
        .transcript
        .items
        .iter()
        .any(|item| item.message.content.as_ref() == "actually, do the other thing"));
}

/// The control: with no steering handle installed, the run is exactly what it
/// was before G1.
#[tokio::test]
async fn a_run_without_steering_answers_its_own_input() {
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        steer: None,
        delay: Duration::from_millis(1),
    });

    let orchestrator = AnswerNewestUserMessage;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);

    let outcome = run_envelope(
        envelope_in("conv-a", "do the first thing"),
        RunId::new("g1-plain"),
        &deps,
    )
    .await
    .expect("the run finishes");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(output.message.content.as_ref(), "do the first thing");
}

/// Runs real turns on the shard thread and records their replies in completion
/// order.
struct RunnerTurns<'a> {
    deps: &'a RunnerDeps<'a>,
    replies: Rc<RefCell<Vec<String>>>,
}

#[async_trait(?Send)]
impl TurnHandler for RunnerTurns<'_> {
    async fn run(&self, turn: Turn) {
        let conversation = turn.input.conversation_id.as_str().to_owned();
        let mut deps = RunnerDeps {
            cancel: turn.cancel,
            steering: Some(turn.steering),
            ..clone_deps(self.deps)
        };
        deps.active_agent = self.deps.active_agent.clone();
        let run = run_envelope(turn.input, RunId::new(format!("g1-{conversation}")), &deps).await;
        let reply = match run {
            Ok(RunOutcome::Finished { output, .. }) => output.message.content.as_ref().to_owned(),
            other => format!("unexpected outcome: {other:?}"),
        };
        self.replies
            .borrow_mut()
            .push(format!("{conversation}:{reply}"));
    }
}

/// `RunnerDeps` is a bundle of borrows, so re-deriving one per turn is a field
/// copy rather than a clone of anything owned.
fn clone_deps<'a>(deps: &RunnerDeps<'a>) -> RunnerDeps<'a> {
    RunnerDeps {
        orchestrator: deps.orchestrator,
        session: deps.session,
        memory_manager: deps.memory_manager,
        hooks: deps.hooks,
        max_turns: deps.max_turns,
        active_agent: deps.active_agent.clone(),
        tools: deps.tools,
        trace_sink: deps.trace_sink,
        task_workspace: deps.task_workspace,
        policy: deps.policy,
        subagents: deps.subagents,
        input_guardrails: deps.input_guardrails,
        output_guardrails: deps.output_guardrails,
        tool_guardrails: deps.tool_guardrails,
        stream_sink: deps.stream_sink.clone(),
        content_limits: deps.content_limits,
        compaction: deps.compaction,
        cancel: deps.cancel.clone(),
        steering: deps.steering.clone(),
        safety_log: deps.safety_log,
        delegated_authority: deps.delegated_authority,
    }
}

/// The item's exit condition, through real runs: one conversation's slow tool
/// does not delay another conversation's answer.
///
/// Both conversations are deliberately put on *one* shard. Spreading them over
/// two threads would prove nothing about the loop — it would only prove that
/// two threads run at once.
#[tokio::test(flavor = "current_thread")]
async fn one_slow_conversation_does_not_stall_another() {
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        steer: None,
        delay: Duration::from_millis(300),
    });

    let orchestrator = AnswerNewestUserMessage;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let replies = Rc::new(RefCell::new(Vec::new()));
    let handler = RunnerTurns {
        deps: &deps,
        replies: Rc::clone(&replies),
    };

    let config = ShardConfig {
        shards: 1,
        inbox_capacity: 8,
        idle_interval: Duration::from_secs(3600),
    };
    let (router, mut inbounds) = shard_set(&config);
    let inbound = inbounds.pop().expect("one shard");

    let driver = async {
        router
            .deliver(envelope_in("slow", "grind"))
            .await
            .expect("delivered");
        router
            .deliver(envelope_in("quick", "hello"))
            .await
            .expect("delivered");
        drop(router);
    };
    tokio::join!(run_shard(0, inbound, &config, &handler), driver);

    assert_eq!(
        replies.borrow().as_slice(),
        ["quick:hello", "slow:grind"],
        "the second conversation must not wait on the first one's tool"
    );
}
