//! Roadmap D3: long work becomes a job instead of failing.
//!
//! `jobs::registry`'s unit tests cover the registry's own contract. These cover
//! the two things that only appear in a whole run: a tool that outlives its
//! deadline is *promoted* rather than killed, and the job tools see exactly the
//! jobs their conversation owns.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::jobs::{JobRegistry, JobState};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_core::tools::{JobKillTool, JobOutputTool, JobStatusTool, ToolRegistry};
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SLOW_TOOL: &str = "slow";

/// The channel these envelopes arrive on. Named because the job registry
/// fences by *principal* now (M3 deliverable 2), so a test that wants to look
/// a job up has to name the same channel the run did.
const CHANNEL: &str = "jobs-channel";

/// An envelope in a named conversation. The job registry fences by principal,
/// and it reads the parts from the input item's metadata, so these tests need
/// to control them rather than take the shared default.
fn envelope_in(conversation: &str, text: &str) -> Envelope {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from("conversation_id"),
        Value::String(conversation.to_owned()),
    );
    metadata.insert(Arc::from("channel_id"), Value::String(CHANNEL.to_owned()));
    Envelope {
        channel_id: ChannelId::new(CHANNEL),
        conversation_id: ConversationId::new(conversation),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata,
    }
}

/// The principal a run in `conversation` fences its jobs by. `golden-agent` is
/// what `support::runner_deps` runs as.
fn owner(conversation: &str) -> Principal {
    Principal::conversation(
        AgentId::new("golden-agent"),
        ChannelId::new(CHANNEL),
        ConversationId::new(conversation),
    )
}

/// Calls the slow tool once, then reports what came back.
struct CallThenReport;

#[async_trait]
impl Orchestrator for CallThenReport {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let last = ctx.state.transcript.items.last();
        match last.map(|item| item.message.role.clone()) {
            Some(MessageRole::Tool) => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                last.expect("matched above").message.content.as_ref(),
            ))),
            _ => Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("call-1"),
                name: Arc::from(SLOW_TOOL),
                args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
            })),
        }
    }
}

/// A tool that outlives any sane deadline, and reports whether it is still
/// running after the turn that called it has ended.
struct SlowTool {
    finished: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for SlowTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(SLOW_TOOL),
            description: Arc::from("Takes far longer than its deadline."),
            input_schema: json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        tokio::time::sleep(Duration::from_millis(400)).await;
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("the slow work finished"),
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

/// The D3 exit condition: work that outruns its deadline is observable and
/// cancellable rather than lost.
#[tokio::test]
async fn a_tool_past_its_deadline_becomes_a_job_that_keeps_running() {
    let jobs = Arc::new(JobRegistry::default());
    let finished = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        finished: finished.clone(),
    });
    tools.register(JobStatusTool::new(jobs.clone()));
    tools.register(JobOutputTool::new(jobs.clone()));
    let tools = tools
        // Far shorter than the tool takes, so promotion is the only outcome.
        .with_timeouts(Duration::from_millis(60), BTreeMap::new())
        .with_jobs(jobs.clone(), [Arc::from(SLOW_TOOL)]);

    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = runner_deps_for(&orchestrator, &session, &policy, &tools);

    let outcome = run_envelope(
        envelope_in("conv-a", "Do the slow thing."),
        RunId::new("d3-promote"),
        &deps,
    )
    .await
    .expect("promotion is not a failure");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    // The model was told where the work went, not that it failed.
    assert!(
        output.message.content.contains("background job"),
        "expected a promotion notice, got: {}",
        output.message.content
    );
    assert_eq!(
        finished.load(Ordering::Relaxed),
        0,
        "the tool cannot have finished inside a 60 ms deadline"
    );

    // The run is over and the job is still going: that is the whole item.
    let conversation = owner("conv-a");
    let running = jobs.list(&conversation);
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].state, JobState::Running);

    // And it does finish, after the run that started it has returned.
    for _ in 0..100 {
        if jobs.list(&conversation)[0].state == JobState::Succeeded {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(jobs.list(&conversation)[0].state, JobState::Succeeded);
    assert_eq!(finished.load(Ordering::Relaxed), 1);
    assert_eq!(
        jobs.output(&conversation, &running[0].id, 0)
            .expect("owned"),
        "the slow work finished"
    );
}

/// A tool that beats its deadline is never revealed to have been a job.
#[tokio::test]
async fn a_promotable_tool_that_finishes_in_time_looks_like_an_ordinary_call() {
    let jobs = Arc::new(JobRegistry::default());
    let finished = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        finished: finished.clone(),
    });
    let tools = tools
        .with_timeouts(Duration::from_secs(10), BTreeMap::new())
        .with_jobs(jobs.clone(), [Arc::from(SLOW_TOOL)]);

    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = runner_deps_for(&orchestrator, &session, &policy, &tools);

    let outcome = run_envelope(
        envelope_in("conv-a", "Do the slow thing."),
        RunId::new("d3-inline"),
        &deps,
    )
    .await
    .expect("the call finishes normally");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    assert_eq!(output.message.content.as_ref(), "the slow work finished");
    assert!(!output.message.content.contains("background job"));
}

/// Owner fencing, through the model-facing tools rather than the registry API.
#[tokio::test]
async fn one_conversations_job_is_invisible_to_another() {
    let jobs = Arc::new(JobRegistry::default());
    let finished = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        finished: finished.clone(),
    });
    tools.register(JobStatusTool::new(jobs.clone()));
    tools.register(JobKillTool::new(jobs.clone()));
    let tools = tools
        .with_timeouts(Duration::from_millis(60), BTreeMap::new())
        .with_jobs(jobs.clone(), [Arc::from(SLOW_TOOL)]);

    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = runner_deps_for(&orchestrator, &session, &policy, &tools);

    run_envelope(
        envelope_in("conv-a", "Do the slow thing."),
        RunId::new("d3-fence-a"),
        &deps,
    )
    .await
    .expect("promotion is not a failure");

    let owner_principal = owner("conv-a");
    let intruder = owner("conv-b");
    let job = jobs
        .list(&owner_principal)
        .first()
        .expect("conv-a owns a job")
        .clone();

    // Naming the id from the other conversation reaches nothing, through every
    // door the registry has.
    assert!(jobs.status(&intruder, &job.id).is_err());
    assert!(jobs.output(&intruder, &job.id, 0).is_err());
    assert!(jobs.kill(&intruder, &job.id).is_err());
    assert!(jobs.list(&intruder).is_empty());
    assert!(jobs.status(&owner_principal, &job.id).is_ok());
}

/// A conversation that runs out of job slots keeps working: the call runs
/// inline under its deadline, which is D2's behaviour.
#[tokio::test]
async fn a_conversation_out_of_job_slots_degrades_to_the_deadline() {
    let jobs = Arc::new(JobRegistry::new(1, 64 * 1024));
    let finished = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(SlowTool {
        finished: finished.clone(),
    });
    let tools = tools
        .with_timeouts(Duration::from_millis(60), BTreeMap::new())
        .with_jobs(jobs.clone(), [Arc::from(SLOW_TOOL)]);

    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = allow(&[SLOW_TOOL]);
    let deps = runner_deps_for(&orchestrator, &session, &policy, &tools);

    // First run takes the only slot.
    run_envelope(envelope_in("conv-a", "one"), RunId::new("d3-slot-1"), &deps)
        .await
        .expect("promoted");

    // Second finds none, and is bounded by the deadline instead of refused.
    let outcome = run_envelope(envelope_in("conv-a", "two"), RunId::new("d3-slot-2"), &deps)
        .await
        .expect("a full job table must not fail the run");

    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("the run should finish");
    };
    assert!(
        output.message.content.contains("did not finish"),
        "expected D2's deadline result, got: {}",
        output.message.content
    );
    assert_eq!(jobs.list(&owner("conv-a")).len(), 1);
}

fn runner_deps_for<'a>(
    orchestrator: &'a dyn Orchestrator,
    session: &'a InMemorySession,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
) -> agentos_core::runner::RunnerDeps<'a> {
    support::runner_deps(orchestrator, session, policy, Some(tools), None)
}
