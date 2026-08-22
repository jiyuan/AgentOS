//! M6 / `STATE-001`, deliverables 6 and 7: nothing leaves without the output
//! policy seeing it, and streaming is the one exception a deployment must ask
//! for.
//!
//! [ADR-0007](../../../docs/adr/0007-BUFFERED_OUTPUT.md). The tests come in a
//! pair on purpose: the second one shows a violation *does* escape in
//! provisional mode, so the guarantee the first one asserts is a real
//! difference rather than an assumption about a code path nobody exercises.

mod support;

use agentos_core::approve::Policy;
use agentos_core::r#loop::{
    LoopDeps, OutputGuardrailEntry, RunError, RunLoopState, StartCtx, StepFailure,
};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::guardrail::{GuardrailError, GuardrailOutcome, OutputGuardrail};
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, StreamSink,
};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::RunState;
use agentos_proto::{AgentId, Message, MessageRole, RunId};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// The string the seeded guardrail refuses. Planted in the reply so a test can
/// look for it in whatever the sink received.
const FORBIDDEN: &str = "the-thing-that-must-not-be-said";

/// Streams the forbidden reply chunk by chunk, exactly as a provider-backed
/// orchestrator would, then returns it as the plan.
struct LeakyOrchestrator;

#[async_trait]
impl Orchestrator for LeakyOrchestrator {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let reply = format!("here it is: {FORBIDDEN}");
        for chunk in reply.split_inclusive(' ') {
            ctx.emit_stream_delta(chunk).await;
        }
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, reply)))
    }
}

/// Refuses anything carrying [`FORBIDDEN`].
struct RefuseTheForbidden;

#[async_trait]
impl OutputGuardrail for RefuseTheForbidden {
    async fn check(
        &self,
        output: &Message,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        if output.content.contains(FORBIDDEN) {
            Ok(GuardrailOutcome::Tripped(Arc::from(
                "the output policy refuses that",
            )))
        } else {
            Ok(GuardrailOutcome::Passed)
        }
    }
}

/// Records every byte a sink is handed, which is the definition of
/// "user-visible" for this test: past the sink there is a terminal or a chat
/// API, and neither can be asked to give the bytes back.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<String>>);

impl Recorder {
    fn sink(&self) -> StreamSink {
        let recorded = Arc::clone(&self.0);
        Arc::new(move |delta: &str| {
            if let Ok(mut guard) = recorded.lock() {
                guard.push_str(delta);
            }
            Box::pin(std::future::ready(()))
        })
    }

    fn seen(&self) -> String {
        self.0.lock().map(|guard| guard.clone()).unwrap_or_default()
    }
}

fn user_state() -> RunState {
    let mut state = RunState::new(RunId::new("output-gate"), AgentId::new("agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, "say the thing"),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

fn deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    policy: &'a Policy,
    tools: &'a ToolRegistry,
    output_guardrails: &'a [OutputGuardrailEntry<'a>],
    stream_sink: Option<StreamSink>,
) -> LoopDeps<'a> {
    LoopDeps {
        orchestrator,
        max_turns: 4,
        hooks: None,
        tools: Some(tools),
        task_workspace: None,
        policy,
        subagents: None,
        input_guardrails: &[],
        output_guardrails,
        tool_guardrails: &[],
        stream_sink,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel: CancellationToken::new(),
        steering: None,
        audit: Default::default(),
        granted_authority: &[],
    }
}

async fn drive(deps: &LoopDeps<'_>) -> Result<RunLoopState, StepFailure> {
    let mut current = RunLoopState::Start(StartCtx {
        state: user_state(),
    });
    loop {
        current = current.step(deps).await?;
        if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
            return Ok(current);
        }
    }
}

#[tokio::test]
async fn a_seeded_violation_emits_zero_bytes_in_stable_mode() {
    let orchestrator = LeakyOrchestrator;
    let policy = Policy::default();
    let tools = ToolRegistry::new();
    let guardrails = [OutputGuardrailEntry {
        name: Arc::from("RefuseTheForbidden"),
        guardrail: &RefuseTheForbidden,
    }];
    let recorder = Recorder::default();
    // Stable mode: no sink installed, which is what `[channels]
    // provisional_streaming = false` produces at every entrypoint.
    let deps = deps(&orchestrator, &policy, &tools, &guardrails, None);

    let failure = drive(&deps)
        .await
        .expect_err("a refused reply fails the run");
    assert!(matches!(failure.error(), RunError::GuardrailTripped { .. }));
    assert_eq!(
        recorder.seen(),
        "",
        "zero user-visible bytes: the check ran before anything was sent"
    );
}

#[tokio::test]
async fn the_same_violation_does_escape_in_provisional_mode() {
    // The other half, and the reason the first test means something. If this
    // one also saw nothing, the guarantee above would be an artifact of a
    // fixture that never streams rather than a property of the default.
    let orchestrator = LeakyOrchestrator;
    let policy = Policy::default();
    let tools = ToolRegistry::new();
    let guardrails = [OutputGuardrailEntry {
        name: Arc::from("RefuseTheForbidden"),
        guardrail: &RefuseTheForbidden,
    }];
    let recorder = Recorder::default();
    let deps = deps(
        &orchestrator,
        &policy,
        &tools,
        &guardrails,
        Some(recorder.sink()),
    );

    let failure = drive(&deps)
        .await
        .expect_err("a refused reply still fails the run");
    assert!(matches!(failure.error(), RunError::GuardrailTripped { .. }));
    assert!(
        recorder.seen().contains(FORBIDDEN),
        "provisional streaming forwards before the check; the run failing afterwards \
         does not unsay it. Recorded: {:?}",
        recorder.seen()
    );
}

/// Refuses everything, so even a fixed notice the loop wrote itself is
/// withheld.
struct RefuseEverything;

#[async_trait]
impl OutputGuardrail for RefuseEverything {
    async fn check(
        &self,
        _output: &Message,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        Ok(GuardrailOutcome::Tripped(Arc::from("nothing leaves here")))
    }
}

/// Plans forever, so the run reaches the cancellation check rather than
/// finishing.
struct NeverFinishes;

#[async_trait]
impl Orchestrator for NeverFinishes {
    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, "hello")))
    }
}

#[tokio::test]
async fn a_cancelled_run_routes_its_notice_through_the_output_policy() {
    // Deliverable 6. Cancellation used to emit a constant without the policy
    // seeing it, on the argument that a constant carries no model content —
    // true of that constant, and not the property the invariant needs. A
    // deployment's output policy is a statement about everything that leaves.
    let orchestrator = NeverFinishes;
    let policy = Policy::default();
    let tools = ToolRegistry::new();
    let guardrails = [OutputGuardrailEntry {
        name: Arc::from("RefuseEverything"),
        guardrail: &RefuseEverything,
    }];
    let deps = deps(&orchestrator, &policy, &tools, &guardrails, None);
    deps.cancel.cancel();

    let RunLoopState::Finish(output) = drive(&deps).await.expect("cancellation finishes the run")
    else {
        panic!("a cancelled run finishes rather than pausing");
    };
    // The normal notice mentions the saved work; the withheld one does not.
    assert_eq!(output.message.content.as_ref(), "Stopped.");
}

#[tokio::test]
async fn a_cancelled_run_keeps_its_usual_notice_when_the_policy_allows_it() {
    let orchestrator = NeverFinishes;
    let policy = Policy::default();
    let tools = ToolRegistry::new();
    let deps = deps(&orchestrator, &policy, &tools, &[], None);
    deps.cancel.cancel();

    let RunLoopState::Finish(output) = drive(&deps).await.expect("cancellation finishes the run")
    else {
        panic!("a cancelled run finishes rather than pausing");
    };
    assert!(
        output
            .message
            .content
            .contains("Anything I had already done"),
        "an allowed notice is not downgraded, got: {}",
        output.message.content
    );
}
