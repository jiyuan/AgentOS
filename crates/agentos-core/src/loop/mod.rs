use crate::approve::Policy;
use crate::hooks::Hooks;
use crate::prompt::Compaction;
use crate::spill::{ContentLimits, SpillRef, SpillSource};
use crate::subagents::{SubAgentError, SubAgentRegistry};
use crate::task_workspace::{TaskWorkspace, TaskWorkspaceError};
use crate::tools::{ToolRegistry, ToolRegistryError};
use crate::trace;
use agentos_interfaces::guardrail::{
    GuardrailError, GuardrailOutcome, Input, InputGuardrail, OutputGuardrail, ToolGuardrail,
};
use agentos_interfaces::orchestrator::{Orchestrator, Plan, RunContext, StreamSink};
use agentos_interfaces::run_state::{ApprovalStatus, Interruption, InterruptionAction, RunState};
use agentos_proto::{AgentId, InterruptionId, Message, SpanKind, ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

mod approval;
mod budget;
mod delegate;
mod escalate;
mod items;
mod planning;
mod request;
mod telemetry;

use approval::{approve_transition, ApproveTransition};
use budget::{budget_exhausted_finish, record_llm_usage};
use delegate::{execute_delegate, execute_resume_delegate, DelegateOutcome};
use escalate::{execute_escalate, EscalateOutcome};
use items::{
    assistant_tool_call_item, metadata_value, subagent_result_item, suborchestrator_result_item,
    tool_result_item, tool_status_name,
};
use request::record_request_header;
use telemetry::{field_key, plan_assignment_fields, record_telemetry_event};

#[derive(Debug, Error)]
pub enum RunError {
    #[error("run has already finished")]
    AlreadyDone,
    #[error("paused run must be resumed through the approval path")]
    NotResumable,
    #[error("maximum turn count exceeded")]
    MaxTurnsExceeded,
    #[error("orchestrator failed: {0}")]
    Orchestrator(#[from] agentos_interfaces::orchestrator::OrchestratorError),
    #[error("guardrail backend failed: {0}")]
    Guardrail(#[from] GuardrailError),
    #[error("guardrail '{guardrail}' tripped: {reason}")]
    GuardrailTripped {
        guardrail: Arc<str>,
        reason: Arc<str>,
    },
    #[error("tool execution failed: {0}")]
    Tool(#[from] ToolRegistryError),
    #[error("sub-agent execution failed: {0}")]
    SubAgent(#[from] SubAgentError),
    #[error("task workspace failed: {0}")]
    TaskWorkspace(#[from] TaskWorkspaceError),
    #[error("approval denied: {reason}")]
    ApprovalDenied { reason: Arc<str> },
    #[error("approval cannot pause this action yet: {reason}")]
    ApprovalUnsupported { reason: Arc<str> },
}

pub struct LoopDeps<'a> {
    pub orchestrator: &'a dyn Orchestrator,
    pub max_turns: usize,
    pub hooks: Option<&'a Hooks>,
    pub tools: Option<&'a ToolRegistry>,
    pub task_workspace: Option<&'a TaskWorkspace>,
    pub policy: &'a Policy,
    pub subagents: Option<&'a SubAgentRegistry>,
    pub input_guardrails: &'a [InputGuardrailEntry<'a>],
    pub output_guardrails: &'a [OutputGuardrailEntry<'a>],
    pub tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    /// Optional sink for incremental assistant text. When set, the loop installs
    /// it on each plan's [`RunContext`] so a streaming-capable orchestrator can
    /// emit tokens as they arrive. `None` keeps planning buffered.
    pub stream_sink: Option<StreamSink>,
    /// Inline cap for tool output and where the overflow is persisted.
    pub content_limits: ContentLimits<'a>,
    /// Who summarizes this run's history when it outgrows the window, and at
    /// what pressure. Unset leaves compaction off.
    pub compaction: Compaction<'a>,
}

pub struct InputGuardrailEntry<'a> {
    pub name: Arc<str>,
    pub guardrail: &'a dyn InputGuardrail,
}

pub struct OutputGuardrailEntry<'a> {
    pub name: Arc<str>,
    pub guardrail: &'a dyn OutputGuardrail,
}

pub struct ToolGuardrailEntry<'a> {
    pub name: Arc<str>,
    pub guardrail: &'a dyn ToolGuardrail,
}

#[derive(Debug)]
pub enum RunLoopState {
    Start(StartCtx),
    Plan(PlanCtx),
    Approve(ApproveCtx),
    Act(ActCtx),
    Observe(ObserveCtx),
    Paused(RunState),
    Finish(FinalOutput),
}

impl RunLoopState {
    pub async fn step(self, deps: &LoopDeps<'_>) -> Result<Self, RunError> {
        match self {
            Self::Start(ctx) => start(ctx, deps).await,
            Self::Plan(ctx) => plan(ctx, deps).await,
            Self::Approve(ctx) => approve(ctx, deps).await,
            Self::Act(ctx) => act(ctx, deps).await,
            Self::Observe(ctx) => observe(ctx).await,
            Self::Paused(_) => Err(RunError::NotResumable),
            Self::Finish(_) => Err(RunError::AlreadyDone),
        }
    }
}

pub fn resume_approved(state: RunState) -> Result<RunLoopState, RunError> {
    let mut state = state;
    if let Some(reason) = state.take_rejected_reason() {
        return Err(RunError::ApprovalDenied { reason });
    }

    let turns = resume_turns(&state);
    let Some(action) = state.take_approved_action() else {
        return Err(RunError::NotResumable);
    };
    let plan = match action {
        InterruptionAction::ToolCall(call) => Plan::CallTool(call),
        InterruptionAction::Delegate(spec) => Plan::Delegate(spec),
        InterruptionAction::Escalate(spec) => Plan::Escalate(spec),
        InterruptionAction::Handoff { agent_id, payload } => Plan::Handoff(agent_id, payload),
        InterruptionAction::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        } => Plan::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        },
    };
    Ok(RunLoopState::Act(ActCtx {
        state,
        plan,
        turns,
        denied: None,
    }))
}

#[derive(Debug)]
pub struct StartCtx {
    pub state: RunState,
}

#[derive(Debug)]
pub struct PlanCtx {
    pub state: RunState,
    pub turns: usize,
}

#[derive(Debug)]
pub struct ApproveCtx {
    pub state: RunState,
    pub plan: Plan,
    pub turns: usize,
}

#[derive(Debug)]
pub struct ActCtx {
    pub state: RunState,
    pub plan: Plan,
    pub turns: usize,
    /// Set when the Approve state denied this (tool) plan. The Act state then
    /// records a denied `ToolResult` instead of executing the tool, keeping the
    /// `Plan -> Approve -> Act -> Observe` progression intact so the model can
    /// read the denial and replan on the next turn.
    pub denied: Option<Arc<str>>,
}

#[derive(Debug)]
pub struct ObserveCtx {
    pub state: RunState,
    pub turns: usize,
}

#[derive(Debug)]
pub struct FinalOutput {
    pub state: RunState,
    pub message: Message,
}

async fn start(ctx: StartCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, RunError> {
    info!(
        run_id = ctx.state.run_id.as_str(),
        active_agent = ctx.state.active_agent.as_str(),
        "run_loop_start"
    );
    if let Some(item) = ctx.state.transcript.items.last() {
        let run_ctx = RunContext::from_state(&ctx.state);
        let input = Input {
            message: item.message.clone(),
        };
        for entry in deps.input_guardrails {
            let outcome = entry.guardrail.check(&input, &run_ctx).await?;
            ensure_guardrail_passed(&entry.name, outcome)?;
        }
    }
    Ok(RunLoopState::Plan(PlanCtx {
        state: ctx.state,
        turns: 0,
    }))
}

async fn plan(ctx: PlanCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, RunError> {
    if ctx.turns >= deps.max_turns {
        // Mechanism-level safeguard. Exhausting the turn budget used to abort
        // the whole run with `MaxTurnsExceeded` — and because sub-agents run
        // this same loop, a sub-agent hitting its (small) budget failed the
        // entire parent chain with the opaque "run loop failed: maximum turn
        // count exceeded" error and discarded every intermediate result.
        //
        // Instead, treat the budget as a *stop condition*, not an error:
        // synthesize a terminal answer from the work already produced and
        // finish normally. The run still cannot exceed `max_turns` tool
        // cycles, but the caller (or parent run) gets the best partial
        // result plus a clear truncation notice rather than a hard failure.
        return Ok(RunLoopState::Finish(
            budget_exhausted_finish(ctx.state, ctx.turns, deps).await,
        ));
    }

    let mut state = ctx.state;
    let mut fields = BTreeMap::new();
    fields.insert(field_key("turn"), Value::from(ctx.turns));
    let parent_id = trace::run_span_id(&state);
    let plan_span_id = trace::record_span(&mut state, parent_id, SpanKind::State, "plan", fields);
    trace::record_event(
        &mut state,
        deps.hooks,
        plan_span_id.clone(),
        "plan_started",
        BTreeMap::new(),
    );

    // C3: summarize the oldest span if the run is already over the configured
    // pressure, before hydration so recall and the orchestrator both see the
    // compacted transcript.
    planning::compact_under_pressure(&mut state, deps, &plan_span_id).await;

    // C4: one attempt, and if the provider rejects it for length, one forced
    // compaction and one retry. A second rejection comes back as a plan
    // carrying a truncation notice, which the reply arm below then takes
    // through the output guardrails like any other answer.
    let mut pending_usage = Vec::new();
    let mut pending_requests = Vec::new();
    let plan = planning::plan_with_overflow_recovery(
        &mut state,
        deps,
        &plan_span_id,
        &mut pending_usage,
        &mut pending_requests,
    )
    .await?;

    trace::record_span(
        &mut state,
        Some(plan_span_id.clone()),
        SpanKind::Llm,
        "orchestrator.plan",
        BTreeMap::new(),
    );
    let assignment_fields = plan_assignment_fields(&state, &plan);
    record_telemetry_event(
        &mut state,
        deps.hooks,
        plan_span_id.clone(),
        "orchestrator_task_assigned",
        assignment_fields,
    );
    // Before the usage events: the header describes the request that produced
    // the usage, so a trace reads request-then-cost.
    for header in pending_requests {
        record_request_header(&mut state, deps.hooks, plan_span_id.clone(), header);
    }
    for usage in pending_usage {
        record_llm_usage(&mut state, deps, plan_span_id.clone(), usage);
    }
    trace::record_event(
        &mut state,
        deps.hooks,
        plan_span_id,
        "plan_finished",
        BTreeMap::new(),
    );
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        turn = ctx.turns,
        "plan_finished"
    );

    match plan {
        Plan::Reply(message) => {
            let run_ctx = RunContext::from_state(&state);
            for entry in deps.output_guardrails {
                let outcome = entry.guardrail.check(&message, &run_ctx).await?;
                ensure_guardrail_passed(&entry.name, outcome)?;
            }
            Ok(RunLoopState::Finish(FinalOutput { state, message }))
        }
        plan => Ok(RunLoopState::Approve(ApproveCtx {
            state,
            plan,
            turns: ctx.turns,
        })),
    }
}

/// Fold one just-completed LLM call's token usage into the run total and emit
/// a trace event carrying both the per-call breakdown and the running totals,
/// so the input/output split and cache hit/miss counts are persisted in
/// `RunState` rather than living only in provider log lines.
///
/// One call to this function corresponds to exactly one LLM round-trip. The
/// orchestrator (which is the only component holding the response `Message`
/// before it is folded into a `Plan` variant) is responsible for pushing each
/// call's `Usage` into `RunContext::usage_sink`; the loop drains the sink after
/// `orchestrator.plan()` returns and calls this for every entry — including
/// calls whose plan was `Plan::CallTool`, `Plan::Delegate`, etc.
async fn approve(ctx: ApproveCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, RunError> {
    match approve_transition(ctx, deps.policy) {
        ApproveTransition::Allow { state, plan, turns } => Ok(RunLoopState::Act(ActCtx {
            state,
            plan,
            turns,
            denied: None,
        })),
        ApproveTransition::DenyTool {
            state,
            call,
            reason,
            turns,
        } => Ok(RunLoopState::Act(ActCtx {
            state,
            plan: Plan::CallTool(call),
            turns,
            denied: Some(reason),
        })),
        ApproveTransition::Deny { reason } => Err(RunError::ApprovalDenied { reason }),
        ApproveTransition::Pause { state } => Ok(RunLoopState::Paused(state)),
        ApproveTransition::Unsupported { reason } => Err(RunError::ApprovalUnsupported { reason }),
    }
}

async fn act(ctx: ActCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, RunError> {
    let ActCtx {
        mut state,
        plan,
        turns,
        denied,
    } = ctx;
    match plan {
        Plan::CallTool(call) => {
            // Record the assistant turn that requested the tool *before*
            // executing it. OpenAI/Anthropic/DeepSeek all 400 if a tool result
            // arrives without a preceding assistant turn carrying that
            // tool_call's id.
            state.transcript.items.push(assistant_tool_call_item(&call));
            // Kept for the spill filename: `execute_tool` consumes the call,
            // and a `ToolResult` carries only the call id.
            let tool_name = Arc::clone(&call.name);
            // A policy denial short-circuits execution: surface it as a denied
            // `ToolResult` the model can read and recover from, rather than
            // aborting the run (and the sub-agent / gateway above it).
            let result = match denied {
                Some(reason) => denied_tool_result(&mut state, deps, &call, reason),
                None => execute_tool(&mut state, deps, call).await?,
            };
            let spilled = spill_oversized(&state, deps, &tool_name, &result).await;
            state.transcript.items.push(tool_result_item(
                result,
                deps.content_limits.tool_result_inline_bytes,
                spilled,
            ));
        }
        Plan::Delegate(spec) => match execute_delegate(&mut state, deps, &spec).await? {
            DelegateOutcome::Finished(result) => {
                state.transcript.items.push(subagent_result_item(result));
            }
            DelegateOutcome::Paused(paused) => {
                return pause_for_subagent_approval(state, spec, paused);
            }
        },
        Plan::Escalate(spec) => match execute_escalate(&mut state, deps, &spec).await? {
            EscalateOutcome::Finished(result) => {
                state
                    .transcript
                    .items
                    .push(suborchestrator_result_item(&spec, result));
            }
            EscalateOutcome::Paused {
                stage_agent,
                paused,
            } => {
                return pause_for_subagent_approval(state, stage_agent, *paused);
            }
        },
        Plan::Handoff(agent_id, payload) => {
            execute_handoff(&mut state, deps, agent_id, payload);
        }
        Plan::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        } => {
            let paused = crate::subagents::SubAgentPausedRun {
                agent_id: spec.agent_id.clone(),
                policy_id: Arc::clone(&spec.policy_id),
                channel_id: child_channel_id,
                conversation_id: child_conversation_id,
                state: *child_state,
            };
            match execute_resume_delegate(&mut state, deps, &spec, paused).await? {
                DelegateOutcome::Finished(result) => {
                    state.transcript.items.push(subagent_result_item(result));
                }
                DelegateOutcome::Paused(paused) => {
                    return pause_for_subagent_approval(state, spec, paused);
                }
            }
        }
        Plan::Reply(_) => {}
    }

    Ok(RunLoopState::Observe(ObserveCtx {
        state,
        turns: turns + 1,
    }))
}

fn pause_for_subagent_approval(
    mut state: RunState,
    spec: agentos_interfaces::orchestrator::SubAgentSpec,
    paused: crate::subagents::SubAgentPausedRun,
) -> Result<RunLoopState, RunError> {
    let child_approval = paused
        .state
        .pending_approvals
        .first()
        .ok_or(RunError::SubAgent(SubAgentError::Paused))?;
    let approval_id = InterruptionId::new(format!(
        "approval-subagent-{}-{}",
        spec.agent_id.as_str(),
        child_approval.id.as_str()
    ));
    state.pending_approvals.push(Interruption {
        id: approval_id,
        action: InterruptionAction::ResumeSubAgent {
            spec,
            child_channel_id: paused.channel_id,
            child_conversation_id: paused.conversation_id,
            child_state: Box::new(paused.state),
        },
        status: ApprovalStatus::Pending,
    });
    Ok(RunLoopState::Paused(state))
}

fn execute_handoff(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    agent_id: AgentId,
    payload: Option<Value>,
) {
    let from_agent = state.active_agent.clone();
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("from_agent"), metadata_value(from_agent.as_str()));
    fields.insert(field_key("to_agent"), metadata_value(agent_id.as_str()));
    if let Some(payload) = payload {
        fields.insert(field_key("payload"), payload);
    }
    let span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Handoff,
        format!("handoff.{}", agent_id.as_str()),
        fields,
    );
    trace::record_event(
        state,
        deps.hooks,
        span_id.clone(),
        "handoff_started",
        BTreeMap::new(),
    );

    state.active_agent = agent_id;

    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("active_agent"),
        metadata_value(state.active_agent.as_str()),
    );
    trace::record_event(state, deps.hooks, span_id, "handoff_finished", fields);
}

async fn observe(ctx: ObserveCtx) -> Result<RunLoopState, RunError> {
    Ok(RunLoopState::Plan(PlanCtx {
        state: ctx.state,
        turns: ctx.turns,
    }))
}

/// Persist an oversized tool result, returning the reference to cite inline.
///
/// Best effort by design: with no store configured, a result within the cap,
/// or a storage failure, this yields `None` and the caller degrades to a plain
/// truncation notice. A full disk must not turn a successful tool call into a
/// failed run.
async fn spill_oversized(
    state: &RunState,
    deps: &LoopDeps<'_>,
    tool_name: &str,
    result: &ToolResult,
) -> Option<SpillRef> {
    let store = deps.content_limits.spill?;
    if result.content.len() <= deps.content_limits.tool_result_inline_bytes {
        return None;
    }
    let source = SpillSource {
        run_id: &state.run_id,
        tool_name,
        call_id: &result.call_id,
    };
    match store.save_text(&source, &result.content).await {
        Ok(saved) => {
            info!(
                run_id = state.run_id.as_str(),
                tool_call_id = result.call_id.as_str(),
                bytes = saved.bytes,
                locator = saved.locator.as_str(),
                "tool output spilled"
            );
            Some(saved)
        }
        Err(error) => {
            tracing::warn!(
                run_id = state.run_id.as_str(),
                tool_call_id = result.call_id.as_str(),
                error = %error,
                "tool output spill failed; falling back to truncation"
            );
            None
        }
    }
}

async fn execute_tool(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    call: ToolCall,
) -> Result<ToolResult, RunError> {
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("tool_name"), metadata_value(call.name.as_ref()));
    fields.insert(field_key("tool_call_id"), metadata_value(call.id.as_str()));
    let tool_span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Tool,
        format!("tool.{}", call.name),
        fields,
    );
    trace::record_event(
        state,
        deps.hooks,
        tool_span_id.clone(),
        "tool_started",
        BTreeMap::new(),
    );

    let preflight_guardrail_result = {
        let run_ctx = RunContext::from_state(state);
        let mut failure = None;
        for entry in deps.tool_guardrails {
            let outcome = entry.guardrail.check_call(&call, &run_ctx).await?;
            if let GuardrailOutcome::Tripped(reason) = outcome {
                failure = Some(guardrail_tool_result(&call, &entry.name, reason));
                break;
            }
        }
        failure
    };

    let result = if let Some(result) = preflight_guardrail_result {
        result
    } else {
        let tools = deps
            .tools
            .ok_or_else(|| ToolRegistryError::UnknownTool(Arc::clone(&call.name)))?;
        // Tool failures (bad path, missing file, malformed args) become a Failed
        // `ToolResult` rather than aborting the run, so the model can read the
        // error in the next turn and self-correct (e.g. create the missing dir
        // and retry). Unknown-tool / isolation errors still bubble up — those
        // indicate a misconfigured runtime, not a recoverable model mistake.
        let run_ctx = RunContext::from_state(state);
        match tools.call_with_context(&call, &run_ctx).await {
            Ok(result) => result,
            Err(ToolRegistryError::Tool(tool_err)) => ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Failed,
                content: Arc::from(tool_err.to_string()),
                metadata: BTreeMap::new(),
            },
            Err(other) => return Err(other.into()),
        }
    };

    {
        let run_ctx = RunContext::from_state(state);
        for entry in deps.tool_guardrails {
            let outcome = entry.guardrail.check_result(&result, &run_ctx).await?;
            ensure_guardrail_passed(&entry.name, outcome)?;
        }
    }

    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("status"),
        metadata_value(tool_status_name(&result.status)),
    );
    trace::record_event(state, deps.hooks, tool_span_id, "tool_finished", fields);
    Ok(result)
}

/// Record a denied tool call: a `Tool` span/event for the trace (so a denied
/// call still shows up alongside executed ones) plus the `Denied` `ToolResult`
/// fed back into the transcript for the model to observe.
fn denied_tool_result(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    call: &ToolCall,
    reason: Arc<str>,
) -> ToolResult {
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("tool_name"), metadata_value(call.name.as_ref()));
    fields.insert(field_key("tool_call_id"), metadata_value(call.id.as_str()));
    fields.insert(field_key("approval_denied"), Value::Bool(true));
    let tool_span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Tool,
        format!("tool.{}", call.name),
        fields,
    );
    let mut event_fields = BTreeMap::new();
    event_fields.insert(
        field_key("status"),
        metadata_value(tool_status_name(&ToolStatus::Denied)),
    );
    event_fields.insert(field_key("reason"), metadata_value(reason.as_ref()));
    trace::record_event(state, deps.hooks, tool_span_id, "tool_denied", event_fields);

    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("approval_denied"), Value::Bool(true));
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Denied,
        content: Arc::from(format!("tool call denied by policy: {reason}")),
        metadata,
    }
}

fn guardrail_tool_result(call: &ToolCall, guardrail: &Arc<str>, reason: Arc<str>) -> ToolResult {
    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("guardrail"), metadata_value(guardrail.as_ref()));
    metadata.insert(field_key("guardrail_tripped"), Value::Bool(true));
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Failed,
        content: Arc::from(format!("guardrail '{guardrail}' tripped: {reason}")),
        metadata,
    }
}

fn ensure_guardrail_passed(name: &Arc<str>, outcome: GuardrailOutcome) -> Result<(), RunError> {
    match outcome {
        GuardrailOutcome::Passed => Ok(()),
        GuardrailOutcome::Tripped(reason) => Err(RunError::GuardrailTripped {
            guardrail: Arc::clone(name),
            reason,
        }),
    }
}

fn resume_turns(state: &RunState) -> usize {
    state
        .trace_spans
        .iter()
        .rev()
        .find(|span| span.kind == SpanKind::State && span.name.as_ref() == "plan")
        .and_then(|span| span.fields.get("turn"))
        .and_then(Value::as_u64)
        .map_or(0, |turn| turn as usize)
}
