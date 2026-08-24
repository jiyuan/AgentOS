use crate::approve::{DelegatedAuthority, Policy};
use crate::audit::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyJournal, SafetyOutcome};
use crate::hooks::Hooks;
use crate::prompt::Compaction;
use crate::spill::ContentLimits;
use crate::subagents::{SubAgentError, SubAgentRegistry};
use crate::task_workspace::TaskWorkspace;
use crate::tools::ToolRegistry;
use crate::trace;
use agentos_interfaces::guardrail::{
    GuardrailOutcome, Input, InputGuardrail, OutputGuardrail, ToolGuardrail,
};
use agentos_interfaces::orchestrator::{Orchestrator, Plan, RunContext, StreamSink};
use agentos_interfaces::run_state::{Interruption, InterruptionAction, RunState};
use agentos_proto::{ApprovalInstanceId, InterruptionId, Message, SpanKind};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod approval;
mod approval_route;
mod authorization;
mod batch;
mod budget;
mod cancel;
mod delegate;
mod delegation_grant;
mod error;
mod escalate;
mod handoff;
mod items;
mod output;
mod planning;
mod request;
mod resume;
mod steering;
mod telemetry;
mod tool_call;

use approval::{approve_transition, ApproveTransition};
pub use approval_route::{
    parse_action_data, prompt_actions, prompt_interruption, route, ApprovalBinding,
    ApprovalOutcome, ApprovalTicket, ResumeWitness, Routed, TicketError, ACTIONS_KEY, APPROVE,
    DECISION_KEY, DENY, EXPIRES_AT_KEY, INTERRUPTION_KEY, PROMPT_KIND, REASON_KEY, TICKET_KEY,
};
use authorization::{plan_subject, ActPlan, AuthorizedPlan};
pub use authorization::{Authorization, Unauthorized};
use budget::{budget_exhausted_finish, record_llm_usage};
use cancel::{cancelled_finish, unless_cancelled};
use delegate::{execute_delegate, execute_resume_delegate, DelegateOutcome};
pub use error::{RunError, StepFailure};
use escalate::{execute_escalate, EscalateOutcome};
use handoff::execute_handoff;
use items::{
    assistant_tool_call_item, subagent_result_item, suborchestrator_result_item, tool_result_item,
};
use request::record_request_header;
pub(crate) use resume::enter_approved;
pub use resume::resume_approved;
pub use steering::{Steered, Steering, DEFAULT_STEERING_CAPACITY};
use tool_call::{denied_tool_result, execute_tool, spill_oversized};

use telemetry::{field_key, plan_assignment_fields, record_telemetry_event};

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
    /// The run's cancellation token. A default token is never cancelled, so a
    /// caller that does not want to stop runs can ignore this field.
    pub cancel: CancellationToken,
    /// Where input that arrived mid-run waits to be claimed (roadmap item G1).
    /// `None` means nothing can steer this run, which is every entrypoint that
    /// does not multiplex a conversation.
    pub steering: Option<Steering>,
    /// Where this run's safety-boundary decisions are recorded (M6 /
    /// `AUD-001`). A detached journal writes nowhere, which is what an
    /// entrypoint with no store configured gets.
    pub audit: SafetyJournal<'a>,
    /// Authority this run holds that its parent does not, from
    /// `[[subagents.delegation_grants]]`. Empty for every run that is not a
    /// sub-agent whose narrowing needed a grant. The `Approve` state records
    /// a use when a call falls under one of these.
    pub delegated_authority: Option<&'a DelegatedAuthority>,
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
    /// Advance one state.
    ///
    /// A failure carries the state back out (see [`StepFailure`]): this method
    /// consumes `self`, so anything it does not hand back is gone, and a run
    /// that fails is exactly the one whose trace someone will want.
    pub async fn step(self, deps: &LoopDeps<'_>) -> Result<Self, StepFailure> {
        match self {
            Self::Start(ctx) => start(ctx, deps).await,
            Self::Plan(ctx) => plan(ctx, deps).await,
            Self::Approve(ctx) => approve(ctx, deps).await,
            Self::Act(ctx) => act(ctx, deps).await,
            Self::Observe(ctx) => Ok(observe(ctx)),
            Self::Paused(state) => Err(StepFailure::new(state, RunError::NotResumable)),
            Self::Finish(output) => Err(StepFailure::new(output.state, RunError::AlreadyDone)),
        }
    }
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
/// State that may enter the action executor only after approval.
///
/// The private authorized plan makes literal construction outside this crate a
/// compile error:
///
/// ```compile_fail
/// use agentos_core::r#loop::ActCtx;
/// use agentos_interfaces::RunState;
/// # fn cannot_fabricate(state: RunState) {
/// let _ = ActCtx { state, turns: 0 };
/// # }
/// ```
pub struct ActCtx {
    pub state: RunState,
    pub turns: usize,
    /// The immutable plan, its approval witness, and any denied-tool
    /// disposition.
    ///
    /// Private, and [`AuthorizedPlan`] has no public constructor, so this
    /// struct cannot be built by a literal outside the crate. Because the plan
    /// itself is inside that private value, a caller holding a genuine
    /// `ActCtx` cannot swap the action, payload, or denial state either.
    authorized_plan: AuthorizedPlan,
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

async fn start(ctx: StartCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, StepFailure> {
    let state = ctx.state;
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        "run_loop_start"
    );
    // The guardrail pass borrows `state` through its `RunContext`, so the
    // failure is decided inside this block and acted on outside it — moving
    // the state into a `StepFailure` while the borrow is live would not
    // compile, and threading an `Option` through the loop reads worse.
    let refusal = {
        let mut run_ctx = RunContext::from_state(&state);
        run_ctx.cancel = deps.cancel.clone();
        let mut refusal = None;
        if let Some(item) = state.transcript.items.last() {
            let input = Input {
                message: item.message.clone(),
            };
            for entry in deps.input_guardrails {
                let checked = match entry.guardrail.check(&input, &run_ctx).await {
                    Ok(outcome) => ensure_guardrail_passed(
                        deps,
                        SafetyEventKind::InputGuardrailTrip,
                        SafetyOutcome::Tripped,
                        &entry.name,
                        outcome,
                    ),
                    Err(error) => Err(RunError::from(error)),
                };
                if let Err(error) = checked {
                    refusal = Some(error);
                    break;
                }
            }
        }
        refusal
    };
    if let Some(error) = refusal {
        return Err(StepFailure::new(state, error));
    }
    Ok(RunLoopState::Plan(PlanCtx { state, turns: 0 }))
}

async fn plan(ctx: PlanCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, StepFailure> {
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

    // Beside the turn budget, and for the same reason: a stop condition, not
    // an error. Checked before any work so a run cancelled between turns costs
    // nothing further.
    if deps.cancel.is_cancelled() {
        return Ok(RunLoopState::Finish(
            cancelled_finish(ctx.state, deps, "between_turns").await,
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

    // G1: claim anything the user said while this run was in flight. Before
    // compaction and hydration so the new input is part of what gets summarized
    // and recalled against, and so the orchestrator plans this turn already
    // knowing about it.
    steering::claim(&mut state, deps, &plan_span_id);

    // X1: a tool-call batch already in flight is drained one call per turn,
    // before the orchestrator is asked for anything new. Going back through
    // `Plan` rather than looping inside `Act` is what keeps the guarantee that
    // every call crosses `Approve` on its own — the loop never carries more
    // than one call past this point, so a batch cannot smuggle a call past the
    // policy engine. It also costs no round trip: the model already said what
    // it wanted.
    if let Some(next) = batch::next_queued(&mut state, deps, &plan_span_id) {
        return Ok(RunLoopState::Approve(ApproveCtx {
            state,
            plan: Plan::CallTool(next),
            turns: ctx.turns,
        }));
    }

    // Both compaction paths and the planning attempts share these, so a
    // summarizer call is accounted for exactly like the turn's own request
    // (M5 / `REQ-001`). Declared before `compact_under_pressure` because that
    // one runs first and can spend a provider call of its own.
    let mut pending_usage = Vec::new();
    let mut pending_requests = Vec::new();

    // C3: summarize the oldest span if the run is already over the configured
    // pressure, before hydration so recall and the orchestrator both see the
    // compacted transcript.
    planning::compact_under_pressure(
        &mut state,
        deps,
        &plan_span_id,
        &mut pending_usage,
        &mut pending_requests,
    )
    .await;

    // C4: one attempt, and if the provider rejects it for length, one forced
    // compaction and one retry. A second rejection comes back as a plan
    // carrying a truncation notice, which the reply arm below then takes
    // through the output guardrails like any other answer.
    let planned = unless_cancelled(
        &deps.cancel,
        planning::plan_with_overflow_recovery(
            &mut state,
            deps,
            &plan_span_id,
            &mut pending_usage,
            &mut pending_requests,
        ),
    )
    .await;
    let planned = match planned.transpose() {
        Ok(planned) => planned,
        Err(error) => return Err(StepFailure::new(state, error)),
    };
    let Some(plan) = planned else {
        return Ok(RunLoopState::Finish(
            cancelled_finish(state, deps, "planning").await,
        ));
    };

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
    // Kept past the move below: a batch split records under this same span.
    let plan_span_id_for_batch = plan_span_id.clone();
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
            // Decided inside the borrow, acted on outside it — see `start`.
            let refusal = {
                let mut run_ctx = RunContext::from_state(&state);
                run_ctx.cancel = deps.cancel.clone();
                let mut refusal = None;
                for entry in deps.output_guardrails {
                    let checked = match entry.guardrail.check(&message, &run_ctx).await {
                        Ok(outcome) => ensure_guardrail_passed(
                            deps,
                            SafetyEventKind::OutputGuardrailTrip,
                            SafetyOutcome::Tripped,
                            &entry.name,
                            outcome,
                        ),
                        Err(error) => Err(RunError::from(error)),
                    };
                    if let Err(error) = checked {
                        refusal = Some(error);
                        break;
                    }
                }
                refusal
            };
            if let Some(error) = refusal {
                return Err(StepFailure::new(state, error));
            }
            Ok(RunLoopState::Finish(FinalOutput { state, message }))
        }
        // X1: the model asked for several tools at once. Take the first and
        // queue the rest, so results append in the order it asked for them.
        Plan::CallTools(calls) => {
            match batch::enqueue(&mut state, deps, &plan_span_id_for_batch, calls) {
                Some(first) => Ok(RunLoopState::Approve(ApproveCtx {
                    state,
                    plan: Plan::CallTool(first),
                    turns: ctx.turns,
                })),
                // An empty batch is not a plan. Say so rather than inventing
                // an outcome: an orchestrator that returns one has a bug, and
                // silently finishing the run would hide it.
                None => Err(StepFailure::new(
                    state,
                    agentos_interfaces::orchestrator::OrchestratorError::Backend(Arc::from(
                        "orchestrator planned an empty tool batch",
                    )),
                )),
            }
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
async fn approve(ctx: ApproveCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, StepFailure> {
    match approve_transition(ctx, deps) {
        ApproveTransition::Allow { state, plan, turns } => {
            // The elevation was recorded when narrowing admitted it; this is
            // the record that it was exercised. Only on `Allow`: a grant that
            // covers a call the child was denied anyway granted nothing.
            if let Err(reason) = delegation_grant::validate(&plan, deps) {
                let subject = plan_subject(&plan);
                if let Plan::CallTool(call) = plan {
                    return Ok(RunLoopState::Act(ActCtx {
                        authorized_plan: AuthorizedPlan::denied_tool(Plan::CallTool(call), reason),
                        state,
                        turns,
                    }));
                }
                return Err(StepFailure::new(
                    state,
                    RunError::StructuralDenial { subject, reason },
                ));
            }
            Ok(RunLoopState::Act(ActCtx {
                authorized_plan: AuthorizedPlan::allowed(plan),
                state,
                turns,
            }))
        }
        ApproveTransition::DenyTool {
            state,
            call,
            reason,
            turns,
        } => {
            deps.audit.record(
                SafetyEvent::new(
                    SafetyEventKind::PolicyDenial,
                    SafetyOutcome::Denied,
                    Arc::clone(&call.name),
                )
                .with_detail(reason.as_ref())
                .with_digest(ArgumentDigest::of(call.args.get().as_bytes())),
            );
            let plan = Plan::CallTool(call);
            Ok(RunLoopState::Act(ActCtx {
                authorized_plan: AuthorizedPlan::denied_tool(plan, reason),
                state,
                turns,
            }))
        }
        ApproveTransition::Deny {
            state,
            subject,
            reason,
        } => {
            deps.audit.record_reason(
                SafetyEventKind::PolicyDenial,
                SafetyOutcome::Denied,
                Arc::clone(&subject),
                reason.as_ref(),
            );
            Err(StepFailure::new(
                state,
                RunError::StructuralDenial { subject, reason },
            ))
        }
        ApproveTransition::Pause { state } => Ok(RunLoopState::Paused(state)),
        ApproveTransition::Unsupported { state, reason } => Err(StepFailure::new(
            state,
            RunError::ApprovalUnsupported { reason },
        )),
    }
}

async fn act(ctx: ActCtx, deps: &LoopDeps<'_>) -> Result<RunLoopState, StepFailure> {
    let ActCtx {
        mut state,
        turns,
        authorized_plan,
    } = ctx;
    // Re-assert, idempotently, on the way in. `Approve` decided this plan, but
    // a decision made one state ago is not the same thing as a decision made
    // now: the witness has to name *this* plan, and the policy has to still
    // agree. A denied call skips the check because it is not going to run —
    // recording the denial is the whole of what happens to it.
    let (plan, denied) = match authorized_plan.into_act(deps.policy) {
        Ok(ActPlan::Execute(plan)) => (plan, None),
        Ok(ActPlan::DeniedTool { call, reason }) => (Plan::CallTool(call), Some(reason)),
        Err(failure) => {
            let plan = *failure.plan;
            let refusal = failure.error;
            let subject = plan_subject(&plan);
            deps.audit.record_reason(
                SafetyEventKind::PolicyDenial,
                SafetyOutcome::Denied,
                Arc::clone(&subject),
                refusal.to_string(),
            );
            return Err(StepFailure::new(
                state,
                RunError::StructuralDenial {
                    subject,
                    reason: Arc::from(refusal.to_string()),
                },
            ));
        }
    };
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
                None => match execute_tool(&mut state, deps, call).await {
                    Ok(result) => result,
                    Err(RunError::Cancelled) => {
                        return Ok(RunLoopState::Finish(
                            cancelled_finish(state, deps, "tool_call").await,
                        ))
                    }
                    Err(other) => return Err(StepFailure::new(state, other)),
                },
            };
            let spilled = spill_oversized(&state, deps, &tool_name, &result).await;
            state.transcript.items.push(tool_result_item(
                result,
                deps.content_limits.tool_result_inline_bytes,
                spilled,
            ));
            // X5: the pairing the comment above depends on, checked against
            // the transcript that was just written rather than left to the
            // provider to reject a turn later.
            #[cfg(debug_assertions)]
            crate::invariants::tool_result_follows_its_call(&state.transcript);
        }
        Plan::Delegate(spec) => match execute_delegate(&mut state, deps, &spec).await {
            Err(error) => return Err(StepFailure::new(state, error)),
            Ok(DelegateOutcome::Finished(result)) => {
                state.transcript.items.push(subagent_result_item(result));
            }
            Ok(DelegateOutcome::Paused(paused)) => {
                return pause_for_subagent_approval(state, spec, paused, deps);
            }
        },
        Plan::Escalate(spec) => match execute_escalate(&mut state, deps, &spec).await {
            Err(error) => return Err(StepFailure::new(state, error)),
            Ok(EscalateOutcome::Finished(result)) => {
                state
                    .transcript
                    .items
                    .push(suborchestrator_result_item(&spec, result));
            }
            Ok(EscalateOutcome::Paused {
                stage_agent,
                paused,
            }) => {
                return pause_for_subagent_approval(state, stage_agent, *paused, deps);
            }
        },
        // Unreachable: `plan` splits every batch into single calls before
        // `Approve`, so `Act` only ever sees one. Failing closed rather than
        // executing a batch here, which would be a way past the per-call
        // approval the split exists to guarantee.
        Plan::CallTools(calls) => {
            let reason = Arc::from(format!(
                "a batch of {} tool calls reached Act without being split",
                calls.len()
            ));
            return Err(StepFailure::new(
                state,
                RunError::ApprovalUnsupported { reason },
            ));
        }
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
            match execute_resume_delegate(&mut state, deps, &spec, paused).await {
                Err(error) => return Err(StepFailure::new(state, error)),
                Ok(DelegateOutcome::Finished(result)) => {
                    state.transcript.items.push(subagent_result_item(result));
                }
                Ok(DelegateOutcome::Paused(paused)) => {
                    return pause_for_subagent_approval(state, spec, paused, deps);
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
    deps: &LoopDeps<'_>,
) -> Result<RunLoopState, StepFailure> {
    let Some(child_approval) = paused.state.pending_approval() else {
        return Err(StepFailure::new(state, SubAgentError::Paused));
    };
    let approval_id = InterruptionId::new(format!(
        "approval-subagent-{}-{}",
        spec.agent_id.as_str(),
        child_approval.id.as_str()
    ));
    let ticket = ApprovalTicket::mint().map_err(|error| {
        StepFailure::new(
            state.clone(),
            RunError::ApprovalUnsupported {
                reason: Arc::from(format!("could not create approval ticket: {error}")),
            },
        )
    })?;
    let prompting_principal = deps.audit.actor_principal().ok_or_else(|| {
        StepFailure::new(
            state.clone(),
            RunError::ApprovalUnsupported {
                reason: Arc::from("approval requires a sender-qualified prompting principal"),
            },
        )
    })?;
    let approval_instance_id = ApprovalInstanceId::new(ticket.as_str());
    deps.audit.record(
        SafetyEvent::new(
            SafetyEventKind::ApprovalRequested,
            SafetyOutcome::Requested,
            format!("subagent {}", spec.agent_id.as_str()),
        )
        .with_interruption(approval_id.clone())
        .with_approval_instance(approval_instance_id.clone()),
    );
    state.approvals.push(Interruption::pending(
        approval_instance_id,
        ticket.as_str(),
        prompting_principal,
        approval_id,
        InterruptionAction::ResumeSubAgent {
            spec,
            child_channel_id: paused.channel_id,
            child_conversation_id: paused.conversation_id,
            child_state: Box::new(paused.state),
        },
    ));
    Ok(RunLoopState::Paused(state))
}

/// The only state that cannot fail: it renames a context and hands control
/// back to `Plan`.
fn observe(ctx: ObserveCtx) -> RunLoopState {
    RunLoopState::Plan(PlanCtx {
        state: ctx.state,
        turns: ctx.turns,
    })
}

/// Turn a guardrail verdict into control flow, recording the trip on the way
/// past.
///
/// The record happens here rather than at each call site because a guardrail
/// that trips without leaving a trace is indistinguishable from one that never
/// ran — which is exactly the gap M6 exists to close. `kind` says which
/// boundary this was, and `outcome_kind` distinguishes a call that was stopped
/// before it ran from one whose output was withheld afterwards; see
/// [`SafetyEventKind`].
fn ensure_guardrail_passed(
    deps: &LoopDeps<'_>,
    kind: SafetyEventKind,
    outcome_kind: SafetyOutcome,
    name: &Arc<str>,
    outcome: GuardrailOutcome,
) -> Result<(), RunError> {
    match outcome {
        GuardrailOutcome::Passed => Ok(()),
        GuardrailOutcome::Tripped(reason) => {
            deps.audit
                .record_reason(kind, outcome_kind, Arc::clone(name), reason.as_ref());
            Err(RunError::GuardrailTripped {
                guardrail: Arc::clone(name),
                reason,
            })
        }
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
