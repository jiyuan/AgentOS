use crate::approve::{DelegatedAuthority, Policy};
use crate::audit::{SafetyJournal, SafetyLog, SafetyOutcome};
use crate::gateway::RunJournal;
use crate::hooks::Hooks;
mod audit;
mod episodes;
mod prompt;
mod task_session;
mod trace_sink;

use crate::memory::MemoryManager;
use crate::prompt::Compaction;
use crate::r#loop::{
    enter_approved, ApprovalOutcome, FinalOutput, InputGuardrailEntry, LoopDeps,
    OutputGuardrailEntry, ResumeWitness, RunError, RunLoopState, StartCtx, Steering,
    ToolGuardrailEntry,
};
use crate::spill::ContentLimits;
use crate::subagents::SubAgentRegistry;
use crate::task_workspace::{TaskWorkspace, TaskWorkspaceError};
use crate::tools::ToolRegistry;
use crate::trace;
use agentos_interfaces::orchestrator::{Orchestrator, RequestAttemptSink, StreamSink};
use agentos_interfaces::run_state::ApprovalStatus;
use agentos_interfaces::session::{Item, Session, SessionError, Transcript};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, ConversationPrincipal, Envelope, RequestAttempt, RunId,
    SpanKind,
};
use audit::{record_failed_run, record_resolution};
use episodes::{record_denied_episode, record_finished_episode, EpisodeSeed};
use prompt::approval_action_label;
pub use prompt::approval_prompt_envelope;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use task_session::{
    activate_for_resume as activate_task_workspace_for_resume,
    activate_for_run as activate_task_workspace_for_run, active as active_task_session,
    persist_items as persist_task_session_items, task_id_for_state,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("run loop failed: {0}")]
    Run(#[from] RunError),
    #[error("session failed: {0}")]
    Session(#[from] SessionError),
    #[error("paused run state I/O failed for {path}: {source}")]
    StateIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("paused run state JSON failed for {path}: {source}")]
    StateJson {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("trace record I/O failed for {path}: {source}")]
    TraceIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("trace record JSON failed for {path}: {source}")]
    TraceJson {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("task workspace failed: {0}")]
    TaskWorkspace(#[from] TaskWorkspaceError),
}

/// Envelope metadata key that scopes how the runner sources and persists
/// conversation history for a run. Absent means the default: load the full
/// conversation transcript before the run and append the run's items back to
/// it afterwards.
pub const SESSION_SCOPE_KEY: &str = "session_scope";

/// `session_scope` value for self-contained runs — cron ticks and other
/// machine-generated envelopes whose prompt already carries everything the
/// run needs. The run starts from an empty transcript and none of its items
/// are written back to the conversation session, so recurring bulk output
/// (fetched feeds, audit trace reads) can never ratchet a shared conversation
/// past the LLM provider's context limit. Delivery is unaffected: the output
/// envelope still targets the original `conversation_id`.
pub const SESSION_SCOPE_EPHEMERAL: &str = "ephemeral";

pub struct RunnerDeps<'a> {
    pub orchestrator: &'a dyn Orchestrator,
    pub session: &'a dyn Session,
    pub memory_manager: Option<&'a MemoryManager>,
    pub hooks: Option<&'a Hooks>,
    pub max_turns: usize,
    pub active_agent: AgentId,
    pub tools: Option<&'a ToolRegistry>,
    pub trace_sink: Option<&'a dyn TraceSink>,
    pub task_workspace: Option<&'a TaskWorkspace>,
    pub policy: &'a Policy,
    pub subagents: Option<&'a SubAgentRegistry>,
    pub input_guardrails: &'a [InputGuardrailEntry<'a>],
    pub output_guardrails: &'a [OutputGuardrailEntry<'a>],
    pub tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    /// Optional incremental-text sink forwarded to the run loop (see
    /// [`StreamSink`]). Entrypoints that render streaming output (the CLI TUI)
    /// set it; everything else leaves it `None` for buffered, byte-identical
    /// behavior.
    pub stream_sink: Option<StreamSink>,
    /// Inline cap for tool output and where the overflow is persisted.
    /// `Default::default()` reproduces the pre-spill behavior exactly.
    pub content_limits: ContentLimits<'a>,
    /// Who summarizes a run's history under pressure, and at what threshold.
    pub compaction: Compaction<'a>,
    /// Cancels this run. Clone it before starting the run to keep a handle;
    /// a default token is never cancelled.
    pub cancel: CancellationToken,
    /// Where input that arrives while this run is in flight waits to be claimed
    /// (roadmap item G1). A sharded gateway sets it so a second message steers
    /// the running turn instead of starting a second, racing run on the same
    /// conversation; a one-shot entrypoint leaves it `None`.
    pub steering: Option<Steering>,
    /// Durable top-level action boundary. Gateways install it for accepted
    /// transport events; one-shot and child runs leave it detached.
    pub run_journal: Option<RunJournal>,
    /// The append-only store this run's safety-boundary decisions go to
    /// (M6 / `AUD-001`). `None` records nothing, which is what an entrypoint
    /// with no store configured gets.
    pub safety_log: Option<&'a dyn SafetyLog>,
    /// Authority this run holds that its parent does not. Set only when this
    /// is a sub-agent run whose narrowing needed a
    /// `[[subagents.delegation_grants]]` entry.
    pub delegated_authority: Option<&'a DelegatedAuthority>,
}

pub trait TraceSink: Send + Sync {
    fn persist(
        &self,
        state: &RunState,
        span_start: usize,
        event_start: usize,
        phase: &'static str,
    ) -> Result<(), RunnerError>;

    /// Append one provider-attempt transition immediately and fail closed.
    fn persist_request_attempt(&self, attempt: &RequestAttempt) -> Result<(), RunnerError>;
}

#[derive(Clone, Debug)]
pub struct JsonlTraceSink {
    dir: PathBuf,
}

impl JsonlTraceSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

impl TraceSink for JsonlTraceSink {
    fn persist(
        &self,
        state: &RunState,
        span_start: usize,
        event_start: usize,
        phase: &'static str,
    ) -> Result<(), RunnerError> {
        persist_trace_records(state, &self.dir, span_start, event_start, phase)
    }

    fn persist_request_attempt(&self, attempt: &RequestAttempt) -> Result<(), RunnerError> {
        trace_sink::persist_request_attempt(&self.dir, attempt)
    }
}

#[derive(Debug)]
pub enum RunOutcome {
    Finished { state: RunState, output: Envelope },
    Paused(RunState),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PausedRun {
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    pub state: RunState,
}

pub async fn run_envelope(
    input: Envelope,
    run_id: RunId,
    deps: &RunnerDeps<'_>,
) -> Result<RunOutcome, RunnerError> {
    let ephemeral_session = input
        .metadata
        .get(SESSION_SCOPE_KEY)
        .and_then(Value::as_str)
        == Some(SESSION_SCOPE_EPHEMERAL);
    // The principal this turn speaks as (M3 deliverable 2). The sender is
    // carried because `Session::load` applies *this participant's* `/clear`
    // epoch; the items it returns belong to the conversation, which is why
    // `append` below can be given the same value and ignore the sender.
    let conversation_principal = ConversationPrincipal::new(
        deps.active_agent.clone(),
        input.channel_id.clone(),
        input.conversation_id.clone(),
    );
    let principal = conversation_principal.actor(Arc::clone(&input.sender));
    let mut transcript = if ephemeral_session {
        Transcript::default()
    } else {
        deps.session.load(principal.as_principal()).await?
    };
    let persisted_len = transcript.items.len();
    let mut input_metadata = input.metadata.clone();
    input_metadata
        .entry(Arc::from("conversation_id"))
        .or_insert_with(|| Value::String(input.conversation_id.as_str().to_owned()));
    input_metadata
        .entry(Arc::from("channel_id"))
        .or_insert_with(|| Value::String(input.channel_id.as_str().to_owned()));
    input_metadata
        .entry(Arc::from("sender"))
        .or_insert_with(|| Value::String(input.sender.as_ref().to_owned()));
    let input_item = Item {
        message: input.message.clone(),
        metadata: input_metadata,
    };
    transcript.items.push(input_item);

    let mut state = RunState::new(run_id.clone(), deps.active_agent.clone());
    state.transcript = transcript;
    let task_session = activate_task_workspace_for_run(&mut state, &input, deps)?;
    let episode_seed = EpisodeSeed::from_input(
        &input,
        &run_id,
        &deps.active_agent,
        state
            .task_id
            .clone()
            .unwrap_or_else(|| task_id_for_state(&state)),
    );
    record_run_start(&mut state, deps.hooks);

    // The sender is part of the principal here and not in `resume_run`: a run
    // is started by somebody, and which participant that was decides who an
    // approval prompt may be answered by. A resume is keyed on the pause it
    // answers, which the interruption id already names.
    let audit = SafetyJournal::new(deps.safety_log)
        .for_run(principal.clone().into_principal(), run_id.clone());

    let request_attempt_sink = |attempt: &RequestAttempt| {
        deps.trace_sink
            .map_or(Ok(()), |sink| sink.persist_request_attempt(attempt))
            .map_err(|error| Arc::from(error.to_string()))
    };
    let loop_deps = LoopDeps {
        orchestrator: deps.orchestrator,
        max_turns: deps.max_turns,
        hooks: deps.hooks,
        tools: deps.tools,
        task_workspace: deps.task_workspace,
        policy: deps.policy,
        subagents: deps.subagents,
        input_guardrails: deps.input_guardrails,
        output_guardrails: deps.output_guardrails,
        tool_guardrails: deps.tool_guardrails,
        stream_sink: deps.stream_sink.clone(),
        request_attempt_sink: Some(&request_attempt_sink as &RequestAttemptSink<'_>),
        content_limits: deps.content_limits,
        compaction: deps.compaction,
        cancel: deps.cancel.clone(),
        steering: deps.steering.clone(),
        run_journal: deps.run_journal.clone(),
        audit: audit.clone(),
        delegated_authority: deps.delegated_authority,
    };
    let mut current = RunLoopState::Start(StartCtx { state });

    loop {
        current = match current.step(&loop_deps).await {
            Ok(next) => next,
            Err(failure) => {
                let error = record_failed_run(failure, &audit, &episode_seed, deps, 0, 0).await;
                return Err(error.into());
            }
        };
        match current {
            RunLoopState::Finish(final_output) => {
                let (state, output) = finish(
                    input.channel_id,
                    input.conversation_id,
                    persisted_len,
                    0,
                    0,
                    final_output,
                    deps,
                )
                .await?;
                return Ok(RunOutcome::Finished { state, output });
            }
            RunLoopState::Paused(state) => {
                if !ephemeral_session {
                    let append_items = state.transcript.items[persisted_len..].to_vec();
                    deps.session
                        .append(principal.as_principal(), append_items)
                        .await?;
                }
                persist_task_session_items(
                    task_session.as_ref(),
                    "paused",
                    &state.transcript.items[persisted_len..],
                )?;
                persist_trace_records_with_sink(&state, deps.trace_sink, 0, 0, "paused")?;
                return Ok(RunOutcome::Paused(state));
            }
            next => current = next,
        }
    }
}

pub async fn resume_run(
    mut paused: PausedRun,
    witness: ResumeWitness,
    deps: &RunnerDeps<'_>,
) -> Result<RunOutcome, RunnerError> {
    let persisted_len = paused.state.transcript.items.len();
    let trace_span_start = paused.state.trace_spans.len();
    let trace_event_start = paused.state.trace_events.len();
    let task_session = activate_task_workspace_for_resume(&mut paused.state, deps)?;
    let outcome = witness.outcome;
    let approval_id = witness.interruption_id.clone();
    let approval_instance_id = witness.approval_instance_id.clone();
    let audit = SafetyJournal::new(deps.safety_log).for_run(
        witness.prompting_principal.clone().into_principal(),
        paused.state.run_id.clone(),
    );
    // What the pause was about, read before the decision is applied — the
    // interruption stops being the pending one the moment it is answered.
    let Some(interruption) = paused.state.approvals.iter_mut().find(|interruption| {
        interruption.approval_instance_id == approval_instance_id
            && interruption.approval_ticket.as_ref() == witness.ticket.as_str()
            && interruption.id == approval_id
            && interruption.prompting_principal == witness.prompting_principal
            && interruption.status == ApprovalStatus::Pending
            && !interruption.consumed
    }) else {
        return Err(RunError::NotResumable.into());
    };
    let prompt = witness.prompting_principal.as_principal();
    if prompt.agent != paused.state.active_agent
        || prompt.channel != paused.channel_id
        || prompt.conversation != paused.conversation_id
    {
        return Err(RunError::NotResumable.into());
    }
    if matches!(
        outcome,
        ApprovalOutcome::Approved | ApprovalOutcome::Rejected
    ) && witness
        .expires_at
        .is_some_and(|expires_at| crate::gateway::unix_now() >= expires_at)
    {
        return Err(RunError::NotResumable.into());
    }
    let subject: Arc<str> = Arc::from(approval_action_label(&interruption.action).1);
    interruption.resolver_principal = Some(witness.resolver_principal.clone());
    if let agentos_interfaces::run_state::InterruptionAction::ResumeSubAgent {
        child_state, ..
    } = &mut interruption.action
    {
        if let Some(child) = child_state.pending_approval_mut() {
            child.resolver_principal = Some(witness.resolver_principal.clone());
        }
    }
    match outcome {
        ApprovalOutcome::Approved => {
            interruption.status = ApprovalStatus::Approved;
            // The record that used to be a deletion. `take_approved_action`
            // now marks the interruption rather than removing it, and this is
            // the durable half of the same change (M6 / `AUD-001`).
            record_resolution(&audit, SafetyOutcome::Approved, &subject, &witness, None).map_err(
                |source| {
                    RunError::safety_evidence(
                        crate::audit::SafetyEventKind::ApprovalResolved,
                        source,
                    )
                },
            )?;
        }
        ApprovalOutcome::Rejected => {
            let reason = witness
                .reason
                .clone()
                .unwrap_or_else(|| Arc::from("approval rejected"));
            interruption.status = ApprovalStatus::Rejected {
                reason: Arc::clone(&reason),
            };
            interruption.consumed = true;
            record_resolution(
                &audit,
                SafetyOutcome::Rejected,
                &subject,
                &witness,
                Some(reason.as_ref()),
            )
            .map_err(|source| {
                RunError::safety_evidence(crate::audit::SafetyEventKind::ApprovalResolved, source)
            })?;
            record_denied_episode(
                &paused.state,
                &paused.channel_id,
                &paused.conversation_id,
                &reason,
                deps,
            )
            .await;
            persist_trace_records_with_sink(
                &paused.state,
                deps.trace_sink,
                trace_span_start,
                trace_event_start,
                "denied",
            )?;
            return Err(RunError::ApprovalDenied { reason }.into());
        }
        // Expired, or nobody to ask. Fails the run closed like a denial, but
        // as a distinct error and a distinct episode outcome — the audit trail
        // has to be able to tell a refusal from a question nobody answered.
        ApprovalOutcome::Cancelled | ApprovalOutcome::Unavailable => {
            let reason = witness
                .reason
                .clone()
                .unwrap_or_else(|| Arc::from("approval went unanswered"));
            let reason: Arc<str> = Arc::from(format!("{reason} ({})", outcome.as_str()));
            interruption.status = ApprovalStatus::Unanswered {
                reason: Arc::clone(&reason),
            };
            interruption.consumed = true;
            record_resolution(
                &audit,
                SafetyOutcome::Unanswered,
                &subject,
                &witness,
                Some(reason.as_ref()),
            )
            .map_err(|source| {
                RunError::safety_evidence(crate::audit::SafetyEventKind::ApprovalResolved, source)
            })?;
            persist_trace_records_with_sink(
                &paused.state,
                deps.trace_sink,
                trace_span_start,
                trace_event_start,
                "unanswered",
            )?;
            return Err(RunError::ApprovalUnanswered { reason }.into());
        }
    }
    let episode_seed =
        EpisodeSeed::from_state(&paused.state, &paused.channel_id, &paused.conversation_id);

    let request_attempt_sink = |attempt: &RequestAttempt| {
        deps.trace_sink
            .map_or(Ok(()), |sink| sink.persist_request_attempt(attempt))
            .map_err(|error| Arc::from(error.to_string()))
    };
    let loop_deps = LoopDeps {
        orchestrator: deps.orchestrator,
        max_turns: deps.max_turns,
        hooks: deps.hooks,
        tools: deps.tools,
        task_workspace: deps.task_workspace,
        policy: deps.policy,
        subagents: deps.subagents,
        input_guardrails: deps.input_guardrails,
        output_guardrails: deps.output_guardrails,
        tool_guardrails: deps.tool_guardrails,
        stream_sink: deps.stream_sink.clone(),
        request_attempt_sink: Some(&request_attempt_sink as &RequestAttemptSink<'_>),
        content_limits: deps.content_limits,
        compaction: deps.compaction,
        cancel: deps.cancel.clone(),
        steering: deps.steering.clone(),
        run_journal: deps.run_journal.clone(),
        audit: audit.clone(),
        delegated_authority: deps.delegated_authority,
    };
    let mut current = match enter_approved(paused.state, &approval_instance_id) {
        Ok(current) => current,
        Err(failure) => {
            let error = record_failed_run(
                failure,
                &audit,
                &episode_seed,
                deps,
                trace_span_start,
                trace_event_start,
            )
            .await;
            return Err(error.into());
        }
    };

    loop {
        current = match current.step(&loop_deps).await {
            Ok(next) => next,
            Err(failure) => {
                let error = record_failed_run(
                    failure,
                    &audit,
                    &episode_seed,
                    deps,
                    trace_span_start,
                    trace_event_start,
                )
                .await;
                return Err(error.into());
            }
        };
        match current {
            RunLoopState::Finish(final_output) => {
                let (state, output) = finish(
                    paused.channel_id,
                    paused.conversation_id,
                    persisted_len,
                    trace_span_start,
                    trace_event_start,
                    final_output,
                    deps,
                )
                .await?;
                return Ok(RunOutcome::Finished { state, output });
            }
            RunLoopState::Paused(state) => {
                persist_task_session_items(task_session.as_ref(), "paused", &[])?;
                persist_trace_records_with_sink(
                    &state,
                    deps.trace_sink,
                    trace_span_start,
                    trace_event_start,
                    "paused",
                )?;
                return Ok(RunOutcome::Paused(state));
            }
            next => current = next,
        }
    }
}

/// Persist the approval record for a paused run.
///
/// Atomic and private (M8 / `GW-001`): this file names the tool call that is
/// about to fire, the conversation that asked for it, and the transcript so
/// far. A crash halfway through a plain rewrite left a truncated document that
/// deserializes as nothing, so the answer to an approval prompt resumed
/// nothing; and the default mode made all of that readable by every user on
/// the box.
pub fn save_paused_run(path: &Path, paused: &PausedRun) -> Result<(), RunnerError> {
    let encoded = serde_json::to_vec_pretty(paused).map_err(|source| RunnerError::StateJson {
        path: path.to_path_buf(),
        source,
    })?;
    crate::paths::write_private_atomic(path, &encoded).map_err(|err| RunnerError::StateIo {
        path: err.path().to_path_buf(),
        source: err.into_io(),
    })
}

pub fn load_paused_run(path: &Path) -> Result<PausedRun, RunnerError> {
    let encoded = std::fs::read(path).map_err(|source| RunnerError::StateIo {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&encoded).map_err(|source| RunnerError::StateJson {
        path: path.to_path_buf(),
        source,
    })
}

pub fn delete_paused_run(path: &Path) -> Result<(), RunnerError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(RunnerError::StateIo {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn persist_trace_records(
    state: &RunState,
    trace_dir: &Path,
    span_start: usize,
    event_start: usize,
    phase: &'static str,
) -> Result<(), RunnerError> {
    // Private (M8 / `GW-001`). A trace is the whole run: prompts, tool
    // arguments, model output. Append-only, so there is nothing to replace
    // atomically — but the mode still has to be set at creation, because
    // chmod-after-create leaves a window where it is readable.
    crate::paths::create_private_dir(trace_dir).map_err(|err| RunnerError::TraceIo {
        path: err.path().to_path_buf(),
        source: err.into_io(),
    })?;
    let path = trace_dir.join(format!(
        "{}.jsonl",
        trace_sink::trace_file_stem(&state.run_id)
    ));
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|source| RunnerError::TraceIo {
        path: path.clone(),
        source,
    })?;

    // Wall-clock at persist time, stamped onto every record in this batch.
    // Trace files are append-only per run_id, so long-lived gateway sessions
    // accumulate weeks of records in one file; without a per-record timestamp
    // any consumer windowing by file mtime (e.g. the audit skill) re-counts the
    // whole history every run. One clock read per persist batch is negligible
    // against the ≤2ms/turn budget.
    let emitted_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);

    for (index, span) in state.trace_spans.iter().enumerate().skip(span_start) {
        let record = json!({
            "record_type": "span",
            "phase": phase,
            "run_id": state.run_id.as_str(),
            "active_agent": state.active_agent.as_str(),
            "index": index,
            "emitted_unix": emitted_unix,
            "span": span,
        });
        write_trace_record(&mut file, &path, &record)?;
    }
    for (index, event) in state.trace_events.iter().enumerate().skip(event_start) {
        let record = json!({
            "record_type": "event",
            "phase": phase,
            "run_id": state.run_id.as_str(),
            "active_agent": state.active_agent.as_str(),
            "index": index,
            "emitted_unix": emitted_unix,
            "event": event,
        });
        write_trace_record(&mut file, &path, &record)?;
    }
    Ok(())
}

fn persist_trace_records_with_sink(
    state: &RunState,
    trace_sink: Option<&dyn TraceSink>,
    span_start: usize,
    event_start: usize,
    phase: &'static str,
) -> Result<(), RunnerError> {
    let Some(trace_sink) = trace_sink else {
        return Ok(());
    };
    trace_sink.persist(state, span_start, event_start, phase)
}

fn write_trace_record(
    file: &mut std::fs::File,
    path: &Path,
    record: &Value,
) -> Result<(), RunnerError> {
    let encoded = serde_json::to_string(record).map_err(|source| RunnerError::TraceJson {
        path: path.to_path_buf(),
        source,
    })?;
    writeln!(file, "{encoded}").map_err(|source| RunnerError::TraceIo {
        path: path.to_path_buf(),
        source,
    })
}

async fn finish(
    channel_id: ChannelId,
    conversation_id: ConversationId,
    persisted_len: usize,
    trace_span_start: usize,
    trace_event_start: usize,
    final_output: FinalOutput,
    deps: &RunnerDeps<'_>,
) -> Result<(RunState, Envelope), RunnerError> {
    let mut state = final_output.state;
    let output_item = Item {
        message: final_output.message.clone(),
        metadata: BTreeMap::new(),
    };
    state.transcript.items.push(output_item);
    record_run_finish(&mut state, deps.hooks);

    // Session-ephemeral runs (cron ticks) deliver their output but leave the
    // shared conversation history untouched. The marker sits on the input
    // transcript item — item 0, since ephemeral runs start from an empty
    // transcript — so it survives pause/resume round-trips through
    // `PausedRun` serialization.
    let ephemeral_session = state.transcript.items.first().is_some_and(|item| {
        item.metadata.get(SESSION_SCOPE_KEY).and_then(Value::as_str)
            == Some(SESSION_SCOPE_EPHEMERAL)
    });
    if !ephemeral_session {
        let append_items = state.transcript.items[persisted_len..].to_vec();
        // No sender: appending is conversation-keyed, and `finish` is also
        // reached from a resume, where the participant who answered the
        // approval is not necessarily the one who spoke.
        let principal = ConversationPrincipal::new(
            deps.active_agent.clone(),
            channel_id.clone(),
            conversation_id.clone(),
        );
        deps.session
            .append(principal.as_principal(), append_items)
            .await?;
    }
    persist_task_session_items(
        active_task_session(&state, deps).as_ref(),
        "finished",
        &state.transcript.items[persisted_len..],
    )?;
    persist_trace_records_with_sink(
        &state,
        deps.trace_sink,
        trace_span_start,
        trace_event_start,
        "finished",
    )?;
    let mut output_metadata = BTreeMap::new();
    if let Some(metadata) =
        record_finished_episode(&state, &channel_id, &conversation_id, deps).await
    {
        output_metadata.extend(metadata);
    }

    let output = Envelope {
        channel_id,
        conversation_id,
        sender: Arc::from(deps.active_agent.as_str()),
        message: final_output.message,
        metadata: output_metadata,
    };

    Ok((state, output))
}

fn record_run_start(state: &mut RunState, hooks: Option<&Hooks>) {
    let mut fields = BTreeMap::new();
    fields.insert(
        Arc::from("run_id"),
        Value::String(state.run_id.as_str().to_owned()),
    );
    fields.insert(
        Arc::from("active_agent"),
        Value::String(state.active_agent.as_str().to_owned()),
    );
    let span_id = trace::record_span(state, None, SpanKind::Run, "run", fields);
    trace::record_event(
        state,
        hooks,
        span_id.clone(),
        "run_started",
        BTreeMap::new(),
    );
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        "run_started"
    );
}

fn record_run_finish(state: &mut RunState, hooks: Option<&Hooks>) {
    let span_id = trace::run_span_id(state)
        .unwrap_or_else(|| trace::record_span(state, None, SpanKind::Run, "run", BTreeMap::new()));
    trace::record_event(state, hooks, span_id, "run_finished", BTreeMap::new());
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        "run_finished"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::{DelegationGrantTemplate, Policy, PolicyAction, PolicyRule, PolicyVerb};
    use crate::audit::{SafetyEventKind, SafetyLog};
    use crate::memory::{InMemorySession, SqliteStore};
    use crate::r#loop::ToolGuardrailEntry;
    use crate::subagents::{SubAgentDefinition, SubAgentRegistry};
    use crate::tools::ToolRegistry;
    use agentos_interfaces::guardrail::{GuardrailError, GuardrailOutcome, ToolGuardrail};
    use agentos_interfaces::orchestrator::{OrchestratorError, Plan, RunContext, SubAgentSpec};
    use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
    use agentos_interfaces::{InterruptionAction, Orchestrator};
    use agentos_proto::{
        AgentId, ConversationId, Message, MessageRole, Principal, ToolCall, ToolCallId, ToolResult,
        ToolStatus,
    };
    use async_trait::async_trait;
    use serde_json::{json, value::RawValue};

    #[tokio::test]
    async fn paused_subagent_tool_approval_resumes_child_and_parent() {
        let session = Arc::new(InMemorySession::default());
        let child_orchestrator = Arc::new(ChildApprovalOrchestrator);
        let parent_orchestrator = ParentDelegateOrchestrator;
        let mut registry = SubAgentRegistry::new().with_session(session.clone());
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let tools = Arc::new(tools);
        registry.register(
            SubAgentDefinition::new(
                AgentId::new("child"),
                "child-policy",
                child_orchestrator,
                Policy::ask_user_tools(["mock"]),
            )
            .with_tools(tools)
            .with_max_turns(4),
        );
        let parent_policy = Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Delegate,
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::new(),
                },
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("mock")),
                    decision: PolicyVerb::AskUser,
                    reason: Some(Arc::from("mock requires approval")),
                    arg_equals: BTreeMap::new(),
                },
            ],
            default_decision: PolicyVerb::Deny,
        };
        let deps = RunnerDeps {
            orchestrator: &parent_orchestrator,
            session: session.as_ref(),
            memory_manager: None,
            hooks: None,
            max_turns: 8,
            active_agent: AgentId::new("parent"),
            tools: None,
            trace_sink: None,
            task_workspace: None,
            policy: &parent_policy,
            subagents: Some(&registry),
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "delegate"),
            metadata: BTreeMap::new(),
        };

        let paused_state = match run_envelope(input, RunId::new("parent-run"), &deps)
            .await
            .expect("run should pause")
        {
            RunOutcome::Paused(state) => state,
            RunOutcome::Finished { .. } => panic!("expected parent pause"),
        };
        let approval = paused_state
            .pending_approval()
            .expect("parent approval expected");
        assert!(matches!(
            &approval.action,
            InterruptionAction::ResumeSubAgent { child_state, .. }
                if child_state.approvals.len() == 1
        ));
        let ticket = crate::r#loop::ApprovalTicket::parse(&approval.approval_ticket)
            .expect("stored ticket parses");
        let binding = crate::r#loop::ApprovalBinding::new(
            approval.approval_instance_id.clone(),
            ticket.clone(),
            approval.id.clone(),
            approval.prompting_principal.clone(),
            None,
        )
        .expect("instance matches ticket");
        let answer = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, format!("/approve {ticket}")),
            metadata: BTreeMap::new(),
        };
        let crate::r#loop::Routed::Decides { witness } =
            crate::r#loop::route(Some(&binding), &answer)
        else {
            panic!("answer should produce witness");
        };

        let paused = PausedRun {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            state: paused_state,
        };
        let output = match resume_run(paused, witness, &deps)
            .await
            .expect("resume should finish")
        {
            RunOutcome::Finished { output, .. } => output,
            RunOutcome::Paused(_) => panic!("expected finished parent run"),
        };

        assert_eq!(
            output.message.content.as_ref(),
            "parent saw: child finished"
        );
    }

    /// End to end: a sub-agent runs a parent-gated tool without pausing *when
    /// an explicit delegation grant says it may*.
    ///
    /// This used to pass on the strength of the tool appearing in the child's
    /// allowlist, which is the `AUTH-002` widening. The behaviour is still
    /// available — an unattended sub-agent that stops to ask is not unattended
    /// — but it now costs one declared grant with a stated reason, and it
    /// covers exactly the tool named.
    #[tokio::test]
    async fn a_granted_subagent_tool_runs_without_parent_approval() {
        let database_path = std::env::temp_dir().join(format!(
            "agentos-grant-events-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("test clock is after the Unix epoch")
                .as_nanos()
        ));
        let session = Arc::new(SqliteStore::open(&database_path).expect("audit store opens"));
        let child_orchestrator = Arc::new(ChildApprovalOrchestrator);
        let parent_orchestrator = ParentDelegateOrchestrator;
        let mut registry = SubAgentRegistry::new()
            .with_session(session.clone())
            .with_safety_log(Some(session.clone()));
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let tools = Arc::new(tools);
        registry.register(
            SubAgentDefinition::new(
                AgentId::new("child"),
                "child-policy",
                child_orchestrator,
                Policy::allow_tools(["mock"]),
            )
            .with_tools(tools)
            .with_max_turns(4)
            .with_delegation_grants(vec![DelegationGrantTemplate {
                action: PolicyAction::Tool(Arc::from("mock")),
                decision: PolicyVerb::Allow,
                arg_equals: BTreeMap::new(),
                reason: Arc::from("the delegated task cannot pause for approval"),
                lifetime_secs: 60,
            }]),
        );
        let parent_policy = Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Delegate,
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::new(),
                },
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("mock")),
                    decision: PolicyVerb::AskUser,
                    reason: Some(Arc::from("mock requires approval")),
                    arg_equals: BTreeMap::new(),
                },
            ],
            default_decision: PolicyVerb::Deny,
        };
        let deps = RunnerDeps {
            orchestrator: &parent_orchestrator,
            session: session.as_ref(),
            memory_manager: None,
            hooks: None,
            max_turns: 8,
            active_agent: AgentId::new("parent"),
            tools: None,
            trace_sink: None,
            task_workspace: None,
            policy: &parent_policy,
            subagents: Some(&registry),
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: Some(session.as_ref()),
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "delegate"),
            metadata: BTreeMap::new(),
        };

        let output = match run_envelope(input, RunId::new("parent-run"), &deps)
            .await
            .expect("allowlisted child tool should finish")
        {
            RunOutcome::Finished { output, .. } => output,
            RunOutcome::Paused(_) => panic!("allowlisted child tool should not pause"),
        };

        assert_eq!(
            output.message.content.as_ref(),
            "parent saw: child finished"
        );
        let events = session.recent(16).expect("grant events read back");
        let issued = events
            .iter()
            .find(|event| event.event.kind == SafetyEventKind::DelegationGrantIssued)
            .expect("grant issuance is durable");
        let used = events
            .iter()
            .find(|event| event.event.kind == SafetyEventKind::DelegationGrantUsed)
            .expect("grant use is durable");
        assert_eq!(
            issued.event.delegation_grant_id,
            used.event.delegation_grant_id
        );
        assert!(issued.event.delegation_grant_id.is_some());
        drop(deps);
        drop(registry);
        drop(session);
        std::fs::remove_file(database_path).expect("temporary audit store removes");
    }

    #[tokio::test]
    async fn tool_guardrail_trip_returns_failed_tool_result_to_model() {
        let session = InMemorySession::default();
        let orchestrator = ToolThenReplyOrchestrator;
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let guardrails = [ToolGuardrailEntry {
            name: Arc::from("MockGuardrail"),
            guardrail: &DenyMockToolGuardrail,
        }];
        let deps = RunnerDeps {
            orchestrator: &orchestrator,
            session: &session,
            memory_manager: None,
            hooks: None,
            max_turns: 4,
            active_agent: AgentId::new("parent"),
            tools: Some(&tools),
            trace_sink: None,
            task_workspace: None,
            policy: &Policy::allow_tools(["mock"]),
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &guardrails,
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "run tool"),
            metadata: BTreeMap::new(),
        };

        let output = match run_envelope(input, RunId::new("guardrail-run"), &deps)
            .await
            .expect("guardrail trip should become tool result")
        {
            RunOutcome::Finished { output, .. } => output,
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };

        assert!(output
            .message
            .content
            .contains("guardrail 'MockGuardrail' tripped: blocked by test"));
    }

    #[tokio::test]
    async fn policy_denied_tool_call_returns_denied_result_to_model_not_run_error() {
        // A policy that denies the tool used to abort the entire run with
        // `ApprovalDenied`, which propagated up and failed the sub-agent and
        // the gateway conversation above it. It must now surface the denial as
        // a `Denied` tool result the model reads and recovers from.
        let session = InMemorySession::default();
        let orchestrator = ToolThenReplyOrchestrator;
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let deps = RunnerDeps {
            orchestrator: &orchestrator,
            session: &session,
            memory_manager: None,
            hooks: None,
            max_turns: 4,
            active_agent: AgentId::new("parent"),
            tools: Some(&tools),
            trace_sink: None,
            task_workspace: None,
            // No rule grants "mock"; the default decision is Deny.
            policy: &Policy::default(),
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "run tool"),
            metadata: BTreeMap::new(),
        };

        let (state, output) = match run_envelope(input, RunId::new("policy-deny-run"), &deps)
            .await
            .expect("policy denial should become a tool result, not a run error")
        {
            RunOutcome::Finished { state, output } => (state, output),
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };

        assert!(
            output
                .message
                .content
                .contains("tool call denied by policy: tool 'mock' is not allowed"),
            "model should see the denial reason, got: {}",
            output.message.content
        );
        // The denied call still records a tool span so the trace stays coherent.
        let denied_spans = state
            .trace_spans
            .iter()
            .filter(|span| {
                span.kind == SpanKind::Tool
                    && span.fields.get("approval_denied") == Some(&serde_json::Value::Bool(true))
            })
            .count();
        assert_eq!(denied_spans, 1, "expected one denied tool span");
    }

    #[tokio::test]
    async fn budget_exhausted_finishes_with_partial_result_not_error() {
        // An orchestrator that never replies used to abort the whole run with
        // `MaxTurnsExceeded`. It must now terminate gracefully: a finished run
        // carrying the best partial result plus a truncation notice, and never
        // exceeding the turn budget.
        let session = InMemorySession::default();
        let orchestrator = AlwaysToolOrchestrator;
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let deps = RunnerDeps {
            orchestrator: &orchestrator,
            session: &session,
            memory_manager: None,
            hooks: None,
            max_turns: 3,
            active_agent: AgentId::new("agent"),
            tools: Some(&tools),
            trace_sink: None,
            task_workspace: None,
            policy: &Policy::allow_tools(["mock"]),
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "do something open-ended"),
            metadata: BTreeMap::new(),
        };

        let (state, output) = match run_envelope(input, RunId::new("budget-run"), &deps)
            .await
            .expect("budget exhaustion must not be a hard error")
        {
            RunOutcome::Finished { state, output } => (state, output),
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };

        assert!(
            output.message.content.contains("step budget"),
            "final message should carry a truncation notice, got: {}",
            output.message.content
        );
        assert_eq!(
            output.message.metadata.get("run_truncated"),
            Some(&serde_json::Value::Bool(true))
        );
        // The safeguard preserves the budget: at most `max_turns` tool spans.
        let tool_spans = state
            .trace_spans
            .iter()
            .filter(|span| span.kind == SpanKind::Tool)
            .count();
        assert!(
            tool_spans <= 3,
            "expected <= 3 tool turns, saw {tool_spans}"
        );
    }

    #[tokio::test]
    async fn ephemeral_scoped_run_neither_loads_nor_persists_conversation_history() {
        // Regression: cron ticks used to run inside the delivery
        // conversation's session, replaying its entire accumulated history
        // into every LLM request and appending their own bulky tool output
        // back into it — until the shared conversation exceeded the provider
        // context limit and every cron on it failed permanently.
        let session = InMemorySession::default();
        let conversation = ConversationId::new("chat-1");
        // The principal the run below will key on: same agent, same channel.
        let principal = Principal::conversation(
            AgentId::new("parent"),
            ChannelId::new("telegram"),
            conversation.clone(),
        );
        session
            .append(
                &principal,
                vec![Item {
                    message: Message::text(MessageRole::User, "prior chat history"),
                    metadata: BTreeMap::new(),
                }],
            )
            .await
            .unwrap();

        let orchestrator = HistoryCountingOrchestrator;
        let deps = RunnerDeps {
            orchestrator: &orchestrator,
            session: &session,
            memory_manager: None,
            hooks: None,
            max_turns: 4,
            active_agent: AgentId::new("parent"),
            tools: None,
            trace_sink: None,
            task_workspace: None,
            policy: &Policy::default(),
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let mut metadata = BTreeMap::new();
        metadata.insert(
            Arc::from(SESSION_SCOPE_KEY),
            Value::String(SESSION_SCOPE_EPHEMERAL.to_owned()),
        );
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: conversation.clone(),
            sender: Arc::from("cron:digest"),
            message: Message::text(MessageRole::User, "run the digest"),
            metadata,
        };

        let output = match run_envelope(input, RunId::new("cron-digest"), &deps)
            .await
            .expect("ephemeral run should finish")
        {
            RunOutcome::Finished { output, .. } => output,
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };

        // The orchestrator saw only the cron input, not the seeded history...
        assert_eq!(output.message.content.as_ref(), "saw 1 items");
        // ...output still delivers to the original conversation...
        assert_eq!(output.conversation_id, conversation);
        // ...and nothing was written back to the shared session.
        let transcript = session.load(&principal).await.unwrap();
        assert_eq!(transcript.items.len(), 1, "ephemeral run polluted session");

        // Contrast: the default scope still loads and persists history.
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: conversation.clone(),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "hello"),
            metadata: BTreeMap::new(),
        };
        let output = match run_envelope(input, RunId::new("chat-run"), &deps)
            .await
            .expect("default-scoped run should finish")
        {
            RunOutcome::Finished { output, .. } => output,
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };
        assert_eq!(output.message.content.as_ref(), "saw 2 items");
        let transcript = session.load(&principal).await.unwrap();
        assert_eq!(transcript.items.len(), 3, "seed + input + reply expected");
    }

    /// Replies with the number of transcript items visible to the planner, so
    /// tests can assert exactly how much history a run was hydrated with.
    struct HistoryCountingOrchestrator;

    #[async_trait]
    impl Orchestrator for HistoryCountingOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                format!("saw {} items", ctx.state.transcript.items.len()),
            )))
        }
    }

    struct ParentDelegateOrchestrator;

    #[async_trait]
    impl Orchestrator for ParentDelegateOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let Some(item) = ctx.state.transcript.items.last() else {
                return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
            };
            match item.message.role {
                MessageRole::User => Ok(Plan::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("child"),
                    policy_id: Arc::from("child-policy"),
                    metadata: BTreeMap::new(),
                })),
                MessageRole::Tool => Ok(Plan::Reply(Message::text(
                    MessageRole::Assistant,
                    format!("parent saw: {}", item.message.content),
                ))),
                MessageRole::Assistant | MessageRole::System => {
                    Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")))
                }
            }
        }
    }

    struct ChildApprovalOrchestrator;

    #[async_trait]
    impl Orchestrator for ChildApprovalOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let Some(item) = ctx.state.transcript.items.last() else {
                return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
            };
            match item.message.role {
                MessageRole::User => {
                    let args = RawValue::from_string(json!({ "ok": true }).to_string()).unwrap();
                    Ok(Plan::CallTool(ToolCall {
                        id: ToolCallId::new("child-mock"),
                        name: Arc::from("mock"),
                        args,
                    }))
                }
                MessageRole::Tool => Ok(Plan::Reply(Message::text(
                    MessageRole::Assistant,
                    "child finished",
                ))),
                MessageRole::Assistant | MessageRole::System => {
                    Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")))
                }
            }
        }
    }

    struct ToolThenReplyOrchestrator;

    #[async_trait]
    impl Orchestrator for ToolThenReplyOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let Some(item) = ctx.state.transcript.items.last() else {
                return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
            };
            match item.message.role {
                MessageRole::User => {
                    let args = RawValue::from_string(json!({ "ok": true }).to_string()).unwrap();
                    Ok(Plan::CallTool(ToolCall {
                        id: ToolCallId::new("guarded-mock"),
                        name: Arc::from("mock"),
                        args,
                    }))
                }
                MessageRole::Tool => Ok(Plan::Reply(Message::text(
                    MessageRole::Assistant,
                    format!("tool result: {}", item.message.content),
                ))),
                MessageRole::Assistant | MessageRole::System => {
                    Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")))
                }
            }
        }
    }

    /// Never replies — always asks for another tool call. Without the
    /// turn-budget safeguard this loops until `MaxTurnsExceeded`.
    struct AlwaysToolOrchestrator;

    #[async_trait]
    impl Orchestrator for AlwaysToolOrchestrator {
        async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let args = RawValue::from_string(json!({ "ok": true }).to_string()).unwrap();
            Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("loop-mock"),
                name: Arc::from("mock"),
                args,
            }))
        }
    }

    struct DenyMockToolGuardrail;

    #[async_trait]
    impl ToolGuardrail for DenyMockToolGuardrail {
        async fn check_call(
            &self,
            call: &ToolCall,
            _ctx: &RunContext<'_>,
        ) -> Result<GuardrailOutcome, GuardrailError> {
            if call.name.as_ref() == "mock" {
                Ok(GuardrailOutcome::Tripped(Arc::from("blocked by test")))
            } else {
                Ok(GuardrailOutcome::Passed)
            }
        }

        async fn check_result(
            &self,
            _result: &ToolResult,
            _ctx: &RunContext<'_>,
        ) -> Result<GuardrailOutcome, GuardrailError> {
            Ok(GuardrailOutcome::Passed)
        }
    }

    struct MockApprovalTool;

    #[async_trait]
    impl Tool for MockApprovalTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: Arc::from("mock"),
                description: Arc::from("mock approval tool"),
                input_schema: json!({"type": "object"}),
                safety: Default::default(),
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

    /// Plan::CallTool on the first turn, Plan::Reply after the tool result —
    /// and pushes a distinct token usage sample onto `ctx.usage_sink` for each
    /// LLM round-trip. Used to verify that the loop records usage for the
    /// tool-calling turn, not just the final reply.
    struct UsageReportingOrchestrator;

    #[async_trait]
    impl Orchestrator for UsageReportingOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let Some(item) = ctx.state.transcript.items.last() else {
                return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
            };
            match item.message.role {
                MessageRole::User => {
                    ctx.push_llm_usage(agentos_proto::Usage {
                        input_tokens: 1000,
                        output_tokens: 20,
                        total_tokens: 1020,
                        cache_read_tokens: 800,
                        cache_write_tokens: 0,
                        cache_miss_tokens: 200,
                        tool_calls: 0,
                    });
                    let args = RawValue::from_string(json!({ "ok": true }).to_string()).unwrap();
                    Ok(Plan::CallTool(ToolCall {
                        id: ToolCallId::new("usage-mock"),
                        name: Arc::from("mock"),
                        args,
                    }))
                }
                MessageRole::Tool => {
                    ctx.push_llm_usage(agentos_proto::Usage {
                        input_tokens: 50,
                        output_tokens: 7,
                        total_tokens: 57,
                        cache_read_tokens: 50,
                        cache_write_tokens: 0,
                        cache_miss_tokens: 0,
                        tool_calls: 0,
                    });
                    Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
                }
                MessageRole::Assistant | MessageRole::System => {
                    Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")))
                }
            }
        }
    }

    #[tokio::test]
    async fn loop_records_llm_usage_for_tool_calling_turns_not_just_replies() {
        // Regression: `record_llm_usage` used to gate on `Plan::Reply` and drop
        // every tool-calling LLM round's tokens on the floor. The local audit
        // therefore under-reported by however many tool rounds happened. The
        // loop now drains a `usage_sink` populated by the orchestrator after
        // each LLM call, regardless of which `Plan` variant follows.
        let session = InMemorySession::default();
        let orchestrator = UsageReportingOrchestrator;
        let mut tools = ToolRegistry::new();
        tools.register(MockApprovalTool);
        let deps = RunnerDeps {
            orchestrator: &orchestrator,
            session: &session,
            memory_manager: None,
            hooks: None,
            max_turns: 4,
            active_agent: AgentId::new("parent"),
            tools: Some(&tools),
            trace_sink: None,
            task_workspace: None,
            policy: &Policy::allow_tools(["mock"]),
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            compaction: Default::default(),
            cancel: Default::default(),
            steering: None,
            run_journal: None,
            safety_log: None,
            delegated_authority: None,
        };
        let input = Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "run tool"),
            metadata: BTreeMap::new(),
        };

        let (output, state) = match run_envelope(input, RunId::new("usage-run"), &deps)
            .await
            .expect("run should finish")
        {
            RunOutcome::Finished { output, state } => (output, state),
            RunOutcome::Paused(_) => panic!("expected finished run"),
        };

        assert_eq!(output.message.content.as_ref(), "done");

        // Both calls folded into the run total: 1000 + 50 = 1050 input, etc.
        assert_eq!(state.usage.input_tokens, 1050);
        assert_eq!(state.usage.output_tokens, 27);
        assert_eq!(state.usage.cache_read_tokens, 850);
        assert_eq!(state.usage.cache_miss_tokens, 200);
        assert_eq!(state.usage.tool_calls, 2);

        // One trace event per LLM round-trip — verifies the tool-calling round
        // is no longer silently dropped.
        let llm_events = state
            .trace_events
            .iter()
            .filter(|e| e.name.as_ref() == "llm_token_usage")
            .count();
        assert_eq!(
            llm_events, 2,
            "expected 2 llm_token_usage trace events (one per LLM call), got {llm_events}"
        );
    }
}
