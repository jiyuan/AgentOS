//! The shard side of the sharded gateway (roadmap item G1).
//!
//! `main.rs` is the router: it receives envelopes and hands each to the shard
//! that owns its conversation. This module is what those shards do — own a
//! `current_thread` runtime, and run one conversation's turns on it while the
//! router keeps receiving for everyone else.

use super::run_memory_reflection;
use super::{
    build_reflection_cron, channel_display_name, channel_stream_sink, command_reply_envelope,
    failure_envelope, fire_due_crons, log_line, log_trace, ServiceConfig, CRON_SCAN_INTERVAL_SECS,
};
use agentos_cli::slash::{self, Parsed, SessionUsage, SlashCommand, SlashContext};
use agentos_core::crons::{CronStore, MemoryMaintenanceCron};
use agentos_core::gateway::{
    run_shard, unix_now, GatewayRun, GatewayService, ShardConfig, ShardInbound, Turn, TurnHandler,
};
use agentos_core::r#loop::{
    route, ApprovalOutcome, ApprovalTicket, InputGuardrailEntry, OutputGuardrailEntry, Routed,
    ToolGuardrailEntry,
};
use agentos_core::runner::{PausedRun, ResumeDecision, RunnerDeps};
use agentos_core::runtime::{AgentRuntime, RuntimeDepsScope};
use agentos_interfaces::{Egress, StreamEgress};
use agentos_proto::{ConversationId, Envelope, InterruptionId, RunId};
use async_trait::async_trait;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Everything a shard thread needs that it cannot borrow from the router.
pub(super) struct ShardContext {
    pub(super) shard: usize,
    pub(super) config: ServiceConfig,
    pub(super) channel_name: &'static str,
    pub(super) run_id: RunId,
    /// One runtime for the whole gateway, shared by every shard. Not one per
    /// shard: separate runtimes would mean separate session stores, separate
    /// memory, and separate job registries for what is one agent.
    pub(super) runtime: Arc<AgentRuntime>,
    pub(super) egress: Arc<dyn Egress>,
    /// The channel's edit-in-place streaming handle, if it has one. Taken on
    /// the router thread and shared, for the same reason as `egress`: the shard
    /// running the turn does not own the channel.
    pub(super) stream_egress: Option<Arc<dyn StreamEgress>>,
    pub(super) session_usage: Arc<SessionUsage>,
}

/// Own one shard: a `current_thread` runtime, because the run loop's future is
/// `!Send` and must never migrate between threads.
pub(super) fn run_shard_thread(
    context: ShardContext,
    inbound: ShardInbound,
    shard_config: ShardConfig,
) {
    let tokio_runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(tokio_runtime) => tokio_runtime,
        Err(err) => {
            let _ = log_line(
                &context.config,
                &format!(
                    "{} shard {} failed to start its runtime: {err}",
                    context.channel_name, context.shard
                ),
            );
            return;
        }
    };
    tokio_runtime.block_on(async move {
        let deps_scope = context.runtime.deps_scope();
        let input_guardrails = deps_scope.input_guardrails();
        let output_guardrails = deps_scope.output_guardrails();
        let tool_guardrails = deps_scope.tool_guardrails();
        let cron_store = CronStore::new(context.config.home.join("workspace/crons"));
        let reflection_cron = match build_reflection_cron(&context.runtime, &context.config) {
            Ok(schedule) => schedule,
            Err(err) => {
                let _ = log_line(
                    &context.config,
                    &format!("reflection schedule failed: {err}"),
                );
                None
            }
        };
        let handler = ShardTurns {
            context: &context,
            deps_scope: &deps_scope,
            input_guardrails: &input_guardrails,
            output_guardrails: &output_guardrails,
            tool_guardrails: &tool_guardrails,
            cron_store,
            reflection_cron: RefCell::new(reflection_cron),
            last_cron_scan: Cell::new(0),
            pending_approvals: RefCell::new(HashMap::new()),
            expired: RefCell::new(Vec::new()),
        };
        run_shard(context.shard, inbound, &shard_config, &handler).await;
    });
}

/// What the shard should do with one inbound envelope.
enum Answered {
    /// It answered the pending approval; resume the run with this decision.
    /// Boxed because a `PausedRun` carries a whole `RunState`, and every other
    /// variant here is a couple of words.
    Resume(Box<Resume>),
    /// It named a prompt that is over. Tell the sender; run nothing.
    Stale(ApprovalTicket),
    /// It is ordinary input; start a run.
    Run,
}

/// A decided approval, ready to resume.
struct Resume {
    pending: PendingApproval,
    decision: ResumeDecision,
}

/// A run parked on an approval, and what it takes to answer it.
struct PendingApproval {
    paused: PausedRun,
    /// Names this asking. An envelope that does not carry it back is ordinary
    /// input, however affirmative it sounds.
    ticket: ApprovalTicket,
    /// Unix seconds after which the prompt stops counting, if it expires.
    expires_at: Option<u64>,
}

impl PendingApproval {
    fn is_expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

/// Runs one conversation's turns on its shard thread.
struct ShardTurns<'a> {
    context: &'a ShardContext,
    deps_scope: &'a RuntimeDepsScope<'a>,
    input_guardrails: &'a [InputGuardrailEntry<'a>],
    output_guardrails: &'a [OutputGuardrailEntry<'a>],
    tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    cron_store: CronStore,
    reflection_cron: RefCell<Option<MemoryMaintenanceCron>>,
    last_cron_scan: Cell<u64>,
    /// Runs waiting on an approval, keyed by conversation.
    ///
    /// The serial loop used to answer an approval by blocking on
    /// `channel.receive()`, which froze the whole gateway until that one user
    /// replied. The paused run is parked here instead, so every other
    /// conversation keeps running, and only an envelope carrying this prompt's
    /// ticket decides it (roadmap G2).
    pending_approvals: RefCell<HashMap<ConversationId, PendingApproval>>,
    /// Prompts that have expired and whose runs still need finishing. Resuming
    /// is `async`; expiry is noticed while the map above is borrowed.
    expired: RefCell<Vec<(PendingApproval, InterruptionId)>>,
}

#[async_trait(?Send)]
impl TurnHandler for ShardTurns<'_> {
    async fn run(&self, turn: Turn) {
        if let Err(err) = self.handle(turn).await {
            let _ = log_line(
                &self.context.config,
                &format!("{} turn failed: {err}", self.context.channel_name),
            );
        }
    }

    async fn idle(&self, shard: usize) {
        // Approval expiry is per-shard state: each shard holds the prompts for
        // its own conversations, so every shard sweeps its own.
        if let Err(err) = self.sweep_expired() {
            let _ = log_line(
                &self.context.config,
                &format!("{} approval sweep failed: {err}", self.context.channel_name),
            );
        }
        if let Err(err) = self.drain_expired().await {
            let _ = log_line(
                &self.context.config,
                &format!(
                    "{} approval expiry failed: {err}",
                    self.context.channel_name
                ),
            );
        }
        // Cron and reflection are process-wide, not per-shard: firing them on
        // every shard would fire every task N times.
        if shard != 0 {
            return;
        }
        if let Err(err) = self.maintenance().await {
            let _ = log_line(
                &self.context.config,
                &format!("{} maintenance failed: {err}", self.context.channel_name),
            );
        }
    }
}

impl ShardTurns<'_> {
    async fn handle(&self, turn: Turn) -> Result<(), String> {
        let config = &self.context.config;
        let channel_name = self.context.channel_name;
        let egress = self.context.egress.as_ref();
        let mut input = turn.input;

        match slash::parse(&input.message.content) {
            Parsed::Cmd(SlashCommand::Clear) => {
                return self.clear_conversation(&input).await;
            }
            Parsed::Cmd(cmd) => {
                let ctx = SlashContext {
                    skill_catalog: &self.context.runtime.skill_catalog,
                    cron_store: &self.cron_store,
                    tool_registry: self.deps_scope.deps().tools,
                    memory_manager: self.context.runtime.memory_manager.as_ref(),
                    orchestrator_handle: Some(&self.context.runtime.orchestrator.strategy_handle()),
                    model_controller: Some(&self.context.runtime.model_controller),
                    session_usage: Some(&self.context.session_usage),
                    agent_id: &self.context.runtime.active_agent,
                    conversation_id: &input.conversation_id,
                };
                let reply = slash::render(cmd, &ctx).await;
                return self.reply(&input, &reply).await;
            }
            Parsed::Usage(hint) => return self.reply(&input, &hint).await,
            Parsed::NotSlash => {}
        }

        input.metadata.insert(
            Arc::from("task_id"),
            serde_json::json!(self
                .context
                .runtime
                .orchestrator
                .current_strategy()
                .task_id()),
        );

        // Per-turn deps: this turn's cancellation token (so `/stop` reaches it)
        // and its steering queue (so a message arriving mid-run is claimed at
        // the next `Plan` instead of starting a second, racing run).
        let mut deps = self.deps_scope.deps_with_guardrails(
            self.input_guardrails,
            self.output_guardrails,
            self.tool_guardrails,
        );
        deps.cancel = turn.cancel;
        deps.steering = Some(turn.steering);
        if let Some(stream) = self.context.stream_egress.clone() {
            deps.stream_sink = Some(channel_stream_sink(stream, input.conversation_id.clone()));
        }
        let service = self.service(&deps);

        // Does this envelope answer the approval this conversation is waiting
        // on? Only an envelope carrying that prompt's ticket does.
        let outcome = match self.decide(&input)? {
            Answered::Resume(resume) => {
                let Resume { pending, decision } = *resume;
                let Some(approval_id) = pending
                    .paused
                    .state
                    .pending_approvals
                    .first()
                    .map(|approval| approval.id.clone())
                else {
                    log_line(
                        config,
                        &format!("{channel_name} paused run had no approval"),
                    )?;
                    return Ok(());
                };
                log_line(
                    config,
                    &format!(
                        "{channel_name} approval {} ({}) resolved: {}",
                        approval_id.as_str(),
                        pending.ticket,
                        decision.outcome().as_str()
                    ),
                )?;
                service
                    .resume(egress, pending.paused, &approval_id, decision)
                    .await
            }
            Answered::Stale(ticket) => {
                // A button from an asking that is over. Say so rather than
                // running it as a message: the sender pressed a control.
                return self
                    .reply(
                        &input,
                        &format!(
                            "That approval ({ticket}) is no longer waiting — it was already \
                             answered or it expired."
                        ),
                    )
                    .await;
            }
            Answered::Run => {
                service
                    .run_envelope(egress, input.clone(), self.context.run_id.clone())
                    .await
            }
        };

        match outcome {
            Ok(GatewayRun::Finished { state, .. }) => {
                self.context.session_usage.record_run(&state.usage);
                log_trace(config, &state)?;
            }
            Ok(GatewayRun::Paused {
                paused,
                ticket,
                expires_at,
                ..
            }) => {
                // Park it and go. Only an answer carrying `ticket` resumes it,
                // and the idle sweep cancels it if nobody ever does.
                self.pending_approvals.borrow_mut().insert(
                    paused.conversation_id.clone(),
                    PendingApproval {
                        paused,
                        ticket,
                        expires_at,
                    },
                );
            }
            Err(err) => {
                log_line(config, &format!("{channel_name} gateway run failed: {err}"))?;
                let reply = failure_envelope(
                    &input,
                    self.context.runtime.active_agent.as_str(),
                    &err.to_string(),
                );
                if let Err(send_err) = egress.send(reply).await {
                    log_line(
                        config,
                        &format!("{channel_name} gateway failed to send error reply: {send_err}"),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// A gateway service carrying this deployment's approval expiry, so every
    /// prompt this shard sends has a window and none outlives the run it gates.
    fn service<'d>(&self, deps: &'d RunnerDeps<'d>) -> GatewayService<'d> {
        GatewayService::new(deps, Arc::from(self.context.runtime.active_agent.as_str()))
            .with_approval_expiry(self.context.runtime.workspace_config.approval.expiry())
    }

    /// Route `input` against whatever this conversation is waiting on.
    ///
    /// Expiry is checked first: a prompt whose window has closed is over
    /// before the envelope is even read, so a late answer cannot decide it.
    fn decide(&self, input: &Envelope) -> Result<Answered, String> {
        let now = unix_now();
        let mut pending = self.pending_approvals.borrow_mut();
        if pending
            .get(&input.conversation_id)
            .is_some_and(|entry| entry.is_expired(now))
        {
            let entry = pending
                .remove(&input.conversation_id)
                .expect("checked just above");
            drop(pending);
            self.expire(entry)?;
            // The prompt is gone, so this envelope cannot decide it — a late
            // answer is stale, and anything else is ordinary input.
            return Ok(match route(None, input) {
                Routed::Stale { ticket } => Answered::Stale(ticket),
                // With nothing pending, `route` cannot report a decision.
                Routed::Unrelated | Routed::Decides { .. } => Answered::Run,
            });
        }

        let ticket = pending
            .get(&input.conversation_id)
            .map(|entry| entry.ticket.clone());
        match route(ticket.as_ref(), input) {
            Routed::Unrelated => Ok(Answered::Run),
            Routed::Stale { ticket } => Ok(Answered::Stale(ticket)),
            Routed::Decides { outcome, reason } => {
                let entry = pending
                    .remove(&input.conversation_id)
                    .expect("a decision implies a pending approval");
                let decision = match outcome {
                    ApprovalOutcome::Approved => ResumeDecision::Approve,
                    _ => ResumeDecision::Reject {
                        reason: reason.unwrap_or_else(|| {
                            Arc::from(format!("rejected by {} user", self.context.channel_name))
                        }),
                    },
                };
                Ok(Answered::Resume(Box::new(Resume {
                    pending: entry,
                    decision,
                })))
            }
        }
    }

    /// Resolve a prompt nobody answered in time.
    ///
    /// Recorded as *cancelled*, not rejected: the action does not run either
    /// way, but the audit trail — and the memory the agent recalls later — must
    /// not learn that this user refuses things they never saw.
    fn expire(&self, pending: PendingApproval) -> Result<(), String> {
        let config = &self.context.config;
        let channel_name = self.context.channel_name;
        let Some(approval_id) = pending
            .paused
            .state
            .pending_approvals
            .first()
            .map(|approval| approval.id.clone())
        else {
            return Ok(());
        };
        log_line(
            config,
            &format!(
                "{channel_name} approval {} ({}) resolved: {}",
                approval_id.as_str(),
                pending.ticket,
                ApprovalOutcome::Cancelled.as_str()
            ),
        )?;
        self.expired.borrow_mut().push((pending, approval_id));
        Ok(())
    }

    /// Finish the runs whose prompts expired.
    ///
    /// Separate from [`ShardTurns::expire`] because resuming a run is `async`
    /// and `expire` is called while the pending-approval map is borrowed.
    async fn drain_expired(&self) -> Result<(), String> {
        loop {
            let Some((pending, approval_id)) = self.expired.borrow_mut().pop() else {
                return Ok(());
            };
            let deps = self.deps_scope.deps_with_guardrails(
                self.input_guardrails,
                self.output_guardrails,
                self.tool_guardrails,
            );
            let resumed = self
                .service(&deps)
                .resume(
                    self.context.egress.as_ref(),
                    pending.paused,
                    &approval_id,
                    ResumeDecision::Cancel {
                        reason: Arc::from("approval prompt expired before it was answered"),
                    },
                )
                .await;
            // Cancelling fails the run closed, so an error here is the
            // expected shape, not a problem: report it and move on.
            if let Err(err) = resumed {
                log_line(
                    &self.context.config,
                    &format!(
                        "{} approval {} expired: {err}",
                        self.context.channel_name,
                        approval_id.as_str()
                    ),
                )?;
            }
        }
    }

    /// Retire every prompt on this shard whose window has closed.
    fn sweep_expired(&self) -> Result<(), String> {
        let now = unix_now();
        let expired: Vec<ConversationId> = self
            .pending_approvals
            .borrow()
            .iter()
            .filter(|(_, entry)| entry.is_expired(now))
            .map(|(conversation, _)| conversation.clone())
            .collect();
        for conversation in expired {
            let Some(entry) = self.pending_approvals.borrow_mut().remove(&conversation) else {
                continue;
            };
            self.expire(entry)?;
        }
        Ok(())
    }

    /// `/clear` ends a conversation, which is the moment D3's job registry has
    /// been waiting for: its background jobs belong to a conversation that no
    /// longer exists, so they are cancelled and their entries reclaimed.
    async fn clear_conversation(&self, input: &Envelope) -> Result<(), String> {
        let config = &self.context.config;
        let channel_name = self.context.channel_name;
        let removed = self
            .context
            .runtime
            .session
            .clear_session(&input.conversation_id)
            .map_err(|err| format!("failed to clear {channel_name} conversation history: {err}"))?;
        let jobs = self
            .context
            .runtime
            .jobs()
            .dispose_conversation(&input.conversation_id);
        log_line(
            config,
            &format!(
                "{channel_name} conversation {} cleared ({removed} items, {jobs} job(s))",
                input.conversation_id.as_str()
            ),
        )?;
        let reply = slash::format_clear(removed, channel_display_name(channel_name));
        self.reply(input, &reply).await
    }

    async fn reply(&self, input: &Envelope, text: &str) -> Result<(), String> {
        let envelope =
            command_reply_envelope(input, self.context.runtime.active_agent.as_str(), text);
        if let Err(err) = self.context.egress.send(envelope).await {
            log_line(
                &self.context.config,
                &format!(
                    "{} gateway failed to send reply: {err}",
                    self.context.channel_name
                ),
            )?;
        }
        Ok(())
    }

    /// The idle phase: cron scan and memory reflection, throttled to
    /// `CRON_SCAN_INTERVAL_SECS` because cron resolution is one minute.
    async fn maintenance(&self) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(self.last_cron_scan.get()) < CRON_SCAN_INTERVAL_SECS {
            return Ok(());
        }
        self.last_cron_scan.set(now);

        // Cron runs go straight through the runner rather than the router.
        // They are safe off-shard because a cron envelope carries
        // `session_scope = ephemeral`: it neither loads nor writes back the
        // conversation transcript, so it cannot race a user's run on the same
        // conversation for the state that sharding exists to protect.
        let deps = self.deps_scope.deps_with_guardrails(
            self.input_guardrails,
            self.output_guardrails,
            self.tool_guardrails,
        );
        let service = self.service(&deps);
        fire_due_crons(
            &self.context.config,
            self.context.egress.as_ref(),
            self.context.channel_name,
            &self.cron_store,
            &service,
            &self.context.session_usage,
            now,
        )
        .await?;
        // Take the schedule out for the call rather than holding the borrow
        // across the await, and put it back afterwards: the schedule carries
        // the last-fired bookkeeping, so dropping it would re-fire reflection
        // on every idle tick.
        let mut reflection = self.reflection_cron.borrow_mut().take();
        let reflected = run_memory_reflection(
            &self.context.config,
            self.context.channel_name,
            reflection.as_mut(),
            &self.context.runtime.memory_manager,
            now,
        )
        .await;
        *self.reflection_cron.borrow_mut() = reflection;
        reflected
    }
}
