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
    run_shard, GatewayRun, GatewayService, ShardConfig, ShardInbound, Turn, TurnHandler,
};
use agentos_core::r#loop::{InputGuardrailEntry, OutputGuardrailEntry, ToolGuardrailEntry};
use agentos_core::runner::{PausedRun, ResumeDecision};
use agentos_core::runtime::{AgentRuntime, RuntimeDepsScope};
use agentos_interfaces::{Egress, StreamEgress};
use agentos_proto::{ConversationId, Envelope, RunId};
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
        };
        run_shard(context.shard, inbound, &shard_config, &handler).await;
    });
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
    /// replied. Here the paused run is parked and the next message on that
    /// conversation is read as the answer, so every other conversation keeps
    /// running. Correlating the answer to a specific prompt is G2's job; until
    /// then the next message decides.
    pending_approvals: RefCell<HashMap<ConversationId, PausedRun>>,
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
        let service =
            GatewayService::new(&deps, Arc::from(self.context.runtime.active_agent.as_str()));

        // A message on a conversation with a paused run answers that approval.
        let pending = self
            .pending_approvals
            .borrow_mut()
            .remove(&input.conversation_id);
        let outcome = match pending {
            Some(paused) => {
                let Some(approval_id) = paused
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
                let decision = if input.message.content.trim().eq_ignore_ascii_case("y") {
                    ResumeDecision::Approve
                } else {
                    ResumeDecision::Reject {
                        reason: Arc::from(format!("rejected by {channel_name} user")),
                    }
                };
                service.resume(egress, paused, &approval_id, decision).await
            }
            None => {
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
            Ok(GatewayRun::Paused { paused, .. }) => {
                // Park it and go; the next message on this conversation decides.
                self.pending_approvals
                    .borrow_mut()
                    .insert(paused.conversation_id.clone(), paused);
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
        let service =
            GatewayService::new(&deps, Arc::from(self.context.runtime.active_agent.as_str()));
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
