//! The shard side of the sharded gateway (roadmap item G1).
//!
//! `main.rs` is the router: it receives envelopes and hands each to the shard
//! that owns its conversation. This module is what those shards do — own a
//! `current_thread` runtime, and run one conversation's turns on it while the
//! router keeps receiving for everyone else.

use super::approval::{restore, PendingApproval};
use super::delivery::{send_approval_prompt, DurableEgress};
use super::{
    channel_display_name, channel_stream_sink, command_reply_envelope, failure_envelope, log_line,
    log_trace, ServiceConfig,
};
use agentos_cli::slash::{self, Parsed, SessionUsage, SlashCommand, SlashContext};
use agentos_core::crons::CronStore;
use agentos_core::gateway::{
    run_shard, unix_now, ApprovalStore, DurableApproval, GatewayError, GatewayRun, GatewayService,
    IngressEventKey, IngressLedger, ShardConfig, ShardInbound, Turn, TurnHandler,
};
use agentos_core::r#loop::{
    route, ApprovalBinding, ApprovalOutcome, ApprovalTicket, InputGuardrailEntry,
    OutputGuardrailEntry, ResumeWitness, Routed, RunError, ToolGuardrailEntry,
};
use agentos_core::runner::{RunnerDeps, RunnerError};
use agentos_core::runtime::{AgentRuntime, RuntimeDepsScope};
use agentos_interfaces::{Egress, StreamEgress};
use agentos_proto::{ActorPrincipal, ConversationId, Envelope, InterruptionId, RunId};
use async_trait::async_trait;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Everything a shard thread needs that it cannot borrow from the router.
pub(super) struct ShardContext {
    pub(super) shard: usize,
    pub(super) config: ServiceConfig,
    pub(super) channel_name: &'static str,
    pub(super) run_id: RunId,
    /// One runtime for the serving process, shared by every channel and shard.
    /// Separate runtimes would mean separate session pools, memory, jobs, and
    /// MCP lifecycle registries for what is one agent.
    pub(super) runtime: Arc<AgentRuntime>,
    pub(super) egress: Arc<dyn Egress>,
    /// The channel's edit-in-place streaming handle, if it has one. Taken on
    /// the router thread and shared, for the same reason as `egress`: the shard
    /// running the turn does not own the channel.
    pub(super) stream_egress: Option<Arc<dyn StreamEgress>>,
    pub(super) session_usage: Arc<SessionUsage>,
    /// Where this channel's accepted events are recorded. The router admits
    /// through it; the shard settles into it once the turn is over
    /// (M8 / `GW-001`, deliverable 3).
    pub(super) ledger: Arc<IngressLedger>,
    pub(super) approval_store: Arc<ApprovalStore>,
    /// Validated pauses assigned to this shard during startup recovery.
    pub(super) restored_approvals: Vec<DurableApproval>,
}

/// Own one shard: a `current_thread` runtime, because the run loop's future is
/// `!Send` and must never migrate between threads.
pub(super) fn run_shard_thread(
    mut context: ShardContext,
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
        let restored = restore(std::mem::take(&mut context.restored_approvals));
        let handler = ShardTurns {
            context: &context,
            deps_scope: &deps_scope,
            input_guardrails: &input_guardrails,
            output_guardrails: &output_guardrails,
            tool_guardrails: &tool_guardrails,
            cron_store,
            pending_approvals: RefCell::new(restored),
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
    /// It named the live prompt, from someone it was not put to. Tell the
    /// sender; run nothing, and above all decide nothing.
    NotYours(ApprovalTicket),
    /// It is ordinary input; start a run.
    Run,
}

/// A decided approval, ready to resume.
struct Resume {
    pending: PendingApproval,
    witness: ResumeWitness,
}

/// Runs one conversation's turns on its shard thread.
struct ShardTurns<'a> {
    context: &'a ShardContext,
    deps_scope: &'a RuntimeDepsScope<'a>,
    input_guardrails: &'a [InputGuardrailEntry<'a>],
    output_guardrails: &'a [OutputGuardrailEntry<'a>],
    tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    cron_store: CronStore,
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
        let primary = match IngressEventKey::from_envelope(&turn.input) {
            Ok(primary) => primary,
            Err(err) => {
                let _ = log_line(
                    &self.context.config,
                    &format!(
                        "{} turn has no durable identity: {err}",
                        self.context.channel_name
                    ),
                );
                return;
            }
        };
        let durable_egress = DurableEgress::new(
            &self.context.ledger,
            self.context.egress.as_ref(),
            primary,
            turn.steering.clone(),
        );

        if let Err(err) = self.handle(turn, &durable_egress).await {
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
        let _ = shard;
    }
}

impl ShardTurns<'_> {
    /// Senders who may answer any prompt, from `[policy]
    /// approval_administrators`. Empty in the usual case, which is what makes
    /// a prompt answerable only by the person it was put to.
    fn approval_administrators(&self) -> Vec<ActorPrincipal> {
        self.context
            .runtime
            .workspace_config
            .policy
            .approval_administrators
            .iter()
            .map(|administrator| administrator.actor())
            .collect()
    }

    async fn handle(&self, turn: Turn, egress: &DurableEgress<'_>) -> Result<(), String> {
        let config = &self.context.config;
        let channel_name = self.context.channel_name;
        let mut input = turn.input;

        match slash::parse(&input.message.content) {
            Parsed::Cmd(SlashCommand::Clear) => {
                return self.clear_conversation(egress, &input).await;
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
                    channel_id: &input.channel_id,
                    conversation_id: &input.conversation_id,
                };
                let reply = slash::render(cmd, &ctx).await;
                return self.reply(egress, &input, &reply).await;
            }
            Parsed::Usage(hint) => return self.reply(egress, &input, &hint).await,
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

        let answered = self.decide(&input, egress.primary())?;
        let resolving_instance = match &answered {
            Answered::Resume(resume) => Some(Arc::<str>::from(
                resume.pending.binding.approval_instance_id.as_str(),
            )),
            Answered::Stale(_) | Answered::NotYours(_) | Answered::Run => None,
        };
        if let Answered::Resume(resume) = &answered {
            egress.include(&resume.pending.ingress_events);
        }

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
        deps.run_journal = Some(
            self.context
                .ledger
                .run_journal_for(egress.journal_events())
                .map_err(|err| format!("durable run journal failed: {err}"))?,
        );
        // Only when the deployment opted in. A chat channel streams
        // edit-in-place, so it *can* revise what it showed — but the bytes
        // were still delivered to a device before any output guardrail ran
        // (M6 / `STATE-001`, ADR-0007).
        if self.context.runtime.provisional_streaming() {
            if let Some(stream) = self.context.stream_egress.clone() {
                deps.stream_sink = Some(channel_stream_sink(stream, input.conversation_id.clone()));
            }
        }
        let service = self.service(&deps);

        // Does this envelope answer the approval this conversation is waiting
        // on? Only an envelope carrying that prompt's ticket does.
        let outcome = match answered {
            Answered::Resume(resume) => {
                let Resume { pending, witness } = *resume;
                let Some(approval_id) = pending
                    .paused
                    .state
                    .pending_approval()
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
                        pending.binding.ticket,
                        witness.outcome().as_str()
                    ),
                )?;
                service.resume_buffered(pending.paused, witness).await
            }
            Answered::NotYours(ticket) => {
                return self
                    .reply(
                        egress,
                        &input,
                        &format!(
                            "Approval {ticket} was put to someone else in this conversation, \
                             so only they can answer it."
                        ),
                    )
                    .await;
            }
            Answered::Stale(ticket) => {
                // A button from an asking that is over. Say so rather than
                // running it as a message: the sender pressed a control.
                return self
                    .reply(
                        egress,
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
                    .run_envelope_buffered(input.clone(), self.context.run_id.clone())
                    .await
            }
        };

        match outcome {
            Ok(GatewayRun::Finished { state, output }) => {
                if let Some(instance) = &resolving_instance {
                    egress.consume_approval(Arc::clone(instance));
                }
                egress
                    .send(output)
                    .await
                    .map_err(|err| format!("terminal delivery failed: {err}"))?;
                self.context.session_usage.record_run(&state.usage);
                log_trace(config, &state)?;
            }
            Ok(GatewayRun::Paused {
                paused,
                prompt,
                ticket,
                expires_at,
                ..
            }) => {
                let approval = paused
                    .state
                    .pending_approval()
                    .ok_or_else(|| "paused run had no approval".to_owned())?;
                let binding = ApprovalBinding::new(
                    approval.approval_instance_id.clone(),
                    ticket,
                    approval.id.clone(),
                    approval.prompting_principal.clone(),
                    expires_at,
                )
                .ok_or_else(|| "approval instance did not match ticket".to_owned())?
                .with_administrators(self.approval_administrators());
                let events = egress
                    .take_events()
                    .map_err(|err| format!("approval ingress group failed: {err}"))?;
                let prompt = prompt.ok_or_else(|| "paused run had no prompt".to_owned())?;
                let batch = self
                    .context
                    .approval_store
                    .prepare_pause(
                        &paused,
                        &binding,
                        &events,
                        prompt,
                        resolving_instance.as_deref(),
                    )
                    .map_err(|err| format!("approval persistence failed: {err}"))?;
                send_approval_prompt(
                    &self.context.approval_store,
                    self.context.egress.as_ref(),
                    binding.approval_instance_id.as_str(),
                    &batch,
                )
                .await
                .map_err(|err| format!("approval prompt delivery failed: {err}"))?;
                // Park it and go. Only an answer carrying the persisted ticket
                // resumes it; restart reconstructs this map from SQLite.
                self.pending_approvals.borrow_mut().insert(
                    paused.conversation_id.clone(),
                    PendingApproval {
                        ingress_events: events,
                        binding,
                        paused,
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
                if let Some(instance) = &resolving_instance {
                    egress.consume_approval(Arc::clone(instance));
                }
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
    fn decide(&self, input: &Envelope, event: &IngressEventKey) -> Result<Answered, String> {
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
                // With nothing pending, `route` can neither report a decision
                // nor find a prompt this sender was not asked.
                Routed::Unrelated | Routed::Decides { .. } | Routed::NotYours { .. } => {
                    Answered::Run
                }
            });
        }

        let binding = pending
            .get(&input.conversation_id)
            .map(|entry| entry.binding.clone());
        match route(binding.as_ref(), input) {
            Routed::Unrelated => Ok(Answered::Run),
            Routed::Stale { ticket } => Ok(Answered::Stale(ticket)),
            Routed::NotYours { ticket } => Ok(Answered::NotYours(ticket)),
            Routed::Decides { witness } => {
                let instance = binding
                    .as_ref()
                    .expect("a decision implies a pending binding")
                    .approval_instance_id
                    .clone();
                if !self
                    .context
                    .approval_store
                    .begin_resolution(instance.as_str(), event)
                    .map_err(|err| format!("approval resolution claim failed: {err}"))?
                {
                    return Ok(Answered::Stale(
                        binding
                            .expect("a decision implies a pending binding")
                            .ticket,
                    ));
                }
                let entry = pending
                    .remove(&input.conversation_id)
                    .expect("a decision implies a pending approval");
                Ok(Answered::Resume(Box::new(Resume {
                    pending: entry,
                    witness,
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
            .pending_approval()
            .map(|approval| approval.id.clone())
        else {
            return Ok(());
        };
        if !self
            .context
            .approval_store
            .begin_unanswered(pending.binding.approval_instance_id.as_str())
            .map_err(|err| format!("approval expiry claim failed: {err}"))?
        {
            return Err("approval expiry was already claimed".to_owned());
        }
        log_line(
            config,
            &format!(
                "{channel_name} approval {} ({}) resolved: {}",
                approval_id.as_str(),
                pending.binding.ticket,
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
            let resolver = ActorPrincipal::new(
                pending.paused.state.active_agent.clone(),
                pending.paused.channel_id.clone(),
                pending.paused.conversation_id.clone(),
                Arc::from("agentos-system"),
            );
            let Some(witness) = pending.binding.unanswered_witness(
                resolver,
                ApprovalOutcome::Cancelled,
                Arc::from("approval prompt expired before it was answered"),
            ) else {
                return Err("could not create expiry witness".to_owned());
            };
            let resumed = self
                .service(&deps)
                .resume(
                    self.context.egress.as_ref(),
                    pending.paused.clone(),
                    witness,
                )
                .await;
            match resumed {
                Err(GatewayError::Runner(RunnerError::Run(RunError::ApprovalUnanswered {
                    ..
                }))) => {
                    log_line(
                        &self.context.config,
                        &format!(
                            "{} approval {} expired unanswered",
                            self.context.channel_name,
                            approval_id.as_str()
                        ),
                    )?;
                }
                Ok(_) | Err(_) => {
                    self.context
                        .approval_store
                        .abort_unanswered(pending.binding.approval_instance_id.as_str())
                        .map_err(|err| format!("approval expiry rollback failed: {err}"))?;
                    self.pending_approvals
                        .borrow_mut()
                        .insert(pending.paused.conversation_id.clone(), pending);
                    return Err("approval expiry did not record its unanswered outcome".to_owned());
                }
            }
            self.context
                .approval_store
                .consume_unanswered(
                    pending.binding.approval_instance_id.as_str(),
                    &pending.ingress_events,
                )
                .map_err(|err| format!("approval expiry completion failed: {err}"))?;
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
    async fn clear_conversation(
        &self,
        egress: &dyn Egress,
        input: &Envelope,
    ) -> Result<(), String> {
        let config = &self.context.config;
        let channel_name = self.context.channel_name;
        // The full principal, sender included: `/clear` moves the epoch of
        // the participant who typed it, and in a group conversation that must
        // not move anybody else's
        // (M3 deliverable 2, [ADR-0006](../../../../../docs/adr/0006-CLEAR_EPOCH.md)).
        let principal = ActorPrincipal::new(
            self.context.runtime.active_agent.clone(),
            input.channel_id.clone(),
            input.conversation_id.clone(),
            Arc::clone(&input.sender),
        );
        let removed = self
            .context
            .runtime
            .session
            .clear_session(principal.as_principal())
            .map_err(|err| format!("failed to clear {channel_name} conversation history: {err}"))?;
        let jobs = self
            .context
            .runtime
            .jobs()
            // Without the sender: the jobs belong to the conversation, and
            // clearing ends all of them, not just the ones this participant
            // started.
            .dispose_conversation(principal.conversation().as_principal());
        log_line(
            config,
            &format!(
                "{channel_name} conversation {} cleared ({removed} items, {jobs} job(s))",
                input.conversation_id.as_str()
            ),
        )?;
        let reply = slash::format_clear(removed, channel_display_name(channel_name));
        self.reply(egress, input, &reply).await
    }

    async fn reply(&self, egress: &dyn Egress, input: &Envelope, text: &str) -> Result<(), String> {
        let envelope =
            command_reply_envelope(input, self.context.runtime.active_agent.as_str(), text);
        egress.send(envelope).await.map_err(|err| {
            format!(
                "{} gateway failed to send reply: {err}",
                self.context.channel_name
            )
        })
    }
}
