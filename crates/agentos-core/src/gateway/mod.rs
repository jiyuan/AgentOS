//! The gateway: what turns inbound envelopes into runs.
//!
//! [`GatewayService`] is the per-run half — guardrails, approval prompts,
//! delivery — and is what a one-shot entrypoint uses directly. [`shard`] and
//! [`inbox`] are the concurrency half (roadmap item G1): conversations as
//! actors, sharded across threads, serial within one. [`ingress`] is the
//! durability half (M8 / `GW-001`): what the gateway has accepted from a
//! transport and how far each of those got, so a crash neither loses an
//! accepted message nor answers a redelivered one twice. [`control`] and
//! [`shutdown`] are the lifecycle half: who is serving, established by a held
//! lock rather than by a pid in a file, and how a `SIGTERM` becomes a bounded
//! drain instead of a turn that ends between two instructions.

mod approval_store;
mod control;
mod control_socket;
mod delivery;
mod inbox;
mod ingress;
mod maintenance;
mod shard;
mod shutdown;

pub use approval_store::{ApprovalStore, DurableApproval, RecoveredApprovals};
pub use control::{holder, read_record, ControlError, ControlFile, ControlRecord};
pub use control_socket::{control_socket_path, ControlEndpoint, ShutdownRequest};
pub use delivery::{
    AmbiguousEvent, DeliveryBatch, DeliveryState, IngressEventKey, RunJournal, DELIVERY_ID_KEY,
};
pub use inbox::{Admitted, Inbox, Requeued, DEFAULT_INBOX_CAPACITY};
pub(crate) use ingress::init_schema as init_ingress_schema;
pub use ingress::{
    AbandonedEvent, Admission, IngressError, IngressLedger, ReplayableEvent, Settlement,
};
pub use maintenance::{
    MaintenanceCadence, MaintenanceScheduleError, MaintenanceTick, MaintenanceTicker,
};
pub use shard::{
    run_shard, shard_set, DeliveryOutcome, RouteError, Router, ShardConfig, ShardInbound, Turn,
    TurnHandler, DEFAULT_IDLE_INTERVAL,
};
pub use shutdown::{
    install_shutdown_handler, receive_or_cancel, request_shutdown, shutdown_requested,
    ProcessShutdown, ReceiveOutcome, DEFAULT_SHUTDOWN_GRACE_SECS,
};

use crate::r#loop::{ApprovalTicket, ResumeWitness, RunError};
use crate::runner::{
    approval_prompt_envelope, resume_run, run_envelope, PausedRun, RunOutcome, RunnerDeps,
    RunnerError,
};
use agentos_interfaces::{Channel, ChannelError, Egress, RunState};
use agentos_proto::{Envelope, RunId};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// How long an approval prompt counts, when the caller does not say.
///
/// Long enough that a user who steps away can still answer, short enough that
/// an abandoned prompt resolves the same day. An expiry is not a nicety: a
/// prompt that never expires is a paused run pinned in memory and an action
/// that could still fire hours after the context that asked for it is gone.
pub const DEFAULT_APPROVAL_EXPIRY: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("channel failed: {0}")]
    Channel(#[from] ChannelError),
    #[error("runner failed: {0}")]
    Runner(#[from] RunnerError),
}

#[derive(Debug)]
pub enum GatewayRun {
    Finished {
        state: RunState,
        output: Envelope,
    },
    Paused {
        paused: PausedRun,
        prompt: Option<Envelope>,
        /// Names this asking (roadmap G2). Whoever holds the paused run must
        /// keep it: an answer that does not carry it back decides nothing.
        ticket: ApprovalTicket,
        /// Unix seconds after which the prompt stops counting, if it expires.
        expires_at: Option<u64>,
    },
}

pub struct GatewayService<'a> {
    deps: &'a RunnerDeps<'a>,
    sender: Arc<str>,
    approval_expiry: Option<Duration>,
}

impl<'a> GatewayService<'a> {
    pub fn new(deps: &'a RunnerDeps<'a>, sender: impl Into<Arc<str>>) -> Self {
        Self {
            deps,
            sender: sender.into(),
            approval_expiry: Some(DEFAULT_APPROVAL_EXPIRY),
        }
    }

    /// How long this service's approval prompts count for. `None` leaves them
    /// open indefinitely, which only makes sense for a one-shot entrypoint
    /// whose process ends with the prompt.
    pub fn with_approval_expiry(mut self, expiry: Option<Duration>) -> Self {
        self.approval_expiry = expiry;
        self
    }

    /// Receive one envelope and run it, replying through the channel's own
    /// egress. For entrypoints that own the channel outright; a sharded gateway
    /// separates the two and calls [`GatewayService::run_envelope`] directly.
    pub async fn receive_and_run<C>(
        &self,
        channel: &mut C,
        run_id: RunId,
    ) -> Result<Option<GatewayRun>, GatewayError>
    where
        C: Channel,
    {
        let Some(inbound) = channel.receive().await else {
            return Ok(None);
        };
        channel.acknowledge(inbound.receipt).await?;
        let egress = channel.egress();
        self.run_envelope(egress.as_ref(), inbound.envelope, run_id)
            .await
            .map(Some)
    }

    /// Run `input` to completion (or to an approval prompt) and deliver the
    /// result through `egress`.
    ///
    /// Takes the send half rather than the channel because a sharded gateway
    /// runs turns on threads that do not own the channel — the router does.
    pub async fn run_envelope(
        &self,
        egress: &dyn Egress,
        input: Envelope,
        run_id: RunId,
    ) -> Result<GatewayRun, GatewayError> {
        let outcome = self.run_envelope_buffered(input, run_id).await?;
        self.deliver(egress, outcome).await
    }

    /// Run without sending terminal output. A durable gateway uses this so it
    /// can commit `reply_pending` before the first transport byte is attempted.
    pub async fn run_envelope_buffered(
        &self,
        input: Envelope,
        run_id: RunId,
    ) -> Result<GatewayRun, GatewayError> {
        let channel_id = input.channel_id.clone();
        let conversation_id = input.conversation_id.clone();
        match run_envelope(input, run_id, self.deps).await? {
            RunOutcome::Finished { state, output } => Ok(GatewayRun::Finished { state, output }),
            RunOutcome::Paused(state) => {
                let paused = PausedRun {
                    channel_id,
                    conversation_id,
                    state,
                };
                self.approval_prompt(paused)
            }
        }
    }

    pub async fn resume(
        &self,
        egress: &dyn Egress,
        paused: PausedRun,
        witness: ResumeWitness,
    ) -> Result<GatewayRun, GatewayError> {
        let outcome = self.resume_buffered(paused, witness).await?;
        self.deliver(egress, outcome).await
    }

    /// Resume without sending terminal output; see [`Self::run_envelope_buffered`].
    pub async fn resume_buffered(
        &self,
        paused: PausedRun,
        witness: ResumeWitness,
    ) -> Result<GatewayRun, GatewayError> {
        let channel_id = paused.channel_id.clone();
        let conversation_id = paused.conversation_id.clone();
        match resume_run(paused, witness, self.deps).await? {
            RunOutcome::Finished { state, output } => Ok(GatewayRun::Finished { state, output }),
            RunOutcome::Paused(state) => {
                let paused = PausedRun {
                    channel_id,
                    conversation_id,
                    state,
                };
                self.approval_prompt(paused)
            }
        }
    }

    fn approval_prompt(&self, paused: PausedRun) -> Result<GatewayRun, GatewayError> {
        let Some(ticket) = paused
            .state
            .pending_approval()
            .and_then(|approval| ApprovalTicket::parse(&approval.approval_ticket))
        else {
            return Err(RunnerError::Run(RunError::NotResumable).into());
        };
        let expires_at = self
            .approval_expiry
            .map(|expiry| unix_now() + expiry.as_secs());
        let prompt = approval_prompt_envelope(&paused, Arc::clone(&self.sender), expires_at);
        Ok(GatewayRun::Paused {
            paused,
            prompt,
            ticket,
            expires_at,
        })
    }

    async fn deliver(
        &self,
        egress: &dyn Egress,
        outcome: GatewayRun,
    ) -> Result<GatewayRun, GatewayError> {
        match &outcome {
            GatewayRun::Finished { output, .. } => egress.send(output.clone()).await?,
            GatewayRun::Paused { prompt, .. } => {
                if let Some(prompt) = prompt {
                    egress.send(prompt.clone()).await?;
                }
            }
        }
        Ok(outcome)
    }
}

/// Seconds since the epoch. A clock before the epoch reads as zero, which
/// expires every prompt immediately — the fail-closed direction.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}
