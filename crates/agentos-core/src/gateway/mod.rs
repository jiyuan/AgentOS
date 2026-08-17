//! The gateway: what turns inbound envelopes into runs.
//!
//! [`GatewayService`] is the per-run half — guardrails, approval prompts,
//! delivery — and is what a one-shot entrypoint uses directly. [`shard`] and
//! [`inbox`] are the concurrency half (roadmap item G1): conversations as
//! actors, sharded across threads, serial within one.

mod inbox;
mod shard;

pub use inbox::{Admitted, Inbox, DEFAULT_INBOX_CAPACITY};
pub use shard::{
    run_shard, shard_set, RouteError, Router, ShardConfig, ShardInbound, Turn, TurnHandler,
    DEFAULT_IDLE_INTERVAL,
};

use crate::runner::{
    approval_prompt_envelope, resume_run, run_envelope, PausedRun, ResumeDecision, RunOutcome,
    RunnerDeps, RunnerError,
};
use agentos_interfaces::{Channel, ChannelError, Egress, RunState};
use agentos_proto::{Envelope, InterruptionId, RunId};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

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
    },
}

pub struct Gateway {
    inbound_tx: mpsc::Sender<Envelope>,
    inbound_rx: mpsc::Receiver<Envelope>,
}

impl Gateway {
    pub fn bounded(capacity: usize) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(capacity);
        Self {
            inbound_tx,
            inbound_rx,
        }
    }

    pub fn sender(&self) -> mpsc::Sender<Envelope> {
        self.inbound_tx.clone()
    }

    pub async fn receive(&mut self) -> Option<Envelope> {
        self.inbound_rx.recv().await
    }
}

pub struct GatewayService<'a> {
    deps: &'a RunnerDeps<'a>,
    sender: Arc<str>,
}

impl<'a> GatewayService<'a> {
    pub fn new(deps: &'a RunnerDeps<'a>, sender: impl Into<Arc<str>>) -> Self {
        Self {
            deps,
            sender: sender.into(),
        }
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
        let Some(input) = channel.receive().await else {
            return Ok(None);
        };
        let egress = channel.egress();
        self.run_envelope(egress.as_ref(), input, run_id)
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
        let channel_id = input.channel_id.clone();
        let conversation_id = input.conversation_id.clone();
        match run_envelope(input, run_id, self.deps).await? {
            RunOutcome::Finished { state, output } => {
                egress.send(output.clone()).await?;
                Ok(GatewayRun::Finished { state, output })
            }
            RunOutcome::Paused(state) => {
                let paused = PausedRun {
                    channel_id,
                    conversation_id,
                    state,
                };
                self.send_approval_prompt(egress, paused).await
            }
        }
    }

    pub async fn resume(
        &self,
        egress: &dyn Egress,
        paused: PausedRun,
        approval_id: &InterruptionId,
        decision: ResumeDecision,
    ) -> Result<GatewayRun, GatewayError> {
        let channel_id = paused.channel_id.clone();
        let conversation_id = paused.conversation_id.clone();
        match resume_run(paused, approval_id, decision, self.deps).await? {
            RunOutcome::Finished { state, output } => {
                egress.send(output.clone()).await?;
                Ok(GatewayRun::Finished { state, output })
            }
            RunOutcome::Paused(state) => {
                let paused = PausedRun {
                    channel_id,
                    conversation_id,
                    state,
                };
                self.send_approval_prompt(egress, paused).await
            }
        }
    }

    async fn send_approval_prompt(
        &self,
        egress: &dyn Egress,
        paused: PausedRun,
    ) -> Result<GatewayRun, GatewayError> {
        let prompt = approval_prompt_envelope(&paused, Arc::clone(&self.sender));
        if let Some(prompt) = &prompt {
            egress.send(prompt.clone()).await?;
        }
        Ok(GatewayRun::Paused { paused, prompt })
    }
}
