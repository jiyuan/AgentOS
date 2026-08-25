//! The sink a run writes safety events to, and the handle it writes through.

use super::event::{SafetyEvent, SafetyEventKind, SafetyOutcome};
use agentos_proto::{ActorPrincipal, Principal, RunId};
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum SafetyLogError {
    #[error("safety log backend failed: {0}")]
    Backend(Arc<str>),
}

/// One stored event, with what the store added to it.
#[derive(Clone, Debug)]
pub struct StoredSafetyEvent {
    /// Monotonic within a store. The append order, which is the only order a
    /// reader can trust — `recorded_at` has one-second resolution.
    pub row_id: i64,
    pub recorded_at: Arc<str>,
    pub event: SafetyEvent,
}

/// Append-only storage for safety events.
///
/// Defined here rather than in `agentos-interfaces`; see the module docs.
/// There is no `delete` and no
/// `update`: retention is a separate, explicitly authorized operation, not
/// something a caller of this trait can reach.
pub trait SafetyLog: Send + Sync {
    fn append(&self, event: &SafetyEvent) -> Result<(), SafetyLogError>;

    /// The newest `limit` events, newest first.
    fn recent(&self, limit: usize) -> Result<Vec<StoredSafetyEvent>, SafetyLogError>;
}

/// A run's handle on the safety log.
///
/// Carries the principal and run id so a call site emits a decision rather
/// than assembling a record — the alternative is every call site remembering
/// to stamp identity, and the one that forgets produces an event nobody can
/// attribute.
#[derive(Clone, Default)]
pub struct SafetyJournal<'a> {
    log: Option<&'a dyn SafetyLog>,
    principal: Option<Principal>,
    run_id: Option<RunId>,
}

impl std::fmt::Debug for SafetyJournal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafetyJournal")
            .field("attached", &self.log.is_some())
            .field("run_id", &self.run_id.as_ref().map(RunId::as_str))
            .finish()
    }
}

impl<'a> SafetyJournal<'a> {
    /// A journal with nowhere to write. Every `record` is a no-op, which is
    /// what an entrypoint with no store configured gets.
    pub fn detached() -> Self {
        Self::default()
    }

    pub fn new(log: Option<&'a dyn SafetyLog>) -> Self {
        Self {
            log,
            principal: None,
            run_id: None,
        }
    }

    /// Bind this journal to one run, so every event it writes is attributable
    /// without the call site restating who is acting.
    pub fn for_run(mut self, principal: Principal, run_id: RunId) -> Self {
        self.principal = Some(principal);
        self.run_id = Some(run_id);
        self
    }

    /// Whether anything is listening. Call sites that would do work to build
    /// an event check this first.
    pub fn is_attached(&self) -> bool {
        self.log.is_some()
    }

    /// The sender-qualified actor this run is executing for, whether or not a
    /// log backend is attached.
    pub fn actor_principal(&self) -> Option<ActorPrincipal> {
        self.principal
            .clone()
            .and_then(|principal| principal.try_into().ok())
    }

    /// Write one event, stamped with this run's identity.
    ///
    /// A configured journal is part of the authorization boundary, so an
    /// append failure is a first-class result. Callers must stop before a
    /// protected transition, or preserve an already-safer refusal and surface
    /// the operational failure. A detached journal remains an explicit no-op
    /// for entrypoints that configured no audit store.
    pub fn record(&self, event: SafetyEvent) -> Result<(), SafetyLogError> {
        let Some(log) = self.log else {
            return Ok(());
        };
        let mut event = event;
        if event.principal.is_none() {
            event.principal = self.principal.clone();
        }
        if event.kind == SafetyEventKind::ApprovalRequested && event.prompting_principal.is_none() {
            event.prompting_principal = self.principal.clone();
        }
        if event.run_id.is_none() {
            event.run_id = self.run_id.clone();
        }
        // At `info!` and not `debug!`: these are the lines an operator greps
        // for when asked what the agent was allowed to do.
        info!(
            kind = event.kind.as_str(),
            outcome = event.outcome.as_str(),
            subject = event.subject.as_ref(),
            run_id = event.run_id.as_ref().map(RunId::as_str),
            "safety_event"
        );
        if let Err(err) = log.append(&event) {
            error!(
                kind = event.kind.as_str(),
                outcome = event.outcome.as_str(),
                error = %err,
                "safety_log_write_failed"
            );
            return Err(err);
        }
        Ok(())
    }

    /// Shorthand for the many call sites that record a kind, an outcome, a
    /// subject, and a reason.
    pub fn record_reason(
        &self,
        kind: SafetyEventKind,
        outcome: SafetyOutcome,
        subject: impl Into<Arc<str>>,
        reason: impl AsRef<str>,
    ) -> Result<(), SafetyLogError> {
        if !self.is_attached() {
            return Ok(());
        }
        self.record(SafetyEvent::new(kind, outcome, subject).with_detail(reason))
    }
}
