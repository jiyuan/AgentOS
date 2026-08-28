//! Durable, immutable records of safety-boundary decisions (M6 / `AUD-001`).
//!
//! [ADR-0005](../../../../docs/adr/0005-SAFETY_EVENTS.md). Before this module
//! the runtime's strongest signal for a *successful* approval was a deletion:
//! `take_approved_action` removed the pending interruption and the CLI
//! unlinked the paused-run file. An absence is indistinguishable from a record
//! that was never written, one that was cleaned up, and one that was deleted
//! to hide something. Approval *requested* and guardrail trips were not
//! recorded at all outside the in-memory trace, which the error path returned
//! without persisting — so the runs most worth reconstructing left the least
//! behind.
//!
//! `memory_access_log` is the in-tree precedent and the model here: an
//! append-only table, never updated, never deleted on the normal path.
//!
//! # Why this is not a trait in `agentos-interfaces`
//!
//! For the same reason `approve` is not. An extension that can replace the
//! audit sink can decide what the audit trail says. [`SafetyLog`] lives in the
//! core, exactly like [`crate::runner::TraceSink`] and the memory accounting
//! trait, so nothing an operator compiles in through `agent.toml` can supply
//! one. Its only implementation is the store the runtime already owns.
//!
//! # What an event may carry
//!
//! Names, decisions, and digests. Never a tool's arguments — [`SafetyEvent`]
//! has no field that accepts them. See [`event`] for the reasoning.
//!
//! # When the store is unreachable
//!
//! A configured journal is part of the authorization boundary. A failed
//! append is returned to the caller and logged at `error!`; protected work
//! does not proceed without its required evidence. Refusals and cancellation
//! remain the safer outcome, but the affected run stops with a typed
//! operational failure instead of continuing mutable work without a record
//! (`AUD-002`).

mod event;
mod journal;
mod purge;
mod sqlite;

pub use event::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyOutcome};
pub use journal::{SafetyJournal, SafetyLog, SafetyLogError, StoredSafetyEvent};
pub use purge::{count_before, purge_before, AuditPurgeCounts};
pub(crate) use sqlite::{init_schema, insert_event};
