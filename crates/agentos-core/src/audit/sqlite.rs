//! `safety_events` — the append-only table behind [`super::SafetyLog`].
//!
//! Shaped after `memory_access_log`: an autoincrementing row id, a
//! `CURRENT_TIMESTAMP` default, and no column anything updates. The principal
//! is stored twice on purpose — as [`Principal::storage_name`] for exact
//! lookup, and as its components for the queries a human actually writes
//! ("everything this channel was allowed to do").

use super::event::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyOutcome};
use super::journal::{SafetyLog, SafetyLogError, StoredSafetyEvent};
use crate::memory::SqliteStore;
use agentos_proto::{InterruptionId, Principal, RunId};
use rusqlite::{params, Connection};
use std::sync::Arc;

/// Create the table if it is not there. Called from `SqliteStore::open`.
pub(crate) fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS safety_events (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            outcome TEXT NOT NULL,
            principal TEXT,
            agent_id TEXT,
            channel_id TEXT,
            conversation_id TEXT,
            sender TEXT,
            run_id TEXT,
            subject TEXT NOT NULL,
            detail TEXT,
            argument_digest TEXT,
            interruption_id TEXT,
            recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_safety_events_principal_row
            ON safety_events(principal, row_id);

        CREATE INDEX IF NOT EXISTS idx_safety_events_run
            ON safety_events(run_id, row_id);
        "#,
    )
}

impl SafetyLog for SqliteStore {
    fn append(&self, event: &SafetyEvent) -> Result<(), SafetyLogError> {
        let conn = self.audit_conn()?;
        conn.execute(
            "INSERT INTO safety_events \
             (kind, outcome, principal, agent_id, channel_id, conversation_id, sender, \
              run_id, subject, detail, argument_digest, interruption_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                event.kind.as_str(),
                event.outcome.as_str(),
                event.principal.as_ref().map(Principal::storage_name),
                event
                    .principal
                    .as_ref()
                    .map(|principal| principal.agent.as_str()),
                event
                    .principal
                    .as_ref()
                    .map(|principal| principal.channel.as_str()),
                event
                    .principal
                    .as_ref()
                    .map(|principal| principal.conversation.as_str()),
                event
                    .principal
                    .as_ref()
                    .and_then(|principal| principal.sender.as_deref()),
                event.run_id.as_ref().map(RunId::as_str),
                event.subject.as_ref(),
                event.detail.as_deref(),
                event.argument_digest.as_ref().map(ArgumentDigest::as_str),
                event.interruption_id.as_ref().map(InterruptionId::as_str),
            ],
        )
        .map_err(sqlite_error)?;
        Ok(())
    }

    fn recent(&self, limit: usize) -> Result<Vec<StoredSafetyEvent>, SafetyLogError> {
        let conn = self.audit_conn()?;
        let mut statement = conn
            .prepare(
                "SELECT row_id, kind, outcome, principal, run_id, subject, detail, \
                 argument_digest, interruption_id, recorded_at \
                 FROM safety_events ORDER BY row_id DESC LIMIT ?1",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                let kind: String = row.get(1)?;
                let outcome: String = row.get(2)?;
                let principal: Option<String> = row.get(3)?;
                let run_id: Option<String> = row.get(4)?;
                let detail: Option<String> = row.get(6)?;
                let digest: Option<String> = row.get(7)?;
                let interruption: Option<String> = row.get(8)?;
                Ok(StoredSafetyEvent {
                    row_id: row.get(0)?,
                    recorded_at: Arc::from(row.get::<_, String>(9)?),
                    event: SafetyEvent {
                        kind: parse_kind(&kind),
                        outcome: parse_outcome(&outcome),
                        // A row whose principal no longer decodes is a row
                        // written by a newer encoding; report it as absent
                        // rather than dropping the whole event, which would
                        // let an unreadable field hide a readable decision.
                        principal: principal.as_deref().and_then(Principal::from_storage_name),
                        run_id: run_id.map(RunId::new),
                        subject: Arc::from(row.get::<_, String>(5)?),
                        detail: detail.map(Arc::from),
                        argument_digest: digest.map(|digest| ArgumentDigest::from_stored(&digest)),
                        interruption_id: interruption.map(InterruptionId::new),
                    },
                })
            })
            .map_err(sqlite_error)?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(sqlite_error)?);
        }
        Ok(events)
    }
}

/// Names that are not in the enum come from a newer writer against the same
/// file. Mapping them to the nearest closed variant would misreport them, so
/// they read back as the terminal-error/failed pair, which is the one shape a
/// reader must not ignore.
fn parse_kind(name: &str) -> SafetyEventKind {
    match name {
        "approval_requested" => SafetyEventKind::ApprovalRequested,
        "approval_resolved" => SafetyEventKind::ApprovalResolved,
        "policy_denial" => SafetyEventKind::PolicyDenial,
        "input_guardrail_trip" => SafetyEventKind::InputGuardrailTrip,
        "tool_guardrail_trip" => SafetyEventKind::ToolGuardrailTrip,
        "output_guardrail_trip" => SafetyEventKind::OutputGuardrailTrip,
        "sandbox_refusal" => SafetyEventKind::SandboxRefusal,
        "cancellation" => SafetyEventKind::Cancellation,
        "delegation_grant_issued" => SafetyEventKind::DelegationGrantIssued,
        "delegation_grant_used" => SafetyEventKind::DelegationGrantUsed,
        "terminal_error" => SafetyEventKind::TerminalError,
        "session_purged" => SafetyEventKind::SessionPurged,
        "audit_purged" => SafetyEventKind::AuditPurged,
        _ => SafetyEventKind::TerminalError,
    }
}

fn parse_outcome(name: &str) -> SafetyOutcome {
    match name {
        "requested" => SafetyOutcome::Requested,
        "approved" => SafetyOutcome::Approved,
        "rejected" => SafetyOutcome::Rejected,
        "unanswered" => SafetyOutcome::Unanswered,
        "denied" => SafetyOutcome::Denied,
        "tripped" => SafetyOutcome::Tripped,
        "refused" => SafetyOutcome::Refused,
        "stopped" => SafetyOutcome::Stopped,
        "issued" => SafetyOutcome::Issued,
        "used" => SafetyOutcome::Used,
        "failed" => SafetyOutcome::Failed,
        "purged" => SafetyOutcome::Purged,
        _ => SafetyOutcome::Failed,
    }
}

fn sqlite_error(err: rusqlite::Error) -> SafetyLogError {
    SafetyLogError::Backend(Arc::from(err.to_string()))
}
