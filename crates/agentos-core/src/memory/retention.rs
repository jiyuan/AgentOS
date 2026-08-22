//! Retention: what a maintenance sweep is allowed to keep.
//!
//! Split out of `reflection.rs` to keep it under the module ceiling, and they
//! are separate concerns anyway: reflection *promotes* — repeated episodes
//! become semantic facts — while retention *removes*. A sweep runs both, in
//! that order, so a fact promoted out of episodes is not archived for age on
//! the same tick that created it.
//!
//! `[memory.retention]` offers three ceilings and, before M7 / `MEM-001`,
//! none of them reached here: `reflect_all` overwrote whatever it was handed
//! with `RetentionRequest::default()`, so the keys parsed, validated, appeared
//! in `docs/CONFIG_CATALOG.md`, and pruned nothing.
//!
//! Records are **archived, not deleted**. `status = 'archived'` drops a record
//! out of every read path while the row stays, which is the same choice
//! `/clear` makes for session items (ADR-0006): a budget is about what the
//! agent recalls, and an operator who set one too low should be able to see
//! what it cost them.

use super::reflection::MemoryMaintenance;
use super::{memory_json_error, memory_sqlite_error, MemoryStore, SqliteStore};
use agentos_interfaces::memory::MemoryError;
use agentos_proto::RecordId;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// What a retention pass is allowed to keep.
///
/// Three independent ceilings plus the per-store budgets, because
/// `[memory.retention]` offers three and they answer different questions: a
/// count bounds how much there is to search, a byte total bounds what it costs
/// to store, and an age bounds how stale what is recalled can be. All are
/// `None` by default — the conservative reading of an unset budget is "keep
/// everything", not "keep some arbitrary amount".
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionRequest {
    #[serde(default)]
    pub store_budgets: Vec<StoreRetentionBudget>,
    /// Ceiling on active records across every store (`[memory.retention]
    /// max_records`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<usize>,
    /// Ceiling on the total serialized size of active records
    /// (`max_bytes`). Approximate by construction: it measures the stored
    /// JSON, which is what the database holds, not what an index costs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    /// Ceiling on a record's age in days (`max_age_days`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_days: Option<u64>,
}

impl RetentionRequest {
    /// Whether this request would do anything at all, so a caller can skip the
    /// scan rather than reading every record to archive none of them.
    pub fn is_empty(&self) -> bool {
        self.store_budgets.is_empty()
            && self.max_records.is_none()
            && self.max_bytes.is_none()
            && self.max_age_days.is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoreRetentionBudget {
    pub store: MemoryStore,
    pub max_active_records: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionReport {
    pub checked_records: usize,
    pub archived_records: Vec<RecordId>,
    pub pruned_records: Vec<RecordId>,
}

impl SqliteStore {
    /// Apply every configured ceiling, archiving what does not fit.
    ///
    /// The `MemoryMaintenance` trait method delegates here; the body lives in
    /// this module because everything it reasons about does.
    pub(super) fn apply_retention_budgets(
        &self,
        request: &RetentionRequest,
    ) -> Result<RetentionReport, MemoryError> {
        let mut report = RetentionReport::default();
        if request.is_empty() {
            return Ok(report);
        }
        let mut archived = BTreeSet::new();

        for budget in &request.store_budgets {
            let mut records = self.active_records_for_store(Some(budget.store))?;
            report.checked_records += records.len();
            let overflow = records.len().saturating_sub(budget.max_active_records);
            if overflow == 0 {
                continue;
            }
            rank_for_eviction(&mut records);
            for record in records.into_iter().take(overflow) {
                self.archive_once(&record.id, "retention_budget", &mut archived, &mut report)?;
            }
        }

        // The three global ceilings, over every store at once, and in this
        // order deliberately: age first, because a record past its age is
        // gone whatever the other two say, and dropping it may bring the
        // count and the byte total back under on its own.
        if request.max_records.is_none()
            && request.max_bytes.is_none()
            && request.max_age_days.is_none()
        {
            return Ok(report);
        }
        let mut records = self.active_records_for_store(None)?;
        records.retain(|record| !archived.contains(record.id.as_str()));
        report.checked_records += records.len();

        if let Some(max_age_days) = request.max_age_days {
            let mut kept = Vec::with_capacity(records.len());
            for record in records {
                // An unknown age is never *too old*: guessing is how a
                // retention pass removes something it should not have.
                if record.age_days.is_some_and(|age| age > max_age_days) {
                    self.archive_once(&record.id, "retention_age", &mut archived, &mut report)?;
                } else {
                    kept.push(record);
                }
            }
            records = kept;
        }

        rank_for_eviction(&mut records);
        if let Some(max_records) = request.max_records {
            let overflow = records.len().saturating_sub(max_records);
            for record in records.drain(..overflow) {
                self.archive_once(&record.id, "retention_records", &mut archived, &mut report)?;
            }
        }

        if let Some(max_bytes) = request.max_bytes {
            let mut total: usize = records.iter().map(|record| record.bytes).sum();
            let mut index = 0;
            while total > max_bytes && index < records.len() {
                let record = &records[index];
                total = total.saturating_sub(record.bytes);
                let id = record.id.clone();
                self.archive_once(&id, "retention_bytes", &mut archived, &mut report)?;
                index += 1;
            }
        }

        Ok(report)
    }

    /// Archive a record once, however many budgets asked for it.
    ///
    /// The three global ceilings overlap by design — an oversized store is
    /// usually an old one — so without this a record could be archived twice
    /// and reported twice, and the report is what an operator reads to decide
    /// whether the budgets are set sanely.
    fn archive_once(
        &self,
        id: &RecordId,
        reason: &'static str,
        archived: &mut BTreeSet<String>,
        report: &mut RetentionReport,
    ) -> Result<(), MemoryError> {
        if !archived.insert(id.as_str().to_owned()) {
            return Ok(());
        }
        self.mark_record_status(id, "archived", reason)?;
        report.archived_records.push(id.clone());
        Ok(())
    }

    /// Active records, for one store or for all of them.
    fn active_records_for_store(
        &self,
        store: Option<MemoryStore>,
    ) -> Result<Vec<RetentionCandidate>, MemoryError> {
        let conn = self.memory_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT row_id, id, metadata_json, access_count, \
                        LENGTH(body_json) + LENGTH(metadata_json), created_at \
                 FROM memory_records \
                 WHERE (?1 IS NULL OR store = ?1) AND status = 'active'",
            )
            .map_err(memory_sqlite_error)?;
        let rows = stmt
            .query_map(params![store.map(|store| store.as_str())], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(memory_sqlite_error)?;

        let mut records = Vec::new();
        for row in rows {
            let (row_id, id, metadata_json, access_count, bytes, created_at) =
                row.map_err(memory_sqlite_error)?;
            let metadata: BTreeMap<Arc<str>, Value> =
                serde_json::from_str(&metadata_json).map_err(memory_json_error)?;
            records.push(RetentionCandidate {
                row_id,
                id: RecordId::new(id),
                importance: metadata
                    .get("importance")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
                access_count,
                bytes: usize::try_from(bytes).unwrap_or(0),
                age_days: created_at.as_deref().and_then(age_in_days),
            });
        }
        Ok(records)
    }
}

struct RetentionCandidate {
    row_id: i64,
    id: RecordId,
    importance: f64,
    access_count: i64,
    /// Serialized size of the stored body and metadata, for the byte budget.
    bytes: usize,
    /// Age in whole days at the moment of the sweep, for the age budget.
    /// `None` when the row predates `created_at` or carries an unparseable
    /// one — such a record is never pruned *for age*, because guessing an age
    /// is how a retention pass deletes something it should not have.
    age_days: Option<u64>,
}

/// Whole days between `created_at` and now.
///
/// `created_at` is SQLite's `CURRENT_TIMESTAMP`, which is UTC
/// `YYYY-MM-DD HH:MM:SS` with no zone marker. `None` for anything that does
/// not parse, and the caller treats an unknown age as not-too-old — a
/// retention pass that guesses ages deletes things it should not have.
fn age_in_days(created_at: &str) -> Option<u64> {
    let parsed = chrono::NaiveDateTime::parse_from_str(created_at.trim(), "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| naive.and_utc())
        .or_else(|| {
            chrono::DateTime::parse_from_rfc3339(created_at.trim())
                .ok()
                .map(|fixed| fixed.with_timezone(&chrono::Utc))
        })?;
    let elapsed = chrono::Utc::now().signed_duration_since(parsed);
    u64::try_from(elapsed.num_days()).ok()
}

/// Least worth keeping first: low importance, then rarely read, then oldest
/// row. Shared by every budget so two ceilings cannot disagree about which
/// record to drop.
fn rank_for_eviction(candidates: &mut [RetentionCandidate]) {
    candidates.sort_by(|left, right| {
        left.importance
            .partial_cmp(&right.importance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.access_count.cmp(&right.access_count))
            .then(left.row_id.cmp(&right.row_id))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MemoryCaller, MemoryManager, MemoryOwner, MemoryScope, MemoryVisibility};
    use agentos_proto::{AgentId, ChannelId, ConversationId, TaskId};
    use serde_json::json;

    #[test]
    fn a_timestamp_sqlite_wrote_parses_and_a_future_one_does_not_wrap() {
        // `CURRENT_TIMESTAMP` is UTC with no zone marker, which no RFC 3339
        // parser accepts — reading it with the wrong one is how an age budget
        // would silently see every record as unknown-aged and prune nothing.
        assert!(age_in_days("2000-01-01 00:00:00").expect("parses") > 8000);
        // RFC 3339 too, for a row a future writer might produce.
        assert!(age_in_days("2000-01-01T00:00:00Z").expect("parses") > 8000);
        // Anything else is unknown rather than zero: a retention pass that
        // guesses ages deletes things it should not have.
        assert_eq!(age_in_days("not a timestamp"), None);
        assert_eq!(age_in_days(""), None);
        // A clock-skewed future timestamp is *not* negative-aged into a huge
        // number by wrapping; `u64::try_from` refuses it.
        assert_eq!(age_in_days("2999-01-01 00:00:00"), None);
    }

    fn caller() -> MemoryCaller {
        MemoryCaller {
            agent_id: AgentId::new("agent"),
            task_id: TaskId::new("task"),
            channel_id: ChannelId::new("channel"),
            conversation_id: ConversationId::new("conversation"),
            user_id: None,
            allowed_shared_domains: Vec::new(),
            writable_shared_domains: Vec::new(),
            audit_read_access: false,
        }
    }

    /// The age budget firing, which an integration test cannot show without
    /// sleeping for a day: seed two records and backdate one of them.
    #[tokio::test]
    async fn the_age_budget_archives_only_what_is_older_than_the_ceiling() {
        let store = Arc::new(SqliteStore::open_in_memory().expect("the store opens"));
        let manager = MemoryManager::new_sqlite(store.clone());
        let scope = MemoryScope::new(
            MemoryStore::Semantic,
            MemoryOwner::Agent(AgentId::new("agent")),
            MemoryVisibility::Private,
            Some(Arc::from("general")),
        );
        let stale = manager
            .write_scoped(
                &caller(),
                scope.clone(),
                json!({ "fact": "written long ago" }),
                Default::default(),
            )
            .await
            .expect("seeding succeeds");
        let fresh = manager
            .write_scoped(
                &caller(),
                scope,
                json!({ "fact": "written just now" }),
                Default::default(),
            )
            .await
            .expect("seeding succeeds");

        store
            .memory_conn()
            .expect("the connection is available")
            .execute(
                "UPDATE memory_records SET created_at = '2001-02-03 04:05:06' WHERE id = ?1",
                params![stale.as_str()],
            )
            .expect("backdating succeeds");

        let report = store
            .apply_retention_budgets(&RetentionRequest {
                max_age_days: Some(30),
                ..RetentionRequest::default()
            })
            .expect("the budget applies");

        assert_eq!(report.archived_records, vec![stale]);
        assert!(
            !report.archived_records.contains(&fresh),
            "a record written moments ago is not past a 30-day ceiling"
        );
    }
}
