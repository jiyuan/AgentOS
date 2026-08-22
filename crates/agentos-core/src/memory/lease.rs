//! One maintenance leader at a time, across threads and across processes
//! (M8 / `GW-001`, deliverable 2).
//!
//! # What was wrong
//!
//! Reflection and retention are *global* sweeps: they promote episodes into
//! semantic facts, supersede stale records, rebuild the lexical index, and
//! archive whatever is over budget. The gateway ran them from the idle phase
//! of shard 0 — and shard 0 exists once *per channel*, so a deployment with
//! Telegram and Feishu enabled swept twice, concurrently, over one database.
//! Add a TUI on the same file and it is three. Duplicate promotion writes the
//! same fact twice; duplicate retention decides what to archive from two
//! different snapshots of the same table.
//!
//! Confining it to shard 0 of the first channel would fix the count and not
//! the problem: the second process is still there.
//!
//! # The lease
//!
//! A row in the database, which is the only thing all the contenders share.
//! Acquisition is one `INSERT … ON CONFLICT DO UPDATE … WHERE` — a single
//! statement, so SQLite's own write serialization decides the winner and there
//! is no read-then-write window for a second contender to slip into. It
//! updates when the current lease has expired or when the asker already holds
//! it, so renewal is the same call as acquisition.
//!
//! # Why a lease and not a lock
//!
//! A leader that crashes mid-sweep must not stop maintenance forever, and
//! nothing can be relied on to run a cleanup path on the way down. So the
//! lease *expires*: the holder renews it every idle tick, and a holder that
//! stops renewing loses it within [`DEFAULT_LEASE_TTL`]. The cost is that a
//! second sweep can start that long after a leader wedged without dying, which
//! is the right trade — the sweep is idempotent enough to survive an overlap,
//! and "maintenance silently stopped" is the failure nobody notices.

use super::{memory_sqlite_error, MemoryError, SqliteStore};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The reflection and retention sweep. One name because they are one job: the
/// retention budgets are applied by the reflection pass.
pub const REFLECTION_LEASE: &str = "memory.reflection";

/// How long a lease lasts without renewal.
///
/// Comfortably more than the gateway's cron-scan interval, so a leader that is
/// merely busy does not lose the lease to a contender mid-sweep, and short
/// enough that a crashed leader is replaced within a few minutes rather than
/// at the next restart.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(300);

/// A held lease. Carrying the expiry rather than a `Drop` that releases it:
/// the holder is a long-lived loop that renews, and a `Drop`-released lease
/// would be released by the unwinding of any error path that happened to be
/// holding it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub name: Arc<str>,
    pub holder: Arc<str>,
    /// Unix seconds after which another contender may take it.
    pub expires_at: u64,
}

/// Who is asking. Distinct per contender, not per process: two channels'
/// shard sets in one process are two contenders, and one of them has to lose.
pub fn lease_holder_id(role: &str) -> Arc<str> {
    Arc::from(format!("{}:{role}", std::process::id()))
}

pub(super) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS maintenance_leases (
            name TEXT PRIMARY KEY,
            holder TEXT NOT NULL,
            acquired_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        );
        "#,
    )
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

impl SqliteStore {
    /// Take `name` for `ttl`, or renew it if this holder already has it.
    ///
    /// `Ok(None)` means somebody else holds an unexpired lease — the ordinary
    /// answer for every contender but one, and not an error.
    pub fn try_acquire_lease(
        &self,
        name: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<Lease>, MemoryError> {
        self.acquire_lease_at(name, holder, ttl, unix_now())
    }

    /// [`try_acquire_lease`](Self::try_acquire_lease) against a supplied clock,
    /// so expiry can be tested without waiting for it.
    pub fn acquire_lease_at(
        &self,
        name: &str,
        holder: &str,
        ttl: Duration,
        now: u64,
    ) -> Result<Option<Lease>, MemoryError> {
        let expires_at = now.saturating_add(ttl.as_secs().max(1));
        let conn = self.memory_conn()?;
        // One statement. The `WHERE` on the conflict arm is what makes this a
        // decision rather than a race: a contender whose row is still held by
        // somebody else updates nothing and gets `0` back.
        let changed = conn
            .execute(
                r#"
                INSERT INTO maintenance_leases (name, holder, acquired_at, expires_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(name) DO UPDATE SET
                    holder = excluded.holder,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at
                WHERE maintenance_leases.expires_at <= ?3
                   OR maintenance_leases.holder = excluded.holder
                "#,
                params![name, holder, now as i64, expires_at as i64],
            )
            .map_err(memory_sqlite_error)?;
        if changed == 0 {
            return Ok(None);
        }
        Ok(Some(Lease {
            name: Arc::from(name),
            holder: Arc::from(holder),
            expires_at,
        }))
    }

    /// Give up a lease this holder has. A holder that no longer has it is not
    /// an error — it has already lost the race it is conceding.
    pub fn release_lease(&self, name: &str, holder: &str) -> Result<bool, MemoryError> {
        let conn = self.memory_conn()?;
        let changed = conn
            .execute(
                "DELETE FROM maintenance_leases WHERE name = ?1 AND holder = ?2",
                params![name, holder],
            )
            .map_err(memory_sqlite_error)?;
        Ok(changed > 0)
    }

    /// Who holds `name` and until when, for diagnostics.
    pub fn lease(&self, name: &str) -> Result<Option<Lease>, MemoryError> {
        let conn = self.memory_conn()?;
        conn.query_row(
            "SELECT holder, expires_at FROM maintenance_leases WHERE name = ?1",
            params![name],
            |row| {
                Ok(Lease {
                    name: Arc::from(name),
                    holder: Arc::from(row.get::<_, String>(0)?.as_str()),
                    expires_at: row.get::<_, i64>(1)? as u64,
                })
            },
        )
        .optional()
        .map_err(memory_sqlite_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SqliteStore {
        SqliteStore::open_in_memory().expect("the store opens")
    }

    #[test]
    fn exactly_one_of_two_contenders_takes_the_lease() {
        let store = store();
        let first = store
            .acquire_lease_at(REFLECTION_LEASE, "1:telegram", DEFAULT_LEASE_TTL, 1_000)
            .expect("the query runs");
        let second = store
            .acquire_lease_at(REFLECTION_LEASE, "1:feishu", DEFAULT_LEASE_TTL, 1_000)
            .expect("the query runs");
        assert!(first.is_some(), "the first contender must win");
        assert!(
            second.is_none(),
            "a second sweep over one database is the defect this exists to stop"
        );
    }

    #[test]
    fn the_holder_renews_rather_than_losing_its_own_lease() {
        let store = store();
        store
            .acquire_lease_at(REFLECTION_LEASE, "1:telegram", DEFAULT_LEASE_TTL, 1_000)
            .expect("the query runs")
            .expect("the first acquisition wins");
        let renewed = store
            .acquire_lease_at(REFLECTION_LEASE, "1:telegram", DEFAULT_LEASE_TTL, 1_060)
            .expect("the query runs")
            .expect("a holder renews");
        assert_eq!(renewed.expires_at, 1_060 + DEFAULT_LEASE_TTL.as_secs());
    }

    /// The reason it is a lease. A leader that dies holding it must not stop
    /// maintenance until the next restart.
    #[test]
    fn an_expired_lease_is_taken_by_the_next_contender() {
        let store = store();
        store
            .acquire_lease_at(
                REFLECTION_LEASE,
                "1:crashed",
                Duration::from_secs(60),
                1_000,
            )
            .expect("the query runs")
            .expect("the first acquisition wins");
        assert!(
            store
                .acquire_lease_at(REFLECTION_LEASE, "2:live", DEFAULT_LEASE_TTL, 1_059)
                .expect("the query runs")
                .is_none(),
            "the lease has not expired yet"
        );
        let taken = store
            .acquire_lease_at(REFLECTION_LEASE, "2:live", DEFAULT_LEASE_TTL, 1_060)
            .expect("the query runs")
            .expect("an expired lease is available");
        assert_eq!(taken.holder.as_ref(), "2:live");
    }

    #[test]
    fn releasing_hands_it_straight_over() {
        let store = store();
        store
            .acquire_lease_at(REFLECTION_LEASE, "1:telegram", DEFAULT_LEASE_TTL, 1_000)
            .expect("the query runs")
            .expect("the first acquisition wins");
        assert!(!store
            .release_lease(REFLECTION_LEASE, "1:feishu")
            .expect("the query runs"));
        assert!(store
            .release_lease(REFLECTION_LEASE, "1:telegram")
            .expect("the query runs"));
        assert!(store
            .acquire_lease_at(REFLECTION_LEASE, "1:feishu", DEFAULT_LEASE_TTL, 1_001)
            .expect("the query runs")
            .is_some());
    }

    #[test]
    fn a_holder_id_names_the_process_and_the_role() {
        let id = lease_holder_id("telegram");
        assert!(id.starts_with(&format!("{}:", std::process::id())));
        assert!(id.ends_with(":telegram"));
        assert_ne!(id, lease_holder_id("feishu"));
    }
}
