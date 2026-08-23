//! The bounded set of SQLite connections the whole process shares
//! (M8 / `GW-001`, deliverables 1 and 2).
//!
//! # What was wrong
//!
//! [`SqliteStore`](super::SqliteStore) held one `Mutex<Connection>`. Every
//! shard thread, every channel's shard set, the memory tool, the session log,
//! and the safety-event log went through that one lock, so a slow write on one
//! conversation stalled a read on an unrelated one — and the gateway and the
//! TUI, two *processes*, both opened `workspace/agentos.sqlite` in rollback
//! journal mode with no busy timeout, where a concurrent writer is not a wait
//! but an immediate `SQLITE_BUSY`.
//!
//! # The two halves of the fix
//!
//! **WAL.** In write-ahead logging a reader never blocks a writer and a writer
//! never blocks a reader; only writer-against-writer serializes. That is the
//! shape of this workload — many short reads for hydration, occasional writes
//! — and it is what makes more than one connection worth having. It is also
//! the half that fixes the *cross-process* case, which no amount of in-process
//! locking can.
//!
//! **A bounded pool.** Connections are created up front, never on demand:
//! `size` is the concurrency limit, stated once, rather than an emergent
//! property of how many threads happen to call at once. A caller that finds
//! none free waits for one — it does not open another, because "a pool that
//! grows under load" is the unbounded-queue mistake with a different name.
//!
//! # What this does not fix
//!
//! These are synchronous calls made from async code. A pooled connection is
//! held across `rusqlite` calls only, never across an `.await`, but a slow
//! statement still occupies the calling thread. Moving the store behind
//! `spawn_blocking` is a separate change with its own `!Send` consequences for
//! the run loop, and pretending the pool did it would be worse than saying so.
//!
//! An in-memory database is *per connection* — two connections to
//! `:memory:` are two different databases — so
//! [`SqliteStore::open_in_memory`](super::SqliteStore::open_in_memory) gets a
//! pool of exactly one. That is not a limitation worth working around: an
//! in-memory store belongs to one test or one ephemeral run.

use super::MemoryError;
use rusqlite::Connection;
use std::ops::{Deref, DerefMut};
use std::sync::{Condvar, Mutex};

/// Connections when nothing says otherwise.
///
/// Four, not one per shard. Under WAL the writes serialize in SQLite whatever
/// the pool does, so past a handful of connections the extra ones only wait in
/// a different place; four keeps hydration reads flowing past an in-flight
/// write without pretending the database is more parallel than it is.
pub const DEFAULT_MAX_CONNECTIONS: usize = 4;

/// Ceiling on `[memory] max_connections`. Each connection is a file
/// descriptor, a page cache, and a WAL reader slot; a typo should fail the
/// load rather than open hundreds.
pub const MAX_MAX_CONNECTIONS: usize = 64;

/// How long a statement waits for a writer in another connection — or another
/// *process* — before giving up with `SQLITE_BUSY`.
///
/// Five seconds is far longer than any statement here should take and far
/// shorter than a user waits before assuming the agent is dead. Without it the
/// default is zero: the very first overlap between the TUI and the gateway
/// fails outright, which is the reported symptom.
pub const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A fixed set of connections to one database, handed out one at a time.
pub(crate) struct ConnectionPool {
    /// Connections nobody is holding. Guarded only for the push and the pop —
    /// never across a statement, which is what makes a poisoned lock here
    /// recoverable: the `Vec` a panicking thread left behind is a perfectly
    /// good `Vec`.
    idle: Mutex<Vec<Connection>>,
    returned: Condvar,
    size: usize,
}

impl ConnectionPool {
    /// Build `size` connections by calling `open`, applying the shared pragmas
    /// to each.
    ///
    /// Eager rather than lazy: a pragma that fails, a database that is locked
    /// by something else, or a path that is not writable should be a startup
    /// error, not the fourth concurrent request's error.
    pub(crate) fn build(
        size: usize,
        open: impl Fn() -> Result<Connection, MemoryError>,
    ) -> Result<Self, MemoryError> {
        let size = size.clamp(1, MAX_MAX_CONNECTIONS);
        let mut idle = Vec::with_capacity(size);
        for _ in 0..size {
            idle.push(open()?);
        }
        Ok(Self {
            idle: Mutex::new(idle),
            returned: Condvar::new(),
            size,
        })
    }

    /// How many connections exist. The concurrency bound, stated.
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// Take a connection, waiting for one if all are in use.
    ///
    /// There is no timeout, deliberately. A caller that waited a bounded time
    /// and then failed would turn "the process is busy" into "the write was
    /// lost", and every holder releases at the end of one statement or one
    /// transaction — never across an `.await`.
    pub(crate) fn get(&self) -> PooledConnection<'_> {
        let mut idle = self.idle.lock().unwrap_or_else(|err| err.into_inner());
        loop {
            if let Some(conn) = idle.pop() {
                return PooledConnection {
                    pool: self,
                    conn: Some(conn),
                };
            }
            idle = self
                .returned
                .wait(idle)
                .unwrap_or_else(|err| err.into_inner());
        }
    }
}

/// A connection borrowed from the pool, returned when dropped.
///
/// Dropped on the unwind path too, so a panicking caller gives the connection
/// back rather than shrinking the pool by one for the life of the process.
/// Anything the panic interrupted is a `rusqlite::Transaction`, whose own
/// `Drop` rolls back first.
pub(crate) struct PooledConnection<'a> {
    pool: &'a ConnectionPool,
    /// `Some` until `Drop` takes it back. The `Option` exists only so `Drop`
    /// can move the connection out.
    conn: Option<Connection>,
}

impl Deref for PooledConnection<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("a live guard holds its connection")
    }
}

impl DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn
            .as_mut()
            .expect("a live guard holds its connection")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        let mut idle = self.pool.idle.lock().unwrap_or_else(|err| err.into_inner());
        idle.push(conn);
        // One waiter, because one connection came back.
        self.pool.returned.notify_one();
    }
}

/// How long [`request_wal`] keeps asking. The same budget as [`BUSY_TIMEOUT`],
/// because it is the same wait — see that function for why SQLite will not do
/// it for us.
const WAL_RETRY_BUDGET: std::time::Duration = BUSY_TIMEOUT;

/// Between attempts. Short, because the contention it waits out is one other
/// connection finishing a schema-creation transaction on a new file.
const WAL_RETRY_PAUSE: std::time::Duration = std::time::Duration::from_millis(20);

/// Put the database into WAL, waiting out another connection that is holding
/// it, and answer with the mode it ended up in.
///
/// **The one place in the store that retries by hand.** `busy_timeout` covers
/// every other statement; it does not cover this one. `PRAGMA journal_mode =
/// WAL` needs an exclusive lock, and SQLite returns `SQLITE_BUSY` for it
/// *immediately* rather than invoking the busy handler, because two
/// connections both blocking on the same upgrade would deadlock.
///
/// It can only contend on a database that is *new*. The journal mode is a
/// property of the file, so once one connection has set it every later one is
/// answered `wal` with no lock taken at all. That is why this survived M8: the
/// pipeline and every test opened a database that already existed. A gateway
/// serving two channels builds two pools on two threads in the same second,
/// and on a first run neither file exists yet — so one channel's loop died at
/// startup with "database is locked" (M9 / `CI-002`).
fn request_wal(conn: &Connection) -> Result<String, MemoryError> {
    let deadline = std::time::Instant::now() + WAL_RETRY_BUDGET;
    loop {
        // `journal_mode` answers with the mode it ended up in, so it is a
        // query rather than a statement — `execute_batch` reports "Execute
        // returned results" for it.
        match conn.query_row("PRAGMA journal_mode = WAL;", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) => return Ok(mode),
            Err(err) if is_busy(&err) && std::time::Instant::now() < deadline => {
                std::thread::sleep(WAL_RETRY_PAUSE);
            }
            Err(err) => return Err(super::memory_sqlite_error(err)),
        }
    }
}

fn is_busy(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked,
                ..
            },
            _
        )
    )
}

/// Settings every connection to a store shares. Per connection, not per
/// database, for `busy_timeout` and `foreign_keys`; `journal_mode` is a
/// property of the file, but setting it on each is harmless and means the
/// first connection to a fresh database establishes it.
pub(crate) fn apply_pragmas(conn: &Connection, wal: bool) -> Result<(), MemoryError> {
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(super::memory_sqlite_error)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(super::memory_sqlite_error)?;
    if !wal {
        return Ok(());
    }
    let mode = request_wal(conn)?;
    if !mode.eq_ignore_ascii_case("wal") {
        // A database on a filesystem without shared-memory support (some
        // network mounts) refuses WAL and stays in rollback mode. Worth
        // knowing about — it is the configuration where cross-process access
        // is still fragile — but not worth refusing to start over.
        tracing::warn!(
            journal_mode = %mode,
            "sqlite refused WAL; concurrent access from a second process may fail"
        );
    }
    // In WAL, `NORMAL` fsyncs at checkpoint rather than at every commit. The
    // durability this gives up is the last few transactions on a *power loss*,
    // not on a process crash — which is the failure the ingress ledger and the
    // atomic state files are written against.
    conn.execute_batch("PRAGMA synchronous = NORMAL;")
        .map_err(super::memory_sqlite_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn memory_pool(size: usize) -> ConnectionPool {
        ConnectionPool::build(size, || {
            Connection::open_in_memory().map_err(super::super::memory_sqlite_error)
        })
        .expect("an in-memory pool builds")
    }

    #[test]
    fn the_pool_hands_out_at_most_its_size_at_once() {
        let pool = memory_pool(2);
        let first = pool.get();
        let second = pool.get();
        assert!(pool.idle.lock().expect("uncontended").is_empty());
        drop(first);
        drop(second);
        assert_eq!(pool.idle.lock().expect("uncontended").len(), 2);
    }

    /// The bound is a bound, not a hint: a third caller waits for a return
    /// rather than getting a connection the pool never opened.
    #[test]
    fn a_caller_past_the_bound_waits_for_a_return() {
        let pool = Arc::new(memory_pool(1));
        let held = pool.get();
        let entered = Arc::new(AtomicUsize::new(0));

        let waiter = {
            let pool = Arc::clone(&pool);
            let entered = Arc::clone(&entered);
            std::thread::spawn(move || {
                let conn = pool.get();
                entered.fetch_add(1, Ordering::SeqCst);
                drop(conn);
            })
        };

        // Give the waiter every chance to have taken a connection it should
        // not have.
        for _ in 0..50 {
            if entered.load(Ordering::SeqCst) > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            entered.load(Ordering::SeqCst),
            0,
            "the pool handed out a second connection from a pool of one"
        );
        drop(held);
        waiter.join().expect("the waiter finishes");
        assert_eq!(entered.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_panicking_holder_returns_its_connection() {
        let pool = Arc::new(memory_pool(1));
        let taken = Arc::clone(&pool);
        let panicked = std::thread::spawn(move || {
            let _conn = taken.get();
            panic!("something went wrong mid-statement");
        })
        .join();
        assert!(panicked.is_err(), "the thread was supposed to panic");
        // Would hang forever if the connection had been lost with the thread.
        let recovered = pool.get();
        recovered
            .execute_batch("SELECT 1;")
            .expect("the connection still works");
    }

    #[test]
    fn a_size_of_zero_is_raised_to_one_rather_than_deadlocking() {
        let pool = memory_pool(0);
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn an_absurd_size_is_clamped() {
        let pool = memory_pool(10_000);
        assert_eq!(pool.size(), MAX_MAX_CONNECTIONS);
    }

    #[test]
    fn pragmas_put_a_file_backed_database_into_wal() {
        let dir = std::env::temp_dir().join(format!(
            "agentos-pool-wal-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("the directory is creatable");
        let path = dir.join("store.sqlite");
        let conn = Connection::open(&path).expect("the database opens");
        apply_pragmas(&conn, true).expect("the pragmas apply");
        let mode: String = conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("the mode reads back");
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        let timeout: i64 = conn
            .query_row("PRAGMA busy_timeout;", [], |row| row.get(0))
            .expect("the timeout reads back");
        assert_eq!(timeout, BUSY_TIMEOUT.as_millis() as i64);
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
