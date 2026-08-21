//! Moving memory written before principals onto principal-keyed namespaces.
//!
//! `ID-002`. [`super::scope`] used to build a namespace component with
//! `trimmed.replace('/', "_")`, and a conversation-owned namespace was keyed by
//! a bare conversation id. `ID-001` replaced both, which leaves every row
//! written before it under a name nothing reads any more.
//!
//! # Why this cannot be a silent rewrite
//!
//! The old encoding threw information away, in two different ways, and neither
//! is recoverable from the stored row:
//!
//! - **The separator is ambiguous.** `a/b` and `a_b` both stored as `a_b`. A
//!   legacy component containing `n` underscores has `2^n` possible originals,
//!   and the row does not say which one it was. Worse, if two owners *did*
//!   collide, their records are already interleaved in one namespace and no
//!   migration can separate them.
//! - **The channel is absent.** A conversation-owned namespace recorded the
//!   conversation id and nothing else, so `telegram:42` and `feishu:42` are
//!   indistinguishable after the fact.
//!
//! So this plans first and reports what it cannot decide, rather than picking
//! an interpretation. A component with no underscore has exactly one original
//! and migrates cleanly; anything else is listed for a human. The channel is
//! supplied by the operator, because the alternative is inventing one.
//!
//! # What a run guarantees
//!
//! - **Planned before applied.** [`plan`] touches nothing and is what
//!   `--dry-run` prints.
//! - **Never merged.** A rewrite whose target namespace already holds rows is
//!   refused, not appended to. Two principals sharing a namespace is the
//!   failure the whole identity change exists to prevent, and doing it during
//!   the fix would be worse than not fixing it.
//! - **Atomic.** Every rewrite lands in one transaction. A crash mid-way
//!   leaves the database exactly as it was, which is also what makes a rerun
//!   safe: there is no half-migrated state to resume from, only "not yet" or
//!   "done", and `schema_version` says which.

use super::sqlite::SqliteStore;
use agentos_interfaces::memory::MemoryError;
use agentos_proto::{AgentId, ChannelId, ConversationId, Principal};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Schema version written once the identity migration has run.
pub const IDENTITY_SCHEMA_VERSION: i64 = 2;

/// Version of a database that predates `schema_version` entirely.
pub const PRE_PRINCIPAL_SCHEMA_VERSION: i64 = 1;

/// What the operator has to supply, because the stored rows cannot.
#[derive(Clone, Debug)]
pub struct MigrationSettings {
    /// The agent legacy rows belong to. Every deployment before `ID-001` ran
    /// one agent; `runtime/mod.rs` hardcoded `main-agent` regardless of
    /// `agent.id`, so that is the usual answer.
    pub agent: AgentId,
    /// The channel legacy conversation-owned rows arrived on. Not inferable:
    /// the old namespace recorded no channel at all.
    pub channel: ChannelId,
    /// Migrate components whose original is ambiguous by assuming the stored
    /// form was literal — that `a_b` was always `a_b`, never `a/b`.
    ///
    /// Off by default. On a deployment whose ids never contained a slash it is
    /// correct and saves the data; on one where they did it silently picks a
    /// side, which is why it has to be asked for.
    pub assume_literal_underscores: bool,
}

/// One namespace the migration would rewrite.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rewrite {
    pub from: String,
    pub to: String,
    pub records: usize,
}

/// One namespace the migration refuses to touch, and why.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Blocked {
    pub namespace: String,
    pub records: usize,
    pub reason: BlockedReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockedReason {
    /// The legacy component contains `_`, which the old encoder also produced
    /// from `/`. Every possible original is listed.
    AmbiguousSeparator { candidates: Vec<String> },
    /// The namespace this would become already holds rows. Merging two
    /// principals is exactly what must not happen.
    TargetOccupied { target: String },
    /// Two legacy namespaces would migrate to the same target.
    TargetContested { target: String, other: String },
    /// The namespace is not in the five-part shape the encoder produces.
    Unrecognised,
}

impl std::fmt::Display for BlockedReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Only the first few readings: a component with eight underscores
            // has hundreds, and printing them all across dozens of namespaces
            // buries the one line the operator needs to act on.
            Self::AmbiguousSeparator { candidates } => {
                let shown = candidates.len().min(3);
                write!(
                    formatter,
                    "the owner id could have been {}{} before the old encoder replaced \
                     '/' with '_'; pass --assume-literal-underscores if this \
                     deployment's ids never contained a slash",
                    candidates[..shown].join(" or "),
                    if candidates.len() > shown {
                        format!(" (or {} other reading(s))", candidates.len() - shown)
                    } else {
                        String::new()
                    }
                )
            }
            Self::TargetOccupied { target } => write!(
                formatter,
                "'{target}' already holds records; migrating would merge two owners"
            ),
            Self::TargetContested { target, other } => {
                write!(formatter, "would collide with '{other}' at '{target}'")
            }
            Self::Unrecognised => write!(formatter, "not a namespace this encoder produced"),
        }
    }
}

/// What a migration would do, before it does any of it.
#[derive(Clone, Debug, Default)]
pub struct MigrationPlan {
    pub rewrites: Vec<Rewrite>,
    pub blocked: Vec<Blocked>,
    /// Namespaces already keyed by a principal, left alone.
    pub already_migrated: usize,
}

impl MigrationPlan {
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty() && self.blocked.is_empty()
    }

    pub fn records_to_move(&self) -> usize {
        self.rewrites.iter().map(|rewrite| rewrite.records).sum()
    }

    pub fn records_blocked(&self) -> usize {
        self.blocked.iter().map(|blocked| blocked.records).sum()
    }

    /// The report `--dry-run` prints. Blocked entries come first: they are the
    /// part that needs a decision, and burying them under a list of successes
    /// is how a migration gets run without them being read.
    pub fn report(&self) -> String {
        let mut out = String::new();
        if !self.blocked.is_empty() {
            out.push_str(&format!(
                "{} namespace(s) holding {} record(s) will NOT be migrated:\n",
                self.blocked.len(),
                self.records_blocked()
            ));
            for blocked in &self.blocked {
                out.push_str(&format!(
                    "  {} ({} record(s))\n    {}\n",
                    blocked.namespace, blocked.records, blocked.reason
                ));
            }
            out.push('\n');
        }
        out.push_str(&format!(
            "{} namespace(s) holding {} record(s) will be rewritten:\n",
            self.rewrites.len(),
            self.records_to_move()
        ));
        for rewrite in &self.rewrites {
            out.push_str(&format!(
                "  {} -> {} ({} record(s))\n",
                rewrite.from, rewrite.to, rewrite.records
            ));
        }
        if self.already_migrated > 0 {
            out.push_str(&format!(
                "\n{} namespace(s) are already principal-keyed.\n",
                self.already_migrated
            ));
        }
        out
    }
}

/// The schema version this database is at.
pub fn schema_version(store: &SqliteStore) -> Result<i64, MemoryError> {
    let conn = store.memory_conn()?;
    ensure_schema_version_table(&conn)?;
    read_schema_version(&conn)
}

pub(super) fn ensure_schema_version_table(conn: &Connection) -> Result<(), MemoryError> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )
    .map_err(super::memory_sqlite_error)
}

fn read_schema_version(conn: &Connection) -> Result<i64, MemoryError> {
    conn.query_row(
        "SELECT version FROM schema_version WHERE id = 1",
        [],
        |row| row.get(0),
    )
    .or_else(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Ok(PRE_PRINCIPAL_SCHEMA_VERSION),
        other => Err(super::memory_sqlite_error(other)),
    })
}

/// Work out what the migration would do. Reads only.
pub fn plan(
    store: &SqliteStore,
    settings: &MigrationSettings,
) -> Result<MigrationPlan, MemoryError> {
    let conn = store.memory_conn()?;
    let counts = namespace_counts(&conn)?;
    let occupied: Vec<&String> = counts.keys().collect();

    let mut plan = MigrationPlan::default();
    // Targets claimed so far, so two legacy namespaces cannot both land on one.
    let mut claimed: BTreeMap<String, String> = BTreeMap::new();

    for (namespace, records) in &counts {
        let records = *records;
        let parts: Vec<&str> = namespace.split('/').collect();
        let [visibility, owner_kind, owner_id, store_kind, domain] = parts.as_slice() else {
            plan.blocked.push(Blocked {
                namespace: namespace.clone(),
                records,
                reason: BlockedReason::Unrecognised,
            });
            continue;
        };

        // Already principal-keyed, or an owner kind whose encoding did not
        // change, and free of the ambiguity the old encoder introduced.
        if *owner_kind == "conversation" && Principal::from_storage_name(owner_id).is_some() {
            plan.already_migrated += 1;
            continue;
        }

        let candidates = legacy_originals(owner_id);
        let original = match candidates.as_slice() {
            [only] => only.clone(),
            _ if settings.assume_literal_underscores => owner_id.to_string(),
            _ => {
                plan.blocked.push(Blocked {
                    namespace: namespace.clone(),
                    records,
                    reason: BlockedReason::AmbiguousSeparator { candidates },
                });
                continue;
            }
        };

        let encoded_owner = match *owner_kind {
            "conversation" => Principal::conversation(
                settings.agent.clone(),
                settings.channel.clone(),
                ConversationId::new(original.as_str()),
            )
            .storage_name(),
            _ => agentos_proto::encode_component(&original),
        };
        let target = format!("{visibility}/{owner_kind}/{encoded_owner}/{store_kind}/{domain}");
        if target == *namespace {
            plan.already_migrated += 1;
            continue;
        }
        if occupied.contains(&&target) {
            plan.blocked.push(Blocked {
                namespace: namespace.clone(),
                records,
                reason: BlockedReason::TargetOccupied { target },
            });
            continue;
        }
        if let Some(other) = claimed.get(&target) {
            plan.blocked.push(Blocked {
                namespace: namespace.clone(),
                records,
                reason: BlockedReason::TargetContested {
                    target: target.clone(),
                    other: other.clone(),
                },
            });
            continue;
        }
        claimed.insert(target.clone(), namespace.clone());
        plan.rewrites.push(Rewrite {
            from: namespace.clone(),
            to: target,
            records,
        });
    }
    Ok(plan)
}

/// Every original a legacy component could have had.
///
/// The old encoder mapped `/` to `_` and left everything else alone, so an
/// original is any string that becomes this one under that map: each `_` was
/// either itself or a `/`. Capped, because a component with twenty underscores
/// has a million candidates and listing them helps nobody — past the cap it is
/// reported as ambiguous with the first few shown.
fn legacy_originals(encoded: &str) -> Vec<String> {
    const MAX_CANDIDATES: usize = 8;
    let underscores = encoded.bytes().filter(|byte| *byte == b'_').count();
    if underscores == 0 {
        return vec![encoded.to_string()];
    }
    let mut candidates = vec![String::new()];
    for character in encoded.chars() {
        candidates = candidates
            .into_iter()
            .flat_map(|prefix| {
                let mut options = Vec::with_capacity(2);
                options.push(format!("{prefix}{character}"));
                if character == '_' {
                    options.push(format!("{prefix}/"));
                }
                options
            })
            .take(MAX_CANDIDATES)
            .collect();
    }
    candidates
}

/// Apply a plan. Every rewrite lands in one transaction, or none does.
///
/// Returns how many records moved. Blocked entries are skipped — they are
/// reported by [`plan`] and are the operator's call, not this function's.
pub fn apply(store: &SqliteStore, plan: &MigrationPlan) -> Result<usize, MemoryError> {
    let mut conn = store.memory_conn()?;
    ensure_schema_version_table(&conn)?;
    let transaction = conn.transaction().map_err(super::memory_sqlite_error)?;

    let mut moved = 0usize;
    for rewrite in &plan.rewrites {
        // Re-check inside the transaction: the plan was computed against an
        // earlier read, and a merge must not slip through on the strength of
        // a stale count.
        let occupied: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM memory_records WHERE namespace = ?1",
                params![rewrite.to],
                |row| row.get(0),
            )
            .map_err(super::memory_sqlite_error)?;
        if occupied > 0 {
            return Err(MemoryError::Backend(Arc::from(format!(
                "refusing to migrate '{}': '{}' gained records since the plan was made",
                rewrite.from, rewrite.to
            ))));
        }
        moved += transaction
            .execute(
                "UPDATE memory_records SET namespace = ?1 WHERE namespace = ?2",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;
        transaction
            .execute(
                "UPDATE memory_records_fts SET namespace = ?1 WHERE namespace = ?2",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;
        // The access log is append-only history, so its rows are relabelled
        // rather than rewritten in meaning: they record reads and writes that
        // really did happen, under the name that resource now has.
        transaction
            .execute(
                "UPDATE memory_access_log SET namespace = ?1 WHERE namespace = ?2",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;
    }

    // Only claim the new version when nothing was left behind. A database with
    // blocked namespaces is still partly legacy, and saying otherwise would
    // make the next run skip the very rows that need attention.
    if plan.blocked.is_empty() {
        transaction
            .execute(
                "INSERT INTO schema_version (id, version) VALUES (1, ?1)
                 ON CONFLICT(id) DO UPDATE SET version = ?1, updated_at = CURRENT_TIMESTAMP",
                params![IDENTITY_SCHEMA_VERSION],
            )
            .map_err(super::memory_sqlite_error)?;
    }
    transaction.commit().map_err(super::memory_sqlite_error)?;
    Ok(moved)
}

fn namespace_counts(conn: &Connection) -> Result<BTreeMap<String, usize>, MemoryError> {
    let mut statement = conn
        .prepare("SELECT namespace, COUNT(*) FROM memory_records GROUP BY namespace")
        .map_err(super::memory_sqlite_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })
        .map_err(super::memory_sqlite_error)?;
    let mut counts = BTreeMap::new();
    for row in rows {
        let (namespace, count) = row.map_err(super::memory_sqlite_error)?;
        counts.insert(namespace, count);
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::memory::{Memory, Record};
    use agentos_proto::Namespace;

    fn settings() -> MigrationSettings {
        MigrationSettings {
            agent: AgentId::new("main-agent"),
            channel: ChannelId::new("telegram"),
            assume_literal_underscores: false,
        }
    }

    async fn store_with(namespaces: &[(&str, usize)]) -> SqliteStore {
        let store = SqliteStore::open_in_memory().expect("sqlite opens");
        for (namespace, count) in namespaces {
            for index in 0..*count {
                let namespace = Namespace::new(*namespace);
                store
                    .write(
                        &namespace,
                        Record {
                            id: None,
                            namespace: namespace.clone(),
                            body: serde_json::json!({ "index": index }),
                            metadata: Default::default(),
                        },
                    )
                    .await
                    .expect("seed record writes");
            }
        }
        store
    }

    #[tokio::test]
    async fn a_fresh_database_reports_the_pre_principal_version() {
        let store = SqliteStore::open_in_memory().expect("sqlite opens");
        assert_eq!(
            schema_version(&store).expect("version reads"),
            PRE_PRINCIPAL_SCHEMA_VERSION
        );
    }

    #[tokio::test]
    async fn a_legacy_conversation_namespace_gains_its_principal() {
        let store = store_with(&[("private/conversation/42/semantic/general", 3)]).await;
        let plan = plan(&store, &settings()).expect("plan builds");

        assert_eq!(plan.blocked, Vec::new());
        assert_eq!(
            plan.rewrites,
            vec![Rewrite {
                from: "private/conversation/42/semantic/general".to_owned(),
                to: "private/conversation/v1.main-agent.telegram.42.n/semantic/general".to_owned(),
                records: 3,
            }]
        );

        assert_eq!(apply(&store, &plan).expect("migration applies"), 3);
        assert_eq!(
            schema_version(&store).expect("version reads"),
            IDENTITY_SCHEMA_VERSION
        );
        // Re-planning after a clean run finds nothing left to do.
        let second = plan_again(&store);
        assert!(second.rewrites.is_empty() && second.blocked.is_empty());
        assert_eq!(second.already_migrated, 1);
    }

    fn plan_again(store: &SqliteStore) -> MigrationPlan {
        plan(store, &settings()).expect("re-plan builds")
    }

    /// The ambiguity the old encoder created. `a_b` might have been `a/b`, and
    /// the row does not say, so the migration reports instead of choosing.
    #[tokio::test]
    async fn an_ambiguous_owner_id_is_reported_not_guessed() {
        let store = store_with(&[("private/user/a_b/semantic/general", 2)]).await;
        let plan = plan(&store, &settings()).expect("plan builds");

        assert!(plan.rewrites.is_empty(), "nothing may be rewritten blindly");
        let [blocked] = plan.blocked.as_slice() else {
            panic!(
                "expected exactly one blocked namespace, got {:?}",
                plan.blocked
            );
        };
        assert_eq!(blocked.records, 2);
        let BlockedReason::AmbiguousSeparator { candidates } = &blocked.reason else {
            panic!("expected an ambiguity, got {:?}", blocked.reason);
        };
        assert!(candidates.contains(&"a_b".to_owned()));
        assert!(candidates.contains(&"a/b".to_owned()));

        // The database is untouched, and the version is not advanced: this is
        // still a partly-legacy database and the next run must say so.
        assert_eq!(apply(&store, &plan).expect("nothing to apply"), 0);
        assert_eq!(
            schema_version(&store).expect("version reads"),
            PRE_PRINCIPAL_SCHEMA_VERSION
        );
    }

    #[tokio::test]
    async fn the_operator_can_accept_the_literal_reading() {
        let store = store_with(&[("private/user/a_b/semantic/general", 2)]).await;
        let settings = MigrationSettings {
            assume_literal_underscores: true,
            ..settings()
        };
        let plan = plan(&store, &settings).expect("plan builds");

        // `a_b` is already what the new encoder emits for the literal reading,
        // so there is nothing to rewrite — which is the point: opting in says
        // "these were never slashes", and then the data is already correct.
        assert!(plan.blocked.is_empty());
        assert_eq!(plan.already_migrated, 1);
    }

    /// The guarantee that matters most: a migration must never put two owners
    /// in one namespace, even when the target looks free at plan time.
    #[tokio::test]
    async fn a_rewrite_onto_an_occupied_namespace_is_refused() {
        let store = store_with(&[
            ("private/conversation/42/semantic/general", 2),
            (
                "private/conversation/v1.main-agent.telegram.42.n/semantic/general",
                1,
            ),
        ])
        .await;
        let plan = plan(&store, &settings()).expect("plan builds");

        assert!(plan.rewrites.is_empty());
        let [blocked] = plan.blocked.as_slice() else {
            panic!(
                "expected the occupied target to block, got {:?}",
                plan.blocked
            );
        };
        assert!(
            matches!(&blocked.reason, BlockedReason::TargetOccupied { target }
                if target.contains("v1.main-agent.telegram.42.n")),
            "{:?}",
            blocked.reason
        );
    }

    /// And if the target fills up between planning and applying, the apply
    /// refuses rather than merging on the strength of a stale count.
    #[tokio::test]
    async fn apply_rechecks_the_target_inside_the_transaction() {
        let store = store_with(&[("private/conversation/42/semantic/general", 2)]).await;
        let plan = plan(&store, &settings()).expect("plan builds");
        assert_eq!(plan.rewrites.len(), 1);

        let target = Namespace::new(plan.rewrites[0].to.as_str());
        store
            .write(
                &target,
                Record {
                    id: None,
                    namespace: target.clone(),
                    body: serde_json::json!({ "arrived": "after planning" }),
                    metadata: Default::default(),
                },
            )
            .await
            .expect("a concurrent write lands");

        let error = apply(&store, &plan).expect_err("the stale plan must be refused");
        assert!(
            error.to_string().contains("gained records since the plan"),
            "{error}"
        );
        // The transaction rolled back, so the original rows are still there.
        let remaining = plan_again(&store);
        assert_eq!(remaining.blocked.len(), 1);
    }

    #[tokio::test]
    async fn two_legacy_namespaces_cannot_land_on_one_target() {
        // Distinct legacy names that both encode to the same modern one.
        let store = store_with(&[
            ("private/user/a.b/semantic/general", 1),
            ("private/user/~YS5i/semantic/general", 1),
        ])
        .await;
        let plan = plan(&store, &settings()).expect("plan builds");

        assert!(
            plan.blocked
                .iter()
                .any(|blocked| matches!(blocked.reason, BlockedReason::TargetContested { .. }))
                || plan
                    .blocked
                    .iter()
                    .any(|blocked| matches!(blocked.reason, BlockedReason::TargetOccupied { .. })),
            "a contested target must block, got {:?}",
            plan.blocked
        );
        assert!(plan.rewrites.len() <= 1, "at most one may claim the target");
    }

    #[tokio::test]
    async fn a_namespace_of_the_wrong_shape_is_reported_rather_than_mangled() {
        let store = store_with(&[("not/a/namespace", 1)]).await;
        let plan = plan(&store, &settings()).expect("plan builds");
        assert_eq!(plan.blocked.len(), 1);
        assert_eq!(plan.blocked[0].reason, BlockedReason::Unrecognised);
    }

    #[tokio::test]
    async fn the_report_leads_with_what_needs_a_decision() {
        let store = store_with(&[
            ("private/conversation/42/semantic/general", 1),
            ("private/user/a_b/semantic/general", 1),
        ])
        .await;
        let report = plan(&store, &settings()).expect("plan builds").report();
        let blocked_at = report
            .find("will NOT be migrated")
            .expect("blocked section");
        let rewrite_at = report.find("will be rewritten").expect("rewrite section");
        assert!(
            blocked_at < rewrite_at,
            "blocked entries come first:\n{report}"
        );
    }

    #[test]
    fn legacy_originals_enumerates_every_reading() {
        assert_eq!(legacy_originals("ab"), vec!["ab".to_owned()]);
        let two = legacy_originals("a_b");
        assert_eq!(two.len(), 2);
        assert!(two.contains(&"a_b".to_owned()) && two.contains(&"a/b".to_owned()));
        assert_eq!(legacy_originals("a_b_c").len(), 4);
        // Capped rather than exploding on a pathological id.
        assert!(legacy_originals("a_b_c_d_e_f_g_h_i_j").len() <= 8);
    }
}
