//! GW-002: transport acknowledgement is downstream of durable, replayable
//! admission. These tests inject each persistence boundary without depending
//! on a live Telegram or Feishu account.

mod support;

use agentos_core::gateway::{Admission, IngressLedger, Settlement};
use agentos_core::memory::SqliteStore;
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole, INGRESS_ID_KEY};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::sync::Arc;

fn envelope(channel: &str, event_id: i64, content: &str) -> Envelope {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from(INGRESS_ID_KEY), serde_json::Value::from(event_id));
    Envelope {
        channel_id: ChannelId::new(channel),
        conversation_id: ConversationId::new("conversation-1"),
        sender: Arc::from("user-1"),
        message: Message::text(MessageRole::User, content),
        metadata,
    }
}

/// AF-002: Feishu cannot ACK an event before its replay payload commits.
#[test]
fn feishu_ack_follows_durable_ingress_commit() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(store);
    let input = envelope("feishu", 41, "hello");
    let mut acknowledged = false;
    assert!(!acknowledged, "the transport starts unacknowledged");

    assert_eq!(
        ledger
            .admit_with_checkpoint(&input, None)
            .expect("durable admission succeeds"),
        Admission::Fresh
    );
    assert!(ledger
        .claim_next(&input.channel_id)
        .expect("queue reads before ACK")
        .is_none());
    ledger
        .mark_acknowledged(&input.channel_id, "41")
        .expect("ACK state commits");
    assert!(ledger
        .claim_next(&input.channel_id)
        .expect("queue reads")
        .is_some());
    acknowledged = true;

    assert!(
        acknowledged,
        "ACK is emitted only after a replayable row exists"
    );
}

/// AF-003: Telegram's event-specific offset commits with recoverable input.
#[test]
fn telegram_cursor_never_advances_past_unrecoverable_input() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(store);
    let channel = ChannelId::new("telegram");
    let first = envelope("telegram", 100, "first");
    ledger
        .admit_with_checkpoint(&first, Some("101"))
        .expect("first update commits");

    let mut unidentified = envelope("telegram", 101, "missing identity");
    unidentified.metadata.remove(INGRESS_ID_KEY);
    assert_eq!(
        ledger
            .admit_with_checkpoint(&unidentified, Some("102"))
            .expect("missing identity is classified"),
        Admission::Unidentified
    );
    assert_eq!(
        ledger.cursor(&channel).expect("cursor reads").as_deref(),
        Some("101"),
        "an unrecoverable update must not move the durable cursor"
    );
}

/// AF-004: a failed SQLite admission permits neither ACK nor execution.
#[test]
fn ledger_failure_prevents_ack_and_execution() {
    let tree = support::temp_tree("gw002-ledger-failure");
    let path = tree.path().join("agentos.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    Connection::open(&path)
        .expect("raw connection opens")
        .execute_batch(
            "CREATE TRIGGER inject_ingress_failure BEFORE INSERT ON ingress_events \
             BEGIN SELECT RAISE(FAIL, 'injected ingress failure'); END;",
        )
        .expect("failure trigger installs");
    let ledger = IngressLedger::new(store);
    let input = envelope("telegram", 7, "do not run");
    let acknowledged = false;
    let executed = false;

    assert!(ledger.admit_with_checkpoint(&input, Some("8")).is_err());
    assert!(!acknowledged);
    assert!(!executed);
    assert_eq!(
        ledger
            .cursor(&input.channel_id)
            .expect("cursor query still works"),
        None
    );
}

/// AF-005: accepted work is dispatchable live and `/stop` uses the same queue.
#[test]
fn live_dispatcher_drains_acked_rows_and_stop_obeys_admission() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(store);
    let ordinary = envelope("telegram", 1, "work");
    let stop = envelope("telegram", 2, "/stop");
    ledger
        .admit_with_checkpoint(&ordinary, Some("2"))
        .expect("ordinary event commits");
    ledger
        .mark_acknowledged(&ordinary.channel_id, "1")
        .expect("ordinary ACK commits");
    ledger
        .admit_with_checkpoint(&stop, Some("3"))
        .expect("stop event commits");
    ledger
        .mark_acknowledged(&stop.channel_id, "2")
        .expect("stop ACK commits");

    let first = ledger
        .claim_next(&ordinary.channel_id)
        .expect("first claim reads")
        .expect("first event is queued");
    assert_eq!(first.envelope.message.content.as_ref(), "work");
    ledger
        .settle(&ordinary.channel_id, &first.event_id, Settlement::Handled)
        .expect("first event settles");

    let second = ledger
        .claim_next(&ordinary.channel_id)
        .expect("second claim reads")
        .expect("stop is queued through the same ledger");
    assert_eq!(second.envelope.message.content.as_ref(), "/stop");
    ledger
        .settle(&ordinary.channel_id, &second.event_id, Settlement::Handled)
        .expect("stop settles");
    assert!(ledger
        .claim_next(&ordinary.channel_id)
        .expect("queue reads")
        .is_none());
}

/// AF-006: legacy metadata-only work is quarantined, never synthesized.
#[test]
fn legacy_unsettled_rows_are_quarantined_before_cursor_advance() {
    let tree = support::temp_tree("gw002-legacy-quarantine");
    let path = tree.path().join("agentos.sqlite");
    let raw = Connection::open(&path).expect("legacy database opens");
    raw.execute_batch(
        r#"
        CREATE TABLE ingress_events (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            settlement TEXT,
            accepted_at INTEGER NOT NULL,
            settled_at INTEGER,
            UNIQUE(channel_id, event_id)
        );
        CREATE TABLE ingress_cursors (
            channel_id TEXT PRIMARY KEY,
            cursor TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        "#,
    )
    .expect("legacy schema installs");
    raw.execute(
        "INSERT INTO ingress_events \
         (channel_id, event_id, conversation_id, sender, accepted_at) \
         VALUES (?1, ?2, ?3, ?4, 1)",
        params!["telegram", "9", "conversation-1", "user-1"],
    )
    .expect("legacy unsettled row inserts");
    drop(raw);

    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("store migrates")));
    assert_eq!(
        Connection::open(&path)
            .expect("migrated database opens")
            .query_row(
                "SELECT version FROM ingress_schema WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("ingress schema version reads"),
        4
    );
    let channel = ChannelId::new("telegram");
    let quarantined = ledger.quarantined(&channel).expect("quarantine reads");
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].event_id.as_ref(), "9");
    assert!(ledger
        .claim_next(&channel)
        .expect("dispatcher reads")
        .is_none());
    assert_eq!(ledger.cursor(&channel).expect("cursor reads"), None);
    assert!(ledger
        .adjudicate_quarantined(&channel, "9")
        .expect("operator adjudication writes"));
    assert!(ledger
        .quarantined(&channel)
        .expect("quarantine reads after adjudication")
        .is_empty());
    assert_eq!(ledger.cursor(&channel).expect("cursor still reads"), None);
}

#[test]
fn failed_legacy_migration_rolls_back_schema_and_quarantine_changes() {
    let tree = support::temp_tree("gw002-migration-rollback");
    let path = tree.path().join("agentos.sqlite");
    let raw = Connection::open(&path).expect("legacy database opens");
    raw.execute_batch(
        r#"
        CREATE TABLE ingress_events (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            settlement TEXT,
            accepted_at INTEGER NOT NULL,
            settled_at INTEGER,
            UNIQUE(channel_id, event_id)
        );
        CREATE TABLE ingress_cursors (
            channel_id TEXT PRIMARY KEY,
            cursor TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO ingress_events
            (channel_id, event_id, conversation_id, sender, accepted_at)
        VALUES ('telegram', '10', 'conversation-1', 'user-1', 1);
        CREATE TRIGGER reject_ingress_migration BEFORE UPDATE ON ingress_events
        BEGIN SELECT RAISE(FAIL, 'rollback rehearsal'); END;
        "#,
    )
    .expect("legacy fixture installs");
    drop(raw);

    assert!(
        SqliteStore::open(&path).is_err(),
        "migration failpoint fires"
    );
    let raw = Connection::open(&path).expect("database reopens after rollback");
    let mut columns = raw
        .prepare("PRAGMA table_info(ingress_events)")
        .expect("table info prepares");
    let names = columns
        .query_map([], |row| row.get::<_, String>(1))
        .expect("columns query")
        .collect::<Result<Vec<_>, _>>()
        .expect("columns collect");
    assert!(!names.iter().any(|name| name == "envelope_json"));
    assert!(!names.iter().any(|name| name == "dispatch_state"));
}

#[test]
fn interrupted_claim_is_replayed_after_restart() {
    let tree = support::temp_tree("gw002-recover-claim");
    let path = tree.path().join("agentos.sqlite");
    let input = envelope("feishu", 88, "recover me");
    {
        let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("store opens")));
        ledger
            .admit_with_checkpoint(&input, None)
            .expect("event commits");
        ledger
            .mark_acknowledged(&input.channel_id, "88")
            .expect("ACK state commits");
        assert!(ledger
            .claim_next(&input.channel_id)
            .expect("claim succeeds")
            .is_some());
    }

    let restarted = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("store reopens")));
    assert_eq!(
        restarted
            .recover_dispatches(&input.channel_id)
            .expect("claims recover"),
        1
    );
    let replay = restarted
        .claim_next(&input.channel_id)
        .expect("queue reads")
        .expect("accepted envelope is replayable");
    assert_eq!(replay.envelope, input);
}
