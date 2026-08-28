use agentos_core::audit::{self, SafetyEvent, SafetyEventKind, SafetyLog, SafetyOutcome};
use agentos_core::memory::SqliteStore;
use agentos_interfaces::session::{Item, Session};
use agentos_proto::{AgentId, ChannelId, ConversationId, Message, MessageRole, Principal};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn database(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("agentos-{name}-{nonce}.sqlite"))
}

fn old_event(store: &SqliteStore, path: &PathBuf, subject: &str) {
    SafetyLog::append(
        store,
        &SafetyEvent::new(
            SafetyEventKind::PolicyDenial,
            SafetyOutcome::Denied,
            subject,
        ),
    )
    .expect("event is recorded");
    rusqlite::Connection::open(path)
        .expect("database reopens")
        .execute(
            "UPDATE safety_events SET recorded_at = '2000-01-01 00:00:00' WHERE subject = ?1",
            [subject],
        )
        .expect("event is backdated");
}

#[tokio::test]
/// AF-033: count drift or marker failure rolls back the purge transaction.
async fn purge_count_race_or_marker_failure_rolls_back_everything() {
    let path = database("audit-purge-race");
    let store = SqliteStore::open(&path).expect("store opens");
    old_event(&store, &path, "reported");
    let cutoff = 1_800_000_000;
    let expected = audit::count_before(&store, cutoff).expect("report succeeds");

    old_event(&store, &path, "arrived-after-report");
    audit::purge_before(&store, cutoff, expected, "test operator")
        .expect_err("a stale report must not authorize a different deletion");
    assert_eq!(
        audit::count_before(&store, cutoff)
            .expect("rows remain countable")
            .safety_events,
        2
    );

    let expected = audit::count_before(&store, cutoff).expect("fresh report succeeds");
    rusqlite::Connection::open(&path)
        .expect("database reopens")
        .execute_batch(
            "CREATE TRIGGER fail_audit_purge_marker
             BEFORE INSERT ON safety_events
             WHEN NEW.kind = 'audit_purged'
             BEGIN SELECT RAISE(ABORT, 'marker unavailable'); END;",
        )
        .expect("failure trigger installs");
    audit::purge_before(&store, cutoff, expected, "test operator")
        .expect_err("a marker failure must abort the deletion transaction");
    assert_eq!(
        audit::count_before(&store, cutoff)
            .expect("rolled-back rows remain countable")
            .safety_events,
        2
    );

    let session_path = database("session-purge-marker");
    let session_store = SqliteStore::open(&session_path).expect("session store opens");
    let principal = Principal::conversation(
        AgentId::new("agent"),
        ChannelId::new("test"),
        ConversationId::new("conversation"),
    );
    Session::append(
        &session_store,
        &principal,
        vec![Item {
            message: Message::text(MessageRole::User, "retain me"),
            metadata: Default::default(),
        }],
    )
    .await
    .expect("session item is stored");
    rusqlite::Connection::open(&session_path)
        .expect("session database reopens")
        .execute_batch(
            "CREATE TRIGGER fail_session_purge_marker
             BEFORE INSERT ON safety_events
             WHEN NEW.kind = 'session_purged'
             BEGIN SELECT RAISE(ABORT, 'marker unavailable'); END;",
        )
        .expect("session marker failure trigger installs");
    session_store
        .purge_session(&principal, 1, "test operator")
        .expect_err("session marker failure aborts deletion");
    assert_eq!(
        session_store
            .session_log(&principal)
            .expect("rolled-back session remains readable")
            .len(),
        1
    );
}
