//! M8 / `GW-001`, deliverable 3: crash injection around the ingress ledger
//! loses no accepted message and commits no side effect twice.
//!
//! The gateway loop is reproduced here in four lines — admit, run, settle —
//! because that ordering *is* the guarantee, and a test that used the real
//! loop would be testing tokio. What is real is the database: one file, opened
//! fresh for each simulated process lifetime, which is the only thing that
//! survives the crashes being injected.

mod support;

use agentos_core::gateway::{Admission, IngressLedger, Settlement};
use agentos_core::memory::SqliteStore;
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole, INGRESS_ID_KEY};
use std::path::Path;
use std::sync::Arc;

/// Where a simulated process died.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashAt {
    /// Between the transport handing the event over and the ledger row
    /// existing.
    BeforeEnqueue,
    /// After the event was recorded as accepted, mid-run.
    AfterEnqueue,
    /// After the run finished, before the transport was acknowledged.
    AfterSettle,
    /// No crash.
    Never,
}

/// One process lifetime: open the database, take the event, maybe die.
///
/// Returns whether the run body executed — the "side effect" the acceptance
/// criterion is about.
fn attempt(path: &Path, envelope: &Envelope, crash: CrashAt) -> bool {
    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(path).expect("the store opens")));
    if crash == CrashAt::BeforeEnqueue {
        return false;
    }
    let admission = ledger.admit(envelope).expect("the ledger is writable");
    if !admission.deliver() {
        return false;
    }
    if crash == CrashAt::AfterEnqueue {
        // The run started and the process died in the middle of it. Nothing
        // committed; nothing settled.
        return false;
    }
    let event_id = envelope.ingress_id().expect("the channel set one");
    ledger
        .settle(&envelope.channel_id, &event_id, Settlement::Handled)
        .expect("the settlement writes");
    // `AfterSettle` has no separate arm on purpose: the settlement already
    // landed, so from this process's side it is indistinguishable from a
    // clean finish. What differs is on the *transport's* side — it never got
    // the acknowledgement, so it redelivers, which the caller models.
    true
}

fn envelope(update_id: i64) -> Envelope {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        Arc::from(INGRESS_ID_KEY),
        serde_json::Value::from(update_id),
    );
    Envelope {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("chat-1"),
        sender: Arc::from("user-9"),
        message: Message::text(MessageRole::User, "transfer the money"),
        metadata,
    }
}

/// Every crash point, each followed by the redelivery the transport makes
/// because it never got an acknowledgement. Exactly one committed run in every
/// case: none lost, none duplicated.
#[test]
fn a_crash_at_any_point_yields_exactly_one_committed_run() {
    for crash in [
        CrashAt::BeforeEnqueue,
        CrashAt::AfterEnqueue,
        CrashAt::AfterSettle,
    ] {
        let tree = support::temp_tree("ledger-crash");
        let path = tree.path().join("agentos.sqlite");
        let envelope = envelope(4242);

        let first = attempt(&path, &envelope, crash);
        // The transport re-sends anything it has no acknowledgement for. All
        // three crash points are before the acknowledgement.
        let second = attempt(&path, &envelope, CrashAt::Never);
        // And a third delivery, because a transport may repeat more than once.
        let third = attempt(&path, &envelope, CrashAt::Never);

        let committed = [first, second, third]
            .into_iter()
            .filter(|ran| *ran)
            .count();
        assert_eq!(
            committed, 1,
            "{crash:?}: expected exactly one committed run, got {committed} \
             (first={first}, second={second}, third={third})"
        );
    }
}

/// The one that is not a crash: the transport simply re-sends something it
/// already delivered. Answering it again is the duplicate side effect.
#[test]
fn a_plain_redelivery_of_a_finished_event_runs_nothing() {
    let tree = support::temp_tree("ledger-redeliver");
    let path = tree.path().join("agentos.sqlite");
    let envelope = envelope(7);

    assert!(attempt(&path, &envelope, CrashAt::Never), "the first run");
    for round in 0..5 {
        assert!(
            !attempt(&path, &envelope, CrashAt::Never),
            "redelivery {round} started a second run"
        );
    }
}

/// A crash after enqueue and before settle is the case that must *not* be
/// suppressed: the user asked, and nobody answered. The event stays in the
/// abandoned list until something finishes it.
#[test]
fn an_abandoned_event_is_reported_and_then_re_run() {
    let tree = support::temp_tree("ledger-abandoned");
    let path = tree.path().join("agentos.sqlite");
    let envelope = envelope(11);
    let channel = ChannelId::new("telegram");

    attempt(&path, &envelope, CrashAt::AfterEnqueue);

    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("the store opens")));
    let abandoned = ledger.unsettled(&channel).expect("the query runs");
    assert_eq!(abandoned.len(), 1, "the crashed run left no record");
    assert_eq!(abandoned[0].event_id.as_ref(), "11");
    assert_eq!(abandoned[0].conversation_id.as_str(), "chat-1");
    drop(ledger);

    assert!(
        attempt(&path, &envelope, CrashAt::Never),
        "an abandoned event must run again rather than be suppressed"
    );
    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("the store opens")));
    assert!(ledger
        .unsettled(&channel)
        .expect("the query runs")
        .is_empty());
}

/// Distinct events are not confused for one another however many times any of
/// them is redelivered.
#[test]
fn distinct_events_each_run_once() {
    let tree = support::temp_tree("ledger-distinct");
    let path = tree.path().join("agentos.sqlite");

    let mut committed = 0;
    for update_id in 100..110 {
        for _ in 0..3 {
            if attempt(&path, &envelope(update_id), CrashAt::Never) {
                committed += 1;
            }
        }
    }
    assert_eq!(committed, 10, "ten events, thirty deliveries, ten runs");
}

/// The cursor survives the process that recorded it, which is what makes a
/// restart resume from the last *recorded* event rather than from nothing.
#[test]
fn the_cursor_survives_a_restart() {
    let tree = support::temp_tree("ledger-cursor");
    let path = tree.path().join("agentos.sqlite");
    let channel = ChannelId::new("telegram");

    {
        let ledger =
            IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("the store opens")));
        assert_eq!(ledger.cursor(&channel).expect("the query runs"), None);
        ledger
            .set_cursor(&channel, "4243")
            .expect("the write lands");
    }

    let restarted = IngressLedger::new(Arc::new(
        SqliteStore::open(&path).expect("the store reopens"),
    ));
    assert_eq!(
        restarted
            .cursor(&channel)
            .expect("the query runs")
            .as_deref(),
        Some("4243")
    );
}

/// A channel that names no transport id is delivered rather than dropped, and
/// leaves nothing behind — the ledger cannot dedupe what it cannot recognise,
/// and pretending otherwise would silently swallow messages.
#[test]
fn an_event_with_no_transport_id_is_always_delivered() {
    let tree = support::temp_tree("ledger-anonymous");
    let path = tree.path().join("agentos.sqlite");
    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("the store opens")));
    let mut envelope = envelope(1);
    envelope.metadata.remove(INGRESS_ID_KEY);

    for _ in 0..3 {
        assert_eq!(
            ledger.admit(&envelope).expect("the query runs"),
            Admission::Unidentified
        );
    }
    assert!(ledger
        .unsettled(&envelope.channel_id)
        .expect("the query runs")
        .is_empty());
}
