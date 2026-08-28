//! M7 deliverable 10 / `QUOTA-001`: every store that grows has a bound, and
//! the two that must not be swept are not swept.
//!
//! The suite is organised around the split the feature is *about*. The first
//! half drives one real [`RetentionSweep`] over a populated tree and asserts
//! each ceiling removed what it was set to remove. The second half is the more
//! important one: it fills the session log and both audit stores, runs the
//! same sweep with every ceiling turned up as high as it goes, and asserts
//! nothing was touched — because a retention feature that quietly acquired the
//! power to delete the record would pass every test in the first half.

mod support;

use agentos_core::audit::{self, SafetyEvent, SafetyEventKind, SafetyLog, SafetyOutcome};
use agentos_core::config::{RetentionConfig, SpillConfig};
use agentos_core::gateway::{IngressLedger, Settlement};
use agentos_core::jobs::{JobRegistry, JobSpec};
use agentos_core::memory::SqliteStore;
use agentos_core::retention::{cutoff_from_date, RetentionSweep, RetentionTargets};
use agentos_core::spill::{SpillSource, SpillStore};
use agentos_interfaces::session::{Item, Session};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId,
    ToolCallId, INGRESS_ID_KEY,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const DAY: u64 = 24 * 60 * 60;

/// The principal a conversation is keyed by here. One agent and one channel
/// throughout: these tests are about retention, not about identity.
fn principal(conversation: &str) -> Principal {
    Principal::conversation(
        AgentId::new("retention-agent"),
        ChannelId::new("telegram"),
        ConversationId::new(conversation),
    )
}

fn item(text: &str) -> Item {
    Item {
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// Reach into the database directly to backdate rows.
///
/// The alternative is a test that waits four hundred days. Opening a second
/// connection to the same file rather than exposing the store's own is
/// deliberate: nothing in the runtime should gain a way to rewrite a timestamp
/// just because a test needed one.
fn backdate_rows(path: &Path, sql: &str) {
    let conn = rusqlite::Connection::open(path).expect("a second connection to the same file");
    conn.execute(sql, []).expect("backdate");
}

/// Set a file's mtime so age can be asserted without waiting for it.
fn backdate(path: &Path, secs_ago: u64) {
    let when = SystemTime::now() - Duration::from_secs(secs_ago);
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for utimes")
        .set_times(std::fs::FileTimes::new().set_modified(when))
        .expect("backdate");
}

fn write_backdated(path: &Path, bytes: usize, secs_ago: u64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent");
    }
    std::fs::write(path, vec![b'x'; bytes]).expect("write");
    backdate(path, secs_ago);
}

/// A tree with a trace directory, an attachment tree and a gateway log, all
/// backdated so a single sweep has something to find in each.
struct Deployment {
    root: support::TempTree,
}

impl Deployment {
    fn new(label: &str) -> Self {
        Self {
            root: support::temp_tree(label),
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.path().join(relative)
    }

    fn targets(&self) -> RetentionTargets {
        RetentionTargets {
            trace_dir: Some(self.path("traces")),
            attachments_dir: Some(self.path("attachments")),
            gateway_log: Some(self.path("gateway.log")),
        }
    }
}

/// Both ceilings, on every store, applied in one pass over a populated tree.
#[tokio::test]
async fn every_configured_ceiling_removes_what_it_names() {
    let deployment = Deployment::new("retention-all");

    // Traces: one from last month, one from this morning.
    write_backdated(&deployment.path("traces/run-old.jsonl"), 512, 40 * DAY);
    write_backdated(&deployment.path("traces/run-new.jsonl"), 512, 60);

    // Attachments: `<channel>/<conversation>/<message>/<file>`.
    write_backdated(
        &deployment.path("attachments/telegram/conv-1/msg-old/photo.png"),
        4096,
        40 * DAY,
    );
    write_backdated(
        &deployment.path("attachments/telegram/conv-1/msg-new/photo.png"),
        4096,
        60,
    );

    // Gateway log: over the rotation size.
    write_backdated(&deployment.path("gateway.log"), 200_000, 60);

    let store =
        Arc::new(SqliteStore::open(deployment.path("agentos.sqlite")).expect("the store opens"));
    let ledger = IngressLedger::new(Arc::clone(&store));
    let channel = ChannelId::new("telegram");
    for (index, settled) in [(1u32, true), (2, false)] {
        let mut metadata = BTreeMap::new();
        metadata.insert(
            Arc::from(INGRESS_ID_KEY),
            serde_json::Value::from(format!("event-{index}")),
        );
        let envelope = Envelope {
            channel_id: channel.clone(),
            conversation_id: ConversationId::new("conv-1"),
            sender: Arc::from("someone"),
            message: Message::text(MessageRole::User, "hello"),
            metadata,
        };
        ledger.admit(&envelope).expect("admitted");
        if settled {
            ledger
                .settle(&channel, &format!("event-{index}"), Settlement::Handled)
                .expect("settled");
        }
    }
    // Age the settled row past the ceiling. The ledger stamps `settled_at` with
    // the current second, so this is the only way to reach the branch without
    // a month-long test.
    backdate_rows(
        &deployment.path("agentos.sqlite"),
        &format!(
            "UPDATE ingress_events SET settled_at = settled_at - {} \
             WHERE settlement IS NOT NULL",
            40 * DAY
        ),
    );

    let spill = SpillStore::new(deployment.path("spill"));
    for run in ["stale", "fresh"] {
        spill
            .save_text(
                &SpillSource {
                    run_id: &RunId::new(run),
                    call_id: &ToolCallId::new("call-1"),
                    tool_name: "shell",
                },
                "spilled output",
            )
            .await
            .expect("the artifact writes");
    }
    let stale_artifact = std::fs::read_dir(deployment.path("spill/stale"))
        .expect("the stale run has a directory")
        .next()
        .expect("and an artifact")
        .expect("readable")
        .path();
    backdate(&stale_artifact, 40 * DAY);

    let jobs = Arc::new(JobRegistry::default());
    let conversation = principal("conv-1");
    let id = jobs
        .start(
            JobSpec {
                kind: Arc::from("tool"),
                label: Arc::from("finished work"),
                conversation: conversation.clone(),
                output_limit_bytes: None,
            },
            |_sink, _cancel| async move { Ok(Arc::from("done")) },
        )
        .expect("the job starts");
    jobs.wait_for(&conversation, &id, Duration::from_secs(5))
        .await
        .expect("the job finishes");
    assert_eq!(jobs.len(), 1);

    let retention = RetentionConfig {
        trace_days: 7,
        trace_max_bytes: 0,
        attachment_days: 7,
        attachment_max_bytes: 0,
        ingress_days: 7,
        gateway_log_max_bytes: 64 * 1024,
        gateway_log_keep: 2,
    };
    let spill_config = SpillConfig {
        root: PathBuf::from("spill"),
        retention_days: 7,
        max_bytes: 0,
    };
    let targets = deployment.targets();
    let report = RetentionSweep {
        retention: &retention,
        spill_config: &spill_config,
        targets: &targets,
        spill: Some(&spill),
        ingress: Some(&ledger),
        jobs: Some(&jobs),
        // Zero rather than the config floor: the floor is validation's job,
        // and asking the sweep for "every finished job" is what makes this
        // assertable without sleeping through a real retention window.
        completed_job_max_age: Some(Duration::ZERO),
    }
    .run()
    .await;

    assert_eq!(report.traces_removed, 1, "the old trace goes");
    assert!(!deployment.path("traces/run-old.jsonl").exists());
    assert!(deployment.path("traces/run-new.jsonl").exists());

    assert_eq!(report.attachments_removed, 1, "the old message goes whole");
    assert!(!deployment
        .path("attachments/telegram/conv-1/msg-old")
        .exists());
    assert!(deployment
        .path("attachments/telegram/conv-1/msg-new/photo.png")
        .exists());

    assert_eq!(report.spill_runs_removed, 1, "the stale run goes whole");
    assert!(!deployment.path("spill/stale").exists());
    assert!(deployment.path("spill/fresh").exists());

    assert_eq!(report.ingress_rows_removed, 1, "only the settled row goes");
    assert_eq!(
        ledger.unsettled(&channel).expect("unsettled").len(),
        1,
        "the unsettled row is kept at any age: it is the crash record"
    );

    assert_eq!(report.jobs_reaped, 1);
    assert!(jobs.is_empty(), "the finished job is forgotten");

    assert!(report.log_rotated);
    assert!(!deployment.path("gateway.log").exists());
    assert_eq!(
        std::fs::metadata(deployment.path("gateway.log.1"))
            .expect("rotated")
            .len(),
        200_000
    );
}

/// The half that matters. Every ceiling set as aggressively as validation
/// allows, run against a database holding a session log and both audit
/// stores, and none of them is touched.
#[tokio::test]
async fn the_record_survives_the_most_aggressive_sweep() {
    let deployment = Deployment::new("retention-record");
    let store =
        Arc::new(SqliteStore::open(deployment.path("agentos.sqlite")).expect("the store opens"));

    let conversation = principal("conv-keep");
    Session::append(
        store.as_ref(),
        &conversation,
        vec![item("remember"), item("remembered")],
    )
    .await
    .expect("the session appends");

    SafetyLog::append(
        store.as_ref(),
        &SafetyEvent::new(
            SafetyEventKind::PolicyDenial,
            SafetyOutcome::Denied,
            "shell",
        ),
    )
    .expect("the event is recorded");

    let before = store.session_log(&conversation).expect("the log reads");
    assert_eq!(before.len(), 2);
    // A cutoff far enough forward to count everything, expressed the way an
    // operator would: `u64::MAX` overflows SQLite's `datetime(?, 'unixepoch')`
    // and silently counts nothing.
    let everything = cutoff_from_date("2100-01-01").expect("a date parses");
    let events_before = audit::count_before(&store, everything).expect("counted");

    let retention = RetentionConfig {
        trace_days: 1,
        trace_max_bytes: 1024 * 1024,
        attachment_days: 1,
        attachment_max_bytes: 1024 * 1024,
        ingress_days: 1,
        gateway_log_max_bytes: 64 * 1024,
        gateway_log_keep: 1,
    };
    let spill_config = SpillConfig {
        root: PathBuf::from("spill"),
        retention_days: 1,
        max_bytes: 1024 * 1024,
    };
    let ledger = IngressLedger::new(Arc::clone(&store));
    let targets = deployment.targets();
    RetentionSweep {
        retention: &retention,
        spill_config: &spill_config,
        targets: &targets,
        spill: None,
        ingress: Some(&ledger),
        jobs: None,
        completed_job_max_age: Some(Duration::ZERO),
    }
    .run()
    .await;

    assert_eq!(
        store.session_log(&conversation).expect("the log reads"),
        before,
        "no background sweep may remove a session item (ADR-0006)"
    );
    assert_eq!(
        audit::count_before(&store, everything).expect("counted"),
        events_before,
        "no background sweep may remove an audit row (ADR-0005)"
    );
    assert!(events_before.safety_events >= 1);
}

/// A quota with no age ceiling still bounds the store, and takes the oldest
/// first — the case an age-only retention story misses entirely.
#[tokio::test]
async fn a_byte_quota_bounds_a_store_that_no_age_ceiling_would() {
    let deployment = Deployment::new("retention-quota");
    // Three traces written in the last minute. Nothing is old; the disk is
    // still full.
    for (name, age) in [("a", 30u64), ("b", 20), ("c", 10)] {
        write_backdated(
            &deployment.path(&format!("traces/run-{name}.jsonl")),
            400_000,
            age,
        );
    }

    let retention = RetentionConfig {
        trace_days: 0,
        trace_max_bytes: 1024 * 1024,
        attachment_days: 0,
        attachment_max_bytes: 0,
        ingress_days: 0,
        gateway_log_max_bytes: 0,
        gateway_log_keep: 1,
    };
    let targets = deployment.targets();
    let report = RetentionSweep {
        retention: &retention,
        spill_config: &SpillConfig::default(),
        targets: &targets,
        spill: None,
        ingress: None,
        jobs: None,
        completed_job_max_age: None,
    }
    .run()
    .await;

    assert_eq!(
        report.traces_removed, 1,
        "one eviction brings 1.2MB under 1MiB, and the sweep stops there"
    );
    assert!(
        !deployment.path("traces/run-a.jsonl").exists(),
        "the oldest is the one that goes"
    );
    assert!(deployment.path("traces/run-c.jsonl").exists());
}

/// Nothing configured is not "sweep with no limit".
#[tokio::test]
async fn a_default_deployment_deletes_no_files() {
    let deployment = Deployment::new("retention-default");
    write_backdated(&deployment.path("traces/ancient.jsonl"), 32, 365 * DAY);
    write_backdated(
        &deployment.path("attachments/telegram/conv/msg/old.png"),
        32,
        365 * DAY,
    );

    let targets = deployment.targets();
    let report = RetentionSweep {
        retention: &RetentionConfig::default(),
        spill_config: &SpillConfig::default(),
        targets: &targets,
        spill: None,
        ingress: None,
        jobs: None,
        completed_job_max_age: None,
    }
    .run()
    .await;

    assert_eq!(report.traces_removed, 0);
    assert_eq!(report.attachments_removed, 0);
    assert!(deployment.path("traces/ancient.jsonl").exists());
    assert!(deployment
        .path("attachments/telegram/conv/msg/old.png")
        .exists());
}

/// The other half of the story: what does bound the two stores the sweep will
/// not touch, and that the deletion is itself recorded.
#[tokio::test]
async fn the_audit_purge_is_authorized_reported_and_recorded() {
    let deployment = Deployment::new("retention-audit");
    let store = SqliteStore::open(deployment.path("agentos.sqlite")).expect("the store opens");

    for subject in ["shell", "file_write", "http_get"] {
        SafetyLog::append(
            &store,
            &SafetyEvent::new(
                SafetyEventKind::PolicyDenial,
                SafetyOutcome::Denied,
                subject,
            ),
        )
        .expect("the event is recorded");
    }
    // Age them past a cutoff an operator could plausibly type.
    backdate_rows(
        &deployment.path("agentos.sqlite"),
        "UPDATE safety_events SET recorded_at = datetime(recorded_at, '-400 days')",
    );
    // One recent event, which must survive.
    SafetyLog::append(
        &store,
        &SafetyEvent::new(
            SafetyEventKind::Cancellation,
            SafetyOutcome::Stopped,
            "recent",
        ),
    )
    .expect("the event is recorded");

    let cutoff = cutoff_from_date("2026-01-01").expect("a date parses");
    let counted = audit::count_before(&store, cutoff).expect("counted");
    assert_eq!(
        counted.safety_events, 3,
        "the report counts before deleting"
    );

    let expected = audit::count_before(&store, cutoff).expect("counted");
    let purged = audit::purge_before(&store, cutoff, expected, "an operator").expect("purged");
    assert_eq!(purged.safety_events, 3);

    // The store now holds the recent event and, newest of all, the record that
    // the purge happened. Deletion is never an absence, even here.
    let remaining = SafetyLog::recent(&store, 10).expect("readable");
    assert_eq!(remaining.len(), 2);
    assert!(
        remaining
            .iter()
            .any(|stored| stored.event.kind == SafetyEventKind::AuditPurged),
        "the purge writes its own record into the store it shortened"
    );
    assert!(
        remaining
            .iter()
            .any(|stored| stored.event.subject.as_ref() == "recent"),
        "a row after the cutoff is untouched"
    );
    assert_eq!(
        audit::count_before(&store, cutoff)
            .expect("counted")
            .safety_events,
        0
    );
}

/// Idle conversations are surveyed, not swept: the survey names them and
/// removes nothing.
#[tokio::test]
async fn idle_conversations_are_surveyed_without_being_touched() {
    let deployment = Deployment::new("retention-idle");
    let store = SqliteStore::open(deployment.path("agentos.sqlite")).expect("the store opens");

    for name in ["stale-1", "stale-2", "live"] {
        Session::append(&store, &principal(name), vec![item(name)])
            .await
            .expect("the session appends");
    }
    backdate_rows(
        &deployment.path("agentos.sqlite"),
        "UPDATE session_items SET created_at = datetime(created_at, '-400 days') \
         WHERE conversation_key LIKE '%.stale-%'",
    );

    let cutoff = cutoff_from_date("2026-01-01").expect("a date parses");
    let idle = store.idle_conversations(cutoff).expect("surveyed");
    assert_eq!(idle.len(), 2, "only the backdated conversations are idle");
    assert!(idle
        .iter()
        .all(|(principal, _)| principal.conversation.as_str().starts_with("stale-")));

    // The survey is read-only. Everything is still there until an operator
    // names a count back.
    for name in ["stale-1", "stale-2", "live"] {
        assert_eq!(
            store
                .session_log(&principal(name))
                .expect("the log reads")
                .len(),
            1,
            "{name} still has its item"
        );
    }

    // And the purge, once authorized, removes the conversation whole.
    let removed = store
        .purge_session(&principal("stale-1"), 1, "test operator")
        .expect("purged");
    assert_eq!(removed, 1);
    assert_eq!(store.idle_conversations(cutoff).expect("surveyed").len(), 1);
}
