//! R2 / `MAINT-002`: process maintenance follows its own bounded timer and
//! lease authority, never a conversation shard's idle state.

mod support;

use agentos_core::crons::{CronSchedule, CronScheduler, CronTask};
use agentos_core::gateway::{
    run_shard, shard_set, IngressLedger, MaintenanceCadence, Settlement, ShardConfig, Turn,
    TurnHandler,
};
use agentos_core::jobs::{JobRegistry, JobSpec};
use agentos_core::memory::{SqliteStore, DEFAULT_LEASE_TTL, REFLECTION_LEASE, RETENTION_LEASE};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, INGRESS_ID_KEY,
};
use async_trait::async_trait;
use rusqlite::Connection;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

struct Saturated {
    started: Rc<Cell<bool>>,
}

#[async_trait(?Send)]
impl TurnHandler for Saturated {
    async fn run(&self, turn: Turn) {
        self.started.set(true);
        turn.cancel.cancelled().await;
    }
}

fn envelope() -> Envelope {
    Envelope {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("saturated-shard-zero"),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, "never finish until stopped"),
        metadata: BTreeMap::new(),
    }
}

/// AF-029: even with shard zero continuously occupied, every process-owned
/// maintenance class runs by its configured deadline and the existing SQLite
/// leases elect exactly one of two channel contenders.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_runs_while_shard_zero_is_saturated() {
    let config = ShardConfig {
        shards: 1,
        inbox_capacity: 8,
        idle_interval: Duration::from_secs(1),
    };
    let (router, mut inbounds) = shard_set(&config);
    let inbound = inbounds.pop().expect("one shard exists");
    let started = Rc::new(Cell::new(false));
    let handler = Saturated {
        started: Rc::clone(&started),
    };
    let store = SqliteStore::open_in_memory().expect("the lease store opens");
    let mut ticker = MaintenanceCadence::new(Duration::from_secs(10), Duration::from_secs(30))
        .expect("the deterministic cadence is valid")
        .start();

    // Real work for each maintenance class, so the assertions below are about
    // what the sweep did rather than about a counter the test incremented.
    const CRON_BASE: u64 = 1_700_000_000;
    let mut crons = CronScheduler::new([CronTask::new(
        "every-minute",
        ChannelId::new("telegram"),
        ConversationId::new("maintenance"),
        "tick",
        CronSchedule::new("* * * * *").expect("the cron expression parses"),
    )]);
    crons
        .anchor_unfired(CRON_BASE)
        .expect("the unfired task anchors to the base clock");

    // A file-backed ledger so the settled row can be backdated: `prune_settled`
    // deletes strictly older than its cutoff, and a row settled this second is
    // not yet old enough for any max age.
    let tree = support::temp_tree("maint-ingress-sweep");
    let ingress_path = tree.path().join("ingress.sqlite");
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open(&ingress_path).expect("the ingress store opens"),
    ));
    let mut swept = envelope();
    swept.conversation_id = ConversationId::new("settled");
    swept
        .metadata
        .insert(Arc::from(INGRESS_ID_KEY), serde_json::Value::from("1"));
    ledger.admit(&swept).expect("the row is admitted");
    ledger
        .settle(&swept.channel_id, "1", Settlement::Handled)
        .expect("the row is settled and therefore sweepable");
    Connection::open(&ingress_path)
        .expect("the ingress database reopens")
        .execute(
            "UPDATE ingress_events SET settled_at = settled_at - 3600",
            [],
        )
        .expect("the settled row is backdated past every retention cutoff");

    let jobs = Arc::new(JobRegistry::new(2, 4096));
    let job_owner = Principal::conversation(
        AgentId::new("agent"),
        ChannelId::new("telegram"),
        ConversationId::new("maintenance"),
    );
    let finished = jobs
        .start(
            JobSpec {
                kind: Arc::from("tool"),
                label: Arc::from("finished work"),
                conversation: job_owner.clone(),
                output_limit_bytes: None,
            },
            |_sink, _cancel| async move { Ok(Arc::from("done")) },
        )
        .expect("the job starts");
    jobs.wait_for(&job_owner, &finished, Duration::from_secs(5))
        .await
        .expect("the job finishes before the sweep");

    let driver = async {
        router
            .deliver(envelope())
            .await
            .expect("the saturating turn is admitted");
        while !started.get() {
            tokio::task::yield_now().await;
        }

        let mut cron_runs = 0;
        let mut reflection_runs = 0;
        let mut retention_runs = 0;
        let mut ingress_sweeps = 0;
        let mut job_sweeps = 0;
        for sequence in 1..=3 {
            tokio::time::advance(Duration::from_secs(10)).await;
            let tick = ticker.tick().await;
            assert!(tick.lag <= Duration::from_millis(1));

            let now = 1_000 + sequence;

            // A channel-bound cron is considered only by its own supervisor.
            // Evaluated for real against this tick's clock: a counter that
            // incremented on every pass would stay green even if the cadence
            // never reached the cron path at all.
            let cron_now = CRON_BASE + 60 * sequence;
            let due = crons
                .due_invocations(cron_now)
                .expect("the cron schedule evaluates");
            cron_runs += due.len();
            for invocation in &due {
                crons
                    .record_success(&invocation.task_id, cron_now)
                    .expect("a dispatched cron records its fire");
            }
            let reflection_leaders = ["test:telegram", "test:feishu"]
                .into_iter()
                .filter(|holder| {
                    store
                        .acquire_lease_at(REFLECTION_LEASE, holder, DEFAULT_LEASE_TTL, now)
                        .expect("reflection lease decision succeeds")
                        .is_some()
                })
                .count();
            assert_eq!(reflection_leaders, 1, "reflection has one leader");
            reflection_runs += reflection_leaders;

            if tick.retention_due {
                let retention_leaders = ["test:telegram", "test:feishu"]
                    .into_iter()
                    .filter(|holder| {
                        store
                            .acquire_lease_at(RETENTION_LEASE, holder, DEFAULT_LEASE_TTL, now)
                            .expect("retention lease decision succeeds")
                            .is_some()
                    })
                    .count();
                assert_eq!(retention_leaders, 1, "retention has one leader");
                retention_runs += retention_leaders;
                // The retention leader owns these two process-wide stores in
                // the same pass, rather than sweeping per channel. Both sweeps
                // run for real and are asserted by what they removed.
                if retention_leaders == 1 {
                    ingress_sweeps += ledger
                        .prune_settled(Duration::ZERO)
                        .expect("the ingress sweep runs under the retention lease");
                    job_sweeps += jobs.reap_completed(Duration::ZERO);
                }
            }
        }

        assert!(started.get(), "shard zero stayed occupied throughout");
        assert_eq!(cron_runs, 3, "one due cron invocation per bounded tick");
        assert_eq!(reflection_runs, 3);
        assert_eq!(retention_runs, 1);
        assert_eq!(
            ingress_sweeps, 1,
            "the settled ingress row was really pruned"
        );
        assert!(
            ledger
                .unsettled(&ChannelId::new("telegram"))
                .expect("ingress reads after the sweep")
                .is_empty(),
            "the swept ledger keeps nothing behind"
        );
        assert_eq!(job_sweeps, 1, "the finished job was really reaped");

        router
            .stop(&ConversationId::new("saturated-shard-zero"))
            .await
            .expect("the saturated turn can be stopped");
        drop(router);
    };

    tokio::join!(run_shard(0, inbound, &config, &handler), driver);
}
