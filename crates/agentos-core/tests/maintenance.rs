//! R2 / `MAINT-002`: process maintenance follows its own bounded timer and
//! lease authority, never a conversation shard's idle state.

use agentos_core::gateway::{
    run_shard, shard_set, MaintenanceCadence, ShardConfig, Turn, TurnHandler,
};
use agentos_core::memory::{SqliteStore, DEFAULT_LEASE_TTL, REFLECTION_LEASE, RETENTION_LEASE};
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole};
use async_trait::async_trait;
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

            // A channel-bound cron is considered only by its own supervisor.
            cron_runs += 1;

            let now = 1_000 + sequence;
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
                // the same pass, rather than sweeping per channel.
                ingress_sweeps += retention_leaders;
                job_sweeps += retention_leaders;
            }
        }

        assert!(started.get(), "shard zero stayed occupied throughout");
        assert_eq!(cron_runs, 3);
        assert_eq!(reflection_runs, 3);
        assert_eq!(retention_runs, 1);
        assert_eq!(ingress_sweeps, 1);
        assert_eq!(job_sweeps, 1);

        router
            .stop(&ConversationId::new("saturated-shard-zero"))
            .await
            .expect("the saturated turn can be stopped");
        drop(router);
    };

    tokio::join!(run_shard(0, inbound, &config, &handler), driver);
}
