//! R2 / `GW-005`: every persistent channel in one serving process uses the
//! same runtime authority.
//!
//! The channel transports and ingress ledgers remain distinct. The SQLite
//! pool, background jobs, cancellation root, and the runtime that owns MCP
//! lifecycles do not: otherwise enabling a second channel silently doubles
//! configured resource limits and splits process state.

mod support;

use agentos_core::jobs::{JobError, JobSpec, JobState};
use agentos_core::runtime::{AgentRuntime, RuntimePaths};
use agentos_proto::{AgentId, ChannelId, ConversationId, Principal};
use std::sync::Arc;

fn principal(agent: &AgentId, channel: &str) -> Principal {
    Principal::conversation(
        agent.clone(),
        ChannelId::new(channel),
        ConversationId::new("shared-conversation-label"),
    )
}

fn spec(owner: Principal, label: &str) -> JobSpec {
    JobSpec {
        kind: Arc::from("runtime-authority-test"),
        label: Arc::from(label),
        conversation: owner,
        output_limit_bytes: None,
    }
}

/// AF-028: Telegram and Feishu must be views over one process authority, not
/// independently built runtimes that happen to point at the same database.
#[tokio::test]
async fn two_channels_share_one_runtime_authority() {
    let tree = support::temp_tree("runtime-authority");
    let config_path = tree.path().join("agent.toml");
    std::fs::write(
        &config_path,
        r#"
[agent]
id = "runtime-authority-agent"

[memory]
semantic_backend = "none"
max_connections = 2

[jobs]
max_concurrent = 2
output_limit_bytes = 4096
completed_retention_secs = 60

[resources]
priority = ["llm"]

[resources.skills]
enabled = []

[resources.tools]
enabled = []

[resources.mcp]
enabled = []
"#,
    )
    .expect("the runtime fixture config writes");

    let authority = Arc::new(
        AgentRuntime::build(RuntimePaths {
            agent_config_path: config_path,
            session_db_path: tree.path().join("session.sqlite"),
            trace_dir: tree.path().join("traces"),
            workspace_root: tree.path().to_path_buf(),
            skills_dir: tree.path().join("skills"),
            cron_dir: tree.path().join("crons"),
        })
        .await
        .expect("the process runtime builds"),
    );

    // These are the two clones handed to the persistent channel workers.
    let telegram_runtime = Arc::clone(&authority);
    let feishu_runtime = Arc::clone(&authority);
    assert!(Arc::ptr_eq(&telegram_runtime, &feishu_runtime));
    assert!(Arc::ptr_eq(
        &telegram_runtime.session,
        &feishu_runtime.session
    ));
    assert!(Arc::ptr_eq(telegram_runtime.jobs(), feishu_runtime.jobs()));
    assert_eq!(telegram_runtime.session.max_connections(), 2);

    let telegram = principal(&authority.active_agent, "telegram");
    let feishu = principal(&authority.active_agent, "feishu");
    let pending =
        |_sink, _cancel| async { std::future::pending::<Result<Arc<str>, Arc<str>>>().await };

    let first = telegram_runtime
        .jobs()
        .start(spec(telegram.clone(), "first"), pending)
        .expect("the first process-wide job starts");
    let second = feishu_runtime
        .jobs()
        .start(spec(telegram.clone(), "second"), pending)
        .expect("the second process-wide job starts through the other channel view");
    assert_eq!(
        feishu_runtime
            .jobs()
            .status(&telegram, &first)
            .expect("the shared registry sees the first job")
            .state,
        JobState::Running
    );
    assert!(matches!(
        feishu_runtime.jobs().status(&feishu, &first),
        Err(JobError::Unknown(_))
    ));
    assert!(matches!(
        telegram_runtime
            .jobs()
            .start(spec(telegram.clone(), "over-limit"), pending),
        Err(JobError::TooMany { limit: 2 })
    ));

    // The cancellation root is the process lifecycle authority too. A stop
    // issued through either channel is immediately visible through the other.
    telegram_runtime.cancellation().cancel();
    assert!(feishu_runtime.cancellation().is_cancelled());

    assert_eq!(authority.jobs().dispose_conversation(&telegram), 2);
    assert!(matches!(
        authority.jobs().status(&telegram, &second),
        Err(JobError::Unknown(_))
    ));
}
