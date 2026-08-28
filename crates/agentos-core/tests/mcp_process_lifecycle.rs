//! R4 / `MCP-004`: process ownership and failure bounds for stdio MCP.

#![cfg(unix)]

use agentos_core::tools::mcp::{StdioMcpClient, MAX_RESTARTS_PER_WINDOW};
use agentos_interfaces::mcp::{McpClient, McpServer};
use agentos_proto::{ToolCall, ToolCallId};
use serde_json::value::RawValue;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

mod support;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp_server.py")
}

fn python3() -> &'static str {
    [
        "/usr/bin/python3",
        "/usr/local/bin/python3",
        "/opt/homebrew/bin/python3",
    ]
    .into_iter()
    .find(|candidate| Path::new(candidate).exists())
    .expect("python3 is required for MCP process lifecycle tests")
}

fn server(mode: &str, argument: Option<&Path>) -> McpServer {
    let argument = argument
        .map(|path| format!(" {}", path.display()))
        .unwrap_or_default();
    McpServer {
        id: Arc::from(format!("lifecycle-{mode}")),
        endpoint: Arc::from(format!(
            "stdio:{} {} {mode}{argument}",
            python3(),
            fixture_path().display()
        )),
    }
}

fn call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("active-call"),
        name: Arc::from("echo"),
        args: RawValue::from_string(r#"{"text":"never"}"#.to_owned()).expect("valid JSON"),
    }
}

fn process_exists(pid: i32) -> bool {
    // SAFETY: signal 0 performs existence/permission checking and does not
    // deliver a signal. The fixture PID came from the child itself.
    unsafe { libc::kill(pid, 0) == 0 }
}

async fn assert_process_gone(pid: i32) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while process_exists(pid) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!process_exists(pid), "process {pid} survived MCP shutdown");
}

/// AF-023: shutdown uses the retained group handle even while an active call
/// holds another `Arc<Connection>`, and reaps both server and descendant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_active_calls_and_kills_the_process_group() {
    let tree = support::temp_tree("mcp-process-lifecycle");
    let pids = tree.path().join("pids.txt");
    let server = Arc::new(server("process-lifecycle", Some(&pids)));
    let client = Arc::new(StdioMcpClient::with_timeout(Duration::from_secs(30)));
    client.list_tools(&server).await.expect("tools list");

    let recorded = std::fs::read_to_string(&pids).expect("fixture records its process group");
    let pids: Vec<i32> = recorded
        .split_whitespace()
        .map(|pid| pid.parse().expect("numeric pid"))
        .collect();
    assert_eq!(pids.len(), 2, "server and descendant pids");

    let active = tokio::spawn({
        let client = Arc::clone(&client);
        let server = Arc::clone(&server);
        async move { client.call_tool(&server, &call()).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    client.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "shutdown exceeded its bound: {:?}",
        started.elapsed()
    );
    assert!(
        active.await.expect("active call joined").is_err(),
        "shutdown must resolve an active call"
    );
    for pid in pids {
        assert_process_gone(pid).await;
    }
}

#[tokio::test]
async fn every_failed_reopen_consumes_restart_budget() {
    let client = StdioMcpClient::with_timeout(Duration::from_millis(200));
    let missing = McpServer {
        id: Arc::from("missing"),
        endpoint: Arc::from("stdio:/definitely/not/an/agentos/mcp/server"),
    };
    for _ in 0..=MAX_RESTARTS_PER_WINDOW {
        client
            .list_tools(&missing)
            .await
            .expect_err("every open should fail");
    }
    let exhausted = client
        .list_tools(&missing)
        .await
        .expect_err("the restart ceiling must stop another spawn");
    assert!(
        exhausted.to_string().contains("not restarting again"),
        "got: {exhausted}"
    );
}

#[tokio::test]
async fn stderr_without_a_newline_is_drained_within_the_ring_bound() {
    let client = StdioMcpClient::with_timeout(Duration::from_secs(10));
    let err = client
        .list_tools(&server("stderr-flood", None))
        .await
        .expect_err("the fixture exits during startup");
    assert!(
        err.to_string().contains("closed"),
        "the no-newline flood must terminate normally, got: {err}"
    );
}
