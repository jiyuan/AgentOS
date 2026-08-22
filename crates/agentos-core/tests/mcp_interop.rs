//! M8 / `MCP-001`, deliverable 9: interoperability against an MCP server that
//! knows nothing about AgentOS.
//!
//! `tests/fixtures/mcp_server.py` is written against the specification and
//! imports nothing from this repository. That is the whole point: the client
//! it used to be could only talk to a server built against its own types, and
//! a test using such a server would have proved that the two halves of one
//! bespoke dialect agreed with each other.
//!
//! The failure modes have most of the tests, because a protocol client is
//! mostly its behaviour when the peer misbehaves — startup, repeated calls,
//! timeout, crash, restart, stderr, malformed frames, out-of-order replies,
//! and graceful shutdown, none of which had a test before.
//!
//! Requires `python3`, which both CI runners have. A skipped interop test is
//! not an interop test, so this fails rather than skips when it is missing.

#![cfg(unix)]

use agentos_core::tools::mcp::StdioMcpClient;
use agentos_interfaces::mcp::{McpClient, McpServer};
use agentos_interfaces::tool::SandboxMode;
use agentos_proto::{ToolCall, ToolCallId, ToolStatus};
use serde_json::value::RawValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod support;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp_server.py")
}

/// A server running the fixture in `mode`.
fn server(mode: &str) -> McpServer {
    McpServer {
        id: Arc::from(format!("interop-{mode}")),
        endpoint: Arc::from(format!(
            "stdio:{} {} {mode}",
            python3(),
            fixture_path().display()
        )),
    }
}

fn python3() -> String {
    for candidate in [
        "/usr/bin/python3",
        "/usr/local/bin/python3",
        "/opt/homebrew/bin/python3",
    ] {
        if std::path::Path::new(candidate).exists() {
            return candidate.to_owned();
        }
    }
    panic!(
        "python3 is required for the MCP interoperability suite; a skipped interop test is not \
         an interop test"
    );
}

fn client(timeout: Duration) -> StdioMcpClient {
    StdioMcpClient::with_timeout(timeout)
}

fn call(name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: ToolCallId::new(format!("call-{name}")),
        name: Arc::from(name),
        args: RawValue::from_string(arguments.to_string()).expect("valid JSON"),
    }
}

/// Startup: the handshake happens, the tool list is walked to the end, and
/// the same connection serves repeated calls.
#[tokio::test]
async fn a_spec_server_is_initialized_paginated_and_called_repeatedly() {
    let client = client(Duration::from_secs(20));
    let server = server("well-behaved");

    let tools = client.list_tools(&server).await.expect("tools list");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    assert_eq!(
        names,
        vec!["echo", "picture", "explode", "write_file"],
        "all three pages must be walked; a client that stops at the first sees only `echo`"
    );
    // The schema comes from the server, verbatim, so the model sees what the
    // server actually accepts rather than an empty object.
    assert_eq!(
        tools[0].input_schema["properties"]["text"]["type"],
        "string"
    );

    // Repeated calls on one connection: the server is started once and reused.
    for round in 0..5 {
        let result = client
            .call_tool(
                &server,
                &call(
                    "echo",
                    serde_json::json!({ "text": format!("round {round}") }),
                ),
            )
            .await
            .expect("the call succeeds");
        assert_eq!(result.status, ToolStatus::Succeeded);
        assert_eq!(result.content.as_ref(), format!("round {round}"));
    }
    client.shutdown().await;
}

/// Content blocks a transcript cannot hold are described rather than dropped,
/// and `isError` is the tool failing rather than the call failing.
#[tokio::test]
async fn content_blocks_and_tool_errors_survive_the_crossing() {
    let client = client(Duration::from_secs(20));
    let server = server("well-behaved");

    let picture = client
        .call_tool(&server, &call("picture", serde_json::json!({})))
        .await
        .expect("the call succeeds");
    assert_eq!(picture.status, ToolStatus::Succeeded);
    assert!(
        picture.content.contains("here it is") && picture.content.contains("[image image/png"),
        "got: {}",
        picture.content
    );

    let failed = client
        .call_tool(&server, &call("explode", serde_json::json!({})))
        .await
        .expect("a tool error is a successful call");
    assert_eq!(
        failed.status,
        ToolStatus::Failed,
        "isError must reach the model as a failed tool, not as a transport error"
    );
    assert_eq!(failed.content.as_ref(), "the tool failed on purpose");
    client.shutdown().await;
}

/// The defect the rewrite exists for. Under the old client a reply was matched
/// to a request by arrival order, so a stray line shifted every later answer
/// onto the wrong question — permanently, and silently.
#[tokio::test]
async fn stray_lines_and_replies_to_nothing_do_not_shift_the_answers() {
    let client = Arc::new(client(Duration::from_secs(20)));
    let server = server("noisy");
    // Establish the connection first so the list walk is not the thing being
    // measured.
    client.list_tools(&server).await.expect("tools list");

    for round in 0..5 {
        let expected = format!("question {round}");
        let result = client
            .call_tool(
                &server,
                &call("echo", serde_json::json!({ "text": expected.clone() })),
            )
            .await
            .expect("the call succeeds");
        assert_eq!(
            result.content.as_ref(),
            expected,
            "round {round} got another question's answer"
        );
    }
    client.shutdown().await;
}

/// Two calls in flight, answered newest-first. Correlation by id means the
/// order the server chooses does not matter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_order_replies_reach_the_right_callers() {
    let client = Arc::new(client(Duration::from_secs(20)));
    let server = Arc::new(server("out-of-order"));
    client.list_tools(&server).await.expect("tools list");

    let first = tokio::spawn({
        let client = Arc::clone(&client);
        let server = Arc::clone(&server);
        async move {
            client
                .call_tool(
                    &server,
                    &call("echo", serde_json::json!({ "text": "first" })),
                )
                .await
        }
    });
    // The fixture holds the first call until a second arrives, then answers
    // both in reverse.
    let second = tokio::spawn({
        let client = Arc::clone(&client);
        let server = Arc::clone(&server);
        async move {
            client
                .call_tool(
                    &server,
                    &call("echo", serde_json::json!({ "text": "second" })),
                )
                .await
        }
    });

    let first = first.await.expect("joined").expect("the call succeeds");
    let second = second.await.expect("joined").expect("the call succeeds");
    assert_eq!(first.content.as_ref(), "first");
    assert_eq!(second.content.as_ref(), "second");
    client.shutdown().await;
}

/// A timeout cancels one request. It used to kill the server, which took every
/// other conversation's tools down with it.
#[tokio::test]
async fn a_timeout_gives_up_on_the_call_and_keeps_the_server() {
    let client = client(Duration::from_millis(400));
    let server = server("silent");
    // `tools/list` still works, so the connection is real.
    assert!(!client
        .list_tools(&server)
        .await
        .expect("tools list")
        .is_empty());

    let err = client
        .call_tool(
            &server,
            &call("echo", serde_json::json!({ "text": "hello" })),
        )
        .await
        .expect_err("a server that never answers must time out");
    assert!(err.to_string().contains("timed out"), "got: {err}");

    // And the server is still there: the next request is answered.
    assert!(
        !client
            .list_tools(&server)
            .await
            .expect("tools list")
            .is_empty(),
        "the timeout took the whole server down with it"
    );
    client.shutdown().await;
}

/// A crash is reported, and the *next* call restarts the server rather than
/// failing forever.
#[tokio::test]
async fn a_crashed_server_is_reported_and_then_restarted() {
    let client = client(Duration::from_secs(20));
    let server = server("crash");
    client.list_tools(&server).await.expect("tools list");

    let err = client
        .call_tool(
            &server,
            &call("echo", serde_json::json!({ "text": "hello" })),
        )
        .await
        .expect_err("the server exits mid-call");
    let message = err.to_string();
    assert!(
        message.contains("closed") || message.contains("crashing"),
        "the error must say the server went away, got: {message}"
    );
    // The stderr the server wrote on its way out is in the message. It used to
    // go to `Stdio::null()`.
    assert!(
        message.contains("crashing after one call"),
        "the server's own explanation must survive, got: {message}"
    );

    // Restarted on the next use: a fresh process answers the list again.
    assert!(
        !client
            .list_tools(&server)
            .await
            .expect("tools list")
            .is_empty(),
        "a crashed server must be restarted rather than gone for the process's life"
    );
    client.shutdown().await;
}

/// A message past the frame limit drops the connection rather than the
/// process's memory.
#[tokio::test]
async fn an_oversized_message_is_refused_rather_than_allocated() {
    let client = client(Duration::from_secs(20));
    let server = server("oversized");
    client.list_tools(&server).await.expect("tools list");

    let err = client
        .call_tool(
            &server,
            &call("echo", serde_json::json!({ "text": "hello" })),
        )
        .await
        .expect_err("an eight-megabyte line is past the four-megabyte frame limit");
    assert!(err.to_string().contains("closed"), "got: {err}");
    client.shutdown().await;
}

/// Two ways a server can be wrong at the handshake, both refused at startup
/// rather than at the first surprising field.
#[tokio::test]
async fn a_handshake_this_build_cannot_honour_is_refused_at_startup() {
    let client = client(Duration::from_secs(20));
    let err = client
        .list_tools(&server("bad-version"))
        .await
        .expect_err("an unknown protocol revision must be refused");
    assert!(err.to_string().contains("1999-01-01"), "got: {err}");

    let err = client
        .list_tools(&server("no-tools"))
        .await
        .expect_err("a server with no tools capability must be refused");
    assert!(err.to_string().contains("tools"), "got: {err}");
}

/// A server that fails before it can speak explains itself through stderr,
/// which used to be discarded.
#[tokio::test]
async fn a_server_that_dies_at_startup_reports_its_own_stderr() {
    let client = client(Duration::from_secs(10));
    let err = client
        .list_tools(&server("stderr-then-exit"))
        .await
        .expect_err("the server exits with an error");
    assert!(
        err.to_string().contains("MCP_TOKEN is not set"),
        "the operator needs the server's own words, got: {err}"
    );
}

/// A server request this client never claimed a capability for is answered
/// with a structured error. Left unanswered, the server blocks and stops
/// answering us.
#[tokio::test]
async fn a_server_request_for_an_unclaimed_capability_is_answered() {
    let client = client(Duration::from_secs(10));
    let server = server("demanding");
    let tools = client.list_tools(&server).await.expect("tools list");
    assert!(
        !tools.is_empty(),
        "the server kept serving after being refused"
    );
    client.shutdown().await;
}

/// Deliverable 7's acceptance criterion, and the one the old design could not
/// meet at all: MCP calls were routed to the shell-only isolation worker,
/// which could not run them, so the declared mode restricted nothing.
#[tokio::test]
async fn a_read_only_server_cannot_write_to_the_workspace() {
    let tree = support::temp_tree("mcp-readonly");
    let target = tree.path().join("should-not-exist.txt");

    let sandboxed = client(Duration::from_secs(20))
        .with_sandbox(SandboxMode::ReadOnly, tree.path().to_path_buf());
    let server = server("well-behaved");

    let outcome = sandboxed
        .call_tool(
            &server,
            &call(
                "write_file",
                serde_json::json!({ "path": target.display().to_string() }),
            ),
        )
        .await;

    match agentos_core::sandbox::availability() {
        agentos_core::sandbox::Availability::Unavailable(_) => {
            // Fail-closed: where the kernel offers nothing, a sandboxed server
            // does not start. The one thing that must never happen is the
            // write landing.
            let err = outcome.expect_err("a mode the kernel cannot enforce is not a mode");
            assert!(
                err.to_string().contains("sandbox"),
                "the refusal must say why, got: {err}"
            );
        }
        agentos_core::sandbox::Availability::Enforced(_) => {
            // The server started under the sandbox and the write was refused
            // by the kernel, which the tool reports as `isError`.
            let result = outcome.expect("the call itself succeeds");
            assert_eq!(
                result.status,
                ToolStatus::Failed,
                "a read-only server reported a successful write: {}",
                result.content
            );
        }
    }
    assert!(
        !target.exists(),
        "a read-only MCP server wrote to the workspace"
    );
    sandboxed.shutdown().await;
}

/// Shutdown: closing stdin is MCP's stdio "please exit", and the client waits
/// for the process rather than killing it outright.
#[tokio::test]
async fn shutdown_is_graceful_and_leaves_nothing_running() {
    let client = client(Duration::from_secs(20));
    let server = server("well-behaved");
    client.list_tools(&server).await.expect("tools list");

    let started = std::time::Instant::now();
    client.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a graceful shutdown still has to be bounded; took {:?}",
        started.elapsed()
    );

    // And the client is usable again afterwards: shutdown closes connections,
    // it does not poison the client.
    assert!(!client
        .list_tools(&server)
        .await
        .expect("tools list")
        .is_empty());
    client.shutdown().await;
}
