//! R4 / `MCP-002`: dynamic MCP tools retain and enforce their own server
//! sandbox witness through full runtime construction.

#![cfg(unix)]

mod support;

use agentos_core::runtime::{register_configured_mcp, AgentRuntime, RuntimePaths};
use agentos_core::sandbox::{self, Availability};
use agentos_core::tools::ToolRegistry;
use agentos_proto::{ToolCall, ToolCallId, ToolStatus};
use serde_json::value::RawValue;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    .expect("python3 is required for the MCP runtime suite")
}

/// AF-021: no generic isolation worker is configured. The dynamic MCP tool is
/// callable because its retained client owns the matching server sandbox.
#[tokio::test]
async fn sandboxed_mcp_tool_registers_and_runs_through_runtime() {
    let mechanism = match sandbox::availability() {
        Availability::Enforced(mechanism) => mechanism,
        Availability::Unavailable(reason) => {
            panic!("native R4 job requires an enforceable sandbox: {reason}")
        }
    };
    eprintln!("native MCP sandbox mechanism: {mechanism}");

    let tree = support::temp_tree("mcp-runtime");
    let config_path = tree.path().join("agent.toml");
    let target = tree.path().join("written-inside-boundary.txt");
    std::fs::write(
        &config_path,
        format!(
            r#"
[agent]
id = "mcp-runtime-test"

[memory]
semantic_backend = "none"

[resources]
priority = ["mcp"]

[resources.skills]
enabled = []

[resources.tools]
enabled = []

[resources.mcp]
enabled = ["write_file"]

[[mcp_servers]]
id = "sandboxed-fixture"
endpoint = "stdio:{} {} well-behaved"
sandbox = "workspace_write"
timeout_ms = 10000
"#,
            python3(),
            fixture_path().display()
        ),
    )
    .expect("runtime config writes");

    let runtime = AgentRuntime::build(RuntimePaths {
        agent_config_path: config_path,
        session_db_path: tree.path().join("session.sqlite"),
        trace_dir: tree.path().join("traces"),
        workspace_root: tree.path().to_path_buf(),
        skills_dir: tree.path().join("skills"),
        cron_dir: tree.path().join("crons"),
    })
    .await
    .expect("sandboxed MCP runtime builds without a generic worker");
    assert!(runtime.workspace_config.isolation.worker_path.is_none());
    assert!(runtime.workspace_config.isolation.worker_path_env.is_none());

    let scope = runtime.deps_scope();
    let deps = scope.deps();
    let tools = deps.tools.expect("runtime supplies its registry");
    let invocation = ToolCall {
        id: ToolCallId::new("sandboxed-write"),
        name: Arc::from("write_file"),
        args: RawValue::from_string(
            serde_json::json!({"path": target.display().to_string()}).to_string(),
        )
        .expect("valid arguments"),
    };
    let result = tools.call(&invocation).await.expect("registry invokes MCP");
    assert_eq!(result.status, ToolStatus::Succeeded, "{}", result.content);
    assert!(target.is_file(), "the write inside the boundary must land");

    // The same startup registration path refuses a boundary it cannot build.
    let mut second_registry = ToolRegistry::new();
    let missing_root = tree.path().join("missing-workspace-root");
    let refusal = match register_configured_mcp(
        &mut second_registry,
        &runtime.workspace_config,
        &missing_root,
    )
    .await
    {
        Ok(_) => panic!("a missing writable boundary must refuse registration"),
        Err(refusal) => refusal,
    };
    assert!(
        refusal.contains("cannot grant writes beneath") || refusal.contains("sandbox"),
        "got: {refusal}"
    );

    runtime
        .shutdown_mcp(Instant::now() + Duration::from_secs(5))
        .await;
}
