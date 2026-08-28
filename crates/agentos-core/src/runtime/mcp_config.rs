//! Registering MCP servers declared in `agent.toml`.
//!
//! Split out of `runtime/mod.rs` when D3's job registry pushed that file over
//! the 800-line ceiling. MCP wiring is the most self-contained thing in there:
//! it reads one config section, produces tool specs, and nothing else in the
//! runtime depends on how it does it.

use crate::config::WorkspaceConfig;
use crate::tools::{StaticMcpClient, StaticMcpTool, StdioMcpClient, ToolRegistry};
use agentos_interfaces::mcp::{McpClient, McpServer};
use agentos_interfaces::tool::ToolSpec;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct ConfiguredMcp {
    pub specs: Vec<ToolSpec>,
    clients: Vec<Arc<StdioMcpClient>>,
}

impl ConfiguredMcp {
    pub async fn shutdown_by(&self, deadline: Instant) {
        for client in &self.clients {
            client.shutdown_by(deadline).await;
        }
    }
}

pub async fn register_configured_mcp(
    tools: &mut ToolRegistry,
    config: &WorkspaceConfig,
    workspace_root: &std::path::Path,
) -> Result<ConfiguredMcp, String> {
    if config.mcp_servers.is_empty() || config.resources.mcp.enabled.is_empty() {
        return Ok(ConfiguredMcp::default());
    }
    let enabled_mcp = config
        .resources
        .mcp
        .enabled
        .iter()
        .map(Arc::clone)
        .collect::<BTreeSet<_>>();

    let static_client = Arc::new(StaticMcpClient::new(config.mcp_tools.iter().map(|tool| {
        StaticMcpTool {
            server_id: Arc::clone(&tool.server_id),
            spec: ToolSpec {
                name: Arc::clone(&tool.name),
                description: Arc::clone(&tool.description),
                input_schema: serde_json::json!({ "type": "object", "properties": {} }),
                safety: Default::default(),
                sandbox: tool.sandbox,
                timeout_ms: None,
            },
            response: Arc::clone(&tool.response),
        }
    })));

    let mut specs = Vec::new();
    let mut clients = Vec::new();
    for server in &config.mcp_servers {
        let server_ref = McpServer {
            id: Arc::clone(&server.id),
            endpoint: Arc::clone(&server.endpoint),
        };
        if server.endpoint.starts_with("stdio://") || server.endpoint.starts_with("stdio:") {
            // Per-server override, else the deployment's tool deadline.
            // The 10 s constant this replaces was the last hardcoded
            // timeout in the runtime (roadmap D2 / review finding F15).
            let timeout = server
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or_else(|| config.limits.tool_timeout());
            let client = Arc::new(
                StdioMcpClient::with_timeout(timeout)
                    .with_env_passthrough(config.isolation.env_passthrough.iter().cloned())
                    // The server *process* is what gets restricted
                    // (M8 / `MCP-001`, deliverable 7). One client per
                    // server, because the mode is per server.
                    .with_sandbox(server.sandbox, workspace_root.to_path_buf()),
            );
            let dynamic: Arc<dyn McpClient> = client.clone();
            specs.extend(
                tools
                    .register_self_hardened_mcp_server_filtered(server_ref, dynamic, |spec| {
                        enabled_mcp.contains(&spec.name)
                    })
                    .await
                    .map_err(|err| format!("failed to register MCP server {}: {err}", server.id))?,
            );
            clients.push(client);
        } else if config
            .mcp_tools
            .iter()
            .any(|tool| tool.server_id == server.id)
            || server.endpoint.starts_with("static://")
        {
            let client: Arc<dyn McpClient> = static_client.clone();
            specs.extend(
                tools
                    .register_mcp_server_filtered(server_ref, client, |spec| {
                        enabled_mcp.contains(&spec.name)
                    })
                    .await
                    .map_err(|err| format!("failed to register MCP server {}: {err}", server.id))?,
            );
        } else {
            return Err(format!("unsupported MCP endpoint: {}", server.endpoint));
        }
    }
    let registered = specs
        .iter()
        .map(|spec| Arc::clone(&spec.name))
        .collect::<BTreeSet<_>>();
    let missing = enabled_mcp
        .difference(&registered)
        .map(|name| name.as_ref())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "resources.mcp.enabled references unavailable MCP tool(s): {}",
            missing.join(", ")
        ));
    }
    Ok(ConfiguredMcp { specs, clients })
}
