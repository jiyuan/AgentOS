//! Model Context Protocol clients, and the [`Tool`] that fronts one
//! (M8 / `MCP-001`).
//!
//! Two clients, for two different things:
//!
//! - [`StdioMcpClient`] speaks MCP to a real server over stdio: one child
//!   process per configured endpoint, sandboxed, initialized, and spoken to in
//!   JSON-RPC 2.0 with request-id correlation. [`protocol`] is the wire format
//!   and [`connection`] is the transport.
//! - [`StaticMcpClient`] answers from a table in `agent.toml`. It is not a
//!   protocol implementation and never was: it exists so a deployment can
//!   declare a canned tool — the reference `remote_echo` — without running a
//!   server, and so the tool-registry path has something to exercise offline.
//!
//! # Why this was a rewrite rather than a fix
//!
//! What was here before had MCP's method names and none of its semantics: no
//! `initialize`, no version, no capabilities, no `nextCursor`, no content
//! blocks, and a request id that was generated and never checked against the
//! reply. Its `tools/list` took a parameter MCP does not have and returned
//! AgentOS's own `ToolSpec`, so only a server built against this repository
//! could answer it — which is the same as saying it interoperated with
//! nothing. See [`protocol`] for the list.

mod connection;
mod protocol;

#[allow(unused_imports)]
pub use connection::{
    MAX_MESSAGE_BYTES, MAX_PENDING_REQUESTS, MAX_RESTARTS_PER_WINDOW, MAX_STDERR_BYTES,
    MAX_TOOLS_PER_SERVER, MAX_TOOL_PAGES,
};
#[allow(unused_imports)]
pub use protocol::{PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS};

use crate::sandbox::Sandbox;
use agentos_interfaces::mcp::{McpClient, McpError, McpServer};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use connection::{Connection, RestartBudget};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct McpTool {
    server: McpServer,
    client: Arc<dyn McpClient>,
    spec: ToolSpec,
}

#[derive(Clone)]
pub struct StaticMcpTool {
    pub server_id: Arc<str>,
    pub spec: ToolSpec,
    pub response: Arc<str>,
}

#[derive(Default)]
pub struct StaticMcpClient {
    tools: BTreeMap<Arc<str>, Vec<StaticMcpTool>>,
}

impl StaticMcpClient {
    pub fn new(tools: impl IntoIterator<Item = StaticMcpTool>) -> Self {
        let mut grouped: BTreeMap<Arc<str>, Vec<StaticMcpTool>> = BTreeMap::new();
        for tool in tools {
            grouped
                .entry(Arc::clone(&tool.server_id))
                .or_default()
                .push(tool);
        }
        Self { tools: grouped }
    }
}

#[async_trait]
impl McpClient for StaticMcpClient {
    async fn list_tools(&self, server: &McpServer) -> Result<Vec<ToolSpec>, McpError> {
        Ok(self
            .tools
            .get(&server.id)
            .map(|tools| tools.iter().map(|tool| tool.spec.clone()).collect())
            .unwrap_or_default())
    }

    async fn call_tool(&self, server: &McpServer, call: &ToolCall) -> Result<ToolResult, McpError> {
        let tool = self
            .tools
            .get(&server.id)
            .and_then(|tools| tools.iter().find(|tool| tool.spec.name == call.name))
            .ok_or_else(|| {
                McpError::Failed(Arc::from(format!(
                    "unknown static MCP tool '{}' on server '{}'",
                    call.name, server.id
                )))
            })?;
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from("static_mcp"), Value::String("true".to_owned()));
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::clone(&tool.response),
            metadata,
        })
    }
}

/// A real MCP client over stdio, holding one initialized connection per
/// configured server.
pub struct StdioMcpClient {
    timeout: Duration,
    /// What a tool from these servers may do to the filesystem. Applied to
    /// the *server process*, and stamped onto every [`ToolSpec`] it offers.
    sandbox: SandboxMode,
    workspace_root: PathBuf,
    /// `[isolation].env_passthrough`: names an MCP server may read from the
    /// deployment's environment on top of the neutral allowlist
    /// (M4 / `PROC-001`).
    env_passthrough: Vec<String>,
    servers: Mutex<BTreeMap<Arc<str>, ServerSlot>>,
}

struct ServerSlot {
    connection: Option<Arc<Connection>>,
    restarts: RestartBudget,
}

impl Default for StdioMcpClient {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            sandbox: SandboxMode::default(),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            env_passthrough: Vec::new(),
            servers: Mutex::new(BTreeMap::new()),
        }
    }
}

impl StdioMcpClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Self::default()
        }
    }

    /// Names this client's servers may read from the deployment's
    /// environment, on top of the neutral allowlist (M4 / `PROC-001`).
    pub fn with_env_passthrough(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.env_passthrough = names.into_iter().collect();
        self
    }

    /// Restrict the server processes this client starts (M8 / `MCP-001`,
    /// deliverable 7).
    ///
    /// The mode applies to the server child, which is the process that touches
    /// the filesystem. Before this, an MCP tool's declared mode reached the
    /// shell-only isolation worker, which could not run MCP calls — so it
    /// restricted nothing.
    pub fn with_sandbox(mut self, sandbox: SandboxMode, workspace_root: PathBuf) -> Self {
        self.sandbox = sandbox;
        self.workspace_root = workspace_root;
        self
    }

    /// The live connection for `server`, starting or restarting it as needed.
    async fn connection(&self, server: &McpServer) -> Result<Arc<Connection>, McpError> {
        let mut servers = self.servers.lock().await;
        let slot = servers
            .entry(Arc::clone(&server.endpoint))
            .or_insert_with(|| ServerSlot {
                connection: None,
                restarts: RestartBudget::default(),
            });

        if let Some(existing) = slot.connection.as_ref() {
            if existing.is_live() {
                return Ok(Arc::clone(existing));
            }
            // The reader task finished, which only happens when stdout closed.
            // Restarting is bounded: a server that dies on every call must not
            // be restarted on every call.
            slot.connection = None;
            let attempt = slot.restarts.take()?;
            tracing::warn!(
                server = %server.id,
                attempt,
                "MCP server exited; restarting"
            );
        }

        let (program, args) = parse_endpoint(&server.endpoint)?;
        let sandbox = Sandbox::new(self.sandbox, self.workspace_root.clone());
        let env = crate::tools::child_env::minimal(&self.env_passthrough);
        let connection =
            Arc::new(Connection::open(&program, &args, env, &sandbox, self.timeout).await?);
        tracing::info!(
            server = %server.id,
            name = %connection.handshake().server_name,
            version = %connection.handshake().server_version,
            protocol = %connection.handshake().protocol_version,
            "MCP server initialized"
        );
        slot.connection = Some(Arc::clone(&connection));
        Ok(connection)
    }

    /// Close every connection: stdin, then `SIGTERM`, then `SIGKILL`.
    pub async fn shutdown(&self) {
        let mut servers = self.servers.lock().await;
        for slot in servers.values_mut() {
            let Some(connection) = slot.connection.take() else {
                continue;
            };
            // Only shut down a connection nothing else holds. One still in use
            // by an in-flight call is left to `kill_on_drop`.
            if let Some(owned) = Arc::into_inner(connection) {
                owned.shutdown().await;
            }
        }
    }
}

#[async_trait]
impl McpClient for StdioMcpClient {
    async fn list_tools(&self, server: &McpServer) -> Result<Vec<ToolSpec>, McpError> {
        self.connection(server)
            .await?
            .list_tools(self.sandbox, self.timeout)
            .await
    }

    async fn call_tool(&self, server: &McpServer, call: &ToolCall) -> Result<ToolResult, McpError> {
        // MCP takes the arguments object, not this runtime's `ToolCall`. The
        // old client sent a serialized `ToolCall` and expected a serialized
        // `ToolResult` back, which is why only a server built against this
        // repository could answer it.
        let arguments: Value = serde_json::from_str(call.args.get())
            .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
        let connection = self.connection(server).await?;
        let result = connection
            .call_tool(&call.name, &arguments, self.timeout)
            .await?;
        let mut result = protocol::tool_result_from(call.id.clone(), &result).map_err(|err| {
            McpError::Failed(Arc::from(format!(
                "MCP server '{}' returned an unreadable result: {err}{}",
                server.id,
                connection.stderr_context()
            )))
        })?;
        result.metadata.insert(
            Arc::from("mcp_protocol_version"),
            Value::String(connection.handshake().protocol_version.as_ref().to_owned()),
        );
        result.metadata.insert(
            Arc::from("mcp_server_name"),
            Value::String(connection.handshake().server_name.as_ref().to_owned()),
        );
        Ok(result)
    }
}

/// Split a `stdio:` endpoint into a program and its arguments.
///
/// Whitespace-separated, so `stdio:/usr/bin/python3 -u server.py` works — a
/// server that needs arguments used to have no way to say so. No shell: the
/// words are passed to `execve` as written, so nothing here expands a glob,
/// substitutes a variable, or interprets a `;`.
fn parse_endpoint(endpoint: &str) -> Result<(String, Vec<String>), McpError> {
    let raw = endpoint
        .strip_prefix("stdio://")
        .or_else(|| endpoint.strip_prefix("stdio:"))
        .ok_or_else(|| {
            McpError::Failed(Arc::from(format!(
                "unsupported MCP stdio endpoint: {endpoint}"
            )))
        })?;
    let mut words = raw.split_whitespace().map(ToOwned::to_owned);
    let program = words
        .next()
        .filter(|program| !program.is_empty())
        .ok_or_else(|| McpError::Failed(Arc::from("stdio MCP endpoint names no program")))?;
    Ok((program, words.collect()))
}

impl McpTool {
    pub fn new(server: McpServer, client: Arc<dyn McpClient>, spec: ToolSpec) -> Self {
        Self {
            server,
            client,
            spec,
        }
    }
}

#[async_trait]
impl Tool for McpTool {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        let mut result = self
            .client
            .call_tool(&self.server, call)
            .await
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        result.metadata.insert(
            Arc::from("mcp_server_id"),
            Value::String(self.server.id.as_ref().to_owned()),
        );
        result.metadata.insert(
            Arc::from("mcp_endpoint"),
            Value::String(self.server.endpoint.as_ref().to_owned()),
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_splits_into_a_program_and_its_arguments() {
        assert_eq!(
            parse_endpoint("stdio:/usr/bin/python3 -u server.py").expect("it parses"),
            (
                "/usr/bin/python3".to_owned(),
                vec!["-u".to_owned(), "server.py".to_owned()]
            )
        );
        assert_eq!(
            parse_endpoint("stdio:///opt/mcp/server").expect("it parses"),
            ("/opt/mcp/server".to_owned(), Vec::new())
        );
    }

    /// No shell means no shell metacharacter is interpreted: they are just
    /// arguments, which `execve` will hand to the program verbatim.
    #[test]
    fn endpoint_words_are_arguments_rather_than_a_shell_line() {
        let (program, args) = parse_endpoint("stdio:/bin/server ; rm -rf /").expect("it parses");
        assert_eq!(program, "/bin/server");
        assert_eq!(args, vec![";", "rm", "-rf", "/"]);
    }

    #[test]
    fn a_non_stdio_endpoint_is_refused() {
        assert!(parse_endpoint("https://example.invalid/mcp").is_err());
        assert!(parse_endpoint("stdio:").is_err());
        assert!(parse_endpoint("stdio:   ").is_err());
    }
}
