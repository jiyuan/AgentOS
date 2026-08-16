use agentos_interfaces::mcp::{McpClient, McpError, McpServer};
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::time::Duration;

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

pub struct StdioMcpClient {
    timeout: Duration,
    servers: Mutex<BTreeMap<Arc<str>, ManagedStdioServer>>,
}

#[derive(serde::Deserialize)]
struct StdioMcpResponse<T> {
    result: Option<T>,
    error: Option<StdioMcpResponseError>,
}

#[derive(serde::Deserialize)]
struct StdioMcpResponseError {
    message: String,
}

#[derive(serde::Deserialize)]
struct StdioListToolsResult {
    tools: Vec<ToolSpec>,
}

#[derive(Clone)]
struct ManagedStdioServer {
    sender: std_mpsc::Sender<StdioMcpRequest>,
    child: Arc<Mutex<Child>>,
}

struct StdioMcpRequest {
    payload: Vec<u8>,
    response: std_mpsc::Sender<Result<Vec<u8>, String>>,
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

impl Default for StdioMcpClient {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
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
            servers: Mutex::new(BTreeMap::new()),
        }
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

#[async_trait]
impl McpClient for StdioMcpClient {
    async fn list_tools(&self, server: &McpServer) -> Result<Vec<ToolSpec>, McpError> {
        let result: StdioListToolsResult = self
            .call_stdio_mcp(
                server,
                json!({
                    "jsonrpc": "2.0",
                    "id": "agentos-list-tools",
                    "method": "tools/list",
                    "params": {
                        "server_id": server.id,
                    },
                }),
            )
            .await?;
        Ok(result.tools)
    }

    async fn call_tool(&self, server: &McpServer, call: &ToolCall) -> Result<ToolResult, McpError> {
        let call_value = serde_json::to_value(call)
            .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
        let mut result: ToolResult = self
            .call_stdio_mcp(
                server,
                json!({
                    "jsonrpc": "2.0",
                    "id": format!("agentos-call-{}", call.id.as_str()),
                    "method": "tools/call",
                    "params": {
                        "server_id": server.id,
                        "call": call_value,
                    },
                }),
            )
            .await?;
        result
            .metadata
            .insert(Arc::from("stdio_mcp"), Value::String("true".to_owned()));
        result.metadata.insert(
            Arc::from("stdio_mcp_lifecycle"),
            Value::String("persistent".to_owned()),
        );
        Ok(result)
    }
}

impl StdioMcpClient {
    /// Send one JSON-RPC request to the managed stdio worker and wait for its
    /// reply.
    ///
    /// Async since roadmap D2. The wait is a blocking `recv_timeout` on a
    /// std channel — the worker owns its child on a dedicated thread, so there
    /// is no async receiver to await — and running it inline held a Tokio
    /// worker thread for up to the full deadline. `spawn_blocking` moves it
    /// onto the blocking pool where a long wait costs nothing but a thread
    /// that exists for exactly this.
    ///
    /// `managed_server` still spawns the child inline on first use. That is a
    /// bounded, one-off cost rather than a per-call one, so it is left alone.
    async fn call_stdio_mcp<T>(&self, server: &McpServer, request: Value) -> Result<T, McpError>
    where
        T: DeserializeOwned,
    {
        let request = serde_json::to_vec(&request)
            .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
        let managed = self.managed_server(server)?;
        let (response_tx, response_rx) = std_mpsc::channel();
        managed
            .sender
            .send(StdioMcpRequest {
                payload: request,
                response: response_tx,
            })
            .map_err(|_| {
                self.remove_managed_server(&server.endpoint);
                McpError::Failed(Arc::from("stdio MCP worker request channel closed"))
            })?;

        let timeout = self.timeout;
        let received = tokio::task::spawn_blocking(move || response_rx.recv_timeout(timeout))
            .await
            .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
        let response = match received {
            Ok(Ok(response)) => response,
            Ok(Err(message)) => {
                self.remove_managed_server(&server.endpoint);
                return Err(McpError::Failed(Arc::from(message)));
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                kill_managed_stdio_worker(&managed);
                self.remove_managed_server(&server.endpoint);
                return Err(McpError::Failed(Arc::from(format!(
                    "stdio MCP worker timed out after {} ms",
                    timeout.as_millis()
                ))));
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                self.remove_managed_server(&server.endpoint);
                return Err(McpError::Failed(Arc::from(
                    "stdio MCP worker response channel closed",
                )));
            }
        };

        let response: StdioMcpResponse<T> = serde_json::from_slice(&response)
            .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
        if let Some(error) = response.error {
            return Err(McpError::Failed(Arc::from(error.message)));
        }
        response
            .result
            .ok_or_else(|| McpError::Failed(Arc::from("stdio MCP response missing result")))
    }

    fn managed_server(&self, server: &McpServer) -> Result<ManagedStdioServer, McpError> {
        let mut servers = self
            .servers
            .lock()
            .map_err(|_| McpError::Failed(Arc::from("stdio MCP server registry lock poisoned")))?;
        if let Some(managed) = servers.get(&server.endpoint) {
            return Ok(managed.clone());
        }
        let managed = spawn_managed_stdio_server(&server.endpoint)?;
        servers.insert(Arc::clone(&server.endpoint), managed.clone());
        Ok(managed)
    }

    fn remove_managed_server(&self, endpoint: &str) {
        if let Ok(mut servers) = self.servers.lock() {
            servers.remove(endpoint);
        }
    }
}

/// Start one persistent stdio MCP worker.
///
/// This is the one `std::process` call D2 deliberately left alone. It only
/// *spawns* — the blocking request/response loop runs on the dedicated thread
/// below, never on a Tokio worker — and a `spawn` is a one-off syscall that
/// returns as soon as the child is forked, so it cannot occupy a thread. The
/// alternative, rebuilding the whole persistent worker on `tokio::process`, is
/// a change with its own risk and no benefit to the exit condition.
fn spawn_managed_stdio_server(endpoint: &str) -> Result<ManagedStdioServer, McpError> {
    let program = stdio_program(endpoint)?;
    let mut child = Command::new(program)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| McpError::Failed(Arc::from(err.to_string())))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| McpError::Failed(Arc::from("stdio MCP worker stdin unavailable")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| McpError::Failed(Arc::from("stdio MCP worker stdout unavailable")))?;
    let (sender, receiver) = std_mpsc::channel();
    let child = Arc::new(Mutex::new(child));
    std::thread::spawn({
        let child = Arc::clone(&child);
        move || run_managed_stdio_server(stdin, stdout, receiver, child)
    });
    Ok(ManagedStdioServer { sender, child })
}

fn run_managed_stdio_server(
    mut stdin: std::process::ChildStdin,
    stdout: std::process::ChildStdout,
    receiver: std_mpsc::Receiver<StdioMcpRequest>,
    child: Arc<Mutex<Child>>,
) {
    let mut stdout = BufReader::new(stdout);
    for request in receiver {
        let response = write_and_read_stdio_response(&mut stdin, &mut stdout, &request.payload);
        let is_terminal = response.is_err();
        let _ = request.response.send(response);
        if is_terminal {
            break;
        }
    }
    kill_managed_stdio_worker(&ManagedStdioServer {
        sender: std_mpsc::channel().0,
        child,
    });
}

fn write_and_read_stdio_response(
    stdin: &mut std::process::ChildStdin,
    stdout: &mut BufReader<std::process::ChildStdout>,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    stdin.write_all(payload).map_err(|err| err.to_string())?;
    stdin.write_all(b"\n").map_err(|err| err.to_string())?;
    stdin.flush().map_err(|err| err.to_string())?;
    let mut line = String::new();
    let bytes_read = stdout.read_line(&mut line).map_err(|err| err.to_string())?;
    if bytes_read == 0 {
        return Err("stdio MCP worker closed stdout".to_owned());
    }
    Ok(line.into_bytes())
}

fn kill_managed_stdio_worker(managed: &ManagedStdioServer) {
    if let Ok(mut child) = managed.child.lock() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn stdio_program(endpoint: &str) -> Result<PathBuf, McpError> {
    let raw_path = endpoint
        .strip_prefix("stdio://")
        .or_else(|| endpoint.strip_prefix("stdio:"))
        .ok_or_else(|| {
            McpError::Failed(Arc::from(format!(
                "unsupported MCP stdio endpoint: {endpoint}"
            )))
        })?;
    if raw_path.is_empty() {
        return Err(McpError::Failed(Arc::from(
            "stdio MCP endpoint path is empty",
        )));
    }
    Ok(Path::new(raw_path).to_path_buf())
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
