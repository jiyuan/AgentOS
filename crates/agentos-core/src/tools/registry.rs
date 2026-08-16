use super::McpTool;
use crate::tools::exec::{Exec, DEFAULT_MAX_OUTPUT_BYTES};
use agentos_interfaces::mcp::{McpClient, McpError, McpServer};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;

#[derive(Debug, Error)]
pub enum ToolRegistryError {
    #[error("unknown tool: {0}")]
    UnknownTool(Arc<str>),
    #[error(transparent)]
    Tool(#[from] ToolError),
    #[error(transparent)]
    Mcp(#[from] McpError),
}

/// Deadline applied to a tool that declares none and that no deployment has
/// configured. Generous enough for an ordinary HTTP call or file read, short
/// enough that a wedged tool does not hold a conversation for minutes.
/// `[limits].tool_timeout_ms` overrides it.
pub const DEFAULT_TOOL_TIMEOUT_MS: u64 = 60_000;

pub struct ToolRegistry {
    tools: BTreeMap<Arc<str>, Arc<dyn Tool>>,
    isolation_runner: Option<PathBuf>,
    default_timeout: Duration,
    timeout_overrides: BTreeMap<Arc<str>, Duration>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self {
            tools: BTreeMap::new(),
            isolation_runner: None,
            default_timeout: Duration::from_millis(DEFAULT_TOOL_TIMEOUT_MS),
            timeout_overrides: BTreeMap::new(),
        }
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the deployment's default deadline and any per-tool overrides
    /// (roadmap item D2).
    pub fn with_timeouts(
        mut self,
        default_timeout: Duration,
        overrides: BTreeMap<Arc<str>, Duration>,
    ) -> Self {
        self.default_timeout = default_timeout;
        self.timeout_overrides = overrides;
        self
    }

    /// How long `spec`'s tool may run.
    ///
    /// Most specific wins: a deployment's per-tool override, then the tool's
    /// own declaration, then the deployment default. A tool that knows it is
    /// slow says so in its spec; an operator who disagrees says so in config.
    fn deadline(&self, spec: &ToolSpec) -> Duration {
        self.timeout_overrides
            .get(&spec.name)
            .copied()
            .or_else(|| spec.timeout_ms.map(Duration::from_millis))
            .unwrap_or(self.default_timeout)
    }

    pub fn register<T>(&mut self, tool: T)
    where
        T: Tool + 'static,
    {
        let spec = tool.spec();
        self.tools.insert(spec.name, Arc::new(tool));
    }

    pub fn with_subprocess_isolation(mut self, runner: impl Into<PathBuf>) -> Self {
        self.isolation_runner = Some(runner.into());
        self
    }

    pub async fn register_mcp_server(
        &mut self,
        server: McpServer,
        client: Arc<dyn McpClient>,
    ) -> Result<Vec<ToolSpec>, ToolRegistryError> {
        self.register_mcp_server_filtered(server, client, |_| true)
            .await
    }

    pub async fn register_mcp_server_filtered(
        &mut self,
        server: McpServer,
        client: Arc<dyn McpClient>,
        enabled: impl Fn(&ToolSpec) -> bool,
    ) -> Result<Vec<ToolSpec>, ToolRegistryError> {
        let specs = client.list_tools(&server).await?;
        let specs = specs
            .into_iter()
            .filter(|spec| enabled(spec))
            .collect::<Vec<_>>();
        for spec in specs.iter().cloned() {
            self.register(McpTool::new(server.clone(), Arc::clone(&client), spec));
        }
        Ok(specs)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tools.values().map(|tool| tool.spec()).collect()
    }

    pub async fn call(&self, call: &ToolCall) -> Result<ToolResult, ToolRegistryError> {
        let tool = self
            .tools
            .get(&call.name)
            .ok_or_else(|| ToolRegistryError::UnknownTool(Arc::clone(&call.name)))?;
        let spec = tool.spec();
        let deadline = self.deadline(&spec);
        if spec.requires_isolation {
            if let Some(runner) = &self.isolation_runner {
                return Ok(call_isolated_subprocess(runner, call, deadline).await?);
            }
        }
        match timeout(deadline, tool.call(call, &call.args)).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Ok(timed_out_result(call, deadline)),
        }
    }

    pub async fn call_with_context(
        &self,
        call: &ToolCall,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolRegistryError> {
        let tool = self
            .tools
            .get(&call.name)
            .ok_or_else(|| ToolRegistryError::UnknownTool(Arc::clone(&call.name)))?;
        let spec = tool.spec();
        let deadline = self.deadline(&spec);
        if spec.requires_isolation {
            if let Some(runner) = &self.isolation_runner {
                return Ok(call_isolated_subprocess(runner, call, deadline).await?);
            }
        }
        match timeout(deadline, tool.call_with_context(call, &call.args, ctx)).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Ok(timed_out_result(call, deadline)),
        }
    }
}

/// A tool that overran its deadline, as a result the model can read.
///
/// Never a `ToolRegistryError`: the run loop already recovers from a failed
/// `ToolResult` by letting the model see it and replan, whereas an error would
/// abort the run and every parent above it — so one slow tool would take down
/// the whole delegation chain and, today, the gateway with it.
fn timed_out_result(call: &ToolCall, deadline: Duration) -> ToolResult {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("timed_out"), Value::Bool(true));
    metadata.insert(
        Arc::from("timeout_ms"),
        Value::from(deadline.as_millis() as u64),
    );
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Failed,
        content: Arc::from(format!(
            "The `{}` tool did not finish within its {} ms deadline and was stopped. \
             Try a narrower request, or a different approach.",
            call.name,
            deadline.as_millis()
        )),
        metadata,
    }
}

#[derive(serde::Serialize)]
struct IsolatedToolRequest<'a> {
    call: &'a ToolCall,
}

/// Run one tool call inside the isolation worker.
///
/// Async since roadmap D2: this used to block a Tokio worker thread on
/// `std::process` for as long as the child chose to live, which on a
/// `current_thread` runtime meant every other conversation on that thread
/// stopped with it.
pub async fn call_isolated_subprocess(
    runner: &std::path::Path,
    call: &ToolCall,
    timeout: Duration,
) -> Result<ToolResult, ToolError> {
    let request = serde_json::to_vec(&IsolatedToolRequest { call })
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;
    let program = runner.to_string_lossy().into_owned();
    let output = crate::tools::exec::run(Exec {
        program: &program,
        args: &[],
        cwd: None,
        stdin: Some(&request),
        timeout,
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
    })
    .await
    .map_err(|err| ToolError::Failed(err.to_string().into()))?;

    if !output.success {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(ToolError::Failed(Arc::from(if message.is_empty() {
            "isolated worker failed".to_owned()
        } else {
            message.to_owned()
        })));
    }

    let mut result: ToolResult = serde_json::from_slice(&output.stdout)
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;
    result.metadata.insert(
        Arc::from("isolation"),
        Value::String("subprocess".to_owned()),
    );
    result.metadata.insert(
        Arc::from("isolation_runner"),
        Value::String(runner.display().to_string()),
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::tool::Tool;
    use agentos_proto::ToolCallId;
    use async_trait::async_trait;
    use serde_json::{json, value::RawValue};

    /// A tool that never returns, so only a deadline can end a call to it.
    struct NeverReturns {
        declared_timeout_ms: Option<u64>,
    }

    #[async_trait]
    impl Tool for NeverReturns {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: Arc::from("hang"),
                description: Arc::from("Never returns."),
                input_schema: json!({"type": "object"}),
                requires_isolation: false,
                timeout_ms: self.declared_timeout_ms,
            }
        }

        async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
            std::future::pending().await
        }
    }

    fn hang_call() -> ToolCall {
        ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from("hang"),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        }
    }

    #[tokio::test]
    async fn a_tool_past_its_deadline_returns_a_failed_result_not_an_error() {
        // The D2 rule: the loop already recovers from a failed `ToolResult`,
        // whereas an error would abort the run and every parent above it.
        let mut registry = ToolRegistry::new();
        registry.register(NeverReturns {
            declared_timeout_ms: None,
        });
        let registry = registry.with_timeouts(Duration::from_millis(100), BTreeMap::new());

        let result = registry
            .call(&hang_call())
            .await
            .expect("a deadline is not a registry error");

        assert_eq!(result.status, ToolStatus::Failed);
        assert!(result.content.contains("did not finish"));
        assert_eq!(result.metadata.get("timed_out"), Some(&Value::Bool(true)));
        assert_eq!(
            result.metadata.get("timeout_ms"),
            Some(&Value::from(100u64))
        );
    }

    #[tokio::test]
    async fn a_tool_declares_its_own_deadline_when_it_knows_it_is_slow() {
        // A tool that needs longer than the deployment default says so in its
        // spec, and the registry honours it rather than cutting it off.
        let mut registry = ToolRegistry::new();
        registry.register(NeverReturns {
            declared_timeout_ms: Some(50_000),
        });
        let registry = registry.with_timeouts(Duration::from_millis(80), BTreeMap::new());
        let spec = registry.specs().pop().expect("one tool");

        assert_eq!(registry.deadline(&spec), Duration::from_millis(50_000));
    }

    #[tokio::test]
    async fn an_operator_override_beats_both_the_default_and_the_declaration() {
        // The last word belongs to whoever owns the machine.
        let mut registry = ToolRegistry::new();
        registry.register(NeverReturns {
            declared_timeout_ms: Some(50_000),
        });
        let registry = registry.with_timeouts(
            Duration::from_millis(80),
            BTreeMap::from([(Arc::from("hang"), Duration::from_millis(120))]),
        );
        let spec = registry.specs().pop().expect("one tool");
        assert_eq!(registry.deadline(&spec), Duration::from_millis(120));

        let started = std::time::Instant::now();
        let result = registry.call(&hang_call()).await.expect("bounded");
        assert_eq!(result.status, ToolStatus::Failed);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_registry_with_no_configuration_still_bounds_every_tool() {
        // The invariant behind the exit condition: there is no path to an
        // unbounded tool call, configured or not.
        let mut registry = ToolRegistry::new();
        registry.register(NeverReturns {
            declared_timeout_ms: None,
        });
        let spec = registry.specs().pop().expect("one tool");
        assert_eq!(
            registry.deadline(&spec),
            Duration::from_millis(DEFAULT_TOOL_TIMEOUT_MS)
        );
    }
}
