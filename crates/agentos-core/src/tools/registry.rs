use super::McpTool;
use crate::jobs::{JobId, JobRegistry, JobSnapshot, JobSpec, JobState};
use crate::memory::conversation_id_from_context;
use crate::sandbox::Sandbox;
use crate::tools::builtin::workspace_root;
use crate::tools::exec::{Exec, DEFAULT_MAX_OUTPUT_BYTES};
use agentos_interfaces::mcp::{McpClient, McpError, McpServer};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::ConversationId;
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tracing::warn;

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
    jobs: Option<Arc<JobRegistry>>,
    promotable: BTreeSet<Arc<str>>,
    /// Bytes captured from an isolated tool's subprocess before the rest is
    /// read and discarded (roadmap item X3, `[limits].tool_output_bytes`).
    max_output_bytes: usize,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self {
            tools: BTreeMap::new(),
            isolation_runner: None,
            default_timeout: Duration::from_millis(DEFAULT_TOOL_TIMEOUT_MS),
            timeout_overrides: BTreeMap::new(),
            jobs: None,
            promotable: BTreeSet::new(),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
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

    /// Set how much output an isolated tool's subprocess may produce before
    /// the rest is read and discarded (roadmap item X3).
    pub fn with_output_limit(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }

    /// Let the listed tools become background jobs instead of failing when
    /// they exceed their deadline (roadmap item D3).
    pub fn with_jobs(
        mut self,
        jobs: Arc<JobRegistry>,
        promotable: impl IntoIterator<Item = Arc<str>>,
    ) -> Self {
        self.jobs = Some(jobs);
        self.promotable = promotable.into_iter().collect();
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
        if spec.sandbox.is_sandboxed() {
            if let Some(runner) = &self.isolation_runner {
                let sandbox = Sandbox::new(spec.sandbox, workspace_root());
                return Ok(call_isolated_subprocess(
                    runner,
                    call,
                    deadline,
                    &sandbox,
                    self.max_output_bytes,
                )
                .await?);
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
        if spec.sandbox.is_sandboxed() {
            if let Some(runner) = &self.isolation_runner {
                let sandbox = Sandbox::new(spec.sandbox, workspace_root());
                return Ok(call_isolated_subprocess(
                    runner,
                    call,
                    deadline,
                    &sandbox,
                    self.max_output_bytes,
                )
                .await?);
            }
        }
        if let Some(promoted) = self.call_promotable(tool, call, ctx, deadline).await {
            return promoted;
        }
        match timeout(deadline, tool.call_with_context(call, &call.args, ctx)).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Ok(timed_out_result(call, deadline)),
        }
    }

    /// Run a promotable tool as a background job, waiting out its deadline
    /// inline.
    ///
    /// The job is started *first* and awaited for the deadline, rather than
    /// racing a plain call and promoting on expiry. Racing cannot work: the
    /// loser of a `timeout` is dropped, so there would be nothing left to
    /// promote — the work would have to be restarted, which for a tool with
    /// side effects means doing it twice. Starting as a job makes the deadline
    /// a question of *how long to wait inline*, and a call that finishes in
    /// time is indistinguishable from one that never involved a job.
    ///
    /// `None` when this call is not a promotion candidate, so the caller falls
    /// through to the ordinary path.
    async fn call_promotable(
        &self,
        tool: &Arc<dyn Tool>,
        call: &ToolCall,
        ctx: &RunContext<'_>,
        deadline: Duration,
    ) -> Option<Result<ToolResult, ToolRegistryError>> {
        let jobs = self.jobs.as_ref()?;
        if !self.promotable.contains(&call.name) {
            return None;
        }
        // A job belongs to a conversation, and there is no safe default owner:
        // guessing one would put a job somewhere another conversation might
        // reach it. Without an owner, this call is simply not promotable.
        let conversation_id = conversation_id_from_context(ctx)?;

        // Deliberately `Tool::call`, not `call_with_context`: the job outlives
        // the borrow the context is built from. `[jobs].promotable` is an
        // operator allowlist precisely so this substitution is a decision
        // rather than a surprise.
        let owned_tool = Arc::clone(tool);
        let owned_call = call.clone();
        let spec = JobSpec {
            kind: Arc::from("tool"),
            label: Arc::clone(&call.name),
            conversation_id: conversation_id.clone(),
            output_limit_bytes: None,
        };
        let id = match jobs.start(spec, move |sink, _cancel| async move {
            match owned_tool.call(&owned_call, &owned_call.args).await {
                Ok(result) => {
                    sink.append(&result.content);
                    Ok(result.content)
                }
                Err(error) => Err(Arc::from(error.to_string())),
            }
        }) {
            Ok(id) => id,
            // Out of job slots: fall through and run it inline, where the
            // deadline still bounds it. A busy conversation degrades to D2's
            // behaviour rather than refusing to run the tool at all.
            Err(error) => {
                warn!(
                    tool = call.name.as_ref(),
                    error = %error,
                    "tool not promoted to a job; running inline under its deadline"
                );
                return None;
            }
        };

        match jobs.wait_for(&conversation_id, &id, deadline).await {
            Ok(Some(snapshot)) => Some(Ok(finished_job_result(
                call,
                jobs,
                &conversation_id,
                &snapshot,
            ))),
            Ok(None) => Some(Ok(promoted_result(call, &id, deadline))),
            // The job vanished mid-wait, which only disposal does.
            Err(error) => Some(Ok(ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Failed,
                content: Arc::from(error.to_string()),
                metadata: BTreeMap::new(),
            })),
        }
    }
}

/// A promoted job's result, as the model sees it: not a failure, an invitation
/// to come back for it.
fn promoted_result(call: &ToolCall, id: &JobId, deadline: Duration) -> ToolResult {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("job_id"), Value::String(id.to_string()));
    metadata.insert(Arc::from("promoted"), Value::Bool(true));
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Succeeded,
        content: Arc::from(format!(
            "The `{}` tool is still running after {} ms, so it is now background job `{id}`. \
             Use `job_status` to check on it, `job_output` to read what it has produced, and \
             `job_kill` to stop it. Carry on with something else in the meantime.",
            call.name,
            deadline.as_millis()
        )),
        metadata,
    }
}

/// A job that finished inside its deadline, unwrapped back into an ordinary
/// tool result. The model is never told a job existed.
fn finished_job_result(
    call: &ToolCall,
    jobs: &JobRegistry,
    conversation_id: &ConversationId,
    snapshot: &JobSnapshot,
) -> ToolResult {
    let content = jobs
        .output(conversation_id, &snapshot.id, 0)
        .unwrap_or_default();
    let mut metadata = BTreeMap::new();
    if snapshot.output_truncated {
        metadata.insert(Arc::from("output_truncated"), Value::Bool(true));
    }
    match snapshot.state {
        JobState::Succeeded => ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(content),
            metadata,
        },
        _ => ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Failed,
            content: snapshot
                .detail
                .clone()
                .unwrap_or_else(|| Arc::from("the tool did not finish")),
            metadata,
        },
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
    sandbox: &Sandbox,
    max_output_bytes: usize,
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
        max_output_bytes,
        sandbox,
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
    // What the worker actually ran under, so a trace shows the enforcement
    // rather than the intent.
    result.metadata.insert(
        Arc::from("sandbox"),
        Value::String(sandbox.mode().as_str().to_owned()),
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
    use agentos_interfaces::tool::SandboxMode;
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
                sandbox: SandboxMode::FullAccess,
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
