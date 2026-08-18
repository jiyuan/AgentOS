use super::isolation::{
    IsolationError, IsolationExecutor, IsolationExecutorCapabilities, IsolationProtocol,
};
use super::McpTool;
use crate::jobs::{JobId, JobRegistry, JobSnapshot, JobSpec, JobState};
use crate::memory::conversation_id_from_context;
use crate::tools::exec::DEFAULT_MAX_OUTPUT_BYTES;
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
    #[error(transparent)]
    Isolation(#[from] IsolationError),
}

/// Deadline applied to a tool that declares none and that no deployment has
/// configured. Generous enough for an ordinary HTTP call or file read, short
/// enough that a wedged tool does not hold a conversation for minutes.
/// `[limits].tool_timeout_ms` overrides it.
pub const DEFAULT_TOOL_TIMEOUT_MS: u64 = 60_000;

pub struct ToolRegistry {
    tools: BTreeMap<Arc<str>, Arc<dyn Tool>>,
    isolation_protocols: BTreeMap<Arc<str>, IsolationProtocol>,
    isolation_executor: Option<IsolationExecutor>,
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
            isolation_protocols: BTreeMap::new(),
            isolation_executor: None,
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
        self.register_with_isolation_protocol(tool, IsolationProtocol::InProcess);
    }

    pub(crate) fn register_with_isolation_protocol<T>(
        &mut self,
        tool: T,
        protocol: IsolationProtocol,
    ) where
        T: Tool + 'static,
    {
        let spec = tool.spec();
        self.isolation_protocols
            .insert(Arc::clone(&spec.name), protocol);
        self.tools.insert(spec.name, Arc::new(tool));
    }

    pub fn with_subprocess_isolation(mut self, runner: impl Into<PathBuf>) -> Self {
        self.isolation_executor = Some(IsolationExecutor::bundled_worker(runner.into()));
        self
    }

    pub fn isolation_capabilities(&self) -> Option<&IsolationExecutorCapabilities> {
        self.isolation_executor
            .as_ref()
            .map(IsolationExecutor::capabilities)
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
            self.register_with_isolation_protocol(
                McpTool::new(server.clone(), Arc::clone(&client), spec),
                IsolationProtocol::McpClient,
            );
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
            return self.call_isolated(&spec, call, deadline).await;
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
            return self.call_isolated(&spec, call, deadline).await;
        }
        if let Some(promoted) = self.call_promotable(tool, call, ctx, deadline).await {
            return promoted;
        }
        match timeout(deadline, tool.call_with_context(call, &call.args, ctx)).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Ok(timed_out_result(call, deadline)),
        }
    }

    async fn call_isolated(
        &self,
        spec: &ToolSpec,
        call: &ToolCall,
        deadline: Duration,
    ) -> Result<ToolResult, ToolRegistryError> {
        let executor = self.isolation_executor.as_ref().ok_or_else(|| {
            IsolationError::ExecutorUnavailable {
                tool: Arc::clone(&call.name),
                mode: spec.sandbox.as_str(),
            }
        })?;
        let protocol = self
            .isolation_protocols
            .get(&call.name)
            .copied()
            .expect("every registered tool has an isolation protocol");
        Ok(executor
            .call(
                call,
                spec.sandbox,
                protocol,
                deadline,
                self.max_output_bytes,
            )
            .await?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::Availability;
    use agentos_interfaces::tool::SandboxMode;
    use agentos_interfaces::tool::Tool;
    use agentos_proto::ToolCallId;
    use async_trait::async_trait;
    use serde_json::{json, value::RawValue};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A tool that never returns, so only a deadline can end a call to it.
    struct NeverReturns {
        declared_timeout_ms: Option<u64>,
    }

    struct SandboxedRecorder {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Tool for SandboxedRecorder {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: Arc::from("sandboxed_recorder"),
                description: Arc::from("Records whether its body ran."),
                input_schema: json!({"type": "object"}),
                sandbox: SandboxMode::ReadOnly,
                timeout_ms: None,
            }
        }

        async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Succeeded,
                content: Arc::from("body ran"),
                metadata: BTreeMap::new(),
            })
        }
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
    async fn a_sandboxed_tool_body_never_runs_without_an_executor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(SandboxedRecorder {
            calls: Arc::clone(&calls),
        });
        let call = ToolCall {
            id: ToolCallId::new("call-sandboxed"),
            name: Arc::from("sandboxed_recorder"),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        };

        let error = registry
            .call(&call)
            .await
            .expect_err("a missing executor must refuse the call");
        assert!(matches!(
            error,
            ToolRegistryError::Isolation(IsolationError::ExecutorUnavailable { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_incompatible_executor_refuses_before_the_tool_body_runs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(SandboxedRecorder {
            calls: Arc::clone(&calls),
        });
        registry.isolation_executor = Some(IsolationExecutor::new(
            PathBuf::from("unused-test-runner"),
            IsolationExecutorCapabilities {
                name: Arc::from("shell-only-test-executor"),
                backend: Availability::Enforced("test-backend"),
                modes: BTreeSet::from([SandboxMode::ReadOnly]),
                protocols: BTreeSet::from([IsolationProtocol::BuiltinShellV1]),
            },
        ));
        let call = ToolCall {
            id: ToolCallId::new("call-incompatible"),
            name: Arc::from("sandboxed_recorder"),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        };

        let error = registry
            .call(&call)
            .await
            .expect_err("the executor does not support in-process tools");
        assert!(matches!(
            error,
            ToolRegistryError::Isolation(IsolationError::UnsupportedProtocol {
                protocol: "in_process",
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_unavailable_backend_is_a_typed_refusal() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register_with_isolation_protocol(
            SandboxedRecorder {
                calls: Arc::clone(&calls),
            },
            IsolationProtocol::BuiltinShellV1,
        );
        registry.isolation_executor = Some(IsolationExecutor::new(
            PathBuf::from("unused-test-runner"),
            IsolationExecutorCapabilities {
                name: Arc::from("unavailable-test-executor"),
                backend: Availability::Unavailable("test backend refused its probe"),
                modes: BTreeSet::from([SandboxMode::ReadOnly]),
                protocols: BTreeSet::from([IsolationProtocol::BuiltinShellV1]),
            },
        ));
        let call = ToolCall {
            id: ToolCallId::new("call-unavailable"),
            name: Arc::from("sandboxed_recorder"),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        };

        let error = registry
            .call(&call)
            .await
            .expect_err("an unavailable backend must refuse the call");
        assert!(matches!(
            error,
            ToolRegistryError::Isolation(IsolationError::BackendUnavailable { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
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
