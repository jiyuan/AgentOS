use super::McpTool;
use crate::jobs::{JobId, JobRegistry, JobSnapshot, JobSpec, JobState};
use crate::memory::conversation_id_from_context;
use crate::sandbox::Sandbox;
use crate::tools::builtin::workspace_root;
use crate::tools::exec::{Exec, DEFAULT_MAX_OUTPUT_BYTES};
use crate::tools::isolation::{self, ExecutorCapabilities};
use agentos_interfaces::mcp::{McpClient, McpError, McpServer};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{Isolation, Tool, ToolError, ToolSpec};
use agentos_proto::ConversationId;
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::OnceCell;
use tokio::time::timeout;
use tracing::warn;

#[derive(Debug, Error)]
pub enum ToolRegistryError {
    #[error("unknown tool: {0}")]
    UnknownTool(Arc<str>),
    /// The tool declared a sandbox mode and this deployment configured no
    /// isolated executor to enforce it (M4 / `SBX-001`, ADR-0002).
    ///
    /// Never a failed `ToolResult`: the model cannot replan its way around a
    /// missing binary, and letting it try would mean asking again with the
    /// same containment gap. `loop/tool_call.rs` bubbles this up as a run
    /// error for that reason.
    #[error(
        "the `{tool}` tool declares sandbox mode `{mode}`, but no isolated executor is \
         configured to enforce it; set `[isolation].worker_path` or \
         AGENTOS_TOOL_WORKER_PATH, or declare `full_access`"
    )]
    NoIsolatedExecutor { tool: Arc<str>, mode: &'static str },
    /// An isolated executor is configured but cannot take this call — wrong
    /// handshake, no implementation for the tool, or no enforceable sandbox on
    /// its machine. Distinct from [`Self::NoIsolatedExecutor`] because the fix
    /// is different: this deployment has an executor and picked the wrong one.
    #[error("the `{tool}` tool needs sandbox mode `{mode}`, but the isolated executor {reason}")]
    IncompatibleExecutor {
        tool: Arc<str>,
        mode: &'static str,
        reason: String,
    },
    /// The executor was compatible and the isolated run itself failed — the
    /// worker could not be started, refused the request, or died before
    /// answering. The tool's own errors do not come through here; they arrive
    /// as [`Self::Tool`] and stay recoverable.
    #[error("the `{tool}` tool could not be run under isolation: {reason}")]
    SandboxExecutionFailed { tool: Arc<str>, reason: Arc<str> },
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

/// Where a call runs, once the sandbox question has been settled.
///
/// A two-variant enum rather than an `Option<&Path>` so the in-process arm is
/// something the code says out loud. The bug this replaces was an absent
/// branch; a named variant is harder to omit again.
enum Dispatch<'a> {
    InProcess,
    Isolated { runner: &'a Path },
}

pub struct ToolRegistry {
    tools: BTreeMap<Arc<str>, Arc<dyn Tool>>,
    isolation_runner: Option<PathBuf>,
    /// What [`Self::isolation_runner`] said it can do, asked at most once.
    ///
    /// Lazy rather than probed at construction: the handshake spawns a
    /// process, and a deployment whose tools are all `full_access` should
    /// never pay for one. `OnceCell` rather than a lock because the answer
    /// cannot change for the life of the registry — the runner path is fixed
    /// at construction.
    executor: OnceCell<Result<ExecutorCapabilities, Arc<str>>>,
    default_timeout: Duration,
    timeout_overrides: BTreeMap<Arc<str>, Duration>,
    jobs: Option<Arc<JobRegistry>>,
    promotable: BTreeSet<Arc<str>>,
    /// Bytes captured from an isolated tool's subprocess before the rest is
    /// read and discarded (roadmap item X3, `[limits].tool_output_bytes`).
    max_output_bytes: usize,
    /// `[isolation].env_passthrough`: names the deployment allows the isolated
    /// executor to read on top of the built-in allowlist (M4 / `PROC-001`).
    env_passthrough: Vec<String>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self {
            tools: BTreeMap::new(),
            isolation_runner: None,
            executor: OnceCell::new(),
            default_timeout: Duration::from_millis(DEFAULT_TOOL_TIMEOUT_MS),
            timeout_overrides: BTreeMap::new(),
            jobs: None,
            promotable: BTreeSet::new(),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            env_passthrough: Vec::new(),
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

    /// Names the isolated executor may read from the deployment's environment,
    /// on top of the built-in allowlist (M4 / `PROC-001`).
    pub fn with_env_passthrough(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.env_passthrough = names.into_iter().collect();
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
        if let Dispatch::Isolated { runner } = self.dispatch(tool, &spec).await? {
            return self.call_isolated(runner, call, &spec, deadline).await;
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
        if let Dispatch::Isolated { runner } = self.dispatch(tool, &spec).await? {
            return self.call_isolated(runner, call, &spec, deadline).await;
        }
        if let Some(promoted) = self.call_promotable(tool, call, ctx, deadline).await {
            return promoted;
        }
        match timeout(deadline, tool.call_with_context(call, &call.args, ctx)).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Ok(timed_out_result(call, deadline)),
        }
    }

    /// Every registered tool whose declared sandbox mode this registry cannot
    /// enforce, as lines an operator can act on.
    ///
    /// The registration half of ADR-0002's "checked at registration where it
    /// can be, at invocation always". Startup is where a containment gap is
    /// cheapest to fix and least likely to be read as a model failure — the
    /// alternative is a run that dies mid-conversation on the first sandboxed
    /// call, which is correct but arrives at the worst possible moment.
    ///
    /// Empty is the answer for the shipped configuration: every built-in but
    /// `shell` declares `full_access`, and `shell` hardens its own child.
    pub async fn isolation_problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for tool in self.tools.values() {
            let spec = tool.spec();
            if let Err(error) = self.dispatch(tool, &spec).await {
                problems.push(error.to_string());
            }
        }
        problems
    }

    /// Where this call runs — decided before the tool's body is reachable.
    ///
    /// The M4 / `SBX-001` rule, from ADR-0002: a declared [`agentos_interfaces::tool::SandboxMode`] other
    /// than `full_access` is a **precondition, not a preference**. This used to
    /// be an `if let Some(runner)` with no `else`, so a deployment with no
    /// isolated executor ran the tool in-process with its declared mode
    /// silently discarded — a tool that asked for `read_only` got everything.
    ///
    /// A [`Isolation::SelfHardened`] tool is the one case that still runs
    /// in-process without an executor, and it is not a fallthrough: that tool
    /// builds the matching [`Sandbox`] itself and hands it to
    /// [`crate::tools::exec::run`], where hardening happens before the child
    /// exists and a failure aborts the call. The mode is enforced either way;
    /// only the process that enforces it differs.
    async fn dispatch<'a>(
        &'a self,
        tool: &Arc<dyn Tool>,
        spec: &ToolSpec,
    ) -> Result<Dispatch<'a>, ToolRegistryError> {
        if !spec.sandbox.is_sandboxed() {
            return Ok(Dispatch::InProcess);
        }
        let self_hardened = matches!(tool.isolation(), Isolation::SelfHardened);
        let Some(runner) = self.isolation_runner.as_deref() else {
            if self_hardened {
                return Ok(Dispatch::InProcess);
            }
            return Err(ToolRegistryError::NoIsolatedExecutor {
                tool: Arc::clone(&spec.name),
                mode: spec.sandbox.as_str(),
            });
        };
        let incompatible = match self.executor_capabilities(runner).await {
            Ok(capabilities) => match capabilities.can_run(&spec.name, spec.sandbox) {
                Ok(()) => return Ok(Dispatch::Isolated { runner }),
                Err(incompatible) => incompatible.describe(),
            },
            Err(reason) => format!("could not be asked what it supports: {reason}"),
        };
        if self_hardened {
            // Not silence: an operator who configured an executor and got a
            // useless one should be told, even though the call is still safe.
            warn!(
                tool = spec.name.as_ref(),
                runner = %runner.display(),
                reason = incompatible.as_str(),
                "running a self-hardening tool in-process instead of the isolated executor"
            );
            return Ok(Dispatch::InProcess);
        }
        Err(ToolRegistryError::IncompatibleExecutor {
            tool: Arc::clone(&spec.name),
            mode: spec.sandbox.as_str(),
            reason: incompatible,
        })
    }

    /// The configured executor's handshake, asked once and reused.
    ///
    /// A failed handshake is cached alongside a successful one. Re-probing per
    /// call would spawn a process for every sandboxed tool call on a
    /// deployment whose executor is simply missing, which is the case most
    /// likely to be making a lot of them.
    async fn executor_capabilities(
        &self,
        runner: &Path,
    ) -> Result<&ExecutorCapabilities, Arc<str>> {
        self.executor
            .get_or_init(|| isolation::probe(runner, &self.env_passthrough))
            .await
            .as_ref()
            .map_err(Arc::clone)
    }

    /// Run one call in the isolated executor, keeping the tool's own failures
    /// recoverable and the executor's own failures typed.
    async fn call_isolated(
        &self,
        runner: &Path,
        call: &ToolCall,
        spec: &ToolSpec,
        deadline: Duration,
    ) -> Result<ToolResult, ToolRegistryError> {
        let sandbox = Sandbox::new(spec.sandbox, workspace_root());
        call_isolated_subprocess(
            runner,
            call,
            deadline,
            &sandbox,
            self.max_output_bytes,
            &self.env_passthrough,
        )
        .await
        .map_err(|error| match error {
            IsolatedCallError::Tool(error) => ToolRegistryError::Tool(error),
            IsolatedCallError::Executor(reason) => ToolRegistryError::SandboxExecutionFailed {
                tool: Arc::clone(&spec.name),
                reason,
            },
        })
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

/// Why an isolated call did not produce a result.
///
/// The split is the point (M4 / `SBX-001`). A tool that fails inside the
/// worker is the model's problem and stays recoverable; a worker that cannot
/// run the tool at all is the deployment's problem and aborts the run. Before
/// this, both arrived as `ToolError::Failed` with a line of stderr, so a
/// containment failure was indistinguishable from a bad file path.
#[derive(Debug, Error)]
pub enum IsolatedCallError {
    /// The worker ran the tool and the tool returned an error.
    #[error(transparent)]
    Tool(#[from] ToolError),
    /// The worker could not run the tool: it would not start, refused the
    /// request, could not apply the sandbox, or answered with something
    /// unreadable.
    #[error("{0}")]
    Executor(Arc<str>),
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
    extra_env: &[String],
) -> Result<ToolResult, IsolatedCallError> {
    let request = serde_json::to_vec(&IsolatedToolRequest { call })
        .map_err(|err| IsolatedCallError::Executor(Arc::from(err.to_string())))?;
    let program = runner.to_string_lossy().into_owned();
    let output = crate::tools::exec::run(Exec {
        program: &program,
        args: &[],
        cwd: None,
        stdin: Some(&request),
        timeout,
        max_output_bytes,
        sandbox,
        extra_env,
    })
    .await
    .map_err(|err| IsolatedCallError::Executor(Arc::from(err.to_string())))?;

    if !output.success {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        let detail = Arc::from(if message.is_empty() {
            "the isolated worker failed without saying why".to_owned()
        } else {
            message.to_owned()
        });
        // The worker's exit code carries which half of the boundary failed:
        // `EXIT_TOOL_FAILED` for the tool's own error, `EXIT_WORKER_FAILED`
        // for anything the worker itself could not do. An unexpected code —
        // including a signal, which leaves no code at all — is the worker's,
        // because a tool error never gets to choose one.
        return Err(match output.status {
            Some(isolation::EXIT_TOOL_FAILED) => ToolError::Failed(detail).into(),
            _ => IsolatedCallError::Executor(detail),
        });
    }

    let mut result: ToolResult = serde_json::from_slice(&output.stdout).map_err(|err| {
        IsolatedCallError::Executor(Arc::from(format!(
            "the isolated worker answered with something unreadable: {err}"
        )))
    })?;
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
    use std::sync::atomic::{AtomicBool, Ordering};

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

    // -----------------------------------------------------------------------
    // M4 / SBX-001 — dispatch decides before the body is reachable.
    // -----------------------------------------------------------------------

    /// A tool that declares a mode, records whether its body ran, and can be
    /// told which enforcement claim to make.
    struct Declares {
        mode: SandboxMode,
        isolation: Isolation,
        ran: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Tool for Declares {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: Arc::from("declares"),
                description: Arc::from("declares a mode"),
                input_schema: json!({"type": "object"}),
                sandbox: self.mode,
                timeout_ms: None,
            }
        }

        fn isolation(&self) -> Isolation {
            self.isolation
        }

        async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
            self.ran.store(true, Ordering::SeqCst);
            Ok(ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Succeeded,
                content: Arc::from("ran"),
                metadata: BTreeMap::new(),
            })
        }
    }

    fn declares_call() -> ToolCall {
        ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from("declares"),
            args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
        }
    }

    fn registry_with(mode: SandboxMode, isolation: Isolation) -> (ToolRegistry, Arc<AtomicBool>) {
        let ran = Arc::new(AtomicBool::new(false));
        let mut registry = ToolRegistry::new();
        registry.register(Declares {
            mode,
            isolation,
            ran: Arc::clone(&ran),
        });
        (registry, ran)
    }

    #[tokio::test]
    async fn a_tool_needing_an_executor_is_refused_when_none_is_configured() {
        let (registry, ran) = registry_with(SandboxMode::ReadOnly, Isolation::RequiresExecutor);

        let error = registry
            .call(&declares_call())
            .await
            .expect_err("no executor means no call");

        assert!(
            matches!(error, ToolRegistryError::NoIsolatedExecutor { mode, .. } if mode == "read_only"),
            "{error:?}"
        );
        assert!(!ran.load(Ordering::SeqCst), "the body must not have run");
    }

    /// The same refusal, stated once at startup instead of once per call.
    #[tokio::test]
    async fn the_same_gap_is_reported_before_anything_is_called() {
        let (registry, _) = registry_with(SandboxMode::WorkspaceWrite, Isolation::RequiresExecutor);
        let problems = registry.isolation_problems().await;
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("workspace_write"), "{problems:?}");
    }

    /// The exception, and the reason it is not a fallthrough: this tool applies
    /// the sandbox to its own child, so the mode is enforced with or without an
    /// executor. `ShellTool` is the shipped instance.
    #[tokio::test]
    async fn a_self_hardening_tool_still_runs_without_an_executor() {
        let (registry, ran) = registry_with(SandboxMode::WorkspaceWrite, Isolation::SelfHardened);

        let result = registry.call(&declares_call()).await.expect("runs");

        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(ran.load(Ordering::SeqCst));
        assert!(registry.isolation_problems().await.is_empty());
    }

    /// `full_access` is not a sandbox, so there is nothing for an executor to
    /// enforce and nothing to refuse.
    #[tokio::test]
    async fn an_unsandboxed_tool_is_unaffected_by_any_of_this() {
        let (registry, ran) = registry_with(SandboxMode::FullAccess, Isolation::RequiresExecutor);

        let result = registry.call(&declares_call()).await.expect("runs");

        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(ran.load(Ordering::SeqCst));
    }

    /// A configured executor that cannot answer the handshake is a different
    /// error from no executor at all, because it wants a different fix.
    #[tokio::test]
    async fn an_executor_that_cannot_be_asked_is_reported_as_incompatible() {
        let (registry, ran) = registry_with(SandboxMode::ReadOnly, Isolation::RequiresExecutor);
        let registry = registry.with_subprocess_isolation("/nonexistent/agentos-tool-worker");

        let error = registry
            .call(&declares_call())
            .await
            .expect_err("an unusable executor is not a usable one");

        assert!(
            matches!(error, ToolRegistryError::IncompatibleExecutor { .. }),
            "{error:?}"
        );
        assert!(!ran.load(Ordering::SeqCst));
    }

    /// Whereas a self-hardening tool degrades to its own enforcement rather
    /// than failing, and says so in the log.
    #[tokio::test]
    async fn a_self_hardening_tool_falls_back_to_its_own_enforcement() {
        let (registry, ran) = registry_with(SandboxMode::WorkspaceWrite, Isolation::SelfHardened);
        let registry = registry.with_subprocess_isolation("/nonexistent/agentos-tool-worker");

        let result = registry.call(&declares_call()).await.expect("runs");

        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(ran.load(Ordering::SeqCst));
    }
}
