use crate::sandbox::{availability, Availability, Sandbox};
use crate::tools::builtin::workspace_root;
use crate::tools::exec::{Exec, ExecError};
use agentos_interfaces::tool::SandboxMode;
use agentos_proto::{ToolCall, ToolResult};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// How a registered tool can cross the process boundary into an isolation
/// executor. Compatibility is explicit: the bundled worker implements only
/// `builtin_shell_v1`; an in-process or MCP client call cannot be made safe by
/// sending its JSON to that worker.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum IsolationProtocol {
    InProcess,
    BuiltinShellV1,
    McpClient,
}

impl IsolationProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::BuiltinShellV1 => "builtin_shell_v1",
            Self::McpClient => "mcp_client",
        }
    }
}

/// Capabilities of the configured isolation executor, captured when the
/// registry is built and checked again before every sandboxed invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationExecutorCapabilities {
    pub name: Arc<str>,
    pub backend: Availability,
    pub modes: BTreeSet<SandboxMode>,
    pub protocols: BTreeSet<IsolationProtocol>,
}

/// Typed reasons a tool that requested kernel isolation was refused.
#[derive(Debug, Error)]
pub enum IsolationError {
    #[error("sandbox executor unavailable for tool '{tool}' in {mode} mode")]
    ExecutorUnavailable { tool: Arc<str>, mode: &'static str },
    #[error("sandbox backend unavailable for tool '{tool}' in {mode} mode: {reason}")]
    BackendUnavailable {
        tool: Arc<str>,
        mode: &'static str,
        reason: &'static str,
    },
    #[error(
        "isolation executor '{executor}' does not support protocol '{protocol}' for tool '{tool}'"
    )]
    UnsupportedProtocol {
        tool: Arc<str>,
        protocol: &'static str,
        executor: Arc<str>,
    },
    #[error("isolation executor '{executor}' does not support {mode} mode for tool '{tool}'")]
    UnsupportedMode {
        tool: Arc<str>,
        mode: &'static str,
        executor: Arc<str>,
    },
    #[error("failed to encode isolated request for tool '{tool}': {source}")]
    RequestEncoding {
        tool: Arc<str>,
        #[source]
        source: serde_json::Error,
    },
    #[error("sandbox execution failed for tool '{tool}': {source}")]
    Execution {
        tool: Arc<str>,
        #[source]
        source: ExecError,
    },
    #[error("isolation worker failed for tool '{tool}': {detail}")]
    WorkerFailed { tool: Arc<str>, detail: Arc<str> },
    #[error("invalid isolation worker response for tool '{tool}': {source}")]
    ResponseDecoding {
        tool: Arc<str>,
        #[source]
        source: serde_json::Error,
    },
}

pub(crate) struct IsolationExecutor {
    runner: PathBuf,
    capabilities: IsolationExecutorCapabilities,
}

impl IsolationExecutor {
    pub(crate) fn bundled_worker(runner: PathBuf) -> Self {
        Self {
            runner,
            capabilities: IsolationExecutorCapabilities {
                name: Arc::from("agentos-tool-worker"),
                backend: availability(),
                modes: BTreeSet::from([SandboxMode::ReadOnly, SandboxMode::WorkspaceWrite]),
                protocols: BTreeSet::from([IsolationProtocol::BuiltinShellV1]),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn new(runner: PathBuf, capabilities: IsolationExecutorCapabilities) -> Self {
        Self {
            runner,
            capabilities,
        }
    }

    pub(crate) fn capabilities(&self) -> &IsolationExecutorCapabilities {
        &self.capabilities
    }

    pub(crate) async fn call(
        &self,
        call: &ToolCall,
        mode: SandboxMode,
        protocol: IsolationProtocol,
        deadline: Duration,
        max_output_bytes: usize,
    ) -> Result<ToolResult, IsolationError> {
        if let Availability::Unavailable(reason) = self.capabilities.backend {
            return Err(IsolationError::BackendUnavailable {
                tool: Arc::clone(&call.name),
                mode: mode.as_str(),
                reason,
            });
        }
        if !self.capabilities.modes.contains(&mode) {
            return Err(IsolationError::UnsupportedMode {
                tool: Arc::clone(&call.name),
                mode: mode.as_str(),
                executor: Arc::clone(&self.capabilities.name),
            });
        }
        if !self.capabilities.protocols.contains(&protocol) {
            return Err(IsolationError::UnsupportedProtocol {
                tool: Arc::clone(&call.name),
                protocol: protocol.as_str(),
                executor: Arc::clone(&self.capabilities.name),
            });
        }
        let sandbox = Sandbox::new(mode, workspace_root());
        call_isolated_subprocess(&self.runner, call, deadline, &sandbox, max_output_bytes).await
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
    runner: &Path,
    call: &ToolCall,
    timeout: Duration,
    sandbox: &Sandbox,
    max_output_bytes: usize,
) -> Result<ToolResult, IsolationError> {
    let request = serde_json::to_vec(&IsolatedToolRequest { call }).map_err(|source| {
        IsolationError::RequestEncoding {
            tool: Arc::clone(&call.name),
            source,
        }
    })?;
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
    .map_err(|source| IsolationError::Execution {
        tool: Arc::clone(&call.name),
        source,
    })?;

    if !output.success {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(IsolationError::WorkerFailed {
            tool: Arc::clone(&call.name),
            detail: Arc::from(if message.is_empty() {
                "worker exited unsuccessfully".to_owned()
            } else {
                message.to_owned()
            }),
        });
    }

    let mut result: ToolResult = serde_json::from_slice(&output.stdout).map_err(|source| {
        IsolationError::ResponseDecoding {
            tool: Arc::clone(&call.name),
            source,
        }
    })?;
    result.metadata.insert(
        Arc::from("isolation"),
        Value::String("subprocess".to_owned()),
    );
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
