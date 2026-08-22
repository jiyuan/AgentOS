use super::common::{elapsed_ms, result_metadata, safe_workspace_path, workspace_root};
use crate::paths::RootDir;
use crate::sandbox::Sandbox;
use crate::tools::{Exec, ExecError, DEFAULT_MAX_OUTPUT_BYTES};
use agentos_interfaces::tool::{Isolation, SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The shell tool's self-declared deadline: long enough for a build or a test
/// suite, short enough that a wedged command does not sit there for an hour.
const SHELL_TIMEOUT_MS: u64 = 5 * 60 * 1_000;

/// What a shell command may write (roadmap item X2).
///
/// `workspace_write` rather than `read_only`: running a build or a test suite
/// is the reason this tool exists, and those write. Rather than the whole
/// filesystem, which is what it had before.
const SHELL_SANDBOX: SandboxMode = SandboxMode::WorkspaceWrite;

/// How much output one command may produce before the rest is read and
/// discarded (roadmap item X3). Carried on the tool for the same reason
/// `FileTool` carries its read bounds: nothing has to be consulted at call
/// time.
pub struct ShellTool {
    max_output_bytes: usize,
    /// `[isolation].env_passthrough`: names the deployment allows a command to
    /// read on top of the built-in allowlist (M4 / `PROC-001`).
    env_passthrough: Vec<String>,
}

impl Default for ShellTool {
    fn default() -> Self {
        Self {
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            env_passthrough: Vec::new(),
        }
    }
}

impl ShellTool {
    pub fn with_output_limit(max_output_bytes: usize) -> Self {
        Self {
            max_output_bytes,
            ..Self::default()
        }
    }

    pub fn with_env_passthrough(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.env_passthrough = names.into_iter().collect();
        self
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellArgs {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
}

#[async_trait]
impl Tool for ShellTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("shell"),
            description: Arc::from(
                "Run a shell command with structured arguments. The deployment's \
                 `[guardrails].shell_allowlist` decides which programs are accepted, \
                 and the command runs sandboxed: it may write beneath the workspace \
                 and the temporary directory, and nowhere else.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": { "type": "string" },
                    "args": { "type": "array", "items": { "type": "string" } },
                    "cwd": { "type": "string" }
                }
            }),
            sandbox: SHELL_SANDBOX,
            // A shell command is the one built-in that legitimately runs for
            // minutes — a build, a test suite. It declares its own deadline so
            // an unconfigured deployment does not hold it to the default that
            // suits an HTTP call. `[limits].tool_timeout_overrides` still wins.
            timeout_ms: Some(SHELL_TIMEOUT_MS),
        }
    }

    /// This tool applies [`SHELL_SANDBOX`] to its own child, so it is one of
    /// the two ways the declared mode can be real (M4 / `SBX-001`, ADR-0002).
    ///
    /// The claim is checked by construction below: `exec::run` hardens the
    /// command *before* the spawn and returns `ExecError::Sandbox` if it
    /// cannot, which this tool turns into an error rather than a result. There
    /// is no arrangement of arguments that reaches an unsandboxed child.
    fn isolation(&self) -> Isolation {
        Isolation::SelfHardened
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: ShellArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();
        let cwd = match parsed.cwd {
            Some(cwd) => {
                let root = workspace_root();
                // Two checks, because they answer different questions.
                // `safe_workspace_path` builds the path to hand to
                // `current_dir`; `RootDir::is_dir` proves that path is a real
                // directory the no-follow walk can reach, so a symlinked
                // `cwd` cannot start the command somewhere else (M4 /
                // `FS-001`). The kernel sandbox, not this, is what bounds
                // where the command may then write.
                let resolved = safe_workspace_path(&root, &cwd)
                    .map_err(|err| ToolError::Failed(Arc::from(err)))?;
                let contained = RootDir::open(&root)
                    .map(|root| root.is_dir(&cwd))
                    .unwrap_or(false);
                if !contained {
                    return Err(ToolError::Failed(Arc::from(format!(
                        "cwd {} is not a directory beneath the workspace root",
                        cwd.display()
                    ))));
                }
                Some(resolved)
            }
            None => None,
        };

        // The registry already holds this call to a resolved deadline; this
        // one is the belt to that brace, so a `ShellTool` used directly still
        // cannot hang its caller.
        let output = crate::tools::exec::run(Exec {
            program: &parsed.command,
            args: &parsed.args,
            cwd,
            stdin: None,
            timeout: Duration::from_millis(SHELL_TIMEOUT_MS),
            max_output_bytes: self.max_output_bytes,
            // The mode this tool declares in its own spec, enforced here
            // because a shell command is run directly rather than through the
            // isolation worker when no worker is configured.
            sandbox: &Sandbox::new(SHELL_SANDBOX, workspace_root()),
            extra_env: &self.env_passthrough,
        })
        .await;

        let duration_ms = elapsed_ms(start);
        let output = match output {
            Ok(output) => output,
            // A command that overran, or one that could not be started, is the
            // model's problem to work around rather than the run's to die on.
            Err(error @ (ExecError::TimedOut { .. } | ExecError::Spawn { .. })) => {
                let mut metadata = result_metadata(duration_ms, 0);
                metadata.insert(
                    Arc::from("timed_out"),
                    Value::Bool(matches!(error, ExecError::TimedOut { .. })),
                );
                return Ok(ToolResult {
                    call_id: call.id.clone(),
                    status: ToolStatus::Failed,
                    content: Arc::from(error.to_string()),
                    metadata,
                });
            }
            Err(error) => return Err(ToolError::Failed(error.to_string().into())),
        };

        let mut content = String::from_utf8_lossy(&output.stdout).into_owned();
        if content.is_empty() {
            content = String::from_utf8_lossy(&output.stderr).into_owned();
        }
        let bytes_out = content.len() as u64;
        let mut metadata = result_metadata(duration_ms, bytes_out);
        metadata.insert(
            Arc::from("exit_code"),
            output
                .status
                .map_or(Value::Null, |code| Value::from(code as i64)),
        );
        metadata.insert(
            Arc::from("stderr_bytes"),
            Value::from(output.stderr.len() as u64),
        );
        if output.truncated {
            metadata.insert(Arc::from("output_truncated"), Value::Bool(true));
        }

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: if output.success {
                ToolStatus::Succeeded
            } else {
                ToolStatus::Failed
            },
            content: Arc::from(content),
            metadata,
        })
    }
}
