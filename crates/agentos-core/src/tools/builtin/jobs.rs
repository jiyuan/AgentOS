//! `job_status`, `job_output`, `job_kill` — how a model observes and stops
//! background work.
//!
//! Roadmap item D3. These three are the read-and-control half of the item; the
//! producing half is *promotion*, in `tools::registry`: a tool that outlives
//! its D2 deadline becomes a job rather than being killed, and these tools are
//! how the model then follows it.
//!
//! # Why there is no `job_start`
//!
//! The roadmap names a fourth tool. It is deliberately not here, and the reason
//! is an invariant rather than scope. A `job_start` that runs another tool
//! would dispatch that inner call from *inside* the tool layer, below the point
//! where the loop applies tool guardrails and the approval engine — so
//! `job_start {tool: "shell", …}` would be a way to run a shell command that
//! the shell guardrail never sees. That is the same shortcut
//! [`ARCHITECTURE.md`](../../../../docs/ARCHITECTURE.md) forbids for
//! MCP-originated calls, which must re-enter the loop at `Approve`.
//!
//! Starting a job safely means re-entering the loop, which is a planning
//! concern (a new `Plan` variant) rather than a tool one. Promotion gives the
//! same capability today with none of the risk: the call has already passed
//! guardrails and approval as an ordinary tool call before it becomes a job.
//!
//! # Fencing
//!
//! Every tool here resolves the calling conversation from the run context and
//! passes it to the registry, which refuses to look up a job owned by anyone
//! else. A call arriving without a context — [`Tool::call`] rather than
//! [`Tool::call_with_context`] — is refused outright: there is no safe default
//! owner, and inventing one would be a way across the fence.

use crate::jobs::{JobError, JobId, JobRegistry, JobSnapshot};
use crate::memory::{conversation_id_from_context, conversation_principal_from_context};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{Principal, ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Refused when a job tool is called without a run context.
const NO_OWNER: &str = "job tools need the conversation they were called from; this call arrived \
     without one, so there is no way to tell which jobs it may see";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobRef {
    /// Absent on `job_status` means "list them all".
    #[serde(default)]
    job_id: Option<String>,
    /// `job_output` only: bytes already seen, so a poll returns just the new
    /// output.
    #[serde(default)]
    offset: Option<usize>,
}

/// Report on one job, or list every job this conversation owns.
pub struct JobStatusTool {
    jobs: Arc<JobRegistry>,
}

/// Read what a job has produced so far.
pub struct JobOutputTool {
    jobs: Arc<JobRegistry>,
}

/// Stop a running job.
pub struct JobKillTool {
    jobs: Arc<JobRegistry>,
}

impl JobStatusTool {
    pub fn new(jobs: Arc<JobRegistry>) -> Self {
        Self { jobs }
    }
}

impl JobOutputTool {
    pub fn new(jobs: Arc<JobRegistry>) -> Self {
        Self { jobs }
    }
}

impl JobKillTool {
    pub fn new(jobs: Arc<JobRegistry>) -> Self {
        Self { jobs }
    }
}

#[async_trait]
impl Tool for JobStatusTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("job_status"),
            description: Arc::from(
                "Report on a background job, or list every job in this conversation when no \
                 job_id is given.",
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" }
                }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Err(ToolError::Failed(Arc::from(NO_OWNER)))
    }

    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let (conversation, parsed) = owner_and_args(ctx, args)?;
        match parsed.job_id {
            Some(id) => {
                let id = JobId::parse(&id);
                match self.jobs.status(&conversation, &id) {
                    Ok(snapshot) => Ok(ok(call, render(&snapshot), snapshot_metadata(&snapshot))),
                    Err(error) => Ok(failed(call, &error)),
                }
            }
            None => {
                let jobs = self.jobs.list(&conversation);
                let content = if jobs.is_empty() {
                    "No background jobs in this conversation.".to_owned()
                } else {
                    jobs.iter().map(render).collect::<Vec<_>>().join("\n")
                };
                let mut metadata = BTreeMap::new();
                metadata.insert(Arc::from("job_count"), Value::from(jobs.len()));
                Ok(ok(call, content, metadata))
            }
        }
    }
}

#[async_trait]
impl Tool for JobOutputTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("job_output"),
            description: Arc::from(
                "Read a background job's output. Pass `offset` with the number of bytes already \
                 read to get only what is new.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["job_id"],
                "properties": {
                    "job_id": { "type": "string" },
                    "offset": { "type": "integer", "minimum": 0 }
                }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Err(ToolError::Failed(Arc::from(NO_OWNER)))
    }

    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let (conversation, parsed) = owner_and_args(ctx, args)?;
        let Some(id) = parsed.job_id else {
            return Ok(failed_text(call, "job_output needs a job_id"));
        };
        let id = JobId::parse(&id);
        let offset = parsed.offset.unwrap_or(0);
        match self.jobs.output(&conversation, &id, offset) {
            Ok(output) => {
                let mut metadata = BTreeMap::new();
                metadata.insert(Arc::from("offset"), Value::from(offset));
                metadata.insert(Arc::from("next_offset"), Value::from(offset + output.len()));
                let content = if output.is_empty() {
                    "No new output.".to_owned()
                } else {
                    output
                };
                Ok(ok(call, content, metadata))
            }
            Err(error) => Ok(failed(call, &error)),
        }
    }
}

#[async_trait]
impl Tool for JobKillTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("job_kill"),
            description: Arc::from("Stop a running background job."),
            input_schema: json!({
                "type": "object",
                "required": ["job_id"],
                "properties": {
                    "job_id": { "type": "string" }
                }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Err(ToolError::Failed(Arc::from(NO_OWNER)))
    }

    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let (conversation, parsed) = owner_and_args(ctx, args)?;
        let Some(id) = parsed.job_id else {
            return Ok(failed_text(call, "job_kill needs a job_id"));
        };
        let id = JobId::parse(&id);
        match self.jobs.kill(&conversation, &id) {
            Ok(()) => Ok(ok(call, format!("Stopped job `{id}`."), BTreeMap::new())),
            Err(error) => Ok(failed(call, &error)),
        }
    }
}

/// Resolve the calling conversation and parse the arguments, refusing a call
/// that has no owner.
fn owner_and_args(ctx: &RunContext<'_>, args: &RawValue) -> Result<(Principal, JobRef), ToolError> {
    // The presence check stays on the bare id: a context that names no
    // conversation has no owner, and `conversation_principal_from_context`
    // would substitute the run id rather than say so. Once there *is* an
    // owner, it is a principal, because that is what the registry fences on
    // (M3 deliverable 2).
    if conversation_id_from_context(ctx).is_none() {
        return Err(ToolError::Failed(Arc::from(NO_OWNER)));
    }
    let parsed: JobRef = serde_json::from_str(args.get())
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;
    Ok((conversation_principal_from_context(ctx), parsed))
}

fn render(snapshot: &JobSnapshot) -> String {
    let detail = snapshot
        .detail
        .as_deref()
        .map(|detail| format!(" — {detail}"))
        .unwrap_or_default();
    let truncated = if snapshot.output_truncated {
        " (output truncated)"
    } else {
        ""
    };
    format!(
        "`{}` [{}] {} · {} bytes of output{truncated}{detail}",
        snapshot.id,
        snapshot.kind,
        snapshot.state.as_str(),
        snapshot.output_bytes,
    )
}

fn snapshot_metadata(snapshot: &JobSnapshot) -> BTreeMap<Arc<str>, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from("job_state"),
        Value::String(snapshot.state.as_str().to_owned()),
    );
    metadata.insert(
        Arc::from("output_bytes"),
        Value::from(snapshot.output_bytes),
    );
    metadata
}

fn ok(call: &ToolCall, content: String, metadata: BTreeMap<Arc<str>, Value>) -> ToolResult {
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Succeeded,
        content: Arc::from(content),
        metadata,
    }
}

/// A registry refusal is a *failed result*, not an error: an unknown job id is
/// something the model should read and correct, not something that should end
/// the run.
fn failed(call: &ToolCall, error: &JobError) -> ToolResult {
    failed_text(call, &error.to_string())
}

fn failed_text(call: &ToolCall, message: &str) -> ToolResult {
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Failed,
        content: Arc::from(message),
        metadata: BTreeMap::new(),
    }
}
