//! `spill_read` — reading back a tool result that outgrew the inline cap.
//!
//! M7 / `SPILL-001`. Before this, the store handed the model an absolute host
//! path and told it to open the path with the `file` tool. Three separate
//! problems, and only the first is the obvious one:
//!
//! 1. The host's directory layout landed in a durable transcript that a
//!    provider reads on every later turn, and that elision re-emits.
//! 2. The locator was a *path*, so it authorized nothing. Anything able to
//!    read one path could read any path, and the "locator" was a suggestion.
//! 3. A spill root outside the workspace was simply unreadable, because the
//!    `file` tool is contained to the workspace. The feature quietly did not
//!    work for a deployment that put spill on another volume — which
//!    `[spill].root` explicitly supports.
//!
//! # What authorizes a read
//!
//! The calling run's own transcript. A locator may be read back exactly when
//! the transcript the model is looking at mentions it, which is the same thing
//! as saying: the model may re-read what it was told about, and nothing else.
//!
//! That is a better fit than the two obvious alternatives. Scoping to the
//! *run* would break the feature, because a conversation's later turns are
//! different runs and the locator lives in the transcript across all of them.
//! Scoping to the conversation would need a conversation id the spill store
//! does not have. The transcript is already the thing that spans exactly the
//! right set of turns, and it is already the thing a forged locator is absent
//! from.
//!
//! Beneath the check, `SpillStore::open` walks the store with
//! `openat(O_NOFOLLOW)` (M4 / `FS-001`), so a locator that somehow passed
//! still cannot name anything outside the store, and a symlink planted between
//! the write and the read is refused rather than followed.

use super::common::failed_result;
use crate::spill::{SpillLocator, SpillStore, SPILL_LOCATOR_KEY};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{
    SandboxMode, Tool, ToolError, ToolPersistenceScope, ToolSafety, ToolSideEffect, ToolSpec,
};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// Default bytes returned when the call names no limit.
pub const DEFAULT_SPILL_READ_BYTES: usize = 32 * 1024;
/// Ceiling on `max_bytes`, whatever the call asks for. A spill artifact is
/// unbounded by construction — it exists because something was too big — so
/// the read has to be the bounded end.
pub const MAX_SPILL_READ_BYTES: usize = 1024 * 1024;

/// Refused when the tool is called without a run context: there is no
/// transcript to check the locator against, and no safe default.
const NO_TRANSCRIPT: &str = "spill_read needs the run it was called from; this call arrived \
     without one, so there is no transcript to check the locator against";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpillReadArgs {
    locator: String,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    tail: bool,
}

/// Reads spilled tool output back, bounded and authorized by the transcript.
pub struct SpillReadTool {
    store: Arc<SpillStore>,
    read_bytes: usize,
    read_max_bytes: usize,
}

impl SpillReadTool {
    pub fn new(store: Arc<SpillStore>) -> Self {
        Self {
            store,
            read_bytes: DEFAULT_SPILL_READ_BYTES,
            read_max_bytes: MAX_SPILL_READ_BYTES,
        }
    }

    /// Bound reads by the deployment's `[limits]`, the same way `file` is
    /// bounded — a spill artifact is not special enough to get its own ceiling.
    pub fn with_limits(mut self, read_bytes: usize, read_max_bytes: usize) -> Self {
        self.read_bytes = read_bytes;
        self.read_max_bytes = read_max_bytes;
        self
    }
}

#[async_trait]
impl Tool for SpillReadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("spill_read"),
            description: Arc::from(
                "Read back the full output of an earlier tool call that was too large to \
                 return inline. Pass the `spill:` locator the truncated result cited.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["locator"],
                "properties": {
                    "locator": {
                        "type": "string",
                        "description": "The `spill:<run>/<artifact>` locator from a truncated tool result."
                    },
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": self.read_max_bytes,
                        "description": format!(
                            "Maximum bytes to return. Defaults to {} and is capped at {}.",
                            self.read_bytes, self.read_max_bytes
                        )
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Byte offset to read from. Ignored when tail is true."
                    },
                    "tail": {
                        "type": "boolean",
                        "description": "Return the end of the artifact instead of the start."
                    }
                }
            }),
            // The read happens in this process, against a descriptor the store
            // walked to itself. There is no child to sandbox, and declaring a
            // mode with nothing to enforce it would be a claim the kernel is
            // not making (M4 / `SBX-001`).
            safety: ToolSafety::new(ToolSideEffect::ReadOnly, ToolPersistenceScope::Conversation),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, _call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Err(ToolError::Failed(Arc::from(NO_TRANSCRIPT)))
    }

    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: SpillReadArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;

        let Some(locator) = SpillLocator::parse(&parsed.locator) else {
            return Ok(failed_result(
                call,
                "that is not a spill locator. Use the `spill:` value a truncated tool result \
                 cited, exactly as written.",
            ));
        };
        if !cited_in_transcript(ctx, &locator) {
            // Deliberately the same answer as "no such artifact". A model that
            // could tell a real locator it may not read from an invented one
            // would have an oracle over what other runs produced.
            return Ok(failed_result(
                call,
                "no such spilled output in this conversation. A locator can only be read \
                 back in the conversation whose transcript cites it.",
            ));
        }

        let file = match self.store.open(&locator) {
            Ok(file) => file,
            Err(err) => {
                tracing::warn!(
                    locator = locator.as_str(),
                    error = %err,
                    "a cited spill artifact could not be opened"
                );
                return Ok(failed_result(
                    call,
                    "that spilled output is no longer available; it may have been swept by \
                     the retention policy.",
                ));
            }
        };

        let max_bytes = parsed
            .max_bytes
            .unwrap_or(self.read_bytes)
            .min(self.read_max_bytes);
        let (content, total, truncated) =
            read_slice(file, max_bytes, parsed.offset.unwrap_or(0), parsed.tail)?;

        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from("locator"), Value::String(locator.to_string()));
        metadata.insert(Arc::from("artifact_bytes"), Value::from(total));
        metadata.insert(Arc::from("truncated"), Value::Bool(truncated));
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(content),
            metadata,
        })
    }
}

/// Whether this run's transcript cites `locator`.
///
/// Reads the metadata `loop::items` wrote rather than searching message text:
/// the text is a rendered sentence that a model could reproduce, and the
/// metadata is the runtime's own record of what it actually spilled.
fn cited_in_transcript(ctx: &RunContext<'_>, locator: &SpillLocator) -> bool {
    ctx.state.transcript.items.iter().any(|item| {
        item.message
            .metadata
            .get(SPILL_LOCATOR_KEY)
            .and_then(Value::as_str)
            .is_some_and(|cited| cited == locator.as_str())
    })
}

/// A refusal the model can read and act on, rather than an error that would
/// surface as a failed tool call. Reading back the wrong locator is an
/// Read a bounded window, reporting the artifact's real size so the model can
/// ask for the next one.
fn read_slice(
    mut file: std::fs::File,
    max_bytes: usize,
    offset: u64,
    tail: bool,
) -> Result<(String, u64, bool), ToolError> {
    let total = file
        .metadata()
        .map_err(|err| ToolError::Failed(err.to_string().into()))?
        .len();
    let start = if tail {
        total.saturating_sub(max_bytes as u64)
    } else {
        offset.min(total)
    };
    file.seek(SeekFrom::Start(start))
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;

    let read_len = (total.saturating_sub(start) as usize).min(max_bytes);
    let mut buf = vec![0; read_len];
    file.read_exact(&mut buf)
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;

    let truncated = start > 0 || start + read_len as u64 > 0 && start + read_len as u64 != total;
    let mut content = String::from_utf8_lossy(&buf).into_owned();
    if truncated {
        content.push_str(&format!(
            "\n[spill read: bytes {}..{} of {total}; pass offset or tail for more]",
            start,
            start + read_len as u64
        ));
    }
    Ok((content, total, truncated))
}
