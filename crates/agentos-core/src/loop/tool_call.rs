//! Executing one approved tool call.
//!
//! Split out of `loop/mod.rs` to keep it under the module ceiling. Everything
//! between "the policy engine said yes" and "there is a `ToolResult` to record"
//! lives here: the tool guardrail pass, the call itself, the spill of an
//! oversized result (roadmap C2), and the synthetic results a denial or a
//! tripped guardrail produce instead.
//!
//! The synthetic results matter as much as the real one. A denied or blocked
//! call comes back as a `ToolResult` the model can read and replan around,
//! rather than as an error that would fail the run — and, above it, the
//! sub-agent or gateway that started the run.

use super::cancel::unless_cancelled;
use super::items::{metadata_value, tool_status_name};
use super::telemetry::field_key;
use super::{ensure_guardrail_passed, LoopDeps, RunError};
use crate::audit::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyOutcome};
use crate::spill::{SpillRef, SpillSource};
use crate::tools::ToolRegistryError;
use crate::trace;
use agentos_interfaces::guardrail::GuardrailOutcome;
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{SpanKind, ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

/// Persist an oversized tool result, returning the reference to cite inline.
///
/// Best effort by design: with no store configured, a result within the cap,
/// or a storage failure, this yields `None` and the caller degrades to a plain
/// truncation notice. A full disk must not turn a successful tool call into a
/// failed run.
pub(super) async fn spill_oversized(
    state: &RunState,
    deps: &LoopDeps<'_>,
    tool_name: &str,
    result: &ToolResult,
) -> Option<SpillRef> {
    let store = deps.content_limits.spill?;
    if result.content.len() <= deps.content_limits.tool_result_inline_bytes {
        return None;
    }
    let source = SpillSource {
        run_id: &state.run_id,
        tool_name,
        call_id: &result.call_id,
    };
    match store.save_text(&source, &result.content).await {
        Ok(saved) => {
            info!(
                run_id = state.run_id.as_str(),
                tool_call_id = result.call_id.as_str(),
                bytes = saved.bytes,
                locator = saved.locator.as_str(),
                "tool output spilled"
            );
            Some(saved)
        }
        Err(error) => {
            tracing::warn!(
                run_id = state.run_id.as_str(),
                tool_call_id = result.call_id.as_str(),
                error = %error,
                "tool output spill failed; falling back to truncation"
            );
            None
        }
    }
}

pub(super) async fn execute_tool(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    call: ToolCall,
) -> Result<ToolResult, RunError> {
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("tool_name"), metadata_value(call.name.as_ref()));
    fields.insert(field_key("tool_call_id"), metadata_value(call.id.as_str()));
    let tool_span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Tool,
        format!("tool.{}", call.name),
        fields,
    );
    trace::record_event(
        state,
        deps.hooks,
        tool_span_id.clone(),
        "tool_started",
        BTreeMap::new(),
    );

    let preflight_guardrail_result = {
        let mut run_ctx = RunContext::from_state(state);
        run_ctx.cancel = deps.cancel.clone();
        let mut failure = None;
        for entry in deps.tool_guardrails {
            let outcome = entry.guardrail.check_call(&call, &run_ctx).await?;
            if let GuardrailOutcome::Tripped(reason) = outcome {
                // `Refused`, not `Tripped`: this one stops the call before it
                // runs. The post-result pass below is the `Tripped` case.
                deps.audit.record(
                    SafetyEvent::new(
                        SafetyEventKind::ToolGuardrailTrip,
                        SafetyOutcome::Refused,
                        Arc::clone(&entry.name),
                    )
                    .with_detail(reason.as_ref())
                    .with_digest(ArgumentDigest::of(call.args.get().as_bytes())),
                );
                failure = Some(guardrail_tool_result(&call, &entry.name, reason));
                break;
            }
        }
        failure
    };

    let result = if let Some(result) = preflight_guardrail_result {
        result
    } else {
        let tools = deps
            .tools
            .ok_or_else(|| ToolRegistryError::UnknownTool(Arc::clone(&call.name)))?;
        // Tool failures (bad path, missing file, malformed args) become a Failed
        // `ToolResult` rather than aborting the run, so the model can read the
        // error in the next turn and self-correct (e.g. create the missing dir
        // and retry). Unknown-tool / isolation errors still bubble up — those
        // indicate a misconfigured runtime, not a recoverable model mistake.
        let mut run_ctx = RunContext::from_state(state);
        run_ctx.cancel = deps.cancel.clone();
        // A tool that awaits is dropped mid-flight when the run is cancelled.
        // A tool that blocks the thread is not — see `cancel`'s module docs.
        let called = unless_cancelled(&deps.cancel, tools.call_with_context(&call, &run_ctx)).await;
        let Some(called) = called else {
            return Err(RunError::Cancelled);
        };
        match called {
            Ok(result) => result,
            Err(ToolRegistryError::Tool(tool_err)) => ToolResult {
                call_id: call.id.clone(),
                status: ToolStatus::Failed,
                content: Arc::from(tool_err.to_string()),
                metadata: BTreeMap::new(),
            },
            Err(other) => {
                // A tool that declared a sandbox mode the runtime cannot
                // enforce is refused rather than run unsandboxed (M4 /
                // `SBX-001`). That refusal is a safety decision and gets a
                // durable record; the other registry errors are ordinary
                // misconfiguration and do not.
                if matches!(
                    other,
                    ToolRegistryError::NoIsolatedExecutor { .. }
                        | ToolRegistryError::IncompatibleExecutor { .. }
                        | ToolRegistryError::SandboxExecutionFailed { .. }
                ) {
                    deps.audit.record(
                        SafetyEvent::new(
                            SafetyEventKind::SandboxRefusal,
                            SafetyOutcome::Refused,
                            Arc::clone(&call.name),
                        )
                        .with_detail(other.to_string())
                        .with_digest(ArgumentDigest::of(call.args.get().as_bytes())),
                    );
                }
                return Err(other.into());
            }
        }
    };

    {
        let mut run_ctx = RunContext::from_state(state);
        run_ctx.cancel = deps.cancel.clone();
        for entry in deps.tool_guardrails {
            let outcome = entry.guardrail.check_result(&result, &run_ctx).await?;
            ensure_guardrail_passed(
                deps,
                SafetyEventKind::ToolGuardrailTrip,
                SafetyOutcome::Tripped,
                &entry.name,
                outcome,
            )?;
        }
    }

    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("status"),
        metadata_value(tool_status_name(&result.status)),
    );
    trace::record_event(state, deps.hooks, tool_span_id, "tool_finished", fields);
    Ok(result)
}

/// Record a denied tool call: a `Tool` span/event for the trace (so a denied
/// call still shows up alongside executed ones) plus the `Denied` `ToolResult`
/// fed back into the transcript for the model to observe.
pub(super) fn denied_tool_result(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    call: &ToolCall,
    reason: Arc<str>,
) -> ToolResult {
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("tool_name"), metadata_value(call.name.as_ref()));
    fields.insert(field_key("tool_call_id"), metadata_value(call.id.as_str()));
    fields.insert(field_key("approval_denied"), Value::Bool(true));
    let tool_span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Tool,
        format!("tool.{}", call.name),
        fields,
    );
    let mut event_fields = BTreeMap::new();
    event_fields.insert(
        field_key("status"),
        metadata_value(tool_status_name(&ToolStatus::Denied)),
    );
    event_fields.insert(field_key("reason"), metadata_value(reason.as_ref()));
    trace::record_event(state, deps.hooks, tool_span_id, "tool_denied", event_fields);

    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("approval_denied"), Value::Bool(true));
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Denied,
        content: Arc::from(format!("tool call denied by policy: {reason}")),
        metadata,
    }
}

pub(super) fn guardrail_tool_result(
    call: &ToolCall,
    guardrail: &Arc<str>,
    reason: Arc<str>,
) -> ToolResult {
    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("guardrail"), metadata_value(guardrail.as_ref()));
    metadata.insert(field_key("guardrail_tripped"), Value::Bool(true));
    ToolResult {
        call_id: call.id.clone(),
        status: ToolStatus::Failed,
        content: Arc::from(format!("guardrail '{guardrail}' tripped: {reason}")),
        metadata,
    }
}
