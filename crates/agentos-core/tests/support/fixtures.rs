//! Deterministic tools and orchestrator wiring shared by the golden suites.
//!
//! Split out of `transcripts.rs` so the scenarios can live in more than one
//! test binary without each one redefining the same echo tool.

use super::{scenario, ScriptedLlm};
use agentos_core::orchestrator::MaxOrchestrator;
use agentos_core::runner::RunOutcome;
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub const ECHO_TOOL: &str = "echo";

/// A deterministic tool: the golden pins its output, so it must not depend on
/// the clock, the filesystem, or the environment.
pub struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(ECHO_TOOL),
            description: Arc::from("Echo the `text` argument back verbatim."),
            input_schema: json!({
                "type": "object",
                "required": ["text"],
                "properties": { "text": { "type": "string" } }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let text = serde_json::from_str::<Value>(args.get())
            .ok()
            .and_then(|value| value.get("text").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default();
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(text),
            metadata: BTreeMap::new(),
        })
    }
}

pub const BULK_TOOL: &str = "bulk";

/// Deterministic numbered lines, so a spilled artifact is byte-identical run
/// to run and the golden can pin the preview it produced.
pub fn bulk_output(lines: usize) -> String {
    (0..lines)
        .map(|index| format!("line {index:04}: the quick brown fox jumps over the lazy dog\n"))
        .collect()
}

/// A tool whose output exceeds any sane inline cap — the case C2 exists for.
pub struct BulkTool;

#[async_trait]
impl Tool for BulkTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(BULK_TOOL),
            description: Arc::from("Emit `lines` numbered lines of filler."),
            input_schema: json!({
                "type": "object",
                "required": ["lines"],
                "properties": { "lines": { "type": "integer" } }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let lines = serde_json::from_str::<Value>(args.get())
            .ok()
            .and_then(|value| value.get("lines").and_then(Value::as_u64))
            .unwrap_or(0) as usize;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(bulk_output(lines)),
            metadata: BTreeMap::new(),
        })
    }
}

pub fn echo_registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(EchoTool);
    tools
}

pub fn max_with(llm: Arc<ScriptedLlm>, tools: Vec<ToolSpec>) -> MaxOrchestrator {
    MaxOrchestrator::with_tools(tools).with_llm(llm)
}

/// Pin a whole scenario — assembled requests, session items, outcome — as one
/// golden artifact.
pub fn scenario_golden(
    name: &str,
    llm: &ScriptedLlm,
    transcript: &agentos_interfaces::session::Transcript,
    outcome: &RunOutcome,
) {
    super::assert_golden(name, &scenario(llm, transcript, outcome));
}
