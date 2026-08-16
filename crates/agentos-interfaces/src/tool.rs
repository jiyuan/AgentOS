use crate::orchestrator::RunContext;
use agentos_proto::{ToolCall, ToolResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool failed: {0}")]
    Failed(Arc<str>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub input_schema: Value,
    #[serde(default)]
    pub requires_isolation: bool,
    /// How long this tool may run before the registry gives up on it, in
    /// milliseconds (roadmap item D2).
    ///
    /// The tool's *own* declaration: set it when a tool knows it is slow (a
    /// build, a long fetch) and should not be held to the deployment's default.
    /// `None` accepts that default. A deployment can override either through
    /// `[limits].tool_timeout_overrides`, which wins over both.
    ///
    /// A deadline that expires produces a **failed `ToolResult`**, never an
    /// error: the run loop already recovers from a failed result by letting the
    /// model read it and replan, whereas an error would kill the run and every
    /// parent above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolMetadata {
    pub duration_ms: u64,
    pub bytes_out: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<Arc<str>, Value>,
}

#[async_trait]
pub trait Tool: Send + Sync {
    /// Return the static schema and safety metadata for this tool.
    fn spec(&self) -> ToolSpec;

    /// Execute a tool call after guardrails and approval pass.
    ///
    /// Implementations receive raw JSON arguments and should parse them only
    /// when execution requires it.
    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError>;

    /// Execute a tool call with the active run context.
    ///
    /// Tools that need authenticated caller metadata should override this.
    /// Most tools can use the default and parse only their raw arguments.
    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        _ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.call(call, args).await
    }
}
