//! Shared fixtures for the loop benchmarks.
//!
//! Everything here is deliberately stateless (no mutex-guarded call
//! recording like the `test_support` mocks) so multi-threaded benchmarks
//! measure loop behavior, not fixture lock contention or unbounded
//! capture-vector growth.

#![allow(dead_code)]

use agentos_core::approve::Policy;
use agentos_core::r#loop::{LoopDeps, RunLoopState, StartCtx};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, Message, MessageRole, RunId, ToolCall, ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

pub const BENCH_TOOL_NAME: &str = "echo";

pub fn current_thread_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current_thread tokio runtime builds")
}

pub fn fresh_state() -> RunState {
    let mut state = RunState::new(RunId::new("bench-run"), AgentId::new("bench-agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, "ping"),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

pub fn echo_tool_call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("bench-call-1"),
        name: Arc::from(BENCH_TOOL_NAME),
        args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
    }
}

/// Orchestrator that plans one tool call, then replies once the tool result
/// is visible in the transcript. Stateless: the decision is derived from the
/// transcript, so the same instance can drive any number of concurrent runs.
pub struct ScriptedToolOrchestrator;

#[async_trait]
impl Orchestrator for ScriptedToolOrchestrator {
    async fn hydrate(&self, _ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        Ok(())
    }

    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let tool_result_seen = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| matches!(item.message.role, MessageRole::Tool));
        if tool_result_seen {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "pong")))
        } else {
            Ok(Plan::CallTool(echo_tool_call()))
        }
    }
}

/// Orchestrator that always replies immediately (pure reply turn).
pub struct AlwaysReplyOrchestrator;

#[async_trait]
impl Orchestrator for AlwaysReplyOrchestrator {
    async fn hydrate(&self, _ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        Ok(())
    }

    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, "pong")))
    }
}

/// Tool that returns a canned result without recording calls.
pub struct NoopEchoTool;

#[async_trait]
impl Tool for NoopEchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(BENCH_TOOL_NAME),
            description: Arc::from("bench echo tool"),
            input_schema: serde_json::json!({"type": "object"}),
            requires_isolation: false,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("echo"),
            metadata: BTreeMap::new(),
        })
    }
}

pub fn echo_tool_registry() -> ToolRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(NoopEchoTool);
    tools
}

pub fn make_deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    policy: &'a Policy,
    tools: Option<&'a ToolRegistry>,
) -> LoopDeps<'a> {
    LoopDeps {
        orchestrator,
        max_turns: 8,
        hooks: None,
        tools,
        task_workspace: None,
        policy,
        subagents: None,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: &[],
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
    }
}

/// Drive a run until it reaches a terminal `Finish` or `Paused` state.
pub async fn drive(deps: &LoopDeps<'_>, state: RunState) -> RunLoopState {
    let mut current = RunLoopState::Start(StartCtx { state });
    loop {
        current = current.step(deps).await.expect("bench step succeeds");
        if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
            return current;
        }
    }
}

/// Drive a run that is expected to finish (not pause).
pub async fn drive_to_finish(deps: &LoopDeps<'_>, state: RunState) -> RunLoopState {
    let terminal = drive(deps, state).await;
    assert!(
        matches!(terminal, RunLoopState::Finish(_)),
        "bench run unexpectedly paused"
    );
    terminal
}
