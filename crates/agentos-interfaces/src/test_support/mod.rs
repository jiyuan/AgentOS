//! Deterministic mock implementations for every public trait in this crate.
//!
//! Every mock is built so a unit test can:
//! - construct it with `new()` and get safe defaults,
//! - override specific responses via `with_*` builders,
//! - inspect captured inputs via `*_calls()` accessors.
//!
//! All mocks are `Send + Sync` and hold their internal state behind
//! `std::sync::Mutex`. The mocks never `.await` while a lock is held, so the
//! sync mutex is safe inside `async` trait methods.
//!
//! Available with `--features test-support` for downstream consumers; always
//! available within `cfg(test)`.

use crate::channel::{Channel, ChannelError};
use crate::guardrail::{
    GuardrailError, GuardrailOutcome, Input, InputGuardrail, OutputGuardrail, ToolGuardrail,
};
use crate::mcp::{McpClient, McpError, McpServer};
use crate::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use crate::skill::{Skill, SkillError, SkillInvocation};
use crate::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{
    ChannelId, Envelope, Message, MessageRole, ToolCall, ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

mod storage;

pub use storage::{MockMemory, MockSession};

// ---------------------------------------------------------------------------
// Channel
// ---------------------------------------------------------------------------

/// Configurable [`Channel`] mock backed by an in-memory queue.
///
/// `receive()` pops the front of a pre-staged inbound queue (FIFO).
/// `send()` pushes outbound envelopes onto a captured list inspectable via
/// [`MockChannel::sent`].
pub struct MockChannel {
    id: ChannelId,
    inbound: Mutex<Vec<Envelope>>,
    sent: Mutex<Vec<Envelope>>,
    send_error: Mutex<Option<Arc<str>>>,
}

impl MockChannel {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self {
            id: ChannelId::new(id),
            inbound: Mutex::new(Vec::new()),
            sent: Mutex::new(Vec::new()),
            send_error: Mutex::new(None),
        }
    }

    pub fn with_inbound(self, envelopes: impl IntoIterator<Item = Envelope>) -> Self {
        self.inbound
            .lock()
            .expect("MockChannel inbound lock not poisoned")
            .extend(envelopes);
        self
    }

    pub fn with_send_error(self, reason: impl Into<Arc<str>>) -> Self {
        *self
            .send_error
            .lock()
            .expect("MockChannel send_error lock not poisoned") = Some(reason.into());
        self
    }

    /// Snapshot the envelopes that have been observed via `send()`.
    pub fn sent(&self) -> Vec<Envelope> {
        self.sent
            .lock()
            .expect("MockChannel sent lock not poisoned")
            .clone()
    }
}

#[async_trait]
impl Channel for MockChannel {
    fn id(&self) -> ChannelId {
        self.id.clone()
    }

    async fn receive(&mut self) -> Option<Envelope> {
        let mut inbound = self
            .inbound
            .lock()
            .expect("MockChannel inbound lock not poisoned");
        if inbound.is_empty() {
            None
        } else {
            Some(inbound.remove(0))
        }
    }

    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        if let Some(reason) = self
            .send_error
            .lock()
            .expect("MockChannel send_error lock not poisoned")
            .clone()
        {
            return Err(ChannelError::Backend(reason));
        }
        self.sent
            .lock()
            .expect("MockChannel sent lock not poisoned")
            .push(env);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Guardrails
// ---------------------------------------------------------------------------

/// Configurable [`InputGuardrail`] that returns a fixed [`GuardrailOutcome`].
pub struct MockInputGuardrail {
    outcome: Mutex<GuardrailOutcome>,
    calls: Mutex<u32>,
}

impl MockInputGuardrail {
    pub fn passing() -> Self {
        Self {
            outcome: Mutex::new(GuardrailOutcome::Passed),
            calls: Mutex::new(0),
        }
    }

    pub fn tripped(reason: impl Into<Arc<str>>) -> Self {
        Self {
            outcome: Mutex::new(GuardrailOutcome::Tripped(reason.into())),
            calls: Mutex::new(0),
        }
    }

    pub fn calls(&self) -> u32 {
        *self
            .calls
            .lock()
            .expect("MockInputGuardrail calls lock not poisoned")
    }
}

#[async_trait]
impl InputGuardrail for MockInputGuardrail {
    async fn check(
        &self,
        _input: &Input,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        *self
            .calls
            .lock()
            .expect("MockInputGuardrail calls lock not poisoned") += 1;
        Ok(self
            .outcome
            .lock()
            .expect("MockInputGuardrail outcome lock not poisoned")
            .clone())
    }
}

/// Configurable [`OutputGuardrail`] that returns a fixed [`GuardrailOutcome`].
pub struct MockOutputGuardrail {
    outcome: Mutex<GuardrailOutcome>,
    calls: Mutex<u32>,
}

impl MockOutputGuardrail {
    pub fn passing() -> Self {
        Self {
            outcome: Mutex::new(GuardrailOutcome::Passed),
            calls: Mutex::new(0),
        }
    }

    pub fn tripped(reason: impl Into<Arc<str>>) -> Self {
        Self {
            outcome: Mutex::new(GuardrailOutcome::Tripped(reason.into())),
            calls: Mutex::new(0),
        }
    }

    pub fn calls(&self) -> u32 {
        *self
            .calls
            .lock()
            .expect("MockOutputGuardrail calls lock not poisoned")
    }
}

#[async_trait]
impl OutputGuardrail for MockOutputGuardrail {
    async fn check(
        &self,
        _output: &Message,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        *self
            .calls
            .lock()
            .expect("MockOutputGuardrail calls lock not poisoned") += 1;
        Ok(self
            .outcome
            .lock()
            .expect("MockOutputGuardrail outcome lock not poisoned")
            .clone())
    }
}

/// Configurable [`ToolGuardrail`] that returns a fixed outcome for `check_call`
/// and `check_result`.
pub struct MockToolGuardrail {
    call_outcome: Mutex<GuardrailOutcome>,
    result_outcome: Mutex<GuardrailOutcome>,
    call_invocations: Mutex<u32>,
    result_invocations: Mutex<u32>,
}

impl MockToolGuardrail {
    pub fn passing() -> Self {
        Self {
            call_outcome: Mutex::new(GuardrailOutcome::Passed),
            result_outcome: Mutex::new(GuardrailOutcome::Passed),
            call_invocations: Mutex::new(0),
            result_invocations: Mutex::new(0),
        }
    }

    pub fn with_call_outcome(self, outcome: GuardrailOutcome) -> Self {
        *self
            .call_outcome
            .lock()
            .expect("MockToolGuardrail call_outcome lock not poisoned") = outcome;
        self
    }

    pub fn with_result_outcome(self, outcome: GuardrailOutcome) -> Self {
        *self
            .result_outcome
            .lock()
            .expect("MockToolGuardrail result_outcome lock not poisoned") = outcome;
        self
    }

    pub fn call_invocations(&self) -> u32 {
        *self
            .call_invocations
            .lock()
            .expect("MockToolGuardrail call_invocations lock not poisoned")
    }

    pub fn result_invocations(&self) -> u32 {
        *self
            .result_invocations
            .lock()
            .expect("MockToolGuardrail result_invocations lock not poisoned")
    }
}

#[async_trait]
impl ToolGuardrail for MockToolGuardrail {
    async fn check_call(
        &self,
        _call: &ToolCall,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        *self
            .call_invocations
            .lock()
            .expect("MockToolGuardrail call_invocations lock not poisoned") += 1;
        Ok(self
            .call_outcome
            .lock()
            .expect("MockToolGuardrail call_outcome lock not poisoned")
            .clone())
    }

    async fn check_result(
        &self,
        _result: &ToolResult,
        _ctx: &RunContext<'_>,
    ) -> Result<GuardrailOutcome, GuardrailError> {
        *self
            .result_invocations
            .lock()
            .expect("MockToolGuardrail result_invocations lock not poisoned") += 1;
        Ok(self
            .result_outcome
            .lock()
            .expect("MockToolGuardrail result_outcome lock not poisoned")
            .clone())
    }
}

// ---------------------------------------------------------------------------
// MCP
// ---------------------------------------------------------------------------

/// Configurable [`McpClient`] mock.
///
/// Returns the constructor-supplied tool list and a canned tool result; records
/// every `call_tool` invocation for inspection via [`MockMcpClient::calls`].
pub struct MockMcpClient {
    tools: Mutex<Vec<ToolSpec>>,
    canned_result: Mutex<ToolResult>,
    calls: Mutex<Vec<(McpServer, ToolCallId, Arc<str>)>>,
}

impl MockMcpClient {
    pub fn new() -> Self {
        Self {
            tools: Mutex::new(Vec::new()),
            canned_result: Mutex::new(ok_tool_result(ToolCallId::new("mock-mcp-call"), "mock")),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn with_tools(self, tools: impl IntoIterator<Item = ToolSpec>) -> Self {
        *self
            .tools
            .lock()
            .expect("MockMcpClient tools lock not poisoned") = tools.into_iter().collect();
        self
    }

    pub fn with_result(self, result: ToolResult) -> Self {
        *self
            .canned_result
            .lock()
            .expect("MockMcpClient canned_result lock not poisoned") = result;
        self
    }

    pub fn calls(&self) -> Vec<(McpServer, ToolCallId, Arc<str>)> {
        self.calls
            .lock()
            .expect("MockMcpClient calls lock not poisoned")
            .clone()
    }
}

impl Default for MockMcpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl McpClient for MockMcpClient {
    async fn list_tools(&self, _server: &McpServer) -> Result<Vec<ToolSpec>, McpError> {
        Ok(self
            .tools
            .lock()
            .expect("MockMcpClient tools lock not poisoned")
            .clone())
    }

    async fn call_tool(&self, server: &McpServer, call: &ToolCall) -> Result<ToolResult, McpError> {
        self.calls
            .lock()
            .expect("MockMcpClient calls lock not poisoned")
            .push((server.clone(), call.id.clone(), Arc::clone(&call.name)));
        let mut result = self
            .canned_result
            .lock()
            .expect("MockMcpClient canned_result lock not poisoned")
            .clone();
        result.call_id = call.id.clone();
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Skill
// ---------------------------------------------------------------------------

/// Configurable [`Skill`] mock.
pub struct MockSkill {
    name: Arc<str>,
    canned_result: Mutex<ToolResult>,
    invocations: Mutex<Vec<Arc<str>>>,
}

impl MockSkill {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        let name = name.into();
        Self {
            name: Arc::clone(&name),
            canned_result: Mutex::new(ok_tool_result(
                ToolCallId::new(format!("mock-skill-{name}")),
                "mock skill",
            )),
            invocations: Mutex::new(Vec::new()),
        }
    }

    pub fn with_result(self, result: ToolResult) -> Self {
        *self
            .canned_result
            .lock()
            .expect("MockSkill canned_result lock not poisoned") = result;
        self
    }

    pub fn invocations(&self) -> Vec<Arc<str>> {
        self.invocations
            .lock()
            .expect("MockSkill invocations lock not poisoned")
            .clone()
    }
}

#[async_trait]
impl Skill for MockSkill {
    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    async fn invoke(&self, invocation: &SkillInvocation) -> Result<ToolResult, SkillError> {
        self.invocations
            .lock()
            .expect("MockSkill invocations lock not poisoned")
            .push(Arc::clone(&invocation.name));
        Ok(self
            .canned_result
            .lock()
            .expect("MockSkill canned_result lock not poisoned")
            .clone())
    }
}

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

/// Configurable [`Tool`] mock.
pub struct MockTool {
    spec: ToolSpec,
    canned_result: Mutex<ToolResult>,
    calls: Mutex<Vec<ToolCallId>>,
}

impl MockTool {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        let name = name.into();
        Self {
            spec: ToolSpec {
                name: Arc::clone(&name),
                description: Arc::from("mock tool"),
                input_schema: serde_json::json!({"type": "object"}),
                requires_isolation: false,
                timeout_ms: None,
            },
            canned_result: Mutex::new(ok_tool_result(
                ToolCallId::new(format!("mock-tool-{name}")),
                "mock tool result",
            )),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn with_spec(mut self, spec: ToolSpec) -> Self {
        self.spec = spec;
        self
    }

    pub fn with_result(self, result: ToolResult) -> Self {
        *self
            .canned_result
            .lock()
            .expect("MockTool canned_result lock not poisoned") = result;
        self
    }

    pub fn calls(&self) -> Vec<ToolCallId> {
        self.calls
            .lock()
            .expect("MockTool calls lock not poisoned")
            .clone()
    }
}

#[async_trait]
impl Tool for MockTool {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.calls
            .lock()
            .expect("MockTool calls lock not poisoned")
            .push(call.id.clone());
        let mut result = self
            .canned_result
            .lock()
            .expect("MockTool canned_result lock not poisoned")
            .clone();
        result.call_id = call.id.clone();
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Configurable [`Orchestrator`] mock that returns a fixed [`Plan`].
pub struct MockOrchestrator {
    plan_response: Mutex<Plan>,
    plan_calls: Mutex<u32>,
    hydrate_calls: Mutex<u32>,
}

impl MockOrchestrator {
    pub fn replying(content: impl Into<Arc<str>>) -> Self {
        Self {
            plan_response: Mutex::new(Plan::Reply(Message::text(MessageRole::Assistant, content))),
            plan_calls: Mutex::new(0),
            hydrate_calls: Mutex::new(0),
        }
    }

    pub fn with_plan(plan: Plan) -> Self {
        Self {
            plan_response: Mutex::new(plan),
            plan_calls: Mutex::new(0),
            hydrate_calls: Mutex::new(0),
        }
    }

    pub fn plan_calls(&self) -> u32 {
        *self
            .plan_calls
            .lock()
            .expect("MockOrchestrator plan_calls lock not poisoned")
    }

    pub fn hydrate_calls(&self) -> u32 {
        *self
            .hydrate_calls
            .lock()
            .expect("MockOrchestrator hydrate_calls lock not poisoned")
    }
}

#[async_trait]
impl Orchestrator for MockOrchestrator {
    async fn hydrate(&self, _ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        *self
            .hydrate_calls
            .lock()
            .expect("MockOrchestrator hydrate_calls lock not poisoned") += 1;
        Ok(())
    }

    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        *self
            .plan_calls
            .lock()
            .expect("MockOrchestrator plan_calls lock not poisoned") += 1;
        let plan = self
            .plan_response
            .lock()
            .expect("MockOrchestrator plan_response lock not poisoned");
        Ok(clone_plan(&plan))
    }
}

fn clone_plan(plan: &Plan) -> Plan {
    match plan {
        Plan::Reply(message) => Plan::Reply(message.clone()),
        Plan::CallTool(call) => Plan::CallTool(ToolCall {
            id: call.id.clone(),
            name: Arc::clone(&call.name),
            args: RawValue::from_string(call.args.get().to_owned())
                .expect("ToolCall.args is valid JSON"),
        }),
        Plan::Handoff(agent_id, payload) => Plan::Handoff(agent_id.clone(), payload.clone()),
        Plan::Delegate(spec) => Plan::Delegate(spec.clone()),
        Plan::Escalate(spec) => Plan::Escalate(spec.clone()),
        Plan::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        } => Plan::ResumeSubAgent {
            spec: spec.clone(),
            child_channel_id: child_channel_id.clone(),
            child_conversation_id: child_conversation_id.clone(),
            child_state: Box::new((**child_state).clone()),
        },
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok_tool_result(call_id: ToolCallId, content: impl Into<Arc<str>>) -> ToolResult {
    ToolResult {
        call_id,
        status: ToolStatus::Succeeded,
        content: content.into(),
        metadata: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_state::RunState;
    use agentos_proto::ConversationId;
    use agentos_proto::{AgentId, RunId};

    fn run_state() -> RunState {
        RunState::new(RunId::new("test-run"), AgentId::new("test-agent"))
    }

    #[tokio::test]
    async fn mock_channel_receives_inbound_and_records_sent() {
        let envelope = Envelope {
            channel_id: ChannelId::new("c1"),
            conversation_id: ConversationId::new("conv-1"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "hi"),
            metadata: BTreeMap::new(),
        };
        let mut channel = MockChannel::new("test").with_inbound([envelope.clone()]);
        let received = channel.receive().await.expect("inbound queue had one item");
        assert_eq!(received.message.content.as_ref(), "hi");
        assert!(channel.receive().await.is_none());
        channel.send(envelope.clone()).await.expect("send ok");
        assert_eq!(channel.sent().len(), 1);
    }

    #[tokio::test]
    async fn mock_input_guardrail_records_calls() {
        let g = MockInputGuardrail::passing();
        let state = run_state();
        let ctx = RunContext::from_state(&state);
        let input = Input {
            message: Message::text(MessageRole::User, "x"),
        };
        let outcome = g.check(&input, &ctx).await.expect("ok");
        assert_eq!(outcome, GuardrailOutcome::Passed);
        assert_eq!(g.calls(), 1);
    }

    #[tokio::test]
    async fn mock_orchestrator_returns_canned_plan_and_counts_calls() {
        let orch = MockOrchestrator::replying("hello");
        let state = run_state();
        let ctx = RunContext::from_state(&state);
        let plan = orch.plan(&ctx).await.expect("plan ok");
        match plan {
            Plan::Reply(message) => assert_eq!(message.content.as_ref(), "hello"),
            other => panic!("unexpected plan: {other:?}"),
        }
        assert_eq!(orch.plan_calls(), 1);
    }

    #[tokio::test]
    async fn mock_tool_records_call_id_and_returns_result() {
        let tool = MockTool::new("mock");
        let raw = RawValue::from_string("{}".to_owned()).unwrap();
        let call = ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from("mock"),
            args: RawValue::from_string("{}".to_owned()).unwrap(),
        };
        let result = tool.call(&call, &raw).await.expect("call ok");
        assert_eq!(result.call_id.as_str(), "call-1");
        assert_eq!(tool.calls(), vec![ToolCallId::new("call-1")]);
    }

    #[tokio::test]
    async fn mock_skill_records_invocation() {
        let skill = MockSkill::new("greet");
        let invocation = SkillInvocation {
            name: Arc::from("greet"),
            args: RawValue::from_string("{}".to_owned()).unwrap(),
            metadata: BTreeMap::new(),
        };
        let _ = skill.invoke(&invocation).await.expect("invoke ok");
        assert_eq!(skill.invocations().len(), 1);
        assert_eq!(skill.name().as_ref(), "greet");
    }

    #[tokio::test]
    async fn mock_mcp_client_lists_tools_and_records_calls() {
        let client = MockMcpClient::new().with_tools([ToolSpec {
            name: Arc::from("ping"),
            description: Arc::from("returns pong"),
            input_schema: serde_json::json!({}),
            requires_isolation: false,
            timeout_ms: None,
        }]);
        let server = McpServer {
            id: Arc::from("server-1"),
            endpoint: Arc::from("stdio"),
        };
        let tools = client.list_tools(&server).await.expect("list ok");
        assert_eq!(tools.len(), 1);
        let call = ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from("ping"),
            args: RawValue::from_string("{}".to_owned()).unwrap(),
        };
        let _ = client.call_tool(&server, &call).await.expect("call ok");
        assert_eq!(client.calls().len(), 1);
    }

    #[tokio::test]
    async fn mock_tool_guardrail_separates_call_and_result_outcomes() {
        let g = MockToolGuardrail::passing()
            .with_call_outcome(GuardrailOutcome::Tripped(Arc::from("blocked")));
        let state = run_state();
        let ctx = RunContext::from_state(&state);
        let call = ToolCall {
            id: ToolCallId::new("c1"),
            name: Arc::from("t"),
            args: RawValue::from_string("{}".to_owned()).unwrap(),
        };
        let outcome = g.check_call(&call, &ctx).await.expect("ok");
        assert!(matches!(outcome, GuardrailOutcome::Tripped(_)));
        let result = ToolResult {
            call_id: ToolCallId::new("c1"),
            status: ToolStatus::Succeeded,
            content: Arc::from("done"),
            metadata: BTreeMap::new(),
        };
        let outcome = g.check_result(&result, &ctx).await.expect("ok");
        assert_eq!(outcome, GuardrailOutcome::Passed);
    }
}
