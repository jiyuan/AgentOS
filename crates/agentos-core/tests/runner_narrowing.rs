//! A5 / A1 invariant at runner level: sub-agent policies only narrow.
//!
//! Unit tests in `approve/` and `runtime/tools_config.rs` already cover
//! `Policy::narrow` in isolation; these tests prove the invariant holds
//! end-to-end through `run_envelope`:
//!
//! 1. A child declaring a tool the parent never grants is rejected at
//!    delegation time — the declaration cannot widen the parent.
//! 2. A parent's own tool call still requires user approval even when a
//!    configured child declares that tool `allow` — the parent does not
//!    inherit child permissions.
//! 3. A child whose `allow` narrows a parent `ask_user` runs the tool
//!    without pausing — narrowing in the permitted direction still works.

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome, RunnerDeps};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Arc;

const TOOL_NAME: &str = "mock";

fn tool_call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("narrowing-call"),
        name: Arc::from(TOOL_NAME),
        args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
    }
}

fn user_envelope(text: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new("test-channel"),
        conversation_id: ConversationId::new("conv-1"),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// Parent: delegates to the child on user input, replies once the child's
/// result lands in the transcript.
struct DelegatingParent;

#[async_trait]
impl Orchestrator for DelegatingParent {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let Some(item) = ctx.state.transcript.items.last() else {
            return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
        };
        match item.message.role {
            MessageRole::User => Ok(Plan::Delegate(SubAgentSpec {
                agent_id: AgentId::new("child"),
                policy_id: Arc::from("child-policy"),
                metadata: BTreeMap::new(),
            })),
            MessageRole::Tool => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                format!("parent saw: {}", item.message.content),
            ))),
            MessageRole::Assistant | MessageRole::System => {
                Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")))
            }
        }
    }
}

/// Parent: calls the tool itself on user input (no delegation involved).
struct ToolCallingParent;

#[async_trait]
impl Orchestrator for ToolCallingParent {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let tool_result_seen = ctx
            .state
            .transcript
            .items
            .iter()
            .any(|item| matches!(item.message.role, MessageRole::Tool));
        if tool_result_seen {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        } else {
            Ok(Plan::CallTool(tool_call()))
        }
    }
}

/// Child: calls the tool once, then replies with its result.
struct ToolCallingChild;

#[async_trait]
impl Orchestrator for ToolCallingChild {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let Some(item) = ctx.state.transcript.items.last() else {
            return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
        };
        match item.message.role {
            MessageRole::Tool => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                format!("child result: {}", item.message.content),
            ))),
            MessageRole::User | MessageRole::Assistant | MessageRole::System => {
                Ok(Plan::CallTool(tool_call()))
            }
        }
    }
}

struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(TOOL_NAME),
            description: Arc::from("narrowing test tool"),
            input_schema: serde_json::json!({"type": "object"}),
            requires_isolation: false,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ok"),
            metadata: BTreeMap::new(),
        })
    }
}

fn child_registry(session: Arc<InMemorySession>, child_policy: Policy) -> SubAgentRegistry {
    let mut tools = ToolRegistry::new();
    tools.register(EchoTool);
    let mut registry = SubAgentRegistry::new().with_session(session);
    registry.register(
        SubAgentDefinition::new(
            AgentId::new("child"),
            "child-policy",
            Arc::new(ToolCallingChild),
            child_policy,
        )
        .with_tools(Arc::new(tools))
        .with_max_turns(4),
    );
    registry
}

fn parent_policy(tool_verb: Option<PolicyVerb>) -> Policy {
    let mut rules = vec![PolicyRule {
        action: PolicyAction::Delegate,
        decision: PolicyVerb::Allow,
        reason: None,
        arg_equals: BTreeMap::new(),
    }];
    if let Some(decision) = tool_verb {
        rules.push(PolicyRule {
            action: PolicyAction::Tool(Arc::from(TOOL_NAME)),
            decision,
            reason: Some(Arc::from("tool is gated for the parent")),
            arg_equals: BTreeMap::new(),
        });
    }
    Policy {
        rules,
        default_decision: PolicyVerb::Deny,
    }
}

fn runner_deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    session: &'a InMemorySession,
    policy: &'a Policy,
    tools: Option<&'a ToolRegistry>,
    subagents: Option<&'a SubAgentRegistry>,
) -> RunnerDeps<'a> {
    RunnerDeps {
        orchestrator,
        session,
        memory_manager: None,
        hooks: None,
        max_turns: 8,
        active_agent: AgentId::new("parent"),
        tools,
        trace_sink: None,
        task_workspace: None,
        policy,
        subagents,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: &[],
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel: Default::default(),
    }
}

#[tokio::test]
async fn child_tool_declaration_cannot_widen_parent_policy() {
    // Parent never grants the tool (no rule for it, default deny); the child
    // declares it `allow`. Delegation must fail with a narrowing error — the
    // child's declaration must not become a permission grant.
    let session = Arc::new(InMemorySession::default());
    let registry = child_registry(session.clone(), Policy::allow_tools([TOOL_NAME]));
    let parent = DelegatingParent;
    let policy = parent_policy(None);
    let deps = runner_deps(&parent, session.as_ref(), &policy, None, Some(&registry));

    let error = run_envelope(
        user_envelope("delegate please"),
        RunId::new("widen-run"),
        &deps,
    )
    .await
    .expect_err("delegation to a widening child must fail");
    assert!(
        error.to_string().contains("not a narrowing"),
        "error must name the narrowing violation, got: {error}"
    );
}

#[tokio::test]
async fn parent_tool_call_still_requires_approval_when_child_declares_allow() {
    // The same child `allow` declaration is configured, but here the PARENT
    // calls the tool itself. The parent's `ask_user` gate must still pause
    // the run — the parent does not inherit the child's permissions.
    let session = Arc::new(InMemorySession::default());
    let registry = child_registry(session.clone(), Policy::allow_tools([TOOL_NAME]));
    let parent = ToolCallingParent;
    let policy = parent_policy(Some(PolicyVerb::AskUser));
    let mut tools = ToolRegistry::new();
    tools.register(EchoTool);
    let deps = runner_deps(
        &parent,
        session.as_ref(),
        &policy,
        Some(&tools),
        Some(&registry),
    );

    let outcome = run_envelope(
        user_envelope("parent, run the tool"),
        RunId::new("parent-gate-run"),
        &deps,
    )
    .await
    .expect("parent run starts cleanly");
    let RunOutcome::Paused(state) = outcome else {
        panic!("parent tool call must pause for approval, got a finished run");
    };
    assert_eq!(
        state.pending_approvals.len(),
        1,
        "exactly one pending approval for the parent's gated tool call"
    );
}

#[tokio::test]
async fn child_allow_narrowing_parent_ask_user_runs_without_pause() {
    // Parent gates the tool behind ask_user; the child allowlists it. This is
    // the permitted narrowing direction: the delegated child executes the
    // tool without pausing, and the parent gets its result.
    let session = Arc::new(InMemorySession::default());
    let registry = child_registry(session.clone(), Policy::allow_tools([TOOL_NAME]));
    let parent = DelegatingParent;
    let policy = parent_policy(Some(PolicyVerb::AskUser));
    let deps = runner_deps(&parent, session.as_ref(), &policy, None, Some(&registry));

    let outcome = run_envelope(
        user_envelope("delegate please"),
        RunId::new("narrow-run"),
        &deps,
    )
    .await
    .expect("delegated run completes");
    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("narrowed child must run without pausing");
    };
    assert!(
        output.message.content.contains("child result: ok"),
        "parent must receive the child's tool result, got: {}",
        output.message.content
    );
}
