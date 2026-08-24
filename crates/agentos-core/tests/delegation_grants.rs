//! AUTH-004: delegation grants are short-lived capabilities for one exact
//! parent actor, delegatee, action, and delegation generation.

use agentos_core::approve::{
    DelegatedAuthority, DelegationGrantTemplate, DelegationScope, Policy, PolicyAction, PolicyVerb,
    MAX_DELEGATION_GRANT_LIFETIME_SECS,
};
use agentos_core::subagents::{SubAgentDefinition, SubAgentRegistry, SubAgentRun};
use agentos_interfaces::orchestrator::{Plan, SubAgentSpec};
use agentos_interfaces::test_support::MockOrchestrator;
use agentos_proto::{
    ActorPrincipal, AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId,
    ToolCall, ToolCallId,
};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

const ISSUED_AT: u64 = 1_000;
const LIFETIME: u64 = 60;

fn actor(agent: &str, channel: &str, conversation: &str, sender: &str) -> ActorPrincipal {
    ActorPrincipal::new(
        AgentId::new(agent),
        ChannelId::new(channel),
        ConversationId::new(conversation),
        sender,
    )
}

fn call(operation: &str) -> Plan {
    Plan::CallTool(ToolCall {
        id: ToolCallId::new("grant-call"),
        name: Arc::from("file"),
        args: RawValue::from_string(serde_json::json!({ "operation": operation }).to_string())
            .expect("test arguments are valid JSON"),
    })
}

fn template(lifetime_secs: u64) -> DelegationGrantTemplate {
    DelegationGrantTemplate {
        action: PolicyAction::Tool(Arc::from("file")),
        decision: PolicyVerb::Allow,
        arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
        reason: Arc::from("child may read the one delegated resource"),
        lifetime_secs,
    }
}

#[test]
// AF-014: one runtime grant is scoped to one actor and delegation generation.
fn grant_is_actor_bound_expiring_and_non_transitive() {
    let parent = Policy::ask_user_tools(["file"]);
    let alice = actor("parent", "telegram", "group-1", "alice");
    let scope = DelegationScope::for_generation(
        alice.clone(),
        AgentId::new("child"),
        "child-policy",
        "delegation.v1.generation-a",
        ISSUED_AT,
    )
    .expect("test generation is valid");
    let templates = [template(LIFETIME)];
    let authority = DelegatedAuthority::issue(&templates, &parent, scope.clone())
        .expect("bounded authority issues");
    let grant = &authority.grants()[0];

    assert!(grant.covers_at(&call("read"), &scope, ISSUED_AT));
    assert!(grant.covers_at(&call("read"), &scope, ISSUED_AT + LIFETIME - 1));
    assert!(!grant.covers_at(&call("write"), &scope, ISSUED_AT));
    assert!(!grant.covers_at(&call("read"), &scope, ISSUED_AT - 1));
    assert!(!grant.covers_at(&call("read"), &scope, ISSUED_AT + LIFETIME));
    assert!(!grant.covers_at(&call("read"), &scope, ISSUED_AT + 10_000));

    let variants = [
        DelegationScope::for_generation(
            actor("parent", "telegram", "group-1", "bob"),
            AgentId::new("child"),
            "child-policy",
            scope.generation_id(),
            ISSUED_AT,
        ),
        DelegationScope::for_generation(
            actor("parent", "feishu", "group-1", "alice"),
            AgentId::new("child"),
            "child-policy",
            scope.generation_id(),
            ISSUED_AT,
        ),
        DelegationScope::for_generation(
            actor("other-parent", "telegram", "group-1", "alice"),
            AgentId::new("child"),
            "child-policy",
            scope.generation_id(),
            ISSUED_AT,
        ),
        DelegationScope::for_generation(
            alice.clone(),
            AgentId::new("other-child"),
            "child-policy",
            scope.generation_id(),
            ISSUED_AT,
        ),
        DelegationScope::for_generation(
            alice.clone(),
            AgentId::new("child"),
            "other-policy",
            scope.generation_id(),
            ISSUED_AT,
        ),
        DelegationScope::for_generation(
            alice,
            AgentId::new("child"),
            "child-policy",
            "delegation.v1.generation-b",
            ISSUED_AT,
        ),
    ];
    for variant in variants {
        let variant = variant.expect("variant generation is valid");
        assert!(!grant.covers_at(&call("read"), &variant, ISSUED_AT));
    }

    // The immediate child holding this grant cannot use it as authority for a
    // grandchild: both the initiating actor and delegatee change at level two.
    let grandchild_scope = DelegationScope::for_generation(
        actor("child", "subagent:child", "child-conversation", "parent"),
        AgentId::new("grandchild"),
        "grandchild-policy",
        scope.generation_id(),
        ISSUED_AT,
    )
    .expect("grandchild scope is structurally valid");
    assert!(!grant.covers_at(&call("read"), &grandchild_scope, ISSUED_AT));

    // Reconstructing the same generation from the same template produces the
    // same audit identity; a new generation above does not cover the grant.
    let reissued = DelegatedAuthority::issue(&templates, &parent, scope)
        .expect("same generation reconstructs");
    assert_eq!(grant.id(), reissued.grants()[0].id());
}

#[test]
fn grant_lifetime_is_mandatory_and_capped() {
    let parent = Policy::ask_user_tools(["file"]);
    let scope = DelegationScope::for_generation(
        actor("parent", "telegram", "group-1", "alice"),
        AgentId::new("child"),
        "child-policy",
        "delegation.v1.generation-a",
        ISSUED_AT,
    )
    .expect("test generation is valid");

    assert!(DelegatedAuthority::issue(&[template(0)], &parent, scope.clone()).is_err());
    assert!(DelegatedAuthority::issue(
        &[template(MAX_DELEGATION_GRANT_LIFETIME_SECS + 1)],
        &parent,
        scope,
    )
    .is_err());
}

#[tokio::test]
async fn paused_child_retains_grant_generation_and_expiry() {
    let parent = Policy::ask_user_tools(["file", "confirm"]);
    let child = Policy {
        rules: vec![
            agentos_core::approve::PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
            },
            agentos_core::approve::PolicyRule {
                action: PolicyAction::Tool(Arc::from("confirm")),
                decision: PolicyVerb::AskUser,
                reason: Some(Arc::from("pause the child")),
                arg_equals: BTreeMap::new(),
            },
        ],
        default_decision: PolicyVerb::Deny,
    };
    let mut registry = SubAgentRegistry::new();
    registry.register(
        SubAgentDefinition::new(
            AgentId::new("child"),
            "child-policy",
            Arc::new(MockOrchestrator::with_plan(call_for("confirm", "{}"))),
            child,
        )
        .with_delegation_grants(vec![template(LIFETIME)]),
    );
    let initiating_actor = actor("parent", "telegram", "group-1", "alice");
    let input = Envelope {
        channel_id: ChannelId::new("subagent:child"),
        conversation_id: ConversationId::new("child-conversation"),
        sender: Arc::from("parent"),
        message: Message::text(MessageRole::User, "pause"),
        metadata: BTreeMap::new(),
    };
    let spec = SubAgentSpec {
        agent_id: AgentId::new("child"),
        policy_id: Arc::from("child-policy"),
        metadata: BTreeMap::new(),
    };
    let invocation = registry
        .prepare(
            &spec,
            &parent,
            initiating_actor.clone(),
            input.clone(),
            RunId::new("child-run"),
        )
        .expect("initial delegation prepares");
    let original_id = invocation.grants_relied_on()[0].id().to_owned();
    let original_expiry = invocation.grants_relied_on()[0].expires_at();
    let original_generation = invocation
        .delegated_authority()
        .scope()
        .generation_id()
        .to_owned();
    let SubAgentRun::Paused(paused) = invocation.run().await.expect("child pauses") else {
        panic!("the confirm call should pause");
    };

    let wrong_actor = actor("parent", "telegram", "group-1", "bob");
    assert!(registry
        .prepare_resume(
            &spec,
            &parent,
            wrong_actor,
            input.clone(),
            paused.state.run_id.clone(),
            &paused.state,
        )
        .is_err());

    let resumed = registry
        .prepare_resume(
            &spec,
            &parent,
            initiating_actor,
            input,
            paused.state.run_id.clone(),
            &paused.state,
        )
        .expect("paused delegation reconstructs");
    assert_eq!(resumed.grants_relied_on()[0].id(), original_id);
    assert_eq!(resumed.grants_relied_on()[0].expires_at(), original_expiry);
    assert_eq!(
        resumed.delegated_authority().scope().generation_id(),
        original_generation
    );
}

fn call_for(tool: &str, args: &str) -> Plan {
    Plan::CallTool(ToolCall {
        id: ToolCallId::new(format!("{tool}-call")),
        name: Arc::from(tool),
        args: RawValue::from_string(args.to_owned()).expect("test args are valid JSON"),
    })
}
