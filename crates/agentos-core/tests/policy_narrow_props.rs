//! A5 invariant: `Policy::narrow` property tests over random parent/child
//! pairs — narrowing must never widen what the parent permits.
//!
//! The implementation's narrowing contract is *tool-granular*, not
//! argument-granular: a child `Allow` on tool T is accepted whenever the
//! parent has any `Allow`/`AskUser` rule for T, even if that parent rule is
//! constrained to specific arguments (see `parent_exposes_tool` in
//! `approve/mod.rs` — this is the documented "explicitly allowlisted
//! sub-agent tool needs no approval" escape hatch). The properties below
//! therefore assert the contract the code actually guarantees:
//!
//! 1. Narrowing is reflexive: every policy narrows to itself.
//! 2. The narrowed default verb never exceeds the parent default verb.
//! 3. For actions the parent never exposes through any `Allow`/`AskUser`
//!    rule, the narrowed policy can never decide more permissively than the
//!    parent's default verb.
//! 4. A child `Allow` rule on a tool the parent does not expose is rejected
//!    with `PolicyError::Widened`.
//! 5. A child default verb more permissive than the parent's is rejected.

use agentos_core::approve::{Policy, PolicyAction, PolicyDecision, PolicyRule, PolicyVerb};
use agentos_interfaces::orchestrator::{OrchestratorTemplate, Plan, SubAgentSpec, SubOrchSpec};
use agentos_proto::{AgentId, TaskId, ToolCall, ToolCallId};
use proptest::prelude::*;
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

const TOOLS: [&str; 3] = ["alpha", "beta", "gamma"];

/// Deny < AskUser < Allow on the permissiveness lattice.
fn verb_rank(verb: &PolicyVerb) -> u8 {
    match verb {
        PolicyVerb::Deny => 0,
        PolicyVerb::AskUser => 1,
        PolicyVerb::Allow => 2,
    }
}

fn decision_rank(decision: &PolicyDecision) -> u8 {
    match decision {
        PolicyDecision::Deny { .. } => 0,
        PolicyDecision::AskUser { .. } => 1,
        PolicyDecision::Allow => 2,
    }
}

fn verb_strategy() -> impl Strategy<Value = PolicyVerb> {
    prop_oneof![
        Just(PolicyVerb::Allow),
        Just(PolicyVerb::Deny),
        Just(PolicyVerb::AskUser),
    ]
}

fn action_strategy() -> impl Strategy<Value = PolicyAction> {
    prop_oneof![
        Just(PolicyAction::Any),
        (0..TOOLS.len()).prop_map(|index| PolicyAction::Tool(Arc::from(TOOLS[index]))),
        Just(PolicyAction::Handoff),
        Just(PolicyAction::Delegate),
        Just(PolicyAction::Escalate),
    ]
}

fn args_strategy() -> impl Strategy<Value = BTreeMap<Arc<str>, Value>> {
    prop_oneof![
        Just(BTreeMap::new()),
        Just(BTreeMap::from([(Arc::from("op"), Value::from("read"))])),
        Just(BTreeMap::from([(Arc::from("op"), Value::from("write"))])),
    ]
}

fn rule_strategy() -> impl Strategy<Value = PolicyRule> {
    (action_strategy(), verb_strategy(), args_strategy()).prop_map(
        |(action, decision, arg_equals)| PolicyRule {
            action,
            decision,
            reason: None,
            arg_equals,
        },
    )
}

fn policy_strategy() -> impl Strategy<Value = Policy> {
    (
        proptest::collection::vec(rule_strategy(), 0..4),
        verb_strategy(),
    )
        .prop_map(|(rules, default_decision)| Policy {
            rules,
            default_decision,
        })
}

fn call_tool_plan(tool: &str, args_json: &str) -> Plan {
    Plan::CallTool(ToolCall {
        id: ToolCallId::new("prop-call"),
        name: Arc::from(tool),
        args: RawValue::from_string(args_json.to_owned()).expect("static args are valid JSON"),
    })
}

/// Every plan shape `Policy::decide` distinguishes, across the generated
/// tool/arg universe. `Reply` and `ResumeSubAgent` are unconditionally
/// allowed by `decide` and carry no narrowing semantics, so they are
/// excluded.
fn plan_universe() -> Vec<Plan> {
    let mut plans = Vec::new();
    for tool in TOOLS {
        for args in ["{}", r#"{"op":"read"}"#, r#"{"op":"write"}"#] {
            plans.push(call_tool_plan(tool, args));
        }
    }
    plans.push(Plan::Handoff(AgentId::new("prop-agent"), None));
    plans.push(Plan::Delegate(SubAgentSpec {
        agent_id: AgentId::new("prop-agent"),
        policy_id: Arc::from("prop-policy"),
        metadata: BTreeMap::new(),
    }));
    plans.push(Plan::Escalate(SubOrchSpec {
        template: OrchestratorTemplate {
            name: Arc::from("prop-template"),
            stages: Vec::new(),
        },
        task_id: TaskId::new("prop-task"),
        policy_id: Arc::from("prop-policy"),
        metadata: BTreeMap::new(),
    }));
    plans
}

/// True when some parent `Allow`/`AskUser` rule could expose this plan's
/// action: a same-tool or `Any` rule for tool calls, a same-kind or `Any`
/// rule for structural plans. Mirrors (conservatively, ignoring argument
/// constraints) the exposure checks `Policy::narrow` performs.
fn parent_exposes(parent: &Policy, plan: &Plan) -> bool {
    parent.rules.iter().any(|rule| {
        if !matches!(rule.decision, PolicyVerb::Allow | PolicyVerb::AskUser) {
            return false;
        }
        match (&rule.action, plan) {
            (PolicyAction::Any, _) => true,
            (PolicyAction::Tool(tool), Plan::CallTool(call)) => tool == &call.name,
            (PolicyAction::Handoff, Plan::Handoff(_, _)) => true,
            (PolicyAction::Delegate, Plan::Delegate(_)) => true,
            (PolicyAction::Escalate, Plan::Escalate(_)) => true,
            _ => false,
        }
    })
}

proptest! {
    // Failure persistence is disabled because source-relative regression
    // files do not resolve inside integration tests; the printed seed and
    // minimal counterexample are sufficient to reproduce a failure.
    #![proptest_config({
        let mut config = ProptestConfig::with_cases(1000);
        config.failure_persistence = Some(Box::new(
            proptest::test_runner::FileFailurePersistence::Off,
        ));
        config
    })]

    #[test]
    fn narrow_is_reflexive(policy in policy_strategy()) {
        prop_assert!(
            Policy::narrow(&policy, &policy).is_ok(),
            "every policy must narrow to itself: {policy:?}"
        );
    }

    #[test]
    fn narrowed_default_never_exceeds_parent_default(
        parent in policy_strategy(),
        child in policy_strategy(),
    ) {
        if let Ok(effective) = Policy::narrow(&parent, &child) {
            prop_assert!(
                verb_rank(&effective.default_decision) <= verb_rank(&parent.default_decision),
                "narrowed default {:?} exceeds parent default {:?}",
                effective.default_decision,
                parent.default_decision,
            );
        }
    }

    #[test]
    fn narrowed_policy_never_exceeds_parent_default_on_unexposed_actions(
        parent in policy_strategy(),
        child in policy_strategy(),
    ) {
        let Ok(effective) = Policy::narrow(&parent, &child) else {
            // Rejected pairs are covered by the widening-rejection properties.
            return Ok(());
        };
        for plan in plan_universe() {
            if parent_exposes(&parent, &plan) {
                continue;
            }
            let decision = effective.decide(&plan);
            prop_assert!(
                decision_rank(&decision) <= verb_rank(&parent.default_decision),
                "unexposed plan {plan:?} decided {decision:?} but parent default is {:?}\n\
                 parent: {parent:?}\nchild: {child:?}",
                parent.default_decision,
            );
        }
    }

    #[test]
    fn allow_rule_on_unexposed_tool_is_rejected(
        parent in policy_strategy(),
        tool_index in 0..TOOLS.len(),
        child_verb in prop_oneof![Just(PolicyVerb::Allow), Just(PolicyVerb::AskUser)],
    ) {
        let tool = TOOLS[tool_index];
        let exposed = parent_exposes(&parent, &call_tool_plan(tool, "{}"));
        prop_assume!(!exposed);

        let child = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from(tool)),
                decision: child_verb.clone(),
                reason: None,
                arg_equals: BTreeMap::new(),
            }],
            default_decision: PolicyVerb::Deny,
        };
        prop_assert!(
            Policy::narrow(&parent, &child).is_err(),
            "child {child_verb:?} rule on unexposed tool '{tool}' must be rejected\nparent: {parent:?}"
        );
    }

    #[test]
    fn widened_child_default_is_rejected(
        mut parent in policy_strategy(),
        // Every strictly-widening (parent, child) default pair.
        (parent_default, child_default) in prop_oneof![
            Just((PolicyVerb::Deny, PolicyVerb::AskUser)),
            Just((PolicyVerb::Deny, PolicyVerb::Allow)),
            Just((PolicyVerb::AskUser, PolicyVerb::Allow)),
        ],
    ) {
        parent.default_decision = parent_default;
        let child = Policy {
            rules: Vec::new(),
            default_decision: child_default.clone(),
        };
        prop_assert!(
            Policy::narrow(&parent, &child).is_err(),
            "child default {child_default:?} must not widen parent default {:?}",
            parent.default_decision,
        );
    }
}
