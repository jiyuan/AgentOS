//! A5 invariant: `Policy::narrow` property tests over random parent/child
//! pairs — narrowing must never widen what the parent permits.
//!
//! The contract is *exact*: for every call, the child's decision is no more
//! permissive than the parent's, arguments included
//! ([ADR-0001](../../../docs/adr/0001-POLICY_NARROWING.md)).
//!
//! This file used to say the opposite — that narrowing was "tool-granular,
//! not argument-granular", and scope its properties to match
//! `parent_exposes_tool`. A property suite written to the contract the code
//! happened to have cannot report that the contract is wrong, which is why
//! 1000 cases per run went green across the whole `AUTH-002` window.
//!
//! 1. Narrowing is reflexive: every policy narrows to itself.
//! 2. The narrowed default verb never exceeds the parent default verb.
//! 3. **The lattice property.** For every generated parent/child pair that
//!    narrows, and every call in the plan universe — tools, operations, and
//!    arguments — `child(call) <= parent(call)`.
//! 4. For actions the parent never exposes through any `Allow`/`AskUser`
//!    rule, the narrowed policy can never decide more permissively than the
//!    parent's default verb.
//! 5. A child `Allow` rule on a tool the parent does not expose is rejected
//!    with `PolicyError::Widened`.
//! 6. A child default verb more permissive than the parent's is rejected.
//! 7. A grant admits exactly what it names, and no more.

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
        // 1000 per run, or whatever `PROPTEST_CASES` says. `with_cases` would
        // otherwise pin it and quietly ignore the variable, which is what the
        // nightly deep-property job needs to turn up (M9 / `CI-002`).
        let cases = std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        let mut config = ProptestConfig::with_cases(cases);
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

    /// The property `AUTH-002` exists to establish, and the one this suite
    /// could not previously express: whatever the rules say, no call comes out
    /// of the child more permissively than it would from the parent.
    ///
    /// Quantified over the *call* universe rather than over rules, so a child
    /// rule that is subtly wider in its arguments is caught by the call that
    /// exploits the difference rather than by a structural comparison that
    /// might share the implementation's blind spot.
    #[test]
    fn a_narrowed_child_never_out_permits_its_parent_on_any_call(
        parent in policy_strategy(),
        child in policy_strategy(),
    ) {
        let Ok(effective) = Policy::narrow(&parent, &child) else {
            // Refusals are property 5's business; this one is about what a
            // policy that *was* admitted may then decide.
            return Ok(());
        };
        for plan in plan_universe() {
            let child_decision = effective.decide(&plan);
            let parent_decision = parent.decide(&plan);
            prop_assert!(
                decision_rank(&child_decision) <= decision_rank(&parent_decision),
                "child decided {child_decision:?} where the parent decides \
                 {parent_decision:?}\n  plan:   {plan:?}\n  parent: {parent:?}\n  child:  {child:?}"
            );
        }
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
        mut parent in policy_strategy(),
        tool_index in 0..TOOLS.len(),
        child_verb in prop_oneof![Just(PolicyVerb::Allow), Just(PolicyVerb::AskUser)],
    ) {
        // A parent with no *rule* for the tool can still permit it through a
        // permissive default, in which case the child widens nothing. Before
        // the lattice rewrite, `narrow` ignored the parent default when
        // judging a child rule and rejected those too. Pinned rather than
        // assumed away, because assuming it rejects most generated cases.
        parent.default_decision = PolicyVerb::Deny;
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
