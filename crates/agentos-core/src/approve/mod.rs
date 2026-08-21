use agentos_interfaces::orchestrator::{OrchestratorTemplate, Plan, SubAgentSpec, SubOrchSpec};
use agentos_proto::{AgentId, TaskId, ToolCall, ToolCallId};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecision {
    Allow,
    Deny { reason: Arc<str> },
    AskUser { reason: Arc<str> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyVerb {
    Allow,
    Deny,
    AskUser,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PolicyAction {
    Any,
    Tool(Arc<str>),
    Handoff,
    Delegate,
    Escalate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyRule {
    pub action: PolicyAction,
    pub decision: PolicyVerb,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Arc<str>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arg_equals: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub rules: Vec<PolicyRule>,
    pub default_decision: PolicyVerb,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("child policy widens parent permissions for {0}")]
    Widened(Arc<str>),
    #[error("invalid policy YAML at line {line}: {message}")]
    InvalidYaml { line: usize, message: Arc<str> },
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            default_decision: PolicyVerb::Deny,
        }
    }
}

impl Policy {
    pub fn allow_tools(tools: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Self {
        Self {
            rules: tools
                .into_iter()
                .map(|tool| PolicyRule {
                    action: PolicyAction::Tool(tool.into()),
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::new(),
                })
                .collect(),
            default_decision: PolicyVerb::Deny,
        }
    }

    pub fn ask_user_tools(tools: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Self {
        Self {
            rules: tools
                .into_iter()
                .map(|tool| PolicyRule {
                    action: PolicyAction::Tool(tool.into()),
                    decision: PolicyVerb::AskUser,
                    reason: Some(Arc::from("tool requires approval")),
                    arg_equals: BTreeMap::new(),
                })
                .collect(),
            default_decision: PolicyVerb::Deny,
        }
    }

    pub fn decide(&self, plan: &Plan) -> PolicyDecision {
        if matches!(plan, Plan::Reply(_) | Plan::ResumeSubAgent { .. }) {
            return PolicyDecision::Allow;
        }

        // A batch is decided by its strictest member (roadmap X1). The loop
        // splits batches before `Approve`, so this is unreachable today — but
        // "unreachable" is not a security property, and a future caller that
        // routed a batch straight here must not get a decision weaker than the
        // one its most dangerous call would have received.
        if let Plan::CallTools(calls) = plan {
            return calls
                .iter()
                .map(|call| self.decide(&Plan::CallTool(call.clone())))
                .reduce(strictest)
                .unwrap_or_else(|| PolicyDecision::Deny {
                    reason: Arc::from("empty tool batch"),
                });
        }

        let tool_args = match plan {
            Plan::CallTool(call) if self.tool_has_arg_constraints(&call.name) => {
                serde_json::from_str::<Value>(call.args.get()).ok()
            }
            _ => None,
        };

        for rule in &self.rules {
            if rule.matches(plan, tool_args.as_ref()) {
                return rule.to_decision();
            }
        }

        default_policy_decision(&self.default_decision, plan)
    }

    fn tool_has_arg_constraints(&self, tool_name: &Arc<str>) -> bool {
        self.rules.iter().any(|rule| {
            if rule.arg_equals.is_empty() {
                return false;
            }
            match &rule.action {
                PolicyAction::Any => true,
                PolicyAction::Tool(name) => name == tool_name,
                PolicyAction::Handoff | PolicyAction::Delegate | PolicyAction::Escalate => false,
            }
        })
    }

    /// Narrow `child` against `parent`, with no delegation grants.
    ///
    /// See [`Self::narrow_with_grants`] for the rule this enforces.
    pub fn narrow(parent: &Self, child: &Self) -> Result<Self, PolicyError> {
        Self::narrow_with_grants(parent, child, &[])
    }

    /// Narrow `child` against `parent`, treating `grants` as authority the
    /// parent additionally holds *for this delegatee only*.
    ///
    /// The property, from [ADR-0001](../../../../docs/adr/0001-POLICY_NARROWING.md):
    /// for every possible call, the child's effective decision is no more
    /// permissive than the parent's, arguments included.
    ///
    /// This replaces a tool-name-granular check. The previous version accepted
    /// a child `Allow` whenever the parent held *any* `Allow`/`AskUser` rule
    /// for the same tool name, which meant a parent rule constrained to
    /// `{operation: "read"}` legitimised an unconstrained child `Allow` that
    /// reached `write`, and a parent `AskUser` silently became a child
    /// `Allow`. Both are widenings, and both were reachable from the shipped
    /// configuration.
    ///
    /// The legitimate need that escape hatch served — an unattended sub-agent
    /// must not stop to ask — is now served by an explicit
    /// [`DelegationGrant`], so the authority is declared and auditable rather
    /// than implied by a tool appearing in an allowlist.
    pub fn narrow_with_grants(
        parent: &Self,
        child: &Self,
        grants: &[DelegationGrant],
    ) -> Result<Self, PolicyError> {
        if !default_decision_covers(&parent.default_decision, &child.default_decision) {
            return Err(PolicyError::Widened(Arc::from("default")));
        }

        // Compare the two policies by *deciding*, not by comparing rules.
        //
        // A structural comparison has to re-derive what `decide` already
        // knows — that rules are first-match-wins, so an earlier rule can make
        // a later one unreachable — and getting that subtly wrong produces a
        // check that rejects correct configurations. An earlier draft of this
        // function did exactly that and was not even reflexive: some policies
        // failed to narrow to themselves.
        //
        // Instead: enumerate a finite set of calls that is complete for these
        // two policies, and ask both of them. Two calls that agree on every
        // (key, value) pair any rule mentions are indistinguishable to every
        // rule, so a representative of each equivalence class is enough.
        for witness in witnesses(parent, child)? {
            let child_decision = child.decide(&witness);
            let parent_decision = parent.decide(&witness);
            if decision_permissiveness(&child_decision) <= decision_permissiveness(&parent_decision)
            {
                continue;
            }
            if grants
                .iter()
                .any(|grant| grant.permits(&witness, &child_decision, parent))
            {
                continue;
            }
            return Err(PolicyError::Widened(witness_label(&witness)));
        }

        Ok(child.clone())
    }
}

/// Standing authority for one delegatee to hold a decision its parent does not.
///
/// The explicit form of what `parent_exposes_tool` used to do implicitly for
/// every sub-agent at once. A grant names exactly one action, may pin exact
/// argument values, and elevates only against the immediate parent — each
/// level of delegation needs its own grant, declared by the operator, so a
/// sub-agent cannot pass its authority further down.
///
/// Bounded expiry and durable issuance/use records are the half of
/// [ADR-0001](../../../../docs/adr/0001-POLICY_NARROWING.md) that needs the
/// safety-event store: `expires_at` is honoured here and optional, and M6 adds
/// issuance at runtime plus a record of every use. Until then a grant is a
/// standing authorization in `agent.toml`, which is at least visible, scoped,
/// and reviewable — none of which was true of the escape hatch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelegationGrant {
    /// The action this grant covers. `Any` is deliberately representable but
    /// should be rare: it grants across every tool.
    pub action: PolicyAction,
    /// The decision the delegatee may hold for `action`.
    pub decision: PolicyVerb,
    /// Exact argument values the grant is limited to. Empty means the grant
    /// covers every call to `action`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arg_equals: BTreeMap<Arc<str>, Value>,
    /// Why this authority exists. Required, because a grant nobody can explain
    /// is a grant nobody can review.
    pub reason: Arc<str>,
    /// Unix seconds after which the grant no longer applies. `None` is a
    /// standing grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl DelegationGrant {
    /// Whether this grant authorises `decision` for `call`.
    ///
    /// Four conditions, all necessary: the grant is unexpired; it matches this
    /// exact call, arguments included; it is at least as permissive as the
    /// decision it is being asked to justify; and the parent does not deny the
    /// call outright.
    ///
    /// The parent is not consulted for *authority* — the point of a grant is
    /// that the parent does not hold it — but an explicit parent `Deny`
    /// outranks a grant, so a grant cannot re-permit what an operator wrote a
    /// rule to stop.
    fn permits(&self, call: &Plan, decision: &PolicyDecision, parent: &Policy) -> bool {
        if self.is_expired(now_unix_seconds()) {
            return false;
        }
        if decision_permissiveness(decision) > permissiveness(&self.decision) {
            return false;
        }
        let tool_args = match call {
            Plan::CallTool(tool_call) => serde_json::from_str::<Value>(tool_call.args.get()).ok(),
            _ => None,
        };
        if !self.as_rule().matches(call, tool_args.as_ref()) {
            return false;
        }
        !matches!(parent.decide(call), PolicyDecision::Deny { .. })
    }

    fn is_expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|expiry| now >= expiry)
    }

    fn as_rule(&self) -> PolicyRule {
        PolicyRule {
            action: self.action.clone(),
            decision: self.decision.clone(),
            reason: None,
            arg_equals: self.arg_equals.clone(),
        }
    }
}

fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// A call name that appears in no rule, standing for "every other tool".
///
/// Without it, a child `Any` rule that is wider than the parent's would go
/// unnoticed whenever every *named* tool happened to be covered.
const UNNAMED_TOOL_WITNESS: &str = "\u{0}agentos.unnamed-tool-witness";

/// The cap on generated witnesses. Reached only by a policy naming many
/// distinct argument keys; refusing is the safe direction, and the error says
/// what to simplify.
const MAX_WITNESSES: usize = 4096;

/// A finite set of calls that distinguishes these two policies.
///
/// Rules match arguments by exact equality, so two calls that agree on every
/// (key, value) pair mentioned anywhere in either policy are treated
/// identically by both. Enumerating one representative per equivalence class
/// therefore decides the universal property exactly, not approximately.
fn witnesses(parent: &Policy, child: &Policy) -> Result<Vec<Plan>, PolicyError> {
    let rules = || parent.rules.iter().chain(child.rules.iter());

    let mut tools: BTreeSet<Arc<str>> = rules()
        .filter_map(|rule| match &rule.action {
            PolicyAction::Tool(name) => Some(Arc::clone(name)),
            _ => None,
        })
        .collect();
    tools.insert(Arc::from(UNNAMED_TOOL_WITNESS));

    // Every argument key any rule constrains, with the values it constrains it
    // to plus one value no rule names.
    let mut arg_space: BTreeMap<Arc<str>, BTreeSet<String>> = BTreeMap::new();
    for rule in rules() {
        for (key, value) in &rule.arg_equals {
            arg_space
                .entry(Arc::clone(key))
                .or_default()
                .insert(value.to_string());
        }
    }

    let mut combinations = tools.len();
    for values in arg_space.values() {
        combinations = combinations.saturating_mul(values.len() + 1);
        if combinations > MAX_WITNESSES {
            return Err(PolicyError::InvalidYaml {
                line: 0,
                message: Arc::from(
                    "policy constrains too many distinct argument keys to verify narrowing; \
                     reduce the number of `arg_equals` keys",
                ),
            });
        }
    }

    // The cross product of one value per constrained key, where `None` means
    // "a value no rule mentions".
    let keys: Vec<Arc<str>> = arg_space.keys().cloned().collect();
    let mut assignments: Vec<Vec<Option<Value>>> = vec![Vec::new()];
    for key in &keys {
        let mut values: Vec<Option<Value>> = vec![None];
        for encoded in &arg_space[key] {
            values.push(serde_json::from_str(encoded).ok());
        }
        assignments = assignments
            .into_iter()
            .flat_map(|prefix| {
                values.iter().map(move |value| {
                    let mut next = prefix.clone();
                    next.push(value.clone());
                    next
                })
            })
            .collect();
    }

    let mut plans = Vec::with_capacity(combinations + 3);
    for tool in &tools {
        for assignment in &assignments {
            let mut args = serde_json::Map::new();
            // A key absent and a key holding an unmentioned value are the
            // same to exact-equality matching, so `None` stands for both.
            for (key, value) in keys.iter().zip(assignment) {
                if let Some(value) = value {
                    args.insert(key.to_string(), value.clone());
                }
            }
            let encoded = Value::Object(args).to_string();
            let Ok(args) = RawValue::from_string(encoded) else {
                continue;
            };
            plans.push(Plan::CallTool(ToolCall {
                id: ToolCallId::new("policy-narrowing-witness"),
                name: Arc::clone(tool),
                args,
            }));
        }
    }
    plans.push(Plan::Handoff(
        AgentId::new("policy-narrowing-witness"),
        None,
    ));
    plans.push(Plan::Delegate(SubAgentSpec {
        agent_id: AgentId::new("policy-narrowing-witness"),
        policy_id: Arc::from("policy-narrowing-witness"),
        metadata: BTreeMap::new(),
    }));
    plans.push(Plan::Escalate(SubOrchSpec {
        template: OrchestratorTemplate {
            name: Arc::from("policy-narrowing-witness"),
            stages: Vec::new(),
        },
        task_id: TaskId::new("policy-narrowing-witness"),
        policy_id: Arc::from("policy-narrowing-witness"),
        metadata: BTreeMap::new(),
    }));
    Ok(plans)
}

/// How a refused witness is named in [`PolicyError::Widened`].
fn witness_label(plan: &Plan) -> Arc<str> {
    match plan {
        Plan::CallTool(call) if call.name.as_ref() == UNNAMED_TOOL_WITNESS => {
            Arc::from("any other tool")
        }
        Plan::CallTool(call) => Arc::clone(&call.name),
        Plan::Handoff(_, _) => Arc::from("handoff"),
        Plan::Delegate(_) => Arc::from("delegate"),
        Plan::Escalate(_) => Arc::from("escalate"),
        Plan::CallTools(_) | Plan::Reply(_) | Plan::ResumeSubAgent { .. } => Arc::from("action"),
    }
}

fn decision_permissiveness(decision: &PolicyDecision) -> u8 {
    match decision {
        PolicyDecision::Deny { .. } => 0,
        PolicyDecision::AskUser { .. } => 1,
        PolicyDecision::Allow => 2,
    }
}

/// `Allow` (2) is more permissive than `AskUser` (1) than `Deny` (0).
fn permissiveness(verb: &PolicyVerb) -> u8 {
    match verb {
        PolicyVerb::Deny => 0,
        PolicyVerb::AskUser => 1,
        PolicyVerb::Allow => 2,
    }
}

fn default_decision_covers(parent: &PolicyVerb, child: &PolicyVerb) -> bool {
    match child {
        PolicyVerb::Deny => true,
        PolicyVerb::AskUser => matches!(parent, PolicyVerb::Allow | PolicyVerb::AskUser),
        PolicyVerb::Allow => matches!(parent, PolicyVerb::Allow),
    }
}

impl PolicyRule {
    fn matches(&self, plan: &Plan, tool_args: Option<&Value>) -> bool {
        match (&self.action, plan) {
            (PolicyAction::Any, _) => self.args_match(tool_args),
            (PolicyAction::Tool(expected), Plan::CallTool(call)) if expected == &call.name => {
                self.args_match(tool_args)
            }
            (PolicyAction::Handoff, Plan::Handoff(_, _)) => true,
            (PolicyAction::Delegate, Plan::Delegate(_)) => true,
            (PolicyAction::Escalate, Plan::Escalate(_)) => true,
            (PolicyAction::Tool(_), _)
            | (PolicyAction::Handoff, _)
            | (PolicyAction::Delegate, _)
            | (PolicyAction::Escalate, _) => false,
        }
    }

    fn args_match(&self, tool_args: Option<&Value>) -> bool {
        if self.arg_equals.is_empty() {
            return true;
        }

        let Some(Value::Object(args)) = tool_args else {
            return false;
        };
        self.arg_equals
            .iter()
            .all(|(key, expected)| args.get(key.as_ref()) == Some(expected))
    }

    fn to_decision(&self) -> PolicyDecision {
        match self.decision {
            PolicyVerb::Allow => PolicyDecision::Allow,
            PolicyVerb::Deny => PolicyDecision::Deny {
                reason: self
                    .reason
                    .clone()
                    .unwrap_or_else(|| Arc::from("policy denied action")),
            },
            PolicyVerb::AskUser => PolicyDecision::AskUser {
                reason: self
                    .reason
                    .clone()
                    .unwrap_or_else(|| Arc::from("policy requires user approval")),
            },
        }
    }

    /// How this rule names its action in an error or an assertion. Already the
    /// payload of [`PolicyError::Widened`]; `pub(crate)` so X5's delegation
    /// invariant reports a violation the same way narrowing reports a refusal.
    pub(crate) fn label(&self) -> Arc<str> {
        match &self.action {
            PolicyAction::Any => Arc::from("any"),
            PolicyAction::Tool(name) => Arc::clone(name),
            PolicyAction::Handoff => Arc::from("handoff"),
            PolicyAction::Delegate => Arc::from("delegate"),
            PolicyAction::Escalate => Arc::from("escalate"),
        }
    }
}

fn default_policy_decision(verb: &PolicyVerb, plan: &Plan) -> PolicyDecision {
    match verb {
        PolicyVerb::Allow => PolicyDecision::Allow,
        PolicyVerb::Deny => PolicyDecision::Deny {
            reason: Arc::from(default_deny_reason(plan)),
        },
        PolicyVerb::AskUser => PolicyDecision::AskUser {
            reason: Arc::from("policy requires user approval"),
        },
    }
}

/// The more restrictive of two decisions: `Deny` beats `AskUser` beats `Allow`.
fn strictest(left: PolicyDecision, right: PolicyDecision) -> PolicyDecision {
    match (&left, &right) {
        (PolicyDecision::Deny { .. }, _) => left,
        (_, PolicyDecision::Deny { .. }) => right,
        (PolicyDecision::AskUser { .. }, _) => left,
        (_, PolicyDecision::AskUser { .. }) => right,
        (PolicyDecision::Allow, PolicyDecision::Allow) => left,
    }
}

fn default_deny_reason(plan: &Plan) -> String {
    match plan {
        Plan::Reply(_) => "reply is allowed".to_owned(),
        Plan::CallTool(call) => format!("tool '{}' is not allowed", call.name),
        Plan::CallTools(calls) => format!("a batch of {} tool calls is not allowed", calls.len()),
        Plan::Handoff(agent_id, _) => format!("handoff to '{}' is not allowed", agent_id.as_str()),
        Plan::Delegate(spec) => {
            format!("delegation to '{}' is not allowed", spec.agent_id.as_str())
        }
        Plan::Escalate(spec) => {
            format!("escalation to '{}' is not allowed", spec.template.name)
        }
        Plan::ResumeSubAgent { spec, .. } => {
            format!(
                "resuming sub-agent '{}' is not allowed",
                spec.agent_id.as_str()
            )
        }
    }
}

pub fn tool_call_approval_id(call: &ToolCall) -> Arc<str> {
    Arc::from(format!("approval-{}", call.id.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::ToolCallId;
    use serde_json::value::RawValue;

    fn tool_call(name: &str, args_json: &str) -> Plan {
        Plan::CallTool(ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from(name),
            args: RawValue::from_string(args_json.to_owned()).expect("valid JSON"),
        })
    }

    #[test]
    fn tool_has_arg_constraints_returns_false_when_no_rule_constrains_args() {
        let policy = Policy::allow_tools(["shell", "file"]);
        assert!(!policy.tool_has_arg_constraints(&Arc::from("shell")));
        assert!(!policy.tool_has_arg_constraints(&Arc::from("file")));
    }

    #[test]
    fn tool_has_arg_constraints_only_matches_constrained_tool() {
        let policy = Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("shell")),
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::new(),
                },
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("file")),
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
                },
            ],
            default_decision: PolicyVerb::Deny,
        };
        assert!(!policy.tool_has_arg_constraints(&Arc::from("shell")));
        assert!(policy.tool_has_arg_constraints(&Arc::from("file")));
        assert!(!policy.tool_has_arg_constraints(&Arc::from("http")));
    }

    #[test]
    fn tool_has_arg_constraints_handles_any_action() {
        let policy = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Any,
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("k"), Value::from("v"))]),
            }],
            default_decision: PolicyVerb::Deny,
        };
        assert!(policy.tool_has_arg_constraints(&Arc::from("anything")));
    }

    #[test]
    fn decide_skips_arg_parse_when_no_rule_needs_args() {
        let policy = Policy::allow_tools(["shell"]);
        let plan = tool_call("shell", "{\"command\":\"ls\"}");
        assert_eq!(policy.decide(&plan), PolicyDecision::Allow);
    }

    /// `shell` behind approval, `file` allowed only for reads — the smallest
    /// policy that exercises both a bare tool rule and an arg-constrained one.
    fn arg_constrained_policy() -> Policy {
        Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("shell")),
                    decision: PolicyVerb::AskUser,
                    reason: Some(Arc::from("shell tool requires user approval")),
                    arg_equals: BTreeMap::new(),
                },
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("file")),
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
                },
            ],
            default_decision: PolicyVerb::Deny,
        }
    }

    #[test]
    fn decide_matches_constrained_args() {
        let policy = arg_constrained_policy();
        let allow = tool_call("file", "{\"operation\":\"read\"}");
        assert_eq!(policy.decide(&allow), PolicyDecision::Allow);

        let deny = tool_call("file", "{\"operation\":\"write\"}");
        assert!(matches!(policy.decide(&deny), PolicyDecision::Deny { .. }));
    }

    /// The `AUTH-002` widening, now refused. A parent that asks about `shell`
    /// is stating that a human decides each call; a child `Allow` removes the
    /// human, and narrowing is what has to notice.
    #[test]
    fn narrow_refuses_a_child_allow_the_parent_only_asks_about() {
        let parent = Policy::ask_user_tools(["shell"]);
        let child = Policy::allow_tools(["shell"]);

        assert!(matches!(
            Policy::narrow(&parent, &child),
            Err(PolicyError::Widened(_))
        ));
    }

    /// The same pair, with the elevation declared. This is the whole trade the
    /// slice makes: the capability stays available, and it becomes visible.
    #[test]
    fn a_grant_admits_exactly_the_pair_narrowing_refused() {
        let parent = Policy::ask_user_tools(["shell"]);
        let child = Policy::allow_tools(["shell"]);
        let grants = vec![DelegationGrant {
            action: PolicyAction::Tool(Arc::from("shell")),
            decision: PolicyVerb::Allow,
            arg_equals: BTreeMap::new(),
            reason: Arc::from("unattended maintenance window"),
            expires_at: None,
        }];

        Policy::narrow_with_grants(&parent, &child, &grants)
            .expect("a declared grant admits the elevation");
    }

    /// A grant is not a skeleton key: it covers the action it names.
    #[test]
    fn a_grant_does_not_cover_a_different_tool() {
        let parent = Policy::ask_user_tools(["shell", "file"]);
        let child = Policy::allow_tools(["file"]);
        let grants = vec![DelegationGrant {
            action: PolicyAction::Tool(Arc::from("shell")),
            decision: PolicyVerb::Allow,
            arg_equals: BTreeMap::new(),
            reason: Arc::from("shell only"),
            expires_at: None,
        }];

        assert!(Policy::narrow_with_grants(&parent, &child, &grants).is_err());
    }

    /// Nor does a grant pinned to specific arguments justify an unconstrained
    /// rule — the same subset check narrowing itself applies.
    #[test]
    fn an_argument_pinned_grant_does_not_justify_an_unconstrained_rule() {
        let parent = Policy::ask_user_tools(["file"]);
        let child = Policy::allow_tools(["file"]);
        let grants = vec![DelegationGrant {
            action: PolicyAction::Tool(Arc::from("file")),
            decision: PolicyVerb::Allow,
            arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            reason: Arc::from("reads only"),
            expires_at: None,
        }];

        assert!(Policy::narrow_with_grants(&parent, &child, &grants).is_err());
    }

    /// A child that keeps the grant's constraint is admitted, so the pinned
    /// grant is usable rather than merely strict.
    #[test]
    fn an_argument_pinned_grant_admits_a_matching_rule() {
        let parent = Policy::ask_user_tools(["file"]);
        let child = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            }],
            default_decision: PolicyVerb::Deny,
        };
        let grants = vec![DelegationGrant {
            action: PolicyAction::Tool(Arc::from("file")),
            decision: PolicyVerb::Allow,
            arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            reason: Arc::from("reads only"),
            expires_at: None,
        }];

        Policy::narrow_with_grants(&parent, &child, &grants).expect("the pinned grant applies");
    }

    /// A child rule may be *more* constrained than the grant: the grant's call
    /// set contains the rule's, which is the direction that stays safe.
    #[test]
    fn a_child_may_be_stricter_than_its_grant() {
        let parent = Policy::ask_user_tools(["file"]);
        let child = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([
                    (Arc::from("operation"), serde_json::json!("read")),
                    (Arc::from("path"), serde_json::json!("README.md")),
                ]),
            }],
            default_decision: PolicyVerb::Deny,
        };
        let grants = vec![DelegationGrant {
            action: PolicyAction::Tool(Arc::from("file")),
            decision: PolicyVerb::Allow,
            arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            reason: Arc::from("reads only"),
            expires_at: None,
        }];

        Policy::narrow_with_grants(&parent, &child, &grants).expect("a stricter child is fine");
    }

    /// A parent rule constrained to one operation does not license a child
    /// rule that reaches every operation.
    #[test]
    fn narrow_refuses_a_child_that_drops_parent_argument_constraints() {
        let parent = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            }],
            default_decision: PolicyVerb::Deny,
        };
        let child = Policy::allow_tools(["file"]);

        assert!(Policy::narrow(&parent, &child).is_err());
    }

    /// A parent that allows everything by default covers a child rule for
    /// which it holds no explicit rule. Guards the fix against being
    /// "narrowing rejects whatever it cannot find".
    #[test]
    fn a_permissive_parent_default_covers_an_unmatched_child_rule() {
        let parent = Policy {
            rules: Vec::new(),
            default_decision: PolicyVerb::Allow,
        };
        let child = Policy::allow_tools(["shell"]);

        Policy::narrow(&parent, &child).expect("the parent allows everything by default");
    }

    /// A parent `Deny` aimed at one operation must not drag down the floor for
    /// a child rule that cannot reach that operation.
    #[test]
    fn a_disjoint_parent_deny_does_not_block_an_unrelated_child_rule() {
        let parent = Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("file")),
                    decision: PolicyVerb::Deny,
                    reason: None,
                    arg_equals: BTreeMap::from([(
                        Arc::from("operation"),
                        serde_json::json!("delete"),
                    )]),
                },
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("file")),
                    decision: PolicyVerb::Allow,
                    reason: None,
                    arg_equals: BTreeMap::new(),
                },
            ],
            default_decision: PolicyVerb::Deny,
        };
        let child = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("operation"), serde_json::json!("read"))]),
            }],
            default_decision: PolicyVerb::Deny,
        };

        Policy::narrow(&parent, &child).expect("the deny cannot match a read");
    }

    #[test]
    fn orchestrator_strategy_round_trips_through_u8() {
        use crate::runtime::OrchestratorStrategy;
        assert_eq!(
            OrchestratorStrategy::from_u8(OrchestratorStrategy::Max as u8),
            OrchestratorStrategy::Max
        );
        assert_eq!(
            OrchestratorStrategy::from_u8(OrchestratorStrategy::Min as u8),
            OrchestratorStrategy::Min
        );
        assert_eq!(
            OrchestratorStrategy::from_u8(255),
            OrchestratorStrategy::Max,
            "unknown bytes fall back to Max"
        );
    }
}
