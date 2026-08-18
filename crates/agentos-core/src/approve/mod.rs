use agentos_interfaces::orchestrator::Plan;
use agentos_proto::ToolCall;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
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

    pub fn narrow(parent: &Self, child: &Self) -> Result<Self, PolicyError> {
        if parent == child {
            return Ok(child.clone());
        }

        // Rules form ordered decision lists. A child rule is safe only when
        // one parent rule covers its complete action/argument region and no
        // earlier overlapping parent rule is stricter. If no rule covers the
        // region, every overlapping parent rule and the parent default must
        // still be at least as permissive. This is conservative by design: a
        // relationship the equality-constraint language cannot prove is a
        // startup error, never an implicit delegation grant.
        for child_rule in &child.rules {
            if matches!(child_rule.decision, PolicyVerb::Deny) {
                continue;
            }

            let mut covered = false;
            for parent_rule in &parent.rules {
                if !parent_rule.overlaps(child_rule) {
                    continue;
                }
                if !decision_covers(&parent_rule.decision, &child_rule.decision) {
                    return Err(PolicyError::Widened(child_rule.label()));
                }
                if parent_rule.covers(child_rule) {
                    covered = true;
                    break;
                }
            }

            if !covered && !decision_covers(&parent.default_decision, &child_rule.decision) {
                return Err(PolicyError::Widened(child_rule.label()));
            }
        }

        // The child default reaches everything no child rule matches. A
        // stricter parent rule is safe only when one child predicate excludes
        // its complete region from that default.
        if !decision_covers(&parent.default_decision, &child.default_decision) {
            return Err(PolicyError::Widened(Arc::from("default")));
        }
        if !matches!(child.default_decision, PolicyVerb::Deny) {
            for parent_rule in &parent.rules {
                if decision_covers(&parent_rule.decision, &child.default_decision) {
                    continue;
                }
                let excluded_from_child_default = child
                    .rules
                    .iter()
                    .any(|child_rule| child_rule.covers(parent_rule));
                if !excluded_from_child_default {
                    return Err(PolicyError::Widened(Arc::from("default")));
                }
            }
        }

        Ok(child.clone())
    }
}

fn decision_covers(parent: &PolicyVerb, child: &PolicyVerb) -> bool {
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

    fn covers(&self, child: &Self) -> bool {
        match (&self.action, &child.action) {
            (PolicyAction::Any, PolicyAction::Handoff)
            | (PolicyAction::Any, PolicyAction::Delegate)
            | (PolicyAction::Any, PolicyAction::Escalate) => {
                return self.arg_equals.is_empty();
            }
            (PolicyAction::Any, PolicyAction::Any | PolicyAction::Tool(_)) => {}
            (PolicyAction::Tool(parent), PolicyAction::Tool(child)) if parent == child => {}
            (PolicyAction::Handoff, PolicyAction::Handoff)
            | (PolicyAction::Delegate, PolicyAction::Delegate)
            | (PolicyAction::Escalate, PolicyAction::Escalate) => return true,
            _ => return false,
        }

        self.arg_equals
            .iter()
            .all(|(key, value)| child.arg_equals.get(key) == Some(value))
    }

    fn overlaps(&self, other: &Self) -> bool {
        let actions_overlap = match (&self.action, &other.action) {
            (PolicyAction::Any, PolicyAction::Handoff)
            | (PolicyAction::Any, PolicyAction::Delegate)
            | (PolicyAction::Any, PolicyAction::Escalate) => self.arg_equals.is_empty(),
            (PolicyAction::Handoff, PolicyAction::Any)
            | (PolicyAction::Delegate, PolicyAction::Any)
            | (PolicyAction::Escalate, PolicyAction::Any) => other.arg_equals.is_empty(),
            (PolicyAction::Any, PolicyAction::Any | PolicyAction::Tool(_))
            | (PolicyAction::Tool(_), PolicyAction::Any) => true,
            (PolicyAction::Tool(left), PolicyAction::Tool(right)) => left == right,
            (PolicyAction::Handoff, PolicyAction::Handoff)
            | (PolicyAction::Delegate, PolicyAction::Delegate)
            | (PolicyAction::Escalate, PolicyAction::Escalate) => true,
            (PolicyAction::Tool(_), _)
            | (PolicyAction::Handoff, _)
            | (PolicyAction::Delegate, _)
            | (PolicyAction::Escalate, _) => false,
        };
        if !actions_overlap {
            return false;
        }
        if matches!(
            (&self.action, &other.action),
            (PolicyAction::Handoff, PolicyAction::Handoff)
                | (PolicyAction::Delegate, PolicyAction::Delegate)
                | (PolicyAction::Escalate, PolicyAction::Escalate)
        ) {
            return true;
        }
        self.arg_equals
            .iter()
            .all(|(key, left)| other.arg_equals.get(key).is_none_or(|right| right == left))
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

    #[test]
    fn narrow_rejects_child_allow_when_parent_would_ask_user() {
        let parent = Policy::ask_user_tools(["shell"]);
        let child = Policy::allow_tools(["shell"]);

        assert!(
            Policy::narrow(&parent, &child).is_err(),
            "delegation cannot turn attended authority into unattended authority"
        );
    }

    #[test]
    fn narrow_rejects_child_rule_that_drops_parent_argument_constraints() {
        let parent = Policy {
            rules: vec![PolicyRule {
                action: PolicyAction::Tool(Arc::from("file")),
                decision: PolicyVerb::Allow,
                reason: None,
                arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
            }],
            default_decision: PolicyVerb::Deny,
        };
        let child = Policy::allow_tools(["file"]);

        assert!(
            Policy::narrow(&parent, &child).is_err(),
            "a read-only parent cannot delegate unconstrained file authority"
        );
    }

    #[test]
    fn narrow_rejects_child_rule_overlapping_an_earlier_parent_deny() {
        let parent = Policy {
            rules: vec![
                PolicyRule {
                    action: PolicyAction::Tool(Arc::from("file")),
                    decision: PolicyVerb::Deny,
                    reason: None,
                    arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("write"))]),
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
        let child = Policy::allow_tools(["file"]);

        assert!(
            Policy::narrow(&parent, &child).is_err(),
            "a later broad allow cannot cover calls denied by an earlier rule"
        );
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
