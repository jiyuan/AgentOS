mod yaml;

use agentos_interfaces::orchestrator::Plan;
use agentos_proto::ToolCall;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use yaml::parse_policy_yaml;

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

    pub fn phase3_reference() -> Self {
        Self {
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

    pub fn phase4_reference() -> Self {
        let mut policy = Self::phase3_reference();
        policy.rules.extend(memory_policy_rules());
        policy
    }

    pub fn from_yaml(input: &str) -> Result<Self, PolicyError> {
        parse_policy_yaml(input)
    }

    pub fn decide(&self, plan: &Plan) -> PolicyDecision {
        if matches!(plan, Plan::Reply(_) | Plan::ResumeSubAgent { .. }) {
            return PolicyDecision::Allow;
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
        if !default_decision_covers(&parent.default_decision, &child.default_decision) {
            return Err(PolicyError::Widened(Arc::from("default")));
        }

        for child_rule in &child.rules {
            match child_rule.decision {
                PolicyVerb::Allow => {
                    // A child `Allow` is valid if the parent already exposes
                    // that exact action (the normal `covers` check) OR — for
                    // tools — if the parent governs the same tool at all with
                    // an Allow/AskUser rule. The latter lets an explicit
                    // sub-agent tool allowlist suppress approval prompts for a
                    // tool the parent itself can use, without letting the
                    // sub-agent reach tools the parent denies or never grants.
                    let covered = parent.rules.iter().any(|parent_rule| {
                        matches!(
                            parent_rule.decision,
                            PolicyVerb::Allow | PolicyVerb::AskUser
                        ) && parent_rule.covers(child_rule)
                    });
                    if !covered && !parent_exposes_tool(parent, child_rule) {
                        return Err(PolicyError::Widened(child_rule.label()));
                    }
                }
                PolicyVerb::AskUser => {
                    if !parent.rules.iter().any(|parent_rule| {
                        matches!(
                            parent_rule.decision,
                            PolicyVerb::Allow | PolicyVerb::AskUser
                        ) && parent_rule.covers(child_rule)
                    }) {
                        return Err(PolicyError::Widened(child_rule.label()));
                    }
                }
                PolicyVerb::Deny => {}
            }
        }

        Ok(child.clone())
    }
}

fn memory_policy_rules() -> Vec<PolicyRule> {
    vec![
        PolicyRule {
            action: PolicyAction::Tool(Arc::from("memory")),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("read"))]),
        },
        PolicyRule {
            action: PolicyAction::Tool(Arc::from("memory")),
            decision: PolicyVerb::AskUser,
            reason: Some(Arc::from("memory write requires user approval")),
            arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("write"))]),
        },
        PolicyRule {
            action: PolicyAction::Tool(Arc::from("memory")),
            decision: PolicyVerb::AskUser,
            reason: Some(Arc::from("memory forget requires user approval")),
            arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from("forget"))]),
        },
    ]
}

/// True when `child_rule` targets a tool the parent already exposes to this
/// delegatee (any same-tool rule whose decision is `Allow` or `AskUser`).
///
/// This is the mechanism behind "an explicitly allowlisted sub-agent tool
/// needs no approval": the parent may gate the tool behind `AskUser` (or
/// per-operation arg constraints) for itself, but a sub-agent that names the
/// tool in its allowlist gets blanket `Allow`. Tools the parent denies or
/// never grants are not exposed, so the sub-agent still cannot reach them.
fn parent_exposes_tool(parent: &Policy, child_rule: &PolicyRule) -> bool {
    let PolicyAction::Tool(child_tool) = &child_rule.action else {
        return false;
    };
    parent.rules.iter().any(|rule| {
        matches!(rule.decision, PolicyVerb::Allow | PolicyVerb::AskUser)
            && matches!(&rule.action, PolicyAction::Tool(tool) if tool == child_tool)
    })
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

    fn covers(&self, child: &Self) -> bool {
        match (&self.action, &child.action) {
            (PolicyAction::Any, _) => {}
            (PolicyAction::Tool(parent), PolicyAction::Tool(child)) if parent == child => {}
            (PolicyAction::Handoff, PolicyAction::Handoff)
            | (PolicyAction::Delegate, PolicyAction::Delegate)
            | (PolicyAction::Escalate, PolicyAction::Escalate) => {}
            _ => return false,
        }

        self.arg_equals
            .iter()
            .all(|(key, value)| child.arg_equals.get(key) == Some(value))
    }

    fn label(&self) -> Arc<str> {
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

fn default_deny_reason(plan: &Plan) -> String {
    match plan {
        Plan::Reply(_) => "reply is allowed".to_owned(),
        Plan::CallTool(call) => format!("tool '{}' is not allowed", call.name),
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

    #[test]
    fn decide_matches_constrained_args() {
        let policy = Policy::phase3_reference();
        let allow = tool_call("file", "{\"operation\":\"read\"}");
        assert_eq!(policy.decide(&allow), PolicyDecision::Allow);

        let deny = tool_call("file", "{\"operation\":\"write\"}");
        assert!(matches!(policy.decide(&deny), PolicyDecision::Deny { .. }));
    }

    #[test]
    fn narrow_allows_child_allow_when_parent_would_ask_user() {
        let parent = Policy::ask_user_tools(["shell"]);
        let child = Policy::allow_tools(["shell"]);

        Policy::narrow(&parent, &child)
            .expect("delegated child allowlist may allow a parent-ask tool");
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
