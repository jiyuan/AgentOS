use super::subagents::SubAgentConfig;
use super::WorkspaceConfig;
use agentos_interfaces::orchestrator::{
    DispatchTarget, OrchestratorTemplate, RoutingRule, Stage, SubAgentSpec, TaskDomain,
};
use agentos_proto::AgentId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct RoutingConfig {
    pub rules: Vec<RoutingRuleConfig>,
    pub fallback: Option<RoutingRuleConfig>,
    /// When false, routing never spends an LLM classifier round-trip:
    /// deterministic routes still apply, and everything else goes to the
    /// fallback dispatch.
    pub llm_classifier: bool,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            fallback: None,
            llm_classifier: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct RoutingRuleConfig {
    pub domain: Arc<str>,
    pub description: Arc<str>,
    pub examples: Vec<Arc<str>>,
    pub dispatch: Arc<str>,
    pub template: Option<Arc<str>>,
    pub agent_id: Option<Arc<str>>,
    pub policy_id: Option<Arc<str>>,
}

impl Default for RoutingRuleConfig {
    fn default() -> Self {
        Self {
            domain: Arc::from("general"),
            description: Arc::from(""),
            examples: Vec::new(),
            dispatch: Arc::from("direct"),
            template: None,
            agent_id: None,
            policy_id: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct TemplateConfig {
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub developer_instructions: Arc<str>,
    pub stages: Vec<StageConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct StageConfig {
    pub name: Arc<str>,
    pub agent_id: Arc<str>,
    pub policy_id: Arc<str>,
    pub depends_on: Vec<Arc<str>>,
}

impl Default for StageConfig {
    fn default() -> Self {
        Self {
            name: Arc::from("stage"),
            agent_id: Arc::from("general-subagent"),
            policy_id: Arc::from("default"),
            depends_on: Vec::new(),
        }
    }
}

impl TemplateConfig {
    pub(super) fn to_template(
        &self,
        config: &WorkspaceConfig,
    ) -> Result<OrchestratorTemplate, String> {
        Ok(OrchestratorTemplate {
            name: Arc::clone(&self.name),
            stages: self
                .stages
                .iter()
                .map(|stage| {
                    Ok(Stage {
                        name: Arc::clone(&stage.name),
                        agent: SubAgentSpec {
                            agent_id: AgentId::new(Arc::clone(&stage.agent_id)),
                            policy_id: Arc::clone(&stage.policy_id),
                            metadata: config
                                .subagent_metadata(&stage.agent_id, &stage.policy_id)?,
                        },
                        depends_on: stage.depends_on.clone(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        })
    }
}

/// Topological execution order over stage dependency lists, generic over the
/// stage representation (`StageConfig` at load time, interface `Stage` at
/// escalation time). Returns the stage indices in a dependency-satisfying
/// order, or `Err` when dependencies are unsatisfied or cyclic — config
/// loading uses this to reject broken templates at startup instead of at the
/// first escalation.
pub(crate) fn stage_execution_order<S>(
    stages: &[S],
    name: impl Fn(&S) -> &Arc<str>,
    depends_on: impl Fn(&S) -> &[Arc<str>],
) -> Result<Vec<usize>, String> {
    let mut order = Vec::with_capacity(stages.len());
    let mut scheduled = vec![false; stages.len()];
    let mut completed: Vec<&Arc<str>> = Vec::with_capacity(stages.len());
    while order.len() < stages.len() {
        let next = stages.iter().enumerate().find(|(index, stage)| {
            !scheduled[*index]
                && depends_on(stage)
                    .iter()
                    .all(|dependency| completed.contains(&dependency))
        });
        let Some((index, stage)) = next else {
            return Err("unsatisfied or cyclic dependencies".to_owned());
        };
        scheduled[index] = true;
        completed.push(name(stage));
        order.push(index);
    }
    Ok(order)
}

pub(super) fn parse_domain(input: &str) -> TaskDomain {
    match input {
        "software_dev" | "software-dev" | "software" => TaskDomain::SoftwareDev,
        "content_ops" | "content-ops" | "content" => TaskDomain::ContentOps,
        "research" => TaskDomain::Research,
        "editing" | "edit" => TaskDomain::Editing,
        "general" => TaskDomain::General,
        other => TaskDomain::Custom(Arc::from(other)),
    }
}

pub(super) fn parse_dispatch(
    rule: &RoutingRuleConfig,
    templates: &BTreeMap<Arc<str>, OrchestratorTemplate>,
    config: &WorkspaceConfig,
) -> Result<DispatchTarget, String> {
    match rule.dispatch.as_ref() {
        "escalate" => {
            let template_name = rule.template.as_ref().ok_or_else(|| {
                format!(
                    "routing domain '{}' uses escalate without template",
                    rule.domain
                )
            })?;
            let template = templates.get(template_name).cloned().ok_or_else(|| {
                format!(
                    "routing domain '{}' references unknown template '{}'",
                    rule.domain, template_name
                )
            })?;
            Ok(DispatchTarget::Escalate(template))
        }
        "delegate" => {
            let agent_id = rule.agent_id.as_ref().ok_or_else(|| {
                format!(
                    "routing domain '{}' uses delegate without agent_id",
                    rule.domain
                )
            })?;
            let policy_id = rule.policy_id.as_ref().ok_or_else(|| {
                format!(
                    "routing domain '{}' uses delegate without policy_id",
                    rule.domain
                )
            })?;
            if !config.subagents.iter().any(|subagent: &SubAgentConfig| {
                subagent.id == *agent_id && subagent.policy_id == *policy_id
            }) {
                return Err(format!(
                    "routing domain '{}' delegates to unknown subagent '{}' with policy '{}'",
                    rule.domain, agent_id, policy_id
                ));
            }
            Ok(DispatchTarget::Delegate(SubAgentSpec {
                agent_id: AgentId::new(Arc::clone(agent_id)),
                policy_id: Arc::clone(policy_id),
                metadata: config.subagent_metadata(agent_id, policy_id)?,
            }))
        }
        "direct" => Ok(DispatchTarget::Direct),
        other => Err(format!(
            "routing domain '{}' has unknown dispatch '{}'",
            rule.domain, other
        )),
    }
}

pub(super) fn rule_from_config(
    rule: &RoutingRuleConfig,
    templates: &BTreeMap<Arc<str>, OrchestratorTemplate>,
    config: &WorkspaceConfig,
) -> Result<RoutingRule, String> {
    Ok(RoutingRule {
        domain: parse_domain(&rule.domain),
        description: Arc::clone(&rule.description),
        examples: rule.examples.clone(),
        dispatch: parse_dispatch(rule, templates, config)?,
    })
}

#[cfg(test)]
mod tests {
    use super::super::WorkspaceConfig;
    use super::{stage_execution_order, StageConfig};
    use std::sync::Arc;

    fn stage(name: &str, depends_on: &[&str]) -> StageConfig {
        StageConfig {
            name: Arc::from(name),
            depends_on: depends_on.iter().map(|dep| Arc::from(*dep)).collect(),
            ..StageConfig::default()
        }
    }

    #[test]
    fn stage_execution_order_respects_dependencies() {
        // Diamond: a → (b, c) → d, listed out of order.
        let stages = [
            stage("d", &["b", "c"]),
            stage("b", &["a"]),
            stage("c", &["a"]),
            stage("a", &[]),
        ];
        let order = stage_execution_order(&stages, |s| &s.name, |s| &s.depends_on)
            .expect("diamond resolves");
        let position = |name: &str| {
            order
                .iter()
                .position(|&index| stages[index].name.as_ref() == name)
                .expect("stage scheduled")
        };
        assert!(position("a") < position("b"));
        assert!(position("a") < position("c"));
        assert!(position("b") < position("d"));
        assert!(position("c") < position("d"));
    }

    #[test]
    fn cyclic_template_is_rejected_at_config_load() {
        let config: WorkspaceConfig = toml::from_str(
            r#"
[[orchestrator_templates]]
name = "cyclic"

[[orchestrator_templates.stages]]
name = "a"
depends_on = ["b"]

[[orchestrator_templates.stages]]
name = "b"
depends_on = ["a"]
"#,
        )
        .expect("config parses");
        let error = config
            .validate_orchestrator_templates()
            .expect_err("cycle must be rejected");
        assert!(
            error.contains("cyclic") && error.contains("'cyclic'"),
            "error should name the template and the problem, got: {error}"
        );
    }

    #[test]
    fn llm_classifier_defaults_to_enabled_and_parses_when_disabled() {
        let default_config: WorkspaceConfig = toml::from_str("").expect("empty config parses");
        assert!(default_config.routing.llm_classifier);

        let disabled: WorkspaceConfig =
            toml::from_str("[routing]\nllm_classifier = false\n").expect("flag parses");
        assert!(!disabled.routing.llm_classifier);
    }
}
