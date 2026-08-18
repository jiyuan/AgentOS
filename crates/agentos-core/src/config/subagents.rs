use super::normalize::{normalize_config_token, normalize_domain};
use agentos_proto::{DelegationGrantScope, DELEGATION_GRANT_SCOPES_KEY, DELEGATION_GRANT_TTL_KEY};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct SubAgentConfig {
    pub name: Arc<str>,
    pub id: Arc<str>,
    pub description: Arc<str>,
    pub developer_instructions: Arc<str>,
    pub policy_id: Arc<str>,
    pub orchestrator: Arc<str>,
    pub model_tier: Arc<str>,
    pub tools: Vec<Arc<str>>,
    /// Exact tool-call regions that may become unattended only after the
    /// delegation itself is approved by an authorized principal. These are
    /// separate from the child policy and never participate in
    /// `Policy::narrow`.
    pub delegation_grants: Vec<DelegationGrantConfig>,
    /// Lifetime of a grant issued from `delegation_grants`, in seconds.
    /// Runtime validation requires 1..=3600 seconds.
    pub delegation_grant_ttl_secs: u64,
    /// Skills (by name) this sub-agent is permitted to dispatch. Each entry
    /// must also appear in the parent runtime's `resources.skills.enabled`
    /// list — unknown names are silently dropped at build time. Skill access
    /// is opt-in: an empty vector means the sub-agent cannot dispatch any
    /// skill, even if the parent has them loaded.
    pub skills: Vec<Arc<str>>,
    pub memory_view: Arc<str>,
    pub memory_domains: Vec<Arc<str>>,
    pub memory_tools: Vec<Arc<str>>,
    pub max_turns: usize,
    pub inherit_guardrails: bool,
    /// Opt-in: permits this sub-agent to write inside the skill-bundle
    /// directory. Defaults to `false`, so every sub-agent is blocked from
    /// tampering with `SKILL.md` bundles by the skill-bundle write guardrail
    /// unless it is the designated skill editor. This is a permission grant,
    /// not a convenience toggle — set it only on the dedicated skill editor.
    pub skill_bundle_writer: bool,
    /// Seed this sub-agent's conversation from the parent's history the first
    /// time it is delegated to (roadmap X6), instead of starting it empty.
    ///
    /// Off by default, and the default is the conservative one. A sub-agent
    /// exists to work a bounded task under a narrowed policy; handing it the
    /// whole parent conversation costs tokens on every turn it takes and shows
    /// a possibly weaker model everything the parent has seen. Turn it on for
    /// the sub-agent that needs the discussion so far — a reviewer, an editor,
    /// a second opinion — not for one that fetches a URL.
    ///
    /// Seeding happens once. A sub-agent's conversation id is stable across a
    /// conversation, so the second and every later delegation find history
    /// already there and leave it alone.
    pub seed_from_parent: bool,
    /// Character cap for `MaxOutputLength` when `inherit_guardrails = true`.
    /// Tripped output aborts the run, so this needs to comfortably exceed any
    /// reply you expect from the model. Defaults are tuned for chat: long
    /// enough to fit a thorough multi-paragraph answer, short enough to catch
    /// runaway generation.
    pub max_output_chars: usize,
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self {
            name: Arc::from("research-subagent"),
            id: Arc::from("research-subagent"),
            description: Arc::from(""),
            developer_instructions: Arc::from(""),
            policy_id: Arc::from("readonly-web"),
            orchestrator: Arc::from("builtin.max"),
            model_tier: Arc::from("medium"),
            tools: vec![Arc::from("http")],
            delegation_grants: Vec::new(),
            delegation_grant_ttl_secs: 300,
            skills: Vec::new(),
            memory_view: Arc::from("none"),
            memory_domains: Vec::new(),
            memory_tools: Vec::new(),
            max_turns: 4,
            inherit_guardrails: true,
            skill_bundle_writer: false,
            seed_from_parent: false,
            max_output_chars: 64_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct DelegationGrantConfig {
    /// Exact tool name whose constrained calls the grant may authorize.
    pub tool: Arc<str>,
    /// Required equality constraints over top-level tool arguments. At least
    /// one constraint is mandatory so a grant cannot become a blanket tool
    /// allowlist under a different name.
    pub arg_equals: BTreeMap<Arc<str>, Value>,
}

pub(super) fn normalize_memory_view(input: &str) -> Result<String, String> {
    match normalize_config_token(input).as_str() {
        "none" | "" => Ok("none".to_owned()),
        "shared_readonly" | "shared_read_only" => Ok("shared_readonly".to_owned()),
        "shared_readwrite" | "shared_read_write" => Ok("shared_readwrite".to_owned()),
        other => Err(format!(
            "unknown subagent memory_view '{other}'; expected none, shared_readonly, or shared_readwrite"
        )),
    }
}

pub(super) fn normalize_memory_tool(input: &str) -> Result<String, String> {
    match normalize_config_token(input).as_str() {
        "read" | "write" | "forget" => Ok(normalize_config_token(input)),
        other => Err(format!(
            "unknown subagent memory_tools entry '{other}'; expected read, write, or forget"
        )),
    }
}

pub(super) fn subagent_metadata(
    subagent: &SubAgentConfig,
) -> Result<BTreeMap<Arc<str>, Value>, String> {
    let memory_view = normalize_memory_view(&subagent.memory_view)?;
    let mut metadata = descriptive_subagent_metadata(subagent);
    if memory_view == "none" {
        if !subagent.memory_domains.is_empty() {
            return Err(format!(
                "subagent '{}' sets memory_domains without enabling memory_view",
                subagent.id
            ));
        }
        return Ok(metadata);
    }

    let memory_domains = subagent
        .memory_domains
        .iter()
        .map(|domain| normalize_domain(domain, "subagents.memory_domains"))
        .collect::<Result<Vec<_>, _>>()?;
    let memory_tools = subagent
        .memory_tools
        .iter()
        .map(|tool| normalize_memory_tool(tool))
        .collect::<Result<Vec<_>, _>>()?;

    metadata.insert(Arc::from("memory_view"), Value::String(memory_view));
    metadata.insert(
        Arc::from("memory_default_owner"),
        Value::String("agent".to_owned()),
    );
    if !memory_domains.is_empty() {
        metadata.insert(
            Arc::from("memory_domains"),
            Value::Array(
                memory_domains
                    .iter()
                    .map(|domain| Value::String(domain.to_string()))
                    .collect(),
            ),
        );
    }
    if !memory_tools.is_empty() {
        metadata.insert(
            Arc::from("memory_tools"),
            Value::Array(memory_tools.into_iter().map(Value::String).collect()),
        );
    }
    Ok(metadata)
}

fn descriptive_subagent_metadata(subagent: &SubAgentConfig) -> BTreeMap<Arc<str>, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from("subagent_name"),
        Value::String(subagent.name.to_string()),
    );
    metadata.insert(
        Arc::from("subagent_description"),
        Value::String(subagent.description.to_string()),
    );
    metadata.insert(
        Arc::from("subagent_tools"),
        Value::Array(
            subagent
                .tools
                .iter()
                .map(|tool| Value::String(tool.to_string()))
                .collect(),
        ),
    );
    metadata.insert(
        Arc::from("subagent_skills"),
        Value::Array(
            subagent
                .skills
                .iter()
                .map(|skill| Value::String(skill.to_string()))
                .collect(),
        ),
    );
    metadata.insert(
        Arc::from("subagent_max_turns"),
        Value::from(subagent.max_turns as u64),
    );
    if !subagent.delegation_grants.is_empty() {
        let scopes = subagent
            .delegation_grants
            .iter()
            .map(|grant| DelegationGrantScope {
                tool: Arc::clone(&grant.tool),
                arg_equals: grant.arg_equals.clone(),
            })
            .collect::<Vec<_>>();
        metadata.insert(
            Arc::from(DELEGATION_GRANT_SCOPES_KEY),
            serde_json::to_value(scopes).expect("delegation grant scopes are serializable"),
        );
        metadata.insert(
            Arc::from(DELEGATION_GRANT_TTL_KEY),
            Value::from(subagent.delegation_grant_ttl_secs),
        );
    }
    metadata
}
