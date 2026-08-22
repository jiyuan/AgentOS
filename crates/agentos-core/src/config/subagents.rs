use super::normalize::{normalize_config_token, normalize_domain};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct SubAgentConfig {
    /// Human-readable name, for logs and the resource catalog.
    pub name: Arc<str>,
    /// The agent id a `Plan::Delegate` names. Together with `policy_id` it
    /// identifies one delegatee, and it becomes one path segment for the
    /// sub-agent's task workspace — so it is validated as a single segment
    /// rather than rewritten.
    pub id: Arc<str>,
    /// What this sub-agent is for, as the parent's model reads it when
    /// deciding whether to delegate.
    pub description: Arc<str>,
    /// The standing brief prepended to this sub-agent's own context on every
    /// delegation.
    pub developer_instructions: Arc<str>,
    /// Which policy this entry defines. One sub-agent id may appear under
    /// several policy ids; each pair is a separate delegatee with separate
    /// permissions.
    pub policy_id: Arc<str>,
    /// Which orchestrator drives the child loop: `builtin.max` or
    /// `builtin.min`.
    pub orchestrator: Arc<str>,
    /// Which model this sub-agent calls: `high`, `medium`, or `low`. A
    /// bounded task under a narrowed policy is often the place to spend less.
    pub model_tier: Arc<str>,
    /// Tools this sub-agent may call, by name. Listing a tool *selects* it; it
    /// does not elevate it — see `delegation_grants`.
    pub tools: Vec<Arc<str>>,
    /// Skills (by name) this sub-agent is permitted to dispatch. Each entry
    /// must also appear in the parent runtime's `resources.skills.enabled`
    /// list — unknown names are silently dropped at build time. Skill access
    /// is opt-in: an empty vector means the sub-agent cannot dispatch any
    /// skill, even if the parent has them loaded.
    pub skills: Vec<Arc<str>>,
    /// How much memory this sub-agent sees: `none`, `own` (its own scopes
    /// only), `shared_readonly`, or `shared_readwrite`. Only the last permits
    /// a shared-domain write, and only for domains the deployment already
    /// allows (M7 / `MEM-001`).
    pub memory_view: Arc<str>,
    /// Which shared domains this sub-agent names, for the two `shared_*`
    /// views. Intersected with `[[memory.shared_domains]]`: naming a domain
    /// the deployment does not share grants nothing.
    pub memory_domains: Vec<Arc<str>>,
    /// Which `memory` tool operations this sub-agent may call: any of `read`,
    /// `write`, `forget`. Empty means it gets no memory tool at all.
    pub memory_tools: Vec<Arc<str>>,
    /// Tool cycles this sub-agent's own run may take. Exhausting it ends the
    /// child with a partial answer rather than failing the parent.
    pub max_turns: usize,
    /// Whether the parent runtime's input, tool, and output guardrails also
    /// apply to this sub-agent's run.
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
    /// Authority this sub-agent holds that its parent does not.
    ///
    /// Naming a tool in `tools` does **not** grant it: the sub-agent's policy
    /// is narrowed against the parent's over exact actions and arguments, so a
    /// tool the parent gates behind `ask_user` stays gated for the sub-agent
    /// too. That is the point — an unattended sub-agent silently inheriting a
    /// blanket `allow` for every tool it listed was the `AUTH-002` finding.
    ///
    /// A grant is how the legitimate case is expressed instead: state the
    /// tool, the decision, and why, and the elevation becomes visible and
    /// reviewable. Grants apply only against the immediate parent — each level
    /// of delegation needs its own.
    pub delegation_grants: Vec<DelegationGrantConfig>,
}

/// One standing elevation for one sub-agent and one tool.
///
/// Deliberately limited to tools. A grant over `handoff`, `delegate`, or
/// `escalate` — let alone every action at once — has no current use case, and
/// the narrow surface is the safe direction to be wrong in.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DelegationGrantConfig {
    /// The tool this grant covers.
    pub tool: Arc<str>,
    /// What the sub-agent may do with it: `allow` or `ask_user`. `deny` is
    /// rejected — a grant exists to widen, and narrowing needs no grant.
    pub decision: Arc<str>,
    /// Exact argument values the grant is limited to, matching `[policy]`
    /// rule syntax. Empty covers every call to the tool.
    #[serde(default)]
    pub arg_equals: BTreeMap<Arc<str>, Value>,
    /// Why this sub-agent needs authority its parent withheld. Required: a
    /// grant nobody can explain is a grant nobody can review.
    pub reason: Arc<str>,
    /// Unix seconds after which the grant stops applying. Omit for a standing
    /// grant.
    #[serde(default)]
    pub expires_at: Option<u64>,
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
            skills: Vec::new(),
            memory_view: Arc::from("none"),
            memory_domains: Vec::new(),
            memory_tools: Vec::new(),
            max_turns: 4,
            inherit_guardrails: true,
            skill_bundle_writer: false,
            seed_from_parent: false,
            max_output_chars: 64_000,
            delegation_grants: Vec::new(),
        }
    }
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
    metadata
}
