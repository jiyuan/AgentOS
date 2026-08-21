use crate::skills::WorkspaceSkillMetadata;
use agentos_interfaces::orchestrator::{
    DispatchPriority, OrchestratorTemplate, ResourceEntry, ResourceIndex, ResourceKind,
    RoutingRule, RoutingTable, TaskDomain,
};
use agentos_interfaces::tool::{SandboxMode, ToolSpec};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod approval;
pub mod catalog;
mod compaction;
mod gateway;
mod jobs;
mod limits;
mod memory;
mod normalize;
mod orchestrator;
mod spill;
mod subagents;

pub use approval::ApprovalConfig;
pub use compaction::CompactionConfig;
pub use gateway::GatewayConfig;
pub use jobs::JobsConfig;
pub use limits::LimitsConfig;
pub use memory::{
    MemoryConfig, MemoryPolicyConfig, MemoryQdrantConfig, MemoryReflectionConfig,
    MemoryRetentionConfig, MemorySharedDomainConfig, MemorySqliteVecConfig,
};
pub use orchestrator::{RoutingConfig, RoutingRuleConfig, StageConfig, TemplateConfig};
pub use spill::{SpillConfig, DEFAULT_SPILL_RELPATH};
pub use subagents::SubAgentConfig;

pub(crate) use orchestrator::stage_execution_order;

use normalize::normalize_domain;
use orchestrator::rule_from_config;
use subagents::{normalize_memory_tool, normalize_memory_view, subagent_metadata};

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct WorkspaceConfig {
    /// Which orchestrator, memory backend, and turn budget this agent runs on.
    pub agent: AgentConfig,
    /// The authorization default and the tools exempt from it.
    pub policy: PolicyConfig,
    /// Content checks applied to input, tool calls, and output.
    pub guardrails: GuardrailsConfig,
    /// Long-term memory: storage, what gets recalled into a request, and what
    /// a run is allowed to write back.
    pub memory: MemoryConfig,
    /// Which channels this deployment answers on, and in what mode.
    pub channels: ChannelsConfig,
    /// Where the subprocess worker that runs sandboxed tools is found.
    pub isolation: IsolationConfig,
    /// Sub-agents this agent may delegate to. Each carries its own tools and a
    /// policy that can only narrow the parent's.
    pub subagents: Vec<SubAgentConfig>,
    /// MCP servers to connect to.
    pub mcp_servers: Vec<McpServerConfig>,
    /// Tools served by a static MCP server, declared inline.
    pub mcp_tools: Vec<McpToolConfig>,
    /// Which skills, tools, MCP tools, and LLM entries are enabled, and in what
    /// order they are offered to the model.
    pub resources: ResourceConfig,
    /// How inbound work is classified and where each class is dispatched.
    pub routing: RoutingConfig,
    /// Multi-stage sub-orchestrator templates a run can escalate into.
    pub orchestrator_templates: Vec<TemplateConfig>,
    /// Where per-task scratch directories are created.
    pub task_workspace: TaskWorkspaceConfig,
    /// Sizes and deadlines a deployment has real reason to change.
    pub limits: LimitsConfig,
    /// When a conversation summarizes its own history, and with which model.
    pub compaction: CompactionConfig,
    /// Bounds on background work promoted out of a tool deadline.
    pub jobs: JobsConfig,
    /// How the persistent gateway spreads conversations over threads.
    pub gateway: GatewayConfig,
    /// How long an approval prompt stays answerable.
    pub approval: ApprovalConfig,
    /// Where oversized tool output is written, and how long it is kept.
    pub spill: SpillConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct AgentConfig {
    pub id: Arc<str>,
    pub orchestrator: Arc<str>,
    pub memory: Arc<str>,
    pub max_turns: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            id: Arc::from("default"),
            orchestrator: Arc::from("builtin.max"),
            memory: Arc::from("memory.in_memory"),
            max_turns: 16,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct PolicyConfig {
    pub default: Arc<str>,
    pub allowlist: Vec<Arc<str>>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            default: Arc::from("deny"),
            allowlist: Vec::new(),
        }
    }
}

/// Programs the shell tool guardrail accepts when `agent.toml` declares no
/// `[guardrails]` section. Deliberately limited to read-only inspection
/// commands; widen it explicitly through `[guardrails] shell_allowlist`.
pub const DEFAULT_SHELL_ALLOWLIST: [&str; 8] =
    ["printf", "echo", "pwd", "ls", "find", "cat", "head", "tail"];

/// A program in the shell allowlist whose *arguments* the guardrail also
/// checks.
///
/// The allowlist alone matches on the program name, which is enough for a
/// command that can only inspect. It is not enough for a command that can be
/// argued into running something else: `python3 -c "<payload>"` and
/// `find . -exec sh -c "<payload>" \;` both clear a program-name check while
/// being arbitrary code execution. A profile names the shape of call that is
/// actually intended, and refuses the rest.
///
/// The two constraints are deliberately different mechanisms, because the two
/// escapes are:
///
/// - `require_first_arg_suffix` is an allowlist, and it is the right tool for
///   an interpreter. Once `python3`'s first argument is a script path, every
///   later argument belongs to the script rather than to the interpreter, so
///   pinning that one position closes `-c`, `-m`, `-i`, a bare `-`, and the
///   short-flag clusters (`-Bc`) that an exact-match denylist would miss.
/// - `deny_args` is a denylist, for a command whose flags do not cluster and
///   where only a handful of them are dangerous. `find` is the shipped case:
///   its actions are whole tokens, so naming them is exact.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShellProfileConfig {
    /// Program this profile governs, as a bare name matching the call's
    /// `command` field. A profile also admits its program, so a program named
    /// here need not repeat itself in `shell_allowlist`; when it appears in
    /// both, the profile still applies.
    pub program: Arc<str>,
    /// When non-empty, the first entry of the structured args array must end
    /// with one of these suffixes. Pins an interpreter to a script file.
    #[serde(default)]
    pub require_first_arg_suffix: Vec<Arc<str>>,
    /// Arguments refused outright, compared literally against each entry of
    /// the structured args array.
    #[serde(default)]
    pub deny_args: Vec<Arc<str>>,
}

/// Argument profiles applied when `agent.toml` declares no `[guardrails]`
/// section. `find` is in the default allowlist and its action predicates
/// (`-exec`, `-delete`, …) run other programs and remove files, so the
/// default allowlist would otherwise ship a code-execution primitive.
pub fn default_shell_profiles() -> Vec<ShellProfileConfig> {
    vec![ShellProfileConfig {
        program: Arc::from("find"),
        require_first_arg_suffix: Vec::new(),
        deny_args: [
            "-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprint", "-fprint0", "-fprintf",
            "-fls",
        ]
        .iter()
        .copied()
        .map(Arc::from)
        .collect(),
    }]
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct GuardrailsConfig {
    /// Programs the shell tool guardrail permits in a call's `command` field.
    /// Each entry is a bare program name — arguments belong in the structured
    /// args array, not here. Defaults to `DEFAULT_SHELL_ALLOWLIST`.
    pub shell_allowlist: Vec<Arc<str>>,
    /// Programs whose structured args array is checked too, not only the
    /// program name. Required for anything that can be argued into running
    /// other code. Defaults to `default_shell_profiles`.
    pub shell_profiles: Vec<ShellProfileConfig>,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            shell_allowlist: DEFAULT_SHELL_ALLOWLIST
                .iter()
                .copied()
                .map(Arc::from)
                .collect(),
            shell_profiles: default_shell_profiles(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ChannelsConfig {
    pub tui: ChannelConfig,
    pub telegram: ChannelConfig,
    pub feishu: ChannelConfig,
}

impl Default for ChannelsConfig {
    fn default() -> Self {
        Self {
            tui: ChannelConfig {
                enabled: true,
                mode: Arc::from("interactive"),
            },
            telegram: ChannelConfig {
                enabled: false,
                mode: Arc::from("poll_once"),
            },
            feishu: ChannelConfig {
                enabled: false,
                mode: Arc::from("long_connection"),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ChannelConfig {
    pub enabled: bool,
    pub mode: Arc<str>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Arc::from("disabled"),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct IsolationConfig {
    pub worker_path: Option<PathBuf>,
    pub worker_path_env: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct McpServerConfig {
    pub id: Arc<str>,
    pub endpoint: Arc<str>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct McpToolConfig {
    pub server_id: Arc<str>,
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub response: Arc<str>,
    /// What this MCP-backed tool may do to the filesystem (roadmap X2).
    ///
    /// Defaults to `full_access`, which is exactly what the old
    /// `requires_isolation = false` meant: the call is made in-process by the
    /// MCP client, so there is no child process for a sandbox to restrict.
    #[serde(default)]
    pub sandbox: SandboxMode,
}

impl Default for McpToolConfig {
    fn default() -> Self {
        Self {
            server_id: Arc::from("static-mcp"),
            name: Arc::from("remote_echo"),
            description: Arc::from("Static MCP-backed tool"),
            response: Arc::from("static MCP response"),
            sandbox: SandboxMode::FullAccess,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ResourceConfig {
    pub priority: Vec<Arc<str>>,
    pub skills: ResourceSection,
    pub tools: ResourceSection,
    pub mcp: ResourceSection,
    pub llm: ResourceSection,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            priority: vec![
                Arc::from("skills"),
                Arc::from("tools"),
                Arc::from("mcp"),
                Arc::from("llm"),
            ],
            skills: ResourceSection::default(),
            tools: ResourceSection {
                enabled: vec![
                    Arc::from("file"),
                    Arc::from("http"),
                    Arc::from("memory"),
                    Arc::from("shell"),
                ],
            },
            mcp: ResourceSection::default(),
            llm: ResourceSection::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ResourceSection {
    pub enabled: Vec<Arc<str>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct TaskWorkspaceConfig {
    pub root: PathBuf,
}

impl Default for TaskWorkspaceConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("tasks"),
        }
    }
}

impl WorkspaceConfig {
    pub fn load(path: &Path) -> Result<Self, std::io::Error> {
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut config = match std::fs::read_to_string(path) {
            Ok(input) => toml::from_str(&input).map_err(std::io::Error::other)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(err) => return Err(err),
        };
        config.resolve_paths(config_dir);
        config.validate_memory().map_err(std::io::Error::other)?;
        config.subagents.extend(load_subagent_files(config_dir)?);
        config
            .orchestrator_templates
            .extend(load_suborch_files(config_dir)?);
        config.validate_policy().map_err(std::io::Error::other)?;
        limits::validate_limits(&config.limits).map_err(std::io::Error::other)?;
        compaction::validate_compaction(&config.compaction).map_err(std::io::Error::other)?;
        jobs::validate_jobs(&config.jobs).map_err(std::io::Error::other)?;
        gateway::validate_gateway(&config.gateway).map_err(std::io::Error::other)?;
        approval::validate_approval(&config.approval).map_err(std::io::Error::other)?;
        spill::validate_spill(&config.spill).map_err(std::io::Error::other)?;
        config
            .validate_guardrails()
            .map_err(std::io::Error::other)?;
        config.validate_channels().map_err(std::io::Error::other)?;
        config.validate_resources().map_err(std::io::Error::other)?;
        config.validate_subagents().map_err(std::io::Error::other)?;
        config
            .validate_orchestrator_templates()
            .map_err(std::io::Error::other)?;
        config.routing_table().map_err(std::io::Error::other)?;
        Ok(config)
    }

    /// Reject templates whose stage dependencies cannot be scheduled, so a
    /// cyclic or dangling `depends_on` fails at config load instead of
    /// mid-run at the first escalation.
    pub fn validate_orchestrator_templates(&self) -> Result<(), String> {
        for template in &self.orchestrator_templates {
            stage_execution_order(
                &template.stages,
                |stage| &stage.name,
                |stage| &stage.depends_on,
            )
            .map_err(|err| format!("orchestrator template '{}' has {err}", template.name))?;
        }
        Ok(())
    }

    fn resolve_paths(&mut self, config_dir: &Path) {
        if let Some(path) = &self.memory.path {
            if path.is_relative() {
                self.memory.path = Some(config_dir.join(path));
            }
        }
        if self.task_workspace.root.is_relative() {
            self.task_workspace.root = config_dir.join(&self.task_workspace.root);
        }
        if let Some(worker_path) = &self.isolation.worker_path {
            if worker_path.is_relative() {
                self.isolation.worker_path = Some(config_dir.join(worker_path));
            }
        }
    }

    pub fn validate_memory(&mut self) -> Result<(), String> {
        self.memory.validate()
    }

    pub fn validate_policy(&self) -> Result<(), String> {
        match self.policy.default.as_ref() {
            "allow" | "ask_user" | "deny" => Ok(()),
            other => Err(format!(
                "unknown policy.default '{other}'; expected allow, ask_user, or deny"
            )),
        }
    }

    pub fn validate_guardrails(&self) -> Result<(), String> {
        for command in &self.guardrails.shell_allowlist {
            // The shell guardrail matches the `command` field against a bare
            // program name; an entry carrying arguments could never match and
            // is almost always an authoring mistake.
            if command.split_whitespace().count() != 1 {
                return Err(format!(
                    "guardrails.shell_allowlist entry '{command}' must be a single bare program name with no arguments"
                ));
            }
        }
        for profile in &self.guardrails.shell_profiles {
            let program = &profile.program;
            if program.split_whitespace().count() != 1 {
                return Err(format!(
                    "guardrails.shell_profiles entry '{program}' must be a single bare program name with no arguments"
                ));
            }
            // A profile with neither constraint reads as "this program is
            // governed" while admitting every call, which is the opposite of
            // what naming it means.
            if profile.require_first_arg_suffix.is_empty() && profile.deny_args.is_empty() {
                return Err(format!(
                    "guardrails.shell_profiles entry '{program}' constrains nothing; set require_first_arg_suffix or deny_args, or drop the profile"
                ));
            }
            if profile
                .require_first_arg_suffix
                .iter()
                .chain(&profile.deny_args)
                .any(|entry| entry.is_empty())
            {
                return Err(format!(
                    "guardrails.shell_profiles entry '{program}' has an empty constraint string"
                ));
            }
        }
        Ok(())
    }

    pub fn validate_channels(&self) -> Result<(), String> {
        validate_channel_mode("channels.tui", &self.channels.tui, &["interactive"])?;
        validate_channel_mode(
            "channels.telegram",
            &self.channels.telegram,
            &["poll_once", "polling"],
        )?;
        validate_channel_mode(
            "channels.feishu",
            &self.channels.feishu,
            &["long_connection"],
        )?;
        Ok(())
    }

    pub fn validate_resources(&self) -> Result<(), String> {
        for priority in &self.resources.priority {
            match priority.as_ref() {
                "skills" | "tools" | "mcp" | "llm" => {}
                other => {
                    return Err(format!(
                        "unknown resources.priority entry '{other}'; expected skills, tools, mcp, or llm"
                    ));
                }
            }
        }
        for tool in &self.resources.tools.enabled {
            match tool.as_ref() {
                "shell" | "http" | "file" | "memory" | "skill_validate" | "cron_create"
                | "cron_list" | "cron_remove" | "job_status" | "job_output" | "job_kill" => {}
                other => return Err(format!("unknown resources.tools.enabled entry '{other}'")),
            }
        }
        for llm in &self.resources.llm.enabled {
            if llm.as_ref() != "llm" {
                return Err(format!(
                    "unknown resources.llm.enabled entry '{llm}'; only 'llm' is supported"
                ));
            }
        }
        let static_mcp_tools = self
            .mcp_tools
            .iter()
            .map(|tool| Arc::clone(&tool.name))
            .collect::<BTreeSet<_>>();
        for tool in &self.resources.mcp.enabled {
            if static_mcp_tools.contains(tool) {
                continue;
            }
            if self.mcp_servers.iter().any(|server| {
                server.endpoint.starts_with("stdio://") || server.endpoint.starts_with("stdio:")
            }) {
                continue;
            }
            return Err(format!(
                "resources.mcp.enabled references unknown MCP tool '{tool}'"
            ));
        }
        Ok(())
    }

    pub fn validate_subagents(&mut self) -> Result<(), String> {
        for subagent in &mut self.subagents {
            subagent.memory_view = Arc::from(normalize_memory_view(&subagent.memory_view)?);
            for domain in &mut subagent.memory_domains {
                *domain = normalize_domain(domain, "subagents.memory_domains")?;
            }
            for tool in &mut subagent.memory_tools {
                *tool = Arc::from(normalize_memory_tool(tool)?);
            }
            if subagent.memory_view.as_ref() == "none" && !subagent.memory_domains.is_empty() {
                return Err(format!(
                    "subagent '{}' sets memory_domains without enabling memory_view",
                    subagent.id
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn subagent_metadata(
        &self,
        agent_id: &Arc<str>,
        policy_id: &Arc<str>,
    ) -> Result<BTreeMap<Arc<str>, serde_json::Value>, String> {
        let subagent = self
            .subagents
            .iter()
            .find(|subagent| subagent.id == *agent_id && subagent.policy_id == *policy_id)
            .ok_or_else(|| format!("unknown subagent '{agent_id}' with policy '{policy_id}'"))?;
        subagent_metadata(subagent)
    }

    pub fn resource_index(
        &self,
        tool_specs: &[ToolSpec],
        mcp_specs: &[ToolSpec],
        skill_specs: &[WorkspaceSkillMetadata],
    ) -> ResourceIndex {
        let mcp_names = mcp_specs
            .iter()
            .map(|spec| Arc::clone(&spec.name))
            .collect::<BTreeSet<_>>();
        let tools = tool_specs
            .iter()
            .filter(|spec| !mcp_names.contains(&spec.name))
            .collect::<Vec<_>>();
        let mcp = mcp_specs.iter().collect::<Vec<_>>();
        let mut entries = Vec::new();

        for priority in &self.resources.priority {
            match priority.as_ref() {
                "skills" => {
                    for skill in skill_specs {
                        entries.push(ResourceEntry {
                            name: Arc::clone(&skill.name),
                            kind: ResourceKind::Skill,
                            summary: Arc::clone(&skill.description),
                            priority: DispatchPriority::Skill,
                        });
                    }
                }
                "tools" => {
                    for tool in &tools {
                        entries.push(ResourceEntry {
                            name: Arc::clone(&tool.name),
                            kind: ResourceKind::Tool,
                            summary: Arc::clone(&tool.description),
                            priority: DispatchPriority::ToolOrMcp,
                        });
                    }
                }
                "mcp" => {
                    for tool in &mcp {
                        entries.push(ResourceEntry {
                            name: Arc::clone(&tool.name),
                            kind: ResourceKind::Mcp,
                            summary: Arc::clone(&tool.description),
                            priority: DispatchPriority::ToolOrMcp,
                        });
                    }
                }
                "llm" => {
                    if self.resources.llm.enabled.is_empty() {
                        entries.push(ResourceEntry {
                            name: Arc::from("llm"),
                            kind: ResourceKind::Llm,
                            summary: Arc::from("Fallback language model reasoning"),
                            priority: DispatchPriority::LlmFallback,
                        });
                    } else {
                        for llm in &self.resources.llm.enabled {
                            entries.push(ResourceEntry {
                                name: Arc::clone(llm),
                                kind: ResourceKind::Llm,
                                summary: Arc::from("Configured language model fallback"),
                                priority: DispatchPriority::LlmFallback,
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        ResourceIndex { entries }.sorted()
    }

    pub fn routing_table(&self) -> Result<RoutingTable, String> {
        self.validate_template_references()?;
        let templates = self.template_map()?;
        let rules = self
            .routing
            .rules
            .iter()
            .map(|rule| rule_from_config(rule, &templates, self))
            .collect::<Result<Vec<_>, _>>()?;
        let fallback = self.routing.fallback.as_ref().map_or_else(
            || -> Result<RoutingRule, String> {
                Ok(RoutingRule {
                    domain: TaskDomain::General,
                    description: Arc::from("General-purpose fallback for unclassified prompts."),
                    examples: Vec::new(),
                    dispatch: agentos_interfaces::orchestrator::DispatchTarget::Direct,
                })
            },
            |rule| rule_from_config(rule, &templates, self),
        )?;
        Ok(RoutingTable { rules, fallback })
    }

    fn template_map(&self) -> Result<BTreeMap<Arc<str>, OrchestratorTemplate>, String> {
        self.orchestrator_templates
            .iter()
            .map(|template| {
                template
                    .to_template(self)
                    .map(|value| (Arc::clone(&template.name), value))
            })
            .collect()
    }

    fn validate_template_references(&self) -> Result<(), String> {
        for template in &self.orchestrator_templates {
            for stage in &template.stages {
                if !self.subagents.iter().any(|subagent| {
                    subagent.id == stage.agent_id && subagent.policy_id == stage.policy_id
                }) {
                    return Err(format!(
                        "template '{}' stage '{}' references unknown subagent '{}' with policy '{}'",
                        template.name, stage.name, stage.agent_id, stage.policy_id
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_channel_mode(
    section: &str,
    channel: &ChannelConfig,
    allowed: &[&str],
) -> Result<(), String> {
    if !channel.enabled {
        return Ok(());
    }
    if allowed.iter().any(|mode| channel.mode.as_ref() == *mode) {
        return Ok(());
    }
    Err(format!(
        "{section}.mode '{}' is not supported; expected one of {}",
        channel.mode,
        allowed.join(", ")
    ))
}

fn load_subagent_files(config_dir: &Path) -> Result<Vec<SubAgentConfig>, std::io::Error> {
    let mut files = workspace_toml_files(&config_dir.join("subagents"))?;
    files
        .drain(..)
        .map(|path| {
            let input = std::fs::read_to_string(&path)?;
            let mut subagent: SubAgentConfig =
                toml::from_str(&input).map_err(std::io::Error::other)?;
            if subagent.name.is_empty() {
                subagent.name = Arc::clone(&subagent.id);
            }
            Ok(subagent)
        })
        .collect()
}

fn load_suborch_files(config_dir: &Path) -> Result<Vec<TemplateConfig>, std::io::Error> {
    let mut files = workspace_toml_files(&config_dir.join("suborchs"))?;
    files
        .drain(..)
        .map(|path| {
            let input = std::fs::read_to_string(&path)?;
            toml::from_str(&input).map_err(std::io::Error::other)
        })
        .collect()
}

fn workspace_toml_files(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut files = entries
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_loader_merges_workspace_files_and_effective_schema() {
        let root = unique_temp_dir("agentos-config-load");
        std::fs::create_dir_all(root.join("subagents")).expect("create subagents dir");
        std::fs::create_dir_all(root.join("suborchs")).expect("create suborchs dir");
        std::fs::write(
            root.join("agent.toml"),
            r#"
[agent]
max_turns = 9

[policy]
default = "ask_user"

[channels.tui]
enabled = true
mode = "interactive"

[channels.telegram]
enabled = true
mode = "poll_once"

[channels.feishu]
enabled = false
mode = "long_connection"

[resources]
priority = ["tools", "mcp", "llm"]

[resources.tools]
enabled = ["file", "memory"]

[resources.mcp]
enabled = ["remote_echo"]

[resources.llm]
enabled = ["llm"]

[task_workspace]
root = "tasks"

[[mcp_servers]]
id = "static-mcp"
endpoint = "static://local"

[[mcp_tools]]
server_id = "static-mcp"
name = "remote_echo"
description = "Static test MCP"
response = "ok"
"#,
        )
        .expect("write agent config");
        std::fs::write(
            root.join("subagents").join("loaded.toml"),
            r#"
name = ""
id = "loaded-agent"
policy_id = "loaded"
tools = ["http"]
memory_view = "none"
"#,
        )
        .expect("write subagent config");
        std::fs::write(
            root.join("suborchs").join("loaded.toml"),
            r#"
name = "loaded-template"
stages = [
  { name = "stage", agent_id = "loaded-agent", policy_id = "loaded" },
]
"#,
        )
        .expect("write template config");

        let config = WorkspaceConfig::load(&root.join("agent.toml")).expect("load config");

        assert_eq!(config.agent.max_turns, 9);
        assert_eq!(config.policy.default.as_ref(), "ask_user");
        assert!(config.channels.telegram.enabled);
        assert_eq!(
            config.resources.tools.enabled,
            vec![Arc::from("file"), Arc::from("memory")]
        );
        assert_eq!(config.resources.mcp.enabled, vec![Arc::from("remote_echo")]);
        assert_eq!(config.resources.llm.enabled, vec![Arc::from("llm")]);
        assert_eq!(config.task_workspace.root, root.join("tasks"));
        assert_eq!(config.subagents.len(), 1);
        assert_eq!(config.subagents[0].name.as_ref(), "loaded-agent");
        assert_eq!(config.orchestrator_templates.len(), 1);
        assert_eq!(
            config.orchestrator_templates[0].name.as_ref(),
            "loaded-template"
        );

        std::fs::remove_dir_all(root).expect("remove temp config dir");
    }

    #[test]
    fn invalid_inert_config_keys_are_rejected_when_known() {
        let mut config = WorkspaceConfig::default();
        config.policy.default = Arc::from("maybe");
        assert!(config.validate_policy().is_err());

        config.policy.default = Arc::from("deny");
        config.channels.telegram.enabled = true;
        config.channels.telegram.mode = Arc::from("webhook");
        assert!(config.validate_channels().is_err());

        config.channels.telegram.mode = Arc::from("poll_once");
        config.resources.llm.enabled = vec![Arc::from("gpt-other")];
        assert!(config.validate_resources().is_err());

        config.guardrails.shell_allowlist = vec![Arc::from("python3 fetch_emails.py")];
        assert!(config.validate_guardrails().is_err());
    }

    #[test]
    fn shell_profile_validation_rejects_authoring_mistakes() {
        let mut config = WorkspaceConfig::default();
        assert!(config.validate_guardrails().is_ok());

        // A profile that constrains nothing reads as "governed" while
        // admitting every call — the exact shape of the finding this
        // mechanism exists to close.
        config.guardrails.shell_profiles = vec![ShellProfileConfig {
            program: Arc::from("python3"),
            require_first_arg_suffix: Vec::new(),
            deny_args: Vec::new(),
        }];
        assert!(config.validate_guardrails().is_err());

        config.guardrails.shell_profiles = vec![ShellProfileConfig {
            program: Arc::from("python3 -c"),
            require_first_arg_suffix: vec![Arc::from(".py")],
            deny_args: Vec::new(),
        }];
        assert!(config.validate_guardrails().is_err());

        config.guardrails.shell_profiles = vec![ShellProfileConfig {
            program: Arc::from("python3"),
            require_first_arg_suffix: vec![Arc::from("")],
            deny_args: Vec::new(),
        }];
        assert!(config.validate_guardrails().is_err());
    }

    #[test]
    fn the_default_guardrails_profile_find() {
        // `find` is in the default allowlist, so the default profile set has
        // to cover it even for a deployment that writes no `[guardrails]`
        // section at all.
        let config = WorkspaceConfig::default();
        assert!(config
            .guardrails
            .shell_allowlist
            .contains(&Arc::from("find")));
        let find = config
            .guardrails
            .shell_profiles
            .iter()
            .find(|profile| profile.program.as_ref() == "find")
            .expect("the default profiles must cover find");
        assert!(find.deny_args.contains(&Arc::from("-exec")));
        assert!(find.deny_args.contains(&Arc::from("-delete")));
    }

    #[test]
    fn repository_workspace_config_declares_effective_resources() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = WorkspaceConfig::load(&repo_root.join("workspace/agent.toml"))
            .expect("load workspace config");

        assert_eq!(config.agent.max_turns, 256);
        assert_eq!(config.policy.default.as_ref(), "deny");
        assert!(config.channels.tui.enabled);
        assert!(!config.channels.telegram.enabled);
        assert!(!config.channels.feishu.enabled);
        assert_eq!(
            config.resources.skills.enabled,
            vec![
                Arc::from("skill-creator"),
                Arc::from("web-research"),
                Arc::from("audit-skill"),
            ]
        );
        // The shipped policy allowlist turns each name into a blanket
        // `Allow`, so the tools with operations worth gating must stay out of
        // it and keep their built-in per-operation rules.
        for tool in ["shell", "file", "memory"] {
            assert!(
                !config.policy.allowlist.contains(&Arc::from(tool)),
                "repo agent.toml must not blanket-allow '{tool}'"
            );
        }
        // `python3` is admitted by profile, not by bare program name: the
        // allowlist does not check arguments and the profile does.
        assert!(
            !config
                .guardrails
                .shell_allowlist
                .contains(&Arc::from("python3")),
            "repo agent.toml must not grant python3 an unchecked-argument allowlist entry"
        );
        let python = config
            .guardrails
            .shell_profiles
            .iter()
            .find(|profile| profile.program.as_ref() == "python3")
            .expect("repo agent.toml should profile python3");
        assert_eq!(
            python.require_first_arg_suffix,
            vec![Arc::from(".py")],
            "python3 must be pinned to a script path so -c cannot reach it"
        );
        assert_eq!(
            config.resources.tools.enabled,
            vec![
                Arc::from("file"),
                Arc::from("http"),
                Arc::from("memory"),
                Arc::from("shell"),
                Arc::from("skill_validate"),
                Arc::from("cron_create"),
                Arc::from("cron_list"),
                Arc::from("cron_remove"),
                Arc::from("job_status"),
                Arc::from("job_output"),
                Arc::from("job_kill"),
            ]
        );
        assert_eq!(config.resources.mcp.enabled, vec![Arc::from("remote_echo")]);
        assert_eq!(config.resources.llm.enabled, vec![Arc::from("llm")]);
        assert_eq!(
            config
                .routing_table()
                .expect("routing table")
                .fallback
                .dispatch,
            agentos_interfaces::orchestrator::DispatchTarget::Direct
        );
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", std::process::id()))
    }
}
