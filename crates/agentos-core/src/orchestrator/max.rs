use super::commands::deterministic_plan_from_user_text;
use super::routing::{
    input_overlaps_vocabulary, latest_user_content, materialize_dispatch, parse_routing_decision,
    routing_classifier_messages, routing_domains_json, routing_vocabulary, rule_for_domain_key,
    ROUTER_CONFIDENCE_THRESHOLD,
};
use crate::memory::{
    memory_caller_from_context, HydrationRequest, MemoryManager, MemoryStore, RetrievalStrategy,
};
use crate::prompt;
use crate::skills::{
    builtin_skill_planners, is_skill_authoring_request, SkillPlanner, WorkspaceSkillCatalog,
};
use agentos_interfaces::orchestrator::{
    DispatchPriority, Orchestrator, OrchestratorError, Plan, ResourceEntry, ResourceIndex,
    ResourceKind, RoutingRule, RoutingTable, RunContext,
};
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::Llm;
use agentos_proto::{Message, MessageRole};
use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::Arc;

const DEFAULT_MEMORY_HYDRATION_MAX_FRAGMENTS: usize = 5;
const DEFAULT_MEMORY_HYDRATION_MAX_TOKENS: usize = 1_200;

pub struct MaxOrchestrator {
    available_tools: Vec<ToolSpec>,
    resource_index: ResourceIndex,
    routing_table: RoutingTable,
    llm: Option<Arc<dyn Llm>>,
    memory_hydrator: Option<MemoryHydrator>,
    skill_catalog: WorkspaceSkillCatalog,
    skill_planners: Vec<Arc<dyn SkillPlanner>>,
    /// `[routing].llm_classifier` config: when false, routing decisions never
    /// spend an LLM round-trip and unmatched input goes to the fallback rule.
    llm_routing_enabled: bool,
    /// Lexical vocabulary of the specialized routes, precomputed by
    /// [`Self::with_routing_table`]. Empty when the pre-filter is disabled
    /// (no rules, or rules without examples).
    routing_vocabulary: BTreeSet<String>,
    /// Classifier domain catalog JSON, precomputed by
    /// [`Self::with_routing_table`] — the table is fixed per orchestrator.
    routing_domains_json: Arc<str>,
    /// Concatenated SKILL.md prelude, precomputed by
    /// [`Self::with_skill_catalog`] — the catalog is fixed per orchestrator.
    skill_prelude: Option<Message>,
}

impl Default for MaxOrchestrator {
    fn default() -> Self {
        Self {
            available_tools: Vec::new(),
            resource_index: ResourceIndex::default(),
            routing_table: RoutingTable::default(),
            llm: None,
            memory_hydrator: None,
            skill_catalog: WorkspaceSkillCatalog::default(),
            skill_planners: Vec::new(),
            llm_routing_enabled: true,
            routing_vocabulary: BTreeSet::new(),
            routing_domains_json: Arc::from("[]"),
            skill_prelude: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryHydrationSettings {
    pub enabled: bool,
    pub max_fragments: usize,
    pub max_estimated_tokens: usize,
    pub stores: Vec<MemoryStore>,
    pub strategy: RetrievalStrategy,
    pub allowed_shared_domains: Vec<Arc<str>>,
}

impl Default for MemoryHydrationSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            max_fragments: DEFAULT_MEMORY_HYDRATION_MAX_FRAGMENTS,
            max_estimated_tokens: DEFAULT_MEMORY_HYDRATION_MAX_TOKENS,
            stores: vec![MemoryStore::Semantic, MemoryStore::Episodic],
            strategy: RetrievalStrategy::Hybrid,
            allowed_shared_domains: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct MemoryHydrator {
    manager: Arc<MemoryManager>,
    settings: MemoryHydrationSettings,
}

impl MaxOrchestrator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tools(available_tools: Vec<ToolSpec>) -> Self {
        let resource_index = resource_index_from_tools(&available_tools);
        Self {
            available_tools,
            resource_index,
            ..Self::default()
        }
    }

    pub fn with_resource_index(mut self, resource_index: ResourceIndex) -> Self {
        self.resource_index = resource_index.sorted();
        self
    }

    pub fn with_routing_table(mut self, routing_table: RoutingTable) -> Self {
        self.routing_vocabulary = routing_vocabulary(&routing_table);
        self.routing_domains_json = Arc::from(routing_domains_json(&routing_table));
        self.routing_table = routing_table;
        self
    }

    /// Enable or disable the LLM routing classifier (`[routing].llm_classifier`
    /// in `agent.toml`). When disabled, input that no deterministic route
    /// claims goes straight to the fallback dispatch.
    pub fn with_llm_routing(mut self, enabled: bool) -> Self {
        self.llm_routing_enabled = enabled;
        self
    }

    pub fn with_llm(mut self, llm: Arc<dyn Llm>) -> Self {
        self.llm = Some(llm);
        self
    }

    pub fn with_memory_hydrator(
        mut self,
        manager: Arc<MemoryManager>,
        settings: MemoryHydrationSettings,
    ) -> Self {
        self.memory_hydrator = Some(MemoryHydrator { manager, settings });
        self
    }

    pub fn with_skill_catalog(mut self, skill_catalog: WorkspaceSkillCatalog) -> Self {
        // Re-derive the deterministic planner registry whenever the catalog
        // changes — each planner gates on `catalog.contains(name)` so an
        // empty / narrowed catalog produces silent no-ops in the chain.
        self.skill_planners = builtin_skill_planners(skill_catalog.clone());
        self.skill_prelude = build_skill_prelude(&skill_catalog);
        self.skill_catalog = skill_catalog;
        self
    }

    pub fn llm(&self) -> Option<&dyn Llm> {
        self.llm.as_deref()
    }

    pub fn available_tools(&self) -> &[ToolSpec] {
        &self.available_tools
    }

    pub fn memory_hydration_settings(&self) -> Option<&MemoryHydrationSettings> {
        self.memory_hydrator
            .as_ref()
            .map(|hydrator| &hydrator.settings)
    }

    pub fn resource_index(&self) -> &ResourceIndex {
        &self.resource_index
    }

    async fn plan_without_routing_fallback(
        &self,
        ctx: &RunContext<'_>,
    ) -> Result<Plan, OrchestratorError> {
        self.plan_internal(ctx, false).await
    }

    fn fallback_route(&self, ctx: &RunContext<'_>, input: &str) -> Option<Plan> {
        materialize_dispatch(ctx, input, &self.routing_table.fallback.dispatch)
    }

    async fn plan_with_llm(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let max_plan = self.plan_without_routing_fallback(ctx).await?;
        let latest_user_input = latest_user_content(ctx);
        let keep_skill_authoring_local = latest_user_input.as_ref().is_some_and(|input| {
            self.skill_catalog.contains("skill-creator")
                && is_skill_authoring_request(input.as_ref())
        });
        let keep_workspace_skill_local = latest_user_input
            .as_ref()
            .is_some_and(|input| workspace_skill_request_matches(&self.skill_catalog, input));
        let keep_workspace_diagnostics_local = latest_user_input
            .as_ref()
            .is_some_and(|input| workspace_diagnostic_request_matches(input));
        if !(should_route_to_llm(ctx, &max_plan)
            || keep_skill_authoring_local
            || keep_workspace_skill_local
            || keep_workspace_diagnostics_local)
        {
            return Ok(max_plan);
        }

        if !(keep_skill_authoring_local
            || keep_workspace_skill_local
            || keep_workspace_diagnostics_local)
        {
            if let Some(input) = latest_user_input.as_ref() {
                if let Some(plan) = self.route_user_text_with_llm(ctx, input).await? {
                    return Ok(plan);
                }
            }
        }
        if let Some(llm) = self.llm.as_ref().filter(|llm| llm.is_available()) {
            // Use the tool-aware path so the model can request `Plan::CallTool`
            // when it needs the filesystem / http / memory tools. The plain
            // `llm.complete(ctx)` default sends no `tools` field, so models
            // reply with "I can't modify files" even when tools are registered.
            //
            // `prompt::assemble` is the only path to a provider request: it
            // prepends the enabled skills' SKILL.md bodies (the "how to use
            // this skill" guidance, not just the one-line resource-index
            // description) and the memory fragments `hydrate()` selected, then
            // the conversation. Building a message vector here instead is what
            // let hydrated memory be computed and dropped (finding F1).
            let prompt = prompt::assemble(ctx, self.skill_prelude.as_ref());
            let response = super::streaming::complete_message(
                llm.as_ref(),
                ctx,
                &prompt.messages,
                &self.available_tools,
            )
            .await
            .map_err(|err| OrchestratorError::Backend(Arc::from(err.to_string())))?;
            ctx.push_llm_usage_from_message(&response);
            if let Some(first) = response.tool_calls.first().cloned() {
                return Ok(Plan::CallTool(first));
            }
            return Ok(Plan::Reply(response));
        }
        if keep_skill_authoring_local
            || keep_workspace_skill_local
            || keep_workspace_diagnostics_local
        {
            return Ok(max_plan);
        }
        self.plan_internal(ctx, true).await
    }

    async fn route_user_text_with_llm(
        &self,
        ctx: &RunContext<'_>,
        input: &str,
    ) -> Result<Option<Plan>, OrchestratorError> {
        if self.routing_table.rules.is_empty() {
            return Ok(self.fallback_route(ctx, input));
        }
        // Skip the classifier's LLM round-trip when routing is disabled by
        // config, or when the input shares no significant vocabulary with any
        // specialized route — strong evidence of general traffic that the
        // classifier would send to the fallback anyway. Any token overlap
        // keeps today's behavior (classifier decides), so the pre-filter can
        // save a round-trip but never reroute a matched input.
        if !self.llm_routing_enabled || self.input_is_clearly_general(input) {
            return Ok(self.fallback_route(ctx, input));
        }
        let Some(llm) = self.llm.as_ref().filter(|llm| llm.is_available()) else {
            return Ok(None);
        };
        let Some(rule) = self.llm_route_rule(ctx, llm.as_ref(), input).await? else {
            return Ok(self.fallback_route(ctx, input));
        };
        Ok(materialize_dispatch(ctx, input, &rule.dispatch))
    }

    fn input_is_clearly_general(&self, input: &str) -> bool {
        !self.routing_vocabulary.is_empty()
            && !input_overlaps_vocabulary(input, &self.routing_vocabulary)
    }

    async fn llm_route_rule(
        &self,
        ctx: &RunContext<'_>,
        llm: &dyn Llm,
        input: &str,
    ) -> Result<Option<&RoutingRule>, OrchestratorError> {
        let messages = routing_classifier_messages(input, &self.routing_domains_json);
        let response = llm
            .complete_messages(&messages, &[])
            .await
            .map_err(|err| OrchestratorError::Backend(Arc::from(err.to_string())))?;
        ctx.push_llm_usage_from_message(&response);
        let Some(decision) = parse_routing_decision(&response.content) else {
            return Ok(None);
        };
        if decision.confidence < ROUTER_CONFIDENCE_THRESHOLD {
            return Ok(None);
        }
        Ok(rule_for_domain_key(&self.routing_table, &decision.domain))
    }
}

fn should_route_to_llm(ctx: &RunContext<'_>, plan: &Plan) -> bool {
    let Some(item) = ctx.state.transcript.items.last() else {
        return false;
    };
    match item.message.role {
        MessageRole::User | MessageRole::Tool => matches!(
            plan,
            Plan::Reply(message)
                if message.role == MessageRole::Assistant && message.content == item.message.content
        ),
        MessageRole::Assistant | MessageRole::System => false,
    }
}

fn workspace_skill_request_matches(catalog: &WorkspaceSkillCatalog, input: &str) -> bool {
    if workspace_skill_maintenance_request_matches(input) {
        return false;
    }
    let normalized_input = normalize_skill_trigger_text(input);
    catalog.skills().any(|skill| {
        let skill_name = skill.name.as_ref();
        if skill_name == "skill-creator" {
            return false;
        }
        skill_trigger_aliases(skill_name).into_iter().any(|alias| {
            let normalized_alias = normalize_skill_trigger_text(&alias);
            contains_trigger_phrase(&normalized_input, &normalized_alias)
        })
    })
}

fn workspace_skill_maintenance_request_matches(input: &str) -> bool {
    let normalized = normalize_skill_trigger_text(input);
    [
        "change",
        "debug",
        "edit",
        "fix",
        "implement",
        "modify",
        "reconstruct",
        "redesign",
        "refactor",
        "repair",
        "rewrite",
        "update",
    ]
    .iter()
    .any(|phrase| contains_trigger_phrase(&normalized, phrase))
}

fn skill_trigger_aliases(name: &str) -> Vec<String> {
    let mut aliases = vec![name.to_owned(), name.replace(['-', '_'], " ")];
    if let Some(base) = name.strip_suffix("-skill") {
        if base.len() >= 4 {
            aliases.push(base.to_owned());
            aliases.push(format!("{} skill", base.replace(['-', '_'], " ")));
        }
    }
    aliases
}

fn normalize_skill_trigger_text(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn contains_trigger_phrase(input: &str, phrase: &str) -> bool {
    if phrase.is_empty() {
        return false;
    }
    let input = format!(" {input} ");
    let phrase = format!(" {phrase} ");
    input.contains(&phrase)
}

fn workspace_diagnostic_request_matches(input: &str) -> bool {
    let normalized = normalize_skill_trigger_text(input);
    let asks_for_diagnosis = [
        "investigate",
        "inspect",
        "check",
        "debug",
        "troubleshoot",
        "diagnose",
        "list",
        "show",
        "why",
    ]
    .iter()
    .any(|phrase| contains_trigger_phrase(&normalized, phrase));
    if !asks_for_diagnosis {
        return false;
    }

    [
        "file",
        "files",
        "config",
        "configuration",
        "session",
        "sessions",
        "log",
        "logs",
        "logging",
        "workspace",
        "repo",
        "repository",
        "codebase",
        "skill",
        "audit skill",
    ]
    .iter()
    .any(|phrase| contains_trigger_phrase(&normalized, phrase))
}

#[async_trait]
impl Orchestrator for MaxOrchestrator {
    async fn hydrate(&self, ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        if ctx.resource_index.entries.is_empty() {
            ctx.resource_index = self.resource_index.clone();
        }
        let Some(hydrator) = &self.memory_hydrator else {
            return Ok(());
        };
        hydrator.hydrate(ctx).await?;
        Ok(())
    }

    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        if self.llm.is_some() {
            return self.plan_with_llm(ctx).await;
        }
        self.plan_internal(ctx, true).await
    }
}

impl MemoryHydrator {
    async fn hydrate(&self, ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        if !self.settings.enabled {
            return Ok(());
        }
        let Some(query) = latest_user_content(ctx) else {
            return Ok(());
        };
        if query.trim().is_empty() {
            return Ok(());
        }

        let caller = memory_caller_from_context(ctx, self.settings.allowed_shared_domains.clone());
        let result = self
            .manager
            .hydrate_with_stats(
                &caller,
                HydrationRequest {
                    query,
                    domain: None,
                    max_fragments: self.settings.max_fragments,
                    max_tokens: self.settings.max_estimated_tokens,
                    stores: self.settings.stores.clone(),
                    strategy: self.settings.strategy,
                },
            )
            .await
            .map_err(|err| OrchestratorError::Backend(Arc::from(err.to_string())))?;

        ctx.memory_fragments.extend(result.fragments);
        ctx.system.metadata.insert(
            Arc::from("memory_hydration_candidate_count"),
            serde_json::Value::from(result.stats.candidate_count),
        );
        ctx.system.metadata.insert(
            Arc::from("memory_hydration_selected_count"),
            serde_json::Value::from(result.stats.selected_count),
        );
        ctx.system.metadata.insert(
            Arc::from("memory_hydration_namespace_count"),
            serde_json::Value::from(result.stats.namespace_count),
        );
        Ok(())
    }
}

impl MaxOrchestrator {
    async fn plan_internal(
        &self,
        ctx: &RunContext<'_>,
        allow_routing_fallback: bool,
    ) -> Result<Plan, OrchestratorError> {
        // Walk the deterministic planner registry. Each planner is gated on
        // its own skill being in the catalog, so a narrowed sub-agent
        // catalog produces silent no-ops here. Adding a new built-in skill
        // is now a registration change in `skills/planner.rs`, no edits to
        // this orchestrator.
        for planner in &self.skill_planners {
            if let Some(plan) = planner.plan(ctx)? {
                return Ok(plan);
            }
        }

        let Some(item) = ctx.state.transcript.items.last() else {
            return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
        };

        match item.message.role {
            MessageRole::Tool => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                Arc::clone(&item.message.content),
            ))),
            MessageRole::User => {
                if let Some(plan) = deterministic_plan_from_user_text(&item.message.content)? {
                    return Ok(plan);
                }
                if let Some(plan) =
                    self.route_user_text(ctx, &item.message.content, allow_routing_fallback)
                {
                    return Ok(plan);
                }
                Ok(Plan::Reply(Message::text(
                    MessageRole::Assistant,
                    Arc::clone(&item.message.content),
                )))
            }
            MessageRole::Assistant | MessageRole::System => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                Arc::clone(&item.message.content),
            ))),
        }
    }

    fn route_user_text(
        &self,
        ctx: &RunContext<'_>,
        input: &str,
        allow_fallback: bool,
    ) -> Option<Plan> {
        let rule = allow_fallback.then_some(&self.routing_table.fallback)?;
        materialize_dispatch(ctx, input, &rule.dispatch)
    }
}

fn build_skill_prelude(catalog: &WorkspaceSkillCatalog) -> Option<Message> {
    let skills = catalog.skills().collect::<Vec<_>>();
    if skills.is_empty() {
        return None;
    }
    let mut body = String::from(
        "# Available workspace skills\n\n\
         The following workspace skills are enabled for this run. Read \
         each skill's instructions before deciding whether to invoke its \
         tools — the SKILL.md body is the contract the skill expects you \
         to follow.\n",
    );
    for skill in skills {
        body.push_str("\n---\n## skill: ");
        body.push_str(&skill.name);
        body.push_str("\n\n");
        body.push_str(skill.instructions.as_ref());
        if !skill.instructions.ends_with('\n') {
            body.push('\n');
        }
    }
    Some(Message::text(MessageRole::System, body))
}

fn resource_index_from_tools(tools: &[ToolSpec]) -> ResourceIndex {
    ResourceIndex {
        entries: tools
            .iter()
            .map(|tool| ResourceEntry {
                name: Arc::clone(&tool.name),
                kind: ResourceKind::Tool,
                summary: Arc::clone(&tool.description),
                priority: DispatchPriority::ToolOrMcp,
            })
            .collect(),
    }
    .sorted()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skills::WorkspaceSkillCatalog;
    use agentos_interfaces::orchestrator::{DispatchTarget, SubAgentSpec, TaskDomain};
    use agentos_interfaces::session::Item;
    use agentos_interfaces::RunState;
    use agentos_llm::LlmError;
    use agentos_proto::MessageRole;
    use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
    use async_trait::async_trait;
    use serde_json::{json, value::RawValue};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn skill_prelude_message_emits_each_skill_instructions() {
        let catalog = WorkspaceSkillCatalog::from_skills([
            crate::skills::WorkspaceSkill {
                name: Arc::from("skill-creator"),
                description: Arc::from("creates skills"),
                path: PathBuf::from("/tmp/skill-creator"),
                instructions: Arc::from("# Skill Creator\n\nAlways pass body and files together."),
            },
            crate::skills::WorkspaceSkill {
                name: Arc::from("web-research"),
                description: Arc::from("does research"),
                path: PathBuf::from("/tmp/web-research"),
                instructions: Arc::from("# Web Research\n\nUse http tool to fetch."),
            },
        ]);
        let orch = MaxOrchestrator::default().with_skill_catalog(catalog);
        let prelude = orch.skill_prelude.expect("prelude expected");
        assert_eq!(prelude.role, MessageRole::System);
        let body = prelude.content.as_ref();
        assert!(body.contains("## skill: skill-creator"));
        assert!(body.contains("Always pass body and files together."));
        assert!(body.contains("## skill: web-research"));
        assert!(body.contains("Use http tool to fetch."));
    }

    #[test]
    fn skill_prelude_message_is_none_when_catalog_empty() {
        let orch = MaxOrchestrator::default();
        assert!(orch.skill_prelude.is_none());
    }

    #[test]
    fn workspace_skill_request_matches_enabled_skill_aliases() {
        let catalog = WorkspaceSkillCatalog::from_skills([
            crate::skills::WorkspaceSkill {
                name: Arc::from("audit-skill"),
                description: Arc::from("reports usage"),
                path: PathBuf::from("/tmp/audit-skill"),
                instructions: Arc::from("# Audit Skill\n\nRead audit memory."),
            },
            crate::skills::WorkspaceSkill {
                name: Arc::from("web-research"),
                description: Arc::from("does research"),
                path: PathBuf::from("/tmp/web-research"),
                instructions: Arc::from("# Web Research\n\nUse http."),
            },
        ]);

        assert!(workspace_skill_request_matches(
            &catalog,
            "Run an audit for the last 24 hours"
        ));
        assert!(workspace_skill_request_matches(
            &catalog,
            "call audit_skill now"
        ));
        assert!(workspace_skill_request_matches(
            &catalog,
            "web research: summarize this URL"
        ));
        assert!(!workspace_skill_request_matches(
            &catalog,
            "Reconstruct audit-skill via analysis of log files"
        ));
        assert!(!workspace_skill_request_matches(
            &catalog,
            "compare these implementation options"
        ));
    }

    #[test]
    fn workspace_diagnostic_request_matches_local_artifacts() {
        assert!(workspace_diagnostic_request_matches(
            "list the files the audit-skill explored, and investigate the memory configuration, sessions and logging"
        ));
        assert!(workspace_diagnostic_request_matches(
            "check workspace logs and config"
        ));
        assert!(!workspace_diagnostic_request_matches(
            "investigate the latest implementation options"
        ));
    }

    #[tokio::test]
    async fn natural_language_skill_creation_stays_in_parent_llm_path() {
        let llm = Arc::new(ToolCallingLlm::default());
        let catalog = WorkspaceSkillCatalog::from_skills([crate::skills::WorkspaceSkill {
            name: Arc::from("skill-creator"),
            description: Arc::from("creates skills"),
            path: PathBuf::from("/tmp/skill-creator"),
            instructions: Arc::from("# Skill Creator\n\nCreate skills with the file tool."),
        }]);
        let orch = MaxOrchestrator::with_tools(vec![ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read and write files"),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
        }])
        .with_skill_catalog(catalog)
        .with_routing_table(RoutingTable {
            rules: vec![RoutingRule {
                domain: TaskDomain::SoftwareDev,
                description: Arc::from("software tasks"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("general-subagent"),
                    policy_id: Arc::from("general"),
                    metadata: BTreeMap::new(),
                }),
            }],
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("fallback"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("general-subagent"),
                    policy_id: Arc::from("general"),
                    metadata: BTreeMap::new(),
                }),
            },
        })
        .with_llm(llm.clone());

        let mut state = RunState::new(RunId::new("run-skill"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(
                MessageRole::User,
                "please create a skill called rss-digest for summarizing feeds",
            ),
            metadata: BTreeMap::new(),
        });
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "skill creation should bypass routing delegation"
        );
        match plan {
            Plan::CallTool(call) => assert_eq!(call.name.as_ref(), "file"),
            other => panic!("expected parent file tool call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn enabled_workspace_skill_request_stays_in_parent_llm_path() {
        let llm = Arc::new(ToolCallingLlm::default());
        let catalog = WorkspaceSkillCatalog::from_skills([crate::skills::WorkspaceSkill {
            name: Arc::from("audit-skill"),
            description: Arc::from("reports usage"),
            path: PathBuf::from("/tmp/audit-skill"),
            instructions: Arc::from("# Audit Skill\n\nRead audit memory."),
        }]);
        let orch = MaxOrchestrator::with_tools(vec![ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read and write files"),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
        }])
        .with_skill_catalog(catalog)
        .with_routing_table(RoutingTable {
            rules: vec![RoutingRule {
                domain: TaskDomain::Research,
                description: Arc::from("research and investigation"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("research-subagent"),
                    policy_id: Arc::from("research"),
                    metadata: BTreeMap::new(),
                }),
            }],
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("fallback"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("general-subagent"),
                    policy_id: Arc::from("general"),
                    metadata: BTreeMap::new(),
                }),
            },
        })
        .with_llm(llm.clone());

        let mut state = RunState::new(RunId::new("run-audit"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::User, "Run an audit for the last 24 hours"),
            metadata: BTreeMap::new(),
        });
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "enabled workspace skill requests should bypass routing delegation"
        );
        match plan {
            Plan::CallTool(call) => assert_eq!(call.name.as_ref(), "file"),
            other => panic!("expected parent tool call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn workspace_skill_maintenance_request_uses_router() {
        let llm = Arc::new(ToolCallingLlm::default());
        let catalog = WorkspaceSkillCatalog::from_skills([crate::skills::WorkspaceSkill {
            name: Arc::from("audit-skill"),
            description: Arc::from("reports usage"),
            path: PathBuf::from("/tmp/audit-skill"),
            instructions: Arc::from("# Audit Skill\n\nRead audit memory."),
        }]);
        let orch = MaxOrchestrator::with_tools(vec![ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read and write files"),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
        }])
        .with_skill_catalog(catalog)
        .with_routing_table(RoutingTable {
            rules: vec![RoutingRule {
                domain: TaskDomain::SoftwareDev,
                description: Arc::from("implementation and repository maintenance"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("codereview-subagent"),
                    policy_id: Arc::from("code-fix"),
                    metadata: BTreeMap::new(),
                }),
            }],
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("fallback"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Direct,
            },
        })
        .with_llm(llm.clone());

        let mut state = RunState::new(RunId::new("run-audit-maintenance"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(
                MessageRole::User,
                "Reconstruct audit-skill via analysis of log files",
            ),
            metadata: BTreeMap::new(),
        });
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            llm.saw_router_prompt.load(Ordering::SeqCst),
            "skill maintenance should route to the appropriate sub-agent"
        );
        match plan {
            Plan::Delegate(spec) => {
                assert_eq!(spec.agent_id.as_str(), "codereview-subagent");
                assert_eq!(spec.policy_id.as_ref(), "code-fix");
            }
            other => panic!("expected delegated maintenance request, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn workspace_diagnostic_request_stays_in_parent_llm_path() {
        let llm = Arc::new(ToolCallingLlm::default());
        let orch = MaxOrchestrator::with_tools(vec![ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read and write files"),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
        }])
        .with_routing_table(RoutingTable {
            rules: vec![RoutingRule {
                domain: TaskDomain::Research,
                description: Arc::from("research and investigation"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("research-subagent"),
                    policy_id: Arc::from("research"),
                    metadata: BTreeMap::new(),
                }),
            }],
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("fallback"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("general-subagent"),
                    policy_id: Arc::from("general"),
                    metadata: BTreeMap::new(),
                }),
            },
        })
        .with_llm(llm.clone());

        let mut state = RunState::new(RunId::new("run-diagnostics"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(
                MessageRole::User,
                "list the files the audit-skill explored, and investigate the memory configuration, sessions and logging",
            ),
            metadata: BTreeMap::new(),
        });
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "workspace diagnostics should bypass research delegation"
        );
        match plan {
            Plan::CallTool(call) => assert_eq!(call.name.as_ref(), "file"),
            other => panic!("expected parent tool call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn validation_failure_returns_to_tool_aware_llm_path() {
        let llm = Arc::new(ToolCallingLlm::default());
        let catalog = WorkspaceSkillCatalog::from_skills([crate::skills::WorkspaceSkill {
            name: Arc::from("skill-creator"),
            description: Arc::from("creates skills"),
            path: PathBuf::from("/tmp/skill-creator"),
            instructions: Arc::from("# Skill Creator\n\nFix validation failures and retry."),
        }]);
        let orch = MaxOrchestrator::with_tools(vec![ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read and write files"),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
        }])
        .with_skill_catalog(catalog)
        .with_llm(llm.clone());

        let validate_call = ToolCall {
            id: ToolCallId::new("skill-creator-validate-audit-skill"),
            name: Arc::from("skill_validate"),
            args: RawValue::from_string(json!({ "name": "audit-skill" }).to_string())
                .expect("raw args"),
        };
        let mut state = RunState::new(RunId::new("run-skill"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::User, "create a 'audit-skill'"),
            metadata: BTreeMap::new(),
        });
        state.transcript.items.push(Item {
            message: Message {
                role: MessageRole::Assistant,
                content: Arc::from(""),
                attachments: Vec::new(),
                tool_calls: vec![validate_call.clone()],
                tool_call_id: None,
                metadata: BTreeMap::new(),
            },
            metadata: BTreeMap::new(),
        });
        let mut tool_metadata = BTreeMap::new();
        tool_metadata.insert(
            Arc::from("tool_status"),
            serde_json::Value::String("failed".to_owned()),
        );
        state.transcript.items.push(Item {
            message: Message {
                role: MessageRole::Tool,
                content: Arc::from("skill_validate: FAIL - body is missing"),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: Some(validate_call.id),
                metadata: tool_metadata,
            },
            metadata: BTreeMap::new(),
        });
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "tool-result repair should not invoke the router"
        );
        match plan {
            Plan::CallTool(call) => assert_eq!(call.name.as_ref(), "file"),
            other => panic!("expected repair file tool call, got {other:?}"),
        }
    }

    /// Routing table whose rules all carry examples, which activates the
    /// lexical pre-filter (sparse tables keep always-classify behavior).
    fn exampled_routing_table() -> RoutingTable {
        RoutingTable {
            rules: vec![RoutingRule {
                domain: TaskDomain::SoftwareDev,
                description: Arc::from("Implementation and debugging work."),
                examples: vec![
                    Arc::from("fix this failing test"),
                    Arc::from("implement the orchestrator plan"),
                ],
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("dev-subagent"),
                    policy_id: Arc::from("dev"),
                    metadata: BTreeMap::new(),
                }),
            }],
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("fallback"),
                examples: Vec::new(),
                dispatch: DispatchTarget::Delegate(SubAgentSpec {
                    agent_id: AgentId::new("general-subagent"),
                    policy_id: Arc::from("general"),
                    metadata: BTreeMap::new(),
                }),
            },
        }
    }

    fn user_run_state(input: &str) -> RunState {
        let mut state = RunState::new(RunId::new("run-routing"), AgentId::new("default"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::User, input),
            metadata: BTreeMap::new(),
        });
        state
    }

    #[tokio::test]
    async fn general_chat_without_vocabulary_overlap_skips_router_llm() {
        let llm = Arc::new(ToolCallingLlm::default());
        let orch = MaxOrchestrator::default()
            .with_routing_table(exampled_routing_table())
            .with_llm(llm.clone());
        let state = user_run_state("good morning, hope the weekend went well");
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "general chat with no route-vocabulary overlap must not spend a classifier call"
        );
        match plan {
            Plan::Delegate(spec) => assert_eq!(spec.agent_id.as_str(), "general-subagent"),
            other => panic!("expected fallback dispatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn vocabulary_overlap_still_uses_router_llm() {
        let llm = Arc::new(ToolCallingLlm::default());
        let orch = MaxOrchestrator::default()
            .with_routing_table(exampled_routing_table())
            .with_llm(llm.clone());
        // "fix" overlaps the software_dev examples, so the classifier decides.
        let state = user_run_state("fix the login flow");
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            llm.saw_router_prompt.load(Ordering::SeqCst),
            "input sharing route vocabulary must still be classified by the LLM"
        );
        match plan {
            Plan::Delegate(spec) => assert_eq!(spec.agent_id.as_str(), "dev-subagent"),
            other => panic!("expected classified dispatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disabled_llm_classifier_routes_to_fallback_without_llm() {
        let llm = Arc::new(ToolCallingLlm::default());
        let orch = MaxOrchestrator::default()
            .with_routing_table(exampled_routing_table())
            .with_llm_routing(false)
            .with_llm(llm.clone());
        // Overlapping input — but the classifier is disabled by config.
        let state = user_run_state("fix this failing test");
        let ctx = RunContext::from_state(&state);

        let plan = orch.plan(&ctx).await.expect("plan should succeed");
        assert!(
            !llm.saw_router_prompt.load(Ordering::SeqCst),
            "llm_classifier = false must never spend a classifier call"
        );
        match plan {
            Plan::Delegate(spec) => assert_eq!(spec.agent_id.as_str(), "general-subagent"),
            other => panic!("expected fallback dispatch, got {other:?}"),
        }
    }

    #[derive(Default)]
    struct ToolCallingLlm {
        saw_router_prompt: AtomicBool,
    }

    #[async_trait]
    impl Llm for ToolCallingLlm {
        async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
            Ok(Message::text(MessageRole::Assistant, "unused"))
        }

        async fn complete_messages(
            &self,
            _messages: &[Message],
            tools: &[ToolSpec],
        ) -> Result<Message, LlmError> {
            if tools.is_empty() {
                self.saw_router_prompt.store(true, Ordering::SeqCst);
                return Ok(Message::text(
                    MessageRole::Assistant,
                    r#"{"domain":"software_dev","confidence":1.0}"#,
                ));
            }
            let args = RawValue::from_string(
                json!({
                    "operation": "write",
                    "path": "skills/rss-digest/SKILL.md",
                    "content": "---\nname: rss-digest\ndescription: Summarize feeds.\n---\n",
                })
                .to_string(),
            )
            .expect("raw args");
            let mut message = Message::text(MessageRole::Assistant, "");
            message.tool_calls.push(ToolCall {
                id: ToolCallId::new("call-file"),
                name: Arc::from("file"),
                args,
            });
            Ok(message)
        }
    }
}
