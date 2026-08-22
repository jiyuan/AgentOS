use crate::approve::Policy;
use crate::config::{SubAgentConfig, WorkspaceConfig};
use crate::guardrails::{
    MaxOutputLength, PiiFilter, ShellCommandAllowlist, SkillBundleWriteGuardrail,
};
use crate::jobs::JobRegistry;
use crate::memory::{
    InMemoryMemory, MemoryManager, QdrantSemanticIndex, SemanticIndex, SqliteStore,
    SqliteVecSemanticIndex,
};
use crate::orchestrator::{
    EchoOrchestrator, MaxOrchestrator, MemoryHydrationSettings, MinOrchestrator,
};
use crate::prompt::Compaction;
use crate::runner::{JsonlTraceSink, TraceSink};
use crate::skills::WorkspaceSkillCatalog;
use crate::spill::{ContentLimits, SpillStore};
use crate::subagents::{SubAgentDefinition, SubAgentRegistry};
use crate::task_workspace::TaskWorkspace;
use crate::tools::{MemoryTool, ToolRegistry};
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, ResourceIndex, RunContext,
};
use agentos_llm::{EnvLlm, Llm, LlmModelController, LlmModelTier};
use agentos_proto::AgentId;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

mod deps;
pub use deps::RuntimeDepsScope;
mod isolation;
pub use isolation::isolation_worker_path;
use isolation::refuse_unenforceable_isolation;
mod mcp_config;
pub use mcp_config::register_configured_mcp;
mod tools_config;

use tools_config::{
    build_parent_tools, subagent_delegation_grants, subagent_memory_tool_enabled, subagent_policy,
};
pub use tools_config::{
    phase5_policy, register_builtin_tool, BUILTIN_TOOL_NAMES, RUNTIME_TOOL_NAMES,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePaths {
    pub agent_config_path: PathBuf,
    pub session_db_path: PathBuf,
    pub trace_dir: PathBuf,
    pub workspace_root: PathBuf,
    pub skills_dir: PathBuf,
    pub cron_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum OrchestratorStrategy {
    Max = 0,
    Min = 1,
}

impl OrchestratorStrategy {
    pub fn from_config(input: &str) -> Result<Self, String> {
        match input.trim().to_ascii_lowercase().as_str() {
            "builtin.max" | "max" | "builtin.tool_selecting" | "tool_selecting" => Ok(Self::Max),
            "builtin.min" | "min" | "builtin.llm" | "builtin.llm_fallback" | "llm" => Ok(Self::Min),
            other => Err(format!(
                "unknown orchestrator strategy '{other}'; expected builtin.max or builtin.min"
            )),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Max => "max",
            Self::Min => "min",
        }
    }

    pub fn task_id(self) -> &'static str {
        match self {
            Self::Max => "main",
            Self::Min => "min",
        }
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Min,
            _ => Self::Max,
        }
    }
}

pub struct StrategyOrchestrator {
    strategy: Arc<AtomicU8>,
    max: MaxOrchestrator,
    min: MinOrchestrator,
}

impl StrategyOrchestrator {
    pub fn new(strategy: OrchestratorStrategy, max: MaxOrchestrator, min: MinOrchestrator) -> Self {
        Self {
            strategy: Arc::new(AtomicU8::new(strategy as u8)),
            max,
            min,
        }
    }

    pub fn strategy_handle(&self) -> Arc<AtomicU8> {
        self.strategy.clone()
    }

    pub fn current_strategy(&self) -> OrchestratorStrategy {
        OrchestratorStrategy::from_u8(self.strategy.load(Ordering::Relaxed))
    }

    pub fn describe_llm(&self) -> String {
        let llm = self
            .max
            .llm()
            .map(|llm| llm.describe())
            .unwrap_or_else(|| "llm provider=builtin.echo".to_owned());
        format!("orchestrator={}, {llm}", self.current_strategy().name())
    }

    pub fn memory_hydration_settings(
        &self,
    ) -> Option<&crate::orchestrator::MemoryHydrationSettings> {
        self.max.memory_hydration_settings()
    }
}

#[async_trait]
impl Orchestrator for StrategyOrchestrator {
    async fn hydrate(&self, ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        match self.current_strategy() {
            OrchestratorStrategy::Max => self.max.hydrate(ctx).await,
            OrchestratorStrategy::Min => self.min.hydrate(ctx).await,
        }
    }

    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        match self.current_strategy() {
            OrchestratorStrategy::Max => self.max.plan(ctx).await,
            OrchestratorStrategy::Min => self.min.plan(ctx).await,
        }
    }
}

pub struct AgentRuntime {
    pub workspace_config: WorkspaceConfig,
    pub session: Arc<SqliteStore>,
    pub memory_manager: Arc<MemoryManager>,
    pub model_controller: LlmModelController,
    pub orchestrator: StrategyOrchestrator,
    pub active_agent: AgentId,
    pub skill_catalog: WorkspaceSkillCatalog,
    tools: ToolRegistry,
    policy: Policy,
    subagents: Option<SubAgentRegistry>,
    trace_sink: Arc<dyn TraceSink>,
    task_workspace: Arc<TaskWorkspace>,
    pii_filter: PiiFilter,
    max_output_length: MaxOutputLength,
    shell_allowlist: ShellCommandAllowlist,
    spill: Option<Arc<SpillStore>>,
    tool_result_inline_bytes: usize,
    summarizer: Arc<dyn Llm>,
    cancel: CancellationToken,
    jobs: Arc<JobRegistry>,
}

impl AgentRuntime {
    /// Whether this deployment has opted into forwarding assistant text before
    /// the output guardrails have seen it (`[channels] provisional_streaming`).
    ///
    /// Every entrypoint that installs a `StreamSink` asks this first. Off by
    /// default: see the field's own documentation and
    /// [ADR-0007](../../../../docs/adr/0007-BUFFERED_OUTPUT.md).
    pub fn provisional_streaming(&self) -> bool {
        self.workspace_config.channels.provisional_streaming
    }

    /// The inline cap and spill store every run in this runtime uses.
    pub fn content_limits(&self) -> ContentLimits<'_> {
        ContentLimits {
            tool_result_inline_bytes: self.tool_result_inline_bytes,
            spill: self.spill.as_deref(),
        }
    }

    /// This runtime's background jobs (roadmap item D3).
    ///
    /// Exposed so a caller that knows a conversation has ended can call
    /// [`JobRegistry::dispose_conversation`]. The sharded gateway (G1) does
    /// that on `/clear`, which is the one point where a conversation is
    /// declared over — its jobs belong to a history that no longer exists.
    pub fn jobs(&self) -> &Arc<JobRegistry> {
        &self.jobs
    }

    /// Delete spill artifacts past `[spill].retention_days`, returning how
    /// many run directories were removed (roadmap item X3).
    ///
    /// A no-op when retention is `0` — "keep everything" — or when no spill
    /// store is configured. Called from the gateway's idle phase, beside cron
    /// and reflection, because sweeping a directory is maintenance and has no
    /// business competing with a run.
    pub async fn sweep_spill(&self) -> usize {
        let Some(retention) = self.workspace_config.spill.retention_secs() else {
            return 0;
        };
        let Some(spill) = self.spill.as_ref() else {
            return 0;
        };
        spill
            .sweep_older_than(std::time::Duration::from_secs(retention))
            .await
    }

    /// This runtime's root cancellation token (roadmap item D1).
    ///
    /// Every run started through [`RuntimeDepsScope::deps`] gets a *child* of
    /// it, so cancelling this stops all of them — the shutdown path. To stop
    /// one run instead, overwrite `RunnerDeps::cancel` with your own child
    /// token before starting it and keep the handle.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }

    /// The summarizer and trigger every run in this runtime compacts with.
    ///
    /// Which model writes the summaries is `[compaction].model`, defaulting to
    /// the same high tier having the conversation. A weaker tier is cheaper and
    /// a real trade: the summary is what the model reads for the rest of the
    /// conversation, so it lowers the ceiling on every later turn.
    pub fn compaction(&self) -> Compaction<'_> {
        Compaction {
            summarizer: Some(self.summarizer.as_ref()),
            config: self.workspace_config.compaction,
        }
    }

    pub async fn build(paths: RuntimePaths) -> Result<Self, String> {
        Self::build_with(paths, &|_| None).await
    }

    /// Build the runtime, consulting `semantic_factory` to resolve the
    /// `[memory].semantic_backend` config string to an externally-provided
    /// [`SemanticIndex`] (e.g. an extension crate's) before falling back to the
    /// built-in `sqlite_vec` / `qdrant` / `none` selection. This is the
    /// boundary-respecting extension hook: the CLI supplies the factory, so core
    /// never names an extension crate.
    pub async fn build_with(
        paths: RuntimePaths,
        semantic_factory: SemanticIndexFactory<'_>,
    ) -> Result<Self, String> {
        let workspace_config = WorkspaceConfig::load(&paths.agent_config_path)
            .map_err(|err| format!("failed to load workspace config: {err}"))?;
        if workspace_config.memory.semantic_backend_is_sqlite_vec() {
            SqliteVecSemanticIndex::register_auto_extension()
                .map_err(|err| format!("failed to register sqlite-vec extension: {err}"))?;
        }
        let session = Arc::new(
            SqliteStore::open(paths.session_db_path)
                .map_err(|err| format!("failed to open session store: {err}"))?,
        );
        let memory_manager =
            build_memory_manager(&workspace_config, session.clone(), semantic_factory)?;
        // One registry for the whole runtime, keyed by conversation. Jobs
        // outlive a run, so nothing narrower can own them until G1 introduces
        // the conversation actor that should.
        let jobs = Arc::new(JobRegistry::new(
            workspace_config.jobs.max_concurrent,
            workspace_config.jobs.output_limit_bytes,
        ));
        // Resolved here rather than beside the other paths below because the
        // spill store needs it, and `spill_read` needs the store before the
        // tool registry is built.
        let resolved_workspace_root = absolutise(&paths.workspace_root);
        // Spill artifacts live beside the other workspace-owned runtime state.
        let spill = Some(Arc::new(SpillStore::new(
            workspace_config.spill.root_in(&resolved_workspace_root),
        )));
        let mut tools = build_parent_tools(
            &workspace_config,
            memory_manager.clone(),
            jobs.clone(),
            spill.clone(),
        )?;
        if let Some(path) = isolation_worker_path(&workspace_config) {
            tools = tools
                .with_subprocess_isolation(path)
                .with_env_passthrough(workspace_config.isolation.env_passthrough.iter().cloned());
        }
        let mcp_specs = register_configured_mcp(&mut tools, &workspace_config).await?;
        refuse_unenforceable_isolation(&tools).await?;
        let model_controller = LlmModelController::new();
        // Pin `AGENTOS_HOME` to the absolute resolved workspace root so every
        // downstream caller of `agentos_interfaces::agentos_home(None)` (tool
        // implementations, slash commands, attachment store) resolves to the
        // same anchor regardless of the process's CWD or ambient env.
        //
        // Skill catalog must be loaded before sub-agents are built so sub-agent
        // MaxOrchestrators can hold a clone of it and dispatch skills (e.g.
        // web-research, skill-creator). Resolve to an absolute path so the
        // skills root is independent of the gateway process's CWD.
        std::env::set_var("AGENTOS_HOME", &resolved_workspace_root);
        let resolved_skills_root = absolutise(&paths.skills_dir);
        let resolved_cron_dir = absolutise(&paths.cron_dir);
        tracing::info!(
            workspace_root = %resolved_workspace_root.display(),
            skills_root = %resolved_skills_root.display(),
            cron_dir = %resolved_cron_dir.display(),
            "runtime paths resolved"
        );
        probe_skills_root(&resolved_skills_root)
            .map_err(|err| format!("skills root write probe failed: {err}"))?;
        let skill_catalog = WorkspaceSkillCatalog::load_enabled(
            &resolved_skills_root,
            &workspace_config.resources.skills.enabled,
        )
        .map_err(|err| format!("failed to load workspace skills: {err}"))?;
        // Before the sub-agents, because each sub-agent's policy is now derived
        // from this one rather than synthesised independently.
        let policy = phase5_policy(&workspace_config, &mcp_specs);
        let subagents = build_subagents(
            &workspace_config,
            model_controller.clone(),
            memory_manager.clone(),
            skill_catalog.clone(),
            &policy,
        )?;
        let resource_index =
            workspace_config.resource_index(&tools.specs(), &mcp_specs, &skill_catalog.metadata());
        let routing_table = workspace_config.routing_table()?;
        let high_llm = Arc::new(EnvLlm::new(LlmModelTier::High, model_controller.clone())?);
        let summarizer = summarizer_for(
            workspace_config.compaction.model,
            &high_llm,
            &model_controller,
        )?;
        let max_orchestrator = MaxOrchestrator::with_tools(tools.specs())
            .with_resource_index(resource_index)
            .with_routing_table(routing_table)
            .with_llm_routing(workspace_config.routing.llm_classifier)
            .with_skill_catalog(skill_catalog.clone())
            .with_llm(high_llm.clone())
            .with_memory_hydrator(
                memory_manager.clone(),
                workspace_config.memory.hydration_settings()?,
            );
        let min_orchestrator = MinOrchestrator::new(high_llm.clone()).with_tools(tools.specs());
        let orchestrator_strategy =
            OrchestratorStrategy::from_config(&workspace_config.agent.orchestrator)?;
        let orchestrator =
            StrategyOrchestrator::new(orchestrator_strategy, max_orchestrator, min_orchestrator);
        let trace_sink: Arc<dyn TraceSink> = Arc::new(JsonlTraceSink::new(paths.trace_dir));
        let task_workspace = Arc::new(TaskWorkspace::new(
            workspace_config.task_workspace.root.clone(),
        ));
        let tool_result_inline_bytes = workspace_config.limits.tool_result_inline_bytes;
        let subagents = subagents.map(|registry| {
            registry
                .with_trace_sink(trace_sink.clone())
                .with_task_workspace(task_workspace.clone())
                .with_session(session.clone())
                .with_content_limits(spill.clone(), tool_result_inline_bytes)
                .with_compaction(Some(high_llm.clone()), workspace_config.compaction)
                .with_safety_log(Some(session.clone()))
        });
        let shell_allowlist =
            ShellCommandAllowlist::new(workspace_config.guardrails.shell_allowlist.iter().cloned())
                .with_profiles(workspace_config.guardrails.shell_profiles.iter().cloned());

        // `[agent].id`, not a constant. Every trace, episode, memory record,
        // and safety event is stamped with this and keyed by the principal it
        // belongs to, so a hardcoded name made two deployments sharing a store
        // into one agent (M7 / `CFG-001`).
        let active_agent = AgentId::new(workspace_config.agent.id.as_ref());

        Ok(Self {
            workspace_config,
            session,
            memory_manager,
            model_controller,
            orchestrator,
            active_agent,
            skill_catalog,
            tools,
            policy,
            subagents,
            trace_sink,
            task_workspace,
            pii_filter: PiiFilter,
            max_output_length: MaxOutputLength::new(64_000),
            shell_allowlist,
            spill,
            tool_result_inline_bytes,
            summarizer,
            cancel: CancellationToken::new(),
            jobs,
        })
    }

    pub fn deps_scope(&self) -> RuntimeDepsScope<'_> {
        RuntimeDepsScope { runtime: self }
    }
}

/// The model that writes compaction summaries (roadmap item X3,
/// `[compaction].model`).
///
/// Reuses the conversation's own client for the `high` default rather than
/// building a second one for the same model — a separate client would mean a
/// separate connection pool and a separate `/model` override state for what an
/// operator asked to be the same model.
fn summarizer_for(
    tier: LlmModelTier,
    high: &Arc<EnvLlm>,
    controller: &LlmModelController,
) -> Result<Arc<dyn Llm>, String> {
    match tier {
        LlmModelTier::High => Ok(high.clone()),
        tier => Ok(Arc::new(EnvLlm::new(tier, controller.clone())?)),
    }
}

/// Resolve `path` to an absolute path against the process CWD at the moment
/// of the call. Used at startup so runtime-owned roots handed to tools are
/// CWD-independent. Falls back to the input path if `current_dir` fails (e.g.
/// CWD was unlinked) — in that pathological case we'd rather keep going than
/// refuse to start.
fn absolutise(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}

/// Write-and-delete a small probe file under the skills root at startup.
/// If anything fails, the gateway refuses to start with a clear error.
/// This rules out the "writes silently swallowed by overlay/NFS/permissions"
/// failure mode — if the probe succeeds, every later `skill_create` write
/// that returns `Ok` from `fs::write` really did land on disk at that
/// location.
fn probe_skills_root(root: &Path) -> Result<(), String> {
    std::fs::create_dir_all(root)
        .map_err(|err| format!("cannot create skills root '{}': {err}", root.display()))?;
    // The filename must be unique per probe call, not just per process: the
    // gateway runs one `AgentRuntime::build` per channel concurrently in the
    // same process, so a process-id-only name lets one thread's `remove_file`
    // delete another thread's probe and the loser fails with ENOENT. Mix in a
    // monotonic counter so concurrent builds never collide.
    static PROBE_SEQ: AtomicU64 = AtomicU64::new(0);
    let probe = root.join(format!(
        ".agentos-probe-{}-{}",
        std::process::id(),
        PROBE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&probe, b"agentos skills root probe")
        .map_err(|err| format!("cannot write to skills root '{}': {err}", root.display()))?;
    let metadata = std::fs::metadata(&probe).map_err(|err| {
        format!(
            "probe file '{}' could not be stat'd after write: {err}",
            probe.display()
        )
    })?;
    if metadata.len() == 0 {
        let _ = std::fs::remove_file(&probe);
        return Err(format!(
            "probe file '{}' was written but reads back as zero bytes",
            probe.display()
        ));
    }
    std::fs::remove_file(&probe).map_err(|err| {
        format!(
            "probe file '{}' could not be removed: {err}",
            probe.display()
        )
    })?;
    tracing::info!(
        skills_root = %root.display(),
        "skills root write probe succeeded"
    );
    Ok(())
}

#[deprecated(
    since = "0.5.0",
    note = "use `config::WorkspaceConfig::load` directly; this compatibility \
            wrapper will be removed (docs/PLAN.md finding A4)"
)]
pub fn load_workspace_config(path: &Path) -> Result<WorkspaceConfig, std::io::Error> {
    WorkspaceConfig::load(path)
}

/// Resolves a `[memory].semantic_backend` config string to an externally
/// provided [`SemanticIndex`], or `None` to use the built-in selection. The CLI
/// supplies this so an extension crate's index can be injected without core
/// depending on the extension. See [`AgentRuntime::build_with`].
pub type SemanticIndexFactory<'a> =
    &'a (dyn Fn(&str) -> Option<Arc<dyn SemanticIndex>> + Send + Sync);

fn build_memory_manager(
    config: &WorkspaceConfig,
    session: Arc<SqliteStore>,
    semantic_factory: SemanticIndexFactory<'_>,
) -> Result<Arc<MemoryManager>, String> {
    let (manager, sqlite_store) = if config.memory.backend_is_in_memory() {
        (
            MemoryManager::new(Arc::new(InMemoryMemory::default())),
            None,
        )
    } else if let Some(path) = &config.memory.path {
        let store = Arc::new(SqliteStore::open(path).map_err(|err| {
            format!(
                "failed to open configured memory store '{}': {err}",
                path.display()
            )
        })?);
        (MemoryManager::new_sqlite(store.clone()), Some(store))
    } else {
        (MemoryManager::new_sqlite(session.clone()), Some(session))
    };

    // An injected (extension) index wins over the built-in backends.
    if let Some(index) = semantic_factory(config.memory.semantic_backend.as_ref()) {
        return Ok(Arc::new(manager.with_semantic_index(index)));
    }

    if config.memory.semantic_backend_is_qdrant() {
        let qdrant = QdrantSemanticIndex::new((&config.memory.qdrant).into())
            .map_err(|err| format!("failed to configure qdrant semantic memory: {err}"))?;
        return Ok(Arc::new(manager.with_semantic_index(Arc::new(qdrant))));
    }

    if config.memory.semantic_backend_is_sqlite_vec() {
        let Some(sqlite_store) = sqlite_store else {
            return Err("sqlite_vec semantic memory requires a sqlite memory backend".to_owned());
        };
        let sqlite_vec =
            SqliteVecSemanticIndex::new(sqlite_store, (&config.memory.sqlite_vec).into())
                .map_err(|err| format!("failed to configure sqlite_vec semantic memory: {err}"))?;
        return Ok(Arc::new(manager.with_semantic_index(Arc::new(sqlite_vec))));
    }

    Ok(Arc::new(manager))
}

pub fn build_subagents(
    config: &WorkspaceConfig,
    models: LlmModelController,
    memory_manager: Arc<MemoryManager>,
    skill_catalog: WorkspaceSkillCatalog,
    parent_policy: &Policy,
) -> Result<Option<SubAgentRegistry>, String> {
    if config.subagents.is_empty() {
        return Ok(None);
    }

    // Hoist memory hydration settings out of the per-sub-agent loop — they
    // come from workspace_config.memory and are identical for every sub-agent
    // that opts in via memory_view.
    let hydration_settings = config.memory.hydration_settings()?;

    let mut registry = SubAgentRegistry::new();
    for subagent in &config.subagents {
        let mut tools = ToolRegistry::new();
        for tool in &subagent.tools {
            if tool.as_ref() != "memory" {
                register_builtin_tool(
                    &mut tools,
                    tool,
                    &config.limits,
                    &config.isolation.env_passthrough,
                )?;
            }
        }
        if subagent_memory_tool_enabled(subagent) {
            tools.register(MemoryTool::with_manager(memory_manager.clone()));
        }
        // A sub-agent's tools run on the same machine under the same operator,
        // so they are held to the same deadlines as the parent's.
        let tools = tools.with_timeouts(
            config.limits.tool_timeout(),
            config.limits.tool_timeout_overrides(),
        );
        // Capture the tool specs *before* moving `tools` into the definition,
        // so we can hand them to the orchestrator (which surfaces them as the
        // LLM's `tools` schema for function calling).
        let tool_specs = tools.specs();
        // A sub-agent naming a skill the workspace never loaded is a
        // configuration error, not a warning. `filtered` drops unknown names
        // silently, so before this the sub-agent came up looking configured
        // while three of `general-subagent`'s six skills did not exist —
        // visible only as a `warn!` nobody reads at the default filter, and
        // indistinguishable at runtime from a skill the model chose not to
        // use (M2 deliverable 5). `load_enabled` already fails this way for
        // the workspace catalog; this makes the sub-agent path agree.
        let unknown: Vec<&str> = subagent
            .skills
            .iter()
            .filter(|declared| !skill_catalog.contains(declared))
            .map(AsRef::as_ref)
            .collect();
        if !unknown.is_empty() {
            return Err(format!(
                "sub-agent '{}' declares skills the workspace has not enabled: {}. \
                 Add them to [resources.skills] enabled, or remove them from the sub-agent.",
                subagent.id,
                unknown.join(", ")
            ));
        }
        // Narrow the parent's skill catalog to just what this sub-agent
        // declared in its `skills` field. An empty list means no access.
        let subagent_skill_catalog = skill_catalog.filtered(&subagent.skills);
        // Build the per-sub-agent resource_index so the LLM sees its own
        // tools AND skills as available resources. Reuses the parent's
        // helper that knows how to weave tools + mcp + skills into the
        // ResourceIndex shape (mcp_specs are parent-only).
        let subagent_resource_index =
            config.resource_index(&tool_specs, &[], &subagent_skill_catalog.metadata());
        // Sub-agents that don't opt into memory_view skip the hydrator —
        // hydrating a transcript the model can't read from is wasted work.
        let memory_hydrator = if subagent.memory_view.as_ref() != "none" {
            Some((memory_manager.clone(), hydration_settings.clone()))
        } else {
            None
        };
        let mut definition = SubAgentDefinition::new(
            AgentId::new(Arc::clone(&subagent.id)),
            Arc::clone(&subagent.policy_id),
            subagent_orchestrator(
                subagent,
                models.clone(),
                tool_specs,
                subagent_skill_catalog,
                subagent_resource_index,
                memory_hydrator,
            )?,
            subagent_policy(subagent, parent_policy)?,
        )
        .with_tools(Arc::new(tools))
        .with_max_turns(subagent.max_turns)
        .with_seed_from_parent(subagent.seed_from_parent)
        .with_delegation_grants(subagent_delegation_grants(subagent)?);
        if subagent.memory_view.as_ref() != "none" || subagent_memory_tool_enabled(subagent) {
            definition = definition.with_memory_manager(memory_manager.clone());
        }
        if subagent.inherit_guardrails {
            definition = definition
                .with_input_guardrail("PiiFilter", PiiFilter)
                .with_output_guardrail(
                    "MaxOutputLength",
                    MaxOutputLength::new(subagent.max_output_chars),
                )
                .with_tool_guardrail(
                    "ShellCommandAllowlist",
                    ShellCommandAllowlist::new(config.guardrails.shell_allowlist.iter().cloned())
                        .with_profiles(config.guardrails.shell_profiles.iter().cloned()),
                );
        }
        // The skill-bundle write boundary is a hard permission gate, not an
        // inherited convenience: it applies even when `inherit_guardrails`
        // is off, and only the designated skill editor opts out of it.
        if !subagent.skill_bundle_writer {
            definition = definition
                .with_tool_guardrail("SkillBundleWriteGuardrail", SkillBundleWriteGuardrail);
        }
        registry.register(definition);
    }
    Ok(Some(registry))
}

fn subagent_orchestrator(
    subagent: &SubAgentConfig,
    models: LlmModelController,
    tool_specs: Vec<agentos_interfaces::tool::ToolSpec>,
    skill_catalog: WorkspaceSkillCatalog,
    resource_index: ResourceIndex,
    memory_hydrator: Option<(Arc<MemoryManager>, MemoryHydrationSettings)>,
) -> Result<Arc<dyn Orchestrator>, String> {
    let tier = LlmModelTier::from_config(&subagent.model_tier)?;
    match subagent.orchestrator.as_ref() {
        "builtin.echo" => Ok(Arc::new(EchoOrchestrator)),
        "builtin.min" | "builtin.llm" | "builtin.llm_fallback" => Ok(Arc::new(
            MinOrchestrator::new(Arc::new(EnvLlm::new(tier, models)?)).with_tools(tool_specs),
        )),
        "builtin.max" | "builtin.tool_selecting" => {
            let mut orchestrator = MaxOrchestrator::with_tools(tool_specs)
                .with_resource_index(resource_index)
                .with_skill_catalog(skill_catalog)
                .with_llm(Arc::new(EnvLlm::new(tier, models)?));
            if let Some((manager, settings)) = memory_hydrator {
                orchestrator = orchestrator.with_memory_hydrator(manager, settings);
            }
            Ok(Arc::new(orchestrator))
        }
        _ => {
            let mut orchestrator = MaxOrchestrator::with_tools(tool_specs)
                .with_resource_index(resource_index)
                .with_skill_catalog(skill_catalog)
                .with_llm(Arc::new(EnvLlm::new(tier, models)?));
            if let Some((manager, settings)) = memory_hydrator {
                orchestrator = orchestrator.with_memory_hydrator(manager, settings);
            }
            Ok(Arc::new(orchestrator))
        }
    }
}

fn main_max_turns(config: &WorkspaceConfig) -> usize {
    config.agent.max_turns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        McpServerConfig, McpToolConfig, ResourceConfig, ResourceSection, DEFAULT_SHELL_ALLOWLIST,
    };
    use agentos_interfaces::guardrail::{GuardrailOutcome, ToolGuardrail};
    use agentos_interfaces::tool::SandboxMode;
    use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
    use serde_json::value::RawValue;

    #[test]
    fn main_max_turns_uses_agent_config() {
        let mut config = WorkspaceConfig::default();
        config.agent.max_turns = 23;

        assert_eq!(main_max_turns(&config), 23);
    }

    #[tokio::test]
    async fn mcp_registration_follows_resources_mcp_enabled() {
        let config = WorkspaceConfig {
            mcp_servers: vec![McpServerConfig {
                id: Arc::from("static-mcp"),
                endpoint: Arc::from("static://local"),
                timeout_ms: None,
            }],
            mcp_tools: vec![
                McpToolConfig {
                    server_id: Arc::from("static-mcp"),
                    name: Arc::from("enabled_mcp"),
                    description: Arc::from("enabled"),
                    response: Arc::from("ok"),
                    sandbox: SandboxMode::FullAccess,
                },
                McpToolConfig {
                    server_id: Arc::from("static-mcp"),
                    name: Arc::from("disabled_mcp"),
                    description: Arc::from("disabled"),
                    response: Arc::from("no"),
                    sandbox: SandboxMode::FullAccess,
                },
            ],
            resources: ResourceConfig {
                mcp: ResourceSection {
                    enabled: vec![Arc::from("enabled_mcp")],
                },
                ..Default::default()
            },
            ..Default::default()
        };
        let mut tools = ToolRegistry::new();

        let specs = register_configured_mcp(&mut tools, &config)
            .await
            .expect("MCP registers");

        assert_eq!(specs.len(), 1);
        assert!(tools.contains("enabled_mcp"));
        assert!(!tools.contains("disabled_mcp"));
    }

    #[tokio::test]
    async fn default_shell_guardrail_allows_readonly_inspection_commands() {
        let guardrail = ShellCommandAllowlist::new(DEFAULT_SHELL_ALLOWLIST);
        for command in DEFAULT_SHELL_ALLOWLIST {
            let args = RawValue::from_string(format!(r#"{{"command":"{command}"}}"#)).unwrap();
            let call = ToolCall {
                id: ToolCallId::new(format!("shell-{command}")),
                name: Arc::from("shell"),
                args,
            };

            let outcome = guardrail
                .check_call(&call, &test_run_context())
                .await
                .expect("guardrail evaluates");

            assert_eq!(outcome, GuardrailOutcome::Passed, "{command} should pass");
        }
    }

    #[tokio::test]
    async fn default_shell_guardrail_still_blocks_unlisted_commands() {
        let guardrail = ShellCommandAllowlist::new(DEFAULT_SHELL_ALLOWLIST);
        let args = RawValue::from_string(r#"{"command":"rm"}"#.to_owned()).unwrap();
        let call = ToolCall {
            id: ToolCallId::new("shell-rm"),
            name: Arc::from("shell"),
            args,
        };

        let outcome = guardrail
            .check_call(&call, &test_run_context())
            .await
            .expect("guardrail evaluates");

        assert!(matches!(outcome, GuardrailOutcome::Tripped(_)));
    }

    fn test_run_context<'a>() -> RunContext<'a> {
        let state = Box::leak(Box::new(agentos_interfaces::RunState::new(
            RunId::new("runtime-test"),
            AgentId::new("main-agent"),
        )));
        RunContext::from_state(state)
    }
    /// The `high` default reuses the conversation's own client rather than
    /// building a second one for the same model — and a configured tier is
    /// what picks, so `[compaction].model` is not a key that reads back
    /// correctly and changes nothing.
    #[test]
    fn the_summarizer_follows_the_configured_tier() {
        let controller = LlmModelController::default();
        let high = Arc::new(
            EnvLlm::new(LlmModelTier::High, controller.clone()).expect("a high client builds"),
        );

        let same = summarizer_for(LlmModelTier::High, &high, &controller).expect("high resolves");
        assert!(
            Arc::ptr_eq(&(high.clone() as Arc<dyn Llm>), &same),
            "the default tier must reuse the conversation's client"
        );

        for tier in [LlmModelTier::Medium, LlmModelTier::Low] {
            let other = summarizer_for(tier, &high, &controller).expect("a tier resolves");
            assert!(
                !Arc::ptr_eq(&(high.clone() as Arc<dyn Llm>), &other),
                "{tier:?} must get its own client, not the high one"
            );
        }
    }
}
