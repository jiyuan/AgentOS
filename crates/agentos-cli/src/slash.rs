//! Shared slash-command parser and renderers for the TUI and channel gateways.
//!
//! `parse` is pure: it inspects a single line of user input and returns a
//! `SlashCommand` (or a usage hint). `render` consumes a parsed command plus
//! a [`SlashContext`] of borrowed runtime resources and produces a response
//! string; callers decide whether to print it (TUI) or wrap it in a reply
//! envelope (chat channel).
//!
//! `Clear` is intentionally separate from `render` because it mutates session
//! state — each caller invokes its own `Session::clear_session` and then asks
//! `format_clear` for the user-facing confirmation.

use agentos_core::crons::CronStore;
use agentos_core::memory::{
    HydrationRequest, MemoryCaller, MemoryManager, MemoryStore, RetrievalStrategy,
};
use agentos_core::runtime::OrchestratorStrategy;
use agentos_core::skills::WorkspaceSkillCatalog;
use agentos_core::tools::ToolRegistry;
use agentos_llm::{configured_selection_for_tier, LlmModelController, LlmModelTier};
use agentos_proto::{AgentId, ConversationId, TaskId, Usage};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

/// Process-lifetime accumulator of token usage across every run in a session.
/// Cloneable and shared (`Arc<Mutex<…>>`) so the surface loop can fold each
/// finished run's `RunState.usage` in while `/usage` reads the running total.
/// A poisoned lock degrades to dropping the sample / reporting zeros rather
/// than panicking the loop.
#[derive(Clone, Default)]
pub struct SessionUsage {
    total: Arc<Mutex<Usage>>,
}

impl SessionUsage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one finished run's accumulated usage into the session total.
    pub fn record_run(&self, run: &Usage) {
        if let Ok(mut guard) = self.total.lock() {
            guard.merge(run);
        }
    }

    pub fn snapshot(&self) -> Usage {
        self.total.lock().map(|guard| *guard).unwrap_or_default()
    }
}

/// One parsed slash command. Variants carry only parsed arguments — never
/// borrowed runtime state — so callers can route them across surfaces freely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlashCommand {
    Help,
    Clear,
    ListSkills,
    ListCrons,
    ListTools,
    ListMemory,
    ShowOrchestrator,
    SetOrchestrator(OrchestratorStrategy),
    ShowModel,
    ResetModel,
    SetModel(String),
    ShowUsage,
}

/// Result of parsing a single line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Parsed {
    /// Not a slash command — treat as agent input.
    NotSlash,
    /// Recognised slash command.
    Cmd(SlashCommand),
    /// Looked like a slash command but the args were invalid; the string is
    /// a one-line usage hint suitable for surfacing to the user.
    Usage(String),
}

/// Borrowed references each renderer may need. Optional fields cover modes
/// where the resource isn't available (e.g., the resume CLI path passes no
/// orchestrator handle).
pub struct SlashContext<'a> {
    pub skill_catalog: &'a WorkspaceSkillCatalog,
    pub cron_store: &'a CronStore,
    pub tool_registry: Option<&'a ToolRegistry>,
    pub memory_manager: &'a MemoryManager,
    pub orchestrator_handle: Option<&'a Arc<AtomicU8>>,
    pub model_controller: Option<&'a LlmModelController>,
    pub session_usage: Option<&'a SessionUsage>,
    pub agent_id: &'a AgentId,
    pub conversation_id: &'a ConversationId,
}

pub fn parse(input: &str) -> Parsed {
    let trimmed = input.trim();
    if trimmed.eq_ignore_ascii_case("clear") || trimmed.eq_ignore_ascii_case("/clear") {
        return Parsed::Cmd(SlashCommand::Clear);
    }
    if trimmed.eq_ignore_ascii_case("/help") || trimmed.eq_ignore_ascii_case("/?") {
        return Parsed::Cmd(SlashCommand::Help);
    }
    if trimmed.strip_prefix("/help ").is_some() {
        return Parsed::Cmd(SlashCommand::Help);
    }
    if let Some(parsed) = parse_simple_list(trimmed, "/skills", SlashCommand::ListSkills) {
        return parsed;
    }
    if let Some(parsed) = parse_simple_list(trimmed, "/crons", SlashCommand::ListCrons) {
        return parsed;
    }
    if let Some(parsed) = parse_simple_list(trimmed, "/tools", SlashCommand::ListTools) {
        return parsed;
    }
    if let Some(parsed) = parse_simple_list(trimmed, "/memory", SlashCommand::ListMemory) {
        return parsed;
    }
    if let Some(parsed) = parse_simple_list(trimmed, "/usage", SlashCommand::ShowUsage) {
        return parsed;
    }
    if let Some(parsed) = parse_orchestrator(trimmed) {
        return parsed;
    }
    if let Some(parsed) = parse_model(trimmed) {
        return parsed;
    }
    Parsed::NotSlash
}

fn parse_simple_list(input: &str, command: &str, cmd: SlashCommand) -> Option<Parsed> {
    if input.eq_ignore_ascii_case(command) {
        return Some(Parsed::Cmd(cmd));
    }
    let with_space = format!("{command} ");
    if let Some(rest) = strip_prefix_ascii(input, &with_space) {
        let arg = rest.trim();
        if arg.is_empty() || arg.eq_ignore_ascii_case("list") || arg.eq_ignore_ascii_case("status")
        {
            return Some(Parsed::Cmd(cmd));
        }
        return Some(Parsed::Usage(format!("Usage: {command} [list|status]")));
    }
    None
}

fn parse_orchestrator(input: &str) -> Option<Parsed> {
    if input.eq_ignore_ascii_case("/orchestrator") {
        return Some(Parsed::Cmd(SlashCommand::ShowOrchestrator));
    }
    let rest = strip_prefix_ascii(input, "/orchestrator ")?;
    let arg = rest.trim();
    if arg.is_empty() || arg.eq_ignore_ascii_case("status") {
        return Some(Parsed::Cmd(SlashCommand::ShowOrchestrator));
    }
    match OrchestratorStrategy::from_config(arg) {
        Ok(strategy) => Some(Parsed::Cmd(SlashCommand::SetOrchestrator(strategy))),
        Err(err) => Some(Parsed::Usage(format!(
            "{err}\nUsage: /orchestrator <max|min|status>"
        ))),
    }
}

fn parse_model(input: &str) -> Option<Parsed> {
    if input.eq_ignore_ascii_case("/model") {
        return Some(Parsed::Cmd(SlashCommand::ShowModel));
    }
    let rest = strip_prefix_ascii(input, "/model ")?;
    let arg = rest.trim();
    if arg.is_empty() || arg.eq_ignore_ascii_case("status") {
        return Some(Parsed::Cmd(SlashCommand::ShowModel));
    }
    if arg.eq_ignore_ascii_case("reset") {
        return Some(Parsed::Cmd(SlashCommand::ResetModel));
    }
    Some(Parsed::Cmd(SlashCommand::SetModel(arg.to_owned())))
}

/// `str::strip_prefix` but case-insensitive on ASCII.
///
/// Compares the leading bytes directly rather than `split_at`-ing the string:
/// for a non-ASCII `input` (e.g. a CJK message), `prefix.len()` can land in
/// the middle of a multi-byte char and `split_at` would panic. When the
/// prefix matches, every prefix byte is ASCII, so `prefix.len()` is
/// guaranteed to be a valid char boundary and the slice is safe.
fn strip_prefix_ascii<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let input_bytes = input.as_bytes();
    let prefix_bytes = prefix.as_bytes();
    if input_bytes.len() < prefix_bytes.len() {
        return None;
    }
    if input_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes) {
        Some(&input[prefix_bytes.len()..])
    } else {
        None
    }
}

/// Render a parsed command (other than `Clear`) into a response string.
///
/// `Clear` is excluded because it requires session mutation; callers handle
/// that themselves and use [`format_clear`] for the confirmation text.
pub async fn render(cmd: SlashCommand, ctx: &SlashContext<'_>) -> String {
    match cmd {
        SlashCommand::Help => format_help(),
        SlashCommand::Clear => format_clear_unavailable(),
        SlashCommand::ListSkills => format_skills(ctx.skill_catalog),
        SlashCommand::ListCrons => format_crons(ctx.cron_store),
        SlashCommand::ListTools => format_tools(ctx.tool_registry),
        SlashCommand::ListMemory => {
            format_memory(ctx.memory_manager, ctx.agent_id, ctx.conversation_id).await
        }
        SlashCommand::ShowOrchestrator => format_orchestrator_status(ctx.orchestrator_handle),
        SlashCommand::SetOrchestrator(strategy) => {
            format_orchestrator_set(ctx.orchestrator_handle, strategy)
        }
        SlashCommand::ShowModel => format_model_status(ctx.model_controller),
        SlashCommand::ResetModel => format_model_reset(ctx.model_controller),
        SlashCommand::SetModel(input) => format_model_set(ctx.model_controller, &input),
        SlashCommand::ShowUsage => format_usage(ctx.session_usage),
    }
}

/// Render the running session token total: input/output split, the prompt
/// cache hit/miss/write breakdown, and the number of LLM calls behind it.
pub fn format_usage(session_usage: Option<&SessionUsage>) -> String {
    let Some(session_usage) = session_usage else {
        return "Token usage: unavailable in this mode.".to_owned();
    };
    let usage = session_usage.snapshot();
    if usage.tool_calls == 0 {
        return "Token usage: no LLM calls recorded in this session yet.".to_owned();
    }
    let cached_input = usage.cache_read_tokens;
    let hit_rate = if usage.input_tokens == 0 {
        0.0
    } else {
        (cached_input as f64 / usage.input_tokens as f64) * 100.0
    };
    format!(
        "Session token usage ({calls} LLM call{plural}):\n  \
         Input:   {input} tokens\n  \
         Output:  {output} tokens\n  \
         Total:   {total} tokens\n  \
         Cache:   {read} read (hit), {write} write, {miss} miss \
         ({hit_rate:.1}% of input served from cache)",
        calls = usage.tool_calls,
        plural = if usage.tool_calls == 1 { "" } else { "s" },
        input = usage.input_tokens,
        output = usage.output_tokens,
        total = usage.total_tokens,
        read = usage.cache_read_tokens,
        write = usage.cache_write_tokens,
        miss = usage.cache_miss_tokens,
    )
}

pub fn format_help() -> String {
    let entries: &[(&str, &str)] = &[
        ("/help", "Show this list of slash commands."),
        ("/clear", "Clear the current conversation history."),
        (
            "/orchestrator [max|min|status]",
            "Show or set the orchestrator strategy.",
        ),
        (
            "/model [provider:model|status|reset]",
            "Show, override, or clear the LLM model.",
        ),
        ("/skills [list|status]", "List enabled workspace skills."),
        ("/crons [list|status]", "List scheduled cron tasks."),
        ("/tools [list|status]", "List registered tools."),
        (
            "/memory [list|status]",
            "List memory records visible to this session.",
        ),
        (
            "/usage [status]",
            "Show total token usage for this session.",
        ),
    ];
    let name_width = entries
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or(0);
    let mut out = String::from("Available slash commands:\n");
    for (name, description) in entries {
        out.push_str(&format!("  {name:<name_width$}  {description}\n"));
    }
    out.push_str("\nType any other text to send a message to the agent.");
    out
}

pub fn format_clear(removed: usize, channel_label: &str) -> String {
    format!("Cleared {channel_label} conversation history ({removed} items).")
}

fn format_clear_unavailable() -> String {
    "Clear is handled by the surface; this should be unreachable.".to_owned()
}

pub fn format_skills(catalog: &WorkspaceSkillCatalog) -> String {
    if catalog.is_empty() {
        return [
            "No workspace skills are enabled in this workspace.",
            "Define skills under the configured skills directory and enable them via",
            "`[resources.skills] enabled = [\"<name>\"]` in workspace/agent.toml.",
        ]
        .join("\n");
    }

    let skills: Vec<_> = catalog.skills().collect();
    let name_width = skills
        .iter()
        .map(|skill| skill.name.len())
        .max()
        .unwrap_or(0);
    let mut out = format!("Legacy skills ({}):\n", skills.len());
    for skill in skills {
        out.push_str(&format!(
            "  {:<width$}  {}\n",
            skill.name.as_ref(),
            skill.description.as_ref(),
            width = name_width
        ));
        out.push_str(&format!(
            "  {:<width$}  path: {}\n",
            "",
            skill.path.display(),
            width = name_width
        ));
    }
    out.trim_end().to_owned()
}

pub fn format_crons(store: &CronStore) -> String {
    let scheduler = match store.load_scheduler() {
        Ok(scheduler) => scheduler,
        Err(err) => {
            return format!(
                "Failed to load cron schedule from {}: {err}",
                store.root().display()
            )
        }
    };
    let tasks = scheduler.tasks();
    if tasks.is_empty() {
        return format!(
            "No scheduled cron tasks in {}.\nDefine tasks under <id>.toml in that directory.",
            store.root().display()
        );
    }

    let now = unix_now();
    let id_width = tasks.iter().map(|task| task.id.len()).max().unwrap_or(0);
    let mut out = format!("Scheduled crons ({}):\n", tasks.len());
    for task in tasks {
        let next_unix = match task.retry_state.next_retry_unix {
            Some(retry_at) => Some(retry_at),
            None => task.schedule.next_fire_unix(now).ok().flatten(),
        };
        let when = match next_unix {
            Some(next) if next <= now => "due now".to_owned(),
            Some(next) => format!("in {}", format_duration_seconds(next - now)),
            None => "no upcoming fire".to_owned(),
        };
        let status = if !task.enabled {
            "disabled"
        } else if task.retry_state.consecutive_failures > 0 {
            "retrying"
        } else {
            "enabled"
        };
        out.push_str(&format!(
            "  {:<width$}  cron '{}', next {}, {}\n",
            task.id.as_ref(),
            task.schedule.expression.as_ref(),
            when,
            status,
            width = id_width,
        ));
        out.push_str(&format!(
            "  {:<width$}    channel: {}, conversation: {}, prompt: {}\n",
            "",
            task.channel_id.as_str(),
            task.conversation_id.as_str(),
            truncate(task.prompt.as_ref(), 80),
            width = id_width,
        ));
        if task.retry_state.consecutive_failures > 0 {
            let last_error = task.retry_state.last_error.as_deref().unwrap_or("(none)");
            out.push_str(&format!(
                "  {:<width$}    failures: {}/{}, last error: {}\n",
                "",
                task.retry_state.consecutive_failures,
                task.retry.max_retries,
                last_error,
                width = id_width,
            ));
        }
    }
    out.trim_end().to_owned()
}

pub fn format_tools(registry: Option<&ToolRegistry>) -> String {
    let Some(registry) = registry else {
        return "No tool registry is wired into this run.".to_owned();
    };
    let mut specs = registry.specs();
    if specs.is_empty() {
        return "No tools are registered.".to_owned();
    }
    specs.sort_by(|left, right| left.name.cmp(&right.name));
    let name_width = specs.iter().map(|spec| spec.name.len()).max().unwrap_or(0);
    let mut out = format!("Registered tools ({}):\n", specs.len());
    for spec in &specs {
        let isolation = if spec.requires_isolation {
            " [requires_isolation]"
        } else {
            ""
        };
        out.push_str(&format!(
            "  {:<width$}  {}{}\n",
            spec.name.as_ref(),
            spec.description.as_ref(),
            isolation,
            width = name_width,
        ));
    }
    out.trim_end().to_owned()
}

pub async fn format_memory(
    manager: &MemoryManager,
    agent_id: &AgentId,
    conversation_id: &ConversationId,
) -> String {
    let caller = MemoryCaller {
        agent_id: agent_id.clone(),
        task_id: TaskId::new("main"),
        conversation_id: conversation_id.clone(),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    };
    let request = HydrationRequest {
        query: Arc::from(""),
        domain: None,
        max_fragments: 100,
        max_tokens: usize::MAX,
        stores: vec![
            MemoryStore::Working,
            MemoryStore::Episodic,
            MemoryStore::Semantic,
            MemoryStore::Procedural,
        ],
        strategy: RetrievalStrategy::Recency,
    };
    let fragments = match manager.hydrate(&caller, request).await {
        Ok(fragments) => fragments,
        Err(err) => return format!("Failed to query memory: {err}"),
    };
    if fragments.is_empty() {
        return "No memory records visible to this session.".to_owned();
    }
    let mut out = format!("Memory records ({}):\n", fragments.len());
    for fragment in &fragments {
        let id = fragment
            .id
            .as_ref()
            .map(|record_id| record_id.as_str())
            .unwrap_or("(no id)");
        let store = fragment
            .metadata
            .get("store")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let owner_kind = fragment
            .metadata
            .get("owner_kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let summary = fragment_summary(&fragment.body, 96);
        out.push_str(&format!(
            "  {id}  store={store}, owner={owner_kind}, ns={}\n",
            fragment.namespace.as_str()
        ));
        out.push_str(&format!("       {summary}\n"));
    }
    out.trim_end().to_owned()
}

pub fn format_orchestrator_status(handle: Option<&Arc<AtomicU8>>) -> String {
    let strategy = current_strategy(handle);
    format!("Current orchestrator strategy: {}.", strategy.name())
}

pub fn format_orchestrator_set(
    handle: Option<&Arc<AtomicU8>>,
    strategy: OrchestratorStrategy,
) -> String {
    let Some(handle) = handle else {
        return "Orchestrator strategy is not switchable in this mode.".to_owned();
    };
    handle.store(strategy as u8, Ordering::Relaxed);
    format!("Orchestrator strategy set to {}.", strategy.name())
}

fn current_strategy(handle: Option<&Arc<AtomicU8>>) -> OrchestratorStrategy {
    handle
        .map(|atomic| OrchestratorStrategy::from_u8(atomic.load(Ordering::Relaxed)))
        .unwrap_or(OrchestratorStrategy::Max)
}

pub fn format_model_status(controller: Option<&LlmModelController>) -> String {
    let Some(controller) = controller else {
        return "Current model: unavailable in this mode.".to_owned();
    };
    if let Some(selection) = controller.override_selection() {
        return format!("Current model override: {}.", selection.label());
    }
    match configured_selection_for_tier(LlmModelTier::High) {
        Ok(Some(selection)) => format!("Current model: {} (high tier default).", selection.label()),
        Ok(None) => "Current model: builtin.echo.".to_owned(),
        Err(err) => format!("Current model config error: {err}"),
    }
}

pub fn format_model_set(controller: Option<&LlmModelController>, input: &str) -> String {
    let Some(controller) = controller else {
        return "Model selection is unavailable in this mode.".to_owned();
    };
    match controller.set_override(input) {
        Ok(selection) => format!("Model set to {}.", selection.label()),
        Err(err) => format!("{err}\nUsage: /model <provider:model|status|reset>"),
    }
}

pub fn format_model_reset(controller: Option<&LlmModelController>) -> String {
    let Some(controller) = controller else {
        return "Model selection is unavailable in this mode.".to_owned();
    };
    controller.clear_override();
    "Model override cleared.".to_owned()
}

pub fn cron_dir_path() -> PathBuf {
    agentos_interfaces::agentos_home(None)
        .join("workspace")
        .join("crons")
}

fn fragment_summary(body: &serde_json::Value, max: usize) -> String {
    if let Some(text) = body.get("summary").and_then(serde_json::Value::as_str) {
        return truncate(text, max);
    }
    if let Some(text) = body.as_str() {
        return truncate(text, max);
    }
    truncate(&body.to_string(), max)
}

fn format_duration_seconds(seconds: u64) -> String {
    if seconds == 0 {
        return "0s".to_owned();
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let mins = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if mins > 0 {
        parts.push(format!("{mins}m"));
    }
    if secs > 0 && parts.len() < 2 {
        parts.push(format!("{secs}s"));
    }
    parts.join(" ")
}

fn truncate(input: &str, max: usize) -> String {
    if input.chars().count() <= max {
        return input.to_owned();
    }
    let mut out: String = input.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn unix_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_recognises_help_aliases() {
        assert_eq!(parse("/help"), Parsed::Cmd(SlashCommand::Help));
        assert_eq!(parse("/?"), Parsed::Cmd(SlashCommand::Help));
        assert_eq!(parse("/help skills"), Parsed::Cmd(SlashCommand::Help));
        assert_eq!(parse("/HELP"), Parsed::Cmd(SlashCommand::Help));
    }

    #[test]
    fn parse_recognises_clear_aliases() {
        assert_eq!(parse("clear"), Parsed::Cmd(SlashCommand::Clear));
        assert_eq!(parse("/clear"), Parsed::Cmd(SlashCommand::Clear));
        assert_eq!(parse("CLEAR"), Parsed::Cmd(SlashCommand::Clear));
    }

    #[test]
    fn parse_simple_lists() {
        assert_eq!(parse("/skills"), Parsed::Cmd(SlashCommand::ListSkills));
        assert_eq!(parse("/skills list"), Parsed::Cmd(SlashCommand::ListSkills));
        assert_eq!(
            parse("/skills status"),
            Parsed::Cmd(SlashCommand::ListSkills)
        );
        assert!(matches!(parse("/skills foo"), Parsed::Usage(_)));
        assert_eq!(parse("/crons"), Parsed::Cmd(SlashCommand::ListCrons));
        assert_eq!(parse("/tools"), Parsed::Cmd(SlashCommand::ListTools));
        assert_eq!(parse("/memory"), Parsed::Cmd(SlashCommand::ListMemory));
    }

    #[test]
    fn parse_orchestrator_variants() {
        assert_eq!(
            parse("/orchestrator"),
            Parsed::Cmd(SlashCommand::ShowOrchestrator)
        );
        assert_eq!(
            parse("/orchestrator status"),
            Parsed::Cmd(SlashCommand::ShowOrchestrator)
        );
        assert_eq!(
            parse("/orchestrator max"),
            Parsed::Cmd(SlashCommand::SetOrchestrator(OrchestratorStrategy::Max))
        );
        assert_eq!(
            parse("/orchestrator min"),
            Parsed::Cmd(SlashCommand::SetOrchestrator(OrchestratorStrategy::Min))
        );
        assert!(matches!(parse("/orchestrator wat"), Parsed::Usage(_)));
    }

    #[test]
    fn parse_model_variants() {
        assert_eq!(parse("/model"), Parsed::Cmd(SlashCommand::ShowModel));
        assert_eq!(parse("/model status"), Parsed::Cmd(SlashCommand::ShowModel));
        assert_eq!(parse("/model reset"), Parsed::Cmd(SlashCommand::ResetModel));
        assert_eq!(
            parse("/model openai:gpt-5.4-mini"),
            Parsed::Cmd(SlashCommand::SetModel("openai:gpt-5.4-mini".to_owned()))
        );
    }

    #[test]
    fn parse_usage_variants() {
        assert_eq!(parse("/usage"), Parsed::Cmd(SlashCommand::ShowUsage));
        assert_eq!(parse("/usage status"), Parsed::Cmd(SlashCommand::ShowUsage));
        assert_eq!(parse("/USAGE"), Parsed::Cmd(SlashCommand::ShowUsage));
        assert!(matches!(parse("/usage foo"), Parsed::Usage(_)));
        assert_eq!(parse("/usagefoo"), Parsed::NotSlash);
    }

    #[test]
    fn format_usage_reports_no_calls_until_a_run_completes() {
        let session = SessionUsage::new();
        assert_eq!(
            format_usage(Some(&session)),
            "Token usage: no LLM calls recorded in this session yet."
        );
        assert_eq!(format_usage(None), "Token usage: unavailable in this mode.");
    }

    #[test]
    fn format_usage_accumulates_runs_and_reports_cache_hit_rate() {
        let session = SessionUsage::new();
        // Two runs, three LLM calls total; 1500 of 2000 input tokens cached.
        session.record_run(&Usage {
            input_tokens: 1000,
            output_tokens: 100,
            total_tokens: 1100,
            cache_read_tokens: 800,
            cache_write_tokens: 0,
            cache_miss_tokens: 200,
            tool_calls: 2,
        });
        session.record_run(&Usage {
            input_tokens: 1000,
            output_tokens: 50,
            total_tokens: 1050,
            cache_read_tokens: 700,
            cache_write_tokens: 0,
            cache_miss_tokens: 300,
            tool_calls: 1,
        });
        let rendered = format_usage(Some(&session));
        assert!(rendered.contains("3 LLM calls"), "{rendered}");
        assert!(rendered.contains("Input:   2000 tokens"), "{rendered}");
        assert!(rendered.contains("Output:  150 tokens"), "{rendered}");
        assert!(rendered.contains("Total:   2150 tokens"), "{rendered}");
        assert!(
            rendered.contains("1500 read (hit), 0 write, 500 miss (75.0% of input served"),
            "{rendered}"
        );
    }

    #[test]
    fn parse_returns_not_slash_for_plain_text() {
        assert_eq!(parse("hello there"), Parsed::NotSlash);
        assert_eq!(parse(""), Parsed::NotSlash);
    }

    #[test]
    fn parse_does_not_swallow_lookalike_commands() {
        // /skillsfoo isn't /skills with an arg.
        assert_eq!(parse("/skillsfoo"), Parsed::NotSlash);
    }

    #[test]
    fn parse_handles_non_ascii_without_panicking() {
        // Regression: `strip_prefix_ascii` used `split_at(prefix.len())`,
        // which panicked when a multi-byte char straddled that byte index
        // (e.g. a Korean/Greek message), crashing the gateway loop.
        assert_eq!(
            parse("인공지능의 장점을 한 문장으로 답해 주세요."),
            Parsed::NotSlash
        );
        assert_eq!(
            parse("Απάντησε με μία πρόταση για τη χρησιμότητα."),
            Parsed::NotSlash
        );
        assert_eq!(parse("日本語のメッセージ"), Parsed::NotSlash);
        // A real command with a non-ASCII argument still parses.
        assert_eq!(
            strip_prefix_ascii("/skills 日本語", "/skills "),
            Some("日本語")
        );
    }
}
