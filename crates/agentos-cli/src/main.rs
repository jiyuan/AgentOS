use agentos_cli::slash::{self, Parsed, SessionUsage, SlashCommand, SlashContext};
use agentos_core::channels::{feishu::FeishuChannel, telegram::TelegramChannel};
use agentos_core::crons::{CronSchedule, CronStore, CronTask};
use agentos_core::gateway::{GatewayRun, GatewayService};
use agentos_core::memory::{MemoryManager, SqliteStore};
use agentos_core::r#loop::{route, ApprovalBinding, ApprovalOutcome, ApprovalTicket, Routed};
use agentos_core::runner::{
    delete_paused_run, load_paused_run, save_paused_run, ResumeDecision, RunnerDeps,
};
use agentos_core::runtime::{AgentRuntime, OrchestratorStrategy, RuntimePaths};
use agentos_core::skills::{
    create_skill, validate_skill_dir, SkillCreation, SkillResourceKind, WorkspaceSkillCatalog,
};
use agentos_interfaces::orchestrator::StreamSink;
use agentos_interfaces::{Channel, ChannelError, Egress};
use agentos_llm::{env as agentos_env, LlmModelController};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId, SpanKind,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_subscriber::EnvFilter;

/// The sender the TUI stamps on every envelope. One local user, named once so
/// the approval binding and the envelope cannot drift apart.
const TUI_SENDER: &str = "user";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_path = load_startup_env().map_err(io::Error::other)?;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let home = agentos_interfaces::agentos_home(env_path.as_deref());
    let args = env::args().skip(1).collect::<Vec<_>>();
    let runtime_paths = runtime_paths(&home);
    if args.first().is_some_and(|arg| arg == "skill") {
        handle_skill_command(&args[1..], &runtime_paths.skills_dir)?;
        return Ok(());
    }

    let attachments_dir = attachments_dir_path(&home);
    let runtime =
        AgentRuntime::build_with(runtime_paths.clone(), &agentos_cli::semantic_index_factory)
            .await
            .map_err(io::Error::other)?;
    let deps_scope = runtime.deps_scope();
    let input_guardrails = deps_scope.input_guardrails();
    let output_guardrails = deps_scope.output_guardrails();
    let tool_guardrails = deps_scope.tool_guardrails();
    let mut deps =
        deps_scope.deps_with_guardrails(&input_guardrails, &output_guardrails, &tool_guardrails);
    let active_agent = runtime.active_agent.clone();
    let orchestrator_switch = runtime.orchestrator.strategy_handle();
    let model_controller = runtime.model_controller.clone();

    let state_path = state_path(&args, &home);
    if args.first().is_some_and(|arg| arg == "resume") {
        let channel = TuiChannel::new(
            ChannelId::new("tui"),
            ConversationId::new("terminal"),
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        );
        resume_from_disk(&channel, &state_path, &args, &deps).await?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "telegram-once") {
        let mut channel = TelegramChannel::from_env()?.with_attachments_root(&attachments_dir);
        run_channel_once(
            &mut channel,
            &state_path,
            &deps,
            &active_agent,
            RunId::new("telegram-once"),
        )
        .await?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "telegram-cron-smoke") {
        let channel = TelegramChannel::from_env()?.with_attachments_root(&attachments_dir);
        run_telegram_cron_smoke(&channel, &args, &deps).await?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "feishu-once") {
        let mut channel = FeishuChannel::from_env()?.with_attachments_root(&attachments_dir);
        run_channel_once(
            &mut channel,
            &state_path,
            &deps,
            &active_agent,
            RunId::new("feishu-once"),
        )
        .await?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "feishu-cron-smoke") {
        let channel = FeishuChannel::from_env()?.with_attachments_root(&attachments_dir);
        run_feishu_cron_smoke(&channel, &args, &deps).await?;
        return Ok(());
    }

    // Streaming TUI egress: a shared flag lets the token sink and the channel's
    // final send coordinate so the reply is printed once.
    //
    // Off unless `[channels] provisional_streaming` says otherwise — a `print!`
    // followed by `flush` cannot be retracted, so on this sink an output
    // guardrail is a check performed after the user has read the output (M6 /
    // `STATE-001`, ADR-0007). AGENTOS_TUI_STREAM=0 remains a kill switch for a
    // deployment that opted in and wants one session without it.
    let streamed = Arc::new(AtomicBool::new(false));
    if runtime.provisional_streaming() && tui_streaming_enabled() {
        let flag = Arc::clone(&streamed);
        let sink: StreamSink = Arc::new(move |delta: &str| {
            print!("{delta}");
            let _ = io::stdout().flush();
            flag.store(true, Ordering::Relaxed);
            // Egress is synchronous (stdout); hand back an already-ready future.
            Box::pin(std::future::ready(()))
        });
        deps.stream_sink = Some(sink);
    }
    let mut channel = TuiChannel::new(
        ChannelId::new("tui"),
        ConversationId::new("terminal"),
        Some(orchestrator_switch),
        Some(model_controller),
        Arc::clone(&streamed),
    );
    let cron_store = CronStore::new(&runtime_paths.cron_dir);
    let memory_manager = runtime.memory_manager.clone();
    let resources = TuiResources {
        skill_catalog: &runtime.skill_catalog,
        cron_store: &cron_store,
        memory_manager: memory_manager.as_ref(),
    };
    run_tui_loop(
        &mut channel,
        &state_path,
        &deps,
        &active_agent,
        runtime.session.as_ref(),
        &resources,
    )
    .await?;
    Ok(())
}

/// Streaming TUI output is on by default; `AGENTOS_TUI_STREAM=0|false|off`
/// reverts to a single buffered print per reply.
fn tui_streaming_enabled() -> bool {
    !matches!(
        env::var("AGENTOS_TUI_STREAM").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    )
}

fn load_startup_env() -> Result<Option<PathBuf>, String> {
    agentos_env::load_startup_env(&agentos_env::EnvLoadOptions {
        explicit_path: None,
        search_parent_dirs: true,
        allow_overrides: agentos_env::allow_env_overrides(),
    })
}

fn handle_skill_command(args: &[String], root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    match args.first().map(String::as_str) {
        Some("create") => {
            let Some(name) = args.get(1) else {
                return Err("usage: agentos skill create <name> <description> [--resources=scripts,references,assets]".into());
            };
            let resources_arg = args.iter().find_map(|arg| arg.strip_prefix("--resources="));
            let description = args
                .iter()
                .skip(2)
                .filter(|arg| !arg.starts_with("--resources="))
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            if description.trim().is_empty() {
                return Err("skill description is required".into());
            }
            let mut creation = SkillCreation::new(name, Arc::from(description));
            for resource in parse_skill_resources(resources_arg)? {
                creation = creation.with_resource(resource);
            }
            let skill = create_skill(root, &creation)?;
            println!("Created skill: {}", skill.path.display());
            println!("Validated Anthropic SKILL.md format: {}", skill.name);
        }
        Some("validate") => {
            let target = args.get(1).map(String::as_str);
            if target.is_none() || target == Some("--all") {
                let catalog = WorkspaceSkillCatalog::load_enabled(root, &[])?;
                for skill in catalog.skills() {
                    println!("valid: {} ({})", skill.name, skill.path.display());
                }
                if catalog.is_empty() {
                    println!("no skills found in {}", root.display());
                }
            } else if let Some(name) = target {
                let dir = root.join(skill_dir_name(name));
                let skill = validate_skill_dir(&dir)?;
                println!("valid: {} ({})", skill.name, skill.path.display());
            }
        }
        Some("list") => {
            let catalog = WorkspaceSkillCatalog::load_enabled(root, &[])?;
            for skill in catalog.skills() {
                println!("{} - {}", skill.name, skill.description);
            }
            if catalog.is_empty() {
                println!("no skills found in {}", root.display());
            }
        }
        _ => {
            println!("usage:");
            println!("  agentos skill create <name> <description> [--resources=scripts,references,assets]");
            println!("  agentos skill validate [--all|<name>]");
            println!("  agentos skill list");
        }
    }
    Ok(())
}

fn parse_skill_resources(
    input: Option<&str>,
) -> Result<Vec<SkillResourceKind>, Box<dyn std::error::Error>> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    let mut resources = Vec::new();
    for item in input.split(',').filter(|item| !item.trim().is_empty()) {
        match item.trim() {
            "scripts" => resources.push(SkillResourceKind::Scripts),
            "references" => resources.push(SkillResourceKind::References),
            "assets" => resources.push(SkillResourceKind::Assets),
            other => return Err(format!("unknown skill resource '{other}'").into()),
        }
    }
    Ok(resources)
}

fn skill_dir_name(input: &str) -> String {
    let mut output = String::new();
    let mut previous_hyphen = false;
    for ch in input.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            previous_hyphen = false;
        } else if !previous_hyphen {
            output.push('-');
            previous_hyphen = true;
        }
    }
    output.trim_matches('-').to_owned()
}

async fn run_telegram_cron_smoke(
    channel: &TelegramChannel,
    args: &[String],
    deps: &RunnerDeps<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_channel_cron_smoke(
        channel,
        args,
        deps,
        "AGENTOS_TELEGRAM_CHAT_ID",
        "daily-telegram-smoke",
        "telegram-cron-smoke",
        "daily cron smoke: summarize yesterday",
    )
    .await
}

async fn run_feishu_cron_smoke(
    channel: &FeishuChannel,
    args: &[String],
    deps: &RunnerDeps<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_channel_cron_smoke(
        channel,
        args,
        deps,
        "AGENTOS_FEISHU_ALLOWED_ID",
        "daily-feishu-smoke",
        "feishu-cron-smoke",
        "daily Feishu cron smoke: summarize yesterday",
    )
    .await
}

async fn run_channel_cron_smoke<C>(
    channel: &C,
    args: &[String],
    deps: &RunnerDeps<'_>,
    chat_id_env: &str,
    task_id: &str,
    run_id: &str,
    default_prompt: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Channel,
{
    let chat_id = env::var(chat_id_env).map_err(|_| format!("missing {chat_id_env}"))?;
    let prompt = args.get(1).map_or(default_prompt, String::as_str);
    let now = unix_now()?;
    let store = CronStore::new(slash::cron_dir_path());
    let mut scheduler = store.load_scheduler()?;
    if !scheduler
        .tasks()
        .iter()
        .any(|task| task.id.as_ref() == task_id)
    {
        // Wildcard expression so the smoke task is always due the moment the
        // command runs — this is a one-shot manual verification of the
        // cron -> envelope -> run path, not a real recurring schedule.
        scheduler.upsert_task(CronTask::new(
            task_id,
            channel.id(),
            ConversationId::new(chat_id),
            prompt,
            CronSchedule::new("* * * * *")?,
        ));
        store.save_scheduler(&scheduler)?;
    }
    let Some(invocation) = scheduler.due_invocations(now)?.into_iter().next() else {
        return Err(format!("{run_id} did not enqueue").into());
    };

    let gateway_service = GatewayService::new(deps, Arc::from(deps.active_agent.as_str()));
    match gateway_service
        .run_envelope(
            channel.egress().as_ref(),
            invocation.envelope,
            RunId::new(run_id.to_owned()),
        )
        .await
    {
        Ok(GatewayRun::Finished { state, .. }) => {
            scheduler.record_success(&invocation.task_id, unix_now()?)?;
            store.save_scheduler(&scheduler)?;
            print_trace(&state);
        }
        Ok(GatewayRun::Paused { .. }) => {
            scheduler.record_failure(
                &invocation.task_id,
                unix_now()?,
                Arc::from("cron run paused unexpectedly"),
            )?;
            store.save_scheduler(&scheduler)?;
            return Err(format!("{run_id} paused unexpectedly").into());
        }
        Err(err) => {
            scheduler.record_failure(
                &invocation.task_id,
                unix_now()?,
                Arc::from(err.to_string()),
            )?;
            store.save_scheduler(&scheduler)?;
            return Err(Box::new(err));
        }
    }
    Ok(())
}

async fn run_channel_once<C>(
    channel: &mut C,
    state_path: &Path,
    deps: &RunnerDeps<'_>,
    active_agent: &AgentId,
    run_id: RunId,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Channel,
{
    let gateway_service = GatewayService::new(deps, Arc::from(active_agent.as_str()));
    let Some(result) = gateway_service.receive_and_run(channel, run_id).await? else {
        return Ok(());
    };

    match result {
        GatewayRun::Finished { state, .. } => {
            print_trace(&state);
        }
        GatewayRun::Paused { paused, ticket, .. } => {
            save_paused_run(state_path, &paused)?;
            let Some(approval_id) = paused
                .state
                .pending_approval()
                .map(|approval| approval.id.clone())
            else {
                eprintln!("run paused without a pending approval");
                return Ok(());
            };

            let Some(decision) = await_approval(channel, &ticket).await else {
                eprintln!("paused run saved: {}", state_path.display());
                return Ok(());
            };
            let paused = load_paused_run(state_path)?;
            let outcome = gateway_service
                .resume(channel.egress().as_ref(), paused, &approval_id, decision)
                .await;
            delete_paused_run(state_path)?;
            match outcome? {
                GatewayRun::Finished { state, .. } => {
                    print_trace(&state);
                }
                GatewayRun::Paused { paused, .. } => {
                    save_paused_run(state_path, &paused)?;
                }
            }
        }
    }

    Ok(())
}

/// Block until someone answers the approval `ticket` names.
///
/// Roadmap G2: an answer has to name its prompt. The old rule — the next line
/// decides, `y` approves — meant any input at all authorised a tool call, which
/// is exactly the ambient authority the item removes. Input that is not an
/// answer is reported and the wait continues; `None` means the channel closed.
async fn await_approval<C>(channel: &mut C, ticket: &ApprovalTicket) -> Option<ResumeDecision>
where
    C: Channel,
{
    // The TUI has one local user, so the prompt is bound to the sender this
    // channel stamps on every envelope. Binding it anyway rather than passing
    // the check something that always matches: the day a second sender can
    // reach this loop, it should already be checked.
    let binding = ApprovalBinding::new(ticket.clone(), TUI_SENDER);
    loop {
        let answer = channel.receive().await?;
        match route(Some(&binding), &answer) {
            Routed::Decides {
                outcome: ApprovalOutcome::Approved,
                ..
            } => return Some(ResumeDecision::Approve),
            Routed::Decides { reason, .. } => {
                return Some(ResumeDecision::Reject {
                    reason: reason.unwrap_or_else(|| Arc::from("rejected by user")),
                })
            }
            Routed::Stale { ticket: named } => {
                println!("That approval ({named}) is not the one waiting. Answer {ticket}.");
            }
            Routed::NotYours { ticket: named } => {
                println!("Approval {named} was put to someone else.");
            }
            Routed::Unrelated => {
                println!(
                    "Waiting on approval {ticket}: reply /approve {ticket} or /deny {ticket}."
                );
            }
        }
    }
}

struct TuiResources<'a> {
    skill_catalog: &'a WorkspaceSkillCatalog,
    cron_store: &'a CronStore,
    memory_manager: &'a MemoryManager,
}

async fn run_tui_loop(
    channel: &mut TuiChannel,
    state_path: &Path,
    deps: &RunnerDeps<'_>,
    active_agent: &AgentId,
    session: &SqliteStore,
    resources: &TuiResources<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let gateway_service = GatewayService::new(deps, Arc::from(active_agent.as_str()));
    let session_usage = SessionUsage::new();
    let mut turn = 1_u64;
    loop {
        let mut input = match channel.receive_tui().await {
            Some(TuiInput::Envelope(input)) => input,
            Some(TuiInput::UsageHint(hint)) => {
                println!("{hint}");
                continue;
            }
            Some(TuiInput::Slash(SlashCommand::Clear)) => {
                // `/clear` moves this participant's epoch, so it is keyed
                // by the full principal (M3 deliverable 2). The TUI has one
                // participant — the person at the keyboard — and no sender to
                // distinguish, so the conversation-wide principal is who they
                // are here.
                let removed = session.clear_session(&Principal::conversation(
                    active_agent.clone(),
                    channel.id.clone(),
                    channel.conversation_id.clone(),
                ))?;
                println!("{}", slash::format_clear(removed, "TUI"));
                turn = 1;
                continue;
            }
            Some(TuiInput::Slash(cmd)) => {
                let ctx = SlashContext {
                    skill_catalog: resources.skill_catalog,
                    cron_store: resources.cron_store,
                    tool_registry: deps.tools,
                    memory_manager: resources.memory_manager,
                    orchestrator_handle: channel.orchestrator_strategy.as_ref(),
                    model_controller: channel.model_controller.as_ref(),
                    session_usage: Some(&session_usage),
                    agent_id: active_agent,
                    channel_id: &channel.id,
                    conversation_id: &channel.conversation_id,
                };
                println!("{}", slash::render(cmd, &ctx).await);
                continue;
            }
            None => return Ok(()),
        };
        input.metadata.insert(
            Arc::from("task_id"),
            serde_json::json!(channel.orchestrator_strategy().task_id()),
        );
        let result = match gateway_service
            .run_envelope(
                channel.egress().as_ref(),
                input,
                RunId::new(format!("cli-run-{turn}")),
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                // Close any partially streamed line before the error message.
                channel.finish_stream_line();
                println!("{}", user_facing_error_message(&err.to_string()));
                turn += 1;
                continue;
            }
        };

        match result {
            GatewayRun::Finished { state, .. } => {
                session_usage.record_run(&state.usage);
                print_trace(&state);
            }
            GatewayRun::Paused { paused, ticket, .. } => {
                save_paused_run(state_path, &paused)?;
                let Some(approval_id) = paused
                    .state
                    .pending_approval()
                    .map(|approval| approval.id.clone())
                else {
                    eprintln!("run paused without a pending approval");
                    turn += 1;
                    continue;
                };

                let Some(decision) = await_approval(channel, &ticket).await else {
                    eprintln!("paused run saved: {}", state_path.display());
                    return Ok(());
                };
                let paused = load_paused_run(state_path)?;
                let outcome = gateway_service
                    .resume(channel.egress().as_ref(), paused, &approval_id, decision)
                    .await;
                delete_paused_run(state_path)?;
                match outcome? {
                    GatewayRun::Finished { state, .. } => {
                        session_usage.record_run(&state.usage);
                        print_trace(&state);
                    }
                    GatewayRun::Paused { paused, .. } => {
                        save_paused_run(state_path, &paused)?;
                    }
                }
            }
        }
        turn += 1;
    }
}

async fn resume_from_disk<C>(
    channel: &C,
    state_path: &Path,
    args: &[String],
    deps: &RunnerDeps<'_>,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Channel,
{
    let paused = load_paused_run(state_path)?;
    let Some(approval_id) = paused
        .state
        .pending_approval()
        .map(|approval| approval.id.clone())
    else {
        eprintln!("paused run has no pending approval");
        return Ok(());
    };
    // The decision follows the optional path, so it sits at index 1 when the
    // path was omitted and at index 2 when it was given.
    let verb_at = if args.get(1).is_some_and(|arg| is_resume_decision(arg)) {
        1
    } else {
        2
    };
    let decision = match args.get(verb_at).map(String::as_str) {
        Some("reject") => ResumeDecision::Reject {
            reason: args.get(verb_at + 1).map_or_else(
                || Arc::from("rejected by user"),
                |reason| Arc::from(reason.as_str()),
            ),
        },
        Some("approve") | None => ResumeDecision::Approve,
        Some(other) => {
            return Err(format!("unknown resume decision: {other}").into());
        }
    };
    let gateway_service = GatewayService::new(deps, Arc::from(deps.active_agent.as_str()));
    let outcome = gateway_service
        .resume(channel.egress().as_ref(), paused, &approval_id, decision)
        .await;
    delete_paused_run(state_path)?;
    match outcome? {
        GatewayRun::Finished { state, .. } => print_trace(&state),
        GatewayRun::Paused { paused, .. } => {
            save_paused_run(state_path, &paused)?;
        }
    }
    Ok(())
}

fn user_facing_error_message(error: &str) -> String {
    if error.contains("insufficient_quota") {
        let mut message = "AgentOS reached OpenAI, but OpenAI returned insufficient_quota for the configured API project or organization. Check OpenAI Platform billing, project budget, org usage limits, and prepaid API credits.".to_owned();
        if let Some(request_id) = extract_openai_request_id(error) {
            message.push_str("\nOpenAI request id: ");
            message.push_str(&request_id);
        }
        return message;
    }

    let mut message =
        "AgentOS could not complete this request. See the gateway log for details.".to_owned();
    if let Some(request_id) = extract_openai_request_id(error) {
        message.push_str("\nOpenAI request id: ");
        message.push_str(&request_id);
    }
    message
}

fn extract_openai_request_id(error: &str) -> Option<String> {
    let (_, rest) = error.split_once("x-request-id=")?;
    let request_id = rest
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace())
        .next()?
        .trim();
    if request_id.is_empty() {
        None
    } else {
        Some(request_id.to_owned())
    }
}

fn print_trace(state: &agentos_interfaces::RunState) {
    if !cli_trace_enabled() {
        return;
    }
    eprintln!(
        "trace: run={}, plan={}, llm={}, assignments={}, subagents={}, suborchs={}",
        count_spans(state, SpanKind::Run),
        count_named_spans(state, SpanKind::State, "plan"),
        count_spans(state, SpanKind::Llm),
        count_trace_events(state, "orchestrator_task_assigned"),
        count_trace_events_with_prefix(state, "subagent_"),
        count_trace_events_with_prefix(state, "suborch_")
    );
}

fn cli_trace_enabled() -> bool {
    env_flag_enabled("AGENTOS_CLI_TRACE") || env_flag_enabled("AGENTOS_TRACE")
}

fn env_flag_enabled(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// The words that follow `resume` as a decision rather than a path.
///
/// `resume` takes an *optional* path — `resume`, `resume approve`, and
/// `resume <path> approve` are all valid — so position alone cannot say
/// whether the first argument is a path. Without this, `agentos resume
/// approve` looked for a paused run in a file named `approve` and reported
/// that it did not exist (M2 deliverable 4).
fn is_resume_decision(arg: &str) -> bool {
    matches!(arg, "approve" | "reject")
}

fn state_path(args: &[String], home: &Path) -> PathBuf {
    if args.first().is_some_and(|arg| arg == "resume") {
        if let Some(path) = args.get(1).filter(|arg| !is_resume_decision(arg)) {
            return PathBuf::from(path);
        }
    }
    home.join("workspace/runs/cli-run-1.json")
}

fn runtime_paths(home: &Path) -> RuntimePaths {
    RuntimePaths {
        agent_config_path: home.join("workspace/agent.toml"),
        session_db_path: home.join("workspace/agentos.sqlite"),
        trace_dir: home.join("workspace/traces"),
        workspace_root: home.to_path_buf(),
        skills_dir: home.join("workspace/skills"),
        cron_dir: home.join("workspace/crons"),
    }
}

fn attachments_dir_path(home: &Path) -> PathBuf {
    home.join("workspace/attachments")
}

fn count_spans(state: &agentos_interfaces::RunState, kind: SpanKind) -> usize {
    state
        .trace_spans
        .iter()
        .filter(|span| span.kind == kind)
        .count()
}

fn count_named_spans(state: &agentos_interfaces::RunState, kind: SpanKind, name: &str) -> usize {
    state
        .trace_spans
        .iter()
        .filter(|span| span.kind == kind && span.name.as_ref() == name)
        .count()
}

fn count_trace_events(state: &agentos_interfaces::RunState, name: &str) -> usize {
    state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref() == name)
        .count()
}

fn count_trace_events_with_prefix(state: &agentos_interfaces::RunState, prefix: &str) -> usize {
    state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref().starts_with(prefix))
        .count()
}

fn unix_now() -> Result<u64, Box<dyn std::error::Error>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

struct TuiChannel {
    id: ChannelId,
    conversation_id: ConversationId,
    orchestrator_strategy: Option<Arc<AtomicU8>>,
    model_controller: Option<LlmModelController>,
    /// Shared with the streaming sink: set true once any token has been printed
    /// incrementally this turn, so `send` (and the error path) emit only a
    /// terminating newline instead of re-printing the whole reply.
    streamed: Arc<AtomicBool>,
}

enum TuiInput {
    Envelope(Envelope),
    Slash(SlashCommand),
    UsageHint(String),
}

impl TuiChannel {
    fn new(
        id: ChannelId,
        conversation_id: ConversationId,
        orchestrator_strategy: Option<Arc<AtomicU8>>,
        model_controller: Option<LlmModelController>,
        streamed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            id,
            conversation_id,
            orchestrator_strategy,
            model_controller,
            streamed,
        }
    }

    /// Terminate a partially streamed line if any tokens were printed this turn,
    /// resetting the flag. Used on the error path where `send` is never reached.
    fn finish_stream_line(&self) {
        if self.streamed.swap(false, Ordering::Relaxed) {
            println!();
            let _ = io::stdout().flush();
        }
    }

    async fn receive_tui(&mut self) -> Option<TuiInput> {
        let input = self.read_line().await?;
        match slash::parse(&input) {
            Parsed::Cmd(cmd) => Some(TuiInput::Slash(cmd)),
            Parsed::Usage(hint) => Some(TuiInput::UsageHint(hint)),
            Parsed::NotSlash => Some(TuiInput::Envelope(self.envelope(input))),
        }
    }

    fn orchestrator_strategy(&self) -> OrchestratorStrategy {
        self.orchestrator_strategy
            .as_ref()
            .map(|current| OrchestratorStrategy::from_u8(current.load(Ordering::Relaxed)))
            .unwrap_or(OrchestratorStrategy::Max)
    }

    async fn read_line(&mut self) -> Option<String> {
        loop {
            print!("agentos> ");
            if io::stdout().flush().is_err() {
                return None;
            }

            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(0) => return None,
                Ok(_) => {}
                Err(_) => return None,
            }

            let input = input.trim();
            if input.eq_ignore_ascii_case("/exit")
                || input.eq_ignore_ascii_case("exit")
                || input.eq_ignore_ascii_case("/quit")
                || input.eq_ignore_ascii_case("quit")
            {
                return None;
            }
            if input.is_empty() {
                continue;
            }
            return Some(input.to_owned());
        }
    }

    fn envelope(&self, input: String) -> Envelope {
        Envelope {
            channel_id: self.id(),
            conversation_id: self.conversation_id.clone(),
            sender: Arc::from(TUI_SENDER),
            message: Message::text(MessageRole::User, input),
            metadata: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl Channel for TuiChannel {
    fn id(&self) -> ChannelId {
        self.id.clone()
    }

    async fn receive(&mut self) -> Option<Envelope> {
        self.read_line().await.map(|input| self.envelope(input))
    }

    fn egress(&self) -> Arc<dyn Egress> {
        Arc::new(TuiEgress {
            streamed: Arc::clone(&self.streamed),
        })
    }
}

/// The TUI's send half: stdout plus the shared "already streamed this turn" flag.
struct TuiEgress {
    streamed: Arc<AtomicBool>,
}

#[async_trait]
impl Egress for TuiEgress {
    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        // When the reply was streamed token-by-token this turn, the text is
        // already on screen — just close the line. Otherwise print it whole.
        if self.streamed.swap(false, Ordering::Relaxed) {
            println!();
        } else {
            println!("{}", env.message.content);
        }
        io::stdout()
            .flush()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    /// M2 deliverable 4. The path is optional, so the decision keyword has to
    /// be told apart from a path by name rather than by position — otherwise
    /// `resume approve` looks for a paused run in a file called `approve`.
    #[test]
    fn a_pathless_resume_uses_the_default_state_path() {
        let home = Path::new("/home/agentos");
        let default = home.join("workspace/runs/cli-run-1.json");

        assert_eq!(state_path(&argv(&["resume"]), home), default);
        assert_eq!(state_path(&argv(&["resume", "approve"]), home), default);
        assert_eq!(
            state_path(&argv(&["resume", "reject", "no thanks"]), home),
            default
        );
    }

    #[test]
    fn an_explicit_resume_path_still_wins() {
        let home = Path::new("/home/agentos");
        assert_eq!(
            state_path(&argv(&["resume", "/tmp/run.json"]), home),
            PathBuf::from("/tmp/run.json")
        );
        assert_eq!(
            state_path(&argv(&["resume", "/tmp/run.json", "reject"]), home),
            PathBuf::from("/tmp/run.json")
        );
    }

    /// Only `resume` takes a state path; every other command uses the default.
    #[test]
    fn other_commands_are_unaffected() {
        let home = Path::new("/home/agentos");
        let default = home.join("workspace/runs/cli-run-1.json");
        assert_eq!(state_path(&argv(&[]), home), default);
        assert_eq!(state_path(&argv(&["telegram-once"]), home), default);
        assert_eq!(state_path(&argv(&["approve"]), home), default);
    }
}
