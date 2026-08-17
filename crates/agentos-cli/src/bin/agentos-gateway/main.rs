use agentos_cli::slash::{self, Parsed, SessionUsage, SlashCommand};
use agentos_core::channels::{feishu::FeishuChannel, telegram::TelegramChannel};
use agentos_core::config::WorkspaceConfig;
use agentos_core::crons::{CronSchedule, CronStore, MemoryMaintenanceCron};
use agentos_core::gateway::{
    shard_set, GatewayRun, GatewayService, Router, ShardConfig, DEFAULT_IDLE_INTERVAL,
};
use agentos_core::memory::MemoryManager;
use agentos_core::runner::ResumeDecision;
use agentos_core::runtime::{AgentRuntime, RuntimePaths};
use agentos_interfaces::orchestrator::StreamSink;
use agentos_interfaces::{Channel, Egress, StreamEgress};
use agentos_llm::env as agentos_env;
use agentos_proto::{ConversationId, Envelope, Message, MessageRole, RunId, SpanKind};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{self, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod shard;

use shard::{run_shard_thread, ShardContext};

const DEFAULT_PID_RELPATH: &str = "workspace/run/agentos-gateway.pid";

/// Channel edit-in-place streaming is on by default; set
/// `AGENTOS_GATEWAY_STREAM=0|false|off` to fall back to a single buffered reply.
fn gateway_streaming_enabled() -> bool {
    !matches!(
        env::var("AGENTOS_GATEWAY_STREAM").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    )
}

/// Build a [`StreamSink`] that forwards each assistant text delta to a channel's
/// edit-in-place [`StreamEgress`] for `conversation`. The closure owns the chunk
/// before awaiting, so the returned future is `'static`.
fn channel_stream_sink(egress: Arc<dyn StreamEgress>, conversation: ConversationId) -> StreamSink {
    Arc::new(move |delta: &str| {
        let egress = Arc::clone(&egress);
        let conversation = conversation.clone();
        let delta = delta.to_owned();
        Box::pin(async move { egress.push_delta(&conversation, &delta).await })
    })
}

/// Build the scheduled memory-reflection cron from `[memory.reflection]`, or
/// `None` when disabled. A malformed schedule expression is a hard config error.
fn build_reflection_cron(
    runtime: &AgentRuntime,
    config: &ServiceConfig,
) -> Result<Option<MemoryMaintenanceCron>, String> {
    let reflection = &runtime.workspace_config.memory.reflection;
    if !reflection.enabled {
        return Ok(None);
    }
    let schedule = CronSchedule::new(reflection.schedule.as_ref())
        .map_err(|err| format!("invalid [memory.reflection].schedule: {err}"))?;
    log_line(
        config,
        &format!("memory reflection scheduled: {}", reflection.schedule),
    )?;
    Ok(Some(MemoryMaintenanceCron::new(
        "memory-maintenance",
        runtime.active_agent.clone(),
        reflection.params(),
        schedule,
    )))
}

/// Drive the reflection cron on an idle tick, logging a one-line summary when a
/// sweep runs. Reflection is best-effort maintenance — a failure is logged, not
/// propagated, so it never takes the gateway loop down.
async fn run_memory_reflection(
    config: &ServiceConfig,
    channel_name: &str,
    cron: Option<&mut MemoryMaintenanceCron>,
    manager: &MemoryManager,
    now_unix: u64,
) -> Result<(), String> {
    let Some(cron) = cron else {
        return Ok(());
    };
    match cron.run_due(now_unix, manager).await {
        Ok(Some(report)) => log_line(
            config,
            &format!(
                "{channel_name} memory reflection: promoted {}, procedural {}, superseded {}, indexed {}",
                report.promoted_records.len(),
                report.procedural_candidates.len(),
                report.superseded_records.len(),
                report.index.indexed_records,
            ),
        ),
        Ok(None) => Ok(()),
        Err(err) => log_line(
            config,
            &format!("{channel_name} memory reflection failed: {err}"),
        ),
    }
}
const DEFAULT_LOG_RELPATH: &str = "logs/agentos-gateway.log";
const OWNER_TOKEN_ENV: &str = "AGENTOS_GATEWAY_OWNER_TOKEN";

/// How often the channel idle tick scans the cron store. Cron expressions
/// have one-minute resolution, so a 30s scan never misses a tick while
/// avoiding a re-read of every TOML on each one-second idle poll.
const CRON_SCAN_INTERVAL_SECS: u64 = 30;

#[cfg(unix)]
unsafe extern "C" {
    fn setsid() -> i32;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceConfig {
    home: PathBuf,
    pid_path: PathBuf,
    log_path: PathBuf,
    agent_config_path: Option<PathBuf>,
    session_db_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PidRecord {
    pid: u32,
    owner_token: Option<String>,
}

impl ServiceConfig {
    fn from_home(home: PathBuf) -> Self {
        Self {
            pid_path: home.join(DEFAULT_PID_RELPATH),
            log_path: home.join(DEFAULT_LOG_RELPATH),
            agent_config_path: None,
            session_db_path: None,
            home,
        }
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let env_path = load_startup_env(&args)?;
    let home = agentos_interfaces::agentos_home(env_path.as_deref());
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        usage();
        return Err("missing subcommand".to_owned());
    };
    let config = parse_config(args, home)?;

    match command.as_str() {
        "start" => start(config),
        "stop" => stop(&config),
        "restart" => {
            stop_if_running(&config)?;
            start(config)
        }
        "status" => status(&config),
        "config" => print_effective_config(&config),
        "serve" => serve(&config),
        "-h" | "--help" | "help" => {
            usage();
            Ok(())
        }
        other => Err(format!("unknown subcommand: {other}")),
    }
}

fn usage() {
    eprintln!(
        "\
Usage: agentos-gateway <start|stop|restart|status|config> [OPTIONS]

Manage the AgentOS gateway as a persistent background service.

Subcommands:
  start      Start the gateway service in the background.
  stop       Stop the running gateway service.
  restart    Stop then start the gateway service.
  status     Report whether the gateway service is running.
  config     Print the effective workspace config used by the gateway.

All workspace paths derive from $AGENTOS_HOME (set in .env or the process env).
If unset, $AGENTOS_HOME defaults to the parent dir of the loaded .env file,
or the current working directory.

Options:
  --pid-path PATH             PID file. Default: $AGENTOS_HOME/{DEFAULT_PID_RELPATH}
  --log-path PATH             Log file. Default: $AGENTOS_HOME/{DEFAULT_LOG_RELPATH}
  --config PATH               Agent workspace config path. Default: $AGENTOS_HOME/workspace/agent.toml
  --session-db-path PATH      Session database path. Default: $AGENTOS_HOME/workspace/agentos.sqlite
  --env-file PATH             Environment file. Default: {}
  --no-env-override           Keep already-exported shell variables over .env values.
  -h, --help                  Show this help.

Environment:
  AGENTOS_HOME                Workspace anchor. The only knob for path resolution.
  AGENTOS_ENV_FILE            Environment file. Default: {}
  AGENTOS_NO_ENV_OVERRIDE     Set to 1 to keep shell variables over .env values.",
        agentos_env::DEFAULT_ENV_PATH,
        agentos_env::DEFAULT_ENV_PATH
    );
}

fn parse_config<I>(args: I, home: PathBuf) -> Result<ServiceConfig, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = ServiceConfig::from_home(home);
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--pid-path" => {
                config.pid_path = next_path(&mut args, "--pid-path")?;
            }
            "--log-path" => {
                config.log_path = next_path(&mut args, "--log-path")?;
            }
            "--config" => {
                config.agent_config_path = Some(next_path(&mut args, "--config")?);
            }
            "--session-db-path" => {
                config.session_db_path = Some(next_path(&mut args, "--session-db-path")?);
            }
            "--env-file" => {
                let _ = next_path(&mut args, "--env-file")?;
            }
            option if option.starts_with("--env-file=") => {}
            "--no-env-override" => {}
            "-h" | "--help" => {
                usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown option: {other}")),
        }
    }
    Ok(config)
}

fn next_path<I>(args: &mut I, option: &str) -> Result<PathBuf, String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| format!("{option} requires a path"))
}

fn load_startup_env(args: &[String]) -> Result<Option<PathBuf>, String> {
    let loaded = agentos_env::load_startup_env(&agentos_env::EnvLoadOptions {
        explicit_path: discover_env_path_arg(args)?,
        search_parent_dirs: false,
        allow_overrides: agentos_env::allow_env_overrides()
            && !args.iter().any(|arg| arg == "--no-env-override"),
    })?;
    if let Some(path) = &loaded {
        eprintln!("Loaded environment file: {}", path.display());
    }
    Ok(loaded)
}

fn discover_env_path_arg(args: &[String]) -> Result<Option<PathBuf>, String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--env-file" {
            let Some(path) = iter.next() else {
                return Err("--env-file requires a path".to_owned());
            };
            return Ok(Some(PathBuf::from(path)));
        } else if let Some(value) = arg.strip_prefix("--env-file=") {
            return Ok(Some(PathBuf::from(value)));
        }
    }
    Ok(None)
}

fn start(config: ServiceConfig) -> Result<(), String> {
    ensure_parent_dir(&config.pid_path)?;
    ensure_parent_dir(&config.log_path)?;

    if let Some(pid) = read_pid(&config.pid_path)? {
        if process_is_running(pid) {
            println!(
                "AgentOS gateway is already running: pid {pid}, pid file {}",
                config.pid_path.display()
            );
            return Ok(());
        }
        eprintln!(
            "Removing stale AgentOS gateway pid file: {}",
            config.pid_path.display()
        );
        fs::remove_file(&config.pid_path).map_err(|err| {
            format!(
                "failed to remove stale pid file {}: {err}",
                config.pid_path.display()
            )
        })?;
    }

    let exe = env::current_exe().map_err(|err| format!("failed to locate executable: {err}"))?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.log_path)
        .map_err(|err| format!("failed to open log {}: {err}", config.log_path.display()))?;
    let err_log = log
        .try_clone()
        .map_err(|err| format!("failed to clone log handle: {err}"))?;

    let owner_token = gateway_owner_token()?;
    let mut command = Command::new(exe);
    command
        .arg("serve")
        .arg("--pid-path")
        .arg(&config.pid_path)
        .arg("--log-path")
        .arg(&config.log_path)
        .env(OWNER_TOKEN_ENV, &owner_token)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err_log));
    if let Some(path) = &config.agent_config_path {
        command.arg("--config").arg(path);
    }
    if let Some(path) = &config.session_db_path {
        command.arg("--session-db-path").arg(path);
    }
    detach_gateway_process(&mut command);

    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start gateway service: {err}"))?;
    let pid = child.id();
    write_pid_record(&config.pid_path, pid, Some(&owner_token))?;
    thread::sleep(Duration::from_secs(1));
    if let Some(status) = child
        .try_wait()
        .map_err(|err| format!("failed to inspect gateway service: {err}"))?
    {
        let _ = fs::remove_file(&config.pid_path);
        return Err(format!(
            "AgentOS gateway service exited during startup with {status}; see {}",
            config.log_path.display()
        ));
    }
    if !process_is_running(pid) {
        let _ = fs::remove_file(&config.pid_path);
        return Err(format!(
            "AgentOS gateway service exited during startup; see {}",
            config.log_path.display()
        ));
    }

    println!(
        "AgentOS gateway started: pid {pid}, pid file {}, log {}",
        config.pid_path.display(),
        config.log_path.display()
    );
    Ok(())
}

fn stop(config: &ServiceConfig) -> Result<(), String> {
    let Some(pid) = read_pid(&config.pid_path)? else {
        println!(
            "AgentOS gateway is not running: pid file {} does not exist",
            config.pid_path.display()
        );
        return Ok(());
    };

    if !process_is_running(pid) {
        fs::remove_file(&config.pid_path).map_err(|err| {
            format!(
                "failed to remove stale pid file {}: {err}",
                config.pid_path.display()
            )
        })?;
        println!("AgentOS gateway was not running; removed stale pid file");
        return Ok(());
    }

    send_signal(pid, "TERM")?;
    for _ in 0..50 {
        if !process_is_running(pid) {
            let _ = fs::remove_file(&config.pid_path);
            println!("AgentOS gateway stopped: pid {pid}");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    send_signal(pid, "KILL")?;
    let _ = fs::remove_file(&config.pid_path);
    println!("AgentOS gateway killed after timeout: pid {pid}");
    Ok(())
}

fn stop_if_running(config: &ServiceConfig) -> Result<(), String> {
    if read_pid(&config.pid_path)?.is_some() {
        stop(config)?;
    }
    Ok(())
}

#[cfg(unix)]
fn detach_gateway_process(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        command.pre_exec(|| {
            if setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn detach_gateway_process(_command: &mut Command) {}

fn status(config: &ServiceConfig) -> Result<(), String> {
    let Some(pid) = read_pid(&config.pid_path)? else {
        println!("AgentOS gateway status: stopped");
        return Ok(());
    };

    if process_is_running(pid) {
        println!("AgentOS gateway status: running, pid {pid}");
    } else {
        println!(
            "AgentOS gateway status: stale pid file {}, pid {pid}",
            config.pid_path.display()
        );
    }
    Ok(())
}

fn print_effective_config(config: &ServiceConfig) -> Result<(), String> {
    let path = agent_config_path(config);
    let runtime_paths = runtime_paths(config);
    let workspace_config = WorkspaceConfig::load(&path)
        .map_err(|err| format!("failed to load workspace config: {err}"))?;
    let channels = persistent_channels(&workspace_config)?;
    println!("config.path={}", path.display());
    println!(
        "paths.workspace_root={}",
        runtime_paths.workspace_root.display()
    );
    println!("paths.skills_dir={}", runtime_paths.skills_dir.display());
    println!("paths.cron_dir={}", runtime_paths.cron_dir.display());
    println!(
        "paths.attachments_dir={}",
        attachments_dir_path(config).display()
    );
    println!("paths.home={}", config.home.display());
    println!("agent.id={}", workspace_config.agent.id);
    println!("agent.orchestrator={}", workspace_config.agent.orchestrator);
    println!("agent.max_turns={}", workspace_config.agent.max_turns);
    println!("policy.default={}", workspace_config.policy.default);
    println!(
        "channels.tui={} ({})",
        workspace_config.channels.tui.enabled, workspace_config.channels.tui.mode
    );
    println!(
        "channels.telegram={} ({})",
        workspace_config.channels.telegram.enabled, workspace_config.channels.telegram.mode
    );
    println!(
        "channels.feishu={} ({})",
        workspace_config.channels.feishu.enabled, workspace_config.channels.feishu.mode
    );
    println!("channels.persistent={}", channels.join(","));
    println!(
        "gateway.shards={} (resolved: {})",
        workspace_config.gateway.shards,
        workspace_config.gateway.shard_count()
    );
    println!(
        "gateway.inbox_capacity={}",
        workspace_config.gateway.inbox_capacity
    );
    println!(
        "approval.expiry_seconds={}",
        workspace_config.approval.expiry_seconds
    );
    println!(
        "resources.priority={}",
        join_arcs(&workspace_config.resources.priority)
    );
    println!(
        "resources.skills.enabled={}",
        join_arcs(&workspace_config.resources.skills.enabled)
    );
    println!(
        "resources.tools.enabled={}",
        join_arcs(&workspace_config.resources.tools.enabled)
    );
    println!(
        "resources.mcp.enabled={}",
        join_arcs(&workspace_config.resources.mcp.enabled)
    );
    println!(
        "resources.llm.enabled={}",
        join_arcs(&workspace_config.resources.llm.enabled)
    );
    println!("subagents.count={}", workspace_config.subagents.len());
    println!(
        "orchestrator_templates.count={}",
        workspace_config.orchestrator_templates.len()
    );
    println!(
        "task_workspace.root={}",
        workspace_config.task_workspace.root.display()
    );
    Ok(())
}

fn join_arcs(values: &[Arc<str>]) -> String {
    values
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>()
        .join(",")
}

fn serve(config: &ServiceConfig) -> Result<(), String> {
    ensure_parent_dir(&config.pid_path)?;
    ensure_parent_dir(&config.log_path)?;
    wait_for_pid_ownership(config)?;
    log_line(config, "AgentOS gateway service starting")?;
    log_line(
        config,
        &format!(
            "config={}, session_db={}",
            display_optional_path(&config.agent_config_path),
            display_optional_path(&config.session_db_path)
        ),
    )?;

    let workspace_config = WorkspaceConfig::load(&agent_config_path(config))
        .map_err(|err| format!("failed to load workspace config: {err}"))?;
    let channels = persistent_channels(&workspace_config)?;
    if !channels.is_empty() {
        for channel in &channels {
            log_line(config, &format!("{channel} channel enabled"))?;
        }
        return run_persistent_gateways(config, &channels);
    }

    log_line(
        config,
        "no persistent channels enabled; enable [channels.telegram]/[channels.feishu] or set AGENTOS_ENABLED_CHANNELS=telegram,feishu",
    )?;
    loop {
        thread::sleep(Duration::from_secs(60));
        if !pid_file_owned_by_current_process(config)? {
            log_line(
                config,
                "AgentOS gateway exiting because pid file belongs to another process",
            )?;
            return Ok(());
        }
        log_line(config, "AgentOS gateway heartbeat")?;
    }
}

fn run_persistent_gateways(
    config: &ServiceConfig,
    channels: &[&'static str],
) -> Result<(), String> {
    let mut handles = Vec::new();
    for channel in channels {
        let config = config.clone();
        let channel = *channel;
        let handle = thread::spawn(move || run_persistent_channel(&config, channel));
        handles.push((channel, handle));
    }

    loop {
        thread::sleep(Duration::from_secs(1));
        if !pid_file_owned_by_current_process(config)? {
            log_line(
                config,
                "AgentOS gateway exiting because pid file belongs to another process",
            )?;
            return Ok(());
        }
        let mut index = 0;
        while index < handles.len() {
            if handles[index].1.is_finished() {
                let (channel, handle) = handles.remove(index);
                match handle.join() {
                    Ok(Ok(())) => {
                        log_line(config, &format!("{channel} gateway loop exited"))?;
                    }
                    Ok(Err(err)) => {
                        log_line(config, &format!("{channel} gateway loop failed: {err}"))?;
                        return Err(err);
                    }
                    Err(_) => {
                        let err = format!("{channel} gateway loop panicked");
                        log_line(config, &err)?;
                        return Err(err);
                    }
                }
            } else {
                index += 1;
            }
        }
        if handles.is_empty() {
            return Ok(());
        }
    }
}

fn run_persistent_channel(config: &ServiceConfig, channel: &'static str) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to start tokio runtime: {err}"))?;
    match channel {
        "telegram" => runtime.block_on(run_telegram_gateway(config)),
        "feishu" => runtime.block_on(run_feishu_gateway(config)),
        _ => Err(format!("unknown persistent channel: {channel}")),
    }
}

async fn run_telegram_gateway(config: &ServiceConfig) -> Result<(), String> {
    let attachments_dir = attachments_dir_path(config);
    let channel = TelegramChannel::from_env()
        .map_err(|err| format!("failed to configure telegram channel: {err}"))?
        .with_attachments_root(attachments_dir)
        .with_receive_error_logging(true);
    run_channel_gateway(config, channel, "telegram", RunId::new("telegram-gateway")).await
}

async fn run_feishu_gateway(config: &ServiceConfig) -> Result<(), String> {
    let attachments_dir = attachments_dir_path(config);
    let channel = FeishuChannel::from_env()
        .map_err(|err| format!("failed to configure feishu channel: {err}"))?
        .with_attachments_root(attachments_dir)
        .with_receive_error_logging(true);
    run_channel_gateway(config, channel, "feishu", RunId::new("feishu-gateway")).await
}

/// The gateway loop, as a router (roadmap item G1).
///
/// This thread does one thing: receive envelopes and hand each to the shard
/// that owns its conversation. It never runs a turn, so a slow tool on one
/// conversation cannot stop it receiving for the others — which was the whole
/// failure the serial receive-run-send loop had.
///
/// Two things bypass the queue. `/stop` is answered here because queueing it
/// behind the run it means to cancel would make it useless. The pid check stays
/// here because it is about this process, not any conversation.
async fn run_channel_gateway<C>(
    config: &ServiceConfig,
    mut channel: C,
    channel_name: &'static str,
    run_id: RunId,
) -> Result<(), String>
where
    C: Channel,
{
    let runtime = Arc::new(
        AgentRuntime::build_with(runtime_paths(config), &agentos_cli::semantic_index_factory)
            .await?,
    );
    log_line(config, &runtime.orchestrator.describe_llm())?;

    let gateway_config = runtime.workspace_config.gateway;
    let shard_config = ShardConfig {
        shards: gateway_config.shard_count(),
        inbox_capacity: gateway_config.inbox_capacity,
        idle_interval: DEFAULT_IDLE_INTERVAL,
    };
    let (router, inbounds) = shard_set(&shard_config);
    let egress = channel.egress();
    let stream_egress = gateway_streaming_enabled()
        .then(|| channel.stream_egress())
        .flatten();
    let session_usage = Arc::new(SessionUsage::new());

    let mut shards = Vec::with_capacity(inbounds.len());
    for (shard, inbound) in inbounds.into_iter().enumerate() {
        let context = ShardContext {
            shard,
            config: config.clone(),
            channel_name,
            run_id: run_id.clone(),
            runtime: Arc::clone(&runtime),
            egress: Arc::clone(&egress),
            stream_egress: stream_egress.clone(),
            session_usage: Arc::clone(&session_usage),
        };
        let handle = thread::Builder::new()
            .name(format!("agentos-{channel_name}-shard-{shard}"))
            .spawn(move || run_shard_thread(context, inbound, shard_config))
            .map_err(|err| format!("failed to start {channel_name} shard {shard}: {err}"))?;
        shards.push(handle);
    }
    log_line(
        config,
        &format!(
            "{channel_name} gateway routing across {} shard(s)",
            shard_config.shards
        ),
    )?;

    let outcome = route_inbound(config, &mut channel, channel_name, &router, egress.as_ref()).await;

    // Dropping the router closes every shard queue, which ends each shard once
    // it has drained. Join so a shard's in-flight turn finishes before the
    // process moves on.
    drop(router);
    for (shard, handle) in shards.into_iter().enumerate() {
        if handle.join().is_err() {
            log_line(config, &format!("{channel_name} shard {shard} panicked"))?;
        }
    }
    outcome
}

/// Receive and route until the pid file changes hands or the channel closes.
async fn route_inbound<C>(
    config: &ServiceConfig,
    channel: &mut C,
    channel_name: &str,
    router: &Router,
    egress: &dyn Egress,
) -> Result<(), String>
where
    C: Channel,
{
    loop {
        if !pid_file_owned_by_current_process(config)? {
            log_line(
                config,
                &format!(
                    "{channel_name} gateway loop exiting because pid file belongs to another process"
                ),
            )?;
            return Ok(());
        }
        let Some(input) = channel.receive().await else {
            // Idle. Maintenance used to run here, competing with receiving;
            // it now runs from a shard's idle phase, so this really is nothing
            // to do. The pause keeps a channel whose `receive` returns
            // immediately from spinning.
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };

        // `/stop` cannot queue behind the run it is trying to cancel.
        if matches!(
            slash::parse(&input.message.content),
            Parsed::Cmd(SlashCommand::Stop)
        ) {
            let stopped = match router.stop(&input.conversation_id).await {
                Ok(stopped) => stopped,
                Err(err) => {
                    log_line(config, &format!("{channel_name} stop failed: {err}"))?;
                    false
                }
            };
            let reply = command_reply_envelope(&input, channel_name, &slash::format_stop(stopped));
            if let Err(err) = egress.send(reply).await {
                log_line(
                    config,
                    &format!("{channel_name} gateway failed to send stop reply: {err}"),
                )?;
            }
            continue;
        }

        if let Err(err) = router.deliver(input).await {
            // Every shard is gone; there is nothing left to route to.
            log_line(config, &format!("{channel_name} routing failed: {err}"))?;
            return Ok(());
        }
    }
}

/// Replay every cron task that is due and bound to this channel as a synthetic
/// envelope through the normal run path, then persist the updated fire/retry
/// bookkeeping so the task does not re-fire on the next scan.
///
/// Cron failures are logged and swallowed — a broken task must never take down
/// the channel gateway loop. Each channel gateway only fires tasks whose
/// `channel_id` matches its own, so a multi-channel deployment neither
/// double-fires a task nor delivers it through the wrong transport. (A task
/// bound to a channel with no running persistent gateway therefore never
/// fires.)
async fn fire_due_crons(
    config: &ServiceConfig,
    egress: &dyn Egress,
    channel_name: &str,
    cron_store: &CronStore,
    gateway_service: &GatewayService<'_>,
    session_usage: &SessionUsage,
    now: u64,
) -> Result<(), String> {
    let mut scheduler = match cron_store.load_scheduler() {
        Ok(scheduler) => scheduler,
        Err(err) => {
            log_line(config, &format!("{channel_name} cron load failed: {err}"))?;
            return Ok(());
        }
    };
    // Anchor any task seen for the first time so it fires on its next
    // scheduled instant instead of back-firing on discovery, then persist the
    // anchor so the next scan agrees. Anchor values are tick-aligned and
    // therefore identical across concurrent channel gateways.
    match scheduler.anchor_unfired(now) {
        Ok(anchored) => {
            for id in anchored {
                if let Some(task) = scheduler.tasks().iter().find(|task| task.id == id) {
                    if let Err(err) = cron_store.save_task(task) {
                        log_line(
                            config,
                            &format!("{channel_name} cron '{id}' anchor persist failed: {err}"),
                        )?;
                    }
                }
            }
        }
        Err(err) => log_line(config, &format!("{channel_name} cron anchor failed: {err}"))?,
    }
    let due = match scheduler.due_invocations(now) {
        Ok(due) => due,
        Err(err) => {
            log_line(config, &format!("{channel_name} cron scan failed: {err}"))?;
            return Ok(());
        }
    };
    for invocation in due {
        if invocation.envelope.channel_id.as_str() != channel_name {
            continue;
        }
        let task_id = invocation.task_id.clone();
        log_line(
            config,
            &format!("{channel_name} cron '{task_id}' due; dispatching"),
        )?;
        let bookkeeping = match gateway_service
            .run_envelope(
                egress,
                invocation.envelope,
                RunId::new(format!("cron-{task_id}")),
            )
            .await
        {
            Ok(GatewayRun::Finished { state, .. }) => {
                session_usage.record_run(&state.usage);
                log_trace(config, &state)?;
                scheduler.record_success(&task_id, now)
            }
            Ok(GatewayRun::Paused { paused, ticket, .. }) => {
                // A cron tick has no one behind it, so the prompt it just sent
                // can never be answered. Resolve it as `Unavailable` rather
                // than leaving a paused run parked forever — and not as a
                // rejection, because nobody rejected anything.
                log_line(
                    config,
                    &format!(
                        "{channel_name} cron '{task_id}' paused for approval ({ticket}); \
                         no interactive user — resolving as unavailable"
                    ),
                )?;
                if let Some(approval_id) = paused
                    .state
                    .pending_approvals
                    .first()
                    .map(|approval| approval.id.clone())
                {
                    // Fails the run closed; the error is the expected shape.
                    let _ = gateway_service
                        .resume(
                            egress,
                            paused,
                            &approval_id,
                            ResumeDecision::Unavailable {
                                reason: Arc::from("no interactive user behind a cron run"),
                            },
                        )
                        .await;
                }
                scheduler.record_failure(
                    &task_id,
                    now,
                    Arc::from("cron run paused awaiting approval"),
                )
            }
            Err(err) => {
                log_line(
                    config,
                    &format!("{channel_name} cron '{task_id}' failed: {err}"),
                )?;
                scheduler.record_failure(&task_id, now, Arc::from(err.to_string()))
            }
        };
        if let Err(err) = bookkeeping {
            log_line(
                config,
                &format!("{channel_name} cron '{task_id}' bookkeeping failed: {err}"),
            )?;
            continue;
        }
        // Persist only the task we touched — writing the whole scheduler back
        // would clobber another channel gateway's concurrent updates.
        match scheduler.tasks().iter().find(|task| task.id == task_id) {
            Some(task) => {
                if let Err(err) = cron_store.save_task(task) {
                    log_line(
                        config,
                        &format!("{channel_name} cron '{task_id}' persist failed: {err}"),
                    )?;
                }
            }
            None => log_line(
                config,
                &format!("{channel_name} cron '{task_id}' vanished before persist"),
            )?,
        }
    }
    Ok(())
}

fn persistent_channels(config: &WorkspaceConfig) -> Result<Vec<&'static str>, String> {
    let override_channels = env::var("AGENTOS_ENABLED_CHANNELS").ok();
    persistent_channels_from_override(config, override_channels.as_deref())
}

fn persistent_channels_from_override(
    config: &WorkspaceConfig,
    override_channels: Option<&str>,
) -> Result<Vec<&'static str>, String> {
    if let Some(channels) = override_channels {
        let mut enabled = Vec::new();
        for channel in channels.split(',').map(str::trim) {
            match channel {
                "" | "tui" => {}
                "telegram" => push_unique_channel(&mut enabled, "telegram"),
                "feishu" => push_unique_channel(&mut enabled, "feishu"),
                other => return Err(format!("unknown persistent channel: {other}")),
            }
        }
        return Ok(enabled);
    }

    let mut enabled = Vec::new();
    if config.channels.telegram.enabled {
        enabled.push("telegram");
    }
    if config.channels.feishu.enabled {
        enabled.push("feishu");
    }
    Ok(enabled)
}

fn push_unique_channel(channels: &mut Vec<&'static str>, channel: &'static str) {
    if !channels.contains(&channel) {
        channels.push(channel);
    }
}

fn channel_display_name(name: &str) -> &str {
    match name {
        "feishu" => "Feishu",
        "telegram" => "Telegram",
        _ => "channel",
    }
}

fn agent_config_path(config: &ServiceConfig) -> PathBuf {
    config
        .agent_config_path
        .clone()
        .unwrap_or_else(|| config.home.join("workspace/agent.toml"))
}

fn runtime_paths(config: &ServiceConfig) -> RuntimePaths {
    RuntimePaths {
        agent_config_path: agent_config_path(config),
        session_db_path: session_path(config),
        trace_dir: config.home.join("workspace/traces"),
        workspace_root: config.home.clone(),
        skills_dir: config.home.join("workspace/skills"),
        cron_dir: config.home.join("workspace/crons"),
    }
}

fn session_path(config: &ServiceConfig) -> PathBuf {
    config
        .session_db_path
        .clone()
        .unwrap_or_else(|| config.home.join("workspace/agentos.sqlite"))
}

fn attachments_dir_path(config: &ServiceConfig) -> PathBuf {
    config.home.join("workspace/attachments")
}

fn log_trace(config: &ServiceConfig, state: &agentos_interfaces::RunState) -> Result<(), String> {
    log_line(
        config,
        &format!(
            "trace: run={}, plan={}, llm={}",
            count_spans(state, SpanKind::Run),
            count_named_spans(state, SpanKind::State, "plan"),
            count_spans(state, SpanKind::Llm)
        ),
    )
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

fn failure_envelope(input: &Envelope, sender: &str, error: &str) -> Envelope {
    command_reply_envelope(input, sender, &user_facing_error_message(error))
}

fn command_reply_envelope(input: &Envelope, sender: &str, content: &str) -> Envelope {
    Envelope {
        channel_id: input.channel_id.clone(),
        conversation_id: input.conversation_id.clone(),
        sender: Arc::from(sender),
        message: Message::text(MessageRole::Assistant, content),
        metadata: BTreeMap::new(),
    }
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

fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    }
    Ok(())
}

fn read_pid(path: &Path) -> Result<Option<u32>, String> {
    Ok(read_pid_record(path)?.map(|record| record.pid))
}

fn read_pid_record(path: &Path) -> Result<Option<PidRecord>, String> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let mut parts = contents.split_whitespace();
            let Some(pid) = parts.next() else {
                return Ok(None);
            };
            let pid = pid
                .parse::<u32>()
                .map_err(|err| format!("invalid pid in {}: {err}", path.display()))?;
            Ok(Some(PidRecord {
                pid,
                owner_token: parts.next().map(ToOwned::to_owned),
            }))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read {}: {err}", path.display())),
    }
}

fn write_pid_record(path: &Path, pid: u32, owner_token: Option<&str>) -> Result<(), String> {
    let contents = match owner_token {
        Some(owner_token) => format!("{pid} {owner_token}\n"),
        None => format!("{pid}\n"),
    };
    fs::write(path, contents)
        .map_err(|err| format!("failed to write pid file {}: {err}", path.display()))
}

fn wait_for_pid_ownership(config: &ServiceConfig) -> Result<(), String> {
    for _ in 0..20 {
        if pid_file_owned_by_current_process(config)? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let owner = read_pid(&config.pid_path)?
        .map(|pid| pid.to_string())
        .unwrap_or_else(|| "<missing>".to_owned());
    Err(format!(
        "pid file {} is not owned by this gateway process {}; owner={owner}",
        config.pid_path.display(),
        process::id()
    ))
}

fn pid_file_owned_by_current_process(config: &ServiceConfig) -> Result<bool, String> {
    let Some(record) = read_pid_record(&config.pid_path)? else {
        return Ok(false);
    };
    if let Ok(owner_token) = env::var(OWNER_TOKEN_ENV) {
        let token_matches =
            !owner_token.is_empty() && record.owner_token.as_deref() == Some(&owner_token);
        return Ok(token_matches || record.pid == process::id());
    }
    Ok(record.pid == process::id())
}

fn gateway_owner_token() -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("failed to generate gateway owner token: {err}"))?
        .as_nanos();
    Ok(format!("{}-{now}", process::id()))
}

fn process_is_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn send_signal(pid: u32, signal: &str) -> Result<(), String> {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .map_err(|err| format!("failed to invoke kill: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to send {signal} to pid {pid}"))
    }
}

fn log_line(config: &ServiceConfig, message: &str) -> Result<(), String> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system time error: {err}"))?
        .as_secs();
    let line = format!("[{ts}] {message}\n");
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.log_path)
        .and_then(|mut file| {
            use std::io::Write;
            file.write_all(line.as_bytes())
        })
        .map_err(|err| format!("failed to write log {}: {err}", config.log_path.display()))
}

fn display_optional_path(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map_or_else(|| "<unset>".to_owned(), |path| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_channels_default_to_config() {
        let mut config = WorkspaceConfig::default();
        config.channels.telegram.enabled = true;
        config.channels.feishu.enabled = false;

        assert_eq!(
            persistent_channels_from_override(&config, None).expect("channels resolve"),
            vec!["telegram"]
        );
    }

    #[test]
    fn persistent_channels_env_override_takes_precedence() {
        let mut config = WorkspaceConfig::default();
        config.channels.telegram.enabled = false;
        config.channels.feishu.enabled = false;

        assert_eq!(
            persistent_channels_from_override(&config, Some("feishu,telegram,telegram"))
                .expect("channels resolve"),
            vec!["feishu", "telegram"]
        );
        assert!(persistent_channels_from_override(&config, Some("slack")).is_err());
    }
}
