use agentos_cli::slash::SessionUsage;
use agentos_core::channels::{feishu::FeishuChannel, telegram::TelegramChannel};
use agentos_core::config::{effective, WorkspaceConfig};
use agentos_core::crons::{CronSchedule, CronStore, MemoryMaintenanceCron};
use agentos_core::gateway::{
    self, shard_set, ApprovalStore, GatewayRun, GatewayService, IngressLedger, ShardConfig,
    DEFAULT_IDLE_INTERVAL,
};
use agentos_core::memory::migrate::{self, MigrationSettings};
use agentos_core::memory::{
    lease_holder_id, MemoryManager, SqliteStore, DEFAULT_LEASE_TTL, REFLECTION_LEASE,
    RETENTION_LEASE,
};
use agentos_core::memory::{migrate_child_sessions, migrate_sessions};
use agentos_core::r#loop::{ApprovalBinding, ApprovalOutcome};
use agentos_core::retention::{RetentionSweep, RetentionTargets};
use agentos_core::runtime::{AgentRuntime, RuntimePaths};
use agentos_core::sandbox;
use agentos_interfaces::orchestrator::StreamSink;
use agentos_interfaces::{Channel, Egress, StreamEgress};
use agentos_llm::env as agentos_env;
use agentos_proto::{
    ActorPrincipal, AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId,
    SpanKind,
};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{self, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod approval;
mod calibrate;
mod catalog;
mod delivery;
mod ingress;
mod maintenance;
mod purge;
mod shard;

use delivery::send_approval_prompt;
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
        runtime.workspace_config.memory.reflection_params(),
        schedule,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceRun {
    Disabled,
    Standby,
    Checked,
    Ran,
}

impl MaintenanceRun {
    fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Standby => "standby",
            Self::Checked => "checked",
            Self::Ran => "ran",
        }
    }
}

/// Drive the reflection cron on its bounded timer, logging a one-line summary
/// when a sweep runs. Reflection is best-effort maintenance: a failure is
/// returned to the supervisor for diagnosis and never stops the gateway.
///
/// Guarded by the `memory.reflection` lease (M8 / `GW-001`, deliverable 2).
/// Shard 0 exists once per *channel*, so a Telegram-and-Feishu deployment used
/// to sweep the one database twice, concurrently — and a TUI on the same file
/// made it three. The lease is a row in that database, which is the only thing
/// all of them share.
async fn run_memory_reflection(
    config: &ServiceConfig,
    channel_name: &str,
    cron: Option<&mut MemoryMaintenanceCron>,
    manager: &MemoryManager,
    session: &SqliteStore,
    now_unix: u64,
) -> Result<MaintenanceRun, String> {
    let Some(cron) = cron else {
        return Ok(MaintenanceRun::Disabled);
    };
    // Asked on every scheduled scan rather than once at startup, because this
    // is also the renewal: a leader that stops asking loses the lease and
    // another contender picks the sweep up.
    let holder = lease_holder_id(channel_name);
    match session.try_acquire_lease(REFLECTION_LEASE, &holder, DEFAULT_LEASE_TTL) {
        Ok(Some(_lease)) => {}
        // Somebody else is the leader. The ordinary answer for every channel
        // but one, and not worth a log line every thirty seconds.
        Ok(None) => return Ok(MaintenanceRun::Standby),
        Err(err) => return Err(format!("could not acquire reflection lease: {err}")),
    }
    match cron.run_due(now_unix, manager).await {
        Ok(Some(report)) => {
            log_line(
                config,
                &format!(
                "{channel_name} memory reflection: promoted {}, procedural {}, superseded {}, indexed {}",
                report.promoted_records.len(),
                report.procedural_candidates.len(),
                report.superseded_records.len(),
                report.index.indexed_records,
            ),
            )?;
            Ok(MaintenanceRun::Ran)
        }
        Ok(None) => Ok(MaintenanceRun::Checked),
        Err(err) => Err(format!("memory reflection failed: {err}")),
    }
}
/// Apply `[retention]`, `[spill]` and `[jobs].completed_retention_secs` on the
/// slower bounded timer, logging one line when anything was actually removed
/// (M7 / `QUOTA-001`).
///
/// Guarded by its own lease because one independent timer runs per enabled
/// channel, and another serving process may share the database. Without the
/// lease those contenders would walk the same directories and process-owned
/// registries concurrently. A separate lease from reflection's because the two
/// are not one job — reflection is off by default and needs an LLM, retention
/// is on and needs nothing, and gating one on the other would silently disable
/// retention on most deployments.
///
/// Nothing here touches the session log or the audit stores. Those are deleted
/// only by `agentos-gateway purge`, which is the whole point of the split — see
/// `agentos_core::retention`.
async fn run_retention_sweep(
    config: &ServiceConfig,
    channel_name: &str,
    runtime: &AgentRuntime,
    ledger: &IngressLedger,
) -> Result<MaintenanceRun, String> {
    let workspace = &runtime.workspace_config;
    if !workspace.retention.sweeps_anything()
        && workspace.spill.retention_secs().is_none()
        && workspace.spill.max_bytes().is_none()
        && workspace.jobs.completed_job_max_age().is_none()
    {
        return Ok(MaintenanceRun::Disabled);
    }
    let holder = lease_holder_id(channel_name);
    match runtime
        .session
        .try_acquire_lease(RETENTION_LEASE, &holder, DEFAULT_LEASE_TTL)
    {
        Ok(Some(_lease)) => {}
        Ok(None) => return Ok(MaintenanceRun::Standby),
        Err(err) => return Err(format!("could not acquire retention lease: {err}")),
    }

    let targets = RetentionTargets {
        trace_dir: Some(runtime_paths(config).trace_dir),
        attachments_dir: Some(attachments_dir_path(config)),
        gateway_log: Some(config.log_path.clone()),
    };
    let report = RetentionSweep {
        retention: &workspace.retention,
        spill_config: &workspace.spill,
        targets: &targets,
        spill: runtime.spill(),
        ingress: Some(ledger),
        jobs: Some(runtime.jobs()),
        completed_job_max_age: workspace.jobs.completed_job_max_age(),
    }
    .run()
    .await;

    if report.is_empty() {
        return Ok(MaintenanceRun::Checked);
    }
    log_line(config, &format!("{channel_name} retention: {report}"))?;
    Ok(MaintenanceRun::Ran)
}

const DEFAULT_LOG_RELPATH: &str = "logs/agentos-gateway.log";
const OWNER_TOKEN_ENV: &str = "AGENTOS_GATEWAY_OWNER_TOKEN";

/// How often the independent maintenance timer scans the cron store. Cron expressions
/// have one-minute resolution, so a 30s scan never misses a tick while
/// avoiding a re-read of every TOML more often than necessary.
const CRON_SCAN_INTERVAL_SECS: u64 = 30;

/// How often the retention sweep walks the stores.
///
/// Far slower than the cron scan, because the two are not the same kind of
/// work. A cron scan re-reads a handful of TOML files and must not miss a
/// minute-resolution tick; a retention sweep stats every trace file and every
/// attachment directory, and nothing goes wrong if a file that became eligible
/// at 09:01 is removed at 09:15.
const RETENTION_SWEEP_INTERVAL_SECS: u64 = 900;

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
    /// The `.env` this invocation was told to read, forwarded to the `serve`
    /// child.
    ///
    /// `start` forwarded `--config` and `--session-db-path` and not this, so a
    /// deployment started with `--env-file /etc/agentos/prod.env` spawned a
    /// service that read whatever `.env` happened to be in the working
    /// directory instead — a different provider, different credentials,
    /// different channels, and nothing saying so (M3 deliverable 2, found by
    /// the upgrade rehearsal).
    env_file: Option<PathBuf>,
}

impl ServiceConfig {
    fn from_home(home: PathBuf) -> Self {
        Self {
            pid_path: home.join(DEFAULT_PID_RELPATH),
            log_path: home.join(DEFAULT_LOG_RELPATH),
            agent_config_path: None,
            session_db_path: None,
            env_file: None,
            home,
        }
    }
}

/// The resources that exist exactly once in a persistent serving process.
///
/// Channel workers receive clones of this handle. Cloning shares each
/// resource; it cannot construct a second database pool, job registry, MCP
/// lifecycle, cancellation root, or session-usage accumulator.
#[derive(Clone)]
struct ProcessRuntimeAuthority {
    tokio_runtime: Arc<tokio::runtime::Runtime>,
    agent_runtime: Arc<AgentRuntime>,
    session_usage: Arc<SessionUsage>,
    shutdown: gateway::ProcessShutdown,
}

impl ProcessRuntimeAuthority {
    fn build(config: &ServiceConfig) -> Result<Self, String> {
        let tokio_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("failed to start tokio runtime: {err}"))?,
        );
        let agent_runtime = Arc::new(tokio_runtime.block_on(AgentRuntime::build_with(
            runtime_paths(config),
            &agentos_cli::semantic_index_factory,
        ))?);
        let shutdown = gateway::ProcessShutdown::new(
            agent_runtime.cancellation().clone(),
            Duration::from_secs(agent_runtime.workspace_config.gateway.shutdown_grace_secs),
        );
        Ok(Self {
            tokio_runtime,
            agent_runtime,
            session_usage: Arc::new(SessionUsage::new()),
            shutdown,
        })
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
        "catalog" => {
            // Not derived from `config`: the catalogs describe what this build
            // offers, so they are located in the source tree and take no
            // account of a deployment's own agent.toml.
            let root = env::args()
                .skip(2)
                .find_map(|arg| arg.strip_prefix("--root=").map(PathBuf::from))
                .unwrap_or_else(catalog::default_root);
            let check = env::args().any(|arg| arg == "--check");
            catalog::run(&root, check)
        }
        "calibrate" => {
            // Same reasoning as `catalog`: the record is checked-in
            // documentation, so it is located in the source tree rather than
            // under $AGENTOS_HOME. The provider, though, comes from the
            // deployment's own environment — the point is to measure the model
            // this machine actually calls.
            let root = env::args()
                .skip(2)
                .find_map(|arg| arg.strip_prefix("--root=").map(PathBuf::from))
                .unwrap_or_else(calibrate::default_root);
            let check = env::args().any(|arg| arg == "--check");
            calibrate::run(&root, check)
        }
        "serve" => serve(&config),
        "migrate" => migrate(&config),
        "purge" => purge::purge(&config),
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
Usage: agentos-gateway <start|stop|restart|status|config|catalog|calibrate|migrate|purge> [OPTIONS]

Manage the AgentOS gateway as a persistent background service.

Subcommands:
  start      Start the gateway service in the background.
  stop       Stop the running gateway service.
  restart    Stop then start the gateway service.
  status     Report whether the gateway service is running.
  config     Print the effective workspace config used by the gateway.
  catalog    Regenerate docs/CONFIG_CATALOG.md and docs/TOOL_CATALOG.md from
             the code. `--check` verifies they are current without writing,
             which is what CI runs; `--root=PATH` names the repository.
  calibrate  Measure the token estimator against the configured provider's own
             counts and record the result (roadmap C1). Spends real requests.
             `--check` re-scores today's estimator against the recorded counts
             offline, spending nothing; `--root=PATH` names the repository.
  migrate    Move memory and the session log written before typed principals
             onto principal-keyed namespaces (`ID-002`, M3 deliverable 2). A
             gateway refuses to start on an unmigrated session log, because
             every conversation would read as empty. Reports and changes nothing by default.
             `--apply` performs it, and needs `--backup PATH` (or an explicit
             `--no-backup`) because it rewrites the database in place.
             `--channel NAME` names the channel legacy conversations arrived
             on, which the old rows do not record; `--agent NAME` likewise.
             `--assume-literal-underscores` accepts the literal reading of ids
             the old encoder made ambiguous — see the report before using it.
             `--adjudicate-ingress EVENT_ID` durably refuses one quarantined
             legacy or ambiguous action/delivery row after reconciliation; it
             also requires `--apply`.
  purge      Irreversibly delete records. Never `/clear`, which hides history
             behind an epoch marker and removes nothing. Three modes, each
             recording a safety event:
             `--conversation ID --yes ID` deletes one conversation's session
             log — the operation for somebody asking to be forgotten.
             `--sessions --before YYYY-MM-DD` deletes whole conversations idle
             since before that date. `--audit --before YYYY-MM-DD` deletes rows
             from safety_events and memory_access_log, which nothing else ever
             removes (ADR-0005). Both bulk modes report and change nothing
             until `--apply --yes N`, where N is the count they printed.

All workspace paths derive from $AGENTOS_HOME (set in .env or the process env).
If unset, $AGENTOS_HOME defaults to the parent dir of the loaded .env file,
or the current working directory.

Options:
  --pid-path PATH             Control file (locked PID record). Default: $AGENTOS_HOME/{DEFAULT_PID_RELPATH}
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
                config.env_file = Some(next_path(&mut args, "--env-file")?);
            }
            option if option.starts_with("--env-file=") => {
                config.env_file = option.strip_prefix("--env-file=").map(PathBuf::from);
            }
            "--no-env-override" => {}
            // `catalog`'s and `calibrate`'s own options. Parsed by those
            // subcommands from the raw argv, since they name a source tree
            // rather than a runtime path.
            "--check" => {}
            option if option.starts_with("--root=") => {}
            // `migrate`'s and `purge`'s own options, parsed the same way and
            // for the same reason: they describe a one-off operation, not the
            // service.
            //
            // Named here as well as read there, because this loop rejects
            // anything it does not recognise — which is right, and which made
            // `purge --conversation ID --yes ID` fail with "unknown option"
            // for as long as it has existed. Every flag a subcommand reads
            // needs a line here or it cannot be typed (M7 / `QUOTA-001`).
            "--apply"
            | "--no-backup"
            | "--assume-literal-underscores"
            | "--audit"
            | "--sessions" => {}
            "--channel"
            | "--agent"
            | "--backup"
            | "--conversation"
            | "--yes"
            | "--before"
            | "--adjudicate-ingress" => {
                let _ = args.next();
            }
            option
                if option.starts_with("--channel=")
                    || option.starts_with("--agent=")
                    || option.starts_with("--backup=")
                    || option.starts_with("--conversation=")
                    || option.starts_with("--yes=")
                    || option.starts_with("--before=")
                    || option.starts_with("--adjudicate-ingress=") => {}
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

    // The lock, not the pid, answers "is one already running" (M8 /
    // `GW-001`, deliverable 5). A file left by a crashed process reads
    // identically to one left by a live one; only the lock tells them apart,
    // and a stale file needs no removing because the next acquisition simply
    // takes it.
    if let Some(record) = gateway::holder(&config.pid_path)
        .map_err(|err| format!("failed to read the gateway control file: {err}"))?
    {
        println!(
            "AgentOS gateway is already running: pid {}, control file {}",
            record.pid,
            config.pid_path.display()
        );
        return Ok(());
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

    // Checked here as well as in `serve`, because `serve` is detached: its
    // stdout goes to the log file and the operator running `start` would see
    // only a startup timeout. The same check twice is cheap; a refusal nobody
    // reads is not.
    refuse_unmigrated_sessions(&config)?;

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
    if let Some(path) = &config.env_file {
        command.arg("--env-file").arg(path);
    }
    detach_gateway_process(&mut command);

    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start gateway service: {err}"))?;
    let pid = child.id();

    // The child takes the lock; this process only watches for it. That is the
    // whole reason the record is trustworthy — a lock written by the parent
    // would be released the moment `start` returned.
    let started = wait_for_control_file(&config, pid, &owner_token, &mut child)?;
    if !started {
        return Err(format!(
            "AgentOS gateway service did not take its control file within {}s; see {}",
            STARTUP_TIMEOUT.as_secs(),
            config.log_path.display()
        ));
    }

    println!(
        "AgentOS gateway started: pid {pid}, control file {}, log {}",
        config.pid_path.display(),
        config.log_path.display()
    );
    Ok(())
}

/// How long `start` waits for the spawned service to take its control file.
///
/// The child loads the whole workspace config, opens the store and builds the
/// runtime before it serves, but it takes the lock *first*, so this only has
/// to cover a fork and an exec.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait until the spawned gateway holds the control file with the token this
/// start issued, or until it exits.
fn wait_for_control_file(
    config: &ServiceConfig,
    pid: u32,
    owner_token: &str,
    child: &mut std::process::Child,
) -> Result<bool, String> {
    let deadline = std::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(record) = gateway::holder(&config.pid_path)
            .map_err(|err| format!("failed to read the gateway control file: {err}"))?
        {
            // The token, not just the pid: two starts racing would otherwise
            // each see "something holds it" and each claim success.
            if record.token.as_ref() == owner_token && record.pid == pid {
                return Ok(true);
            }
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|err| format!("failed to inspect gateway service: {err}"))?
        {
            return Err(format!(
                "AgentOS gateway service exited during startup with {status}; see {}",
                config.log_path.display()
            ));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// How long `stop` waits for a `SIGTERM`ed gateway to drain before escalating.
///
/// Deliberately longer than the gateway's own
/// `[gateway] shutdown_grace_secs`, so the service gets to finish its drain
/// and report what it abandoned rather than being killed halfway through
/// doing so.
const STOP_SCHEDULER_TOLERANCE: Duration = Duration::from_secs(5);

fn stop_timeout(config: &ServiceConfig) -> Duration {
    let grace = WorkspaceConfig::load(&agent_config_path(config))
        .map(|workspace| workspace.gateway.shutdown_grace_secs)
        .unwrap_or(gateway::DEFAULT_SHUTDOWN_GRACE_SECS);
    Duration::from_secs(grace).saturating_add(STOP_SCHEDULER_TOLERANCE)
}

fn stop(config: &ServiceConfig) -> Result<(), String> {
    // The request captures the locked holder's private token and presents it
    // to the holder-directed socket. If that holder exits and a replacement
    // starts in between, the replacement rejects the old token; no numeric pid
    // is ever signalled.
    let Some(request) = gateway::ShutdownRequest::capture(&config.pid_path)
        .map_err(|err| format!("failed to inspect the gateway holder: {err}"))?
    else {
        match gateway::read_record(&config.pid_path)
            .map_err(|err| format!("failed to read the gateway control file: {err}"))?
        {
            Some(stale) => {
                let _ = fs::remove_file(&config.pid_path);
                let _ = fs::remove_file(gateway::control_socket_path(&config.pid_path));
                println!(
                    "AgentOS gateway was not running; removed the control file left by pid {}",
                    stale.pid
                );
            }
            None => println!(
                "AgentOS gateway is not running: control file {} does not exist",
                config.pid_path.display()
            ),
        }
        return Ok(());
    };
    let record = request
        .send()
        .map_err(|err| format!("failed to request gateway shutdown safely: {err}"))?;

    // The lock releasing is the definitive "it exited": the kernel drops it
    // when the process ends, however it ends.
    let timeout = stop_timeout(config);
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if gateway::holder(&config.pid_path)
            .map_err(|err| format!("failed to read the gateway control file: {err}"))?
            .is_none()
        {
            let _ = fs::remove_file(&config.pid_path);
            println!("AgentOS gateway stopped: pid {}", record.pid);
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    Err(format!(
        "gateway pid {} did not release {} within {}s; refusing a numeric-pid kill because the \
         holder may have changed",
        record.pid,
        config.pid_path.display(),
        timeout.as_secs()
    ))
}

fn stop_if_running(config: &ServiceConfig) -> Result<(), String> {
    if config.pid_path.exists() {
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
    if let Some(record) = gateway::holder(&config.pid_path)
        .map_err(|err| format!("failed to read the gateway control file: {err}"))?
    {
        println!(
            "AgentOS gateway status: running, pid {}, since {}",
            record.pid, record.started_at
        );
        return Ok(());
    }
    match gateway::read_record(&config.pid_path)
        .map_err(|err| format!("failed to read the gateway control file: {err}"))?
    {
        // A record with no lock behind it. Reported as stale rather than as
        // running, which is the distinction `kill -0` could not make.
        Some(stale) => println!(
            "AgentOS gateway status: stopped; control file {} is stale (pid {})",
            config.pid_path.display(),
            stale.pid
        ),
        None => println!("AgentOS gateway status: stopped"),
    }
    Ok(())
}

/// Print what this deployment's config resolved to.
///
/// Two halves, and the split is the point. The first is *deployment* facts the
/// config file does not contain — where the paths landed, which sandbox this
/// kernel offers, which channels will actually be served. The second is every
/// key `agent.toml` accepts, derived from the same walk that generates
/// `docs/CONFIG_CATALOG.md`.
///
/// The second half used to be fifty-five hand-written `println!`s naming a
/// subset of the keys (M7 / `CFG-001`, deliverable 7). It went stale silently,
/// and it could not answer the questions an operator opens this command for:
/// did I set that, or is it the default; is this key still effective; is
/// anything I wrote being ignored. Deriving it from the walk means a key in
/// the structs cannot be missing from it.
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
    println!("paths.session_db={}", session_path(config).display());
    println!("paths.home={}", config.home.display());
    println!("sandbox.enforcement={}", sandbox::availability().describe());
    println!(
        "channels.persistent={}",
        if channels.is_empty() {
            "(none)".to_owned()
        } else {
            channels.join(",")
        }
    );
    println!();
    print!("{}", effective::report(&workspace_config));
    Ok(())
}

fn serve(config: &ServiceConfig) -> Result<(), String> {
    ensure_parent_dir(&config.pid_path)?;
    ensure_parent_dir(&config.log_path)?;

    // A visible lock is the publication point. Everything needed to stop the
    // process must therefore be ready first: the signal disposition and the
    // authenticated socket accept loop.
    gateway::install_shutdown_handler()
        .map_err(|err| format!("failed to install the shutdown handler: {err}"))?;
    let owner_token = match env::var(OWNER_TOKEN_ENV) {
        Ok(token) if !token.is_empty() => token,
        _ => gateway_owner_token()?,
    };
    let control_token = gateway_owner_token()?;
    let control_endpoint = gateway::ControlEndpoint::bind(&config.pid_path, control_token.clone())
        .map_err(|err| format!("failed to bind the gateway control endpoint: {err}"))?;

    // A debug-only process fixture holds execution at the final pre-publication
    // barrier. It lets the lifecycle regression prove the lock remains absent
    // until shutdown handling is ready.
    #[cfg(debug_assertions)]
    if let Ok(delay) = env::var("AGENTOS_TEST_PRE_PUBLICATION_DELAY_MS") {
        if let Ok(path) = env::var("AGENTOS_TEST_PRE_PUBLICATION_READY_PATH") {
            fs::write(&path, b"ready\n")
                .map_err(|err| format!("failed to publish test startup barrier {path}: {err}"))?;
        }
        let millis = delay
            .parse::<u64>()
            .map_err(|_| "AGENTOS_TEST_PRE_PUBLICATION_DELAY_MS must be an integer".to_owned())?;
        thread::sleep(Duration::from_millis(millis));
    }

    // Taken here and held for the life of this process. Not by `start`: a lock
    // the parent held would be released the moment `start` returned.
    let control = gateway::ControlFile::acquire(
        &config.pid_path,
        &gateway::ControlRecord::with_control_token(
            process::id(),
            owner_token,
            control_token,
            unix_seconds(),
        ),
    )
    .map_err(|err| format!("failed to take the gateway control file: {err}"))?;

    log_line(config, "AgentOS gateway service starting")?;
    log_line(
        config,
        &format!(
            "config={}, session_db={}",
            display_optional_path(&config.agent_config_path),
            display_optional_path(&config.session_db_path)
        ),
    )?;

    // Before anything is served, and independently of whether any channel is
    // enabled: a session log still keyed by bare conversation ids would read
    // as empty under principal keys, which is the one failure that looks like
    // working software (M3 deliverable 2).
    refuse_unmigrated_sessions(config)?;

    let workspace_config = WorkspaceConfig::load(&agent_config_path(config))
        .map_err(|err| format!("failed to load workspace config: {err}"))?;
    let channels = persistent_channels(&workspace_config)?;
    let outcome = if channels.is_empty() {
        log_line(
            config,
            "no persistent channels enabled; enable [channels.telegram]/[channels.feishu] or set AGENTOS_ENABLED_CHANNELS=telegram,feishu",
        )?;
        idle_until_shutdown(config)
    } else {
        for channel in &channels {
            log_line(config, &format!("{channel} channel enabled"))?;
        }
        run_persistent_gateways(config, &channels)
    };

    log_line(config, "AgentOS gateway service stopped")?;
    drop(control_endpoint);
    control.release();
    outcome
}

/// The no-channels case: nothing to serve, but the control file is held and a
/// `SIGTERM` must still land somewhere.
fn idle_until_shutdown(config: &ServiceConfig) -> Result<(), String> {
    let mut ticks = 0u64;
    while !gateway::shutdown_requested() {
        thread::sleep(Duration::from_secs(1));
        ticks += 1;
        if ticks.is_multiple_of(60) {
            log_line(config, "AgentOS gateway heartbeat")?;
        }
    }
    log_line(config, "AgentOS gateway shutting down on signal")?;
    Ok(())
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

fn run_persistent_gateways(
    config: &ServiceConfig,
    channels: &[&'static str],
) -> Result<(), String> {
    // GW-005: transports are channel-local, but the resources that define one
    // serving agent are process-wide. In particular, building here (rather
    // than in `run_channel_gateway`) gives Telegram and Feishu the same SQLite
    // pool, job registry, MCP lifecycle registry, cancellation root, and
    // session-usage counter.
    let authority = ProcessRuntimeAuthority::build(config)?;
    log_line(config, &authority.agent_runtime.orchestrator.describe_llm())?;

    let mut handles = Vec::new();
    for channel in channels {
        let config = config.clone();
        let channel = *channel;
        let worker_authority = authority.clone();
        let handle =
            thread::spawn(move || run_persistent_channel(&config, channel, worker_authority));
        handles.push((channel, handle));
    }

    let mut announced = false;
    let mut first_error = None;
    let mut mcp_shutdown = None;
    loop {
        thread::sleep(Duration::from_millis(25));
        if authority.shutdown.observe_process_request().is_some() && !announced {
            announced = true;
            log_line(
                config,
                "shutdown requested; admission stopped and the process drain deadline started",
            )?;
        }
        if mcp_shutdown.is_none() {
            if let Some(deadline) = authority.shutdown.deadline() {
                let runtime = Arc::clone(&authority.agent_runtime);
                mcp_shutdown = Some(authority.tokio_runtime.spawn(async move {
                    runtime.shutdown_mcp(deadline).await;
                }));
            }
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
                        authority.shutdown.begin();
                        log_line(config, &format!("{channel} gateway loop failed: {err}"))?;
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                    Err(_) => {
                        authority.shutdown.begin();
                        let err = format!("{channel} gateway loop panicked");
                        log_line(config, &err)?;
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            } else {
                index += 1;
            }
        }
        if handles.is_empty() {
            authority.shutdown.begin();
            break;
        }
        if authority
            .shutdown
            .deadline()
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            let abandoned: Vec<_> = handles.iter().map(|(channel, _)| *channel).collect();
            log_line(
                config,
                &format!(
                    "process shutdown deadline reached; abandoning channel worker(s) {abandoned:?}"
                ),
            )?;
            break;
        }
    }

    let deadline = authority.shutdown.begin();
    let shutdown = mcp_shutdown.unwrap_or_else(|| {
        let runtime = Arc::clone(&authority.agent_runtime);
        authority.tokio_runtime.spawn(async move {
            runtime.shutdown_mcp(deadline).await;
        })
    });
    authority
        .tokio_runtime
        .block_on(shutdown)
        .map_err(|err| format!("MCP shutdown task failed: {err}"))?;

    // Report from the process-owned store even if a channel worker itself was
    // what missed the deadline and could not reach its local epilogue.
    let ledger = IngressLedger::new(Arc::clone(&authority.agent_runtime.session));
    for channel in channels {
        ingress::report_unsettled(config, channel, &ledger, &ChannelId::new(*channel))?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

fn run_persistent_channel(
    config: &ServiceConfig,
    channel: &'static str,
    authority: ProcessRuntimeAuthority,
) -> Result<(), String> {
    match channel {
        "telegram" => authority.tokio_runtime.block_on(run_telegram_gateway(
            config,
            authority.agent_runtime,
            authority.session_usage,
            authority.shutdown,
        )),
        "feishu" => authority.tokio_runtime.block_on(run_feishu_gateway(
            config,
            authority.agent_runtime,
            authority.session_usage,
            authority.shutdown,
        )),
        _ => Err(format!("unknown persistent channel: {channel}")),
    }
}

/// The deployment's `[limits]` for inbound attachments (M4 / `ING-001`).
///
/// Falls back to the defaults when `agent.toml` cannot be read: a channel that
/// starts with unbounded downloads because a config file was missing would be
/// the failure this bound exists to prevent, arriving by a different route.
fn attachment_limits(config: &ServiceConfig) -> (u64, usize) {
    let limits = WorkspaceConfig::load(&agent_config_path(config))
        .map(|workspace| workspace.limits)
        .unwrap_or_default();
    (limits.attachment_bytes, limits.attachments_per_message)
}

async fn run_telegram_gateway(
    config: &ServiceConfig,
    runtime: Arc<AgentRuntime>,
    session_usage: Arc<SessionUsage>,
    shutdown: gateway::ProcessShutdown,
) -> Result<(), String> {
    let attachments_dir = attachments_dir_path(config);
    let (max_bytes, per_message) = attachment_limits(config);
    let channel = TelegramChannel::from_env()
        .map_err(|err| format!("failed to configure telegram channel: {err}"))?
        .with_attachments_root(attachments_dir)
        .with_attachment_limits(max_bytes, per_message)
        .with_receive_error_logging(true);
    run_channel_gateway(
        config,
        channel,
        "telegram",
        RunId::new("telegram-gateway"),
        runtime,
        session_usage,
        shutdown,
    )
    .await
}

async fn run_feishu_gateway(
    config: &ServiceConfig,
    runtime: Arc<AgentRuntime>,
    session_usage: Arc<SessionUsage>,
    shutdown: gateway::ProcessShutdown,
) -> Result<(), String> {
    let attachments_dir = attachments_dir_path(config);
    let (max_bytes, per_message) = attachment_limits(config);
    let channel = FeishuChannel::from_env()
        .map_err(|err| format!("failed to configure feishu channel: {err}"))?
        .with_attachments_root(attachments_dir)
        .with_attachment_limits(max_bytes, per_message)
        .with_receive_error_logging(true);
    run_channel_gateway(
        config,
        channel,
        "feishu",
        RunId::new("feishu-gateway"),
        runtime,
        session_usage,
        shutdown,
    )
    .await
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
    runtime: Arc<AgentRuntime>,
    session_usage: Arc<SessionUsage>,
    shutdown: gateway::ProcessShutdown,
) -> Result<(), String>
where
    C: Channel,
{
    let gateway_config = runtime.workspace_config.gateway;
    let shard_config = ShardConfig {
        shards: gateway_config.shard_count(),
        inbox_capacity: gateway_config.inbox_capacity,
        idle_interval: DEFAULT_IDLE_INTERVAL,
    };
    let (router, inbounds) = shard_set(&shard_config);

    // The durable record of what this gateway has accepted (M8 / `GW-001`,
    // deliverable 3). Built before the shards, because the shards settle into
    // it and the router admits through it.
    let ledger = Arc::new(IngressLedger::new(Arc::clone(&runtime.session)));
    let approval_store = Arc::new(ApprovalStore::new(ledger.as_ref().clone()));
    let quarantined = ledger
        .quarantined(&channel.id())
        .map_err(|err| format!("failed to inspect {channel_name} ingress quarantine: {err}"))?;
    if !quarantined.is_empty() {
        return Err(format!(
            "refusing to serve {channel_name}: {} legacy unsettled ingress row(s) have no \
             replayable envelope; inspect and adjudicate the ingress quarantine before start",
            quarantined.len()
        ));
    }
    let recovered = ledger
        .recover_dispatches(&channel.id())
        .map_err(|err| format!("failed to recover {channel_name} ingress dispatches: {err}"))?;
    if recovered > 0 {
        log_line(
            config,
            &format!("{channel_name} recovered {recovered} interrupted ingress dispatch(es)"),
        )?;
    }
    let ambiguous = ledger
        .ambiguities(&channel.id())
        .map_err(|err| format!("failed to inspect {channel_name} delivery ambiguity: {err}"))?;
    if !ambiguous.is_empty() {
        return Err(format!(
            "refusing to serve {channel_name}: {} ingress event(s) have ambiguous external \
             action or delivery outcomes; reconcile them and run migrate \
             --adjudicate-ingress EVENT_ID --apply before restart",
            ambiguous.len()
        ));
    }
    ingress::report_unsettled(config, channel_name, &ledger, &channel.id())?;
    if let Some(cursor) = ledger
        .cursor(&channel.id())
        .map_err(|err| format!("failed to read the {channel_name} ingress cursor: {err}"))?
    {
        log_line(
            config,
            &format!("{channel_name} resuming ingress from cursor {cursor}"),
        )?;
        channel.resume_from(&cursor);
    }

    let egress = channel.egress();
    let current_administrators: Vec<ActorPrincipal> = runtime
        .workspace_config
        .policy
        .approval_administrators
        .iter()
        .map(|administrator| administrator.actor())
        .collect();
    let recovery = approval_store
        .recover(&channel.id(), &current_administrators)
        .map_err(|err| format!("failed to recover {channel_name} approvals: {err}"))?;
    for (instance, batch) in recovery.prompts {
        send_approval_prompt(&approval_store, egress.as_ref(), &instance, &batch)
            .await
            .map_err(|err| {
                format!("failed to deliver recovered {channel_name} approval prompt: {err}")
            })?;
    }
    let recovered_approvals = approval_store
        .recover(&channel.id(), &current_administrators)
        .map_err(|err| format!("failed to validate {channel_name} approvals: {err}"))?
        .pending;
    if !recovered_approvals.is_empty() {
        log_line(
            config,
            &format!(
                "{channel_name} restored {} pending approval(s)",
                recovered_approvals.len()
            ),
        )?;
    }
    let mut restored_by_shard: Vec<Vec<_>> = (0..shard_config.shards).map(|_| Vec::new()).collect();
    for approval in recovered_approvals {
        let shard = router.shard_of(&approval.paused.conversation_id);
        restored_by_shard[shard].push(approval);
    }
    let stream_egress = gateway_streaming_enabled()
        .then(|| channel.stream_egress())
        .flatten();
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
            ledger: Arc::clone(&ledger),
            approval_store: Arc::clone(&approval_store),
            restored_approvals: std::mem::take(&mut restored_by_shard[shard]),
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

    let active_channel_id = channel.id();
    let dispatch_gate = tokio::sync::Mutex::new(());
    let outcome = {
        let maintenance = maintenance::run_channel_maintenance(
            config,
            channel_name,
            Arc::clone(&runtime),
            Arc::clone(&egress),
            Arc::clone(&ledger),
            Arc::clone(&session_usage),
        );
        let receiver = ingress::receive(
            config,
            &mut channel,
            channel_name,
            &ledger,
            &dispatch_gate,
            &shutdown,
        );
        let dispatcher = ingress::dispatch(
            config,
            channel_name,
            &router,
            egress.as_ref(),
            &ledger,
            &active_channel_id,
            &dispatch_gate,
        );
        tokio::pin!(maintenance);
        tokio::pin!(receiver);
        tokio::pin!(dispatcher);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => Ok(()),
            result = &mut maintenance => result,
            result = &mut receiver => result,
            result = &mut dispatcher => result,
        }
    };
    let deadline = shutdown.begin();

    // Dropping the router closes every shard queue, which ends each shard once
    // it has drained. The wait is *bounded* (M8 / `GW-001`, deliverable 5): a
    // shard wedged on a tool that ignores its deadline must not turn a
    // `SIGTERM` into a hang, so past the grace period the gateway says what it
    // is abandoning and exits.
    drop(router);
    let mut wedged = Vec::new();
    for (shard, handle) in shards.into_iter().enumerate() {
        while !handle.is_finished() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        if !handle.is_finished() {
            // Deliberately not joined: joining is what would hang.
            wedged.push(shard);
            continue;
        }
        if handle.join().is_err() {
            log_line(config, &format!("{channel_name} shard {shard} panicked"))?;
        }
    }
    if !wedged.is_empty() {
        log_line(
            config,
            &format!(
                "{channel_name} shard(s) {wedged:?} did not drain within {}s; exiting anyway",
                runtime.workspace_config.gateway.shutdown_grace_secs
            ),
        )?;
    }
    // What this process accepted and never settled. The turns that were still
    // running when the deadline passed are in here, which is the "reports
    // abandoned work" half of the shutdown criterion.
    ingress::report_unsettled(config, channel_name, &ledger, &active_channel_id)?;
    outcome
}

/// Replay every cron task that is due and bound to this channel as a synthetic
/// envelope through the normal run path, then persist the updated fire/retry
/// bookkeeping so the task does not re-fire on the next scan.
///
/// Cron failures are reported to the maintenance supervisor and swallowed
/// there — a broken task must never take down the channel gateway loop. Each
/// channel gateway only fires tasks whose `channel_id` matches its own, so a
/// multi-channel deployment neither
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
) -> Result<usize, String> {
    let mut scheduler = match cron_store.load_scheduler() {
        Ok(scheduler) => scheduler,
        Err(err) => return Err(format!("cron load failed: {err}")),
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
        Err(err) => return Err(format!("cron scan failed: {err}")),
    };
    let mut dispatched = 0;
    for invocation in due {
        if invocation.envelope.channel_id.as_str() != channel_name {
            continue;
        }
        dispatched += 1;
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
            Ok(GatewayRun::Paused {
                paused,
                ticket,
                expires_at,
                ..
            }) => {
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
                if let Some(approval) = paused.state.pending_approval() {
                    let binding = ApprovalBinding::new(
                        approval.approval_instance_id.clone(),
                        ticket,
                        approval.id.clone(),
                        approval.prompting_principal.clone(),
                        expires_at,
                    );
                    let resolver = ActorPrincipal::new(
                        paused.state.active_agent.clone(),
                        paused.channel_id.clone(),
                        paused.conversation_id.clone(),
                        Arc::from("agentos-system"),
                    );
                    let witness = binding.and_then(|binding| {
                        binding.unanswered_witness(
                            resolver,
                            ApprovalOutcome::Unavailable,
                            Arc::from("no interactive user behind a cron run"),
                        )
                    });
                    // Fails the run closed; the error is the expected shape.
                    if let Some(witness) = witness {
                        let _ = gateway_service.resume(egress, paused, witness).await;
                    }
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
    Ok(dispatched)
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

/// Move memory written before typed principals onto principal-keyed
/// namespaces.
///
/// Reports by default and applies only when asked, because it rewrites rows in
/// place and the report is the part that needs reading: the old encoding lost
/// information, so some namespaces cannot be migrated without a human deciding
/// what they meant.
fn migrate(config: &ServiceConfig) -> Result<(), String> {
    let flags: Vec<String> = env::args().skip(2).collect();
    let has = |name: &str| flags.iter().any(|flag| flag == name);
    // Both `--channel=NAME` and `--channel NAME`. This read only the first,
    // which was survivable while `parse_config` rejected the second outright —
    // it failed loudly. Once `parse_config` learned to accept the separated
    // form for `purge`, the separated form here started being *silently
    // ignored*, and `migrate --channel telegram` failed claiming no channel
    // was given. One lookup shape for every subcommand, so the two cannot
    // disagree again (M9 / `CI-002`).
    let value = |name: &str| {
        let mut args = flags.iter();
        while let Some(flag) = args.next() {
            if let Some(inline) = flag.strip_prefix(&format!("{name}=")) {
                return Some(inline.to_owned());
            }
            if flag == name {
                return args.next().cloned();
            }
        }
        None
    };

    let db_path = session_path(config);
    if !db_path.exists() {
        return Err(format!("no database at {}", db_path.display()));
    }
    let workspace_config = WorkspaceConfig::load(&agent_config_path(config))
        .map_err(|err| format!("failed to load workspace config: {err}"))?;

    let settings = MigrationSettings {
        agent: AgentId::new(
            value("--agent").unwrap_or_else(|| workspace_config.agent.id.to_string()),
        ),
        // No default. Every legacy conversation namespace needs a channel, the
        // rows do not record one, and guessing would put two channels'
        // conversations under one principal — the exact failure the identity
        // work exists to remove.
        channel: ChannelId::new(value("--channel").ok_or_else(|| {
            "--channel NAME is required: legacy rows record no channel, and it cannot be inferred"
                .to_owned()
        })?),
        assume_literal_underscores: has("--assume-literal-underscores"),
    };

    let store = Arc::new(
        SqliteStore::open(&db_path)
            .map_err(|err| format!("failed to open {}: {err}", db_path.display()))?,
    );
    let ingress_ledger = IngressLedger::new(Arc::clone(&store));
    let version = migrate::schema_version(&store)
        .map_err(|err| format!("failed to read the schema version: {err}"))?;
    println!("database: {}", db_path.display());
    println!("schema version: {version}");

    let plan = migrate::plan(&store, &settings)
        .map_err(|err| format!("failed to plan the migration: {err}"))?;
    print!("{}", plan.report());

    // The session log is the second half, added by M3 deliverable 2. Reported
    // together with the memory namespaces because they are one upgrade from
    // the operator's side — and because a database that migrated one and not
    // the other is a gateway that refuses to start.
    let session_plan = migrate_sessions::plan(&store, &settings)
        .map_err(|err| format!("failed to plan the session migration: {err}"))?;
    if !session_plan.is_empty() {
        println!();
        print!("{}", session_plan.report());
    }
    let mut child_session_plan = migrate_child_sessions::plan(&store)
        .map_err(|err| format!("failed to plan the child session migration: {err}"))?;
    if !child_session_plan.is_empty() {
        println!();
        print!("{}", child_session_plan.report());
    }
    let ingress_quarantine = ingress_ledger
        .quarantined(&settings.channel)
        .map_err(|err| format!("failed to inspect ingress quarantine: {err}"))?;
    if !ingress_quarantine.is_empty() {
        println!(
            "\n{} quarantined ingress row(s) for channel {}:",
            ingress_quarantine.len(),
            settings.channel.as_str()
        );
        for event in &ingress_quarantine {
            println!(
                "  event {} conversation {} sender {} accepted_at {}",
                event.event_id,
                event.conversation_id.as_str(),
                event.sender,
                event.accepted_at
            );
        }
    }
    let ingress_ambiguities = ingress_ledger
        .ambiguities(&settings.channel)
        .map_err(|err| format!("failed to inspect ingress ambiguity: {err}"))?;
    if !ingress_ambiguities.is_empty() {
        println!(
            "\n{} ambiguous ingress outcome(s) for channel {}:",
            ingress_ambiguities.len(),
            settings.channel.as_str()
        );
        for event in &ingress_ambiguities {
            println!(
                "  event {} state {} action {} delivery {} reason {}",
                event.key.event_id,
                event.state.as_str(),
                event.action_id.as_deref().unwrap_or("-"),
                event.delivery_id.as_deref().unwrap_or("-"),
                event.reason
            );
        }
    }
    let ingress_adjudication = value("--adjudicate-ingress");

    if !has("--apply") {
        println!("\nNothing was changed. Re-run with --apply to perform this migration.");
        return Ok(());
    }
    if plan.rewrites.is_empty()
        && session_plan.is_empty()
        && child_session_plan.rewrites.is_empty()
        && ingress_adjudication.is_none()
    {
        println!("\nNothing to apply.");
        return Ok(());
    }

    // A backup, or a deliberate statement that none is wanted. The migration
    // is atomic, so a crash cannot corrupt the database — but a *correct*
    // migration the operator did not intend is equally unrecoverable, and that
    // is what a backup is for.
    match value("--backup") {
        Some(backup) => {
            let backup = PathBuf::from(backup);
            std::fs::copy(&db_path, &backup)
                .map_err(|err| format!("failed to write backup {}: {err}", backup.display()))?;
            println!("\nbackup: {}", backup.display());
        }
        None if has("--no-backup") => {
            println!("\nproceeding without a backup, as requested");
        }
        None => {
            return Err(
                "--backup PATH is required with --apply (or pass --no-backup deliberately)"
                    .to_owned(),
            );
        }
    }

    let moved = migrate::apply(&store, &plan)
        .map_err(|err| format!("the migration was rolled back: {err}"))?;
    println!("moved {moved} record(s)");
    // Its own transaction, after the namespaces. A failure here leaves the
    // namespace migration applied and the session tables untouched, which is
    // a state the next run plans correctly rather than one it has to detect.
    let rekeyed = migrate_sessions::apply(&store, &settings, &session_plan)
        .map_err(|err| format!("the session migration was rolled back: {err}"))?;
    if rekeyed > 0 {
        println!("rekeyed {rekeyed} session item(s)");
    }
    // A pre-principal table cannot expose delegated principal keys until the
    // table rebuild above. Re-plan after it, so one `migrate --apply` handles
    // both generations without asking the operator to discover a second pass.
    if !session_plan.is_empty() {
        child_session_plan = migrate_child_sessions::plan(&store)
            .map_err(|err| format!("failed to plan the child session migration: {err}"))?;
        if !child_session_plan.is_empty() {
            println!();
            print!("{}", child_session_plan.report());
        }
    }
    let child_rekeyed = migrate_child_sessions::apply(&store, &child_session_plan)
        .map_err(|err| format!("the child session migration was rolled back: {err}"))?;
    if child_rekeyed > 0 {
        println!("rekeyed {child_rekeyed} legacy child session item(s)");
    }
    if let Some(event_id) = ingress_adjudication {
        let changed = ingress_ledger
            .adjudicate_quarantined(&settings.channel, &event_id)
            .map_err(|err| format!("ingress adjudication failed: {err}"))?;
        if !changed {
            return Err(format!(
                "ingress event {event_id} is not quarantined or ambiguous for channel {}",
                settings.channel.as_str()
            ));
        }
        println!(
            "adjudicated ingress event {event_id} for channel {} as refused",
            settings.channel.as_str()
        );
    }
    if plan.blocked.is_empty() {
        println!(
            "schema version is now {}",
            migrate_sessions::SESSION_PRINCIPAL_SCHEMA_VERSION
        );
    } else {
        println!(
            "{} namespace(s) still need a decision, so the schema version stays at {version}",
            plan.blocked.len()
        );
    }
    Ok(())
}

/// Refuse to serve a database whose session log predates principal keying.
///
/// The check is cheap and the alternative is silent: every conversation would
/// start over, the model would answer as if it had never spoken to anyone, and
/// nothing in the log would say why. Reported here rather than left to the
/// runtime because a gateway with no channels enabled builds no runtime, and
/// "it started fine" is exactly the wrong thing to learn.
fn refuse_unmigrated_sessions(config: &ServiceConfig) -> Result<(), String> {
    let db_path = session_path(config);
    if !db_path.exists() {
        return Ok(());
    }
    let store = SqliteStore::open(&db_path)
        .map_err(|err| format!("failed to open {}: {err}", db_path.display()))?;
    let schema = migrate_sessions::session_schema(&store)
        .map_err(|err| format!("failed to inspect the session schema: {err}"))?;
    if schema != migrate_sessions::SessionSchema::Legacy {
        return Ok(());
    }
    let message = format!(
        "refusing to serve {}: its session log is still keyed by conversation id, so every \
         conversation would read as empty. Run `agentos-gateway migrate --channel NAME --apply \
         --backup PATH` first (M3 deliverable 2).",
        db_path.display()
    );
    let _ = log_line(config, &message);
    Err(message)
}

/// The agent id this deployment is configured with, for a subcommand that
/// needs a principal and was given only part of one.
///
/// Read from `agent.toml` rather than defaulted to a constant: `[agent].id`
/// keys every principal in the store (M7 / `CFG-001`), so a purge that
/// guessed would name a conversation that does not exist and report zero.
pub(crate) fn configured_agent_id(config: &ServiceConfig) -> Result<String, String> {
    WorkspaceConfig::load(&agent_config_path(config))
        .map(|workspace| workspace.agent.id.to_string())
        .map_err(|err| format!("failed to load workspace config: {err}"))
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

/// A value naming one particular start, so `start` can tell the gateway it
/// spawned from one that was already there.
fn gateway_owner_token() -> Result<String, String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|err| format!("failed to generate gateway owner token: {err}"))?;
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut token, "{byte:02x}")
            .map_err(|err| format!("failed to encode gateway owner token: {err}"))?;
    }
    Ok(token)
}

fn log_line(config: &ServiceConfig, message: &str) -> Result<(), String> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system time error: {err}"))?
        .as_secs();
    let line = format!("[{ts}] {message}\n");
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    // Private, like every other file the runtime writes (M8 / `GW-001`). The
    // log names conversations, senders and the text of errors, and rotation
    // (M7 / `QUOTA-001`) creates more of these files rather than fewer.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
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
