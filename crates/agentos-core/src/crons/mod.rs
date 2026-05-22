use crate::memory::{MemoryCaller, MemoryManager, ReflectionReport, ReflectionRequest};
use agentos_interfaces::memory::MemoryError;
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole};
use chrono::{DateTime, TimeZone, Utc};
use croner::Cron;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum CronError {
    #[error("invalid cron expression '{expression}': {message}")]
    InvalidExpression {
        expression: Arc<str>,
        message: Arc<str>,
    },
    #[error("gateway receiver is closed")]
    GatewayClosed,
    #[error("memory maintenance failed: {0}")]
    Memory(#[from] MemoryError),
    #[error("cron storage failed: {0}")]
    Storage(Arc<str>),
}

/// Absolute cron-expression schedule. The expression is the canonical source
/// of truth — there is no mutable "next due" field. Firing decisions are made
/// by comparing the most recent scheduled instant against `CronTask::last_fired_unix`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronSchedule {
    pub expression: Arc<str>,
}

impl CronSchedule {
    pub fn new(expression: impl Into<Arc<str>>) -> Result<Self, CronError> {
        let expression = expression.into();
        let schedule = Self { expression };
        schedule.parse()?;
        Ok(schedule)
    }

    fn parse(&self) -> Result<Cron, CronError> {
        Cron::from_str(self.expression.as_ref()).map_err(|err| CronError::InvalidExpression {
            expression: Arc::clone(&self.expression),
            message: Arc::from(err.to_string()),
        })
    }

    /// Most recent scheduled instant at or before `now`. Returns `None` if the
    /// expression has no occurrence in the year preceding `now`.
    pub fn previous_fire_unix(&self, now_unix: u64) -> Result<Option<u64>, CronError> {
        let cron = self.parse()?;
        let now = unix_to_utc(now_unix)?;
        match cron.find_previous_occurrence(&now, true) {
            Ok(dt) => Ok(Some(dt.timestamp().max(0) as u64)),
            Err(_) => Ok(None),
        }
    }

    /// Next scheduled instant strictly after `now`.
    pub fn next_fire_unix(&self, now_unix: u64) -> Result<Option<u64>, CronError> {
        let cron = self.parse()?;
        let now = unix_to_utc(now_unix)?;
        match cron.find_next_occurrence(&now, false) {
            Ok(dt) => Ok(Some(dt.timestamp().max(0) as u64)),
            Err(_) => Ok(None),
        }
    }
}

fn unix_to_utc(unix: u64) -> Result<DateTime<Utc>, CronError> {
    Utc.timestamp_opt(unix as i64, 0)
        .single()
        .ok_or_else(|| CronError::Storage(Arc::from(format!("invalid unix timestamp {unix}"))))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CronTask {
    pub id: Arc<str>,
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    pub sender: Arc<str>,
    pub prompt: Arc<str>,
    pub schedule: CronSchedule,
    #[serde(default)]
    pub retry: CronRetryPolicy,
    #[serde(default)]
    pub retry_state: CronRetryState,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Unix timestamp of the most recent scheduled tick we have already
    /// dispatched. The scheduler will re-fire only after the cron expression
    /// produces a new occurrence strictly greater than this value.
    ///
    /// `CronCreatorTool` anchors this at creation to the most recent instant
    /// at or before "now", so a freshly created task never back-fires for
    /// occurrences that predate it. `None` is treated as the epoch — only
    /// tasks meant to fire on first sight (e.g. the cron smoke test, which
    /// builds `CronTask` directly) should leave it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_unix: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronRetryPolicy {
    pub max_retries: u32,
    pub backoff_seconds: u64,
}

impl Default for CronRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronRetryState {
    pub consecutive_failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<Arc<str>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CronInvocation {
    pub task_id: Arc<str>,
    pub envelope: Envelope,
}

/// Prepended to a cron's `prompt` when it is replayed as a run envelope.
///
/// Without it, a prompt phrased like a scheduling request ("At 2:17 AM every
/// day, generate an audit…") reads to the model as "please set up a cron", so
/// it calls `cron_create`/`cron_list` and answers with a summary of the
/// schedule instead of doing the work. The directive pins the envelope as the
/// *execution* of an already-registered task, not a request to schedule one.
const CRON_EXECUTION_DIRECTIVE: &str = "\
[SCHEDULED TASK — EXECUTE NOW]
This message is an automated cron tick firing right now. The cron is already \
registered; you are the run it triggers. Carry out the instruction below \
immediately: call whatever tools and skills it needs and deliver the result \
to this conversation. Do NOT call cron_create, cron_list, or cron_remove, and \
do NOT merely describe, confirm, or summarize what the schedule would do. \
Treat the text after \"Instruction:\" as the task to perform now.

Instruction:
";

impl CronTask {
    pub fn new(
        id: impl Into<Arc<str>>,
        channel_id: ChannelId,
        conversation_id: ConversationId,
        prompt: impl Into<Arc<str>>,
        schedule: CronSchedule,
    ) -> Self {
        let id = id.into();
        Self {
            sender: Arc::from(format!("cron:{id}")),
            id,
            channel_id,
            conversation_id,
            prompt: prompt.into(),
            schedule,
            retry: CronRetryPolicy::default(),
            retry_state: CronRetryState::default(),
            enabled: true,
            last_fired_unix: None,
        }
    }

    pub fn to_envelope(&self) -> Envelope {
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from("kind"), Value::String("cron".to_owned()));
        metadata.insert(
            Arc::from("cron_id"),
            Value::String(self.id.as_ref().to_owned()),
        );
        if self.retry_state.consecutive_failures > 0 {
            metadata.insert(
                Arc::from("cron_retry_attempt"),
                Value::from(self.retry_state.consecutive_failures),
            );
        }

        Envelope {
            channel_id: self.channel_id.clone(),
            conversation_id: self.conversation_id.clone(),
            sender: Arc::clone(&self.sender),
            message: Message::text(
                MessageRole::User,
                Arc::from(format!("{CRON_EXECUTION_DIRECTIVE}{}", self.prompt)),
            ),
            metadata,
        }
    }

    fn is_due(&self, now_unix: u64) -> Result<bool, CronError> {
        if let Some(next_retry) = self.retry_state.next_retry_unix {
            return Ok(next_retry <= now_unix);
        }
        let Some(previous) = self.schedule.previous_fire_unix(now_unix)? else {
            return Ok(false);
        };
        Ok(previous > self.last_fired_unix.unwrap_or(0))
    }

    fn mark_success(&mut self, now_unix: u64) -> Result<(), CronError> {
        self.retry_state = CronRetryState::default();
        self.last_fired_unix = Some(self.fire_tick(now_unix)?);
        Ok(())
    }

    fn mark_failure(&mut self, now_unix: u64, error: impl Into<Arc<str>>) -> Result<(), CronError> {
        self.retry_state.consecutive_failures =
            self.retry_state.consecutive_failures.saturating_add(1);
        self.retry_state.last_error = Some(error.into());
        if self.retry_state.consecutive_failures <= self.retry.max_retries {
            let delay = self
                .retry
                .backoff_seconds
                .saturating_mul(u64::from(self.retry_state.consecutive_failures));
            self.retry_state.next_retry_unix = Some(now_unix.saturating_add(delay));
        } else {
            self.retry_state.consecutive_failures = 0;
            self.retry_state.next_retry_unix = None;
            self.last_fired_unix = Some(self.fire_tick(now_unix)?);
        }
        Ok(())
    }

    fn fire_tick(&self, now_unix: u64) -> Result<u64, CronError> {
        Ok(self
            .schedule
            .previous_fire_unix(now_unix)?
            .unwrap_or(now_unix))
    }
}

fn default_enabled() -> bool {
    true
}

#[derive(Default)]
pub struct CronScheduler {
    tasks: Vec<CronTask>,
}

pub struct CronStore {
    root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryMaintenanceCron {
    pub id: Arc<str>,
    pub caller: MemoryCaller,
    pub request: ReflectionRequest,
    pub schedule: CronSchedule,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_unix: Option<u64>,
}

impl MemoryMaintenanceCron {
    pub fn new(
        id: impl Into<Arc<str>>,
        caller: MemoryCaller,
        request: ReflectionRequest,
        schedule: CronSchedule,
    ) -> Self {
        Self {
            id: id.into(),
            caller,
            request,
            schedule,
            enabled: true,
            last_fired_unix: None,
        }
    }

    pub async fn run_due(
        &mut self,
        now_unix: u64,
        manager: &MemoryManager,
    ) -> Result<Option<ReflectionReport>, CronError> {
        if !self.enabled {
            return Ok(None);
        }
        let Some(previous) = self.schedule.previous_fire_unix(now_unix)? else {
            return Ok(None);
        };
        // First sight: anchor to the current tick rather than back-firing for
        // every occurrence that predates the cron being seen.
        let Some(last_fired) = self.last_fired_unix else {
            self.last_fired_unix = Some(previous);
            return Ok(None);
        };
        if previous <= last_fired {
            return Ok(None);
        }
        let report = manager.reflect(&self.caller, self.request.clone()).await?;
        self.last_fired_unix = Some(previous);
        Ok(Some(report))
    }
}

impl CronScheduler {
    pub fn new(tasks: impl IntoIterator<Item = CronTask>) -> Self {
        Self {
            tasks: tasks.into_iter().collect(),
        }
    }

    pub fn tasks(&self) -> &[CronTask] {
        &self.tasks
    }

    pub fn upsert_task(&mut self, task: CronTask) {
        if let Some(existing) = self
            .tasks
            .iter_mut()
            .find(|existing| existing.id == task.id)
        {
            *existing = task;
        } else {
            self.tasks.push(task);
        }
        self.tasks.sort_by(|left, right| left.id.cmp(&right.id));
    }

    /// Anchor every task that has never been dispatched (`last_fired_unix` is
    /// `None`) to `now`, recording the most recent scheduled instant at or
    /// before it as already handled. Returns the ids of the tasks that were
    /// changed so the caller can persist them.
    ///
    /// `CronCreatorTool` already anchors the tasks it creates; this is the
    /// safety net for tasks that reach the scheduler another way — a
    /// hand-written TOML, or a file written before anchoring existed — so they
    /// fire on their next scheduled instant instead of back-firing for every
    /// occurrence that predates the scheduler seeing them.
    pub fn anchor_unfired(&mut self, now_unix: u64) -> Result<Vec<Arc<str>>, CronError> {
        let mut anchored = Vec::new();
        for task in &mut self.tasks {
            if task.last_fired_unix.is_some() {
                continue;
            }
            if let Some(previous) = task.schedule.previous_fire_unix(now_unix)? {
                task.last_fired_unix = Some(previous);
                anchored.push(Arc::clone(&task.id));
            }
        }
        Ok(anchored)
    }

    pub fn due_invocations(&self, now_unix: u64) -> Result<Vec<CronInvocation>, CronError> {
        let mut due = Vec::new();
        for task in &self.tasks {
            if !task.enabled {
                continue;
            }
            if task.is_due(now_unix)? {
                due.push(CronInvocation {
                    task_id: Arc::clone(&task.id),
                    envelope: task.to_envelope(),
                });
            }
        }
        Ok(due)
    }

    pub fn record_success(&mut self, task_id: &str, now_unix: u64) -> Result<(), CronError> {
        self.task_mut(task_id)?.mark_success(now_unix)
    }

    pub fn record_failure(
        &mut self,
        task_id: &str,
        now_unix: u64,
        error: impl Into<Arc<str>>,
    ) -> Result<(), CronError> {
        self.task_mut(task_id)?.mark_failure(now_unix, error)
    }

    fn task_mut(&mut self, task_id: &str) -> Result<&mut CronTask, CronError> {
        self.tasks
            .iter_mut()
            .find(|task| task.id.as_ref() == task_id)
            .ok_or_else(|| CronError::Storage(Arc::from(format!("unknown cron task '{task_id}'"))))
    }

    pub async fn enqueue_due(
        &mut self,
        now_unix: u64,
        gateway: &mpsc::Sender<Envelope>,
    ) -> Result<usize, CronError> {
        let mut sent = 0;
        for task in &mut self.tasks {
            if !task.enabled || !task.is_due(now_unix)? {
                continue;
            }
            gateway
                .send(task.to_envelope())
                .await
                .map_err(|_| CronError::GatewayClosed)?;
            task.mark_success(now_unix)?;
            sent += 1;
        }
        Ok(sent)
    }
}

impl CronStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn load_scheduler(&self) -> Result<CronScheduler, CronError> {
        let mut tasks = Vec::new();
        for path in self.cron_files()? {
            let input = std::fs::read_to_string(&path).map_err(storage_error)?;
            tasks.push(toml::from_str(&input).map_err(toml_de_error)?);
        }
        Ok(CronScheduler::new(tasks))
    }

    pub fn save_task(&self, task: &CronTask) -> Result<(), CronError> {
        std::fs::create_dir_all(&self.root).map_err(storage_error)?;
        let encoded = toml::to_string_pretty(task).map_err(toml_ser_error)?;
        std::fs::write(self.task_path(&task.id)?, encoded).map_err(storage_error)
    }

    pub fn save_scheduler(&self, scheduler: &CronScheduler) -> Result<(), CronError> {
        for task in scheduler.tasks() {
            self.save_task(task)?;
        }
        Ok(())
    }

    pub fn task_path(&self, id: &str) -> Result<PathBuf, CronError> {
        let file_name = cron_file_name(id)?;
        Ok(self.root.join(file_name))
    }

    fn cron_files(&self) -> Result<Vec<PathBuf>, CronError> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(storage_error(err)),
        };
        let mut files = entries
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage_error)?
            .into_iter()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }
}

fn cron_file_name(id: &str) -> Result<String, CronError> {
    if id.is_empty()
        || !id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
    {
        return Err(CronError::Storage(Arc::from(format!(
            "invalid cron id '{id}'; expected letters, digits, '-' or '_'"
        ))));
    }
    Ok(format!("{id}.toml"))
}

fn storage_error(err: std::io::Error) -> CronError {
    CronError::Storage(Arc::from(err.to_string()))
}

fn toml_de_error(err: toml::de::Error) -> CronError {
    CronError::Storage(Arc::from(err.to_string()))
}

fn toml_ser_error(err: toml::ser::Error) -> CronError {
    CronError::Storage(Arc::from(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(expression: &str) -> CronTask {
        CronTask::new(
            "test",
            ChannelId::new("tui"),
            ConversationId::new("default"),
            "hello",
            CronSchedule::new(expression).expect("valid expression"),
        )
    }

    fn unix(year: i32, month: u32, day: u32, hour: u32, min: u32) -> u64 {
        Utc.with_ymd_and_hms(year, month, day, hour, min, 0)
            .single()
            .expect("valid datetime")
            .timestamp() as u64
    }

    #[test]
    fn schedule_rejects_invalid_expression() {
        let err = CronSchedule::new("not a cron").unwrap_err();
        assert!(matches!(err, CronError::InvalidExpression { .. }));
    }

    #[test]
    fn previous_and_next_fire_are_absolute() {
        let schedule = CronSchedule::new("17 2 * * *").unwrap();
        let now = unix(2026, 5, 20, 10, 0);
        let prev = schedule.previous_fire_unix(now).unwrap().unwrap();
        assert_eq!(prev, unix(2026, 5, 20, 2, 17));
        let next = schedule.next_fire_unix(now).unwrap().unwrap();
        assert_eq!(next, unix(2026, 5, 21, 2, 17));
    }

    #[test]
    fn envelope_wraps_prompt_with_execution_directive() {
        let envelope = task("17 2 * * *").to_envelope();
        assert_eq!(
            envelope.metadata.get("kind").and_then(Value::as_str),
            Some("cron"),
        );
        let content = envelope.message.content.as_ref();
        // The directive must mark the run as an execution, not a scheduling
        // request — otherwise the model just describes the cron back.
        assert!(
            content.contains("EXECUTE NOW"),
            "missing execution directive: {content}",
        );
        assert!(content.contains("Do NOT call cron_create"));
        // The original prompt is still present, verbatim, for the model to act on.
        assert!(
            content.ends_with("hello"),
            "directive dropped the original prompt: {content}",
        );
    }

    #[test]
    fn task_is_due_only_once_per_scheduled_tick() {
        let mut t = task("17 2 * * *");
        let now = unix(2026, 5, 20, 2, 18);
        assert!(t.is_due(now).unwrap());
        t.mark_success(now).unwrap();
        assert_eq!(t.last_fired_unix, Some(unix(2026, 5, 20, 2, 17)));

        // Still inside the same tick — must not refire.
        assert!(!t.is_due(unix(2026, 5, 20, 2, 30)).unwrap());
        assert!(!t.is_due(unix(2026, 5, 20, 23, 59)).unwrap());
        // Next day, after 02:17 — fires again.
        assert!(t.is_due(unix(2026, 5, 21, 2, 17)).unwrap());
    }

    #[test]
    fn retry_backoff_overrides_schedule() {
        let mut t = task("17 2 * * *");
        let fire_now = unix(2026, 5, 20, 2, 18);
        t.mark_failure(fire_now, "boom").unwrap();
        assert!(!t.is_due(fire_now + 30).unwrap());
        let retry_at = fire_now + t.retry.backoff_seconds;
        assert!(t.is_due(retry_at).unwrap());
    }

    #[test]
    fn retry_exhaustion_skips_to_next_tick() {
        let mut t = task("17 2 * * *");
        let fire_now = unix(2026, 5, 20, 2, 18);
        for _ in 0..=t.retry.max_retries {
            t.mark_failure(fire_now, "boom").unwrap();
        }
        assert_eq!(t.retry_state.consecutive_failures, 0);
        assert_eq!(t.last_fired_unix, Some(unix(2026, 5, 20, 2, 17)));
        assert!(!t.is_due(fire_now + 3600).unwrap());
    }

    #[test]
    fn persisted_task_fires_once_per_tick_across_reloads() {
        // Mimic the gateway scheduler tick: load -> due_invocations -> run ->
        // record_success -> save_task, repeated every 30s within one tick.
        let dir = std::env::temp_dir().join(format!(
            "agentos-cron-reload-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = CronStore::new(&dir);
        store.save_task(&task("17 2 * * *")).unwrap();

        let mut fires = 0;
        for offset in 0..5u64 {
            let now = unix(2026, 5, 21, 7, 17) + offset * 30;
            let mut scheduler = store.load_scheduler().unwrap();
            for invocation in scheduler.due_invocations(now).unwrap() {
                fires += 1;
                scheduler.record_success(&invocation.task_id, now).unwrap();
                let fired = scheduler
                    .tasks()
                    .iter()
                    .find(|t| t.id == invocation.task_id)
                    .unwrap();
                store.save_task(fired).unwrap();
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            fires, 1,
            "task fired {fires} times across one scheduled tick"
        );
    }

    #[test]
    fn fired_task_round_trips_through_toml() {
        let mut t = task("17 2 * * *");
        t.mark_success(unix(2026, 5, 21, 7, 17)).unwrap();
        assert_eq!(t.last_fired_unix, Some(unix(2026, 5, 21, 2, 17)));

        let encoded = toml::to_string_pretty(&t).expect("serialize cron task");
        let decoded: CronTask = toml::from_str(&encoded).expect("deserialize cron task");
        assert_eq!(decoded, t, "toml round-trip dropped fire state:\n{encoded}");

        // The reloaded task must not re-fire inside the same scheduled tick.
        assert!(!decoded.is_due(unix(2026, 5, 21, 7, 47)).unwrap());
    }

    #[test]
    fn anchor_unfired_stops_back_firing_on_first_sight() {
        let mut scheduler = CronScheduler::new([task("17 2 * * *")]);
        let now = unix(2026, 5, 21, 7, 17);
        // An unanchored task would otherwise be due immediately.
        assert!(!scheduler.due_invocations(now).unwrap().is_empty());

        let anchored = scheduler.anchor_unfired(now).unwrap();
        assert_eq!(anchored.len(), 1);
        // Anchored: no longer back-fires for today's earlier tick...
        assert!(scheduler.due_invocations(now).unwrap().is_empty());
        // ...but still fires at the next genuine occurrence.
        assert!(!scheduler
            .due_invocations(unix(2026, 5, 22, 2, 17))
            .unwrap()
            .is_empty());
        // Idempotent: an already-anchored task is left untouched.
        assert!(scheduler
            .anchor_unfired(unix(2026, 5, 22, 9, 0))
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn memory_maintenance_cron_anchors_on_first_sight() {
        use crate::memory::InMemoryMemory;
        use agentos_proto::{AgentId, TaskId};

        let manager = MemoryManager::new(Arc::new(InMemoryMemory::default()));
        let caller = MemoryCaller {
            agent_id: AgentId::new("alice"),
            task_id: TaskId::new("t1"),
            conversation_id: ConversationId::new("c1"),
            user_id: None,
            allowed_shared_domains: Vec::new(),
            audit_read_access: false,
        };
        let mut cron = MemoryMaintenanceCron::new(
            "mem-maint",
            caller.clone(),
            ReflectionRequest::for_conversation(&caller),
            CronSchedule::new("17 2 * * *").unwrap(),
        );

        // First sight anchors to the current tick without running reflection.
        let report = cron
            .run_due(unix(2026, 5, 21, 7, 17), &manager)
            .await
            .unwrap();
        assert!(report.is_none(), "memory cron back-fired on first sight");
        assert_eq!(cron.last_fired_unix, Some(unix(2026, 5, 21, 2, 17)));

        // Still within the same tick — must not run.
        assert!(cron
            .run_due(unix(2026, 5, 21, 23, 0), &manager)
            .await
            .unwrap()
            .is_none());
    }
}
