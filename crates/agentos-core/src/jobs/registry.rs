//! The registry itself: what a job is, and the bounds it runs under.

use agentos_proto::SessionKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Jobs one typed session may have running at once.
///
/// A cap per session rather than per process: the failure this prevents is
/// one model spawning work in a loop, and a global cap would let that starve
/// every *other* conversation instead of only its own.
pub const DEFAULT_MAX_CONCURRENT_JOBS: usize = 4;

/// Output one job retains before it starts discarding.
pub const DEFAULT_JOB_OUTPUT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Arc<str>);

impl JobId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Rebuild an id from model-supplied text. Never trusted as a capability:
    /// every lookup is fenced by session principal, so a guessed id reaches nothing.
    pub fn parse(raw: &str) -> Self {
        Self(Arc::from(raw))
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Succeeded,
    Failed,
    /// Killed on request, or because its conversation was disposed.
    Cancelled,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Error)]
pub enum JobError {
    /// No such job *for the asking conversation*. Deliberately does not
    /// distinguish "does not exist" from "belongs to someone else": telling a
    /// caller which one it is leaks that the other conversation's job exists.
    #[error("no job '{0}' in this conversation")]
    Unknown(JobId),
    #[error("this conversation already has {limit} jobs running")]
    TooMany { limit: usize },
    #[error("job '{0}' has already finished")]
    AlreadyFinished(JobId),
}

/// What a producer declares when it starts a job.
#[derive(Clone, Debug)]
pub struct JobSpec {
    /// Coarse category, for the model and for traces: `tool`, `shell`, …
    pub kind: Arc<str>,
    /// Human- and model-facing description of what this job is doing.
    pub label: Arc<str>,
    pub session_key: SessionKey,
    /// Bytes of output retained. `None` takes the registry's default.
    pub output_limit_bytes: Option<usize>,
}

/// A job as an observer sees it. A snapshot, not a handle — holding one cannot
/// keep a disposed job alive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSnapshot {
    pub id: JobId,
    pub kind: Arc<str>,
    pub label: Arc<str>,
    pub state: JobState,
    /// Bytes of output retained so far.
    pub output_bytes: usize,
    /// Whether output was discarded because the cap was reached.
    pub output_truncated: bool,
    /// Set once the job reaches a terminal state: the tool's own message, or
    /// the reason it failed.
    pub detail: Option<Arc<str>>,
}

/// The write end a running job appends its output through.
///
/// Cloneable and `Send`, so a producer can hand it to whatever is actually
/// producing bytes — a subprocess reader, a stream, a loop over pages.
#[derive(Clone)]
pub struct JobSink {
    output: Arc<Mutex<Output>>,
}

impl JobSink {
    /// Append `chunk`, discarding anything past the job's cap.
    ///
    /// Discarding rather than failing: a job that produced too much output is
    /// still a job whose *first* output is worth reading, and the snapshot's
    /// `output_truncated` says what happened.
    pub fn append(&self, chunk: &str) {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let room = output.limit.saturating_sub(output.buffer.len());
        if room == 0 {
            output.truncated = !chunk.is_empty() || output.truncated;
            return;
        }
        if chunk.len() <= room {
            output.buffer.push_str(chunk);
            return;
        }
        let mut end = room;
        while !chunk.is_char_boundary(end) {
            end -= 1;
        }
        output.buffer.push_str(&chunk[..end]);
        output.truncated = true;
    }
}

#[derive(Debug)]
struct Output {
    buffer: String,
    limit: usize,
    truncated: bool,
}

struct Job {
    spec: JobSpec,
    state: JobState,
    detail: Option<Arc<str>>,
    output: Arc<Mutex<Output>>,
    cancel: CancellationToken,
    /// Fired once when the job reaches a terminal state, so a caller can await
    /// it instead of polling. Promotion (D2 → D3) is the reason this exists:
    /// the registry runs a promotable tool as a job from the start and waits
    /// on it for the tool's deadline, so a call that finishes in time is
    /// indistinguishable from one that never involved a job at all.
    finished: Arc<Notify>,
}

impl Job {
    fn snapshot(&self, id: &JobId) -> JobSnapshot {
        let output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        JobSnapshot {
            id: id.clone(),
            kind: Arc::clone(&self.spec.kind),
            label: Arc::clone(&self.spec.label),
            state: self.state,
            output_bytes: output.buffer.len(),
            output_truncated: output.truncated,
            detail: self.detail.clone(),
        }
    }
}

/// Background work, fenced by typed session principal.
pub struct JobRegistry {
    jobs: Mutex<BTreeMap<JobId, Job>>,
    next_id: AtomicU64,
    max_concurrent: usize,
    default_output_limit_bytes: usize,
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_JOBS, DEFAULT_JOB_OUTPUT_BYTES)
    }
}

impl JobRegistry {
    pub fn new(max_concurrent: usize, default_output_limit_bytes: usize) -> Self {
        Self {
            jobs: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            max_concurrent,
            default_output_limit_bytes,
        }
    }

    /// Start `work` as a job and return its id.
    ///
    /// `work` receives the sink to append output through and the job's
    /// cancellation token, and returns the terminal detail — the tool's message
    /// on success, the reason on failure. It is spawned, so it keeps running
    /// after the turn that started it returns.
    ///
    /// **`work` may be dropped at any await point.** Cancellation races the
    /// future and drops it rather than asking it to wind down, because that is
    /// the only way to stop arbitrary async work; code *after* an await in
    /// `work` is therefore not guaranteed to run, and anything that must happen
    /// on the way out belongs in a `Drop` impl (which is how `tools::exec`
    /// reaps its child). The token is passed anyway so a producer can hand it
    /// further down and get a prompt, explicit stop instead of waiting for the
    /// drop to propagate.
    pub fn start<F, Fut>(self: &Arc<Self>, spec: JobSpec, work: F) -> Result<JobId, JobError>
    where
        F: FnOnce(JobSink, CancellationToken) -> Fut,
        Fut: Future<Output = Result<Arc<str>, Arc<str>>> + Send + 'static,
    {
        let limit = spec
            .output_limit_bytes
            .unwrap_or(self.default_output_limit_bytes);
        let cancel = CancellationToken::new();
        let output = Arc::new(Mutex::new(Output {
            buffer: String::new(),
            limit,
            truncated: false,
        }));

        let id = {
            let mut jobs = self.lock();
            let running = jobs
                .values()
                .filter(|job| {
                    job.spec.session_key == spec.session_key && job.state == JobState::Running
                })
                .count();
            if running >= self.max_concurrent {
                return Err(JobError::TooMany {
                    limit: self.max_concurrent,
                });
            }
            let id = JobId(Arc::from(format!(
                "job-{}",
                self.next_id.fetch_add(1, Ordering::Relaxed)
            )));
            jobs.insert(
                id.clone(),
                Job {
                    spec: spec.clone(),
                    state: JobState::Running,
                    detail: None,
                    output: Arc::clone(&output),
                    cancel: cancel.clone(),
                    finished: Arc::new(Notify::new()),
                },
            );
            id
        };

        info!(
            job_id = id.as_str(),
            kind = spec.kind.as_ref(),
            session_key = spec.session_key.storage_key(),
            "job started"
        );

        let future = work(JobSink { output }, cancel.clone());
        let finished_id = id.clone();
        // The registry outlives the turn, so the task holds an `Arc` to it
        // rather than a borrow. Nothing awaits this handle — that is the point:
        // the turn that started the job has already moved on.
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let outcome = tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                outcome = future => Some(outcome),
            };
            registry.complete(&finished_id, outcome);
        });

        Ok(id)
    }

    /// Snapshot of one job, if it belongs to `session_key`.
    pub fn status(&self, session_key: &SessionKey, id: &JobId) -> Result<JobSnapshot, JobError> {
        let jobs = self.lock();
        let job = Self::owned(&jobs, session_key, id)?;
        Ok(job.snapshot(id))
    }

    /// Every job this session owns, oldest first.
    pub fn list(&self, session_key: &SessionKey) -> Vec<JobSnapshot> {
        self.lock()
            .iter()
            .filter(|(_, job)| job.spec.session_key == *session_key)
            .map(|(id, job)| job.snapshot(id))
            .collect()
    }

    /// Output retained so far, from `offset` bytes in.
    ///
    /// The offset is what makes reading incremental: a caller polls, remembers
    /// how much it has seen, and asks for the rest.
    pub fn output(
        &self,
        session_key: &SessionKey,
        id: &JobId,
        offset: usize,
    ) -> Result<String, JobError> {
        let jobs = self.lock();
        let job = Self::owned(&jobs, session_key, id)?;
        let output = job
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if offset >= output.buffer.len() {
            return Ok(String::new());
        }
        let mut start = offset;
        while !output.buffer.is_char_boundary(start) {
            start += 1;
        }
        Ok(output.buffer[start..].to_owned())
    }

    /// Cancel a job. Idempotent on an already-finished one, which reports
    /// [`JobError::AlreadyFinished`] rather than pretending it killed anything.
    pub fn kill(&self, session_key: &SessionKey, id: &JobId) -> Result<(), JobError> {
        let mut jobs = self.lock();
        let job = jobs
            .get_mut(id)
            .filter(|job| job.spec.session_key == *session_key)
            .ok_or_else(|| JobError::Unknown(id.clone()))?;
        if job.state.is_terminal() {
            return Err(JobError::AlreadyFinished(id.clone()));
        }
        job.cancel.cancel();
        job.state = JobState::Cancelled;
        job.detail = Some(Arc::from("killed on request"));
        job.finished.notify_waiters();
        info!(job_id = id.as_str(), "job killed");
        Ok(())
    }

    /// Wait up to `timeout` for a job to finish.
    ///
    /// `Ok(Some(..))` when it reached a terminal state in time, `Ok(None)` when
    /// it is still running. The waiter is registered *before* the state is
    /// checked, so a job that finishes in the gap is not missed.
    pub async fn wait_for(
        &self,
        session_key: &SessionKey,
        id: &JobId,
        timeout: std::time::Duration,
    ) -> Result<Option<JobSnapshot>, JobError> {
        let (finished, settled) = {
            let jobs = self.lock();
            let job = Self::owned(&jobs, session_key, id)?;
            (Arc::clone(&job.finished), job.state.is_terminal())
        };
        if settled {
            return self.status(session_key, id).map(Some);
        }

        let notified = finished.notified();
        tokio::pin!(notified);
        // Register before re-checking: `notified()` only listens from the point
        // it is enabled, so checking first would drop a notification sent in
        // between.
        notified.as_mut().enable();
        if self
            .status(session_key, id)
            .is_ok_and(|snapshot| snapshot.state.is_terminal())
        {
            return self.status(session_key, id).map(Some);
        }

        match tokio::time::timeout(timeout, notified).await {
            Ok(()) => self.status(session_key, id).map(Some),
            Err(_elapsed) => Ok(None),
        }
    }

    /// Cancel and forget every job a session owns.
    ///
    /// Called when the gateway clears a principal's session.
    pub fn dispose_session(&self, session_key: &SessionKey) -> usize {
        let mut jobs = self.lock();
        let doomed: Vec<JobId> = jobs
            .iter()
            .filter(|(_, job)| job.spec.session_key == *session_key)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &doomed {
            if let Some(job) = jobs.remove(id) {
                job.cancel.cancel();
            }
        }
        if !doomed.is_empty() {
            info!(
                session_key = session_key.storage_key(),
                jobs = doomed.len(),
                "conversation disposed; jobs cancelled"
            );
        }
        doomed.len()
    }

    /// Record a finished job. `None` means it was cancelled mid-flight.
    fn complete(&self, id: &JobId, outcome: Option<Result<Arc<str>, Arc<str>>>) {
        let mut jobs = self.lock();
        // Absent when the conversation was disposed while the job ran, which is
        // not an error: disposal is the stronger statement.
        let Some(job) = jobs.get_mut(id) else {
            return;
        };
        if job.state.is_terminal() {
            return;
        }
        match outcome {
            Some(Ok(detail)) => {
                job.state = JobState::Succeeded;
                job.detail = Some(detail);
            }
            Some(Err(detail)) => {
                warn!(job_id = id.as_str(), detail = detail.as_ref(), "job failed");
                job.state = JobState::Failed;
                job.detail = Some(detail);
            }
            None => {
                job.state = JobState::Cancelled;
                job.detail = Some(Arc::from("cancelled"));
            }
        }
        job.finished.notify_waiters();
    }

    fn owned<'a>(
        jobs: &'a BTreeMap<JobId, Job>,
        session_key: &SessionKey,
        id: &JobId,
    ) -> Result<&'a Job, JobError> {
        jobs.get(id)
            .filter(|job| job.spec.session_key == *session_key)
            .ok_or_else(|| JobError::Unknown(id.clone()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<JobId, Job>> {
        // The map is only ever inserted into and updated; a panic while held
        // leaves it structurally valid, so recovering beats propagating.
        self.jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, ChannelId, ConversationId, PrincipalKey, SenderIdentity};
    use std::time::Duration;

    fn session_key(conversation: &str) -> SessionKey {
        SessionKey::initial(PrincipalKey::v1(
            AgentId::new("agent"),
            ChannelId::new("test"),
            ConversationId::new(conversation),
            SenderIdentity::identified("user"),
        ))
    }

    fn spec(conversation: &str, label: &str) -> JobSpec {
        JobSpec {
            kind: Arc::from("test"),
            label: Arc::from(label),
            session_key: session_key(conversation),
            output_limit_bytes: None,
        }
    }

    /// Wait until `check` holds, or fail. Jobs complete on their own task, so
    /// there is no handle to await — polling is the honest way to observe them.
    async fn until(mut check: impl FnMut() -> bool) {
        for _ in 0..200 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition never held");
    }

    #[tokio::test]
    async fn a_job_outlives_the_call_that_started_it() {
        // The whole point of the item: `start` returns immediately and the work
        // keeps running.
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let id = registry
            .start(
                spec("conv-a", "slow work"),
                move |sink, _cancel| async move {
                    sink.append("started\n");
                    let _ = rx.await;
                    sink.append("finished\n");
                    Ok(Arc::from("all done"))
                },
            )
            .expect("the registry has room");

        // Still running, and already reporting output — incrementally, before
        // it has produced its result.
        until(|| {
            registry
                .output(&conversation, &id, 0)
                .is_ok_and(|out| out.contains("started"))
        })
        .await;
        let snapshot = registry.status(&conversation, &id).expect("owned");
        assert_eq!(snapshot.state, JobState::Running);
        assert_eq!(snapshot.detail, None);

        tx.send(()).expect("the job is still listening");
        until(|| {
            registry
                .status(&conversation, &id)
                .is_ok_and(|snapshot| snapshot.state == JobState::Succeeded)
        })
        .await;

        let snapshot = registry.status(&conversation, &id).expect("owned");
        assert_eq!(snapshot.detail.as_deref(), Some("all done"));
        assert_eq!(
            registry.output(&conversation, &id, 0).expect("owned"),
            "started\nfinished\n"
        );
    }

    #[tokio::test]
    async fn reading_from_an_offset_returns_only_what_is_new() {
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let id = registry
            .start(spec("conv-a", "chatty"), |sink, _cancel| async move {
                sink.append("first\n");
                sink.append("second\n");
                Ok(Arc::from("done"))
            })
            .expect("room");

        until(|| {
            registry
                .output(&conversation, &id, 0)
                .is_ok_and(|out| out.contains("second"))
        })
        .await;
        let seen = registry.output(&conversation, &id, 0).expect("owned");
        assert_eq!(seen, "first\nsecond\n");
        assert_eq!(
            registry
                .output(&conversation, &id, "first\n".len())
                .expect("owned"),
            "second\n"
        );
        assert_eq!(registry.output(&conversation, &id, 999).expect("owned"), "");
    }

    #[tokio::test]
    async fn a_job_is_invisible_to_every_other_conversation() {
        // Owner fencing, which is the security property of this module: naming
        // another conversation's id must not reach it.
        let registry = Arc::new(JobRegistry::default());
        let id = registry
            .start(spec("conv-a", "private"), |_sink, cancel| async move {
                cancel.cancelled().await;
                Ok(Arc::from("stopped"))
            })
            .expect("room");

        let intruder = session_key("conv-b");
        assert!(matches!(
            registry.status(&intruder, &id),
            Err(JobError::Unknown(_))
        ));
        assert!(matches!(
            registry.output(&intruder, &id, 0),
            Err(JobError::Unknown(_))
        ));
        assert!(matches!(
            registry.kill(&intruder, &id),
            Err(JobError::Unknown(_))
        ));
        assert!(registry.list(&intruder).is_empty());

        // And the owner still sees it, so the fence is the reason, not a bug.
        let owner = session_key("conv-a");
        assert!(registry.status(&owner, &id).is_ok());
        assert_eq!(registry.list(&owner).len(), 1);
    }

    #[tokio::test]
    async fn a_job_is_invisible_to_the_same_conversation_on_another_channel() {
        let registry = Arc::new(JobRegistry::default());
        let owner = session_key("42");
        let intruder = SessionKey::initial(PrincipalKey::v1(
            AgentId::new("agent"),
            ChannelId::new("other-channel"),
            ConversationId::new("42"),
            SenderIdentity::identified("user"),
        ));
        let id = registry
            .start(
                JobSpec {
                    kind: Arc::from("test"),
                    label: Arc::from("private"),
                    session_key: owner.clone(),
                    output_limit_bytes: None,
                },
                |_sink, cancel| async move {
                    cancel.cancelled().await;
                    Ok(Arc::from("stopped"))
                },
            )
            .expect("room");

        assert!(registry.status(&intruder, &id).is_err());
        assert!(registry.status(&owner, &id).is_ok());
    }

    #[tokio::test]
    async fn killing_a_job_stops_it_and_is_not_repeatable() {
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let id = registry
            .start(spec("conv-a", "endless"), |_sink, cancel| async move {
                cancel.cancelled().await;
                Ok(Arc::from("never observed"))
            })
            .expect("room");

        registry
            .kill(&conversation, &id)
            .expect("owned and running");
        let snapshot = registry.status(&conversation, &id).expect("owned");
        assert_eq!(snapshot.state, JobState::Cancelled);

        // A second kill reports the truth rather than pretending.
        assert!(matches!(
            registry.kill(&conversation, &id),
            Err(JobError::AlreadyFinished(_))
        ));

        // The task's own completion must not overwrite the kill.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            registry.status(&conversation, &id).expect("owned").state,
            JobState::Cancelled
        );
    }

    #[tokio::test]
    async fn disposing_a_conversation_cancels_and_forgets_its_jobs() {
        // The G1 seam. Nothing calls this yet, so its contract is only
        // guaranteed by this test.
        let registry = Arc::new(JobRegistry::default());
        let doomed = session_key("conv-a");
        let survivor = session_key("conv-b");
        // A drop guard rather than a flag set after `cancelled()`: cancelling
        // *drops* the work future, so code after an await point in it never
        // runs. Observing the drop is what proves the work actually stopped.
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct StoppedOnDrop(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for StoppedOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let id = registry
            .start(spec("conv-a", "doomed"), {
                let dropped = Arc::clone(&dropped);
                move |sink, _cancel| async move {
                    let _guard = StoppedOnDrop(dropped);
                    sink.append("running\n");
                    std::future::pending::<()>().await;
                    Ok(Arc::from("never reached"))
                }
            })
            .expect("room");
        let kept = registry
            .start(spec("conv-b", "kept"), |_sink, cancel| async move {
                cancel.cancelled().await;
                Ok(Arc::from("stopped"))
            })
            .expect("room");

        // Wait until the job is actually running. `start` only queues the work
        // on the executor; disposing before its first poll would drop it
        // unstarted, which is a different (and less interesting) path.
        until(|| {
            registry
                .output(&doomed, &id, 0)
                .is_ok_and(|out| out.contains("running"))
        })
        .await;

        assert_eq!(registry.dispose_session(&doomed), 1);
        until(|| dropped.load(Ordering::Relaxed)).await;

        // Forgotten, not merely cancelled.
        assert!(matches!(
            registry.status(&doomed, &id),
            Err(JobError::Unknown(_))
        ));
        // And another conversation's jobs are untouched.
        assert!(registry.status(&survivor, &kept).is_ok());
    }

    #[tokio::test]
    async fn a_job_cancelled_before_its_first_poll_never_runs() {
        // `start` queues work on the executor rather than running it, so a job
        // killed in the same turn it was started never executes a line of it.
        // Worth pinning: it is the difference between "stopped early" and
        // "never happened", and a producer with side effects needs to know
        // which it gets.
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let id = registry
            .start(spec("conv-a", "stillborn"), {
                let started = Arc::clone(&started);
                move |_sink, _cancel| async move {
                    started.store(true, Ordering::Relaxed);
                    Ok(Arc::from("ran"))
                }
            })
            .expect("room");
        registry
            .kill(&conversation, &id)
            .expect("owned and running");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !started.load(Ordering::Relaxed),
            "the work should not have run"
        );
        assert_eq!(
            registry.status(&conversation, &id).expect("owned").state,
            JobState::Cancelled
        );
    }

    #[tokio::test]
    async fn the_concurrency_cap_is_per_conversation() {
        // A cap shared across conversations would let one model's runaway loop
        // starve everybody else instead of only itself.
        let registry = Arc::new(JobRegistry::new(2, DEFAULT_JOB_OUTPUT_BYTES));
        let block = |_sink: JobSink, cancel: CancellationToken| async move {
            cancel.cancelled().await;
            Ok(Arc::from("stopped"))
        };

        registry.start(spec("conv-a", "1"), block).expect("room");
        registry.start(spec("conv-a", "2"), block).expect("room");
        assert!(matches!(
            registry.start(spec("conv-a", "3"), block),
            Err(JobError::TooMany { limit: 2 })
        ));
        // The other conversation still has its own budget.
        registry.start(spec("conv-b", "1"), block).expect("room");
    }

    #[tokio::test]
    async fn a_finished_job_frees_its_slot() {
        let registry = Arc::new(JobRegistry::new(1, DEFAULT_JOB_OUTPUT_BYTES));
        let conversation = session_key("conv-a");
        let id = registry
            .start(spec("conv-a", "quick"), |_sink, _cancel| async move {
                Ok(Arc::from("done"))
            })
            .expect("room");

        until(|| {
            registry
                .status(&conversation, &id)
                .is_ok_and(|snapshot| snapshot.state.is_terminal())
        })
        .await;
        registry
            .start(spec("conv-a", "next"), |_sink, _cancel| async move {
                Ok(Arc::from("done"))
            })
            .expect("the finished job released its slot");
    }

    #[tokio::test]
    async fn output_past_the_cap_is_discarded_and_flagged() {
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let mut spec = spec("conv-a", "loud");
        spec.output_limit_bytes = Some(8);
        let id = registry
            .start(spec, |sink, _cancel| async move {
                sink.append("12345");
                sink.append("6789012345");
                Ok(Arc::from("done"))
            })
            .expect("room");

        until(|| {
            registry
                .status(&conversation, &id)
                .is_ok_and(|snapshot| snapshot.state.is_terminal())
        })
        .await;
        let snapshot = registry.status(&conversation, &id).expect("owned");
        assert_eq!(snapshot.output_bytes, 8);
        assert!(snapshot.output_truncated);
        assert_eq!(
            registry.output(&conversation, &id, 0).expect("owned"),
            "12345678"
        );
    }

    #[tokio::test]
    async fn a_failing_job_records_why() {
        let registry = Arc::new(JobRegistry::default());
        let conversation = session_key("conv-a");
        let id = registry
            .start(spec("conv-a", "doomed"), |_sink, _cancel| async move {
                Err(Arc::from("the command exited 1"))
            })
            .expect("room");

        until(|| {
            registry
                .status(&conversation, &id)
                .is_ok_and(|snapshot| snapshot.state == JobState::Failed)
        })
        .await;
        assert_eq!(
            registry
                .status(&conversation, &id)
                .expect("owned")
                .detail
                .as_deref(),
            Some("the command exited 1")
        );
    }
}
