use crate::paths::{path_segment, ContainmentError, PathSegmentError, RootDir};
use agentos_interfaces::orchestrator::{MemoryFragment, OrchestratorTemplate};
use agentos_proto::TaskId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub enum TaskWorkspaceError {
    #[error("task workspace I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("task workspace TOML failed at {path}: {source}")]
    TomlSer {
        path: PathBuf,
        source: toml::ser::Error,
    },
    #[error("task workspace TOML failed at {path}: {source}")]
    TomlDe {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("task workspace JSON failed at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("immutable task config already exists at {path}")]
    ImmutableConfig { path: PathBuf },
    /// An identifier that becomes one component of a path is not usable as
    /// one (M4 / `FS-001`).
    ///
    /// Task ids, sub-agent names and orchestrator template names are chosen by
    /// a model's plan or by an envelope, and were joined onto the task root
    /// unvalidated. `Path::join` replaces the whole left-hand side when its
    /// argument is absolute, so a name of `/etc/cron.d` never landed beneath
    /// the task directory at all.
    #[error(transparent)]
    UnusableName(#[from] PathSegmentError),
    #[error(transparent)]
    Containment(#[from] ContainmentError),
}

/// Bound on the in-flight queue between `append_session_event` and the
/// background flusher task. Sized for ~25 seconds of run-loop events at
/// nominal write rates.
const SESSION_QUEUE_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub struct TaskWorkspace {
    root: PathBuf,
    boundary: PathBuf,
    tasks_prefix: Arc<str>,
    rooted: Arc<OnceLock<Arc<RootDir>>>,
    writer: Arc<StdMutex<Option<Arc<SessionWriter>>>>,
}

#[derive(Debug)]
struct SessionWriter {
    root: Arc<RootDir>,
    sender: mpsc::Sender<SessionWrite>,
    _flusher: JoinHandle<()>,
}

#[derive(Debug)]
struct SessionWrite {
    path: PathBuf,
    line: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub task_id: TaskId,
    pub origin: Arc<str>,
    pub status: Arc<str>,
    pub created_at: Arc<str>,
    pub updated_at: Arc<str>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_step: Option<Arc<str>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fragments: Vec<MemoryFragment>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubAgentWorkspaceConfig {
    pub role: Arc<str>,
    pub instructions: Arc<str>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<Arc<str>>,
}

impl TaskWorkspace {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let boundary = root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let tasks_prefix: Arc<str> = Arc::from(
            root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("tasks"),
        );
        Self {
            root,
            boundary,
            tasks_prefix,
            rooted: Arc::new(OnceLock::new()),
            writer: Arc::new(StdMutex::new(None)),
        }
    }

    fn root_dir(&self) -> Result<Arc<RootDir>, TaskWorkspaceError> {
        if let Some(root) = self.rooted.get() {
            return Ok(root.clone());
        }
        crate::paths::create_private_dir(&self.boundary).map_err(|err| TaskWorkspaceError::Io {
            path: err.path().to_path_buf(),
            source: err.into_io(),
        })?;
        let root = Arc::new(RootDir::open(&self.boundary)?);
        let _ = self.rooted.set(root.clone());
        Ok(self.rooted.get().cloned().unwrap_or(root))
    }

    fn relative_task_dir(&self, task_id: &TaskId) -> Result<PathBuf, TaskWorkspaceError> {
        let name = path_segment("task id", task_id.as_str())?;
        if matches!(name, "main" | "min") {
            return Ok(PathBuf::from(name));
        }
        Ok(Path::new(self.tasks_prefix.as_ref()).join(name))
    }

    fn writer(&self) -> Result<Option<Arc<SessionWriter>>, TaskWorkspaceError> {
        // A poisoned guard still holds a structurally valid Option<Arc<_>>
        // (assignment happens after construction completes), so recover
        // instead of propagating a panic into every session write.
        let mut guard = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(writer) = guard.as_ref() {
            return Ok(Some(writer.clone()));
        }
        let Some(handle) = Handle::try_current().ok() else {
            return Ok(None);
        };
        let root = self.root_dir()?;
        let (sender, receiver) = mpsc::channel::<SessionWrite>(SESSION_QUEUE_CAPACITY);
        let flusher = handle.spawn(session_flusher(root.clone(), receiver));
        let writer = Arc::new(SessionWriter {
            root,
            sender,
            _flusher: flusher,
        });
        *guard = Some(writer.clone());
        Ok(Some(writer))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where a task's files live, once its id has been shown to be a name.
    ///
    /// Fallible since M4 / `FS-001`: this is the join every other path in this
    /// module is built from, so validating here covers `state.toml`,
    /// `task.toml`, and the `subagents`, `suborchestrators` and `sessions`
    /// subtrees in one place.
    pub fn task_dir(&self, task_id: &TaskId) -> Result<PathBuf, TaskWorkspaceError> {
        Ok(self.boundary.join(self.relative_task_dir(task_id)?))
    }

    pub fn init_task(&self, task_id: &TaskId) -> Result<(), TaskWorkspaceError> {
        let root = self.root_dir()?;
        let dir = self.relative_task_dir(task_id)?;
        root.create_dir_all(dir.join("subagents"))?;
        root.create_dir_all(dir.join("suborchestrators"))?;
        root.create_dir_all(dir.join("sessions"))?;

        let metadata_path = dir.join("task.toml");
        if !rooted_exists(&root, &metadata_path)? {
            let now = timestamp();
            write_toml(
                &root,
                &metadata_path,
                &TaskMetadata {
                    task_id: task_id.clone(),
                    origin: Arc::from("run_loop"),
                    status: Arc::from("active"),
                    created_at: Arc::from(now.as_str()),
                    updated_at: Arc::from(now),
                },
            )?;
        }

        let state_path = dir.join("state.toml");
        if !rooted_exists(&root, &state_path)? {
            write_toml(&root, &state_path, &TaskState::default())?;
        }
        Ok(())
    }

    pub fn load_state(&self, task_id: &TaskId) -> Result<Option<TaskState>, TaskWorkspaceError> {
        let root = self.root_dir()?;
        let path = self.relative_task_dir(task_id)?.join("state.toml");
        match root.open_file(&path) {
            Ok(mut file) => {
                let mut input = String::new();
                file.read_to_string(&mut input)
                    .map_err(|source| TaskWorkspaceError::Io {
                        path: self.boundary.join(&path),
                        source,
                    })?;
                toml::from_str(&input)
                    .map(Some)
                    .map_err(|source| TaskWorkspaceError::TomlDe {
                        path: self.boundary.join(path),
                        source,
                    })
            }
            Err(ContainmentError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(source) => Err(source.into()),
        }
    }

    pub fn save_state(
        &self,
        task_id: &TaskId,
        state: &TaskState,
    ) -> Result<(), TaskWorkspaceError> {
        let root = self.root_dir()?;
        write_toml(
            &root,
            &self.relative_task_dir(task_id)?.join("state.toml"),
            state,
        )
    }

    pub fn create_subagent_config(
        &self,
        task_id: &TaskId,
        name: &str,
        config: &SubAgentWorkspaceConfig,
    ) -> Result<(), TaskWorkspaceError> {
        let root = self.root_dir()?;
        let dir = self
            .relative_task_dir(task_id)?
            .join("subagents")
            .join(path_segment("sub-agent name", name)?);
        root.create_dir_all(&dir)?;
        let path = dir.join("config.toml");
        if rooted_exists(&root, &path)? {
            return Err(TaskWorkspaceError::ImmutableConfig {
                path: self.boundary.join(path),
            });
        }
        write_toml(&root, &path, config)
    }

    pub fn write_suborchestrator_graph(
        &self,
        task_id: &TaskId,
        template: &OrchestratorTemplate,
    ) -> Result<(), TaskWorkspaceError> {
        let root = self.root_dir()?;
        let dir = self
            .relative_task_dir(task_id)?
            .join("suborchestrators")
            .join(path_segment(
                "orchestrator template name",
                template.name.as_ref(),
            )?);
        root.create_dir_all(&dir)?;
        write_toml(&root, &dir.join("graph.toml"), template)
    }

    pub fn append_session_event(
        &self,
        task_id: &TaskId,
        session_id: &str,
        event: &Value,
    ) -> Result<(), TaskWorkspaceError> {
        // Validated before the extension is appended, so a `session_id` of
        // `../../x` cannot become a legitimate-looking `x.jsonl` somewhere
        // else.
        let path = self
            .relative_task_dir(task_id)?
            .join("sessions")
            .join(format!("{}.jsonl", path_segment("session id", session_id)?));
        let encoded = serde_json::to_string(event).map_err(|source| TaskWorkspaceError::Json {
            path: self.boundary.join(&path),
            source,
        })?;
        let line = format!("{encoded}\n");

        if let Some(writer) = self.writer()? {
            match writer.sender.try_send(SessionWrite {
                path: path.clone(),
                line,
            }) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(write))
                | Err(mpsc::error::TrySendError::Closed(write)) => {
                    // Backpressure or flusher gone — preserve durability via
                    // a synchronous direct write rather than dropping the
                    // event.
                    return write_session_line_sync(&writer.root, &write.path, &write.line);
                }
            }
        }

        let root = self.root_dir()?;
        write_session_line_sync(&root, &path, &line)
    }
}

async fn session_flusher(root: Arc<RootDir>, mut rx: mpsc::Receiver<SessionWrite>) {
    let mut files: HashMap<PathBuf, File> = HashMap::new();
    while let Some(write) = rx.recv().await {
        match cached_file(&root, &mut files, &write.path) {
            Ok(file) => {
                if let Err(err) = file.write_all(write.line.as_bytes()) {
                    tracing::warn!(
                        path = %write.path.display(),
                        error = %err,
                        "session flusher write failed; dropping cached handle"
                    );
                    files.remove(&write.path);
                }
            }
            Err(err) => {
                tracing::warn!(
                    path = %write.path.display(),
                    error = %err,
                    "session flusher could not open file"
                );
            }
        }
    }
}

fn cached_file<'a>(
    root: &RootDir,
    cache: &'a mut HashMap<PathBuf, File>,
    path: &Path,
) -> Result<&'a mut File, ContainmentError> {
    if !cache.contains_key(path) {
        let file = root.append_file(path)?;
        cache.insert(path.to_path_buf(), file);
    }
    Ok(cache.get_mut(path).expect("file just inserted into cache"))
}

fn write_session_line_sync(
    root: &RootDir,
    path: &Path,
    line: &str,
) -> Result<(), TaskWorkspaceError> {
    let mut file = root.append_file(path)?;
    file.write_all(line.as_bytes())
        .map_err(|source| TaskWorkspaceError::Io {
            path: root.path().join(path),
            source,
        })
}

fn rooted_exists(root: &RootDir, path: &Path) -> Result<bool, TaskWorkspaceError> {
    match root.open_file(path) {
        Ok(_) => Ok(true),
        Err(ContainmentError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(source) => Err(source.into()),
    }
}

fn write_toml<T>(root: &RootDir, path: &Path, value: &T) -> Result<(), TaskWorkspaceError>
where
    T: Serialize,
{
    let encoded = toml::to_string_pretty(value).map_err(|source| TaskWorkspaceError::TomlSer {
        path: path.to_path_buf(),
        source,
    })?;
    root.write_file_atomic(path, encoded.as_bytes())?;
    Ok(())
}

fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_root() -> PathBuf {
        let nonce = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "agentos-task-workspace-test-{}-{nonce}-{nanos}",
            std::process::id()
        ))
    }

    fn read_lines(path: &Path) -> Vec<String> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn append_session_event_falls_back_to_sync_when_no_runtime() {
        // Outside any tokio runtime: writer() returns None, so the call must
        // synchronously hit disk before returning.
        let root = temp_root();
        let workspace = TaskWorkspace::new(&root);
        let task_id = TaskId::new("alpha");
        workspace.init_task(&task_id).unwrap();
        workspace
            .append_session_event(&task_id, "session-1", &json!({"k": 1}))
            .unwrap();
        workspace
            .append_session_event(&task_id, "session-1", &json!({"k": 2}))
            .unwrap();

        let path = workspace
            .task_dir(&task_id)
            .expect("alpha is a name")
            .join("sessions")
            .join("session-1.jsonl");
        let lines = read_lines(&path);
        assert_eq!(lines, vec![r#"{"k":1}"#, r#"{"k":2}"#]);
        fs::remove_dir_all(&root).ok();
    }

    // -----------------------------------------------------------------------
    // M4 / FS-001 — identifiers that become path components are names.
    // -----------------------------------------------------------------------

    /// The join that made this necessary: `Path::join` with an absolute
    /// argument discards the left-hand side entirely, so an unvalidated task
    /// id of `/tmp/elsewhere` did not land beneath the task root at all.
    #[test]
    fn a_task_id_that_is_a_path_is_refused_rather_than_joined() {
        let root = temp_root();
        let workspace = TaskWorkspace::new(&root);

        for id in ["/tmp/elsewhere", "../escape", "..", "a/b", ""] {
            let error = workspace
                .task_dir(&TaskId::new(id))
                .expect_err("a path is not a task id");
            assert!(
                matches!(error, TaskWorkspaceError::UnusableName(_)),
                "{id:?}: {error:?}"
            );
            assert!(
                workspace.init_task(&TaskId::new(id)).is_err(),
                "{id:?} must not create a directory"
            );
        }

        assert!(!root.join("..").join("escape").exists());
        fs::remove_dir_all(&root).ok();
    }

    /// Sub-agent names and orchestrator template names come from a model's
    /// plan, which is the least trustworthy source of a filename in the
    /// system.
    #[test]
    fn a_model_chosen_name_cannot_walk_out_of_its_task_directory() {
        let root = temp_root();
        let workspace = TaskWorkspace::new(&root);
        let task_id = TaskId::new("alpha");
        workspace.init_task(&task_id).expect("alpha initialises");

        let config = SubAgentWorkspaceConfig {
            role: Arc::from("researcher"),
            instructions: Arc::from("look things up"),
            resources: Vec::new(),
        };
        let error = workspace
            .create_subagent_config(&task_id, "../../evil", &config)
            .expect_err("a traversal is not a sub-agent name");
        assert!(
            matches!(error, TaskWorkspaceError::UnusableName(_)),
            "{error:?}"
        );

        let template = OrchestratorTemplate {
            name: Arc::from("/etc/cron.d/agentos"),
            stages: Vec::new(),
        };
        let error = workspace
            .write_suborchestrator_graph(&task_id, &template)
            .expect_err("an absolute path is not a template name");
        assert!(
            matches!(error, TaskWorkspaceError::UnusableName(_)),
            "{error:?}"
        );

        fs::remove_dir_all(&root).ok();
    }

    /// Validated before `.jsonl` is appended, so a traversal cannot arrive
    /// wearing a legitimate-looking extension.
    #[test]
    fn a_session_id_is_checked_before_its_extension_is_added() {
        let root = temp_root();
        let workspace = TaskWorkspace::new(&root);
        let task_id = TaskId::new("alpha");
        workspace.init_task(&task_id).expect("alpha initialises");

        let error = workspace
            .append_session_event(&task_id, "../../../escape", &json!({"k": 1}))
            .expect_err("a traversal is not a session id");
        assert!(
            matches!(error, TaskWorkspaceError::UnusableName(_)),
            "{error:?}"
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn append_session_event_buffers_through_flusher_when_runtime_present() {
        let root = temp_root();
        let task_id = TaskId::new("beta");
        let path = {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let path = runtime.block_on(async {
                let workspace = TaskWorkspace::new(&root);
                workspace.init_task(&task_id).unwrap();
                for n in 0..5 {
                    workspace
                        .append_session_event(&task_id, "session-2", &json!({"n": n}))
                        .unwrap();
                }
                // Drop the workspace inside the runtime so the flusher's
                // sender is closed; then yield until the flusher has drained
                // the queue.
                drop(workspace);
                let path = root.join("beta").join("sessions").join("session-2.jsonl");
                while !path.exists() || read_lines(&path).len() < 5 {
                    tokio::task::yield_now().await;
                }
                path
            });
            // Allow background tasks (the flusher) to settle before we drop
            // the runtime.
            runtime.shutdown_timeout(std::time::Duration::from_secs(2));
            path
        };
        let lines = read_lines(&path);
        assert_eq!(
            lines,
            (0..5)
                .map(|n| format!(r#"{{"n":{n}}}"#))
                .collect::<Vec<_>>()
        );
        fs::remove_dir_all(&root).ok();
    }
}
