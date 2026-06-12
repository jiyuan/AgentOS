use agentos_interfaces::orchestrator::{MemoryFragment, OrchestratorTemplate};
use agentos_proto::TaskId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
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
}

/// Bound on the in-flight queue between `append_session_event` and the
/// background flusher task. Sized for ~25 seconds of run-loop events at
/// nominal write rates.
const SESSION_QUEUE_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub struct TaskWorkspace {
    root: PathBuf,
    writer: Arc<StdMutex<Option<Arc<SessionWriter>>>>,
}

#[derive(Debug)]
struct SessionWriter {
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
        Self {
            root: root.into(),
            writer: Arc::new(StdMutex::new(None)),
        }
    }

    fn writer(&self) -> Option<Arc<SessionWriter>> {
        // A poisoned guard still holds a structurally valid Option<Arc<_>>
        // (assignment happens after construction completes), so recover
        // instead of propagating a panic into every session write.
        let mut guard = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(writer) = guard.as_ref() {
            return Some(writer.clone());
        }
        let handle = Handle::try_current().ok()?;
        let (sender, receiver) = mpsc::channel::<SessionWrite>(SESSION_QUEUE_CAPACITY);
        let flusher = handle.spawn(session_flusher(receiver));
        let writer = Arc::new(SessionWriter {
            sender,
            _flusher: flusher,
        });
        *guard = Some(writer.clone());
        Some(writer)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn task_dir(&self, task_id: &TaskId) -> PathBuf {
        if matches!(task_id.as_str(), "main" | "min") {
            return self
                .root
                .parent()
                .unwrap_or_else(|| self.root())
                .join(task_id.as_str());
        }
        self.root.join(task_id.as_str())
    }

    pub fn init_task(&self, task_id: &TaskId) -> Result<(), TaskWorkspaceError> {
        let dir = self.task_dir(task_id);
        create_dir_all(&dir)?;
        create_dir_all(&dir.join("subagents"))?;
        create_dir_all(&dir.join("suborchestrators"))?;
        create_dir_all(&dir.join("sessions"))?;

        let metadata_path = dir.join("task.toml");
        if !metadata_path.exists() {
            let now = timestamp();
            write_toml(
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
        if !state_path.exists() {
            write_toml(&state_path, &TaskState::default())?;
        }
        Ok(())
    }

    pub fn load_state(&self, task_id: &TaskId) -> Result<Option<TaskState>, TaskWorkspaceError> {
        let path = self.task_dir(task_id).join("state.toml");
        match fs::read_to_string(&path) {
            Ok(input) => toml::from_str(&input)
                .map(Some)
                .map_err(|source| TaskWorkspaceError::TomlDe { path, source }),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(TaskWorkspaceError::Io { path, source }),
        }
    }

    pub fn save_state(
        &self,
        task_id: &TaskId,
        state: &TaskState,
    ) -> Result<(), TaskWorkspaceError> {
        write_toml(&self.task_dir(task_id).join("state.toml"), state)
    }

    pub fn create_subagent_config(
        &self,
        task_id: &TaskId,
        name: &str,
        config: &SubAgentWorkspaceConfig,
    ) -> Result<(), TaskWorkspaceError> {
        let dir = self.task_dir(task_id).join("subagents").join(name);
        create_dir_all(&dir)?;
        let path = dir.join("config.toml");
        if path.exists() {
            return Err(TaskWorkspaceError::ImmutableConfig { path });
        }
        write_toml(&path, config)
    }

    pub fn write_suborchestrator_graph(
        &self,
        task_id: &TaskId,
        template: &OrchestratorTemplate,
    ) -> Result<(), TaskWorkspaceError> {
        let dir = self
            .task_dir(task_id)
            .join("suborchestrators")
            .join(template.name.as_ref());
        create_dir_all(&dir)?;
        write_toml(&dir.join("graph.toml"), template)
    }

    pub fn append_session_event(
        &self,
        task_id: &TaskId,
        session_id: &str,
        event: &Value,
    ) -> Result<(), TaskWorkspaceError> {
        let path = self
            .task_dir(task_id)
            .join("sessions")
            .join(format!("{session_id}.jsonl"));
        let encoded = serde_json::to_string(event).map_err(|source| TaskWorkspaceError::Json {
            path: path.clone(),
            source,
        })?;
        let line = format!("{encoded}\n");

        if let Some(writer) = self.writer() {
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
                    return write_session_line_sync(&write.path, &write.line);
                }
            }
        }

        write_session_line_sync(&path, &line)
    }
}

async fn session_flusher(mut rx: mpsc::Receiver<SessionWrite>) {
    let mut files: HashMap<PathBuf, File> = HashMap::new();
    while let Some(write) = rx.recv().await {
        match cached_file(&mut files, &write.path) {
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
    cache: &'a mut HashMap<PathBuf, File>,
    path: &Path,
) -> std::io::Result<&'a mut File> {
    if !cache.contains_key(path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        cache.insert(path.to_path_buf(), file);
    }
    Ok(cache.get_mut(path).expect("file just inserted into cache"))
}

fn write_session_line_sync(path: &Path, line: &str) -> Result<(), TaskWorkspaceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| TaskWorkspaceError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| TaskWorkspaceError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(line.as_bytes())
        .map_err(|source| TaskWorkspaceError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn create_dir_all(path: &Path) -> Result<(), TaskWorkspaceError> {
    fs::create_dir_all(path).map_err(|source| TaskWorkspaceError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn write_toml<T>(path: &Path, value: &T) -> Result<(), TaskWorkspaceError>
where
    T: Serialize,
{
    let encoded = toml::to_string_pretty(value).map_err(|source| TaskWorkspaceError::TomlSer {
        path: path.to_path_buf(),
        source,
    })?;
    fs::write(path, encoded).map_err(|source| TaskWorkspaceError::Io {
        path: path.to_path_buf(),
        source,
    })
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
            .join("sessions")
            .join("session-1.jsonl");
        let lines = read_lines(&path);
        assert_eq!(lines, vec![r#"{"k":1}"#, r#"{"k":2}"#]);
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
