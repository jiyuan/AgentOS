//! Immediate provider-attempt trace records.

use super::{write_trace_record, RunnerError};
use agentos_proto::{RequestAttempt, RunId};
use serde_json::json;
use std::fs::OpenOptions;
use std::path::Path;

pub(super) fn persist_request_attempt(
    trace_dir: &Path,
    attempt: &RequestAttempt,
) -> Result<(), RunnerError> {
    crate::paths::create_private_dir(trace_dir).map_err(|err| RunnerError::TraceIo {
        path: err.path().to_path_buf(),
        source: err.into_io(),
    })?;
    let path = trace_dir.join(format!("{}.jsonl", trace_file_stem(&attempt.run_id)));
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|source| RunnerError::TraceIo {
        path: path.clone(),
        source,
    })?;
    let emitted_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    write_trace_record(
        &mut file,
        &path,
        &json!({
            "record_type": "request_attempt",
            "run_id": attempt.run_id.as_str(),
            "active_agent": attempt.active_agent.as_str(),
            "emitted_unix": emitted_unix,
            "attempt": attempt,
        }),
    )?;
    file.sync_data()
        .map_err(|source| RunnerError::TraceIo { path, source })
}

pub(super) fn trace_file_stem(run_id: &RunId) -> String {
    run_id
        .as_str()
        .chars()
        .map(|ch| match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '_' | '-' | ':' => ch,
            _ => '_',
        })
        .collect()
}
