//! Append-only span and event trace records for one run.
//!
//! Split out of `runner.rs`, which sat at exactly the 800-line production
//! ceiling: the next line added to it would have failed the module-size gate.
//! These three functions are the whole of the JSONL trace-writing path and have
//! no coupling to the run loop beyond `RunState`, so they move as one piece.

use super::trace_sink;
use super::{RunnerError, TraceSink};
use agentos_interfaces::RunState;
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub(super) fn persist_trace_records(
    state: &RunState,
    trace_dir: &Path,
    span_start: usize,
    event_start: usize,
    phase: &'static str,
) -> Result<(), RunnerError> {
    // Private (M8 / `GW-001`). A trace is the whole run: prompts, tool
    // arguments, model output. Append-only, so there is nothing to replace
    // atomically — but the mode still has to be set at creation, because
    // chmod-after-create leaves a window where it is readable.
    crate::paths::create_private_dir(trace_dir).map_err(|err| RunnerError::TraceIo {
        path: err.path().to_path_buf(),
        source: err.into_io(),
    })?;
    let path = trace_dir.join(format!(
        "{}.jsonl",
        trace_sink::trace_file_stem(&state.run_id)
    ));
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

    // Wall-clock at persist time, stamped onto every record in this batch.
    // Trace files are append-only per run_id, so long-lived gateway sessions
    // accumulate weeks of records in one file; without a per-record timestamp
    // any consumer windowing by file mtime (e.g. the audit skill) re-counts the
    // whole history every run. One clock read per persist batch is negligible
    // against the ≤2ms/turn budget.
    let emitted_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);

    for (index, span) in state.trace_spans.iter().enumerate().skip(span_start) {
        let record = json!({
            "record_type": "span",
            "phase": phase,
            "run_id": state.run_id.as_str(),
            "active_agent": state.active_agent.as_str(),
            "index": index,
            "emitted_unix": emitted_unix,
            "span": span,
        });
        write_trace_record(&mut file, &path, &record)?;
    }
    for (index, event) in state.trace_events.iter().enumerate().skip(event_start) {
        let record = json!({
            "record_type": "event",
            "phase": phase,
            "run_id": state.run_id.as_str(),
            "active_agent": state.active_agent.as_str(),
            "index": index,
            "emitted_unix": emitted_unix,
            "event": event,
        });
        write_trace_record(&mut file, &path, &record)?;
    }
    Ok(())
}

pub(super) fn persist_trace_records_with_sink(
    state: &RunState,
    trace_sink: Option<&dyn TraceSink>,
    span_start: usize,
    event_start: usize,
    phase: &'static str,
) -> Result<(), RunnerError> {
    let Some(trace_sink) = trace_sink else {
        return Ok(());
    };
    trace_sink.persist(state, span_start, event_start, phase)
}

pub(super) fn write_trace_record(
    file: &mut std::fs::File,
    path: &Path,
    record: &Value,
) -> Result<(), RunnerError> {
    let encoded = serde_json::to_string(record).map_err(|source| RunnerError::TraceJson {
        path: path.to_path_buf(),
        source,
    })?;
    writeln!(file, "{encoded}").map_err(|source| RunnerError::TraceIo {
        path: path.to_path_buf(),
        source,
    })
}
