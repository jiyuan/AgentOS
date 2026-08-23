//! Retention and quotas for the stores nothing else bounds (M7 / `QUOTA-001`).
//!
//! # Two mechanisms, and the line between them
//!
//! Seven things this runtime writes grow with use, and they do not all deserve
//! the same treatment. The line is whether the store *is* the record.
//!
//! **Swept in the background, here.** Run traces, inbound attachments, the
//! gateway log, spill artifacts, the ingress ledger's settled rows, and
//! finished background jobs. Each is either a by-product of work that is
//! recorded elsewhere or a transport-level detail; losing an old one loses
//! nothing a later question can be answered from. A sweep runs from the
//! gateway's maintenance tick under a durable lease, so two processes on one
//! database do not sweep the same directories at once.
//!
//! **Never swept; deleted only by an authorized operator command.** The
//! session log and the two audit stores, `safety_events` and
//! `memory_access_log`. These are the record. `[ADR-0006]` makes the session
//! log append-only *without qualification* — compaction, elision, fork and
//! `/clear` are projections over it, and the single deletion path is
//! `agentos-gateway purge` with the conversation named twice. `[ADR-0005]`
//! asks for the same shape for the audit stores in as many words: "an
//! explicit, authorized operation rather than a background cleanup." Adding a
//! timer that quietly deleted either would have been the second deletion path
//! both documents exist to forbid.
//!
//! So there is deliberately nothing in this module that touches
//! `session_items`, `session_epochs`, `safety_events`, or `memory_access_log`.
//! What bounds those lives in `agentos-gateway purge`, which reports what it
//! would delete, requires the count back, and writes a safety event saying it
//! happened.
//!
//! [ADR-0005]: ../../../../docs/adr/0005-SAFETY_EVENTS.md
//! [ADR-0006]: ../../../../docs/adr/0006-CLEAR_EPOCH.md

mod files;
mod log;

use crate::config::{RetentionConfig, SpillConfig};
use crate::gateway::IngressLedger;
use crate::jobs::JobRegistry;
use crate::spill::SpillStore;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// Where the swept stores live. Assembled by whoever is running the sweep,
/// because two of these paths are the *deployment's* layout rather than the
/// runtime's: the gateway and the one-shot CLI put attachments and logs in
/// different places, and a sweep that guessed would tidy the wrong tree.
#[derive(Clone, Debug, Default)]
pub struct RetentionTargets {
    /// Directory of per-run `<run-id>.jsonl` trace files.
    pub trace_dir: Option<PathBuf>,
    /// Root of the `<channel>/<conversation>/<message>/<file>` attachment tree.
    pub attachments_dir: Option<PathBuf>,
    /// The gateway's own log file. `None` for an entrypoint that has no log
    /// file of its own, such as the TUI.
    pub gateway_log: Option<PathBuf>,
}

/// What one sweep removed, for the single line the gateway logs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetentionReport {
    pub traces_removed: usize,
    pub trace_bytes: u64,
    pub attachments_removed: usize,
    pub attachment_bytes: u64,
    pub spill_runs_removed: usize,
    pub spill_bytes: u64,
    pub ingress_rows_removed: usize,
    pub jobs_reaped: usize,
    pub log_rotated: bool,
    pub logs_discarded: usize,
}

impl RetentionReport {
    /// Whether the sweep did anything at all. The gateway logs only when it
    /// did: a maintenance tick that finds nothing to do should be silent, or
    /// the log this module rotates becomes the log this module fills.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl fmt::Display for RetentionReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::new();
        for (label, removed, bytes) in [
            ("traces", self.traces_removed, self.trace_bytes),
            (
                "attachments",
                self.attachments_removed,
                self.attachment_bytes,
            ),
            ("spill runs", self.spill_runs_removed, self.spill_bytes),
        ] {
            if removed > 0 {
                parts.push(format!("{removed} {label} ({bytes} bytes)"));
            }
        }
        if self.ingress_rows_removed > 0 {
            parts.push(format!(
                "{} settled ingress rows",
                self.ingress_rows_removed
            ));
        }
        if self.jobs_reaped > 0 {
            parts.push(format!("{} finished jobs", self.jobs_reaped));
        }
        if self.log_rotated {
            parts.push("rotated the log".to_owned());
        }
        if self.logs_discarded > 0 {
            parts.push(format!("{} old log files", self.logs_discarded));
        }
        if parts.is_empty() {
            return formatter.write_str("nothing to sweep");
        }
        formatter.write_str(&parts.join(", "))
    }
}

/// Everything one sweep needs. A struct rather than eight arguments, and
/// borrowed rather than owned, so the caller keeps its registry and ledger.
pub struct RetentionSweep<'a> {
    pub retention: &'a RetentionConfig,
    pub spill_config: &'a SpillConfig,
    pub targets: &'a RetentionTargets,
    pub spill: Option<&'a SpillStore>,
    pub ingress: Option<&'a IngressLedger>,
    pub jobs: Option<&'a JobRegistry>,
    /// How long a finished job is kept, from `[jobs].completed_retention_secs`.
    /// `None` is that key's `0`: finished jobs are kept until their
    /// conversation is cleared.
    ///
    /// Resolved by the caller rather than re-derived here, so the "0 means
    /// off" convention lives in exactly one place — the config — and a sweep
    /// can be asked to reap everything by passing `Duration::ZERO`.
    pub completed_job_max_age: Option<Duration>,
}

impl RetentionSweep<'_> {
    /// Apply every configured ceiling once.
    ///
    /// Never fails. Each store is swept independently and a store that cannot
    /// be read or written is skipped, because this runs on the same tick that
    /// fires crons on a gateway that is still answering messages — a full disk
    /// or a permissions mistake in the trace directory must not become a
    /// gateway that stops.
    pub async fn run(&self) -> RetentionReport {
        let mut report = RetentionReport::default();

        if let Some(trace_dir) = self.targets.trace_dir.as_deref() {
            let swept = files::sweep(
                trace_dir,
                1,
                self.retention.trace_max_age(),
                nonzero(self.retention.trace_max_bytes),
            )
            .await;
            report.traces_removed = swept.removed;
            report.trace_bytes = swept.bytes;
        }

        if let Some(attachments) = self.targets.attachments_dir.as_deref() {
            // Depth 3: `<channel>/<conversation>/<message>/`. The message
            // directory is the unit — see `files`.
            let swept = files::sweep(
                attachments,
                3,
                self.retention.attachment_max_age(),
                nonzero(self.retention.attachment_max_bytes),
            )
            .await;
            report.attachments_removed = swept.removed;
            report.attachment_bytes = swept.bytes;
        }

        if let Some(spill) = self.spill {
            let swept = files::sweep(
                spill.root(),
                1,
                self.spill_config.retention_secs().map(Duration::from_secs),
                self.spill_config.max_bytes(),
            )
            .await;
            report.spill_runs_removed = swept.removed;
            report.spill_bytes = swept.bytes;
        }

        if let (Some(ingress), Some(max_age)) = (self.ingress, self.retention.ingress_max_age()) {
            match ingress.prune_settled(max_age) {
                Ok(removed) => report.ingress_rows_removed = removed,
                Err(err) => {
                    tracing::warn!(error = %err, "ingress ledger prune failed");
                }
            }
        }

        if let (Some(jobs), Some(max_age)) = (self.jobs, self.completed_job_max_age) {
            report.jobs_reaped = jobs.reap_completed(max_age);
        }

        if let Some(path) = self.targets.gateway_log.as_deref() {
            let rotated = log::rotate(
                path,
                self.retention.gateway_log_max_bytes,
                self.retention.gateway_log_keep,
            )
            .await;
            report.log_rotated = rotated.rotated;
            report.logs_discarded = rotated.discarded;
        }

        report
    }
}

/// Turn a `YYYY-MM-DD` written by an operator into the Unix second that day
/// begins, UTC.
///
/// The purges take an absolute date rather than "N days ago" on purpose. Both
/// report first and apply second, and the operator confirms by typing back the
/// count they were shown; a relative cutoff drifts between the two commands,
/// so the count they read would not be the count the apply computes and the
/// confirmation would be theatre.
pub fn cutoff_from_date(date: &str) -> Result<u64, String> {
    let parsed = chrono::NaiveDate::parse_from_str(date.trim(), "%Y-%m-%d")
        .map_err(|err| format!("'{date}' is not a YYYY-MM-DD date: {err}"))?;
    let midnight = parsed
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| format!("'{date}' has no midnight"))?;
    u64::try_from(midnight.and_utc().timestamp())
        .map_err(|_| format!("'{date}' is before the Unix epoch"))
}

fn nonzero(bytes: u64) -> Option<u64> {
    (bytes > 0).then_some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_report_says_so() {
        assert!(RetentionReport::default().is_empty());
        assert_eq!(RetentionReport::default().to_string(), "nothing to sweep");
    }

    #[test]
    fn a_date_becomes_utc_midnight() {
        assert_eq!(
            cutoff_from_date("2026-05-01").expect("parses"),
            1_777_593_600
        );
        assert_eq!(
            cutoff_from_date(" 1970-01-02 ").expect("parses"),
            24 * 60 * 60
        );
    }

    /// The cutoff decides what is destroyed, so anything but an unambiguous
    /// date is an error rather than a guess.
    #[test]
    fn a_vague_date_is_refused() {
        for input in ["last tuesday", "2026-13-01", "2026/05/01", "1969-12-31"] {
            assert!(
                cutoff_from_date(input).is_err(),
                "{input} must not be accepted"
            );
        }
    }

    /// The line an operator reads in the gateway log, so it has to name both
    /// the count and the space it recovered.
    #[test]
    fn a_report_names_what_went() {
        let report = RetentionReport {
            traces_removed: 4,
            trace_bytes: 2048,
            ingress_rows_removed: 91,
            log_rotated: true,
            ..Default::default()
        };
        assert!(!report.is_empty());
        assert_eq!(
            report.to_string(),
            "4 traces (2048 bytes), 91 settled ingress rows, rotated the log"
        );
    }
}
