//! `[retention]` — how long the stores nothing else bounds are kept.
//!
//! M7 deliverable 10 / `QUOTA-001`. Seven things this runtime writes grow with
//! use. Three already had a section of their own to be bounded in — spill
//! (`[spill]`), memory records (`[memory.retention]`), and background jobs
//! (`[jobs]`) — and two are the *record* rather than a by-product, so they are
//! deleted by an explicitly authorized operator command and never by a
//! background sweep: the session log ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md))
//! and the two audit stores ([ADR-0005](../../../../docs/adr/0005-SAFETY_EVENTS.md)).
//!
//! What is left is this section: run traces, inbound attachments, the gateway's
//! own log, and the ingress ledger. None of them had a bound of any kind, and
//! all four are swept from the gateway's maintenance tick.
//!
//! # Two kinds of ceiling
//!
//! Age alone does not bound anything. A single busy day can write more than a
//! quiet year, so a deployment that keeps traces for thirty days has still not
//! said how much disk that is. Where the store is a directory the runtime owns
//! outright, there is therefore an age ceiling *and* a byte ceiling, applied in
//! that order: expire what is old, then evict oldest-first until the total
//! fits. Either may be `0`, which means that ceiling is not applied.
//!
//! # Why the destructive defaults are off
//!
//! Traces and attachments default to `0` — keep everything — for the same
//! reason `[spill].retention_days` does. They are what a deployment reads when
//! it needs to know what the agent actually did, and a runtime upgrade is not
//! the right moment to discover that a month of them has been deleted.
//!
//! The other two default to *on*, because neither is a record anybody reads
//! after the fact: a rotated log is the universal expectation for a log, and a
//! settled ingress row older than a month cannot affect deduplication for any
//! transport this runtime speaks.

use serde::{Deserialize, Serialize};

/// Shortest age ceiling a deployment may set on a store a live run may still be
/// writing to.
///
/// A trace file is appended to for the whole life of its run, and a run can sit
/// paused on an approval for a long time; a message's attachments are read by
/// the turn that received them. Sweeping at an hour's granularity would race
/// both. A day is the same floor `[spill].retention_days` uses and for the same
/// reason.
const MIN_RETENTION_DAYS: u64 = 1;

/// Smallest byte quota a deployment may set on a store.
///
/// Below roughly this the quota deletes everything on every sweep, including
/// what was written seconds ago, which is a broken deployment rather than a
/// tight one.
const MIN_STORE_BYTES: u64 = 1024 * 1024;

/// Smallest size the gateway log may be rotated at. Rotating more often than
/// this turns a log into a ring of fragments too short to read.
const MIN_LOG_BYTES: u64 = 64 * 1024;

/// Ceiling on how many rotated log files are kept. Past this the rotation is
/// the thing consuming the disk it exists to protect.
const MAX_LOG_KEEP: usize = 100;

/// Default rotation size for the gateway log.
pub const DEFAULT_GATEWAY_LOG_BYTES: u64 = 32 * 1024 * 1024;

/// Default number of rotated gateway logs kept beside the live one.
pub const DEFAULT_GATEWAY_LOG_KEEP: usize = 3;

/// Default age at which a *settled* ingress row is dropped.
pub const DEFAULT_INGRESS_DAYS: u64 = 30;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    /// Days a run's trace file is kept, or `0` to keep every trace.
    ///
    /// Age is the file's last write, so a run that is still appending — or one
    /// paused on an approval — is not swept out from under itself.
    pub trace_days: u64,
    /// Bytes all trace files may occupy together, or `0` for no ceiling.
    ///
    /// Applied after `trace_days`, oldest file first, until the total fits. A
    /// whole file at a time: half a trace is a run whose end cannot be read.
    pub trace_max_bytes: u64,
    /// Days an inbound attachment is kept, or `0` to keep every attachment.
    ///
    /// The unit is one message's attachment directory, not one file: a message
    /// that arrived with three images is answered by a turn that saw all
    /// three, so keeping one of them is not keeping the message.
    pub attachment_days: u64,
    /// Bytes all inbound attachments may occupy together, or `0` for no
    /// ceiling. Applied after `attachment_days`, oldest message first.
    pub attachment_max_bytes: u64,
    /// Days a *settled* ingress ledger row is kept, or `0` to keep every row.
    ///
    /// Only settled rows. An unsettled row is the record that a message was
    /// accepted and never finished — the thing the ledger exists for — and is
    /// never swept at any age.
    pub ingress_days: u64,
    /// Bytes the gateway log may reach before it is rotated, or `0` to let it
    /// grow without bound.
    pub gateway_log_max_bytes: u64,
    /// Rotated gateway logs kept beside the live one. The oldest is deleted
    /// once there are more than this.
    pub gateway_log_keep: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            trace_days: 0,
            trace_max_bytes: 0,
            attachment_days: 0,
            attachment_max_bytes: 0,
            ingress_days: DEFAULT_INGRESS_DAYS,
            gateway_log_max_bytes: DEFAULT_GATEWAY_LOG_BYTES,
            gateway_log_keep: DEFAULT_GATEWAY_LOG_KEEP,
        }
    }
}

impl RetentionConfig {
    /// Seconds a trace is kept, or `None` when traces are never swept by age.
    pub fn trace_max_age(&self) -> Option<std::time::Duration> {
        days_to_duration(self.trace_days)
    }

    /// Seconds an attachment is kept, or `None` when attachments are never
    /// swept by age.
    pub fn attachment_max_age(&self) -> Option<std::time::Duration> {
        days_to_duration(self.attachment_days)
    }

    /// Seconds a settled ingress row is kept, or `None` when the ledger is
    /// never pruned.
    pub fn ingress_max_age(&self) -> Option<std::time::Duration> {
        days_to_duration(self.ingress_days)
    }

    /// Whether anything in this section would delete or rotate anything. Lets
    /// the gateway skip the whole sweep, and the lease it takes to run it,
    /// rather than walking directories to find nothing to do.
    pub fn sweeps_anything(&self) -> bool {
        self.trace_days > 0
            || self.trace_max_bytes > 0
            || self.attachment_days > 0
            || self.attachment_max_bytes > 0
            || self.ingress_days > 0
            || self.gateway_log_max_bytes > 0
    }
}

fn days_to_duration(days: u64) -> Option<std::time::Duration> {
    (days > 0).then(|| std::time::Duration::from_secs(days * 24 * 60 * 60))
}

/// Reject a misconfigured section at load rather than at the first sweep.
///
/// Every branch here is a value that would delete more than the operator meant
/// — which is the one class of misconfiguration that cannot be noticed after
/// the fact, because what would have shown it is what was deleted.
pub fn validate_retention(config: &RetentionConfig) -> Result<(), String> {
    for (key, days) in [
        ("retention.trace_days", config.trace_days),
        ("retention.attachment_days", config.attachment_days),
        ("retention.ingress_days", config.ingress_days),
    ] {
        if days > 0 && days < MIN_RETENTION_DAYS {
            return Err(format!(
                "{key} must be at least {MIN_RETENTION_DAYS} (or 0 to keep everything), got {days}"
            ));
        }
    }
    for (key, bytes) in [
        ("retention.trace_max_bytes", config.trace_max_bytes),
        (
            "retention.attachment_max_bytes",
            config.attachment_max_bytes,
        ),
    ] {
        if bytes > 0 && bytes < MIN_STORE_BYTES {
            return Err(format!(
                "{key} must be at least {MIN_STORE_BYTES} (or 0 for no ceiling), got {bytes}; \
                 below that the quota deletes what was written seconds ago"
            ));
        }
    }
    if config.gateway_log_max_bytes > 0 && config.gateway_log_max_bytes < MIN_LOG_BYTES {
        return Err(format!(
            "retention.gateway_log_max_bytes must be at least {MIN_LOG_BYTES} (or 0 to never \
             rotate), got {}",
            config.gateway_log_max_bytes
        ));
    }
    if config.gateway_log_max_bytes > 0 && config.gateway_log_keep == 0 {
        return Err(
            "retention.gateway_log_keep must be at least 1 when the log is rotated; set \
             gateway_log_max_bytes = 0 to keep one growing file instead"
                .to_owned(),
        );
    }
    if config.gateway_log_keep > MAX_LOG_KEEP {
        return Err(format!(
            "retention.gateway_log_keep must be at most {MAX_LOG_KEEP}, got {}",
            config.gateway_log_keep
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: RetentionConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, RetentionConfig::default());
        assert!(validate_retention(&parsed).is_ok());
    }

    /// The two destructive ceilings are off and the two harmless ones are on.
    /// Pinned because the argument for each default is in the module docs and
    /// a silent flip would make those docs wrong.
    #[test]
    fn the_defaults_keep_the_record_and_bound_the_rest() {
        let config = RetentionConfig::default();
        assert_eq!(config.trace_max_age(), None);
        assert_eq!(config.attachment_max_age(), None);
        assert_eq!(config.trace_max_bytes, 0);
        assert_eq!(config.attachment_max_bytes, 0);
        assert!(config.ingress_max_age().is_some());
        assert!(config.gateway_log_max_bytes > 0);
        assert!(config.sweeps_anything());
    }

    #[test]
    fn everything_off_sweeps_nothing() {
        let config = RetentionConfig {
            trace_days: 0,
            trace_max_bytes: 0,
            attachment_days: 0,
            attachment_max_bytes: 0,
            ingress_days: 0,
            gateway_log_max_bytes: 0,
            gateway_log_keep: 3,
        };
        assert!(validate_retention(&config).is_ok());
        assert!(!config.sweeps_anything());
    }

    #[test]
    fn days_become_seconds() {
        let config = RetentionConfig {
            trace_days: 7,
            ..Default::default()
        };
        assert_eq!(
            config.trace_max_age(),
            Some(std::time::Duration::from_secs(7 * 24 * 60 * 60))
        );
    }

    /// A quota of a few kilobytes would delete every trace on every sweep,
    /// including the one being written.
    #[test]
    fn an_unusably_small_quota_is_rejected() {
        let config = RetentionConfig {
            trace_max_bytes: 4096,
            ..Default::default()
        };
        assert!(validate_retention(&config)
            .expect_err("4 KiB must be rejected")
            .contains("trace_max_bytes"));
    }

    /// Rotation with nothing kept is deletion wearing rotation's name, so the
    /// error points at the setting that actually means "one growing file".
    #[test]
    fn rotating_into_nothing_is_rejected_with_the_alternative_named() {
        let config = RetentionConfig {
            gateway_log_keep: 0,
            ..Default::default()
        };
        let error = validate_retention(&config).expect_err("keeping zero must be rejected");
        assert!(error.contains("gateway_log_max_bytes = 0"), "got: {error}");
    }

    #[test]
    fn an_absurd_keep_count_is_rejected() {
        let config = RetentionConfig {
            gateway_log_keep: 10_000,
            ..Default::default()
        };
        assert!(validate_retention(&config)
            .expect_err("10000 files must be rejected")
            .contains("gateway_log_keep"));
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error =
            toml::from_str::<RetentionConfig>("trace_day = 7").expect_err("a typo must fail");
        assert!(error.to_string().contains("trace_day"));
    }
}
