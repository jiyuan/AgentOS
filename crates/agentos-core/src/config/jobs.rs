//! `[jobs]` — bounds on background work.
//!
//! Roadmap item D3, extended by M7 / `QUOTA-001`. Three numbers and a list,
//! all of which exist to stop one conversation's model from spending the
//! machine: how many jobs it may have running, how much output each retains,
//! how long a finished one is still answerable, and which tools may be
//! promoted to a job when they outlive their D2 deadline.
//!
//! The third is the one `QUOTA-001` added. `max_concurrent` counts only
//! *running* jobs, so it never bounded the registry: a finished job kept its
//! snapshot and its retained output in memory for the life of the process,
//! and the only thing that ever removed one was a `/clear` on its
//! conversation. A gateway that runs for months and is never cleared holds
//! every job it has ever run.

use crate::jobs::{
    DEFAULT_COMPLETED_JOB_SECS, DEFAULT_JOB_OUTPUT_BYTES, DEFAULT_MAX_CONCURRENT_JOBS,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Smallest output budget a job may be given. Below roughly this, the retained
/// slice is too short to tell a caller anything about what the job did.
const MIN_JOB_OUTPUT_BYTES: usize = 1_024;

/// Shortest a finished job may be kept.
///
/// The turn that started a job typically reads its result on a later turn, and
/// a user who stepped away comes back in minutes. Reaping faster than this
/// would answer `job_status` with "no such job" for work that succeeded, which
/// reads as a bug rather than as a budget.
const MIN_COMPLETED_JOB_SECS: u64 = 60;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobsConfig {
    /// Jobs one conversation may have running at once.
    pub max_concurrent: usize,
    /// Bytes of output a job retains before discarding the rest.
    pub output_limit_bytes: usize,
    /// Seconds a finished job stays answerable by `job_status` and
    /// `job_output` before the registry forgets it, or `0` to keep every
    /// finished job until its conversation is cleared.
    ///
    /// This is memory, not disk: each retained job holds its snapshot and up
    /// to `output_limit_bytes` of output. `0` is the historical behaviour and
    /// is a legitimate choice for a short-lived process; it is a leak in a
    /// gateway that runs for months (M7 / `QUOTA-001`).
    pub completed_retention_secs: u64,
    /// Tools that become a background job instead of failing when they exceed
    /// their deadline (roadmap D2 → D3 promotion).
    ///
    /// An allowlist rather than "everything", because a promoted call is
    /// re-issued through [`agentos_interfaces::tool::Tool::call`] without the
    /// run context. Only `MemoryTool` uses that context today, but a
    /// third-party tool might, and silently dropping caller identity for a tool
    /// that authorises on it would be a security bug rather than a degraded
    /// result. Naming the tools makes it the operator's decision.
    pub promotable: Vec<Arc<str>>,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT_JOBS,
            output_limit_bytes: DEFAULT_JOB_OUTPUT_BYTES,
            completed_retention_secs: DEFAULT_COMPLETED_JOB_SECS,
            // The tool D2 exists for: a build or a test suite is exactly the
            // work worth keeping rather than killing.
            promotable: vec![Arc::from("shell")],
        }
    }
}

impl JobsConfig {
    /// How long a finished job is kept, or `None` when `0` says to keep every
    /// finished job until its conversation is cleared.
    pub fn completed_job_max_age(&self) -> Option<std::time::Duration> {
        (self.completed_retention_secs > 0)
            .then(|| std::time::Duration::from_secs(self.completed_retention_secs))
    }
}

/// Reject a misconfigured section at load rather than at the first job.
pub fn validate_jobs(config: &JobsConfig) -> Result<(), String> {
    if config.max_concurrent == 0 {
        return Err(
            "jobs.max_concurrent must be at least 1; set [resources.tools] to drop the job \
             tools instead of capping them at zero"
                .to_owned(),
        );
    }
    if config.output_limit_bytes < MIN_JOB_OUTPUT_BYTES {
        return Err(format!(
            "jobs.output_limit_bytes must be at least {MIN_JOB_OUTPUT_BYTES}, got {}",
            config.output_limit_bytes
        ));
    }
    if config.completed_retention_secs > 0
        && config.completed_retention_secs < MIN_COMPLETED_JOB_SECS
    {
        return Err(format!(
            "jobs.completed_retention_secs must be at least {MIN_COMPLETED_JOB_SECS} (or 0 to \
             keep finished jobs until the conversation is cleared), got {}",
            config.completed_retention_secs
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: JobsConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, JobsConfig::default());
        assert!(validate_jobs(&parsed).is_ok());
    }

    #[test]
    fn the_shell_tool_is_promotable_by_default() {
        // The whole reason promotion exists: D2 reliably kills the long build
        // it was meant to bound, and a job is what makes that recoverable.
        assert!(JobsConfig::default()
            .promotable
            .iter()
            .any(|name| name.as_ref() == "shell"));
    }

    #[test]
    fn a_zero_cap_is_rejected_with_the_alternative_named() {
        // Zero would look like "jobs disabled" but behave as "every job_start
        // fails", so the error points at the switch that actually turns them
        // off.
        let config = JobsConfig {
            max_concurrent: 0,
            ..Default::default()
        };
        let error = validate_jobs(&config).expect_err("zero must be rejected");
        assert!(error.contains("resources.tools"));
    }

    #[test]
    fn an_unreadably_small_output_budget_is_rejected() {
        let config = JobsConfig {
            output_limit_bytes: 16,
            ..Default::default()
        };
        assert!(validate_jobs(&config)
            .expect_err("16 bytes must be rejected")
            .contains("output_limit_bytes"));
    }

    /// A reap interval measured in seconds would forget a job before the turn
    /// that started it came back to read it.
    #[test]
    fn an_instant_reap_is_rejected() {
        let config = JobsConfig {
            completed_retention_secs: 5,
            ..Default::default()
        };
        assert!(validate_jobs(&config)
            .expect_err("5 seconds must be rejected")
            .contains("completed_retention_secs"));
    }

    /// Zero is the pre-`QUOTA-001` behaviour and stays available.
    #[test]
    fn keeping_finished_jobs_forever_is_still_allowed() {
        let config = JobsConfig {
            completed_retention_secs: 0,
            ..Default::default()
        };
        assert!(validate_jobs(&config).is_ok());
        assert_eq!(config.completed_job_max_age(), None);
    }

    #[test]
    fn the_default_reaps_finished_jobs_after_an_hour() {
        assert_eq!(
            JobsConfig::default().completed_job_max_age(),
            Some(std::time::Duration::from_secs(3600))
        );
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error =
            toml::from_str::<JobsConfig>("max_concurrency = 2").expect_err("a typo must fail");
        assert!(error.to_string().contains("max_concurrency"));
    }
}
