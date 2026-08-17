//! `[approval]` — how long an ask stays askable.
//!
//! Roadmap item G2. One number, and it exists because the alternative is
//! unbounded: a prompt nobody answers keeps a paused run pinned in memory and
//! keeps an action live that could still fire hours after the conversation
//! that asked for it moved on. Expiry resolves it as *cancelled* — not
//! rejected, because nobody rejected anything.

use crate::gateway::DEFAULT_APPROVAL_EXPIRY;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Floor on `expiry_seconds`. Below roughly this a prompt would expire while
/// the user is still reading it, which reads as the agent ignoring them.
const MIN_EXPIRY_SECONDS: u64 = 30;

/// Ceiling on `expiry_seconds` — one day. Past that "expires" is a fiction:
/// the run is pinned for practical purposes, and an operator who wants that
/// should say so with `expiry_seconds = 0` rather than a very large number.
const MAX_EXPIRY_SECONDS: u64 = 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApprovalConfig {
    /// Seconds an approval prompt counts for, or `0` for no expiry.
    ///
    /// `0` is a deliberate choice, not a disabled feature: a deployment where
    /// approvals are answered hours later by a human on another schedule is
    /// real. It costs the memory of every unanswered paused run.
    pub expiry_seconds: u64,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            expiry_seconds: DEFAULT_APPROVAL_EXPIRY.as_secs(),
        }
    }
}

impl ApprovalConfig {
    /// The configured expiry, or `None` when prompts do not expire.
    pub fn expiry(&self) -> Option<Duration> {
        (self.expiry_seconds > 0).then(|| Duration::from_secs(self.expiry_seconds))
    }
}

/// Reject a misconfigured section at load rather than at the first prompt.
pub fn validate_approval(config: &ApprovalConfig) -> Result<(), String> {
    if config.expiry_seconds == 0 {
        return Ok(());
    }
    if config.expiry_seconds < MIN_EXPIRY_SECONDS {
        return Err(format!(
            "approval.expiry_seconds must be at least {MIN_EXPIRY_SECONDS} (or 0 for no expiry), \
             got {}",
            config.expiry_seconds
        ));
    }
    if config.expiry_seconds > MAX_EXPIRY_SECONDS {
        return Err(format!(
            "approval.expiry_seconds must be at most {MAX_EXPIRY_SECONDS} (or 0 for no expiry), \
             got {}",
            config.expiry_seconds
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: ApprovalConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, ApprovalConfig::default());
        assert!(validate_approval(&parsed).is_ok());
        assert_eq!(parsed.expiry(), Some(DEFAULT_APPROVAL_EXPIRY));
    }

    /// Zero means "no expiry", not "expire instantly" — an instant expiry
    /// would refuse every approval before anyone could answer.
    #[test]
    fn zero_means_no_expiry() {
        let config = ApprovalConfig { expiry_seconds: 0 };
        assert!(validate_approval(&config).is_ok());
        assert_eq!(config.expiry(), None);
    }

    #[test]
    fn an_expiry_shorter_than_reading_the_prompt_is_rejected() {
        let config = ApprovalConfig { expiry_seconds: 5 };
        assert!(validate_approval(&config)
            .expect_err("5 seconds must be rejected")
            .contains("expiry_seconds"));
    }

    #[test]
    fn an_expiry_past_a_day_is_rejected_with_the_alternative_named() {
        let config = ApprovalConfig {
            expiry_seconds: 90_000,
        };
        let error = validate_approval(&config).expect_err("a week must be rejected");
        assert!(error.contains("0 for no expiry"));
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error = toml::from_str::<ApprovalConfig>("expiry = 60").expect_err("a typo must fail");
        assert!(error.to_string().contains("expiry"));
    }
}
