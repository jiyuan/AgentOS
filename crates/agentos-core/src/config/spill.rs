//! `[spill]` — where oversized tool output goes, and how long it stays.
//!
//! Roadmap item C2 built the store and fixed both of these in code: the root at
//! `<workspace>/spill`, and retention at "forever". X3 makes them a
//! deployment's decision, which is what the C2 status block deferred here.
//!
//! Retention matters more than it looks. A spill artifact is whatever a tool
//! read or fetched, so it is the most sensitive thing the runtime writes to
//! disk, and it accumulates for as long as the agent runs. "Keep everything"
//! is a legitimate choice for a machine that is audited; it should be a chosen
//! one.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Where spill artifacts go, relative to the workspace root.
pub const DEFAULT_SPILL_RELPATH: &str = "spill";

/// Shortest retention a deployment may set.
///
/// A spill is written so the *model* can read it back later in the same
/// conversation. Sweeping faster than a day would race the run that produced
/// it, turning a recoverable result back into the destroyed one C2 replaced.
const MIN_RETENTION_DAYS: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpillConfig {
    /// Where artifacts are written. Relative paths resolve against the
    /// workspace root; an absolute path is taken as given, for a deployment
    /// that wants spill on a different volume from the session database.
    pub root: PathBuf,
    /// Days an artifact is kept, or `0` to keep everything.
    ///
    /// `0` is a choice rather than a disabled feature — see the module docs.
    pub retention_days: u64,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from(DEFAULT_SPILL_RELPATH),
            retention_days: 0,
        }
    }
}

impl SpillConfig {
    /// The configured root, resolved against `workspace_root`.
    pub fn root_in(&self, workspace_root: &Path) -> PathBuf {
        if self.root.is_absolute() {
            self.root.clone()
        } else {
            workspace_root.join(&self.root)
        }
    }

    /// Seconds an artifact is kept, or `None` when nothing is swept.
    pub fn retention_secs(&self) -> Option<u64> {
        (self.retention_days > 0).then(|| self.retention_days * 24 * 60 * 60)
    }
}

/// Reject a misconfigured section at load rather than at the first sweep.
pub fn validate_spill(config: &SpillConfig) -> Result<(), String> {
    if config.root.as_os_str().is_empty() {
        return Err("spill.root must not be empty".to_owned());
    }
    // A relative root that climbs out of the workspace would put artifacts
    // somewhere the operator did not name and the retention sweep would then
    // delete from. Absolute roots are fine: those *are* named.
    if !config.root.is_absolute()
        && config
            .root
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(format!(
            "spill.root '{}' must not climb above the workspace; use an absolute path to store \
             artifacts elsewhere",
            config.root.display()
        ));
    }
    if config.retention_days > 0 && config.retention_days < MIN_RETENTION_DAYS {
        return Err(format!(
            "spill.retention_days must be at least {MIN_RETENTION_DAYS} (or 0 to keep \
             everything), got {}",
            config.retention_days
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unconfigured runtime must behave exactly as it did before this
    /// section existed: `<workspace>/spill`, nothing swept.
    #[test]
    fn the_default_matches_the_historical_behaviour() {
        let config = SpillConfig::default();
        assert!(validate_spill(&config).is_ok());
        assert_eq!(config.root_in(Path::new("/ws")), PathBuf::from("/ws/spill"));
        assert_eq!(config.retention_secs(), None);
    }

    #[test]
    fn an_absolute_root_is_taken_as_given() {
        let config = SpillConfig {
            root: PathBuf::from("/var/lib/agentos-spill"),
            ..Default::default()
        };
        assert!(validate_spill(&config).is_ok());
        assert_eq!(
            config.root_in(Path::new("/ws")),
            PathBuf::from("/var/lib/agentos-spill")
        );
    }

    /// A relative root that escapes the workspace would have the sweep deleting
    /// from a directory nobody named.
    #[test]
    fn a_relative_root_cannot_climb_out_of_the_workspace() {
        let config = SpillConfig {
            root: PathBuf::from("../../elsewhere"),
            ..Default::default()
        };
        assert!(validate_spill(&config)
            .expect_err("a climbing root must be rejected")
            .contains("must not climb"));
    }

    #[test]
    fn retention_converts_to_seconds() {
        let config = SpillConfig {
            retention_days: 7,
            ..Default::default()
        };
        assert_eq!(config.retention_secs(), Some(7 * 24 * 60 * 60));
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error = toml::from_str::<SpillConfig>("retention = 7").expect_err("a typo must fail");
        assert!(error.to_string().contains("retention"));
    }
}
