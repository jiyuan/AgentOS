//! `[limits]` — sizes a deployment has real reason to change.
//!
//! Roadmap item C2 opens this section with the tool-result inline cap, which
//! was a compile-time constant (review finding F15: a `DEFAULT_*` constant is
//! not configurability). X3 folds the remaining hardcoded sizes in here rather
//! than adding a section per item.

use crate::spill::DEFAULT_TOOL_RESULT_INLINE_BYTES;
use crate::tools::{
    DEFAULT_DIRECTORY_LIST_ENTRIES, DEFAULT_FILE_READ_BYTES, DEFAULT_MAX_OUTPUT_BYTES,
    DEFAULT_TOOL_TIMEOUT_MS, MAX_FILE_READ_BYTES,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Smallest inline cap a deployment may set.
///
/// Below roughly this, a truncation notice and its spill hint would crowd out
/// the output they describe, so a run would spend tokens saying nothing. The
/// floor is a sanity bound on a misconfiguration, not a tuning knob.
const MIN_TOOL_RESULT_INLINE_BYTES: usize = 512;

/// Shortest deadline a deployment may set for a tool.
///
/// Below roughly this, process startup alone can exceed the budget, so every
/// call would fail for a reason that has nothing to do with the tool.
const MIN_TOOL_TIMEOUT_MS: u64 = 100;

/// Smallest useful directory listing. One entry tells a model the directory is
/// non-empty and nothing else.
const MIN_DIRECTORY_LIST_ENTRIES: usize = 1;

/// Smallest useful file read. Below a line's worth, every read reports
/// truncation and shows nothing.
const MIN_FILE_READ_BYTES: usize = 256;

/// Smallest capture from a child process. Below this the deadline and the
/// capture cap become indistinguishable to a caller reading the result.
const MIN_TOOL_OUTPUT_BYTES: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Bytes of a tool result kept inline in the transcript. The rest is
    /// spilled to a file the model can read back.
    pub tool_result_inline_bytes: usize,
    /// Deadline for a tool that declares none of its own, in milliseconds
    /// (roadmap item D2).
    pub tool_timeout_ms: u64,
    /// Per-tool deadlines, keyed by tool name. These win over both the default
    /// above and a tool's own `ToolSpec::timeout_ms`, so an operator always has
    /// the last word on how long their machine will wait.
    pub tool_timeout_overrides: BTreeMap<Arc<str>, u64>,
    /// Entries the `file` tool lists before it truncates.
    ///
    /// A cap on what one call can put in front of the model, not on what the
    /// directory holds: a listing of ten thousand names is a turn spent on
    /// nothing useful.
    pub directory_list_entries: usize,
    /// Bytes the `file` tool reads when a call does not ask for a size.
    pub file_read_bytes: usize,
    /// Bytes the `file` tool will read however large a call asks for. The
    /// ceiling, where `file_read_bytes` is the default.
    pub file_read_max_bytes: usize,
    /// Bytes captured from each of a child process's stdout and stderr before
    /// the rest is read and discarded.
    ///
    /// Bounds memory against a runaway process; it is not what shapes what the
    /// model sees, which is `tool_result_inline_bytes` and the spill store.
    pub tool_output_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            tool_result_inline_bytes: DEFAULT_TOOL_RESULT_INLINE_BYTES,
            tool_timeout_ms: DEFAULT_TOOL_TIMEOUT_MS,
            tool_timeout_overrides: BTreeMap::new(),
            directory_list_entries: DEFAULT_DIRECTORY_LIST_ENTRIES,
            file_read_bytes: DEFAULT_FILE_READ_BYTES,
            file_read_max_bytes: MAX_FILE_READ_BYTES,
            tool_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

impl LimitsConfig {
    /// The per-tool overrides as durations, ready for the tool registry.
    pub fn tool_timeout_overrides(&self) -> BTreeMap<Arc<str>, Duration> {
        self.tool_timeout_overrides
            .iter()
            .map(|(name, ms)| (Arc::clone(name), Duration::from_millis(*ms)))
            .collect()
    }

    pub fn tool_timeout(&self) -> Duration {
        Duration::from_millis(self.tool_timeout_ms)
    }
}

/// Reject a misconfigured section at load, not at the first oversized tool
/// result hours into a run.
pub fn validate_limits(config: &LimitsConfig) -> Result<(), String> {
    if config.tool_result_inline_bytes < MIN_TOOL_RESULT_INLINE_BYTES {
        return Err(format!(
            "limits.tool_result_inline_bytes must be at least {MIN_TOOL_RESULT_INLINE_BYTES}, got {}",
            config.tool_result_inline_bytes
        ));
    }
    for (name, ms) in std::iter::once((&Arc::from("tool_timeout_ms"), &config.tool_timeout_ms))
        .chain(config.tool_timeout_overrides.iter())
    {
        if *ms < MIN_TOOL_TIMEOUT_MS {
            return Err(format!(
                "limits tool timeout for '{name}' must be at least {MIN_TOOL_TIMEOUT_MS} ms, got {ms}"
            ));
        }
    }
    if config.directory_list_entries < MIN_DIRECTORY_LIST_ENTRIES {
        return Err(format!(
            "limits.directory_list_entries must be at least {MIN_DIRECTORY_LIST_ENTRIES}, got {}",
            config.directory_list_entries
        ));
    }
    for (name, bytes) in [
        ("file_read_bytes", config.file_read_bytes),
        ("file_read_max_bytes", config.file_read_max_bytes),
    ] {
        if bytes < MIN_FILE_READ_BYTES {
            return Err(format!(
                "limits.{name} must be at least {MIN_FILE_READ_BYTES}, got {bytes}"
            ));
        }
    }
    if config.file_read_bytes > config.file_read_max_bytes {
        return Err(format!(
            "limits.file_read_bytes ({}) cannot exceed limits.file_read_max_bytes ({}); the \
             default read would always be clamped",
            config.file_read_bytes, config.file_read_max_bytes
        ));
    }
    if config.tool_output_bytes < MIN_TOOL_OUTPUT_BYTES {
        return Err(format!(
            "limits.tool_output_bytes must be at least {MIN_TOOL_OUTPUT_BYTES}, got {}",
            config.tool_output_bytes
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_matches_the_historical_constant() {
        // An unconfigured runtime must behave exactly as it did before this
        // section existed.
        assert_eq!(LimitsConfig::default().tool_result_inline_bytes, 64 * 1024);
        assert!(validate_limits(&LimitsConfig::default()).is_ok());
    }

    #[test]
    fn an_absurdly_small_cap_fails_loud_at_load() {
        let config = LimitsConfig {
            tool_result_inline_bytes: 16,
            ..Default::default()
        };
        let error = validate_limits(&config).expect_err("16 bytes must be rejected");
        assert!(error.contains("tool_result_inline_bytes"));
    }

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: LimitsConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, LimitsConfig::default());
    }

    #[test]
    fn an_absurdly_short_deadline_fails_loud_at_load() {
        // Both the default and a per-tool override are floored: a 5 ms budget
        // would fail every call for a reason that has nothing to do with the
        // tool.
        let short = LimitsConfig {
            tool_timeout_ms: 5,
            ..Default::default()
        };
        assert!(validate_limits(&short)
            .expect_err("5 ms must be rejected")
            .contains("tool_timeout_ms"));

        let overridden = LimitsConfig {
            tool_timeout_overrides: BTreeMap::from([(Arc::from("shell"), 1)]),
            ..Default::default()
        };
        assert!(validate_limits(&overridden)
            .expect_err("a 1 ms override must be rejected")
            .contains("shell"));
    }

    #[test]
    fn per_tool_overrides_parse_into_durations() {
        let parsed: LimitsConfig =
            toml::from_str("tool_timeout_ms = 30000\n[tool_timeout_overrides]\nshell = 300000\n")
                .expect("the section parses");
        assert_eq!(parsed.tool_timeout(), Duration::from_millis(30_000));
        assert_eq!(
            parsed.tool_timeout_overrides().get("shell"),
            Some(&Duration::from_millis(300_000))
        );
        assert!(validate_limits(&parsed).is_ok());
    }

    /// A default read larger than the ceiling would be clamped on every call,
    /// so the configured default would silently not be the default.
    #[test]
    fn a_default_read_above_the_ceiling_is_rejected() {
        let config = LimitsConfig {
            file_read_bytes: 1024 * 1024,
            file_read_max_bytes: 64 * 1024,
            ..Default::default()
        };
        let error = validate_limits(&config).expect_err("an inverted pair must be rejected");
        assert!(error.contains("file_read_bytes"));
        assert!(error.contains("file_read_max_bytes"));
    }

    #[test]
    fn unusable_sizes_fail_loud_at_load() {
        for (toml, key) in [
            ("directory_list_entries = 0", "directory_list_entries"),
            ("file_read_bytes = 8", "file_read_bytes"),
            ("tool_output_bytes = 16", "tool_output_bytes"),
        ] {
            let config: LimitsConfig = toml::from_str(toml).expect("the key parses");
            assert!(
                validate_limits(&config)
                    .expect_err("must be rejected")
                    .contains(key),
                "{toml} should be rejected naming {key}"
            );
        }
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        // A typo'd limit that silently did nothing would be worse than a load
        // failure naming it.
        let error = toml::from_str::<LimitsConfig>("tool_result_inline_byte = 2048")
            .expect_err("a typo must fail");
        assert!(error.to_string().contains("tool_result_inline_byte"));
    }
}
