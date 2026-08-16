//! `[limits]` — sizes a deployment has real reason to change.
//!
//! Roadmap item C2 opens this section with the tool-result inline cap, which
//! was a compile-time constant (review finding F15: a `DEFAULT_*` constant is
//! not configurability). X3 folds the remaining hardcoded sizes in here rather
//! than adding a section per item.

use crate::spill::DEFAULT_TOOL_RESULT_INLINE_BYTES;
use serde::{Deserialize, Serialize};

/// Smallest inline cap a deployment may set.
///
/// Below roughly this, a truncation notice and its spill hint would crowd out
/// the output they describe, so a run would spend tokens saying nothing. The
/// floor is a sanity bound on a misconfiguration, not a tuning knob.
const MIN_TOOL_RESULT_INLINE_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Bytes of a tool result kept inline in the transcript. The rest is
    /// spilled to a file the model can read back.
    pub tool_result_inline_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            tool_result_inline_bytes: DEFAULT_TOOL_RESULT_INLINE_BYTES,
        }
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
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        // A typo'd limit that silently did nothing would be worse than a load
        // failure naming it.
        let error = toml::from_str::<LimitsConfig>("tool_result_inline_byte = 2048")
            .expect_err("a typo must fail");
        assert!(error.to_string().contains("tool_result_inline_byte"));
    }
}
