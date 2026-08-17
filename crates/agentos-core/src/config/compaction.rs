//! `[compaction]` — when a conversation summarizes its own history.
//!
//! Roadmap item C3. Compaction is the last and most expensive response to
//! context pressure: C1 measures it, C2 elides tool output for free, and only
//! then does this spend a model call to replace the oldest span of a
//! conversation with a summary.
//!
//! # On the default trigger
//!
//! `pressure_percent` is **provisional**. C1 shipped with its ~15% accuracy
//! check outstanding — it needs a live provider call to compare the estimate
//! against a reported `input_tokens` — and until that has been run against real
//! traffic, 90 is a reasoned choice rather than a measured one. It sits
//! deliberately above C2's 80% elision trigger so free pruning always gets the
//! first attempt, and the estimator is biased high, so a run reaching an
//! estimated 90% is very likely below that in truth. Tune this once the check
//! has run.
//!
//! It is an integer percent, not a ratio, because that is the unit C1 already
//! traces on every request. A float here would have to be compared against that
//! integer anyway, and would cost `WorkspaceConfig` its `Eq`.

use agentos_llm::LlmModelTier;
use serde::{Deserialize, Serialize};

/// Smallest tail a deployment may retain.
///
/// A checkpoint plus a single turn leaves the model no recent context to reason
/// over, so it would answer from a summary alone.
const MIN_RETAIN_TAIL_TURNS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactionConfig {
    /// Whether a run may summarize its own history. Disabling it does not
    /// disable C2's elision — pruning is free and always applies.
    pub enabled: bool,
    /// Percent of the context window above which the next turn compacts first.
    pub pressure_percent: usize,
    /// Conversation items kept verbatim after the checkpoint. The summary
    /// covers everything before them.
    pub retain_tail_turns: usize,
    /// Which model tier writes the summaries: `high`, `medium`, or `low`
    /// (roadmap item X3; deferred here by C3).
    ///
    /// A tier, not a `provider:model` string, because that is how every other
    /// model in this runtime is chosen — by `AGENTOS_LLM_MODEL_<TIER>` and by
    /// `/model` — and a second spelling would be a second thing to keep in
    /// sync. A name that is not a tier fails the load, naming the key.
    ///
    /// The default is `high`, meaning the summarizer is the same model that is
    /// having the conversation. Dropping to a cheaper tier is a real saving and
    /// a real trade: a summary is what the model reads for the rest of the
    /// conversation, so one written by a weaker model lowers the ceiling on
    /// every later turn, and it does so invisibly. Worth doing deliberately,
    /// which is what naming it here makes it.
    pub model: LlmModelTier,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pressure_percent: 90,
            retain_tail_turns: 8,
            model: LlmModelTier::High,
        }
    }
}

/// Reject a misconfigured section at load rather than on the turn that would
/// have compacted.
pub fn validate_compaction(config: &CompactionConfig) -> Result<(), String> {
    if config.pressure_percent == 0 || config.pressure_percent > 100 {
        return Err(format!(
            "compaction.pressure_percent must be between 1 and 100, got {}",
            config.pressure_percent
        ));
    }
    // Free pruning must get the first attempt at a request before a
    // summarizer call is spent on it. This was a comment and a test on the
    // default; a deployment that lowered `pressure_percent` under the elision
    // trigger would have quietly inverted the ladder.
    let elide_percent = (crate::prompt::PRUNE_TRIGGER_RATIO * 100.0) as usize;
    if config.pressure_percent <= elide_percent {
        return Err(format!(
            "compaction.pressure_percent must be above {elide_percent}, the point at which \
             oversized tool output is elided for free; got {}",
            config.pressure_percent
        ));
    }
    if config.retain_tail_turns < MIN_RETAIN_TAIL_TURNS {
        return Err(format!(
            "compaction.retain_tail_turns must be at least {MIN_RETAIN_TAIL_TURNS}, got {}",
            config.retain_tail_turns
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default keeps C3's behaviour: the summarizer is the model having
    /// the conversation.
    #[test]
    fn the_default_summarizer_is_the_conversation_model() {
        assert_eq!(CompactionConfig::default().model, LlmModelTier::High);
    }

    /// A typo'd tier is a parse failure, so it is caught at load with the key
    /// named rather than on the turn that would have compacted.
    #[test]
    fn an_unknown_tier_fails_loud_at_load() {
        let error = toml::from_str::<CompactionConfig>("model = \"cheapest\"")
            .expect_err("an unknown tier must be rejected");
        assert!(error.to_string().contains("model"));
    }

    #[test]
    fn a_tier_parses_the_way_it_is_spelled_everywhere_else() {
        let parsed: CompactionConfig =
            toml::from_str("model = \"low\"").expect("a tier name parses");
        assert_eq!(parsed.model, LlmModelTier::Low);
        assert!(validate_compaction(&parsed).is_ok());
    }

    #[test]
    fn the_default_trigger_sits_above_the_elision_trigger() {
        // Order matters: free pruning must always get the first attempt at a
        // request before a summarizer call is spent on it.
        let config = CompactionConfig::default();
        let elision_percent = (crate::prompt::PRUNE_TRIGGER_RATIO * 100.0) as usize;
        assert!(config.pressure_percent > elision_percent);
        assert!(validate_compaction(&config).is_ok());
    }

    /// ...and a deployment cannot invert the ladder. Compacting before eliding
    /// spends a model call on a request that free pruning would have fixed.
    #[test]
    fn a_trigger_below_the_elision_trigger_is_rejected() {
        let config = CompactionConfig {
            pressure_percent: 50,
            ..Default::default()
        };
        assert!(validate_compaction(&config)
            .expect_err("an inverted ladder must be rejected")
            .contains("elided for free"));
    }

    #[test]
    fn an_omitted_section_parses_to_the_default() {
        let parsed: CompactionConfig = toml::from_str("").expect("an empty section parses");
        assert_eq!(parsed, CompactionConfig::default());
    }

    #[test]
    fn a_percent_outside_the_range_fails_loud_at_load() {
        for percent in [0usize, 101, 1_000] {
            let config = CompactionConfig {
                pressure_percent: percent,
                ..Default::default()
            };
            let error =
                validate_compaction(&config).expect_err("an out-of-range percent is rejected");
            assert!(error.contains("pressure_percent"));
        }
    }

    #[test]
    fn a_tail_too_short_to_reason_over_is_rejected() {
        let config = CompactionConfig {
            retain_tail_turns: 1,
            ..Default::default()
        };
        let error = validate_compaction(&config).expect_err("a one-item tail must be rejected");
        assert!(error.contains("retain_tail_turns"));
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error = toml::from_str::<CompactionConfig>("pressure_percentage = 50")
            .expect_err("a typo must fail");
        assert!(error.to_string().contains("pressure_percentage"));
    }
}
