use serde::{Deserialize, Serialize};

/// Message-metadata key under which an LLM provider records the token usage of
/// the call that produced that message. The value is a JSON object whose shape
/// matches [`Usage`]'s token fields (`tool_calls` is the run-level accumulator
/// only and is absent on per-call metadata). The loop reads this key off each
/// assistant reply and folds it into the run's [`RunState`](crate) usage.
pub const TOKEN_USAGE_METADATA_KEY: &str = "agentos.token_usage";

/// Token accounting for a run. Used two ways: as the per-call breakdown a
/// provider attaches to a reply (see [`TOKEN_USAGE_METADATA_KEY`]), and as the
/// running per-run total the loop maintains by folding each call in with
/// [`Usage::record_call`].
///
/// The cache breakdown always satisfies
/// `cache_read_tokens + cache_write_tokens + cache_miss_tokens == input_tokens`:
/// - `cache_read_tokens`  — prompt tokens served from a prompt cache (hits).
/// - `cache_write_tokens` — prompt tokens written into the cache this call.
/// - `cache_miss_tokens`  — prompt tokens processed with no cache benefit.
///
/// Every token field carries `#[serde(default)]` so `RunState` snapshots
/// written before the cache breakdown existed still deserialize (the older
/// fields `input_tokens`/`output_tokens`/`tool_calls` are unchanged).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    /// `input_tokens + output_tokens` as reported by the provider (may differ
    /// from the sum when a provider counts reasoning/other tokens).
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub cache_write_tokens: u64,
    #[serde(default)]
    pub cache_miss_tokens: u64,
    /// Number of LLM calls folded in (run-level accumulator only; always 0 on
    /// per-call metadata).
    #[serde(default)]
    pub tool_calls: u64,
}

impl Usage {
    /// Read a provider's per-call accounting off a reply message.
    ///
    /// `None` when the metadata key is absent or unreadable — a provider that
    /// reports no usage is normal, and inventing zeros would make "not
    /// reported" indistinguishable from "cost nothing" in a run total.
    ///
    /// Lives here rather than at the two call sites so the loop and the
    /// provider gateway read usage the same way (M5 / `REQ-001`).
    pub fn from_message_metadata(message: &crate::Message) -> Option<Self> {
        let raw = message.metadata.get(TOKEN_USAGE_METADATA_KEY)?;
        // Deserialize by reference — `&Value` implements `Deserializer`, so
        // there is no need to clone the metadata value first.
        serde::Deserialize::deserialize(raw).ok()
    }

    /// Add every field of `other` into `self`, `tool_calls` included.
    /// Saturating so a pathological provider response cannot panic the caller
    /// on overflow. Use this to roll one run's accumulated usage into a wider
    /// (e.g. session) total; use [`Usage::record_call`] for a single LLM call.
    pub fn merge(&mut self, other: &Usage) {
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(other.cache_read_tokens);
        self.cache_write_tokens = self
            .cache_write_tokens
            .saturating_add(other.cache_write_tokens);
        self.cache_miss_tokens = self
            .cache_miss_tokens
            .saturating_add(other.cache_miss_tokens);
        self.tool_calls = self.tool_calls.saturating_add(other.tool_calls);
    }

    /// Fold one LLM call's token counts into this running per-run total and
    /// bump the call counter by one. Per-call metadata never carries its own
    /// `tool_calls`, so this is `merge` plus the single increment.
    pub fn record_call(&mut self, call: &Usage) {
        self.merge(call);
        self.tool_calls = self.tool_calls.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_call_accumulates_and_counts() {
        let mut total = Usage::default();
        total.record_call(&Usage {
            input_tokens: 100,
            output_tokens: 20,
            total_tokens: 120,
            cache_read_tokens: 80,
            cache_write_tokens: 0,
            cache_miss_tokens: 20,
            tool_calls: 0,
        });
        total.record_call(&Usage {
            input_tokens: 50,
            output_tokens: 10,
            total_tokens: 60,
            cache_read_tokens: 0,
            cache_write_tokens: 50,
            cache_miss_tokens: 0,
            tool_calls: 0,
        });
        assert_eq!(
            total,
            Usage {
                input_tokens: 150,
                output_tokens: 30,
                total_tokens: 180,
                cache_read_tokens: 80,
                cache_write_tokens: 50,
                cache_miss_tokens: 20,
                tool_calls: 2,
            }
        );
    }

    #[test]
    fn merge_sums_all_fields_including_call_count() {
        // A run that made 3 LLM calls folded into a session that already saw 2.
        let mut session = Usage {
            input_tokens: 10,
            output_tokens: 4,
            total_tokens: 14,
            cache_read_tokens: 6,
            cache_write_tokens: 0,
            cache_miss_tokens: 4,
            tool_calls: 2,
        };
        session.merge(&Usage {
            input_tokens: 90,
            output_tokens: 16,
            total_tokens: 106,
            cache_read_tokens: 40,
            cache_write_tokens: 10,
            cache_miss_tokens: 40,
            tool_calls: 3,
        });
        assert_eq!(
            session,
            Usage {
                input_tokens: 100,
                output_tokens: 20,
                total_tokens: 120,
                cache_read_tokens: 46,
                cache_write_tokens: 10,
                cache_miss_tokens: 44,
                tool_calls: 5,
            }
        );
    }

    #[test]
    fn legacy_snapshot_without_cache_fields_deserializes() {
        let legacy = r#"{"input_tokens":12,"output_tokens":3,"tool_calls":1}"#;
        let usage: Usage = serde_json::from_str(legacy).unwrap();
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(usage.tool_calls, 1);
        assert_eq!(usage.cache_read_tokens, 0);
        assert_eq!(usage.total_tokens, 0);
    }
}
