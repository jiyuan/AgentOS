//! Estimating how much of the context window a request will occupy.
//!
//! Roadmap item C1 in `docs/TRANSFER_ROADMAP.md`. This is a heuristic, not a
//! tokenizer: it exists so pressure is *observable* before compaction (C3)
//! exists to relieve it, and so C3's thresholds can be tuned against recorded
//! traffic instead of guessed.
//!
//! # Why two rates
//!
//! The usual "four characters per token" rule describes English. It is badly
//! wrong for Chinese, Japanese, and Korean, where a character is roughly a
//! whole token — a 4:1 divisor under-counts such a conversation by about four
//! times. This runtime is deployed against Feishu and Telegram with DeepSeek,
//! so mixed CJK traffic is the normal case, not an edge case.
//!
//! # Which way it errs
//!
//! Usually high, but not always, and the difference matters. An over-estimate
//! compacts earlier than strictly necessary; an under-estimate lets a request
//! hit the provider's hard context limit with no compaction attempted, which is
//! the failure C4 exists to recover from.
//!
//! This module used to claim it was never dangerously low. **It is.** C1's
//! accuracy check measured it against a live provider
//! (`docs/TOKEN_CALIBRATION.md`) and found:
//!
//! - Chinese prose **+32%** — one token per wide character is well above what a
//!   byte-pair encoder actually charges for CJK. Safe, and the single largest
//!   reason a pressure reading runs high.
//! - Tool schemas **+38%** — JSON packs far better than 4:1 under BPE.
//! - English prose **+22%**.
//! - Symbol-dense ASCII (code) **−19%** — the 4:1 rate is derived from prose,
//!   and code tokenizes at closer to 3.2 characters per token.
//! - Realistic mixed requests — a transcript with prose, CJK, a tool call and
//!   its output — land between **+1% and +16%**.
//!
//! No single divisor fixes the last two together: code is denser than 4:1 while
//! JSON is sparser, and both are "symbol-heavy ASCII" to any character-class
//! rule. Rather than fit a more elaborate heuristic to one provider's
//! tokenizer, the under-estimate is carried by the threshold instead:
//! `[compaction].pressure_percent` defaults to 84 so that a request reading 84%
//! of the window is at most 100% of it at the measured error. See
//! `config::compaction` for that derivation.
//!
//! # Checking it against reality
//!
//! `agentos-gateway calibrate` sends a fixed corpus to the configured provider
//! and records what it counted; `--check` re-scores this estimator against
//! those recorded counts offline. `prompt::calibration` holds the corpus and
//! the arithmetic, and `tests/token_calibration.rs` keeps the checked-in
//! figures honest. Re-run it when the deployment changes provider — a
//! tokenizer is a per-provider fact, and the numbers above are one family's.
//!
//! There is a second, passive source: every assembled request traces its
//! estimate on `request_header`, and every completed call traces the provider's
//! own `input_tokens` on `llm_token_usage`, both under the same plan span. That
//! measures real traffic rather than a corpus, at the cost of being ambiguous
//! where a plan span holds several calls — a routing classifier round-trip
//! records usage with no header of its own, and the two sinks are drained
//! separately, so their interleaving is not recoverable from the trace. Pair by
//! span, and only trust spans holding exactly one of each.

use agentos_interfaces::tool::ToolSpec;
use agentos_proto::Message;

/// ASCII characters per token. The conventional English approximation.
const ASCII_CHARS_PER_TOKEN: usize = 4;

/// Per-message envelope cost: role, delimiters, and the separators a provider
/// inserts between turns. Four is the figure OpenAI documents for its chat
/// encodings and is close enough for the others.
const MESSAGE_OVERHEAD_TOKENS: usize = 4;

/// Estimated tokens for one text body.
pub fn estimate_text(text: &str) -> usize {
    let mut ascii = 0usize;
    let mut wide = 0usize;
    for character in text.chars() {
        if character.is_ascii() {
            ascii += 1;
        } else {
            wide += 1;
        }
    }
    ascii.div_ceil(ASCII_CHARS_PER_TOKEN) + wide
}

/// Estimated tokens for one message: its text, any tool-call payloads it
/// carries, and the per-message envelope.
pub fn estimate_message(message: &Message) -> usize {
    let mut tokens = MESSAGE_OVERHEAD_TOKENS + estimate_text(&message.content);
    for call in &message.tool_calls {
        tokens += estimate_text(&call.name) + estimate_text(call.args.get());
    }
    if let Some(id) = &message.tool_call_id {
        tokens += estimate_text(id.as_str());
    }
    tokens
}

/// Estimated tokens for the tool schemas sent alongside a request.
///
/// These are easy to forget and far from free: a dozen tools with detailed
/// JSON Schemas can outweigh a short conversation.
pub fn estimate_tool_specs(tools: &[ToolSpec]) -> usize {
    tools
        .iter()
        .map(|tool| {
            estimate_text(&tool.name)
                + estimate_text(&tool.description)
                + serde_json::to_string(&tool.input_schema)
                    .map(|schema| estimate_text(&schema))
                    // An unserializable schema cannot have been sent either, so
                    // contributing zero matches what the provider will see.
                    .unwrap_or(0)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::tool::SandboxMode;
    use agentos_proto::{MessageRole, ToolCall, ToolCallId};
    use serde_json::{json, value::RawValue};
    use std::sync::Arc;

    #[test]
    fn ascii_text_uses_the_four_to_one_rate() {
        // 16 ASCII characters -> 4 tokens.
        assert_eq!(estimate_text("abcdefghijklmnop"), 4);
        // Partial groups round up rather than truncating toward an
        // under-estimate.
        assert_eq!(estimate_text("abcde"), 2);
    }

    #[test]
    fn cjk_text_is_not_divided_by_four() {
        // The case a single 4:1 divisor gets wrong by ~4x. Eight Chinese
        // characters cost about eight tokens, not two.
        let text = "今天天气很好啊哈";
        assert_eq!(text.chars().count(), 8);
        assert_eq!(estimate_text(text), 8);
    }

    #[test]
    fn mixed_scripts_are_counted_per_character_class() {
        // 8 ASCII (2 tokens) + 4 CJK (4 tokens).
        assert_eq!(
            estimate_text("deploy: 部署完成"),
            estimate_text("deploy: ") + 4
        );
    }

    #[test]
    fn a_message_costs_its_envelope_plus_its_text() {
        let message = Message::text(MessageRole::User, "abcdefghijklmnop");
        assert_eq!(estimate_message(&message), MESSAGE_OVERHEAD_TOKENS + 4);
    }

    #[test]
    fn tool_call_payloads_are_counted() {
        // A tool-call turn has almost no text but a real payload cost; missing
        // it would under-count exactly the turns that grow a transcript.
        let bare = Message::text(MessageRole::Assistant, "");
        let mut calling = bare.clone();
        calling.tool_calls.push(ToolCall {
            id: ToolCallId::new("call-1"),
            name: Arc::from("shell"),
            args: RawValue::from_string(r#"{"command":"ls","args":["-la"]}"#.to_owned())
                .expect("static args are valid JSON"),
        });
        assert!(estimate_message(&calling) > estimate_message(&bare));
    }

    #[test]
    fn tool_schemas_are_counted() {
        let tools = vec![ToolSpec {
            name: Arc::from("echo"),
            description: Arc::from("Echo the text argument back verbatim."),
            input_schema: json!({
                "type": "object",
                "required": ["text"],
                "properties": { "text": { "type": "string" } }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }];
        // Name, description, and schema all contribute; a dozen such tools is
        // a meaningful share of a small context window.
        assert!(estimate_tool_specs(&tools) > 20);
        assert_eq!(estimate_tool_specs(&[]), 0);
    }

    #[test]
    fn the_estimate_never_falls_below_the_character_count_for_cjk() {
        // The safety property: for text where a naive 4:1 rate would be
        // dangerously low, the estimate stays at or above one token per
        // character.
        for text in ["日本語のテキスト", "한국어 텍스트", "中文文本"] {
            let wide = text.chars().filter(|c| !c.is_ascii()).count();
            assert!(estimate_text(text) >= wide, "under-estimated {text}");
        }
    }
}
