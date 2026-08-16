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
//! Deliberately high. Counting every non-ASCII character as a full token
//! over-estimates some scripts by up to half. That is the safe direction: an
//! over-estimate compacts earlier than strictly necessary, while an
//! under-estimate lets a request hit the provider's hard context limit with no
//! compaction attempted — the failure C4 exists to recover from. A cheap
//! estimator that is never dangerously low beats an accurate one that
//! sometimes is.
//!
//! # Checking it against reality
//!
//! Every assembled request traces its estimate on `request_header`, and every
//! completed call traces the provider's own `input_tokens` on
//! `llm_token_usage`, both under the same plan span. Comparing the two across a
//! deployment's traces measures the real error rate. Note that a routing
//! classifier round-trip records usage without a header, so pair by span rather
//! than by position.

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
            requires_isolation: false,
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
