//! The parts of an assembled provider *reply* that are easy to accept and wrong
//! to accept.
//!
//! Every provider path ends the same way: fold a wire response into one
//! assistant [`Message`] and hand it to the run loop. Two of the ways that can
//! go wrong are invisible at the point where they happen, because what comes out
//! is a perfectly well-formed message:
//!
//! - **A tool call whose arguments did not parse.** Truncation cuts the
//!   argument JSON mid-object. Every path builds tool calls through
//!   `RawValue::from_string`, which validates, and every path then drops the
//!   call on the floor. What reaches the run loop is a turn that asked for
//!   nothing, so the agent stops instead of acting — indistinguishable from a
//!   model that chose to stop.
//! - **A reply with nothing in it.** No content, no tool calls, no reasoning
//!   text. The loop treats it as a finished turn and the operator sees the agent
//!   answer with silence.
//!
//! Neither is recoverable here, so both become errors: a failure naming the stop
//! reason is strictly more useful than a turn that ends for no stated reason.
//!
//! # One vocabulary
//!
//! Each wire shape spells the stop reason differently — Chat Completions
//! `finish_reason: "length"`, Responses `status: "incomplete"` with
//! `incomplete_details.reason: "max_output_tokens"`, Anthropic
//! `stop_reason: "max_tokens"`. Providers translate into the Chat Completions
//! vocabulary before calling in, so [`STOP_REASON_METADATA_KEY`] means the same
//! thing in a session log regardless of which provider wrote it. The reason is
//! recorded only when it is *abnormal*, so its presence is itself the signal.
//!
//! Wired into every provider path that assembles a reply: Chat Completions
//! (buffered [`super::deepseek::complete`], streamed
//! [`super::stream::openai_compatible_stream`]), Responses (`openai::responses`
//! and `openai::events`), and Anthropic Messages
//! ([`super::anthropic::complete`] and its SSE accumulator). Ollama has no
//! streaming path and reports no stop reason, so it is the one shape left out.

use agentos_proto::Message;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

/// Message-metadata key carrying why the model stopped, in the Chat Completions
/// vocabulary, present only when that reason is worth keeping (see
/// [`record_reply_outcome`]). Nothing in the run loop branches on it today; it
/// exists so a truncated turn is still identifiable in a session log written
/// hours earlier.
pub(crate) const STOP_REASON_METADATA_KEY: &str = "agentos.stop_reason";

/// The stop reasons that mean "the model finished normally" and are therefore
/// not worth recording: a plain completion and a completion that ended in order
/// to call tools.
const NORMAL_REASONS: [&str; 2] = ["stop", "tool_calls"];

/// Stand-in reason for a reply that never reported one. Streams that end
/// without a terminal chunk and buffered bodies missing the field both land
/// here, so an error message always names something.
const UNREPORTED: &str = "unreported";

/// The two reasons a reply is rejected after it has already been assembled.
/// Both mean the reply parsed but is missing something the model said it
/// produced.
#[derive(Debug, Error)]
pub(crate) enum ReplyDefect {
    #[error(
        "{provider} returned a reply with no content, no tool calls and no reasoning \
         (finish_reason={reason})"
    )]
    Empty {
        provider: &'static str,
        reason: Arc<str>,
    },
    #[error(
        "{provider} announced {announced} tool call(s) but {lost} carried arguments that were \
         not valid JSON (finish_reason={reason}); accepting the reply would hide a call the \
         model asked for"
    )]
    LostToolCall {
        provider: &'static str,
        announced: usize,
        lost: usize,
        reason: Arc<str>,
    },
}

/// Check an assembled reply for lost information and record how it ended.
///
/// `announced_tool_calls` is how many calls the wire fully identified (id *and*
/// name); anything by which `message.tool_calls` falls short of it was dropped
/// for unparseable arguments. `produced_reasoning` reports whether the model
/// emitted a reasoning channel, which counts as output even when the visible
/// content is empty — a thinking model answering entirely in `reasoning_content`
/// is unusual, not broken.
///
/// On success the `finish_reason` is written to
/// [`STOP_REASON_METADATA_KEY`], but only when it is neither `stop` nor
/// `tool_calls`. Normal replies stay byte-identical to what they were before,
/// so the key's *presence* is the signal: this turn ended abnormally.
pub(crate) fn record_reply_outcome(
    provider: &'static str,
    message: &mut Message,
    finish_reason: Option<&str>,
    announced_tool_calls: usize,
    produced_reasoning: bool,
) -> Result<(), ReplyDefect> {
    let reason = finish_reason.unwrap_or(UNREPORTED);
    let lost = announced_tool_calls.saturating_sub(message.tool_calls.len());
    if lost > 0 {
        return Err(ReplyDefect::LostToolCall {
            provider,
            announced: announced_tool_calls,
            lost,
            reason: Arc::from(reason),
        });
    }
    if message.content.is_empty() && message.tool_calls.is_empty() && !produced_reasoning {
        return Err(ReplyDefect::Empty {
            provider,
            reason: Arc::from(reason),
        });
    }
    if NORMAL_REASONS.contains(&reason) {
        return Ok(());
    }
    if reason == "length" {
        tracing::warn!(
            provider,
            finish_reason = reason,
            content_chars = message.content.len(),
            tool_calls = message.tool_calls.len(),
            "llm reply hit the output limit and was cut off"
        );
    }
    message.metadata.insert(
        Arc::from(STOP_REASON_METADATA_KEY),
        Value::String(reason.to_owned()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{MessageRole, ToolCall, ToolCallId};
    use serde_json::value::RawValue;

    fn assistant(content: &str) -> Message {
        Message::text(MessageRole::Assistant, content)
    }

    fn call() -> ToolCall {
        ToolCall {
            id: ToolCallId::new("call_1"),
            name: Arc::from("shell"),
            args: RawValue::from_string("{}".to_owned()).expect("valid json"),
        }
    }

    #[test]
    fn an_ordinary_reply_is_accepted_and_left_unmarked() {
        let mut message = assistant("here you go");

        record_reply_outcome("deepseek", &mut message, Some("stop"), 0, false)
            .expect("an ordinary reply is not a defect");

        assert!(message.metadata.is_empty());
    }

    #[test]
    fn a_tool_call_turn_is_accepted_and_left_unmarked() {
        let mut message = assistant("");
        message.tool_calls.push(call());

        record_reply_outcome("deepseek", &mut message, Some("tool_calls"), 1, false)
            .expect("a tool-call turn is not a defect");

        assert!(message.metadata.is_empty());
    }

    #[test]
    fn a_truncated_reply_keeps_its_text_but_carries_the_stop_reason() {
        let mut message = assistant("the answer is fo");

        record_reply_outcome("deepseek", &mut message, Some("length"), 0, false)
            .expect("truncated text is still usable output");

        assert_eq!(
            message
                .metadata
                .get(STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("length")
        );
    }

    #[test]
    fn an_unrecognised_stop_reason_is_recorded_rather_than_guessed_at() {
        let mut message = assistant("blocked");

        record_reply_outcome("deepseek", &mut message, Some("content_filter"), 0, false)
            .expect("an unusual finish is not itself a defect");

        assert_eq!(
            message
                .metadata
                .get(STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("content_filter")
        );
    }

    #[test]
    fn a_dropped_tool_call_is_a_defect_even_when_text_survived() {
        let mut message = assistant("calling the tool");

        let error = record_reply_outcome("deepseek", &mut message, Some("length"), 2, false)
            .expect_err("a call the model asked for went missing");

        let rendered = error.to_string();
        assert!(
            rendered.contains("announced 2 tool call(s) but 2"),
            "{rendered}"
        );
        assert!(rendered.contains("finish_reason=length"), "{rendered}");
    }

    #[test]
    fn an_empty_reply_is_a_defect() {
        let mut message = assistant("");

        let error = record_reply_outcome("deepseek", &mut message, Some("stop"), 0, false)
            .expect_err("a reply with nothing in it is not a finished turn");

        assert!(error.to_string().contains("no content"), "{error}");
    }

    #[test]
    fn a_reasoning_only_reply_is_accepted() {
        let mut message = assistant("");

        record_reply_outcome("deepseek", &mut message, Some("stop"), 0, true)
            .expect("reasoning is output too");
    }

    #[test]
    fn a_missing_finish_reason_still_names_something() {
        let mut message = assistant("");

        let error = record_reply_outcome("deepseek", &mut message, None, 0, false)
            .expect_err("empty is empty regardless of what the wire reported");

        assert!(
            error.to_string().contains("finish_reason=unreported"),
            "{error}"
        );
    }
}
