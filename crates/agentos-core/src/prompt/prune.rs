//! Deterministic elision of oversized tool results under context pressure.
//!
//! Roadmap item C2 in `docs/TRANSFER_ROADMAP.md`, the second half. The first
//! half ([`crate::spill`]) makes a large tool result *recoverable*: the full
//! text goes to a file and the transcript keeps a preview plus a locator. This
//! half makes it *cheap*: once a run is close to its context window, the middle
//! of an already-recorded result is replaced by a marker citing that locator,
//! so the bytes stop being paid for on every subsequent turn without ceasing to
//! exist.
//!
//! # Why this is a view, not a rewrite
//!
//! Elision happens at assembly time on cloned messages. The session log is
//! untouched, exactly as under P2's projection — but for a different reason.
//! P2 exists because compaction (C3) is expensive and non-deterministic, so its
//! result *must* be durable. Elision is the opposite: it is a pure function of
//! the visible transcript and the context window, costs microseconds, and would
//! be wrong to freeze — a run that later compacts its history could afford to
//! show a result in full again, and a durable elision would have thrown those
//! bytes away for good.
//!
//! # Why the newest result is pruned last
//!
//! [`to_fit`] walks the transcript oldest-first. The newest tool result is the
//! one the model just asked for and is reasoning about; gutting it invites the
//! model to re-run the call, which costs more window than the elision saved.
//! Ordering alone gets this right, so there is no special case for it — if the
//! newest result is the only thing that will not fit, it is still pruned.

use super::tokens;
use crate::spill::SPILL_LOCATOR_KEY;
use agentos_proto::{Message, MessageRole};
use serde_json::Value;
use std::sync::Arc;

/// Share of the context window at or below which a request is left alone.
///
/// The remaining fifth is headroom for the reply, which the estimate does not
/// cover: a request that exactly filled the window would leave the model no
/// room to answer.
pub const PRUNE_TRIGGER_RATIO: f64 = 0.8;

/// Message-metadata key recording how many bytes an elision removed. Present
/// only on the assembled copy — nothing durable carries it.
pub const ELIDED_BYTES_KEY: &str = "content_elided_bytes";

/// Leading bytes kept. Enough for a command echo, a header row, or the first
/// stanza of a stack trace — the part that says what the output *is*.
const HEAD_BYTES: usize = 2 * 1024;

/// Trailing bytes kept. Enough for an exit status, a summary line, and the
/// spill hint `bounded_tool_content` appends, which is what makes the marker's
/// locator reachable even if the marker itself is skimmed.
const TAIL_BYTES: usize = 1024;

/// Below this a result is left whole: the marker would replace a span barely
/// larger than itself, spending a turn's worth of confusion to save nothing.
const MIN_PRUNABLE_BYTES: usize = HEAD_BYTES + TAIL_BYTES + 1024;

/// What the transcript has to fit into.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Budget {
    /// The model's context window, when the provider resolved one.
    ///
    /// `None` disables elision entirely. Without a window there is no pressure
    /// to measure, and a guessed one would silently discard output on models
    /// that had room for it.
    pub context_budget_tokens: Option<usize>,
    /// Estimated tokens already committed by the other sections and the tool
    /// schemas. The transcript gets what is left.
    pub reserved_tokens: usize,
}

/// What elision removed from one assembled request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Elision {
    /// Messages that lost their middle.
    pub messages: usize,
    /// Characters removed, across all of them.
    pub chars: usize,
}

/// Elide oversized tool results until the request fits, oldest first.
///
/// Returns what was removed. Deterministic: the same messages and the same
/// budget always produce the same output, and no model call is involved.
pub fn to_fit(messages: &mut [Message], budget: &Budget) -> Elision {
    let mut elision = Elision::default();
    let Some(window) = budget.context_budget_tokens.filter(|window| *window > 0) else {
        return elision;
    };
    // Cheap gate before the expensive one. Estimating the request means
    // scanning every character of the transcript, and on a run with nothing
    // large enough to elide that scan can only ever conclude "do nothing" —
    // which is almost every turn. Comparing lengths is O(messages).
    let prunable = messages.iter().any(|message| {
        message.role == MessageRole::Tool && message.content.len() >= MIN_PRUNABLE_BYTES
    });
    if !prunable {
        return elision;
    }

    let ceiling = (window as f64 * PRUNE_TRIGGER_RATIO) as usize;
    let mut total =
        budget.reserved_tokens + messages.iter().map(tokens::estimate_message).sum::<usize>();
    if total <= ceiling {
        return elision;
    }

    for message in messages.iter_mut() {
        if total <= ceiling {
            break;
        }
        let before = tokens::estimate_message(message);
        let Some(removed) = elide(message) else {
            continue;
        };
        total = total.saturating_sub(before.saturating_sub(tokens::estimate_message(message)));
        elision.messages += 1;
        elision.chars += removed;
    }

    elision
}

/// Replace the middle of one tool result with a marker. `None` when the
/// message is not a candidate, or when eliding it would not shrink it.
fn elide(message: &mut Message) -> Option<usize> {
    if message.role != MessageRole::Tool || message.content.len() < MIN_PRUNABLE_BYTES {
        return None;
    }

    let mut head = HEAD_BYTES;
    while !message.content.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = message.content.len() - TAIL_BYTES;
    while !message.content.is_char_boundary(tail) {
        tail += 1;
    }
    let removed = tail - head;

    let marker = match message
        .metadata
        .get(SPILL_LOCATOR_KEY)
        .and_then(Value::as_str)
    {
        Some(locator) => format!(
            "\n\n[... {removed} bytes elided from the middle of this tool result to fit the \
             context window. The full output is at {locator}; read the range you need rather \
             than re-running the call ...]\n\n"
        ),
        None => format!(
            "\n\n[... {removed} bytes elided from the middle of this tool result to fit the \
             context window. They were not spilled and cannot be read back ...]\n\n"
        ),
    };
    let pruned = format!(
        "{}{marker}{}",
        &message.content[..head],
        &message.content[tail..]
    );
    // A marker longer than the span it replaces would grow the request. Only
    // reachable through a pathologically long locator, but `to_fit` relies on
    // every accepted elision making progress toward the ceiling.
    if pruned.len() >= message.content.len() {
        return None;
    }

    message.content = Arc::from(pruned);
    message
        .metadata
        .insert(Arc::from(ELIDED_BYTES_KEY), Value::from(removed as u64));
    Some(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::ToolCallId;
    use std::collections::BTreeMap;

    /// A tool result of `bytes` ASCII characters with a recognisable head and
    /// tail, so a test can assert which end survived.
    fn tool_result(bytes: usize) -> Message {
        let mut content = String::from("HEAD-MARKER");
        content.push_str(&"x".repeat(bytes - "HEAD-MARKER".len() - "TAIL-MARKER".len()));
        content.push_str("TAIL-MARKER");
        Message {
            role: MessageRole::Tool,
            content: Arc::from(content),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(ToolCallId::new("call-1")),
            metadata: BTreeMap::new(),
        }
    }

    /// A budget that is guaranteed to be exceeded by `messages`.
    fn tight() -> Budget {
        Budget {
            context_budget_tokens: Some(1_000),
            reserved_tokens: 0,
        }
    }

    #[test]
    fn a_request_within_the_ceiling_is_untouched() {
        // The common case: nothing is rewritten until there is real pressure,
        // so a short run's messages reach the provider byte-identical.
        let mut messages = vec![tool_result(64 * 1024)];
        let original = messages[0].content.clone();

        let elision = to_fit(
            &mut messages,
            &Budget {
                context_budget_tokens: Some(1_000_000),
                reserved_tokens: 0,
            },
        );

        assert_eq!(elision, Elision::default());
        assert_eq!(messages[0].content, original);
    }

    #[test]
    fn without_a_resolved_window_nothing_is_elided() {
        // A provider that reports no context budget must not have output
        // thrown away on a guess.
        let mut messages = vec![tool_result(512 * 1024)];
        let original = messages[0].content.clone();

        let elision = to_fit(
            &mut messages,
            &Budget {
                context_budget_tokens: None,
                reserved_tokens: 0,
            },
        );

        assert_eq!(elision, Elision::default());
        assert_eq!(messages[0].content, original);
    }

    #[test]
    fn elision_keeps_both_ends_and_names_the_byte_count() {
        let mut messages = vec![tool_result(64 * 1024)];
        let elision = to_fit(&mut messages, &tight());

        assert_eq!(elision.messages, 1);
        let content = &messages[0].content;
        assert!(content.starts_with("HEAD-MARKER"));
        assert!(content.ends_with("TAIL-MARKER"));
        assert!(content.contains(&format!("{} bytes elided", elision.chars)));
        assert!(content.len() < 64 * 1024);
        assert_eq!(
            messages[0].metadata.get(ELIDED_BYTES_KEY),
            Some(&Value::from(elision.chars as u64))
        );
    }

    #[test]
    fn a_spilled_result_cites_its_locator_in_the_marker() {
        // The C2 exit condition at the request level: elided bytes are still
        // reachable, and the model is told where.
        let mut messages = vec![tool_result(64 * 1024)];
        messages[0].metadata.insert(
            Arc::from(SPILL_LOCATOR_KEY),
            Value::String("/w/spill/run-1/shell-call_1.txt".to_owned()),
        );

        to_fit(&mut messages, &tight());

        assert!(messages[0]
            .content
            .contains("/w/spill/run-1/shell-call_1.txt"));
        assert!(!messages[0].content.contains("cannot be read back"));
    }

    #[test]
    fn an_unspilled_result_says_so_rather_than_implying_recovery() {
        let mut messages = vec![tool_result(64 * 1024)];
        to_fit(&mut messages, &tight());
        assert!(messages[0].content.contains("cannot be read back"));
    }

    #[test]
    fn the_oldest_result_is_pruned_first_and_the_newest_kept_longest() {
        // Two oversized results, only enough pressure to need one gone. The
        // model is reasoning about the newest, so that is the one that stays.
        let mut messages = vec![tool_result(64 * 1024), tool_result(8 * 1024)];
        let newest = messages[1].content.clone();

        // A ceiling that the pair exceeds but that one elision brings it under.
        let elision = to_fit(
            &mut messages,
            &Budget {
                context_budget_tokens: Some(6_000),
                reserved_tokens: 0,
            },
        );

        assert_eq!(elision.messages, 1);
        assert!(messages[0].metadata.contains_key(ELIDED_BYTES_KEY));
        assert_eq!(messages[1].content, newest);
    }

    #[test]
    fn only_tool_results_are_candidates() {
        // A user turn is the run's actual input and an assistant turn is the
        // model's own reasoning; neither is recoverable from a spill file, so
        // neither may be gutted to make room.
        let mut messages = vec![
            Message::text(MessageRole::User, "u".repeat(64 * 1024)),
            Message::text(MessageRole::Assistant, "a".repeat(64 * 1024)),
        ];
        let original: Vec<Arc<str>> = messages
            .iter()
            .map(|message| message.content.clone())
            .collect();

        let elision = to_fit(&mut messages, &tight());

        assert_eq!(elision, Elision::default());
        assert_eq!(messages[0].content, original[0]);
        assert_eq!(messages[1].content, original[1]);
    }

    #[test]
    fn a_result_barely_over_the_keep_window_is_left_whole() {
        // Below the floor the marker would replace a span barely larger than
        // itself, so the transcript gets a confusing note and no saving.
        let mut messages = vec![tool_result(MIN_PRUNABLE_BYTES - 1)];
        let original = messages[0].content.clone();

        assert_eq!(to_fit(&mut messages, &tight()), Elision::default());
        assert_eq!(messages[0].content, original);
    }

    #[test]
    fn elision_never_splits_a_multibyte_character() {
        // A CJK-heavy result is the normal case for this deployment, and a cut
        // at a non-boundary would panic rather than degrade.
        for filler in ["今", "日本語", "🙂"] {
            let body = filler.repeat(MIN_PRUNABLE_BYTES);
            let mut messages = vec![Message {
                role: MessageRole::Tool,
                content: Arc::from(body),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: Some(ToolCallId::new("call-1")),
                metadata: BTreeMap::new(),
            }];

            let elision = to_fit(&mut messages, &tight());

            assert_eq!(elision.messages, 1, "{filler} result was not elided");
            // The survivors are still valid UTF-8 by construction; assert the
            // ends were not silently emptied by a boundary walk.
            assert!(messages[0].content.starts_with(filler));
            assert!(messages[0].content.ends_with(filler));
        }
    }

    #[test]
    fn elision_is_deterministic_across_runs() {
        // C3 will diff assembled requests turn over turn; an elision that
        // moved would make every such diff meaningless.
        let mut first = vec![tool_result(64 * 1024), tool_result(64 * 1024)];
        let mut second = first.clone();

        assert_eq!(to_fit(&mut first, &tight()), to_fit(&mut second, &tight()));
        assert_eq!(first, second);
    }

    #[test]
    fn elision_stops_once_the_request_fits() {
        // Pruning is not a sweep: results past the point where the request
        // fits keep their full text.
        let mut messages = vec![
            tool_result(256 * 1024),
            tool_result(8 * 1024),
            tool_result(8 * 1024),
        ];
        let untouched = messages[2].content.clone();

        let elision = to_fit(
            &mut messages,
            &Budget {
                context_budget_tokens: Some(8_000),
                reserved_tokens: 0,
            },
        );

        assert_eq!(elision.messages, 1);
        assert_eq!(messages[2].content, untouched);
    }
}
