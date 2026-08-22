//! Summarizing the oldest span of a conversation into one checkpoint.
//!
//! Roadmap item C3 in `docs/TRANSFER_ROADMAP.md`, the last and most expensive
//! answer to context pressure. C1 measures pressure, C2 elides oversized tool
//! output for free, and only when that is not enough does this spend a model
//! call to replace the oldest part of a conversation with a summary.
//!
//! # Append-only
//!
//! Nothing in the session is mutated or deleted. A pass appends one checkpoint
//! item carrying the inclusive range it summarizes, and P2's projection folds
//! that range out of the model-visible list. The log still holds every original
//! item, so a later reader — a fork (X6), an audit, a bug report — sees what
//! actually happened rather than what was left of it.
//!
//! # The one rule that produces a hard failure
//!
//! A span may not end between an assistant item carrying tool calls and their
//! results. Every provider rejects a request whose tool result has no preceding
//! call with that id, so a mis-cut span is a 400 rather than a worse answer.
//! [`select_span`] therefore only ever cuts where every tool call it has seen
//! is closed.
//!
//! # Why this cannot recurse
//!
//! The summarizer is an LLM call made from inside the planning path, which is
//! exactly where compaction is triggered. It is safe structurally rather than
//! by a flag: [`compact`] builds a [`RequestKind::Compaction`] request over
//! rendered text and dispatches it through [`gateway::call_detached`], never
//! through the orchestrator — so it assembles no turn, pushes nothing onto a
//! context, and records no pressure. There is nothing for a compaction request
//! to trigger.
//!
//! # What it records
//!
//! M5 / `REQ-001`. This call used to leave no trace at all: no request header,
//! so a reader could not tell what the summarizer saw, and no usage, so its
//! tokens were spent and never counted in the run's totals. It has no
//! `RunContext` to push either onto — it holds `&mut RunState` — so
//! [`Compacted`] carries them back out and `loop/planning.rs` emits them
//! alongside the `conversation_compacted` event. Run token totals are larger
//! than they used to be; the old ones were wrong.
//!
//! # One span per turn
//!
//! A pass summarizes one span and returns. If the next request is still over
//! the trigger, the next turn compacts again. Looping here would mean an
//! unbounded number of model calls inside one turn.

use super::{gateway, projection, prune, tokens, RequestKind};
use crate::config::CompactionConfig;
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::{Item, Transcript};
use agentos_llm::Llm;
use agentos_proto::{Message, MessageRole, RequestHeader, Usage};
use serde_json::Value;
use std::collections::BTreeSet;
use tracing::{info, warn};

/// Share of the model's window the span may occupy in the summarization
/// request, leaving room for the instruction and for the summary itself.
const SPAN_SHARE_OF_WINDOW: f64 = 0.5;

/// Fewest visible items worth a model call. Summarizing a single turn spends a
/// round-trip to produce something no shorter than what it replaced.
const MIN_SPAN_ITEMS: usize = 2;

const SUMMARY_INSTRUCTION: &str = "You are compacting a conversation transcript so it can \
    continue within a smaller context window. Write a summary of the exchange below that \
    preserves what a participant would need to carry on: decisions reached, facts established, \
    file paths, identifiers, and open questions. Preserve specifics — names, numbers, and paths \
    — over general description. Do not add information that is not in the transcript, do not \
    offer opinions, and do not address the reader. Write the summary and nothing else.";

/// Who summarizes a run's history, and when.
///
/// One struct rather than two dependency fields so the many `RunnerDeps` and
/// `LoopDeps` construction sites can write `Default::default()` — which leaves
/// the summarizer unset, and so leaves compaction off.
#[derive(Clone, Copy, Default)]
pub struct Compaction<'a> {
    /// The provider that writes summaries. `None` disables compaction however
    /// the config reads: without a model there is nothing to summarize with.
    pub summarizer: Option<&'a dyn Llm>,
    pub config: CompactionConfig,
}

impl std::fmt::Debug for Compaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `dyn Llm` is not `Debug`; the provider's own description is the
        // useful part anyway.
        f.debug_struct("Compaction")
            .field("summarizer", &self.summarizer.map(|llm| llm.describe()))
            .field("config", &self.config)
            .finish()
    }
}

/// The inclusive transcript positions one pass replaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    /// Always 0: a pass summarizes from the beginning of the conversation, so
    /// a second pass subsumes the first checkpoint along with the turns since.
    pub start: usize,
    pub end: usize,
    /// Visible items the span covers. Smaller than `end - start + 1` whenever
    /// an earlier checkpoint already hid part of the range.
    pub items: usize,
}

/// What one completed pass did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Compacted {
    pub span: Span,
    /// Characters the checkpoint replaced, and characters it cost.
    pub replaced_chars: usize,
    pub summary_chars: usize,
    /// What the summarization request was made of (M5 / `REQ-001`).
    ///
    /// Carried out rather than pushed, because this code has a `RunState` and
    /// no `RunContext` to push onto. `loop/planning.rs` traces it.
    pub header: RequestHeader,
    /// What the summarization cost, when the provider reported it. Folded into
    /// the run's totals by the same caller.
    pub usage: Option<Usage>,
}

/// Summarize the oldest span of `state`'s transcript if the last request was
/// over the configured pressure, appending one checkpoint.
///
/// Returns `None` whenever nothing was done — disabled, no summarizer, no
/// measured pressure, pressure under the trigger, no balanced span, or a failed
/// summarizer call. A compaction failure must never fail a run: the turn then
/// proceeds at its original pressure, which is where it would have been anyway.
pub async fn compact(state: &mut RunState, compaction: &Compaction<'_>) -> Option<Compacted> {
    let summarizer = enabled_summarizer(compaction)?;
    let window = summarizer.context_budget_tokens()?;
    if pressure_percent(state, window) < compaction.config.pressure_percent {
        return None;
    }
    compact_now(state, compaction).await
}

/// Summarize regardless of the measured pressure.
///
/// The trigger for this one is not an estimate: the provider rejected the
/// request for length (roadmap item C4), so the conversation is definitively
/// too long however the estimator reads it. `[compaction].enabled = false`
/// still suppresses it — an operator who turned summarization off did not ask
/// for it back on a bad turn — in which case the loop answers with a
/// truncation notice instead.
pub async fn compact_now(state: &mut RunState, compaction: &Compaction<'_>) -> Option<Compacted> {
    let summarizer = enabled_summarizer(compaction)?;
    // Without a resolved window there is no budget to size the span against.
    let window = summarizer.context_budget_tokens()?;

    let span_budget = (window as f64 * SPAN_SHARE_OF_WINDOW) as usize;
    let span = select_span(
        &state.transcript,
        compaction.config.retain_tail_turns,
        span_budget,
    )?;

    let rendered = render_span(&state.transcript, &span, span_budget);
    let request = super::summary_request(
        Message::text(MessageRole::System, SUMMARY_INSTRUCTION),
        Message::text(MessageRole::User, rendered.text),
    );

    let exchange = match gateway::call_detached(summarizer, &request, &[]).await {
        Ok(exchange) => exchange,
        Err(error) => {
            warn!(
                run_id = state.run_id.as_str(),
                span_end = span.end,
                error = %error,
                "compaction summarizer failed; continuing uncompacted"
            );
            return None;
        }
    };
    let summary = exchange.message;
    if summary.content.trim().is_empty() {
        warn!(
            run_id = state.run_id.as_str(),
            span_end = span.end,
            "compaction summarizer returned nothing; continuing uncompacted"
        );
        return None;
    }

    let summary_chars = summary.content.len();
    state.transcript.items.push(projection::checkpoint(
        checkpoint_message(&summary),
        span.start,
        span.end,
    ));

    info!(
        run_id = state.run_id.as_str(),
        kind = RequestKind::Compaction.as_str(),
        span_start = span.start,
        span_end = span.end,
        span_items = span.items,
        replaced_chars = rendered.replaced_chars,
        summary_chars,
        "conversation compacted"
    );

    Some(Compacted {
        span,
        replaced_chars: rendered.replaced_chars,
        summary_chars,
        header: exchange.header,
        usage: exchange.usage,
    })
}

/// The summarizer, if a deployment has one and has not turned compaction off.
fn enabled_summarizer<'a>(compaction: &Compaction<'a>) -> Option<&'a dyn Llm> {
    compaction.config.enabled.then_some(compaction.summarizer)?
}

/// Wrap a summary as the message a checkpoint carries.
///
/// The role is `User`, not `System`. Providers disagree about a system message
/// appearing anywhere but first — Anthropic takes it as a separate parameter
/// entirely — and a checkpoint sits in the middle of a conversation by
/// construction. A user-role message is accepted everywhere, so the prefix
/// carries the framing instead.
fn checkpoint_message(summary: &Message) -> Message {
    Message::text(
        MessageRole::User,
        format!(
            "[Summary of the earlier conversation, replacing it in full:]\n\n{}",
            summary.content
        ),
    )
}

/// The newest balanced cut that leaves `retain_tail_turns` items untouched and
/// whose span fits `span_budget` tokens.
///
/// `None` when no such cut exists: too short a conversation, a tail that
/// already covers everything, or an unbalanced history with no safe edge.
pub fn select_span(
    transcript: &Transcript,
    retain_tail_turns: usize,
    span_budget: usize,
) -> Option<Span> {
    let visible = projection::visible_positions(transcript);
    // The newest item the span may reach: everything after it is retained.
    let newest = visible.len().checked_sub(retain_tail_turns + 1)?;

    // Walk forward once, recording after each item whether every tool call
    // seen so far has its result, and what the span would cost to summarize.
    let mut pending: BTreeSet<&str> = BTreeSet::new();
    let mut balanced = Vec::with_capacity(newest + 1);
    let mut cumulative = Vec::with_capacity(newest + 1);
    let mut cost = 0usize;
    for position in visible.iter().take(newest + 1) {
        let message = &transcript.items[*position].message;
        for call in &message.tool_calls {
            pending.insert(call.id.as_str());
        }
        if let Some(id) = &message.tool_call_id {
            pending.remove(id.as_str());
        }
        cost += tokens::estimate_message(message);
        balanced.push(pending.is_empty());
        cumulative.push(cost);
    }

    let cut = (0..=newest)
        .rev()
        .find(|index| balanced[*index] && cumulative[*index] <= span_budget)?;
    if cut + 1 < MIN_SPAN_ITEMS {
        return None;
    }
    Some(Span {
        start: 0,
        end: visible[cut],
        items: cut + 1,
    })
}

/// The span as text for the summarizer, and how much it replaced.
struct RenderedSpan {
    text: String,
    replaced_chars: usize,
}

/// Render the span as a plain transcript.
///
/// Text rather than a replayed message list: the summarizer needs the content,
/// not the protocol, and a message list carrying tool calls must satisfy every
/// provider's pairing rules or the compaction call itself 400s — the one
/// failure compaction exists to prevent. Oversized tool results are elided
/// first, so a single huge result cannot dominate the summary.
fn render_span(transcript: &Transcript, span: &Span, span_budget: usize) -> RenderedSpan {
    let mut messages: Vec<Message> = projection::visible_positions(transcript)
        .into_iter()
        .filter(|position| *position <= span.end)
        .map(|position| transcript.items[position].message.clone())
        .collect();
    prune::to_fit(
        &mut messages,
        &prune::Budget {
            context_budget_tokens: Some(span_budget),
            reserved_tokens: tokens::estimate_text(SUMMARY_INSTRUCTION),
        },
    );

    let mut text = String::new();
    let mut replaced_chars = 0;
    for message in &messages {
        replaced_chars += message.content.len();
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
            MessageRole::Tool => "tool",
        };
        text.push_str(role);
        text.push_str(": ");
        if message.content.trim().is_empty() && !message.tool_calls.is_empty() {
            // A tool-call turn carries its intent in the call, not the text.
            for call in &message.tool_calls {
                text.push_str(&format!("called {} with {}", call.name, call.args.get()));
            }
        } else {
            text.push_str(&message.content);
        }
        text.push('\n');
    }
    RenderedSpan {
        text,
        replaced_chars,
    }
}

/// How full the window is, as an integer percent, from the better of two
/// readings.
///
/// Neither alone is enough. The recorded header is *exact* — it is what C1
/// measured on a real assembled request, tool schemas and skill prelude
/// included — but a run only has one after its own first request, and a chat
/// gateway starts a fresh run per user message, so relying on it alone would
/// mean the common shape never compacts at all. The transcript estimate is
/// always available but *low*: it omits every section the orchestrator
/// contributes.
///
/// Taking the larger keeps the trigger from ever reading lower than either
/// source says, so the reading errs toward compacting rather than toward
/// hitting the provider's hard limit.
fn pressure_percent(state: &RunState, window: usize) -> usize {
    let transcript: usize = projection::visible(&state.transcript)
        .into_iter()
        .map(|item| tokens::estimate_message(&item.message))
        .sum();
    let from_transcript = transcript.saturating_mul(100) / window.max(1);
    from_transcript.max(last_pressure_percent(state).unwrap_or(0))
}

/// Pressure recorded for the most recent assembled request, as an integer
/// percent.
///
/// Read back from the trace rather than kept as new run state. C1 already
/// records it on every request, the trace survives a pause/resume round-trip
/// with the rest of `RunState`, and a second copy could disagree with the first.
/// `None` when no request has been assembled yet, or when the provider resolved
/// no window — in which case pressure is unmeasured, not low.
fn last_pressure_percent(state: &RunState) -> Option<usize> {
    state
        .trace_events
        .iter()
        .rev()
        .find(|event| event.name.as_ref() == "request_header")?
        .fields
        .get("pressure_percent")
        .and_then(Value::as_u64)
        .and_then(|percent| usize::try_from(percent).ok())
}

/// Whether `item` is a checkpoint this module wrote. Used by tests and by
/// callers asserting the log kept its originals.
pub fn is_checkpoint(item: &Item) -> bool {
    item.metadata
        .contains_key(projection::TRANSCRIPT_SHADOW_KEY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
    use proptest::prelude::*;
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn item(role: MessageRole, content: &str) -> Item {
        Item {
            message: Message::text(role, content),
            metadata: BTreeMap::new(),
        }
    }

    fn call_item(id: &str) -> Item {
        let mut message = Message::text(MessageRole::Assistant, "");
        message.tool_calls.push(ToolCall {
            id: ToolCallId::new(id),
            name: Arc::from("shell"),
            args: RawValue::from_string(r#"{"command":"ls"}"#.to_owned())
                .expect("static args are valid JSON"),
        });
        Item {
            message,
            metadata: BTreeMap::new(),
        }
    }

    fn result_item(id: &str) -> Item {
        let mut message = Message::text(MessageRole::Tool, "output");
        message.tool_call_id = Some(ToolCallId::new(id));
        Item {
            message,
            metadata: BTreeMap::new(),
        }
    }

    fn transcript(items: Vec<Item>) -> Transcript {
        Transcript { items }
    }

    fn chat(turns: usize) -> Vec<Item> {
        (0..turns)
            .flat_map(|index| {
                [
                    item(MessageRole::User, &format!("question {index}")),
                    item(MessageRole::Assistant, &format!("answer {index}")),
                ]
            })
            .collect()
    }

    #[test]
    fn a_short_conversation_is_not_compacted() {
        // The tail alone covers everything, so there is no span to summarize.
        let log = transcript(chat(3));
        assert_eq!(select_span(&log, 8, 100_000), None);
    }

    #[test]
    fn the_span_stops_short_of_the_retained_tail() {
        let log = transcript(chat(10));
        let span = select_span(&log, 8, 100_000).expect("20 items leaves a span past an 8 tail");
        assert_eq!(span.start, 0);
        // 20 items, 8 retained, so the newest summarizable position is 11.
        assert_eq!(span.end, 11);
        assert_eq!(span.items, 12);
    }

    #[test]
    fn a_span_never_splits_a_tool_call_from_its_result() {
        // The cut would land between the call and its result; it must move
        // back to the last position where nothing is outstanding.
        let mut items = chat(5);
        items.push(call_item("call-1"));
        items.push(result_item("call-1"));
        items.extend(chat(4));
        let log = transcript(items);

        let span = select_span(&log, 4, 100_000).expect("a span exists");
        let message = &log.items[span.end].message;
        assert!(
            message.tool_calls.is_empty(),
            "a span may not end on an unanswered tool call"
        );
    }

    #[test]
    fn a_span_too_large_for_the_summarizer_is_cut_shorter() {
        // A budget that only a few turns fit into: the span shrinks rather
        // than sending a request that would itself overflow.
        let log = transcript(chat(20));
        let generous = select_span(&log, 4, 100_000).expect("a span exists");
        let tight = select_span(&log, 4, 60).expect("a smaller span still exists");
        assert!(tight.end < generous.end, "{tight:?} vs {generous:?}");
    }

    #[test]
    fn a_second_pass_subsumes_the_first_checkpoint() {
        // Compaction runs twice. The second span starts at 0 again, so it
        // covers the first checkpoint and the turns since — the projection
        // then shows one summary, not two.
        let mut items = chat(10);
        items.push(projection::checkpoint(
            Message::text(MessageRole::User, "first summary"),
            0,
            11,
        ));
        items.extend(chat(10));
        let log = transcript(items);

        let span = select_span(&log, 4, 100_000).expect("a second span exists");
        assert_eq!(span.start, 0);
        assert!(
            span.end > 20,
            "the second span must cover the first summary"
        );

        // Applying it hides the first checkpoint along with everything else.
        let mut with_second = log.items.clone();
        with_second.push(projection::checkpoint(
            Message::text(MessageRole::User, "second summary"),
            span.start,
            span.end,
        ));
        let second = transcript(with_second);
        let projected = projection::visible(&second);
        assert!(!projected
            .iter()
            .any(|item| item.message.content.contains("first summary")));
    }

    #[test]
    fn rendering_names_the_tool_a_call_turn_invoked() {
        // A tool-call turn has no text; rendering it as an empty line would
        // tell the summarizer nothing about what the run did.
        let log = transcript(vec![
            item(MessageRole::User, "list the directory"),
            call_item("call-1"),
            result_item("call-1"),
        ]);
        let rendered = render_span(
            &log,
            &Span {
                start: 0,
                end: 2,
                items: 3,
            },
            100_000,
        );
        assert!(rendered.text.contains("called shell with"));
        assert!(rendered.text.contains("user: list the directory"));
    }

    #[test]
    fn pressure_is_read_from_the_last_request_header() {
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        assert_eq!(last_pressure_percent(&state), None);

        for percent in [40usize, 93] {
            let mut fields = BTreeMap::new();
            fields.insert(Arc::from("pressure_percent"), Value::from(percent));
            crate::trace::record_event(
                &mut state,
                None,
                agentos_proto::SpanId::new("span-1"),
                "request_header",
                fields,
            );
        }
        // The newest reading wins; an older, calmer turn must not mask it.
        assert_eq!(last_pressure_percent(&state), Some(93));
    }

    #[test]
    fn a_run_with_no_request_yet_still_reads_pressure_from_its_transcript() {
        // The gateway shape: one run per user message, so the first plan of
        // every run has no header of its own. Reading only the header would
        // mean a chat never compacts however long it grew.
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        state.transcript.items = chat(40);
        assert!(state.trace_events.is_empty());
        assert!(pressure_percent(&state, 200) > 90);
    }

    #[test]
    fn the_exact_header_reading_wins_when_it_is_higher() {
        // The transcript estimate omits the tool schemas and skill prelude, so
        // on a tool-heavy agent it reads well below the truth. The recorded
        // header measured the real request; it must not be discarded.
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        state.transcript.items = chat(1);
        let mut fields = BTreeMap::new();
        fields.insert(Arc::from("pressure_percent"), Value::from(97usize));
        crate::trace::record_event(
            &mut state,
            None,
            agentos_proto::SpanId::new("span-1"),
            "request_header",
            fields,
        );
        assert_eq!(pressure_percent(&state, 1_000_000), 97);
    }

    #[test]
    fn an_unmeasured_window_reads_as_no_pressure_rather_than_low() {
        // C1 omits `pressure_percent` when the provider resolved no window.
        // Treating that as 0 would mean a run on an unknown model never
        // compacts *and* never says why.
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        crate::trace::record_event(
            &mut state,
            None,
            agentos_proto::SpanId::new("span-1"),
            "request_header",
            BTreeMap::new(),
        );
        assert_eq!(last_pressure_percent(&state), None);
    }

    proptest! {
        /// The rule whose violation is a provider 400 rather than a worse
        /// answer: whatever the history looks like, a selected span never ends
        /// with a tool call outstanding.
        #[test]
        fn every_selected_span_is_tool_pairing_balanced(
            shape in prop::collection::vec(0usize..4, 0..40),
            retain in 2usize..10,
        ) {
            let mut items = Vec::new();
            let mut next_call = 0usize;
            for kind in shape {
                match kind {
                    0 => items.push(item(MessageRole::User, "u")),
                    1 => items.push(item(MessageRole::Assistant, "a")),
                    2 => {
                        // A complete call/result pair.
                        let id = format!("call-{next_call}");
                        next_call += 1;
                        items.push(call_item(&id));
                        items.push(result_item(&id));
                    }
                    // A call whose result never arrives: the run was cut off
                    // mid-turn. No cut at or after it can ever be balanced.
                    _ => {
                        let id = format!("call-{next_call}");
                        next_call += 1;
                        items.push(call_item(&id));
                    }
                }
            }
            let log = transcript(items);
            let Some(span) = select_span(&log, retain, 100_000) else {
                return Ok(());
            };

            let mut pending: BTreeSet<&str> = BTreeSet::new();
            for position in projection::visible_positions(&log) {
                if position > span.end {
                    break;
                }
                let message = &log.items[position].message;
                for call in &message.tool_calls {
                    pending.insert(call.id.as_str());
                }
                if let Some(id) = &message.tool_call_id {
                    pending.remove(id.as_str());
                }
            }
            prop_assert!(pending.is_empty(), "span {span:?} left {pending:?} unanswered");
        }

        /// A span always leaves the configured tail visible, so the model
        /// never answers from a summary alone.
        #[test]
        fn the_retained_tail_is_never_summarized(turns in 0usize..40, retain in 2usize..10) {
            let log = transcript(chat(turns));
            let Some(span) = select_span(&log, retain, 100_000) else {
                return Ok(());
            };
            let visible = projection::visible_positions(&log);
            let retained = visible.iter().filter(|position| **position > span.end).count();
            prop_assert!(retained >= retain, "only {retained} items left after {span:?}");
            prop_assert!(span.items >= MIN_SPAN_ITEMS);
        }
    }
}
