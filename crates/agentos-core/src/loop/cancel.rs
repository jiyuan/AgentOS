//! Stopping a run in flight.
//!
//! Roadmap item D1 in `docs/TRANSFER_ROADMAP.md`. Before it, a run had no way
//! to be stopped: a gateway that wanted to abandon a turn — because the user
//! said stop, because the process is shutting down, because a deadline passed
//! (D2) — could only drop the future and hope, and a sub-agent tree had no
//! relationship to its parent's fate at all.
//!
//! One [`CancellationToken`] per run, carried on [`LoopDeps`] and installed on
//! every [`RunContext`] the loop builds. Sub-agents get a *child* token, so
//! cancelling a parent cancels its whole delegation tree while a sub-agent
//! cancelling itself leaves the parent alone.
//!
//! # Cancellation finishes the run; it does not fail it
//!
//! A cancelled run returns a normal terminal reply carrying a stop notice,
//! which is the same choice `max_turns` and C4's context overflow already
//! make. It matters more here than in either: the alternative is an `Err` out
//! of `run_envelope`, and the runner only appends a run's new transcript items
//! to the session on the success path. Failing the run would therefore throw
//! away the tool results it had already produced — the exact work a user who
//! pressed stop most wants kept.
//!
//! # What cancellation can and cannot interrupt
//!
//! The loop checks the token at the top of each turn and races it against the
//! two long awaits — planning and tool execution. Losing that race drops the
//! in-flight future, which for an `async` call (an HTTP request, a
//! `tokio::process` child) really does abort the work.
//!
//! It cannot interrupt work that **blocks the thread instead of awaiting**.
//! `ShellTool` and the isolation subprocess both block on `std::process`
//! today, so a `select!` never gets to poll the cancellation branch until they
//! return. That is not a gap in this item — it is exactly what D2 (async
//! subprocess execution) exists to fix, and D1 is what D2 will hang off.

use super::telemetry::field_key;
use super::{FinalOutput, LoopDeps};
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{Message, MessageRole, SpanKind};
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// What the user sees when a run is stopped.
///
/// A constant, and deliberately not passed through the output guardrails: it
/// carries no model or user content for a content guardrail to inspect, and
/// `budget_exhausted_finish` already establishes that a fixed stop notice goes
/// out unchecked.
const CANCELLED_NOTICE: &str = "Stopped. Anything I had already done in this turn is saved, \
     so you can pick up from here.";

/// Run `work` unless the token fires first.
///
/// `biased` so an already-cancelled run never starts the work: without it,
/// `select!` picks a ready branch at random and a cancelled run could still
/// dispatch one more tool call.
pub(super) async fn unless_cancelled<T>(
    cancel: &CancellationToken,
    work: impl Future<Output = T>,
) -> Option<T> {
    select! {
        biased;
        () = cancel.cancelled() => None,
        result = work => Some(result),
    }
}

/// The terminal answer for a run that was stopped.
pub(super) fn cancelled_finish(
    mut state: RunState,
    deps: &LoopDeps<'_>,
    at: &'static str,
) -> FinalOutput {
    let parent_id = trace::run_span_id(&state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("reason"), Value::from(at));
    let span_id = trace::record_span(
        &mut state,
        parent_id,
        SpanKind::State,
        "run_cancelled",
        fields.clone(),
    );
    trace::record_event(&mut state, deps.hooks, span_id, "run_cancelled", fields);
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        reason = at,
        "run_cancelled"
    );

    FinalOutput {
        state,
        message: Message::text(MessageRole::Assistant, CANCELLED_NOTICE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_already_cancelled_token_never_starts_the_work() {
        // The `biased` guarantee. Without it `select!` polls in random order
        // and a cancelled run could still dispatch one more tool call.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut ran = false;
        let outcome = unless_cancelled(&cancel, async {
            ran = true;
            7
        })
        .await;
        assert_eq!(outcome, None);
        assert!(!ran, "work must not start once the run is cancelled");
    }

    #[tokio::test]
    async fn work_that_finishes_first_keeps_its_result() {
        let cancel = CancellationToken::new();
        assert_eq!(unless_cancelled(&cancel, async { 7 }).await, Some(7));
    }

    #[tokio::test]
    async fn cancelling_mid_await_drops_the_work() {
        let cancel = CancellationToken::new();
        let fired = cancel.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            fired.cancel();
        });
        let outcome = unless_cancelled(&cancel, async {
            // Never completes: only cancellation can end this.
            std::future::pending::<u8>().await
        })
        .await;
        assert_eq!(outcome, None);
    }

    #[test]
    fn a_child_token_follows_its_parent_but_not_the_reverse() {
        // The delegation contract: cancelling a parent run stops its whole
        // sub-agent tree, while a sub-agent that stops itself leaves the
        // parent free to carry on with the result it got.
        let parent = CancellationToken::new();
        let child = parent.child_token();
        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());

        let other_parent = CancellationToken::new();
        let other_child = other_parent.child_token();
        other_parent.cancel();
        assert!(other_child.is_cancelled());
    }
}
