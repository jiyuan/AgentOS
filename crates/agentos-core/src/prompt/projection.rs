//! Folding the session log into the items the model actually sees.
//!
//! Roadmap item P2 in `docs/TRANSFER_ROADMAP.md`. The session log is
//! append-only: nothing in it is ever rewritten or deleted. What the model sees
//! is a *view* over that log, computed here, so history can be summarized
//! without mutating the record of what happened — which is what lets
//! compaction (C3), fork (X6), and folded collaboration state (X7) each be a
//! read rather than a bespoke mutation path.
//!
//! A **checkpoint** is an ordinary transcript item carrying
//! [`TRANSCRIPT_SHADOW_KEY`]: it summarizes an inclusive range of earlier
//! positions, and [`visible`] hides that range. Positions are indices into the
//! loaded transcript, which the session store keys by a dense monotonic
//! ordinal, so an existing item's position never moves.
//!
//! Until C3 writes checkpoints, no transcript has any, and [`visible`] is the
//! identity function.

use agentos_interfaces::session::{Item, Transcript};
use agentos_proto::Message;
use serde_json::{json, Value};
use std::sync::Arc;

/// Item-metadata key marking a checkpoint. The value is
/// `{"start": <usize>, "end": <usize>}`: the inclusive position range this
/// item summarizes and therefore hides.
pub const TRANSCRIPT_SHADOW_KEY: &str = "agentos.transcript_shadow";

/// Build a checkpoint item: a summary message plus the inclusive range of
/// earlier positions it replaces in the model's view.
///
/// The range must lie entirely before the checkpoint's own position once
/// appended; [`visible`] ignores a range that does not (see its contract).
pub fn checkpoint(summary: Message, start: usize, end: usize) -> Item {
    let mut item = Item {
        message: summary,
        metadata: Default::default(),
    };
    item.metadata.insert(
        Arc::from(TRANSCRIPT_SHADOW_KEY),
        json!({ "start": start, "end": end }),
    );
    item
}

/// The items the model sees, in transcript order.
///
/// Two rules keep the fold safe:
///
/// - **Shadowing is monotonic.** Every checkpoint's range applies, including a
///   checkpoint that a later one hides. Ignoring a hidden checkpoint's range
///   could resurrect content an earlier compaction already replaced, and the
///   model would then see a summary and its originals at once.
/// - **A malformed range is ignored, never clamped.** A range that is reversed,
///   runs past the end, or reaches the checkpoint's own position or beyond is
///   skipped entirely. The failure mode is then duplicated context — wasteful
///   but correct — rather than hiding an unintended span, which could separate
///   a tool call from its result and make the request invalid.
pub fn visible(transcript: &Transcript) -> Vec<&Item> {
    visible_positions(transcript)
        .into_iter()
        .map(|position| &transcript.items[position])
        .collect()
}

/// The positions [`visible`] keeps, in transcript order.
///
/// Compaction (C3) works in positions rather than items: the checkpoint it
/// writes names the range it hides, and that range is meaningless without the
/// index each surviving item sits at.
pub fn visible_positions(transcript: &Transcript) -> Vec<usize> {
    let len = transcript.items.len();
    let mut shadowed = vec![false; len];
    for (position, item) in transcript.items.iter().enumerate() {
        let Some((start, end)) = shadow_range(item) else {
            continue;
        };
        // A checkpoint may only hide items strictly before itself.
        if start > end || end >= position {
            continue;
        }
        for hidden in shadowed.iter_mut().take(end + 1).skip(start) {
            *hidden = true;
        }
    }
    (0..len).filter(|position| !shadowed[*position]).collect()
}

fn shadow_range(item: &Item) -> Option<(usize, usize)> {
    let value = item.metadata.get(TRANSCRIPT_SHADOW_KEY)?;
    let start = value.get("start").and_then(Value::as_u64)?;
    let end = value.get("end").and_then(Value::as_u64)?;
    Some((usize::try_from(start).ok()?, usize::try_from(end).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::MessageRole;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn item(content: &str) -> Item {
        Item {
            message: Message::text(MessageRole::User, content),
            metadata: BTreeMap::new(),
        }
    }

    fn transcript(items: Vec<Item>) -> Transcript {
        Transcript { items }
    }

    fn contents(items: &[&Item]) -> Vec<String> {
        items
            .iter()
            .map(|item| item.message.content.as_ref().to_owned())
            .collect()
    }

    fn summary(content: &str, start: usize, end: usize) -> Item {
        checkpoint(Message::text(MessageRole::User, content), start, end)
    }

    #[test]
    fn a_checkpoint_hides_the_range_it_summarizes() {
        let log = transcript(vec![
            item("a"),
            item("b"),
            item("c"),
            summary("summary of a..b", 0, 1),
            item("d"),
        ]);
        assert_eq!(contents(&visible(&log)), vec!["c", "summary of a..b", "d"]);
    }

    #[test]
    fn a_later_checkpoint_may_hide_an_earlier_one() {
        // Compaction running twice: the second pass replaces a span that
        // includes the first summary.
        let log = transcript(vec![
            item("a"),
            item("b"),
            summary("first", 0, 1),
            item("c"),
            summary("second", 0, 3),
            item("d"),
        ]);
        assert_eq!(contents(&visible(&log)), vec!["second", "d"]);
    }

    #[test]
    fn hiding_a_checkpoint_does_not_resurrect_what_it_hid() {
        // `second` hides only `first`, not the range `first` covered. The
        // items `first` replaced must stay hidden: shadowing is monotonic.
        let log = transcript(vec![
            item("a"),
            item("b"),
            summary("first", 0, 1),
            summary("second", 2, 2),
            item("c"),
        ]);
        assert_eq!(contents(&visible(&log)), vec!["second", "c"]);
    }

    #[test]
    fn malformed_ranges_are_ignored_rather_than_applied() {
        for (start, end, case) in [
            (2usize, 1usize, "reversed"),
            (0, 99, "past the end"),
            (0, 3, "reaching the checkpoint itself"),
            (4, 5, "entirely after the checkpoint"),
        ] {
            let log = transcript(vec![
                item("a"),
                item("b"),
                item("c"),
                summary("bad", start, end),
                item("d"),
            ]);
            assert_eq!(
                contents(&visible(&log)),
                vec!["a", "b", "c", "bad", "d"],
                "a {case} range must hide nothing"
            );
        }
    }

    proptest! {
        /// The exit condition for P2: with no checkpoints written, the
        /// projection is the identity function, so nothing changes until C3
        /// starts writing them.
        #[test]
        fn a_log_without_checkpoints_projects_to_itself(count in 0usize..40) {
            let log = transcript((0..count).map(|index| item(&index.to_string())).collect());
            let projected = visible(&log);
            prop_assert_eq!(projected.len(), log.items.len());
            for (projected, original) in projected.iter().zip(log.items.iter()) {
                prop_assert_eq!(*projected, original);
            }
        }

        /// Whatever a checkpoint claims, the projection never returns more
        /// items than the log holds, never reorders them, and always keeps the
        /// checkpoint itself — the invariants C3 will rely on.
        #[test]
        fn projection_is_order_preserving_and_never_grows(
            count in 1usize..30,
            start in 0usize..40,
            end in 0usize..40,
        ) {
            let mut items: Vec<Item> = (0..count).map(|index| item(&index.to_string())).collect();
            items.push(summary("checkpoint", start, end));
            let log = transcript(items);
            let projected = visible(&log);

            prop_assert!(projected.len() <= log.items.len());
            let mut original = log.items.iter();
            for kept in &projected {
                prop_assert!(
                    original.any(|candidate| std::ptr::eq(candidate, *kept)),
                    "projection preserved log order"
                );
            }
            prop_assert!(
                projected.iter().any(|kept| kept.message.content.as_ref() == "checkpoint"),
                "a checkpoint never hides itself"
            );
        }
    }
}
