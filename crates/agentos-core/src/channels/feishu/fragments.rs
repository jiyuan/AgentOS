//! Reassembling an event a peer chose to split up, without letting it choose
//! how much memory that costs.
//!
//! M4 / `ING-001`. Feishu splits a large event across frames and describes the
//! split in three headers: `sum` (how many pieces), `seq` (which one), and
//! `message_id` (which event). All three are numbers a peer sends, and the
//! reassembly believed all three:
//!
//! ```text
//! .or_insert_with(|| vec![None; sum])
//! ```
//!
//! A frame claiming `sum: 18446744073709551615` allocated the process to death
//! before anything looked at the payload — no authentication needed, because
//! this runs before the event is parsed, let alone admitted. Two quieter
//! versions of the same problem sat beside it: the map of partially-received
//! events had no bound, so a peer that opened fragmented events and never
//! finished them leaked memory indefinitely, and the reassembled total had no
//! bound either, so a handful of very large fragments did the same job as many
//! small ones.
//!
//! Every one of those is now a named ceiling, checked before it is used to
//! size anything.

use agentos_interfaces::ChannelError;
use std::collections::HashMap;
use std::sync::Arc;

use super::limits::MAX_EVENT_BYTES;

/// Fragments one event may be split into.
///
/// Feishu splits at a few hundred kilobytes, so a genuine event needs a
/// handful; 64 is well past any of them and far below what an allocator will
/// notice.
const MAX_FRAGMENTS: usize = 64;

/// Partially-received events held at once.
///
/// Nothing ever arrives to say an incomplete event will never be finished, so
/// without a bound the map only grows.
const MAX_PENDING_EVENTS: usize = 16;

/// Fragments waiting for the rest of themselves.
#[derive(Debug, Default)]
pub(super) struct FragmentBuffer {
    pending: HashMap<String, PendingEvent>,
    /// Monotonic counter, used only to decide which pending event is oldest.
    received: u64,
}

#[derive(Debug)]
struct PendingEvent {
    chunks: Vec<Option<Vec<u8>>>,
    last_touched: u64,
}

impl FragmentBuffer {
    /// Take one frame's payload and return the whole event once it is
    /// complete.
    ///
    /// The headers arrive as `Option<&str>` exactly as they came off the wire,
    /// so the parsing and the bounds checking happen in the same place — a
    /// caller cannot parse `sum` itself and hand in a number this never saw.
    pub(super) fn accept(
        &mut self,
        sum: Option<&str>,
        seq: Option<&str>,
        message_id: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ChannelError> {
        if payload.len() > MAX_EVENT_BYTES {
            return Err(backend(format!(
                "Feishu event exceeds the {MAX_EVENT_BYTES}-byte limit"
            )));
        }
        if sum.is_none() && (seq.is_some() || message_id.is_some()) {
            return Err(backend(
                "Feishu fragment metadata carries seq/message_id without sum",
            ));
        }
        let sum = match sum {
            Some(value) => value
                .parse::<usize>()
                .map_err(|_| backend("Feishu fragment sum header is not a valid number"))?,
            None => 1,
        };
        if sum == 0 {
            return Err(backend("Feishu fragment sum header must be at least 1"));
        }
        if sum <= 1 {
            return Ok(Some(payload));
        }
        // Before it is used to size anything.
        if sum > MAX_FRAGMENTS {
            return Err(backend(format!(
                "Feishu event claims {sum} fragments; at most {MAX_FRAGMENTS} are accepted"
            )));
        }
        let seq = seq
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| backend("Feishu fragmented event is missing seq header"))?;
        let message_id = message_id
            .ok_or_else(|| backend("Feishu fragmented event is missing message_id header"))?
            .to_owned();
        if seq >= sum {
            return Err(backend(format!(
                "Feishu fragmented event has invalid seq={seq}, sum={sum}",
            )));
        }

        self.received = self.received.saturating_add(1);
        let arrived = self.received;
        if !self.pending.contains_key(&message_id) {
            self.evict_oldest();
        }
        let entry = self
            .pending
            .entry(message_id.clone())
            .or_insert_with(|| PendingEvent {
                chunks: vec![None; sum],
                last_touched: arrived,
            });
        // A peer that changes its mind about `sum` mid-event starts over
        // rather than indexing past the end of what it asked for.
        if entry.chunks.len() != sum {
            entry.chunks = vec![None; sum];
        }
        entry.last_touched = arrived;
        entry.chunks[seq] = Some(payload);

        let total = entry.chunks.iter().try_fold(0usize, |total, chunk| {
            total.checked_add(chunk.as_ref().map_or(0, Vec::len))
        });
        let Some(total) = total else {
            self.pending.remove(&message_id);
            return Err(backend("Feishu fragmented event byte count overflow"));
        };
        if total > MAX_EVENT_BYTES {
            // Dropped, not truncated: half an event is not an event, and
            // keeping the fragments would leave the peer holding the memory by
            // simply never finishing.
            self.pending.remove(&message_id);
            return Err(backend(format!(
                "Feishu event exceeds {MAX_EVENT_BYTES} bytes across its fragments"
            )));
        }
        if entry.chunks.iter().any(Option::is_none) {
            return Ok(None);
        }

        let complete = self
            .pending
            .remove(&message_id)
            .expect("the entry was just written");
        let mut combined = Vec::with_capacity(total);
        for chunk in complete.chunks.into_iter().flatten() {
            combined.extend_from_slice(&chunk);
        }
        Ok(Some(combined))
    }

    /// Make room for one more partially-received event.
    ///
    /// Oldest first, by the frame that last touched it. Refusing the *new*
    /// event instead would let a peer that opens junk fragments lock out every
    /// real conversation behind it, which is the worse failure of the two.
    fn evict_oldest(&mut self) {
        while self.pending.len() >= MAX_PENDING_EVENTS {
            let Some(oldest) = self
                .pending
                .iter()
                .min_by_key(|(_, pending)| pending.last_touched)
                .map(|(id, _)| id.clone())
            else {
                return;
            };
            self.pending.remove(&oldest);
        }
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

fn backend(message: impl Into<String>) -> ChannelError {
    ChannelError::Backend(Arc::from(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unfragmented_event_passes_straight_through() {
        let mut buffer = FragmentBuffer::default();
        let whole = buffer
            .accept(None, None, None, b"hello".to_vec())
            .expect("no headers means one piece");
        assert_eq!(whole, Some(b"hello".to_vec()));
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn fragments_are_reassembled_in_sequence_order_however_they_arrive() {
        let mut buffer = FragmentBuffer::default();
        // Out of order on purpose: `seq` decides the position, not arrival.
        assert_eq!(
            buffer
                .accept(Some("3"), Some("2"), Some("m1"), b"c".to_vec())
                .expect("a partial event is not an error"),
            None
        );
        assert_eq!(
            buffer
                .accept(Some("3"), Some("0"), Some("m1"), b"a".to_vec())
                .expect("a partial event is not an error"),
            None
        );
        let whole = buffer
            .accept(Some("3"), Some("1"), Some("m1"), b"b".to_vec())
            .expect("the last fragment completes it");
        assert_eq!(whole, Some(b"abc".to_vec()));
        assert_eq!(buffer.pending_count(), 0);
    }

    /// The finding: `sum` was used directly as an allocation size.
    #[test]
    fn a_fragment_count_a_peer_invented_does_not_allocate() {
        let mut buffer = FragmentBuffer::default();
        let error = buffer
            .accept(
                Some(&usize::MAX.to_string()),
                Some("0"),
                Some("m1"),
                b"x".to_vec(),
            )
            .expect_err("a peer does not get to choose an allocation size");
        assert!(error.to_string().contains("at most"), "{error}");
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn a_sequence_number_past_the_end_is_refused_rather_than_indexed() {
        let mut buffer = FragmentBuffer::default();
        assert!(buffer
            .accept(Some("2"), Some("5"), Some("m1"), b"x".to_vec())
            .is_err());
    }

    /// A peer that changes `sum` between frames of the same event. Without the
    /// reset, `chunks[seq]` would index past the end of the first allocation.
    #[test]
    fn an_event_that_changes_its_own_length_starts_over() {
        let mut buffer = FragmentBuffer::default();
        assert_eq!(
            buffer
                .accept(Some("2"), Some("0"), Some("m1"), b"a".to_vec())
                .expect("a partial event is not an error"),
            None
        );
        assert_eq!(
            buffer
                .accept(Some("4"), Some("3"), Some("m1"), b"d".to_vec())
                .expect("a partial event is not an error"),
            None
        );
        assert_eq!(buffer.pending_count(), 1);
    }

    /// The slow leak: incomplete events that nobody ever finishes.
    #[test]
    fn incomplete_events_do_not_accumulate_without_bound() {
        let mut buffer = FragmentBuffer::default();
        for index in 0..MAX_PENDING_EVENTS * 4 {
            assert_eq!(
                buffer
                    .accept(
                        Some("2"),
                        Some("0"),
                        Some(&format!("m{index}")),
                        b"x".to_vec(),
                    )
                    .expect("a partial event is not an error"),
                None
            );
        }
        assert!(
            buffer.pending_count() <= MAX_PENDING_EVENTS,
            "held {} partial events",
            buffer.pending_count()
        );
    }

    /// Eviction takes the oldest, so a burst of junk cannot push out an event
    /// that is still actively arriving.
    #[test]
    fn an_actively_arriving_event_survives_a_burst_of_junk() {
        let mut buffer = FragmentBuffer::default();
        assert_eq!(
            buffer
                .accept(Some("2"), Some("0"), Some("live"), b"a".to_vec())
                .expect("a partial event is not an error"),
            None
        );
        for index in 0..MAX_PENDING_EVENTS - 1 {
            let _ = buffer.accept(
                Some("2"),
                Some("0"),
                Some(&format!("junk{index}")),
                b"x".to_vec(),
            );
            // Touch the live event so it stays the most recent.
            let _ = buffer.accept(Some("2"), Some("0"), Some("live"), b"a".to_vec());
        }
        let whole = buffer
            .accept(Some("2"), Some("1"), Some("live"), b"b".to_vec())
            .expect("the live event completes");
        assert_eq!(whole, Some(b"ab".to_vec()));
    }

    /// Few fragments, each enormous, is the same problem as many small ones.
    #[test]
    fn a_reassembled_event_is_bounded_by_bytes_as_well_as_by_count() {
        let mut buffer = FragmentBuffer::default();
        let chunk = vec![b'x'; MAX_EVENT_BYTES / 2 + 1];
        assert_eq!(
            buffer
                .accept(Some("4"), Some("0"), Some("m1"), chunk.clone())
                .expect("a partial event is not an error"),
            None
        );
        let error = buffer
            .accept(Some("4"), Some("1"), Some("m1"), chunk)
            .expect_err("past the byte ceiling");
        assert!(error.to_string().contains("exceeds"), "{error}");
        // And the fragments are released rather than held by a peer that will
        // never finish.
        assert_eq!(buffer.pending_count(), 0);
    }
}
