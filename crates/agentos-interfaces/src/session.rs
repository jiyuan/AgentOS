use agentos_proto::{ConversationId, Message};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session backend failed: {0}")]
    Backend(Arc<str>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub message: Message,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Transcript {
    pub items: Vec<Item>,
}

#[async_trait]
pub trait Session: Send + Sync {
    /// Load the transcript for a conversation before a run starts.
    ///
    /// Missing conversations should return an empty transcript rather than an
    /// error.
    async fn load(&self, conv_id: &ConversationId) -> Result<Transcript, SessionError>;

    /// Append items after a run progresses.
    ///
    /// Implementations must preserve item ordering exactly as supplied.
    async fn append(&self, conv_id: &ConversationId, items: Vec<Item>) -> Result<(), SessionError>;

    /// Seed `child_id` with the first `boundary` items of `source`, returning
    /// how many were copied.
    ///
    /// Branching a conversation: the child starts from the parent's history up
    /// to a point and diverges from there. It is the seeding primitive for a
    /// sub-agent that should begin with what the parent knew at the moment of
    /// delegation rather than with a summary of it.
    ///
    /// # The prefix always starts at item 0
    ///
    /// `boundary` is a length, not a range, and that is a correctness
    /// requirement rather than a simplification. A compaction checkpoint names
    /// the span it hides by *absolute position* in the log, so a fork that
    /// dropped a head would leave every checkpoint in the copied tail pointing
    /// at the wrong items — the child's projection would hide text the parent
    /// showed, or show text the parent had folded away. Copying from 0 keeps
    /// positions identical, which is what makes the child's projection equal
    /// the projection of the parent's prefix.
    ///
    /// # Contract
    ///
    /// - A `boundary` past the end of `source` copies the whole log rather
    ///   than failing. Callers name a point in a conversation they are holding
    ///   in memory, and the store legitimately holds less of it — the current
    ///   turn is not persisted until the run that produced it finishes. The
    ///   return value is what actually landed.
    /// - `child_id` must be empty. Appending a prefix onto a conversation that
    ///   already has history interleaves two logs and invalidates every
    ///   checkpoint position in both, so implementations must refuse rather
    ///   than merge.
    /// - Forking a conversation onto itself is refused for the same reason.
    /// - Spilled tool output is *not* copied. A locator is absolute and the
    ///   store is keyed by run, so the child's inherited items resolve to the
    ///   same artifacts the parent wrote; copying them would double the disk
    ///   cost of a fork and leave two paths to the same bytes.
    ///
    /// The default implementation is correct for any backend that implements
    /// [`Session::load`] and [`Session::append`]. Override it when the store
    /// can copy a prefix without moving it through memory.
    async fn fork(
        &self,
        source: &ConversationId,
        boundary: usize,
        child_id: &ConversationId,
    ) -> Result<usize, SessionError> {
        if source == child_id {
            return Err(SessionError::Backend(Arc::from(format!(
                "cannot fork conversation '{}' onto itself",
                source.as_str()
            ))));
        }
        let existing = self.load(child_id).await?;
        if !existing.items.is_empty() {
            return Err(SessionError::Backend(Arc::from(format!(
                "fork target '{}' already holds {} items; seeding it would interleave two \
                 histories",
                child_id.as_str(),
                existing.items.len()
            ))));
        }

        let mut transcript = self.load(source).await?;
        transcript.items.truncate(boundary);
        let seeded = transcript.items.len();
        self.append(child_id, transcript.items).await?;
        Ok(seeded)
    }
}
