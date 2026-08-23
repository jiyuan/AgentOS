use agentos_proto::{Message, Principal};
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

/// The conversation transcript, keyed by [`Principal`].
///
/// # Why a principal and not a conversation id
///
/// A bare `ConversationId` is a number the *transport* chose. Telegram's chat
/// `42`, Feishu's chat `42`, and a second agent sharing the same database all
/// produced the same key, so they shared one transcript — the collision
/// [ADR-0003](../../../../docs/adr/0003-TYPED_PRINCIPAL.md) removed from memory
/// and episodes, still present here until M3 deliverable 2 finished the job.
/// Changing this signature is the semver break that milestone was waiting on.
///
/// # The two names, and which one an implementation uses
///
/// [`Principal::conversation_name`] identifies the *transcript*: everyone in a
/// group conversation shares it, so `load`, `append` and `fork` must key on it
/// and must ignore the sender.
///
/// [`Principal::storage_name`] identifies the *participant*, and only the
/// `/clear` epoch uses it — one member clearing their view must not clear
/// anybody else's ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)). So
/// `load` reads one conversation's items and applies the calling participant's
/// epoch, and two participants can legitimately see different prefixes of the
/// same log.
#[async_trait]
pub trait Session: Send + Sync {
    /// Load the transcript this participant can see, before a run starts.
    ///
    /// The items are the conversation's; the visible prefix is this
    /// principal's. Missing conversations return an empty transcript rather
    /// than an error.
    async fn load(&self, principal: &Principal) -> Result<Transcript, SessionError>;

    /// Append items after a run progresses.
    ///
    /// Keyed by the conversation, so the sender on `principal` is irrelevant
    /// here and two participants appending to one conversation write to one
    /// log. Implementations must preserve item ordering exactly as supplied.
    async fn append(&self, principal: &Principal, items: Vec<Item>) -> Result<(), SessionError>;

    /// Seed `child` with the first `boundary` items of `source`, returning
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
    /// - `child` must be empty. Appending a prefix onto a conversation that
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
        source: &Principal,
        boundary: usize,
        child: &Principal,
    ) -> Result<usize, SessionError> {
        let (source_key, child_key) = (source.conversation_name(), child.conversation_name());
        if source_key == child_key {
            return Err(SessionError::Backend(Arc::from(format!(
                "cannot fork conversation '{source_key}' onto itself"
            ))));
        }
        let existing = self.load(child).await?;
        if !existing.items.is_empty() {
            return Err(SessionError::Backend(Arc::from(format!(
                "fork target '{child_key}' already holds {} items; seeding it would interleave \
                 two histories",
                existing.items.len()
            ))));
        }

        let mut transcript = self.load(source).await?;
        transcript.items.truncate(boundary);
        let seeded = transcript.items.len();
        self.append(child, transcript.items).await?;
        Ok(seeded)
    }
}
