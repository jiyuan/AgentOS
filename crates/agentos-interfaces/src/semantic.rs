use crate::memory::{MemoryError, Record};
use agentos_proto::{Namespace, RecordId};
use async_trait::async_trait;

/// One similarity hit from a [`SemanticIndex`] search: the matched record id and
/// its backend-defined relevance score (higher is more relevant).
#[derive(Clone, Debug, PartialEq)]
pub struct SemanticSearchHit {
    pub record_id: RecordId,
    pub score: f64,
}

/// A vector / similarity index over memory records, layered beside the exact
/// [`crate::memory::Memory`] store to power semantic retrieval (hydration fuses
/// the two). Implementations embed and index a record on `upsert`, return ranked
/// hits for a free-text `query` scoped to a namespace on `search`, and drop
/// vectors on `delete`.
///
/// Everything the index needs is carried on the [`Record`] — `record.namespace`
/// plus the managed metadata keys (`store`, `owner_kind`, `visibility`,
/// `domain`) — so this trait depends only on the public ABI and can be
/// implemented by an out-of-tree extension crate without touching the core.
#[async_trait]
pub trait SemanticIndex: Send + Sync {
    /// Index (or re-index) a record's embedding for later similarity search.
    async fn upsert(&self, record: &Record) -> Result<(), MemoryError>;

    /// Return up to `limit` records most similar to `query` within `namespace`,
    /// ranked most-relevant first.
    async fn search(
        &self,
        namespace: &Namespace,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SemanticSearchHit>, MemoryError>;

    /// Drop the indexed vectors for `record_ids` within `namespace`.
    async fn delete(
        &self,
        namespace: &Namespace,
        record_ids: &[RecordId],
    ) -> Result<(), MemoryError>;
}
