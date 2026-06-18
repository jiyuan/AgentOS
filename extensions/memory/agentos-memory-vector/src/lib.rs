//! `agentos-memory-vector` — a first-party AgentOS extension crate.
//!
//! This is the reference example of the extension contract: it depends **only**
//! on `agentos-interfaces` (plus `agentos-proto` for the id types) and
//! implements the public [`SemanticIndex`] trait. The runtime never names this
//! crate; the CLI selects it by the `[memory].semantic_backend = "vector"`
//! config string and injects it through `AgentRuntime::build_with`.
//!
//! [`VectorSemanticIndex`] is an in-process cosine-similarity index over a
//! deterministic local hashing embedder — no network, no extra storage. The
//! embedder is intentionally simple (hashed-token bag-of-words), so retrieval is
//! lexical-vector rather than true paraphrase semantics; swapping in a real
//! embedding source (a local model, or a provider endpoint) is a drop-in change
//! behind the same trait. Vectors live in memory and are rebuilt from new writes
//! after a restart; the core lexical (FTS) path is the durable fallback.

use agentos_interfaces::memory::{MemoryError, Record};
use agentos_interfaces::semantic::{SemanticIndex, SemanticSearchHit};
use agentos_proto::{Namespace, RecordId};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

/// Default embedding width. Wide enough to keep hash collisions low for short
/// memory records while staying cheap to score.
pub const DEFAULT_DIMENSIONS: usize = 384;

/// In-memory cosine-similarity [`SemanticIndex`]. Records are embedded with a
/// local hashing embedder and grouped by namespace; `search` ranks a namespace's
/// vectors against the embedded query.
pub struct VectorSemanticIndex {
    dimensions: usize,
    // namespace -> (record id -> unit-normalized embedding)
    vectors: Mutex<HashMap<String, HashMap<String, Vec<f32>>>>,
}

impl VectorSemanticIndex {
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions: dimensions.max(1),
            vectors: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, HashMap<String, Vec<f32>>>> {
        // A poisoned lock still holds a structurally valid map; recover it
        // rather than propagating a panic into the run loop.
        self.vectors.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl Default for VectorSemanticIndex {
    fn default() -> Self {
        Self::new(DEFAULT_DIMENSIONS)
    }
}

#[async_trait]
impl SemanticIndex for VectorSemanticIndex {
    async fn upsert(&self, record: &Record) -> Result<(), MemoryError> {
        let Some(record_id) = &record.id else {
            return Err(MemoryError::Backend(
                "vector index upsert requires a stable record id".into(),
            ));
        };
        let vector = hash_embedding(&searchable_text(record), self.dimensions);
        self.lock()
            .entry(record.namespace.as_str().to_owned())
            .or_default()
            .insert(record_id.as_str().to_owned(), vector);
        Ok(())
    }

    async fn search(
        &self,
        namespace: &Namespace,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SemanticSearchHit>, MemoryError> {
        if limit == 0 || query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let query_vector = hash_embedding(query, self.dimensions);
        let guard = self.lock();
        let Some(namespace_vectors) = guard.get(namespace.as_str()) else {
            return Ok(Vec::new());
        };
        let mut hits = namespace_vectors
            .iter()
            .map(|(record_id, vector)| SemanticSearchHit {
                record_id: RecordId::new(record_id.as_str()),
                score: dot(&query_vector, vector) as f64,
            })
            .filter(|hit| hit.score > 0.0)
            .collect::<Vec<_>>();
        // Highest score first; break ties by id for a deterministic order.
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.record_id.as_str().cmp(right.record_id.as_str()))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    async fn delete(
        &self,
        namespace: &Namespace,
        record_ids: &[RecordId],
    ) -> Result<(), MemoryError> {
        let mut guard = self.lock();
        if let Some(namespace_vectors) = guard.get_mut(namespace.as_str()) {
            for record_id in record_ids {
                namespace_vectors.remove(record_id.as_str());
            }
        }
        Ok(())
    }
}

/// The text a record is embedded from: its JSON body plus serialized metadata.
fn searchable_text(record: &Record) -> String {
    let mut text = record.body.to_string();
    if let Ok(metadata) = serde_json::to_string(&record.metadata) {
        text.push(' ');
        text.push_str(&metadata);
    }
    text
}

/// Deterministic bag-of-hashed-tokens embedding, L2-normalized so a dot product
/// is cosine similarity. Each alphanumeric token hashes to one dimension with a
/// sign, so shared tokens raise similarity.
fn hash_embedding(input: &str, dimensions: usize) -> Vec<f32> {
    let mut vector = vec![0.0f32; dimensions];
    for token in input
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let hash = fnv1a64(token.to_ascii_lowercase().as_bytes());
        let index = (hash as usize) % dimensions;
        vector[index] += if hash & 1 == 0 { 1.0 } else { -1.0 };
    }
    let magnitude = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for component in &mut vector {
            *component /= magnitude;
        }
    }
    vector
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn record(id: &str, namespace: &str, text: &str) -> Record {
        Record {
            id: Some(RecordId::new(id)),
            namespace: Namespace::new(namespace),
            body: json!({ "summary": text }),
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn search_ranks_token_overlap_above_unrelated() {
        let index = VectorSemanticIndex::default();
        index
            .upsert(&record(
                "r1",
                "ns",
                "deploy the payment service to production",
            ))
            .await
            .unwrap();
        index
            .upsert(&record(
                "r2",
                "ns",
                "favorite breakfast recipes and cooking tips",
            ))
            .await
            .unwrap();

        let hits = index
            .search(&Namespace::new("ns"), "deploy payment service", 5)
            .await
            .unwrap();
        assert_eq!(hits.first().map(|h| h.record_id.as_str()), Some("r1"));
    }

    #[tokio::test]
    async fn search_is_namespace_scoped() {
        let index = VectorSemanticIndex::default();
        index
            .upsert(&record("r1", "ns-a", "shared topic words"))
            .await
            .unwrap();
        let hits = index
            .search(&Namespace::new("ns-b"), "shared topic words", 5)
            .await
            .unwrap();
        assert!(hits.is_empty(), "other namespaces must not match");
    }

    #[tokio::test]
    async fn delete_removes_a_record_from_results() {
        let index = VectorSemanticIndex::default();
        index
            .upsert(&record("r1", "ns", "alpha beta gamma"))
            .await
            .unwrap();
        index
            .delete(&Namespace::new("ns"), &[RecordId::new("r1")])
            .await
            .unwrap();
        let hits = index
            .search(&Namespace::new("ns"), "alpha beta gamma", 5)
            .await
            .unwrap();
        assert!(hits.is_empty());
    }
}
