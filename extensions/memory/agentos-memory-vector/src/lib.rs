//! `agentos-memory-vector` — a first-party AgentOS extension crate.
//!
//! Reference example of the extension contract: depends only on
//! `agentos-interfaces` (+ `agentos-proto` for id types, `reqwest` for the
//! embeddings HTTP call) and implements the public [`SemanticIndex`] trait. The
//! runtime never names this crate; the CLI selects it by the
//! `[memory].semantic_backend = "vector"` config string and injects it through
//! `AgentRuntime::build_with`.
//!
//! [`VectorSemanticIndex`] is an in-process cosine-similarity index over a
//! pluggable [`Embedder`]:
//!
//! - [`ApiEmbedder`] calls an OpenAI-compatible `/embeddings` endpoint for real
//!   (paraphrase-capable) semantic vectors. Selected by [`VectorSemanticIndex::from_env`]
//!   when `AGENTOS_EMBEDDINGS_API_KEY` (or `OPENAI_API_KEY`) is set.
//! - [`HashingEmbedder`] is a deterministic, offline bag-of-hashed-tokens
//!   embedder (lexical-vector, not true semantics) used as the fallback when no
//!   embeddings API is configured.
//!
//! Embedding failures degrade gracefully: a failed `upsert` skips indexing that
//! record and a failed `search` returns no hits, so the core lexical (FTS) path
//! still serves retrieval. Vectors live in memory and are rebuilt from new
//! writes after a restart.

use agentos_interfaces::memory::{MemoryError, Record};
use agentos_interfaces::semantic::{SemanticIndex, SemanticSearchHit};
use agentos_proto::{Namespace, RecordId};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

/// Default width for the offline [`HashingEmbedder`]. Wide enough to keep hash
/// collisions low for short memory records while staying cheap to score.
pub const DEFAULT_DIMENSIONS: usize = 384;

const DEFAULT_EMBEDDINGS_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_EMBEDDINGS_MODEL: &str = "text-embedding-3-small";

/// Turns text into a unit-normalized embedding vector. `None` signals the
/// embedding is unavailable (e.g. the API errored), so the index degrades to
/// "no semantic hit" rather than failing the run.
#[async_trait]
pub trait Embedder: Send + Sync {
    async fn embed(&self, text: &str) -> Option<Vec<f32>>;
}

/// In-memory cosine-similarity [`SemanticIndex`] over a pluggable [`Embedder`].
/// Records are embedded and grouped by namespace; `search` ranks a namespace's
/// vectors against the embedded query.
pub struct VectorSemanticIndex {
    embedder: Arc<dyn Embedder>,
    // namespace -> (record id -> unit-normalized embedding)
    vectors: Mutex<HashMap<String, HashMap<String, Vec<f32>>>>,
}

impl VectorSemanticIndex {
    pub fn new(embedder: Arc<dyn Embedder>) -> Self {
        Self {
            embedder,
            vectors: Mutex::new(HashMap::new()),
        }
    }

    /// Offline index backed by the deterministic hashing embedder.
    pub fn with_hashing(dimensions: usize) -> Self {
        Self::new(Arc::new(HashingEmbedder::new(dimensions)))
    }

    /// Select the embedder from the environment: an OpenAI-compatible
    /// [`ApiEmbedder`] when `AGENTOS_EMBEDDINGS_API_KEY` (or `OPENAI_API_KEY`) is
    /// set, otherwise the offline [`HashingEmbedder`]. Knobs:
    /// `AGENTOS_EMBEDDINGS_BASE_URL` (default OpenAI), `AGENTOS_EMBEDDINGS_MODEL`
    /// (default `text-embedding-3-small`).
    pub fn from_env() -> Self {
        let api_key = std::env::var("AGENTOS_EMBEDDINGS_API_KEY")
            .ok()
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .filter(|key| !key.trim().is_empty());
        match api_key {
            Some(api_key) => {
                let base_url = std::env::var("AGENTOS_EMBEDDINGS_BASE_URL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_EMBEDDINGS_BASE_URL.to_owned());
                let model = std::env::var("AGENTOS_EMBEDDINGS_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_EMBEDDINGS_MODEL.to_owned());
                tracing::info!(
                    target: "agentos_memory_vector",
                    model = model.as_str(),
                    "vector semantic index using API embeddings"
                );
                Self::new(Arc::new(ApiEmbedder::new(&base_url, Some(api_key), model)))
            }
            None => {
                tracing::info!(
                    target: "agentos_memory_vector",
                    "vector semantic index using offline hashing embedder (no embeddings API configured)"
                );
                Self::with_hashing(DEFAULT_DIMENSIONS)
            }
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
        Self::with_hashing(DEFAULT_DIMENSIONS)
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
        // Best-effort: if the embedder is unavailable, leave the record
        // unindexed (the core lexical path still retrieves it).
        let Some(vector) = self.embedder.embed(&searchable_text(record)).await else {
            return Ok(());
        };
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
        let Some(query_vector) = self.embedder.embed(query).await else {
            return Ok(Vec::new());
        };
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

/// Deterministic offline embedder: a bag of hashed alphanumeric tokens mapped
/// onto a fixed-width vector, L2-normalized so a dot product is cosine
/// similarity. Shared tokens raise similarity (lexical-vector, not paraphrase).
pub struct HashingEmbedder {
    dimensions: usize,
}

impl HashingEmbedder {
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions: dimensions.max(1),
        }
    }
}

#[async_trait]
impl Embedder for HashingEmbedder {
    async fn embed(&self, text: &str) -> Option<Vec<f32>> {
        let mut vector = vec![0.0f32; self.dimensions];
        for token in text
            .split(|ch: char| !ch.is_alphanumeric())
            .filter(|token| !token.is_empty())
        {
            let hash = fnv1a64(token.to_ascii_lowercase().as_bytes());
            let index = (hash as usize) % self.dimensions;
            vector[index] += if hash & 1 == 0 { 1.0 } else { -1.0 };
        }
        normalize(&mut vector);
        Some(vector)
    }
}

/// Real semantic embedder: calls an OpenAI-compatible `POST {base}/embeddings`
/// endpoint (`{ "model", "input" }` → `data[0].embedding`). Returns `None` on
/// any transport/parse error so the index degrades gracefully.
pub struct ApiEmbedder {
    client: reqwest::Client,
    url: String,
    api_key: Option<String>,
    model: String,
}

impl ApiEmbedder {
    pub fn new(base_url: &str, api_key: Option<String>, model: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self {
            client,
            url: format!("{}/embeddings", base_url.trim_end_matches('/')),
            api_key,
            model: model.into(),
        }
    }
}

#[async_trait]
impl Embedder for ApiEmbedder {
    async fn embed(&self, text: &str) -> Option<Vec<f32>> {
        let mut request = self
            .client
            .post(&self.url)
            .json(&serde_json::json!({ "model": self.model, "input": text }));
        if let Some(api_key) = &self.api_key {
            request = request.bearer_auth(api_key);
        }
        let response = match request.send().await.and_then(|r| r.error_for_status()) {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(
                    target: "agentos_memory_vector",
                    error = %err,
                    "embeddings request failed; record will not be semantically indexed"
                );
                return None;
            }
        };
        let body: serde_json::Value = response.json().await.ok()?;
        let values = body.get("data")?.get(0)?.get("embedding")?.as_array()?;
        let mut vector: Vec<f32> = values
            .iter()
            .filter_map(|value| value.as_f64().map(|number| number as f32))
            .collect();
        if vector.is_empty() {
            return None;
        }
        normalize(&mut vector);
        Some(vector)
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

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

/// Scale a vector to unit length so a dot product equals cosine similarity.
/// No-op for the zero vector.
fn normalize(vector: &mut [f32]) {
    let magnitude = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for component in vector {
            *component /= magnitude;
        }
    }
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

    /// A deterministic embedder that hands back caller-chosen vectors, so index
    /// ranking can be asserted independently of any embedding scheme.
    struct FixedEmbedder;

    #[async_trait]
    impl Embedder for FixedEmbedder {
        async fn embed(&self, text: &str) -> Option<Vec<f32>> {
            // "near" -> aligned with the query; "far" -> orthogonal; else zero.
            let vector = match text {
                t if t.contains("near") => vec![1.0, 0.0],
                t if t.contains("far") => vec![0.0, 1.0],
                _ => vec![0.0, 0.0],
            };
            Some(vector)
        }
    }

    #[tokio::test]
    async fn ranks_by_embedder_similarity() {
        let index = VectorSemanticIndex::new(Arc::new(FixedEmbedder));
        index.upsert(&record("r-near", "ns", "near")).await.unwrap();
        index.upsert(&record("r-far", "ns", "far")).await.unwrap();

        // Query embeds to [1,0] ("near"); only the aligned record scores > 0.
        let hits = index
            .search(&Namespace::new("ns"), "near", 5)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record_id.as_str(), "r-near");
    }

    #[tokio::test]
    async fn hashing_fallback_ranks_token_overlap() {
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
