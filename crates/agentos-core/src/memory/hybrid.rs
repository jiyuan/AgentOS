use super::MemoryError;
use agentos_interfaces::memory::Record;
use agentos_proto::RecordId;
use std::collections::BTreeMap;
use std::sync::Arc;

// The vector-index ABI now lives in `agentos-interfaces` so out-of-tree
// extension crates can implement it without depending on core. Re-exported here
// so the in-crate reference impls and call sites keep their `super::` paths.
pub use agentos_interfaces::semantic::{SemanticIndex, SemanticSearchHit};

pub(crate) const RRF_K: f64 = 60.0;

pub(crate) fn reciprocal_rank_fusion(
    ranked_lists: &[Vec<RecordId>],
    limit: usize,
) -> Vec<RecordId> {
    if limit == 0 {
        return Vec::new();
    }

    let mut scores = BTreeMap::<RecordId, (f64, usize)>::new();
    let mut next_seen = 0usize;
    for ranked in ranked_lists {
        for (rank, record_id) in ranked.iter().enumerate() {
            let entry = scores.entry(record_id.clone()).or_insert_with(|| {
                let first_seen = next_seen;
                next_seen += 1;
                (0.0, first_seen)
            });
            entry.0 += 1.0 / (RRF_K + rank as f64 + 1.0);
        }
    }

    let mut fused = scores
        .into_iter()
        .map(|(record_id, (score, first_seen))| (record_id, score, first_seen))
        .collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.2.cmp(&right.2))
            .then(left.0.as_str().cmp(right.0.as_str()))
    });
    fused
        .into_iter()
        .take(limit)
        .map(|(record_id, _, _)| record_id)
        .collect()
}

pub(crate) fn searchable_record_text(record: &Record) -> String {
    let mut text = record.body.to_string();
    if let Ok(metadata) = serde_json::to_string(&record.metadata) {
        text.push(' ');
        text.push_str(&metadata);
    }
    text
}

pub(crate) fn hash_embedding(input: &str, dimensions: usize) -> Vec<f32> {
    let dimensions = dimensions.max(1);
    let mut vector = vec![0.0f32; dimensions];
    for token in input
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let hash = fnv1a64(token.to_ascii_lowercase().as_bytes());
        let index = (hash as usize) % dimensions;
        let sign = if hash & 1 == 0 { 1.0 } else { -1.0 };
        vector[index] += sign;
    }
    normalize_vector(&mut vector);
    vector
}

pub(crate) fn vector_json(vector: &[f32]) -> String {
    let values = vector
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    format!("[{}]", values.join(","))
}

/// The vector to store for `record`: a caller-supplied embedding when the
/// metadata carries one, else the offline hash embedding of its searchable
/// text. Both vector backends decide this identically, and a backend that
/// drifted from the other would index the same record two different ways.
pub(crate) fn record_embedding(record: &Record, dimensions: usize) -> Vec<f32> {
    metadata_embedding(record)
        .unwrap_or_else(|| hash_embedding(&searchable_record_text(record), dimensions))
}

pub(crate) fn metadata_embedding(record: &Record) -> Option<Vec<f32>> {
    record
        .metadata
        .get("embedding")
        .or_else(|| record.body.get("embedding"))
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_f64().map(|number| number as f32))
                .collect::<Vec<_>>()
        })
        .filter(|vector| !vector.is_empty())
}

fn normalize_vector(vector: &mut [f32]) {
    let magnitude = vector
        .iter()
        .map(|component| component * component)
        .sum::<f32>()
        .sqrt();
    if magnitude == 0.0 {
        return;
    }
    for component in vector {
        *component /= magnitude;
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

pub(crate) fn stable_hash_u64(input: &str) -> u64 {
    fnv1a64(input.as_bytes())
}

pub(crate) fn memory_backend_error(message: impl Into<String>) -> MemoryError {
    MemoryError::Backend(Arc::from(message.into()))
}
