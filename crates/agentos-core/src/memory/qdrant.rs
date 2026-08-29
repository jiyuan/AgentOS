use super::hybrid::{
    hash_embedding, memory_backend_error, record_embedding, SemanticIndex, SemanticSearchHit,
};
use super::MemoryError;
use crate::http::shared_client;
use agentos_interfaces::memory::Record;
use agentos_proto::{Namespace, RecordId};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QdrantSemanticConfig {
    pub url: Arc<str>,
    pub collection: Arc<str>,
    pub vector_name: Option<Arc<str>>,
    pub vector_dimensions: usize,
    pub api_key: Option<Arc<str>>,
    pub timeout_ms: u64,
}

impl Default for QdrantSemanticConfig {
    fn default() -> Self {
        Self {
            url: Arc::from("http://127.0.0.1:6333"),
            collection: Arc::from("agentos_memory"),
            vector_name: None,
            vector_dimensions: 384,
            api_key: None,
            timeout_ms: 3_000,
        }
    }
}

pub struct QdrantSemanticIndex {
    config: QdrantSemanticConfig,
    base_url: String,
}

impl QdrantSemanticIndex {
    pub fn new(config: QdrantSemanticConfig) -> Result<Self, MemoryError> {
        if !config.url.starts_with("http://") && !config.url.starts_with("https://") {
            return Err(memory_backend_error(
                "qdrant url must use http:// or https://",
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(memory_backend_error(
                "qdrant collection must not be empty when semantic memory is enabled",
            ));
        }
        if config.vector_dimensions == 0 {
            return Err(memory_backend_error(
                "qdrant vector_dimensions must be greater than 0",
            ));
        }
        let base_url = config.url.trim_end_matches('/').to_owned();
        Ok(Self { config, base_url })
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: &Value,
    ) -> Result<Value, MemoryError> {
        let url = format!("{}{path}", self.base_url);
        let payload = serde_json::to_vec(body).map_err(super::memory_json_error)?;
        let mut request = shared_client()
            .request(method, &url)
            .timeout(Duration::from_millis(self.config.timeout_ms))
            .header("Content-Type", "application/json")
            .body(payload);
        if let Some(api_key) = &self.config.api_key {
            request = request.header("api-key", api_key.as_ref());
        }
        let response = request.send().await.map_err(|err| {
            memory_backend_error(format!(
                "failed to call qdrant '{}': {err}",
                self.config.url
            ))
        })?;
        let status = response.status();
        let body = response.bytes().await.map_err(|err| {
            memory_backend_error(format!("failed to read qdrant response: {err}"))
        })?;
        if !status.is_success() {
            return Err(memory_backend_error(format!(
                "qdrant request failed with HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )));
        }
        if body.is_empty() {
            return Ok(json!({}));
        }
        serde_json::from_slice(&body).map_err(super::memory_json_error)
    }

    fn search_vector(&self, query: &str) -> Vec<f32> {
        hash_embedding(query, self.config.vector_dimensions)
    }

    fn upsert_vector_value(&self, vector: Vec<f32>) -> Value {
        if let Some(vector_name) = &self.config.vector_name {
            let mut vectors = serde_json::Map::new();
            vectors.insert(vector_name.to_string(), json!(vector));
            Value::Object(vectors)
        } else {
            json!(vector)
        }
    }

    fn search_vector_value(&self, vector: Vec<f32>) -> Value {
        if let Some(vector_name) = &self.config.vector_name {
            json!({
                "name": vector_name.as_ref(),
                "vector": vector,
            })
        } else {
            json!(vector)
        }
    }

    fn upsert_body(&self, record: &Record) -> Result<Value, MemoryError> {
        let Some(record_id) = &record.id else {
            return Err(memory_backend_error(
                "qdrant upsert requires a stable memory record id",
            ));
        };
        // The scope fields are mirrored onto the record by `managed_metadata`,
        // so the payload is built from the record alone (the index ABI carries
        // no `MemoryScope`).
        let meta = |key: &str| {
            record
                .metadata
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        };
        Ok(json!({
            "points": [{
                "id": qdrant_point_id(record_id),
                "vector": self.upsert_vector_value(record_embedding(record, self.config.vector_dimensions)),
                "payload": {
                    "record_id": record_id.as_str(),
                    "namespace": record.namespace.as_str(),
                    "store": meta("store"),
                    "owner_kind": meta("owner_kind"),
                    "visibility": meta("visibility"),
                    "domain": meta("domain"),
                },
            }],
        }))
    }
}

#[async_trait]
impl SemanticIndex for QdrantSemanticIndex {
    async fn upsert(&self, record: &Record) -> Result<(), MemoryError> {
        let body = self.upsert_body(record)?;
        self.request_json(
            Method::PUT,
            &format!(
                "/collections/{}/points?wait=true",
                percent_encode_path(&self.config.collection)
            ),
            &body,
        )
        .await?;
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

        let body = json!({
            "vector": self.search_vector_value(self.search_vector(query)),
            "filter": {
                "must": [{
                    "key": "namespace",
                    "match": { "value": namespace.as_str() }
                }]
            },
            "limit": limit,
            "with_payload": true,
        });
        let response = self
            .request_json(
                Method::POST,
                &format!(
                    "/collections/{}/points/search",
                    percent_encode_path(&self.config.collection)
                ),
                &body,
            )
            .await?;

        let Some(results) = response.get("result").and_then(Value::as_array) else {
            return Ok(Vec::new());
        };
        Ok(results
            .iter()
            .filter_map(|result| {
                let score = result.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                let record_id = result
                    .get("payload")
                    .and_then(|payload| payload.get("record_id"))
                    .and_then(Value::as_str)?;
                Some(SemanticSearchHit {
                    record_id: RecordId::new(record_id),
                    score,
                })
            })
            .collect())
    }

    async fn delete(
        &self,
        _namespace: &Namespace,
        record_ids: &[RecordId],
    ) -> Result<(), MemoryError> {
        if record_ids.is_empty() {
            return Ok(());
        }
        let body = json!({
            "points": record_ids.iter().map(qdrant_point_id).collect::<Vec<_>>(),
        });
        self.request_json(
            Method::POST,
            &format!(
                "/collections/{}/points/delete?wait=true",
                percent_encode_path(&self.config.collection)
            ),
            &body,
        )
        .await?;
        Ok(())
    }
}

fn percent_encode_path(input: &str) -> String {
    input
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                vec![char::from(byte)]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn qdrant_point_id(record_id: &RecordId) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in record_id.as_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
