//! The two stateful mocks: an in-memory [`Memory`] and an in-memory
//! [`Session`].
//!
//! Split out of the parent module when it reached the 800-line ceiling. These
//! two belong together and apart from the rest: every other mock answers from
//! a canned response, while these are real (if naive) stores whose behaviour a
//! test builds up across several calls.

use crate::memory::{Memory, MemoryError, Query, QueryType, Record, Selector};
use crate::session::{Item, Session, SessionError, Transcript};
use agentos_proto::{ConversationId, Namespace, RecordId};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

/// In-memory [`Memory`] mock.
///
/// Records are stored in a `Vec` per namespace. `read` honors `Query::limit`
/// and matches `QueryType::Lexical` against `record.body.to_string()` /
/// metadata as a substring search; `Filter` and `Semantic` queries return all
/// records up to the limit.
pub struct MockMemory {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    records: Vec<Record>,
    next_id: u64,
}

impl MockMemory {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
        }
    }

    pub fn with_records(self, records: impl IntoIterator<Item = Record>) -> Self {
        let mut state = self
            .state
            .lock()
            .expect("MockMemory state lock not poisoned");
        state.records.extend(records);
        drop(state);
        self
    }

    /// Snapshot every record currently held by the mock, in insertion order.
    pub fn snapshot(&self) -> Vec<Record> {
        self.state
            .lock()
            .expect("MockMemory state lock not poisoned")
            .records
            .clone()
    }
}

impl Default for MockMemory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Memory for MockMemory {
    async fn write(&self, ns: &Namespace, mut record: Record) -> Result<RecordId, MemoryError> {
        let mut state = self
            .state
            .lock()
            .expect("MockMemory state lock not poisoned");
        record.namespace = ns.clone();
        let id = record.id.clone().unwrap_or_else(|| {
            state.next_id += 1;
            RecordId::new(format!("mock-record-{}", state.next_id))
        });
        record.id = Some(id.clone());
        state.records.push(record);
        Ok(id)
    }

    async fn read(&self, ns: &Namespace, q: &Query) -> Result<Vec<Record>, MemoryError> {
        let state = self
            .state
            .lock()
            .expect("MockMemory state lock not poisoned");
        if q.limit == 0 {
            return Ok(Vec::new());
        }
        let lexical = match &q.query_type {
            QueryType::Lexical(text) => Some(text.to_ascii_lowercase()),
            QueryType::Filter | QueryType::Semantic => None,
        };
        let mut out = Vec::new();
        for record in &state.records {
            if &record.namespace != ns {
                continue;
            }
            if let Some(needle) = &lexical {
                let body = record.body.to_string().to_ascii_lowercase();
                let metadata = serde_json::to_string(&record.metadata)
                    .map(|json| json.to_ascii_lowercase())
                    .unwrap_or_default();
                if !body.contains(needle) && !metadata.contains(needle) {
                    continue;
                }
            }
            out.push(record.clone());
            if out.len() >= q.limit {
                break;
            }
        }
        Ok(out)
    }

    async fn forget(&self, ns: &Namespace, sel: &Selector) -> Result<usize, MemoryError> {
        let mut state = self
            .state
            .lock()
            .expect("MockMemory state lock not poisoned");
        let before = state.records.len();
        state.records.retain(|record| {
            if &record.namespace != ns {
                return true;
            }
            if let Some(id) = &sel.id {
                return record.id.as_ref() != Some(id);
            }
            if let Some(namespace) = &sel.namespace {
                return &record.namespace != namespace;
            }
            false
        });
        Ok(before - state.records.len())
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/// In-memory [`Session`] mock backed by a `BTreeMap` of conversations.
pub struct MockSession {
    transcripts: Mutex<BTreeMap<ConversationId, Transcript>>,
}

impl MockSession {
    pub fn new() -> Self {
        Self {
            transcripts: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_transcript(self, conv_id: impl Into<Arc<str>>, transcript: Transcript) -> Self {
        self.transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned")
            .insert(ConversationId::new(conv_id), transcript);
        self
    }
}

impl Default for MockSession {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Session for MockSession {
    async fn load(&self, conv_id: &ConversationId) -> Result<Transcript, SessionError> {
        Ok(self
            .transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned")
            .get(conv_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn append(&self, conv_id: &ConversationId, items: Vec<Item>) -> Result<(), SessionError> {
        let mut transcripts = self
            .transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned");
        transcripts
            .entry(conv_id.clone())
            .or_default()
            .items
            .extend(items);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{Message, MessageRole};

    #[tokio::test]
    async fn mock_memory_round_trips_and_forgets() {
        let memory = MockMemory::new();
        let ns = Namespace::new("test:ns");
        let id = memory
            .write(
                &ns,
                Record {
                    id: None,
                    namespace: ns.clone(),
                    body: serde_json::json!({"text": "hello"}),
                    metadata: BTreeMap::new(),
                },
            )
            .await
            .expect("write ok");
        let records = memory.read(&ns, &Query::filter(10)).await.expect("read ok");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id.as_ref(), Some(&id));
        let removed = memory
            .forget(
                &ns,
                &Selector {
                    id: Some(id),
                    namespace: None,
                },
            )
            .await
            .expect("forget ok");
        assert_eq!(removed, 1);
        assert!(memory
            .read(&ns, &Query::filter(10))
            .await
            .expect("read ok")
            .is_empty());
    }

    #[tokio::test]
    async fn mock_session_appends_and_loads() {
        let session = MockSession::new();
        let conv = ConversationId::new("conv-1");
        session
            .append(
                &conv,
                vec![Item {
                    message: Message::text(MessageRole::User, "first"),
                    metadata: BTreeMap::new(),
                }],
            )
            .await
            .expect("append ok");
        let transcript = session.load(&conv).await.expect("load ok");
        assert_eq!(transcript.items.len(), 1);
    }
}
