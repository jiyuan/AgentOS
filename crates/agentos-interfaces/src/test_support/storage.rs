//! The two stateful mocks: an in-memory [`Memory`] and an in-memory
//! [`Session`].
//!
//! Split out of the parent module when it reached the 800-line ceiling. These
//! two belong together and apart from the rest: every other mock answers from
//! a canned response, while these are real (if naive) stores whose behaviour a
//! test builds up across several calls.

use crate::memory::{Memory, MemoryError, Query, QueryType, Record, Selector};
use crate::session::{Item, Session, SessionError, Transcript};
use agentos_proto::{Namespace, Principal, RecordId};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Mutex;

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

/// In-memory [`Session`] mock, keyed by [`Principal::conversation_name`].
///
/// Conversation-keyed, not principal-keyed, for the same reason a real store
/// is: everyone in a conversation shares one transcript. The mock has no
/// epochs, so it does not model `/clear` — a test about clearing needs the
/// SQLite store, which is where the epoch lives.
pub struct MockSession {
    transcripts: Mutex<BTreeMap<String, Transcript>>,
}

impl MockSession {
    pub fn new() -> Self {
        Self {
            transcripts: Mutex::new(BTreeMap::new()),
        }
    }

    /// Seed the transcript of a conversation, named by the principal that
    /// speaks in it. The sender is dropped, as everywhere else.
    pub fn with_transcript(self, principal: &Principal, transcript: Transcript) -> Self {
        self.transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned")
            .insert(principal.conversation_name(), transcript);
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
    async fn load(&self, principal: &Principal) -> Result<Transcript, SessionError> {
        Ok(self
            .transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned")
            .get(&principal.conversation_name())
            .cloned()
            .unwrap_or_default())
    }

    async fn append(&self, principal: &Principal, items: Vec<Item>) -> Result<(), SessionError> {
        let mut transcripts = self
            .transcripts
            .lock()
            .expect("MockSession transcripts lock not poisoned");
        transcripts
            .entry(principal.conversation_name())
            .or_default()
            .items
            .extend(items);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, ChannelId, ConversationId, Message, MessageRole};

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

    fn principal(conversation: &str) -> Principal {
        Principal::conversation(
            AgentId::new("main"),
            ChannelId::new("telegram"),
            ConversationId::new(conversation),
        )
    }

    #[tokio::test]
    async fn mock_session_appends_and_loads() {
        let session = MockSession::new();
        let alice = principal("conv-1").with_sender("alice");
        session
            .append(
                &alice,
                vec![Item {
                    message: Message::text(MessageRole::User, "first"),
                    metadata: BTreeMap::new(),
                }],
            )
            .await
            .expect("append ok");
        assert_eq!(session.load(&alice).await.expect("load ok").items.len(), 1);

        // The same conversation seen by a second participant: one transcript.
        let bob = principal("conv-1").with_sender("bob");
        assert_eq!(session.load(&bob).await.expect("load ok").items.len(), 1);

        // The same conversation *number* on another channel: not the same
        // transcript. This is the collision M3 deliverable 2 removed.
        let elsewhere = Principal::conversation(
            AgentId::new("main"),
            ChannelId::new("feishu"),
            ConversationId::new("conv-1"),
        );
        assert!(session
            .load(&elsewhere)
            .await
            .expect("load ok")
            .items
            .is_empty());
    }
}
