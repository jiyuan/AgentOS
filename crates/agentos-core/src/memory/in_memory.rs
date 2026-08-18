use super::{record_matches_query, MemoryError};
use agentos_interfaces::memory::{Memory, Query, Record, Selector};
use agentos_interfaces::session::{Item, Session, SessionError, Transcript};
use agentos_proto::{Namespace, RecordId, SessionKey};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct InMemoryMemory {
    records: Mutex<Vec<Record>>,
}

#[async_trait]
impl Memory for InMemoryMemory {
    async fn write(&self, ns: &Namespace, mut record: Record) -> Result<RecordId, MemoryError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryError::Backend(Arc::from("in-memory store lock poisoned")))?;
        let id = RecordId::new(format!("record-{}", records.len() + 1));
        record.namespace = ns.clone();
        record.id = Some(id.clone());
        records.push(record);
        Ok(id)
    }

    async fn read(&self, ns: &Namespace, q: &Query) -> Result<Vec<Record>, MemoryError> {
        let records = self
            .records
            .lock()
            .map_err(|_| MemoryError::Backend(Arc::from("in-memory store lock poisoned")))?;
        Ok(records
            .iter()
            .filter(|record| &record.namespace == ns)
            .filter(|record| record_matches_query(record, q))
            .take(q.limit)
            .cloned()
            .collect())
    }

    async fn forget(&self, ns: &Namespace, sel: &Selector) -> Result<usize, MemoryError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryError::Backend(Arc::from("in-memory store lock poisoned")))?;
        let before = records.len();
        records.retain(|record| {
            if &record.namespace != ns {
                return true;
            }

            match (&sel.id, &sel.namespace) {
                (Some(id), _) => record.id.as_ref() != Some(id),
                (None, Some(namespace)) => &record.namespace != namespace,
                (None, None) => false,
            }
        });
        Ok(before - records.len())
    }
}

#[derive(Default)]
pub struct InMemorySession {
    transcripts: Mutex<BTreeMap<SessionKey, Transcript>>,
}

#[async_trait]
impl Session for InMemorySession {
    async fn load(&self, session_key: &SessionKey) -> Result<Transcript, SessionError> {
        let transcripts = self
            .transcripts
            .lock()
            .map_err(|_| SessionError::Backend(Arc::from("in-memory session lock poisoned")))?;
        Ok(transcripts.get(session_key).cloned().unwrap_or_default())
    }

    async fn append(&self, session_key: &SessionKey, items: Vec<Item>) -> Result<(), SessionError> {
        let mut transcripts = self
            .transcripts
            .lock()
            .map_err(|_| SessionError::Backend(Arc::from("in-memory session lock poisoned")))?;
        transcripts
            .entry(session_key.clone())
            .or_default()
            .items
            .extend(items);
        Ok(())
    }
}
