use super::{MemoryCaller, MemoryError, MemoryScope};
use agentos_proto::{Namespace, RecordId};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MemoryOperation {
    Read,
    Write,
    Forget,
}

impl MemoryOperation {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Forget => "forget",
        }
    }
}

pub(super) trait MemoryAccounting: Send + Sync {
    fn record_read_access(&self, record_ids: &[RecordId]) -> Result<(), MemoryError>;
    fn append_access_log(&self, entry: MemoryAccessLogEntry<'_>) -> Result<(), MemoryError>;

    fn append_access_log_for_records(
        &self,
        operation: &'static str,
        record_ids: &[RecordId],
        namespace: &Namespace,
        caller: &MemoryCaller,
        reason: Option<&str>,
    ) -> Result<(), MemoryError> {
        for record_id in record_ids {
            self.append_access_log(MemoryAccessLogEntry {
                operation,
                record_id: Some(record_id),
                namespace,
                caller,
                reason,
            })?;
        }
        Ok(())
    }
}

pub(super) fn managed_metadata(
    caller: &MemoryCaller,
    scope: &MemoryScope,
    mut metadata: BTreeMap<Arc<str>, Value>,
) -> BTreeMap<Arc<str>, Value> {
    metadata.insert(
        Arc::from("store"),
        Value::String(scope.store.as_str().to_owned()),
    );
    metadata.insert(
        Arc::from("owner_kind"),
        Value::String(scope.owner.kind().to_owned()),
    );
    metadata.insert(
        Arc::from("owner_id"),
        Value::String(scope.owner.namespace_id()),
    );
    metadata.insert(
        Arc::from("visibility"),
        Value::String(scope.visibility.as_str().to_owned()),
    );
    metadata.insert(Arc::from("domain"), Value::String(scope.domain_name()));
    metadata.insert(
        Arc::from("source_agent_id"),
        Value::String(caller.agent_id.as_str().to_owned()),
    );
    metadata.insert(
        Arc::from("source_task_id"),
        Value::String(caller.task_id.as_str().to_owned()),
    );
    metadata.insert(
        Arc::from("conversation_id"),
        Value::String(caller.conversation_id.as_str().to_owned()),
    );
    metadata
        .entry(Arc::from("importance"))
        .or_insert_with(|| Value::from(0.0));
    metadata
        .entry(Arc::from("confidence"))
        .or_insert_with(|| Value::from(1.0));
    metadata
        .entry(Arc::from("status"))
        .or_insert_with(|| Value::String("active".to_owned()));
    metadata
        .entry(Arc::from("schema"))
        .or_insert_with(|| Value::String("agentos.memory.v1".to_owned()));
    metadata
}

pub(super) struct MemoryAccessLogEntry<'a> {
    pub(super) operation: &'static str,
    pub(super) record_id: Option<&'a RecordId>,
    pub(super) namespace: &'a Namespace,
    pub(super) caller: &'a MemoryCaller,
    pub(super) reason: Option<&'a str>,
}
