use crate::memory::{
    memory_caller_from_context, MemoryCaller, MemoryManager, MemoryOwner, MemoryScope, MemoryStore,
    MemoryVisibility,
};
use agentos_interfaces::memory::{Query, Selector};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ConversationId, Namespace, RecordId, TaskId, ToolCall, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

const LEGACY_FACTS_NAMESPACE: &str = "facts";

pub struct MemoryTool {
    manager: Arc<MemoryManager>,
    legacy_namespace: Namespace,
}

impl MemoryTool {
    pub fn with_manager(manager: Arc<MemoryManager>) -> Self {
        Self {
            manager,
            legacy_namespace: Namespace::new(LEGACY_FACTS_NAMESPACE),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MemoryArgs {
    operation: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    store: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    visibility: Option<String>,
    #[serde(default)]
    domain: Option<String>,
}

#[async_trait]
impl Tool for MemoryTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("memory"),
            description: Arc::from("Store, recall, or forget persistent memory records."),
            input_schema: json!({
                "type": "object",
                "required": ["operation"],
                "properties": {
                    "operation": { "type": "string", "enum": ["write", "read", "forget"] },
                    "namespace": { "type": "string" },
                    "id": { "type": "string" },
                    "body": { "type": "object" },
                    "text": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 0 },
                    "store": { "type": "string", "enum": ["working", "episodic", "semantic", "procedural", "audit"] },
                    "owner": {
                        "type": "string",
                        "description": "Optional owner selector: user, agent, task, conversation, shared, or kind:id."
                    },
                    "visibility": { "type": "string", "enum": ["private", "shared", "public"] },
                    "domain": { "type": "string" }
                }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let caller = fallback_caller();
        self.call_scoped(call, args, &caller, None, true).await
    }

    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let caller = memory_caller_from_context(ctx, Vec::new());
        let default_owner = context_default_owner(ctx, &caller);
        self.call_scoped(
            call,
            args,
            &caller,
            default_owner,
            !is_subagent_context(ctx),
        )
        .await
    }
}

impl MemoryTool {
    async fn call_scoped(
        &self,
        call: &ToolCall,
        args: &RawValue,
        caller: &MemoryCaller,
        default_owner: Option<MemoryOwner>,
        allow_legacy_read: bool,
    ) -> Result<ToolResult, ToolError> {
        let parsed: MemoryArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();

        match parsed.operation.as_str() {
            "write" => self.write(call, caller, parsed, start, default_owner).await,
            "read" => {
                self.read(
                    call,
                    caller,
                    parsed,
                    start,
                    default_owner,
                    allow_legacy_read,
                )
                .await
            }
            "forget" => {
                self.forget(call, caller, parsed, start, default_owner)
                    .await
            }
            operation => Err(ToolError::Failed(
                format!("unsupported memory operation: {operation}").into(),
            )),
        }
    }

    async fn write(
        &self,
        call: &ToolCall,
        caller: &MemoryCaller,
        parsed: MemoryArgs,
        start: Instant,
        default_owner: Option<MemoryOwner>,
    ) -> Result<ToolResult, ToolError> {
        let body = parsed
            .body
            .clone()
            .ok_or_else(|| ToolError::Failed(Arc::from("memory write requires body")))?;
        let scope = scope_from_args(caller, &parsed, &self.legacy_namespace, default_owner)?;
        let mut record_metadata = BTreeMap::new();
        record_metadata.insert(
            Arc::from("tool_operation"),
            Value::String("write".to_owned()),
        );
        let id = self
            .manager
            .write_scoped_with_reason(
                caller,
                scope.clone(),
                body.clone(),
                record_metadata,
                Arc::from("memory_tool_write"),
            )
            .await
            .map_err(memory_tool_error)?;
        let namespace = scope.namespace();
        let summary = record_summary(&body);
        let content = format!("remembered: {summary}");
        let mut metadata = metadata(start, content.len() as u64);
        metadata.insert(Arc::from("operation"), Value::String("write".to_owned()));
        metadata.insert(
            Arc::from("record_id"),
            Value::String(id.as_str().to_owned()),
        );
        metadata.insert(
            Arc::from("namespace"),
            Value::String(namespace.as_str().to_owned()),
        );
        insert_scope_metadata(&mut metadata, &scope);

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(content),
            metadata,
        })
    }

    async fn read(
        &self,
        call: &ToolCall,
        caller: &MemoryCaller,
        parsed: MemoryArgs,
        start: Instant,
        default_owner: Option<MemoryOwner>,
        allow_legacy_read: bool,
    ) -> Result<ToolResult, ToolError> {
        let query = Query::lexical(
            parsed.text.clone().unwrap_or_default(),
            parsed.limit.unwrap_or(5),
        );
        let scope = scope_from_args(caller, &parsed, &self.legacy_namespace, default_owner)?;
        let mut records = self
            .manager
            .read_scoped_with_reason(caller, scope.clone(), &query, Arc::from("memory_tool_read"))
            .await
            .map_err(memory_tool_error)?;

        if allow_legacy_read
            && should_read_legacy_namespace(&parsed, &self.legacy_namespace)
            && records.len() < query.limit
        {
            let remaining = query.limit - records.len();
            let legacy_query = Query::lexical(query.lexical_text().unwrap_or_default(), remaining);
            records.extend(
                self.manager
                    .backend()
                    .read(&self.legacy_namespace, &legacy_query)
                    .await
                    .map_err(memory_tool_error)?,
            );
        }

        let content = if records.is_empty() {
            "no memories found".to_owned()
        } else {
            let items = records
                .iter()
                .map(|record| format!("- {}", record_summary(&record.body)))
                .collect::<Vec<_>>()
                .join("\n");
            format!("memories:\n{items}")
        };
        let mut metadata = metadata(start, content.len() as u64);
        metadata.insert(Arc::from("operation"), Value::String("read".to_owned()));
        metadata.insert(Arc::from("count"), Value::from(records.len() as u64));
        metadata.insert(
            Arc::from("namespace"),
            Value::String(scope.namespace().as_str().to_owned()),
        );
        insert_scope_metadata(&mut metadata, &scope);

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(content),
            metadata,
        })
    }

    async fn forget(
        &self,
        call: &ToolCall,
        caller: &MemoryCaller,
        parsed: MemoryArgs,
        start: Instant,
        default_owner: Option<MemoryOwner>,
    ) -> Result<ToolResult, ToolError> {
        let scope = scope_from_args(caller, &parsed, &self.legacy_namespace, default_owner)?;
        let namespace = scope.namespace();
        let selector = Selector {
            id: parsed.id.map(RecordId::new),
            namespace: Some(namespace.clone()),
        };
        let removed = self
            .manager
            .forget_scoped_with_reason(
                caller,
                scope.clone(),
                selector,
                Arc::from("memory_tool_forget"),
            )
            .await
            .map_err(memory_tool_error)?;
        let content = format!("forgot {removed} memory record(s)");
        let mut metadata = metadata(start, content.len() as u64);
        metadata.insert(Arc::from("operation"), Value::String("forget".to_owned()));
        metadata.insert(Arc::from("removed"), Value::from(removed as u64));
        metadata.insert(
            Arc::from("namespace"),
            Value::String(namespace.as_str().to_owned()),
        );
        insert_scope_metadata(&mut metadata, &scope);

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(content),
            metadata,
        })
    }
}

fn scope_from_args(
    caller: &MemoryCaller,
    parsed: &MemoryArgs,
    legacy_namespace: &Namespace,
    default_owner: Option<MemoryOwner>,
) -> Result<MemoryScope, ToolError> {
    if let Some(namespace) = parsed.namespace.as_deref() {
        if namespace != legacy_namespace.as_str() {
            return parse_scoped_namespace(namespace);
        }
    }

    let store = parsed
        .store
        .as_deref()
        .map(parse_store)
        .transpose()?
        .unwrap_or(MemoryStore::Semantic);
    let owner = parsed
        .owner
        .as_deref()
        .map(|owner| parse_owner(caller, owner))
        .transpose()?
        .unwrap_or_else(|| default_owner.unwrap_or_else(|| caller_default_owner(caller)));
    let visibility = parsed
        .visibility
        .as_deref()
        .map(parse_visibility)
        .transpose()?
        .unwrap_or(match owner {
            MemoryOwner::Shared => MemoryVisibility::Shared,
            _ => MemoryVisibility::Private,
        });
    Ok(MemoryScope::new(
        store,
        owner,
        visibility,
        parsed
            .domain
            .as_deref()
            .map(str::trim)
            .filter(|domain| !domain.is_empty())
            .map(Arc::from),
    ))
}

fn parse_scoped_namespace(namespace: &str) -> Result<MemoryScope, ToolError> {
    let parts = namespace.split('/').collect::<Vec<_>>();
    if parts.len() != 5 {
        return Err(ToolError::Failed(Arc::from(format!(
            "unsupported memory namespace: {namespace}"
        ))));
    }
    Ok(MemoryScope::new(
        parse_store(parts[3])?,
        parse_namespace_owner(parts[1], parts[2])?,
        parse_visibility(parts[0])?,
        Some(Arc::from(parts[4])),
    ))
}

fn parse_namespace_owner(kind: &str, id: &str) -> Result<MemoryOwner, ToolError> {
    match kind {
        "user" => Ok(MemoryOwner::User(Arc::from(id))),
        "agent" => Ok(MemoryOwner::Agent(AgentId::new(id))),
        "task" => Ok(MemoryOwner::Task(TaskId::new(id))),
        "conversation" => Ok(MemoryOwner::Conversation(ConversationId::new(id))),
        "shared" if id == "global" => Ok(MemoryOwner::Shared),
        "shared" => Err(ToolError::Failed(Arc::from(
            "shared memory namespace must use owner id global",
        ))),
        _ => Err(ToolError::Failed(Arc::from(format!(
            "unsupported memory owner kind: {kind}"
        )))),
    }
}

fn parse_owner(caller: &MemoryCaller, input: &str) -> Result<MemoryOwner, ToolError> {
    let trimmed = input.trim();
    let (kind, explicit_id) = trimmed.split_once(':').unwrap_or((trimmed, ""));
    match kind {
        "user" => {
            let id = if explicit_id.is_empty() {
                caller
                    .user_id
                    .clone()
                    .unwrap_or_else(|| Arc::from(caller.conversation_id.as_str()))
            } else {
                Arc::from(explicit_id)
            };
            Ok(MemoryOwner::User(id))
        }
        "agent" => Ok(MemoryOwner::Agent(if explicit_id.is_empty() {
            caller.agent_id.clone()
        } else {
            AgentId::new(explicit_id)
        })),
        "task" => Ok(MemoryOwner::Task(if explicit_id.is_empty() {
            caller.task_id.clone()
        } else {
            TaskId::new(explicit_id)
        })),
        "conversation" => Ok(MemoryOwner::Conversation(if explicit_id.is_empty() {
            caller.conversation_id.clone()
        } else {
            ConversationId::new(explicit_id)
        })),
        "shared" => Ok(MemoryOwner::Shared),
        _ => Err(ToolError::Failed(Arc::from(format!(
            "unsupported memory owner: {input}"
        )))),
    }
}

fn caller_default_owner(caller: &MemoryCaller) -> MemoryOwner {
    caller
        .user_id
        .clone()
        .map(MemoryOwner::User)
        .unwrap_or_else(|| MemoryOwner::Conversation(caller.conversation_id.clone()))
}

fn context_default_owner(ctx: &RunContext<'_>, caller: &MemoryCaller) -> Option<MemoryOwner> {
    let metadata = ctx
        .transcript
        .items
        .last()
        .map(|item| &item.metadata)
        .unwrap_or(&ctx.system.metadata);
    match metadata.get("memory_default_owner").and_then(Value::as_str) {
        Some("agent") => Some(MemoryOwner::Agent(caller.agent_id.clone())),
        Some("task") => Some(MemoryOwner::Task(caller.task_id.clone())),
        Some("conversation") => Some(MemoryOwner::Conversation(caller.conversation_id.clone())),
        Some("user") => caller.user_id.clone().map(MemoryOwner::User),
        Some("shared") => Some(MemoryOwner::Shared),
        _ => None,
    }
}

fn is_subagent_context(ctx: &RunContext<'_>) -> bool {
    ctx.transcript
        .items
        .last()
        .map(|item| &item.metadata)
        .unwrap_or(&ctx.system.metadata)
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "subagent_input")
}

fn parse_store(input: &str) -> Result<MemoryStore, ToolError> {
    match input.trim() {
        "working" => Ok(MemoryStore::Working),
        "episodic" => Ok(MemoryStore::Episodic),
        "semantic" => Ok(MemoryStore::Semantic),
        "procedural" => Ok(MemoryStore::Procedural),
        "audit" => Ok(MemoryStore::Audit),
        other => Err(ToolError::Failed(Arc::from(format!(
            "unsupported memory store: {other}"
        )))),
    }
}

fn parse_visibility(input: &str) -> Result<MemoryVisibility, ToolError> {
    match input.trim() {
        "private" => Ok(MemoryVisibility::Private),
        "shared" => Ok(MemoryVisibility::Shared),
        "public" => Ok(MemoryVisibility::Public),
        other => Err(ToolError::Failed(Arc::from(format!(
            "unsupported memory visibility: {other}"
        )))),
    }
}

fn should_read_legacy_namespace(parsed: &MemoryArgs, legacy_namespace: &Namespace) -> bool {
    parsed
        .namespace
        .as_deref()
        .is_none_or(|namespace| namespace == legacy_namespace.as_str())
}

fn insert_scope_metadata(metadata: &mut BTreeMap<Arc<str>, Value>, scope: &MemoryScope) {
    metadata.insert(
        Arc::from("scope_namespace"),
        Value::String(scope.namespace().as_str().to_owned()),
    );
    metadata.insert(
        Arc::from("scope"),
        serde_json::to_value(scope).unwrap_or(Value::Null),
    );
}

fn fallback_caller() -> MemoryCaller {
    MemoryCaller {
        agent_id: AgentId::new("memory-tool"),
        task_id: TaskId::new("memory-tool"),
        conversation_id: ConversationId::new("memory-tool"),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    }
}

fn memory_tool_error(err: impl std::error::Error) -> ToolError {
    ToolError::Failed(Arc::from(err.to_string()))
}

fn record_summary(body: &Value) -> String {
    if let Some(text) = body.as_str() {
        return text.to_owned();
    }
    if let Some(fact) = body.get("fact").and_then(Value::as_str) {
        return fact.to_owned();
    }
    body.to_string()
}

fn metadata(start: Instant, bytes_out: u64) -> BTreeMap<Arc<str>, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from("duration_ms"),
        Value::from(start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
    );
    metadata.insert(Arc::from("bytes_out"), Value::from(bytes_out));
    metadata
}
