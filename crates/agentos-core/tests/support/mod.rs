//! Shared harness for the golden-transcript suite (`tests/transcripts.rs`).
//!
//! Roadmap item T1 in [`docs/TRANSFER_ROADMAP.md`]. The suite exists because
//! unit tests cover each stage of a run in isolation but nothing asserts what a
//! complete run actually *sends to a provider*. [`ScriptedLlm`] closes that gap:
//! it answers from a fixed script and records every request it received, so a
//! golden can pin the assembled messages, the tool schemas offered alongside
//! them, the resulting session items, and the run outcome as one artifact.
//!
//! Goldens live in `tests/golden/*.json` and are compared byte-for-byte against
//! pretty-printed JSON. Re-record after reviewing a diff:
//!
//! ```sh
//! AGENTOS_GOLDEN=record cargo test -p agentos-core --test transcripts
//! ```

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{RunOutcome, RunnerDeps};
use agentos_core::skills::WorkspaceSkillCatalog;
use agentos_core::subagents::SubAgentRegistry;
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, RunContext};
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::Transcript;
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{Llm, LlmError};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, ToolCall, ToolCallId,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// ScriptedLlm
// ---------------------------------------------------------------------------

/// One provider request as the orchestrator assembled it.
///
/// `tools` holds only tool *names*: the JSON schemas are large, stable, and
/// owned by each tool's own unit tests, so pinning them here would make every
/// golden churn on an unrelated description edit. Which tools were offered is
/// the fact this suite is asserting.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<Arc<str>>,
}

/// An [`Llm`] that answers from a fixed script and records every request.
///
/// Responses are consumed in order. Exhausting the script is a test authoring
/// error, so it surfaces as an [`LlmError`] naming the request index rather
/// than a silent default reply that a golden would then enshrine.
pub struct ScriptedLlm {
    responses: Mutex<VecDeque<Message>>,
    requests: Mutex<Vec<RecordedRequest>>,
}

/// The context window every scripted run is measured against. Deliberately
/// small so a golden's `pressure_percent` is a legible number rather than a
/// rounding artifact of a 128k window.
pub const SCRIPTED_CONTEXT_BUDGET: usize = 4_096;

impl ScriptedLlm {
    pub fn new(responses: impl IntoIterator<Item = Message>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Every request received so far, in call order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .expect("scripted llm request log is never poisoned: no panics while held")
            .clone()
    }

    fn answer(&self, messages: Vec<Message>, tools: &[ToolSpec]) -> Result<Message, LlmError> {
        let mut requests = self
            .requests
            .lock()
            .expect("scripted llm request log is never poisoned: no panics while held");
        requests.push(RecordedRequest {
            messages,
            tools: tools.iter().map(|spec| Arc::clone(&spec.name)).collect(),
        });
        let index = requests.len();
        drop(requests);

        self.responses
            .lock()
            .expect("scripted llm response queue is never poisoned: no panics while held")
            .pop_front()
            .ok_or_else(|| {
                LlmError::Provider(Arc::from(format!(
                    "scripted llm: script exhausted at request {index}; extend the script"
                )))
            })
    }
}

#[async_trait]
impl Llm for ScriptedLlm {
    fn describe(&self) -> String {
        "llm provider=scripted".to_owned()
    }

    fn context_budget_tokens(&self) -> Option<usize> {
        Some(SCRIPTED_CONTEXT_BUDGET)
    }

    async fn complete(&self, ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        let messages = ctx
            .state
            .transcript
            .items
            .iter()
            .map(|item| item.message.clone())
            .collect();
        self.answer(messages, &[])
    }

    async fn complete_messages(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        self.answer(messages.to_vec(), tools)
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

pub const CHANNEL: &str = "golden-channel";
pub const CONVERSATION: &str = "golden-conversation";

pub fn user_envelope(text: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new(CHANNEL),
        conversation_id: ConversationId::new(CONVERSATION),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// An assistant reply carrying one tool call, as a provider would return it.
pub fn tool_call_response(call_id: &str, tool: &str, args: &str) -> Message {
    Message {
        role: MessageRole::Assistant,
        content: Arc::from(""),
        attachments: Vec::new(),
        tool_calls: vec![ToolCall {
            id: ToolCallId::new(call_id),
            name: Arc::from(tool),
            args: RawValue::from_string(args.to_owned()).expect("scripted args are valid JSON"),
        }],
        tool_call_id: None,
        metadata: BTreeMap::new(),
    }
}

pub fn assistant(text: &str) -> Message {
    Message::text(MessageRole::Assistant, text)
}

/// A policy granting `verb` on every listed tool, denying everything else.
pub fn tool_policy(tools: &[&str], verb: PolicyVerb) -> Policy {
    Policy {
        rules: tools
            .iter()
            .map(|name| PolicyRule {
                action: PolicyAction::Tool(Arc::from(*name)),
                decision: verb.clone(),
                reason: None,
                arg_equals: BTreeMap::new(),
            })
            .collect(),
        default_decision: PolicyVerb::Deny,
    }
}

/// A directory removed when the guard drops, so a failing test cannot leave
/// a skill tree behind in the system temp dir.
pub struct TempTree(PathBuf);

impl Drop for TempTree {
    fn drop(&mut self) {
        // Best effort: a cleanup failure must not mask the test's own result.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Write a one-skill workspace tree and load it as a catalog.
///
/// The catalog is the *other* thing that reaches a request today (alongside the
/// transcript), so a golden suite that never populates one cannot detect the
/// prelude going missing.
pub fn skill_catalog(
    label: &str,
    name: &str,
    description: &str,
    instructions: &str,
) -> (WorkspaceSkillCatalog, TempTree) {
    let root = std::env::temp_dir().join(format!(
        "agentos-golden-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let skill_dir = root.join(name);
    std::fs::create_dir_all(&skill_dir).expect("skill directory is creatable");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n\n{instructions}\n"),
    )
    .expect("SKILL.md is writable");

    let catalog = WorkspaceSkillCatalog::load_enabled(&root, &[Arc::from(name)])
        .expect("the written skill tree is valid");
    (catalog, TempTree(root))
}

pub fn runner_deps<'a>(
    orchestrator: &'a dyn Orchestrator,
    session: &'a InMemorySession,
    policy: &'a Policy,
    tools: Option<&'a ToolRegistry>,
    subagents: Option<&'a SubAgentRegistry>,
) -> RunnerDeps<'a> {
    RunnerDeps {
        orchestrator,
        session,
        memory_manager: None,
        hooks: None,
        max_turns: 8,
        active_agent: AgentId::new("golden-agent"),
        tools,
        trace_sink: None,
        task_workspace: None,
        policy,
        subagents,
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: &[],
        stream_sink: None,
    }
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

/// Project a message down to the fields a golden should pin.
///
/// Metadata is dropped wholesale: it carries wall-clock durations, byte counts,
/// and token usage, none of which are reproducible across runs. Everything that
/// decides what the *model* sees — role, content, tool-call pairing — is kept.
pub fn normalize_message(message: &Message) -> Value {
    let mut out = json!({
        "role": message.role,
        "content": message.content.as_ref(),
    });
    if !message.tool_calls.is_empty() {
        out["tool_calls"] = Value::Array(
            message
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id.as_str(),
                        "name": call.name.as_ref(),
                        "args": call.args.get(),
                    })
                })
                .collect(),
        );
    }
    if let Some(id) = &message.tool_call_id {
        out["tool_call_id"] = Value::String(id.as_str().to_owned());
    }
    out
}

pub fn normalize_requests(requests: &[RecordedRequest]) -> Value {
    Value::Array(
        requests
            .iter()
            .map(|request| {
                json!({
                    "messages": request.messages.iter().map(normalize_message).collect::<Vec<_>>(),
                    "tools": request.tools.iter().map(|name| name.as_ref()).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

pub fn normalize_transcript(transcript: &Transcript) -> Value {
    Value::Array(
        transcript
            .items
            .iter()
            .map(|item| normalize_message(&item.message))
            .collect(),
    )
}

/// Render a run outcome. Approval ids are derived from the action they gate
/// (`approval-tool-<call id>`), so they are stable across runs and worth
/// pinning — a golden that changed one would be reporting a real regression in
/// approval correlation.
pub fn normalize_outcome(outcome: &RunOutcome) -> Value {
    match outcome {
        RunOutcome::Finished { output, .. } => json!({
            "kind": "finished",
            "reply": output.message.content.as_ref(),
            "conversation_id": output.conversation_id.as_str(),
        }),
        RunOutcome::Paused(state) => json!({
            "kind": "paused",
            "pending_approvals": normalize_approvals(state),
        }),
    }
}

pub fn normalize_approvals(state: &RunState) -> Value {
    Value::Array(
        state
            .pending_approvals
            .iter()
            .map(|approval| {
                json!({
                    "id": approval.id.as_str(),
                    "status": approval.status,
                    "action": serde_json::to_value(&approval.action)
                        .expect("interruption actions serialize"),
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Golden files
// ---------------------------------------------------------------------------

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("{name}.json"))
}

fn recording() -> bool {
    std::env::var("AGENTOS_GOLDEN").as_deref() == Ok("record")
}

/// Compare `actual` against the stored golden, or rewrite it when recording.
pub fn assert_golden(name: &str, actual: &Value) {
    let path = golden_path(name);
    let mut rendered =
        serde_json::to_string_pretty(actual).expect("golden values are plain JSON trees");
    rendered.push('\n');

    if recording() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("golden directory is creatable");
        }
        std::fs::write(&path, &rendered).expect("golden file is writable");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "missing golden {}: {err}\nrecord it with: AGENTOS_GOLDEN=record cargo test -p \
             agentos-core --test transcripts",
            path.display()
        )
    });

    if expected != rendered {
        panic!(
            "golden mismatch for {name}\n\n--- expected ({}) ---\n{expected}\n--- actual ---\n\
             {rendered}\nReview the diff, then re-record with: AGENTOS_GOLDEN=record cargo test \
             -p agentos-core --test transcripts",
            path.display()
        );
    }
}

/// The `request_header` trace events a finished run recorded — the durable
/// answer to "what was this request made of", pinned alongside the request
/// itself so a golden proves the two agree.
pub fn normalize_request_headers(state: &RunState) -> Value {
    Value::Array(
        state
            .trace_events
            .iter()
            .filter(|event| event.name.as_ref() == "request_header")
            .map(|event| {
                Value::Object(
                    event
                        .fields
                        .iter()
                        .map(|(key, value)| (key.as_ref().to_owned(), value.clone()))
                        .collect(),
                )
            })
            .collect(),
    )
}

/// Assemble the standard golden document for one scenario.
pub fn scenario(llm: &ScriptedLlm, transcript: &Transcript, outcome: &RunOutcome) -> Value {
    let mut document = json!({
        "requests": normalize_requests(&llm.requests()),
        "session_items": normalize_transcript(transcript),
        "outcome": normalize_outcome(outcome),
    });
    if let RunOutcome::Finished { state, .. } = outcome {
        document["request_headers"] = normalize_request_headers(state);
    }
    document
}
