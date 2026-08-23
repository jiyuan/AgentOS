//! M7 / `SPILL-001`: reading spilled output back through the normal loop.
//!
//! The unit tests in `spill/mod.rs` cover what the store writes. These cover
//! what a *run* can get at, which is the part the audit was about: the locator
//! that reaches the model is not a path, retrieval goes through the registry
//! and the policy engine like any other call, and a locator the conversation
//! was never given resolves to nothing.

mod support;

use agentos_core::approve::Policy;
use agentos_core::memory::InMemorySession;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_core::spill::{ContentLimits, SpillStore, SPILL_LOCATOR_KEY};
use agentos_core::tools::{SpillReadTool, ToolRegistry};
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::Session;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId, ToolCall,
    ToolCallId, ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const BULK: &str = "bulk";
const CONVERSATION: &str = "spill-conversation";
/// Long enough to spill past the tiny cap these tests set, and distinctive
/// enough that a partial read is obvious.
fn bulk_output() -> String {
    (0..200)
        .map(|line| format!("line {line:04}: the quick brown fox\n"))
        .collect()
}

/// Produces more output than the inline cap allows.
struct BulkTool;

#[async_trait]
impl Tool for BulkTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(BULK),
            description: Arc::from("emits a lot"),
            input_schema: json!({"type": "object"}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(bulk_output()),
            metadata: BTreeMap::new(),
        })
    }
}

/// Calls `bulk`, then calls `spill_read` with whatever locator the recorded
/// result carries — or with a fixed one, when the scenario is about forgery.
struct Scripted {
    /// `None` means "use the locator the transcript recorded".
    forged: Option<String>,
    turn: AtomicUsize,
    /// What `spill_read` answered, so the test can assert on it.
    retrieved: Arc<Mutex<Option<String>>>,
}

impl Scripted {
    fn new(forged: Option<&str>) -> Self {
        Self {
            forged: forged.map(str::to_owned),
            turn: AtomicUsize::new(0),
            retrieved: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Orchestrator for Scripted {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        match self.turn.fetch_add(1, Ordering::Relaxed) {
            0 => Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("call-bulk"),
                name: Arc::from(BULK),
                args: RawValue::from_string("{}".to_owned()).expect("valid JSON"),
            })),
            1 => {
                let locator = self.forged.clone().unwrap_or_else(|| {
                    ctx.state
                        .transcript
                        .items
                        .iter()
                        .find_map(|item| item.message.metadata.get(SPILL_LOCATOR_KEY))
                        .and_then(Value::as_str)
                        .expect("the spilled result recorded a locator")
                        .to_owned()
                });
                Ok(Plan::CallTool(ToolCall {
                    id: ToolCallId::new("call-read"),
                    name: Arc::from("spill_read"),
                    args: RawValue::from_string(json!({ "locator": locator }).to_string())
                        .expect("valid JSON"),
                }))
            }
            _ => {
                // Capture what came back before answering.
                let answer = ctx
                    .state
                    .transcript
                    .items
                    .iter()
                    .rev()
                    .find(|item| {
                        item.message
                            .tool_call_id
                            .as_ref()
                            .is_some_and(|id| id.as_str() == "call-read")
                    })
                    .map(|item| item.message.content.as_ref().to_owned());
                if let Ok(mut guard) = self.retrieved.lock() {
                    *guard = answer;
                }
                Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
            }
        }
    }
}

/// The principal a run on [`envelope`] keys its session on. `golden-agent` is
/// what `support::runner_deps` runs as; this test's own channel and
/// conversation are otherwise unrelated to the golden ones.
fn principal() -> Principal {
    Principal::conversation(
        AgentId::new("golden-agent"),
        ChannelId::new("spill"),
        ConversationId::new(CONVERSATION),
    )
    .with_sender("user")
}

fn envelope() -> Envelope {
    Envelope {
        channel_id: ChannelId::new("spill"),
        conversation_id: ConversationId::new(CONVERSATION),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, "dump it"),
        metadata: BTreeMap::new(),
    }
}

/// Drive one run with a tiny inline cap so `bulk` spills, and a `spill_read`
/// bound to the same store.
async fn run_with(
    forged: Option<&str>,
    store: &SpillStore,
    run: &str,
) -> (Arc<Mutex<Option<String>>>, String) {
    let orchestrator = Scripted::new(forged);
    let retrieved = Arc::clone(&orchestrator.retrieved);
    let mut tools = ToolRegistry::new();
    tools.register(BulkTool);
    tools.register(SpillReadTool::new(Arc::new(store.clone())));
    let session = InMemorySession::default();
    let policy = Policy::allow_tools([BULK, "spill_read"]);
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.content_limits = ContentLimits {
        tool_result_inline_bytes: 256,
        spill: Some(store),
    };

    // A distinct run id per call: the store writes with `O_EXCL`, so two runs
    // sharing an id and a call id would collide and the second would silently
    // not spill at all.
    let outcome = run_envelope(envelope(), RunId::new(run), &deps)
        .await
        .expect("the run finishes");
    assert!(matches!(outcome, RunOutcome::Finished { .. }));

    let transcript = session.load(&principal()).await.expect("the session loads");
    let locator = transcript
        .items
        .iter()
        .find_map(|item| item.message.metadata.get(SPILL_LOCATOR_KEY))
        .and_then(Value::as_str)
        .expect("the spilled result recorded a locator")
        .to_owned();
    (retrieved, locator)
}

#[tokio::test]
async fn a_large_result_is_recovered_through_the_loop_with_an_opaque_locator() {
    // Also the "spill storage outside the workspace" case: the store is under
    // the system temp directory, which the `file` tool could never have read.
    let tree = support::temp_tree("spill-recover");
    let store = SpillStore::new(tree.path().join("artifacts"));

    let (retrieved, locator) = run_with(None, &store, "recover").await;

    assert!(
        locator.starts_with("spill:") && !locator.contains(&*tree.path().to_string_lossy()),
        "the locator must not be a host path: {locator}"
    );
    let answer = retrieved
        .lock()
        .expect("the recorder is not poisoned")
        .clone()
        .expect("the second turn saw the spill_read result");
    // `spill_read`'s own answer is a tool result like any other, so this run's
    // 256-byte inline cap truncates and re-spills it — which is the right
    // behaviour and worth pinning rather than tuning around. What matters is
    // that the bytes came from the artifact and that the model is told how
    // much more there is.
    let expected = bulk_output();
    assert!(
        expected.starts_with(&answer[..200]),
        "spill_read must return the artifact's own bytes, got: {}",
        &answer[..answer.len().min(200)]
    );
    assert!(
        answer.contains(&format!("{} bytes", expected.len())),
        "and must report the artifact's real size, got: {answer}"
    );
}

#[tokio::test]
async fn a_locator_this_conversation_was_never_given_reads_nothing() {
    // The forgery case. The artifact genuinely exists — another run wrote it —
    // so this is not "no such file"; it is the authorization check.
    let tree = support::temp_tree("spill-forge");
    let store = SpillStore::new(tree.path().join("artifacts"));
    let other = store
        .save_text(
            &agentos_core::spill::SpillSource {
                run_id: &RunId::new("someone-elses-run"),
                tool_name: "shell",
                call_id: &ToolCallId::new("call-1"),
            },
            "a secret another conversation produced",
        )
        .await
        .expect("the other run's artifact is written");

    let (retrieved, _) = run_with(Some(other.locator.as_str()), &store, "forge").await;

    let answer = retrieved
        .lock()
        .expect("the recorder is not poisoned")
        .clone()
        .expect("the second turn saw the spill_read result");
    assert!(
        !answer.contains("a secret another conversation produced"),
        "a locator the transcript never cited must not resolve: {answer}"
    );
    assert!(
        answer.contains("no such spilled output"),
        "and the refusal must be readable: {answer}"
    );
}

#[tokio::test]
async fn a_locator_shaped_like_a_path_reads_nothing() {
    let tree = support::temp_tree("spill-path");
    let store = SpillStore::new(tree.path().join("artifacts"));

    // Three ways to spell "somewhere else", all refused before the store is
    // touched: an absolute path, a traversal, and a locator whose run segment
    // tries to climb out.
    for (index, forged) in [
        "/etc/passwd",
        "spill:../../../../etc/passwd",
        "spill:../etc/passwd",
    ]
    .into_iter()
    .enumerate()
    {
        let (retrieved, _) = run_with(Some(forged), &store, &format!("path-{index}")).await;
        let answer = retrieved
            .lock()
            .expect("the recorder is not poisoned")
            .clone()
            .expect("the second turn saw the spill_read result");
        assert!(
            answer.contains("not a spill locator"),
            "'{forged}' must be refused as malformed, got: {answer}"
        );
    }
}
