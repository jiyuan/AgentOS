//! Every provider call records what it was made of, and the classifier's
//! isolation survives that.
//!
//! M5 / `REQ-001`, verifying `docs/adr/0004-REQUEST_KINDS.md`. Two production
//! calls used to leave no record: the routing classifier and the compaction
//! summarizer. Both had a reason — the classifier is deliberately not
//! assembled from the transcript, and the summarizer has no `RunContext` to
//! record onto — and neither reason justified being unrecordable.
//!
//! The test that matters most here is
//! `the_routing_classifier_sees_no_memory_and_no_skill_prelude`. It is the
//! regression guard on a prompt-injection defence: if a later refactor
//! "unifies" the classifier into full assembly, a poisoned memory record could
//! choose which orchestrator handles the next turn, and this is what must fail
//! before that ships.

mod support;

use agentos_core::approve::Policy;
use agentos_core::config::CompactionConfig;
use agentos_core::memory::InMemorySession;
use agentos_core::orchestrator::MaxOrchestrator;
use agentos_core::prompt::Compaction;
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_interfaces::orchestrator::{
    DispatchTarget, MemoryFragment, Orchestrator, RoutingRule, RoutingTable, RunContext,
    SubAgentSpec, TaskDomain,
};
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::Item;
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{Llm, LlmError};
use agentos_proto::{
    AgentId, Message, MessageRole, Namespace, RunId, Usage, TOKEN_USAGE_METADATA_KEY,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use support::{runner_deps, user_envelope};

const WINDOW: usize = 2_000;

/// Attach a provider's per-call token accounting to a reply, the way a real
/// adapter does.
fn with_usage(mut message: Message, input: u64, output: u64) -> Message {
    let usage = Usage {
        input_tokens: input,
        output_tokens: output,
        total_tokens: input + output,
        ..Default::default()
    };
    message.metadata.insert(
        Arc::from(TOKEN_USAGE_METADATA_KEY),
        serde_json::to_value(usage).expect("usage serializes"),
    );
    message
}

/// Records every request it is asked and answers routing questions with a
/// fixed decision.
struct RecordingLlm {
    requests: Mutex<Vec<Vec<Message>>>,
}

impl RecordingLlm {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<Vec<Message>> {
        self.requests.lock().expect("not poisoned").clone()
    }
}

#[async_trait]
impl Llm for RecordingLlm {
    fn describe(&self) -> String {
        "llm provider=recording".to_owned()
    }

    fn context_budget_tokens(&self) -> Option<usize> {
        Some(WINDOW)
    }

    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        Err(LlmError::Unconfigured(Arc::from("complete")))
    }

    async fn complete_messages(
        &self,
        messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        let is_routing = messages
            .first()
            .is_some_and(|message| message.content.contains("Classify the user's task"));
        self.requests
            .lock()
            .expect("not poisoned")
            .push(messages.to_vec());
        let reply = if is_routing {
            // Low confidence, so routing falls through to the parent's own
            // planning turn and this run makes exactly two provider calls.
            Message::text(
                MessageRole::Assistant,
                r#"{"domain":"general","confidence":0.1}"#,
            )
        } else {
            Message::text(MessageRole::Assistant, "an ordinary answer")
        };
        Ok(with_usage(reply, 11, 7))
    }
}

fn routing_table() -> RoutingTable {
    let delegate = DispatchTarget::Delegate(SubAgentSpec {
        agent_id: AgentId::new("research-subagent"),
        policy_id: Arc::from("research"),
        metadata: BTreeMap::new(),
    });
    RoutingTable {
        rules: vec![RoutingRule {
            domain: TaskDomain::Research,
            description: Arc::from("bounded research over configured sources"),
            examples: Vec::new(),
            dispatch: delegate.clone(),
        }],
        fallback: RoutingRule {
            domain: TaskDomain::General,
            description: Arc::from("everything else"),
            examples: Vec::new(),
            dispatch: DispatchTarget::Direct,
        },
    }
}

/// A state carrying one user turn plus a recalled memory fragment, so a
/// classifier that reached either would be visibly wrong.
fn state_with_memory() -> (RunState, MemoryFragment) {
    let mut state = RunState::new(RunId::new("req-kinds"), AgentId::new("default"));
    state.transcript.items.push(Item {
        message: Message::text(MessageRole::User, "compare these two research sources"),
        metadata: BTreeMap::new(),
    });
    let fragment = MemoryFragment {
        id: None,
        namespace: Namespace::new("private/conversation/c/semantic/general"),
        body: json!({ "fact": "POISONED-MEMORY-MARKER" }),
        metadata: BTreeMap::new(),
    };
    (state, fragment)
}

/// The ADR-0004 regression guard, stated twice over: the classifier's *header*
/// names only its own two sections, and the classifier's *messages* contain
/// none of the recalled fact.
#[tokio::test]
async fn the_routing_classifier_sees_no_memory_and_no_skill_prelude() {
    let llm = Arc::new(RecordingLlm::new());
    let orchestrator = MaxOrchestrator::new()
        .with_routing_table(routing_table())
        .with_llm(llm.clone());

    let (state, fragment) = state_with_memory();
    let mut ctx = RunContext::from_state(&state);
    ctx.memory_fragments.push(fragment);
    orchestrator.plan(&ctx).await.expect("planning succeeds");

    let headers = std::mem::take(&mut *ctx.request_sink.lock().expect("not poisoned"));
    let routing: Vec<_> = headers
        .iter()
        .filter(|header| header.kind.as_ref() == "routing")
        .collect();
    let [routing] = routing.as_slice() else {
        panic!("exactly one routing header, got {headers:?}");
    };

    let ids: Vec<&str> = routing
        .sections
        .iter()
        .map(|section| section.id.as_ref())
        .collect();
    assert_eq!(ids, vec!["routing_instruction", "routing_input"]);
    for forbidden in ["memory", "skill_prelude", "transcript"] {
        assert!(
            !ids.contains(&forbidden),
            "the classifier must not carry `{forbidden}`: {ids:?}"
        );
    }

    // And the messages themselves, in case a section were ever mislabelled.
    let classifier = llm
        .requests()
        .into_iter()
        .find(|messages| {
            messages
                .first()
                .is_some_and(|m| m.content.contains("Classify the user's task"))
        })
        .expect("the classifier ran");
    let rendered: String = classifier
        .iter()
        .map(|message| message.content.as_ref())
        .collect();
    assert!(
        !rendered.contains("POISONED-MEMORY-MARKER"),
        "a recalled fact reached the routing decision"
    );
}

/// The classifier still leaves a record, which is the half that was missing.
#[tokio::test]
async fn every_provider_call_in_a_turn_records_exactly_one_manifest() {
    let llm = Arc::new(RecordingLlm::new());
    let orchestrator = MaxOrchestrator::new()
        .with_routing_table(routing_table())
        .with_llm(llm.clone());

    let (state, fragment) = state_with_memory();
    let mut ctx = RunContext::from_state(&state);
    ctx.memory_fragments.push(fragment);
    orchestrator.plan(&ctx).await.expect("planning succeeds");

    let headers = std::mem::take(&mut *ctx.request_sink.lock().expect("not poisoned"));
    let calls = llm.requests().len();
    assert_eq!(
        headers.len(),
        calls,
        "one manifest per provider call: {calls} calls, {} headers",
        headers.len()
    );
    let kinds: Vec<&str> = headers.iter().map(|header| header.kind.as_ref()).collect();
    assert!(kinds.contains(&"routing"), "{kinds:?}");
    assert!(kinds.contains(&"turn"), "{kinds:?}");

    // Every header describes the request it names, rather than being an empty
    // placeholder that satisfies the count.
    for header in &headers {
        assert!(header.total_messages > 0, "{header:?}");
        assert!(header.total_chars > 0, "{header:?}");
    }

    // And the usage sink saw one entry per call too.
    let usage = std::mem::take(&mut *ctx.usage_sink.lock().expect("not poisoned"));
    assert_eq!(usage.len(), calls);
}

/// A summarizer that answers with a fixed summary and reports its cost.
struct CountingSummarizer {
    calls: AtomicUsize,
}

#[async_trait]
impl Llm for CountingSummarizer {
    fn describe(&self) -> String {
        "llm provider=counting-summarizer".to_owned()
    }

    fn context_budget_tokens(&self) -> Option<usize> {
        Some(WINDOW)
    }

    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        Err(LlmError::Unconfigured(Arc::from("complete")))
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(with_usage(
            Message::text(MessageRole::Assistant, "Earlier turns are summarized here."),
            SUMMARIZER_INPUT_TOKENS,
            SUMMARIZER_OUTPUT_TOKENS,
        ))
    }
}

/// Deliberately distinctive, so a run total can be shown to contain *these*
/// tokens rather than merely to be non-zero.
const SUMMARIZER_INPUT_TOKENS: u64 = 977;
const SUMMARIZER_OUTPUT_TOKENS: u64 = 31;

fn long_reply(turn: usize) -> Message {
    with_usage(
        Message::text(
            MessageRole::Assistant,
            format!("Answer {turn}. {}", "detail ".repeat(40)),
        ),
        5,
        5,
    )
}

/// The accounting defect: compaction spent tokens and reported none, so every
/// run total that involved a summarization was wrong. It also left no header,
/// so a reader could not tell what the summarizer had been shown.
#[tokio::test]
async fn a_summarization_reaches_the_run_totals_and_the_trace() {
    const TURNS: usize = 60;

    let llm =
        Arc::new(support::ScriptedLlm::new((0..TURNS).map(long_reply)).with_context_budget(WINDOW));
    let summarizer = CountingSummarizer {
        calls: AtomicUsize::new(0),
    };
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        config: CompactionConfig::default(),
    };

    let mut compacted_state = None;
    for turn in 0..TURNS {
        let outcome = run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("req-compact-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("turn {turn} failed: {error}"));
        let RunOutcome::Finished { state, .. } = outcome else {
            panic!("turn {turn} paused unexpectedly");
        };
        let summarized = state
            .trace_events
            .iter()
            .any(|event| event.name.as_ref() == "conversation_compacted");
        if summarized {
            compacted_state = Some(state);
            break;
        }
    }

    let state = compacted_state.expect("60 turns in a 2 000-token window must have compacted");
    assert!(summarizer.calls.load(Ordering::Relaxed) > 0);

    // The header: a `compaction` request, with the summarizer's own two
    // sections and none of the turn's.
    let kinds: Vec<String> = state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref() == "request_header")
        .filter_map(|event| event.fields.get("kind"))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    assert!(
        kinds.iter().any(|kind| kind == "compaction"),
        "the summarization left no header: {kinds:?}"
    );

    let sections: Vec<String> = state
        .trace_events
        .iter()
        .filter(|event| event.name.as_ref() == "request_header")
        .filter(|event| event.fields.get("kind").and_then(Value::as_str) == Some("compaction"))
        .filter_map(|event| event.fields.get("sections"))
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(|section| section.get("id"))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    assert_eq!(sections, vec!["summary_instruction", "summary_span"]);

    // The tokens: the run total contains the summarizer's, not just the
    // planner's. Checked by the distinctive figure rather than by "> 0", so a
    // total that happened to be non-zero for other reasons cannot pass.
    assert!(
        state.usage.input_tokens >= SUMMARIZER_INPUT_TOKENS,
        "run input tokens {} do not include the summarizer's {SUMMARIZER_INPUT_TOKENS}",
        state.usage.input_tokens
    );
    assert!(
        state.usage.output_tokens >= SUMMARIZER_OUTPUT_TOKENS,
        "run output tokens {} do not include the summarizer's {SUMMARIZER_OUTPUT_TOKENS}",
        state.usage.output_tokens
    );
}
