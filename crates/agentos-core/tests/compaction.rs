//! Roadmap C3: a conversation that outgrows its window compacts itself.
//!
//! The unit tests in `prompt::compact` cover span selection and the pairing
//! rule. These cover the two properties that only show up in a whole run:
//! a long conversation stays inside the provider's window, and the session log
//! still holds everything it ever held.
//!
//! Deliberately not goldens. Pinning a 200-turn replay would mean a megabyte of
//! filler in `tests/golden/`, and the assertion worth making is a property of
//! every request rather than the exact bytes of one.

mod support;

use agentos_core::approve::Policy;
use agentos_core::config::CompactionConfig;
use agentos_core::memory::InMemorySession;
use agentos_core::orchestrator::MaxOrchestrator;
use agentos_core::prompt::{is_checkpoint, Compaction};
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::session::Session;
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{Llm, LlmError};
use agentos_proto::{Message, MessageRole, RunId};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use support::{assistant, runner_deps, user_envelope, ScriptedLlm};

/// The window every scenario here is measured against. Small enough that a few
/// dozen ordinary turns reach it.
const WINDOW: usize = 2_000;

/// A summarizer that answers any request with a fixed summary and counts how
/// often it was asked.
///
/// Separate from the conversation's [`ScriptedLlm`] on purpose: a shared
/// script would make the test depend on the exact interleaving of planning and
/// summarization calls, which is the thing under test.
struct StubSummarizer {
    calls: AtomicUsize,
}

impl StubSummarizer {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Llm for StubSummarizer {
    fn describe(&self) -> String {
        "llm provider=stub-summarizer".to_owned()
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
        Ok(Message::text(
            MessageRole::Assistant,
            "Earlier turns covered topics 0 onward; nothing is outstanding.",
        ))
    }
}

/// A provider that rejects its first `rejections` planning calls for length,
/// exactly as OpenAI does, then answers.
struct OverflowingLlm {
    rejections: usize,
    calls: AtomicUsize,
}

impl OverflowingLlm {
    fn new(rejections: usize) -> Self {
        Self {
            rejections,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Llm for OverflowingLlm {
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
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        // Only reject once the conversation is long enough to be worth
        // compacting, so the seeding turns above complete normally.
        if call >= 40 && call - 40 < self.rejections {
            return Err(LlmError::ContextLengthExceeded(Arc::from(
                "openai error: this model's maximum context length is 8192 tokens",
            )));
        }
        Ok(Message::text(MessageRole::Assistant, "recovered"))
    }
}

/// A summarizer that always fails, to prove compaction degrades rather than
/// taking the run down with it.
struct FailingSummarizer;

#[async_trait]
impl Llm for FailingSummarizer {
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
        Err(LlmError::Provider(Arc::from("summarizer unavailable")))
    }
}

/// A reply long enough that a few dozen turns fill a 2 000-token window.
fn long_reply(turn: usize) -> Message {
    assistant(&format!(
        "Answer {turn}: {}",
        "the deploy key rotates every ninety days and the runbook lives in docs. ".repeat(6)
    ))
}

/// Every `request_header` the runs recorded, as (estimated tokens, window).
fn request_sizes(states: &[agentos_interfaces::run_state::RunState]) -> Vec<(usize, usize)> {
    states
        .iter()
        .flat_map(|state| state.trace_events.iter())
        .filter(|event| event.name.as_ref() == "request_header")
        .filter_map(|event| {
            let tokens = event
                .fields
                .get("prompt_estimated_tokens")
                .and_then(Value::as_u64)?;
            let window = event
                .fields
                .get("context_budget_tokens")
                .and_then(Value::as_u64)?;
            Some((tokens as usize, window as usize))
        })
        .collect()
}

/// The C3 exit condition: a conversation runs far past its window without a
/// request ever exceeding it, and the log keeps every original item.
#[tokio::test]
async fn a_long_conversation_stays_within_the_window_and_keeps_its_log() {
    const TURNS: usize = 200;

    let llm = Arc::new(ScriptedLlm::new((0..TURNS).map(long_reply)).with_context_budget(WINDOW));
    let summarizer = StubSummarizer::new();
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        config: CompactionConfig::default(),
    };

    let mut states = Vec::with_capacity(TURNS);
    for turn in 0..TURNS {
        let outcome = run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("c3-long-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("turn {turn} failed: {error}"));
        let RunOutcome::Finished { state, .. } = outcome else {
            panic!("turn {turn} paused unexpectedly");
        };
        states.push(state);
    }

    // Every assembled request fit the provider's window.
    let sizes = request_sizes(&states);
    assert_eq!(sizes.len(), TURNS, "one request per turn");
    let over: Vec<_> = sizes
        .iter()
        .enumerate()
        .filter(|(_, (tokens, window))| tokens > window)
        .collect();
    assert!(over.is_empty(), "requests exceeded the window: {over:?}");
    // Guard against the assertion above going vacuous: the run must actually
    // have approached the window rather than merely stayed small. Measured at
    // 27 compactions and a peak of 1 790 of 2 000 tokens — just under the 90%
    // trigger, which is where the design puts it.
    let peak = sizes.iter().map(|(tokens, _)| *tokens).max().unwrap_or(0);
    assert!(
        peak > WINDOW / 2,
        "the conversation never grew: peak {peak}"
    );

    // Compaction is what kept them there.
    assert!(
        summarizer.calls() > 0,
        "200 turns in a 2 000-token window must have compacted at least once"
    );

    // Append-only: the log still holds every user turn and every reply, plus
    // the checkpoints. Nothing was rewritten or dropped.
    let transcript = session
        .load(&support::user_session_key())
        .await
        .expect("session loads");
    let originals = transcript
        .items
        .iter()
        .filter(|item| !is_checkpoint(item))
        .count();
    assert_eq!(
        originals,
        TURNS * 2,
        "the log must retain every question and answer"
    );
    let checkpoints = transcript.items.len() - originals;
    assert_eq!(
        checkpoints,
        summarizer.calls(),
        "one checkpoint per summarization"
    );

    // And the model was not answering from a summary alone.
    let visible = agentos_core::prompt::visible(&transcript);
    assert!(visible.len() >= CompactionConfig::default().retain_tail_turns);
    assert!(visible.len() < transcript.items.len(), "nothing was folded");
}

/// A checkpoint written on one run is honoured by the next, which is what makes
/// compaction survive the gateway's one-run-per-message shape.
#[tokio::test]
async fn a_checkpoint_written_on_one_run_still_hides_its_span_on_the_next() {
    const TURNS: usize = 60;

    let llm = Arc::new(ScriptedLlm::new((0..TURNS).map(long_reply)).with_context_budget(WINDOW));
    let summarizer = StubSummarizer::new();
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        config: CompactionConfig::default(),
    };

    for turn in 0..TURNS {
        run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("c3-resume-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("turn {turn} failed: {error}"));
    }
    assert!(
        summarizer.calls() > 0,
        "the conversation must have compacted"
    );

    // The last request the model saw carries the summary and not the turns it
    // replaced — proving positions recorded on an earlier run still resolve
    // against the transcript this one reloaded from the session.
    let requests = llm.requests();
    let last = requests.last().expect("the model was called");
    assert!(
        last.messages.iter().any(|message| message
            .content
            .contains("Summary of the earlier conversation")),
        "the final request should carry a checkpoint"
    );
    assert!(
        !last
            .messages
            .iter()
            .any(|message| message.content.contains("Question 0:")),
        "the first turn should be folded out of the final request"
    );
}

/// A summarizer failure is not a run failure.
#[tokio::test]
async fn a_failing_summarizer_leaves_the_run_uncompacted_rather_than_broken() {
    const TURNS: usize = 40;

    let llm = Arc::new(ScriptedLlm::new((0..TURNS).map(long_reply)).with_context_budget(WINDOW));
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let failing = FailingSummarizer;
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&failing),
        config: CompactionConfig::default(),
    };

    for turn in 0..TURNS {
        run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("c3-fail-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("turn {turn} must survive a failed summarizer: {error}"));
    }

    // No checkpoint was written, and the conversation is intact.
    let transcript = session
        .load(&support::user_session_key())
        .await
        .expect("session loads");
    assert!(!transcript.items.iter().any(is_checkpoint));
    assert_eq!(transcript.items.len(), TURNS * 2);
}

/// Roadmap C4, end to end: a provider that rejects the request for length is
/// recovered from without an operator, through the real orchestrator.
///
/// The unit tests in `loop::planning` stub the orchestrator. This one stubs
/// the *provider*, so it also covers the link they skip: `MaxOrchestrator`
/// mapping `LlmError::ContextLengthExceeded` to the typed orchestrator error
/// instead of folding it into `Backend`.
#[tokio::test]
async fn a_provider_length_rejection_compacts_and_retries_once() {
    const TURNS: usize = 40;

    let llm = Arc::new(OverflowingLlm::new(1));
    let summarizer = StubSummarizer::new();
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        // Pressure well above anything this conversation reaches, so only the
        // provider rejection can trigger compaction.
        config: CompactionConfig {
            pressure_percent: 100,
            ..Default::default()
        },
    };

    // Seed a conversation long enough to have a span worth summarizing.
    for turn in 0..TURNS {
        run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("c4-seed-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("seed turn {turn} failed: {error}"));
    }
    assert_eq!(summarizer.calls(), 0, "pressure alone must not have fired");

    // The next turn is the one the provider rejects.
    let before = llm.calls();
    let outcome = run_envelope(
        user_envelope("And the one that overflows?"),
        RunId::new("c4-overflow"),
        &deps,
    )
    .await
    .expect("a length rejection is recovered, not surfaced");

    let RunOutcome::Finished { state, output } = outcome else {
        panic!("the recovered turn should finish");
    };
    assert_eq!(output.message.content.as_ref(), "recovered");
    assert_eq!(llm.calls() - before, 2, "one rejection, one retry");
    assert_eq!(summarizer.calls(), 1, "exactly one forced compaction");
    assert!(state
        .trace_events
        .iter()
        .any(|event| event.name.as_ref() == "conversation_compacted"));

    // The rejected request still left its header in the trace: two requests
    // were assembled for this one turn.
    assert_eq!(request_sizes(&[state]).len(), 2);
}

/// A conversation that cannot be made to fit answers rather than failing.
#[tokio::test]
async fn an_unrecoverable_overflow_answers_with_a_truncation_notice() {
    const TURNS: usize = 40;

    let llm = Arc::new(OverflowingLlm::new(usize::MAX));
    let summarizer = StubSummarizer::new();
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        config: CompactionConfig {
            pressure_percent: 100,
            ..Default::default()
        },
    };

    for turn in 0..TURNS {
        let _ = run_envelope(
            user_envelope(&format!("Question {turn}")),
            RunId::new(format!("c4-hopeless-{turn}")),
            &deps,
        )
        .await;
    }

    let outcome = run_envelope(
        user_envelope("Anything at all?"),
        RunId::new("c4-hopeless-final"),
        &deps,
    )
    .await
    .expect("an unrecoverable overflow answers rather than failing the run");

    let RunOutcome::Finished { state, output } = outcome else {
        panic!("the turn should finish");
    };
    assert!(output
        .message
        .content
        .contains("too long for the model's context window"));
    assert!(state
        .trace_events
        .iter()
        .any(|event| event.name.as_ref() == "context_overflow_unrecovered"));
}

/// With compaction off, nothing changes — the C2-and-earlier behaviour.
#[tokio::test]
async fn a_disabled_deployment_writes_no_checkpoints() {
    const TURNS: usize = 40;

    let llm = Arc::new(ScriptedLlm::new((0..TURNS).map(long_reply)).with_context_budget(WINDOW));
    let summarizer = StubSummarizer::new();
    let orchestrator = MaxOrchestrator::new().with_llm(llm.clone());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let mut deps = runner_deps(&orchestrator, &session, &policy, None, None);
    deps.compaction = Compaction {
        summarizer: Some(&summarizer),
        config: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
    };

    for turn in 0..TURNS {
        run_envelope(
            user_envelope(&format!("Question {turn}: what did we decide?")),
            RunId::new(format!("c3-off-{turn}")),
            &deps,
        )
        .await
        .unwrap_or_else(|error| panic!("turn {turn} failed: {error}"));
    }

    assert_eq!(summarizer.calls(), 0);
    let transcript = session
        .load(&support::user_session_key())
        .await
        .expect("session loads");
    assert!(!transcript.items.iter().any(is_checkpoint));
}
