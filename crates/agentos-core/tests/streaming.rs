//! Track A3: orchestrator-level streaming wiring.
//!
//! Verifies that when a run installs a `stream_sink` on the `RunContext`, an
//! LLM-backed core orchestrator (`MinOrchestrator`) drains the streaming LLM
//! path, forwards each text chunk incrementally to the sink, and still returns
//! a `Plan::Reply` whose message equals the concatenated chunks. With no sink
//! it takes the buffered path unchanged. The two paths are distinguished by a
//! mock LLM that returns deliberately different content per path.

use agentos_core::orchestrator::MinOrchestrator;
use agentos_interfaces::orchestrator::{Orchestrator, Plan, RunContext, StreamSink};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::tool::ToolSpec;
use agentos_interfaces::RunState;
use agentos_llm::{CompletionEvent, CompletionStream, Llm, LlmError};
use agentos_proto::{AgentId, Message, MessageRole, RunId};
use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Streams `chunks` as separate `Text` events then a `Done` whose content is
/// their concatenation. The buffered `complete_messages` returns a distinct
/// marker so a test can tell which path an orchestrator took.
struct MockStreamLlm {
    chunks: Vec<&'static str>,
}

impl MockStreamLlm {
    fn streamed_content(&self) -> String {
        self.chunks.concat()
    }
}

#[async_trait]
impl Llm for MockStreamLlm {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        Ok(Message::text(MessageRole::Assistant, "BUFFERED"))
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        Ok(Message::text(MessageRole::Assistant, "BUFFERED"))
    }

    async fn complete_messages_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<CompletionStream, LlmError> {
        let mut events: Vec<Result<CompletionEvent, LlmError>> = self
            .chunks
            .iter()
            .map(|chunk| Ok(CompletionEvent::Text(Arc::from(*chunk))))
            .collect();
        events.push(Ok(CompletionEvent::Done(Message::text(
            MessageRole::Assistant,
            self.streamed_content(),
        ))));
        Ok(stream::iter(events).boxed())
    }
}

/// Emits one `Text` chunk and then a mid-stream `StreamTransport` error (no
/// `Done`), simulating an SSE body cut by a flaky network/proxy. The buffered
/// path returns a distinct marker so a test can prove the fallback ran.
struct MockMidStreamFailLlm;

#[async_trait]
impl Llm for MockMidStreamFailLlm {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        Ok(Message::text(MessageRole::Assistant, "BUFFERED-FALLBACK"))
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        Ok(Message::text(MessageRole::Assistant, "BUFFERED-FALLBACK"))
    }

    async fn complete_messages_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<CompletionStream, LlmError> {
        let events: Vec<Result<CompletionEvent, LlmError>> = vec![
            Ok(CompletionEvent::Text(Arc::from("partial"))),
            Err(LlmError::StreamTransport(Arc::from(
                "error decoding response body: connection reset by peer",
            ))),
        ];
        Ok(stream::iter(events).boxed())
    }
}

fn user_state(content: &str) -> RunState {
    let mut state = RunState::new(RunId::new("stream-run"), AgentId::new("stream-agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, content),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

#[tokio::test]
async fn min_orchestrator_streams_chunks_and_returns_full_reply() {
    let llm = Arc::new(MockStreamLlm {
        chunks: vec!["Hel", "lo, ", "world"],
    });
    let orchestrator = MinOrchestrator::new(llm);
    let state = user_state("hi");
    let mut ctx = RunContext::from_state(&state);

    // Capture each delta separately to prove the output is incremental, not one
    // buffered blob.
    let deltas: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&deltas);
    let sink: StreamSink = Arc::new(move |delta: &str| {
        captured.lock().expect("sink lock").push(delta.to_owned());
        Box::pin(std::future::ready(()))
    });
    ctx.stream_sink = Some(sink);

    let plan = orchestrator.plan(&ctx).await.expect("plan succeeds");
    let Plan::Reply(message) = plan else {
        panic!("expected Plan::Reply, got {plan:?}");
    };

    assert_eq!(message.content.as_ref(), "Hello, world");
    assert_eq!(
        *deltas.lock().expect("deltas lock"),
        vec!["Hel".to_owned(), "lo, ".to_owned(), "world".to_owned()],
        "sink must receive each streamed chunk in order"
    );
}

#[tokio::test]
async fn min_orchestrator_uses_buffered_path_without_sink() {
    let llm = Arc::new(MockStreamLlm {
        chunks: vec!["streamed"],
    });
    let orchestrator = MinOrchestrator::new(llm);
    let state = user_state("hi");
    let ctx = RunContext::from_state(&state); // no stream_sink installed

    let plan = orchestrator.plan(&ctx).await.expect("plan succeeds");
    let Plan::Reply(message) = plan else {
        panic!("expected Plan::Reply, got {plan:?}");
    };
    // The buffered marker proves complete_messages (not the stream) was used.
    assert_eq!(message.content.as_ref(), "BUFFERED");
}

#[tokio::test]
async fn mid_stream_transport_error_falls_back_to_buffered() {
    let orchestrator = MinOrchestrator::new(Arc::new(MockMidStreamFailLlm));
    let state = user_state("hi");
    let mut ctx = RunContext::from_state(&state);

    let deltas: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&deltas);
    let sink: StreamSink = Arc::new(move |delta: &str| {
        captured.lock().expect("sink lock").push(delta.to_owned());
        Box::pin(std::future::ready(()))
    });
    ctx.stream_sink = Some(sink);

    // The stream cuts out mid-flight, but the turn must still succeed by
    // retrying the buffered completion instead of erroring.
    let plan = orchestrator
        .plan(&ctx)
        .await
        .expect("plan succeeds via fallback");
    let Plan::Reply(message) = plan else {
        panic!("expected Plan::Reply, got {plan:?}");
    };
    assert_eq!(message.content.as_ref(), "BUFFERED-FALLBACK");
    // The partial chunk seen before the cut was still forwarded to the sink;
    // edit-in-place channels replace it with the buffered final text on send.
    assert_eq!(
        *deltas.lock().expect("deltas lock"),
        vec!["partial".to_owned()]
    );
}
