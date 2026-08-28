use agentos_core::prompt::{self, Assembly};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::ToolSpec;
use agentos_interfaces::RunState;
use agentos_llm::{CompletionStream, Llm, LlmError};
use agentos_proto::{
    AgentId, Message, MessageRole, RequestAttempt, RequestAttemptStatus, RunId, Usage,
    TOKEN_USAGE_METADATA_KEY,
};
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn state() -> RunState {
    let mut state = RunState::new(RunId::new("attempt-run"), AgentId::new("attempt-agent"));
    state
        .transcript
        .items
        .push(agentos_interfaces::session::Item {
            message: Message::text(MessageRole::User, "hello"),
            metadata: Default::default(),
        });
    state
}

fn turn_request(ctx: &RunContext<'_>) -> prompt::Request {
    prompt::assemble(ctx, &Assembly::default())
}

struct Failed;

#[async_trait]
impl Llm for Failed {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        unreachable!("the request gateway uses the explicit-message API")
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        Err(LlmError::Provider(Arc::from("provider refused")))
    }
}

struct Retried;

#[async_trait]
impl Llm for Retried {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        unreachable!("the request gateway uses the explicit-message API")
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        let mut message = Message::text(MessageRole::Assistant, "recovered");
        message.metadata.insert(
            Arc::from(TOKEN_USAGE_METADATA_KEY),
            json!({"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}),
        );
        Ok(message)
    }

    async fn complete_messages_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<CompletionStream, LlmError> {
        Ok(stream::iter([Err(LlmError::StreamTransport(Arc::from(
            "connection reset",
        )))])
        .boxed())
    }
}

struct Pending;

#[async_trait]
impl Llm for Pending {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        unreachable!("the request gateway uses the explicit-message API")
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        std::future::pending().await
    }
}

struct Counted<'a>(&'a AtomicUsize);

#[async_trait]
impl Llm for Counted<'_> {
    async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        unreachable!("the request gateway uses the explicit-message API")
    }

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Message::text(MessageRole::Assistant, "must not run"))
    }
}

#[tokio::test]
async fn every_failed_retried_or_cancelled_attempt_is_durable() {
    let state = state();
    let records = Mutex::new(Vec::<RequestAttempt>::new());
    let sink = |record: &RequestAttempt| {
        records
            .lock()
            .expect("record mutex is healthy")
            .push(record.clone());
        Ok(())
    };

    let mut failed_ctx = RunContext::from_state(&state);
    failed_ctx.request_attempt_sink = Some(&sink);
    let failed_request = turn_request(&failed_ctx);
    prompt::call_with_context(&Failed, &failed_ctx, &failed_request, &[])
        .await
        .expect_err("provider failure is returned");

    let routing_request = prompt::routing_request(
        Message::text(MessageRole::System, "classify"),
        Message::text(MessageRole::User, "input"),
    );
    prompt::call_with_context(&Failed, &failed_ctx, &routing_request, &[])
        .await
        .expect_err("routing failure is journaled too");

    let summary_request = prompt::summary_request(
        Message::text(MessageRole::System, "summarize"),
        Message::text(MessageRole::User, "old span"),
    );
    prompt::call_detached(
        &Failed,
        &summary_request,
        &[],
        &state.run_id,
        &state.active_agent,
        Some(&sink),
    )
    .await
    .expect_err("detached compaction failure is journaled");

    let mut retried_ctx = RunContext::from_state(&state);
    retried_ctx.request_attempt_sink = Some(&sink);
    retried_ctx.stream_sink = Some(Arc::new(|_: &str| Box::pin(async {})));
    let retried_request = turn_request(&retried_ctx);
    prompt::call_with_context(&Retried, &retried_ctx, &retried_request, &[])
        .await
        .expect("buffered retry succeeds");

    let mut cancelled_ctx = RunContext::from_state(&state);
    cancelled_ctx.request_attempt_sink = Some(&sink);
    let cancelled_request = turn_request(&cancelled_ctx);
    tokio::time::timeout(
        Duration::from_millis(5),
        prompt::call_with_context(&Pending, &cancelled_ctx, &cancelled_request, &[]),
    )
    .await
    .expect_err("timeout drops the pending provider future");

    let records = records.lock().expect("record mutex is healthy");
    assert_eq!(records.len(), 12);
    assert_eq!(records[0].status, RequestAttemptStatus::Started);
    assert_eq!(records[1].status, RequestAttemptStatus::Failed);
    assert_eq!(records[2].status, RequestAttemptStatus::Started);
    assert_eq!(records[3].status, RequestAttemptStatus::Failed);
    assert_eq!(records[4].status, RequestAttemptStatus::Started);
    assert_eq!(records[5].status, RequestAttemptStatus::Failed);
    assert_eq!(records[6].status, RequestAttemptStatus::Started);
    assert_eq!(records[7].status, RequestAttemptStatus::Failed);
    assert_eq!(records[8].status, RequestAttemptStatus::Started);
    assert_eq!(records[9].status, RequestAttemptStatus::Succeeded);
    assert_eq!(records[8].attempt, 2);
    assert_eq!(records[6].logical_request_id, records[8].logical_request_id);
    assert_eq!(
        records[9].usage,
        Some(Usage {
            input_tokens: 7,
            output_tokens: 3,
            total_tokens: 10,
            ..Usage::default()
        })
    );
    assert_eq!(records[10].status, RequestAttemptStatus::Started);
    assert_eq!(records[11].status, RequestAttemptStatus::Cancelled);

    drop(records);
    let calls = AtomicUsize::new(0);
    let refusing_sink = |_record: &RequestAttempt| Err(Arc::from("disk unavailable"));
    let mut refused_ctx = RunContext::from_state(&state);
    refused_ctx.request_attempt_sink = Some(&refusing_sink);
    let refused_request = turn_request(&refused_ctx);
    prompt::call_with_context(&Counted(&calls), &refused_ctx, &refused_request, &[])
        .await
        .expect_err("a failed pre-attempt append fails closed");
    assert_eq!(calls.load(Ordering::SeqCst), 0, "provider I/O never began");
}
