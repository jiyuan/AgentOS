//! The one durable path from a built [`Request`] to a provider.

use super::{Request, RequestKind};
use agentos_interfaces::orchestrator::{RequestAttemptSink, RunContext};
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{CompletionEvent, Llm, LlmError};
use agentos_proto::{
    AgentId, Message, RequestAttempt, RequestAttemptStatus, RequestHeader, RunId, Usage,
};
use futures_util::StreamExt;
use std::sync::Arc;

/// One completed provider round-trip and the record of it.
#[derive(Clone, Debug)]
pub struct Exchange {
    pub message: Message,
    pub header: RequestHeader,
    pub usage: Option<Usage>,
}

/// Send a non-streaming request whose caller has no [`RunContext`].
pub async fn call_detached(
    llm: &dyn Llm,
    request: &Request,
    tools: &[ToolSpec],
    run_id: &RunId,
    active_agent: &AgentId,
    sink: Option<&RequestAttemptSink<'_>>,
) -> Result<Exchange, LlmError> {
    let message = buffered_attempt(llm, request, tools, run_id, active_agent, sink, 1).await?;
    Ok(record(request, message))
}

/// Send `request`, recording a durable start before each physical invocation
/// and a terminal outcome afterwards. A streamed transport retry is attempt 2
/// under the same logical request id.
pub async fn call_with_context(
    llm: &dyn Llm,
    ctx: &RunContext<'_>,
    request: &Request,
    tools: &[ToolSpec],
) -> Result<Message, LlmError> {
    let message = if request.kind == RequestKind::Turn && ctx.has_stream_sink() {
        match streamed_attempt(llm, ctx, request, tools, 1).await {
            Ok(message) => message,
            Err(LlmError::StreamTransport(detail)) => {
                tracing::warn!(
                    error = %detail,
                    "llm stream failed mid-flight; retrying with a buffered completion"
                );
                buffered_attempt(
                    llm,
                    request,
                    tools,
                    &ctx.state.run_id,
                    &ctx.state.active_agent,
                    ctx.request_attempt_sink,
                    2,
                )
                .await?
            }
            Err(error) => return Err(error),
        }
    } else {
        buffered_attempt(
            llm,
            request,
            tools,
            &ctx.state.run_id,
            &ctx.state.active_agent,
            ctx.request_attempt_sink,
            1,
        )
        .await?
    };

    let exchange = record(request, message);
    if request.kind != RequestKind::Turn {
        ctx.push_request_header(exchange.header);
    }
    if let Some(usage) = exchange.usage {
        ctx.push_llm_usage(usage);
    }
    Ok(exchange.message)
}

async fn buffered_attempt(
    llm: &dyn Llm,
    request: &Request,
    tools: &[ToolSpec],
    run_id: &RunId,
    active_agent: &AgentId,
    sink: Option<&RequestAttemptSink<'_>>,
    attempt: u32,
) -> Result<Message, LlmError> {
    let guard = AttemptGuard::begin(request, run_id, active_agent, sink, attempt)?;
    match llm.complete_messages(&request.messages, tools).await {
        Ok(message) => {
            let usage = Usage::from_message_metadata(&message);
            guard.finish(RequestAttemptStatus::Succeeded, usage, None)?;
            Ok(message)
        }
        Err(error) => {
            guard.finish(
                RequestAttemptStatus::Failed,
                None,
                Some(bounded_detail(&error.to_string())),
            )?;
            Err(error)
        }
    }
}

async fn streamed_attempt(
    llm: &dyn Llm,
    ctx: &RunContext<'_>,
    request: &Request,
    tools: &[ToolSpec],
    attempt: u32,
) -> Result<Message, LlmError> {
    let guard = AttemptGuard::begin(
        request,
        &ctx.state.run_id,
        &ctx.state.active_agent,
        ctx.request_attempt_sink,
        attempt,
    )?;
    let result = drain_stream(llm, ctx, &request.messages, tools).await;
    match result {
        Ok(message) => {
            let usage = Usage::from_message_metadata(&message);
            guard.finish(RequestAttemptStatus::Succeeded, usage, None)?;
            Ok(message)
        }
        Err(error) => {
            guard.finish(
                RequestAttemptStatus::Failed,
                None,
                Some(bounded_detail(&error.to_string())),
            )?;
            Err(error)
        }
    }
}

async fn drain_stream(
    llm: &dyn Llm,
    ctx: &RunContext<'_>,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, LlmError> {
    let mut stream = llm.complete_messages_stream(messages, tools).await?;
    let mut done = None;
    while let Some(event) = stream.next().await {
        match event? {
            CompletionEvent::Text(chunk) => ctx.emit_stream_delta(&chunk).await,
            CompletionEvent::Done(message) => done = Some(message),
        }
    }
    done.ok_or_else(|| LlmError::Provider(Arc::from("stream ended without a final message")))
}

struct AttemptGuard<'a> {
    sink: Option<&'a RequestAttemptSink<'a>>,
    record: RequestAttempt,
    finished: bool,
}

impl<'a> AttemptGuard<'a> {
    fn begin(
        request: &Request,
        run_id: &RunId,
        active_agent: &AgentId,
        sink: Option<&'a RequestAttemptSink<'a>>,
        attempt: u32,
    ) -> Result<Self, LlmError> {
        let record = RequestAttempt {
            logical_request_id: Arc::clone(&request.logical_request_id),
            attempt,
            run_id: run_id.clone(),
            active_agent: active_agent.clone(),
            header: request.manifest.header(),
            status: RequestAttemptStatus::Started,
            usage: None,
            detail: None,
        };
        append(sink, &record)?;
        Ok(Self {
            sink,
            record,
            finished: false,
        })
    }

    fn finish(
        mut self,
        status: RequestAttemptStatus,
        usage: Option<Usage>,
        detail: Option<Arc<str>>,
    ) -> Result<(), LlmError> {
        self.finished = true;
        self.record.status = status;
        self.record.usage = usage;
        self.record.detail = detail;
        append(self.sink, &self.record)
    }
}

impl Drop for AttemptGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.record.status = RequestAttemptStatus::Cancelled;
        self.record.detail = Some(Arc::from("provider future dropped before an outcome"));
        if let Err(error) = append(self.sink, &self.record) {
            tracing::error!(error = %error, "cancelled request attempt could not be journaled");
        }
    }
}

fn append(sink: Option<&RequestAttemptSink<'_>>, record: &RequestAttempt) -> Result<(), LlmError> {
    let Some(sink) = sink else {
        return Ok(());
    };
    sink(record).map_err(|error| {
        LlmError::Provider(Arc::from(format!(
            "request attempt journal failed before provider transition: {error}"
        )))
    })
}

fn bounded_detail(detail: &str) -> Arc<str> {
    let mut end = detail.len().min(512);
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    Arc::from(&detail[..end])
}

fn record(request: &Request, message: Message) -> Exchange {
    let usage = Usage::from_message_metadata(&message);
    Exchange {
        header: request.manifest.header(),
        usage,
        message,
    }
}
