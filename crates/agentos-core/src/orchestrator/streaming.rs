//! Shared streaming helper for the LLM-backed core orchestrators.
//!
//! `MinOrchestrator` and `MaxOrchestrator` both produce their reply with an
//! explicit message list and tool set. This helper picks the streamed or
//! buffered LLM path based on whether the run installed a [`StreamSink`], and
//! returns the assembled final message either way — so each orchestrator's
//! `Plan::Reply` / `Plan::CallTool` decision and the loop's downstream
//! guardrail + finish handling stay identical to the non-streaming path.

use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{CompletionEvent, Llm, LlmError};
use agentos_proto::Message;
use futures_util::StreamExt;
use std::sync::Arc;

/// Complete one LLM turn, streaming incremental text to the run's sink when one
/// is installed. With a sink, drains [`Llm::complete_messages_stream`], forwards
/// each `Text` chunk via [`RunContext::emit_stream_delta`], and returns the
/// terminal `Done` message (which still carries any tool calls + usage). Without
/// a sink, falls back to the buffered [`Llm::complete_messages`].
pub(crate) async fn complete_message(
    llm: &dyn Llm,
    ctx: &RunContext<'_>,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, LlmError> {
    if !ctx.has_stream_sink() {
        return llm.complete_messages(messages, tools).await;
    }
    let mut stream = llm.complete_messages_stream(messages, tools).await?;
    let mut done = None;
    while let Some(event) = stream.next().await {
        match event? {
            CompletionEvent::Text(chunk) => ctx.emit_stream_delta(&chunk),
            CompletionEvent::Done(message) => done = Some(message),
        }
    }
    done.ok_or_else(|| LlmError::Provider(Arc::from("stream ended without a final message")))
}
