//! The one path from a built [`Request`] to a provider.
//!
//! M5 / `REQ-001`, closing `docs/adr/0004-REQUEST_KINDS.md`. `AGENTS.md` says
//! every request records what it was made of in a `RequestHeader`, and two
//! production calls did not: the routing classifier and the compaction
//! summarizer. Both called `Llm::complete_messages` directly with a
//! hand-built message vector, so neither had a manifest, and compaction
//! recorded no usage at all — its summarizer tokens were spent and never
//! counted.
//!
//! The fix is not "route everything through assembly". Routing's separation
//! from assembly is a prompt-injection defence and must survive
//! (see [`RequestKind`]). The fix is that a *kind* determines the section set
//! and the gateway takes a request rather than a message vector, so recording
//! is something the gateway does rather than something each call site
//! remembers.
//!
//! # Two entry points, one recording site
//!
//! [`call_with_context`] is for a caller that holds a [`RunContext`]: it
//! records the header and usage on the context's sinks, which the loop drains
//! after `plan()` returns. [`call_detached`] is for a caller that has no
//! context — compaction holds `&mut RunState` — and hands the record back for
//! the caller to carry upward. The first is implemented in terms of the
//! second, so there is exactly one place that turns a response into a record.

use super::{Request, RequestKind};
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::{Llm, LlmError};
use agentos_proto::{Message, RequestHeader, Usage};

/// One completed provider round-trip and the record of it.
#[derive(Clone, Debug)]
pub struct Exchange {
    /// The provider's final message.
    pub message: Message,
    /// What the request was made of.
    pub header: RequestHeader,
    /// Tokens the round-trip cost, when the provider reported them.
    pub usage: Option<Usage>,
}

/// Send `request` and hand back the record, for a caller with no
/// [`RunContext`] to record it on.
///
/// Never streams: a caller without a context has no stream sink, and a routing
/// or summarization request has nothing a user would want to watch arrive.
pub async fn call_detached(
    llm: &dyn Llm,
    request: &Request,
    tools: &[ToolSpec],
) -> Result<Exchange, LlmError> {
    let message = llm.complete_messages(&request.messages, tools).await?;
    Ok(record(request, message))
}

/// Send `request` and record it on the run context's sinks.
///
/// Streams when the run installed a sink *and* the kind is one a user would
/// watch. A routing classification and a summarization are machinery: emitting
/// their tokens into a chat window would show the user the agent's internal
/// monologue as if it were the answer.
pub async fn call_with_context(
    llm: &dyn Llm,
    ctx: &RunContext<'_>,
    request: &Request,
    tools: &[ToolSpec],
) -> Result<Message, LlmError> {
    let message = if request.kind == RequestKind::Turn {
        crate::orchestrator::streaming::complete_message(llm, ctx, &request.messages, tools).await?
    } else {
        llm.complete_messages(&request.messages, tools).await?
    };

    let exchange = record(request, message);
    // Assembly already pushed the turn's header, deliberately: a request the
    // provider *rejects* still has to leave one, and only the assembling side
    // runs in that case. Pushing again here would double every turn.
    if request.kind != RequestKind::Turn {
        ctx.push_request_header(exchange.header);
    }
    if let Some(usage) = exchange.usage {
        ctx.push_llm_usage(usage);
    }
    Ok(exchange.message)
}

fn record(request: &Request, message: Message) -> Exchange {
    let usage = Usage::from_message_metadata(&message);
    Exchange {
        header: request.manifest.header(),
        usage,
        message,
    }
}
