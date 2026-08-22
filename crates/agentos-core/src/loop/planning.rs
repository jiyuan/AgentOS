//! One planning turn, including the two ways it responds to context pressure.
//!
//! Roadmap items C3 and C4 in `docs/TRANSFER_ROADMAP.md`. Planning is the only
//! point in the loop where a request is assembled and sent, so it is where both
//! pressure responses live:
//!
//! - **Before the attempt (C3).** If the run is over the configured pressure,
//!   summarize its oldest span first. A guess, but a measured one.
//! - **After a rejected attempt (C4).** If the provider itself refused the
//!   request for length, that is the one trigger that is never a guess — the
//!   model said no. Force one compaction pass and retry the attempt exactly
//!   once.
//!
//! # Why the give-up path is a reply, not an error
//!
//! A second consecutive overflow ends the turn with a plain
//! [`Plan::Reply`] carrying a truncation notice, so the caller — a user, or a
//! parent run — gets an answer it can act on rather than an opaque failure.
//! Returning it as a plan rather than a `Finish` state also means it goes
//! through the loop's normal reply handling, so the output guardrails still
//! run on it. This mirrors how `max_turns` became a stop condition rather than
//! an error.

use super::telemetry::field_key;
use super::LoopDeps;
use crate::trace;
use agentos_interfaces::orchestrator::{OrchestratorError, Plan, RunContext};
use agentos_interfaces::run_state::RunState;
use agentos_proto::{Message, MessageRole, RequestHeader, SpanId, SpanKind, Usage};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Plan one turn, recovering once from a context-length rejection.
///
/// `usage` and `requests` accumulate across both attempts, so a rejected
/// request still leaves its header in the trace — that header is the record of
/// exactly what was too long.
pub(super) async fn plan_with_overflow_recovery(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    usage: &mut Vec<Usage>,
    requests: &mut Vec<RequestHeader>,
) -> Result<Plan, OrchestratorError> {
    let detail = match attempt(state, deps, plan_span_id, usage, requests).await {
        Err(OrchestratorError::ContextLengthExceeded(detail)) => detail,
        other => return other,
    };

    warn!(
        run_id = state.run_id.as_str(),
        error = %detail,
        "provider rejected the request for length; compacting and retrying once"
    );
    if !compact_now(state, deps, plan_span_id, usage, requests).await {
        // Nothing left to summarize: the tail alone is too long for the model.
        // Retrying would send the same request and fail identically.
        return Ok(overflow_reply(
            state,
            deps,
            plan_span_id,
            &detail,
            "no_span",
        ));
    }

    match attempt(state, deps, plan_span_id, usage, requests).await {
        // Exactly once. A second overflow after a real compaction means the
        // retained tail does not fit either, and a third attempt would be the
        // start of a loop that spends a summarizer call per iteration.
        Err(OrchestratorError::ContextLengthExceeded(detail)) => Ok(overflow_reply(
            state,
            deps,
            plan_span_id,
            &detail,
            "retry_rejected",
        )),
        other => other,
    }
}

/// Summarize the oldest span if the run is over the configured pressure.
///
/// Called once at the top of a planning turn, before hydration, so recall and
/// the orchestrator both see the compacted transcript.
pub(super) async fn compact_under_pressure(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    usage: &mut Vec<Usage>,
    requests: &mut Vec<RequestHeader>,
) {
    // Racing the token here is C3's outstanding promise: summarization is an
    // LLM round-trip inside the planning path, so a run cancelled while it is
    // in flight must drop it rather than pay for a summary nobody will read.
    let compacted = super::unless_cancelled(
        &deps.cancel,
        crate::prompt::compact(state, &deps.compaction),
    )
    .await
    .flatten();
    record_compaction(
        state,
        deps,
        plan_span_id,
        compacted,
        "pressure",
        usage,
        requests,
    );
}

/// Summarize regardless of the measured pressure. `false` when there was
/// nothing to summarize.
async fn compact_now(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    usage: &mut Vec<Usage>,
    requests: &mut Vec<RequestHeader>,
) -> bool {
    let compacted = super::unless_cancelled(
        &deps.cancel,
        crate::prompt::compact_now(state, &deps.compaction),
    )
    .await
    .flatten();
    let did = compacted.is_some();
    record_compaction(
        state,
        deps,
        plan_span_id,
        compacted,
        "overflow",
        usage,
        requests,
    );
    did
}

fn record_compaction(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    compacted: Option<crate::prompt::Compacted>,
    trigger: &'static str,
    usage: &mut Vec<Usage>,
    requests: &mut Vec<RequestHeader>,
) {
    let Some(compacted) = compacted else {
        return;
    };
    // M5 / `REQ-001`: the summarizer is a provider call like any other, and
    // these are the two things every provider call owes. Compaction has no
    // `RunContext` to push them onto, so it hands them back and they join the
    // turn's own here — which is also why compaction's tokens now appear in
    // run totals that previously omitted them entirely.
    requests.push(compacted.header.clone());
    usage.extend(compacted.usage);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("reason"), Value::from(trigger));
    fields.insert(field_key("span_start"), Value::from(compacted.span.start));
    fields.insert(field_key("span_end"), Value::from(compacted.span.end));
    fields.insert(field_key("span_items"), Value::from(compacted.span.items));
    fields.insert(
        field_key("replaced_chars"),
        Value::from(compacted.replaced_chars),
    );
    fields.insert(
        field_key("summary_chars"),
        Value::from(compacted.summary_chars),
    );
    trace::record_event(
        state,
        deps.hooks,
        plan_span_id.clone(),
        "conversation_compacted",
        fields,
    );
}

/// One hydrate-and-plan attempt, with its own hydration span so a retry is
/// visible in the trace as a second attempt rather than a mystery.
async fn attempt(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    usage: &mut Vec<Usage>,
    requests: &mut Vec<RequestHeader>,
) -> Result<Plan, OrchestratorError> {
    let hydrate_span_id = trace::record_span(
        state,
        Some(plan_span_id.clone()),
        SpanKind::State,
        "orchestrator.hydrate",
        BTreeMap::new(),
    );
    trace::record_event(
        state,
        deps.hooks,
        hydrate_span_id.clone(),
        "hydrate_started",
        BTreeMap::new(),
    );

    let mut run_ctx = RunContext::from_state(state);
    run_ctx.stream_sink = deps.stream_sink.clone();
    run_ctx.cancel = deps.cancel.clone();
    let hydrated = deps.orchestrator.hydrate(&mut run_ctx).await;

    let mut hydrate_fields = BTreeMap::new();
    let planned = match hydrated {
        Ok(()) => {
            hydrate_fields.insert(
                field_key("memory_fragments"),
                Value::from(run_ctx.memory_fragments.len()),
            );
            hydrate_fields.insert(
                field_key("resources"),
                Value::from(run_ctx.resource_index.entries.len()),
            );
            for key in [
                "memory_hydration_candidate_count",
                "memory_hydration_selected_count",
                "memory_hydration_namespace_count",
            ] {
                if let Some(value) = run_ctx.system.metadata.get(key) {
                    hydrate_fields.insert(Arc::from(key), value.clone());
                }
            }
            deps.orchestrator.plan(&run_ctx).await
        }
        Err(error) => Err(error),
    };

    // The sinks are append-only, so even a poisoned guard (an orchestrator
    // panicked mid-push) holds a structurally valid Vec — recover it rather
    // than propagating the panic into the run loop. Drained on the error path
    // too: a request that was rejected for length still recorded the header
    // describing it, which is the most useful thing in the trace.
    usage.append(&mut std::mem::take(
        &mut *run_ctx
            .usage_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    ));
    requests.append(&mut std::mem::take(
        &mut *run_ctx
            .request_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    ));
    drop(run_ctx);

    trace::record_event(
        state,
        deps.hooks,
        hydrate_span_id,
        "hydrate_finished",
        hydrate_fields,
    );
    planned
}

/// The terminal answer for a turn that cannot be made to fit.
///
/// Text only — no model call. Asking a provider that just rejected the request
/// to write the apology would fail the same way.
fn overflow_reply(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    detail: &str,
    reason: &'static str,
) -> Plan {
    let mut fields = BTreeMap::new();
    fields.insert(field_key("reason"), Value::from(reason));
    fields.insert(field_key("error"), Value::from(detail));
    trace::record_event(
        state,
        deps.hooks,
        plan_span_id.clone(),
        "context_overflow_unrecovered",
        fields,
    );
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        reason,
        error = %detail,
        "context overflow could not be recovered; answering with a truncation notice"
    );

    Plan::Reply(Message::text(
        MessageRole::Assistant,
        "This conversation is too long for the model's context window, and \
         summarizing the earlier turns was not enough to bring it back under. \
         Nothing has been lost — the full history is still on disk — but I \
         can't answer in this thread as it stands. Start a new conversation, \
         or ask me something narrower that doesn't need the earlier context.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::Policy;
    use crate::config::CompactionConfig;
    use crate::prompt::Compaction;
    use agentos_interfaces::tool::ToolSpec;
    use agentos_llm::{Llm, LlmError};
    use agentos_proto::{AgentId, RunId};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// An orchestrator that reports a context-length rejection for its first
    /// `overflows` planning attempts, then replies normally.
    struct OverflowingOrchestrator {
        overflows: usize,
        attempts: AtomicUsize,
        /// Transcript length seen on each attempt, so a test can prove the
        /// retry planned against a *compacted* conversation.
        seen: Mutex<Vec<usize>>,
    }

    impl OverflowingOrchestrator {
        fn new(overflows: usize) -> Self {
            Self {
                overflows,
                attempts: AtomicUsize::new(0),
                seen: Mutex::new(Vec::new()),
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl agentos_interfaces::orchestrator::Orchestrator for OverflowingOrchestrator {
        async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
            let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            self.seen
                .lock()
                .expect("never poisoned in tests")
                .push(crate::prompt::visible(&ctx.state.transcript).len());
            if attempt < self.overflows {
                return Err(OrchestratorError::ContextLengthExceeded(Arc::from(
                    "openai error: this model's maximum context length is 8192 tokens",
                )));
            }
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "fitted")))
        }
    }

    /// A summarizer that answers anything, so compaction always succeeds.
    struct Summarizer;

    #[async_trait]
    impl Llm for Summarizer {
        fn context_budget_tokens(&self) -> Option<usize> {
            Some(4_096)
        }

        async fn complete(&self, _ctx: &RunContext<'_>) -> Result<Message, LlmError> {
            Err(LlmError::Unconfigured(Arc::from("complete")))
        }

        async fn complete_messages(
            &self,
            _messages: &[Message],
            _tools: &[ToolSpec],
        ) -> Result<Message, LlmError> {
            Ok(Message::text(MessageRole::Assistant, "a summary"))
        }
    }

    fn state_with(turns: usize) -> RunState {
        let mut state = RunState::new(RunId::new("overflow"), AgentId::new("agent"));
        state.transcript.items = (0..turns)
            .map(|index| agentos_interfaces::session::Item {
                message: Message::text(MessageRole::User, format!("turn {index}")),
                metadata: BTreeMap::new(),
            })
            .collect();
        state
    }

    fn deps<'a>(
        orchestrator: &'a dyn agentos_interfaces::orchestrator::Orchestrator,
        summarizer: Option<&'a dyn Llm>,
        policy: &'a Policy,
    ) -> LoopDeps<'a> {
        LoopDeps {
            orchestrator,
            max_turns: 8,
            hooks: None,
            tools: None,
            task_workspace: None,
            policy,
            subagents: None,
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: Default::default(),
            cancel: Default::default(),
            steering: None,
            audit: crate::audit::SafetyJournal::detached(),
            granted_authority: &[],
            compaction: Compaction {
                summarizer,
                config: CompactionConfig::default(),
            },
        }
    }

    #[tokio::test]
    async fn one_overflow_compacts_and_succeeds_on_the_retry() {
        let orchestrator = OverflowingOrchestrator::new(1);
        let summarizer = Summarizer;
        let policy = Policy::default();
        let deps = deps(&orchestrator, Some(&summarizer), &policy);
        let mut state = state_with(40);
        let (mut usage, mut requests) = (Vec::new(), Vec::new());

        let plan = plan_with_overflow_recovery(
            &mut state,
            &deps,
            &SpanId::new("plan-1"),
            &mut usage,
            &mut requests,
        )
        .await
        .expect("the retry succeeds");

        let Plan::Reply(message) = plan else {
            panic!("expected the retried plan's reply");
        };
        assert_eq!(message.content.as_ref(), "fitted");
        assert_eq!(orchestrator.attempts(), 2, "exactly one retry");

        // The retry planned against a shorter conversation, which is the whole
        // point: retrying the identical request would fail identically.
        let seen = orchestrator.seen.lock().expect("never poisoned in tests");
        assert!(seen[1] < seen[0], "retry saw {seen:?}");
    }

    #[tokio::test]
    async fn a_second_overflow_answers_rather_than_looping() {
        let orchestrator = OverflowingOrchestrator::new(usize::MAX);
        let summarizer = Summarizer;
        let policy = Policy::default();
        let deps = deps(&orchestrator, Some(&summarizer), &policy);
        let mut state = state_with(40);
        let (mut usage, mut requests) = (Vec::new(), Vec::new());

        let plan = plan_with_overflow_recovery(
            &mut state,
            &deps,
            &SpanId::new("plan-1"),
            &mut usage,
            &mut requests,
        )
        .await
        .expect("an unrecoverable overflow answers rather than failing");

        let Plan::Reply(message) = plan else {
            panic!("expected a truncation notice");
        };
        assert!(message.content.contains("too long for the model"));
        assert_eq!(orchestrator.attempts(), 2, "no third attempt");
        assert!(state
            .trace_events
            .iter()
            .any(|event| event.name.as_ref() == "context_overflow_unrecovered"));
    }

    #[tokio::test]
    async fn with_nothing_to_compact_the_retry_is_not_attempted() {
        // A conversation shorter than the retained tail has no span to
        // summarize, so a retry would send the identical request.
        let orchestrator = OverflowingOrchestrator::new(usize::MAX);
        let summarizer = Summarizer;
        let policy = Policy::default();
        let deps = deps(&orchestrator, Some(&summarizer), &policy);
        let mut state = state_with(2);
        let (mut usage, mut requests) = (Vec::new(), Vec::new());

        let plan = plan_with_overflow_recovery(
            &mut state,
            &deps,
            &SpanId::new("plan-1"),
            &mut usage,
            &mut requests,
        )
        .await
        .expect("the turn still answers");

        assert!(matches!(plan, Plan::Reply(_)));
        assert_eq!(orchestrator.attempts(), 1, "no pointless retry");
    }

    #[tokio::test]
    async fn without_a_summarizer_an_overflow_still_answers() {
        // Compaction disabled or unconfigured: there is no recovery, but the
        // turn must still end with something a caller can read.
        let orchestrator = OverflowingOrchestrator::new(usize::MAX);
        let policy = Policy::default();
        let deps = deps(&orchestrator, None, &policy);
        let mut state = state_with(40);
        let (mut usage, mut requests) = (Vec::new(), Vec::new());

        let plan = plan_with_overflow_recovery(
            &mut state,
            &deps,
            &SpanId::new("plan-1"),
            &mut usage,
            &mut requests,
        )
        .await
        .expect("the turn still answers");

        assert!(matches!(plan, Plan::Reply(_)));
        assert_eq!(orchestrator.attempts(), 1);
    }

    #[tokio::test]
    async fn an_unrelated_planning_failure_is_not_swallowed() {
        struct Failing;
        #[async_trait]
        impl agentos_interfaces::orchestrator::Orchestrator for Failing {
            async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
                Err(OrchestratorError::Backend(Arc::from("provider down")))
            }
        }

        let orchestrator = Failing;
        let summarizer = Summarizer;
        let policy = Policy::default();
        let deps = deps(&orchestrator, Some(&summarizer), &policy);
        let mut state = state_with(40);
        let (mut usage, mut requests) = (Vec::new(), Vec::new());

        let error = plan_with_overflow_recovery(
            &mut state,
            &deps,
            &SpanId::new("plan-1"),
            &mut usage,
            &mut requests,
        )
        .await
        .expect_err("a backend failure must still fail the turn");
        assert!(matches!(error, OrchestratorError::Backend(_)));
    }
}
