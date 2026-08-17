//! Roadmap G2: an answer has to name the approval it answers.
//!
//! `loop::approval_route` unit-tests the matching in isolation. These drive a
//! real paused run through `GatewayService`, which is where the property has to
//! hold: the prompt it sends must carry what an answer needs, and no envelope
//! that lacks it may resume the run.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::gateway::{GatewayRun, GatewayService};
use agentos_core::memory::InMemorySession;
use agentos_core::r#loop::{
    route, ApprovalOutcome, ApprovalTicket, Routed, EXPIRES_AT_KEY, INTERRUPTION_KEY, PROMPT_KIND,
    TICKET_KEY,
};
use agentos_core::runner::{ResumeDecision, RunnerError};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::test_support::MockChannel;
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_interfaces::{Channel, RunState};
use agentos_proto::{
    ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall, ToolCallId,
    ToolResult, ToolStatus,
};
use async_trait::async_trait;
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const GATED_TOOL: &str = "gated";

fn envelope(text: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new("approvals"),
        conversation_id: ConversationId::new("conv"),
        sender: Arc::from("user"),
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// Calls the gated tool once, then reports what came back.
struct CallThenReport;

#[async_trait]
impl Orchestrator for CallThenReport {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let last = ctx.state.transcript.items.last();
        match last.map(|item| item.message.role.clone()) {
            Some(MessageRole::Tool) => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                last.expect("matched above").message.content.as_ref(),
            ))),
            _ => Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("call-gated"),
                name: Arc::from(GATED_TOOL),
                args: RawValue::from_string("{}".to_owned()).expect("static args are valid JSON"),
            })),
        }
    }
}

/// Counts how many times it actually ran, which is what "fails closed" means
/// in the end.
struct GatedTool {
    ran: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for GatedTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from(GATED_TOOL),
            description: Arc::from("Does something worth asking about."),
            input_schema: json!({"type": "object"}),
            requires_isolation: false,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.ran.fetch_add(1, Ordering::Relaxed);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("the gated work ran"),
            metadata: BTreeMap::new(),
        })
    }
}

fn ask_user(tool: &str) -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from(tool)),
            decision: PolicyVerb::AskUser,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    }
}

/// A paused run plus everything an answer needs to name it.
struct Paused {
    paused: agentos_core::runner::PausedRun,
    prompt: Envelope,
    ticket: ApprovalTicket,
    expires_at: Option<u64>,
}

/// Drive a run to its approval prompt.
async fn pause_for_approval(
    service: &GatewayService<'_>,
    channel: &MockChannel,
    run_id: &str,
) -> Paused {
    let outcome = service
        .run_envelope(
            channel.egress().as_ref(),
            envelope("do the gated thing"),
            RunId::new(run_id),
        )
        .await
        .expect("a gated call pauses cleanly");
    let GatewayRun::Paused {
        paused,
        prompt,
        ticket,
        expires_at,
    } = outcome
    else {
        panic!("an ask_user tool call must pause, not finish");
    };
    Paused {
        paused,
        prompt: prompt.expect("a pause sends a prompt"),
        ticket,
        expires_at,
    }
}

/// The prompt has to carry what an answer needs, or the rest of the item is
/// unusable: the ticket to name, the interruption it gates, and when it stops
/// counting.
#[tokio::test]
async fn the_prompt_carries_its_ticket_and_what_it_gates() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"))
        .with_approval_expiry(Some(Duration::from_secs(600)));
    let channel = MockChannel::new("approvals");

    let paused = pause_for_approval(&service, &channel, "g2-prompt").await;

    assert_eq!(
        paused.prompt.metadata.get("kind").and_then(Value::as_str),
        Some(PROMPT_KIND)
    );
    assert_eq!(
        paused
            .prompt
            .metadata
            .get(TICKET_KEY)
            .and_then(Value::as_str),
        Some(paused.ticket.as_str())
    );
    // The interruption id travels alongside the ticket: one says which asking,
    // the other says what it authorises, and the pair is the audit record.
    assert_eq!(
        paused
            .prompt
            .metadata
            .get(INTERRUPTION_KEY)
            .and_then(Value::as_str),
        paused
            .paused
            .state
            .pending_approvals
            .first()
            .map(|approval| approval.id.as_str())
    );
    assert!(paused.prompt.metadata.contains_key(EXPIRES_AT_KEY));
    assert!(paused.expires_at.is_some());
    // The ticket is spelled out in the body: on a channel with no buttons it is
    // the only way to answer.
    assert!(paused
        .prompt
        .message
        .content
        .contains(&format!("/approve {}", paused.ticket)));
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// The item's exit condition: no envelope can answer an approval it does not
/// name, however affirmative it sounds.
#[tokio::test]
async fn an_unrelated_message_does_not_decide_a_pending_approval() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"));
    let channel = MockChannel::new("approvals");

    let paused = pause_for_approval(&service, &channel, "g2-unrelated").await;

    for text in [
        "y",
        "yes",
        "yes, go ahead",
        "approve",
        "sure",
        "/approve",
        "ok do it",
    ] {
        assert_eq!(
            route(Some(&paused.ticket), &envelope(text)),
            Routed::Unrelated,
            "{text:?} must be ordinary input, not an approval"
        );
    }
    // The one thing that matters: the gated tool never ran.
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// A press on a button from some earlier prompt names that prompt, so it
/// decides nothing here. This is what per-prompt tickets buy over the
/// action-derived `InterruptionId`, which two prompts for the same tool call
/// would share.
#[tokio::test]
async fn a_stale_ticket_does_not_decide_the_current_prompt() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"));
    let channel = MockChannel::new("approvals");

    let first = pause_for_approval(&service, &channel, "g2-stale-1").await;
    let second = pause_for_approval(&service, &channel, "g2-stale-2").await;
    assert_ne!(
        first.ticket, second.ticket,
        "two prompts for the same tool call must not share a ticket"
    );
    // Both prompts do name the same interruption — which is exactly why the
    // interruption id cannot be the correlation token.
    assert_eq!(
        first.prompt.metadata.get(INTERRUPTION_KEY),
        second.prompt.metadata.get(INTERRUPTION_KEY)
    );

    let answer = envelope(&format!("/approve {}", first.ticket));
    assert_eq!(
        route(Some(&second.ticket), &answer),
        Routed::Stale {
            ticket: first.ticket.clone()
        }
    );
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// The other half: an answer that does name the prompt resumes the run, and
/// the gated action finally runs.
#[tokio::test]
async fn an_answer_naming_the_prompt_approves_it() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"));
    let channel = MockChannel::new("approvals");

    let paused = pause_for_approval(&service, &channel, "g2-approve").await;
    let answer = envelope(&format!("/approve {}", paused.ticket));
    let Routed::Decides { outcome, .. } = route(Some(&paused.ticket), &answer) else {
        panic!("an answer naming the prompt must decide it");
    };
    assert_eq!(outcome, ApprovalOutcome::Approved);

    let approval_id = approval_id(&paused.paused.state);
    let resumed = service
        .resume(
            channel.egress().as_ref(),
            paused.paused,
            &approval_id,
            ResumeDecision::Approve,
        )
        .await
        .expect("an approved run resumes");
    let GatewayRun::Finished { output, .. } = resumed else {
        panic!("the resumed run should finish");
    };
    assert_eq!(output.message.content.as_ref(), "the gated work ran");
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

/// An expired prompt is recorded as *cancelled*, not rejected. Both fail
/// closed; only one of them is a decision somebody made, and an audit trail
/// that conflates them says the user refused something they never saw.
#[tokio::test]
async fn an_expired_prompt_records_cancelled_not_rejected() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"));
    let channel = MockChannel::new("approvals");

    let expired = pause_for_approval(&service, &channel, "g2-expired").await;
    let expired_id = approval_id(&expired.paused.state);
    let cancelled = service
        .resume(
            channel.egress().as_ref(),
            expired.paused,
            &expired_id,
            ResumeDecision::Cancel {
                reason: Arc::from("approval prompt expired before it was answered"),
            },
        )
        .await
        .expect_err("a cancelled approval fails the run closed");

    let refused = pause_for_approval(&service, &channel, "g2-refused").await;
    let refused_id = approval_id(&refused.paused.state);
    let rejected = service
        .resume(
            channel.egress().as_ref(),
            refused.paused,
            &refused_id,
            ResumeDecision::Reject {
                reason: Arc::from("not today"),
            },
        )
        .await
        .expect_err("a rejected approval fails the run closed");

    assert!(
        cancelled.to_string().contains("unanswered"),
        "expiry must not read as a refusal, got: {cancelled}"
    );
    assert!(
        cancelled.to_string().contains("cancelled"),
        "the outcome must be named, got: {cancelled}"
    );
    assert!(
        rejected.to_string().contains("denied"),
        "a refusal must read as one, got: {rejected}"
    );
    assert_ne!(
        std::mem::discriminant(&as_run_error(&cancelled)),
        std::mem::discriminant(&as_run_error(&rejected)),
        "the audit trail must be able to tell the two apart"
    );
    // Neither ran the tool.
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// A cron tick has nobody behind it, so its prompt is resolved as unavailable
/// rather than parked forever — and not as a refusal.
#[tokio::test]
async fn a_run_with_nobody_to_ask_records_unavailable() {
    let ran = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(GatedTool { ran: ran.clone() });
    let orchestrator = CallThenReport;
    let session = InMemorySession::default();
    let policy = ask_user(GATED_TOOL);
    let deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    let service = GatewayService::new(&deps, Arc::from("agent"));
    let channel = MockChannel::new("approvals");

    let paused = pause_for_approval(&service, &channel, "g2-unavailable").await;
    let approval_id = approval_id(&paused.paused.state);
    let error = service
        .resume(
            channel.egress().as_ref(),
            paused.paused,
            &approval_id,
            ResumeDecision::Unavailable {
                reason: Arc::from("no interactive user behind a cron run"),
            },
        )
        .await
        .expect_err("an unanswerable approval fails the run closed");
    assert!(error.to_string().contains("unavailable"), "got: {error}");
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// Every decision maps to exactly one outcome, and only one of them runs the
/// action.
#[test]
fn every_resume_decision_names_its_outcome() {
    let reason: Arc<str> = Arc::from("because");
    let pairs = [
        (ResumeDecision::Approve, ApprovalOutcome::Approved),
        (
            ResumeDecision::Reject {
                reason: Arc::clone(&reason),
            },
            ApprovalOutcome::Rejected,
        ),
        (
            ResumeDecision::Cancel {
                reason: Arc::clone(&reason),
            },
            ApprovalOutcome::Cancelled,
        ),
        (
            ResumeDecision::Unavailable { reason },
            ApprovalOutcome::Unavailable,
        ),
    ];
    for (decision, expected) in pairs {
        assert_eq!(decision.outcome(), expected);
        assert_eq!(
            expected.permits_action(),
            matches!(decision, ResumeDecision::Approve)
        );
    }
}

fn approval_id(state: &RunState) -> agentos_proto::InterruptionId {
    state
        .pending_approvals
        .first()
        .map(|approval| approval.id.clone())
        .expect("a paused run carries the approval it is waiting on")
}

/// Reach the `RunError` inside a gateway error, so two failures can be compared
/// by variant rather than by message text.
fn as_run_error(error: &agentos_core::gateway::GatewayError) -> agentos_core::r#loop::RunError {
    match error {
        agentos_core::gateway::GatewayError::Runner(RunnerError::Run(run)) => match run {
            agentos_core::r#loop::RunError::ApprovalDenied { reason } => {
                agentos_core::r#loop::RunError::ApprovalDenied {
                    reason: Arc::clone(reason),
                }
            }
            agentos_core::r#loop::RunError::ApprovalUnanswered { reason } => {
                agentos_core::r#loop::RunError::ApprovalUnanswered {
                    reason: Arc::clone(reason),
                }
            }
            other => panic!("unexpected run error: {other}"),
        },
        other => panic!("unexpected gateway error: {other}"),
    }
}
