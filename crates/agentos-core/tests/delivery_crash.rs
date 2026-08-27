//! GW-004: external actions and terminal delivery use conservative crash states.

mod support;

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::gateway::{
    DeliveryState, IngressEventKey, IngressLedger, Settlement, DELIVERY_ID_KEY,
};
use agentos_core::memory::SqliteStore;
use agentos_core::r#loop::RunError;
use agentos_core::runner::{run_envelope, RunOutcome, RunnerError};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{
    ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, ToolCall, ToolCallId,
    ToolResult, ToolStatus, INGRESS_ID_KEY,
};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use serde_json::{json, value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn envelope(event_id: &str, text: &str) -> Envelope {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(event_id));
    Envelope {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("conversation"),
        sender: Arc::from("sender"),
        message: Message::text(MessageRole::User, text),
        metadata,
    }
}

fn claimed(ledger: &IngressLedger, input: &Envelope) -> IngressEventKey {
    ledger.admit(input).expect("input is durable");
    let key = IngressEventKey::from_envelope(input).expect("fixture has ingress id");
    ledger
        .mark_acknowledged(&key.channel_id, &key.event_id)
        .expect("input is dispatchable");
    assert_eq!(
        ledger
            .claim_next(&key.channel_id)
            .expect("queue reads")
            .expect("input is claimed")
            .envelope,
        *input
    );
    key
}

/// AF-030: neither uncertainty window may become `Handled` or return to the
/// automatic input/reply queues after restart.
#[test]
fn ambiguous_delivery_is_neither_silently_settled_nor_blindly_retried() {
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open_in_memory().expect("store opens"),
    ));

    // Crash after a tool may have performed its side effect, before the tool
    // completion/run-state commit.
    let action_input = envelope("action-1", "perform side effect");
    let action_key = claimed(&ledger, &action_input);
    let journal = ledger
        .run_journal(&action_input)
        .expect("run journal binds exact input");
    journal
        .action_started("call-side-effect")
        .expect("start commits before invocation");
    ledger
        .recover_dispatches(&action_key.channel_id)
        .expect("restart classifies action boundary");
    assert_eq!(
        ledger.state(&action_key).expect("state reads"),
        DeliveryState::ActionAmbiguous
    );
    assert!(ledger
        .claim_next(&action_key.channel_id)
        .expect("input queue reads")
        .is_none());

    // A complete reply is safe to claim after restart only until transport
    // delivery starts. Once bytes may have been accepted, restart must stop.
    let delivery_input = envelope("delivery-1", "answer me");
    let delivery_key = claimed(&ledger, &delivery_input);
    let reply = envelope("outbound", "terminal answer");
    let pending = ledger
        .prepare_reply(
            std::slice::from_ref(&delivery_key),
            reply,
            Settlement::Handled,
        )
        .expect("terminal reply commits before egress");
    assert_eq!(
        ledger.state(&delivery_key).expect("state reads"),
        DeliveryState::ReplyPending
    );
    assert_eq!(
        pending
            .envelope
            .metadata
            .get(DELIVERY_ID_KEY)
            .and_then(Value::as_str),
        Some(pending.delivery_id.as_ref())
    );

    ledger
        .recover_dispatches(&delivery_key.channel_id)
        .expect("reply-pending restart is safe");
    let sending = ledger
        .claim_pending_delivery(&delivery_key.channel_id)
        .expect("pending reply queue reads")
        .expect("durable reply is claimed once");
    assert_eq!(sending.delivery_id, pending.delivery_id);
    assert_eq!(
        ledger.state(&delivery_key).expect("state reads"),
        DeliveryState::DeliveryStarted
    );

    // This is the exact crash point after transport acceptance and before the
    // settlement commit. No resend can be assumed safe.
    ledger
        .recover_dispatches(&delivery_key.channel_id)
        .expect("restart classifies delivery boundary");
    assert_eq!(
        ledger.state(&delivery_key).expect("state reads"),
        DeliveryState::DeliveryAmbiguous
    );
    assert!(ledger
        .claim_pending_delivery(&delivery_key.channel_id)
        .expect("pending reply queue reads")
        .is_none());
    assert!(ledger
        .claim_next(&delivery_key.channel_id)
        .expect("input queue reads")
        .is_none());

    let ambiguous = ledger
        .ambiguities(&delivery_key.channel_id)
        .expect("ambiguities are operator-visible");
    assert_eq!(ambiguous.len(), 2);
    assert!(ambiguous
        .iter()
        .any(|event| event.key == action_key && event.state == DeliveryState::ActionAmbiguous));
    assert!(ambiguous.iter().any(|event| {
        event.key == delivery_key && event.state == DeliveryState::DeliveryAmbiguous
    }));
}

#[test]
fn handled_requires_a_successful_delivery_transition() {
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open_in_memory().expect("store opens"),
    ));
    let input = envelope("delivery-2", "answer me");
    ledger.admit(&input).expect("input is durable");
    let key = IngressEventKey::from_envelope(&input).expect("fixture has ingress id");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Accepted
    );
    ledger
        .mark_acknowledged(&key.channel_id, &key.event_id)
        .expect("input is dispatchable");
    ledger
        .claim_next(&key.channel_id)
        .expect("queue reads")
        .expect("input is claimed");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Running
    );
    let batch = ledger
        .prepare_reply(
            std::slice::from_ref(&key),
            envelope("outbound", "answer"),
            Settlement::Handled,
        )
        .expect("reply persists");
    ledger
        .begin_delivery(&batch)
        .expect("delivery start persists");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::DeliveryStarted
    );
    ledger
        .delivery_succeeded(&batch)
        .expect("success settles exact event");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Handled
    );
    assert!(ledger
        .unsettled(&key.channel_id)
        .expect("unsettled rows read")
        .is_empty());
}

#[test]
fn settlement_commit_failure_after_transport_acceptance_is_delivery_ambiguous() {
    let tree = support::temp_tree("gw004-settlement-failpoint");
    let path = tree.path().join("agentos.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    let ledger = IngressLedger::new(store);
    let input = envelope("delivery-settlement-failpoint", "answer me");
    let key = claimed(&ledger, &input);
    let batch = ledger
        .prepare_reply(
            std::slice::from_ref(&key),
            envelope("outbound", "answer"),
            Settlement::Handled,
        )
        .expect("reply persists before transport");
    ledger
        .begin_delivery(&batch)
        .expect("transport boundary persists");

    let failpoint = Connection::open(&path).expect("failpoint connection opens");
    failpoint
        .execute_batch(
            r#"
            CREATE TRIGGER fail_delivery_settlement
            BEFORE UPDATE OF delivery_state ON ingress_events
            WHEN NEW.delivery_state = 'handled'
            BEGIN
                SELECT RAISE(FAIL, 'injected settlement failure');
            END;
            "#,
        )
        .expect("failpoint installs");
    ledger
        .delivery_succeeded(&batch)
        .expect_err("transport acceptance alone must not settle handled");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::DeliveryStarted
    );
    ledger
        .delivery_ambiguous(&batch, "transport accepted but settlement commit failed")
        .expect("ambiguity remains persistable");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::DeliveryAmbiguous
    );
    assert!(ledger
        .claim_pending_delivery(&key.channel_id)
        .expect("reply queue reads")
        .is_none());
}

#[test]
fn pre_egress_transition_failures_leave_the_last_safe_stage_recoverable() {
    let tree = support::temp_tree("gw004-pre-egress-failpoints");
    let path = tree.path().join("agentos.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    let ledger = IngressLedger::new(store);
    let input = envelope("pre-egress-failpoints", "answer me");
    let key = claimed(&ledger, &input);
    let failpoint = Connection::open(&path).expect("failpoint connection opens");
    failpoint
        .execute_batch(
            r#"
            CREATE TRIGGER fail_reply_persist
            BEFORE UPDATE OF delivery_state ON ingress_events
            WHEN NEW.delivery_state = 'reply_pending'
            BEGIN
                SELECT RAISE(FAIL, 'injected reply-persist failure');
            END;
            "#,
        )
        .expect("reply failpoint installs");
    ledger
        .prepare_reply(
            std::slice::from_ref(&key),
            envelope("outbound", "answer"),
            Settlement::Handled,
        )
        .expect_err("a reply that is not durable never reaches egress");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Running
    );

    failpoint
        .execute_batch(
            r#"
            DROP TRIGGER fail_reply_persist;
            CREATE TRIGGER fail_delivery_start
            BEFORE UPDATE OF delivery_state ON ingress_events
            WHEN NEW.delivery_state = 'delivery_started'
            BEGIN
                SELECT RAISE(FAIL, 'injected delivery-start failure');
            END;
            "#,
        )
        .expect("delivery-start failpoint installs");
    let batch = ledger
        .prepare_reply(
            std::slice::from_ref(&key),
            envelope("outbound", "answer"),
            Settlement::Handled,
        )
        .expect("reply persists after the failpoint clears");
    ledger
        .begin_delivery(&batch)
        .expect_err("delivery cannot touch transport without its boundary");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::ReplyPending
    );

    failpoint
        .execute_batch("DROP TRIGGER fail_delivery_start;")
        .expect("delivery failpoint clears");
    let recovered = ledger
        .claim_pending_delivery(&key.channel_id)
        .expect("pending queue reads")
        .expect("the durable reply remains safely deliverable");
    ledger
        .delivery_succeeded(&recovered)
        .expect("successful transport result settles");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Handled
    );
}

#[test]
fn action_completion_pause_and_rejection_have_explicit_states() {
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open_in_memory().expect("store opens"),
    ));

    let action_input = envelope("action-2", "tool then answer");
    let action_key = claimed(&ledger, &action_input);
    let journal = ledger.run_journal(&action_input).expect("journal binds");
    journal.action_started("call-2").expect("start persists");
    journal
        .action_completed("call-2")
        .expect("completion persists");
    assert_eq!(
        ledger.state(&action_key).expect("state reads"),
        DeliveryState::ActionCompleted
    );

    let pause_input = envelope("pause-1", "needs approval");
    let pause_key = claimed(&ledger, &pause_input);
    let pause = ledger
        .prepare_pause(
            std::slice::from_ref(&pause_key),
            envelope("approval-prompt", "approve?"),
        )
        .expect("pause and prompt persist together");
    assert_eq!(
        ledger.state(&pause_key).expect("state reads"),
        DeliveryState::Paused
    );
    ledger
        .begin_delivery(&pause)
        .expect("prompt delivery starts");
    ledger
        .pause_delivered(&pause)
        .expect("delivered prompt returns to paused");
    assert_eq!(
        ledger.state(&pause_key).expect("state reads"),
        DeliveryState::Paused
    );

    let rejected_input = envelope("rejected-1", "overflow");
    let rejected_key = claimed(&ledger, &rejected_input);
    let rejection = ledger
        .prepare_reply(
            std::slice::from_ref(&rejected_key),
            envelope("rejection", "retry later"),
            Settlement::Refused,
        )
        .expect("rejection reply persists");
    ledger
        .begin_delivery(&rejection)
        .expect("rejection delivery starts");
    ledger
        .delivery_succeeded(&rejection)
        .expect("delivered rejection settles");
    assert_eq!(
        ledger.state(&rejected_key).expect("state reads"),
        DeliveryState::Rejected
    );
}

#[test]
fn approval_resolution_carries_original_paused_rows_through_action_and_settlement() {
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open_in_memory().expect("store opens"),
    ));
    let original = envelope("approval-original", "perform protected action");
    let original_key = claimed(&ledger, &original);
    let prompt = ledger
        .prepare_pause(
            std::slice::from_ref(&original_key),
            envelope("approval-prompt", "approve?"),
        )
        .expect("prompt persists");
    ledger.begin_delivery(&prompt).expect("prompt starts");
    ledger
        .pause_delivered(&prompt)
        .expect("prompt delivery preserves the paused row");

    let resolution = envelope("approval-resolution", "approve");
    let resolution_key = claimed(&ledger, &resolution);
    let events = vec![resolution_key.clone(), original_key.clone()];
    let journal = ledger
        .run_journal_for(events.clone())
        .expect("resolution binds both exact inputs");
    journal
        .action_started("approved-side-effect")
        .expect("both inputs enter the action boundary atomically");
    assert_eq!(
        ledger.state(&original_key).expect("state reads"),
        DeliveryState::ActionStarted
    );
    assert_eq!(
        ledger.state(&resolution_key).expect("state reads"),
        DeliveryState::ActionStarted
    );
    journal
        .action_completed("approved-side-effect")
        .expect("both inputs leave the action boundary atomically");
    let reply = ledger
        .prepare_reply(
            &events,
            envelope("approval-result", "protected action complete"),
            Settlement::Handled,
        )
        .expect("one final reply binds both inputs");
    ledger.begin_delivery(&reply).expect("reply starts");
    ledger
        .delivery_succeeded(&reply)
        .expect("one delivery settles both inputs");
    assert_eq!(
        ledger.state(&original_key).expect("state reads"),
        DeliveryState::Handled
    );
    assert_eq!(
        ledger.state(&resolution_key).expect("state reads"),
        DeliveryState::Handled
    );
}

struct ToolThenReply {
    plans: AtomicUsize,
}

#[async_trait]
impl Orchestrator for ToolThenReply {
    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        if self.plans.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Plan::CallTool(ToolCall {
                id: ToolCallId::new("side-effect-call"),
                name: Arc::from("boundary-tool"),
                args: RawValue::from_string("{}".to_owned()).expect("static JSON is valid"),
            }))
        } else {
            Ok(Plan::Reply(Message::text(MessageRole::Assistant, "done")))
        }
    }
}

struct BoundaryTool {
    ledger: IngressLedger,
    key: IngressEventKey,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for BoundaryTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("boundary-tool"),
            description: Arc::from("asserts its durable pre-invocation boundary"),
            input_schema: json!({"type": "object"}),
            safety: Default::default(),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        assert_eq!(
            self.ledger.state(&self.key).expect("state reads"),
            DeliveryState::ActionStarted,
            "action_started must commit before the tool observes invocation"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("side effect complete"),
            metadata: BTreeMap::new(),
        })
    }
}

#[tokio::test]
async fn runner_commits_action_started_before_invoking_the_tool() {
    let ledger = IngressLedger::new(Arc::new(
        SqliteStore::open_in_memory().expect("store opens"),
    ));
    let input = envelope("runner-action", "use the tool");
    let key = claimed(&ledger, &input);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(BoundaryTool {
        ledger: ledger.clone(),
        key: key.clone(),
        calls: Arc::clone(&calls),
    });
    let orchestrator = ToolThenReply {
        plans: AtomicUsize::new(0),
    };
    let session = agentos_core::memory::InMemorySession::default();
    let policy = Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from("boundary-tool")),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    };
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.run_journal = Some(ledger.run_journal(&input).expect("journal binds"));

    let outcome = run_envelope(input, RunId::new("gw004-action-run"), &deps)
        .await
        .expect("run finishes");
    assert!(matches!(outcome, RunOutcome::Finished { .. }));
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::ActionCompleted
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn action_start_commit_failure_aborts_before_tool_invocation() {
    let tree = support::temp_tree("gw004-action-start-failpoint");
    let path = tree.path().join("agentos.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    let ledger = IngressLedger::new(store);
    let input = envelope("action-start-failpoint", "use the tool");
    let key = claimed(&ledger, &input);
    let failpoint = Connection::open(&path).expect("failpoint connection opens");
    failpoint
        .execute_batch(
            r#"
            CREATE TRIGGER fail_action_start
            BEFORE UPDATE OF delivery_state ON ingress_events
            WHEN NEW.delivery_state = 'action_started'
            BEGIN
                SELECT RAISE(FAIL, 'injected action-start failure');
            END;
            "#,
        )
        .expect("failpoint installs");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(BoundaryTool {
        ledger: ledger.clone(),
        key: key.clone(),
        calls: Arc::clone(&calls),
    });
    let orchestrator = ToolThenReply {
        plans: AtomicUsize::new(0),
    };
    let session = agentos_core::memory::InMemorySession::default();
    let policy = Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from("boundary-tool")),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    };
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.run_journal = Some(ledger.run_journal(&input).expect("journal binds"));

    let error = run_envelope(input, RunId::new("gw004-action-start-failure"), &deps)
        .await
        .expect_err("required pre-invocation commit fails the run");
    assert!(matches!(
        error,
        RunnerError::Run(RunError::DurableState { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::Running
    );
}

#[tokio::test]
async fn action_completion_commit_failure_recovers_as_ambiguous_without_rerun() {
    let tree = support::temp_tree("gw004-action-complete-failpoint");
    let path = tree.path().join("agentos.sqlite");
    let store = Arc::new(SqliteStore::open(&path).expect("store opens"));
    let ledger = IngressLedger::new(store);
    let input = envelope("action-complete-failpoint", "use the tool");
    let key = claimed(&ledger, &input);
    let failpoint = Connection::open(&path).expect("failpoint connection opens");
    failpoint
        .execute_batch(
            r#"
            CREATE TRIGGER fail_action_completion
            BEFORE UPDATE OF delivery_state ON ingress_events
            WHEN NEW.delivery_state = 'action_completed'
            BEGIN
                SELECT RAISE(FAIL, 'injected action-completion failure');
            END;
            "#,
        )
        .expect("failpoint installs");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(BoundaryTool {
        ledger: ledger.clone(),
        key: key.clone(),
        calls: Arc::clone(&calls),
    });
    let orchestrator = ToolThenReply {
        plans: AtomicUsize::new(0),
    };
    let session = agentos_core::memory::InMemorySession::default();
    let policy = Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Tool(Arc::from("boundary-tool")),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    };
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.run_journal = Some(ledger.run_journal(&input).expect("journal binds"));

    let error = run_envelope(input, RunId::new("gw004-action-complete-failure"), &deps)
        .await
        .expect_err("post-side-effect commit failure stops the run");
    assert!(matches!(
        error,
        RunnerError::Run(RunError::DurableState { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::ActionStarted
    );

    drop(failpoint);
    ledger
        .recover_dispatches(&key.channel_id)
        .expect("restart classifies the incomplete boundary");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::ActionAmbiguous
    );
    assert!(ledger
        .claim_next(&key.channel_id)
        .expect("input queue reads")
        .is_none());
}

#[test]
fn pre_state_machine_in_flight_rows_migrate_to_action_ambiguity() {
    let tree = support::temp_tree("gw004-v2-in-flight");
    let path = tree.path().join("agentos.sqlite");
    let input = envelope("legacy-in-flight", "unknown old action boundary");
    let raw = Connection::open(&path).expect("legacy database opens");
    raw.execute_batch(
        r#"
        CREATE TABLE ingress_schema (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL
        );
        INSERT INTO ingress_schema(singleton, version) VALUES (1, 2);
        CREATE TABLE ingress_events (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            settlement TEXT,
            envelope_json TEXT,
            checkpoint TEXT,
            dispatch_state TEXT NOT NULL DEFAULT 'awaiting_ack',
            quarantine_reason TEXT,
            accepted_at INTEGER NOT NULL,
            settled_at INTEGER,
            UNIQUE(channel_id, event_id)
        );
        "#,
    )
    .expect("v2 schema installs");
    raw.execute(
        "INSERT INTO ingress_events \
         (channel_id, event_id, conversation_id, sender, envelope_json, \
          dispatch_state, accepted_at) VALUES (?1, ?2, ?3, ?4, ?5, 'in_flight', 1)",
        params![
            input.channel_id.as_str(),
            "legacy-in-flight",
            input.conversation_id.as_str(),
            input.sender.as_ref(),
            serde_json::to_string(&input).expect("envelope serializes"),
        ],
    )
    .expect("legacy in-flight row inserts");
    drop(raw);

    let ledger = IngressLedger::new(Arc::new(SqliteStore::open(&path).expect("store migrates")));
    let key = IngressEventKey::from_envelope(&input).expect("fixture has ingress id");
    assert_eq!(
        ledger.state(&key).expect("state reads"),
        DeliveryState::ActionAmbiguous
    );
    assert_eq!(
        ledger
            .ambiguities(&input.channel_id)
            .expect("ambiguities read")
            .len(),
        1
    );
    assert!(ledger
        .claim_next(&input.channel_id)
        .expect("queue reads")
        .is_none());
}
