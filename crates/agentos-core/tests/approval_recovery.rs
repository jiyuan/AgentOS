//! `STATE-002` restart and approval-authority regression gates.

use agentos_core::audit::{SafetyEvent, SafetyEventKind, SafetyLog, SafetyOutcome};
use agentos_core::gateway::{
    ApprovalStore, DeliveryState, IngressEventKey, IngressLedger, Settlement,
};
use agentos_core::memory::SqliteStore;
use agentos_core::r#loop::{route, ApprovalBinding, ApprovalTicket, Routed, PROMPT_KIND};
use agentos_core::runner::PausedRun;
use agentos_interfaces::run_state::{Interruption, InterruptionAction};
use agentos_interfaces::RunState;
use agentos_proto::{
    ActorPrincipal, AgentId, ApprovalInstanceId, ChannelId, ConversationId, Envelope,
    InterruptionId, Message, MessageRole, RunId, ToolCall, ToolCallId, INGRESS_ID_KEY,
};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn actor(sender: &str) -> ActorPrincipal {
    ActorPrincipal::new(
        AgentId::new("agent"),
        ChannelId::new("telegram"),
        ConversationId::new("conversation"),
        sender,
    )
}

fn envelope(event_id: &str, sender: &str, text: impl Into<Arc<str>>) -> Envelope {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(event_id));
    Envelope {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("conversation"),
        sender: Arc::from(sender),
        message: Message::text(MessageRole::User, text),
        metadata,
    }
}

fn claim(ledger: &IngressLedger, input: &Envelope) -> IngressEventKey {
    ledger.admit(input).expect("input persists");
    let event = IngressEventKey::from_envelope(input).expect("fixture has identity");
    ledger
        .mark_acknowledged(&event.channel_id, &event.event_id)
        .expect("input becomes dispatchable");
    ledger
        .claim_next(&event.channel_id)
        .expect("queue reads")
        .expect("input is claimed");
    event
}

fn paused(ticket: &ApprovalTicket) -> PausedRun {
    let requester = actor("requester");
    let mut state = RunState::new(RunId::new("restart-run"), AgentId::new("agent"));
    state.approvals.push(Interruption::pending(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.as_str(),
        requester,
        InterruptionId::new("approval-gated-call"),
        InterruptionAction::ToolCall(ToolCall {
            id: ToolCallId::new("gated-call"),
            name: Arc::from("external-side-effect"),
            args: RawValue::from_string("{\"target\":\"fixed\"}".to_owned()).expect("static JSON"),
        }),
    ));
    PausedRun {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("conversation"),
        state,
    }
}

fn prompt(ticket: &ApprovalTicket) -> Envelope {
    let mut envelope = envelope("prompt", "agent", "approve external side effect?");
    envelope.message.role = MessageRole::Assistant;
    envelope
        .metadata
        .insert(Arc::from("kind"), Value::from(PROMPT_KIND));
    envelope
        .metadata
        .insert(Arc::from("approval_ticket"), Value::from(ticket.as_str()));
    envelope
}

fn database_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agentos-state-002-{label}-{}-{nonce}.sqlite",
        std::process::id()
    ))
}

#[test]
/// AF-008: pause, terminate, restart, and approve exactly once while policy
/// administrator additions do not widen old authority and removals revoke it.
fn paused_approval_survives_restart_and_policy_change() {
    let path = database_path("restart");
    let ticket = ApprovalTicket::parse("restartticket").expect("fixture ticket");
    let input = envelope("original", "requester", "perform protected action");
    let paused = paused(&ticket);
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval-gated-call"),
        actor("requester"),
        None,
    )
    .expect("instance matches ticket")
    .with_administrators(vec![actor("old-admin")]);

    {
        let sqlite = Arc::new(SqliteStore::open(&path).expect("store opens"));
        let ledger = IngressLedger::new(sqlite);
        let event = claim(&ledger, &input);
        let approvals = ApprovalStore::new(ledger);
        let batch = approvals
            .prepare_pause(&paused, &binding, &[event], prompt(&ticket), None)
            .expect("pause persists before prompt delivery");
        approvals
            .begin_prompt_delivery(ticket.as_str(), &batch)
            .expect("prompt delivery begins");
        approvals
            .prompt_delivered(ticket.as_str(), &batch)
            .expect("prompt delivery commits");
    }

    let sqlite = Arc::new(SqliteStore::open(&path).expect("store reopens"));
    let ledger = IngressLedger::new(sqlite);
    assert_eq!(
        ledger
            .recover_dispatches(&ChannelId::new("telegram"))
            .expect("dispatch recovery succeeds"),
        0
    );
    let approvals = ApprovalStore::new(ledger.clone());
    let with_added_admin = approvals
        .recover(
            &ChannelId::new("telegram"),
            &[actor("old-admin"), actor("new-admin")],
        )
        .expect("approval restores");
    assert_eq!(with_added_admin.pending.len(), 1);
    assert_eq!(
        with_added_admin.pending[0].binding.administrators,
        vec![actor("old-admin")],
        "new authority is not retroactive"
    );
    let revoked = approvals
        .recover(&ChannelId::new("telegram"), &[actor("new-admin")])
        .expect("approval restores under current policy");
    let restored = revoked.pending.into_iter().next().expect("one pause");
    assert!(restored.binding.administrators.is_empty());
    assert!(matches!(
        route(
            Some(&restored.binding),
            &envelope("wrong", "new-admin", format!("/approve {ticket}"))
        ),
        Routed::NotYours { .. }
    ));
    assert!(matches!(
        route(
            Some(&restored.binding),
            &envelope("right", "requester", format!("/approve {ticket}"))
        ),
        Routed::Decides { .. }
    ));

    let resolution = envelope("resolution", "requester", format!("/approve {ticket}"));
    let resolution_event = claim(&ledger, &resolution);
    assert!(approvals
        .begin_resolution(ticket.as_str(), &resolution_event)
        .expect("resolution claim succeeds"));
    assert!(!approvals
        .begin_resolution(ticket.as_str(), &resolution_event)
        .expect("duplicate claim is checked"));
    let original_event = IngressEventKey {
        channel_id: ChannelId::new("telegram"),
        event_id: Arc::from("original"),
    };
    let terminal = ledger
        .prepare_reply(
            &[original_event.clone(), resolution_event],
            prompt(&ticket),
            Settlement::Handled,
        )
        .expect("terminal reply persists");
    ledger
        .begin_delivery(&terminal)
        .expect("terminal delivery starts");
    ledger
        .delivery_succeeded_consuming(&terminal, Some(ticket.as_str()))
        .expect("settlement and resolution consumption commit together");
    assert_eq!(
        ledger.state(&original_event).expect("state reads"),
        DeliveryState::Handled
    );
    assert!(approvals
        .recover(&ChannelId::new("telegram"), &[])
        .expect("post-resolution recovery succeeds")
        .pending
        .is_empty());
    drop(approvals);
    drop(ledger);
    let _ = std::fs::remove_file(path);
}

#[test]
fn prompt_crash_windows_are_conservative() {
    let sqlite = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(sqlite);
    let approvals = ApprovalStore::new(ledger.clone());
    let ticket = ApprovalTicket::parse("crashwindow").expect("fixture ticket");
    let input = envelope("crash-original", "requester", "protected action");
    let event = claim(&ledger, &input);
    let paused = paused(&ticket);
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval-gated-call"),
        actor("requester"),
        Some(1),
    )
    .expect("instance matches ticket");
    let batch = approvals
        .prepare_pause(
            &paused,
            &binding,
            std::slice::from_ref(&event),
            prompt(&ticket),
            None,
        )
        .expect("pause persists");
    let before_send = approvals
        .recover(&ChannelId::new("telegram"), &[])
        .expect("known-not-sent prompt recovers");
    assert_eq!(before_send.prompts.len(), 1);
    assert!(before_send.pending.is_empty());

    approvals
        .begin_prompt_delivery(ticket.as_str(), &batch)
        .expect("delivery starts");
    ledger
        .recover_dispatches(&ChannelId::new("telegram"))
        .expect("restart classifies uncertain delivery");
    assert_eq!(
        ledger.state(&event).expect("state reads"),
        DeliveryState::DeliveryAmbiguous
    );
    assert!(approvals.recover(&ChannelId::new("telegram"), &[]).is_err());
}

#[test]
fn resolution_replays_only_before_an_external_action_starts() {
    let sqlite = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(Arc::clone(&sqlite));
    let approvals = ApprovalStore::new(ledger.clone());
    let ticket = ApprovalTicket::parse("resolutioncrash").expect("fixture ticket");
    let original = claim(
        &ledger,
        &envelope("resolution-original", "requester", "protected action"),
    );
    let paused = paused(&ticket);
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval-gated-call"),
        actor("requester"),
        None,
    )
    .expect("instance matches ticket");
    let prompt_batch = approvals
        .prepare_pause(
            &paused,
            &binding,
            std::slice::from_ref(&original),
            prompt(&ticket),
            None,
        )
        .expect("pause persists");
    approvals
        .begin_prompt_delivery(ticket.as_str(), &prompt_batch)
        .expect("prompt starts");
    approvals
        .prompt_delivered(ticket.as_str(), &prompt_batch)
        .expect("prompt completes");
    let resolution = claim(
        &ledger,
        &envelope(
            "resolution-answer",
            "requester",
            format!("/approve {ticket}"),
        ),
    );
    assert!(approvals
        .begin_resolution(ticket.as_str(), &resolution)
        .expect("resolution claims"));

    ledger
        .recover_dispatches(&ChannelId::new("telegram"))
        .expect("pre-action crash recovers");
    assert_eq!(
        approvals
            .recover(&ChannelId::new("telegram"), &[])
            .expect("pre-action resolution is replayable")
            .pending
            .len(),
        1
    );
    ledger
        .claim_next(&ChannelId::new("telegram"))
        .expect("queue reads")
        .expect("resolution replays");
    assert!(approvals
        .begin_resolution(ticket.as_str(), &resolution)
        .expect("replayed resolution claims"));
    let journal = ledger
        .run_journal_for(vec![original, resolution])
        .expect("journal binds approval group");
    journal
        .action_started("external-side-effect:gated-call")
        .expect("pre-action boundary commits");
    ledger
        .recover_dispatches(&ChannelId::new("telegram"))
        .expect("post-action crash classifies ambiguity");
    assert!(approvals.recover(&ChannelId::new("telegram"), &[]).is_err());
}

#[test]
fn downtime_expiry_is_unanswered_and_settled() {
    let sqlite = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(sqlite);
    let approvals = ApprovalStore::new(ledger.clone());
    let ticket = ApprovalTicket::parse("expiredpause").expect("fixture ticket");
    let original = claim(
        &ledger,
        &envelope("expired-original", "requester", "protected action"),
    );
    let paused = paused(&ticket);
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval-gated-call"),
        actor("requester"),
        Some(1),
    )
    .expect("instance matches ticket");
    let batch = approvals
        .prepare_pause(
            &paused,
            &binding,
            std::slice::from_ref(&original),
            prompt(&ticket),
            None,
        )
        .expect("pause persists");
    approvals
        .begin_prompt_delivery(ticket.as_str(), &batch)
        .expect("prompt starts");
    approvals
        .prompt_delivered(ticket.as_str(), &batch)
        .expect("prompt completes");
    let restored = approvals
        .recover(&ChannelId::new("telegram"), &[])
        .expect("expired pause still restores for explicit resolution");
    assert_eq!(restored.pending[0].expires_at, Some(1));
    assert!(approvals
        .begin_unanswered(ticket.as_str())
        .expect("expiry claims once"));
    approvals
        .abort_unanswered(ticket.as_str())
        .expect("failed expiry evidence restores the pause");
    assert_eq!(
        approvals
            .recover(&ChannelId::new("telegram"), &[])
            .expect("restored expiry remains pending")
            .pending
            .len(),
        1
    );
    assert!(approvals
        .begin_unanswered(ticket.as_str())
        .expect("expiry can be retried after rollback"));
    approvals
        .consume_unanswered(ticket.as_str(), std::slice::from_ref(&original))
        .expect("unanswered lifecycle and ingress settle together");
    assert_eq!(
        ledger.state(&original).expect("state reads"),
        DeliveryState::Handled
    );
}

#[test]
fn approval_resolution_audit_is_idempotent_but_not_confusable() {
    let store = SqliteStore::open_in_memory().expect("store opens");
    let event = SafetyEvent::new(
        SafetyEventKind::ApprovalResolved,
        SafetyOutcome::Approved,
        "tool external-side-effect",
    )
    .with_interruption(InterruptionId::new("approval-gated-call"))
    .with_approval_instance(ApprovalInstanceId::new("auditinstance"))
    .with_approval_actors(
        actor("requester").into_principal(),
        Some(actor("requester").into_principal()),
    );
    store.append(&event).expect("first resolution appends");
    store
        .append(&event)
        .expect("exact crash replay is idempotent");
    assert_eq!(store.recent(10).expect("audit reads").len(), 1);
    let conflict = SafetyEvent::new(
        SafetyEventKind::ApprovalResolved,
        SafetyOutcome::Rejected,
        "tool external-side-effect",
    )
    .with_interruption(InterruptionId::new("approval-gated-call"))
    .with_approval_instance(ApprovalInstanceId::new("auditinstance"))
    .with_approval_actors(
        actor("requester").into_principal(),
        Some(actor("requester").into_principal()),
    );
    assert!(store.append(&conflict).is_err());
}
