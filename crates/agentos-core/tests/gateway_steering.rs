//! GW-003: every queue transition carries the complete inbound envelope.

use agentos_core::gateway::{Admitted, Inbox, IngressLedger, Requeued, Settlement};
use agentos_core::memory::SqliteStore;
use agentos_core::r#loop::Steering;
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole, INGRESS_ID_KEY};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn envelope(sender: &str, event_id: &str, content: &str) -> Envelope {
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from(INGRESS_ID_KEY), serde_json::Value::from(event_id));
    metadata.insert(
        Arc::from("transport.signature"),
        serde_json::Value::from(format!("signature-{event_id}")),
    );
    Envelope {
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new("shared-conversation"),
        sender: Arc::from(sender),
        message: Message::text(MessageRole::User, content),
        metadata,
    }
}

/// AF-007: steering twice, displacement, requeue, and saturation all name the
/// original event rather than reconstructing it from the active turn.
#[test]
fn displaced_envelope_preserves_principal_and_ingress_id() {
    let first_steer = envelope("alice", "event-1", "first steer");
    let second_steer = envelope("bob", "event-2", "second steer");
    let queued = envelope("carol", "event-3", "already queued");
    let refused = envelope("dave", "event-4", "queue is full");

    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let ledger = IngressLedger::new(store);
    for input in [&first_steer, &second_steer, &queued, &refused] {
        ledger.admit(input).expect("event is durable");
        ledger
            .mark_acknowledged(
                &input.channel_id,
                &input.ingress_id().expect("fixture has ingress id"),
            )
            .expect("event is dispatchable");
    }
    for _ in 0..4 {
        ledger
            .claim_next(&first_steer.channel_id)
            .expect("durable queue reads")
            .expect("one exact event is dispatchable");
    }

    let steering = Steering::bounded(1);
    let mut active = Inbox::bounded(4);
    active.begin(steering.clone(), CancellationToken::new());
    assert_eq!(active.admit(first_steer.clone()), Admitted::NextStep);
    assert_eq!(
        active.admit(second_steer.clone()),
        Admitted::NextStepDisplacing {
            displaced: first_steer.clone()
        },
        "the displaced event keeps Alice's principal and all transport metadata"
    );

    let unclaimed = steering.take_unclaimed();
    assert_eq!(unclaimed.as_slice(), std::slice::from_ref(&second_steer));
    active.end().expect("active steering is returned");
    assert_eq!(active.requeue(second_steer.clone()), Requeued);
    assert_eq!(active.claim(), Some(second_steer));

    let mut saturated = Inbox::bounded(1);
    assert_eq!(saturated.admit(queued.clone()), Admitted::NextTurn);
    assert_eq!(
        saturated.admit(refused.clone()),
        Admitted::Full {
            refused: refused.clone()
        },
        "saturation returns Dave's exact refused event"
    );
    assert_eq!(saturated.claim(), Some(queued));

    ledger
        .settle(
            &first_steer.channel_id,
            &first_steer.ingress_id().expect("fixture has ingress id"),
            Settlement::Refused,
        )
        .expect("the displaced event settles by its own identity");
    let unsettled = ledger
        .unsettled(&first_steer.channel_id)
        .expect("unsettled rows read");
    assert!(
        unsettled
            .iter()
            .all(|event| event.event_id.as_ref() != "event-1"),
        "only the displaced event was settled"
    );
    assert_eq!(
        unsettled
            .iter()
            .map(|event| event.event_id.as_ref())
            .collect::<Vec<_>>(),
        ["event-2", "event-3", "event-4"]
    );
}
