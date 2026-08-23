//! M6 / `STATE-001` deliverable 5 and M3 deliverable 2: `/clear` starts an
//! epoch, it does not delete, and the epoch belongs to the participant who
//! typed it.
//!
//! [ADR-0006](../../../docs/adr/0006-CLEAR_EPOCH.md). `AGENTS.md`, `DESIGN.md`
//! and `docs/ARCHITECTURE.md` all said the session log is append-only and that
//! compaction, elision, and fork are projections over it. That was true of
//! those three and false of `/clear`, which ran `DELETE FROM session_items` —
//! so the one operation a user reaches for most casually was the only one that
//! destroyed the record, and the documented invariant was false at exactly the
//! point where it mattered.

mod support;

use agentos_core::memory::SqliteStore;
use agentos_interfaces::session::{Item, Session};
use agentos_proto::{AgentId, ChannelId, ConversationId, Message, MessageRole, Principal};
use std::collections::BTreeMap;

const CONVERSATION: &str = "epoch-conversation";

fn item(text: &str) -> Item {
    Item {
        message: Message::text(MessageRole::User, text),
        metadata: BTreeMap::new(),
    }
}

/// The conversation itself — no sender. What the TUI clears as, and what a
/// migrated pre-principal epoch becomes.
fn conversation(name: &str) -> Principal {
    Principal::conversation(
        AgentId::new("epoch-agent"),
        ChannelId::new("telegram"),
        ConversationId::new(name),
    )
}

async fn seeded(store: &SqliteStore, principal: &Principal, texts: &[&str]) {
    store
        .append(principal, texts.iter().map(|text| item(text)).collect())
        .await
        .expect("appending to a session works");
}

/// Reads the log rather than the projection, because the whole question is
/// whether `load` is hiding rows or whether they are gone.
fn contents(store: &SqliteStore) -> Vec<String> {
    store
        .session_log(&conversation(CONVERSATION))
        .expect("the session log is readable")
        .into_iter()
        .map(|item| item.message.content.as_ref().to_owned())
        .collect()
}

#[tokio::test]
async fn clear_hides_the_history_and_keeps_it() {
    let tree = support::temp_tree("epoch-hides");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let conv = conversation(CONVERSATION);
    seeded(&store, &conv, &["first", "second"]).await;

    let hidden = store.clear_session(&conv).expect("clear works");
    assert_eq!(hidden, 2, "clear reports what it hid");

    assert!(
        store
            .load(&conv)
            .await
            .expect("loading works")
            .items
            .is_empty(),
        "the model sees a fresh conversation"
    );
    assert_eq!(
        contents(&store),
        vec!["first".to_owned(), "second".to_owned()],
        "and the store still holds what happened"
    );
}

#[tokio::test]
async fn history_after_a_clear_starts_from_the_epoch() {
    let tree = support::temp_tree("epoch-resumes");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let conv = conversation(CONVERSATION);
    seeded(&store, &conv, &["before"]).await;
    store.clear_session(&conv).expect("clear works");
    seeded(&store, &conv, &["after"]).await;

    let visible: Vec<_> = store
        .load(&conv)
        .await
        .expect("loading works")
        .items
        .into_iter()
        .map(|item| item.message.content.as_ref().to_owned())
        .collect();
    assert_eq!(visible, vec!["after".to_owned()]);
    assert_eq!(contents(&store).len(), 2, "nothing was removed");

    // A second clear hides only what has become visible since the first, which
    // is the number a user is told about.
    assert_eq!(store.clear_session(&conv).expect("clear works"), 1);
    assert_eq!(
        store.clear_session(&conv).expect("clear works"),
        0,
        "clearing an already-cleared conversation hides nothing"
    );
}

#[tokio::test]
async fn a_fork_after_a_clear_copies_what_the_parent_can_see() {
    // Every reader of `session_items` has to respect the epoch. Fork is the
    // one that would otherwise resurrect history into a child — which is the
    // failure mode ADR-0006 names, and the reason the filter lives beside the
    // queries rather than at the call sites.
    let tree = support::temp_tree("epoch-fork");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let conv = conversation(CONVERSATION);
    let child = conversation("epoch-child");
    seeded(&store, &conv, &["forgotten"]).await;
    store.clear_session(&conv).expect("clear works");
    seeded(&store, &conv, &["remembered"]).await;

    let seeded_count = store.fork(&conv, 1, &child).await.expect("fork works");
    assert_eq!(seeded_count, 1);
    let inherited: Vec<_> = store
        .load(&child)
        .await
        .expect("loading works")
        .items
        .into_iter()
        .map(|item| item.message.content.as_ref().to_owned())
        .collect();
    assert_eq!(inherited, vec!["remembered".to_owned()]);
}

#[tokio::test]
async fn purge_is_the_operation_that_actually_deletes() {
    // The legitimate requirement `/clear` does not serve. A separate method
    // with a separate name, reachable from the host's shell rather than from a
    // message in the conversation it would destroy.
    let tree = support::temp_tree("epoch-purge");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let conv = conversation(CONVERSATION);
    seeded(&store, &conv, &["first", "second"]).await;
    store.clear_session(&conv).expect("clear works");

    assert_eq!(store.purge_session(&conv).expect("purge works"), 2);
    assert!(contents(&store).is_empty(), "the rows are gone");

    // The epoch went with them, so a conversation that starts again under the
    // same id is not silently truncated by a marker nothing points at.
    seeded(&store, &conv, &["fresh start"]).await;
    assert_eq!(
        store.load(&conv).await.expect("loading works").items.len(),
        1
    );
}

/// The acceptance criterion M3 deliverable 2 was waiting on: "a second group
/// participant cannot approve or clear the initiator's state."
///
/// Alice and Bob speak in one conversation, so they share one transcript.
/// Alice clears. Bob's next load is unaffected — and the log is untouched for
/// both of them, because `/clear` still deletes nothing.
#[tokio::test]
async fn one_participants_clear_does_not_clear_anothers_view() {
    let tree = support::temp_tree("epoch-participants");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let alice = conversation(CONVERSATION).with_sender("alice");
    let bob = conversation(CONVERSATION).with_sender("bob");

    seeded(&store, &alice, &["shared one", "shared two"]).await;
    assert_eq!(store.load(&bob).await.expect("load").items.len(), 2);

    assert_eq!(store.clear_session(&alice).expect("clear works"), 2);
    assert!(
        store.load(&alice).await.expect("load").items.is_empty(),
        "the participant who cleared sees a fresh conversation"
    );
    assert_eq!(
        store.load(&bob).await.expect("load").items.len(),
        2,
        "and nobody else's view moved"
    );
    assert_eq!(contents(&store).len(), 2, "nothing was deleted");

    // What is said next is one conversation, seen from two places.
    seeded(&store, &bob, &["after"]).await;
    assert_eq!(store.load(&alice).await.expect("load").items.len(), 1);
    assert_eq!(store.load(&bob).await.expect("load").items.len(), 3);
}

/// A clear with no sender is the conversation's, and it does move everybody —
/// which is what the TUI writes and what a migrated legacy epoch becomes.
#[tokio::test]
async fn a_conversation_wide_clear_moves_every_participant() {
    let tree = support::temp_tree("epoch-conversation-wide");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");
    let conv = conversation(CONVERSATION);
    let alice = conv.clone().with_sender("alice");
    let bob = conv.clone().with_sender("bob");

    seeded(&store, &conv, &["one", "two"]).await;
    store.clear_session(&conv).expect("clear works");

    for (who, participant) in [("alice", &alice), ("bob", &bob)] {
        assert!(
            store
                .load(participant)
                .await
                .expect("load")
                .items
                .is_empty(),
            "{who} is behind the conversation-wide epoch"
        );
    }

    // And a participant clearing afterwards still moves only their own line.
    seeded(&store, &conv, &["three"]).await;
    store.clear_session(&alice).expect("clear works");
    assert!(store.load(&alice).await.expect("load").items.is_empty());
    assert_eq!(store.load(&bob).await.expect("load").items.len(), 1);
}

/// Two channels' conversation `42`, and two agents' — the collision this
/// milestone existed to remove, checked at the session layer.
#[tokio::test]
async fn the_same_conversation_number_elsewhere_is_a_different_transcript() {
    let tree = support::temp_tree("epoch-collision");
    let store = SqliteStore::open(tree.path().join("store.sqlite")).expect("the store opens");

    let telegram = Principal::conversation(
        AgentId::new("main"),
        ChannelId::new("telegram"),
        ConversationId::new("42"),
    );
    let feishu = Principal::conversation(
        AgentId::new("main"),
        ChannelId::new("feishu"),
        ConversationId::new("42"),
    );
    let other_agent = Principal::conversation(
        AgentId::new("second"),
        ChannelId::new("telegram"),
        ConversationId::new("42"),
    );

    seeded(&store, &telegram, &["telegram only"]).await;
    for (name, elsewhere) in [("feishu", &feishu), ("another agent", &other_agent)] {
        assert!(
            store.load(elsewhere).await.expect("load").items.is_empty(),
            "{name}'s conversation 42 must not see telegram's"
        );
    }

    // And clearing one leaves the others alone, since they are not one log.
    seeded(&store, &feishu, &["feishu only"]).await;
    store.clear_session(&feishu).expect("clear works");
    assert_eq!(store.load(&telegram).await.expect("load").items.len(), 1);
}
