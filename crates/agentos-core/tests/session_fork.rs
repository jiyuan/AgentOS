//! Roadmap X6: a conversation can be branched.
//!
//! `Session::fork` seeds a child conversation from a prefix of a parent's log.
//! The property that matters is not "the items were copied" but "the child's
//! *projection* equals the projection of the parent's prefix" — P2's projection
//! is what the model actually sees, and a compaction checkpoint names the span
//! it hides by absolute position, so a copy that shifted positions would give
//! the child a different conversation than the one it branched from.
//!
//! Both the generic default implementation and SQLite's single-statement
//! override are exercised, and one test asserts they agree: an override that
//! quietly diverges from the trait's documented contract is the failure this
//! shape of API invites.

use agentos_core::memory::{InMemorySession, SqliteStore};
use agentos_core::prompt;
use agentos_core::spill::{SpillLocator, SpillSource, SpillStore, SPILL_LOCATOR_KEY};
use agentos_interfaces::session::{Item, Session, Transcript};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Message, MessageRole, Principal, RunId, ToolCallId,
};

/// A conversation as a principal. Forking is keyed on the conversation name,
/// so the agent and channel are fixed here and only the conversation varies —
/// which is also what makes `a_fork_onto_itself_is_refused` still meaningful.
fn conversation(name: &str) -> Principal {
    Principal::conversation(
        AgentId::new("fork-agent"),
        ChannelId::new("telegram"),
        ConversationId::new(name),
    )
}
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

/// A private directory for one test's spill root. Mirrors `tests/sandbox.rs`:
/// no dev-dependency for something this small, and the name says which test
/// left it behind.
fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "agentos-fork-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir is creatable");
    dir
}

fn item(role: MessageRole, text: &str) -> Item {
    Item {
        message: Message::text(role, text),
        metadata: BTreeMap::new(),
    }
}

/// A conversation with a compaction checkpoint in it: four turns, then a
/// checkpoint hiding the first two, then two more turns.
fn transcript_with_a_checkpoint() -> Vec<Item> {
    vec![
        item(MessageRole::User, "one"),
        item(MessageRole::Assistant, "two"),
        item(MessageRole::User, "three"),
        item(MessageRole::Assistant, "four"),
        prompt::checkpoint(
            Message::text(MessageRole::Assistant, "summary of one and two"),
            0,
            1,
        ),
        item(MessageRole::User, "five"),
        item(MessageRole::Assistant, "six"),
    ]
}

fn projection_of(items: &[Item]) -> Vec<Arc<str>> {
    let transcript = Transcript {
        items: items.to_vec(),
    };
    prompt::visible(&transcript)
        .into_iter()
        .map(|item| Arc::clone(&item.message.content))
        .collect()
}

async fn seeded_parent(session: &dyn Session, items: Vec<Item>) -> Principal {
    let parent = conversation("chat:parent");
    session
        .append(&parent, items)
        .await
        .expect("the parent transcript is written");
    parent
}

/// The exit condition, on the generic implementation every backend inherits.
#[tokio::test]
async fn a_forked_child_projects_the_parents_prefix() {
    let session = InMemorySession::default();
    let items = transcript_with_a_checkpoint();
    let parent = seeded_parent(&session, items.clone()).await;
    let child = conversation("chat:child");

    let seeded = session
        .fork(&parent, 6, &child)
        .await
        .expect("a fresh child accepts a seed");
    assert_eq!(seeded, 6);

    let forked = session.load(&child).await.expect("the child loads");
    assert_eq!(forked.items, items[..6].to_vec());
    // The point of the test: what the *model* sees. The checkpoint at index 4
    // still hides items 0 and 1, so the child's view is the summary plus the
    // turns after it — the same view the parent had at that point.
    assert_eq!(
        projection_of(&forked.items),
        projection_of(&items[..6]),
        "the child's projection must equal the projection of the parent's prefix"
    );
    assert_eq!(
        projection_of(&forked.items),
        vec![
            Arc::from("three"),
            Arc::from("four"),
            Arc::from("summary of one and two"),
            Arc::from("five"),
        ] as Vec<Arc<str>>
    );
}

/// A checkpoint's positions are absolute, so a fork that dropped a head would
/// leave it pointing at the wrong items. This pins the reason `boundary` is a
/// length rather than a range.
#[tokio::test]
async fn a_checkpoints_shadow_still_names_the_right_items_in_the_child() {
    let session = InMemorySession::default();
    let items = transcript_with_a_checkpoint();
    let parent = seeded_parent(&session, items.clone()).await;
    let child = conversation("chat:child");
    session.fork(&parent, 7, &child).await.expect("seeded");

    let forked = session.load(&child).await.expect("the child loads");
    let hidden = projection_of(&forked.items);
    assert!(
        !hidden.iter().any(|content| content.as_ref() == "one"),
        "the child shows text its parent had folded away: {hidden:?}"
    );
    assert_eq!(
        hidden,
        vec![
            Arc::from("three"),
            Arc::from("four"),
            Arc::from("summary of one and two"),
            Arc::from("five"),
            Arc::from("six"),
        ] as Vec<Arc<str>>,
        "the whole log was copied, so the child's view must equal the parent's"
    );
}

#[tokio::test]
async fn a_boundary_past_the_end_seeds_what_exists_and_says_how_much() {
    // The delegation case: the parent names a point from its in-memory
    // transcript, and the store holds less of it because the turn in flight is
    // not persisted yet.
    let session = InMemorySession::default();
    let parent = seeded_parent(&session, vec![item(MessageRole::User, "only")]).await;
    let child = conversation("chat:child");

    let seeded = session
        .fork(&parent, 99, &child)
        .await
        .expect("an over-long boundary is not an error");
    assert_eq!(seeded, 1);
    assert_eq!(session.load(&child).await.expect("loads").items.len(), 1);
}

#[tokio::test]
async fn a_zero_boundary_branches_an_empty_conversation() {
    let session = InMemorySession::default();
    let parent = seeded_parent(&session, transcript_with_a_checkpoint()).await;
    let child = conversation("chat:child");

    assert_eq!(session.fork(&parent, 0, &child).await.expect("ok"), 0);
    assert!(session.load(&child).await.expect("loads").items.is_empty());
}

#[tokio::test]
async fn forking_onto_a_conversation_with_history_is_refused() {
    // Appending a prefix onto an existing log interleaves two histories and
    // invalidates every checkpoint position in both. Refusing is the whole
    // reason a sub-agent can be seeded exactly once.
    let session = InMemorySession::default();
    let parent = seeded_parent(&session, transcript_with_a_checkpoint()).await;
    let child = conversation("chat:child");
    session.fork(&parent, 4, &child).await.expect("first seed");

    let error = session
        .fork(&parent, 4, &child)
        .await
        .expect_err("a second seed must be refused");
    assert!(
        error.to_string().contains("already holds"),
        "unhelpful refusal: {error}"
    );
    assert_eq!(
        session.load(&child).await.expect("loads").items.len(),
        4,
        "the refused fork must not have appended anything"
    );
}

#[tokio::test]
async fn forking_a_conversation_onto_itself_is_refused() {
    let session = InMemorySession::default();
    let parent = seeded_parent(&session, transcript_with_a_checkpoint()).await;

    let error = session
        .fork(&parent, 4, &parent)
        .await
        .expect_err("a self-fork must be refused");
    assert!(error.to_string().contains("onto itself"), "{error}");
    assert_eq!(session.load(&parent).await.expect("loads").items.len(), 7);
}

/// SQLite copies the rows without moving them through memory. It has to reach
/// the same answer as the implementation it overrides, on the same input.
#[tokio::test]
async fn the_sqlite_override_agrees_with_the_default_implementation() {
    let items = transcript_with_a_checkpoint();
    let generic = InMemorySession::default();
    let sqlite = SqliteStore::open_in_memory().expect("an in-memory store opens");
    let parent = conversation("chat:parent");
    let child = conversation("chat:child");

    for store in [&generic as &dyn Session, &sqlite as &dyn Session] {
        store
            .append(&parent, items.clone())
            .await
            .expect("the parent is written");
    }

    let by_default = generic.fork(&parent, 6, &child).await.expect("seeded");
    let by_sqlite = sqlite.fork(&parent, 6, &child).await.expect("seeded");
    assert_eq!(by_default, by_sqlite);
    assert_eq!(
        generic.load(&child).await.expect("loads").items,
        sqlite.load(&child).await.expect("loads").items
    );

    // ...and the refusals, which a hand-written override is just as likely to
    // forget as the copy itself.
    assert!(sqlite.fork(&parent, 6, &child).await.is_err());
    assert!(sqlite.fork(&parent, 6, &parent).await.is_err());
}

/// The child's ordinals carry over from the parent, so its next append
/// continues the log rather than colliding with a copied row.
#[tokio::test]
async fn a_forked_child_can_be_appended_to() {
    let sqlite = SqliteStore::open_in_memory().expect("opens");
    let parent = seeded_parent(&sqlite, transcript_with_a_checkpoint()).await;
    let child = conversation("chat:child");
    sqlite.fork(&parent, 5, &child).await.expect("seeded");

    sqlite
        .append(&child, vec![item(MessageRole::User, "its own turn")])
        .await
        .expect("the child accepts its own history");

    let forked = sqlite.load(&child).await.expect("loads");
    assert_eq!(forked.items.len(), 6);
    assert_eq!(forked.items[5].message.content.as_ref(), "its own turn");
    // The parent is untouched: a branch diverges, it does not write back.
    assert_eq!(sqlite.load(&parent).await.expect("loads").items.len(), 7);
}

/// Spilled tool output is not copied, and does not need to be: a locator is an
/// absolute path into a run-keyed store, so the child's inherited item resolves
/// to the artifact the parent's run wrote.
#[tokio::test]
async fn spill_locators_in_a_seeded_prefix_resolve_from_the_child() {
    let root = temp_dir("spill");
    let store = SpillStore::new(root.join("spill"));
    let run_id = RunId::new("run-parent");
    let call_id = ToolCallId::new("call-1");
    let saved = store
        .save_text(
            &SpillSource {
                run_id: &run_id,
                tool_name: "shell",
                call_id: &call_id,
            },
            "the full output the parent's tool produced",
        )
        .await
        .expect("the spill is written");

    let mut spilled = Message::text(MessageRole::Tool, "preview…");
    spilled.tool_call_id = Some(call_id);
    spilled.metadata.insert(
        Arc::from(SPILL_LOCATOR_KEY),
        Value::String(saved.locator.as_str().to_owned()),
    );

    let session = InMemorySession::default();
    let parent = seeded_parent(
        &session,
        vec![
            item(MessageRole::User, "run it"),
            Item {
                message: spilled,
                metadata: BTreeMap::new(),
            },
        ],
    )
    .await;
    let child = conversation("chat:child");
    session.fork(&parent, 2, &child).await.expect("seeded");

    let forked = session.load(&child).await.expect("loads");
    let locator = forked.items[1]
        .message
        .metadata
        .get(SPILL_LOCATOR_KEY)
        .and_then(Value::as_str)
        .expect("the seeded item still carries its locator");
    assert_eq!(locator, saved.locator.as_str());

    // The artifact is readable from the child's side with nothing copied: one
    // file on disk, referenced by two conversations. Resolved through the
    // store, because a locator is not a path (M7 / `SPILL-001`).
    let parsed = SpillLocator::parse(locator).expect("the locator parses");
    let mut content = String::new();
    std::io::Read::read_to_string(
        &mut store.open(&parsed).expect("the artifact resolves"),
        &mut content,
    )
    .expect("the artifact reads");
    assert_eq!(content, "the full output the parent's tool produced");
    // Read the run directory off the locator rather than rebuilding its name:
    // the store sanitizes path segments, and this test is about the fork, not
    // about that rule.
    let run_dir = store.root().join(parsed.run());
    let files: Vec<_> = std::fs::read_dir(&run_dir)
        .expect("the run directory exists")
        .collect();
    assert_eq!(files.len(), 1, "forking must not duplicate spilled output");
}

// ---------------------------------------------------------------------------
// The delegation path: a definition that asks to be seeded actually is.
//
// The primitive above is only half of X6. A `seed_from_parent` flag that
// nothing honours is dead code that passes every test, so these two drive a
// real delegation through the run loop and assert on what the child's
// conversation holds afterwards.
// ---------------------------------------------------------------------------

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::runner::{run_envelope, RunnerDeps};
use agentos_core::subagents::{child_input_envelope, SubAgentDefinition, SubAgentRegistry};
use agentos_interfaces::orchestrator::{
    Orchestrator, OrchestratorError, Plan, RunContext, SubAgentSpec,
};
use agentos_proto::Envelope;
use async_trait::async_trait;

const PARENT_CONVERSATION: &str = "chat:seeding";

/// Delegates on user input, replies once the child's result lands.
struct DelegatingParent;

#[async_trait]
impl Orchestrator for DelegatingParent {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let Some(item) = ctx.state.transcript.items.last() else {
            return Ok(Plan::Reply(Message::text(MessageRole::Assistant, "")));
        };
        match item.message.role {
            MessageRole::User => Ok(Plan::Delegate(SubAgentSpec {
                agent_id: AgentId::new("child"),
                policy_id: Arc::from("child-policy"),
                metadata: BTreeMap::new(),
            })),
            // Echo the child's answer out, so a test can assert on what the
            // child saw rather than on rows in the session store.
            _ => Ok(Plan::Reply(Message::text(
                MessageRole::Assistant,
                Arc::clone(&item.message.content),
            ))),
        }
    }
}

/// Reports how much conversation it was handed, so the assertion is about what
/// the child could actually see rather than about rows in a table.
struct CountingChild;

#[async_trait]
impl Orchestrator for CountingChild {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Reply(Message::text(
            MessageRole::Assistant,
            format!("saw {} items", ctx.state.transcript.items.len()),
        )))
    }
}

fn delegating_policy() -> Policy {
    Policy {
        rules: vec![PolicyRule {
            action: PolicyAction::Delegate,
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        }],
        default_decision: PolicyVerb::Deny,
    }
}

/// The parent's session key: agent `parent`, channel `test-channel`. Same
/// three components `parent_principal` reads back out of the run.
fn seeding_parent() -> Principal {
    Principal::conversation(
        AgentId::new("parent"),
        ChannelId::new("test-channel"),
        ConversationId::new(PARENT_CONVERSATION),
    )
}

/// The child's, which is the one that matters: the sub-agent runs as `child`
/// on its own `subagent:child` channel, so the fork has to land under exactly
/// that key or the seed goes somewhere the sub-agent never loads from — and
/// nothing would say so, because an unseeded sub-agent just starts empty.
fn seeding_child() -> Principal {
    let mut parent = RunState::new(RunId::new("seeding-run"), AgentId::new("parent"));
    parent.transcript.items.push(Item {
        message: Message::text(MessageRole::User, "ask the child"),
        metadata: BTreeMap::from([
            (
                Arc::from("channel_id"),
                Value::String("test-channel".to_owned()),
            ),
            (
                Arc::from("conversation_id"),
                Value::String(PARENT_CONVERSATION.to_owned()),
            ),
        ]),
    });
    let envelope = child_input_envelope(
        &SubAgentSpec {
            agent_id: AgentId::new("child"),
            policy_id: Arc::from("child-policy"),
            metadata: BTreeMap::new(),
        },
        &parent,
    );
    Principal::conversation(
        AgentId::new("child"),
        envelope.channel_id,
        envelope.conversation_id,
    )
}

async fn delegate_with_seeding(seed_from_parent: bool) -> (Arc<InMemorySession>, Arc<str>) {
    let session = Arc::new(InMemorySession::default());
    let parent_conversation = seeding_parent();
    session
        .append(
            &parent_conversation,
            vec![
                item(MessageRole::User, "we discussed the deploy plan"),
                item(MessageRole::Assistant, "and agreed to stage it"),
            ],
        )
        .await
        .expect("the parent has history");

    let mut registry = SubAgentRegistry::new().with_session(session.clone());
    registry.register(
        SubAgentDefinition::new(
            AgentId::new("child"),
            "child-policy",
            Arc::new(CountingChild),
            Policy {
                rules: Vec::new(),
                default_decision: PolicyVerb::Deny,
            },
        )
        .with_seed_from_parent(seed_from_parent),
    );

    let parent = DelegatingParent;
    let policy = delegating_policy();
    let deps = RunnerDeps {
        orchestrator: &parent,
        session: session.as_ref(),
        memory_manager: None,
        hooks: None,
        max_turns: 8,
        active_agent: AgentId::new("parent"),
        tools: None,
        trace_sink: None,
        task_workspace: None,
        policy: &policy,
        subagents: Some(&registry),
        input_guardrails: &[],
        output_guardrails: &[],
        tool_guardrails: &[],
        stream_sink: None,
        content_limits: Default::default(),
        compaction: Default::default(),
        cancel: Default::default(),
        steering: None,
        run_journal: None,
        safety_log: None,
        delegated_authority: None,
    };

    let outcome = run_envelope(
        Envelope {
            channel_id: ChannelId::new("test-channel"),
            conversation_id: parent_conversation.conversation.clone(),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, "ask the child"),
            metadata: BTreeMap::new(),
        },
        RunId::new("seeding-run"),
        &deps,
    )
    .await
    .expect("the delegation completes");

    let reply = match outcome {
        agentos_core::runner::RunOutcome::Finished { output, .. } => output.message.content,
        agentos_core::runner::RunOutcome::Paused(_) => panic!("no approval was expected"),
    };
    (session, reply)
}

#[tokio::test]
async fn a_seeded_subagent_starts_from_the_parents_conversation() {
    let (session, _) = delegate_with_seeding(true).await;

    // `child_input_envelope` derives this from the parent's conversation id.
    let child_conversation = seeding_child();
    let child = session
        .load(&child_conversation)
        .await
        .expect("the child conversation exists");

    let contents: Vec<&str> = child
        .items
        .iter()
        .map(|item| item.message.content.as_ref())
        .collect();
    assert!(
        contents.contains(&"we discussed the deploy plan"),
        "the child was not seeded with the parent's history: {contents:?}"
    );
    assert!(contents.contains(&"and agreed to stage it"));
}

#[tokio::test]
async fn an_unseeded_subagent_starts_from_nothing_but_its_own_input() {
    // The default, and the behaviour every sub-agent had before X6.
    let (session, reply) = delegate_with_seeding(false).await;

    let child_conversation = seeding_child();
    let child = session
        .load(&child_conversation)
        .await
        .expect("the child conversation exists");
    let contents: Vec<&str> = child
        .items
        .iter()
        .map(|item| item.message.content.as_ref())
        .collect();
    assert!(
        !contents.contains(&"we discussed the deploy plan"),
        "an unseeded sub-agent must not receive the parent's history: {contents:?}"
    );
    // Its own input plus its reply, and nothing else: the child planned over a
    // one-item transcript.
    assert!(
        reply.contains("saw 1 items"),
        "unseeded child saw more than its own input: {reply}"
    );
}

#[tokio::test]
async fn a_seeded_subagent_is_seeded_once_not_on_every_turn() {
    // A sub-agent's conversation id is stable, so a second delegation finds
    // history already there. Re-seeding would duplicate the parent's log into
    // the child on every turn of a long conversation.
    let (session, _) = delegate_with_seeding(true).await;
    let child_conversation = seeding_child();
    let after_first = session
        .load(&child_conversation)
        .await
        .expect("loads")
        .items
        .len();

    let seeded_again = session
        .fork(&seeding_parent(), 99, &child_conversation)
        .await;
    assert!(
        seeded_again.is_err(),
        "a second seed of the same child must be refused"
    );
    assert_eq!(
        session
            .load(&child_conversation)
            .await
            .expect("loads")
            .items
            .len(),
        after_first
    );
}
