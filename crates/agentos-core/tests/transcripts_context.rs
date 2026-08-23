//! Roadmap T1, continued: goldens for how a run assembles its *context*.
//!
//! `transcripts.rs` pins the loop's shapes — reply, tool call, batch, approval,
//! delegation. These pin the two things that change what the model is shown
//! rather than what the loop does: memory hydration (what gets recalled into a
//! request) and oversized tool output (what gets spilled or elided out of one).
//! Split from `transcripts.rs` when it outgrew the module ceiling; the harness
//! and the re-record command are unchanged.

mod support;

use agentos_core::approve::{Policy, PolicyVerb};
use agentos_core::memory::{
    InMemorySession, MemoryManager, MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility,
    RetrievalStrategy, SqliteStore,
};
use agentos_core::orchestrator::{MaxOrchestrator, MemoryHydrationSettings};
use agentos_core::runner::run_envelope;
use agentos_core::spill::{ContentLimits, SpillLocator, SpillStore, SPILL_LOCATOR_KEY};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::session::Session;
use agentos_interfaces::tool::Tool;
use agentos_proto::{AgentId, ConversationId, MessageRole, RunId};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use support::{
    assistant, bulk_output, max_with, runner_deps, scenario, scenario_golden, tool_call_response,
    tool_policy, user_envelope, BulkTool, ScriptedLlm, BULK_TOOL, CONVERSATION,
};

// ---------------------------------------------------------------------------
// 8. Hydrated memory
// ---------------------------------------------------------------------------

/// Build a manager holding one semantic fact in the run's conversation scope.
async fn manager_with_fact(fact: &str) -> Arc<MemoryManager> {
    let store = Arc::new(SqliteStore::open_in_memory().expect("sqlite opens in memory"));
    let manager = Arc::new(MemoryManager::new_sqlite(store));
    let caller = agentos_core::memory::MemoryCaller {
        agent_id: AgentId::new("golden-agent"),
        task_id: agentos_proto::TaskId::new("golden-task"),
        channel_id: agentos_proto::ChannelId::new("golden-channel"),
        conversation_id: ConversationId::new(CONVERSATION),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        writable_shared_domains: Vec::new(),
        audit_read_access: false,
    };
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Conversation(agentos_proto::Principal::conversation(
            AgentId::new("golden-agent"),
            agentos_proto::ChannelId::new("golden-channel"),
            ConversationId::new(CONVERSATION),
        )),
        MemoryVisibility::Private,
        None,
    );
    manager
        .write_scoped(&caller, scope, json!({ "fact": fact }), BTreeMap::new())
        .await
        .expect("seeding a semantic fact succeeds");
    manager
}

fn hydration_settings() -> MemoryHydrationSettings {
    MemoryHydrationSettings {
        enabled: true,
        max_fragments: 5,
        max_estimated_tokens: 1200,
        // Recency, not Hybrid: this scenario asserts whether hydrated fragments
        // reach the request, not how well retrieval ranks them.
        stores: vec![MemoryStore::Semantic],
        strategy: RetrievalStrategy::Recency,
        shared_domains: Default::default(),
        default_domain: Arc::from("general"),
    }
}

/// Pins today's behavior: hydration runs, selects the fact, and the assembled
/// request does **not** contain it (roadmap finding F1). The golden is expected
/// to change when roadmap item P1 lands a prompt-assembly step — that diff is
/// the point, not a regression.
#[tokio::test]
async fn golden_memory_hydration() {
    let manager = manager_with_fact("The deploy key rotates every 90 days.").await;
    let llm = Arc::new(ScriptedLlm::new([assistant("Noted.")]));
    let orchestrator = MaxOrchestrator::new()
        .with_llm(llm.clone())
        .with_memory_hydrator(manager, hydration_settings());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    let outcome = run_envelope(
        user_envelope("How often does the deploy key rotate?"),
        RunId::new("golden-memory"),
        &deps,
    )
    .await
    .expect("a hydrated run finishes");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    scenario_golden("memory_hydration", &llm, &transcript, &outcome);
}

/// The F1 acceptance test, landed by roadmap item P1: hydrated fragments must
/// appear in the assembled request. Before P1 they were written to
/// `RunContext::memory_fragments`, counted in telemetry, and dropped before the
/// request was built.
#[tokio::test]
async fn hydrated_memory_reaches_the_model() {
    let fact = "The deploy key rotates every 90 days.";
    let manager = manager_with_fact(fact).await;
    let llm = Arc::new(ScriptedLlm::new([assistant("Noted.")]));
    let orchestrator = MaxOrchestrator::new()
        .with_llm(llm.clone())
        .with_memory_hydrator(manager, hydration_settings());
    let session = InMemorySession::default();
    let policy = Policy::default();
    let deps = runner_deps(&orchestrator, &session, &policy, None, None);

    run_envelope(
        user_envelope("How often does the deploy key rotate?"),
        RunId::new("p1-target"),
        &deps,
    )
    .await
    .expect("a hydrated run finishes");

    let requests = llm.requests();
    let request = requests.first().expect("the orchestrator called the model");
    let carried = request
        .messages
        .iter()
        .any(|message| message.content.contains("90 days"));
    assert!(
        carried,
        "the hydrated fact must appear in the assembled request; got: {:#?}",
        request
            .messages
            .iter()
            .map(|message| message.content.as_ref())
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// 9. Oversized tool result: spilled, not destroyed
// ---------------------------------------------------------------------------

/// Roadmap item C2. Before it, output past the inline cap was cut and the
/// remainder destroyed. Now the full text goes to the spill store and the
/// transcript keeps a preview plus a locator, so the golden pins both what the
/// model was told and — by reading the locator back — that nothing was lost.
#[tokio::test]
async fn golden_tool_result_spilled() {
    let llm = Arc::new(ScriptedLlm::new([
        tool_call_response("call-1", BULK_TOOL, r#"{"lines":128}"#),
        assistant("Read the first lines; the rest is on disk."),
    ]));
    let mut tools = ToolRegistry::new();
    tools.register(BulkTool);
    let orchestrator = max_with(llm.clone(), vec![BulkTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[BULK_TOOL], PolicyVerb::Allow);

    let spill_root = support::temp_tree("spill");
    let store = SpillStore::new(spill_root.path());
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    // A cap far below the default, so the scenario spills without a golden
    // carrying 64 KiB of filler.
    deps.content_limits = ContentLimits {
        tool_result_inline_bytes: 512,
        spill: Some(&store),
    };

    let outcome = run_envelope(
        user_envelope("Dump the log for me."),
        RunId::new("golden-spill"),
        &deps,
    )
    .await
    .expect("an oversized tool result finishes the run");

    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");

    // The exit condition, end to end: the locator the model was handed names a
    // file holding the output in full, not the preview it saw.
    let locator = transcript
        .items
        .iter()
        .find_map(|item| item.message.metadata.get(SPILL_LOCATOR_KEY))
        .and_then(Value::as_str)
        .expect("a spilled result records its locator");
    let parsed = SpillLocator::parse(locator).expect("the locator parses");
    let mut recovered = String::new();
    std::io::Read::read_to_string(
        &mut store
            .open(&parsed)
            .expect("the locator names a readable artifact"),
        &mut recovered,
    )
    .expect("the artifact reads");
    assert_eq!(recovered, bulk_output(128));

    // M7 / `SPILL-001`: nothing about the host reaches the transcript, so
    // there is no temp path left to redact before pinning. The golden holds
    // the locator verbatim, which is the point — it is stable across machines
    // because it is not a path.
    assert!(
        !locator.contains(&*spill_root.path().to_string_lossy()),
        "the locator leaked the host spill directory: {locator}"
    );
    support::assert_golden(
        "tool_result_spilled",
        &scenario(&llm, &transcript, &outcome),
    );
}

/// The other half of C2, end to end: once a run is near its context window,
/// the middle of an already-recorded tool result is elided from the *request*
/// while the session log keeps what it always held.
///
/// Not a golden — pinning it would mean storing kilobytes of filler to assert
/// one marker. `prompt::prune`'s unit tests cover the elision rules; this
/// covers that they reach a provider.
#[tokio::test]
async fn elision_reaches_the_model_but_not_the_log() {
    let llm = Arc::new(
        ScriptedLlm::new([
            tool_call_response("call-1", BULK_TOOL, r#"{"lines":128}"#),
            assistant("Summarised from the head and tail."),
        ])
        // Small enough that one spilled result puts the run over the trigger.
        .with_context_budget(1_024),
    );
    let mut tools = ToolRegistry::new();
    tools.register(BulkTool);
    let orchestrator = max_with(llm.clone(), vec![BulkTool.spec()]);
    let session = InMemorySession::default();
    let policy = tool_policy(&[BULK_TOOL], PolicyVerb::Allow);

    let spill_root = support::temp_tree("elision");
    let store = SpillStore::new(spill_root.path());
    let mut deps = runner_deps(&orchestrator, &session, &policy, Some(&tools), None);
    deps.content_limits = ContentLimits {
        tool_result_inline_bytes: 4_096,
        spill: Some(&store),
    };

    run_envelope(
        user_envelope("Dump the log for me."),
        RunId::new("c2-elision"),
        &deps,
    )
    .await
    .expect("a run under pressure still finishes");

    let requests = llm.requests();
    let second = requests.get(1).expect("the tool result is planned on");
    let tool_message = second
        .messages
        .iter()
        .find(|message| message.role == MessageRole::Tool)
        .expect("the request carries the tool result");
    assert!(
        tool_message
            .content
            .contains("bytes elided from the middle"),
        "the request should have been elided; got {} bytes",
        tool_message.content.len()
    );
    // Elided, not destroyed: the marker names the file holding the rest.
    assert!(tool_message.content.contains("bulk-call_1.txt"));

    // The log is untouched. Elision is a view over it, recomputed each turn,
    // so a later compaction could still show these bytes in full.
    let transcript = session
        .load(&support::golden_participant())
        .await
        .expect("session loads");
    let logged = transcript
        .items
        .iter()
        .find(|item| item.message.role == MessageRole::Tool)
        .expect("the log carries the tool result");
    assert!(!logged
        .message
        .content
        .contains("bytes elided from the middle"));
    assert!(logged.message.content.len() > tool_message.content.len());
}
