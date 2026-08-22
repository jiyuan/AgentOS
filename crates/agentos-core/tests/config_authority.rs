//! M7 / `CFG-001`: a config key either does something or fails to load.
//!
//! The audit's finding was not that keys were missing — it was that keys
//! parsed, validated, appeared in `docs/CONFIG_CATALOG.md`, and changed
//! nothing. So every test here asserts a *behavioural* consequence, or a
//! load-time refusal. A parse test would have passed against the broken code.

mod support;

use agentos_core::config::WorkspaceConfig;
use agentos_interfaces::orchestrator::ResourceKind;
use std::sync::Arc;

fn load(toml: &str) -> Result<WorkspaceConfig, String> {
    let tree = support::temp_tree("config-authority");
    let path = tree.path().join("agent.toml");
    std::fs::write(&path, toml).expect("the config is writable");
    WorkspaceConfig::load(&path).map_err(|err| err.to_string())
}

#[test]
fn a_typo_is_a_load_time_error() {
    // The audit's own example. `[memory.polcy]` used to load silently with
    // defaults, so a deployment that meant to gate memory writes got the
    // default and no indication of it.
    let error = load("[memory.polcy]\nwrites = \"deny\"\n")
        .expect_err("an unknown section must not load silently");
    assert!(error.contains("polcy"), "got: {error}");

    // And a misspelled key inside a section it does recognise.
    let error = load("[memory.retention]\nmax_reocrds = 10\n")
        .expect_err("an unknown key must not load silently");
    assert!(error.contains("max_reocrds"), "got: {error}");

    // The control: spelled correctly, it loads and takes effect.
    let config = load("[memory.retention]\nmax_records = 10\n").expect("the config loads");
    assert_eq!(config.memory.retention.max_records, Some(10));
}

#[test]
fn every_section_refuses_an_unknown_key() {
    // One typo per section, so a struct that loses `deny_unknown_fields`
    // later fails here rather than silently accepting nonsense again.
    for section in [
        "[agent]\nnope = 1",
        "[policy]\nnope = 1",
        "[guardrails]\nnope = 1",
        "[memory]\nnope = 1",
        "[memory.reflection]\nnope = 1",
        "[memory.policy]\nnope = 1",
        "[memory.retention]\nnope = 1",
        "[channels]\nnope = 1",
        "[channels.tui]\nnope = 1",
        "[isolation]\nnope = 1",
        "[limits]\nnope = 1",
        "[compaction]\nnope = 1",
        "[jobs]\nnope = 1",
        "[gateway]\nnope = 1",
        "[approval]\nnope = 1",
        "[spill]\nnope = 1",
        "[routing]\nnope = 1",
        "[resources]\nnope = 1",
        "[resources.tools]\nnope = 1",
        "[task_workspace]\nnope = 1",
    ] {
        let error = load(section).err().unwrap_or_else(|| {
            panic!("`{section}` loaded with an unknown key; the section lost deny_unknown_fields")
        });
        assert!(
            error.contains("nope"),
            "the error must name the key, got: {error}"
        );
    }
}

#[test]
fn the_agent_id_reaches_the_config_and_cannot_be_blank() {
    let config = load("[agent]\nid = \"analytics\"\n").expect("the config loads");
    assert_eq!(config.agent.id.as_ref(), "analytics");

    let error = load("[agent]\nid = \"  \"\n").expect_err("a blank agent id is refused");
    assert!(error.contains("agent.id must not be empty"), "got: {error}");
}

#[test]
fn the_deprecated_agent_memory_key_still_loads() {
    // Deprecated, not removed: an `agent.toml` written before M7 keeps
    // loading, and the operator finds out from a `warn!` on the next start
    // rather than from a failed boot.
    let config =
        load("[agent]\nmemory = \"memory.sqlite\"\n").expect("a deprecated key still loads");
    assert_eq!(config.agent.memory.as_deref(), Some("memory.sqlite"));
    // And it decides nothing: `[memory].backend` is the authority.
    assert_eq!(config.memory.backend.as_ref(), "sqlite");
}

#[test]
fn resource_priority_decides_the_order_the_model_sees() {
    // `[resources].priority` built the index in order and then
    // `MaxOrchestrator::with_resource_index` re-sorted it by
    // `DispatchPriority` — which nothing else reads — so the configured order
    // was discarded and every deployment got skills, tools, mcp, llm.
    let config = load(
        "[resources]\npriority = [\"llm\", \"tools\", \"skills\", \"mcp\"]\n\n\
         [resources.tools]\nenabled = [\"file\"]\n",
    )
    .expect("the config loads");

    let index = config.resource_index(&[], &[], &[]);
    let kinds: Vec<ResourceKind> = index
        .entries
        .iter()
        .map(|entry| entry.kind.clone())
        .collect();
    assert_eq!(
        kinds.first(),
        Some(&ResourceKind::Llm),
        "the configured priority puts llm first, got {kinds:?}"
    );

    // The same index reaches the orchestrator unreordered, which is the half
    // that was broken.
    let orchestrator =
        agentos_core::orchestrator::MaxOrchestrator::new().with_resource_index(index.clone());
    assert_eq!(
        orchestrator
            .resource_index()
            .entries
            .first()
            .map(|entry| entry.kind.clone()),
        Some(ResourceKind::Llm),
        "the orchestrator must not re-sort a configured order"
    );
}

#[test]
fn the_llm_fallback_is_decided_by_priority_alone() {
    // `[resources.llm].enabled` looked like a list of models to enable and
    // validation only ever accepted the single literal `"llm"`, so both
    // spellings produced one identical entry. It is deprecated; `priority` is
    // the control that was doing the work all along.
    let names = |toml: &str| -> Vec<Arc<str>> {
        load(toml)
            .expect("the config loads")
            .resource_index(&[], &[], &[])
            .entries
            .iter()
            .map(|entry| Arc::clone(&entry.name))
            .collect()
    };

    assert_eq!(
        names("[resources]\npriority = [\"llm\"]\n"),
        vec![Arc::from("llm")]
    );
    // The deprecated section changes nothing either way.
    assert_eq!(
        names("[resources]\npriority = [\"llm\"]\n\n[resources.llm]\nenabled = [\"llm\"]\n"),
        vec![Arc::from("llm")]
    );
    // Omitting `llm` from `priority` omits the entry, which is the whole of
    // the control.
    assert!(names("[resources]\npriority = [\"tools\"]\n")
        .iter()
        .all(|name| name.as_ref() != "llm"));
}
