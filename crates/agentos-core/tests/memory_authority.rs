//! M7 / `MEM-001`: `[memory]` decides what memory does.
//!
//! Every key exercised here parsed, validated, and appeared in
//! `docs/CONFIG_CATALOG.md` before this milestone, and none of them changed
//! any behaviour. The tests are therefore *behavioural* on purpose — a parse
//! test would have passed against the broken code, which is how the keys went
//! unnoticed.

mod support;

use agentos_core::approve::{Policy, PolicyDecision, PolicyVerb};
use agentos_core::config::WorkspaceConfig;
use agentos_core::memory::{
    MemoryCaller, MemoryManager, MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility,
    ReflectionParams, RetentionRequest, SharedDomainGrants, SqliteStore,
};
use agentos_core::runtime::phase5_policy;
use agentos_interfaces::memory::Query;
use agentos_interfaces::orchestrator::Plan;
use agentos_interfaces::tool::{
    SandboxMode, ToolPersistenceScope, ToolSafety, ToolSideEffect, ToolSpec,
};
use agentos_proto::{AgentId, ChannelId, ConversationId, TaskId, ToolCall, ToolCallId};
use serde_json::{json, value::RawValue};
use std::sync::Arc;

fn memory_call(operation: &str) -> Plan {
    Plan::CallTool(ToolCall {
        id: ToolCallId::new("memory-call"),
        name: Arc::from("memory"),
        args: RawValue::from_string(json!({ "operation": operation }).to_string())
            .expect("valid JSON"),
    })
}

fn memory_policy(config: &WorkspaceConfig) -> Policy {
    let specs = config
        .resources
        .tools
        .enabled
        .iter()
        .map(|name| ToolSpec {
            name: Arc::clone(name),
            description: Arc::from("memory policy test tool"),
            input_schema: json!({ "type": "object" }),
            safety: match name.as_ref() {
                "shell" | "file" => ToolSafety::new(
                    ToolSideEffect::PersistentMutation,
                    ToolPersistenceScope::Workspace,
                ),
                "cron_create" | "cron_remove" | "memory" => ToolSafety::new(
                    ToolSideEffect::PersistentMutation,
                    ToolPersistenceScope::CrossConversation,
                ),
                "cron_list" => ToolSafety::new(
                    ToolSideEffect::ReadOnly,
                    ToolPersistenceScope::CrossConversation,
                ),
                "job_status" | "job_output" | "spill_read" => {
                    ToolSafety::new(ToolSideEffect::ReadOnly, ToolPersistenceScope::Conversation)
                }
                "job_kill" => ToolSafety::new(
                    ToolSideEffect::TransientMutation,
                    ToolPersistenceScope::Conversation,
                ),
                "skill_validate" => {
                    ToolSafety::new(ToolSideEffect::ReadOnly, ToolPersistenceScope::Workspace)
                }
                "http" => ToolSafety::new(ToolSideEffect::ReadOnly, ToolPersistenceScope::None),
                _ => ToolSafety::default(),
            },
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        })
        .collect::<Vec<_>>();
    phase5_policy(config, &specs).expect("memory policy builds")
}

/// Load a config from TOML text, the way a deployment's `agent.toml` is loaded.
fn config_from(toml: &str) -> Result<WorkspaceConfig, String> {
    let tree = support::temp_tree("memory-authority");
    let path = tree.path().join("agent.toml");
    std::fs::write(&path, toml).expect("the config is writable");
    WorkspaceConfig::load(&path).map_err(|err| err.to_string())
}

const BASE: &str = r#"
[agent]
orchestrator = "builtin.min"

[resources.tools]
enabled = ["memory"]
"#;

#[test]
fn the_memory_policy_section_decides_each_operation() {
    let config = config_from(&format!(
        "{BASE}\n[memory.policy]\nreads = \"deny\"\nwrites = \"allow\"\nforgets = \"ask_user\"\n"
    ))
    .expect("the config loads");
    let policy = memory_policy(&config);

    // All three come from `[memory.policy]`. Before M7 these were hardcoded
    // allow-read / ask-write / ask-forget, and the keys were read nowhere.
    assert!(matches!(
        policy.decide(&memory_call("read")),
        PolicyDecision::Deny { .. }
    ));
    assert_eq!(policy.decide(&memory_call("write")), PolicyDecision::Allow);
    assert!(matches!(
        policy.decide(&memory_call("forget")),
        PolicyDecision::AskUser { .. }
    ));
}

#[test]
fn a_coarse_allowlist_cannot_silently_override_the_memory_policy() {
    // The precedence that used to be documented as intentional in
    // `runtime/tools_config.rs`: naming `memory` in `[policy] allowlist`
    // turned every operation into a blanket `Allow`, `[memory.policy]`
    // included. It is now a load-time error rather than a silent win.
    let error = config_from(&format!(
        "{BASE}\n[policy]\nallowlist = [\"memory\"]\n\n[memory.policy]\nwrites = \"ask_user\"\n"
    ))
    .expect_err("a config that says two contradictory things must not load");
    assert!(
        error.contains("policy.allowlist must not name 'memory'"),
        "the error has to say which two settings disagree, got: {error}"
    );
}

fn caller_with(grants: &SharedDomainGrants, writable: bool) -> MemoryCaller {
    MemoryCaller {
        agent_id: AgentId::new("agent"),
        task_id: TaskId::new("task"),
        channel_id: ChannelId::new("channel"),
        conversation_id: ConversationId::new("conversation"),
        user_id: None,
        allowed_shared_domains: grants.readable.clone(),
        writable_shared_domains: if writable {
            grants.writable.clone()
        } else {
            Vec::new()
        },
        audit_read_access: false,
    }
}

fn shared_scope(domain: &str) -> MemoryScope {
    MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Shared,
        MemoryVisibility::Shared,
        Some(Arc::from(domain)),
    )
}

async fn manager() -> Arc<MemoryManager> {
    let store = Arc::new(SqliteStore::open_in_memory().expect("the store opens"));
    Arc::new(MemoryManager::new_sqlite(store))
}

#[tokio::test]
async fn a_shared_write_needs_the_global_switch_the_domain_and_the_caller() {
    // Three gates, tested by removing one at a time. Removing any one of them
    // must refuse the write, which is what makes them gates rather than one
    // setting spelled three ways.
    let manager = manager().await;
    let body = json!({ "fact": "shared" });

    let permissive = config_from(&format!(
        "{BASE}\n[memory.policy]\nshared_writes = true\n\n\
         [[memory.shared_domains]]\nname = \"team\"\nread = true\nwrite = true\n"
    ))
    .expect("the config loads");
    let grants = permissive.memory.shared_domain_grants();
    assert_eq!(grants.writable, vec![Arc::from("team")]);

    // All three present: the write lands.
    manager
        .write_scoped(
            &caller_with(&grants, true),
            shared_scope("team"),
            body.clone(),
            Default::default(),
        )
        .await
        .expect("a fully permitted shared write succeeds");

    // Caller gate removed — the deployment allows it, this caller does not
    // hold it.
    manager
        .write_scoped(
            &caller_with(&grants, false),
            shared_scope("team"),
            body.clone(),
            Default::default(),
        )
        .await
        .expect_err("a caller without the grant must not write shared memory");

    // Global gate removed.
    let no_global = config_from(&format!(
        "{BASE}\n[memory.policy]\nshared_writes = false\n\n\
         [[memory.shared_domains]]\nname = \"team\"\nread = true\nwrite = true\n"
    ))
    .expect("the config loads");
    let grants = no_global.memory.shared_domain_grants();
    assert!(
        grants.writable.is_empty(),
        "shared_writes = false grants no domain, however the domain is marked"
    );
    manager
        .write_scoped(
            &caller_with(&grants, true),
            shared_scope("team"),
            body.clone(),
            Default::default(),
        )
        .await
        .expect_err("shared_writes = false must refuse every shared write");

    // Domain gate removed.
    let no_domain = config_from(&format!(
        "{BASE}\n[memory.policy]\nshared_writes = true\n\n\
         [[memory.shared_domains]]\nname = \"team\"\nread = true\nwrite = false\n"
    ))
    .expect("the config loads");
    let grants = no_domain.memory.shared_domain_grants();
    assert!(grants.writable.is_empty());
    assert_eq!(grants.readable, vec![Arc::from("team")], "reads are intact");
    manager
        .write_scoped(
            &caller_with(&grants, true),
            shared_scope("team"),
            body,
            Default::default(),
        )
        .await
        .expect_err("a read-only domain must refuse a write");
}

#[tokio::test]
async fn the_default_domain_decides_where_a_write_lands() {
    // `[memory] default_domain` reached nothing before M7: every scope built
    // with no domain read as the literal `general`, so configuring one changed
    // neither writes nor hydration.
    let config = config_from(&format!(
        "{BASE}\n[memory]\ndefault_domain = \"projectx\"\n"
    ))
    .expect("the config loads");
    assert_eq!(config.memory.default_domain.as_ref(), "projectx");

    let settings = config
        .memory
        .hydration_settings()
        .expect("hydration settings build");
    assert_eq!(settings.default_domain.as_ref(), "projectx");

    // And the namespace a scope resolves to carries it, which is what makes
    // two default domains two separate bodies of memory.
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Agent(AgentId::new("agent")),
        MemoryVisibility::Private,
        Some(Arc::clone(&config.memory.default_domain)),
    );
    assert!(
        scope.namespace().as_str().ends_with("/projectx"),
        "got {}",
        scope.namespace().as_str()
    );
}

#[tokio::test]
async fn count_age_and_byte_budgets_each_prune_seeded_records() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("the store opens"));
    let manager = Arc::new(MemoryManager::new_sqlite(store));
    let caller = MemoryCaller {
        agent_id: AgentId::new("agent"),
        task_id: TaskId::new("task"),
        channel_id: ChannelId::new("channel"),
        conversation_id: ConversationId::new("conversation"),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        writable_shared_domains: Vec::new(),
        audit_read_access: false,
    };
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Agent(AgentId::new("agent")),
        MemoryVisibility::Private,
        Some(Arc::from("general")),
    );

    for index in 0..6 {
        manager
            .write_scoped(
                &caller,
                scope.clone(),
                json!({ "fact": format!("seeded fact number {index}") }),
                Default::default(),
            )
            .await
            .expect("seeding succeeds");
    }
    let active = |manager: &Arc<MemoryManager>| {
        let manager = Arc::clone(manager);
        let caller = caller.clone();
        let scope = scope.clone();
        async move {
            manager
                .read_scoped(&caller, scope, &Query::filter(usize::MAX))
                .await
                .expect("reading succeeds")
                .len()
        }
    };
    assert_eq!(active(&manager).await, 6);

    // Count: four survive.
    let mut params = ReflectionParams {
        rebuild_lexical_index: false,
        retention: RetentionRequest {
            max_records: Some(4),
            ..RetentionRequest::default()
        },
        ..ReflectionParams::default()
    };
    manager
        .reflect_all(&AgentId::new("agent"), &params)
        .await
        .expect("the sweep runs");
    assert_eq!(active(&manager).await, 4, "the count budget archived two");

    // Bytes: a ceiling below what four records occupy takes more.
    params.retention = RetentionRequest {
        max_bytes: Some(60),
        ..RetentionRequest::default()
    };
    manager
        .reflect_all(&AgentId::new("agent"), &params)
        .await
        .expect("the sweep runs");
    let after_bytes = active(&manager).await;
    assert!(
        after_bytes < 4,
        "the byte budget archived nothing; {after_bytes} records remain"
    );

    // Age, the control half: everything written in this test is younger than a
    // day, so neither a one-day nor a zero-day ceiling touches it — a record
    // created today is zero days old, and the budget archives what is *older*
    // than the ceiling. The half that fires is unit-tested in
    // `memory::retention`, where a row's `created_at` can be backdated
    // directly; an integration test cannot age a record without sleeping.
    for ceiling in [1, 0] {
        params.retention = RetentionRequest {
            max_age_days: Some(ceiling),
            ..RetentionRequest::default()
        };
        manager
            .reflect_all(&AgentId::new("agent"), &params)
            .await
            .expect("the sweep runs");
        assert_eq!(
            active(&manager).await,
            after_bytes,
            "a record written moments ago is not older than {ceiling} day(s)"
        );
    }
}

#[test]
fn the_shipped_config_still_loads() {
    // The load-time rejection above is only safe because the shipped
    // `agent.toml` does not allowlist `memory`. M1 removed it; this is the
    // guard that it stays removed.
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config = WorkspaceConfig::load(&repo_root.join("workspace/agent.toml"))
        .expect("the shipped config loads");
    assert!(!config
        .policy
        .allowlist
        .iter()
        .any(|t| t.as_ref() == "memory"));
    let policy: Policy = memory_policy(&config);
    assert!(
        policy.rules.iter().any(|rule| {
            rule.decision == PolicyVerb::AskUser && rule.arg_equals.values().any(|v| v == "write")
        }),
        "the shipped config still gates memory writes behind an approval"
    );
}
