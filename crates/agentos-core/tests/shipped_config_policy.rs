//! The shipped `workspace/agent.toml`, judged by what it actually decides.
//!
//! Audit remediation M1 (`CFG-000`). Every assertion here loads the file the
//! project ships rather than a fixture, because the finding was never about
//! what the engine *can* express — `Policy` already carried per-argument
//! rules and the guardrail already carried an allowlist. It was about what the
//! shipped configuration asked them to do: `[policy] allowlist` naming `shell`
//! and `file` collapsed their operation-level rules into a blanket `Allow`,
//! and `python3` in `shell_allowlist` was admitted on program name while its
//! arguments went unread. A fixture would prove the mechanism and miss the
//! exposure.

use agentos_core::approve::{PolicyDecision, PolicyVerb};
use agentos_core::config::WorkspaceConfig;
use agentos_core::guardrails::ShellCommandAllowlist;
use agentos_core::jobs::JobRegistry;
use agentos_core::memory::{InMemoryMemory, MemoryManager};
use agentos_core::runtime::{phase5_policy, register_builtin_tool};
use agentos_core::spill::SpillStore;
use agentos_core::tools::{
    JobKillTool, JobOutputTool, JobStatusTool, MemoryTool, SpillReadTool, ToolRegistry,
};
use agentos_interfaces::guardrail::{GuardrailOutcome, ToolGuardrail};
use agentos_interfaces::orchestrator::{Plan, RunContext};
use agentos_interfaces::tool::{ToolSideEffect, ToolSpec};
use agentos_interfaces::RunState;
use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
use serde_json::{json, value::RawValue};
use std::path::Path;
use std::sync::Arc;

fn shipped_config() -> WorkspaceConfig {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workspace/agent.toml");
    WorkspaceConfig::load(&path).expect("the shipped workspace config must load")
}

fn shipped_specs(config: &WorkspaceConfig) -> Vec<ToolSpec> {
    let mut registry = ToolRegistry::new();
    let memory = Arc::new(MemoryManager::new(Arc::new(InMemoryMemory::default())));
    let jobs = Arc::new(JobRegistry::default());
    for name in &config.resources.tools.enabled {
        match name.as_ref() {
            "memory" => registry.register(MemoryTool::with_manager(memory.clone())),
            "job_status" => registry.register(JobStatusTool::new(jobs.clone())),
            "job_output" => registry.register(JobOutputTool::new(jobs.clone())),
            "job_kill" => registry.register(JobKillTool::new(jobs.clone())),
            "spill_read" => registry.register(SpillReadTool::new(Arc::new(SpillStore::new(
                std::env::temp_dir().join("agentos-shipped-policy-spec-only"),
            )))),
            other => register_builtin_tool(
                &mut registry,
                other,
                &config.limits,
                &config.isolation.env_passthrough,
            )
            .expect("every shipped built-in tool registers"),
        }
    }
    registry.specs()
}

fn shipped_policy() -> agentos_core::approve::Policy {
    let config = shipped_config();
    phase5_policy(&config, &shipped_specs(&config)).expect("the shipped policy builds")
}

fn call(name: &str, args: serde_json::Value) -> ToolCall {
    ToolCall {
        id: ToolCallId::new(format!("{name}-shipped-config-test")),
        name: Arc::from(name),
        args: RawValue::from_string(args.to_string()).expect("test args are valid JSON"),
    }
}

/// Run the shipped shell guardrail over one call and report why it refused.
async fn shell_refusal(command: &str, args: serde_json::Value) -> Option<String> {
    let config = shipped_config();
    let guardrail = ShellCommandAllowlist::new(config.guardrails.shell_allowlist.iter().cloned())
        .with_profiles(config.guardrails.shell_profiles.iter().cloned());
    let state = RunState::new(RunId::new("shipped-config"), AgentId::new("agent"));
    let ctx = RunContext::from_state(&state);

    let outcome = guardrail
        .check_call(
            &call("shell", json!({ "command": command, "args": args })),
            &ctx,
        )
        .await
        .expect("guardrail should reach a decision");
    match outcome {
        GuardrailOutcome::Tripped(reason) => Some(reason.to_string()),
        GuardrailOutcome::Passed => None,
    }
}

#[tokio::test]
async fn python_inline_code_is_refused() {
    // The finding, verbatim: the guardrail read only the program name, so
    // this was arbitrary code execution behind an allowlist that looked
    // restrictive.
    let reason = shell_refusal("python3", json!(["-c", "import os; os.system('id')"]))
        .await
        .expect("python3 -c must be refused under the shipped configuration");
    assert!(
        reason.contains("-c"),
        "the refusal should name the offending argument, got: {reason}"
    );
}

#[tokio::test]
async fn python_module_and_stdin_forms_are_refused() {
    // `-c` is not a denylist entry, so the sibling escapes close with it
    // rather than needing to be enumerated. `-Bc` is the case an exact-match
    // denylist would have missed.
    for args in [
        json!(["-m", "http.server"]),
        json!(["-"]),
        json!(["-i"]),
        json!(["-Bc", "import os; os.system('id')"]),
        json!([]),
    ] {
        assert!(
            shell_refusal("python3", args.clone()).await.is_some(),
            "python3 {args} must be refused under the shipped configuration"
        );
    }
}

#[tokio::test]
async fn python_running_a_bundled_script_still_works() {
    // The constraint has to leave the shipped skill bundles working, or it
    // would be traded for an outage rather than for safety.
    assert_eq!(
        shell_refusal(
            "python3",
            json!([
                "workspace/skills/audit-skill/scripts/audit_tokens.py",
                "--hours",
                "24"
            ])
        )
        .await,
        None,
        "a bundled skill script with its own flags must still run"
    );
}

#[tokio::test]
async fn find_cannot_execute_or_delete() {
    // `find` ships in the default read-only inspection set, and its action
    // predicates are a code-execution and deletion primitive.
    for args in [
        json!([".", "-exec", "sh", "-c", "id", ";"]),
        json!([".", "-delete"]),
        json!([".", "-execdir", "sh", "-c", "id", ";"]),
        json!([".", "-fprintf", "/tmp/out", "%p"]),
    ] {
        assert!(
            shell_refusal("find", args.clone()).await.is_some(),
            "find {args} must be refused under the shipped configuration"
        );
    }
    assert_eq!(
        shell_refusal("find", json!([".", "-name", "*.rs"])).await,
        None,
        "an ordinary find must still run"
    );
}

#[tokio::test]
async fn an_unprofiled_program_is_still_refused_by_name() {
    let reason = shell_refusal("curl", json!(["https://example.com"]))
        .await
        .expect("curl is not allowlisted");
    assert!(reason.contains("not allowlisted"), "got: {reason}");
    // The refusal must list the profiled programs too, or it tells the model
    // that a program it may in fact call does not exist.
    assert!(reason.contains("python3"), "got: {reason}");
}

#[test]
fn file_write_asks_and_read_allows() {
    let config = shipped_config();
    let policy = phase5_policy(&config, &shipped_specs(&config)).expect("policy builds");

    assert_eq!(
        policy.decide(&Plan::CallTool(call(
            "file",
            json!({ "operation": "read", "path": "README.md" })
        ))),
        PolicyDecision::Allow,
        "reading a file must not prompt"
    );

    let write = policy.decide(&Plan::CallTool(call(
        "file",
        json!({ "operation": "write", "path": "README.md", "content": "x" }),
    )));
    assert!(
        matches!(write, PolicyDecision::AskUser { .. }),
        "a file write must reach the user under the shipped configuration, got {write:?}"
    );
}

#[test]
fn an_unknown_file_operation_is_denied_rather_than_allowed() {
    // The `file` tool implements read and write only. The plan asks for
    // `delete` to reach `AskUser`; there is no such operation, so the
    // contract that matters is that an operation nobody wrote a rule for
    // falls to the default rather than through a blanket tool allow.
    let config = shipped_config();
    assert_eq!(config.policy.default.as_ref(), "deny");
    let policy = phase5_policy(&config, &shipped_specs(&config)).expect("policy builds");

    let decision = policy.decide(&Plan::CallTool(call(
        "file",
        json!({ "operation": "delete", "path": "README.md" }),
    )));
    assert!(
        matches!(decision, PolicyDecision::Deny { .. }),
        "an unrecognised file operation must be denied, got {decision:?}"
    );
}

#[test]
fn shell_and_memory_mutations_reach_the_user() {
    let config = shipped_config();
    let policy = phase5_policy(&config, &shipped_specs(&config)).expect("policy builds");

    let shell = policy.decide(&Plan::CallTool(call(
        "shell",
        json!({ "command": "ls", "args": [] }),
    )));
    assert!(
        matches!(shell, PolicyDecision::AskUser { .. }),
        "a shell call must reach the user under the shipped configuration, got {shell:?}"
    );

    // `[memory.policy]` declares ask_user for writes and forgets. It is not
    // yet the authority (M7 wires it), but the coarse allowlist entry that
    // pre-empted it outright is gone, so the built-in rules now hold.
    assert_eq!(config.memory.policy.writes.as_ref(), "ask_user");
    for operation in ["write", "forget"] {
        let decision = policy.decide(&Plan::CallTool(call(
            "memory",
            json!({ "operation": operation, "content": "x" }),
        )));
        assert!(
            matches!(decision, PolicyDecision::AskUser { .. }),
            "memory {operation} must reach the user, got {decision:?}"
        );
    }
    assert_eq!(
        policy.decide(&Plan::CallTool(call(
            "memory",
            json!({ "operation": "read", "query": "x" })
        ))),
        PolicyDecision::Allow,
        "reading memory must not prompt"
    );
}

/// AF-031: shipped risky mutations must never acquire an unconstrained Allow.
#[test]
fn metadata_drives_the_blanket_allow_ratchet_for_every_shipped_tool() {
    let config = shipped_config();
    let specs = shipped_specs(&config);
    let policy = phase5_policy(&config, &specs).expect("the shipped policy builds");

    for spec in &specs {
        assert_ne!(
            spec.safety.side_effect,
            ToolSideEffect::Unspecified,
            "'{}' must declare side-effect metadata",
            spec.name
        );
        if !spec.safety.rejects_blanket_allow() {
            continue;
        }
        assert!(
            policy.rules.iter().all(|rule| {
                rule.action != agentos_core::approve::PolicyAction::Tool(Arc::clone(&spec.name))
                    || rule.decision != PolicyVerb::Allow
                    || !rule.arg_equals.is_empty()
            }),
            "'{}' must not carry an unconstrained Allow rule",
            spec.name
        );
    }
}

#[test]
fn cron_mutations_ask_while_cron_and_job_inspection_remain_noninteractive() {
    let config = shipped_config();
    assert!(!config
        .policy
        .allowlist
        .iter()
        .any(|name| matches!(name.as_ref(), "cron_create" | "cron_remove")));
    let policy = shipped_policy();

    let create = policy.decide(&Plan::CallTool(call(
        "cron_create",
        json!({
            "id": "daily-report",
            "channel_id": "telegram",
            "conversation_id": "42",
            "prompt": "prepare the daily report",
            "expression": "0 9 * * *"
        }),
    )));
    assert!(
        matches!(create, PolicyDecision::AskUser { .. }),
        "{create:?}"
    );

    let remove = policy.decide(&Plan::CallTool(call(
        "cron_remove",
        json!({ "id": "daily-report" }),
    )));
    assert!(
        matches!(remove, PolicyDecision::AskUser { .. }),
        "{remove:?}"
    );

    assert_eq!(
        policy.decide(&Plan::CallTool(call("cron_list", json!({})))),
        PolicyDecision::Allow
    );
    assert_eq!(
        policy.decide(&Plan::CallTool(call("job_status", json!({})))),
        PolicyDecision::Allow
    );
    assert_eq!(
        policy.decide(&Plan::CallTool(call(
            "job_output",
            json!({ "job_id": "job-1", "offset": 0 })
        ))),
        PolicyDecision::Allow
    );
}
