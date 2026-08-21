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
use agentos_core::runtime::phase5_policy;
use agentos_interfaces::guardrail::{GuardrailOutcome, ToolGuardrail};
use agentos_interfaces::orchestrator::{Plan, RunContext};
use agentos_interfaces::RunState;
use agentos_proto::{AgentId, RunId, ToolCall, ToolCallId};
use serde_json::{json, value::RawValue};
use std::path::Path;
use std::sync::Arc;

fn shipped_config() -> WorkspaceConfig {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workspace/agent.toml");
    WorkspaceConfig::load(&path).expect("the shipped workspace config must load")
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
    let policy = phase5_policy(&config, &[]);

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
    let policy = phase5_policy(&config, &[]);

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
    let policy = phase5_policy(&config, &[]);

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

#[test]
fn the_shipped_policy_carries_no_blanket_rule_for_a_gated_tool() {
    // The structural version of the assertions above: whatever rules exist,
    // none of the three tools may carry an unconstrained `Allow`. This is what
    // catches a future edit that re-adds one under a different spelling.
    let policy = phase5_policy(&shipped_config(), &[]);
    for rule in &policy.rules {
        let agentos_core::approve::PolicyAction::Tool(tool) = &rule.action else {
            continue;
        };
        if !matches!(tool.as_ref(), "shell" | "file" | "memory") {
            continue;
        }
        assert!(
            !(rule.decision == PolicyVerb::Allow && rule.arg_equals.is_empty()),
            "'{tool}' must not carry an unconstrained Allow rule: {rule:?}"
        );
    }
}
