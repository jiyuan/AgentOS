//! One failing test per open P0/P1 audit finding.
//!
//! M0 deliverable 6 of [`docs/AUDIT_REMEDIATION_PLAN.md`]: turn each finding
//! into a regression test *before* changing implementation behavior, so the
//! milestone that closes it has an unambiguous definition of done and cannot
//! close by accident.
//!
//! **A test here was `#[ignore]`d while it was red on purpose.** That was not
//! a weakened test — the opposite. Each asserts the behavior the owning ADR
//! specifies, against code that did not yet provide it, and named the
//! milestone in its ignore reason. When that milestone landed, its commit
//! deleted the `#[ignore]` and the test became the proof.
//!
//! **Every test in this file now runs.** `AUTH-002`, `ID-001`, `SBX-001`,
//! `FS-001`, `PROC-001`, and `SPILL-001` have all been through that
//! transition, and the file is now what a future change to `Policy::narrow`,
//! to namespace encoding, to registry dispatch, to filesystem containment, to
//! the child environment, or to the spill locator is measured against.
//!
//! Findings that could not be expressed as a test at this layer are listed at
//! the bottom of the file, with the reason. They are not silently absent.

use agentos_core::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use agentos_core::memory::{MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility};
use agentos_core::sandbox::Sandbox;
use agentos_core::tools::exec::{run, Exec, DEFAULT_MAX_OUTPUT_BYTES};
use agentos_core::tools::ToolRegistry;
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolCallId, ToolResult, ToolStatus};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn tool_rule(tool: &str, decision: PolicyVerb, args: &[(&str, serde_json::Value)]) -> PolicyRule {
    PolicyRule {
        action: PolicyAction::Tool(Arc::from(tool)),
        decision,
        reason: None,
        arg_equals: args
            .iter()
            .map(|(key, value)| (Arc::from(*key), value.clone()))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// M3 / AUTH-002 — CLOSED. Narrowing is exact over actions and arguments.
//
// These three were red against `parent_exposes_tool`, which matched on the
// tool name alone. They run normally now and are the regression guard: the
// first two must stay red-if-reverted, and the third stops the fix from being
// a `narrow` that rejects everything.
// ---------------------------------------------------------------------------

/// A parent that gates `shell` behind an approval prompt is stating that a
/// human decides each call. A sub-agent naming `shell` in its allowlist gets
/// `Allow`, which removes the human without any record that it happened.
#[test]
fn a_child_cannot_promote_a_parent_ask_user_to_allow() {
    let parent = Policy {
        rules: vec![tool_rule("shell", PolicyVerb::AskUser, &[])],
        default_decision: PolicyVerb::Deny,
    };
    let child = Policy {
        rules: vec![tool_rule("shell", PolicyVerb::Allow, &[])],
        default_decision: PolicyVerb::Deny,
    };

    assert!(
        Policy::narrow(&parent, &child).is_err(),
        "a child Allow for a tool the parent only asks about is a widening"
    );
}

/// The parent may use `file` for reads and nothing else. An unconstrained
/// child `Allow` reaches `write`, which the parent never held.
#[test]
fn a_child_cannot_drop_the_parents_argument_constraints() {
    let parent = Policy {
        rules: vec![tool_rule(
            "file",
            PolicyVerb::Allow,
            &[("operation", serde_json::json!("read"))],
        )],
        default_decision: PolicyVerb::Deny,
    };
    let child = Policy {
        rules: vec![tool_rule("file", PolicyVerb::Allow, &[])],
        default_decision: PolicyVerb::Deny,
    };

    assert!(
        Policy::narrow(&parent, &child).is_err(),
        "an unconstrained child Allow reaches operations the constrained parent rule does not"
    );
}

/// The control: a child that is genuinely no wider than its parent still
/// narrows. Guards against "fixing" narrowing by rejecting everything, which
/// would otherwise go unnoticed until someone tried to configure a sub-agent.
#[test]
fn an_equally_constrained_child_still_narrows() {
    let parent = Policy {
        rules: vec![tool_rule(
            "file",
            PolicyVerb::Allow,
            &[("operation", serde_json::json!("read"))],
        )],
        default_decision: PolicyVerb::Deny,
    };
    let child = Policy {
        rules: vec![tool_rule(
            "file",
            PolicyVerb::Allow,
            &[("operation", serde_json::json!("read"))],
        )],
        default_decision: PolicyVerb::Deny,
    };

    assert!(
        Policy::narrow(&parent, &child).is_ok(),
        "an exactly-matching child rule is not a widening and must be admitted"
    );
}

// ---------------------------------------------------------------------------
// M3 / ID-001 — CLOSED. Namespace encoding is injective.
//
// `scope_component` was `trimmed.replace('/', "_")`. These two were red
// against it and now guard the replacement.
// ---------------------------------------------------------------------------

/// Two different owners sharing one namespace means one reads and overwrites
/// the other's memory. `a/b` is a realistic channel-derived id.
#[test]
fn two_owners_that_differ_only_by_a_slash_get_different_namespaces() {
    let slashed = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::User(Arc::from("a/b")),
        MemoryVisibility::Private,
        None,
    );
    let underscored = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::User(Arc::from("a_b")),
        MemoryVisibility::Private,
        None,
    );

    assert_ne!(
        slashed.namespace(),
        underscored.namespace(),
        "`a/b` and `a_b` are different owners and must not share a namespace"
    );
}

/// The same defect on the domain component, which a deployment controls
/// through `[memory] default_domain` and `[memory.shared_domains]`.
#[test]
fn two_domains_that_differ_only_by_a_slash_get_different_namespaces() {
    let owner = MemoryOwner::User(Arc::from("someone"));
    let slashed = MemoryScope::new(
        MemoryStore::Semantic,
        owner.clone(),
        MemoryVisibility::Private,
        Some(Arc::from("team/notes")),
    );
    let underscored = MemoryScope::new(
        MemoryStore::Semantic,
        owner,
        MemoryVisibility::Private,
        Some(Arc::from("team_notes")),
    );

    assert_ne!(slashed.namespace(), underscored.namespace());
}

// ---------------------------------------------------------------------------
// M4 / SBX-001 — CLOSED. The registry no longer falls through to the
// in-process body.
//
// ADR-0002. `if let Some(runner)` in `registry.rs` had no `else`, so a
// declared mode was discarded whenever no executor was configured. Dispatch is
// now a two-variant enum and the missing-executor arm is a typed error.
// ---------------------------------------------------------------------------

/// A tool that declares a mode and records whether its body ran.
struct SandboxedProbe {
    ran: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Tool for SandboxedProbe {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("sandboxed_probe"),
            description: Arc::from("declares read_only and does its work in-process"),
            input_schema: serde_json::json!({"type": "object"}),
            sandbox: SandboxMode::ReadOnly,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, _args: &RawValue) -> Result<ToolResult, ToolError> {
        self.ran.store(true, Ordering::SeqCst);
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from("ran unsandboxed"),
            metadata: BTreeMap::new(),
        })
    }
}

/// The invariant `AGENTS.md` states: "Where no backend exists, a sandboxed tool
/// fails rather than running unsandboxed." The probe declares `read_only` and
/// does not harden anything itself, so with no executor configured it must not
/// run at all.
#[tokio::test]
async fn a_sandboxed_tool_does_not_run_in_process_without_an_executor() {
    let ran = Arc::new(AtomicBool::new(false));
    let mut registry = ToolRegistry::new();
    registry.register(SandboxedProbe {
        ran: Arc::clone(&ran),
    });

    let call = ToolCall {
        id: ToolCallId::new("probe-1"),
        name: Arc::from("sandboxed_probe"),
        args: RawValue::from_string("{}".to_owned()).expect("valid json"),
    };
    let result = registry.call(&call).await;

    assert!(
        !ran.load(Ordering::SeqCst),
        "the tool declared read_only and no isolated executor was configured, \
         so its body must never have been reached"
    );
    assert!(
        result.is_err(),
        "the caller must be told the tool could not be isolated, not handed a success"
    );
}

// ---------------------------------------------------------------------------
// M4 / FS-001 — CLOSED. Containment is the kernel's, not a string check's.
//
// `safe_workspace_path` rejected `..` and absolute paths and then handed back
// `root.join(requested)` for an ordinary `std::fs` call. A symlink inside the
// workspace made a traversal-free relative path read and write outside it, and
// the gap between the check and the open was a race anything with write access
// to the tree could win.
// ---------------------------------------------------------------------------

/// The finding, in the shape an attacker would use it: no `..` anywhere, and
/// the path still leaves the root.
#[test]
fn a_symlink_inside_the_root_does_not_make_an_outside_path_reachable() {
    use agentos_core::paths::{ContainmentError, RootDir};

    let inside = std::env::temp_dir().join(format!("agentos-fs001-in-{}", std::process::id()));
    let outside = std::env::temp_dir().join(format!("agentos-fs001-out-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&inside);
    let _ = std::fs::remove_dir_all(&outside);
    std::fs::create_dir_all(&inside).expect("root creates");
    std::fs::create_dir_all(&outside).expect("target creates");
    std::fs::write(outside.join("secrets.env"), b"API_KEY=leaked").expect("target file");
    std::os::unix::fs::symlink(&outside, inside.join("shared")).expect("symlink");

    let root = RootDir::open(&inside).expect("root opens");
    let error = root
        .open_file("shared/secrets.env")
        .expect_err("a link out of the root is not a path inside it");

    assert!(
        matches!(error, ContainmentError::Symlink { .. }),
        "a symlinked component must be refused, not resolved: {error:?}"
    );
    // And the write half, which is the more damaging one.
    assert!(root.create_file("shared/planted.txt").is_err());
    assert!(!outside.join("planted.txt").exists());

    let _ = std::fs::remove_dir_all(&inside);
    let _ = std::fs::remove_dir_all(&outside);
}

/// The other half of the finding: an identifier a model chose becomes one
/// component of a path, and `Path::join` discards the root when handed an
/// absolute one.
#[test]
fn an_identifier_that_is_a_path_cannot_become_a_directory() {
    use agentos_core::task_workspace::{TaskWorkspace, TaskWorkspaceError};
    use agentos_proto::TaskId;

    let root = std::env::temp_dir().join(format!("agentos-fs001-task-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root creates");
    let workspace = TaskWorkspace::new(&root);

    for id in ["/tmp/agentos-fs001-escape", "../escape", ".."] {
        let error = workspace
            .init_task(&TaskId::new(id))
            .expect_err("a path is not a task id");
        assert!(
            matches!(error, TaskWorkspaceError::UnusableName(_)),
            "{id:?}: {error:?}"
        );
    }
    assert!(!std::path::Path::new("/tmp/agentos-fs001-escape").exists());

    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// M4 / PROC-001 — CLOSED. The child no longer inherits the whole process
// environment.
//
// `tools/exec.rs` never called `env_clear`. It now clears and re-adds an
// allowlist (`tools/child_env.rs`).
// ---------------------------------------------------------------------------

/// Every provider and channel credential the gateway holds is in its
/// environment, and every shell command the model runs could read all of them.
/// This is the credential-egress half of the M1 exposure.
#[tokio::test]
async fn a_child_process_does_not_inherit_the_parents_secrets() {
    // SAFETY-adjacent: single-threaded within this test, and the value is a
    // canary rather than a real credential.
    unsafe { std::env::set_var("AGENTOS_CANARY_API_KEY", "sk-canary-must-not-leak") };

    let args = vec!["-c".to_owned(), "env".to_owned()];
    let sandbox = Sandbox::unrestricted();
    let output = run(Exec {
        program: "sh",
        args: &args,
        sandbox: &sandbox,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        extra_env: &[],
    })
    .await
    .expect("sh runs");

    unsafe { std::env::remove_var("AGENTOS_CANARY_API_KEY") };

    let env = String::from_utf8_lossy(&output.stdout);
    assert!(
        !env.contains("sk-canary-must-not-leak"),
        "the child inherited a parent credential; a tool the model drives can read \
         every provider and channel key the runtime holds"
    );
    // The control: clearing the environment is only useful if the child still
    // has one. A `PATH`-less child cannot find `sh`'s own helpers and would
    // fail in ways that look like this test passing.
    assert!(env.contains("PATH="), "got: {env}");
}

// ---------------------------------------------------------------------------
// M7 / SPILL-001 — the spill locator was an absolute host path
// `spill/mod.rs` rendered the locator with `path.to_string_lossy()` and
// embedded it in a model-visible retrieval hint that landed in the durable
// transcript. Closed; the assertions below are now a regression gate.
// ---------------------------------------------------------------------------

/// The model used to be handed a real filesystem path and told to open it with
/// `file`. That leaked the host layout into the transcript and made the
/// locator a capability anyone who could echo a string could forge.
///
/// Green since M7 / `SPILL-001`.
#[tokio::test]
async fn a_spill_locator_is_not_a_host_path() {
    use agentos_core::spill::{SpillSource, SpillStore};
    use agentos_proto::RunId;

    let dir = std::env::temp_dir().join(format!("agentos-spill-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir is creatable");
    let store = SpillStore::new(&dir);

    let run_id = RunId::new("run-1");
    let call_id = ToolCallId::new("call-1");
    let spilled = store
        .save_text(
            &SpillSource {
                run_id: &run_id,
                tool_name: "shell",
                call_id: &call_id,
            },
            "a large tool result",
        )
        .await
        .expect("spilling succeeds");

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        !spilled.locator.as_str().starts_with('/'),
        "the locator is an absolute host path: {}",
        spilled.locator.as_str()
    );
    assert!(
        !spilled.retrieval_hint.contains(&*dir.to_string_lossy()),
        "the retrieval hint embeds the host spill directory in the durable transcript"
    );
}

// ---------------------------------------------------------------------------
// Findings not represented here, and why
// ---------------------------------------------------------------------------
//
// Each of these is a real P0/P1 finding. None is omitted for convenience; each
// needs a seam that does not exist yet, and building that seam is part of the
// owning milestone rather than of M0.
//
// - **M3 / AUTH-001, remote channels fail open.** `parse_update` and
//   `feishu_allowed_source_matches` are private to their channel modules, and
//   neither channel is constructible without a live transport. The red tests
//   belong in those modules' own `#[cfg(test)]` once M3 gives the allowlist
//   check a named, testable entry point.
// - **M3 / AUTH-001, approval tickets bound to no principal.** There is no
//   principal type to bind to yet; the test is written against `ID-001`'s
//   output.
// - **M4 / FS-001, traversal and symlink races.** Needs the validated
//   path-segment API the milestone introduces; asserting today's lexical
//   containment would pin the behavior being replaced.
// - **M4 / NET-001, egress policy.** No egress layer exists to test.
// - **M5 / REQ-001, unrecorded routing and compaction manifests.**
//   `compact()` holds `&mut RunState`, not a `RunContext`, so there is no
//   header sink to assert against. Deliverable 3 of that milestone is
//   precisely to add one.
// - **M6, `/clear` deletes and safety events are absent.** Both need the
//   epoch marker and the event store the milestone builds; there is no
//   observable to assert on beforehand.
// - **M7 / MEM-001 and CFG-001, inert config keys.** These assert that a key
//   *changes behavior*, which requires the wiring the milestone adds. Verified
//   today only as "parses and is never read", which is a grep, not a test.

// ---------------------------------------------------------------------------
// M3 / ID-001 — CLOSED. A conversation id is not an identity on its own.
// ---------------------------------------------------------------------------

/// The acceptance criterion in the plan, stated directly: "`telegram:42`,
/// `feishu:42`, and another agent's `telegram:42` never share session or
/// memory state."
///
/// A conversation id is channel-local — Telegram's chat 42 and Feishu's chat
/// 42 are both the string `"42"` — so keying memory on it alone put three
/// different conversations in one namespace.
#[test]
fn the_same_conversation_number_on_two_channels_is_two_namespaces() {
    use agentos_core::memory::{MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility};
    use agentos_proto::{ChannelId, ConversationId, Principal};

    let namespace_for = |agent: &str, channel: &str| {
        MemoryScope::new(
            MemoryStore::Semantic,
            MemoryOwner::Conversation(Principal::conversation(
                agentos_proto::AgentId::new(agent),
                ChannelId::new(channel),
                ConversationId::new("42"),
            )),
            MemoryVisibility::Private,
            None,
        )
        .namespace()
    };

    let telegram = namespace_for("main", "telegram");
    let feishu = namespace_for("main", "feishu");
    let other_agent = namespace_for("second", "telegram");

    assert_ne!(telegram, feishu, "two channels, two namespaces");
    assert_ne!(telegram, other_agent, "two agents, two namespaces");
    assert_ne!(feishu, other_agent);
}

/// And the namespace still reads as itself, because an operator has to be able
/// to tell whose rows these are.
#[test]
fn a_conversation_namespace_names_its_principal_readably() {
    use agentos_core::memory::{MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility};
    use agentos_proto::{AgentId, ChannelId, ConversationId, Principal};

    let namespace = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Conversation(Principal::conversation(
            AgentId::new("main"),
            ChannelId::new("telegram"),
            ConversationId::new("42"),
        )),
        MemoryVisibility::Private,
        None,
    )
    .namespace();

    assert_eq!(
        namespace.as_str(),
        "private/conversation/v1.main.telegram.42.n/semantic/general"
    );
}
