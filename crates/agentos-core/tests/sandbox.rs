//! Roadmap X2: the sandbox is enforced by the kernel, or it is not claimed.
//!
//! Every test below asserts *enforcement*, so on a platform AgentOS claims to
//! sandbox — Linux and macOS — a missing backend is a failure, not a skip. The
//! earlier version of this file skipped instead, which made the whole suite
//! green on a kernel without Landlock: a test that reports a guarantee nobody
//! is making is worse than no test (`TEST-001` in
//! `docs/AUDIT_REMEDIATION_PLAN.md`).
//!
//! The one named exception is `AGENTOS_ALLOW_UNSANDBOXED_TESTS=1`, for a
//! machine that genuinely cannot enforce one — an old kernel, a container
//! without the Landlock LSM. It downgrades the failure to a loud skip. CI must
//! not set it; that is what makes the exception visible rather than hidden.
//!
//! Run with `-- --nocapture` to see which mechanism enforced each test.

use agentos_core::sandbox::{availability, Sandbox};
use agentos_core::tools::call_isolated_subprocess;
use agentos_core::tools::exec::{run, Exec, DEFAULT_MAX_OUTPUT_BYTES};
use agentos_interfaces::tool::SandboxMode;
use agentos_proto::{ToolCall, ToolCallId};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The mechanism doing the enforcing, or a failure.
///
/// On Linux and macOS an unavailable sandbox fails: those are the platforms the
/// `SandboxMode` contract is written against, so "no backend here" is a real
/// regression and not a property of the test. Everywhere else there is nothing
/// to enforce and the test skips.
macro_rules! enforced_or_fail {
    ($test:literal) => {
        match availability() {
            agentos_core::sandbox::Availability::Enforced(mechanism) => mechanism,
            agentos_core::sandbox::Availability::Unavailable(reason) => {
                if !cfg!(any(target_os = "linux", target_os = "macos")) {
                    eprintln!("skipping {}: {reason}", $test);
                    return;
                }
                if std::env::var_os("AGENTOS_ALLOW_UNSANDBOXED_TESTS").is_some() {
                    eprintln!(
                        "skipping {}: {reason} \
                         (AGENTOS_ALLOW_UNSANDBOXED_TESTS is set)",
                        $test
                    );
                    return;
                }
                panic!(
                    "{} needs an enforced sandbox on this platform, and there is \
                     none: {reason}. Set AGENTOS_ALLOW_UNSANDBOXED_TESTS=1 to \
                     downgrade this to a skip on a machine that cannot provide \
                     one.",
                    $test
                );
            }
        }
    };
}

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "agentos-sandbox-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir is creatable");
    dir
}

/// A directory outside both the workspace and the temp dir that this process
/// can genuinely write to, so "the sandbox refused it" is distinguishable from
/// "nobody could have written there anyway".
///
/// Returns `None` when there is no such place, which makes the caller skip
/// rather than assert something the machine was going to be true regardless.
fn outside_dir(label: &str) -> Option<PathBuf> {
    let home = PathBuf::from(std::env::var_os("HOME")?);
    let dir = home.join(format!(
        "agentos-sandbox-outside-{label}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).ok()?;
    let probe = dir.join(".probe");
    let writable = std::fs::write(&probe, b"x").is_ok();
    let _ = std::fs::remove_file(&probe);
    writable.then_some(dir)
}

/// `sh -c` so the assertion is about the *child's* view of the filesystem,
/// which is what the sandbox restricts.
async fn write_attempt(sandbox: &Sandbox, target: &Path) -> bool {
    let script = format!("printf x > {}", target.display());
    let args = vec!["-c".to_owned(), script];
    let output = run(Exec {
        program: "sh",
        args: &args,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        sandbox,
    })
    .await
    .expect("sh runs");
    output.success && target.exists()
}

/// The item's verification criterion: a `read_only` tool cannot write to the
/// workspace.
#[tokio::test]
async fn a_read_only_child_cannot_write_to_the_workspace() {
    let mechanism = enforced_or_fail!("a_read_only_child_cannot_write_to_the_workspace");
    let workspace = temp_dir("read-only");
    let target = workspace.join("forbidden.txt");

    // Control first: unsandboxed, this write succeeds. Without it the
    // assertion below would also pass on a machine where nothing could have
    // written there — which is the vacuous pass this item warns about.
    assert!(
        write_attempt(&Sandbox::unrestricted(), &target).await,
        "the control write must succeed, or the assertion below proves nothing"
    );
    std::fs::remove_file(&target).expect("control artifact is removable");

    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());
    assert!(
        !write_attempt(&sandbox, &target).await,
        "{mechanism} must refuse a read_only child's write to {}",
        workspace.display()
    );
    let _ = std::fs::remove_dir_all(&workspace);
}

/// ...and cannot write anywhere else either, which is what distinguishes a
/// sandbox from a `cwd`.
#[tokio::test]
async fn a_read_only_child_cannot_write_outside_it_either() {
    enforced_or_fail!("a_read_only_child_cannot_write_outside_it_either");
    let workspace = temp_dir("read-only-outside");
    let Some(elsewhere) = outside_dir("read-only") else {
        eprintln!("skipping a_read_only_child_cannot_write_outside_it_either: nowhere writable outside the workspace");
        return;
    };
    let target = elsewhere.join("forbidden.txt");

    assert!(write_attempt(&Sandbox::unrestricted(), &target).await);
    std::fs::remove_file(&target).expect("control artifact is removable");

    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());
    assert!(!write_attempt(&sandbox, &target).await);
    let _ = std::fs::remove_dir_all(&workspace);
    let _ = std::fs::remove_dir_all(&elsewhere);
}

/// Reading is untouched. The mode bounds writes; a tool that must not *read*
/// something is a policy question, and pretending otherwise would make the
/// sandbox's scope unclear.
#[tokio::test]
async fn a_read_only_child_can_still_read() {
    enforced_or_fail!("a_read_only_child_can_still_read");
    let workspace = temp_dir("read-only-reads");
    let readable = workspace.join("readable.txt");
    std::fs::write(&readable, "contents").expect("fixture is writable from the test");
    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());

    let args = vec!["-c".to_owned(), format!("cat {}", readable.display())];
    let output = run(Exec {
        program: "sh",
        args: &args,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        sandbox: &sandbox,
    })
    .await
    .expect("sh runs");
    assert!(output.success);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "contents");
    let _ = std::fs::remove_dir_all(&workspace);
}

/// `workspace_write` is the useful middle: the tool can do its work in the
/// workspace and nothing outside it.
#[tokio::test]
async fn a_workspace_write_child_writes_inside_and_not_outside() {
    enforced_or_fail!("a_workspace_write_child_writes_inside_and_not_outside");
    let workspace = temp_dir("workspace-write");
    // Not another temp dir: `workspace_write` grants the temp dir on purpose,
    // so "outside" has to mean somewhere the mode really does refuse.
    let Some(elsewhere) = outside_dir("workspace-write") else {
        eprintln!("skipping a_workspace_write_child_writes_inside_and_not_outside: nowhere writable outside the workspace");
        return;
    };
    let outside_target = elsewhere.join("escape.txt");
    assert!(write_attempt(&Sandbox::unrestricted(), &outside_target).await);
    std::fs::remove_file(&outside_target).expect("control artifact is removable");

    let sandbox = Sandbox::new(SandboxMode::WorkspaceWrite, workspace.clone());
    assert!(
        write_attempt(&sandbox, &workspace.join("allowed.txt")).await,
        "a workspace_write child must be able to write in its workspace"
    );
    assert!(
        !write_attempt(&sandbox, &outside_target).await,
        "a workspace_write child must not write outside its workspace"
    );
    let _ = std::fs::remove_dir_all(&workspace);
    let _ = std::fs::remove_dir_all(&elsewhere);
}

/// The escape hatch a sandbox has to close: a child that spawns a child.
/// Landlock is inherited and Seatbelt wraps the process tree, so the
/// grandchild is bound by the same rules.
#[tokio::test]
async fn a_grandchild_inherits_the_sandbox() {
    enforced_or_fail!("a_grandchild_inherits_the_sandbox");
    let workspace = temp_dir("grandchild");
    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());
    let target = workspace.join("via-grandchild.txt");

    assert!(write_attempt(&Sandbox::unrestricted(), &target).await);
    std::fs::remove_file(&target).expect("control artifact is removable");

    let args = vec![
        "-c".to_owned(),
        format!("sh -c 'printf x > {}'", target.display()),
    ];
    let output = run(Exec {
        program: "sh",
        args: &args,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        sandbox: &sandbox,
    })
    .await
    .expect("sh runs");
    assert!(
        !(output.success && target.exists()),
        "a grandchild must not escape its parent's sandbox"
    );
    let _ = std::fs::remove_dir_all(&workspace);
}

/// `/dev/null` stays writable in every sandboxed mode. Without it
/// `2>/dev/null` fails, which reads as the tool being broken rather than as
/// the sandbox working.
#[tokio::test]
async fn dev_null_stays_writable() {
    enforced_or_fail!("dev_null_stays_writable");
    let workspace = temp_dir("dev-null");
    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());

    let args = vec!["-c".to_owned(), "echo noise > /dev/null".to_owned()];
    let output = run(Exec {
        program: "sh",
        args: &args,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        sandbox: &sandbox,
    })
    .await
    .expect("sh runs");
    assert!(output.success, "writing to /dev/null must stay allowed");
    let _ = std::fs::remove_dir_all(&workspace);
}

/// `full_access` needs no backend and changes nothing, so this one runs
/// everywhere — including the platforms where the tests above skip.
#[tokio::test]
async fn full_access_is_unrestricted_on_every_platform() {
    let workspace = temp_dir("full-access");
    let sandbox = Sandbox::new(SandboxMode::FullAccess, workspace.clone());
    assert!(write_attempt(&sandbox, &workspace.join("allowed.txt")).await);
    let _ = std::fs::remove_dir_all(&workspace);
}

/// The path a deployment actually takes: a tool marked with a sandbox mode is
/// run through the isolation worker, and the worker is what gets restricted.
///
/// Driven with a stand-in worker rather than the real binary, because the real
/// one only knows `shell` and would build its sandbox from the machine's own
/// agentos home. What is under test here is the wiring — that
/// `call_isolated_subprocess` applies the sandbox it is handed to the process
/// it starts.
#[tokio::test]
async fn the_isolation_worker_runs_under_its_sandbox() {
    enforced_or_fail!("the_isolation_worker_runs_under_its_sandbox");
    let workspace = temp_dir("worker");
    let Some(elsewhere) = outside_dir("worker") else {
        eprintln!("skipping the_isolation_worker_runs_under_its_sandbox: nowhere writable outside the workspace");
        return;
    };
    let escape = elsewhere.join("worker-escape.txt");

    // A worker that answers correctly *and* tries to write where it should not.
    let result = serde_json::json!({
        "call_id": "call-1",
        "status": "succeeded",
        "content": "worker ran",
        "metadata": {},
    });
    let worker = workspace.join("worker.sh");
    std::fs::write(
        &worker,
        format!(
            "#!/bin/sh\ncat > /dev/null\nprintf x > {} 2>/dev/null\nprintf '%s' '{}'\n",
            escape.display(),
            result
        ),
    )
    .expect("worker script is writable");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755))
            .expect("worker script is executable");
    }

    let call = ToolCall {
        id: ToolCallId::new("call-1"),
        name: std::sync::Arc::from("shell"),
        args: serde_json::value::RawValue::from_string("{}".to_owned()).expect("valid JSON"),
    };
    let sandbox = Sandbox::new(SandboxMode::ReadOnly, workspace.clone());
    let tool_result = call_isolated_subprocess(
        &worker,
        &call,
        Duration::from_secs(10),
        &sandbox,
        DEFAULT_MAX_OUTPUT_BYTES,
    )
    .await
    .expect("the worker answers");

    assert_eq!(tool_result.content.as_ref(), "worker ran");
    // The mode is recorded on the result, so a trace shows what was enforced.
    assert_eq!(
        tool_result
            .metadata
            .get("sandbox")
            .and_then(serde_json::Value::as_str),
        Some("read_only")
    );
    assert!(
        !escape.exists(),
        "the worker must not be able to write outside its sandbox"
    );
    let _ = std::fs::remove_dir_all(&workspace);
    let _ = std::fs::remove_dir_all(&elsewhere);
}

/// A sandbox that cannot be built fails the call. The alternative — running
/// the tool anyway — is the one outcome nobody asked for.
#[tokio::test]
async fn an_unbuildable_sandbox_fails_the_call() {
    enforced_or_fail!("an_unbuildable_sandbox_fails_the_call");
    // Under a root this process cannot write to, so it cannot be created
    // either — a directory that is merely absent is created on demand.
    let missing = PathBuf::from("/agentos-sandbox-uncreatable/workspace");
    let sandbox = Sandbox::new(SandboxMode::WorkspaceWrite, missing);

    let args = vec!["-c".to_owned(), "true".to_owned()];
    let error = run(Exec {
        program: "sh",
        args: &args,
        cwd: None,
        stdin: None,
        timeout: Duration::from_secs(10),
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        sandbox: &sandbox,
    })
    .await
    .expect_err("a workspace that does not exist cannot be granted");
    assert!(error.to_string().contains("cannot sandbox"), "got: {error}");
}
