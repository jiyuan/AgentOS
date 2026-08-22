//! The executor contract, against the real worker binary.
//!
//! M4 / `SBX-001`, verifying `docs/adr/0002-FAIL_CLOSED_ISOLATION.md`. The unit
//! tests in `tools/isolation.rs` and `tools/registry.rs` cover the decision
//! logic with hand-built capability documents; these check that the shipped
//! `agentos-tool-worker` actually answers the handshake that logic is built on,
//! and that the two halves agree about what a failure means.
//!
//! Kernel enforcement itself is `tests/sandbox.rs`. Nothing here needs a
//! sandbox to be available: an executor on a machine with no backend is
//! supposed to *say so*, and that is one of the cases under test.

use agentos_core::sandbox::{availability, Availability, Sandbox};
use agentos_core::tools::isolation::{probe, EXECUTOR_PROTOCOL_VERSION};
use agentos_core::tools::{call_isolated_subprocess, IsolatedCallError};
use agentos_interfaces::tool::SandboxMode;
use agentos_proto::{ToolCall, ToolCallId};
use serde_json::value::RawValue;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Built by `cargo test` alongside this file, so it is always the worker from
/// this tree rather than whatever is installed.
const WORKER: &str = env!("CARGO_BIN_EXE_agentos-tool-worker");

fn call(name: &str) -> ToolCall {
    ToolCall {
        id: ToolCallId::new("call-1"),
        name: Arc::from(name),
        args: RawValue::from_string(r#"{"command":"true"}"#.to_owned()).expect("valid JSON"),
    }
}

#[tokio::test]
async fn the_worker_describes_itself_when_asked() {
    let capabilities = probe(Path::new(WORKER))
        .await
        .expect("the shipped worker speaks the handshake");

    assert_eq!(capabilities.protocol, EXECUTOR_PROTOCOL_VERSION);
    assert_eq!(capabilities.worker, "agentos-tool-worker");
    assert!(
        capabilities.tools.contains("shell"),
        "the worker carries the shell tool: {:?}",
        capabilities.tools
    );
}

/// The point of asking rather than assuming: the answer tracks the machine.
///
/// Both processes run on the same kernel, so the worker's report and this
/// process's own probe must agree. A worker that claimed enforcement its
/// machine cannot provide would be the ADR-0002 defect moved one process
/// along.
#[tokio::test]
async fn the_worker_reports_the_enforcement_its_machine_actually_has() {
    let capabilities = probe(Path::new(WORKER)).await.expect("handshake");

    match availability() {
        Availability::Enforced(mechanism) => {
            assert_eq!(capabilities.sandbox_mechanism.as_deref(), Some(mechanism));
            assert_eq!(
                capabilities.can_run("shell", SandboxMode::WorkspaceWrite),
                Ok(())
            );
        }
        Availability::Unavailable(_) => {
            assert_eq!(capabilities.sandbox_mechanism, None);
            assert!(
                capabilities.sandbox_modes.is_empty(),
                "a machine with no backend enforces no mode: {:?}",
                capabilities.sandbox_modes
            );
            assert!(capabilities
                .can_run("shell", SandboxMode::WorkspaceWrite)
                .is_err());
        }
    }
}

/// A tool the worker has no implementation for is refused by the worker with
/// the exit code that means "my problem, not the model's" — so the registry
/// aborts the run rather than handing the model a failure it cannot replan
/// around.
#[tokio::test]
async fn an_unsupported_tool_is_an_executor_failure_not_a_tool_failure() {
    let sandbox = Sandbox::unrestricted();
    let error = call_isolated_subprocess(
        Path::new(WORKER),
        &call("scrape_page"),
        Duration::from_secs(10),
        &sandbox,
        64 * 1024,
    )
    .await
    .expect_err("the worker has no scrape_page");

    assert!(
        matches!(error, IsolatedCallError::Executor(_)),
        "expected an executor failure, got {error:?}"
    );
    assert!(error.to_string().contains("scrape_page"), "{error}");
}

/// The other half of the split: a tool that runs and fails stays recoverable.
///
/// `shell` refuses a command its argument schema rejects, which is an ordinary
/// tool error — the model reads it and tries something else. Before the exit
/// codes were separated this was indistinguishable from the case above.
#[tokio::test]
async fn a_tool_that_runs_and_fails_stays_a_tool_failure() {
    let bad_args = ToolCall {
        id: ToolCallId::new("call-2"),
        name: Arc::from("shell"),
        args: RawValue::from_string(r#"{"nonsense":true}"#.to_owned()).expect("valid JSON"),
    };
    let sandbox = Sandbox::unrestricted();
    let error = call_isolated_subprocess(
        Path::new(WORKER),
        &bad_args,
        Duration::from_secs(10),
        &sandbox,
        64 * 1024,
    )
    .await
    .expect_err("shell rejects arguments it cannot parse");

    assert!(
        matches!(error, IsolatedCallError::Tool(_)),
        "expected a tool failure, got {error:?}"
    );
}

/// An argument the worker does not recognise is refused rather than ignored.
/// A caller passing one thinks this binary is something else, and running the
/// stdin request anyway would be guessing on its behalf.
#[tokio::test]
async fn the_worker_refuses_arguments_it_does_not_understand() {
    let output = tokio::process::Command::new(WORKER)
        .arg("--pretty-please")
        .output()
        .await
        .expect("the worker runs");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--capabilities"),
        "the refusal should say what the binary does accept"
    );
}
