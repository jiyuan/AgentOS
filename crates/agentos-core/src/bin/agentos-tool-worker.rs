//! The isolated executor: one tool call, one process, one sandbox.
//!
//! Invoked two ways. With [`CAPABILITIES_FLAG`] it prints what it can do and
//! exits — the M4 / `SBX-001` handshake, so the registry can refuse an
//! incompatible pairing before dispatching rather than discovering it from a
//! non-zero exit. With no arguments it reads one JSON request from stdin, runs
//! the named tool, and writes the `ToolResult` to stdout.
//!
//! # Exit codes are load-bearing
//!
//! [`EXIT_TOOL_FAILED`] means the tool ran and returned an error: the model
//! reads it and replans. [`EXIT_WORKER_FAILED`] means this process could not
//! run the tool at all — unreadable request, unknown tool, sandbox refused —
//! and the run aborts, because replanning cannot conjure a containment
//! boundary. Collapsing the two, as this binary used to, made a missing
//! sandbox look like a bad file path.

use agentos_core::tools::isolation::{
    ExecutorCapabilities, CAPABILITIES_FLAG, EXIT_TOOL_FAILED, EXIT_WORKER_FAILED,
};
use agentos_core::tools::ShellTool;
use agentos_interfaces::tool::Tool;
use agentos_proto::ToolCall;
use serde::Deserialize;
use std::io::{self, Read, Write};

/// Tools compiled into this worker.
///
/// The registry asks for this list rather than assuming it. A tool that is not
/// here and declares a sandbox mode is refused at dispatch with a message
/// naming the executor, instead of reaching this process and being rejected by
/// the fallthrough arm below.
const SUPPORTED_TOOLS: &[&str] = &["shell"];

#[derive(Deserialize)]
struct WorkerRequest {
    call: ToolCall,
}

/// A failure, and which side of the boundary it came from.
struct WorkerFailure {
    message: String,
    exit_code: i32,
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        let _ = writeln!(io::stderr(), "{}", failure.message);
        std::process::exit(failure.exit_code);
    }
}

async fn run() -> Result<(), WorkerFailure> {
    // Skip argv[0]. Any argument at all other than the handshake is a caller
    // that thinks this binary is something else, and is refused rather than
    // ignored.
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some(CAPABILITIES_FLAG) if args.next().is_none() => {
            let capabilities = ExecutorCapabilities::for_this_worker(
                env!("CARGO_BIN_NAME"),
                SUPPORTED_TOOLS.iter().copied(),
            );
            serde_json::to_writer(io::stdout(), &capabilities).map_err(worker_failure)?;
            return Ok(());
        }
        None => {}
        Some(other) => {
            let other = other.to_owned();
            return Err(WorkerFailure {
                message: format!(
                    "unexpected argument '{other}'; pass {CAPABILITIES_FLAG} or a request on stdin"
                ),
                exit_code: EXIT_WORKER_FAILED,
            });
        }
    }

    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .map_err(worker_failure)?;
    let request: WorkerRequest = serde_json::from_slice(&input).map_err(worker_failure)?;

    match request.call.name.as_ref() {
        "shell" => {
            // The default output cap, not the deployment's: the worker is a
            // separate process and does not read `agent.toml`. The registry
            // that spawned it applies the configured cap to what it reads back
            // from this process, which is the bound that reaches the model.
            let tool = ShellTool::default();
            let result = tool
                .call(&request.call, &request.call.args)
                .await
                // The one failure that belongs to the tool rather than to this
                // process, and the only one that exits `EXIT_TOOL_FAILED`.
                .map_err(|err| WorkerFailure {
                    message: err.to_string(),
                    exit_code: EXIT_TOOL_FAILED,
                })?;
            serde_json::to_writer(io::stdout(), &result).map_err(worker_failure)?;
            Ok(())
        }
        other => Err(WorkerFailure {
            message: format!("isolated worker does not support tool '{other}'"),
            exit_code: EXIT_WORKER_FAILED,
        }),
    }
}

fn worker_failure(error: impl std::fmt::Display) -> WorkerFailure {
    WorkerFailure {
        message: error.to_string(),
        exit_code: EXIT_WORKER_FAILED,
    }
}
