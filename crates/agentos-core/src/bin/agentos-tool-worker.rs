use agentos_core::tools::ShellTool;
use agentos_interfaces::tool::Tool;
use agentos_proto::ToolCall;
use serde::Deserialize;
use std::io::{self, Read, Write};

#[derive(Deserialize)]
struct WorkerRequest {
    call: ToolCall,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        let _ = writeln!(io::stderr(), "{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .map_err(|err| err.to_string())?;
    let request: WorkerRequest = serde_json::from_slice(&input).map_err(|err| err.to_string())?;

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
                .map_err(|err| err.to_string())?;
            serde_json::to_writer(io::stdout(), &result).map_err(|err| err.to_string())?;
            Ok(())
        }
        other => Err(format!("isolated worker does not support tool '{other}'")),
    }
}
