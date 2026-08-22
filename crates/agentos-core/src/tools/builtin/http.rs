use super::common::{elapsed_ms, result_metadata};
use crate::egress::{fetch_bounded, tool_client, DEFAULT_MAX_RESPONSE_BYTES};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long one fetch may take, and how much of the answer it may keep
/// (M4 / `NET-001`).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpTool {
    max_response_bytes: usize,
}

impl Default for HttpTool {
    fn default() -> Self {
        Self {
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

impl HttpTool {
    pub fn with_response_limit(max_response_bytes: usize) -> Self {
        Self { max_response_bytes }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpArgs {
    url: String,
    #[serde(default = "default_get")]
    method: String,
}

#[async_trait]
impl Tool for HttpTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("http"),
            description: Arc::from(
                "Fetch an HTTP or HTTPS URL with a GET request. Only publicly \
                 routable addresses are reachable: loopback, private networks, \
                 link-local and cloud metadata endpoints are refused, on the \
                 requested URL and on every redirect it follows.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "enum": ["GET"] }
                }
            }),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: HttpArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        if !parsed.method.eq_ignore_ascii_case("GET") {
            return Err(ToolError::Failed(Arc::from("http tool only supports GET")));
        }

        let start = Instant::now();
        // Scheme, destination, redirect chain and response size are all
        // decided by the egress module — the client this uses resolves through
        // `GuardedResolver`, so there is no way to reach a private address by
        // pointing a name at one.
        let fetched = match fetch_bounded(
            tool_client(),
            &parsed.url,
            self.max_response_bytes,
            REQUEST_TIMEOUT,
        )
        .await
        {
            Ok(fetched) => fetched,
            // Every failure here is the model's problem to work around, not
            // the run's to die on. A refused destination is flagged as such,
            // because "you may not go there" and "it did not answer" call for
            // different next moves and the model can only tell them apart if
            // the result says which happened.
            Err(error) => {
                let refused = error.is_egress_refusal();
                let mut metadata = result_metadata(elapsed_ms(start), 0);
                if refused {
                    metadata.insert(Arc::from("egress_refused"), Value::Bool(true));
                }
                return Ok(ToolResult {
                    call_id: call.id.clone(),
                    status: ToolStatus::Failed,
                    content: Arc::from(error.to_string()),
                    metadata,
                });
            }
        };

        let bytes_out = fetched.body.len() as u64;
        let mut metadata = result_metadata(elapsed_ms(start), bytes_out);
        metadata.insert(
            Arc::from("status_line"),
            Value::String(format!(
                "HTTP {} {}",
                fetched.status.as_u16(),
                fetched.status.canonical_reason().unwrap_or("")
            )),
        );
        if fetched.final_url.as_ref() != parsed.url {
            metadata.insert(
                Arc::from("final_url"),
                Value::String(fetched.final_url.to_string()),
            );
        }
        if fetched.truncated {
            metadata.insert(Arc::from("truncated"), Value::Bool(true));
        }

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: status_from_http_code(fetched.status.as_u16()),
            content: Arc::from(fetched.body),
            metadata,
        })
    }
}

fn status_from_http_code(code: u16) -> ToolStatus {
    if (200..300).contains(&code) {
        ToolStatus::Succeeded
    } else {
        ToolStatus::Failed
    }
}

fn default_get() -> String {
    "GET".to_owned()
}
