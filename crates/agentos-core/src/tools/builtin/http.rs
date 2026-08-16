use super::common::{elapsed_ms, result_metadata};
use crate::http::shared_client;
use agentos_interfaces::tool::{Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct HttpTool;

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
            description: Arc::from("Fetch an HTTP or HTTPS URL with a GET request."),
            input_schema: json!({
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "enum": ["GET"] }
                }
            }),
            requires_isolation: false,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: HttpArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        if !parsed.method.eq_ignore_ascii_case("GET") {
            return Err(ToolError::Failed(Arc::from("http tool only supports GET")));
        }
        if !(parsed.url.starts_with("http://") || parsed.url.starts_with("https://")) {
            return Err(ToolError::Failed(Arc::from(
                "http tool requires an http:// or https:// URL",
            )));
        }

        let start = Instant::now();
        let response = shared_client()
            .get(&parsed.url)
            .send()
            .await
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let status_code = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let bytes_out = body.len() as u64;
        let mut metadata = result_metadata(elapsed_ms(start), bytes_out);
        metadata.insert(
            Arc::from("status_line"),
            Value::String(format!(
                "HTTP {} {}",
                status_code.as_u16(),
                status_code.canonical_reason().unwrap_or("")
            )),
        );

        Ok(ToolResult {
            call_id: call.id.clone(),
            status: status_from_http_code(status_code.as_u16()),
            content: Arc::from(body),
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
