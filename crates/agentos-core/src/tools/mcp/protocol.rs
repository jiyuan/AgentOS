//! The MCP wire format, as MCP defines it (M8 / `MCP-001`, deliverable 6).
//!
//! # What was there before
//!
//! `tools/mcp.rs` spoke a dialect that borrowed JSON-RPC's envelope and none of
//! its rules, and MCP's method names and none of its semantics. There was no
//! `initialize`, so no protocol version was ever agreed and no capability was
//! ever negotiated. `tools/list` took a `server_id` parameter MCP has never
//! had and returned AgentOS's own [`ToolSpec`] — meaning only a server built
//! against this repository could answer it. `tools/call` sent an AgentOS
//! [`ToolCall`] and expected an AgentOS [`ToolResult`] back, so a real MCP
//! server's `{"content": [...]}` was a deserialization failure. There was no
//! `nextCursor`, so a server with more tools than one page silently offered a
//! prefix. And the request `id` was generated and never checked against the
//! reply, so one stray line desynchronized every later call: from that point
//! on, every answer belonged to the previous question.
//!
//! None of that is a bug in the sense of a mistake in the code. It is a
//! different protocol that happened to be called MCP.
//!
//! # What this is
//!
//! JSON-RPC 2.0 envelopes and the MCP methods, pinned to one protocol revision
//! and prepared to accept the ones this build knows how to read. Nothing here
//! does I/O; [`super::connection`] owns the transport, and keeping the two
//! apart is what makes the wire format testable without a child process.

use agentos_interfaces::tool::{SandboxMode, ToolSpec};
use agentos_proto::{ToolResult, ToolStatus};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use thiserror::Error;

/// The MCP revision this client asks for.
///
/// Pinned rather than "latest": a client that sends whatever it was compiled
/// against and accepts whatever comes back is not negotiating, and the whole
/// point of the handshake is that both sides know which rules apply.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// Revisions this build can read a response from, newest first.
///
/// A server that answers with one of these is accepted; the differences
/// between them do not reach the subset of MCP used here (tools only). A
/// server that answers with anything else is refused at startup rather than at
/// the first surprising field.
pub const SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

/// What this client calls itself in `initialize`.
pub const CLIENT_NAME: &str = "agentos";

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("MCP message is not valid JSON: {0}")]
    Malformed(Arc<str>),
    #[error("MCP message is not JSON-RPC 2.0: {0}")]
    NotJsonRpc(Arc<str>),
    #[error("MCP server returned an error: {code} {message}")]
    Rpc {
        code: i64,
        message: Arc<str>,
        data: Option<Value>,
    },
    #[error("MCP server offered protocol version '{offered}'; this build speaks {supported:?}")]
    UnsupportedVersion {
        offered: Arc<str>,
        supported: &'static [&'static str],
    },
    #[error("MCP server does not advertise the 'tools' capability")]
    NoToolsCapability,
    #[error("MCP response has the wrong shape for {method}: {reason}")]
    Unexpected {
        method: &'static str,
        reason: Arc<str>,
    },
}

/// A JSON-RPC request id.
///
/// Numeric here, always. MCP permits strings too, and a server must echo
/// whatever it was sent — but *generating* strings buys nothing and a
/// monotonic integer makes "is this the reply to the request I am waiting on"
/// a comparison rather than a parse.
pub type RequestId = u64;

/// One outbound JSON-RPC message.
#[derive(Debug, Serialize)]
pub struct Request {
    pub jsonrpc: &'static str,
    /// Absent for a notification, which by definition has no reply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl Request {
    pub fn call(id: RequestId, method: &'static str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id: Some(id),
            method,
            params: Some(params),
        }
    }

    pub fn notify(method: &'static str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id: None,
            method,
            params: Some(params),
        }
    }

    /// One line of newline-delimited JSON, which is MCP's stdio framing.
    ///
    /// `serde_json::to_string` never emits a bare newline inside a JSON string
    /// — it escapes them — so the framing cannot be broken by content.
    pub fn encode(&self) -> Result<String, ProtocolError> {
        let mut line = serde_json::to_string(self)
            .map_err(|err| ProtocolError::Malformed(Arc::from(err.to_string())))?;
        line.push('\n');
        Ok(line)
    }
}

/// One inbound JSON-RPC message, before it is known which kind it is.
#[derive(Debug, Deserialize)]
pub struct Incoming {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// What an inbound line turned out to be.
#[derive(Debug)]
pub enum Message {
    /// A reply to a request this client sent.
    Response {
        id: RequestId,
        outcome: Result<Value, ProtocolError>,
    },
    /// A server-initiated notification. Kept rather than dropped so
    /// `notifications/tools/list_changed` and the like have somewhere to go.
    Notification { method: String, params: Value },
    /// A server-initiated *request*. This client implements no server-callable
    /// methods, so these are answered with `method not found` rather than
    /// ignored — a server waiting forever for a reply is worse than a server
    /// told no.
    ServerRequest { id: Value, method: String },
}

/// Classify one inbound line.
pub fn parse(line: &str) -> Result<Message, ProtocolError> {
    let incoming: Incoming = serde_json::from_str(line)
        .map_err(|err| ProtocolError::Malformed(Arc::from(err.to_string())))?;
    if incoming.jsonrpc.as_deref() != Some("2.0") {
        return Err(ProtocolError::NotJsonRpc(Arc::from(format!(
            "jsonrpc field was {:?}",
            incoming.jsonrpc
        ))));
    }

    if let Some(method) = incoming.method {
        return Ok(match incoming.id {
            Some(id) => Message::ServerRequest { id, method },
            None => Message::Notification {
                method,
                params: incoming.params.unwrap_or(Value::Null),
            },
        });
    }

    // A response. Its id has to be one this client can match, which means a
    // number: anything else is a reply to a request nobody here sent.
    let id = incoming
        .id
        .as_ref()
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ProtocolError::NotJsonRpc(Arc::from(format!(
                "response id {:?} is not one this client issued",
                incoming.id
            )))
        })?;
    let outcome = match (incoming.result, incoming.error) {
        (_, Some(error)) => Err(ProtocolError::Rpc {
            code: error.code,
            message: Arc::from(error.message.as_str()),
            data: error.data,
        }),
        (Some(result), None) => Ok(result),
        (None, None) => Err(ProtocolError::NotJsonRpc(Arc::from(
            "response carried neither result nor error",
        ))),
    };
    Ok(Message::Response { id, outcome })
}

/// JSON-RPC's code for a method the peer does not implement.
pub const METHOD_NOT_FOUND: i64 = -32601;

/// A structured refusal for a server-initiated request this client cannot
/// answer, as one line ready to write.
///
/// The id is echoed verbatim — JSON-RPC requires it, and a server's own ids
/// may be strings, which is fine because this side never has to match them.
pub fn method_not_found(id: &Value, method: &str) -> String {
    let mut line = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": METHOD_NOT_FOUND,
            "message": format!("agentos does not implement '{method}'"),
        },
    }))
    .unwrap_or_else(|_| {
        // Serializing a document built from a `Value` that already parsed
        // cannot fail; this arm exists so the reader task has no `expect`.
        format!(
            r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":{METHOD_NOT_FOUND},"message":"unsupported"}}}}"#
        )
    });
    line.push('\n');
    line
}

/// `initialize` params.
pub fn initialize_params() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        // Truthful rather than aspirational: this client consumes tools and
        // implements no roots, sampling, or elicitation, so it claims none.
        // A server reads this to decide what it may ask of us.
        "capabilities": {},
        "clientInfo": {
            "name": CLIENT_NAME,
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

/// What the server said about itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerHandshake {
    pub protocol_version: Arc<str>,
    pub server_name: Arc<str>,
    pub server_version: Arc<str>,
    /// Whether the server told us it will send
    /// `notifications/tools/list_changed`.
    pub tools_list_changed: bool,
}

/// Read the `initialize` result, refusing a version this build cannot read.
pub fn parse_initialize(result: &Value) -> Result<ServerHandshake, ProtocolError> {
    let unexpected = |reason: &str| ProtocolError::Unexpected {
        method: "initialize",
        reason: Arc::from(reason),
    };
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| unexpected("no protocolVersion"))?;
    if !SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol_version) {
        return Err(ProtocolError::UnsupportedVersion {
            offered: Arc::from(protocol_version),
            supported: &SUPPORTED_PROTOCOL_VERSIONS,
        });
    }
    // A server with no `tools` capability has no tools to offer, and every
    // later `tools/list` would be a method the server never claimed. Refused
    // at the handshake, where the operator can act on it.
    let tools = result
        .get("capabilities")
        .and_then(|capabilities| capabilities.get("tools"))
        .ok_or(ProtocolError::NoToolsCapability)?;
    let info = result.get("serverInfo");
    Ok(ServerHandshake {
        protocol_version: Arc::from(protocol_version),
        server_name: Arc::from(
            info.and_then(|info| info.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("unnamed"),
        ),
        server_version: Arc::from(
            info.and_then(|info| info.get("version"))
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        ),
        tools_list_changed: tools
            .get("listChanged")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// One page of `tools/list`.
pub struct ToolPage {
    pub tools: Vec<ToolSpec>,
    /// Present when the server has more. An absent cursor ends the walk; a
    /// *repeated* cursor is the caller's problem to notice, and
    /// [`super::connection`] does.
    pub next_cursor: Option<String>,
}

/// Read a `tools/list` result into this runtime's [`ToolSpec`]s.
///
/// `sandbox` is applied here rather than read from the server: what a tool may
/// do to *this* filesystem is the deployment's decision, and a server that
/// could name its own sandbox mode would be choosing its own restrictions.
pub fn parse_tool_page(result: &Value, sandbox: SandboxMode) -> Result<ToolPage, ProtocolError> {
    let unexpected = |reason: &str| ProtocolError::Unexpected {
        method: "tools/list",
        reason: Arc::from(reason),
    };
    let listed = result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("no tools array"))?;
    let mut tools = Vec::with_capacity(listed.len());
    for descriptor in listed {
        let name = descriptor
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| unexpected("a tool has no name"))?;
        tools.push(ToolSpec {
            name: Arc::from(name),
            description: Arc::from(
                descriptor
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            ),
            // MCP's `inputSchema` is a JSON Schema object, which is what
            // `ToolSpec::input_schema` already is. A server that omits it is
            // saying "no arguments", not "any arguments".
            input_schema: descriptor
                .get("inputSchema")
                .cloned()
                .unwrap_or_else(|| json!({ "type": "object", "properties": {} })),
            sandbox,
            timeout_ms: None,
        });
    }
    Ok(ToolPage {
        tools,
        next_cursor: result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

/// `tools/call` params.
pub fn call_params(name: &str, arguments: &Value) -> Value {
    json!({ "name": name, "arguments": arguments })
}

/// `notifications/cancelled` params, for a call this side gave up on.
pub fn cancelled_params(id: RequestId, reason: &str) -> Value {
    json!({ "requestId": id, "reason": reason })
}

/// Render a `tools/call` result as a [`ToolResult`].
///
/// The transcript is text, and MCP content is not. Text blocks are joined;
/// everything else becomes a one-line description of what was returned. That
/// is a deliberate loss: an image's bytes have no useful rendering in a
/// transcript, and silently dropping the block would tell the model the tool
/// returned nothing.
pub fn tool_result_from(
    call_id: agentos_proto::ToolCallId,
    result: &Value,
) -> Result<ToolResult, ProtocolError> {
    let blocks = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ProtocolError::Unexpected {
            method: "tools/call",
            reason: Arc::from("no content array"),
        })?;
    let mut rendered = Vec::with_capacity(blocks.len());
    for block in blocks {
        rendered.push(render_block(block));
    }
    // `isError` is the *tool's* failure, reported through a successful
    // JSON-RPC response on purpose: the model reads it and replans, exactly as
    // it would an in-process tool error. A JSON-RPC error means the call never
    // ran, which is a different thing and never arrives here.
    let failed = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut metadata = std::collections::BTreeMap::new();
    if let Some(structured) = result.get("structuredContent") {
        metadata.insert(Arc::from("mcp_structured_content"), structured.clone());
    }
    Ok(ToolResult {
        call_id,
        status: if failed {
            ToolStatus::Failed
        } else {
            ToolStatus::Succeeded
        },
        content: Arc::from(rendered.join("\n")),
        metadata,
    })
}

fn render_block(block: &Value) -> String {
    let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        "image" | "audio" => {
            let mime = block
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            let bytes = block
                .get("data")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0);
            format!("[{kind} {mime}, {bytes} base64 bytes]")
        }
        "resource" => {
            let resource = block.get("resource");
            let uri = resource
                .and_then(|resource| resource.get("uri"))
                .and_then(Value::as_str)
                .unwrap_or("(no uri)");
            // An embedded text resource is readable, so it is included; a
            // blob is not, so it is described.
            match resource
                .and_then(|resource| resource.get("text"))
                .and_then(Value::as_str)
            {
                Some(text) => format!("[resource {uri}]\n{text}"),
                None => format!("[resource {uri}]"),
            }
        }
        "resource_link" => {
            let uri = block
                .get("uri")
                .and_then(Value::as_str)
                .unwrap_or("(no uri)");
            format!("[resource_link {uri}]")
        }
        // An unknown block type is a newer revision's, or a broken server's.
        // Named rather than dropped: the model should see that something came
        // back that this build could not read.
        other => format!("[unsupported content block '{other}']"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::ToolCallId;

    #[test]
    fn a_request_is_one_line_of_json_rpc() {
        let encoded = Request::call(7, "tools/list", json!({ "cursor": "p2" }))
            .encode()
            .expect("it encodes");
        assert!(encoded.ends_with('\n'));
        assert_eq!(encoded.matches('\n').count(), 1, "one message, one line");
        let value: Value = serde_json::from_str(&encoded).expect("valid JSON");
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], 7);
        assert_eq!(value["method"], "tools/list");
    }

    /// A notification has no id, which is how the peer knows not to answer it.
    #[test]
    fn a_notification_carries_no_id() {
        let encoded = Request::notify("notifications/initialized", json!({}))
            .encode()
            .expect("it encodes");
        let value: Value = serde_json::from_str(&encoded).expect("valid JSON");
        assert!(value.get("id").is_none(), "got {value}");
    }

    /// Content in a text block cannot break the framing, because JSON escapes
    /// the newline.
    #[test]
    fn an_embedded_newline_does_not_split_a_message() {
        let encoded = Request::call(
            1,
            "tools/call",
            call_params("echo", &json!({"text": "a\nb"})),
        )
        .encode()
        .expect("it encodes");
        assert_eq!(encoded.matches('\n').count(), 1, "got {encoded:?}");
    }

    #[test]
    fn a_response_is_matched_to_its_id() {
        let message = parse(r#"{"jsonrpc":"2.0","id":42,"result":{"ok":true}}"#).expect("parses");
        match message {
            Message::Response { id, outcome } => {
                assert_eq!(id, 42);
                assert_eq!(outcome.expect("a result")["ok"], true);
            }
            other => panic!("expected a response, got {other:?}"),
        }
    }

    #[test]
    fn a_structured_error_survives_with_its_code() {
        let message = parse(
            r#"{"jsonrpc":"2.0","id":42,"error":{"code":-32601,"message":"no such method"}}"#,
        )
        .expect("parses");
        let Message::Response { outcome, .. } = message else {
            panic!("expected a response");
        };
        match outcome.expect_err("an error") {
            ProtocolError::Rpc { code, message, .. } => {
                assert_eq!(code, -32601);
                assert_eq!(message.as_ref(), "no such method");
            }
            other => panic!("expected an rpc error, got {other}"),
        }
    }

    /// A server blocked on a reply that never comes is a server that stops
    /// answering us, so an unimplemented method gets a structured refusal.
    #[test]
    fn an_unimplemented_server_request_gets_a_structured_refusal() {
        let line = method_not_found(&json!("s1"), "sampling/createMessage");
        assert!(line.ends_with('\n'));
        let value: Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], "s1", "the id has to be echoed verbatim");
        assert_eq!(value["error"]["code"], METHOD_NOT_FOUND);
        assert!(value["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("sampling/createMessage"));
    }

    #[test]
    fn notifications_and_server_requests_are_told_apart() {
        let notification =
            parse(r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#)
                .expect("parses");
        assert!(matches!(notification, Message::Notification { .. }));

        let request =
            parse(r#"{"jsonrpc":"2.0","id":"s1","method":"roots/list"}"#).expect("parses");
        assert!(matches!(request, Message::ServerRequest { .. }));
    }

    #[test]
    fn a_line_that_is_not_json_rpc_is_refused_rather_than_guessed_at() {
        assert!(matches!(
            parse("not json").expect_err("must fail"),
            ProtocolError::Malformed(_)
        ));
        assert!(matches!(
            parse(r#"{"id":1,"result":{}}"#).expect_err("must fail"),
            ProtocolError::NotJsonRpc(_)
        ));
        // A response with neither result nor error is still *addressed*, so
        // it is delivered as a failure to whoever is waiting on that id
        // rather than dropped — dropping it would leave them on their
        // deadline for no reason.
        let Message::Response { id, outcome } =
            parse(r#"{"jsonrpc":"2.0","id":1}"#).expect("it is addressed")
        else {
            panic!("expected a response");
        };
        assert_eq!(id, 1);
        assert!(matches!(
            outcome.expect_err("must fail"),
            ProtocolError::NotJsonRpc(_)
        ));
    }

    #[test]
    fn the_handshake_is_read_and_an_unknown_version_is_refused() {
        let handshake = parse_initialize(&json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": true } },
            "serverInfo": { "name": "fixture", "version": "0.1.0" },
        }))
        .expect("the handshake reads");
        assert_eq!(handshake.server_name.as_ref(), "fixture");
        assert!(handshake.tools_list_changed);

        let err = parse_initialize(&json!({
            "protocolVersion": "1999-01-01",
            "capabilities": { "tools": {} },
        }))
        .expect_err("an unknown revision must be refused");
        assert!(matches!(err, ProtocolError::UnsupportedVersion { .. }));

        let err = parse_initialize(&json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
        }))
        .expect_err("a server with no tools capability must be refused");
        assert!(matches!(err, ProtocolError::NoToolsCapability));
    }

    /// The half that was missing entirely: a server with more tools than one
    /// page used to be silently truncated to the first page.
    #[test]
    fn a_page_carries_its_cursor() {
        let page = parse_tool_page(
            &json!({
                "tools": [
                    { "name": "a", "description": "first", "inputSchema": {"type":"object"} },
                    { "name": "b" },
                ],
                "nextCursor": "page-2",
            }),
            SandboxMode::ReadOnly,
        )
        .expect("the page reads");
        assert_eq!(page.tools.len(), 2);
        assert_eq!(page.tools[0].name.as_ref(), "a");
        assert_eq!(page.tools[1].description.as_ref(), "");
        assert_eq!(page.next_cursor.as_deref(), Some("page-2"));
        assert!(page
            .tools
            .iter()
            .all(|tool| tool.sandbox == SandboxMode::ReadOnly));

        let last = parse_tool_page(&json!({ "tools": [] }), SandboxMode::ReadOnly)
            .expect("the page reads");
        assert_eq!(last.next_cursor, None);
    }

    #[test]
    fn text_blocks_are_joined_and_other_blocks_are_described() {
        let result = tool_result_from(
            ToolCallId::new("call-1"),
            &json!({
                "content": [
                    { "type": "text", "text": "first" },
                    { "type": "image", "mimeType": "image/png", "data": "AAAA" },
                    { "type": "resource", "resource": { "uri": "file:///x", "text": "inline" } },
                    { "type": "resource_link", "uri": "file:///y" },
                    { "type": "video", "data": "…" },
                ],
            }),
        )
        .expect("the result reads");
        assert_eq!(result.status, ToolStatus::Succeeded);
        assert_eq!(
            result.content.as_ref(),
            "first\n[image image/png, 4 base64 bytes]\n[resource file:///x]\ninline\n\
             [resource_link file:///y]\n[unsupported content block 'video']"
        );
    }

    /// `isError` is a failed *tool*, not a failed call: the model reads it and
    /// replans. A JSON-RPC error means the call never ran and never gets here.
    #[test]
    fn is_error_becomes_a_failed_tool_result() {
        let result = tool_result_from(
            ToolCallId::new("call-1"),
            &json!({ "content": [{ "type": "text", "text": "no such file" }], "isError": true }),
        )
        .expect("the result reads");
        assert_eq!(result.status, ToolStatus::Failed);
        assert_eq!(result.content.as_ref(), "no such file");
    }
}
