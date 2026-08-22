//! Serializable wire types shared across Agent OS crates and process boundaries.

pub mod envelope;
pub mod ids;
pub mod message;
pub mod principal;
pub mod request;
pub mod tool;
pub mod trace;
pub mod usage;

pub use envelope::{Envelope, INGRESS_ID_KEY};
pub use ids::{
    AgentId, ChannelId, ConversationId, InterruptionId, Namespace, RecordId, RunId, SchemaVersion,
    SpanId, TaskId, ToolCallId,
};
pub use message::{Attachment, AttachmentKind, Message, MessageRole};
pub use principal::{
    base64url, base64url_decode, decode_component, encode_component, Principal, PRINCIPAL_VERSION,
};
pub use request::{RequestHeader, RequestSection, RequestSource};
pub use tool::{ToolCall, ToolResult, ToolStatus};
pub use trace::{SpanKind, TraceEvent, TraceSpan};
pub use usage::{Usage, TOKEN_USAGE_METADATA_KEY};
