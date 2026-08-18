//! Serializable wire types shared across Agent OS crates and process boundaries.

pub mod delegation;
pub mod envelope;
pub mod ids;
pub mod message;
pub mod request;
pub mod tool;
pub mod trace;
pub mod usage;

pub use delegation::{
    DelegationGrant, DelegationGrantScope, DELEGATION_GRANT_SCOPES_KEY, DELEGATION_GRANT_TTL_KEY,
};
pub use envelope::Envelope;
pub use ids::{
    decode_base64url, encode_base64url, AgentId, ChannelId, ConversationId, InterruptionId,
    Namespace, PrincipalKey, PrincipalKeyV1, RecordId, RunId, SchemaVersion, SenderId,
    SenderIdentity, SessionKey, SpanId, TaskId, ToolCallId,
};
pub use message::{Attachment, AttachmentKind, Message, MessageRole};
pub use request::{RequestHeader, RequestSection, RequestSource};
pub use tool::{ToolCall, ToolResult, ToolStatus};
pub use trace::{SpanKind, TraceEvent, TraceSpan};
pub use usage::{Usage, TOKEN_USAGE_METADATA_KEY};
