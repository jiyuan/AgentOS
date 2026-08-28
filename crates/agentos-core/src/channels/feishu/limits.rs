//! Named ceilings for data controlled by the Feishu transport peer.

/// Bytes one decoded event payload may contain, fragmented or not.
pub(super) const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// Bytes accepted in one WebSocket message or physical frame.
///
/// A protobuf envelope needs a little room around a maximum-sized event. The
/// transport enforces this while reading the WebSocket frame, before returning
/// an owned payload to channel code.
pub(super) const MAX_FRAME_BYTES: usize = MAX_EVENT_BYTES + 64 * 1024;

/// Protobuf headers retained from one frame.
pub(super) const MAX_HEADERS: usize = 128;

/// Bytes retained for one protobuf header key or value.
pub(super) const MAX_HEADER_BYTES: usize = 4 * 1024;

/// Bytes accumulated from one Feishu JSON API response.
pub(super) const MAX_HTTP_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
