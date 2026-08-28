//! Bounded decoding for channel API responses controlled by remote services.

use agentos_interfaces::ChannelError;
use serde_json::Value;
use std::sync::Arc;

pub(crate) async fn json(
    mut response: reqwest::Response,
    service: &str,
    max_bytes: usize,
) -> Result<Value, ChannelError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(too_large(service, max_bytes));
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|err| {
        ChannelError::Backend(Arc::from(format!("{service} response read failed: {err}")))
    })? {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(too_large(service, max_bytes));
        }
        bytes.extend_from_slice(&chunk);
    }

    serde_json::from_slice(&bytes).map_err(|err| {
        ChannelError::Backend(Arc::from(format!(
            "{service} response contains invalid JSON: {err}"
        )))
    })
}

fn too_large(service: &str, max_bytes: usize) -> ChannelError {
    ChannelError::Backend(Arc::from(format!(
        "{service} response exceeds the {max_bytes}-byte limit"
    )))
}
