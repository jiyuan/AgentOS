//! The send half of the Feishu channel.
//!
//! [`Channel::receive`](agentos_interfaces::Channel::receive) takes `&mut self`,
//! so while `FeishuChannel` is parked on its long connection nothing can call
//! `send` on it. Everything `send` needs therefore lives here, behind an `Arc`
//! the channel hands out through
//! [`Channel::egress`](agentos_interfaces::Channel::egress): the API
//! credentials, the shared tenant-token cache, and the per-conversation
//! edit-in-place state a streamed reply is finalized against.

use super::{
    clamp_feishu_text, feishu_edit_text, feishu_send_text_message, feishu_tenant_token, post_json,
    reqwest_to_channel_err, CachedTenantToken, FEISHU_EDIT_INTERVAL, FEISHU_TEXT_LIMIT,
};
use crate::channels::text::split_text;
use crate::http::shared_client;
use agentos_interfaces::{ChannelError, Egress, StreamEgress};
use agentos_proto::{Attachment, AttachmentKind, ConversationId, Envelope};
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::fs;

/// In-flight streamed reply for one chat: the placeholder message being edited,
/// the accumulated text, and when it was last edited (for throttling).
#[derive(Default)]
pub(super) struct FeishuEditState {
    message_id: Option<String>,
    buffer: String,
    last_edit: Option<Instant>,
}

/// Shareable, `'static` send half of `FeishuChannel`.
pub(super) struct FeishuEgress {
    pub(super) api_base: Arc<str>,
    pub(super) app_id: Arc<str>,
    pub(super) app_secret: Arc<str>,
    pub(super) receive_id_type: Arc<str>,
    pub(super) tenant_token: Arc<Mutex<Option<CachedTenantToken>>>,
    pub(super) stream_state: Arc<Mutex<HashMap<String, FeishuEditState>>>,
}

impl FeishuEgress {
    fn api_url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base, path.trim_start_matches('/'))
    }

    /// The streaming half of this channel, sharing these credentials and the
    /// same per-conversation edit state so a streamed placeholder is finalized
    /// by the reply rather than duplicated by it.
    pub(super) fn stream_handle(&self) -> Arc<dyn StreamEgress> {
        Arc::new(FeishuStreamEgress {
            api_base: Arc::clone(&self.api_base),
            app_id: Arc::clone(&self.app_id),
            app_secret: Arc::clone(&self.app_secret),
            receive_id_type: Arc::clone(&self.receive_id_type),
            tenant_token: Arc::clone(&self.tenant_token),
            state: Arc::clone(&self.stream_state),
        })
    }

    async fn tenant_access_token(&self) -> Result<Arc<str>, ChannelError> {
        feishu_tenant_token(
            &self.api_base,
            &self.app_id,
            &self.app_secret,
            &self.tenant_token,
        )
        .await
    }

    async fn send_text(&self, receive_id: &str, text: &str) -> Result<(), ChannelError> {
        for chunk in split_text(text, FEISHU_TEXT_LIMIT) {
            let content = json!({ "text": chunk }).to_string();
            self.send_message(receive_id, "text", &content).await?;
        }
        Ok(())
    }

    async fn send_message(
        &self,
        receive_id: &str,
        msg_type: &str,
        content_json: &str,
    ) -> Result<(), ChannelError> {
        let token = self.tenant_access_token().await?;
        let body = json!({
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": content_json,
        });
        let url = format!(
            "{}?receive_id_type={}",
            self.api_url("im/v1/messages"),
            self.receive_id_type.as_ref()
        );
        let response: Value = post_json(&url, Some(token.as_ref()), &body).await?;
        if response.get("code").and_then(Value::as_i64) == Some(0) {
            Ok(())
        } else {
            Err(ChannelError::Backend(Arc::from(response.to_string())))
        }
    }
    async fn upload_image(&self, path: &Path) -> Result<String, ChannelError> {
        let token = self.tenant_access_token().await?;
        let bytes = fs::read(path)
            .await
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        let file_name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "image".to_owned());
        let form = Form::new()
            .text("image_type", "message")
            .part("image", Part::bytes(bytes).file_name(file_name));
        let response: Value = shared_client()
            .post(self.api_url("im/v1/images"))
            .bearer_auth(token.as_ref())
            .multipart(form)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(reqwest_to_channel_err)?
            .json()
            .await
            .map_err(reqwest_to_channel_err)?;
        if response.get("code").and_then(Value::as_i64) != Some(0) {
            return Err(ChannelError::Backend(Arc::from(response.to_string())));
        }
        response
            .get("data")
            .and_then(|d| d.get("image_key"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from("Feishu image upload missing image_key"))
            })
    }

    async fn upload_file(&self, name: &str, path: &Path) -> Result<String, ChannelError> {
        let token = self.tenant_access_token().await?;
        let bytes = fs::read(path)
            .await
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        let part = Part::bytes(bytes).file_name(name.to_owned());
        let form = Form::new()
            .text("file_type", "stream")
            .text("file_name", name.to_owned())
            .part("file", part);
        let response: Value = shared_client()
            .post(self.api_url("im/v1/files"))
            .bearer_auth(token.as_ref())
            .multipart(form)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(reqwest_to_channel_err)?
            .json()
            .await
            .map_err(reqwest_to_channel_err)?;
        if response.get("code").and_then(Value::as_i64) != Some(0) {
            return Err(ChannelError::Backend(Arc::from(response.to_string())));
        }
        response
            .get("data")
            .and_then(|d| d.get("file_key"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| ChannelError::Backend(Arc::from("Feishu file upload missing file_key")))
    }

    async fn send_attachment(
        &self,
        receive_id: &str,
        attachment: &Attachment,
    ) -> Result<(), ChannelError> {
        match attachment.kind {
            AttachmentKind::Image => {
                let key = self.upload_image(&attachment.path).await?;
                let content = json!({ "image_key": key }).to_string();
                self.send_message(receive_id, "image", &content).await
            }
            AttachmentKind::Document => {
                let key = self.upload_file(&attachment.name, &attachment.path).await?;
                let content = json!({ "file_key": key }).to_string();
                self.send_message(receive_id, "file", &content).await
            }
        }
    }
}

#[async_trait]
impl Egress for FeishuEgress {
    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        let receive_id = env.conversation_id.as_str();
        let text = env.message.content.as_ref();

        // Finalize a streamed reply by editing the placeholder to the full text
        // (flushing the last throttled delta) instead of posting a duplicate.
        let streamed = self
            .stream_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(receive_id);
        let text_finalized = if let Some(message_id) = streamed.and_then(|state| state.message_id) {
            if !text.is_empty() && text.chars().count() <= FEISHU_TEXT_LIMIT {
                let token = self.tenant_access_token().await?;
                feishu_edit_text(&self.api_base, &token, &message_id, text).await?;
                true
            } else {
                // Too long for one editable message; deliver as fresh chunks.
                self.send_text(receive_id, text).await?;
                true
            }
        } else {
            false
        };

        if !text_finalized && !text.is_empty() {
            self.send_text(receive_id, text).await?;
        }
        for attachment in &env.message.attachments {
            self.send_attachment(receive_id, attachment).await?;
        }
        if !text_finalized && text.is_empty() && env.message.attachments.is_empty() {
            return self.send_text(receive_id, "").await;
        }
        Ok(())
    }
}

/// Shareable, `'static` streaming handle decoupled from the receive-owning
/// channel. Shares the tenant-token cache and per-conversation edit state.
struct FeishuStreamEgress {
    api_base: Arc<str>,
    app_id: Arc<str>,
    app_secret: Arc<str>,
    receive_id_type: Arc<str>,
    tenant_token: Arc<Mutex<Option<CachedTenantToken>>>,
    state: Arc<Mutex<HashMap<String, FeishuEditState>>>,
}

#[async_trait]
impl StreamEgress for FeishuStreamEgress {
    async fn push_delta(&self, conversation: &ConversationId, delta: &str) {
        let receive_id = conversation.as_str().to_owned();
        // Accumulate under the lock, decide whether an edit is due, then release
        // the lock before the (async) HTTP call — a std Mutex guard is not held
        // across an await.
        let (due, text, message_id) = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let entry = guard.entry(receive_id.clone()).or_default();
            entry.buffer.push_str(delta);
            let now = Instant::now();
            let due = entry
                .last_edit
                .is_none_or(|last| now.duration_since(last) >= FEISHU_EDIT_INTERVAL);
            if due {
                entry.last_edit = Some(now);
                (
                    true,
                    clamp_feishu_text(&entry.buffer),
                    entry.message_id.clone(),
                )
            } else {
                (false, String::new(), None)
            }
        };
        if !due || text.is_empty() {
            return;
        }
        let Ok(token) = feishu_tenant_token(
            &self.api_base,
            &self.app_id,
            &self.app_secret,
            &self.tenant_token,
        )
        .await
        else {
            return;
        };
        // Best-effort: a failed placeholder/edit just skips this tick; the
        // channel's `send` still delivers the complete reply.
        match message_id {
            None => {
                if let Ok(Some(id)) = feishu_send_text_message(
                    &self.api_base,
                    &token,
                    &self.receive_id_type,
                    &receive_id,
                    &text,
                )
                .await
                {
                    self.state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .entry(receive_id)
                        .or_default()
                        .message_id = Some(id);
                }
            }
            Some(id) => {
                let _ = feishu_edit_text(&self.api_base, &token, &id, &text).await;
            }
        }
    }
}
