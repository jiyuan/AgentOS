use crate::channels::attachments::{file_size, AttachmentStore};
use crate::channels::text::split_text;
use crate::http::shared_client;
use agentos_interfaces::{Channel, ChannelError};
use agentos_proto::{Attachment, AttachmentKind, ChannelId, Envelope};
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde_json::{json, Value};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tracing::debug;

mod event;
mod long_connection;
mod proto;
mod websocket;

use event::{feishu_allowed_source_ids_from_env, AttachmentDescriptor};
use long_connection::{FeishuEndpoint, FeishuLongConnection};

const DEFAULT_API_BASE: &str = "https://open.feishu.cn/open-apis";

/// Conservative per-message character cap for Feishu text messages. The raw
/// API limit is ~30 KB, but UTF-8 multi-byte chars eat into that and bot
/// clients render long bodies poorly — chunking at 4000 chars matches what
/// users actually see in chat.
const FEISHU_TEXT_LIMIT: usize = 4000;

pub struct FeishuChannel {
    app_id: Arc<str>,
    app_secret: Arc<str>,
    id: ChannelId,
    api_base: Arc<str>,
    receive_id_type: Arc<str>,
    allowed_source_ids: Vec<Arc<str>>,
    tenant_token: Mutex<Option<CachedTenantToken>>,
    long_connection: Option<FeishuLongConnection>,
    log_receive_errors: bool,
    attachments: AttachmentStore,
}

#[derive(Clone, Debug)]
struct CachedTenantToken {
    token: Arc<str>,
    expires_at: u64,
}

impl FeishuChannel {
    pub fn from_env() -> Result<Self, ChannelError> {
        let app_id = env::var("AGENTOS_FEISHU_APP_ID")
            .map_err(|_| ChannelError::Backend(Arc::from("missing AGENTOS_FEISHU_APP_ID")))?;
        let app_secret = env::var("AGENTOS_FEISHU_APP_SECRET")
            .map_err(|_| ChannelError::Backend(Arc::from("missing AGENTOS_FEISHU_APP_SECRET")))?;
        let api_base =
            env::var("AGENTOS_FEISHU_API_BASE").unwrap_or_else(|_| DEFAULT_API_BASE.to_owned());
        let receive_id_type =
            env::var("AGENTOS_FEISHU_RECEIVE_ID_TYPE").unwrap_or_else(|_| "chat_id".to_owned());
        let allowed_source_ids = feishu_allowed_source_ids_from_env();

        Ok(Self {
            app_id: Arc::from(app_id),
            app_secret: Arc::from(app_secret),
            id: ChannelId::new("feishu"),
            api_base: Arc::from(api_base.trim_end_matches('/').to_owned()),
            receive_id_type: Arc::from(receive_id_type),
            allowed_source_ids,
            tenant_token: Mutex::new(None),
            long_connection: None,
            log_receive_errors: false,
            attachments: AttachmentStore::from_env("feishu"),
        })
    }

    pub fn with_receive_error_logging(mut self, enabled: bool) -> Self {
        self.log_receive_errors = enabled;
        self
    }

    pub fn with_attachments_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.attachments = AttachmentStore::new(root, "feishu");
        self
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base, path.trim_start_matches('/'))
    }

    fn cached_tenant_token(&self) -> Result<Option<Arc<str>>, ChannelError> {
        let now = unix_now()?;
        let cache = self
            .tenant_token
            .lock()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        Ok(cache
            .as_ref()
            .and_then(|cached| (cached.expires_at > now).then(|| Arc::clone(&cached.token))))
    }

    fn store_tenant_token(&self, token: Arc<str>, expires_at: u64) -> Result<(), ChannelError> {
        let mut cache = self
            .tenant_token
            .lock()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        *cache = Some(CachedTenantToken { token, expires_at });
        Ok(())
    }

    async fn tenant_access_token(&self) -> Result<Arc<str>, ChannelError> {
        if let Some(token) = self.cached_tenant_token()? {
            return Ok(token);
        }

        let body = json!({
            "app_id": self.app_id.as_ref(),
            "app_secret": self.app_secret.as_ref(),
        });
        let response: Value = post_json(
            &self.api_url("auth/v3/tenant_access_token/internal"),
            None,
            &body,
        )
        .await?;
        if response.get("code").and_then(Value::as_i64) != Some(0) {
            return Err(ChannelError::Backend(Arc::from(response.to_string())));
        }
        let token = response
            .get("tenant_access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from(
                    "Feishu token response missing tenant_access_token",
                ))
            })?;
        let expire = response
            .get("expire")
            .and_then(Value::as_u64)
            .unwrap_or(7_200);
        let token: Arc<str> = Arc::from(token);
        let now = unix_now()?;
        self.store_tenant_token(
            Arc::clone(&token),
            now.saturating_add(expire.saturating_sub(60)),
        )?;
        Ok(token)
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

    async fn download_resource(
        &self,
        message_id: &str,
        key: &str,
        kind: &str,
        target: &Path,
    ) -> Result<(), ChannelError> {
        let token = self.tenant_access_token().await?;
        let url = format!(
            "{}?type={kind}",
            self.api_url(&format!("im/v1/messages/{message_id}/resources/{key}"))
        );
        let bytes = shared_client()
            .get(url)
            .bearer_auth(token.as_ref())
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(reqwest_to_channel_err)?
            .bytes()
            .await
            .map_err(reqwest_to_channel_err)?;
        fs::write(target, &bytes)
            .await
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        Ok(())
    }

    async fn download_attachments(
        &self,
        descriptors: &[AttachmentDescriptor],
        conversation: &str,
        message_id: &str,
    ) -> Result<Vec<Attachment>, ChannelError> {
        let mut out = Vec::with_capacity(descriptors.len());
        for desc in descriptors {
            let target = self
                .attachments
                .target_path(conversation, message_id, &desc.name)?;
            let kind = match desc.kind {
                AttachmentKind::Image => "image",
                AttachmentKind::Document => "file",
            };
            self.download_resource(message_id, &desc.key, kind, &target)
                .await?;
            let size = file_size(&target);
            out.push(Attachment {
                kind: desc.kind.clone(),
                name: Arc::from(desc.name.as_str()),
                path: target,
                mime: desc.mime.clone(),
                size,
                source: Some(Arc::from(desc.key.as_str())),
            });
        }
        Ok(out)
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

    async fn websocket_endpoint(&self) -> Result<FeishuEndpoint, ChannelError> {
        let body = json!({
            "AppID": self.app_id.as_ref(),
            "AppSecret": self.app_secret.as_ref(),
        });
        let response: Value = shared_client()
            .post(self.platform_url("callback/ws/endpoint"))
            .header("locale", "zh")
            .json(&body)
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
        let url = response
            .get("data")
            .and_then(|data| data.get("URL").or_else(|| data.get("url")))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from("Feishu WebSocket endpoint response missing URL"))
            })?;
        Ok(FeishuEndpoint {
            url: url.to_owned(),
        })
    }

    async fn long_connection(&mut self) -> Result<&mut FeishuLongConnection, ChannelError> {
        if self.long_connection.is_none() {
            let endpoint = self.websocket_endpoint().await?;
            self.long_connection = Some(FeishuLongConnection::connect(&endpoint).await?);
        }
        Ok(self
            .long_connection
            .as_mut()
            .expect("long connection was initialized"))
    }

    fn platform_url(&self, path: &str) -> String {
        let base = self
            .api_base
            .strip_suffix("/open-apis")
            .unwrap_or(self.api_base.as_ref());
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn receive_long_connection(&mut self) -> Result<Option<Envelope>, ChannelError> {
        let channel_id = self.id.clone();
        let allowed_source_ids = self.allowed_source_ids.clone();
        let receive_id_type = Arc::clone(&self.receive_id_type);
        let log_receive_errors = self.log_receive_errors;
        let connection = self.long_connection().await?;
        let parsed = match connection
            .receive_next_event(
                &channel_id,
                &allowed_source_ids,
                receive_id_type.as_ref(),
                log_receive_errors,
            )
            .await
        {
            Ok(parsed) => parsed,
            Err(err) => {
                // Drop the socket so the next poll dials a fresh endpoint.
                self.long_connection = None;
                // Feishu rotates the long connection periodically; the server
                // sends a Close frame or just drops the TCP/TLS session (which
                // rustls reports as a missing close_notify). That is expected
                // lifecycle, not a backend failure — reconnect quietly on the
                // next poll instead of surfacing it as an error every rotation.
                if is_expected_disconnect(&err) {
                    debug!(error = %err, "feishu long connection rotated; reconnecting");
                    return Ok(None);
                }
                return Err(err);
            }
        };
        let Some(parsed) = parsed else {
            return Ok(None);
        };

        let mut envelope = parsed.envelope;
        if !parsed.attachments.is_empty() {
            let attachments = self
                .download_attachments(
                    &parsed.attachments,
                    envelope.conversation_id.as_str(),
                    &parsed.message_id,
                )
                .await?;
            envelope.message.attachments = attachments;
        }
        Ok(Some(envelope))
    }
}

#[async_trait]
impl Channel for FeishuChannel {
    fn id(&self) -> ChannelId {
        self.id.clone()
    }

    async fn receive(&mut self) -> Option<Envelope> {
        match self.receive_long_connection().await {
            Ok(envelope) => envelope,
            Err(err) => {
                if self.log_receive_errors {
                    eprintln!("feishu long connection receive failed: {err}");
                }
                None
            }
        }
    }

    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        let receive_id = env.conversation_id.as_str();
        let text = env.message.content.as_ref();
        if !text.is_empty() {
            self.send_text(receive_id, text).await?;
        }
        for attachment in &env.message.attachments {
            self.send_attachment(receive_id, attachment).await?;
        }
        if text.is_empty() && env.message.attachments.is_empty() {
            return self.send_text(receive_id, "").await;
        }
        Ok(())
    }
}

async fn post_json(url: &str, bearer: Option<&str>, body: &Value) -> Result<Value, ChannelError> {
    let mut request = shared_client().post(url).json(body);
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    request
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(reqwest_to_channel_err)?
        .json()
        .await
        .map_err(reqwest_to_channel_err)
}

/// Whether a long-connection read error is just the server recycling the
/// socket (so the right response is a silent reconnect) rather than a fault
/// worth logging. Matches the messages produced by [`websocket`] on a server
/// Close frame, a stream EOF, and rustls' strict close-without-`close_notify`.
fn is_expected_disconnect(err: &ChannelError) -> bool {
    let ChannelError::Backend(message) = err;
    const BENIGN: [&str; 4] = [
        "Feishu WebSocket closed by server",
        "Feishu WebSocket stream ended",
        "close_notify",
        "peer closed connection",
    ];
    BENIGN.iter().any(|needle| message.contains(needle))
}

fn reqwest_to_channel_err(err: reqwest::Error) -> ChannelError {
    ChannelError::Backend(Arc::from(err.to_string()))
}

fn unix_now() -> Result<u64, ChannelError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(message: &str) -> ChannelError {
        ChannelError::Backend(Arc::from(message))
    }

    #[test]
    fn server_rotation_messages_are_expected_disconnects() {
        // The exact strings observed in production logs.
        assert!(is_expected_disconnect(&backend(
            "Feishu WebSocket closed by server"
        )));
        assert!(is_expected_disconnect(&backend(
            "Feishu WebSocket read failed: IO error: peer closed connection \
             without sending TLS close_notify"
        )));
        assert!(is_expected_disconnect(&backend(
            "Feishu WebSocket stream ended"
        )));
    }

    #[test]
    fn genuine_faults_are_not_treated_as_disconnects() {
        assert!(!is_expected_disconnect(&backend(
            "Feishu event payload JSON parse failed: expected value"
        )));
        assert!(!is_expected_disconnect(&backend(
            "Feishu WebSocket endpoint response missing URL"
        )));
    }
}
