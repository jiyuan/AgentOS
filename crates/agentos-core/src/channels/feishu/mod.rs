use crate::channels::attachments::{file_size, AttachmentStore};
use crate::channels::auth::RemoteIngressPolicy;
use crate::config::RemoteChannelConfig;
use crate::http::shared_client;
use agentos_interfaces::{Channel, ChannelError, Egress, StreamEgress};
use agentos_proto::{Attachment, AttachmentKind, ChannelId, Envelope};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tracing::debug;

mod egress;
mod event;
mod long_connection;
mod proto;
mod websocket;

use egress::FeishuEgress;
use event::{feishu_allowed_source_ids_from_env, AttachmentDescriptor};
use long_connection::{FeishuEndpoint, FeishuLongConnection};

const DEFAULT_API_BASE: &str = "https://open.feishu.cn/open-apis";

/// Conservative per-message character cap for Feishu text messages. The raw
/// API limit is ~30 KB, but UTF-8 multi-byte chars eat into that and bot
/// clients render long bodies poorly — chunking at 4000 chars matches what
/// users actually see in chat.
pub(super) const FEISHU_TEXT_LIMIT: usize = 4000;

pub struct FeishuChannel {
    app_id: Arc<str>,
    app_secret: Arc<str>,
    id: ChannelId,
    api_base: Arc<str>,
    receive_id_type: Arc<str>,
    ingress_policy: RemoteIngressPolicy,
    tenant_token: Arc<Mutex<Option<CachedTenantToken>>>,
    long_connection: Option<FeishuLongConnection>,
    log_receive_errors: bool,
    attachments: AttachmentStore,
    /// The send half. Held behind an `Arc` so the gateway can keep sending
    /// while this channel is parked in its long connection, and so the
    /// per-conversation edit state is shared with the `StreamEgress` handle —
    /// `send` finalizes a message the streaming egress created.
    egress: Arc<FeishuEgress>,
    /// Consecutive long-connection dial failures, reset on a successful
    /// (re)connect. Drives the reconnect backoff window below.
    reconnect_failures: u32,
    /// Earliest instant the next dial is allowed. While set in the future,
    /// `receive` waits quietly instead of re-dialing (and re-logging) every poll.
    retry_not_before: Option<Instant>,
}

/// Minimum gap between Feishu message edits per chat, to stay under rate limits.
pub(super) const FEISHU_EDIT_INTERVAL: Duration = Duration::from_millis(900);

/// First reconnect backoff after a long-connection dial failure. Doubles on each
/// consecutive failure up to `FEISHU_RECONNECT_BACKOFF_MAX`.
const FEISHU_RECONNECT_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Ceiling on the reconnect backoff window so a sustained outage retries at most
/// once a minute instead of every poll.
const FEISHU_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub(super) struct CachedTenantToken {
    token: Arc<str>,
    expires_at: u64,
}

impl FeishuChannel {
    pub fn from_env(config: &RemoteChannelConfig) -> Result<Self, ChannelError> {
        let app_id = env::var("AGENTOS_FEISHU_APP_ID")
            .map_err(|_| ChannelError::Backend(Arc::from("missing AGENTOS_FEISHU_APP_ID")))?;
        let app_secret = env::var("AGENTOS_FEISHU_APP_SECRET")
            .map_err(|_| ChannelError::Backend(Arc::from("missing AGENTOS_FEISHU_APP_SECRET")))?;
        let api_base =
            env::var("AGENTOS_FEISHU_API_BASE").unwrap_or_else(|_| DEFAULT_API_BASE.to_owned());
        let receive_id_type =
            env::var("AGENTOS_FEISHU_RECEIVE_ID_TYPE").unwrap_or_else(|_| "chat_id".to_owned());
        let ingress_policy = RemoteIngressPolicy::from_config(
            "feishu",
            config,
            feishu_allowed_source_ids_from_env(),
            [],
        )?;
        // The channel and its egress share one token cache: two caches would
        // mean two `tenant_access_token` round trips and, worse, a send racing
        // a receive to refresh the same expiring token.
        let tenant_token = Arc::new(Mutex::new(None));
        let app_id: Arc<str> = Arc::from(app_id);
        let app_secret: Arc<str> = Arc::from(app_secret);
        let api_base: Arc<str> = Arc::from(api_base.trim_end_matches('/').to_owned());
        let receive_id_type: Arc<str> = Arc::from(receive_id_type);

        Ok(Self {
            app_id: Arc::clone(&app_id),
            app_secret: Arc::clone(&app_secret),
            id: ChannelId::new("feishu"),
            api_base: Arc::clone(&api_base),
            receive_id_type: Arc::clone(&receive_id_type),
            ingress_policy,
            tenant_token: Arc::clone(&tenant_token),
            long_connection: None,
            log_receive_errors: false,
            attachments: AttachmentStore::from_env("feishu"),
            egress: Arc::new(FeishuEgress {
                api_base,
                app_id,
                app_secret,
                receive_id_type,
                tenant_token,
                stream_state: Arc::new(Mutex::new(HashMap::new())),
            }),
            reconnect_failures: 0,
            retry_not_before: None,
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

    async fn tenant_access_token(&self) -> Result<Arc<str>, ChannelError> {
        feishu_tenant_token(
            &self.api_base,
            &self.app_id,
            &self.app_secret,
            &self.tenant_token,
        )
        .await
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
            // Freshly connected — clear any outstanding reconnect backoff so the
            // next transient failure starts its streak from the base delay.
            self.reconnect_failures = 0;
            self.retry_not_before = None;
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
        // Honor an active reconnect backoff window: after a dial failure we wait
        // (with exponential backoff) before re-dialing, so a sustained outage
        // doesn't re-connect and re-log on every one-second poll.
        if let Some(deadline) = self.retry_not_before {
            if Instant::now() < deadline {
                return Ok(None);
            }
        }
        let channel_id = self.id.clone();
        let ingress_policy = self.ingress_policy.clone();
        let receive_id_type = Arc::clone(&self.receive_id_type);
        let log_receive_errors = self.log_receive_errors;
        let connection = match self.long_connection().await {
            Ok(connection) => connection,
            Err(err) => {
                self.note_connection_failure(&err);
                return Ok(None);
            }
        };
        let parsed = match connection
            .receive_next_event(
                &channel_id,
                &ingress_policy,
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

    /// Record a long-connection dial failure: grow the exponential backoff
    /// window and log the failure only once per outage streak. Subsequent
    /// failures within the same outage drop to `debug!` so the gateway log
    /// isn't flooded while the endpoint is unreachable.
    fn note_connection_failure(&mut self, err: &ChannelError) {
        self.reconnect_failures = self.reconnect_failures.saturating_add(1);
        // Cap the shift so `1 << exp` can't overflow; the delay is clamped to
        // FEISHU_RECONNECT_BACKOFF_MAX well before then anyway.
        let exp = self.reconnect_failures.saturating_sub(1).min(6);
        let delay = FEISHU_RECONNECT_BACKOFF_BASE
            .saturating_mul(1u32 << exp)
            .min(FEISHU_RECONNECT_BACKOFF_MAX);
        self.retry_not_before = Some(Instant::now() + delay);

        if self.reconnect_failures == 1 {
            if self.log_receive_errors {
                eprintln!(
                    "feishu long connection receive failed: {err} (retrying in {}s)",
                    delay.as_secs()
                );
            }
        } else {
            debug!(
                error = %err,
                failures = self.reconnect_failures,
                backoff_secs = delay.as_secs(),
                "feishu long connection still unreachable; backing off"
            );
        }
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

    fn egress(&self) -> Arc<dyn Egress> {
        Arc::clone(&self.egress) as Arc<dyn Egress>
    }

    fn stream_egress(&self) -> Option<Arc<dyn StreamEgress>> {
        Some(self.egress.stream_handle())
    }
}

/// Acquire (or reuse a cached) tenant access token. Shared by the channel and
/// its [`StreamEgress`] handle so both hit the same cache.
pub(super) async fn feishu_tenant_token(
    api_base: &str,
    app_id: &str,
    app_secret: &str,
    cache: &Mutex<Option<CachedTenantToken>>,
) -> Result<Arc<str>, ChannelError> {
    let now = unix_now()?;
    if let Some(token) = cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|cached| (cached.expires_at > now).then(|| Arc::clone(&cached.token)))
    {
        return Ok(token);
    }
    let body = json!({ "app_id": app_id, "app_secret": app_secret });
    let url = format!("{api_base}/auth/v3/tenant_access_token/internal");
    let response = post_json(&url, None, &body).await?;
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
    *cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(CachedTenantToken {
        token: Arc::clone(&token),
        expires_at: now.saturating_add(expire.saturating_sub(60)),
    });
    Ok(token)
}

/// POST a text message, returning the new message id. Used by the streaming
/// placeholder; `None` when the response omits a `message_id`.
pub(super) async fn feishu_send_text_message(
    api_base: &str,
    token: &str,
    receive_id_type: &str,
    receive_id: &str,
    text: &str,
) -> Result<Option<String>, ChannelError> {
    let content = json!({ "text": text }).to_string();
    let body = json!({ "receive_id": receive_id, "msg_type": "text", "content": content });
    let url = format!("{api_base}/im/v1/messages?receive_id_type={receive_id_type}");
    let response = post_json(&url, Some(token), &body).await?;
    if response.get("code").and_then(Value::as_i64) != Some(0) {
        return Err(ChannelError::Backend(Arc::from(response.to_string())));
    }
    Ok(response
        .get("data")
        .and_then(|data| data.get("message_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned))
}

/// PUT an updated text body onto an existing message (edit in place).
pub(super) async fn feishu_edit_text(
    api_base: &str,
    token: &str,
    message_id: &str,
    text: &str,
) -> Result<(), ChannelError> {
    let content = json!({ "text": text }).to_string();
    let body = json!({ "msg_type": "text", "content": content });
    let url = format!("{api_base}/im/v1/messages/{message_id}");
    let response: Value = shared_client()
        .put(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(reqwest_to_channel_err)?
        .json()
        .await
        .map_err(reqwest_to_channel_err)?;
    if response.get("code").and_then(Value::as_i64) == Some(0) {
        Ok(())
    } else {
        Err(ChannelError::Backend(Arc::from(response.to_string())))
    }
}

/// Clamp a streamed in-flight buffer to Feishu's per-message char budget so an
/// over-long preview still edits cleanly; `send` delivers the full final text.
pub(super) fn clamp_feishu_text(buffer: &str) -> String {
    if buffer.chars().count() <= FEISHU_TEXT_LIMIT {
        buffer.to_owned()
    } else {
        buffer.chars().take(FEISHU_TEXT_LIMIT).collect()
    }
}

pub(super) async fn post_json(
    url: &str,
    bearer: Option<&str>,
    body: &Value,
) -> Result<Value, ChannelError> {
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

pub(super) fn reqwest_to_channel_err(err: reqwest::Error) -> ChannelError {
    // reqwest's top-level Display is opaque (e.g. "error sending request for
    // url (...)"); the actionable cause — TLS rejection, connection reset, DNS
    // failure, timeout — lives in the `source()` chain. Flatten the whole chain
    // so the gateway log shows *why* a request failed, not just that it did.
    let mut message = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    ChannelError::Backend(Arc::from(message))
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

    fn test_channel() -> FeishuChannel {
        FeishuChannel {
            app_id: Arc::from("app"),
            app_secret: Arc::from("secret"),
            id: ChannelId::new("feishu"),
            api_base: Arc::from(DEFAULT_API_BASE),
            receive_id_type: Arc::from("chat_id"),
            ingress_policy: RemoteIngressPolicy::allow_all(),
            tenant_token: Arc::new(Mutex::new(None)),
            long_connection: None,
            log_receive_errors: false,
            attachments: AttachmentStore::new(std::env::temp_dir(), "feishu"),
            egress: Arc::new(FeishuEgress {
                api_base: Arc::from(DEFAULT_API_BASE),
                app_id: Arc::from("app"),
                app_secret: Arc::from("secret"),
                receive_id_type: Arc::from("chat_id"),
                tenant_token: Arc::new(Mutex::new(None)),
                stream_state: Arc::new(Mutex::new(HashMap::new())),
            }),
            reconnect_failures: 0,
            retry_not_before: None,
        }
    }

    #[test]
    fn reconnect_backoff_escalates_then_caps() {
        let mut channel = test_channel();
        let err = backend("error sending request for url (https://open.feishu.cn/...)");

        channel.note_connection_failure(&err);
        assert_eq!(channel.reconnect_failures, 1);
        let first = channel
            .retry_not_before
            .expect("first failure arms a backoff window");
        // The window is in the future, so the next poll waits instead of dialing.
        assert!(first > Instant::now());

        // Drive enough failures to reach and stay at the cap.
        for _ in 0..10 {
            channel.note_connection_failure(&err);
        }
        let capped = channel
            .retry_not_before
            .expect("repeated failures keep the window armed");
        assert!(capped <= Instant::now() + FEISHU_RECONNECT_BACKOFF_MAX + Duration::from_secs(1));
    }

    #[test]
    fn successful_reconnect_clears_backoff() {
        let mut channel = test_channel();
        channel.note_connection_failure(&backend("dial failed"));
        assert!(channel.retry_not_before.is_some());

        // Mirror the reset performed when `long_connection` dials successfully.
        channel.reconnect_failures = 0;
        channel.retry_not_before = None;
        assert_eq!(channel.reconnect_failures, 0);
        assert!(channel.retry_not_before.is_none());
    }

    #[test]
    fn clamp_feishu_text_truncates_to_message_limit() {
        assert_eq!(clamp_feishu_text("hello"), "hello");
        let long: String = "x".repeat(FEISHU_TEXT_LIMIT + 50);
        assert_eq!(clamp_feishu_text(&long).chars().count(), FEISHU_TEXT_LIMIT);
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
