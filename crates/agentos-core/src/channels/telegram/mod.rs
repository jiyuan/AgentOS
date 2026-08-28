use crate::channels::admission::{env_flag, AdmissionPolicy};
use crate::channels::attachments::AttachmentStore;
use crate::http::shared_client;
use crate::r#loop::{parse_action_data, DECISION_KEY, TICKET_KEY};
use agentos_interfaces::{
    Channel, ChannelError, Egress, InboundEvent, IngressReceipt, StreamEgress,
};
use agentos_proto::{
    Attachment, AttachmentKind, ChannelId, ConversationId, Envelope, Message, MessageRole,
    INGRESS_ID_KEY,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Telegram Bot API origin. Overridable via `AGENTOS_TELEGRAM_API_BASE` so the
/// channel can be pointed at a local mock during tests.
mod egress;

use egress::TelegramEgress;

const DEFAULT_API_BASE: &str = "https://api.telegram.org";

pub struct TelegramChannel {
    token: Arc<str>,
    id: ChannelId,
    admission: AdmissionPolicy,
    offset: Option<i64>,
    log_receive_errors: bool,
    attachments: AttachmentStore,
    api_base: Arc<str>,
    file_base: Arc<str>,
    /// The send half. Held behind an `Arc` so the gateway can keep sending
    /// while this channel is parked in a `getUpdates` long poll, and so the
    /// per-conversation edit state is shared with the `StreamEgress` handle —
    /// `send` finalizes a message the streaming egress created.
    egress: Arc<TelegramEgress>,
}

/// In-flight streamed reply for one chat: the placeholder message being edited,
/// the accumulated text, and when it was last edited (for throttling).
#[derive(Default)]
struct TelegramEditState {
    message_id: Option<String>,
    buffer: String,
    last_edit: Option<Instant>,
}

/// Minimum gap between Telegram `editMessageText` calls per chat. Telegram rate
/// limits edits per chat; ~1 update/sec stays comfortably under the cap.
const STREAM_EDIT_INTERVAL: Duration = Duration::from_millis(900);

/// Shareable, `'static` streaming handle decoupled from the receive-owning
/// channel (whose `receive` is `&mut self`). Holds just the HTTP credentials and
/// the shared edit state.
struct TelegramStreamEgress {
    api_base: Arc<str>,
    token: Arc<str>,
    state: Arc<Mutex<HashMap<String, TelegramEditState>>>,
}

#[async_trait]
impl StreamEgress for TelegramStreamEgress {
    async fn push_delta(&self, conversation: &ConversationId, delta: &str) {
        let chat = conversation.as_str().to_owned();
        // Accumulate under the lock, decide whether an edit is due, then release
        // the lock before the (blocking) HTTP call.
        let (due, text, message_id) = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let entry = guard.entry(chat.clone()).or_default();
            entry.buffer.push_str(delta);
            let now = Instant::now();
            let due = entry
                .last_edit
                .is_none_or(|last| now.duration_since(last) >= STREAM_EDIT_INTERVAL);
            if due {
                entry.last_edit = Some(now);
                (
                    true,
                    clamp_stream_text(&entry.buffer),
                    entry.message_id.clone(),
                )
            } else {
                (false, String::new(), None)
            }
        };
        if !due || text.is_empty() {
            return;
        }
        // Best-effort: a failed placeholder/edit just means this tick isn't
        // shown; `Channel::send` still delivers the complete reply.
        match message_id {
            None => {
                if let Some(id) = tg_send_message(&self.api_base, &self.token, &chat, &text).await {
                    self.state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .entry(chat)
                        .or_default()
                        .message_id = Some(id);
                }
            }
            Some(id) => {
                let _ = tg_edit_message(&self.api_base, &self.token, &chat, &id, &text).await;
            }
        }
    }
}

impl TelegramChannel {
    pub fn from_env() -> Result<Self, ChannelError> {
        let token = env::var("AGENTOS_TELEGRAM_BOT_TOKEN")
            .map_err(|_| ChannelError::Backend(Arc::from("missing AGENTOS_TELEGRAM_BOT_TOKEN")))?;
        // An unset or empty allowlist admits *nothing*. It used to mean
        // "accept any chat", so a deployment that forgot the variable — or
        // typo'd it, which parses the same — exposed the agent to anyone who
        // could find the bot (`AUTH-001`). `AGENTOS_TELEGRAM_ALLOW_ALL=1` is
        // the explicit way to ask for an open channel.
        let admission = AdmissionPolicy::new(
            AdmissionPolicy::parse_ids(env::var("AGENTOS_TELEGRAM_CHAT_ID").ok().as_deref()),
            AdmissionPolicy::parse_ids(env::var("AGENTOS_TELEGRAM_SENDER_ID").ok().as_deref()),
            env_flag("AGENTOS_TELEGRAM_ALLOW_ALL"),
        );
        if admission.admits_nothing() {
            return Err(ChannelError::Backend(Arc::from(
                "telegram is configured to accept nothing: set AGENTOS_TELEGRAM_CHAT_ID or \
                 AGENTOS_TELEGRAM_SENDER_ID, or set AGENTOS_TELEGRAM_ALLOW_ALL=1 to accept \
                 every attributable sender",
            )));
        }
        let api_base = env::var("AGENTOS_TELEGRAM_API_BASE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_API_BASE.to_owned());
        let file_base = env::var("AGENTOS_TELEGRAM_FILE_BASE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| api_base.clone());
        let token: Arc<str> = Arc::from(token);
        let api_base: Arc<str> = Arc::from(api_base.trim_end_matches('/').to_owned());
        Ok(Self {
            token: Arc::clone(&token),
            id: ChannelId::new("telegram"),
            admission,
            offset: None,
            log_receive_errors: false,
            attachments: AttachmentStore::from_env("telegram"),
            api_base: Arc::clone(&api_base),
            file_base: Arc::from(file_base.trim_end_matches('/').to_owned()),
            egress: Arc::new(TelegramEgress {
                api_base,
                token,
                stream_state: Arc::new(Mutex::new(HashMap::new())),
            }),
        })
    }

    pub fn with_receive_error_logging(mut self, enabled: bool) -> Self {
        self.log_receive_errors = enabled;
        self
    }

    pub fn with_attachments_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.attachments = self.attachments.with_root(root);
        self
    }

    /// Apply a deployment's `[limits]` to inbound attachments
    /// (M4 / `ING-001`).
    pub fn with_attachment_limits(mut self, max_bytes: u64, max_per_message: usize) -> Self {
        self.attachments = self.attachments.with_limits(max_bytes, max_per_message);
        self
    }

    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.api_base, self.token)
    }

    fn file_url(&self, file_path: &str) -> String {
        format!("{}/file/bot{}/{file_path}", self.file_base, self.token)
    }

    /// Long-poll `getUpdates`. `Ok(None)` means the long poll elapsed with no
    /// new updates (or curl hit its own deadline waiting on an idle socket) —
    /// that is the steady state, not a failure, so the caller must not log it.
    async fn fetch_updates(&self) -> Result<Option<Value>, ChannelError> {
        let mut form = vec![("timeout", "25".to_owned())];
        if let Some(offset) = self.offset {
            form.push(("offset", offset.to_string()));
        }
        let response = match shared_client()
            .post(self.api_url("getUpdates"))
            .timeout(Duration::from_secs(40))
            .form(&form)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_timeout() => {
                return Ok(None);
            }
            Err(error) => return Err(channel_http_error(error)),
        };
        telegram_json(response).await.map(Some)
    }

    async fn get_file_path(&self, file_id: &str) -> Result<String, ChannelError> {
        let response = shared_client()
            .post(self.api_url("getFile"))
            .timeout(Duration::from_secs(10))
            .form(&[("file_id", file_id)])
            .send()
            .await
            .map_err(channel_http_error)?;
        let response = telegram_json(response).await?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(ChannelError::Backend(Arc::from(response.to_string())));
        }
        response
            .get("result")
            .and_then(|r| r.get("file_path"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from("Telegram getFile response missing file_path"))
            })
    }

    async fn download(&self, file_id: &str) -> Result<Vec<u8>, ChannelError> {
        let file_path = self.get_file_path(file_id).await?;
        let url = self.file_url(&file_path);
        let max_bytes = self.attachments.max_bytes();
        let mut response = shared_client()
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(channel_http_error)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(channel_http_error)? {
            if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(ChannelError::Backend(Arc::from(format!(
                    "Telegram attachment exceeds the {max_bytes}-byte limit"
                ))));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn download_attachments(
        &self,
        descriptors: &[AttachmentDescriptor],
        conversation: &str,
        message_id: &str,
    ) -> Result<Vec<Attachment>, ChannelError> {
        // Bounded before the loop: `descriptors.len()` is a count the sender
        // chose, and each entry is a download and a write.
        let accepted = descriptors.len().min(self.attachments.max_per_message());
        if descriptors.len() > accepted {
            eprintln!(
                "telegram message carries {} attachments; taking the first {accepted}",
                descriptors.len()
            );
        }
        let mut out = Vec::with_capacity(accepted);
        for desc in descriptors.iter().take(accepted) {
            let bytes = self.download(&desc.file_id).await?;
            let size = desc.size.or(Some(bytes.len() as u64));
            let path = self
                .attachments
                .publish(conversation, message_id, &desc.name, &bytes)?;
            out.push(Attachment {
                kind: desc.kind.clone(),
                name: Arc::from(desc.name.as_str()),
                path,
                mime: desc.mime.clone(),
                size,
                source: Some(Arc::from(desc.file_id.as_str())),
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn id(&self) -> ChannelId {
        self.id.clone()
    }

    fn resume_from(&mut self, cursor: &str) {
        match cursor.parse::<i64>() {
            Ok(offset) => self.offset = Some(offset),
            // A cursor this build does not understand is not worth failing
            // over: Telegram re-sends anything it has no acknowledgement for,
            // and the ledger recognises the duplicates.
            Err(err) => eprintln!("telegram ignoring unreadable ingress cursor '{cursor}': {err}"),
        }
    }

    async fn receive(&mut self) -> Option<InboundEvent> {
        let response = match self.fetch_updates().await {
            Ok(Some(response)) => response,
            // Idle long poll: no updates this cycle. Not an error — poll again.
            Ok(None) => return None,
            Err(err) => {
                if self.log_receive_errors {
                    eprintln!("telegram getUpdates failed: {err}");
                }
                return None;
            }
        };
        let updates = response.get("result")?.as_array()?;
        for update in updates {
            let update_id = update.get("update_id")?.as_i64()?;
            let Some(parsed) = parse_update(update, &self.id, &self.admission) else {
                continue;
            };
            let attachments = match self
                .download_attachments(
                    &parsed.attachments,
                    parsed.envelope.conversation_id.as_str(),
                    &parsed.message_id_str,
                )
                .await
            {
                Ok(a) => a,
                Err(err) => {
                    if self.log_receive_errors {
                        eprintln!("telegram attachment download failed: {err}");
                    }
                    continue;
                }
            };
            if parsed.envelope.message.content.is_empty() && attachments.is_empty() {
                continue;
            }
            let mut envelope = parsed.envelope;
            envelope.message.attachments = attachments;
            let receipt = IngressReceipt::new(
                Some(Arc::from((update_id + 1).to_string())),
                parsed
                    .callback_query_id
                    .map(|id| Arc::<[u8]>::from(id.into_bytes())),
            )
            .ok()?;
            return Some(InboundEvent::new(envelope, receipt));
        }
        None
    }

    async fn acknowledge(&mut self, receipt: IngressReceipt) -> Result<(), ChannelError> {
        if let Some(checkpoint) = receipt.checkpoint() {
            self.offset = Some(checkpoint.parse::<i64>().map_err(|err| {
                ChannelError::Backend(Arc::from(format!(
                    "telegram receipt has invalid offset '{checkpoint}': {err}"
                )))
            })?);
        }
        if let Some(token) = receipt.token() {
            let callback_id = std::str::from_utf8(token).map_err(|err| {
                ChannelError::Backend(Arc::from(format!(
                    "telegram receipt has invalid callback id: {err}"
                )))
            })?;
            // Best effort: failure only leaves the client's button spinning;
            // the durable update remains accepted and replayable.
            tg_answer_callback(&self.api_base, &self.token, callback_id).await;
        }
        Ok(())
    }

    fn egress(&self) -> Arc<dyn Egress> {
        Arc::clone(&self.egress) as Arc<dyn Egress>
    }

    fn stream_egress(&self) -> Option<Arc<dyn StreamEgress>> {
        Some(Arc::new(TelegramStreamEgress {
            api_base: Arc::clone(&self.api_base),
            token: Arc::clone(&self.token),
            state: Arc::clone(&self.egress.stream_state),
        }))
    }
}

/// curl `--max-time` for the `getUpdates` long poll. Must stay well above the
/// server-side `timeout=25` so a full-length poll plus connect/TLS setup never
/// trips curl's own deadline and surfaces as a spurious backend error.
const TELEGRAM_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Telegram sendMessage hard limit: 4096 characters per message body.
const TELEGRAM_TEXT_LIMIT: usize = 4096;

/// Telegram sendPhoto/sendDocument caption hard limit: 1024 characters.
const TELEGRAM_CAPTION_LIMIT: usize = 1024;

/// Clamp a streamed in-flight buffer to Telegram's per-message limit so an
/// over-long preview still edits cleanly. The final authoritative text is
/// delivered by [`Channel::send`].
fn clamp_stream_text(buffer: &str) -> String {
    if buffer.chars().count() <= TELEGRAM_TEXT_LIMIT {
        buffer.to_owned()
    } else {
        buffer.chars().take(TELEGRAM_TEXT_LIMIT).collect()
    }
}

/// POST `sendMessage`, returning the new message id. Best-effort (`None` on any
/// failure) — the streaming placeholder is non-essential.
///
/// Uses the pooled async HTTP client (not a `curl` subprocess) because this runs
/// on the streaming hot path: a fresh process + TLS handshake per edit stalls
/// the single-threaded loop and makes streaming slower than a buffered reply.
/// Acknowledge a button press so Telegram stops showing a progress indicator.
async fn tg_answer_callback(api_base: &str, token: &str, callback_query_id: &str) {
    let _ = shared_client()
        .post(format!("{api_base}/bot{token}/answerCallbackQuery"))
        .form(&[("callback_query_id", callback_query_id)])
        .send()
        .await;
}

async fn tg_send_message(api_base: &str, token: &str, chat_id: &str, text: &str) -> Option<String> {
    let response = shared_client()
        .post(format!("{api_base}/bot{token}/sendMessage"))
        .form(&[("chat_id", chat_id), ("text", text)])
        .send()
        .await
        .ok()?;
    let response = telegram_json(response).await.ok()?;
    response
        .get("result")?
        .get("message_id")?
        .as_i64()
        .map(|id| id.to_string())
}

/// POST `editMessageText`. Returns an error so `Channel::send`'s finalize can
/// fall back to a fresh message; a no-op "message is not modified" reply (the
/// placeholder already shows the final text) counts as success.
async fn tg_edit_message(
    api_base: &str,
    token: &str,
    chat_id: &str,
    message_id: &str,
    text: &str,
) -> Result<(), ChannelError> {
    // Pooled async client, same rationale as `tg_send_message`: this is the
    // per-delta streaming edit and must not spawn a process or re-handshake TLS.
    // A 4xx (e.g. "message is not modified") still returns a JSON body, so parse
    // the response regardless of status — matching the prior `curl` behavior.
    let response = shared_client()
        .post(format!("{api_base}/bot{token}/editMessageText"))
        .form(&[
            ("chat_id", chat_id),
            ("message_id", message_id),
            ("text", text),
        ])
        .send()
        .await
        .map_err(channel_http_error)?;
    let response = telegram_json(response).await?;
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let description = response
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if description.contains("not modified") {
        return Ok(());
    }
    Err(ChannelError::Backend(Arc::from(response.to_string())))
}

fn check_send_response(response: &Value) -> Result<(), ChannelError> {
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(ChannelError::Backend(Arc::from(response.to_string())))
    }
}

fn channel_http_error(error: reqwest::Error) -> ChannelError {
    ChannelError::Backend(Arc::from(error.to_string()))
}

async fn telegram_json(response: reqwest::Response) -> Result<Value, ChannelError> {
    crate::channels::bounded_response::json(response, "Telegram", TELEGRAM_RESPONSE_BYTES).await
}

#[derive(Debug)]
struct AttachmentDescriptor {
    kind: AttachmentKind,
    file_id: String,
    name: String,
    mime: Option<Arc<str>>,
    size: Option<u64>,
}

struct ParsedUpdate {
    envelope: Envelope,
    attachments: Vec<AttachmentDescriptor>,
    message_id_str: String,
    /// Set when this update was a button press. Telegram spins the client's
    /// button until `answerCallbackQuery` acknowledges it.
    callback_query_id: Option<String>,
}

fn parse_update(
    update: &Value,
    channel_id: &ChannelId,
    admission: &AdmissionPolicy,
) -> Option<ParsedUpdate> {
    if let Some(callback) = update.get("callback_query") {
        return parse_callback_query(update, callback, channel_id, admission);
    }
    let message = update.get("message")?;
    let chat_id = chat_id_string(message.get("chat")?)?;

    let attachments = collect_attachment_descriptors(message);
    let text = message
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message.get("caption").and_then(Value::as_str))
        .unwrap_or("")
        .trim()
        .to_owned();
    if text.is_empty() && attachments.is_empty() {
        return None;
    }

    // No `map_or_else(|| "telegram-user", ..)`: a message Telegram did not
    // attribute is refused below rather than filed under an invented person.
    let sender = message.get("from").and_then(|from| {
        from.get("id")
            .and_then(Value::as_i64)
            .map(|id| id.to_string())
            .or_else(|| {
                from.get("username")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
    });
    admission.admit(Some(&chat_id), sender.as_deref()).ok()?;
    let sender: Arc<str> = Arc::from(sender?);
    let update_id = update.get("update_id")?.as_i64()?;
    let message_id = message.get("message_id").and_then(Value::as_i64);
    let message_id_str = message_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| format!("u{update_id}"));

    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("kind"), Value::String("telegram".to_owned()));
    metadata.insert(Arc::from("update_id"), Value::from(update_id));
    // What the gateway's ingress ledger dedupes on. `update_id` is Telegram's
    // own identity for the delivery, which is exactly the thing that repeats
    // when a poll is retried (M8 / `GW-001`).
    metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(update_id));
    if let Some(message_id) = message_id {
        metadata.insert(Arc::from("message_id"), Value::from(message_id));
    }

    Some(ParsedUpdate {
        envelope: Envelope {
            channel_id: channel_id.clone(),
            conversation_id: ConversationId::new(chat_id),
            sender,
            message: Message::text(MessageRole::User, text),
            metadata,
        },
        attachments,
        message_id_str,
        callback_query_id: None,
    })
}

/// An inline-keyboard press on an approval prompt (roadmap G2).
///
/// The payload is the one `prompt_actions` encoded, so a press carries the
/// prompt's ticket structurally — nothing about it is guessable from prose,
/// and a press on an older prompt's button names that older prompt and is
/// refused as stale rather than deciding whatever is pending now.
fn parse_callback_query(
    update: &Value,
    callback: &Value,
    channel_id: &ChannelId,
    admission: &AdmissionPolicy,
) -> Option<ParsedUpdate> {
    let data = callback.get("data").and_then(Value::as_str)?;
    let (decision, ticket) = parse_action_data(data)?;
    let chat = callback
        .get("message")
        .and_then(|message| message.get("chat"))?;
    let chat_id = chat_id_string(chat)?;
    // A button press decides an approval, so an unattributable one matters
    // more than an unattributable message, not less.
    let sender = callback
        .get("from")
        .and_then(|from| from.get("id").and_then(Value::as_i64))
        .map(|id| id.to_string());
    admission.admit(Some(&chat_id), sender.as_deref()).ok()?;
    let sender: Arc<str> = Arc::from(sender?);
    let update_id = update.get("update_id")?.as_i64()?;

    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("kind"), Value::String("telegram".to_owned()));
    metadata.insert(Arc::from("update_id"), Value::from(update_id));
    // What the gateway's ingress ledger dedupes on. `update_id` is Telegram's
    // own identity for the delivery, which is exactly the thing that repeats
    // when a poll is retried (M8 / `GW-001`).
    metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(update_id));
    metadata.insert(
        Arc::from(TICKET_KEY),
        Value::String(ticket.as_str().to_owned()),
    );
    metadata.insert(Arc::from(DECISION_KEY), Value::String(decision.to_owned()));

    Some(ParsedUpdate {
        envelope: Envelope {
            channel_id: channel_id.clone(),
            conversation_id: ConversationId::new(chat_id),
            sender,
            // Echoed so the transcript and the gateway log read as the command
            // the press stands for, rather than as an empty message.
            message: Message::text(MessageRole::User, format!("/{decision} {ticket}")),
            metadata,
        },
        attachments: Vec::new(),
        message_id_str: format!("cb{update_id}"),
        callback_query_id: callback
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn collect_attachment_descriptors(message: &Value) -> Vec<AttachmentDescriptor> {
    let mut out = Vec::new();
    if let Some(photos) = message.get("photo").and_then(Value::as_array) {
        if let Some(largest) = largest_photo(photos) {
            let file_id = largest
                .get("file_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            if !file_id.is_empty() {
                let size = largest.get("file_size").and_then(Value::as_u64);
                out.push(AttachmentDescriptor {
                    kind: AttachmentKind::Image,
                    name: photo_name(largest),
                    file_id,
                    mime: Some(Arc::from("image/jpeg")),
                    size,
                });
            }
        }
    }
    if let Some(document) = message.get("document") {
        let file_id = document
            .get("file_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if !file_id.is_empty() {
            let name = document
                .get("file_name")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("{file_id}.bin"));
            let mime = document
                .get("mime_type")
                .and_then(Value::as_str)
                .map(Arc::from);
            let size = document.get("file_size").and_then(Value::as_u64);
            out.push(AttachmentDescriptor {
                kind: AttachmentKind::Document,
                file_id,
                name,
                mime,
                size,
            });
        }
    }
    out
}

fn largest_photo(photos: &[Value]) -> Option<&Value> {
    photos.iter().max_by_key(|p| {
        p.get("file_size")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| {
                let w = p.get("width").and_then(Value::as_u64).unwrap_or(0);
                let h = p.get("height").and_then(Value::as_u64).unwrap_or(0);
                w.saturating_mul(h)
            })
    })
}

fn photo_name(photo: &Value) -> String {
    photo
        .get("file_unique_id")
        .and_then(Value::as_str)
        .map(|id| format!("{id}.jpg"))
        .unwrap_or_else(|| "photo.jpg".to_owned())
}

fn chat_id_string(chat: &Value) -> Option<String> {
    if let Some(id) = chat.get("id").and_then(Value::as_i64) {
        return Some(id.to_string());
    }
    chat.get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use egress::inline_keyboard;
    use serde_json::json;

    /// Admission for tests that are not about admission: open, but still
    /// refusing anything the transport could not attribute.
    fn open_admission() -> AdmissionPolicy {
        AdmissionPolicy::new(Vec::new(), Vec::new(), true)
    }

    fn only_chat(chat: &str) -> AdmissionPolicy {
        AdmissionPolicy::new(vec![Arc::from(chat)], Vec::new(), false)
    }

    fn channel_id() -> ChannelId {
        ChannelId::new("telegram")
    }

    /// A button press carries the prompt's ticket structurally, so the router
    /// can correlate it without reading any prose (roadmap G2).
    #[test]
    fn a_button_press_becomes_a_correlated_answer() {
        let ticket = crate::r#loop::ApprovalTicket::mint().expect("OS entropy is available");
        let update = json!({
            "update_id": 42,
            "callback_query": {
                "id": "cbq-1",
                "from": { "id": 7 },
                "message": { "chat": { "id": 99 } },
                "data": format!("approve:{ticket}"),
            }
        });
        let parsed = parse_update(&update, &channel_id(), &open_admission()).expect("envelope");
        assert_eq!(parsed.envelope.conversation_id.as_str(), "99");
        assert_eq!(parsed.callback_query_id.as_deref(), Some("cbq-1"));
        assert_eq!(
            parsed
                .envelope
                .metadata
                .get(TICKET_KEY)
                .and_then(Value::as_str),
            Some(ticket.as_str())
        );
        assert_eq!(
            parsed
                .envelope
                .metadata
                .get(DECISION_KEY)
                .and_then(Value::as_str),
            Some("approve")
        );
        // The echoed text is the command the press stands for, so the log and
        // the transcript read the same on a channel with buttons and without.
        assert_eq!(
            parsed.envelope.message.content.as_ref(),
            format!("/approve {ticket}")
        );
    }

    /// Every parsed update names Telegram's own delivery id, which is what
    /// the gateway's ingress ledger dedupes on (M8 / `GW-001`). Without it a
    /// redelivered update is a new message and the agent answers twice.
    #[test]
    fn both_kinds_of_update_carry_the_transport_delivery_id() {
        let ticket = crate::r#loop::ApprovalTicket::mint().expect("OS entropy is available");
        for update in [
            json!({
                "update_id": 4242,
                "message": { "chat": { "id": 99 }, "from": { "id": 7 }, "text": "hello" }
            }),
            json!({
                "update_id": 4242,
                "callback_query": {
                    "id": "cbq-1",
                    "from": { "id": 7 },
                    "message": { "chat": { "id": 99 } },
                    "data": format!("approve:{ticket}"),
                }
            }),
        ] {
            let parsed = parse_update(&update, &channel_id(), &open_admission()).expect("envelope");
            assert_eq!(
                parsed.envelope.ingress_id().as_deref(),
                Some("4242"),
                "got {:?}",
                parsed.envelope.metadata
            );
        }
    }

    /// The offset is the gateway's to persist and hand back. It used to live
    /// only in process memory, so a restart re-read everything Telegram still
    /// held.
    #[test]
    fn the_offset_resumes_from_a_durable_receipt_checkpoint() {
        let token: Arc<str> = Arc::from("token");
        let api_base: Arc<str> = Arc::from(DEFAULT_API_BASE);
        let mut channel = TelegramChannel {
            token: Arc::clone(&token),
            id: channel_id(),
            admission: open_admission(),
            offset: None,
            log_receive_errors: false,
            attachments: AttachmentStore::from_env("telegram"),
            api_base: Arc::clone(&api_base),
            file_base: Arc::clone(&api_base),
            egress: Arc::new(TelegramEgress {
                api_base,
                token,
                stream_state: Arc::new(Mutex::new(HashMap::new())),
            }),
        };
        assert_eq!(channel.offset, None, "a fresh channel has no position");
        channel.resume_from("4243");
        assert_eq!(channel.offset, Some(4243));

        // A cursor from some other build is ignored rather than fatal: the
        // transport re-sends what it has no acknowledgement for anyway.
        channel.resume_from("not a number");
        assert_eq!(channel.offset, Some(4243));
    }

    /// A press on some other feature's button is not an approval answer.
    #[test]
    fn a_callback_that_is_not_ours_is_ignored() {
        let update = json!({
            "update_id": 42,
            "callback_query": {
                "id": "cbq-1",
                "from": { "id": 7 },
                "message": { "chat": { "id": 99 } },
                "data": "open:settings",
            }
        });
        assert!(parse_update(&update, &channel_id(), &open_admission()).is_none());
    }

    /// The chat allowlist covers button presses too — otherwise a stranger who
    /// guessed a ticket could approve someone else's tool call.
    #[test]
    fn a_button_press_from_a_disallowed_chat_is_dropped() {
        let update = json!({
            "update_id": 42,
            "callback_query": {
                "id": "cbq-1",
                "from": { "id": 7 },
                "message": { "chat": { "id": 99 } },
                "data": "approve:k3f",
            }
        });
        assert!(parse_update(&update, &channel_id(), &only_chat("100")).is_none());
        assert!(parse_update(&update, &channel_id(), &only_chat("99")).is_some());
    }

    /// `AUTH-001`. A message Telegram did not attribute used to arrive under
    /// the literal sender `telegram-user`, which reads like a person and is
    /// not one.
    #[test]
    fn a_message_with_no_sender_is_refused_even_on_an_open_channel() {
        let update = json!({
            "update_id": 9,
            "message": {
                "message_id": 1,
                "chat": { "id": 99 },
                "text": "who am I?"
            }
        });
        assert!(parse_update(&update, &channel_id(), &open_admission()).is_none());
    }

    /// The same for a button press, which decides an approval.
    #[test]
    fn a_button_press_with_no_sender_is_refused() {
        let update = json!({
            "update_id": 10,
            "callback_query": {
                "id": "cb-1",
                "data": "agentos:approve:ticket-1",
                "message": { "chat": { "id": 99 } }
            }
        });
        assert!(parse_update(&update, &channel_id(), &open_admission()).is_none());
    }

    /// A sender allowlist keeps out someone who is in an allowed chat.
    #[test]
    fn a_sender_outside_the_allowlist_is_refused_in_an_allowed_chat() {
        let admission = AdmissionPolicy::new(vec![Arc::from("99")], vec![Arc::from("7")], false);
        let from = |id: i64| {
            json!({
                "update_id": 11,
                "message": {
                    "message_id": 2,
                    "chat": { "id": 99 },
                    "from": { "id": id },
                    "text": "hello"
                }
            })
        };
        assert!(parse_update(&from(7), &channel_id(), &admission).is_some());
        assert!(parse_update(&from(8), &channel_id(), &admission).is_none());
    }

    #[test]
    fn approval_actions_render_as_one_keyboard_row() {
        let ticket = crate::r#loop::ApprovalTicket::mint().expect("OS entropy is available");
        let markup =
            inline_keyboard(Some(&crate::r#loop::prompt_actions(&ticket))).expect("a keyboard");
        let parsed: Value = serde_json::from_str(&markup).expect("valid JSON");
        let rows = parsed
            .get("inline_keyboard")
            .and_then(Value::as_array)
            .expect("rows");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_array().expect("buttons");
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].get("text").and_then(Value::as_str), Some("Approve"));
        assert_eq!(
            row[0].get("callback_data").and_then(Value::as_str),
            Some(format!("approve:{ticket}").as_str())
        );
    }

    /// Missing or malformed action metadata sends the prompt as plain text
    /// rather than failing: the body still says how to answer.
    #[test]
    fn absent_actions_produce_no_keyboard() {
        assert!(inline_keyboard(None).is_none());
        assert!(inline_keyboard(Some(&json!([]))).is_none());
        assert!(inline_keyboard(Some(&json!("not an array"))).is_none());
    }

    #[test]
    fn clamp_stream_text_truncates_to_message_limit() {
        let short = "hello";
        assert_eq!(clamp_stream_text(short), short);

        let long: String = "x".repeat(TELEGRAM_TEXT_LIMIT + 50);
        let clamped = clamp_stream_text(&long);
        assert_eq!(clamped.chars().count(), TELEGRAM_TEXT_LIMIT);
    }

    #[test]
    fn parse_update_extracts_text_only() {
        let update = json!({
            "update_id": 1,
            "message": {
                "message_id": 10,
                "chat": { "id": 99 },
                "from": { "id": 7 },
                "text": "hello world"
            }
        });
        let parsed = parse_update(&update, &channel_id(), &open_admission()).expect("envelope");
        assert_eq!(parsed.envelope.message.content.as_ref(), "hello world");
        assert!(parsed.attachments.is_empty());
        assert_eq!(parsed.message_id_str, "10");
    }

    #[test]
    fn parse_update_picks_largest_photo_and_caption() {
        let update = json!({
            "update_id": 2,
            "message": {
                "message_id": 11,
                "chat": { "id": 99 },
                "from": { "id": 7 },
                "caption": "look at this",
                "photo": [
                    { "file_id": "small", "file_unique_id": "u1", "width": 90, "height": 60, "file_size": 1000 },
                    { "file_id": "big",   "file_unique_id": "u2", "width": 800, "height": 600, "file_size": 50_000 },
                ]
            }
        });
        let parsed = parse_update(&update, &channel_id(), &open_admission()).expect("envelope");
        assert_eq!(parsed.envelope.message.content.as_ref(), "look at this");
        assert_eq!(parsed.attachments.len(), 1);
        let desc = &parsed.attachments[0];
        assert_eq!(desc.kind, AttachmentKind::Image);
        assert_eq!(desc.file_id, "big");
        assert_eq!(desc.name, "u2.jpg");
        assert_eq!(desc.size, Some(50_000));
    }

    #[test]
    fn parse_update_extracts_document() {
        let update = json!({
            "update_id": 3,
            "message": {
                "message_id": 12,
                "chat": { "id": 99 },
                "from": { "id": 7 },
                "document": {
                    "file_id": "doc-1",
                    "file_name": "report.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 4096
                }
            }
        });
        let parsed = parse_update(&update, &channel_id(), &open_admission()).expect("envelope");
        assert!(parsed.envelope.message.content.is_empty());
        assert_eq!(parsed.attachments.len(), 1);
        let desc = &parsed.attachments[0];
        assert_eq!(desc.kind, AttachmentKind::Document);
        assert_eq!(desc.file_id, "doc-1");
        assert_eq!(desc.name, "report.pdf");
        assert_eq!(desc.mime.as_deref(), Some("application/pdf"));
    }

    #[test]
    fn parse_update_drops_empty_message() {
        let update = json!({
            "update_id": 4,
            "message": { "message_id": 13, "chat": { "id": 99 } }
        });
        assert!(parse_update(&update, &channel_id(), &open_admission()).is_none());
    }

    #[test]
    fn parse_update_filters_chat_id() {
        let update = json!({
            "update_id": 5,
            "message": {
                "message_id": 14,
                "chat": { "id": 99 },
                "from": { "id": 7 },
                "text": "hi"
            }
        });
        assert!(parse_update(&update, &channel_id(), &only_chat("100")).is_none());
        assert!(parse_update(&update, &channel_id(), &only_chat("99")).is_some());
    }
}
