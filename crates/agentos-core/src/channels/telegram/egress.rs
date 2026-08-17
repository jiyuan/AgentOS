//! The send half of the Telegram channel.
//!
//! [`Channel::receive`](agentos_interfaces::Channel::receive) takes `&mut self`,
//! so while `TelegramChannel` is parked in a 40-second `getUpdates` long poll
//! nothing can call `send` on it. Everything `send` needs therefore lives here,
//! behind an `Arc` the channel hands out through
//! [`Channel::egress`](agentos_interfaces::Channel::egress): the API
//! credentials and the per-conversation edit-in-place state a streamed reply is
//! finalized against.
//!
//! Approval prompts (roadmap G2) also get their inline keyboard built here,
//! because the payload behind each button is part of what `send` emits.

use super::{
    check_send_response, tg_edit_message, TelegramEditState, TELEGRAM_CAPTION_LIMIT,
    TELEGRAM_TEXT_LIMIT,
};
use crate::channels::text::split_text;
use crate::r#loop::{ACTIONS_KEY, PROMPT_KIND};
use agentos_interfaces::{ChannelError, Egress};
use agentos_proto::{Attachment, AttachmentKind, Envelope};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

/// Render `approval_actions` metadata as a Telegram `reply_markup` payload.
///
/// One row, so the two choices sit side by side. Returns `None` when the
/// metadata is missing or malformed, which sends the prompt as plain text —
/// the `/approve <ticket>` instruction in the body still works.
pub(super) fn inline_keyboard(actions: Option<&Value>) -> Option<String> {
    let row: Vec<Value> = actions?
        .as_array()?
        .iter()
        .filter_map(|action| {
            Some(serde_json::json!({
                "text": action.get("label")?.as_str()?,
                "callback_data": action.get("data")?.as_str()?,
            }))
        })
        .collect();
    if row.is_empty() {
        return None;
    }
    Some(serde_json::json!({ "inline_keyboard": [row] }).to_string())
}

/// Shareable, `'static` send half of [`TelegramChannel`].
///
/// Holds only what `send` needs: the HTTP credentials and the per-conversation
/// streaming edit state, so a reply finalizes the placeholder a stream delta
/// created rather than posting a duplicate.
pub(super) struct TelegramEgress {
    pub(super) api_base: Arc<str>,
    pub(super) token: Arc<str>,
    pub(super) stream_state: Arc<Mutex<HashMap<String, TelegramEditState>>>,
}

impl TelegramEgress {
    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.api_base, self.token)
    }

    /// Remove and return any streaming state for `chat_id`, so `send` can
    /// finalize a streamed reply exactly once.
    fn take_stream_state(&self, chat_id: &str) -> Option<TelegramEditState> {
        self.stream_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(chat_id)
    }

    /// Post one message carrying an inline keyboard.
    ///
    /// Not chunked: an approval prompt is two lines, and splitting it would
    /// leave the buttons on a fragment.
    fn send_with_keyboard(
        &self,
        chat_id: &str,
        text: &str,
        keyboard: &str,
    ) -> Result<(), ChannelError> {
        let text_arg = format!("text={text}");
        let chat_arg = format!("chat_id={chat_id}");
        let markup_arg = format!("reply_markup={keyboard}");
        let output = Command::new("curl")
            .args(["--silent", "--show-error", "--max-time", "10", "-X", "POST"])
            .arg(self.api_url("sendMessage"))
            .args([
                "-d",
                &chat_arg,
                "--data-urlencode",
                &text_arg,
                "--data-urlencode",
                &markup_arg,
            ])
            .output()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        check_send_response(&output.status, &output.stdout, &output.stderr)
    }

    fn send_text(&self, chat_id: &str, text: &str) -> Result<(), ChannelError> {
        for chunk in split_text(text, TELEGRAM_TEXT_LIMIT) {
            self.send_text_chunk(chat_id, &chunk)?;
        }
        Ok(())
    }

    fn send_text_chunk(&self, chat_id: &str, text: &str) -> Result<(), ChannelError> {
        let text_arg = format!("text={text}");
        let chat_arg = format!("chat_id={chat_id}");
        let output = Command::new("curl")
            .args(["--silent", "--show-error", "--max-time", "10", "-X", "POST"])
            .arg(self.api_url("sendMessage"))
            .args(["-d", &chat_arg, "--data-urlencode", &text_arg])
            .output()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        check_send_response(&output.status, &output.stdout, &output.stderr)
    }

    fn send_attachment(
        &self,
        chat_id: &str,
        attachment: &Attachment,
        caption: Option<&str>,
    ) -> Result<(), ChannelError> {
        let (method, field) = match attachment.kind {
            AttachmentKind::Image => ("sendPhoto", "photo"),
            AttachmentKind::Document => ("sendDocument", "document"),
        };
        let file_form = format!("{field}=@{}", attachment.path.display());
        let chat_form = format!("chat_id={chat_id}");
        let mut command = Command::new("curl");
        command
            .args(["--silent", "--show-error", "--max-time", "60", "-X", "POST"])
            .arg(self.api_url(method))
            .args(["-F", &chat_form, "-F", &file_form]);
        if let Some(caption) = caption {
            if !caption.is_empty() {
                let caption_form = format!("caption={caption}");
                command.args(["-F", &caption_form]);
            }
        }
        let output = command
            .output()
            .map_err(|err| ChannelError::Backend(Arc::from(err.to_string())))?;
        check_send_response(&output.status, &output.stdout, &output.stderr)
    }
}

#[async_trait]
impl Egress for TelegramEgress {
    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        let chat_id = env.conversation_id.as_str();
        let text = env.message.content.as_ref();

        // An approval prompt gets buttons. The payload behind each is the
        // prompt's own ticket, so a press names the asking it came from and
        // cannot be replayed against a later one (roadmap G2). The text still
        // spells out `/approve <ticket>`, so a client that cannot render the
        // keyboard is not locked out.
        if env.metadata.get("kind").and_then(Value::as_str) == Some(PROMPT_KIND) {
            if let Some(keyboard) = inline_keyboard(env.metadata.get(ACTIONS_KEY)) {
                return self.send_with_keyboard(chat_id, text, &keyboard);
            }
        }

        // If this reply was streamed, finalize the placeholder by editing it to
        // the authoritative full text (so the last throttled delta is flushed),
        // instead of posting a duplicate message. Falls back to a fresh send
        // when there's no placeholder or the text is too long for one message.
        let streamed = self.take_stream_state(chat_id);
        let text_finalized = if let Some(message_id) = streamed.and_then(|state| state.message_id) {
            if !text.is_empty() && text.chars().count() <= TELEGRAM_TEXT_LIMIT {
                tg_edit_message(&self.api_base, &self.token, chat_id, &message_id, text).await?;
                true
            } else {
                // Too long to fit the edited message; deliver as fresh chunks.
                self.send_text(chat_id, text)?;
                true
            }
        } else {
            false
        };

        if env.message.attachments.is_empty() {
            if text_finalized {
                return Ok(());
            }
            return self.send_text(chat_id, text);
        }

        // Telegram captions are capped at 1024 chars. If the reply text is
        // longer (or was already delivered via streaming), send it as a separate
        // message first and don't attach a caption — otherwise the multipart
        // sendPhoto/sendDocument would 400.
        let caption = if text_finalized || text.is_empty() {
            None
        } else if text.chars().count() <= TELEGRAM_CAPTION_LIMIT {
            Some(text)
        } else {
            self.send_text(chat_id, text)?;
            None
        };
        let mut caption = caption;
        for attachment in &env.message.attachments {
            self.send_attachment(chat_id, attachment, caption)?;
            caption = None;
        }
        Ok(())
    }
}
