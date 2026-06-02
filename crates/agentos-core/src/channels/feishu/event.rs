use agentos_proto::{AttachmentKind, ChannelId, ConversationId, Envelope, Message, MessageRole};
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct AttachmentDescriptor {
    pub(super) kind: AttachmentKind,
    pub(super) key: String,
    pub(super) name: String,
    pub(super) mime: Option<Arc<str>>,
}

pub(super) struct ParsedFeishuEvent {
    pub(super) envelope: Envelope,
    pub(super) attachments: Vec<AttachmentDescriptor>,
    pub(super) message_id: String,
}

pub(super) fn parse_event(
    payload: &Value,
    channel_id: &ChannelId,
    allowed_source_ids: &[Arc<str>],
    receive_id_type: &str,
) -> Option<ParsedFeishuEvent> {
    let event_type = payload
        .get("header")
        .and_then(|header| header.get("event_type"))
        .and_then(Value::as_str);
    if event_type.is_some_and(|value| value != "im.message.receive_v1") {
        return None;
    }

    let event = payload.get("event")?;
    let message = event.get("message")?;
    let message_type = message.get("message_type").and_then(Value::as_str)?;
    let chat_id = message.get("chat_id").and_then(Value::as_str)?;
    let message_id = message
        .get("message_id")
        .and_then(Value::as_str)?
        .to_owned();
    let sender_id = event
        .get("sender")
        .and_then(|sender| sender.get("sender_id"));
    if !feishu_allowed_source_matches(allowed_source_ids, sender_id) {
        return None;
    }
    let raw_content = message.get("content").and_then(Value::as_str)?;
    let content_json: Value = serde_json::from_str(raw_content).ok()?;

    let (text, attachments) = match message_type {
        "text" => {
            let text = content_json
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_owned();
            if text.is_empty() {
                return None;
            }
            (text, Vec::new())
        }
        "image" => {
            let key = content_json
                .get("image_key")
                .and_then(Value::as_str)?
                .to_owned();
            let descriptor = AttachmentDescriptor {
                kind: AttachmentKind::Image,
                name: format!("{key}.jpg"),
                key,
                mime: Some(Arc::from("image/jpeg")),
            };
            (String::new(), vec![descriptor])
        }
        "file" => {
            let key = content_json
                .get("file_key")
                .and_then(Value::as_str)?
                .to_owned();
            let name = content_json
                .get("file_name")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("{key}.bin"));
            let descriptor = AttachmentDescriptor {
                kind: AttachmentKind::Document,
                key,
                name,
                mime: None,
            };
            (String::new(), vec![descriptor])
        }
        _ => return None,
    };

    let sender = sender_id
        .and_then(preferred_feishu_sender_id)
        .map_or_else(|| Arc::from("feishu-user"), Arc::from);
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("kind"), Value::String("feishu".to_owned()));
    if let Some(event_id) = payload
        .get("header")
        .and_then(|header| header.get("event_id"))
        .and_then(Value::as_str)
    {
        metadata.insert(Arc::from("event_id"), Value::String(event_id.to_owned()));
    }
    if let Some(tenant_key) = payload
        .get("header")
        .and_then(|header| header.get("tenant_key"))
        .and_then(Value::as_str)
    {
        metadata.insert(
            Arc::from("tenant_key"),
            Value::String(tenant_key.to_owned()),
        );
    }
    metadata.insert(Arc::from("message_id"), Value::String(message_id.clone()));
    metadata.insert(
        Arc::from("message_type"),
        Value::String(message_type.to_owned()),
    );
    if let Some(chat_type) = message.get("chat_type").and_then(Value::as_str) {
        metadata.insert(Arc::from("chat_type"), Value::String(chat_type.to_owned()));
    }

    let conversation_id = match receive_id_type {
        "open_id" | "user_id" | "union_id" => sender_id
            .and_then(|sender| sender.get(receive_id_type))
            .and_then(Value::as_str)
            .unwrap_or(chat_id),
        _ => chat_id,
    };

    Some(ParsedFeishuEvent {
        envelope: Envelope {
            channel_id: channel_id.clone(),
            conversation_id: ConversationId::new(conversation_id),
            sender,
            message: Message::text(MessageRole::User, text),
            metadata,
        },
        attachments,
        message_id,
    })
}

pub(super) fn feishu_drop_reason(
    payload: &Value,
    allowed_source_ids: &[Arc<str>],
) -> Option<String> {
    let event_type = payload
        .get("header")
        .and_then(|header| header.get("event_type"))
        .and_then(Value::as_str);
    if event_type.is_some_and(|value| value != "im.message.receive_v1") {
        return Some(format!(
            "unsupported event_type={}, expected im.message.receive_v1",
            event_type.unwrap_or("<missing>")
        ));
    }

    let Some(event) = payload.get("event") else {
        return Some("missing event body".to_owned());
    };
    let Some(message) = event.get("message") else {
        return Some("missing event.message".to_owned());
    };
    let message_type = message.get("message_type").and_then(Value::as_str);
    match message_type {
        Some("text") | Some("image") | Some("file") => {}
        Some(other) => {
            return Some(format!(
                "unsupported message_type={other}, expected text|image|file"
            ));
        }
        None => return Some("missing message.message_type".to_owned()),
    }
    let Some(chat_id) = message.get("chat_id").and_then(Value::as_str) else {
        return Some("missing message.chat_id".to_owned());
    };
    let sender_id = event
        .get("sender")
        .and_then(|sender| sender.get("sender_id"));
    if !feishu_allowed_source_matches(allowed_source_ids, sender_id) {
        return Some(format!(
            "filtered by allowed sender ids: allowed={}, chat_id={}, sender_ids={}",
            feishu_allowed_ids_summary(allowed_source_ids),
            chat_id,
            feishu_sender_ids_summary(sender_id)
        ));
    }
    let Some(raw_content) = message.get("content").and_then(Value::as_str) else {
        return Some("missing message.content".to_owned());
    };
    let Ok(content_json) = serde_json::from_str::<Value>(raw_content) else {
        return Some("message.content is not valid JSON".to_owned());
    };
    match message_type {
        Some("text") => {
            let text = content_json
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("");
            if text.trim().is_empty() {
                return Some("message text is empty after trimming".to_owned());
            }
        }
        Some("image")
            if content_json
                .get("image_key")
                .and_then(Value::as_str)
                .is_none() =>
        {
            return Some("image message missing image_key".to_owned());
        }
        Some("file")
            if content_json
                .get("file_key")
                .and_then(Value::as_str)
                .is_none() =>
        {
            return Some("file message missing file_key".to_owned());
        }
        _ => {}
    }
    None
}

fn feishu_allowed_source_matches(
    allowed_source_ids: &[Arc<str>],
    sender_id: Option<&Value>,
) -> bool {
    if allowed_source_ids.is_empty() {
        return true;
    }
    allowed_source_ids.iter().any(|allowed| {
        sender_id.is_some_and(|sender_id| feishu_sender_id_matches(sender_id, allowed))
    })
}

fn feishu_sender_id_matches(sender_id: &Value, allowed: &str) -> bool {
    ["open_id", "user_id", "union_id"]
        .into_iter()
        .any(|key| sender_id.get(key).and_then(Value::as_str) == Some(allowed))
}

fn preferred_feishu_sender_id(sender_id: &Value) -> Option<&str> {
    sender_id
        .get("open_id")
        .and_then(Value::as_str)
        .or_else(|| sender_id.get("user_id").and_then(Value::as_str))
        .or_else(|| sender_id.get("union_id").and_then(Value::as_str))
}

fn feishu_sender_ids_summary(sender_id: Option<&Value>) -> String {
    let Some(sender_id) = sender_id else {
        return "<missing>".to_owned();
    };
    let mut parts = Vec::new();
    for key in ["open_id", "user_id", "union_id"] {
        if let Some(value) = sender_id.get(key).and_then(Value::as_str) {
            parts.push(format!("{key}={value}"));
        }
    }
    if parts.is_empty() {
        "<missing>".to_owned()
    } else {
        parts.join(",")
    }
}

fn feishu_allowed_ids_summary(allowed_source_ids: &[Arc<str>]) -> String {
    if allowed_source_ids.is_empty() {
        "<unset>".to_owned()
    } else {
        allowed_source_ids
            .iter()
            .map(|value| value.as_ref())
            .collect::<Vec<_>>()
            .join(",")
    }
}

pub(super) fn feishu_allowed_source_ids_from_env() -> Vec<Arc<str>> {
    let mut values = Vec::new();
    extend_allowed_source_ids(&mut values, env::var("AGENTOS_FEISHU_ALLOWED_ID").ok());
    values
}

fn extend_allowed_source_ids(values: &mut Vec<Arc<str>>, raw: Option<String>) {
    let Some(raw) = raw else {
        return;
    };
    for value in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if values.iter().any(|existing| existing.as_ref() == value) {
            continue;
        }
        values.push(Arc::from(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn channel_id() -> ChannelId {
        ChannelId::new("feishu")
    }

    fn text_event(text: &str) -> Value {
        json!({
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_a" } },
                "message": {
                    "chat_id": "oc_1",
                    "message_id": "om_1",
                    "message_type": "text",
                    "content": json!({ "text": text }).to_string(),
                }
            }
        })
    }

    #[test]
    fn parses_text_message() {
        let parsed =
            parse_event(&text_event("hello"), &channel_id(), &[], "chat_id").expect("parsed");
        assert_eq!(parsed.envelope.message.content.as_ref(), "hello");
        assert!(parsed.attachments.is_empty());
        assert_eq!(parsed.message_id, "om_1");
    }

    #[test]
    fn parses_image_message() {
        let payload = json!({
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_a" } },
                "message": {
                    "chat_id": "oc_1",
                    "message_id": "om_2",
                    "message_type": "image",
                    "content": json!({ "image_key": "img_abc" }).to_string(),
                }
            }
        });
        let parsed = parse_event(&payload, &channel_id(), &[], "chat_id").expect("parsed");
        assert!(parsed.envelope.message.content.is_empty());
        assert_eq!(parsed.attachments.len(), 1);
        let desc = &parsed.attachments[0];
        assert_eq!(desc.kind, AttachmentKind::Image);
        assert_eq!(desc.key, "img_abc");
        assert_eq!(desc.name, "img_abc.jpg");
    }

    #[test]
    fn parses_file_message() {
        let payload = json!({
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_a" } },
                "message": {
                    "chat_id": "oc_1",
                    "message_id": "om_3",
                    "message_type": "file",
                    "content": json!({ "file_key": "file_xyz", "file_name": "spec.pdf" }).to_string(),
                }
            }
        });
        let parsed = parse_event(&payload, &channel_id(), &[], "chat_id").expect("parsed");
        assert_eq!(parsed.attachments.len(), 1);
        let desc = &parsed.attachments[0];
        assert_eq!(desc.kind, AttachmentKind::Document);
        assert_eq!(desc.key, "file_xyz");
        assert_eq!(desc.name, "spec.pdf");
    }

    #[test]
    fn drops_unsupported_message_type() {
        let payload = json!({
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_a" } },
                "message": {
                    "chat_id": "oc_1",
                    "message_id": "om_4",
                    "message_type": "audio",
                    "content": "{}",
                }
            }
        });
        assert!(parse_event(&payload, &channel_id(), &[], "chat_id").is_none());
        let reason = feishu_drop_reason(&payload, &[]).expect("reason");
        assert!(reason.contains("unsupported message_type=audio"));
    }

    #[test]
    fn allowed_source_filter_applies() {
        let allowed = [Arc::from("ou_other")];
        assert!(parse_event(&text_event("hi"), &channel_id(), &allowed, "chat_id").is_none());
    }

    #[test]
    fn open_id_receive_type_uses_sender_open_id_as_conversation() {
        let parsed = parse_event(&text_event("hi"), &channel_id(), &[], "open_id").expect("parsed");
        assert_eq!(parsed.envelope.conversation_id.as_str(), "ou_a");
    }

    #[test]
    fn open_id_receive_type_falls_back_to_chat_id_when_sender_missing() {
        let payload = json!({
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "user_id": "uid_x" } },
                "message": {
                    "chat_id": "oc_1",
                    "message_id": "om_5",
                    "message_type": "text",
                    "content": json!({ "text": "hi" }).to_string(),
                }
            }
        });
        let parsed = parse_event(&payload, &channel_id(), &[], "open_id").expect("parsed");
        assert_eq!(parsed.envelope.conversation_id.as_str(), "oc_1");
    }
}
