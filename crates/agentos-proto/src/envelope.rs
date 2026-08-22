use crate::ids::{ChannelId, ConversationId};
use crate::message::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    pub sender: Arc<str>,
    pub message: Message,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

/// Metadata key naming the transport's own identifier for an inbound event
/// (M8 / `GW-001`, deliverable 3).
///
/// A channel sets this to whatever its transport calls the thing it just
/// received — Telegram's `update_id`, Feishu's `event_id`, a webhook's
/// delivery id. The gateway's ingress ledger keys on `(channel_id, this)` to
/// decide whether an envelope is new, a replay of one that never finished, or
/// a replay of one that did.
///
/// A channel that does not set it gets at-most-once-per-delivery behaviour —
/// which is what every channel had before the ledger existed — because there
/// is nothing to recognise a redelivery *by*. It is not the gateway's place to
/// invent one: a hash of the content would call two identical messages one
/// message, and hashing a timestamp in would call every redelivery new.
pub const INGRESS_ID_KEY: &str = "agentos.ingress_id";

impl Envelope {
    /// The transport's identifier for this event, if the channel set one.
    /// See [`INGRESS_ID_KEY`].
    pub fn ingress_id(&self) -> Option<Arc<str>> {
        match self.metadata.get(INGRESS_ID_KEY)? {
            Value::String(id) => Some(Arc::from(id.as_str())),
            // A numeric id (Telegram's `update_id`) is as good a name as a
            // string one; rendering it here keeps the channels from each
            // having to remember to stringify.
            Value::Number(id) => Some(Arc::from(id.to_string().as_str())),
            _ => None,
        }
    }
}
