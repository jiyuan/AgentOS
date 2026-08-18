use crate::ids::{AgentId, ChannelId, ConversationId, PrincipalKey, SenderIdentity, SessionKey};
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

impl Envelope {
    /// Derive the complete persistence principal for this ingress envelope.
    pub fn principal_key(&self, active_agent: &AgentId) -> PrincipalKey {
        let sender = if self.sender.is_empty() {
            SenderIdentity::Unattributed
        } else {
            SenderIdentity::identified(Arc::clone(&self.sender))
        };
        PrincipalKey::v1(
            active_agent.clone(),
            self.channel_id.clone(),
            self.conversation_id.clone(),
            sender,
        )
    }

    /// Derive the initial session epoch for this ingress envelope.
    pub fn session_key(&self, active_agent: &AgentId) -> SessionKey {
        SessionKey::initial(self.principal_key(active_agent))
    }
}
