use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ChannelConfig {
    /// Whether this channel is enabled.
    pub enabled: bool,
    /// The channel's supported receive mode.
    pub mode: Arc<str>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Arc::from("disabled"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct RemoteChannelConfig {
    /// Whether this remote channel is enabled.
    pub enabled: bool,
    /// The channel's supported receive mode.
    pub mode: Arc<str>,
    /// Stable provider sender identifiers allowed to submit input.
    pub allowed_sender_ids: Vec<Arc<str>>,
    /// Provider conversation or chat identifiers whose authenticated members may submit input.
    pub allowed_conversation_ids: Vec<Arc<str>>,
    /// Stable provider sender identifiers allowed to resolve another member's approval in the same conversation.
    pub administrator_ids: Vec<Arc<str>>,
    /// Explicit compatibility opt-in accepting every provider-authenticated sender.
    pub allow_all_senders: bool,
}

impl RemoteChannelConfig {
    pub fn validate(&self, name: &str, supported_modes: &[&str]) -> Result<(), String> {
        super::validate_channel_mode(name, self.enabled, &self.mode, supported_modes)?;
        for (field, values) in [
            ("allowed_sender_ids", &self.allowed_sender_ids),
            ("allowed_conversation_ids", &self.allowed_conversation_ids),
            ("administrator_ids", &self.administrator_ids),
        ] {
            if values.iter().any(|value| value.trim().is_empty()) {
                return Err(format!("{name}.{field} entries must not be empty"));
            }
        }
        Ok(())
    }

    pub fn is_administrator(&self, sender: &str) -> bool {
        !sender.is_empty()
            && self
                .administrator_ids
                .iter()
                .any(|administrator| administrator.as_ref() == sender)
    }
}

impl Default for RemoteChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Arc::from("disabled"),
            allowed_sender_ids: Vec::new(),
            allowed_conversation_ids: Vec::new(),
            administrator_ids: Vec::new(),
            allow_all_senders: false,
        }
    }
}
