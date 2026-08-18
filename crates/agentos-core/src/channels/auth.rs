use crate::config::RemoteChannelConfig;
use agentos_interfaces::ChannelError;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(super) struct RemoteIngressPolicy {
    allowed_sender_ids: Vec<Arc<str>>,
    allowed_conversation_ids: Vec<Arc<str>>,
    allow_all_senders: bool,
}

impl RemoteIngressPolicy {
    pub(super) fn from_config(
        channel: &str,
        config: &RemoteChannelConfig,
        legacy_sender_ids: impl IntoIterator<Item = Arc<str>>,
        legacy_conversation_ids: impl IntoIterator<Item = Arc<str>>,
    ) -> Result<Self, ChannelError> {
        let mut allowed_sender_ids = config.allowed_sender_ids.clone();
        extend_unique(&mut allowed_sender_ids, legacy_sender_ids);
        let mut allowed_conversation_ids = config.allowed_conversation_ids.clone();
        extend_unique(&mut allowed_conversation_ids, legacy_conversation_ids);
        if !config.allow_all_senders
            && allowed_sender_ids.is_empty()
            && allowed_conversation_ids.is_empty()
            && config.administrator_ids.is_empty()
        {
            return Err(ChannelError::Backend(Arc::from(format!(
                "{channel} remote ingress requires allowed_sender_ids, allowed_conversation_ids, administrator_ids, or explicit allow_all_senders = true"
            ))));
        }
        extend_unique(
            &mut allowed_sender_ids,
            config.administrator_ids.iter().cloned(),
        );
        Ok(Self {
            allowed_sender_ids,
            allowed_conversation_ids,
            allow_all_senders: config.allow_all_senders,
        })
    }

    pub(super) fn authorizes(&self, sender_ids: &[&str], conversation_id: &str) -> bool {
        if sender_ids.is_empty() || sender_ids.iter().any(|sender| sender.is_empty()) {
            return false;
        }
        self.allow_all_senders
            || self
                .allowed_sender_ids
                .iter()
                .any(|allowed| sender_ids.contains(&allowed.as_ref()))
            || self
                .allowed_conversation_ids
                .iter()
                .any(|allowed| allowed.as_ref() == conversation_id)
    }

    #[cfg(test)]
    pub(super) fn allow_all() -> Self {
        Self {
            allowed_sender_ids: Vec::new(),
            allowed_conversation_ids: Vec::new(),
            allow_all_senders: true,
        }
    }
}

fn extend_unique(target: &mut Vec<Arc<str>>, values: impl IntoIterator<Item = Arc<str>>) {
    for value in values {
        if !target.iter().any(|existing| existing == &value) {
            target.push(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_ingress_fails_closed_without_explicit_authority() {
        let error =
            RemoteIngressPolicy::from_config("telegram", &RemoteChannelConfig::default(), [], [])
                .expect_err("empty remote policy must fail closed");
        assert!(error.to_string().contains("allow_all_senders"));
    }

    #[test]
    fn sender_chat_and_administrator_entries_authorize_authenticated_ingress() {
        let config = RemoteChannelConfig {
            allowed_sender_ids: vec![Arc::from("sender")],
            allowed_conversation_ids: vec![Arc::from("chat")],
            administrator_ids: vec![Arc::from("admin")],
            ..RemoteChannelConfig::default()
        };
        let policy =
            RemoteIngressPolicy::from_config("telegram", &config, [], []).expect("explicit policy");
        assert!(policy.authorizes(&["sender"], "other-chat"));
        assert!(policy.authorizes(&["participant"], "chat"));
        assert!(policy.authorizes(&["admin"], "other-chat"));
        assert!(!policy.authorizes(&["stranger"], "other-chat"));
        assert!(!policy.authorizes(&[], "chat"));
    }
}
