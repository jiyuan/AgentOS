use serde::{Deserialize, Serialize};
use std::sync::Arc;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Arc<str>);

        impl $name {
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

id_type!(AgentId);
id_type!(ChannelId);
id_type!(ConversationId);
id_type!(InterruptionId);
id_type!(Namespace);
id_type!(RecordId);
id_type!(RunId);
id_type!(SenderId);
id_type!(SpanId);
id_type!(TaskId);
id_type!(ToolCallId);

/// The sender component of a principal.
///
/// Some ingress has no sender identifier. Keeping that case as a variant
/// prevents an absent sender from being represented by an empty string and
/// accidentally comparing equal to a malformed identified sender. Whether an
/// unattributed envelope is allowed is an ingress authorization decision.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum SenderIdentity {
    Identified(SenderId),
    Unattributed,
}

impl SenderIdentity {
    pub fn identified(value: impl Into<Arc<str>>) -> Self {
        Self::Identified(SenderId::new(value))
    }
}

/// Version one of the complete persistence and authorization principal.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PrincipalKeyV1 {
    pub agent_id: AgentId,
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    pub sender: SenderIdentity,
}

impl PrincipalKeyV1 {
    pub fn new(
        agent_id: AgentId,
        channel_id: ChannelId,
        conversation_id: ConversationId,
        sender: SenderIdentity,
    ) -> Self {
        Self {
            agent_id,
            channel_id,
            conversation_id,
            sender,
        }
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_u16.to_be_bytes());
        push_component(&mut bytes, self.agent_id.as_str());
        push_component(&mut bytes, self.channel_id.as_str());
        push_component(&mut bytes, self.conversation_id.as_str());
        match &self.sender {
            SenderIdentity::Identified(sender_id) => {
                bytes.push(1);
                push_component(&mut bytes, sender_id.as_str());
            }
            SenderIdentity::Unattributed => bytes.push(0),
        }
        bytes
    }
}

/// A versioned principal key. New formats must be added as new variants rather
/// than changing the bytes emitted for an existing version.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "version", content = "principal")]
pub enum PrincipalKey {
    V1(PrincipalKeyV1),
}

impl PrincipalKey {
    pub fn v1(
        agent_id: AgentId,
        channel_id: ChannelId,
        conversation_id: ConversationId,
        sender: SenderIdentity,
    ) -> Self {
        Self::V1(PrincipalKeyV1::new(
            agent_id,
            channel_id,
            conversation_id,
            sender,
        ))
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        match self {
            Self::V1(principal) => principal.canonical_bytes(),
        }
    }

    pub fn storage_key(&self) -> String {
        format!("pk1_{}", encode_base64url(&self.canonical_bytes()))
    }

    /// Parse and validate a canonical storage key.
    pub fn parse_storage_key(input: &str) -> Option<Self> {
        let encoded = input.strip_prefix("pk1_")?;
        let bytes = decode_base64url(encoded)?;
        if encode_base64url(&bytes) != encoded {
            return None;
        }
        let (principal, consumed) = parse_principal_bytes(&bytes)?;
        (consumed == bytes.len()).then_some(principal)
    }
}

/// A session is one epoch of one principal's append-only log.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SessionKey {
    pub principal: PrincipalKey,
    pub epoch: u64,
}

impl SessionKey {
    pub fn new(principal: PrincipalKey, epoch: u64) -> Self {
        Self { principal, epoch }
    }

    pub fn initial(principal: PrincipalKey) -> Self {
        Self::new(principal, 0)
    }

    pub fn storage_key(&self) -> String {
        let mut bytes = self.principal.canonical_bytes();
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        format!("sk1_{}", encode_base64url(&bytes))
    }

    /// Parse and validate a canonical storage key.
    pub fn parse_storage_key(input: &str) -> Option<Self> {
        let encoded = input.strip_prefix("sk1_")?;
        let bytes = decode_base64url(encoded)?;
        if encode_base64url(&bytes) != encoded {
            return None;
        }
        let (principal, consumed) = parse_principal_bytes(&bytes)?;
        let epoch_bytes: [u8; 8] = bytes.get(consumed..)?.try_into().ok()?;
        Some(Self::new(principal, u64::from_be_bytes(epoch_bytes)))
    }
}

/// Encode arbitrary bytes as unpadded, filesystem-safe base64url.
///
/// Unlike replacement-based sanitizers, this mapping is injective.
pub fn encode_base64url(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        }
        if chunk.len() > 2 {
            output.push(TABLE[(third & 0x3f) as usize] as char);
        }
    }
    output
}

/// Decode unpadded canonical base64url text.
pub fn decode_base64url(input: &str) -> Option<Vec<u8>> {
    if input.len() % 4 == 1 {
        return None;
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3 + 2);
    for chunk in input.as_bytes().chunks(4) {
        let first = decode_base64url_byte(*chunk.first()?)?;
        let second = decode_base64url_byte(*chunk.get(1)?)?;
        output.push((first << 2) | (second >> 4));
        if let Some(third_byte) = chunk.get(2) {
            let third = decode_base64url_byte(*third_byte)?;
            output.push((second << 4) | (third >> 2));
            if let Some(fourth_byte) = chunk.get(3) {
                let fourth = decode_base64url_byte(*fourth_byte)?;
                output.push((third << 6) | fourth);
            }
        }
    }
    (encode_base64url(&output) == input).then_some(output)
}

fn decode_base64url_byte(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn parse_principal_bytes(bytes: &[u8]) -> Option<(PrincipalKey, usize)> {
    let version = u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?);
    if version != 1 {
        return None;
    }
    let mut offset = 2;
    let agent_id = take_component(bytes, &mut offset)?;
    let channel_id = take_component(bytes, &mut offset)?;
    let conversation_id = take_component(bytes, &mut offset)?;
    let sender = match *bytes.get(offset)? {
        0 => {
            offset += 1;
            SenderIdentity::Unattributed
        }
        1 => {
            offset += 1;
            SenderIdentity::identified(take_component(bytes, &mut offset)?)
        }
        _ => return None,
    };
    Some((
        PrincipalKey::v1(
            AgentId::new(agent_id),
            ChannelId::new(channel_id),
            ConversationId::new(conversation_id),
            sender,
        ),
        offset,
    ))
}

fn take_component(bytes: &[u8], offset: &mut usize) -> Option<Arc<str>> {
    let length_end = offset.checked_add(8)?;
    let length = u64::from_be_bytes(bytes.get(*offset..length_end)?.try_into().ok()?);
    let length = usize::try_from(length).ok()?;
    let component_end = length_end.checked_add(length)?;
    let component = std::str::from_utf8(bytes.get(length_end..component_end)?).ok()?;
    *offset = component_end;
    Some(Arc::from(component))
}

fn push_component(bytes: &mut Vec<u8>, component: &str) {
    let component = component.as_bytes();
    let length = u64::try_from(component.len()).unwrap_or(u64::MAX);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(component);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaVersion(pub u16);

impl Default for SchemaVersion {
    fn default() -> Self {
        Self(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn principal(agent: &str, channel: &str, conversation: &str, sender: &str) -> PrincipalKey {
        PrincipalKey::v1(
            AgentId::new(agent),
            ChannelId::new(channel),
            ConversationId::new(conversation),
            SenderIdentity::identified(sender),
        )
    }

    #[test]
    fn base64url_encoding_is_injective_for_formerly_colliding_names() {
        assert_ne!(encode_base64url(b"a/b"), encode_base64url(b"a_b"));
        assert!(encode_base64url(b"a/b").chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
        }));
    }

    #[test]
    fn principal_storage_key_separates_every_identity_component() {
        let keys = [
            principal("agent-a", "telegram", "42", "alice"),
            principal("agent-a", "feishu", "42", "alice"),
            principal("agent-b", "telegram", "42", "alice"),
            principal("agent-a", "telegram", "43", "alice"),
            principal("agent-a", "telegram", "42", "bob"),
        ]
        .into_iter()
        .map(|key| key.storage_key())
        .collect::<BTreeSet<_>>();
        assert_eq!(keys.len(), 5);
    }

    #[test]
    fn unattributed_is_not_an_empty_identified_sender() {
        let local = PrincipalKey::v1(
            AgentId::new("agent"),
            ChannelId::new("cli"),
            ConversationId::new("42"),
            SenderIdentity::Unattributed,
        );
        let empty = principal("agent", "cli", "42", "");
        assert_ne!(local.storage_key(), empty.storage_key());
    }

    #[test]
    fn session_epoch_changes_the_storage_key() {
        let principal = principal("agent", "telegram", "42", "alice");
        assert_ne!(
            SessionKey::new(principal.clone(), 0).storage_key(),
            SessionKey::new(principal, 1).storage_key()
        );
    }

    #[test]
    fn storage_keys_round_trip_and_reject_noncanonical_text() {
        let principal = principal("agent/a", "telegram", "42", "alice_b");
        let session = SessionKey::new(principal.clone(), 7);
        assert_eq!(
            PrincipalKey::parse_storage_key(&principal.storage_key()),
            Some(principal)
        );
        assert_eq!(
            SessionKey::parse_storage_key(&session.storage_key()),
            Some(session)
        );
        assert!(SessionKey::parse_storage_key("sk1_not+base64").is_none());
        assert!(SessionKey::parse_storage_key("conversation-42").is_none());
    }
}
