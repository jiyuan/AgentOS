//! Who an action is attributed to, as one typed key.
//!
//! Identity used to be stringly typed end to end: a bare `ConversationId`
//! keyed sessions and memory, so `telegram:42`, `feishu:42`, and a second
//! agent's `telegram:42` were the same string and shared state. Namespaces
//! were built by replacement sanitization — `trimmed.replace('/', "_")` — which
//! is not injective, so `a/b` and `a_b` collided too, with a test asserting the
//! collision rather than reporting it.
//!
//! A [`Principal`] is the replacement key: agent, channel, conversation, and —
//! where authorization depends on *who asked* — sender. See
//! [ADR-0003](../../../docs/adr/0003-TYPED_PRINCIPAL.md).
//!
//! # Two encodings, for two jobs
//!
//! [`Principal::canonical_bytes`] is length-prefixed, so no component's
//! contents can be mistaken for a delimiter. It is what a hash or a signature
//! should be taken over.
//!
//! [`Principal::storage_name`] is the readable form used for namespaces and
//! file-safe identifiers. Each component is unpadded base64url, joined by `.`
//! — a character base64url never emits, which is what keeps the join
//! unambiguous. It is deliberately *not* a hash: an operator reading a
//! namespace out of the database should be able to tell whose it is, and
//! `RUST_LOG` should not print opaque digests. Both encodings are injective,
//! which is the property the old scheme lacked and the one everything else
//! here depends on.

use crate::ids::{AgentId, ChannelId, ConversationId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Bumped when the encoding changes shape. Present in both encodings so a
/// stored key states which reader understands it, rather than leaving that to
/// be inferred — the absence of exactly this is why the pre-principal schema
/// cannot be migrated without guessing.
pub const PRINCIPAL_VERSION: u8 = 1;

/// The agent, channel, conversation, and optional sender an action belongs to.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Principal {
    pub agent: AgentId,
    pub channel: ChannelId,
    pub conversation: ConversationId,
    /// Set where authorization depends on which participant acted — a group
    /// chat approving a prompt, say. `None` means "the conversation as a
    /// whole", which is a different principal, not a missing one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<Arc<str>>,
}

impl Principal {
    /// The conversation as a whole, with no particular sender.
    pub fn conversation(agent: AgentId, channel: ChannelId, conversation: ConversationId) -> Self {
        Self {
            agent,
            channel,
            conversation,
            sender: None,
        }
    }

    /// The same conversation, narrowed to one participant.
    pub fn with_sender(mut self, sender: impl Into<Arc<str>>) -> Self {
        self.sender = Some(sender.into());
        self
    }

    /// Drop the sender, yielding the conversation-wide principal this one
    /// belongs to. Useful for state that is shared across participants.
    pub fn without_sender(mut self) -> Self {
        self.sender = None;
        self
    }

    /// Canonical bytes: a version, then each component as a big-endian u32
    /// length followed by its UTF-8. The sender is preceded by a presence
    /// byte, so `None` and `Some("")` do not encode alike.
    ///
    /// Length-prefixed rather than delimited because a delimiter has to be
    /// escaped, and every escaping scheme is one forgotten call site away from
    /// letting a component impersonate a boundary.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![PRINCIPAL_VERSION];
        for component in [
            self.agent.as_str(),
            self.channel.as_str(),
            self.conversation.as_str(),
        ] {
            push_component(&mut bytes, component);
        }
        match &self.sender {
            Some(sender) => {
                bytes.push(1);
                push_component(&mut bytes, sender);
            }
            None => bytes.push(0),
        }
        bytes
    }

    /// The stable, readable, injective name this principal is stored under.
    ///
    /// `v1.<agent>.<channel>.<conversation>.<sender>`, with the sender field
    /// `n` when absent and `s<component>` when present.
    ///
    /// A component made only of `[A-Za-z0-9_-]` appears verbatim; anything
    /// else — a `.`, a space, an empty string, any non-ASCII — appears as `~`
    /// followed by unpadded base64url. So the common case reads as
    /// `v1.main.telegram.42.n` rather than as four base64 blobs, which matters
    /// because these names show up in namespaces, logs, and database rows that
    /// people have to make sense of.
    ///
    /// Injective because the two forms cannot be confused: `~` is in neither
    /// the safe set nor the base64url alphabet, so a verbatim component can
    /// never begin with it. `encode_component` and [`decode_component`] are
    /// exact inverses, which the round-trip test checks over every shape that
    /// distinction has to survive.
    pub fn storage_name(&self) -> String {
        let sender = match &self.sender {
            Some(sender) => format!("s{}", encode_component(sender)),
            None => "n".to_owned(),
        };
        format!(
            "v{PRINCIPAL_VERSION}.{}.{}.{}.{}",
            encode_component(self.agent.as_str()),
            encode_component(self.channel.as_str()),
            encode_component(self.conversation.as_str()),
            sender
        )
    }

    /// The storage name of the conversation-wide principal this one belongs
    /// to — [`Self::without_sender`] then [`Self::storage_name`], without the
    /// clone.
    ///
    /// The two names exist because session state splits on exactly this line
    /// (M3 deliverable 2). A conversation has *one* transcript, shared by
    /// everyone in it, keyed by this. A `/clear` epoch belongs to the
    /// participant who typed it, keyed by [`Self::storage_name`] — so one
    /// member of a group chat clearing their view does not clear anybody
    /// else's ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)).
    pub fn conversation_name(&self) -> String {
        format!(
            "v{PRINCIPAL_VERSION}.{}.{}.{}.n",
            encode_component(self.agent.as_str()),
            encode_component(self.channel.as_str()),
            encode_component(self.conversation.as_str()),
        )
    }

    /// Recover a principal from [`Self::storage_name`].
    ///
    /// Round-tripping matters beyond convenience: maintenance paths read owner
    /// ids back out of storage and need to know whose they are, and a decoder
    /// that agrees with the encoder on every input is a *proof* of
    /// injectivity rather than an argument for it.
    ///
    /// `None` for anything not produced by this version's encoder, including a
    /// name written by a future version — the version prefix is checked, not
    /// skipped.
    pub fn from_storage_name(name: &str) -> Option<Self> {
        let rest = name.strip_prefix(&format!("v{PRINCIPAL_VERSION}."))?;
        let fields: Vec<&str> = rest.split('.').collect();
        let [agent, channel, conversation, sender] = fields.as_slice() else {
            return None;
        };
        let sender = match sender.split_at_checked(1)? {
            ("n", "") => None,
            ("s", encoded) => Some(Arc::<str>::from(decode_component(encoded)?)),
            _ => return None,
        };
        Some(Self {
            agent: AgentId::new(decode_component(agent)?),
            channel: ChannelId::new(decode_component(channel)?),
            conversation: ConversationId::new(decode_component(conversation)?),
            sender,
        })
    }
}

/// A component in the readable-when-possible form [`Principal::storage_name`]
/// documents.
pub fn encode_component(value: &str) -> String {
    let safe = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if safe {
        return value.to_owned();
    }
    format!("~{}", base64url(value.as_bytes()))
}

/// Inverse of [`encode_component`].
pub fn decode_component(encoded: &str) -> Option<String> {
    match encoded.strip_prefix('~') {
        Some(base64) => String::from_utf8(base64url_decode(base64)?).ok(),
        // Reject anything the encoder would not have emitted verbatim, so a
        // hand-written name cannot decode to something the encoder never
        // produces and break the round trip.
        None if !encoded.is_empty()
            && encoded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_') =>
        {
            Some(encoded.to_owned())
        }
        None => None,
    }
}

fn push_component(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

/// Unpadded base64url, per RFC 4648 §5 with the padding omitted.
///
/// Hand-rolled rather than pulled in: it is fifteen lines, `agentos-proto` has
/// no dependencies beyond serde by design, and this needs to stay stable
/// forever because stored names are derived from it.
pub fn base64url(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        // 4 output characters per 3 input bytes, minus one for each byte the
        // final chunk is short: 2 bytes -> 3 chars, 1 byte -> 2 chars.
        let take = chunk.len() + 1;
        for index in 0..take {
            let shift = 18 - 6 * index;
            out.push(ALPHABET[((triple >> shift) & 0x3F) as usize] as char);
        }
    }
    out
}

/// Inverse of [`base64url`]. `None` on any character outside the alphabet or
/// on a length that no input could have produced.
pub fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    fn value(byte: u8) -> Option<u32> {
        match byte {
            b'A'..=b'Z' => Some((byte - b'A') as u32),
            b'a'..=b'z' => Some((byte - b'a') as u32 + 26),
            b'0'..=b'9' => Some((byte - b'0') as u32 + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in input.as_bytes().chunks(4) {
        // 1 leftover character cannot encode any whole byte.
        if chunk.len() == 1 {
            return None;
        }
        let mut triple = 0u32;
        for (index, byte) in chunk.iter().enumerate() {
            triple |= value(*byte)? << (18 - 6 * index);
        }
        for index in 0..chunk.len() - 1 {
            out.push(((triple >> (16 - 8 * index)) & 0xFF) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    /// The two names differ only in the sender field, and the conversation
    /// name is what every participant shares.
    #[test]
    fn a_conversation_name_drops_the_sender_and_matches_the_senderless_principal() {
        let base = super::Principal::conversation(
            super::AgentId::new("main"),
            super::ChannelId::new("telegram"),
            super::ConversationId::new("42"),
        );
        assert_eq!(base.conversation_name(), "v1.main.telegram.42.n");
        assert_eq!(base.conversation_name(), base.storage_name());

        let alice = base.clone().with_sender("alice");
        let bob = base.clone().with_sender("bob");
        assert_eq!(alice.conversation_name(), bob.conversation_name());
        assert_ne!(alice.storage_name(), bob.storage_name());
        assert_eq!(
            alice.conversation_name(),
            alice.clone().without_sender().storage_name()
        );
    }

    /// A conversation id needing escaping must escape the same way in both,
    /// or a cleared participant would key an epoch against a transcript that
    /// is not theirs.
    #[test]
    fn an_escaped_component_encodes_alike_in_both_names() {
        let awkward = super::Principal::conversation(
            super::AgentId::new("main"),
            super::ChannelId::new("feishu"),
            super::ConversationId::new("oc.chat/1"),
        )
        .with_sender("ou_x");
        assert_eq!(
            awkward.conversation_name(),
            awkward.clone().without_sender().storage_name()
        );
        assert!(awkward.conversation_name().ends_with(".n"));
    }

    use super::*;

    fn principal(agent: &str, channel: &str, conversation: &str) -> Principal {
        Principal::conversation(
            AgentId::new(agent),
            ChannelId::new(channel),
            ConversationId::new(conversation),
        )
    }

    /// The audit's headline collision: the same conversation number on two
    /// channels, and on two agents, is three principals.
    #[test]
    fn channel_and_agent_separate_the_same_conversation_number() {
        let telegram = principal("main", "telegram", "42");
        let feishu = principal("main", "feishu", "42");
        let other_agent = principal("second", "telegram", "42");

        assert_ne!(telegram.storage_name(), feishu.storage_name());
        assert_ne!(telegram.storage_name(), other_agent.storage_name());
        assert_ne!(feishu.storage_name(), other_agent.storage_name());
        assert_ne!(telegram.canonical_bytes(), feishu.canonical_bytes());
    }

    /// The encoding property the old `replace('/', "_")` lacked.
    #[test]
    fn components_that_differ_only_by_a_separator_stay_distinct() {
        let slashed = principal("main", "telegram", "a/b");
        let underscored = principal("main", "telegram", "a_b");
        let dotted = principal("main", "telegram", "a.b");

        assert_ne!(slashed.storage_name(), underscored.storage_name());
        assert_ne!(slashed.storage_name(), dotted.storage_name());
        assert_ne!(underscored.storage_name(), dotted.storage_name());
    }

    /// A component cannot impersonate a field boundary, which is the failure
    /// length prefixing exists to prevent.
    #[test]
    fn a_component_cannot_forge_a_boundary() {
        let split = principal("main", "telegram", "42");
        let smuggled = principal("main", "telegram.42", "");
        assert_ne!(split.storage_name(), smuggled.storage_name());
        assert_ne!(split.canonical_bytes(), smuggled.canonical_bytes());

        let shifted = principal("main.telegram", "42", "");
        assert_ne!(split.canonical_bytes(), shifted.canonical_bytes());
    }

    /// `None` is a principal, not a missing field, so it cannot alias the
    /// empty sender.
    #[test]
    fn an_absent_sender_differs_from_an_empty_one() {
        let anonymous = principal("main", "telegram", "42");
        let empty = anonymous.clone().with_sender("");

        assert_ne!(anonymous.storage_name(), empty.storage_name());
        assert_ne!(anonymous.canonical_bytes(), empty.canonical_bytes());
        assert_eq!(empty.without_sender(), anonymous);
    }

    #[test]
    fn senders_separate_participants_in_one_conversation() {
        let base = principal("main", "telegram", "42");
        let alice = base.clone().with_sender("alice");
        let bob = base.clone().with_sender("bob");

        assert_ne!(alice.storage_name(), bob.storage_name());
        assert_ne!(alice.storage_name(), base.storage_name());
    }

    /// Exhaustive over a small alphabet: no two distinct principals share a
    /// storage name or canonical encoding. Injectivity is the property every
    /// other guarantee here rests on, so it is checked by construction rather
    /// than by example.
    #[test]
    fn the_encodings_are_injective_over_a_small_alphabet() {
        let parts = ["", "a", "b", "a/b", "a_b", "a.b", "ab", ".", "/"];
        let mut names = std::collections::BTreeMap::new();
        let mut bytes = std::collections::BTreeMap::new();
        for agent in parts {
            for channel in parts {
                for conversation in parts {
                    for sender in [None, Some(""), Some("a"), Some("a/b")] {
                        let mut candidate = principal(agent, channel, conversation);
                        if let Some(sender) = sender {
                            candidate = candidate.with_sender(sender);
                        }
                        if let Some(clash) =
                            names.insert(candidate.storage_name(), candidate.clone())
                        {
                            assert_eq!(clash, candidate, "storage name collision");
                        }
                        if let Some(clash) =
                            bytes.insert(candidate.canonical_bytes(), candidate.clone())
                        {
                            assert_eq!(clash, candidate, "canonical byte collision");
                        }
                    }
                }
            }
        }
        assert_eq!(names.len(), 9 * 9 * 9 * 4);
        assert_eq!(bytes.len(), 9 * 9 * 9 * 4);
    }

    /// Against RFC 4648 §10's test vectors, minus the padding.
    #[test]
    fn base64url_matches_the_rfc_vectors() {
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(b"foob"), "Zm9vYg");
        assert_eq!(base64url(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url(b"foobar"), "Zm9vYmFy");
    }

    /// The two characters that distinguish base64url from base64, on the two
    /// byte patterns that produce them.
    #[test]
    fn base64url_uses_the_url_safe_alphabet() {
        assert_eq!(base64url(&[0xFF, 0xFF, 0xFE]), "___-");
        assert!(!base64url(&[0xFB, 0xFF, 0xFE]).contains('+'));
    }

    /// Every principal the injectivity test builds survives a round trip, so
    /// the encoding is not merely collision-free but reversible.
    #[test]
    fn storage_names_round_trip() {
        let parts = ["", "a", "b", "a/b", "a_b", "a.b", "~", "\u{e9}", "  "];
        for agent in parts {
            for channel in parts {
                for conversation in parts {
                    for sender in [None, Some(""), Some("a/b")] {
                        let mut original = principal(agent, channel, conversation);
                        if let Some(sender) = sender {
                            original = original.with_sender(sender);
                        }
                        let decoded = Principal::from_storage_name(&original.storage_name());
                        assert_eq!(decoded.as_ref(), Some(&original));
                    }
                }
            }
        }
    }

    #[test]
    fn a_name_this_version_did_not_write_is_refused() {
        assert_eq!(Principal::from_storage_name(""), None);
        assert_eq!(Principal::from_storage_name("v2.a.b.c.n"), None);
        assert_eq!(Principal::from_storage_name("v1.a.b.c"), None);
        assert_eq!(Principal::from_storage_name("v1.a.b.c.x"), None);
        // `!` is outside the base64url alphabet.
        assert_eq!(Principal::from_storage_name("v1.!.b.c.n"), None);
        // A bare conversation id, which is what the pre-principal schema
        // stored. Refusing it is what lets a migration tell the two apart.
        assert_eq!(Principal::from_storage_name("42"), None);
    }

    /// The readability the marker scheme buys, stated as a test so a future
    /// change to the encoder has to justify losing it.
    #[test]
    fn an_ordinary_principal_reads_as_itself() {
        assert_eq!(
            principal("main", "telegram", "42").storage_name(),
            "v1.main.telegram.42.n"
        );
        assert_eq!(
            principal("main", "telegram", "42")
                .with_sender("alice")
                .storage_name(),
            "v1.main.telegram.42.salice"
        );
        // A dot in a component would break the join, so that component — and
        // only that one — is escaped.
        assert_eq!(
            principal("main", "telegram", "a.b").storage_name(),
            "v1.main.telegram.~YS5i.n"
        );
    }

    #[test]
    fn base64url_decode_inverts_the_encoder() {
        for vector in [
            b"".as_slice(),
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
        ] {
            assert_eq!(
                base64url_decode(&base64url(vector)).as_deref(),
                Some(vector)
            );
        }
        assert_eq!(base64url_decode("!"), None);
        assert_eq!(base64url_decode("Z"), None);
    }

    #[test]
    fn the_storage_name_states_its_version() {
        assert!(principal("main", "telegram", "42")
            .storage_name()
            .starts_with("v1."));
        assert_eq!(
            principal("main", "telegram", "42").canonical_bytes()[0],
            PRINCIPAL_VERSION
        );
    }
}
