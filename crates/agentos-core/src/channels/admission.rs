//! Whether a remote channel accepts an inbound event at all.
//!
//! `AUTH-001`. Both reference channels used to fail *open*: Telegram accepted
//! every chat when `AGENTOS_TELEGRAM_CHAT_ID` was unset and had no sender
//! allowlist at all, and Feishu's `feishu_allowed_source_matches` returned
//! `true` for an empty allowlist. A deployment that forgot to configure one —
//! or whose variable was typo'd, so it parsed as empty — accepted the whole
//! internet into an agent holding tools and credentials.
//!
//! One type, shared by both channels, so the two cannot drift and a reader
//! does not have to check each transport to learn what the rule is
//! ([ADR-0003](../../../../docs/adr/0003-TYPED_PRINCIPAL.md)).
//!
//! # The rule
//!
//! An event is admitted when its chat is allowed **and** its sender is allowed.
//! An empty allowlist admits nothing. `allow_all` is the escape hatch for a
//! deployment that genuinely wants an open channel — a private bot on a
//! machine nobody else can reach — and it is explicit, so it appears in the
//! configuration rather than in its absence.
//!
//! **An unattributed event is refused even under `allow_all`.** "Allow every
//! sender" is a statement about senders, not permission to invent one. The old
//! Telegram path substituted the literal `telegram-user` for a message with no
//! `from.id`, which put unattributable traffic under a principal that looks
//! like a person.

use std::sync::Arc;

/// Whether an environment variable is set to something meaning "yes".
///
/// Shared by the channels so `AGENTOS_TELEGRAM_ALLOW_ALL` and its Feishu
/// counterpart accept the same words. Anything else — including the variable
/// being present but empty — is a no, because an opt-out of a security default
/// should require saying so clearly.
pub fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Why an event was refused. Carried rather than logged at the check so the
/// caller decides how loud to be — a poll loop that logged every refusal at
/// `warn` would hand anyone a way to fill the disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// No allowlist is configured, and `allow_all` was not set. The
    /// fail-closed default.
    NoAllowlist,
    ChatNotAllowed,
    SenderNotAllowed,
    /// The event carries no attributable sender.
    Unattributed,
}

impl Refusal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoAllowlist => {
                "no chat or sender allowlist is configured; set one, or opt in to an open \
                 channel explicitly"
            }
            Self::ChatNotAllowed => "the chat is not allowlisted",
            Self::SenderNotAllowed => "the sender is not allowlisted",
            Self::Unattributed => "the event carries no sender to attribute it to",
        }
    }
}

/// Who a channel accepts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmissionPolicy {
    chats: Vec<Arc<str>>,
    senders: Vec<Arc<str>>,
    allow_all: bool,
}

impl AdmissionPolicy {
    pub fn new(
        chats: impl IntoIterator<Item = Arc<str>>,
        senders: impl IntoIterator<Item = Arc<str>>,
        allow_all: bool,
    ) -> Self {
        Self {
            chats: chats.into_iter().collect(),
            senders: senders.into_iter().collect(),
            allow_all,
        }
    }

    /// Parse the comma-separated form the channel environment variables use.
    /// Empty entries are dropped, so `"a,,b"` is two ids and `" "` is none —
    /// and none means the channel refuses rather than opens.
    pub fn parse_ids(raw: Option<&str>) -> Vec<Arc<str>> {
        raw.into_iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Arc::from)
            .collect()
    }

    /// True when nothing would ever be admitted, so a caller can say so at
    /// startup instead of silently dropping every message.
    pub fn admits_nothing(&self) -> bool {
        !self.allow_all && self.chats.is_empty() && self.senders.is_empty()
    }

    /// Whether this event is accepted. `sender` is `None` when the transport
    /// could not attribute it.
    pub fn admit(&self, chat: Option<&str>, sender: Option<&str>) -> Result<(), Refusal> {
        self.admit_matching(chat, sender, |allowed| Some(allowed) == sender)
    }

    /// [`Self::admit`] with the sender comparison supplied by the caller.
    ///
    /// Feishu identifies one person by three ids — `open_id`, `user_id`,
    /// `union_id` — and an allowlist entry may name any of them, so equality
    /// against a single string is not the right test there. `attribution` is
    /// still what decides whether the event is attributable at all, so the
    /// unattributed rule holds regardless of how matching is done.
    pub fn admit_matching(
        &self,
        chat: Option<&str>,
        attribution: Option<&str>,
        sender_matches: impl Fn(&str) -> bool,
    ) -> Result<(), Refusal> {
        // Checked before anything else, including `allow_all`: an event nobody
        // can be held responsible for is refused on every path.
        if !attribution.is_some_and(|value| !value.trim().is_empty()) {
            return Err(Refusal::Unattributed);
        }
        if self.allow_all {
            return Ok(());
        }
        if self.admits_nothing() {
            return Err(Refusal::NoAllowlist);
        }
        if !self.chats.is_empty() {
            let chat = chat.unwrap_or_default();
            if !self.chats.iter().any(|allowed| allowed.as_ref() == chat) {
                return Err(Refusal::ChatNotAllowed);
            }
        }
        if !self.senders.is_empty()
            && !self
                .senders
                .iter()
                .any(|allowed| sender_matches(allowed.as_ref()))
        {
            return Err(Refusal::SenderNotAllowed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[&str]) -> Vec<Arc<str>> {
        values.iter().map(|value| Arc::from(*value)).collect()
    }

    /// The finding. An unconfigured channel used to accept everyone.
    #[test]
    fn an_unconfigured_channel_admits_nothing() {
        let policy = AdmissionPolicy::default();
        assert!(policy.admits_nothing());
        assert_eq!(
            policy.admit(Some("42"), Some("alice")),
            Err(Refusal::NoAllowlist)
        );
    }

    /// Including when the variable is present but parses to nothing, which is
    /// how a typo'd or half-edited `.env` presents.
    #[test]
    fn an_empty_allowlist_string_is_not_an_open_channel() {
        let policy =
            AdmissionPolicy::new(AdmissionPolicy::parse_ids(Some("  , ,")), Vec::new(), false);
        assert!(policy.admits_nothing());
        assert!(policy.admit(Some("42"), Some("alice")).is_err());
    }

    #[test]
    fn a_chat_allowlist_admits_only_those_chats() {
        let policy = AdmissionPolicy::new(ids(&["42", "43"]), Vec::new(), false);
        assert_eq!(policy.admit(Some("42"), Some("alice")), Ok(()));
        assert_eq!(policy.admit(Some("43"), Some("bob")), Ok(()));
        assert_eq!(
            policy.admit(Some("99"), Some("alice")),
            Err(Refusal::ChatNotAllowed)
        );
    }

    #[test]
    fn a_sender_allowlist_narrows_within_an_allowed_chat() {
        let policy = AdmissionPolicy::new(ids(&["42"]), ids(&["alice"]), false);
        assert_eq!(policy.admit(Some("42"), Some("alice")), Ok(()));
        assert_eq!(
            policy.admit(Some("42"), Some("mallory")),
            Err(Refusal::SenderNotAllowed)
        );
    }

    /// The compatibility option the plan allows, and the limit on it.
    #[test]
    fn allow_all_opens_the_channel_but_not_to_the_unattributed() {
        let policy = AdmissionPolicy::new(Vec::new(), Vec::new(), true);
        assert_eq!(policy.admit(Some("anything"), Some("anyone")), Ok(()));
        assert_eq!(
            policy.admit(Some("anything"), None),
            Err(Refusal::Unattributed),
            "allow_all is a statement about senders, not permission to invent one"
        );
        assert_eq!(
            policy.admit(Some("anything"), Some("   ")),
            Err(Refusal::Unattributed),
            "a blank sender is no more attributable than a missing one"
        );
    }

    #[test]
    fn an_unattributed_event_is_refused_under_every_configuration() {
        for policy in [
            AdmissionPolicy::default(),
            AdmissionPolicy::new(ids(&["42"]), Vec::new(), false),
            AdmissionPolicy::new(ids(&["42"]), ids(&["alice"]), false),
            AdmissionPolicy::new(Vec::new(), Vec::new(), true),
        ] {
            assert_eq!(
                policy.admit(Some("42"), None),
                Err(Refusal::Unattributed),
                "{policy:?}"
            );
        }
    }

    /// A sender allowlist alone is a valid configuration: accept this person
    /// wherever they write from.
    #[test]
    fn a_sender_allowlist_alone_is_a_configuration() {
        let policy = AdmissionPolicy::new(Vec::new(), ids(&["alice"]), false);
        assert!(!policy.admits_nothing());
        assert_eq!(policy.admit(Some("any-chat"), Some("alice")), Ok(()));
        assert_eq!(
            policy.admit(Some("any-chat"), Some("mallory")),
            Err(Refusal::SenderNotAllowed)
        );
    }

    /// Feishu's shape: one person, three ids, an allowlist naming any of them.
    #[test]
    fn a_caller_supplied_matcher_can_accept_any_of_several_ids() {
        let policy = AdmissionPolicy::new(ids(&["42"]), ids(&["union-1"]), false);
        let their_ids = ["open-1", "user-1", "union-1"];

        assert_eq!(
            policy.admit_matching(Some("42"), Some("open-1"), |allowed| their_ids
                .contains(&allowed)),
            Ok(())
        );
        assert_eq!(
            policy.admit_matching(Some("42"), Some("open-9"), |allowed| allowed == "open-9"),
            Err(Refusal::SenderNotAllowed)
        );
        // Attribution still governs, whatever the matcher would have said.
        assert_eq!(
            policy.admit_matching(Some("42"), None, |_| true),
            Err(Refusal::Unattributed)
        );
    }

    #[test]
    fn env_flag_needs_an_affirmative_word() {
        // Serialised against nothing: each name is unique to this test.
        for (value, expected) in [
            ("1", true),
            ("true", true),
            ("YES", true),
            ("on", true),
            ("0", false),
            ("", false),
            ("maybe", false),
        ] {
            let name = format!("AGENTOS_ADMISSION_FLAG_TEST_{value:?}");
            unsafe { std::env::set_var(&name, value) };
            assert_eq!(env_flag(&name), expected, "for {value:?}");
            unsafe { std::env::remove_var(&name) };
        }
        assert!(!env_flag("AGENTOS_ADMISSION_FLAG_TEST_UNSET"));
    }

    #[test]
    fn parse_ids_drops_blanks_and_trims() {
        assert_eq!(
            AdmissionPolicy::parse_ids(Some(" 42 , ,43,")),
            ids(&["42", "43"])
        );
        assert!(AdmissionPolicy::parse_ids(None).is_empty());
        assert!(AdmissionPolicy::parse_ids(Some("")).is_empty());
    }
}
