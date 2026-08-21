# ADR-0003 — A typed principal is the key for every isolated resource

- Status: accepted
- Date: 2026-08-21
- Milestone: M3 (`ID-001`)

## Context

Identity is stringly typed end to end. `agentos-proto/src/ids.rs` is 41 lines
of `pub struct X(pub Arc<str>)`; `Envelope.sender` is a bare `Arc<str>`. Three
failures follow:

- **Namespaces collide.** `memory/scope.rs` builds a namespace with
  `trimmed.replace('/', "_")`, which is not injective: `a/b` and `a_b` map to
  the same namespace. `channels/attachments.rs` contains a test *asserting*
  the collision, so the behavior is pinned rather than merely present.
- **Nothing separates agents or channels.** `telegram:42`, `feishu:42`, and
  another agent's `telegram:42` are the same string, so they share session and
  memory state.
- **Approval is unbound.** Tickets in `loop/approval_route.rs` are minted from
  a clock-seeded counter and carry no principal, so any member of an allowed
  chat can answer another user's approval prompt.

There is also no `schema_version` table — the schema is four bare
`CREATE TABLE IF NOT EXISTS` — so no migration can currently be sequenced.

## Decision

**One versioned principal type is the key for sessions, memory scopes,
approval tickets, `/clear`, jobs, task sessions, and audit events.** It
carries agent-or-tenant, channel, conversation, and — wherever authorization
depends on *who asked* — sender. It serializes to canonical
length-prefixed bytes, so no component's contents can be mistaken for a
delimiter, and to a stable storage name derived from those bytes.

**Encoding is injective, never sanitizing.** Arbitrary components are encoded
with unpadded base64url. Replacement sanitization is prohibited: it is exactly
what produced the `a/b` = `a_b` collision, and a replacement that is
"unlikely" to collide is a collision waiting for an adversarial channel id.

**Approval resolution binds to the principal that received the prompt.** In a
group conversation, only the initiating sender — or an explicitly configured
administrator — can resolve it. An answer from anyone else is ordinary input.

**Remote channels fail closed on identity.** Both currently fail open:
Telegram accepts every chat when `AGENTOS_TELEGRAM_CHAT_ID` is unset and has
no sender allowlist at all; Feishu returns `true` for an empty allowlist. An
unset allowlist must reject. An explicit `allow_all_senders = true`
compatibility option may exist, but an event with no attributable sender is
rejected even under it — "allow everyone" is still not "allow nobody in
particular".

`Session` keys on `&ConversationId` in `agentos-interfaces`, so this is a
deliberate semver break, recorded as such.

## Consequences

- Existing stored data is keyed the old way and must migrate. The migration
  needs the `schema_version` table built alongside it, plus a dry-run report,
  collision detection, a backup requirement, atomicity, and a restart-safe
  progress marker.
- Legacy records that collide under the old encoding cannot be
  disambiguated after the fact. They are **reported, never silently merged** —
  merging two principals' memory is the failure this ADR exists to prevent.
- ~254 `conversation_id` and ~111 `channel_id` call sites change. The type
  change is what makes the compiler find them.

## Verification

- `telegram:42`, `feishu:42`, and another agent's `telegram:42` never share
  session or memory state.
- `a/b` and `a_b` remain distinct namespaces; the attachment test that
  asserted the collision is inverted.
- A second group participant cannot approve or `/clear` the initiator's state.
- A channel with an unset allowlist rejects an inbound message.
- Under `allow_all_senders = true`, an event with no sender is still rejected.
- Round-trip property test: distinct principals never share a storage name.
