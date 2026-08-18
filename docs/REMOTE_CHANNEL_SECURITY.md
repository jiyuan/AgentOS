# Remote channel security

Telegram and Feishu ingress fails closed unless the deployment explicitly
authorizes stable provider identities. Configure each enabled remote channel in
`workspace/agent.toml`:

```toml
[channels.telegram]
enabled = true
mode = "polling"
allowed_sender_ids = ["123456789"]
allowed_conversation_ids = ["-1001234567890"]
administrator_ids = ["987654321"]
allow_all_senders = false
```

The allow rules are alternatives: an authenticated event is admitted when its
stable sender id is listed, its conversation id is listed, or the sender is an
administrator. `allow_all_senders = true` is an explicit compatibility opt-in;
even then, an event without a provider-issued sender id is rejected.

For Telegram, sender ids are numeric Bot API user ids and conversation ids are
chat ids. `AGENTOS_TELEGRAM_CHAT_ID` and
`AGENTOS_TELEGRAM_ALLOWED_SENDER_ID` remain supported as legacy comma-separated
allowlists and are merged with the typed configuration.

For Feishu, use stable `open_id`, `user_id`, or `union_id` values for sender
allowlisting; `open_id` is preferred for administrator identity because it is
the principal id when present. Conversation ids are Feishu `chat_id` values.
`AGENTOS_FEISHU_ALLOWED_ID` remains a legacy sender allowlist.

## Approval binding

An approval ticket is bound to the complete principal that initiated its
paused run. Another participant in the same group cannot resolve it, even if
they can see or guess the ticket. A configured administrator may resolve the
ticket only from the same channel and conversation. Different participants
retain distinct session keys while being routed to the same shard so this
same-conversation administrator check can be enforced without sharing session
or memory state.

One-shot remote commands and the persistent gateway apply the same resolver
rule. Local TUI resume remains a trusted local operation.
