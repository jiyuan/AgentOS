//! Resolving *who* a memory operation belongs to, from a run context.
//!
//! Split out of `memory/mod.rs` to keep it under the module ceiling. Every
//! function here answers the same question from a different angle: a
//! `RunState` carries no conversation, channel, or sender — those arrive on
//! the `Envelope` and are stamped onto the input item's metadata — so this is
//! the one place that reads them back, and the reason it is one place is that
//! two readings of "who is this" is how one caller ends up with another's
//! memory.

use super::{MemoryCaller, SharedDomainGrants};
use agentos_interfaces::orchestrator::RunContext;
use agentos_proto::{ChannelId, ConversationId};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Build the caller a memory operation is decided against.
///
/// `grants` is what the *deployment* permits — `[[memory.shared_domains]]`
/// intersected with `[memory.policy].shared_writes`. A sub-agent's own
/// `memory_view` can only ever narrow it: a declared domain is added to the
/// caller's lists only when the deployment already permits it for that
/// operation, so the three gates M7 / `MEM-001` calls for (global, domain,
/// caller) compose here and cannot be reordered by a call site.
pub fn memory_caller_from_context(
    ctx: &RunContext<'_>,
    grants: &SharedDomainGrants,
) -> MemoryCaller {
    let metadata = caller_metadata_from_context(ctx);
    let conversation_id = conversation_id_from_context(ctx)
        .unwrap_or_else(|| ConversationId::new(ctx.state.run_id.as_str()));
    let user_id_key = if metadata
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "subagent_input")
    {
        "user_id"
    } else {
        "sender"
    };
    let user_id = metadata
        .get(user_id_key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|sender| !sender.is_empty())
        .map(Arc::from);
    let view = metadata.get("memory_view").and_then(Value::as_str);
    let declared: Vec<Arc<str>> = match view {
        Some("shared_readonly" | "shared_readwrite") => metadata
            .get("memory_domains")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(Arc::from)
            .collect(),
        _ => Vec::new(),
    };
    let mut allowed_shared_domains = grants.readable.clone();
    allowed_shared_domains.extend(
        declared
            .iter()
            .filter(|domain| grants.readable.iter().any(|allowed| allowed == *domain))
            .cloned(),
    );
    allowed_shared_domains.sort();
    allowed_shared_domains.dedup();

    // Writes are never inherited from the deployment alone: a run has to be
    // acting under a `shared_readwrite` view *and* name the domain, on top of
    // the deployment permitting it. A top-level run declares no view, so it
    // writes to no shared domain — which is the conservative reading of a
    // setting whose whole risk is that one conversation's writes are another's
    // reads.
    let mut writable_shared_domains: Vec<Arc<str>> = match view {
        Some("shared_readwrite") => declared
            .into_iter()
            .filter(|domain| grants.permits_write(domain))
            .collect(),
        _ => Vec::new(),
    };
    writable_shared_domains.sort();
    writable_shared_domains.dedup();

    MemoryCaller {
        agent_id: ctx.system.active_agent.clone(),
        task_id: ctx.system.task_id.clone(),
        channel_id: channel_id_from_context(ctx),
        conversation_id,
        user_id,
        allowed_shared_domains,
        writable_shared_domains,
        audit_read_access: audit_read_access_from_context(ctx),
    }
}

/// The conversation a run belongs to, if the transcript records one.
///
/// `RunState` does not carry a conversation id — it arrives on the `Envelope`
/// and is stamped onto the input item's metadata — so this is the one place
/// that resolution lives. Shared with the job registry (roadmap D3) rather than
/// duplicated: both use it to fence one conversation's data off from another's,
/// and two copies of a security boundary are two chances to drift.
///
/// `None` when nothing in the transcript names one, which callers must treat as
/// "no owner" rather than substituting a default.
pub fn conversation_id_from_context(ctx: &RunContext<'_>) -> Option<ConversationId> {
    caller_metadata_from_context(ctx)
        .get("conversation_id")
        .and_then(Value::as_str)
        .map(ConversationId::new)
}

/// The channel the conversation arrived on, which is half of what makes a
/// conversation id unique: `telegram:42` and `feishu:42` are both `"42"`.
///
/// Falls back to the reserved `unknown-channel` rather than to an empty
/// string, so a context that never carried one is visibly distinct from a
/// channel actually named "".
pub fn channel_id_from_context(ctx: &RunContext<'_>) -> ChannelId {
    caller_metadata_from_context(ctx)
        .get("channel_id")
        .and_then(Value::as_str)
        .map_or_else(|| ChannelId::new("unknown-channel"), ChannelId::new)
}

fn caller_metadata_from_context<'a>(ctx: &'a RunContext<'_>) -> &'a BTreeMap<Arc<str>, Value> {
    ctx.transcript
        .items
        .iter()
        .rev()
        .map(|item| &item.metadata)
        .find(|metadata| {
            metadata.contains_key("conversation_id")
                || metadata.contains_key("sender")
                || metadata.contains_key("user_id")
                || metadata.contains_key("memory_view")
        })
        .unwrap_or_else(|| {
            ctx.transcript
                .items
                .last()
                .map(|item| &item.metadata)
                .unwrap_or(&ctx.system.metadata)
        })
}

fn audit_read_access_from_context(ctx: &RunContext<'_>) -> bool {
    let Some(item) = ctx.transcript.items.iter().rev().find(|item| {
        item.metadata
            .get("channel_id")
            .and_then(Value::as_str)
            .is_some()
    }) else {
        return false;
    };
    let channel_is_trusted_parent = item
        .metadata
        .get("channel_id")
        .and_then(Value::as_str)
        .is_some_and(|channel| matches!(channel, "tui" | "telegram" | "feishu"));
    let not_subagent = item
        .metadata
        .get("kind")
        .and_then(Value::as_str)
        .is_none_or(|kind| kind != "subagent_input");
    channel_is_trusted_parent && not_subagent
}
