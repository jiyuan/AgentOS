//! `agentos-gateway purge` — the only way records leave this runtime on
//! purpose.
//!
//! Three modes, one shape. Each of them deletes something no background sweep
//! is allowed to touch, so each of them makes the operator look at what they
//! are destroying before it happens:
//!
//! - `--conversation ID --channel NAME --yes ID` removes one conversation's
//!   session log. The id is small enough to be the confirmation itself
//!   ([ADR-0006](../../../../../../docs/adr/0006-CLEAR_EPOCH.md)). `--channel`
//!   became required with M3 deliverable 2: a session is keyed by a principal
//!   now, so "conversation 42" no longer names one thing — Telegram's 42 and
//!   Feishu's 42 are different logs, which is the point. `--agent` defaults to
//!   `[agent].id`, as it does for `migrate`.
//! - `--sessions --before DATE` removes whole conversations idle since before
//!   that date.
//! - `--audit --before DATE` removes rows from `safety_events` and
//!   `memory_access_log` ([ADR-0005](../../../../../../docs/adr/0005-SAFETY_EVENTS.md)).
//!
//! The two bulk modes report and change nothing by default, and apply only
//! with `--apply --yes N`, where `N` is the count the report printed. Typing a
//! number back is a weak confirmation on its own; what makes it worth anything
//! is that the only place to *get* the number is the report, so an operator
//! who has one has read the other.
//!
//! Why the date is absolute and not "N days ago": a relative cutoff moves
//! between the report and the apply, so the count the operator was shown would
//! not be the count the apply computes, and the confirmation would refuse
//! correct commands while proving nothing.
//!
//! None of this is gated on `Approve`. That decides what the *model* may do;
//! this is an operator deciding about their own records, and there is no run
//! to pause and no model asking. What it is gated on is shell access to the
//! host, so it cannot be reached by a message in the conversation it would
//! destroy.

use super::{session_path, ServiceConfig};
use agentos_core::audit;
use agentos_core::memory::SqliteStore;
use agentos_core::retention::cutoff_from_date;
use agentos_proto::{AgentId, ChannelId, ConversationId, Principal};
use std::env;

/// Rows a single report will list before it starts summarizing. A deployment
/// with ten thousand idle conversations should get a number and a sample, not
/// ten thousand lines nobody reads.
const MAX_LISTED: usize = 50;

pub(super) fn purge(config: &ServiceConfig) -> Result<(), String> {
    let flags: Vec<String> = env::args().skip(2).collect();
    let value = |name: &str| {
        let mut args = flags.iter();
        while let Some(flag) = args.next() {
            if let Some(inline) = flag.strip_prefix(&format!("{name}=")) {
                return Some(inline.to_owned());
            }
            if flag == name {
                return args.next().cloned();
            }
        }
        None
    };
    let has = |name: &str| flags.iter().any(|flag| flag == name);

    let store = || {
        let db_path = session_path(config);
        if !db_path.exists() {
            return Err(format!("no database at {}", db_path.display()));
        }
        SqliteStore::open(&db_path)
            .map_err(|err| format!("failed to open {}: {err}", db_path.display()))
    };

    if has("--audit") {
        return purge_audit(
            &store()?,
            &value("--before"),
            has("--apply"),
            &value("--yes"),
        );
    }
    if has("--sessions") {
        return purge_idle_sessions(
            &store()?,
            &value("--before"),
            has("--apply"),
            &value("--yes"),
        );
    }
    purge_one_conversation(
        &store()?,
        config,
        &value("--conversation"),
        &value("--channel"),
        &value("--agent"),
        &value("--yes"),
    )
}

/// Irreversibly delete one conversation's session log.
fn purge_one_conversation(
    store: &SqliteStore,
    config: &ServiceConfig,
    conversation: &Option<String>,
    channel: &Option<String>,
    agent: &Option<String>,
    confirmed: &Option<String>,
) -> Result<(), String> {
    let conversation = conversation.clone().ok_or_else(|| {
        "--conversation ID is required (or --sessions --before DATE for idle conversations, \
         or --audit --before DATE for the audit stores)"
            .to_owned()
    })?;
    // No default, for the reason `migrate` has none: a session is keyed by
    // channel now, and guessing would delete a conversation the operator did
    // not name.
    let channel = channel.clone().ok_or_else(|| {
        "--channel NAME is required: a session is keyed by principal, so a conversation id \
         alone names one log per channel rather than one log"
            .to_owned()
    })?;
    let agent = match agent {
        Some(agent) => agent.clone(),
        None => super::configured_agent_id(config)?,
    };
    let confirmed = confirmed.clone().ok_or_else(|| {
        format!(
            "--yes {conversation} is required: this deletes the log irreversibly, and `/clear` \
             is what you want if you only need the model to start fresh"
        )
    })?;
    if confirmed != conversation {
        return Err(format!(
            "--yes named '{confirmed}' but --conversation named '{conversation}'"
        ));
    }

    let principal = Principal::conversation(
        AgentId::new(agent),
        ChannelId::new(channel.clone()),
        ConversationId::new(conversation.clone()),
    );
    let expected = store
        .session_log(&principal)
        .map_err(|err| format!("failed to survey '{conversation}': {err}"))?
        .len();
    let removed = store
        .purge_session(&principal, expected, "an operator")
        .map_err(|err| format!("failed to purge '{conversation}': {err}"))?;
    println!("purged {removed} item(s) from {channel} conversation '{conversation}'");
    Ok(())
}

/// Delete whole conversations that have been idle since before `--before`.
///
/// Whole conversations, never the old *items* of a live one. The session log
/// is append-only without qualification: trimming its head would leave a
/// conversation whose remaining history begins mid-sentence and whose
/// compaction summaries refer to items that are gone. A conversation nobody
/// has spoken in for months is a different thing, and removing all of it is
/// the same operation `--conversation` already performs, applied to a list.
fn purge_idle_sessions(
    store: &SqliteStore,
    before: &Option<String>,
    apply: bool,
    confirmed: &Option<String>,
) -> Result<(), String> {
    let before = before
        .clone()
        .ok_or_else(|| "--before YYYY-MM-DD is required with --sessions".to_owned())?;
    let cutoff = cutoff_from_date(&before)?;
    let idle = store
        .idle_conversations(cutoff)
        .map_err(|err| format!("failed to survey idle conversations: {err}"))?;

    if idle.is_empty() {
        println!("no conversation has been idle since before {before}; nothing to purge");
        return Ok(());
    }
    let items: usize = idle.iter().map(|(_, count)| count).sum();
    println!(
        "{} conversation(s) with {items} item(s) have no activity on or after {before}:",
        idle.len()
    );
    for (principal, count) in idle.iter().take(MAX_LISTED) {
        println!(
            "  {}:{} ({count} items, agent {})",
            principal.channel.as_str(),
            principal.conversation.as_str(),
            principal.agent.as_str()
        );
    }
    if idle.len() > MAX_LISTED {
        println!("  … and {} more", idle.len() - MAX_LISTED);
    }

    if !apply {
        println!(
            "\nnothing deleted. To apply: --sessions --before {before} --apply --yes {}",
            idle.len()
        );
        return Ok(());
    }
    let confirmed = confirmed.clone().ok_or_else(|| {
        format!(
            "--yes {} is required to apply: that is the conversation count above, and typing it \
             back is the confirmation",
            idle.len()
        )
    })?;
    if confirmed != idle.len().to_string() {
        return Err(format!(
            "--yes named '{confirmed}' but there are {} conversation(s) to purge; re-run without \
             --apply to see the current list",
            idle.len()
        ));
    }

    // One `purge_session` per conversation, and one safety event per
    // conversation. Not a single bulk `DELETE`: the storage layer has exactly
    // one path that removes session items and this must not become a second
    // one, and an operator reading the events later wants to see which
    // conversations went, not that some number of them did.
    let mut removed_total = 0;
    for (principal, expected) in &idle {
        let removed = store
            .purge_session(
                principal,
                *expected,
                &format!("an operator, idle before {before}"),
            )
            .map_err(|err| format!("failed to purge '{}': {err}", principal.conversation_name()))?;
        removed_total += removed;
    }
    println!(
        "\npurged {removed_total} item(s) from {} conversation(s)",
        idle.len()
    );
    Ok(())
}

/// Delete audit rows recorded before `--before`.
fn purge_audit(
    store: &SqliteStore,
    before: &Option<String>,
    apply: bool,
    confirmed: &Option<String>,
) -> Result<(), String> {
    let before = before
        .clone()
        .ok_or_else(|| "--before YYYY-MM-DD is required with --audit".to_owned())?;
    let cutoff = cutoff_from_date(&before)?;
    let counts = audit::count_before(store, cutoff)
        .map_err(|err| format!("failed to count audit rows: {err}"))?;

    if counts.is_empty() {
        println!("no audit row was recorded before {before}; nothing to purge");
        return Ok(());
    }
    println!("recorded before {before}:");
    println!("  safety_events:     {}", counts.safety_events);
    println!("  memory_access_log: {}", counts.memory_access_log);

    if !apply {
        println!(
            "\nnothing deleted. To apply: --audit --before {before} --apply --yes {}\n\
             \n\
             These are the record of what was authorized and what memory was read. Deleting \
             them is a deliberate act with no undo, and the deletion is itself recorded as an \
             `audit_purged` safety event.",
            counts.total()
        );
        return Ok(());
    }
    let confirmed = confirmed.clone().ok_or_else(|| {
        format!(
            "--yes {} is required to apply: that is the total row count above",
            counts.total()
        )
    })?;
    if confirmed != counts.total().to_string() {
        return Err(format!(
            "--yes named '{confirmed}' but there are {} row(s) to purge; re-run without --apply \
             to see the current counts",
            counts.total()
        ));
    }
    let purged = audit::purge_before(store, cutoff, counts, "an operator")
        .map_err(|err| format!("failed to purge audit rows: {err}"))?;
    println!(
        "\npurged {} safety event(s) and {} memory access row(s); the purge itself is recorded",
        purged.safety_events, purged.memory_access_log
    );
    Ok(())
}
