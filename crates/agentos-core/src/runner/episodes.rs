use super::{task_id_for_state, RunnerDeps};
use crate::memory::{EpisodeOutcome, EpisodeRecord};
use crate::r#loop::RunError;
use agentos_interfaces::RunState;
use agentos_proto::{AgentId, ConversationId, Envelope, MessageRole, RunId, SpanKind, TaskId};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tracing::warn;

#[derive(Clone, Debug)]
pub(super) struct EpisodeSeed {
    run_id: RunId,
    task_id: TaskId,
    active_agent: AgentId,
    conversation_id: ConversationId,
    user_id: Option<Arc<str>>,
    user_content: Arc<str>,
}

impl EpisodeSeed {
    pub(super) fn from_input(
        input: &Envelope,
        run_id: &RunId,
        active_agent: &AgentId,
        task_id: TaskId,
    ) -> Self {
        Self {
            run_id: run_id.clone(),
            task_id,
            active_agent: active_agent.clone(),
            conversation_id: input.conversation_id.clone(),
            user_id: Some(Arc::clone(&input.sender)),
            user_content: Arc::clone(&input.message.content),
        }
    }

    pub(super) fn from_state(state: &RunState, conversation_id: &ConversationId) -> Self {
        Self {
            run_id: state.run_id.clone(),
            task_id: state
                .task_id
                .clone()
                .unwrap_or_else(|| task_id_for_state(state)),
            active_agent: state.active_agent.clone(),
            conversation_id: conversation_id.clone(),
            user_id: user_id_from_state(state),
            user_content: state
                .transcript
                .items
                .iter()
                .find(|item| item.message.role == MessageRole::User)
                .map(|item| Arc::clone(&item.message.content))
                .unwrap_or_else(|| Arc::from("")),
        }
    }
}

pub(super) async fn record_finished_episode(
    state: &RunState,
    conversation_id: &ConversationId,
    deps: &RunnerDeps<'_>,
) -> Option<BTreeMap<Arc<str>, Value>> {
    let manager = deps.memory_manager?;
    let episode = episode_from_state(state, conversation_id, EpisodeOutcome::Succeeded);
    match manager.record_episode(episode).await {
        Ok(Some(record_id)) => {
            let mut metadata = BTreeMap::new();
            metadata.insert(Arc::from("episode_recorded"), Value::Bool(true));
            metadata.insert(
                Arc::from("episode_record_id"),
                Value::String(record_id.as_str().to_owned()),
            );
            Some(metadata)
        }
        Ok(None) => {
            let mut metadata = BTreeMap::new();
            metadata.insert(Arc::from("episode_recorded"), Value::Bool(false));
            metadata.insert(
                Arc::from("episode_skip_reason"),
                Value::String("trivial_run".to_owned()),
            );
            Some(metadata)
        }
        Err(err) => {
            warn!(
                run_id = state.run_id.as_str(),
                active_agent = state.active_agent.as_str(),
                error = %err,
                "episode_record_failed"
            );
            let mut metadata = BTreeMap::new();
            metadata.insert(Arc::from("episode_recorded"), Value::Bool(false));
            metadata.insert(Arc::from("episode_error"), Value::String(err.to_string()));
            Some(metadata)
        }
    }
}

pub(super) async fn record_denied_episode(
    state: &RunState,
    conversation_id: &ConversationId,
    reason: &Arc<str>,
    deps: &RunnerDeps<'_>,
) {
    let Some(manager) = deps.memory_manager else {
        return;
    };
    let mut episode = episode_from_state(state, conversation_id, EpisodeOutcome::Denied);
    episode.summary = Arc::from(truncate_summary(
        &format!("Denied approval: {reason}\n{}", episode.summary),
        480,
    ));
    episode.metadata.insert(
        Arc::from("denial_reason"),
        Value::String(reason.to_string()),
    );
    if let Err(err) = manager.record_episode(episode).await {
        warn!(
            run_id = state.run_id.as_str(),
            active_agent = state.active_agent.as_str(),
            error = %err,
            "episode_record_failed"
        );
    }
}

pub(super) async fn record_error_episode(
    seed: &EpisodeSeed,
    err: &RunError,
    deps: &RunnerDeps<'_>,
) {
    let Some(manager) = deps.memory_manager else {
        return;
    };
    let outcome = match err {
        RunError::ApprovalDenied { .. } => EpisodeOutcome::Denied,
        RunError::NotResumable
        | RunError::AlreadyDone
        | RunError::MaxTurnsExceeded
        | RunError::ApprovalUnsupported { .. }
        | RunError::Tool(_)
        | RunError::Orchestrator(_)
        | RunError::Guardrail(_)
        | RunError::GuardrailTripped { .. }
        | RunError::TaskWorkspace(_)
        | RunError::SubAgent(_)
        // Unreachable by construction: `act` turns cancellation into a
        // terminal stop notice, so it never propagates this far. Classified
        // rather than `unreachable!()` because an episode record is not worth
        // a panic if a future path ever does surface it.
        | RunError::Cancelled => EpisodeOutcome::Failed,
    };
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("error"), Value::String(err.to_string()));
    let episode = EpisodeRecord {
        run_id: seed.run_id.clone(),
        task_id: seed.task_id.clone(),
        active_agent: seed.active_agent.clone(),
        conversation_id: seed.conversation_id.clone(),
        user_id: seed.user_id.clone(),
        outcome,
        tools_used: Vec::new(),
        subagents_used: Vec::new(),
        summary: Arc::from(truncate_summary(
            &format!(
                "Run ended with {outcome:?}: {err}\nUser: {}",
                seed.user_content
            ),
            480,
        )),
        turn_count: 0,
        metadata,
    };
    if let Err(record_err) = manager.record_episode(episode).await {
        warn!(
            run_id = seed.run_id.as_str(),
            active_agent = seed.active_agent.as_str(),
            error = %record_err,
            "episode_record_failed"
        );
    }
}

fn episode_from_state(
    state: &RunState,
    conversation_id: &ConversationId,
    outcome: EpisodeOutcome,
) -> EpisodeRecord {
    let tools_used = tools_used(state);
    let subagents_used = subagents_used(state);
    let explicit_memory_write = has_explicit_memory_write(state);
    let explicit_user_preference = has_explicit_user_preference(state);
    let turn_count = count_trace_spans(state, SpanKind::State, "plan");
    let mut metadata = BTreeMap::new();
    metadata.insert(
        Arc::from("explicit_memory_write"),
        Value::Bool(explicit_memory_write),
    );
    metadata.insert(
        Arc::from("explicit_user_preference"),
        Value::Bool(explicit_user_preference),
    );
    metadata.insert(
        Arc::from("approval_recorded"),
        Value::Bool(had_approval(state)),
    );

    EpisodeRecord {
        run_id: state.run_id.clone(),
        task_id: state
            .task_id
            .clone()
            .unwrap_or_else(|| task_id_for_state(state)),
        active_agent: state.active_agent.clone(),
        conversation_id: conversation_id.clone(),
        user_id: user_id_from_state(state),
        outcome,
        tools_used,
        subagents_used,
        summary: Arc::from(compact_episode_summary(state)),
        turn_count,
        metadata,
    }
}

fn tools_used(state: &RunState) -> Vec<Arc<str>> {
    let mut tools = BTreeSet::new();
    for span in &state.trace_spans {
        if span.kind != SpanKind::Tool {
            continue;
        }
        if let Some(tool_name) = span.fields.get("tool_name").and_then(Value::as_str) {
            tools.insert(Arc::from(tool_name));
        } else if let Some(name) = span.name.as_ref().strip_prefix("tool.") {
            tools.insert(Arc::from(name));
        }
    }
    tools.into_iter().collect()
}

fn subagents_used(state: &RunState) -> Vec<AgentId> {
    let mut subagents = BTreeSet::new();
    for event in &state.trace_events {
        if let Some(subagent_id) = event.fields.get("subagent_id").and_then(Value::as_str) {
            subagents.insert(subagent_id.to_owned());
        }
    }
    state
        .transcript
        .items
        .iter()
        .filter_map(|item| item.message.metadata.get("subagent_id"))
        .filter_map(Value::as_str)
        .for_each(|subagent_id| {
            subagents.insert(subagent_id.to_owned());
        });
    subagents.into_iter().map(AgentId::new).collect()
}

fn has_explicit_memory_write(state: &RunState) -> bool {
    state.transcript.items.iter().any(|item| {
        item.message.role == MessageRole::Tool
            && item
                .message
                .metadata
                .get("operation")
                .and_then(Value::as_str)
                == Some("write")
            && item.message.metadata.contains_key("namespace")
    })
}

fn has_explicit_user_preference(state: &RunState) -> bool {
    state.transcript.items.iter().any(|item| {
        if item.message.role != MessageRole::User {
            return false;
        }
        let content = item.message.content.to_ascii_lowercase();
        content.contains("prefer")
            || content.contains("preference")
            || content.contains("correction")
            || content.contains("actually")
            || content.contains("instead")
            || content.contains("remember:")
    })
}

fn had_approval(state: &RunState) -> bool {
    state.trace_events.iter().any(|event| {
        event.name.as_ref().contains("approval") || event.name.as_ref().contains("paused")
    }) || state.transcript.items.iter().any(|item| {
        item.message.role == MessageRole::Tool
            && item.message.metadata.contains_key("tool_call_id")
            && count_trace_spans(state, SpanKind::State, "plan") > 1
    })
}

fn user_id_from_state(state: &RunState) -> Option<Arc<str>> {
    state
        .transcript
        .items
        .iter()
        .find(|item| item.message.role == MessageRole::User)
        .and_then(|item| item.metadata.get("sender"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|sender| !sender.is_empty())
        .map(Arc::from)
}

fn compact_episode_summary(state: &RunState) -> String {
    let user = state
        .transcript
        .items
        .iter()
        .find(|item| item.message.role == MessageRole::User)
        .map(|item| item.message.content.as_ref())
        .unwrap_or("");
    let assistant = state
        .transcript
        .items
        .iter()
        .rev()
        .find(|item| item.message.role == MessageRole::Assistant)
        .map(|item| item.message.content.as_ref())
        .unwrap_or("");
    truncate_summary(&format!("User: {user}\nAssistant: {assistant}"), 480)
}

fn truncate_summary(input: &str, max_chars: usize) -> String {
    let mut chars = input.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn count_trace_spans(state: &RunState, kind: SpanKind, name: &str) -> usize {
    state
        .trace_spans
        .iter()
        .filter(|span| span.kind == kind && span.name.as_ref() == name)
        .count()
}
