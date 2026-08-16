//! Recording what each assembled provider request was made of.
//!
//! Roadmap item P3 in `docs/TRANSFER_ROADMAP.md`. P1 made `prompt::assemble`
//! the single authority over a request but left it unreconstructable: the
//! manifest never left the orchestrator, so "what did the model see on turn 3"
//! could only be answered by re-reading the code.
//!
//! An orchestrator pushes one [`RequestHeader`] per LLM round-trip onto
//! `RunContext::request_sink`; the loop drains it after `plan()` returns and
//! records each as a `request_header` trace event. The header names its sources
//! rather than copying them, so a trace file never carries user memory bodies
//! ([`ARCHITECTURE.md` §14](../../../../docs/ARCHITECTURE.md)) and the
//! reconstruction standard is "log + code": the trace names the skills and
//! memory records, the workspace and memory store hold their content, and
//! `RunState` holds the transcript.

use super::telemetry::field_key;
use crate::hooks::Hooks;
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{RequestHeader, RequestSection, RequestSource, SpanId};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tracing::info;

/// Record one assembled request as a `request_header` trace event.
///
/// Sections are one structured array field rather than a key per section:
/// trace keys are interned `&'static str` on the loop hot path, and a section
/// vocabulary that grows would otherwise allocate a fresh key per event.
pub(super) fn record_request_header(
    state: &mut RunState,
    hooks: Option<&Hooks>,
    span_id: SpanId,
    header: RequestHeader,
) {
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("total_messages"),
        Value::from(header.total_messages),
    );
    fields.insert(field_key("total_chars"), Value::from(header.total_chars));
    fields.insert(
        field_key("prompt_estimated_tokens"),
        Value::from(header.total_tokens),
    );
    fields.insert(field_key("tool_tokens"), Value::from(header.tool_tokens));
    if let Some(budget) = header.context_budget_tokens {
        fields.insert(field_key("context_budget_tokens"), Value::from(budget));
        // Integer percent, so a trace reader can threshold on it without
        // float comparison. Saturates rather than wrapping on an oversized
        // request, which is exactly when the number matters most.
        let percent = header
            .total_tokens
            .saturating_mul(100)
            .checked_div(budget)
            .unwrap_or(0);
        fields.insert(field_key("pressure_percent"), Value::from(percent));
    }
    // Absent rather than zero on the overwhelming majority of requests, so a
    // trace search for elision returns only the requests that had some.
    if header.elided_messages > 0 {
        fields.insert(
            field_key("elided_messages"),
            Value::from(header.elided_messages),
        );
        fields.insert(field_key("elided_chars"), Value::from(header.elided_chars));
    }
    fields.insert(
        field_key("sections"),
        Value::Array(header.sections.iter().map(section_value).collect()),
    );
    trace::record_event(state, hooks, span_id, "request_header", fields);

    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        sections = header.sections.len(),
        total_messages = header.total_messages,
        total_chars = header.total_chars,
        prompt_estimated_tokens = header.total_tokens,
        context_budget_tokens = header.context_budget_tokens,
        "request_header"
    );
}

fn section_value(section: &RequestSection) -> Value {
    let mut value = json!({
        "id": section.id.as_ref(),
        "messages": section.messages,
        "chars": section.chars,
        "tokens": section.tokens,
    });
    if !section.sources.is_empty() {
        value["sources"] = Value::Array(section.sources.iter().map(source_value).collect());
    }
    value
}

/// Render one source as the value that identifies it. Memory keeps namespace
/// and record id — enough to re-read the record, nothing of its body.
fn source_value(source: &RequestSource) -> Value {
    match source {
        RequestSource::Skill(name) => Value::String(name.as_ref().to_owned()),
        RequestSource::Memory {
            namespace,
            record_id,
        } => match record_id {
            Some(id) => Value::String(format!("{}/{id}", namespace.as_str())),
            None => Value::String(namespace.as_str().to_owned()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, Namespace, RunId};
    use std::sync::Arc;

    fn header() -> RequestHeader {
        RequestHeader {
            sections: vec![
                RequestSection {
                    id: Arc::from("skill_prelude"),
                    messages: 1,
                    chars: 40,
                    tokens: 14,
                    sources: vec![RequestSource::Skill(Arc::from("deploy-notes"))],
                },
                RequestSection {
                    id: Arc::from("memory"),
                    messages: 1,
                    chars: 20,
                    tokens: 9,
                    sources: vec![RequestSource::Memory {
                        namespace: Namespace::new("private/conversation/c/semantic/general"),
                        record_id: Some(Arc::from("rec-1")),
                    }],
                },
                RequestSection {
                    id: Arc::from("transcript"),
                    messages: 3,
                    chars: 60,
                    tokens: 27,
                    sources: Vec::new(),
                },
            ],
            total_messages: 5,
            total_chars: 120,
            total_tokens: 60,
            tool_tokens: 10,
            context_budget_tokens: Some(1_000),
            elided_messages: 0,
            elided_chars: 0,
        }
    }

    fn traced_sections() -> Vec<Value> {
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        record_request_header(&mut state, None, SpanId::new("span-1"), header());
        let event = state
            .trace_events
            .iter()
            .find(|event| event.name.as_ref() == "request_header")
            .expect("the header is traced");
        assert_eq!(
            event.fields.get(&field_key("total_messages")),
            Some(&Value::from(5usize))
        );
        event
            .fields
            .get(&field_key("sections"))
            .and_then(Value::as_array)
            .cloned()
            .expect("sections is an array field")
    }

    #[test]
    fn sources_name_where_content_lives() {
        let sections = traced_sections();
        assert_eq!(sections[0]["sources"], json!(["deploy-notes"]));
        assert_eq!(
            sections[1]["sources"],
            json!(["private/conversation/c/semantic/general/rec-1"])
        );
    }

    #[test]
    fn the_transcript_names_no_sources() {
        // The transcript is run state the trace already carries; repeating it
        // as a source would be noise.
        let sections = traced_sections();
        assert_eq!(sections[2]["id"], json!("transcript"));
        assert!(sections[2].get("sources").is_none());
    }

    #[test]
    fn pressure_is_traced_as_an_integer_percent() {
        // C1's exit condition: every request traces its estimate and the
        // window it is measured against, so pressure is observable before
        // compaction exists to relieve it.
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        record_request_header(&mut state, None, SpanId::new("span-1"), header());
        let fields = &state
            .trace_events
            .iter()
            .find(|event| event.name.as_ref() == "request_header")
            .expect("the header is traced")
            .fields;

        assert_eq!(
            fields.get(&field_key("prompt_estimated_tokens")),
            Some(&Value::from(60usize))
        );
        assert_eq!(
            fields.get(&field_key("context_budget_tokens")),
            Some(&Value::from(1_000usize))
        );
        assert_eq!(
            fields.get(&field_key("pressure_percent")),
            Some(&Value::from(6usize))
        );
    }

    #[test]
    fn an_unknown_budget_traces_no_pressure_rather_than_a_guess() {
        let mut state = RunState::new(RunId::new("r"), AgentId::new("a"));
        let mut unknown = header();
        unknown.context_budget_tokens = None;
        record_request_header(&mut state, None, SpanId::new("span-1"), unknown);
        let fields = &state
            .trace_events
            .iter()
            .find(|event| event.name.as_ref() == "request_header")
            .expect("the header is traced")
            .fields;

        // The estimate is still useful on its own; a pressure figure against
        // an invented window would not be.
        assert!(fields.contains_key(&field_key("prompt_estimated_tokens")));
        assert!(!fields.contains_key(&field_key("pressure_percent")));
        assert!(!fields.contains_key(&field_key("context_budget_tokens")));
    }

    #[test]
    fn no_memory_body_reaches_the_trace() {
        // ARCHITECTURE.md §14: memory bodies stay out of traces. The header
        // records the record's address, never its content.
        let rendered = serde_json::to_string(&traced_sections()).expect("sections serialize");
        assert!(rendered.contains("rec-1"));
        assert!(!rendered.contains("fact"));
    }
}
