//! OpenAI provider, speaking the Responses API (`/v1/responses`) exclusively.
//!
//! Chat Completions is not used at all: reasoning models reject function tools
//! there outright, multimodal input and server-side session state are Responses
//! features, and maintaining two request shapes for one provider bought nothing
//! but drift. This module owns credentials, the endpoint, and the retry that
//! absorbs per-model parameter rejections; [`responses`] owns the wire mapping
//! and [`events`] the streamed event decoding.
//!
//! An OpenAI-*compatible* gateway that only implements `/v1/chat/completions`
//! is no longer addressable through this provider — point such a deployment at
//! the `deepseek` or `ollama` adapter, which are Chat-Completions-shaped.

use crate::providers::{first_env, post_json, post_sse, JsonHttpResponse, ProviderError};
use crate::CompletionStream;
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::Message;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::{OnceLock, RwLock};

mod events;
mod responses;

pub async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, ProviderError> {
    responses::complete(model, messages, tools).await
}

pub async fn complete_stream(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<CompletionStream, ProviderError> {
    responses::complete_stream(model, messages, tools).await
}

/// `(responses URL, request headers)` for one request.
type Endpoint = (String, Vec<(&'static str, String)>);

fn endpoint_and_headers() -> Result<Endpoint, ProviderError> {
    let api_key = env::var("OPENAI_API_KEY").map_err(|_| ProviderError::MissingCredentials {
        variable: "OPENAI_API_KEY",
    })?;
    let base_url = env::var("AGENTOS_OPENAI_BASE_URL")
        .or_else(|_| env::var("OPENAI_BASE_URL"))
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_owned());
    let mut headers = vec![
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_owned()),
    ];
    if let Some(organization) = first_env(["OPENAI_ORGANIZATION", "OPENAI_ORG_ID"]) {
        headers.push(("OpenAI-Organization", organization));
    }
    if let Some(project) = first_env(["OPENAI_PROJECT", "OPENAI_PROJECT_ID"]) {
        headers.push(("OpenAI-Project", project));
    }
    Ok((
        format!("{}/responses", base_url.trim_end_matches('/')),
        headers,
    ))
}

/// The API error object in a response body, if any. Responses sets the field to
/// `null` on success, so a bare `get` isn't enough.
fn api_error(body: &Value) -> Option<&Value> {
    body.get("error").filter(|error| !error.is_null())
}

/// A request parameter a model refuses to accept. The fix is always the same —
/// drop it and resend — so the variant only has to name the parameter.
///
/// Every such rejection is an HTTP 400 carrying no completion: in the buffered
/// body, or from `post_sse` *before* any stream byte flows, never mid-stream.
/// The corrected payload is therefore always safe to resend.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RejectedParam {
    /// Reasoning models accept only their default temperature ("Unsupported
    /// value: 'temperature' does not support 0.7 with this model").
    Temperature,
    /// Some models reject the flag outright rather than ignoring it. Dropping
    /// it only re-permits parallel calls, which costs a few output tokens: the
    /// orchestrators act on `tool_calls.first()` and record just that one call
    /// in the transcript, so extra calls change no behaviour.
    ParallelToolCalls,
}

impl RejectedParam {
    fn name(self) -> &'static str {
        match self {
            Self::Temperature => "temperature",
            Self::ParallelToolCalls => "parallel_tool_calls",
        }
    }

    /// Classify a rejection. Matches both the buffered error object's `message`
    /// and the streaming [`ProviderError`]'s rendered detail, which embeds the
    /// same text; `None` means the error is not a rejected parameter and the
    /// request must fail.
    fn from_error_text(message: &str) -> Option<Self> {
        let message = message.to_ascii_lowercase();
        let rejected = message.contains("unsupported")
            || message.contains("not supported")
            || message.contains("does not support")
            || message.contains("unrecognized");
        if !rejected {
            return None;
        }
        [Self::Temperature, Self::ParallelToolCalls]
            .into_iter()
            .find(|param| message.contains(param.name()))
    }

    fn drop_from(self, payload: &mut Value) {
        if let Some(payload) = payload.as_object_mut() {
            payload.remove(self.name());
        }
    }
}

/// How many rejected parameters one request may absorb: every parameter this
/// client sends that a model could refuse. The API reports them one at a time,
/// so the retry loop runs until the payload is accepted or exhausts the list.
const MAX_PARAM_FIXES: usize = 2;

/// Parameters already refused, keyed by `"<url> <model>"`, so a long-lived
/// gateway pays the rejected round trip once instead of once per turn. The
/// endpoint is part of the key because a deployment behind a different base URL
/// may serve the same model name with different capabilities.
fn refused_params() -> &'static RwLock<BTreeMap<String, BTreeSet<RejectedParam>>> {
    static REFUSED: OnceLock<RwLock<BTreeMap<String, BTreeSet<RejectedParam>>>> = OnceLock::new();
    REFUSED.get_or_init(RwLock::default)
}

fn remember_refusal(request: &str, param: RejectedParam) {
    if let Ok(mut refused) = refused_params().write() {
        refused.entry(request.to_owned()).or_default().insert(param);
    }
}

fn drop_refused_params(request: &str, payload: &mut Value) {
    let Ok(refused) = refused_params().read() else {
        return;
    };
    let Some(params) = refused.get(request) else {
        return;
    };
    for param in params {
        param.drop_from(payload);
    }
}

/// POST a buffered request, absorbing rejected-parameter 400s by dropping the
/// parameter and resending.
async fn post_with_param_fixes(
    model: &str,
    url: &str,
    headers: &[(&str, String)],
    payload: &mut Value,
) -> Result<JsonHttpResponse, ProviderError> {
    let request = format!("{url} {model}");
    drop_refused_params(&request, payload);
    let mut response = post_json("llm", url, headers, payload).await?;
    for _ in 0..MAX_PARAM_FIXES {
        let Some(param) = api_error(&response.body)
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .and_then(RejectedParam::from_error_text)
        else {
            break;
        };
        param.drop_from(payload);
        remember_refusal(&request, param);
        response = post_json("llm", url, headers, payload).await?;
    }
    Ok(response)
}

/// Open an SSE request, absorbing the same rejected-parameter 400s as
/// [`post_with_param_fixes`]. Safe because those rejections are raised before
/// the stream body starts, so nothing has been emitted to the caller yet.
async fn post_sse_with_param_fixes(
    model: &str,
    url: &str,
    headers: &[(&str, String)],
    payload: &mut Value,
) -> Result<reqwest::Response, ProviderError> {
    let request = format!("{url} {model}");
    drop_refused_params(&request, payload);
    let mut fixes = 0;
    loop {
        let err = match post_sse("openai", url, headers, payload).await {
            Ok(response) => return Ok(response),
            Err(err) => err,
        };
        let param =
            RejectedParam::from_error_text(&err.to_string()).filter(|_| fixes < MAX_PARAM_FIXES);
        let Some(param) = param else {
            return Err(err);
        };
        param.drop_from(payload);
        remember_refusal(&request, param);
        fixes += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_every_temperature_rejection_phrasing() {
        for message in [
            "Unsupported value: 'temperature' does not support 0.7 with this model. \
             Only the default (1) value is supported.",
            "Unsupported parameter: 'temperature' is not supported with this model.",
            "Unrecognized request argument supplied: temperature",
        ] {
            assert_eq!(
                RejectedParam::from_error_text(message),
                Some(RejectedParam::Temperature),
                "{message}"
            );
        }
    }

    #[test]
    fn classifies_parallel_tool_calls_rejection() {
        assert_eq!(
            RejectedParam::from_error_text(
                "Unsupported parameter: 'parallel_tool_calls' is not supported with this model."
            ),
            Some(RejectedParam::ParallelToolCalls)
        );
    }

    #[test]
    fn classifies_rejection_in_streaming_error_detail() {
        // The streaming path matches on the rendered ProviderError detail, which
        // embeds the error object's text plus an http_status suffix.
        let detail = "openai streaming error: {\"message\":\"Unsupported value: \
            'temperature' does not support 0.7 with this model.\",\
            \"param\":\"temperature\"}; http_status=400";
        assert_eq!(
            RejectedParam::from_error_text(detail),
            Some(RejectedParam::Temperature)
        );
    }

    #[test]
    fn leaves_unrelated_errors_unclassified() {
        for message in [
            "Incorrect API key provided",
            "openai streaming error: rate limited; http_status=429",
            // Names a parameter, but is not a rejection of it.
            "the temperature sensor tool returned no reading",
        ] {
            assert_eq!(RejectedParam::from_error_text(message), None, "{message}");
        }
    }

    #[test]
    fn dropping_a_param_leaves_the_rest_of_the_payload_intact() {
        let mut payload = json!({ "model": "m", "temperature": 0.7, "parallel_tool_calls": false });
        RejectedParam::Temperature.drop_from(&mut payload);
        assert_eq!(
            payload,
            json!({ "model": "m", "parallel_tool_calls": false })
        );
        RejectedParam::ParallelToolCalls.drop_from(&mut payload);
        assert_eq!(payload, json!({ "model": "m" }));
    }

    #[test]
    fn refusals_are_scoped_to_one_endpoint_and_model() {
        // Keys are per-test so the process-wide table can't leak between tests.
        let learned = "https://example.test/responses refusal-test-model";
        let other = "https://other.test/responses refusal-test-model";
        remember_refusal(learned, RejectedParam::Temperature);

        let mut replayed = json!({ "temperature": 0.7 });
        drop_refused_params(learned, &mut replayed);
        assert_eq!(replayed, json!({}));

        // A different deployment may serve the same model name with different
        // capabilities, so nothing is assumed for it until it says so.
        let mut untouched = json!({ "temperature": 0.7 });
        drop_refused_params(other, &mut untouched);
        assert_eq!(untouched, json!({ "temperature": 0.7 }));
    }
}
