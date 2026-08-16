pub mod anthropic;
pub(crate) mod content;
pub mod deepseek;
pub mod ollama;
pub mod openai;
pub(crate) mod stream;

use crate::LlmError;
use agentos_proto::{Message, TOKEN_USAGE_METADATA_KEY};
use bytes::Bytes;
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

#[derive(Debug)]
pub(crate) struct JsonHttpResponse {
    pub status: Option<u16>,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
}

/// Structured error for the provider adapter layer (roadmap Phase 4.3).
///
/// Surfaced through the `Llm` trait boundary as `LlmError::Provider` text;
/// the variants exist so provider plumbing follows the project's typed-error
/// convention and failure classes stay matchable without parsing strings.
/// `detail` fields carry the full human-readable diagnostics (HTTP metadata,
/// request ids, hints) that operators already rely on in logs.
#[derive(Debug, Error)]
pub enum ProviderError {
    /// A required credential environment variable is unset.
    #[error("missing {variable}")]
    MissingCredentials { variable: &'static str },
    /// The request payload could not be serialized.
    #[error("failed to encode LLM request: {source}")]
    Encode {
        #[source]
        source: serde_json::Error,
    },
    /// A configured header name or value was invalid.
    #[error("{detail}")]
    InvalidHeader { detail: String },
    /// The HTTP exchange failed before a JSON body was decoded. Always
    /// retried by `post_json`'s backoff loop.
    #[error("LLM request failed: {detail}")]
    Transport { detail: String },
    /// The response was not the JSON shape the provider promises.
    #[error("{detail}")]
    MalformedResponse { detail: String },
    /// The provider returned an explicit API error object.
    #[error("{detail}")]
    Api {
        provider: Arc<str>,
        status: Option<u16>,
        detail: String,
    },
    /// The provider rejected the request because it was too long for the
    /// model's context window.
    ///
    /// Split out of [`Self::Api`] because it is the one provider error a run
    /// can *act* on: roadmap item C4 compacts the conversation and retries,
    /// rather than surfacing a failure the operator has to fix. It renders
    /// identically to `Api`, so the streaming paths that match on a rendered
    /// detail are unaffected.
    #[error("{detail}")]
    ContextLength {
        provider: Arc<str>,
        status: Option<u16>,
        detail: String,
    },
    /// Configuration names a provider this build does not support.
    #[error("unknown LLM provider: {0}")]
    UnknownProvider(Arc<str>),
}

impl ProviderError {
    /// Project into the error the `Llm` trait boundary exposes.
    ///
    /// Every class but one collapses into [`LlmError::Provider`] text, as it
    /// always has. Context-length rejection stays typed because the run loop
    /// branches on it — matching a substring of a provider's prose there is
    /// exactly what this crate's typed-error convention exists to avoid.
    pub fn into_llm_error(self) -> LlmError {
        match self {
            Self::ContextLength { detail, .. } => {
                LlmError::ContextLengthExceeded(Arc::from(detail))
            }
            other => LlmError::Provider(Arc::from(other.to_string())),
        }
    }
}

/// Tokens consumed by a single LLM call, normalised across providers.
///
/// `input_tokens` is the full prompt-side token count (including any portion
/// served from cache), so it stays comparable across providers. The cache
/// breakdown always satisfies `cache_read + cache_write + cache_miss ==
/// input_tokens`:
/// - `cache_read_tokens`  — prompt tokens served from a prompt cache (hits;
///   billed at a discount by OpenAI/DeepSeek/Anthropic).
/// - `cache_write_tokens` — prompt tokens written into the cache this call
///   (Anthropic `cache_creation_input_tokens`; 0 for providers that cache
///   transparently).
/// - `cache_miss_tokens`  — prompt tokens processed without any cache benefit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub cache_miss_tokens: u64,
}

impl TokenUsage {
    /// Extract and normalise token counts from a provider response body.
    /// Handles the three shapes AgentOS talks to:
    /// - OpenAI: `usage.{prompt_tokens, completion_tokens, total_tokens}` with
    ///   `usage.prompt_tokens_details.cached_tokens` for cache hits.
    /// - DeepSeek: as OpenAI, plus explicit
    ///   `usage.{prompt_cache_hit_tokens, prompt_cache_miss_tokens}`.
    /// - OpenAI Responses: `usage.{input_tokens, output_tokens, total_tokens}`
    ///   with `usage.input_tokens_details.cached_tokens` for cache hits, where
    ///   `input_tokens` *includes* the cached portion.
    /// - Anthropic: `usage.{input_tokens, output_tokens,
    ///   cache_read_input_tokens, cache_creation_input_tokens}` where
    ///   `input_tokens` excludes cached/created tokens.
    /// - Ollama: top-level `prompt_eval_count` / `eval_count` (no caching).
    ///
    /// Returns `None` when the response carries no usage information at all.
    pub fn from_response_body(body: &Value) -> Option<Self> {
        let u = |v: &Value, key: &str| v.get(key).and_then(Value::as_u64);

        if let Some(usage) = body.get("usage") {
            // OpenAI Responses: `input_tokens` already covers the whole prompt,
            // cache hits included, so the hit count is subtracted out rather
            // than added on the way to `input_tokens`.
            if let Some(cached) = usage
                .get("input_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_u64)
            {
                let input = u(usage, "input_tokens").unwrap_or(0);
                let output = u(usage, "output_tokens").unwrap_or(0);
                let cache_read = cached.min(input);
                return Some(Self {
                    input_tokens: input,
                    output_tokens: output,
                    total_tokens: u(usage, "total_tokens").unwrap_or(input + output),
                    cache_read_tokens: cache_read,
                    cache_write_tokens: 0,
                    cache_miss_tokens: input - cache_read,
                });
            }

            // Anthropic: no `prompt_tokens`; `input_tokens` is the uncached
            // remainder, with cache read/creation reported separately.
            if usage.get("prompt_tokens").is_none()
                && (usage.get("input_tokens").is_some() || usage.get("output_tokens").is_some())
            {
                let miss = u(usage, "input_tokens").unwrap_or(0);
                let cache_read = u(usage, "cache_read_input_tokens").unwrap_or(0);
                let cache_write = u(usage, "cache_creation_input_tokens").unwrap_or(0);
                let output = u(usage, "output_tokens").unwrap_or(0);
                let input = miss + cache_read + cache_write;
                return Some(Self {
                    input_tokens: input,
                    output_tokens: output,
                    total_tokens: input + output,
                    cache_read_tokens: cache_read,
                    cache_write_tokens: cache_write,
                    cache_miss_tokens: miss,
                });
            }

            // OpenAI / DeepSeek shared shape.
            let prompt = u(usage, "prompt_tokens");
            let completion = u(usage, "completion_tokens");
            let total = u(usage, "total_tokens");
            if prompt.is_none() && completion.is_none() && total.is_none() {
                return None;
            }
            let input = prompt.unwrap_or(0);
            let output = completion.unwrap_or(0);
            // DeepSeek reports explicit hit/miss; OpenAI nests cached_tokens.
            let cache_read = u(usage, "prompt_cache_hit_tokens").or_else(|| {
                usage
                    .get("prompt_tokens_details")
                    .and_then(|d| d.get("cached_tokens"))
                    .and_then(Value::as_u64)
            });
            let cache_miss = u(usage, "prompt_cache_miss_tokens");
            let cache_read =
                cache_read.unwrap_or_else(|| input.saturating_sub(cache_miss.unwrap_or(input)));
            let cache_miss = cache_miss.unwrap_or_else(|| input.saturating_sub(cache_read));
            return Some(Self {
                input_tokens: input,
                output_tokens: output,
                total_tokens: total.unwrap_or(input + output),
                cache_read_tokens: cache_read,
                cache_write_tokens: 0,
                cache_miss_tokens: cache_miss,
            });
        }

        // Ollama reports counts at the top level and has no prompt cache.
        let prompt = body.get("prompt_eval_count").and_then(Value::as_u64);
        let completion = body.get("eval_count").and_then(Value::as_u64);
        if prompt.is_none() && completion.is_none() {
            return None;
        }
        let input = prompt.unwrap_or(0);
        let output = completion.unwrap_or(0);
        Some(Self {
            input_tokens: input,
            output_tokens: output,
            total_tokens: input + output,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            cache_miss_tokens: input,
        })
    }
}

/// Emit a structured tracing event recording the tokens consumed by one LLM
/// call. Logged under the dedicated `agentos_llm::usage` target so operators
/// can filter or aggregate token spend per provider+model without parsing
/// free-form log lines (e.g. `RUST_LOG=agentos_llm::usage=info`).
pub(crate) fn log_token_usage(provider: &str, model: &str, body: &Value) -> Option<TokenUsage> {
    let usage = TokenUsage::from_response_body(body);
    match usage {
        Some(usage) => tracing::info!(
            target: "agentos_llm::usage",
            provider,
            model,
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            total_tokens = usage.total_tokens,
            cache_read_tokens = usage.cache_read_tokens,
            cache_write_tokens = usage.cache_write_tokens,
            cache_miss_tokens = usage.cache_miss_tokens,
            "llm token usage"
        ),
        None => tracing::debug!(
            target: "agentos_llm::usage",
            provider,
            model,
            "llm call returned no token usage metadata"
        ),
    }
    usage
}

/// Record the call's token usage on the assistant message so the run loop can
/// fold it into `RunState.usage`. Keyed by [`TOKEN_USAGE_METADATA_KEY`]; the
/// JSON shape matches `agentos_proto::Usage`'s token fields. A serialization
/// failure here must never fail the LLM call, so it degrades to a trace-only
/// record (the `agentos_llm::usage` event above already fired).
pub(crate) fn attach_token_usage(message: &mut Message, usage: TokenUsage) {
    match serde_json::to_value(usage) {
        Ok(value) => {
            message
                .metadata
                .insert(Arc::from(TOKEN_USAGE_METADATA_KEY), value);
        }
        Err(err) => tracing::debug!(
            target: "agentos_llm::usage",
            error = %err,
            "failed to serialize token usage into message metadata"
        ),
    }
}

/// Build tool-call args from a provider field that encodes them as a JSON
/// *string* (OpenAI/DeepSeek `function.arguments`). Validates the untrusted
/// string and allocates exactly once, preserving the provider's bytes.
pub(crate) fn raw_args_from_json_str(
    args: Option<&Value>,
) -> Option<Box<serde_json::value::RawValue>> {
    let args_str = args.and_then(Value::as_str).unwrap_or("{}");
    serde_json::value::RawValue::from_string(args_str.to_owned()).ok()
}

/// Build tool-call args from a provider field that carries them as a JSON
/// *object* (Anthropic `input`, Ollama `function.arguments`). Serializes the
/// borrowed value straight into a `RawValue` — no intermediate clone and no
/// re-parse. Object keys serialize in sorted order.
pub(crate) fn raw_args_from_json_value(
    args: Option<&Value>,
) -> Option<Box<serde_json::value::RawValue>> {
    match args {
        Some(value) => serde_json::value::to_raw_value(value).ok(),
        None => serde_json::value::RawValue::from_string("{}".to_owned()).ok(),
    }
}

const MAX_ATTEMPTS: u32 = 5;
const BASE_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(8);
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);

/// Total request budget — reqwest's `.timeout()` also covers reading the
/// response body, and `post_json` buffers the whole completion via
/// `response.bytes()`. Large prompts (e.g. the audit skill, which feeds a
/// multi-KB SKILL.md plus several 64 KB file tails) drive generations well
/// past a minute, so a 60 s cap aborted mid-body and surfaced as
/// `error decoding response body; http_status=200`. Default to 300 s and let
/// operators tune it for slower models via the env var.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const REQUEST_TIMEOUT_ENV: &str = "AGENTOS_LLM_REQUEST_TIMEOUT_SECS";

fn shared_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        let request_timeout = env::var(REQUEST_TIMEOUT_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .map_or(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs);
        Client::builder()
            .timeout(request_timeout)
            // Fail fast on a dead endpoint instead of burning the full body
            // budget; only the connection phase is bounded here.
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent(concat!("agentos-llm/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client builds with rustls + http2 features compiled in")
    })
}

pub(crate) async fn post_json(
    _header_prefix: &str,
    url: &str,
    headers: &[(&str, String)],
    payload: &Value,
) -> Result<JsonHttpResponse, ProviderError> {
    // `Bytes` so the retry loop re-sends by bumping a refcount instead of
    // copying the full serialized body on every attempt.
    let body = Bytes::from(
        serde_json::to_vec(payload).map_err(|source| ProviderError::Encode { source })?,
    );
    let header_map = build_header_map(headers)?;
    let client = shared_client();

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match send_once(client, url, &header_map, body.clone()).await {
            Ok(response) => {
                if attempt < MAX_ATTEMPTS && is_retryable_status(response.status) {
                    let retry_after = parse_retry_after(&response.headers);
                    sleep(backoff_delay(attempt - 1, retry_after)).await;
                    continue;
                }
                return Ok(response);
            }
            Err(message) => {
                if attempt < MAX_ATTEMPTS {
                    sleep(backoff_delay(attempt - 1, None)).await;
                    continue;
                }
                return Err(message);
            }
        }
    }
}

/// Send a streaming chat-completion request and hand back the live response for
/// SSE decoding. Unlike [`post_json`] this neither buffers nor retries the body
/// — the caller consumes `response.bytes_stream()`, and re-sending a partially
/// consumed stream is unsafe. A non-success status is read to completion and
/// surfaced as a typed [`ProviderError`] so streaming callers get the same
/// operator diagnostics as the buffered path.
pub(crate) async fn post_sse(
    provider: &str,
    url: &str,
    headers: &[(&str, String)],
    payload: &Value,
) -> Result<reqwest::Response, ProviderError> {
    let body = Bytes::from(
        serde_json::to_vec(payload).map_err(|source| ProviderError::Encode { source })?,
    );
    let header_map = build_header_map(headers)?;
    let client = shared_client();
    let response = client
        .post(url)
        .headers(header_map)
        .body(body)
        .send()
        .await
        .map_err(|err| ProviderError::Transport {
            detail: transport_detail(&err),
        })?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(response);
    }
    let header_map = collect_headers(response.headers());
    let bytes = response
        .bytes()
        .await
        .map_err(|err| ProviderError::Transport {
            detail: format!(
                "{err}; {}",
                describe_http_response(Some(status), &header_map)
            ),
        })?;
    let error_body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let error = error_body.get("error").cloned().unwrap_or(error_body);
    Err(ProviderError::Api {
        provider: Arc::from(provider),
        status: Some(status),
        detail: format!(
            "{provider} streaming error: {error}; {}",
            describe_http_response(Some(status), &header_map)
        ),
    })
}

async fn send_once(
    client: &Client,
    url: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<JsonHttpResponse, ProviderError> {
    let response = client
        .post(url)
        .headers(headers.clone())
        .body(body)
        .send()
        .await
        .map_err(|err| ProviderError::Transport {
            detail: transport_detail(&err),
        })?;
    let status = response.status().as_u16();
    let header_map = collect_headers(response.headers());
    let bytes = response
        .bytes()
        .await
        .map_err(|err| ProviderError::Transport {
            detail: format!(
                "{err}; {}",
                describe_http_response(Some(status), &header_map)
            ),
        })?;
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|err| ProviderError::MalformedResponse {
            detail: format!(
                "failed to parse LLM response: {err}; {}; body={}",
                describe_http_response(Some(status), &header_map),
                String::from_utf8_lossy(&bytes),
            ),
        })?
    };
    Ok(JsonHttpResponse {
        status: Some(status),
        headers: header_map,
        body,
    })
}

fn build_header_map(headers: &[(&str, String)]) -> Result<HeaderMap, ProviderError> {
    let mut map = HeaderMap::with_capacity(headers.len() + 1);
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|err| {
            ProviderError::InvalidHeader {
                detail: format!("invalid header name {name}: {err}"),
            }
        })?;
        let header_value =
            HeaderValue::from_str(value).map_err(|err| ProviderError::InvalidHeader {
                detail: format!("invalid header value for {name}: {err}"),
            })?;
        map.insert(header_name, header_value);
    }
    if !map.contains_key(CONTENT_TYPE) {
        map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    Ok(map)
}

fn collect_headers(map: &HeaderMap) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, value) in map.iter() {
        if let Ok(text) = value.to_str() {
            out.insert(name.as_str().to_ascii_lowercase(), text.to_owned());
        }
    }
    out
}

fn is_retryable_status(status: Option<u16>) -> bool {
    match status {
        Some(code) => {
            code == StatusCode::TOO_MANY_REQUESTS.as_u16()
                || code == StatusCode::REQUEST_TIMEOUT.as_u16()
                || (500..=599).contains(&code)
        }
        None => false,
    }
}

fn parse_retry_after(headers: &BTreeMap<String, String>) -> Option<Duration> {
    let value = headers.get("retry-after")?.trim();
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .map(|delay| delay.min(RETRY_AFTER_CAP))
}

fn backoff_delay(attempt: u32, retry_after: Option<Duration>) -> Duration {
    if let Some(after) = retry_after {
        return after;
    }
    let multiplier = 2_u32.saturating_pow(attempt);
    let exp = BASE_BACKOFF.saturating_mul(multiplier).min(MAX_BACKOFF);
    let upper = exp.as_nanos().min(u64::MAX as u128) as u64;
    if upper == 0 {
        return Duration::ZERO;
    }
    let jitter = rand::thread_rng().gen_range(0..=upper);
    Duration::from_nanos(jitter)
}

pub(crate) fn first_env<const N: usize>(names: [&str; N]) -> Option<String> {
    names.into_iter().find_map(|name| env::var(name).ok())
}

pub(crate) fn format_openai_error(response: &JsonHttpResponse, error: &Value) -> ProviderError {
    format_provider_error_with_hint("OpenAI", response, error, openai_quota_hint(error))
}

pub(crate) fn format_provider_error(
    provider: &str,
    response: &JsonHttpResponse,
    error: &Value,
) -> ProviderError {
    format_provider_error_with_hint(
        provider,
        response,
        error,
        "inspect the provider API error code and request id",
    )
}

fn format_provider_error_with_hint(
    provider: &str,
    response: &JsonHttpResponse,
    error: &Value,
    hint: &str,
) -> ProviderError {
    let detail = format!(
        "{provider} error: {}; {}; hint={}",
        error,
        describe_http_response(response.status, &response.headers),
        hint,
    );
    if is_context_length_error(error) {
        return ProviderError::ContextLength {
            provider: Arc::from(provider),
            status: response.status,
            detail,
        };
    }
    ProviderError::Api {
        provider: Arc::from(provider),
        status: response.status,
        detail,
    }
}

/// Phrases that identify a context-length rejection in a provider's prose.
///
/// Only OpenAI and its compatibles set a machine-readable code; Anthropic and
/// Ollama say it in the message and nowhere else, so prose is all there is.
/// Each entry is a phrase its provider emits verbatim, and each is long enough
/// that an unrelated failure cannot match it — a false positive here costs a
/// pointless summarization call and a retry of a request that was never too
/// long.
const CONTEXT_LENGTH_PHRASES: &[&str] = &[
    // OpenAI, DeepSeek, and OpenAI-compatible proxies.
    "maximum context length",
    "context_length_exceeded",
    // Anthropic: "prompt is too long: 250000 tokens > 200000 maximum".
    "prompt is too long",
    // Ollama and several self-hosted runtimes.
    "exceeds the context window",
    "too many tokens",
];

/// Whether a provider's error object is a rejection for request length.
fn is_context_length_error(error: &Value) -> bool {
    if error.get("code").and_then(Value::as_str) == Some("context_length_exceeded") {
        return true;
    }
    // The whole object rather than `message` alone: providers nest the text
    // differently, and proxies that drop the structured `code` still echo it
    // in their prose.
    let rendered = error.to_string().to_ascii_lowercase();
    CONTEXT_LENGTH_PHRASES
        .iter()
        .any(|phrase| rendered.contains(phrase))
}

/// Flatten a reqwest transport error and its `source()` chain into one detail
/// string. reqwest's top-level Display (e.g. "error sending request for url
/// (...)") hides the actionable cause — proxy DNS failure, connection reset,
/// TLS rejection, timeout — in the source chain, which `{err}` alone discards.
fn transport_detail(err: &reqwest::Error) -> String {
    let mut detail = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    detail.push_str("; http_metadata=unavailable");
    detail
}

fn describe_http_response(status: Option<u16>, headers: &BTreeMap<String, String>) -> String {
    let mut parts = Vec::new();
    if let Some(status) = status {
        parts.push(format!("http_status={status}"));
    }
    for name in [
        "x-request-id",
        "openai-organization",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-remaining-tokens",
    ] {
        if let Some(value) = headers.get(name) {
            parts.push(format!("{name}={value}"));
        }
    }
    if parts.is_empty() {
        "http_metadata=unavailable".to_owned()
    } else {
        parts.join(", ")
    }
}

fn openai_quota_hint(error: &Value) -> &'static str {
    if error.get("code").and_then(Value::as_str) == Some("insufficient_quota") {
        return "check the OpenAI Platform project/org tied to this API key, project monthly budget, org usage limit, and prepaid API credits";
    }
    "inspect the OpenAI API error code and request id"
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{MessageRole, Usage};

    use serde_json::json;

    #[test]
    fn attached_usage_deserializes_into_proto_usage_with_cache_breakdown() {
        let usage = TokenUsage {
            input_tokens: 2100,
            output_tokens: 45,
            total_tokens: 2145,
            cache_read_tokens: 800,
            cache_write_tokens: 1200,
            cache_miss_tokens: 100,
        };
        let mut message = Message::text(MessageRole::Assistant, "hi");
        attach_token_usage(&mut message, usage);

        let raw = message
            .metadata
            .get(TOKEN_USAGE_METADATA_KEY)
            .expect("usage attached under the shared metadata key");
        let decoded: Usage =
            serde_json::from_value(raw.clone()).expect("TokenUsage shape matches proto Usage");
        assert_eq!(
            decoded,
            Usage {
                input_tokens: 2100,
                output_tokens: 45,
                total_tokens: 2145,
                cache_read_tokens: 800,
                cache_write_tokens: 1200,
                cache_miss_tokens: 100,
                // tool_calls is the run-level accumulator; absent on per-call
                // metadata, so it defaults to 0 here.
                tool_calls: 0,
            }
        );
    }

    #[test]
    fn token_usage_parses_openai_with_cached_prompt_details() {
        let body = json!({
            "usage": {
                "prompt_tokens": 2000,
                "completion_tokens": 300,
                "total_tokens": 2300,
                "prompt_tokens_details": { "cached_tokens": 1920 }
            }
        });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 2000,
                output_tokens: 300,
                total_tokens: 2300,
                cache_read_tokens: 1920,
                cache_write_tokens: 0,
                cache_miss_tokens: 80,
            })
        );
    }

    #[test]
    fn token_usage_parses_deepseek_explicit_hit_miss() {
        let body = json!({
            "usage": {
                "prompt_tokens": 2006,
                "completion_tokens": 30,
                "total_tokens": 2036,
                "prompt_cache_hit_tokens": 1920,
                "prompt_cache_miss_tokens": 86
            }
        });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 2006,
                output_tokens: 30,
                total_tokens: 2036,
                cache_read_tokens: 1920,
                cache_write_tokens: 0,
                cache_miss_tokens: 86,
            })
        );
    }

    #[test]
    fn token_usage_no_cache_fields_treats_all_input_as_miss() {
        let body = json!({
            "usage": { "prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150 }
        });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 120,
                output_tokens: 30,
                total_tokens: 150,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                cache_miss_tokens: 120,
            })
        );
    }

    #[test]
    fn token_usage_parses_anthropic_cache_read_and_creation() {
        let body = json!({
            "usage": {
                "input_tokens": 100,
                "output_tokens": 45,
                "cache_read_input_tokens": 800,
                "cache_creation_input_tokens": 1200
            }
        });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 2100,
                output_tokens: 45,
                total_tokens: 2145,
                cache_read_tokens: 800,
                cache_write_tokens: 1200,
                cache_miss_tokens: 100,
            })
        );
    }

    #[test]
    fn token_usage_parses_openai_responses_inclusive_input_tokens() {
        // Unlike Anthropic's shape, `input_tokens` here already counts the
        // cached prompt tokens, so the hit is carved out of it, not added to it.
        let body = json!({
            "usage": {
                "input_tokens": 2100,
                "output_tokens": 45,
                "total_tokens": 2145,
                "input_tokens_details": { "cached_tokens": 800 }
            }
        });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 2100,
                output_tokens: 45,
                total_tokens: 2145,
                cache_read_tokens: 800,
                cache_write_tokens: 0,
                cache_miss_tokens: 1300,
            })
        );
    }

    #[test]
    fn token_usage_parses_ollama_top_level_counts() {
        let body = json!({ "prompt_eval_count": 18, "eval_count": 7 });
        assert_eq!(
            TokenUsage::from_response_body(&body),
            Some(TokenUsage {
                input_tokens: 18,
                output_tokens: 7,
                total_tokens: 25,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                cache_miss_tokens: 18,
            })
        );
    }

    #[test]
    fn token_usage_absent_when_no_usage_metadata() {
        assert_eq!(
            TokenUsage::from_response_body(&json!({ "choices": [] })),
            None
        );
    }

    #[test]
    fn retry_after_parses_seconds() {
        let mut headers = BTreeMap::new();
        headers.insert("retry-after".to_owned(), "3".to_owned());
        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(3)));
    }

    #[test]
    fn retry_after_caps_long_waits() {
        let mut headers = BTreeMap::new();
        headers.insert("retry-after".to_owned(), "9000".to_owned());
        assert_eq!(parse_retry_after(&headers), Some(RETRY_AFTER_CAP));
    }

    #[test]
    fn retry_after_ignores_http_dates() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "retry-after".to_owned(),
            "Wed, 21 Oct 2026 07:28:00 GMT".to_owned(),
        );
        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn retryable_status_covers_throttling_and_5xx() {
        assert!(is_retryable_status(Some(429)));
        assert!(is_retryable_status(Some(408)));
        assert!(is_retryable_status(Some(500)));
        assert!(is_retryable_status(Some(503)));
        assert!(!is_retryable_status(Some(400)));
        assert!(!is_retryable_status(Some(401)));
        assert!(!is_retryable_status(Some(200)));
        assert!(!is_retryable_status(None));
    }

    #[test]
    fn backoff_delay_honors_retry_after_over_exponential() {
        let delay = backoff_delay(3, Some(Duration::from_secs(2)));
        assert_eq!(delay, Duration::from_secs(2));
    }

    #[test]
    fn backoff_delay_grows_within_cap() {
        for attempt in 0..6 {
            let delay = backoff_delay(attempt, None);
            assert!(delay <= MAX_BACKOFF, "attempt {attempt}: {delay:?}");
        }
    }
}

#[cfg(test)]
mod arg_extraction_tests {
    use super::{raw_args_from_json_str, raw_args_from_json_value};
    use serde_json::{json, Value};

    #[test]
    fn both_arg_shapes_produce_identical_raw_values_for_sorted_input() {
        // String-encoded shape (OpenAI/DeepSeek) vs object shape
        // (Anthropic/Ollama) must hand identical bytes downstream when the
        // provider emits sorted keys.
        let as_object = json!({"a": 1, "b": "two"});
        let as_string = Value::String(r#"{"a":1,"b":"two"}"#.to_owned());

        let from_value = raw_args_from_json_value(Some(&as_object)).expect("object args");
        let from_str = raw_args_from_json_str(Some(&as_string)).expect("string args");
        assert_eq!(from_value.get(), from_str.get());
    }

    #[test]
    fn missing_args_default_to_empty_object_in_both_shapes() {
        assert_eq!(raw_args_from_json_value(None).expect("default").get(), "{}");
        assert_eq!(raw_args_from_json_str(None).expect("default").get(), "{}");
    }

    #[test]
    fn invalid_arg_string_is_rejected() {
        let invalid = Value::String("{not json".to_owned());
        assert!(raw_args_from_json_str(Some(&invalid)).is_none());
    }
}

#[cfg(test)]
mod provider_error_tests {
    use super::{format_openai_error, JsonHttpResponse, ProviderError};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn api_errors_are_matchable_and_keep_operator_diagnostics() {
        let response = JsonHttpResponse {
            status: Some(429),
            headers: BTreeMap::from([("x-request-id".to_owned(), "req-123".to_owned())]),
            body: json!({}),
        };
        let error = format_openai_error(&response, &json!({"code": "insufficient_quota"}));
        let ProviderError::Api {
            provider,
            status,
            detail,
        } = &error
        else {
            panic!("expected Api variant, got {error:?}");
        };
        assert_eq!(provider.as_ref(), "OpenAI");
        assert_eq!(*status, Some(429));
        assert!(detail.contains("http_status=429"));
        assert!(detail.contains("x-request-id=req-123"));
        assert!(
            detail.contains("prepaid API credits"),
            "quota hint preserved"
        );
        // Display carries the full diagnostic line for logs.
        assert_eq!(error.to_string(), *detail);
    }

    #[test]
    fn missing_credentials_text_matches_legacy_message() {
        let error = ProviderError::MissingCredentials {
            variable: "OPENAI_API_KEY",
        };
        assert_eq!(error.to_string(), "missing OPENAI_API_KEY");
    }

    fn rejected(error: serde_json::Value) -> ProviderError {
        let response = JsonHttpResponse {
            status: Some(400),
            headers: BTreeMap::new(),
            body: json!({}),
        };
        super::format_provider_error("Test", &response, &error)
    }

    #[test]
    fn every_provider_shape_of_a_length_rejection_is_classified() {
        // Roadmap C4: the run loop recovers from this class and only this
        // class, so each provider's actual wording has to be recognised. These
        // are the shapes the four supported providers return.
        for (label, error) in [
            (
                "openai structured",
                json!({"code": "context_length_exceeded", "message": "…"}),
            ),
            (
                "openai prose",
                json!({"message": "This model's maximum context length is 8192 tokens, however you requested 9000."}),
            ),
            (
                "anthropic",
                json!({"type": "invalid_request_error", "message": "prompt is too long: 250000 tokens > 200000 maximum"}),
            ),
            (
                "deepseek",
                json!({"message": "This model's maximum context length is 65536 tokens."}),
            ),
            (
                "self-hosted",
                json!({"error": "input exceeds the context window for this model"}),
            ),
        ] {
            assert!(
                matches!(rejected(error), ProviderError::ContextLength { .. }),
                "{label} was not classified as a length rejection"
            );
        }
    }

    #[test]
    fn unrelated_failures_stay_unrecoverable() {
        // A false positive costs a pointless summarization call and a retry of
        // a request that was never too long, so the phrase list must not be
        // greedy.
        for error in [
            json!({"code": "insufficient_quota", "message": "You exceeded your current quota."}),
            json!({"code": "rate_limit_exceeded", "message": "Rate limit reached for requests."}),
            json!({"message": "Invalid API key provided."}),
            json!({"message": "max_tokens must be less than the model's output limit."}),
        ] {
            assert!(
                matches!(rejected(error.clone()), ProviderError::Api { .. }),
                "{error} should not be treated as recoverable"
            );
        }
    }

    #[test]
    fn only_the_length_class_survives_the_trait_boundary_typed() {
        use crate::LlmError;

        let recovered = rejected(json!({"code": "context_length_exceeded"})).into_llm_error();
        assert!(matches!(recovered, LlmError::ContextLengthExceeded(_)));

        let opaque = rejected(json!({"code": "insufficient_quota"})).into_llm_error();
        let LlmError::Provider(detail) = opaque else {
            panic!("every other class stays opaque text");
        };
        // The operator diagnostics survive the collapse, as they always have.
        assert!(detail.contains("http_status=400"));
    }
}
