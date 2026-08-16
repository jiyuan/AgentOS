//! Provider-neutral LLM interface and environment-backed provider adapters.

pub mod env;
pub mod providers;

pub use providers::ProviderError;

use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Message, MessageRole};
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use std::env as std_env;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmRequest {
    pub model: Arc<str>,
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmResponse {
    pub message: Message,
}

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("llm provider failed: {0}")]
    Provider(Arc<str>),
    #[error("llm is not configured for {0} tier")]
    Unconfigured(Arc<str>),
    /// A transport failure that occurred *after* the streaming response began
    /// (e.g. the SSE body was cut mid-stream). Distinct from [`LlmError::Provider`]
    /// so callers can retry with a buffered completion instead of failing the
    /// whole turn — a flaky stream shouldn't kill a recoverable request.
    #[error("streaming transport error: {0}")]
    StreamTransport(Arc<str>),
    /// The provider rejected the request for exceeding the model's context
    /// window.
    ///
    /// The one provider failure a run can recover from on its own, and the one
    /// pressure trigger that is never a guess — the model itself said no.
    /// Roadmap item C4 compacts the conversation and retries the turn once.
    /// Distinct from [`LlmError::Provider`] so the run loop branches on a type
    /// rather than on a substring of a provider's prose.
    #[error("context length exceeded: {0}")]
    ContextLengthExceeded(Arc<str>),
}

/// One event emitted while streaming a single LLM completion.
///
/// A streaming completion yields zero or more [`CompletionEvent::Text`] chunks
/// followed by exactly one terminal [`CompletionEvent::Done`]. Concatenating
/// every `Text` chunk reproduces the final message's `content`, so a consumer
/// can render incremental output and still treat `Done` as authoritative — it
/// carries any tool calls and the `agentos.token_usage` metadata that the
/// run loop folds into `RunState`. Providers without a native streaming API
/// satisfy this contract through the default [`Llm::complete_stream`], which
/// emits the whole reply as one `Text` chunk and then `Done`.
#[derive(Clone, Debug, PartialEq)]
pub enum CompletionEvent {
    /// An incremental chunk of assistant-visible text.
    Text(Arc<str>),
    /// The fully assembled assistant message. Always the final event of a
    /// successful stream.
    Done(Message),
}

/// A boxed stream of [`CompletionEvent`]s. `'static` because providers build it
/// from owned data (cloned transcript messages, an owned HTTP body stream) and
/// never borrow the `RunContext` that started the call.
pub type CompletionStream = BoxStream<'static, Result<CompletionEvent, LlmError>>;

#[async_trait]
pub trait Llm: Send + Sync {
    fn is_available(&self) -> bool {
        true
    }

    fn describe(&self) -> String {
        "llm provider=custom".to_owned()
    }

    /// The context window of the model this adapter will call, in tokens.
    ///
    /// Only the provider adapter knows which model a request will reach, so
    /// budget resolution belongs here rather than in a table inside the run
    /// loop. `None` means unknown — a caller must then treat pressure as
    /// unmeasurable rather than assume a default window and compact against a
    /// number nobody chose.
    fn context_budget_tokens(&self) -> Option<usize> {
        None
    }

    /// Complete a turn from the context's transcript alone.
    ///
    /// This crate cannot depend on `agentos-core`, so implementations here see
    /// only [`RunContext::state`] — not the skill prelude or the memory
    /// fragments an orchestrator contributes. **An orchestrator must not build
    /// a conversation request this way**: assemble through
    /// `agentos_core::prompt::assemble` and call [`Llm::complete_messages`], or
    /// every section but the transcript is silently dropped (the F1 defect
    /// roadmap item P1 fixed). This method remains for LLM-only callers that
    /// have no sections to contribute.
    async fn complete(&self, ctx: &RunContext<'_>) -> Result<Message, LlmError>;

    async fn complete_messages(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        Err(LlmError::Unconfigured(Arc::from("messages")))
    }

    /// Stream a single completion as a sequence of [`CompletionEvent`]s.
    ///
    /// The default implementation calls [`Llm::complete`] and adapts the result
    /// into a non-incremental stream — one `Text` chunk carrying the whole
    /// reply (omitted when the reply has no text body) followed by `Done`. A
    /// provider with a native streaming API overrides this to emit `Text`
    /// chunks as tokens arrive; overrides must uphold the [`CompletionEvent`]
    /// contract so the concatenated chunks equal the final message `content`.
    async fn complete_stream(&self, ctx: &RunContext<'_>) -> Result<CompletionStream, LlmError> {
        let message = self.complete(ctx).await?;
        Ok(single_message_stream(message))
    }

    /// Stream a completion over an explicit message list and tool set — the
    /// streaming counterpart of [`Llm::complete_messages`]. Unlike
    /// [`Llm::complete_stream`] this carries tool specs, so a tool-capable
    /// orchestrator can stream the final reply while still receiving tool calls
    /// on the terminal [`CompletionEvent::Done`] (a tool-call turn emits no
    /// `Text`). The default falls back to the buffered call adapted into a
    /// single terminal chunk.
    async fn complete_messages_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<CompletionStream, LlmError> {
        let message = self.complete_messages(messages, tools).await?;
        Ok(single_message_stream(message))
    }
}

/// Adapt a complete (non-streamed) assistant message into a [`CompletionStream`]
/// that satisfies the [`CompletionEvent`] contract: the full text as one `Text`
/// chunk (skipped when empty, e.g. a tool-call-only reply) followed by `Done`.
/// Used by the default [`Llm::complete_stream`] and by providers that have no
/// native streaming path.
pub fn single_message_stream(message: Message) -> CompletionStream {
    let mut events: Vec<Result<CompletionEvent, LlmError>> = Vec::with_capacity(2);
    if !message.content.is_empty() {
        events.push(Ok(CompletionEvent::Text(message.content.clone())));
    }
    events.push(Ok(CompletionEvent::Done(message)));
    stream::iter(events).boxed()
}

pub struct LlmOrchestrator {
    llm: Arc<dyn Llm>,
}

impl LlmOrchestrator {
    pub fn new(llm: Arc<dyn Llm>) -> Self {
        Self { llm }
    }
}

#[async_trait]
impl Orchestrator for LlmOrchestrator {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let response = if ctx.has_stream_sink() {
            // Recover a stream cut mid-flight (flaky network/proxy) by retrying
            // once buffered, rather than failing the turn.
            match drain_reply_stream(self.llm.complete_stream(ctx).await, ctx).await {
                Err(LlmError::StreamTransport(detail)) => {
                    tracing::warn!(
                        error = %detail,
                        "llm stream failed mid-flight; retrying with a buffered completion"
                    );
                    self.llm.complete(ctx).await
                }
                other => other,
            }
        } else {
            self.llm.complete(ctx).await
        }
        .map_err(|err| match err {
            // Keep the recoverable class typed all the way to the run loop.
            LlmError::ContextLengthExceeded(detail) => {
                OrchestratorError::ContextLengthExceeded(detail)
            }
            other => OrchestratorError::Backend(Arc::from(other.to_string())),
        })?;
        ctx.push_llm_usage_from_message(&response);
        Ok(Plan::Reply(response))
    }
}

/// Drain a [`CompletionStream`], forwarding each `Text` chunk to the run's
/// stream sink and returning the assembled final message. Shared by the
/// in-crate `LlmOrchestrator`; core orchestrators have their own helper over
/// [`Llm::complete_messages_stream`].
async fn drain_reply_stream(
    stream: Result<CompletionStream, LlmError>,
    ctx: &RunContext<'_>,
) -> Result<Message, LlmError> {
    let mut stream = stream?;
    let mut done = None;
    while let Some(event) = stream.next().await {
        match event? {
            CompletionEvent::Text(chunk) => ctx.emit_stream_delta(&chunk).await,
            CompletionEvent::Done(message) => done = Some(message),
        }
    }
    done.ok_or_else(|| LlmError::Provider(Arc::from("stream ended without a final message")))
}

/// Environment override for the resolved context window, in tokens.
///
/// A deployment pointing at a self-hosted or proxied model the table below
/// does not know needs a way to state its window without a rebuild. Roadmap
/// item X3 moves this to `agent.toml` alongside the compaction thresholds that
/// will consume it; it is an env var here only because the whole LLM layer is
/// env-configured today.
pub const CONTEXT_BUDGET_ENV: &str = "AGENTOS_CONTEXT_BUDGET_TOKENS";

/// Published context windows by model-id prefix, longest prefix first.
///
/// These are external facts about each model, not deployment policy, so they
/// are a table rather than configuration — the override above covers the case
/// where the table is wrong or incomplete. Values are the *total* window;
/// output shares it, so a caller budgeting for input should leave headroom.
const CONTEXT_BUDGETS: &[(&str, usize)] = &[
    ("claude-haiku-4", 200_000),
    ("claude-opus-4", 200_000),
    ("claude-sonnet-4", 200_000),
    ("claude-3-5", 200_000),
    ("claude-3", 200_000),
    ("deepseek-reasoner", 128_000),
    ("deepseek-chat", 128_000),
    ("gpt-4.1", 1_047_576),
    ("gpt-4o", 128_000),
    ("gpt-4-turbo", 128_000),
    ("gpt-4", 8_192),
    ("gpt-5", 400_000),
    ("o1", 200_000),
    ("o3", 200_000),
    ("qwen", 32_768),
    ("llama3", 8_192),
];

fn context_budget_override() -> Option<usize> {
    std_env::var(CONTEXT_BUDGET_ENV)
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|tokens| *tokens > 0)
}

/// Resolve a model id to its context window by longest matching prefix.
///
/// Unknown models return `None` rather than a guess: a wrong window is worse
/// than an absent one, because compaction would then run against a number
/// nobody chose.
pub fn context_budget_for_model(model: &str) -> Option<usize> {
    let normalized = model.trim().to_ascii_lowercase();
    CONTEXT_BUDGETS
        .iter()
        .filter(|(prefix, _)| normalized.starts_with(prefix))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|(_, budget)| *budget)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LlmModelTier {
    High,
    Medium,
    Low,
}

impl LlmModelTier {
    pub fn from_config(input: &str) -> Result<Self, String> {
        match input.trim().to_ascii_lowercase().as_str() {
            "high" => Ok(Self::High),
            "medium" | "" => Ok(Self::Medium),
            "low" => Ok(Self::Low),
            other => Err(format!(
                "unknown model_tier '{other}'; expected high, medium, or low"
            )),
        }
    }

    pub fn env_suffix(self) -> &'static str {
        match self {
            Self::High => "HIGH",
            Self::Medium => "MEDIUM",
            Self::Low => "LOW",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LlmSelection {
    pub provider: Arc<str>,
    pub model: Arc<str>,
}

impl LlmSelection {
    pub fn label(&self) -> String {
        format!("{}:{}", self.provider, self.model)
    }
}

#[derive(Clone, Default)]
pub struct LlmModelController {
    override_selection: Arc<std::sync::Mutex<Option<LlmSelection>>>,
}

impl LlmModelController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_override(&self, input: &str) -> Result<LlmSelection, String> {
        let selection = parse_model_identifier(input, None)?;
        validate_llm_selection(&selection)?;
        if let Ok(mut guard) = self.override_selection.lock() {
            *guard = Some(selection.clone());
        }
        Ok(selection)
    }

    pub fn clear_override(&self) {
        if let Ok(mut guard) = self.override_selection.lock() {
            *guard = None;
        }
    }

    pub fn override_selection(&self) -> Option<LlmSelection> {
        self.override_selection
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }
}

#[derive(Clone)]
pub struct EnvLlm {
    tier: LlmModelTier,
    fallback: Option<LlmSelection>,
    controller: LlmModelController,
}

impl EnvLlm {
    pub fn new(tier: LlmModelTier, controller: LlmModelController) -> Result<Self, String> {
        let fallback = configured_selection_for_tier(tier)?;
        if let Some(selection) = &fallback {
            validate_llm_selection(selection)?;
        }
        Ok(Self {
            tier,
            fallback,
            controller,
        })
    }

    pub fn current_selection(&self) -> Option<LlmSelection> {
        self.controller
            .override_selection()
            .or_else(|| self.fallback.clone())
    }
}

#[async_trait]
impl Llm for EnvLlm {
    fn is_available(&self) -> bool {
        self.current_selection().is_some()
    }

    fn describe(&self) -> String {
        match self.current_selection() {
            Some(selection) if selection.provider.as_ref() == "openai" => format!(
                "llm provider={}, model={}, tier={}, openai_org={}, openai_project={}",
                selection.provider,
                selection.model,
                self.tier.env_suffix(),
                env_presence(&["OPENAI_ORGANIZATION", "OPENAI_ORG_ID"]),
                env_presence(&["OPENAI_PROJECT", "OPENAI_PROJECT_ID"])
            ),
            Some(selection) => format!(
                "llm provider={}, model={}, tier={}",
                selection.provider,
                selection.model,
                self.tier.env_suffix()
            ),
            None => "llm provider=builtin.echo".to_owned(),
        }
    }

    fn context_budget_tokens(&self) -> Option<usize> {
        if let Some(override_tokens) = context_budget_override() {
            return Some(override_tokens);
        }
        let selection = self.current_selection()?;
        context_budget_for_model(&selection.model)
    }

    async fn complete(&self, ctx: &RunContext<'_>) -> Result<Message, LlmError> {
        let messages = ctx
            .state
            .transcript
            .items
            .iter()
            .map(|item| item.message.clone())
            .collect::<Vec<_>>();
        self.complete_messages(&messages, &[]).await
    }

    async fn complete_messages(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Message, LlmError> {
        let Some(selection) = self.current_selection() else {
            return Err(LlmError::Unconfigured(Arc::from(self.tier.name())));
        };
        validate_llm_selection(&selection).map_err(|err| LlmError::Provider(Arc::from(err)))?;
        let message = match selection.provider.as_ref() {
            "openai" => providers::openai::complete(&selection.model, messages, tools).await,
            "anthropic" => providers::anthropic::complete(&selection.model, messages, tools).await,
            "deepseek" => providers::deepseek::complete(&selection.model, messages, tools).await,
            "ollama" => providers::ollama::complete(&selection.model, messages, tools).await,
            other => Err(ProviderError::UnknownProvider(Arc::from(other))),
        }
        .map_err(ProviderError::into_llm_error)?;
        // Belt-and-braces: providers may forget to set the role; force Assistant.
        let mut message = message;
        message.role = MessageRole::Assistant;
        Ok(message)
    }

    async fn complete_stream(&self, ctx: &RunContext<'_>) -> Result<CompletionStream, LlmError> {
        let messages = ctx
            .state
            .transcript
            .items
            .iter()
            .map(|item| item.message.clone())
            .collect::<Vec<_>>();
        // Mirror `complete`, which sends no tools field on the bare reply path.
        self.complete_messages_stream(&messages, &[]).await
    }

    async fn complete_messages_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<CompletionStream, LlmError> {
        let Some(selection) = self.current_selection() else {
            return Err(LlmError::Unconfigured(Arc::from(self.tier.name())));
        };
        validate_llm_selection(&selection).map_err(|err| LlmError::Provider(Arc::from(err)))?;
        // Providers with a native SSE path stream incrementally. Ollama has no
        // native path yet, so it falls back to the buffered completion adapted
        // into a single terminal chunk — an identical final message, just
        // without incremental text.
        let stream = match selection.provider.as_ref() {
            "openai" => providers::openai::complete_stream(&selection.model, messages, tools).await,
            "deepseek" => {
                providers::deepseek::complete_stream(&selection.model, messages, tools).await
            }
            "anthropic" => {
                providers::anthropic::complete_stream(&selection.model, messages, tools).await
            }
            _ => {
                return Ok(single_message_stream(
                    self.complete_messages(messages, tools).await?,
                ))
            }
        };
        stream.map_err(ProviderError::into_llm_error)
    }
}

pub fn configured_selection_for_tier(tier: LlmModelTier) -> Result<Option<LlmSelection>, String> {
    let provider_config =
        std_env::var("AGENTOS_LLM_PROVIDER").unwrap_or_else(|_| infer_llm_provider());
    let model_config = std_env::var(format!("AGENTOS_LLM_MODEL_{}", tier.env_suffix()))
        .ok()
        .filter(|model| !model.trim().is_empty())
        .or_else(|| {
            std_env::var("AGENTOS_LLM_MODEL")
                .ok()
                .filter(|model| !model.trim().is_empty())
        });

    if let Some(model_config) = model_config {
        let default_provider = provider_config
            .split_once(':')
            .map_or(provider_config.as_str(), |(provider, _)| provider);
        if default_provider == "builtin.echo" && !model_config.contains(':') {
            return Ok(None);
        }
        return parse_model_identifier(&model_config, Some(default_provider)).map(Some);
    }

    if provider_config.contains(':') {
        return parse_model_identifier(&provider_config, None).map(Some);
    }
    if provider_config == "builtin.echo" {
        return Ok(None);
    }
    let model = default_model_for_provider(&provider_config);
    if model.is_empty() {
        return Err(format!("unknown AGENTOS_LLM_PROVIDER: {provider_config}"));
    }
    Ok(Some(LlmSelection {
        provider: Arc::from(provider_config),
        model: Arc::from(model),
    }))
}

pub fn parse_model_identifier(
    input: &str,
    default_provider: Option<&str>,
) -> Result<LlmSelection, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("model identifier cannot be empty".to_owned());
    }
    let (provider, model) = match trimmed.split_once(':') {
        Some((provider, model)) => (provider.trim(), model.trim()),
        None => (
            default_provider.ok_or_else(|| {
                "model identifier must use provider:model, for example openai:gpt-5.4-mini"
                    .to_owned()
            })?,
            trimmed,
        ),
    };
    let provider = provider.to_ascii_lowercase();
    if provider.is_empty() || model.is_empty() {
        return Err("model identifier must use provider:model with both parts set".to_owned());
    }
    if provider == "builtin.echo" {
        return Err("builtin.echo is not a selectable LLM model".to_owned());
    }
    if !matches!(
        provider.as_str(),
        "openai" | "anthropic" | "deepseek" | "ollama"
    ) {
        return Err(format!("unknown LLM provider: {provider}"));
    }
    Ok(LlmSelection {
        provider: Arc::from(provider),
        model: Arc::from(model.to_owned()),
    })
}

pub fn validate_llm_selection(selection: &LlmSelection) -> Result<(), String> {
    match selection.provider.as_ref() {
        "openai" => require_env_var("OPENAI_API_KEY")?,
        "anthropic" => require_env_var("ANTHROPIC_API_KEY")?,
        "deepseek" => require_env_var("DEEPSEEK_API_KEY")?,
        "ollama" => {}
        _ => return Err(format!("unknown LLM provider: {}", selection.provider)),
    }
    Ok(())
}

pub fn default_model_for_provider(provider: &str) -> String {
    match provider {
        "openai" => "gpt-5.4-mini".to_owned(),
        "anthropic" => "claude-sonnet-4-5".to_owned(),
        "deepseek" => "deepseek-chat".to_owned(),
        "ollama" => "llama3.2".to_owned(),
        _ => String::new(),
    }
}

pub fn infer_llm_provider() -> String {
    if std_env::var_os("OPENAI_API_KEY").is_some() {
        "openai".to_owned()
    } else if std_env::var_os("ANTHROPIC_API_KEY").is_some() {
        "anthropic".to_owned()
    } else if std_env::var_os("DEEPSEEK_API_KEY").is_some() {
        "deepseek".to_owned()
    } else if std_env::var_os("OLLAMA_HOST").is_some() {
        "ollama".to_owned()
    } else {
        "builtin.echo".to_owned()
    }
}

fn require_env_var(name: &str) -> Result<(), String> {
    if std_env::var_os(name).is_none() {
        return Err(format!(
            "{name} is required for the configured LLM provider"
        ));
    }
    Ok(())
}

fn env_presence(names: &[&str]) -> &'static str {
    if names.iter().any(|name| std_env::var_os(name).is_some()) {
        "set"
    } else {
        "unset"
    }
}

#[cfg(test)]
mod context_budget_tests {
    use super::*;

    #[test]
    fn a_longer_prefix_wins() {
        // `gpt-4` and `gpt-4o` both match a gpt-4o id; the specific window
        // must win, or a 128k model would be budgeted as an 8k one.
        assert_eq!(context_budget_for_model("gpt-4o-2024-08-06"), Some(128_000));
        assert_eq!(context_budget_for_model("gpt-4-0613"), Some(8_192));
    }

    #[test]
    fn matching_ignores_case_and_surrounding_space() {
        assert_eq!(context_budget_for_model("  DeepSeek-Chat  "), Some(128_000));
    }

    #[test]
    fn an_unknown_model_resolves_to_nothing() {
        // Not a default: compaction against an invented window is worse than
        // no pressure signal at all.
        assert_eq!(context_budget_for_model("some-self-hosted-model"), None);
        assert_eq!(context_budget_for_model(""), None);
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use agentos_proto::MessageRole;

    async fn collect(stream: CompletionStream) -> Vec<CompletionEvent> {
        stream
            .map(|item| item.expect("fallback stream never errors"))
            .collect()
            .await
    }

    #[tokio::test]
    async fn single_message_stream_emits_text_then_done() {
        let message = Message::text(MessageRole::Assistant, "hello world");
        let events = collect(single_message_stream(message.clone())).await;
        assert_eq!(
            events,
            vec![
                CompletionEvent::Text(Arc::from("hello world")),
                CompletionEvent::Done(message),
            ]
        );
    }

    #[tokio::test]
    async fn single_message_stream_skips_empty_text_for_tool_call_only_reply() {
        // A tool-call-only assistant reply has empty content, so the stream
        // carries only the terminal Done event.
        let message = Message::text(MessageRole::Assistant, "");
        let events = collect(single_message_stream(message.clone())).await;
        assert_eq!(events, vec![CompletionEvent::Done(message)]);
    }

    #[tokio::test]
    async fn concatenated_text_chunks_equal_final_content() {
        // The core contract every streaming provider must also uphold.
        let message = Message::text(MessageRole::Assistant, "the quick brown fox");
        let events = collect(single_message_stream(message)).await;
        let mut text = String::new();
        let mut done = None;
        for event in events {
            match event {
                CompletionEvent::Text(chunk) => text.push_str(&chunk),
                CompletionEvent::Done(message) => done = Some(message),
            }
        }
        assert_eq!(text, done.expect("done event present").content.as_ref());
    }
}
