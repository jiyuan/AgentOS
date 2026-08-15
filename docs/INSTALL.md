# Install Guide

## Prerequisites

For source installs:

- `rustup`
- `cargo`
- `curl`
- `openssl` for Feishu long-connection support

For release-bundle installs:

- no Rust toolchain is required
- `curl` and `openssl` are still needed at runtime for Telegram and Feishu

## Install from source

```sh
cp .env.example .env
scripts/install-agentos.sh --from-source
```

This installs AgentOS into:

- binaries: `~/.local/bin`
- runtime home: `~/.local/share/agentos`

## Install from a release bundle

```sh
tar -xzf agentos-v0.2.0-<platform>-<arch>.tar.gz
cd agentos-v0.2.0-<platform>-<arch>
scripts/install-agentos.sh
```

## Configuration

After install, copy and edit:

```sh
cp ~/.local/share/agentos/.env.example ~/.local/share/agentos/.env
```

Minimum TUI setup (OpenAI):

```env
AGENTOS_LLM_PROVIDER=openai
AGENTOS_LLM_MODEL=gpt-5.4-mini
OPENAI_API_KEY=...
```

Supported LLM providers (`AGENTOS_LLM_PROVIDER`):

| Provider | Required environment | Default model |
|---|---|---|
| `openai` | `OPENAI_API_KEY` | `gpt-5.4-mini` |
| `anthropic` | `ANTHROPIC_API_KEY` | set via `AGENTOS_LLM_MODEL` |
| `deepseek` | `DEEPSEEK_API_KEY` | set via `AGENTOS_LLM_MODEL` |
| `ollama` | `OLLAMA_HOST` (defaults to `http://localhost:11434`) | set via `AGENTOS_LLM_MODEL` |
| `builtin.echo` | none (offline fallback) | `builtin.echo` |

If `AGENTOS_LLM_PROVIDER` is unset, the provider is inferred from whichever
credential is present, in this order: OpenAI, Anthropic, DeepSeek, Ollama, then
the `builtin.echo` offline fallback.

The `openai` provider talks to the Responses API (`/v1/responses`) only, so an
`AGENTOS_OPENAI_BASE_URL` override must point at a deployment that serves it —
an OpenAI-compatible gateway offering only `/v1/chat/completions` should be
configured as the `deepseek` or `ollama` provider instead. Responses are stored
server-side so each turn continues the previous one instead of re-uploading the
transcript; set `AGENTOS_OPENAI_STATEFUL=0` to disable that and keep every
request self-contained.

Telegram setup:

```env
AGENTOS_ENABLED_CHANNELS=telegram
AGENTOS_TELEGRAM_BOT_TOKEN=...
AGENTOS_TELEGRAM_CHAT_ID=...
```

Feishu setup:

```env
AGENTOS_ENABLED_CHANNELS=feishu
AGENTOS_FEISHU_APP_ID=...
AGENTOS_FEISHU_APP_SECRET=...
AGENTOS_FEISHU_ALLOWED_ID=...
```

## Verify install

```sh
~/.local/bin/agentos gateway-status
~/.local/bin/agentos tui
```
