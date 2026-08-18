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

If `XDG_DATA_HOME` is set, the runtime home is
`$XDG_DATA_HOME/agentos`. Override the paths independently with `--bindir` and
`--home`, or use `--prefix PATH` to install under `PATH/bin` and
`PATH/share/agentos`.

## Install from a release bundle

```sh
tar -xzf agentos-v<version>-<platform>-<arch>.tar.gz
cd agentos-v<version>-<platform>-<arch>
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

The `anthropic` provider must send a `max_tokens` on every request — the
Messages API requires it — so a reply is always capped. The cap comes from a
table of each model's published output limit, and a truncated reply is recorded
on the message as `agentos.stop_reason: "length"`. Set
`AGENTOS_ANTHROPIC_MAX_OUTPUT_TOKENS` for a model the table has wrong or has
never heard of. Buffered (non-streaming) requests are additionally clamped to
8,192, because Anthropic refuses a non-streaming request that could run past its
ten-minute limit; streaming requests use the full cap.

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
~/.local/bin/agentos config
~/.local/bin/agentos gateway-status
printf 'offline install smoke\n/exit\n' | \
  AGENTOS_LLM_PROVIDER=builtin.echo ~/.local/bin/agentos tui
```

`agentos gateway-status` prints `AgentOS gateway status: stopped` before the
gateway has been started. `agentos resume` without a path uses
`~/.local/share/agentos/workspace/runs/cli-run-1.json`; if it does not exist,
the command exits non-zero and names that path.
