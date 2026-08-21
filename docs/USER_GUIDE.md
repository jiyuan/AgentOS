# User Guide

## Installed commands

After installation, the main entrypoint is:

```sh
~/.local/bin/agentos
```

## Modes

### TUI

```sh
agentos tui
```

Starts the interactive terminal interface.

Slash commands inside the TUI (the Telegram and Feishu gateways accept the
same set):

- `/help` — list available slash commands.
- `/clear` — clear the current conversation history. On a gateway this also
  cancels and reclaims that conversation's background jobs.
- `/stop` — stop whatever this conversation is currently running. Gateway only:
  the TUI runs one turn at a time, so there is never anything to interrupt.
  Work already finished this turn is saved.
- `/approve <ticket>` — allow a pending approval.
- `/deny <ticket> [reason]` — refuse a pending approval.
- `/orchestrator [max|min|status]` — show or switch the orchestrator strategy.
- `/model [provider:model|status|reset]` — show, set, or reset the LLM model.
- `/skills [list|status]` — list enabled workspace skills.
- `/crons [list|status]` — list scheduled cron tasks.
- `/tools [list|status]` — list registered tools.
- `/memory [list|status]` — list scoped memory fragments.
- `/usage [status]` — show session token usage.

### Gateway lifecycle

```sh
agentos gateway-start
agentos gateway-restart
agentos gateway-stop
agentos gateway-status
```

The `shell` tool runs sandboxed: it may write beneath the workspace and the
temporary directory, and nowhere else, enforced by the kernel rather than by
the tool's own good behaviour. Which programs it will accept at all is a
separate question, decided by `[guardrails].shell_allowlist`, and which
*arguments* it will accept is decided by `[[guardrails.shell_profiles]]`. Run
`agentos config` to see whether this machine can enforce a sandbox — where it
cannot, a sandboxed tool fails rather than running unrestricted.

The gateway runs conversations concurrently: each is pinned to one of
`[gateway].shards` worker threads, so one conversation waiting on a slow tool
does not hold up anybody else's. Within a conversation everything stays in
order — one turn at a time.

When the agent needs permission for something, it asks and prints a short
**ticket**. Only an answer carrying that ticket decides it — `/approve <ticket>`,
`/deny <ticket> [reason]`, or the inline buttons on channels that render them.
Saying "yes, go ahead" does not approve anything: it is an ordinary message, and
the agent will still be waiting. That is deliberate, because otherwise any
message at all could authorise a tool call the user was not thinking about.

Prompts expire (`[approval].expiry_seconds`, 15 minutes by default). An expired
prompt is recorded as *cancelled*, not refused: the action does not run either
way, but the log does not claim you turned something down that you never saw.

If you send another message while the agent is still working, it is not queued
behind the current turn and it does not start a second one. It reaches the
agent at its next planning step, so "actually, do X instead" lands within
seconds rather than after the tool call you are trying to interrupt. Use
`/stop` when you want the turn abandoned outright.

The gateway uses:

- config: `~/.local/share/agentos/workspace/agent.toml`
- env file: `~/.local/share/agentos/.env`
- session db: `~/.local/share/agentos/workspace/agentos.sqlite`
- log: `~/.local/share/agentos/logs/agentos-gateway.log`

To inspect the loaded runtime configuration, run:

```sh
agentos config
```

That goes through the installed wrapper, which knows where the runtime home
is. `agentos-gateway` itself lives inside that home rather than on `PATH`.

Every key `agent.toml` accepts — with its type, default, and what it does — is
in [`CONFIG_CATALOG.md`](CONFIG_CATALOG.md), and every built-in tool with its
sandbox mode and deadline is in [`TOOL_CATALOG.md`](TOOL_CATALOG.md). Both are
generated from the code by `agentos-gateway catalog`, so they cannot describe a
version of the runtime that no longer exists.

Gateway persistent channel selection uses this precedence:

1. `AGENTOS_ENABLED_CHANNELS=telegram,feishu` overrides workspace channel enablement.
2. Without that override, `[channels.telegram].enabled` and `[channels.feishu].enabled` in `workspace/agent.toml` control persistent channels.
3. `channels.tui` is for the interactive TUI and is never started by the persistent gateway.

The effective config keys are:

- `[agent].max_turns`, `[policy].default`, `[channels.*].enabled`, and `[channels.*].mode`.
- `[resources.skills].enabled`, `[resources.tools].enabled`, `[resources.mcp].enabled`, and `[resources.llm].enabled`.
- Workspace file discovery from `subagents/*.toml` and `suborchs/*.toml` through the same loader used by the runtime.
- Runtime path injection for workspace root, skills, crons, traces, sessions, and channel attachments is documented in `docs/ARCHITECTURE.md`.

### One-shot channel runs

```sh
agentos telegram-once
agentos feishu-once
```

These process a single inbound event and exit.

### Resuming a paused run

When an action requires approval (`ask_user`), the run is serialized to disk.
Resume it with:

```sh
agentos resume [<state-file>]
```

Without an explicit path, it falls back to
`~/.local/share/agentos/workspace/runs/cli-run-1.json`.

## Logs and state

- gateway log: `~/.local/share/agentos/logs/agentos-gateway.log`
- session database: `~/.local/share/agentos/workspace/agentos.sqlite`
- paused runs: `~/.local/share/agentos/workspace/runs/`

## Packaging a release

From a source checkout:

```sh
scripts/package-release.sh
```

Artifacts are written to `dist/`.
