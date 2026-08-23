#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agentos_home="${AGENTOS_HOME:-$(cd "$script_dir/.." && pwd)}"
env_file="${AGENTOS_ENV_FILE:-$agentos_home/.env}"
allow_env_overrides=1
mode="tui"
# Whether an explicit command word was given. Distinguishes `agentos` (default
# TUI) from `agentos confg` (a typo that must not silently start the TUI).
saw_command=0
resume_path=""
resume_path_given=0
resume_decision="approve"
resume_reason=""
llm_provider="${AGENTOS_LLM_PROVIDER:-}"
llm_model="${AGENTOS_LLM_MODEL:-}"

cli_bin="$agentos_home/bin/agentos-cli"
gateway_bin="$agentos_home/bin/agentos-gateway"
tool_worker_bin="$agentos_home/bin/agentos-tool-worker"
gateway_pid_path="${AGENTOS_GATEWAY_PID_PATH:-$agentos_home/workspace/run/agentos-gateway.pid}"
gateway_log_path="${AGENTOS_GATEWAY_LOG_PATH:-$agentos_home/logs/agentos-gateway.log}"
cli_args=()

trim_spaces() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "$value"
}

secure_env_file_permissions() {
  local file="$1"
  local mode=""
  mode="$(stat -f '%Lp' "$file" 2>/dev/null || stat -c '%a' "$file" 2>/dev/null || true)"
  if [[ "$mode" =~ ^[0-9]+$ ]]; then
    local group_digit=$(((10#$mode / 10) % 10))
    local other_digit=$((10#$mode % 10))
    if (( group_digit != 0 || other_digit != 0 )); then
      chmod 600 "$file"
    fi
  fi
}

decode_env_value() {
  local value
  value="$(trim_spaces "$1")"
  if [[ "$value" == \"*\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "$value" == \'*\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s\n' "$value"
}

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  secure_env_file_permissions "$file"
  local line key raw_value value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="$(trim_spaces "$line")"
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ "$line" =~ ^export[[:space:]]+(.+)$ ]]; then
      line="${BASH_REMATCH[1]}"
    fi
    if [[ ! "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=(.*)$ ]]; then
      echo "Invalid .env entry: $line" >&2
      exit 2
    fi
    key="${BASH_REMATCH[1]}"
    raw_value="${BASH_REMATCH[2]}"
    value="$(decode_env_value "$raw_value")"
    if [[ "$allow_env_overrides" == "1" || -z "${!key+x}" ]]; then
      export "$key=$value"
    fi
  done <"$file"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$2" >&2
    exit 1
  fi
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "$2" >&2
    exit 1
  fi
}

infer_llm_provider() {
  if [[ -n "${OPENAI_API_KEY:-}" ]]; then
    echo "openai"
  elif [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "anthropic"
  elif [[ -n "${DEEPSEEK_API_KEY:-}" ]]; then
    echo "deepseek"
  elif [[ -n "${OLLAMA_HOST:-}" ]]; then
    echo "ollama"
  else
    echo "builtin.echo"
  fi
}

setup_llm_provider() {
  if [[ -z "$llm_provider" ]]; then
    llm_provider="$(infer_llm_provider)"
  fi
  export AGENTOS_LLM_PROVIDER="$llm_provider"
  [[ -n "$llm_model" ]] && export AGENTOS_LLM_MODEL="$llm_model"
  case "$llm_provider" in
    builtin.echo)
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-builtin.echo}"
      ;;
    openai)
      require_command curl "OpenAI provider requires curl"
      require_env OPENAI_API_KEY "OpenAI provider requires OPENAI_API_KEY"
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-gpt-5.4-mini}"
      ;;
    anthropic)
      require_command curl "Anthropic provider requires curl"
      require_env ANTHROPIC_API_KEY "Anthropic provider requires ANTHROPIC_API_KEY"
      ;;
    deepseek)
      require_command curl "DeepSeek provider requires curl"
      require_env DEEPSEEK_API_KEY "DeepSeek provider requires DEEPSEEK_API_KEY"
      ;;
    ollama)
      require_command curl "Ollama provider requires curl"
      export OLLAMA_HOST="${OLLAMA_HOST:-http://localhost:11434}"
      ;;
    *)
      echo "unknown AGENTOS_LLM_PROVIDER: $llm_provider" >&2
      exit 2
      ;;
  esac
}

usage() {
  cat <<'USAGE'
Usage: scripts/start-agentos.sh [COMMAND] [OPTIONS] [-- AGENTOS_ARGS...]

Commands:
  tui                   Start the interactive TUI. Default.
  telegram-once         Process one Telegram poll cycle.
  telegram-cron-smoke   Run one Telegram cron smoke task.
  feishu-once           Process one Feishu long-connection event.
  feishu-cron-smoke     Run one Feishu cron smoke task.
  gateway-start         Start the persistent gateway.
  gateway-restart       Restart the persistent gateway.
  gateway-stop          Stop the persistent gateway.
  gateway-status        Show gateway status.
  config                Show the effective configuration.
  resume [PATH]         Resume a paused run. Without PATH, the default
                        workspace/runs/cli-run-1.json is used.

Options:
  --env-file PATH       Load environment from PATH.
  --no-env-override     Keep already-exported shell variables over .env values.
  --llm-provider NAME   Override the LLM provider.
  --llm-model NAME      Override the LLM model.
  --reject [REASON]     Reject a paused run after resume.
  -h, --help            Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    tui|telegram-once|telegram-cron-smoke|feishu-once|feishu-cron-smoke)
      mode="$1"
      saw_command=1
      shift
      ;;
    gateway-start)
      mode="gateway-start"
      saw_command=1
      shift
      ;;
    gateway-restart)
      mode="gateway-restart"
      saw_command=1
      shift
      ;;
    gateway-stop)
      mode="gateway-stop"
      saw_command=1
      shift
      ;;
    gateway-status)
      mode="gateway-status"
      saw_command=1
      shift
      ;;
    config)
      mode="config"
      saw_command=1
      shift
      ;;
    resume)
      # `resume [PATH] [approve|reject [REASON]]` — every part optional. The
      # path used to be read as a bare `$2`, which died with "unbound variable"
      # under `set -u` for a plain `agentos resume`, even though the CLI already
      # defaults to workspace/runs/cli-run-1.json (M2 deliverable 4).
      mode="resume"
      saw_command=1
      shift
      if [[ $# -ge 1 && "$1" != --* && "$1" != "approve" && "$1" != "reject" ]]; then
        resume_path="$1"
        resume_path_given=1
        shift
      fi
      if [[ $# -ge 1 && ( "$1" == "approve" || "$1" == "reject" ) ]]; then
        resume_decision="$1"
        shift
        if [[ "$resume_decision" == "reject" && $# -ge 1 && "$1" != --* ]]; then
          resume_reason="$1"
          shift
        fi
      fi
      ;;
    --env-file)
      env_file="$2"
      shift 2
      ;;
    --no-env-override)
      allow_env_overrides=0
      shift
      ;;
    --llm-provider)
      llm_provider="$2"
      shift 2
      ;;
    --llm-model)
      llm_model="$2"
      shift 2
      ;;
    --reject)
      resume_decision="reject"
      if [[ $# -ge 2 && "$2" != --* ]]; then
        resume_reason="$2"
        shift 2
      else
        shift
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      cli_args+=("$@")
      break
      ;;
    -*)
      cli_args+=("$1")
      shift
      ;;
    *)
      # A bare word in first position is a command, and an unrecognised one is
      # an error. Previously every such word was appended to cli_args with mode
      # left at its default, so `agentos confg` silently started the TUI and the
      # "unknown command" branch below could never be reached.
      if [[ ${#cli_args[@]} -eq 0 && "$saw_command" == "0" ]]; then
        echo "unknown command: $1" >&2
        usage >&2
        exit 2
      fi
      cli_args+=("$1")
      shift
      ;;
  esac
done

load_env_file "$env_file"

# The shipped .env.example carries a bare `AGENTOS_HOME=` placeholder, and the
# loader above exports it — set, and empty. Every path this script derives is
# already anchored on the `$agentos_home` computed at the top, but the *binary*
# reads `AGENTOS_HOME` for itself and an empty one is no anchor at all: it
# resolved the workspace root relative to whatever directory the process
# started in. From `/`, `agentos tui` opened `/workspace/agentos.sqlite` — a
# database outside the install prefix entirely, which the clean-room release
# check had been quietly relying on. Re-export the anchor this script actually
# resolved (M3 deliverable 2).
export AGENTOS_HOME="${AGENTOS_HOME:-$agentos_home}"

export AGENTOS_AGENT_CONFIG_PATH="${AGENTOS_AGENT_CONFIG_PATH:-$agentos_home/workspace/agent.toml}"
export AGENTOS_SESSION_DB_PATH="${AGENTOS_SESSION_DB_PATH:-$agentos_home/workspace/agentos.sqlite}"
export AGENTOS_RUN_STATE_PATH="${AGENTOS_RUN_STATE_PATH:-$agentos_home/workspace/runs/cli-run-1.json}"
export AGENTOS_TOOL_WORKER_PATH="${AGENTOS_TOOL_WORKER_PATH:-$tool_worker_bin}"

mkdir -p "$(dirname "$AGENTOS_SESSION_DB_PATH")" "$(dirname "$AGENTOS_RUN_STATE_PATH")" "$(dirname "$gateway_pid_path")" "$(dirname "$gateway_log_path")"

setup_llm_provider

case "$mode" in
  gateway-start)
    exec "$gateway_bin" start --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  gateway-restart)
    exec "$gateway_bin" restart --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  gateway-stop)
    exec "$gateway_bin" stop --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  gateway-status)
    exec "$gateway_bin" status --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  config)
    # Through the wrapper, so diagnostics do not require the private
    # agentos-gateway binary to be on PATH (M2 deliverable 3).
    exec "$gateway_bin" config --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  tui)
    exec "$cli_bin" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  telegram-once|telegram-cron-smoke|feishu-once|feishu-cron-smoke)
    exec "$cli_bin" "$mode" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  resume)
    # Built up rather than interpolated: passing an empty "$resume_path" would
    # hand the CLI "" as the state path instead of letting it use its default.
    resume_argv=(resume)
    if [[ "$resume_path_given" == "1" ]]; then
      resume_argv+=("$resume_path")
    fi
    resume_argv+=("$resume_decision")
    if [[ "$resume_decision" == "reject" && -n "$resume_reason" ]]; then
      resume_argv+=("$resume_reason")
    fi
    exec "$cli_bin" "${resume_argv[@]}" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  *)
    echo "unknown command: $mode" >&2
    usage >&2
    exit 2
    ;;
esac
