#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agentos_home="${AGENTOS_HOME:-$(cd "$script_dir/.." && pwd)}"
env_file="${AGENTOS_ENV_FILE:-$agentos_home/.env}"
allow_env_overrides=1
mode="tui"
resume_path=""
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
  config                Show effective runtime configuration and sandbox status.
  resume [PATH]         Resume a paused run; defaults to the standard state path.

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
      shift
      ;;
    gateway-start)
      mode="gateway-start"
      shift
      ;;
    gateway-restart)
      mode="gateway-restart"
      shift
      ;;
    gateway-stop)
      mode="gateway-stop"
      shift
      ;;
    gateway-status)
      mode="gateway-status"
      shift
      ;;
    config)
      mode="config"
      shift
      ;;
    resume)
      mode="resume"
      shift
      if [[ $# -gt 0 && "$1" != --* ]]; then
        resume_path="$1"
        shift
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
    *)
      cli_args+=("$1")
      shift
      ;;
  esac
done

load_env_file "$env_file"

# The selected home locates this script, its binaries, and its .env. Keep that
# anchor authoritative even when the example .env contains `AGENTOS_HOME=`.
export AGENTOS_HOME="$agentos_home"

export AGENTOS_AGENT_CONFIG_PATH="${AGENTOS_AGENT_CONFIG_PATH:-$agentos_home/workspace/agent.toml}"
export AGENTOS_SESSION_DB_PATH="${AGENTOS_SESSION_DB_PATH:-$agentos_home/workspace/agentos.sqlite}"
export AGENTOS_RUN_STATE_PATH="${AGENTOS_RUN_STATE_PATH:-$agentos_home/workspace/runs/cli-run-1.json}"
export AGENTOS_TOOL_WORKER_PATH="${AGENTOS_TOOL_WORKER_PATH:-$tool_worker_bin}"
if [[ "$mode" == "resume" && -z "$resume_path" ]]; then
  resume_path="$AGENTOS_RUN_STATE_PATH"
fi

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
    exec "$gateway_bin" config --pid-path "$gateway_pid_path" --log-path "$gateway_log_path" --config "$AGENTOS_AGENT_CONFIG_PATH" --session-db-path "$AGENTOS_SESSION_DB_PATH"
    ;;
  tui)
    exec "$cli_bin" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  telegram-once|telegram-cron-smoke|feishu-once|feishu-cron-smoke)
    exec "$cli_bin" "$mode" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  resume)
    if [[ "$resume_decision" == "reject" && -n "$resume_reason" ]]; then
      exec "$cli_bin" resume "$resume_path" reject "$resume_reason" ${cli_args[@]+"${cli_args[@]}"}
    fi
    exec "$cli_bin" resume "$resume_path" "$resume_decision" ${cli_args[@]+"${cli_args[@]}"}
    ;;
  *)
    echo "unknown command: $mode" >&2
    usage >&2
    exit 2
    ;;
esac
