#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${AGENTOS_ENV_FILE:-$root/.env}"
allow_env_overrides=1

discover_env_file() {
  local discovered="$env_file"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env-file)
        if [[ $# -lt 2 ]]; then
          echo "--env-file requires a path" >&2
          exit 2
        fi
        discovered="$2"
        shift 2
        ;;
      --env-file=*)
        discovered="${1#--env-file=}"
        shift
        ;;
      --no-env-override)
        shift
        ;;
      --)
        break
        ;;
      *)
        shift
        ;;
    esac
  done
  printf '%s\n' "$discovered"
}

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
      echo "Secured env file permissions: $file" >&2
    fi
  fi
}

decode_env_value() {
  local value
  value="$(trim_spaces "$1")"
  if [[ "$value" == \"*\" ]]; then
    if [[ "$value" != *\" || ${#value} -lt 2 ]]; then
      return 1
    fi
    value="${value:1:${#value}-2}"
  elif [[ "$value" == \'*\' ]]; then
    if [[ "$value" != *\' || ${#value} -lt 2 ]]; then
      return 1
    fi
    value="${value:1:${#value}-2}"
  fi
  printf '%s\n' "$value"
}

load_env_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    return
  fi

  secure_env_file_permissions "$file"

  local line key raw_value value lineno=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    lineno=$((lineno + 1))
    line="${line%$'\r'}"
    line="$(trim_spaces "$line")"
    if [[ -z "$line" || "$line" == \#* ]]; then
      continue
    fi
    if [[ "$line" =~ ^export[[:space:]]+(.+)$ ]]; then
      line="${BASH_REMATCH[1]}"
    fi
    if [[ ! "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=(.*)$ ]]; then
      echo "Invalid .env entry at $file:$lineno" >&2
      echo "Use KEY=value syntax with shell-safe variable names." >&2
      exit 2
    fi
    key="${BASH_REMATCH[1]}"
    raw_value="${BASH_REMATCH[2]}"
    if ! value="$(decode_env_value "$raw_value")"; then
      echo "Invalid quoted value at $file:$lineno" >&2
      exit 2
    fi
    if [[ "$allow_env_overrides" == "1" || -z "${!key+x}" ]]; then
      export "$key=$value"
    fi
  done <"$file"

  echo "Loaded environment file: $file" >&2
}

env_file="$(discover_env_file "$@")"
if [[ "${AGENTOS_NO_ENV_OVERRIDE:-}" == "1" ]]; then
  allow_env_overrides=0
fi
for arg in "$@"; do
  if [[ "$arg" == "--no-env-override" ]]; then
    allow_env_overrides=0
  fi
done
load_env_file "$env_file"

# shellcheck source=release-versions.env
source "$root/scripts/release-versions.env"
rust_toolchain="${AGENTOS_RUST_TOOLCHAIN}"
mode="tui"
build_first=1
gateway_command="${AGENTOS_GATEWAY_COMMAND:-}"
gateway_bin="${AGENTOS_GATEWAY_BIN:-$root/target/debug/agentos-gateway}"
gateway_action="${AGENTOS_GATEWAY_ACTION:-restart}"
gateway_pid_path="${AGENTOS_GATEWAY_PID_PATH:-$root/workspace/run/agentos-gateway.pid}"
gateway_log_path="${AGENTOS_GATEWAY_LOG_PATH:-$root/logs/agentos-gateway.log}"
llm_provider="${AGENTOS_LLM_PROVIDER:-}"
llm_model="${AGENTOS_LLM_MODEL:-}"

cargo_for_toolchain() {
  rustup run "$rust_toolchain" cargo "$@"
}

run_agentos() {
  if [[ ${#cli_args[@]} -gt 0 ]]; then
    rustup run "$rust_toolchain" cargo run \
      --locked \
      --manifest-path "$root/Cargo.toml" \
      -p agentos-cli \
      -- "$@" "${cli_args[@]}"
    return $?
  fi

  rustup run "$rust_toolchain" cargo run \
    --locked \
    --manifest-path "$root/Cargo.toml" \
    -p agentos-cli \
    -- "$@"
}

start_gateway() {
  if [[ -n "$gateway_command" ]]; then
    mkdir -p "$(dirname "$gateway_pid_path")"
    mkdir -p "$(dirname "$gateway_log_path")"

    if [[ -f "$gateway_pid_path" ]]; then
      existing_pid="$(cat "$gateway_pid_path")"
      if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
        export AGENTOS_GATEWAY_MODE="${AGENTOS_GATEWAY_MODE:-external-persistent}"
        export AGENTOS_GATEWAY_PID="$existing_pid"
        export AGENTOS_GATEWAY_PID_PATH="$gateway_pid_path"
        export AGENTOS_GATEWAY_LOG_PATH="$gateway_log_path"
        echo "Using existing AgentOS gateway service: pid $existing_pid" >&2
        return
      fi
      echo "Removing stale AgentOS gateway pid file: $gateway_pid_path" >&2
      rm -f "$gateway_pid_path"
    fi

    echo "Starting persistent AgentOS gateway service: $gateway_command" >&2
    nohup bash -lc "$gateway_command" >>"$gateway_log_path" 2>&1 </dev/null &
    gateway_pid="$!"
    echo "$gateway_pid" >"$gateway_pid_path"
    sleep 0.2
    if ! kill -0 "$gateway_pid" >/dev/null 2>&1; then
      echo "AgentOS gateway service exited during startup; see $gateway_log_path" >&2
      rm -f "$gateway_pid_path"
      exit 1
    fi

    export AGENTOS_GATEWAY_MODE="${AGENTOS_GATEWAY_MODE:-external-persistent}"
    export AGENTOS_GATEWAY_PID="$gateway_pid"
    export AGENTOS_GATEWAY_PID_PATH="$gateway_pid_path"
    export AGENTOS_GATEWAY_LOG_PATH="$gateway_log_path"
    echo "AgentOS gateway service started: pid $gateway_pid, log $gateway_log_path" >&2
    return
  fi

  if [[ ! -x "$gateway_bin" ]]; then
    echo "AgentOS gateway binary not found: $gateway_bin" >&2
    echo "Run without --no-build, or set AGENTOS_GATEWAY_BIN to an executable gateway binary." >&2
    exit 1
  fi

  case "$gateway_action" in
    start|restart) ;;
    *)
      echo "unknown AGENTOS_GATEWAY_ACTION: $gateway_action" >&2
      exit 2
      ;;
  esac

  "$gateway_bin" "$gateway_action" \
    --pid-path "$gateway_pid_path" \
    --log-path "$gateway_log_path" \
    --config "$AGENTOS_AGENT_CONFIG_PATH" \
    --session-db-path "$AGENTOS_SESSION_DB_PATH"
  export AGENTOS_GATEWAY_MODE="${AGENTOS_GATEWAY_MODE:-external-persistent}"
  if [[ -f "$gateway_pid_path" ]]; then
    export AGENTOS_GATEWAY_PID="$(cat "$gateway_pid_path")"
  fi
  export AGENTOS_GATEWAY_PID_PATH="$gateway_pid_path"
  export AGENTOS_GATEWAY_LOG_PATH="$gateway_log_path"
}

setup_llm_provider() {
  if [[ -z "$llm_provider" ]]; then
    llm_provider="$(infer_llm_provider)"
  fi
  export AGENTOS_LLM_PROVIDER="$llm_provider"
  if [[ -n "$llm_model" ]]; then
    export AGENTOS_LLM_MODEL="$llm_model"
  fi

  case "$llm_provider" in
    builtin.echo)
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-builtin.echo}"
      ;;
    openai)
      require_command curl "OpenAI provider requires curl"
      require_secret_env OPENAI_API_KEY "AGENTOS_LLM_PROVIDER=openai requires OPENAI_API_KEY"
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-gpt-5.4-mini}"
      ;;
    anthropic)
      require_command curl "Anthropic provider requires curl"
      require_secret_env ANTHROPIC_API_KEY "AGENTOS_LLM_PROVIDER=anthropic requires ANTHROPIC_API_KEY"
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-claude-sonnet-4-5}"
      ;;
    deepseek)
      require_command curl "DeepSeek provider requires curl"
      require_secret_env DEEPSEEK_API_KEY "AGENTOS_LLM_PROVIDER=deepseek requires DEEPSEEK_API_KEY"
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-deepseek-chat}"
      ;;
    ollama)
      require_command curl "Ollama provider requires curl"
      export OLLAMA_HOST="${OLLAMA_HOST:-http://localhost:11434}"
      export AGENTOS_LLM_MODEL="${AGENTOS_LLM_MODEL:-llama3.2}"
      ;;
    *)
      echo "unknown AGENTOS_LLM_PROVIDER: $llm_provider" >&2
      exit 2
      ;;
  esac

  echo "Configured LLM provider: $AGENTOS_LLM_PROVIDER ($AGENTOS_LLM_MODEL)" >&2
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

setup_channels() {
  export AGENTOS_CHANNEL_MODE="$mode"
  case "$mode" in
    tui|resume)
      export AGENTOS_ENABLED_CHANNELS="${AGENTOS_ENABLED_CHANNELS:-tui}"
      ;;
    telegram-once|telegram-cron-smoke)
      require_command curl "Telegram channel requires curl"
      require_env AGENTOS_TELEGRAM_BOT_TOKEN "Telegram channel requires AGENTOS_TELEGRAM_BOT_TOKEN"
      if [[ "$mode" == "telegram-cron-smoke" ]]; then
        require_env AGENTOS_TELEGRAM_CHAT_ID "telegram-cron-smoke requires AGENTOS_TELEGRAM_CHAT_ID"
      fi
      export AGENTOS_ENABLED_CHANNELS="${AGENTOS_ENABLED_CHANNELS:-telegram}"
      ;;
    feishu-once|feishu-cron-smoke)
      require_command curl "Feishu channel requires curl"
      require_command openssl "Feishu long-connection channel requires openssl"
      require_env AGENTOS_FEISHU_APP_ID "Feishu channel requires AGENTOS_FEISHU_APP_ID"
      require_env AGENTOS_FEISHU_APP_SECRET "Feishu channel requires AGENTOS_FEISHU_APP_SECRET"
      if [[ "$mode" == "feishu-cron-smoke" ]]; then
        require_env AGENTOS_FEISHU_ALLOWED_ID "feishu-cron-smoke requires AGENTOS_FEISHU_ALLOWED_ID"
      fi
      export AGENTOS_ENABLED_CHANNELS="${AGENTOS_ENABLED_CHANNELS:-feishu}"
      ;;
  esac

  echo "Configured channels: $AGENTOS_ENABLED_CHANNELS" >&2
  validate_enabled_channels
}

validate_enabled_channels() {
  if [[ ",$AGENTOS_ENABLED_CHANNELS," == *,telegram,* ]]; then
    require_command curl "Telegram channel requires curl"
    require_secret_env AGENTOS_TELEGRAM_BOT_TOKEN "Telegram channel requires AGENTOS_TELEGRAM_BOT_TOKEN"
  fi
  if [[ ",$AGENTOS_ENABLED_CHANNELS," == *,feishu,* ]]; then
    require_command curl "Feishu channel requires curl"
    require_command openssl "Feishu long-connection channel requires openssl"
    require_secret_env AGENTOS_FEISHU_APP_ID "Feishu channel requires AGENTOS_FEISHU_APP_ID"
    require_secret_env AGENTOS_FEISHU_APP_SECRET "Feishu channel requires AGENTOS_FEISHU_APP_SECRET"
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$2" >&2
    exit 1
  fi
}

require_env() {
  local name="$1"
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    echo "$2" >&2
    exit 1
  fi
}

require_secret_env() {
  require_env "$1" "$2"
  local name="$1"
  local value="${!name:-}"
  if is_placeholder_secret "$value"; then
    echo "$name is still a placeholder in $env_file" >&2
    exit 1
  fi
}

is_placeholder_secret() {
  local value
  value="$(printf '%s\n' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    your_*|*_here|replace-me|replace_me|changeme|change-me|todo|none|null|placeholder|sk-...|proj_...|org_...)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

usage() {
  cat <<'USAGE'
Usage: scripts/bootstrap-agentos.sh [OPTIONS] [-- AGENTOS_ARGS...]

Builds and launches AgentOS from the local workspace.

Options:
  --mode tui                  Launch the terminal channel. Default.
  --mode telegram-once        Poll Telegram once and process one inbound message.
  --mode telegram-cron-smoke  Enqueue and post one Telegram cron smoke message.
  --mode feishu-once          Wait for one Feishu long-connection event and process it.
  --mode feishu-cron-smoke    Enqueue and post one Feishu cron smoke message.
  --env-file PATH             Load local environment file. Default: .env
  --no-env-override           Keep already-exported shell variables over .env values.
  --gateway-command COMMAND   Start COMMAND as a persistent gateway service override.
  --gateway-bin PATH          Gateway binary. Default: target/debug/agentos-gateway
  --gateway-action ACTION     start or restart managed gateway. Default: restart
  --gateway-pid-path PATH     Gateway PID file. Default: workspace/run/agentos-gateway.pid
  --gateway-log-path PATH     Gateway log file. Default: logs/agentos-gateway.log
  --llm-provider PROVIDER     Configure builtin.echo, openai, anthropic, deepseek, or ollama.
  --llm-model MODEL           Configure the provider model name.
  --resume PATH               Resume a paused run from PATH.
  --reject [REASON]           Reject the paused run when used with --resume.
  --no-build                  Skip the initial cargo build.
  -h, --help                  Show this help.

Environment:
  AGENTOS_ENV_FILE            Local environment file path. Default: .env
  AGENTOS_NO_ENV_OVERRIDE     Set to 1 to keep shell variables over .env values.
  AGENTOS_RUST_TOOLCHAIN      Exact Rust toolchain to use.
  AGENTOS_AGENT_CONFIG_PATH   Workspace config path. Default: workspace/agent.toml
  AGENTOS_SESSION_DB_PATH     Session SQLite path. Default: workspace/agentos.sqlite
  AGENTOS_RUN_STATE_PATH      Paused run path. Default: workspace/runs/cli-run-1.json
  AGENTOS_TOOL_WORKER_PATH    Tool worker binary path. Default: target/debug/agentos-tool-worker
  AGENTOS_GATEWAY_COMMAND     Optional persistent gateway service command override.
  AGENTOS_GATEWAY_BIN         Gateway binary path.
  AGENTOS_GATEWAY_ACTION      start or restart. Default: restart.
  AGENTOS_GATEWAY_PID_PATH    Gateway service PID file.
  AGENTOS_GATEWAY_LOG_PATH    Gateway service log file.
  AGENTOS_LLM_PROVIDER        LLM provider. Inferred from API keys, else builtin.echo.
  AGENTOS_LLM_MODEL           Model name for the selected provider.
  AGENTOS_CLI_TRACE           Set to 1 to print per-turn TUI trace diagnostics.
  OPENAI_API_KEY              Required when AGENTOS_LLM_PROVIDER=openai.
  OPENAI_PROJECT_ID           Optional OpenAI project ID for legacy/user keys.
  OPENAI_PROJECT              Optional alias for OPENAI_PROJECT_ID.
  OPENAI_ORG_ID               Optional OpenAI organization ID.
  OPENAI_ORGANIZATION         Optional alias for OPENAI_ORG_ID.
  ANTHROPIC_API_KEY           Required when AGENTOS_LLM_PROVIDER=anthropic.
  DEEPSEEK_API_KEY            Required when AGENTOS_LLM_PROVIDER=deepseek.
  DEEPSEEK_BASE_URL           Optional DeepSeek endpoint. Default: https://api.deepseek.com
  DEEPSEEK_HOST               Optional alias for DEEPSEEK_BASE_URL.
  AGENTOS_DEEPSEEK_BASE_URL   Optional AgentOS-specific DeepSeek endpoint override.
  OLLAMA_HOST                 Ollama endpoint. Default: http://localhost:11434

Extra arguments after -- are passed to agentos-cli.
USAGE
}

cli_args=()
resume_path=""
resume_decision="approve"
resume_reason=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      if [[ $# -lt 2 ]]; then
        echo "--env-file requires a path" >&2
        exit 2
      fi
      env_file="$2"
      shift 2
      ;;
    --env-file=*)
      env_file="${1#--env-file=}"
      shift
      ;;
    --no-env-override)
      allow_env_overrides=0
      shift
      ;;
    --mode)
      if [[ $# -lt 2 ]]; then
        echo "--mode requires a value" >&2
        exit 2
      fi
      mode="$2"
      shift 2
      ;;
    --gateway-command)
      if [[ $# -lt 2 ]]; then
        echo "--gateway-command requires a command" >&2
        exit 2
      fi
      gateway_command="$2"
      shift 2
      ;;
    --gateway-bin)
      if [[ $# -lt 2 ]]; then
        echo "--gateway-bin requires a path" >&2
        exit 2
      fi
      gateway_bin="$2"
      shift 2
      ;;
    --gateway-action)
      if [[ $# -lt 2 ]]; then
        echo "--gateway-action requires start or restart" >&2
        exit 2
      fi
      gateway_action="$2"
      shift 2
      ;;
    --gateway-pid-path)
      if [[ $# -lt 2 ]]; then
        echo "--gateway-pid-path requires a path" >&2
        exit 2
      fi
      gateway_pid_path="$2"
      shift 2
      ;;
    --gateway-log-path)
      if [[ $# -lt 2 ]]; then
        echo "--gateway-log-path requires a path" >&2
        exit 2
      fi
      gateway_log_path="$2"
      shift 2
      ;;
    --llm-provider)
      if [[ $# -lt 2 ]]; then
        echo "--llm-provider requires a provider" >&2
        exit 2
      fi
      llm_provider="$2"
      shift 2
      ;;
    --llm-model)
      if [[ $# -lt 2 ]]; then
        echo "--llm-model requires a model" >&2
        exit 2
      fi
      llm_model="$2"
      shift 2
      ;;
    --resume)
      if [[ $# -lt 2 ]]; then
        echo "--resume requires a path" >&2
        exit 2
      fi
      resume_path="$2"
      mode="resume"
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
    --no-build)
      build_first=0
      shift
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
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$mode" in
  tui|telegram-once|telegram-cron-smoke|feishu-once|feishu-cron-smoke|resume) ;;
  *)
    echo "unknown mode: $mode" >&2
    usage >&2
    exit 2
    ;;
esac

export AGENTOS_AGENT_CONFIG_PATH="${AGENTOS_AGENT_CONFIG_PATH:-$root/workspace/agent.toml}"
export AGENTOS_SESSION_DB_PATH="${AGENTOS_SESSION_DB_PATH:-$root/workspace/agentos.sqlite}"
export AGENTOS_RUN_STATE_PATH="${AGENTOS_RUN_STATE_PATH:-$root/workspace/runs/cli-run-1.json}"
export AGENTOS_TOOL_WORKER_PATH="${AGENTOS_TOOL_WORKER_PATH:-$root/target/debug/agentos-tool-worker}"

mkdir -p "$(dirname "$AGENTOS_SESSION_DB_PATH")"
mkdir -p "$(dirname "$AGENTOS_RUN_STATE_PATH")"

setup_llm_provider
setup_channels

if [[ "$build_first" == "1" ]]; then
  cargo_for_toolchain build \
    --manifest-path "$root/Cargo.toml" \
    -p agentos-cli \
    -p agentos-core \
    --bins
fi

start_gateway

case "$mode" in
  tui)
    run_agentos
    ;;
  telegram-once|telegram-cron-smoke|feishu-once|feishu-cron-smoke)
    run_agentos "$mode"
    ;;
  resume)
    if [[ -z "$resume_path" ]]; then
      resume_path="$AGENTOS_RUN_STATE_PATH"
    fi
    if [[ "$resume_decision" == "reject" && -n "$resume_reason" ]]; then
      run_agentos resume "$resume_path" reject "$resume_reason"
      exit $?
    fi
    run_agentos resume "$resume_path" "$resume_decision"
    ;;
esac
