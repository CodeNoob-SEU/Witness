#!/usr/bin/env bash
# usage: run.sh <subcommand> [args...]   (wraps swe_harness.py with pinned env)
set -euo pipefail
export WITNESS_SWE_ROOT="$HOME/witness-swebench"
export WITNESS_SWE_IMAGE="docker.1panel.live/swebench/sweb.eval.x86_64.pytest-dev_1776_pytest-7490:latest"
export REACT_AGENT_POSTGRES_DSN="postgresql://witness:witness@127.0.0.1:55990/witness"
export OPENAI_BASE_URL="https://su.kelaode.sbs:8443/v1"
export WITNESS_MODEL="${WITNESS_MODEL:-gpt-5.5}"
export WITNESS_REASONING_EFFORT="${WITNESS_REASONING_EFFORT:-high}"
set -a; source "$HOME/witness-swebench/.secrets.env"; set +a
cd "$WITNESS_SWE_ROOT/witness"
export PATH="$HOME/.local/bin:$PATH"
if [[ "${WITNESS_OTEL:-0}" == "1" ]]; then
  # Export to the local Collector (docker-compose.observability.yml). The
  # adapter's allowlist keeps prompts, tool arguments/results and reasoning out.
  export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-witness-swebench}"
  export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318}"
  export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
  export OTEL_TRACES_EXPORTER=otlp OTEL_METRICS_EXPORTER=otlp OTEL_LOGS_EXPORTER=otlp
  export OTEL_METRIC_EXPORT_INTERVAL="${OTEL_METRIC_EXPORT_INTERVAL:-15000}"
  export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
  exec uv run --extra dev --extra debug --extra otel opentelemetry-instrument \
    python "$WITNESS_SWE_ROOT/harness/swe_harness.py" "$@"
fi
exec uv run --extra dev --extra debug python "$WITNESS_SWE_ROOT/harness/swe_harness.py" "$@"
