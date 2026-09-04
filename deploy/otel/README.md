# Local observability stack

Install the optional Python adapter first:

```bash
uv sync --extra otel
```

Start the pinned Collector, Jaeger, and Prometheus stack from the repository root:

```bash
docker compose -f docker-compose.observability.yml up -d
```

Check the rendered Compose configuration and service health without exposing any
application secrets:

```bash
docker compose -f docker-compose.observability.yml config
docker compose -f docker-compose.observability.yml ps
curl -fsS http://127.0.0.1:13133/
curl -fsS http://127.0.0.1:9090/-/ready
```

Endpoints:

- OTLP/gRPC: `127.0.0.1:4317`
- OTLP/HTTP: `http://127.0.0.1:4318`
- Collector health: `http://127.0.0.1:13133`
- Jaeger UI: `http://127.0.0.1:16686`
- Prometheus UI: `http://127.0.0.1:9090`

The Python module has no mandatory OpenTelemetry dependency. Configure OTLP and initialize an
SDK/provider before starting the web process:

```bash
export OTEL_SERVICE_NAME=react-agent
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
# Application-side bounded metric dimensions for this acceptance profile.
export REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS=gpt-5.6-terra
export REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS=calculate_expression
# Keep loopback OTLP traffic away from any shell-wide HTTP proxy.
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
uv run opentelemetry-instrument react-agent-web
```

The exporter environment variables do not initialize a provider by themselves. The
`opentelemetry-instrument` command above installs the SDK/provider for the process; an application
that already owns provider initialization may instead call its normal bootstrap before
`create_telemetry()`. If the OTel API package is unavailable, `create_telemetry()` returns the NoOp
adapter; if the API exists but no SDK/provider was initialized, the API objects remain effectively
no-op and no data is exported.

The two `REACT_AGENT_OTEL_METRIC_ALLOWED_*` variables belong to the Web process,
not the Collector, so they intentionally do not appear in
`docker-compose.observability.yml`. When they are absent, the Web Runtime freezes
the configured model and its startup tool registry as the finite defaults. An
override is an exact comma-separated allowlist (maximum 64 values, 128 characters
per value); blank entries, control characters, and wildcards fail startup rather
than enabling arbitrary metric labels.

To follow Collector logs or stop the local stack:

```bash
docker compose -f docker-compose.observability.yml logs -f otel-collector
docker compose -f docker-compose.observability.yml down
```

## Sampling and privacy

The local Collector uses tail sampling. Traces containing an error status, a
Resume root (`react_agent.execution.kind=resume`), budget exhaustion, or loop
detection are retained deterministically. Other successful traces use a 10%
baseline sample. This is a single-Collector development example; a production
multi-Collector deployment must route every span from one trace to the same
tail-sampling Collector.

Tail sampling decides `decision_wait` (5 s) after the *first* span of a trace
arrives. In a 30-minute run that first span is an early `chat` or
`execute_tool` span; the `invoke_agent` root only arrives when the execution
ends, so a policy that keys on a root-only attribute would never match. The
adapter therefore repeats `react_agent.execution.kind` on every child span, and
a resumed execution starts a new trace (linked to the crashed one) whose first
span already says `resume`. Errors are kept the same way: the failing `chat`
span carries the ERROR status itself. Unremarkable successful traces are
sampled at `WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT` (default 10); set it to 100 on
the Collector container when you need every trace of an evidence run:

```bash
WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT=100 docker compose -f docker-compose.observability.yml up -d
```

Prompt text, system instructions, tool arguments/results, reasoning, opaque
provider state, and credentials are not exported. Identifiers such as
`run_id`, `request_id`, and `tool_call_id` may correlate spans and logs but are
never metric dimensions. Replay mode suppresses all telemetry. Keep the same
allowlist at any log/trace gateway added downstream; the local `debug` exporter
is intended only for the already-sanitized application telemetry.

The adapter freezes the OpenTelemetry GenAI Development contract reviewed on
2026-08-20 as `development-snapshot-2026-08-20`. Golden tests cover the
`gen_ai.client.*`, `gen_ai.invoke_agent.*`, and `gen_ai.execute_tool.*` names;
TTFC, cost, recovery, and policy signals remain under `react_agent.*` until
stable standard fields exist.

Useful Prometheus queries (the exporter normalizes dots to underscores):

```promql
sum(rate(react_agent_run_resume_count_total[5m]))

histogram_quantile(
  0.95,
  sum by (le) (
    rate(react_agent_gen_ai_client_time_to_first_content_seconds_bucket[5m])
  )
)
```
