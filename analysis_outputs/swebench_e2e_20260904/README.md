# SWE-bench end-to-end runs on server3 (2026-09-04)

Instance: `pytest-dev__pytest-7490` (SWE-bench Verified, "15 min - 1 hour").
Model: `gpt-5.5` via an OpenAI-compatible relay, Responses API, reasoning effort high.
Runtime: `AgentRuntime` + `PostgresRunJournal` + `GitWorktreeWorkspace`; tests executed
inside the official SWE-bench image with the worktree bind-mounted (`ContainerCommandRunner`).

| Run | Tools | Crashes injected | Result |
| --- | --- | --- | --- |
| `run_13016e96` | harness-defined (pre-module) | SIGKILL during `run_tests`, SIGKILL during model call | 400 events, 3 executions, FAIL_TO_PASS 2/2, PASS_TO_PASS 78/78 |
| `run_16393f75_builtin_tools` | `react_agent.repo_tools` (built-in) | SIGKILL during `run_tests` | 392 events, 2 executions, FAIL_TO_PASS 2/2, PASS_TO_PASS 78/78 |

`harness/swe_harness.py` is the current harness (built-in tools). `logs/provider_http.ndjson`
is the diagnostic capture that exposed the `parsed_arguments` replay bug fixed in
`provider.py`. Run 2's evaluation copy had to be seeded with the gitignored
`src/_pytest/_version.py` (see `evaluation.json` note) because Git worktrees only
materialize tracked files.

## 2026-09-04 afternoon: OpenTelemetry end-to-end + Tier 2 pressure runs

Same instance, model and relay. Three more runs, all with the Collector + Jaeger + Prometheus stack
from `docker-compose.observability.yml` running on server3 (`WITNESS_OTEL=1` in `harness/run.sh`
→ `opentelemetry-instrument`, `WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT=100` on the Collector).
Per-run directories hold `report.json`, `evaluation.json`, `model_patch.diff`,
`events.public.ndjson`, worker/supervisor logs, and `otel/`: `jaeger_traces.json` (every trace
tagged with the run id, exported from Jaeger's API), `collector_metrics.prom` (the Collector's
Prometheus endpoint), `prometheus_queries.json`, `stack.txt`.

| Run | `max_context_chars` | Crashes / outages | Steps · model calls (of which Tier 2 compressions) · hard fallbacks | Peak projected chars | Wall | Result |
| --- | --- | --- | --- | --- | --- | --- |
| `run_bd04f7b3_tier2_60k_otel` | 60,000 | SIGKILL at step 17; two real provider 503 storms (steps 18 and 32) | 40 · 96 (**44**) · **38** | 236,164 | 93 min, then aborted by operator | patch at step 40: FAIL_TO_PASS 0/2, PASS_TO_PASS 78/78 |
| `run_51a838d6_120k_otel` | 120,000 | none | 22 · 22 (0) · 0 | 86,050 | 3.5 min | **resolved** 2/2 + 78/78 |
| `run_2f166b1b_120k_crash_otel` | 120,000 | SIGKILL during the step-6 model call; supervisor resumed 29 s later | 33 · 33 (0) · 0 | 125,951 (73 deterministic evictions) | 6.0 min | **resolved** 2/2 + 78/78 |

Journal numbers above come from the PostgreSQL journal (`events.public.ndjson` is the public
projection and omits `compression_calls` / `input_chars`).

### What the Tier 2 run answered

At 60k chars the governor is pathological, not merely lossy: 44 generative compressions
(gpt-5.5, ~104 s each) consumed 64 of the first 89 minutes, and 38 of them still could not meet
the budget and ended in a hard fallback. The model's own turns took 4.3 minutes in total. At 120k
the same task never needed a compression call (Tier 1 eviction alone absorbed a 126k peak) and
resolved in 3.5–6 minutes. Decision: keep Tier 2 as the safety net it is, do not lower the default
budget, and do not invest further in compression quality before a task actually needs it.

### What the outages answered

The 60k run hit two genuine relay outages (`503 server_is_overloaded`; the raw status/timing
capture is in `logs/provider_http_503_window.ndjson`, 45 rows). Both produced exactly the journal
shape `af03f91` was written for: four `model_failed(retryable=true, terminal_decision=false)`
attempts with 2/4/8 s backoff, then `retry_exhausted=true`, no `run.completed`, lease released,
`run_resumed(resume_reason=model_retry)` at the **same step**. The first outage exposed a harness
bug — the `supervise` subcommand stopped sweeping after its first Resume, so the run sat idle for
18 minutes until a fresh supervisor was started; `swe_harness.py` now keeps sweeping until the run
is terminal, and the second outage cost under a minute (resumed 1 s after the lease release,
again 30 s later when the provider was still down).

### What OpenTelemetry answered

- Traces: the crash run produces two traces, the killed execution (child spans only — its
  `invoke_agent` root never ended) and the resumed execution whose root carries
  `react_agent.execution.kind=resume` and a link to the first. Model spans carry
  `react_agent.model.status_code/error_code/retryable/retry_exhausted`.
- Metrics: `gen_ai.client.token.usage`, `gen_ai.client.operation.duration`,
  `gen_ai.execute_tool.operation.count` by tool, `react_agent.run.resume.count`. The Collector's
  Prometheus exporter expires series a few minutes after a worker process exits, so
  `prometheus_queries.json` reflects the live window at query time, not run totals; the journal is
  the source of truth for totals.
- Two findings fixed in `e1b805d`: tail sampling decides 5 s after the *first* span of a trace
  arrives, long before the root span (the only one that carried `execution.kind`) ends, so the
  shipped keep-recovery policy could never match a real run — child spans now repeat the attribute.
  And the success sampling percentage is now an environment variable (10 by default, 100 here).
- Not fixed: the `httpx` auto-instrumentation spans (`POST …/v1/responses`) are separate
  single-span traces rather than children of `chat`, because the adapter starts spans without
  activating them as the current context. They carry only method/URL/status and are excluded from
  the exported `jaeger_traces.json`.

## Tier 2 redesign, replayed offline on the 60k run (`replay_tier2_working_state/`)

The 60k run above is also the test bed for the form-style working state (`0fca6cd`). The
per-step canonical transcripts are in that run's `checkpoint(phase=before_model)` facts, so
`harness/replay_context.py` re-runs `ContextGovernor.prepare` on all 40 of them with the same
60,000-char budget — identical inputs, only the governor differs. `replay_60k_fake/` uses a
deterministic notes stub (measures the algorithm), `replay_60k_gpt54mini/` uses `gpt-5.4-mini` as
the notes model (measures latency and form quality; every rendered form is in `replay_forms.json`).

| | old prose Tier 2 (live run, gpt-5.5) | working state, replay (gpt-5.4-mini) |
| --- | --- | --- |
| compression calls over 40 steps | 44 | 35 (one per step over budget) |
| chars sent per compression | 35k at step 6 → **202k** at step 40 (whole raw prefix, every step) | mean **5.9k**, max 35.5k (first fold only); e.g. 614 at step 33 |
| seconds per compression | **104** mean | **7.3** mean |
| compression wall time | 64 min | 4.2 min |
| compression tokens | (dominant share of 1.76M input) | 75k total |
| hard fallbacks | 38 (recent tool outputs blanked) | **0** |
| final projected chars | 34k–54k | 23k–56k |
| cache hits on identical prefix | never (key changed every step) | by construction (chain hash; see tests) |

What the form looks like at step 40 (`replay_60k_gpt54mini/replay_forms.json`): the verbatim
goal; a ledger that says, mechanically, `read_file src/_pytest/skipping.py […] x26`,
`run_tests args=testing/test_skipping.py -q: exit 0`; and notes with four findings, a hypothesis,
four next steps and three open questions, all checkable against the transcript. The `x26` is itself
a diagnosis of the old design: with recent reads blanked by the hard fallback, the model re-read
the same file twenty-six times.

Caveat: this replays the *inputs* of the old run; a model steering with the new form would have
produced a different trajectory. A live 60k run with the new Tier 2 is the remaining experiment,
and it is now cheap enough to be worth it.
