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
produced a different trajectory. The live runs below close that gap.

## Live 60k runs with the working-state Tier 2 (`run_9c94f307_60k_form_otel`, `run_d9b1a4dc_60k_form_ledger_only`)

Same instance, budget and relay as the 60k pressure run; `gpt-5.5` steers, `gpt-5.4-mini` writes
the notes (`WITNESS_COMPRESSION_MODEL`), OTel on. Two runs, because the first one found a bug:

| Run | What the model saw | Steps | Model calls (of which compressions) | Chars per compression | s per compression | Compression tokens | Total input tokens | Hard fallbacks | Wall | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `run_bd04f7b3_tier2_60k_otel` (old prose Tier 2, for reference) | prose summary, newest tool outputs blanked | 40 (aborted) | 96 (44) | 35k → 202k | 104 | ~1.5M | 1.76M | 38 | 93 min | 0/2 |
| `run_d9b1a4dc_60k_form_ledger_only` | goal + mechanical ledger only — notes were generated but **never persisted** (see below), so every step recompressed the whole prefix and no notes ever reached the model | 60 (`max_steps`) | 115 (55) | mean 129k, max 211k | 16.2 | 1.07M | 1.47M | 0 | 21 min | **0/2**, 78/78 |
| `run_9c94f307_60k_form_otel` | goal + ledger + incrementally updated notes | 60 (`max_steps`) | 112 (52) | **mean 5.3k**, max 45k (first fold) | **7.9** | **110k** | **549k** | 0 | 15 min | **resolved** 2/2 + 78/78 |

The bug: the harness pre-created `context-summaries/` with the default mode (0755), and
`FileContextSummaryStore` refused every write as "not private" — but only at the first `put`, where
the refusal degraded into a per-step `compression_error=ValueError` in the journal. No run today
had persisted a single summary; the old design's "never a cache hit" was partly this too. `3b41bbb`
makes the store reject a world-readable root at construction and the harness create it 0700.

What the two live runs say:

- With the notes actually reaching the model, the same 60k budget that was pathological in the
  morning **resolved the task** — the hidden tests pass on the workspace at step 60 — while spending
  a third of the input tokens of the ledger-only run and a tenth of its compression tokens. The
  agent still ran out of steps (it kept re-running its selected tests rather than answering), so
  `stop_reason=max_steps`; the 120k runs finished in 22–33 steps. A 60k budget with 30k-char tool
  outputs and three raw recent turns forces re-reading (31 `read_file` calls, `x14` on
  `skipping.py` in the final ledger); that is the remaining cost of the small budget, not of Tier 2.
- The ledger alone is not enough: the ledger-only run read the same files, ran the same tests, and
  never converged. The notes (findings / hypothesis / next steps) are what carry the reasoning
  across the compression boundary.
- `final_working_state.txt` is the exact 10.9k-char form the model saw before step 60: verbatim
  goal, the ledger above, and notes whose every line can be checked against the transcript.
