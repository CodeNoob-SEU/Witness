# Context governance synthetic A/B

Provider-neutral serialized-envelope hard character budget: `8000`.

| Scenario | Category | Strategy | Input | Projected | Saved | Compressor calls | Evictions | Recency masks | Hard fallback | Overflow | Live-fact recall |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |
| same_file_rereads | replacement_heavy | raw_hard_stop | 32623 | 32623 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| same_file_rereads | replacement_heavy | recency_observation_masking | 32623 | 7377 | 77.4% | 0 | 0 | 11 | False | False | 1.00 |
| same_file_rereads | replacement_heavy | generic_summary | 32623 | 6164 | 81.1% | 1 | 0 | 0 | False | False | 1.00 |
| same_file_rereads | replacement_heavy | deterministic_only | 32623 | 7885 | 75.8% | 0 | 11 | 0 | False | False | 1.00 |
| same_file_rereads | replacement_heavy | tiered | 32623 | 7885 | 75.8% | 0 | 11 | 0 | False | False | 1.00 |
| edit_read_churn | replacement_heavy | raw_hard_stop | 22592 | 22592 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| edit_read_churn | replacement_heavy | recency_observation_masking | 22592 | 7964 | 64.7% | 0 | 0 | 12 | False | False | 0.14 |
| edit_read_churn | replacement_heavy | generic_summary | 22592 | 4141 | 81.7% | 1 | 0 | 0 | False | False | 1.00 |
| edit_read_churn | replacement_heavy | deterministic_only | 22592 | 3961 | 82.5% | 0 | 6 | 0 | False | False | 0.29 |
| edit_read_churn | replacement_heavy | tiered | 22592 | 4039 | 82.1% | 1 | 6 | 0 | False | False | 1.00 |
| test_reruns | replacement_heavy | raw_hard_stop | 27426 | 27426 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| test_reruns | replacement_heavy | recency_observation_masking | 27426 | 6699 | 75.6% | 0 | 0 | 9 | False | False | 1.00 |
| test_reruns | replacement_heavy | generic_summary | 27426 | 6252 | 77.2% | 1 | 0 | 0 | False | False | 1.00 |
| test_reruns | replacement_heavy | deterministic_only | 27426 | 7113 | 74.1% | 0 | 9 | 0 | False | False | 1.00 |
| test_reruns | replacement_heavy | tiered | 27426 | 7113 | 74.1% | 0 | 9 | 0 | False | False | 1.00 |
| successful_retry | replacement_heavy | raw_hard_stop | 22140 | 22140 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| successful_retry | replacement_heavy | recency_observation_masking | 22140 | 6005 | 72.9% | 0 | 0 | 7 | False | False | 1.00 |
| successful_retry | replacement_heavy | generic_summary | 22140 | 6249 | 71.8% | 1 | 0 | 0 | False | False | 1.00 |
| successful_retry | replacement_heavy | deterministic_only | 22140 | 6404 | 71.1% | 0 | 7 | 0 | False | False | 1.00 |
| successful_retry | replacement_heavy | tiered | 22140 | 6404 | 71.1% | 0 | 7 | 0 | False | False | 1.00 |
| success_then_failed_rerun | replacement_heavy | raw_hard_stop | 14042 | 14042 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| success_then_failed_rerun | replacement_heavy | recency_observation_masking | 14042 | 7113 | 49.3% | 0 | 0 | 3 | False | False | 0.50 |
| success_then_failed_rerun | replacement_heavy | generic_summary | 14042 | 6244 | 55.5% | 1 | 0 | 0 | False | False | 1.00 |
| success_then_failed_rerun | replacement_heavy | deterministic_only | 14042 | 7238 | 48.5% | 0 | 3 | 0 | False | False | 1.00 |
| success_then_failed_rerun | replacement_heavy | tiered | 14042 | 7238 | 48.5% | 0 | 3 | 0 | False | False | 1.00 |
| unrelated_reads | non_redundant | raw_hard_stop | 32768 | 32768 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| unrelated_reads | non_redundant | recency_observation_masking | 32768 | 7478 | 77.2% | 0 | 0 | 11 | False | False | 0.08 |
| unrelated_reads | non_redundant | generic_summary | 32768 | 6483 | 80.2% | 1 | 0 | 0 | False | False | 1.00 |
| unrelated_reads | non_redundant | deterministic_only | 32768 | 6320 | 80.7% | 0 | 0 | 0 | False | False | 0.17 |
| unrelated_reads | non_redundant | tiered | 32768 | 6483 | 80.2% | 1 | 0 | 0 | False | False | 1.00 |
| opaque_append_only | non_redundant | raw_hard_stop | 27322 | 27322 | 0.0% | 0 | 0 | 0 | False | True | 1.00 |
| opaque_append_only | non_redundant | recency_observation_masking | 27322 | 6640 | 75.7% | 0 | 0 | 9 | False | False | 0.10 |
| opaque_append_only | non_redundant | generic_summary | 27322 | 6180 | 77.4% | 1 | 0 | 0 | False | False | 1.00 |
| opaque_append_only | non_redundant | deterministic_only | 27322 | 6057 | 77.8% | 0 | 0 | 0 | False | False | 0.20 |
| opaque_append_only | non_redundant | tiered | 27322 | 6180 | 77.4% | 1 | 0 | 0 | False | False | 1.00 |

## Aggregate

- Replacement-heavy compressor-call reduction vs generic: **80.0%**.
- Tier-1-fit replacement scenarios with strict zero Tiered compressor calls: **same_file_rereads, success_then_failed_rerun, successful_retry, test_reruns**.
- Replacement-heavy paired bootstrap tiered/generic call-ratio 95% CI: **[0.000, 0.600]** (`n=5` synthetic scenario pairs, `10000` replicates).
- Replacement-heavy tiered mean character saving: **70.3%**.
- Non-redundant tiered/generic projected-character ratio: **1.000**.
- Replacement-heavy tiered recall gain vs recency masking: **27.1 points**.
- Non-redundant tiered recall gain vs recency masking: **90.8 points**.

## Scripted repository Raw-reference vs Tiered

Two independent temporary workspaces run the same deterministic state machine through real `read_file`, `write_file`, and `run_tests` tools.
Raw intentionally uses a high budget as a semantic reference; this subsection is an equivalence check, not an equal-budget efficiency comparison.

| Arm | Hard limit | Status | Main model calls | Tool executions | Canonical chars | Peak input chars | Peak active chars | Cumulative evictions | Compressor calls | Compressor source chars | Hard fallbacks |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw reference | 100000 | completed | 10 | 9 | 23867 | 23782 | 23782 | 0 | 0 | 0 | 0 |
| Tiered | 8000 | completed | 10 | 9 | 23867 | 23782 | 7799 | 30 | 1 | 3389 | 0 |

- Tiered peak-active-context reduction vs Raw reference: **67.2%**.
- Final answer (both arms): `VALUE=42; TESTS=PASS`.
- Necessary/stateful tool trace (both arms): `[{"name":"run_tests","target":"app.py"},{"name":"write_file","path":"app.py","value":42},{"name":"run_tests","target":"app.py"}]`.
- Final workspace tree SHA-256 (both arms): `a473bd771d35fd23139d576f0711b5218107d14003fa4c44fb9c1c0b8267f57e`.
- `final_answer_equal`: **PASS**
- `necessary_stateful_tool_calls_equal`: **PASS**
- `workspace_digest_equal`: **PASS**

> This scripted acceptance makes zero live-model or network calls. It is a deterministic repository-like integration fixture, not a repository solve-rate measurement. Real tools execute, but their non-semantic `duration_ms` transcript field is normalized to zero so archived context counts remain byte-reproducible.

## Acceptance

- `tier1_fit_replacement_tiered_zero_compressor_calls`: **PASS**
- `replacement_call_reduction_at_least_50_percent`: **PASS**
- `replacement_call_ratio_bootstrap_upper_below_1`: **PASS**
- `failed_rerun_preserves_prior_success`: **PASS**
- `all_tiered_requests_within_hard_budget`: **PASS**
- `all_tiered_live_facts_retained`: **PASS**
- `non_redundant_no_material_regression`: **PASS**
- `all_recency_requests_within_hard_budget`: **PASS**
- `replacement_tiered_recall_better_than_recency`: **PASS**
- `non_redundant_tiered_recall_gain_at_least_50_points`: **PASS**
- `scripted_arms_completed`: **PASS**
- `scripted_raw_active_context_exceeds_tiered_budget`: **PASS**
- `scripted_tiered_active_context_within_budget`: **PASS**
- `scripted_tiered_exercised_deterministic_eviction`: **PASS**
- `scripted_final_answer_equal`: **PASS**
- `scripted_necessary_stateful_tool_calls_equal`: **PASS**
- `scripted_workspace_digest_equal`: **PASS**

## Mechanism-level safety gates

- Summary content addressing includes algorithm, compressor implementation, declared compressor revision, model identity/configuration, exact prompt revision, source hash, and summary limit.
- The model-backed compressor uses bounded hierarchical map/reduce: each untrusted source payload is at most 64,000 characters by default and a 64-call ceiling fails closed. Partial results are never persisted.
- The Agent durable-journal seam records compression `started`, `completed`, `failed`, and controlled-cancellation `abandoned` phases before emitting the final projection fact.
- Runtime resume deterministically reconciles an unmatched compression `started` fact to `abandoned` before retrying; a completed content-addressed summary remains reusable. The synthetic A/B itself does not inject crashes.

> This offline benchmark measures the context mechanism, not model task quality. The generated-summary arm uses a deterministic extractive stand-in so repeated runs are byte-comparable. The recency arm masks oldest observations without tool semantics until the same hard budget is met. The separate live-model trace-QA is archived in `docs/evaluations/context_live_ab_results.md`; it is still not a repository patch solve-rate measurement.

The paired bootstrap unit is one synthetic scenario, not one repository task; its interval must not be interpreted as a solve-rate confidence interval.
