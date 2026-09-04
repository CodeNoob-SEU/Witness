# Context live-model paired trace-QA

> Scope: repository-derived long-history fact recovery with a fixed requested model served through an OpenAI-compatible Chat Completions endpoint. This is **not SWE-bench and not repository patch solve-rate**.

## Provenance

- Generated at: `2026-08-21T00:26:46.798500+00:00`
- Provenance mode: `external_api`
- Provider label: `third-party-openai-compatible`
- Endpoint label: `su.kelaode.sbs-8443-v1` (label only; URL and credentials are not archived)
- Exact requested model: `gpt-5.6-terra`
- Expected response model: `gpt-5.6-terra`
- Observed response models: `['gpt-5.6-terra']`
- Model revision: `provider-alias-unpinned`
- Git SHA: `f6ea1fa55351c2a8473a8d9647b17443d798813d`; dirty: `True`
- Source-tree SHA-256: `665e0e68a45c3ec49f979e170353cb186768449b8d20cc72959c87f23bfd4e4e`
- Seed: `20260820`; bootstrap replicates: `10000`
- Hardware: `provider-managed inference; client Apple M3 arm64`
- Network topology: `external TLS API through local proxy 127.0.0.1-7890`
- Container provenance: `not applicable (external API)`

> External endpoint caveat: the requested seed fixes local arm ordering and bootstrap only; provider inference determinism is not assumed. Model-name echo does not prove the underlying revision. Returned usage is archived unchanged even when it exceeds the requested output-token cap.

## Aggregate

- Pairs: **20** (16 replacement-heavy, 4 append-only).
- Exact accuracy, generic / tiered: **5.0% / 70.0%**.
- Tiered minus generic exact-accuracy paired bootstrap 95% CI: **[0.45, 0.85]**.
- Necessary-fact recall, generic / tiered: **28.7% / 77.5%**.
- Compressor calls, generic / tiered: **20 / 8**.
- Replacement-heavy tiered/generic compressor-call ratio 95% CI: **[0.0625, 0.5]**.
- Append-only tiered/generic compressor-call ratio 95% CI: **[1.0, 1.0]**.
- Compression tokens, generic / tiered: **191217 / 64565**.
- Mean wall time per run, generic / tiered: **20732.9 / 9298.1 ms**.

## Acceptance

- `twenty_paired_instances_analyzed`: **PASS**
- `all_assigned_runs_completed`: **PASS**
- `no_provider_projection_overflow`: **PASS**
- `all_response_models_match_expected_model`: **PASS**
- `both_arms_exact_accuracy_at_least_80_percent`: **FAIL**
- `tiered_exact_accuracy_ci_lower_not_below_minus_2pp`: **PASS**
- `replacement_compression_ratio_ci_upper_below_1`: **PASS**
- `append_only_compression_ratio_is_1`: **PASS**

## Per-run evidence

| Instance | Stratum | Arm | Order | Requested model | Observed model | Status | Exact | Recall | Evictions | Compressor calls | Compression tokens | Main tokens | Total ms |
| --- | --- | --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| reread-00 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 0 | 1 | 9572 | 1702 | 26840.5 |
| reread-00 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3506 | 2495.4 |
| reread-01 | replacement_heavy | tiered | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3634 | 2412.1 |
| reread-01 | replacement_heavy | generic | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9699 | 1623 | 21918.7 |
| reread-02 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9474 | 1630 | 21288.6 |
| reread-02 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3543 | 2896.1 |
| reread-03 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10595 | 1845 | 24493.9 |
| reread-03 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3796 | 2441.9 |
| reread-04 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9711 | 1644 | 22539.6 |
| reread-04 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3656 | 3304.3 |
| reread-05 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10436 | 1636 | 20817.0 |
| reread-05 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3725 | 2825.5 |
| reread-06 | replacement_heavy | tiered | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3561 | 2586.8 |
| reread-06 | replacement_heavy | generic | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9386 | 1697 | 23596.7 |
| reread-07 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 11130 | 1653 | 22890.4 |
| reread-07 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 16 | 0 | 0 | 3865 | 2455.0 |
| edit_reread-08 | replacement_heavy | tiered | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 1 | 6085 | 1670 | 19101.0 |
| edit_reread-08 | replacement_heavy | generic | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9203 | 1446 | 14443.9 |
| edit_reread-09 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10452 | 1349 | 12578.1 |
| edit_reread-09 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 12 | 1 | 6665 | 1717 | 19769.5 |
| edit_reread-10 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9448 | 1626 | 19034.3 |
| edit_reread-10 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 12 | 1 | 5843 | 1616 | 15900.5 |
| edit_reread-11 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9108 | 1378 | 13991.9 |
| edit_reread-11 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 1 | 5874 | 1578 | 17989.2 |
| retry-12 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 7828 | 1640 | 20550.0 |
| retry-12 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 0 | 0 | 3216 | 2354.1 |
| retry-13 | replacement_heavy | tiered | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 0 | 0 | 3444 | 2273.9 |
| retry-13 | replacement_heavy | generic | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 8462 | 1899 | 21536.6 |
| retry-14 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 8006 | 1626 | 24591.1 |
| retry-14 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 0 | 0 | 3164 | 2215.4 |
| retry-15 | replacement_heavy | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 8806 | 1642 | 26487.1 |
| retry-15 | replacement_heavy | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | True | 1.00 | 12 | 0 | 0 | 3404 | 2911.8 |
| append-only-00 | append_only | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9257 | 1559 | 17561.7 |
| append-only-00 | append_only | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9540 | 1565 | 24761.9 |
| append-only-01 | append_only | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9700 | 1706 | 21188.7 |
| append-only-01 | append_only | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 9554 | 1583 | 17129.9 |
| append-only-02 | append_only | tiered | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10748 | 1805 | 18635.9 |
| append-only-02 | append_only | generic | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10708 | 1816 | 17770.6 |
| append-only-03 | append_only | generic | 1 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10236 | 1651 | 20538.4 |
| append-only-03 | append_only | tiered | 2 | gpt-5.6-terra | gpt-5.6-terra | completed | False | 0.25 | 0 | 1 | 10256 | 1639 | 21502.0 |

Failures remain in both the analyzed denominator and paired bootstrap as zero accuracy/recall. Monetary cost is not estimated; token counts and wall-clock latency are archived instead.
