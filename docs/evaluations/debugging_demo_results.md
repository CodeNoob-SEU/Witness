# Runtime debugging golden demo

This result was produced through the official MCP v2 stdio client, the local MCP server, DAP, and `debugpy==1.8.21` against the real golden target.

The unkeyed SHA-256 checks below establish internal sequence/hash/digest consistency only. Without a trusted external head, HMAC, or signature, a writer able to rewrite the complete journal can recompute the chain; this benchmark does not test origin authenticity or resistance to such a writer.

## Result

- MCP protocol: `2025-11-25`
- MCP tools listed: `7/7`
- MCP/Agent schema parity: `True`
- Breakpoint verified exactly: `True`
- Suspicious frame exact: `True`
- Expected locals recall: `1.000`
- Required evidence coverage: `1.000`
- Report-generation model calls: `0`
- Replay debugger calls: `0`
- Unique JSON hashes across five replays: `1`
- Unique Markdown hashes across five replays: `1`
- Stale event-hash mismatch detected: `True`
- Stale observation-digest mismatch detected: `True`
- Orphan processes after shutdown: `0`

## Acceptance

- `mcp_initialize_and_7_tools`: **PASS**
- `mcp_schema_parity`: **PASS**
- `all_7_tools_called`: **PASS**
- `verified_breakpoint`: **PASS**
- `uncaught_exception_observed`: **PASS**
- `suspicious_frame_exact`: **PASS**
- `expected_locals_recall_1`: **PASS**
- `required_evidence_coverage_1`: **PASS**
- `zero_report_external_calls`: **PASS**
- `five_replays_byte_identical`: **PASS**
- `two_layer_internal_consistency_checks`: **PASS**
- `no_orphan_processes`: **PASS**
- `server_stderr_empty`: **PASS**

## Artifacts

- `debugging_pr_evidence.json`: canonical replay output.
- `debugging_pr_evidence.md`: reviewer-facing fixed-template report.
- The private raw event log is intentionally not committed.

Reproduce with:

```bash
uv run --extra debug python examples/runtime_debug_demo/run_demo.py
```
