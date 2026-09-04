from __future__ import annotations

import json
from pathlib import Path

import pytest

from examples.runtime_debug_demo.run_demo import EXPECTED_TOOLS, run_golden_demo
from react_agent.events import canonical_json


@pytest.mark.asyncio
async def test_official_mcp_stdio_golden_demo_and_deterministic_evidence(
    tmp_path: Path,
) -> None:
    run = await run_golden_demo(output_dir=tmp_path)
    result = run.result
    metrics = result["metrics"]
    acceptance = result["acceptance"]

    assert result["transport"] == "official-mcp-v2-stdio"
    assert result["versions"]["mcp"] == "2.0.0"
    assert result["versions"]["debugpy"] == "1.8.21"
    assert metrics["mcp_tools_listed"] == len(EXPECTED_TOOLS) == 7
    assert metrics["mcp_schema_tool_count"] == 7
    assert metrics["mcp_schema_parity"] is True
    assert metrics["debug_tool_calls"] == 7
    assert metrics["event_count"] == 23
    assert metrics["breakpoint_exact"] is True
    assert metrics["suspicious_frame_exact"] is True
    assert metrics["expected_locals_recall"] == 1.0
    assert metrics["required_evidence_coverage"] == 1.0
    assert metrics["report_model_calls"] == 0
    assert metrics["replay_debugger_calls"] == 0
    assert metrics["report_json_hashes_unique"] == 1
    assert metrics["report_markdown_hashes_unique"] == 1
    assert metrics["stale_event_hash_mismatch_detected"] is True
    assert metrics["stale_observation_digest_mismatch_detected"] is True
    assert result["hash_check_scope"] == {
        "full_chain_rewrite_resistance_tested": False,
        "mechanism": "unkeyed-sha256",
        "provides": "internal-consistency-mismatch-detection",
        "requires_trusted_external_head_for_authenticity": True,
    }
    assert metrics["process_check_supported"] is True
    assert metrics["active_debuggee_processes"] >= 1
    assert metrics["orphan_processes"] == 0
    assert metrics["process_reaped"] is True
    assert metrics["server_stderr_empty"] is True
    assert all(acceptance.values())
    assert result["all_acceptance_passed"] is True

    evidence = json.loads(run.evidence_json)
    assert canonical_json(evidence) == run.evidence_json
    assert evidence["generation"] == {"debugger_calls": 0, "model_calls": 0}
    assert evidence["observed_failure"]["type"].startswith("ZeroDivisionError")
    assert evidence["selected_frame"]["function"] == "price_order"
    locals_by_name = {item["name"]: item["value"] for item in evidence["locals"]}
    assert locals_by_name["billable_items"] == "[]"
    assert locals_by_name["item_count"] == "0"
    assert locals_by_name["subtotal"] == "99.0"
    assert evidence["debuggee_exit"]["status"] in {"exited", "terminated"}
    assert len(evidence["timeline"]) == 21

    assert "no model summarization was used" in run.evidence_markdown
    assert "## Durable evidence timeline" in run.evidence_markdown
    assert (tmp_path / "debugging_demo_results.json").is_file()
    assert (tmp_path / "debugging_demo_results.md").is_file()
    assert (tmp_path / "debugging_pr_evidence.json").read_text() == run.evidence_json
    assert (tmp_path / "debugging_pr_evidence.md").read_text() == run.evidence_markdown


@pytest.mark.asyncio
async def test_golden_demo_repeats_without_leaking_processes(tmp_path: Path) -> None:
    first = await run_golden_demo(output_dir=tmp_path / "first")
    second = await run_golden_demo(output_dir=tmp_path / "second")

    for run in (first, second):
        metrics = run.result["metrics"]
        assert metrics["orphan_processes"] == 0
        assert metrics["process_reaped"] is True
        assert metrics["report_json_hashes_unique"] == 1
        assert metrics["report_markdown_hashes_unique"] == 1
