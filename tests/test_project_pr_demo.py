from __future__ import annotations

import hashlib
import json

import pytest

from react_agent.project_pr import ProjectPRState
from react_agent.project_pr_demo import run_project_pr_demo


@pytest.mark.asyncio
async def test_project_pr_demo_reconciles_ack_loss_and_writes_evidence(tmp_path) -> None:
    result = await run_project_pr_demo(tmp_path)

    assert result.snapshot.state is ProjectPRState.CONFIRMED
    assert result.snapshot.remote_check_adopted is True
    assert result.outbound_create_post_count == 1
    assert result.physical_check_run_count == 1
    assert result.publisher_takeovers == 1
    assert result.code_worker_takeovers == 1
    assert result.workspace_restored_before_idempotent_retry is True
    assert result.primary_worktree_unchanged is True

    metrics = json.loads((tmp_path / "metrics.json").read_text())
    evidence = json.loads((tmp_path / "pr_evidence.json").read_text())
    event_lines = (tmp_path / "events.safe.ndjson").read_text().splitlines()
    runtime_events_text = (tmp_path / "runtime.events.safe.ndjson").read_text()
    runtime_events = [json.loads(line) for line in runtime_events_text.splitlines()]

    assert metrics["outcome"] == "auto_completed"
    assert metrics["outbound_create_check_post_count"] == 1
    assert metrics["physical_check_run_count"] == 1
    assert metrics["uncertain_non_idempotent_auto_retries"] == 0
    assert metrics["code_worker_takeovers"] == 1
    assert metrics["workspace_restored_before_idempotent_retry"] is True
    assert metrics["primary_worktree_unchanged"] is True
    assert evidence["publication"]["remote_check_adopted"] is True
    assert len(event_lines) == result.snapshot.last_sequence
    assert (tmp_path / "pr_evidence.md").is_file()
    assert (tmp_path / "patch.diff").read_text().startswith("diff --git")
    assert (tmp_path / "runtime_recovery.json").is_file()
    assert len(runtime_events) == 23
    assert [event["sequence"] for event in runtime_events] == list(range(1, 24))
    assert any(event["kind"] == "run_resumed" for event in runtime_events)
    assert any(
        event["kind"] == "workspace_checkpointed"
        and event["data"].get("phase") == "resume_restored"
        for event in runtime_events
    )
    assert any(
        event["kind"] == "tool_started" and event["data"].get("attempt") == 2
        for event in runtime_events
    )
    assert hashlib.sha256(runtime_events_text.encode()).hexdigest() == evidence[
        "change_and_verification"
    ]["source_evidence_sha256"]
