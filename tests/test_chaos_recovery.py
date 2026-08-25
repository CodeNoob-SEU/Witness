"""Crash-recovery tests: SIGKILL a worker mid-tool-call and resume the Run.

These drive `examples/chaos_resume.py` as real child processes rather than
simulating a crash in-process. A cooperative shutdown would prove much less:
only a hard kill leaves the lease, the pending tool, and the partially executed
side effect exactly as an OOM kill or a lost node would.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from react_agent.events import RunEventKind, verify_event_chain
from react_agent.journal import LeaseConflictError, RunNotFoundError
from react_agent.postgres_journal import PostgresRunJournal

POSTGRES_DSN = os.environ.get("TEST_POSTGRES_DSN")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEMO = REPOSITORY_ROOT / "examples" / "chaos_resume.py"

pytestmark = pytest.mark.skipif(
    not POSTGRES_DSN,
    reason="set TEST_POSTGRES_DSN to run crash-recovery tests",
)


def _spawn(mode: str, run_id_file: Path, session_id: str, effects: Path) -> subprocess.Popen[str]:
    environment = dict(os.environ)
    environment["REACT_AGENT_CHAOS_DIR"] = str(effects)
    assert POSTGRES_DSN is not None
    environment["REACT_AGENT_POSTGRES_DSN"] = POSTGRES_DSN
    return subprocess.Popen(
        [
            sys.executable,
            str(DEMO),
            "--worker",
            mode,
            "--run-id-file",
            str(run_id_file),
            "--session-id",
            session_id,
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(REPOSITORY_ROOT),
    )


async def _open_journal() -> PostgresRunJournal:
    assert POSTGRES_DSN is not None
    journal = PostgresRunJournal(POSTGRES_DSN)
    await journal.open()
    await journal.migrate()
    return journal


async def _await_kind(
    journal: PostgresRunJournal,
    run_id: str,
    kind: RunEventKind,
    *,
    timeout_s: float,
) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            events = await journal.read(run_id)
        except RunNotFoundError:
            events = ()
        if any(event.kind is kind for event in events):
            return True
        await asyncio.sleep(0.1)
    return False


def _await_run_id(run_id_file: Path, *, timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if run_id_file.is_file():
            value = run_id_file.read_text(encoding="utf-8").strip()
            if value:
                return value
        time.sleep(0.05)
    raise AssertionError("the first worker never reported a run id")


@pytest.mark.asyncio
async def test_sigkill_during_a_tool_call_still_reaches_a_final_answer(
    tmp_path: Path,
) -> None:
    session_id = f"chaos-{uuid.uuid4().hex[:12]}"
    run_id_file = tmp_path / "run_id.txt"
    journal = await _open_journal()
    first = _spawn("start", run_id_file, session_id, tmp_path)
    try:
        run_id = _await_run_id(run_id_file, timeout_s=90.0)

        # Kill only once the side-effect boundary is durable. Before that the
        # test would prove nothing interesting: there would be no tool to recover.
        assert await _await_kind(
            journal, run_id, RunEventKind.TOOL_STARTED, timeout_s=90.0
        ), "tool_started never committed, so nothing was at risk"
        crashed = await journal.load(run_id)
        assert crashed.state.value == "waiting_tool"

        os.kill(first.pid, signal.SIGKILL)
        assert first.wait(timeout=60) != 0

        second = _spawn("resume", run_id_file, session_id, tmp_path)
        output, _ = second.communicate(timeout=240)
        assert second.returncode == 0, output
        assert "CHAOS_RESULT status=completed" in output, output
    finally:
        for process in (first,):
            if process.poll() is None:
                process.kill()

    final = await journal.load(run_id)
    events = await journal.read(run_id)

    # The crash must not have rewritten history: the chain still verifies from
    # sequence 1, and the terminal fact is appended, never patched in place.
    verify_event_chain(events)
    assert final.status == "completed"
    assert final.stop_reason == "completed"
    assert final.result is not None
    assert final.result["output"] == "Note recorded. Task complete."

    # A crash forces a new execution; the Run id and its lineage are unchanged.
    assert len(final.executions) >= 2
    assert events[0].kind is RunEventKind.RUN_STARTED
    assert any(event.kind is RunEventKind.RUN_RESUMED for event in events)
    assert events[-1].kind is RunEventKind.RUN_COMPLETED

    # The interrupted attempt is recorded as an unknown cost rather than zero:
    # the provider may already have billed for work we never committed.
    assert any(record.get("amount_micros") is None for record in final.costs)

    # The idempotent tool ran twice, but both attempts carry one stable
    # idempotency key, which is exactly what lets a real service dedupe them.
    attempts = (tmp_path / "attempts.tsv").read_text(encoding="utf-8").splitlines()
    parsed = [line.split("\t") for line in attempts]
    assert [row[0] for row in parsed] == ["1", "2"]
    assert len({row[1] for row in parsed}) == 1


@pytest.mark.asyncio
async def test_a_killed_workers_lease_blocks_takeover_until_it_expires(
    tmp_path: Path,
) -> None:
    session_id = f"chaos-{uuid.uuid4().hex[:12]}"
    run_id_file = tmp_path / "run_id.txt"
    journal = await _open_journal()
    first = _spawn("start", run_id_file, session_id, tmp_path)
    try:
        run_id = _await_run_id(run_id_file, timeout_s=90.0)
        assert await _await_kind(
            journal, run_id, RunEventKind.TOOL_STARTED, timeout_s=90.0
        )
        os.kill(first.pid, signal.SIGKILL)
        first.wait(timeout=60)

        # A SIGKILLed worker cannot release its lease, so the row outlives it.
        # Another worker must be refused until the fencing generation expires,
        # otherwise two processes could commit facts for one Run. The wording
        # differs per adapter; the refusal type is the contract.
        with pytest.raises(LeaseConflictError):
            await journal.acquire(run_id, owner="impatient-worker", ttl_s=5.0)

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            try:
                lease = await journal.acquire(run_id, owner="patient-worker", ttl_s=5.0)
            except LeaseConflictError:
                await asyncio.sleep(0.25)
                continue
            break
        else:
            raise AssertionError("the expired lease was never reclaimable")

        # The new owner gets a strictly higher fencing token, so any write from
        # the old generation is identifiable as stale.
        assert lease.fence >= 2
        await journal.release(lease)
    finally:
        if first.poll() is None:
            first.kill()
