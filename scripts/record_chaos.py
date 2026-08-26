"""Record a real crash-and-resume run so the console can show it offline.

The console does not kill processes on demand: a demo that forks workers on a
button press would be a different, more fragile program than the one being
demonstrated. Instead this script runs the *unmodified*
``examples/chaos_resume.py`` — real child processes, a real ``SIGKILL``, a real
PostgreSQL journal — and exports the resulting durable chain to a JSON file the
console renders as a recording.

The distinction matters and the UI states it: what you see is a recording of a
real run, not a simulation of one.

    export REACT_AGENT_POSTGRES_DSN=postgresql://user:pass@host:5432/db
    uv run python scripts/record_chaos.py

Writes ``src/react_agent/static/assets/recordings/chaos-resume.json``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from react_agent.events import StoredRunEvent, verify_event_chain  # noqa: E402
from react_agent.postgres_journal import PostgresRunJournal  # noqa: E402

EXAMPLE = ROOT / "examples" / "chaos_resume.py"
RECORDINGS = ROOT / "src" / "react_agent" / "static" / "assets" / "recordings"


def _event_json(event: StoredRunEvent) -> dict[str, Any]:
    """Match the wire shape of `/api/runs/{id}/events` exactly.

    The recording is rendered by the same timeline component as a live run, so
    it has to arrive in the same shape — otherwise the console would need a
    second code path, and the recording would stop being a faithful stand-in.
    """

    return {
        "run_id": event.run_id,
        "kind": str(event.kind),
        "sequence": event.sequence,
        "durable_sequence": event.sequence,
        "live_sequence": None,
        "step": event.step,
        "call_key": event.call_key,
        "tool_name": event.data.get("tool_name"),
        "execution_id": event.execution_id,
        "timestamp": event.occurred_at,
        "safe_checkpoint": event.safe_checkpoint,
        "terminal": bool(event.terminal),
        "data": dict(event.data),
    }


def _run_the_real_example(effects: Path) -> str:
    """Run the example untouched and return the run id it produced."""

    environment = dict(os.environ)
    environment["REACT_AGENT_CHAOS_DIR"] = str(effects)
    print(f"running {EXAMPLE.relative_to(ROOT)} (this really does SIGKILL a child)…\n")
    completed = subprocess.run(
        [sys.executable, str(EXAMPLE)],
        env=environment,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(f"the chaos example failed (exit {completed.returncode})")
    run_id_file = effects / "run_id.txt"
    if not run_id_file.is_file():
        raise SystemExit("the example did not report a run id")
    return run_id_file.read_text(encoding="utf-8").strip()


def _attempts(effects: Path) -> list[dict[str, str]]:
    """The tool's own record of what it actually did, attempt by attempt."""

    log = effects / "attempts.tsv"
    if not log.is_file():
        return []
    rows = []
    for line in log.read_text(encoding="utf-8").splitlines():
        attempt, key, marker = line.split("\t")
        rows.append({"attempt": attempt, "idempotency_key": key, "marker": marker})
    return rows


async def _export(dsn: str, run_id: str, effects: Path) -> dict[str, Any]:
    journal = PostgresRunJournal(dsn)
    await journal.open()
    try:
        events = await journal.read(run_id)
        snapshot = await journal.load(run_id)
    finally:
        await journal.close()

    verified = True
    reason: str | None = None
    try:
        verify_event_chain(events)
    except Exception as exc:
        verified = False
        reason = type(exc).__name__

    executions: list[str] = []
    for event in events:
        if event.execution_id and event.execution_id not in executions:
            executions.append(event.execution_id)

    attempts = _attempts(effects)
    return {
        "name": "chaos-resume",
        "title": "Killed mid-tool-call, resumed by a second worker",
        "source": "examples/chaos_resume.py",
        "kind": "recorded",
        "summary": (
            "Worker A was sent SIGKILL immediately after PostgreSQL committed "
            "tool_started — the exact moment a side effect may already have "
            "happened. Worker B resumed the run from durable facts alone and "
            "drove it to a final answer."
        ),
        "run_id": run_id,
        "status": snapshot.status,
        "stop_reason": snapshot.stop_reason,
        "answer": (snapshot.result or {}).get("output"),
        "integrity": {
            "verified": verified,
            "reason": reason,
            "events": len(events),
            "first_sequence": events[0].sequence if events else None,
            "last_sequence": events[-1].sequence if events else None,
            "executions": len(executions),
            "execution_ids": executions,
            "resumed": len(executions) > 1,
        },
        # The point of the whole exercise: the side effect ran twice under one
        # stable key, so a real service can dedupe it.
        "side_effects": attempts,
        "events": [_event_json(event) for event in events],
    }


def _write(recording: dict[str, Any]) -> None:
    RECORDINGS.mkdir(parents=True, exist_ok=True)
    target = RECORDINGS / f"{recording['name']}.json"
    target.write_text(json.dumps(recording, indent=2) + "\n", encoding="utf-8")

    index_file = RECORDINGS / "index.json"
    index: list[dict[str, Any]] = []
    if index_file.is_file():
        index = [
            entry
            for entry in json.loads(index_file.read_text(encoding="utf-8"))
            if entry.get("name") != recording["name"]
        ]
    index.append(
        {
            "name": recording["name"],
            "title": recording["title"],
            "summary": recording["summary"],
            "source": recording["source"],
            "events": recording["integrity"]["events"],
            "executions": recording["integrity"]["executions"],
            "verified": recording["integrity"]["verified"],
        }
    )
    index.sort(key=lambda entry: entry["name"])
    index_file.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    print(f"\nwrote {target.relative_to(ROOT)}")
    print(f"wrote {index_file.relative_to(ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    dsn = os.getenv("REACT_AGENT_POSTGRES_DSN") or os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        print(
            "Set REACT_AGENT_POSTGRES_DSN to a PostgreSQL 16+ database first.\n"
            "A crash recording needs a journal that survives the process, so an\n"
            "in-memory journal cannot produce one.",
            file=sys.stderr,
        )
        return 2

    with tempfile.TemporaryDirectory(prefix="witness-record-") as directory:
        effects = Path(directory)
        run_id = _run_the_real_example(effects)
        recording = asyncio.run(_export(dsn, run_id, effects))

    _write(recording)
    integrity = recording["integrity"]
    print(
        f"\n{integrity['events']} events · {integrity['executions']} executions · "
        f"chain {'verified' if integrity['verified'] else 'BROKEN'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
