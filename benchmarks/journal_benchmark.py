"""Measure the journal's read/write cost as a run's event log grows.

Every number in the README's benchmark table comes from this script, so the
claims stay checkable:

    export TEST_POSTGRES_DSN=postgresql://user:pass@127.0.0.1:5432/db
    uv run python benchmarks/journal_benchmark.py
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time

import psycopg

from react_agent.events import PrivacyClass, RunEventDraft, RunEventKind
from react_agent.postgres_journal import PostgresRunJournal

EVENT_COUNTS = (100, 500, 2000)
SAMPLES = 7
FOLLOWERS = 3
FOLLOW_SECONDS = 4.0


def _median_ms(samples: list[float]) -> float:
    return statistics.median(samples) * 1000


async def _sessions_established(dsn: str) -> int:
    """pg_stat_database.sessions is the server's own cumulative connect count."""

    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        cursor = await connection.execute(
            "SELECT sessions FROM pg_stat_database WHERE datname = current_database()"
        )
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])


async def _seed_run(journal: PostgresRunJournal, run_id: str) -> object:
    await journal.create(
        run_id,
        RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            privacy=PrivacyClass.PRIVATE,
            session_id=run_id,
            execution_id="execution-1",
            agent_revision="agent-v1",
            tool_manifest_hash="tools-v1",
            data={"status": "running"},
            checkpoint={"transcript": [{"role": "user", "content": "x" * 200}]},
        ),
        operation_id="run:started",
    )
    return await journal.acquire(run_id, owner="benchmark", ttl_s=900)


async def measure_growth(dsn: str) -> list[tuple[int, float, float, float]]:
    journal = PostgresRunJournal(dsn)
    await journal.open()
    await journal.migrate()
    run_id = f"bench-{time.time_ns()}"
    lease = await _seed_run(journal, run_id)

    rows: list[tuple[int, float, float, float]] = []
    sequence = 1
    for target in EVENT_COUNTS:
        appends: list[float] = []
        while sequence < target:
            sequence += 1
            started = time.perf_counter()
            await journal.append(
                run_id,
                RunEventDraft(
                    kind=RunEventKind.MODEL_STARTED,
                    step=sequence,
                    data={"attempt": 1, "note": "y" * 100},
                ),
                expected_sequence=sequence - 1,
                operation_id=f"op-{sequence}",
                lease=lease,  # type: ignore[arg-type]
            )
            appends.append(time.perf_counter() - started)

        cold: list[float] = []
        for _ in range(SAMPLES):
            # evict_snapshot drops this process's fold cache too, so each
            # sample pays the full read-and-verify a fresh process would.
            await journal.evict_snapshot(run_id)
            started = time.perf_counter()
            await journal.load(run_id)
            cold.append(time.perf_counter() - started)

        warm: list[float] = []
        for _ in range(SAMPLES):
            started = time.perf_counter()
            await journal.load(run_id)
            warm.append(time.perf_counter() - started)

        rows.append((sequence, _median_ms(appends), _median_ms(cold), _median_ms(warm)))
        print(
            f"  {sequence:>5} events   append {_median_ms(appends):>7.1f} ms"
            f"   load(cold) {_median_ms(cold):>7.1f} ms"
            f"   load(warm) {_median_ms(warm):>7.2f} ms"
        )

    await journal.close()
    return rows


async def measure_follower_churn(dsn: str) -> tuple[int, int]:
    """Count PostgreSQL connections an idle SSE follower establishes."""

    journal = PostgresRunJournal(dsn)
    await journal.open()
    await journal.migrate()
    run_id = f"churn-{time.time_ns()}"
    await _seed_run(journal, run_id)

    async def follower(seconds: float) -> int:
        calls = 0
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await journal.wait(run_id, after_sequence=1, timeout_s=0.1)
            calls += 1
        return calls

    await asyncio.gather(*(follower(0.6) for _ in range(FOLLOWERS)))
    before = await _sessions_established(dsn)
    calls = await asyncio.gather(*(follower(FOLLOW_SECONDS) for _ in range(FOLLOWERS)))
    after = await _sessions_established(dsn)
    await journal.close()
    return sum(calls), after - before - 1


async def main() -> int:
    dsn = os.getenv("TEST_POSTGRES_DSN") or os.getenv("REACT_AGENT_POSTGRES_DSN")
    if not dsn:
        print("Set TEST_POSTGRES_DSN to a PostgreSQL 16+ database.", file=sys.stderr)
        return 2

    print(f"journal growth (median of {SAMPLES} samples)")
    rows = await measure_growth(dsn)
    print("\nfollower connection churn")
    calls, established = await measure_follower_churn(dsn)
    print(f"  {FOLLOWERS} followers, {calls} wait() calls -> {established} new sessions")

    print("\n--- markdown ---\n")
    print("| events | append | `load()` cold | `load()` warm |")
    print("| ---: | ---: | ---: | ---: |")
    for count, append_ms, cold_ms, warm_ms in rows:
        print(f"| {count} | {append_ms:.1f} ms | {cold_ms:.1f} ms | {warm_ms:.2f} ms |")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
