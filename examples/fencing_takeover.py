"""Show why a fencing token, not just a lease, is what makes takeover safe.

A lease alone answers "who should be writing". It cannot answer "was this
write issued by the current owner", because a worker can stall — GC pause, a
frozen VM, a partitioned network — and wake up still believing it holds the
lease. The fencing token is the generation counter that makes such a write
identifiable as stale and rejects it.

    export REACT_AGENT_POSTGRES_DSN=postgresql://user:pass@127.0.0.1:5432/db
    uv run python examples/fencing_takeover.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid

from react_agent.events import PrivacyClass, RunEventDraft, RunEventKind
from react_agent.journal import JournalLease, LeaseConflictError, LeaseLostError
from react_agent.postgres_journal import PostgresRunJournal

LEASE_TTL_S = 2.0


def _step(number: int, title: str) -> None:
    print(f"\n{'=' * 68}\nSTEP {number}  {title}\n{'=' * 68}")


async def _append(
    journal: PostgresRunJournal,
    run_id: str,
    *,
    sequence: int,
    operation_id: str,
    lease: JournalLease,
) -> None:
    await journal.append(
        run_id,
        RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=sequence,
            data={"attempt": 1},
        ),
        expected_sequence=sequence,
        operation_id=operation_id,
        lease=lease,
    )


async def demonstrate(dsn: str) -> int:
    journal = PostgresRunJournal(dsn)
    await journal.open()
    await journal.migrate()
    run_id = f"fencing-{uuid.uuid4().hex[:12]}"
    try:
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
            ),
            operation_id="run:started",
        )

        _step(1, "worker A takes the lease and commits a fact")
        worker_a = await journal.acquire(run_id, owner="worker-A", ttl_s=LEASE_TTL_S)
        print(f"   worker A holds fence={worker_a.fence}")
        await _append(journal, run_id, sequence=1, operation_id="a-1", lease=worker_a)
        print("   worker A committed sequence 2")

        _step(2, "worker B is refused while A's lease is still live")
        try:
            await journal.acquire(run_id, owner="worker-B", ttl_s=LEASE_TTL_S)
        except LeaseConflictError as exc:
            print(f"   worker B refused: {exc}")
        else:
            print("   worker B acquired a live lease -- this must not happen")
            return 1

        _step(3, "worker A stalls (GC pause / frozen VM) and stops renewing")
        print(f"   sleeping past the {LEASE_TTL_S}s TTL without a heartbeat...")
        await asyncio.sleep(LEASE_TTL_S + 0.5)

        _step(4, "worker B takes over with a strictly higher fence")
        worker_b = await journal.acquire(run_id, owner="worker-B", ttl_s=60.0)
        print(f"   worker B holds fence={worker_b.fence} (was {worker_a.fence})")
        await _append(journal, run_id, sequence=2, operation_id="b-1", lease=worker_b)
        print("   worker B committed sequence 3")

        _step(5, "worker A wakes up still believing it owns the run")
        # This is the case a lease alone cannot handle: process A is alive,
        # holds a lease object, and has no way to know time passed.
        try:
            await _append(
                journal, run_id, sequence=3, operation_id="a-2", lease=worker_a
            )
        except LeaseLostError as exc:
            print(f"   worker A's write REJECTED: {exc}")
            print(
                f"   (its token is fence={worker_a.fence}; "
                f"the run is now at fence={worker_b.fence})"
            )
        else:
            print("   worker A committed after being fenced out -- data loss window")
            return 1

        events = await journal.read(run_id)
        print("\n   durable chain — A's stale write never landed:")
        for event in events:
            print(
                f"      {event.sequence:>2}  {event.kind.value:<18} "
                f"operation={event.operation_id}"
            )
        print(
            "\n   Two workers believed they owned this run at the same instant.\n"
            "   Only the one holding the current fencing token could commit."
        )
        return 0
    finally:
        await journal.close()


def main() -> int:
    dsn = os.getenv("REACT_AGENT_POSTGRES_DSN") or os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        print(
            "Set REACT_AGENT_POSTGRES_DSN to a PostgreSQL 16+ database first.",
            file=sys.stderr,
        )
        return 2
    started = time.monotonic()
    code = asyncio.run(demonstrate(dsn))
    print(f"\ncompleted in {time.monotonic() - started:.1f}s")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
