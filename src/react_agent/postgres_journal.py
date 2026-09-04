"""PostgreSQL adapter for the append-only run journal seam."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .cost_ledger import (
    CostAdjustmentAppend,
    CostAdjustmentConflictError,
    CostAdjustmentDraft,
    CostRecordNotFoundError,
    StoredCostAdjustment,
    build_cost_adjustment,
)
from .events import (
    GENESIS_HASH,
    TERMINAL_EVENT_KINDS,
    EventValidationError,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    RunSnapshot,
    StoredRunEvent,
    canonical_json,
    fold_events,
    fold_events_from,
)
from .journal import (
    JournalError,
    JournalLease,
    LeaseConflictError,
    LeaseLostError,
    OperationConflictError,
    RunAlreadyExistsError,
    RunMetadataConflictError,
    RunNotFoundError,
    SequenceConflictError,
    SessionBusyError,
    TerminalRunError,
)
from .models import Usage
from .telemetry import TraceReference

_PACKAGE_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_MIGRATIONS_DIR = (
    _PACKAGE_MIGRATIONS_DIR
    if _PACKAGE_MIGRATIONS_DIR.is_dir()
    else Path(__file__).resolve().parents[2] / "migrations"
)


class RequestConflictError(JournalError):
    """An idempotency key or proposed run id was reused inconsistently."""


class SessionNotFoundError(JournalError):
    """A requested session does not exist."""


class SessionVersionConflictError(JournalError):
    """A session commit lost its optimistic version race."""

    def __init__(self, *, expected: int, actual: int) -> None:
        super().__init__(f"expected session version {expected}, actual version is {actual}")
        self.expected = expected
        self.actual = actual


class UnsafeForkError(JournalError):
    """A run lineage targeted a checkpoint that is not safe to fork."""


@dataclass(frozen=True, slots=True)
class RequestReservation:
    """Durable result of reserving one idempotent start request."""

    run_id: str
    created: bool


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """Versioned session state used for completed-run transcript commits."""

    session_id: str
    version: int
    transcript: tuple[Mapping[str, Any], ...]
    status: str


def _json_value(value: object) -> object:
    """Thaw immutable event mappings through their canonical representation."""

    return json.loads(canonical_json(value))


def _snapshot_payload(snapshot: RunSnapshot) -> dict[str, object]:
    """Return the observability projection stored alongside the event log.

    Deliberately metadata only. Copying the folded transcript here would
    re-serialize the whole conversation on every append — O(n) write per event,
    O(n^2) per run — and would duplicate private checkpoint content into a
    second table with its own grants and backups. Recovery never reads this
    row; it folds the events.
    """

    return {
        "run_id": snapshot.run_id,
        "session_id": snapshot.session_id,
        "execution_id": snapshot.execution_id,
        "agent_revision": snapshot.agent_revision,
        "tool_manifest_hash": snapshot.tool_manifest_hash,
        "state": snapshot.state.value,
        "status": snapshot.status,
        "stop_reason": snapshot.stop_reason,
        "transcript_items": len(snapshot.transcript),
        "usage": {
            "input_tokens": snapshot.usage.input_tokens,
            "output_tokens": snapshot.usage.output_tokens,
            "total_tokens": snapshot.usage.total_tokens,
            "cached_input_tokens": snapshot.usage.cached_input_tokens,
            "reasoning_output_tokens": snapshot.usage.reasoning_output_tokens,
            "billable_tokens": snapshot.usage.billable_tokens,
        },
        "counts": {
            "model_calls": snapshot.counts.model_calls,
            "tool_calls": snapshot.counts.tool_calls,
            "tool_executions": snapshot.counts.tool_executions,
        },
        "pending": {
            key: {
                "kind": value.kind.value,
                "sequence": value.sequence,
                "operation_id": value.operation_id,
                "step": value.step,
                "call_key": value.call_key,
            }
            for key, value in snapshot.pending.items()
        },
        "safe_checkpoint_sequences": list(snapshot.safe_checkpoint_sequences),
        "terminal_sequence": (
            snapshot.terminal.sequence if snapshot.terminal is not None else None
        ),
        "last_sequence": snapshot.last_sequence,
        "last_hash": snapshot.last_hash,
    }


class _NotificationHub:
    """One shared ``LISTEN`` connection that wakes every follower in-process.

    ``wait`` used to open, subscribe on, and close a dedicated connection per
    call.  A follower polls continuously, so that churned several PostgreSQL
    connections per second per open stream and bypassed the pool's bounds
    entirely.  One connection per journal serves every local waiter instead.

    Notifications remain hints.  Waiters always re-query the sequence-ordered
    journal, so a hub that cannot connect, or that loses its connection, only
    costs latency: callers fall back to their poll interval.
    """

    def __init__(self, dsn: str, *, retry_interval_s: float) -> None:
        self._dsn = dsn
        self._retry_interval_s = retry_interval_s
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._retry_after = 0.0
        self._closed = False

    async def arm(self) -> asyncio.Event:
        """Return the event to await; call before reading to avoid lost wakeups.

        A commit that lands between a caller's read and its sleep still fires
        the event captured here, which is the same guarantee the previous
        ``LISTEN``-before-``SELECT`` ordering provided.
        """

        await self._ensure_listening()
        return self._wake

    async def _ensure_listening(self) -> None:
        if self._closed or (self._task is not None and not self._task.done()):
            return
        async with self._lock:
            if self._closed or (self._task is not None and not self._task.done()):
                return
            if time.monotonic() < self._retry_after:
                return
            self._retry_after = time.monotonic() + self._retry_interval_s
            try:
                connection: AsyncConnection[dict[str, Any]] = await AsyncConnection.connect(
                    self._dsn,
                    autocommit=True,
                    row_factory=dict_row,
                )
            except Exception:
                # Fail open: waiters degrade to polling on their own timeout.
                return
            self._task = asyncio.create_task(
                self._listen(connection),
                name="react-agent-journal-listen",
            )

    async def _listen(self, connection: AsyncConnection[dict[str, Any]]) -> None:
        try:
            await connection.execute("LISTEN react_agent_events")
            async for _ in connection.notifies():
                self._release_waiters()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            # Release anyone parked on a hub that just died so they re-poll
            # instead of sleeping out their full timeout on a dead connection.
            self._release_waiters()
            with suppress(Exception):
                await connection.close()

    def _release_waiters(self) -> None:
        armed = self._wake
        self._wake = asyncio.Event()
        armed.set()

    async def close(self) -> None:
        self._closed = True
        async with self._lock:
            task = self._task
            self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._release_waiters()


class PostgresRunJournal:
    """Transactional PostgreSQL journal with CAS, fencing, and durable replay.

    ``LISTEN/NOTIFY`` is deliberately only a wake-up hint. Followers always
    query the table by sequence, so a dropped notification cannot drop data.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        poll_interval_s: float = 0.2,
        max_cached_folds: int = 128,
    ) -> None:
        if not dsn.strip():
            raise ValueError("PostgreSQL DSN must not be blank")
        if min_size < 0 or max_size < 1 or min_size > max_size:
            raise ValueError("invalid PostgreSQL pool bounds")
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be positive")
        if max_cached_folds < 0:
            raise ValueError("max_cached_folds must be non-negative")
        self._dsn = dsn
        self._poll_interval_s = poll_interval_s
        self._max_cached_folds = max_cached_folds
        # Reducer state this process folded from a chain it verified itself.
        # Never populated from stored projections: trusting those would let a
        # forged row stand in for history, which the hash chain exists to stop.
        self._folds: OrderedDict[str, RunSnapshot] = OrderedDict()
        self._notifications = _NotificationHub(dsn, retry_interval_s=poll_interval_s)
        self._pool: AsyncConnectionPool[AsyncConnection[dict[str, Any]]] = AsyncConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    async def __aenter__(self) -> PostgresRunJournal:
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def open(self) -> None:
        await self._pool.open(wait=True)

    async def close(self) -> None:
        await self._notifications.close()
        self._folds.clear()
        await self._pool.close()

    def _cached_fold(self, run_id: str) -> RunSnapshot | None:
        snapshot = self._folds.get(run_id)
        if snapshot is not None:
            # Refresh recency so an active run stays cached under pressure.
            self._folds.move_to_end(run_id)
        return snapshot

    def _remember_fold(self, snapshot: RunSnapshot) -> None:
        self._folds[snapshot.run_id] = snapshot
        self._folds.move_to_end(snapshot.run_id)
        while len(self._folds) > self._max_cached_folds:
            self._folds.popitem(last=False)

    async def _fold_with_new_events(
        self,
        connection: AsyncConnection[dict[str, Any]],
        run_id: str,
        new_events: Sequence[StoredRunEvent],
        *,
        previous_hash: str,
    ) -> RunSnapshot:
        """Fold the run including ``new_events``, re-reading history only if needed.

        Re-reading and re-folding every row on each append is O(n) per event and
        O(n^2) per run. The cached prefix may only be used when it provably ends
        exactly where these events begin, so a concurrent writer or a stale
        cache falls back to the authoritative full fold instead of guessing.
        """

        first = new_events[0]
        cached = self._cached_fold(run_id)
        if (
            cached is not None
            and cached.last_sequence == first.sequence - 1
            and cached.last_hash == previous_hash
        ):
            return fold_events_from(cached, new_events)
        cursor = await connection.execute(
            """
            SELECT * FROM react_agent_run_events
            WHERE run_id = %s ORDER BY sequence
            """,
            (run_id,),
        )
        rows = await cursor.fetchall()
        history = tuple(self._event_from_row(row) for row in rows)
        committed = {event.sequence for event in history}
        missing = tuple(event for event in new_events if event.sequence not in committed)
        return fold_events((*history, *missing))

    async def migrate(self) -> None:
        """Apply bundled idempotent migrations in lexical order."""

        migration_paths = sorted(_MIGRATIONS_DIR.glob("*.sql"))
        if not migration_paths:
            raise RuntimeError(f"No migrations found in {_MIGRATIONS_DIR}")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 3))",
                    ("react-agent:migrations",),
                )
                for path in migration_paths:
                    await connection.execute(path.read_text(encoding="utf-8"))

    @staticmethod
    def _transcript_payload(
        transcript: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, object]]:
        payload = _json_value(tuple(transcript))
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise ValueError("session transcript must be a sequence of JSON objects")
        return payload

    @staticmethod
    def _session_from_row(row: Mapping[str, Any]) -> SessionRecord:
        raw_transcript = row["transcript"]
        if not isinstance(raw_transcript, list) or any(
            not isinstance(item, dict) for item in raw_transcript
        ):
            raise RuntimeError("stored session transcript is not a JSON object array")
        return SessionRecord(
            session_id=str(row["session_id"]),
            version=int(row["version"]),
            transcript=tuple(raw_transcript),
            status=str(row["status"]),
        )

    @staticmethod
    def _validate_identity(run_id: str, operation_id: str) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        if not operation_id.strip():
            raise ValueError("operation_id must not be blank")

    @staticmethod
    def _event_values(
        event: StoredRunEvent,
        *,
        payload_hash: str,
    ) -> tuple[object, ...]:
        return (
            event.run_id,
            event.sequence,
            uuid.UUID(event.event_id),
            event.operation_id,
            uuid.UUID(event.causation_id) if event.causation_id is not None else None,
            payload_hash,
            event.schema_version,
            event.kind.value,
            event.privacy.value,
            event.occurred_at,
            event.step,
            event.call_key,
            event.session_id,
            event.execution_id,
            event.agent_revision,
            event.tool_manifest_hash,
            Jsonb(_json_value(event.data)),
            Jsonb(_json_value(event.checkpoint)) if event.checkpoint is not None else None,
            event.safe_checkpoint,
            event.usage_delta.input_tokens,
            event.usage_delta.output_tokens,
            event.usage_delta.total_tokens,
            event.usage_delta.cached_input_tokens,
            event.usage_delta.reasoning_output_tokens,
            event.usage_delta.billable_tokens,
            event.model_calls_delta,
            event.tool_calls_delta,
            event.tool_executions_delta,
            event.previous_hash,
            event.event_hash,
        )

    @staticmethod
    async def _insert_event(
        connection: AsyncConnection[dict[str, Any]],
        event: StoredRunEvent,
        *,
        payload_hash: str,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO react_agent_run_events (
                run_id, sequence, event_id, operation_id, causation_id,
                operation_payload_hash,
                schema_version, event_type, privacy_class, occurred_at, step,
                call_key, session_id, execution_id, agent_revision,
                tool_manifest_hash, public_payload, private_payload,
                safe_checkpoint, input_tokens, output_tokens, total_tokens,
                cached_input_tokens, reasoning_output_tokens, billable_tokens,
                model_calls_delta, tool_calls_delta, tool_executions_delta,
                previous_hash, event_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            PostgresRunJournal._event_values(event, payload_hash=payload_hash),
        )

    @staticmethod
    async def _store_snapshot(
        connection: AsyncConnection[dict[str, Any]], snapshot: RunSnapshot
    ) -> None:
        await connection.execute(
            """
            INSERT INTO react_agent_run_snapshots (run_id, last_sequence, state)
            VALUES (%s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                last_sequence = EXCLUDED.last_sequence,
                state = EXCLUDED.state,
                updated_at = clock_timestamp()
            """,
            (snapshot.run_id, snapshot.last_sequence, Jsonb(_snapshot_payload(snapshot))),
        )

    @staticmethod
    def _event_from_row(row: Mapping[str, Any]) -> StoredRunEvent:
        return StoredRunEvent(
            run_id=str(row["run_id"]),
            sequence=int(row["sequence"]),
            operation_id=str(row["operation_id"]),
            event_id=str(row["event_id"]),
            kind=RunEventKind(str(row["event_type"])),
            privacy=PrivacyClass(str(row["privacy_class"])),
            schema_version=int(row["schema_version"]),
            occurred_at=float(row["occurred_at"]),
            previous_hash=str(row["previous_hash"]),
            event_hash=str(row["event_hash"]),
            step=int(row["step"]) if row["step"] is not None else None,
            call_key=str(row["call_key"]) if row["call_key"] is not None else None,
            causation_id=(
                str(row["causation_id"]) if row.get("causation_id") is not None else None
            ),
            session_id=(str(row["session_id"]) if row["session_id"] is not None else None),
            execution_id=(str(row["execution_id"]) if row["execution_id"] is not None else None),
            agent_revision=(
                str(row["agent_revision"]) if row["agent_revision"] is not None else None
            ),
            tool_manifest_hash=(
                str(row["tool_manifest_hash"]) if row["tool_manifest_hash"] is not None else None
            ),
            data=row["public_payload"] or {},
            checkpoint=row["private_payload"],
            safe_checkpoint=bool(row["safe_checkpoint"]),
            usage_delta=Usage(
                input_tokens=int(row["input_tokens"]),
                output_tokens=int(row["output_tokens"]),
                total_tokens=int(row["total_tokens"]),
                cached_input_tokens=(
                    int(row["cached_input_tokens"])
                    if row["cached_input_tokens"] is not None
                    else None
                ),
                reasoning_output_tokens=(
                    int(row["reasoning_output_tokens"])
                    if row["reasoning_output_tokens"] is not None
                    else None
                ),
                billable_tokens=(
                    int(row["billable_tokens"]) if row["billable_tokens"] is not None else None
                ),
            ),
            model_calls_delta=int(row["model_calls_delta"]),
            tool_calls_delta=int(row["tool_calls_delta"]),
            tool_executions_delta=int(row["tool_executions_delta"]),
        )

    @staticmethod
    async def _existing_operation(
        connection: AsyncConnection[dict[str, Any]],
        run_id: str,
        operation_id: str,
        payload_hash: str,
    ) -> StoredRunEvent | None:
        cursor = await connection.execute(
            """
            SELECT * FROM react_agent_run_events
            WHERE run_id = %s AND operation_id = %s
            """,
            (run_id, operation_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        if str(row["operation_payload_hash"]) != payload_hash:
            raise OperationConflictError(
                f"operation id {operation_id!r} was reused with different content"
            )
        return PostgresRunJournal._event_from_row(row)

    async def reserve_request(
        self,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> RequestReservation:
        """Atomically reserve a run id for an idempotent start request.

        The reservation intentionally precedes run creation, so it lives in a
        separate registry table without a run foreign key. A retry with the
        same session/key/hash returns the originally reserved run id; changing
        the hash or trying to bind one run id to another request fails closed.
        """

        values = {
            "session_id": session_id,
            "idempotency_key": idempotency_key,
            "request_hash": request_hash,
            "proposed_run_id": proposed_run_id,
        }
        for name, value in values.items():
            if not value.strip():
                raise ValueError(f"{name} must not be blank")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO react_agent_sessions (session_id)
                    VALUES (%s) ON CONFLICT (session_id) DO NOTHING
                    """,
                    (session_id,),
                )
                session_cursor = await connection.execute(
                    """
                    SELECT active_run_id FROM react_agent_sessions
                    WHERE session_id = %s FOR UPDATE
                    """,
                    (session_id,),
                )
                session = await session_cursor.fetchone()
                if session is None:  # pragma: no cover - protected by INSERT
                    raise SessionNotFoundError(f"session not found: {session_id}")
                # Hash collisions only serialize unrelated requests; they do
                # not affect correctness because the exact key is queried next.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (session_id, idempotency_key),
                )
                await connection.execute(
                    # Use the same run-id lock as ``create`` so a reservation
                    # cannot race an unrelated run into existence.
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (proposed_run_id,),
                )
                existing_cursor = await connection.execute(
                    """
                    SELECT * FROM react_agent_requests
                    WHERE session_id = %s AND idempotency_key = %s
                    FOR UPDATE
                    """,
                    (session_id, idempotency_key),
                )
                existing = await existing_cursor.fetchone()
                if existing is not None:
                    if str(existing["request_hash"]) != request_hash:
                        raise RequestConflictError(
                            "idempotency key was reused with a different request"
                        )
                    existing_run_id = str(existing["run_id"])
                    active_run_id = (
                        str(session["active_run_id"])
                        if session["active_run_id"] is not None
                        else None
                    )
                    existing_run_cursor = await connection.execute(
                        "SELECT terminal FROM react_agent_runs WHERE run_id = %s",
                        (existing_run_id,),
                    )
                    existing_run = await existing_run_cursor.fetchone()
                    existing_is_active = (
                        existing_run is None or not bool(existing_run["terminal"])
                    )
                    if existing_is_active and active_run_id is None:
                        await connection.execute(
                            """
                            UPDATE react_agent_sessions SET
                                active_run_id = %s,
                                updated_at = clock_timestamp()
                            WHERE session_id = %s
                            """,
                            (existing_run_id, session_id),
                        )
                    elif (
                        existing_is_active
                        and active_run_id is not None
                        and active_run_id != existing_run_id
                    ):
                        raise SessionBusyError(
                            session_id=session_id,
                            active_run_id=active_run_id,
                        )
                    return RequestReservation(existing_run_id, created=False)

                run_cursor = await connection.execute(
                    """
                    SELECT session_id, idempotency_key FROM react_agent_requests
                    WHERE run_id = %s
                    """,
                    (proposed_run_id,),
                )
                if await run_cursor.fetchone() is not None:
                    raise RequestConflictError(
                        "request conflict: proposed run id is already reserved"
                    )
                existing_run_cursor = await connection.execute(
                    "SELECT 1 FROM react_agent_runs WHERE run_id = %s",
                    (proposed_run_id,),
                )
                if await existing_run_cursor.fetchone() is not None:
                    raise RequestConflictError(
                        "request conflict: proposed run id already exists"
                    )

                active_run_id = (
                    str(session["active_run_id"])
                    if session["active_run_id"] is not None
                    else None
                )
                if active_run_id is not None:
                    active_cursor = await connection.execute(
                        "SELECT terminal FROM react_agent_runs WHERE run_id = %s",
                        (active_run_id,),
                    )
                    active = await active_cursor.fetchone()
                    if active is None or not bool(active["terminal"]):
                        raise SessionBusyError(
                            session_id=session_id,
                            active_run_id=active_run_id,
                        )
                    # A terminal event normally clears this in its append
                    # transaction.  This conditional repair handles upgraded
                    # data and a process lost after the terminal commit.
                    await connection.execute(
                        """
                        UPDATE react_agent_sessions SET
                            active_run_id = NULL,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s AND active_run_id = %s
                        """,
                        (session_id, active_run_id),
                    )

                await connection.execute(
                    """
                    INSERT INTO react_agent_requests (
                        session_id, idempotency_key, request_hash, run_id
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (session_id, idempotency_key, request_hash, proposed_run_id),
                )
                await connection.execute(
                    """
                    UPDATE react_agent_sessions SET
                        active_run_id = %s,
                        updated_at = clock_timestamp()
                    WHERE session_id = %s
                    """,
                    (proposed_run_id, session_id),
                )
                return RequestReservation(proposed_run_id, created=True)

    async def load_session(self, session_id: str) -> SessionRecord:
        if not session_id.strip():
            raise ValueError("session_id must not be blank")
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT session_id, version, transcript, status
                FROM react_agent_sessions WHERE session_id = %s
                """,
                (session_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise SessionNotFoundError(f"session not found: {session_id}")
            return self._session_from_row(row)

    async def load_trace_reference(
        self, run_id: str, execution_id: str
    ) -> TraceReference | None:
        """Load a content-free OTel projection, if one was exported in time."""

        if not run_id.strip() or not execution_id.strip():
            raise ValueError("trace reference identities must not be blank")
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT run_id, execution_id, trace_id, span_id, trace_flags
                FROM react_agent_execution_trace_references
                WHERE run_id = %s AND execution_id = %s
                """,
                (run_id, execution_id),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return TraceReference(
                run_id=str(row["run_id"]),
                execution_id=str(row["execution_id"]),
                trace_id=str(row["trace_id"]),
                span_id=str(row["span_id"]),
                trace_flags=int(row["trace_flags"]),
            )

    async def put_trace_reference(self, reference: TraceReference) -> None:
        """Best-effort callers may freeze the first root for one execution."""

        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO react_agent_execution_trace_references (
                        run_id, execution_id, trace_id, span_id, trace_flags
                    )
                    SELECT %s, %s, %s, %s, %s
                    WHERE EXISTS (
                        SELECT 1 FROM react_agent_run_events
                        WHERE run_id = %s
                            AND execution_id = %s
                            AND event_type IN ('run_started', 'run_resumed')
                    )
                    ON CONFLICT (run_id, execution_id) DO NOTHING
                    """,
                    (
                        reference.run_id,
                        reference.execution_id,
                        reference.trace_id,
                        reference.span_id,
                        reference.trace_flags,
                        reference.run_id,
                        reference.execution_id,
                    ),
                )

    async def claim_active_run(self, session_id: str, run_id: str) -> None:
        """Atomically verify-or-claim a nonterminal Run as Session owner."""

        if not session_id.strip() or not run_id.strip():
            raise ValueError("session_id and run_id must not be blank")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    SELECT active_run_id FROM react_agent_sessions
                    WHERE session_id = %s FOR UPDATE
                    """,
                    (session_id,),
                )
                session = await cursor.fetchone()
                if session is None:
                    raise SessionNotFoundError(f"session not found: {session_id}")

                run_cursor = await connection.execute(
                    """
                    SELECT session_id, terminal FROM react_agent_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                run = await run_cursor.fetchone()
                if run is None:
                    raise RunNotFoundError(f"run not found: {run_id}")
                if run["session_id"] is None or str(run["session_id"]) != session_id:
                    raise RunMetadataConflictError(
                        "run belongs to a different Session"
                    )
                if bool(run["terminal"]):
                    raise TerminalRunError(f"run is already terminal: {run_id}")

                active_run_id = (
                    str(session["active_run_id"])
                    if session["active_run_id"] is not None
                    else None
                )
                if active_run_id is None:
                    await connection.execute(
                        """
                        UPDATE react_agent_sessions SET
                            active_run_id = %s,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s
                        """,
                        (run_id, session_id),
                    )
                    return
                if active_run_id != run_id:
                    raise SessionBusyError(
                        session_id=session_id,
                        active_run_id=active_run_id,
                    )

    async def release_active_run(self, session_id: str, run_id: str) -> None:
        """Idempotently release one terminal Run without clearing a successor."""

        if not session_id.strip() or not run_id.strip():
            raise ValueError("session_id and run_id must not be blank")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    SELECT active_run_id FROM react_agent_sessions
                    WHERE session_id = %s FOR UPDATE
                    """,
                    (session_id,),
                )
                session = await cursor.fetchone()
                if session is None:
                    raise SessionNotFoundError(f"session not found: {session_id}")
                active_run_id = (
                    str(session["active_run_id"])
                    if session["active_run_id"] is not None
                    else None
                )
                if active_run_id is None:
                    return
                if active_run_id != run_id:
                    raise SessionBusyError(
                        session_id=session_id,
                        active_run_id=active_run_id,
                    )
                await connection.execute(
                    """
                    UPDATE react_agent_sessions SET
                        active_run_id = NULL,
                        updated_at = clock_timestamp()
                    WHERE session_id = %s AND active_run_id = %s
                    """,
                    (session_id, run_id),
                )

    async def commit_session(
        self,
        session_id: str,
        *,
        expected_version: int,
        transcript: Sequence[Mapping[str, Any]],
        operation_id: str | None = None,
    ) -> SessionRecord:
        """CAS-commit a completed run's transcript to its session."""

        if not session_id.strip():
            raise ValueError("session_id must not be blank")
        if expected_version < 0:
            raise ValueError("expected_version must be non-negative")
        payload = self._transcript_payload(transcript)
        transcript_hash = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
        resolved_operation = (
            f"legacy:{expected_version}:{transcript_hash}" if operation_id is None else operation_id
        )
        if not resolved_operation.strip():
            raise ValueError("operation_id must not be blank")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (session_id, resolved_operation),
                )
                committed_cursor = await connection.execute(
                    """
                    SELECT committed.*, session.status
                    FROM react_agent_session_commits AS committed
                    JOIN react_agent_sessions AS session USING (session_id)
                    WHERE committed.session_id = %s
                        AND committed.operation_id = %s
                    """,
                    (session_id, resolved_operation),
                )
                committed = await committed_cursor.fetchone()
                if committed is not None:
                    committed_transcript = committed["transcript"]
                    if not isinstance(committed_transcript, list) or any(
                        not isinstance(item, dict) for item in committed_transcript
                    ):
                        raise RuntimeError("stored session commit transcript is invalid")
                    stored_transcript_hash = hashlib.sha256(
                        canonical_json(committed_transcript).encode()
                    ).hexdigest()
                    if stored_transcript_hash != str(committed["transcript_hash"]):
                        raise RuntimeError("stored session commit hash is invalid")
                    if (
                        int(committed["expected_version"]) != expected_version
                        or str(committed["transcript_hash"]) != transcript_hash
                    ):
                        raise RequestConflictError(
                            "session commit operation was reused with different content"
                        )
                    return SessionRecord(
                        session_id=session_id,
                        version=int(committed["committed_version"]),
                        transcript=tuple(committed_transcript),
                        status=str(committed["status"]),
                    )
                updated_cursor = await connection.execute(
                    """
                    UPDATE react_agent_sessions SET
                        version = version + 1,
                        transcript = %s,
                        updated_at = clock_timestamp()
                    WHERE session_id = %s AND version = %s
                    RETURNING session_id, version, transcript, status
                    """,
                    (Jsonb(payload), session_id, expected_version),
                )
                updated = await updated_cursor.fetchone()
                if updated is None:
                    current_cursor = await connection.execute(
                        """
                        SELECT session_id, version, transcript, status
                        FROM react_agent_sessions WHERE session_id = %s
                        """,
                        (session_id,),
                    )
                    current = await current_cursor.fetchone()
                    if current is None:
                        raise SessionNotFoundError(f"session not found: {session_id}")
                    raise SessionVersionConflictError(
                        expected=expected_version,
                        actual=int(current["version"]),
                    )
                record = self._session_from_row(updated)
                await connection.execute(
                    """
                    INSERT INTO react_agent_session_commits (
                        session_id, operation_id, expected_version,
                        committed_version, transcript_hash, transcript
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        session_id,
                        resolved_operation,
                        expected_version,
                        record.version,
                        transcript_hash,
                        Jsonb(payload),
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO react_agent_session_snapshots (session_id, version, state)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        version = EXCLUDED.version,
                        state = EXCLUDED.state,
                        updated_at = clock_timestamp()
                    """,
                    (
                        record.session_id,
                        record.version,
                        Jsonb(
                            {
                                "session_id": record.session_id,
                                "version": record.version,
                                "status": record.status,
                                "transcript": payload,
                            }
                        ),
                    ),
                )
                return record

    async def create(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        operation_id: str,
    ) -> StoredRunEvent:
        self._validate_identity(run_id, operation_id)
        if draft.kind is not RunEventKind.RUN_STARTED:
            raise ValueError("create requires a run_started draft")
        payload_hash = draft.payload_hash()
        async with self._pool.connection() as connection:
            async with connection.transaction():
                session: Mapping[str, Any] | None = None
                if draft.session_id is not None:
                    await connection.execute(
                        """
                        INSERT INTO react_agent_sessions (session_id)
                        VALUES (%s) ON CONFLICT (session_id) DO NOTHING
                        """,
                        (draft.session_id,),
                    )
                    session_cursor = await connection.execute(
                        """
                        SELECT active_run_id FROM react_agent_sessions
                        WHERE session_id = %s FOR UPDATE
                        """,
                        (draft.session_id,),
                    )
                    session = await session_cursor.fetchone()
                # ``FOR UPDATE`` cannot lock a missing row. Serializing creators
                # of one logical run keeps concurrent create/retry behavior in
                # the journal's domain instead of leaking UniqueViolation.  A
                # Session row is locked first to match reserve_request's order.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (run_id,),
                )
                cursor = await connection.execute(
                    "SELECT run_id FROM react_agent_runs WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                if await cursor.fetchone() is not None:
                    existing = await self._existing_operation(
                        connection, run_id, operation_id, payload_hash
                    )
                    if existing is not None:
                        return existing
                    raise RunAlreadyExistsError(f"run already exists: {run_id}")

                reservation_cursor = await connection.execute(
                    """
                    SELECT session_id FROM react_agent_requests WHERE run_id = %s
                    """,
                    (run_id,),
                )
                reservation = await reservation_cursor.fetchone()
                if reservation is not None and (
                    draft.session_id is None or str(reservation["session_id"]) != draft.session_id
                ):
                    raise RequestConflictError("run id is reserved for a different session")

                if draft.session_id is not None and session is not None:
                    active_run_id = (
                        str(session["active_run_id"])
                        if session["active_run_id"] is not None
                        else None
                    )
                    if active_run_id is not None and active_run_id != run_id:
                        active_cursor = await connection.execute(
                            "SELECT terminal FROM react_agent_runs WHERE run_id = %s",
                            (active_run_id,),
                        )
                        active = await active_cursor.fetchone()
                        if active is None or not bool(active["terminal"]):
                            raise SessionBusyError(
                                session_id=draft.session_id,
                                active_run_id=active_run_id,
                            )
                    await connection.execute(
                        """
                        UPDATE react_agent_sessions SET
                            active_run_id = %s,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s
                        """,
                        (run_id, draft.session_id),
                    )
                event = StoredRunEvent.from_draft(
                    draft,
                    run_id=run_id,
                    sequence=1,
                    operation_id=operation_id,
                    previous_hash=GENESIS_HASH,
                    occurred_at=time.time(),
                )
                await connection.execute(
                    """
                    INSERT INTO react_agent_runs (
                        run_id, session_id, agent_revision, tool_manifest_hash,
                        head_sequence, head_hash, terminal
                    ) VALUES (%s, %s, %s, %s, 1, %s, %s)
                    """,
                    (
                        run_id,
                        event.session_id,
                        event.agent_revision,
                        event.tool_manifest_hash,
                        event.event_hash,
                        event.kind in TERMINAL_EVENT_KINDS,
                    ),
                )
                await self._insert_event(connection, event, payload_hash=payload_hash)
                await self._store_snapshot(connection, fold_events((event,)))
                await connection.execute(
                    "SELECT pg_notify('react_agent_events', %s)",
                    (f"{run_id}:1",),
                )
                return event

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        self._validate_identity(run_id, operation_id)
        if expected_sequence < 1:
            raise ValueError("expected_sequence must be positive")
        if draft.kind is RunEventKind.RUN_STARTED:
            raise ValueError("run_started can only be committed by create")
        payload_hash = draft.payload_hash()
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    SELECT *, lease_expires_at > clock_timestamp() AS lease_is_live
                    FROM react_agent_runs WHERE run_id = %s FOR UPDATE
                    """,
                    (run_id,),
                )
                run = await cursor.fetchone()
                if run is None:
                    raise RunNotFoundError(f"run not found: {run_id}")
                existing = await self._existing_operation(
                    connection, run_id, operation_id, payload_hash
                )
                if existing is not None:
                    return existing
                if bool(run["terminal"]):
                    raise TerminalRunError(f"run is already terminal: {run_id}")
                actual = int(run["head_sequence"])
                if actual != expected_sequence:
                    raise SequenceConflictError(expected=expected_sequence, actual=actual)
                if run["fence"] and (
                    lease is None
                    or lease.run_id != run_id
                    or lease.owner != run["lease_owner"]
                    or lease.fence != int(run["fence"])
                    or run["lease_expires_at"] is None
                    or not bool(run["lease_is_live"])
                ):
                    raise LeaseLostError("writer lease is missing, expired, or stale")
                for field in ("session_id", "agent_revision", "tool_manifest_hash"):
                    proposed = getattr(draft, field)
                    current = run[field]
                    if proposed is not None and proposed != current:
                        raise RunMetadataConflictError(f"{field} cannot change within a run")

                previous = await connection.execute(
                    """
                    SELECT * FROM react_agent_run_events
                    WHERE run_id = %s AND sequence = %s
                    """,
                    (run_id, actual),
                )
                previous_row = await previous.fetchone()
                if previous_row is None:
                    raise RuntimeError("run head points to a missing event")
                previous_event = self._event_from_row(previous_row)
                event = StoredRunEvent.from_draft(
                    draft,
                    run_id=run_id,
                    sequence=actual + 1,
                    operation_id=operation_id,
                    previous_hash=previous_event.event_hash,
                    occurred_at=time.time(),
                    causation_id=previous_event.event_id,
                    session_id=str(run["session_id"]) if run["session_id"] else None,
                    execution_id=draft.execution_id or previous_event.execution_id,
                    agent_revision=(str(run["agent_revision"]) if run["agent_revision"] else None),
                    tool_manifest_hash=(
                        str(run["tool_manifest_hash"]) if run["tool_manifest_hash"] else None
                    ),
                )
                await self._insert_event(connection, event, payload_hash=payload_hash)
                snapshot = await self._fold_with_new_events(
                    connection,
                    run_id,
                    (event,),
                    previous_hash=previous_event.event_hash,
                )
                await self._store_snapshot(connection, snapshot)
                await connection.execute(
                    """
                    UPDATE react_agent_runs SET
                        head_sequence = %s,
                        head_hash = %s,
                        terminal = %s,
                        updated_at = clock_timestamp()
                    WHERE run_id = %s
                    """,
                    (
                        event.sequence,
                        event.event_hash,
                        event.kind in TERMINAL_EVENT_KINDS,
                        run_id,
                    ),
                )
                if (
                    event.kind in TERMINAL_EVENT_KINDS
                    and run["session_id"] is not None
                ):
                    await connection.execute(
                        """
                        UPDATE react_agent_sessions SET
                            active_run_id = NULL,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s AND active_run_id = %s
                        """,
                        (str(run["session_id"]), run_id),
                    )
                await connection.execute(
                    "SELECT pg_notify('react_agent_events', %s)",
                    (f"{run_id}:{event.sequence}",),
                )
        # Cache only after the transaction commits. Remembering a snapshot the
        # database rolled back would make later reads return state that never
        # existed, which is exactly the failure the journal is meant to rule out.
        self._remember_fold(snapshot)
        return event

    async def append_many(
        self,
        run_id: str,
        entries: Sequence[tuple[RunEventDraft, str]],
        *,
        expected_sequence: int,
        lease: JournalLease | None = None,
    ) -> tuple[StoredRunEvent, ...]:
        if expected_sequence < 1:
            raise ValueError("expected_sequence must be positive")
        resolved = tuple(entries)
        if not resolved:
            raise ValueError("append_many requires at least one event")
        operation_ids = [operation_id for _, operation_id in resolved]
        if len(operation_ids) != len(set(operation_ids)):
            raise OperationConflictError("operation ids must be unique within one batch")
        for draft, operation_id in resolved:
            self._validate_identity(run_id, operation_id)
            if draft.kind is RunEventKind.RUN_STARTED:
                raise ValueError("run_started can only be committed by create")
        payload_hashes = tuple(draft.payload_hash() for draft, _ in resolved)

        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    SELECT *, lease_expires_at > clock_timestamp() AS lease_is_live
                    FROM react_agent_runs WHERE run_id = %s FOR UPDATE
                    """,
                    (run_id,),
                )
                run = await cursor.fetchone()
                if run is None:
                    raise RunNotFoundError(f"run not found: {run_id}")

                existing = tuple(
                    [
                        await self._existing_operation(
                            connection,
                            run_id,
                            operation_id,
                            payload_hash,
                        )
                        for (_, operation_id), payload_hash in zip(
                            resolved,
                            payload_hashes,
                            strict=True,
                        )
                    ]
                )
                if all(event is not None for event in existing):
                    return tuple(event for event in existing if event is not None)
                if any(event is not None for event in existing):
                    raise OperationConflictError("atomic append batch is only partially present")
                if bool(run["terminal"]):
                    raise TerminalRunError(f"run is already terminal: {run_id}")
                actual = int(run["head_sequence"])
                if actual != expected_sequence:
                    raise SequenceConflictError(expected=expected_sequence, actual=actual)
                if run["fence"] and (
                    lease is None
                    or lease.run_id != run_id
                    or lease.owner != run["lease_owner"]
                    or lease.fence != int(run["fence"])
                    or run["lease_expires_at"] is None
                    or not bool(run["lease_is_live"])
                ):
                    raise LeaseLostError("writer lease is missing, expired, or stale")
                for draft, _ in resolved:
                    for field in ("session_id", "agent_revision", "tool_manifest_hash"):
                        proposed = getattr(draft, field)
                        current = run[field]
                        if proposed is not None and proposed != current:
                            raise RunMetadataConflictError(f"{field} cannot change within a run")

                rows_cursor = await connection.execute(
                    """
                    SELECT * FROM react_agent_run_events
                    WHERE run_id = %s ORDER BY sequence
                    """,
                    (run_id,),
                )
                rows = await rows_cursor.fetchall()
                if not rows or int(rows[-1]["sequence"]) != actual:
                    raise RuntimeError("run head points to a missing event")
                committed_history = [self._event_from_row(row) for row in rows]
                previous_event = committed_history[-1]
                new_events: list[StoredRunEvent] = []
                for draft, operation_id in resolved:
                    event = StoredRunEvent.from_draft(
                        draft,
                        run_id=run_id,
                        sequence=previous_event.sequence + 1,
                        operation_id=operation_id,
                        previous_hash=previous_event.event_hash,
                        occurred_at=time.time(),
                        causation_id=previous_event.event_id,
                        session_id=(str(run["session_id"]) if run["session_id"] else None),
                        execution_id=draft.execution_id or previous_event.execution_id,
                        agent_revision=(
                            str(run["agent_revision"]) if run["agent_revision"] else None
                        ),
                        tool_manifest_hash=(
                            str(run["tool_manifest_hash"]) if run["tool_manifest_hash"] else None
                        ),
                    )
                    new_events.append(event)
                    committed_history.append(event)
                    previous_event = event

                snapshot = await self._fold_with_new_events(
                    connection,
                    run_id,
                    tuple(new_events),
                    previous_hash=new_events[0].previous_hash,
                )
                for event, payload_hash in zip(
                    new_events,
                    payload_hashes,
                    strict=True,
                ):
                    await self._insert_event(
                        connection,
                        event,
                        payload_hash=payload_hash,
                    )
                    await connection.execute(
                        """
                        UPDATE react_agent_runs SET
                            head_sequence = %s,
                            head_hash = %s,
                            terminal = %s,
                            updated_at = clock_timestamp()
                        WHERE run_id = %s
                        """,
                        (
                            event.sequence,
                            event.event_hash,
                            event.kind in TERMINAL_EVENT_KINDS,
                            run_id,
                        ),
                    )
                await self._store_snapshot(connection, snapshot)
                final_event = new_events[-1]
                if (
                    final_event.kind in TERMINAL_EVENT_KINDS
                    and run["session_id"] is not None
                ):
                    await connection.execute(
                        """
                        UPDATE react_agent_sessions SET
                            active_run_id = NULL,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s AND active_run_id = %s
                        """,
                        (str(run["session_id"]), run_id),
                    )
                await connection.execute(
                    "SELECT pg_notify('react_agent_events', %s)",
                    (f"{run_id}:{final_event.sequence}",),
                )
        self._remember_fold(snapshot)
        return tuple(new_events)

    async def read(self, run_id: str, *, after_sequence: int = 0) -> tuple[StoredRunEvent, ...]:
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        async with self._pool.connection() as connection:
            return await self._read_from_connection(
                connection, run_id, after_sequence=after_sequence
            )

    @classmethod
    async def _read_from_connection(
        cls,
        connection: AsyncConnection[dict[str, Any]],
        run_id: str,
        *,
        after_sequence: int,
    ) -> tuple[StoredRunEvent, ...]:
        cursor = await connection.execute(
            """
            SELECT * FROM react_agent_run_events
            WHERE run_id = %s AND sequence > %s ORDER BY sequence
            """,
            (run_id, after_sequence),
        )
        rows = await cursor.fetchall()
        if not rows:
            exists = await connection.execute(
                "SELECT 1 FROM react_agent_runs WHERE run_id = %s", (run_id,)
            )
            if await exists.fetchone() is None:
                raise RunNotFoundError(f"run not found: {run_id}")
        return tuple(cls._event_from_row(row) for row in rows)

    async def load(self, run_id: str) -> RunSnapshot:
        """Fold the run, reading only the events this process has not folded yet.

        The cache holds reducer state derived from a chain this process already
        verified, so continuing from it is exactly equivalent to folding from
        sequence 1 — the tail's hash link back into the cached prefix is still
        checked on every call. Nothing here trusts stored projections.
        """

        cached = self._cached_fold(run_id)
        if cached is None:
            snapshot = fold_events(await self.read(run_id))
            self._remember_fold(snapshot)
            return snapshot

        tail = await self.read(run_id, after_sequence=cached.last_sequence)
        if not tail:
            return cached
        try:
            snapshot = fold_events_from(cached, tail)
        except EventValidationError:
            # The cached prefix could not be joined to what the journal now
            # holds. Never repair a chain from memory: drop the cache and let
            # the authoritative full fold decide whether the log is corrupt.
            self._folds.pop(run_id, None)
            snapshot = fold_events(await self.read(run_id))
        self._remember_fold(snapshot)
        return snapshot

    async def evict_snapshot(self, run_id: str) -> bool:
        """Delete only the rebuildable projection for a retained run."""

        self._folds.pop(run_id, None)
        async with self._pool.connection() as connection:
            async with connection.transaction():
                deleted = await connection.execute(
                    "DELETE FROM react_agent_run_snapshots WHERE run_id = %s",
                    (run_id,),
                )
                if deleted.rowcount:
                    return True
                exists = await connection.execute(
                    "SELECT 1 FROM react_agent_runs WHERE run_id = %s",
                    (run_id,),
                )
                if await exists.fetchone() is None:
                    raise RunNotFoundError(f"run not found: {run_id}")
                return False

    @staticmethod
    async def _is_terminal_on_connection(
        connection: AsyncConnection[dict[str, Any]], run_id: str
    ) -> bool:
        cursor = await connection.execute(
            "SELECT terminal FROM react_agent_runs WHERE run_id = %s",
            (run_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise RunNotFoundError(f"run not found: {run_id}")
        return bool(row["terminal"])

    async def acquire(self, run_id: str, *, owner: str, ttl_s: float) -> JournalLease:
        if not owner.strip():
            raise ValueError("lease owner must not be blank")
        if ttl_s <= 0:
            raise ValueError("lease ttl_s must be positive")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    SELECT *, lease_expires_at > clock_timestamp() AS lease_is_live
                    FROM react_agent_runs WHERE run_id = %s FOR UPDATE
                    """,
                    (run_id,),
                )
                run = await cursor.fetchone()
                if run is None:
                    raise RunNotFoundError(f"run not found: {run_id}")
                if (
                    run["lease_owner"] is not None
                    and run["lease_expires_at"] is not None
                    and bool(run["lease_is_live"])
                ):
                    raise LeaseConflictError("the run already has a live writer lease")
                fence = int(run["fence"]) + 1
                updated = await connection.execute(
                    """
                    UPDATE react_agent_runs SET
                        fence = %s,
                        lease_owner = %s,
                        lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        updated_at = clock_timestamp()
                    WHERE run_id = %s
                    RETURNING extract(epoch FROM lease_expires_at) AS expires_at
                    """,
                    (fence, owner, ttl_s, run_id),
                )
                updated_row = await updated.fetchone()
                if updated_row is None:  # pragma: no cover - protected by row lock
                    raise RunNotFoundError(f"run not found: {run_id}")
                expires_at = float(updated_row["expires_at"])
                return JournalLease(run_id, owner, fence, expires_at)

    async def renew(self, lease: JournalLease, *, ttl_s: float) -> JournalLease:
        """Extend a live lease without changing its fencing generation."""

        if ttl_s <= 0:
            raise ValueError("lease ttl_s must be positive")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                updated = await connection.execute(
                    """
                    UPDATE react_agent_runs SET
                        lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        updated_at = clock_timestamp()
                    WHERE run_id = %s
                        AND lease_owner = %s
                        AND fence = %s
                        AND lease_expires_at > clock_timestamp()
                    RETURNING extract(epoch FROM lease_expires_at) AS expires_at
                    """,
                    (ttl_s, lease.run_id, lease.owner, lease.fence),
                )
                row = await updated.fetchone()
                if row is None:
                    raise LeaseLostError("cannot renew a stale or expired lease")
                return JournalLease(
                    run_id=lease.run_id,
                    owner=lease.owner,
                    fence=lease.fence,
                    expires_at=float(row["expires_at"]),
                )

    async def release(self, lease: JournalLease) -> None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE react_agent_runs SET
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = clock_timestamp()
                WHERE run_id = %s AND lease_owner = %s AND fence = %s
                    AND lease_expires_at > clock_timestamp()
                """,
                (lease.run_id, lease.owner, lease.fence),
            )
            if cursor.rowcount == 0:
                raise LeaseLostError("cannot release a stale lease")

    async def wait(
        self,
        run_id: str,
        *,
        after_sequence: int,
        timeout_s: float | None = None,
    ) -> tuple[StoredRunEvent, ...]:
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if timeout_s is not None and timeout_s < 0:
            raise ValueError("timeout_s must be non-negative or None")
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            # Arm before reading. A commit that lands between this read and the
            # sleep below still fires the captured event, which is the same
            # lost-wakeup guarantee as subscribing before the first SELECT.
            wake = await self._notifications.arm()
            async with self._pool.connection() as connection:
                events = await self._read_from_connection(
                    connection, run_id, after_sequence=after_sequence
                )
                if events:
                    return events
                if await self._is_terminal_on_connection(connection, run_id):
                    # A terminal append may commit between the event read and
                    # status check. Always re-read durable rows before saying a
                    # terminal follower is permanently caught up.
                    return await self._read_from_connection(
                        connection, run_id, after_sequence=after_sequence
                    )
            if deadline is None:
                wake_timeout = self._poll_interval_s
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ()
                wake_timeout = min(self._poll_interval_s, remaining)
            # Notifications are hints only: any payload wakes us, and each wake
            # (or timeout) re-queries the sequence-ordered journal.
            with suppress(TimeoutError):
                async with asyncio.timeout(wake_timeout):
                    await wake.wait()

    async def set_lineage(
        self,
        run_id: str,
        *,
        parent_run_id: str,
        fork_sequence: int,
        workspace_tree: str | None,
    ) -> None:
        """Attach immutable fork lineage after validating a safe checkpoint."""

        if not run_id.strip() or not parent_run_id.strip():
            raise ValueError("run_id and parent_run_id must not be blank")
        if run_id == parent_run_id:
            raise ValueError("a run cannot fork from itself")
        if fork_sequence < 1:
            raise ValueError("fork_sequence must be positive")
        if workspace_tree is not None and not workspace_tree.strip():
            raise ValueError("workspace_tree must not be blank")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                # Lock both rows in a stable order to avoid reciprocal-fork
                # deadlocks between concurrent operators.
                rows_cursor = await connection.execute(
                    """
                    SELECT * FROM react_agent_runs
                    WHERE run_id IN (%s, %s)
                    ORDER BY run_id FOR UPDATE
                    """,
                    (run_id, parent_run_id),
                )
                rows = {str(row["run_id"]): row for row in await rows_cursor.fetchall()}
                child = rows.get(run_id)
                if child is None:
                    raise RunNotFoundError(f"run not found: {run_id}")
                if parent_run_id not in rows:
                    raise RunNotFoundError(f"parent run not found: {parent_run_id}")

                current_parent = child["parent_run_id"]
                current_sequence = child["fork_sequence"]
                current_tree = child["workspace_tree"]
                if current_parent is not None or current_sequence is not None:
                    if (
                        str(current_parent) == parent_run_id
                        and int(current_sequence) == fork_sequence
                        and (str(current_tree) if current_tree is not None else None)
                        == workspace_tree
                    ):
                        return
                    raise RunMetadataConflictError("run fork lineage is immutable")
                if int(child["head_sequence"]) != 1 or bool(child["terminal"]):
                    raise RunMetadataConflictError(
                        "lineage must be attached before child execution"
                    )

                ancestry_cursor = await connection.execute(
                    """
                    WITH RECURSIVE ancestry (run_id, parent_run_id) AS (
                        SELECT run_id, parent_run_id FROM react_agent_runs
                        WHERE run_id = %s
                        UNION
                        SELECT candidate.run_id, candidate.parent_run_id
                        FROM react_agent_runs AS candidate
                        JOIN ancestry
                            ON candidate.run_id = ancestry.parent_run_id
                    )
                    SELECT 1 FROM ancestry WHERE run_id = %s
                    """,
                    (parent_run_id, run_id),
                )
                if await ancestry_cursor.fetchone() is not None:
                    raise RunMetadataConflictError("run fork lineage cannot contain cycles")

                checkpoint_cursor = await connection.execute(
                    """
                    SELECT safe_checkpoint FROM react_agent_run_events
                    WHERE run_id = %s AND sequence = %s
                    """,
                    (parent_run_id, fork_sequence),
                )
                checkpoint = await checkpoint_cursor.fetchone()
                if checkpoint is None or not bool(checkpoint["safe_checkpoint"]):
                    raise UnsafeForkError(f"sequence {fork_sequence} is not a safe fork checkpoint")
                await connection.execute(
                    """
                    UPDATE react_agent_runs SET
                        parent_run_id = %s,
                        fork_sequence = %s,
                        workspace_tree = %s,
                        updated_at = clock_timestamp()
                    WHERE run_id = %s
                    """,
                    (parent_run_id, fork_sequence, workspace_tree, run_id),
                )

    async def list_runs(self, session_id: str, *, limit: int = 100) -> tuple[str, ...]:
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT run_id FROM react_agent_runs
                WHERE session_id = %s ORDER BY created_at DESC, run_id LIMIT %s
                """,
                (session_id, limit),
            )
            rows = await cursor.fetchall()
            return tuple(str(row["run_id"]) for row in rows)

    async def list_orphaned_runs(
        self,
        *,
        limit: int = 100,
        agent_revision: str | None = None,
    ) -> tuple[str, ...]:
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT run_id FROM react_agent_runs
                WHERE NOT terminal
                    AND head_sequence > 0
                    AND (%s::text IS NULL OR agent_revision = %s)
                    AND (lease_owner IS NULL
                         OR lease_expires_at IS NULL
                         OR lease_expires_at <= clock_timestamp())
                ORDER BY updated_at DESC, run_id LIMIT %s
                """,
                (agent_revision, agent_revision, limit),
            )
            rows = await cursor.fetchall()
            return tuple(str(row["run_id"]) for row in rows)

    @staticmethod
    def _cost_adjustment_from_row(row: Mapping[str, Any]) -> StoredCostAdjustment:
        payload = row["public_payload"]
        if not isinstance(payload, dict):
            raise RuntimeError("stored cost adjustment payload is not a JSON object")
        return StoredCostAdjustment(
            run_id=str(row["run_id"]),
            ledger_sequence=int(row["ledger_sequence"]),
            record_id=str(row["record_id"]),
            operation_id=str(row["operation_id"]),
            previous_record_id=str(row["previous_record_id"]),
            payload_hash=str(row["operation_payload_hash"]),
            occurred_at=float(row["occurred_at"]),
            public_payload=payload,
        )

    @staticmethod
    def _retry_cost_adjustment(
        row: Mapping[str, Any], *, payload_hash: str
    ) -> CostAdjustmentAppend:
        record = PostgresRunJournal._cost_adjustment_from_row(row)
        if record.payload_hash != payload_hash:
            raise CostAdjustmentConflictError(
                "cost adjustment identity was reused with different content"
            )
        return CostAdjustmentAppend(record, created=False)

    async def append_cost_adjustment(
        self,
        run_id: str,
        draft: CostAdjustmentDraft,
        *,
        previous_record: Mapping[str, Any],
    ) -> CostAdjustmentAppend:
        """Append a post-hoc correction without reopening the run journal."""

        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        if previous_record.get("record_id") != draft.previous_record_id:
            raise CostAdjustmentConflictError("previous cost identity changed before append")
        payload_hash = draft.payload_hash(run_id)
        async with self._pool.connection() as connection:
            async with connection.transaction():
                # This lock serializes ledger sequence allocation and competing
                # successors. It intentionally does not inspect ``terminal``.
                run_cursor = await connection.execute(
                    "SELECT run_id FROM react_agent_runs WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                if await run_cursor.fetchone() is None:
                    raise RunNotFoundError(f"run not found: {run_id}")

                operation_cursor = await connection.execute(
                    """
                    SELECT *, extract(epoch FROM occurred_at) AS occurred_at
                    FROM react_agent_cost_adjustments
                    WHERE run_id = %s AND operation_id = %s
                    """,
                    (run_id, draft.operation_id),
                )
                operation_row = await operation_cursor.fetchone()
                if operation_row is not None:
                    return self._retry_cost_adjustment(
                        operation_row, payload_hash=payload_hash
                    )

                record_cursor = await connection.execute(
                    """
                    SELECT *, extract(epoch FROM occurred_at) AS occurred_at
                    FROM react_agent_cost_adjustments
                    WHERE run_id = %s AND record_id = %s
                    """,
                    (run_id, draft.record_id),
                )
                record_row = await record_cursor.fetchone()
                if record_row is not None:
                    return self._retry_cost_adjustment(
                        record_row, payload_hash=payload_hash
                    )

                base_record_collision = await connection.execute(
                    """
                    SELECT 1 FROM react_agent_run_events
                    WHERE run_id = %s
                        AND event_type IN ('cost_recorded', 'cost_adjusted')
                        AND public_payload->>'record_id' = %s
                    LIMIT 1
                    """,
                    (run_id, draft.record_id),
                )
                if await base_record_collision.fetchone() is not None:
                    raise CostAdjustmentConflictError(
                        "cost adjustment record id collides with a run cost record"
                    )

                successor_cursor = await connection.execute(
                    """
                    SELECT 1 FROM react_agent_cost_adjustments
                    WHERE run_id = %s AND previous_record_id = %s
                    """,
                    (run_id, draft.previous_record_id),
                )
                if await successor_cursor.fetchone() is not None:
                    raise CostAdjustmentConflictError(
                        "previous cost already has an adjustment; reference the latest record"
                    )

                previous_adjustment_cursor = await connection.execute(
                    """
                    SELECT public_payload FROM react_agent_cost_adjustments
                    WHERE run_id = %s AND record_id = %s
                    """,
                    (run_id, draft.previous_record_id),
                )
                previous_adjustment = await previous_adjustment_cursor.fetchone()
                if previous_adjustment is not None:
                    authoritative_previous = previous_adjustment["public_payload"]
                else:
                    previous_base_cursor = await connection.execute(
                        """
                        SELECT public_payload FROM react_agent_run_events
                        WHERE run_id = %s
                            AND event_type IN ('cost_recorded', 'cost_adjusted')
                            AND public_payload->>'record_id' = %s
                        ORDER BY sequence
                        """,
                        (run_id, draft.previous_record_id),
                    )
                    previous_base_rows = await previous_base_cursor.fetchall()
                    if not previous_base_rows:
                        raise CostRecordNotFoundError(
                            f"cost record not found: {draft.previous_record_id}"
                        )
                    if len(previous_base_rows) != 1:
                        raise CostAdjustmentConflictError(
                            "previous cost record identity is ambiguous"
                        )
                    authoritative_previous = previous_base_rows[0]["public_payload"]
                if not isinstance(authoritative_previous, dict):
                    raise RuntimeError("stored predecessor cost is not a JSON object")

                sequence_cursor = await connection.execute(
                    """
                    SELECT COALESCE(max(ledger_sequence), 0) + 1 AS next_sequence,
                        extract(epoch FROM clock_timestamp()) AS occurred_at
                    FROM react_agent_cost_adjustments WHERE run_id = %s
                    """,
                    (run_id,),
                )
                allocation = await sequence_cursor.fetchone()
                assert allocation is not None
                record = build_cost_adjustment(
                    run_id,
                    draft,
                    previous_record=authoritative_previous,
                    ledger_sequence=int(allocation["next_sequence"]),
                    occurred_at=float(allocation["occurred_at"]),
                )
                await connection.execute(
                    """
                    INSERT INTO react_agent_cost_adjustments (
                        run_id, ledger_sequence, record_id, operation_id,
                        previous_record_id, operation_payload_hash, occurred_at,
                        public_payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), %s)
                    """,
                    (
                        record.run_id,
                        record.ledger_sequence,
                        record.record_id,
                        record.operation_id,
                        record.previous_record_id,
                        record.payload_hash,
                        record.occurred_at,
                        Jsonb(_json_value(record.public_payload)),
                    ),
                )
                return CostAdjustmentAppend(record, created=True)

    async def list_cost_adjustments(
        self, run_id: str
    ) -> tuple[StoredCostAdjustment, ...]:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        async with self._pool.connection() as connection:
            exists = await connection.execute(
                "SELECT 1 FROM react_agent_runs WHERE run_id = %s",
                (run_id,),
            )
            if await exists.fetchone() is None:
                raise RunNotFoundError(f"run not found: {run_id}")
            cursor = await connection.execute(
                """
                SELECT *, extract(epoch FROM occurred_at) AS occurred_at
                FROM react_agent_cost_adjustments
                WHERE run_id = %s ORDER BY ledger_sequence
                """,
                (run_id,),
            )
            return tuple(
                self._cost_adjustment_from_row(row) for row in await cursor.fetchall()
            )
