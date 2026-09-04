"""Append-only run journal seam and its concurrent in-memory adapter."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .events import (
    GENESIS_HASH,
    TERMINAL_EVENT_KINDS,
    EventValidationError,
    RunEventDraft,
    RunEventKind,
    RunSnapshot,
    StoredRunEvent,
    fold_events,
    fold_events_from,
)


class JournalError(RuntimeError):
    """Base class for append-only journal failures."""


class RunAlreadyExistsError(JournalError):
    """A non-idempotent create targeted an existing run id."""


class RunNotFoundError(JournalError):
    """The requested run id is not retained by this journal."""


class SequenceConflictError(JournalError):
    """Optimistic append expected a different committed tail."""

    def __init__(self, *, expected: int, actual: int) -> None:
        super().__init__(f"expected sequence {expected}, actual sequence is {actual}")
        self.expected = expected
        self.actual = actual


class OperationConflictError(JournalError):
    """An operation id was reused with different caller-controlled content."""


class TerminalRunError(JournalError):
    """New facts cannot be appended after a terminal run event."""


class RunMetadataConflictError(JournalError):
    """Immutable run identity metadata changed after creation."""


class LeaseConflictError(JournalError):
    """Another non-expired writer currently owns the run lease."""


class LeaseLostError(JournalError):
    """A missing, expired, released, or stale fencing lease attempted a write."""


class SessionBusyError(JournalError):
    """A different nonterminal run or reservation owns a Session."""

    def __init__(self, *, session_id: str, active_run_id: str) -> None:
        super().__init__(
            f"session {session_id!r} already has active run {active_run_id!r}"
        )
        self.session_id = session_id
        self.active_run_id = active_run_id


@dataclass(frozen=True, slots=True)
class JournalLease:
    run_id: str
    owner: str
    fence: int
    expires_at: float


@runtime_checkable
class RunJournal(Protocol):
    """Persistence seam used by orchestration and replay modules."""

    async def create(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        operation_id: str,
    ) -> StoredRunEvent: ...

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent: ...

    async def append_many(
        self,
        run_id: str,
        entries: Sequence[tuple[RunEventDraft, str]],
        *,
        expected_sequence: int,
        lease: JournalLease | None = None,
    ) -> tuple[StoredRunEvent, ...]:
        """Atomically append an ordered group of facts under one CAS."""
        ...

    async def read(self, run_id: str, *, after_sequence: int = 0) -> tuple[StoredRunEvent, ...]: ...

    async def load(self, run_id: str) -> RunSnapshot: ...

    async def acquire(self, run_id: str, *, owner: str, ttl_s: float) -> JournalLease: ...

    async def renew(self, lease: JournalLease, *, ttl_s: float) -> JournalLease: ...

    async def release(self, lease: JournalLease) -> None: ...

    async def wait(
        self,
        run_id: str,
        *,
        after_sequence: int,
        timeout_s: float | None = None,
    ) -> tuple[StoredRunEvent, ...]: ...

    async def list_orphaned_runs(
        self,
        *,
        limit: int = 100,
        agent_revision: str | None = None,
    ) -> tuple[str, ...]:
        """Non-terminal runs whose writer lease is absent or expired.

        The candidates a supervisor may try to Resume, most recently updated
        first. ``agent_revision`` restricts the listing to runs this Agent
        binding could actually Resume. Reconciliation state is still the
        Runtime's decision.
        """
        ...


@dataclass(frozen=True, slots=True)
class _CommittedOperation:
    payload_hash: str
    event: StoredRunEvent


@dataclass(slots=True)
class _RunLog:
    events: list[StoredRunEvent]
    operations: dict[str, _CommittedOperation]
    snapshot: RunSnapshot | None
    fence_counter: int = 0
    lease_owner: str | None = None
    lease_fence: int | None = None
    lease_expires_at: float = 0.0
    fencing_enabled: bool = False


class InMemoryRunJournal:
    """Concurrent reference adapter with CAS, idempotency, wait, and fencing."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._clock = clock
        self._wall_clock = wall_clock
        self._condition = asyncio.Condition()
        self._runs: dict[str, _RunLog] = {}

    @staticmethod
    def _validate_identity(run_id: str, operation_id: str) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        if not operation_id.strip():
            raise ValueError("operation_id must not be blank")

    def _run(self, run_id: str) -> _RunLog:
        try:
            return self._runs[run_id]
        except KeyError:
            raise RunNotFoundError(f"run not found: {run_id}") from None

    @staticmethod
    def _retry(
        log: _RunLog,
        *,
        operation_id: str,
        payload_hash: str,
    ) -> StoredRunEvent | None:
        committed = log.operations.get(operation_id)
        if committed is None:
            return None
        if committed.payload_hash != payload_hash:
            raise OperationConflictError(
                f"operation id {operation_id!r} was reused with different content"
            )
        return committed.event

    def _assert_lease(self, log: _RunLog, run_id: str, lease: JournalLease | None) -> None:
        if not log.fencing_enabled:
            if lease is not None:
                raise LeaseLostError("the run has no active fencing generation")
            return
        now = self._clock()
        if (
            lease is None
            or lease.run_id != run_id
            or lease.owner != log.lease_owner
            or lease.fence != log.lease_fence
            or lease.expires_at != log.lease_expires_at
            or log.lease_expires_at <= now
        ):
            raise LeaseLostError("writer lease is missing, expired, or stale")

    @staticmethod
    def _static_metadata(
        first: StoredRunEvent,
        draft: RunEventDraft,
    ) -> tuple[str | None, str | None, str | None]:
        fields = (
            ("session_id", first.session_id, draft.session_id),
            ("agent_revision", first.agent_revision, draft.agent_revision),
            (
                "tool_manifest_hash",
                first.tool_manifest_hash,
                draft.tool_manifest_hash,
            ),
        )
        for name, original, proposed in fields:
            if proposed is not None and proposed != original:
                raise RunMetadataConflictError(f"{name} cannot change within a run")
        return first.session_id, first.agent_revision, first.tool_manifest_hash

    async def create(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        operation_id: str,
    ) -> StoredRunEvent:
        self._validate_identity(run_id, operation_id)
        if draft.kind is not RunEventKind.RUN_STARTED:
            raise EventValidationError("create requires a run_started draft")
        payload_hash = draft.payload_hash()
        async with self._condition:
            existing = self._runs.get(run_id)
            if existing is not None:
                retried = self._retry(
                    existing,
                    operation_id=operation_id,
                    payload_hash=payload_hash,
                )
                if retried is not None:
                    return retried
                raise RunAlreadyExistsError(f"run already exists: {run_id}")

            event = StoredRunEvent.from_draft(
                draft,
                run_id=run_id,
                sequence=1,
                operation_id=operation_id,
                previous_hash=GENESIS_HASH,
                occurred_at=self._wall_clock(),
            )
            snapshot = fold_events((event,))
            self._runs[run_id] = _RunLog(
                events=[event],
                operations={
                    operation_id: _CommittedOperation(
                        payload_hash=payload_hash,
                        event=event,
                    )
                },
                snapshot=snapshot,
            )
            self._condition.notify_all()
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
            raise EventValidationError("run_started can only be committed by create")
        payload_hash = draft.payload_hash()
        async with self._condition:
            log = self._run(run_id)
            retried = self._retry(
                log,
                operation_id=operation_id,
                payload_hash=payload_hash,
            )
            if retried is not None:
                return retried
            if log.events[-1].kind in TERMINAL_EVENT_KINDS:
                raise TerminalRunError(f"run is already terminal: {run_id}")
            self._assert_lease(log, run_id, lease)
            actual_sequence = log.events[-1].sequence
            if expected_sequence != actual_sequence:
                raise SequenceConflictError(
                    expected=expected_sequence,
                    actual=actual_sequence,
                )

            first = log.events[0]
            session_id, agent_revision, tool_manifest_hash = self._static_metadata(first, draft)
            current_execution_id = (
                log.snapshot.execution_id
                if log.snapshot is not None
                else log.events[-1].execution_id
            )
            event = StoredRunEvent.from_draft(
                draft,
                run_id=run_id,
                sequence=actual_sequence + 1,
                operation_id=operation_id,
                previous_hash=log.events[-1].event_hash,
                occurred_at=self._wall_clock(),
                causation_id=log.events[-1].event_id,
                session_id=session_id,
                execution_id=(
                    draft.execution_id if draft.execution_id is not None else current_execution_id
                ),
                agent_revision=agent_revision,
                tool_manifest_hash=tool_manifest_hash,
            )
            # Folding the whole log on every append is O(n) per event and
            # O(n^2) per run. The cached snapshot is reducer state this adapter
            # produced from an already-verified chain, so continuing from it is
            # equivalent -- the new event's hash link is still checked.
            snapshot = (
                fold_events_from(log.snapshot, (event,))
                if log.snapshot is not None
                else fold_events((*log.events, event))
            )
            log.events.append(event)
            log.operations[operation_id] = _CommittedOperation(
                payload_hash=payload_hash,
                event=event,
            )
            log.snapshot = snapshot
            self._condition.notify_all()
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
                raise EventValidationError("run_started can only be committed by create")

        payload_hashes = tuple(draft.payload_hash() for draft, _ in resolved)
        async with self._condition:
            log = self._run(run_id)
            retried = tuple(
                self._retry(
                    log,
                    operation_id=operation_id,
                    payload_hash=payload_hash,
                )
                for (_, operation_id), payload_hash in zip(
                    resolved,
                    payload_hashes,
                    strict=True,
                )
            )
            if all(event is not None for event in retried):
                return tuple(event for event in retried if event is not None)
            if any(event is not None for event in retried):
                raise OperationConflictError(
                    "atomic append batch is only partially present"
                )
            if log.events[-1].kind in TERMINAL_EVENT_KINDS:
                raise TerminalRunError(f"run is already terminal: {run_id}")
            self._assert_lease(log, run_id, lease)
            actual_sequence = log.events[-1].sequence
            if expected_sequence != actual_sequence:
                raise SequenceConflictError(
                    expected=expected_sequence,
                    actual=actual_sequence,
                )

            first = log.events[0]
            candidate_events = list(log.events)
            new_events: list[StoredRunEvent] = []
            previous = candidate_events[-1]
            for draft, operation_id in resolved:
                session_id, agent_revision, tool_manifest_hash = self._static_metadata(
                    first,
                    draft,
                )
                event = StoredRunEvent.from_draft(
                    draft,
                    run_id=run_id,
                    sequence=previous.sequence + 1,
                    operation_id=operation_id,
                    previous_hash=previous.event_hash,
                    occurred_at=self._wall_clock(),
                    causation_id=previous.event_id,
                    session_id=session_id,
                    execution_id=draft.execution_id or previous.execution_id,
                    agent_revision=agent_revision,
                    tool_manifest_hash=tool_manifest_hash,
                )
                candidate_events.append(event)
                new_events.append(event)
                previous = event

            snapshot = (
                fold_events_from(log.snapshot, tuple(new_events))
                if log.snapshot is not None
                else fold_events(tuple(candidate_events))
            )
            log.events.extend(new_events)
            for event, payload_hash in zip(new_events, payload_hashes, strict=True):
                log.operations[event.operation_id] = _CommittedOperation(
                    payload_hash=payload_hash,
                    event=event,
                )
            log.snapshot = snapshot
            self._condition.notify_all()
            return tuple(new_events)

    async def read(self, run_id: str, *, after_sequence: int = 0) -> tuple[StoredRunEvent, ...]:
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        async with self._condition:
            log = self._run(run_id)
            return tuple(event for event in log.events if event.sequence > after_sequence)

    async def load(self, run_id: str) -> RunSnapshot:
        async with self._condition:
            log = self._run(run_id)
            if log.snapshot is None:
                log.snapshot = fold_events(tuple(log.events))
            return log.snapshot

    async def evict_snapshot(self, run_id: str) -> bool:
        """Evict only the rebuildable projection, never committed events."""

        async with self._condition:
            log = self._run(run_id)
            existed = log.snapshot is not None
            log.snapshot = None
            return existed

    async def acquire(
        self,
        run_id: str,
        *,
        owner: str,
        ttl_s: float,
    ) -> JournalLease:
        if not owner.strip():
            raise ValueError("lease owner must not be blank")
        if ttl_s <= 0:
            raise ValueError("lease ttl_s must be positive")
        async with self._condition:
            log = self._run(run_id)
            now = self._clock()
            if log.lease_owner is not None and log.lease_expires_at > now:
                raise LeaseConflictError(f"run {run_id!r} is leased by {log.lease_owner!r}")
            log.fence_counter += 1
            log.lease_owner = owner
            log.lease_fence = log.fence_counter
            log.lease_expires_at = now + ttl_s
            log.fencing_enabled = True
            lease = JournalLease(
                run_id=run_id,
                owner=owner,
                fence=log.fence_counter,
                expires_at=log.lease_expires_at,
            )
            self._condition.notify_all()
            return lease

    async def release(self, lease: JournalLease) -> None:
        async with self._condition:
            log = self._run(lease.run_id)
            now = self._clock()
            if (
                lease.owner != log.lease_owner
                or lease.fence != log.lease_fence
                or lease.expires_at != log.lease_expires_at
                or log.lease_expires_at <= now
            ):
                raise LeaseLostError("cannot release a stale or expired lease")
            log.lease_owner = None
            log.lease_fence = None
            log.lease_expires_at = 0.0
            self._condition.notify_all()

    async def list_orphaned_runs(
        self,
        *,
        limit: int = 100,
        agent_revision: str | None = None,
    ) -> tuple[str, ...]:
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._condition:
            now = self._clock()
            orphaned = [
                (log.events[-1].occurred_at, run_id)
                for run_id, log in self._runs.items()
                if log.events
                and log.events[-1].kind not in TERMINAL_EVENT_KINDS
                and (log.lease_owner is None or log.lease_expires_at <= now)
                and (agent_revision is None or log.events[0].agent_revision == agent_revision)
            ]
            orphaned.sort(key=lambda item: (-item[0], item[1]))
            return tuple(run_id for _, run_id in orphaned[:limit])

    async def renew(self, lease: JournalLease, *, ttl_s: float) -> JournalLease:
        if ttl_s <= 0:
            raise ValueError("lease ttl_s must be positive")
        async with self._condition:
            log = self._run(lease.run_id)
            now = self._clock()
            if (
                lease.owner != log.lease_owner
                or lease.fence != log.lease_fence
                or lease.expires_at != log.lease_expires_at
                or log.lease_expires_at <= now
            ):
                raise LeaseLostError("cannot renew a stale or expired lease")
            log.lease_expires_at = now + ttl_s
            renewed = JournalLease(
                run_id=lease.run_id,
                owner=lease.owner,
                fence=lease.fence,
                expires_at=log.lease_expires_at,
            )
            self._condition.notify_all()
            return renewed

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

        async with self._condition:
            log = self._run(run_id)

            def ready() -> bool:
                return (
                    log.events[-1].sequence > after_sequence
                    or log.events[-1].kind in TERMINAL_EVENT_KINDS
                )

            if not ready():
                if timeout_s == 0:
                    return ()
                try:
                    if timeout_s is None:
                        await self._condition.wait_for(ready)
                    else:
                        async with asyncio.timeout(timeout_s):
                            await self._condition.wait_for(ready)
                except TimeoutError:
                    return ()
            return tuple(event for event in log.events if event.sequence > after_sequence)
