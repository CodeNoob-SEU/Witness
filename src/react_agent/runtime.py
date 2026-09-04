"""Durable, event-sourced orchestration for ReAct agents.

``AgentRuntime`` is the intentionally small external seam.  It owns request
idempotency, fencing, conservative recovery, session CAS commits, public live
fan-out, cost projection, telemetry, and optional workspace checkpoints.  The
append-only journal remains the sole source of truth; every in-process object
in this module is disposable coordination state.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import math
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

from .agent import ReActAgent
from .cost import PricingCatalog, UsageBreakdown
from .cost_ledger import (
    CostAdjustmentAppend,
    CostAdjustmentDraft,
    CostAdjustmentError,
    CostAdjustmentStore,
    InMemoryCostAdjustmentStore,
    StoredCostAdjustment,
    deterministic_adjustment_record_id,
)
from .events import (
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    RunSnapshot,
    RunState,
    StoredRunEvent,
    canonical_json,
    fold_events,
)
from .journal import (
    JournalLease,
    LeaseConflictError,
    LeaseLostError,
    RunJournal,
    RunNotFoundError,
    SessionBusyError,
)
from .models import (
    AgentJournalEvent,
    AgentJournalEventKind,
    AgentResult,
    AgentResumeState,
    AgentStreamEvent,
    AssistantMessage,
    JsonValue,
    RecoveredToolCall,
    RunStatus,
    StopReason,
    ToolCall,
    ToolMessage,
    Usage,
    agent_events_from_json,
    tool_action_fingerprint,
    transcript_from_json,
    transcript_item_from_json,
    transcript_item_to_json,
    transcript_to_json,
)
from .telemetry import (
    NoOpTelemetry,
    Telemetry,
    TelemetryEvent,
    TelemetryEventKind,
    TraceReference,
)
from .tools import ToolResumePolicy
from .workspace import (
    DiffSummary,
    WorkspaceCheckpoint,
    WorkspaceCheckpointStore,
    WorkspaceConflictError,
    WorkspaceError,
    WorkspaceHandle,
)


class RuntimeErrorBase(RuntimeError):
    """Base error raised by the Runtime interface."""


class RuntimeNotFound(RuntimeErrorBase):
    """A run or session is not present in the fact store."""


class RuntimeConflict(RuntimeErrorBase):
    """A command conflicts with durable state or another writer."""


class ResumeRejected(RuntimeConflict):
    """The current Agent/tool revision cannot safely resume a run."""


class ReconciliationRequired(RuntimeConflict):
    """An uncertain non-idempotent tool requires an operator decision."""


class RequestPayloadConflict(RuntimeConflict):
    """An idempotency key was reused for a different logical request."""


class SessionVersionConflict(RuntimeConflict):
    """A completed run lost the optimistic Session transcript race."""


class ResolutionAction(StrEnum):
    RETRY = "retry"
    USE_RESULT = "use_result"
    ABORT = "abort"


_WORKSPACE_RECONCILIATION_KEY = "workspace"
_INITIAL_EXECUTION_NAMESPACE = uuid.UUID("c5d2e5df-5b72-48a6-a65b-9c2dc1ed30b2")
_AGENT_BINDING_REVISION_SCHEMA_VERSION = 1


def _initial_execution_id(run_id: str) -> str:
    """Return the stable first execution identity for a reserved run."""

    return uuid.uuid5(_INITIAL_EXECUTION_NAMESPACE, run_id).hex


def _agent_binding_revision(
    *, agent_revision: str, provider_name: str, request_model: str
) -> str:
    """Fingerprint every model binding that affects safe Run recovery."""

    payload = {
        "schema_version": _AGENT_BINDING_REVISION_SCHEMA_VERSION,
        "react_agent_revision": agent_revision,
        "provider_name": provider_name,
        "request_model": request_model,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class StartRun:
    prompt: str
    session_id: str | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumeRun:
    run_id: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ForkRun:
    run_id: str
    from_sequence: int | None = None
    session_id: str | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelRun:
    run_id: str
    reason: str = "user_requested"


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolveRun:
    run_id: str
    call_key: str
    action: ResolutionAction | str
    result: object | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True, kw_only=True)
class AdjustCost:
    run_id: str
    previous_record_id: str
    revised_total_micros: int
    note: str | None = None
    operation_id: str | None = None
    record_id: str | None = None


RunCommand: TypeAlias = (
    StartRun | ResumeRun | ForkRun | CancelRun | ResolveRun | AdjustCost
)


@dataclass(frozen=True, slots=True)
class RunHandle:
    run_id: str
    session_id: str
    execution_id: str | None
    created: bool


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """Public event union used by both live following and historical replay."""

    run_id: str
    kind: str
    timestamp: float
    durable_sequence: int | None = None
    live_sequence: int | None = None
    event_id: str | None = None
    causation_id: str | None = None
    execution_id: str | None = None
    step: int | None = None
    call_key: str | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    public_data: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}), repr=False)
    safe_checkpoint: bool = False
    terminal: bool = False


def _merge_follow_batch(
    durable: Sequence[RuntimeEvent],
    live: Sequence[RuntimeEvent],
) -> tuple[RuntimeEvent, ...]:
    """Merge ordered streams without ever reordering durable facts.

    Provider/live timestamps and journal commit timestamps share a wall clock,
    but that clock is not a sequencing primitive and may move backwards.  A
    global timestamp sort can therefore emit durable sequence ``N + 1`` before
    ``N``.  Preserve each source's authoritative order and use timestamps only
    to choose between the two current heads.  A terminal durable fact is
    deliberately emitted last so the HTTP follower drains every live delta
    already present in this batch before it closes the stream.
    """

    terminal = durable[-1] if durable and durable[-1].terminal else None
    durable_prefix = durable[:-1] if terminal is not None else durable
    merged: list[RuntimeEvent] = []
    durable_index = 0
    live_index = 0
    while durable_index < len(durable_prefix) and live_index < len(live):
        durable_event = durable_prefix[durable_index]
        live_event = live[live_index]
        if live_event.timestamp < durable_event.timestamp:
            merged.append(live_event)
            live_index += 1
        else:
            merged.append(durable_event)
            durable_index += 1
    merged.extend(durable_prefix[durable_index:])
    merged.extend(live[live_index:])
    if terminal is not None:
        merged.append(terminal)
    return tuple(merged)


@dataclass(frozen=True, slots=True)
class RequestReservation:
    run_id: str
    created: bool


@dataclass(frozen=True, slots=True)
class SessionRecord:
    session_id: str
    version: int
    transcript: tuple[Mapping[str, Any], ...] = field(repr=False)
    status: str = "active"


@runtime_checkable
class RuntimeStore(Protocol):
    """Internal multi-process registry seam; PostgreSQL and memory satisfy it."""

    async def reserve_request(
        self,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> object: ...

    async def load_session(self, session_id: str) -> object: ...

    async def claim_active_run(self, session_id: str, run_id: str) -> None: ...

    async def release_active_run(self, session_id: str, run_id: str) -> None: ...

    async def load_trace_reference(
        self, run_id: str, execution_id: str
    ) -> TraceReference | None: ...

    async def put_trace_reference(self, reference: TraceReference) -> None: ...

    async def commit_session(
        self,
        session_id: str,
        *,
        expected_version: int,
        transcript: Sequence[Mapping[str, Any]],
        operation_id: str | None = None,
    ) -> object: ...

    async def list_runs(self, session_id: str, *, limit: int = 100) -> tuple[str, ...]: ...

    async def set_lineage(
        self,
        run_id: str,
        *,
        parent_run_id: str | None,
        fork_sequence: int | None,
        workspace_tree: str | None,
    ) -> None: ...


@dataclass(slots=True)
class _MemorySession:
    version: int = 0
    transcript: tuple[Mapping[str, Any], ...] = ()
    status: str = "active"


class InMemoryRuntimeStore:
    """Disposable registry adapter used with :class:`InMemoryRunJournal`."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._cost_adjustments = InMemoryCostAdjustmentStore()
        self._sessions: dict[str, _MemorySession] = {}
        self._requests: dict[tuple[str, str], tuple[str, str]] = {}
        self._runs: dict[str, list[str]] = {}
        self._active_runs: dict[str, str] = {}
        self._trace_references: dict[tuple[str, str], TraceReference] = {}
        self._lineage: dict[str, tuple[str | None, int | None, str | None]] = {}
        self._session_commits: dict[
            tuple[str, str], tuple[int, str, SessionRecord]
        ] = {}

    async def reserve_request(
        self,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> RequestReservation:
        async with self._lock:
            self._sessions.setdefault(session_id, _MemorySession())
            identity = (session_id, idempotency_key)
            existing = self._requests.get(identity)
            if existing is not None:
                existing_hash, existing_run_id = existing
                if existing_hash != request_hash:
                    raise RequestPayloadConflict(
                        "idempotency key was reused with a different request"
                    )
                return RequestReservation(existing_run_id, False)
            active_run_id = self._active_runs.get(session_id)
            if active_run_id is not None:
                raise SessionBusyError(
                    session_id=session_id,
                    active_run_id=active_run_id,
                )
            self._requests[identity] = (request_hash, proposed_run_id)
            self._runs.setdefault(session_id, []).append(proposed_run_id)
            self._active_runs[session_id] = proposed_run_id
            return RequestReservation(proposed_run_id, True)

    async def load_session(self, session_id: str) -> SessionRecord:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise RuntimeNotFound(f"session not found: {session_id}")
            return SessionRecord(
                session_id,
                session.version,
                session.transcript,
                session.status,
            )

    async def release_active_run(self, session_id: str, run_id: str) -> None:
        async with self._lock:
            active_run_id = self._active_runs.get(session_id)
            if active_run_id is None:
                return
            if active_run_id != run_id:
                raise SessionBusyError(
                    session_id=session_id,
                    active_run_id=active_run_id,
                )
            self._active_runs.pop(session_id, None)

    async def claim_active_run(self, session_id: str, run_id: str) -> None:
        async with self._lock:
            if session_id not in self._sessions:
                raise RuntimeNotFound(f"session not found: {session_id}")
            active_run_id = self._active_runs.get(session_id)
            if active_run_id is None:
                self._active_runs[session_id] = run_id
                return
            if active_run_id != run_id:
                raise SessionBusyError(
                    session_id=session_id,
                    active_run_id=active_run_id,
                )

    async def load_trace_reference(
        self, run_id: str, execution_id: str
    ) -> TraceReference | None:
        if not run_id.strip() or not execution_id.strip():
            raise ValueError("trace reference identities must not be blank")
        async with self._lock:
            return self._trace_references.get((run_id, execution_id))

    async def put_trace_reference(self, reference: TraceReference) -> None:
        async with self._lock:
            self._trace_references.setdefault(
                (reference.run_id, reference.execution_id), reference
            )

    async def append_cost_adjustment(
        self,
        run_id: str,
        draft: CostAdjustmentDraft,
        *,
        previous_record: Mapping[str, Any],
    ) -> CostAdjustmentAppend:
        return await self._cost_adjustments.append_cost_adjustment(
            run_id,
            draft,
            previous_record=previous_record,
        )

    async def list_cost_adjustments(
        self, run_id: str
    ) -> tuple[StoredCostAdjustment, ...]:
        return await self._cost_adjustments.list_cost_adjustments(run_id)

    async def commit_session(
        self,
        session_id: str,
        *,
        expected_version: int,
        transcript: Sequence[Mapping[str, Any]],
        operation_id: str | None = None,
    ) -> SessionRecord:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise RuntimeNotFound(f"session not found: {session_id}")
            payload = tuple(dict(item) for item in transcript)
            payload_hash = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
            resolved_operation = operation_id or (
                f"legacy:{expected_version}:{payload_hash}"
            )
            previous = self._session_commits.get(
                (session_id, resolved_operation)
            )
            if previous is not None:
                previous_expected, previous_hash, previous_record = previous
                if (
                    previous_expected != expected_version
                    or previous_hash != payload_hash
                ):
                    raise SessionVersionConflict(
                        "session commit operation was reused with different content"
                    )
                return previous_record
            if session.version != expected_version:
                raise SessionVersionConflict(
                    f"expected session version {expected_version}, got {session.version}"
                )
            session.version += 1
            session.transcript = payload
            record = SessionRecord(
                session_id,
                session.version,
                session.transcript,
                session.status,
            )
            self._session_commits[(session_id, resolved_operation)] = (
                expected_version,
                payload_hash,
                record,
            )
            return record

    async def list_runs(self, session_id: str, *, limit: int = 100) -> tuple[str, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        async with self._lock:
            return tuple(reversed(self._runs.get(session_id, ())[-limit:]))

    async def set_lineage(
        self,
        run_id: str,
        *,
        parent_run_id: str | None,
        fork_sequence: int | None,
        workspace_tree: str | None,
    ) -> None:
        async with self._lock:
            self._lineage[run_id] = (
                parent_run_id,
                fork_sequence,
                workspace_tree,
            )


class _LiveBus:
    def __init__(self, *, max_events: int = 10_000) -> None:
        self._condition = asyncio.Condition()
        self._events: list[RuntimeEvent] = []
        self._max_events = max_events
        self._sequence = 0

    async def publish(self, event: AgentStreamEvent) -> None:
        async with self._condition:
            self._sequence += 1
            self._events.append(
                RuntimeEvent(
                    run_id=event.run_id,
                    kind=event.kind.value,
                    timestamp=event.timestamp,
                    live_sequence=self._sequence,
                    step=event.step,
                    call_key=event.call_key,
                    tool_call_id=event.tool_call_id,
                    tool_name=event.tool_name,
                    public_data=MappingProxyType(dict(event.data)),
                )
            )
            if len(self._events) > self._max_events:
                del self._events[: len(self._events) - self._max_events]
            self._condition.notify_all()

    async def read(self, after_sequence: int) -> tuple[RuntimeEvent, ...]:
        async with self._condition:
            return tuple(
                event for event in self._events if (event.live_sequence or 0) > after_sequence
            )

    async def tail(self) -> int:
        async with self._condition:
            return self._sequence

    async def wait(self, after_sequence: int, timeout_s: float) -> None:
        async with self._condition:
            if self._sequence > after_sequence:
                return
            try:
                async with asyncio.timeout(timeout_s):
                    await self._condition.wait_for(lambda: self._sequence > after_sequence)
            except TimeoutError:
                pass


def _thaw(value: object) -> Any:
    return json.loads(canonical_json(value))


def _usage_from_public(data: Mapping[str, Any]) -> Usage:
    raw = data.get("usage")
    if not isinstance(raw, Mapping):
        return Usage()

    def optional_int(name: str) -> int | None:
        value = raw.get(name)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None

    return Usage(
        input_tokens=int(raw.get("input_tokens", 0) or 0),
        output_tokens=int(raw.get("output_tokens", 0) or 0),
        total_tokens=int(raw.get("total_tokens", 0) or 0),
        cached_input_tokens=optional_int("cached_input_tokens"),
        reasoning_output_tokens=optional_int("reasoning_output_tokens"),
        billable_tokens=optional_int("billable_tokens"),
    )


_PROGRESS_EVENT_KINDS: frozenset[RunEventKind] = frozenset(
    {
        RunEventKind.CONTEXT_GOVERNED,
        RunEventKind.MODEL_STARTED,
        RunEventKind.MODEL_COMPLETED,
        RunEventKind.MODEL_FAILED,
        RunEventKind.MODEL_ABANDONED,
        RunEventKind.TOOL_PLANNED,
        RunEventKind.TOOL_STARTED,
        RunEventKind.TOOL_CLAIMED,
        RunEventKind.TOOL_COMPLETED,
        RunEventKind.TOOL_REUSED,
        RunEventKind.BUDGET_EXHAUSTED,
        RunEventKind.LOOP_DETECTED,
        RunEventKind.RECONCILIATION_REQUIRED,
    }
)


def _last_progress_event(events: Sequence[StoredRunEvent]) -> StoredRunEvent | None:
    """The last orchestration-progress fact; lifecycle and bookkeeping are skipped."""

    return next(
        (event for event in reversed(events) if event.kind in _PROGRESS_EVENT_KINDS),
        None,
    )


_AGENT_KIND_MAP: Mapping[AgentJournalEventKind, RunEventKind] = MappingProxyType(
    {
        AgentJournalEventKind.RUN_STARTED: RunEventKind.RUN_STARTED,
        AgentJournalEventKind.RUN_RESUMED: RunEventKind.RUN_RESUMED,
        AgentJournalEventKind.CONTEXT_GOVERNED: RunEventKind.CONTEXT_GOVERNED,
        AgentJournalEventKind.MODEL_STARTED: RunEventKind.MODEL_STARTED,
        AgentJournalEventKind.MODEL_COMPLETED: RunEventKind.MODEL_COMPLETED,
        AgentJournalEventKind.MODEL_FAILED: RunEventKind.MODEL_FAILED,
        AgentJournalEventKind.TOOL_PLANNED: RunEventKind.TOOL_PLANNED,
        AgentJournalEventKind.TOOL_STARTED: RunEventKind.TOOL_STARTED,
        AgentJournalEventKind.TOOL_COMPLETED: RunEventKind.TOOL_COMPLETED,
        AgentJournalEventKind.TOOL_REUSED: RunEventKind.TOOL_REUSED,
        AgentJournalEventKind.BUDGET_EXHAUSTED: RunEventKind.BUDGET_EXHAUSTED,
        AgentJournalEventKind.LOOP_DETECTED: RunEventKind.LOOP_DETECTED,
        # The Agent has finished computing, but only the Runtime may commit the
        # Session and append the canonical terminal fact.
        AgentJournalEventKind.RUN_COMPLETED: RunEventKind.RUN_RESULT_READY,
    }
)


def agent_event_to_draft(
    event: AgentJournalEvent,
    *,
    session_id: str,
    session_version: int,
    agent_revision: str,
    tool_manifest_hash: str,
    model_attempt: int = 1,
) -> RunEventDraft:
    """Translate the Agent's dotted vocabulary into the durable contract."""

    kind = _AGENT_KIND_MAP[event.kind]
    data: dict[str, Any] = dict(event.public_data)
    if event.tool_call_id is not None:
        data["tool_call_id"] = event.tool_call_id
    if event.tool_name is not None:
        data["tool_name"] = event.tool_name
    if event.kind in {
        AgentJournalEventKind.MODEL_STARTED,
        AgentJournalEventKind.MODEL_COMPLETED,
        AgentJournalEventKind.MODEL_FAILED,
    }:
        data["attempt"] = model_attempt
    if event.kind is AgentJournalEventKind.RUN_STARTED:
        data["session_version"] = session_version
        data["agent_revision"] = agent_revision
        data["tool_manifest_hash"] = tool_manifest_hash

    usage = (
        _usage_from_public(event.public_data)
        if event.kind
        in {
            AgentJournalEventKind.CONTEXT_GOVERNED,
            AgentJournalEventKind.MODEL_COMPLETED,
        }
        else Usage()
    )
    checkpoint = dict(event.private_data) if event.private_data else None
    privacy = PrivacyClass.PRIVATE if checkpoint is not None else PrivacyClass.METADATA
    safe_checkpoint = event.kind in {
        AgentJournalEventKind.RUN_STARTED,
        AgentJournalEventKind.MODEL_COMPLETED,
        AgentJournalEventKind.MODEL_FAILED,
        AgentJournalEventKind.TOOL_PLANNED,
        AgentJournalEventKind.BUDGET_EXHAUSTED,
        AgentJournalEventKind.LOOP_DETECTED,
        AgentJournalEventKind.RUN_COMPLETED,
    }
    terminal_model_tool_calls = 0
    if (
        event.kind is AgentJournalEventKind.MODEL_COMPLETED
        and event.public_data.get("outcome") in {"incomplete", "refused"}
    ):
        raw_tool_calls = event.public_data.get("tool_calls")
        if (
            isinstance(raw_tool_calls, int)
            and not isinstance(raw_tool_calls, bool)
            and raw_tool_calls >= 0
        ):
            terminal_model_tool_calls = raw_tool_calls
    raw_compression_calls = event.public_data.get("compression_calls", 0)
    declared_compression_calls = (
        raw_compression_calls
        if isinstance(raw_compression_calls, int)
        and not isinstance(raw_compression_calls, bool)
        and raw_compression_calls >= 0
        else 0
    )
    compression_phase = event.public_data.get("compression_phase")
    if (
        event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
        and compression_phase == "projection_completed"
        and event.public_data.get("compression_accounted_in_terminal") is True
    ):
        usage = Usage()
    compression_calls = (
        declared_compression_calls
        if compression_phase in {"completed", "failed", "abandoned"}
        or (
            compression_phase in {None, "projection_completed"}
            and event.public_data.get("compression_accounted_in_terminal") is not True
        )
        else 0
    )
    return RunEventDraft(
        kind=kind,
        privacy=privacy,
        # Store time is authoritative. Excluding observer time from the draft
        # also makes the synthetic pre-create and Agent retry byte-identical.
        occurred_at=None,
        step=event.step,
        call_key=event.call_key,
        session_id=session_id if event.kind is AgentJournalEventKind.RUN_STARTED else None,
        execution_id=event.execution_id,
        agent_revision=(
            agent_revision if event.kind is AgentJournalEventKind.RUN_STARTED else None
        ),
        tool_manifest_hash=(
            tool_manifest_hash if event.kind is AgentJournalEventKind.RUN_STARTED else None
        ),
        data=data,
        checkpoint=checkpoint,
        safe_checkpoint=safe_checkpoint,
        usage_delta=usage,
        model_calls_delta=(
            1
            if event.kind is AgentJournalEventKind.MODEL_STARTED
            else (
                compression_calls
                if event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
                else 0
            )
        ),
        tool_calls_delta=(
            1
            if event.kind is AgentJournalEventKind.TOOL_PLANNED
            else terminal_model_tool_calls
        ),
        tool_executions_delta=(
            1
            if event.kind is AgentJournalEventKind.TOOL_COMPLETED
            and event.public_data.get("executed") is True
            else 0
        ),
    )


def _interrupted_compression_abandonments(
    events: Sequence[StoredRunEvent],
    *,
    execution_id: str,
) -> tuple[tuple[RunEventDraft, str], ...]:
    """Close unmatched compression starts before a resumed attempt retries.

    A process can die after the write-ahead ``started`` fact but before the
    compressor result or failure is durable.  The next execution records that
    uncertainty explicitly; a content-addressed result already in the summary
    store can still be reused by the governor, otherwise a fresh call is made.
    """

    def compression_identity(event: StoredRunEvent) -> tuple[str | None, JsonValue]:
        private_compression = (
            event.checkpoint.get("context_compression")
            if event.checkpoint is not None
            else None
        )
        private_summary_key = (
            private_compression.get("summary_key")
            if isinstance(private_compression, Mapping)
            else None
        )
        summary_key = (
            private_summary_key
            if isinstance(private_summary_key, str)
            else event.data.get("summary_key")
        )
        source_hash = (
            private_compression.get("source_hash")
            if isinstance(private_compression, Mapping)
            else event.data.get("source_hash")
        )
        return (
            summary_key if isinstance(summary_key, str) and summary_key else None,
            cast(JsonValue, source_hash),
        )

    pending: dict[str, list[StoredRunEvent]] = {}
    terminal_phases = {"completed", "failed", "abandoned"}
    for event in events:
        if event.kind is not RunEventKind.CONTEXT_GOVERNED:
            continue
        phase = event.data.get("compression_phase")
        summary_key, _ = compression_identity(event)
        if summary_key is None:
            continue
        if phase == "started":
            pending.setdefault(summary_key, []).append(event)
        elif phase in terminal_phases:
            starts = pending.get(summary_key)
            if starts:
                starts.pop()
                if not starts:
                    pending.pop(summary_key, None)

    abandonments: list[tuple[RunEventDraft, str]] = []
    unmatched = sorted(
        (event for starts in pending.values() for event in starts),
        key=lambda event: event.sequence,
    )
    for started in unmatched:
        summary_key, source_hash = compression_identity(started)
        assert summary_key is not None
        attempted = started.data.get("attempted_model_calls")
        attempted_calls = (
            attempted
            if isinstance(attempted, int)
            and not isinstance(attempted, bool)
            and attempted > 0
            else 1
        )
        abandonments.append(
            (
                RunEventDraft(
                    kind=RunEventKind.CONTEXT_GOVERNED,
                    privacy=PrivacyClass.PRIVATE,
                    occurred_at=started.occurred_at,
                    step=started.step,
                    execution_id=execution_id,
                    data={
                        "compression_phase": "abandoned",
                        "compression_source_chars": started.data.get(
                            "compression_source_chars"
                        ),
                        "compressor_revision": started.data.get(
                            "compressor_revision"
                        ),
                        "attempted_model_calls": attempted_calls,
                        "compression_calls": attempted_calls,
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "total_tokens": 0,
                            "cached_input_tokens": None,
                            "reasoning_output_tokens": None,
                            "billable_tokens": None,
                        },
                        "compression_error": "process_interrupted_before_terminal",
                        "cost_unknown": True,
                        "recovered_interruption": True,
                    },
                    checkpoint={
                        "context_compression": {
                            "summary_key": summary_key,
                            "source_hash": source_hash,
                        }
                    },
                    model_calls_delta=attempted_calls,
                ),
                f"{started.operation_id}:resume_abandoned",
            )
        )
    return tuple(abandonments)


def _cost_payload(record: object) -> dict[str, Any]:
    # Kept duck-typed to make the frozen CostRecord interface the only thing
    # this orchestration module needs to know.
    value = cast(Any, record)
    return {
        "record_id": value.record_id,
        "operation_id": value.operation_id,
        "kind": value.kind.value,
        "amount_micros": value.amount_micros,
        "currency": value.currency,
        "provider": value.provider,
        "model": value.model,
        "request_model": value.model,
        "response_model": value.response_model or value.model,
        "source": value.source.value,
        "pricing_source": value.source.value,
        "catalog_version": value.catalog_version,
        "pricing_catalog_version": value.catalog_version,
        "price_version": value.price_version,
        "priced_at": value.priced_at.isoformat(),
        "price_effective_from": (
            value.price_effective_from.isoformat()
            if value.price_effective_from is not None
            else None
        ),
        "unit_prices_per_million": {
            "input": (
                str(value.input_per_million)
                if value.input_per_million is not None
                else None
            ),
            "output": (
                str(value.output_per_million)
                if value.output_per_million is not None
                else None
            ),
            "cached_input": (
                str(value.cached_input_per_million)
                if value.cached_input_per_million is not None
                else None
            ),
            "reasoning_output": (
                str(value.reasoning_output_per_million)
                if value.reasoning_output_per_million is not None
                else None
            ),
        },
        "estimated": value.source.value == "catalog_estimate",
        "adjusts_record_id": value.adjusts_record_id,
        "unknown_reason": value.unknown_reason,
        "usage": {
            "input_tokens": value.usage.input_tokens,
            "output_tokens": value.usage.output_tokens,
            "cached_input_tokens": value.usage.cached_input_tokens,
            "reasoning_output_tokens": value.usage.reasoning_output_tokens,
            "billable_tokens": value.usage.billable_tokens,
        },
    }


def _workspace_checkpoint_payload(
    checkpoint: WorkspaceCheckpoint,
    *,
    phase: str,
    call_key: str | None,
) -> dict[str, Any]:
    """Return the durable, content-free identity of one immutable checkpoint."""

    diff = checkpoint.diff
    return {
        "phase": phase,
        "call_key": call_key,
        "checkpoint_id": checkpoint.checkpoint_id,
        "baseline_revision": checkpoint.baseline_revision,
        "tree_id": checkpoint.tree_id,
        "commit_id": checkpoint.commit_id,
        "internal_ref": checkpoint.internal_ref,
        "created_at": checkpoint.created_at,
        "diff": {
            "files_changed": diff.files_changed,
            "additions": diff.additions,
            "deletions": diff.deletions,
            "binary_files": diff.binary_files,
            "paths": list(diff.paths),
        },
    }


class _JournalWriter:
    """Execution-local serialized CAS writer with a fencing lease."""

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        run_id: str,
        session_id: str,
        session_version: int,
        execution_id: str,
        snapshot: RunSnapshot | None = None,
    ) -> None:
        self.runtime = runtime
        self.run_id = run_id
        self.session_id = session_id
        self.session_version = session_version
        self.execution_id = execution_id
        self.sequence = snapshot.last_sequence if snapshot is not None else 0
        self.lease: JournalLease | None = None
        self._lock = asyncio.Lock()
        self._model_attempts = dict(snapshot.model_attempts) if snapshot else {}
        self._operation_attempts: dict[str, int] = {}
        self._active_tool_steps: dict[str, int] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._execution_task: asyncio.Task[None] | None = None
        self._lease_error: BaseException | None = None
        self._lifecycle_recorded = False
        self._pending_lifecycle_telemetry: StoredRunEvent | None = None

    async def acquire(self) -> None:
        acquire_operation = self.runtime.journal.acquire(
            self.run_id,
            owner=self.runtime.worker_id,
            ttl_s=self.runtime.lease_ttl_s,
        )
        try:
            acquire_task = asyncio.create_task(
                acquire_operation,
                name=f"react-acquire:{self.run_id}:{self.execution_id}",
            )
        except BaseException:
            acquire_operation.close()
            raise
        try:
            lease = await asyncio.shield(acquire_task)
        except asyncio.CancelledError as cancellation:
            # The adapter may already have committed the lease before its
            # coroutine returns the fencing token. Shield the acquisition so
            # cancellation cannot make that outcome unknowable; if ownership
            # was obtained, release it before preserving caller cancellation.
            try:
                acquired = await acquire_task
            except BaseException:
                raise cancellation from None
            with contextlib.suppress(BaseException):
                await asyncio.shield(self.runtime.journal.release(acquired))
            raise cancellation
        except LeaseConflictError as exc:
            raise RuntimeConflict(str(exc)) from exc
        self.lease = lease
        self._lease_error = None
        heartbeat = self._keep_lease_alive()
        try:
            self._heartbeat_task = asyncio.create_task(
                heartbeat,
                name=f"react-lease:{self.run_id}:{self.execution_id}",
            )
        except BaseException:
            # ``journal.acquire`` has already committed ownership.  If local
            # heartbeat startup fails, relinquish that ownership before the
            # exception escapes; otherwise no handoff context has entered yet
            # and a fencing lease could remain live until its TTL expires.
            heartbeat.close()
            lease = self.lease
            self.lease = None
            if lease is not None:
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self.runtime.journal.release(lease))
            raise

    async def project_pending_lifecycle(self) -> None:
        """Open a deferred first-execution root after all Start preflight passes."""

        pending_telemetry = self._pending_lifecycle_telemetry
        self._pending_lifecycle_telemetry = None
        if pending_telemetry is not None:
            # run_started is committed before fencing can be acquired so an
            # idempotent repair can fill the reservation/run gap. Only the
            # eventual lease holder may open and persist that execution root.
            await self.runtime._project_telemetry(pending_telemetry)

    def bind_execution_task(self, task: asyncio.Task[None]) -> None:
        """Associate the owned lease with the coroutine issuing side effects."""

        self._execution_task = task
        if self._lease_error is not None and not task.done():
            task.cancel("run fencing lease was lost")

    async def _keep_lease_alive(self) -> None:
        interval_s = min(5.0, self.runtime.lease_ttl_s / 3)
        try:
            while True:
                await asyncio.sleep(interval_s)
                execution_task: asyncio.Task[None] | None = None
                async with self._lock:
                    if self.lease is None:
                        return
                    try:
                        self.lease = await self.runtime.journal.renew(
                            self.lease,
                            ttl_s=self.runtime.lease_ttl_s,
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        # Losing a fencing lease makes every later write and
                        # side effect unsafe. Cancel an in-flight provider/tool
                        # await promptly; a future execution will recover from
                        # the last committed fact.
                        self._lease_error = exc
                        self.lease = None
                        execution_task = self._execution_task
                if execution_task is not None and not execution_task.done():
                    execution_task.cancel("run fencing lease was lost")
                    return
        except asyncio.CancelledError:
            raise

    async def guard(self) -> None:
        async with self._lock:
            if self._lease_error is not None:
                raise LeaseLostError("execution lost its fencing lease") from self._lease_error
            if self.lease is None:
                raise LeaseLostError("execution has no fencing lease")
            self.lease = await self.runtime.journal.renew(
                self.lease, ttl_s=self.runtime.lease_ttl_s
            )

    async def release(self) -> None:
        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat is not None and heartbeat is not asyncio.current_task():
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        async with self._lock:
            if self.lease is None:
                return
            lease = self.lease
            self.lease = None
            with contextlib.suppress(LeaseLostError, RunNotFoundError):
                await self.runtime.journal.release(lease)

    async def _append_unlocked(self, draft: RunEventDraft, *, operation_id: str) -> StoredRunEvent:
        previous_sequence = self.sequence
        if draft.kind is RunEventKind.RUN_STARTED:
            committed = await self.runtime.journal.create(
                self.run_id, draft, operation_id=operation_id
            )
        else:
            committed = await self.runtime.journal.append(
                self.run_id,
                draft,
                expected_sequence=self.sequence,
                operation_id=operation_id,
                lease=self.lease,
            )
        self.sequence = max(self.sequence, committed.sequence)
        if (
            draft.kind in {RunEventKind.RUN_STARTED, RunEventKind.RUN_RESUMED}
            and committed.execution_id == self.execution_id
        ):
            self._lifecycle_recorded = True
        # Idempotent retries return the original committed event. Project each
        # durable sequence at most once from this writer so telemetry counters
        # and logs do not double-count a retry.
        if committed.sequence > previous_sequence:
            if committed.kind is RunEventKind.RUN_STARTED and self.lease is None:
                self._pending_lifecycle_telemetry = committed
            else:
                await self.runtime._project_telemetry(committed)
        if committed.kind in {RunEventKind.RUN_COMPLETED, RunEventKind.RUN_ABORTED}:
            await self.runtime.store.release_active_run(self.session_id, self.run_id)
        return committed

    async def append(self, draft: RunEventDraft, *, operation_id: str) -> StoredRunEvent:
        async with self._lock:
            return await self._append_unlocked(draft, operation_id=operation_id)

    async def _append_many_unlocked(
        self,
        entries: Sequence[tuple[RunEventDraft, str]],
    ) -> tuple[StoredRunEvent, ...]:
        previous_sequence = self.sequence
        committed = await self.runtime.journal.append_many(
            self.run_id,
            entries,
            expected_sequence=self.sequence,
            lease=self.lease,
        )
        self.sequence = max(self.sequence, committed[-1].sequence)
        for event in committed:
            if (
                event.kind in {RunEventKind.RUN_STARTED, RunEventKind.RUN_RESUMED}
                and event.execution_id == self.execution_id
            ):
                self._lifecycle_recorded = True
            if event.sequence > previous_sequence:
                await self.runtime._project_telemetry(event)
        if committed[-1].kind in {
            RunEventKind.RUN_COMPLETED,
            RunEventKind.RUN_ABORTED,
        }:
            await self.runtime.store.release_active_run(self.session_id, self.run_id)
        return committed

    async def append_many(
        self,
        entries: Sequence[tuple[RunEventDraft, str]],
    ) -> tuple[StoredRunEvent, ...]:
        async with self._lock:
            return await self._append_many_unlocked(entries)

    def _attempt_for(self, event: AgentJournalEvent) -> int:
        if event.operation_id in self._operation_attempts:
            return self._operation_attempts[event.operation_id]
        step = event.step or 0
        if event.kind is AgentJournalEventKind.MODEL_STARTED:
            attempt = self._model_attempts.get(step, 0) + 1
            self._model_attempts[step] = attempt
        else:
            attempt = max(1, self._model_attempts.get(step, 1))
        self._operation_attempts[event.operation_id] = attempt
        return attempt

    async def record_agent(self, event: AgentJournalEvent) -> None:
        if event.run_id != self.run_id or event.execution_id != self.execution_id:
            raise RuntimeConflict("Agent journal fact has the wrong execution identity")
        async with self._lock:
            if (
                event.kind
                in {
                    AgentJournalEventKind.RUN_STARTED,
                    AgentJournalEventKind.RUN_RESUMED,
                }
                and self._lifecycle_recorded
            ):
                return
            operation_id = (
                "run:result_ready"
                if event.kind is AgentJournalEventKind.RUN_COMPLETED
                else event.operation_id
            )
            attempt = self._attempt_for(event)
            if event.kind is AgentJournalEventKind.MODEL_STARTED:
                checkpoint = RunEventDraft(
                    kind=RunEventKind.CHECKPOINT,
                    privacy=PrivacyClass.PRIVATE,
                    occurred_at=event.timestamp,
                    step=event.step,
                    execution_id=event.execution_id,
                    data={"phase": "before_model", "attempt": attempt},
                    checkpoint=dict(event.private_data),
                    safe_checkpoint=True,
                )
                await self._append_unlocked(
                    checkpoint,
                    operation_id=f"{operation_id}:checkpoint",
                )
            if (
                self.runtime.workspace is not None
                and event.kind is AgentJournalEventKind.TOOL_STARTED
            ):
                checkpoint_is_safe = not any(
                    active_call_key != event.call_key and active_step == event.step
                    for active_call_key, active_step in self._active_tool_steps.items()
                )
                await self._workspace_checkpoint_unlocked(
                    phase="before_tool",
                    call_key=event.call_key,
                    safe=checkpoint_is_safe,
                )

            draft = agent_event_to_draft(
                event,
                session_id=self.session_id,
                session_version=self.session_version,
                agent_revision=self.runtime.agent_revision,
                tool_manifest_hash=self.runtime.agent.tool_manifest_hash,
                model_attempt=attempt,
            )
            raw_compression_calls = event.public_data.get("compression_calls")
            compression_phase = event.public_data.get("compression_phase")
            compression_has_calls = (
                event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
                and isinstance(raw_compression_calls, int)
                and not isinstance(raw_compression_calls, bool)
                and raw_compression_calls > 0
            )
            terminal_compression = compression_has_calls and compression_phase in {
                "completed",
                "failed",
                "abandoned",
            }
            legacy_projection_compression = (
                compression_has_calls
                and compression_phase in {None, "projection_completed"}
                and event.public_data.get("compression_accounted_in_terminal") is not True
            )
            unknown_compression = (
                terminal_compression and event.public_data.get("cost_unknown") is True
            )
            priced_compression = (
                terminal_compression and not unknown_compression
            ) or legacy_projection_compression
            if event.kind is AgentJournalEventKind.MODEL_COMPLETED or priced_compression:
                draft, cost_draft = self.runtime._price_model_event(
                    draft, event=event, attempt=attempt
                )
                await self._append_many_unlocked(
                    (
                        (draft, operation_id),
                        (cost_draft, f"cost:{operation_id}"),
                    )
                )
            elif unknown_compression:
                draft, cost_draft = self.runtime._unknown_compression_cost_event(
                    draft,
                    event=event,
                )
                await self._append_many_unlocked(
                    (
                        (draft, operation_id),
                        (cost_draft, f"cost:{operation_id}"),
                    )
                )
            elif (
                self.runtime.workspace is not None
                and event.kind is AgentJournalEventKind.TOOL_COMPLETED
            ):
                workspace_entry = await self._workspace_checkpoint_entry(
                    phase="after_tool",
                    call_key=event.call_key,
                    safe=False,
                )
                await self._append_many_unlocked(
                    ((draft, operation_id), workspace_entry)
                )
            else:
                await self._append_unlocked(draft, operation_id=operation_id)
            if event.kind is AgentJournalEventKind.TOOL_STARTED and event.call_key is not None:
                self._active_tool_steps[event.call_key] = event.step or 0
            elif (
                event.kind is AgentJournalEventKind.TOOL_COMPLETED
                and event.call_key is not None
            ):
                self._active_tool_steps.pop(event.call_key, None)

    async def _workspace_checkpoint_entry(
        self,
        *,
        phase: str,
        call_key: str | None,
        safe: bool,
        checkpoint: WorkspaceCheckpoint | None = None,
        execution_id: str | None = None,
    ) -> tuple[RunEventDraft, str]:
        assert self.runtime.workspace is not None
        resolved_checkpoint = checkpoint
        if resolved_checkpoint is None:
            resolved_checkpoint = await asyncio.to_thread(
                self.runtime.workspace.checkpoint,
                self.session_id,
            )
        resolved_execution_id = execution_id or self.execution_id
        draft = RunEventDraft(
            kind=RunEventKind.WORKSPACE_CHECKPOINTED,
            occurred_at=resolved_checkpoint.created_at,
            execution_id=resolved_execution_id,
            call_key=None,
            data=_workspace_checkpoint_payload(
                resolved_checkpoint,
                phase=phase,
                call_key=call_key,
            ),
            safe_checkpoint=safe,
        )
        return (
            draft,
            (
                f"workspace:{phase}:{call_key or 'run'}:{resolved_execution_id}:"
                f"{resolved_checkpoint.checkpoint_id}"
            ),
        )

    async def _workspace_checkpoint_unlocked(
        self,
        *,
        phase: str,
        call_key: str | None,
        safe: bool,
        checkpoint: WorkspaceCheckpoint | None = None,
        execution_id: str | None = None,
    ) -> StoredRunEvent:
        draft, operation_id = await self._workspace_checkpoint_entry(
            phase=phase,
            call_key=call_key,
            safe=safe,
            checkpoint=checkpoint,
            execution_id=execution_id,
        )
        return await self._append_unlocked(draft, operation_id=operation_id)


@dataclass(slots=True)
class _WriterLeaseHandoff:
    """Release an acquired writer unless ownership reaches a scheduled task."""

    writer: _JournalWriter
    acquired: bool = False
    transferred: bool = False

    async def __aenter__(self) -> _WriterLeaseHandoff:
        await self.writer.acquire()
        self.acquired = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, traceback
        if not self.acquired or self.transferred:
            return
        if exc is None:
            await self.writer.release()
            return
        # Preserve the preflight error. release() has already stopped the
        # heartbeat before touching the adapter, so even a transient release
        # outage cannot leave a process-local renewer running forever.
        with contextlib.suppress(Exception):
            await self.writer.release()

    def transfer(self) -> None:
        if not self.acquired:
            raise RuntimeError("cannot transfer a writer before acquiring its lease")
        self.transferred = True


class AgentRuntime:
    """Deep module for durable Start/Resume/Fork/Cancel/Resolve execution."""

    def __init__(
        self,
        agent: ReActAgent,
        journal: RunJournal,
        *,
        store: RuntimeStore | None = None,
        cost_store: CostAdjustmentStore | None = None,
        telemetry: Telemetry | None = None,
        pricing: PricingCatalog | None = None,
        provider_name: str = "openai_compatible",
        model_name: str | None = None,
        workspace: WorkspaceCheckpointStore | None = None,
        worker_id: str | None = None,
        lease_ttl_s: float = 30.0,
        max_retained_runs: int = 256,
    ) -> None:
        if lease_ttl_s <= 0:
            raise ValueError("lease_ttl_s must be positive")
        if max_retained_runs < 1:
            raise ValueError("max_retained_runs must be positive")
        self.agent = agent
        self.journal = journal
        if store is not None:
            self.store = store
        elif isinstance(journal, RuntimeStore):
            self.store = cast(RuntimeStore, journal)
        else:
            self.store = InMemoryRuntimeStore()
        if cost_store is not None:
            self.cost_store = cost_store
        elif isinstance(self.store, CostAdjustmentStore):
            self.cost_store = cast(CostAdjustmentStore, self.store)
        else:
            self.cost_store = InMemoryCostAdjustmentStore()
        self.telemetry = telemetry or NoOpTelemetry()
        self.pricing = pricing or PricingCatalog("unconfigured", ())
        self.provider_name = provider_name
        request_model = model_name or getattr(agent.model, "model", None)
        self.model_name = str(request_model or type(agent.model).__name__)
        self.agent_revision = _agent_binding_revision(
            agent_revision=agent.revision,
            provider_name=self.provider_name,
            # Test doubles and custom providers may not expose a request model.
            # Their Python class name is useful for display, but is not a model
            # binding and must not make equivalent Runtime instances diverge.
            request_model=str(request_model or "unspecified"),
        )
        self.workspace = workspace
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex}"
        self.lease_ttl_s = lease_ttl_s
        self.max_retained_runs = max_retained_runs
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._writers: dict[str, _JournalWriter] = {}
        self._buses: dict[str, _LiveBus] = {}
        self._results: dict[str, AgentResult] = {}
        self._task_errors: dict[str, BaseException] = {}
        self._workspace_paths: dict[str, Path] = {}
        self._coordination_lock = asyncio.Lock()

    def _is_run_idle(self, run_id: str) -> bool:
        task = self._tasks.get(run_id)
        return task is None or task.done()

    def _forget_finished_runs(self) -> None:
        """Bound disposable process-local coordination state.

        The journal is the source of truth; live buses, in-process results and
        task errors are caches. A long-lived web process would otherwise retain
        one bus (up to ``_LiveBus._max_events`` deltas), one full transcript and
        one traceback for every run it has ever executed.

        Only runs with no live execution are evicted. Producers and followers
        resolve their bus once and keep that reference, so dropping the entry
        cannot split an in-flight pair; a follower attaching later gets a fresh
        bus, which is correct because live deltas are never replayable.
        """

        active_sessions = {
            writer.session_id
            for run_id, writer in self._writers.items()
            if not self._is_run_idle(run_id)
        }
        for cache in (self._buses, self._results, self._task_errors):
            while len(cache) > self.max_retained_runs:
                evicted = next(
                    (run_id for run_id in cache if self._is_run_idle(run_id)),
                    None,
                )
                if evicted is None:
                    break
                cache.pop(evicted, None)
        while len(self._workspace_paths) > self.max_retained_runs:
            evicted_session = next(
                (
                    session_id
                    for session_id in self._workspace_paths
                    if session_id not in active_sessions
                ),
                None,
            )
            if evicted_session is None:
                break
            self._workspace_paths.pop(evicted_session, None)

    async def submit(self, command: RunCommand) -> RunHandle:
        self._forget_finished_runs()
        if isinstance(command, StartRun):
            return await self._submit_start(command)
        if isinstance(command, ResumeRun):
            return await self._submit_resume(command.run_id)
        if isinstance(command, ForkRun):
            return await self._submit_fork(command)
        if isinstance(command, CancelRun):
            return await self._submit_cancel(command)
        if isinstance(command, ResolveRun):
            return await self._submit_resolve(command)
        if isinstance(command, AdjustCost):
            return await self._submit_adjust_cost(command)
        raise TypeError(f"unsupported Runtime command: {type(command).__name__}")

    async def load(self, run_id: str) -> RunSnapshot:
        try:
            snapshot = await self.journal.load(run_id)
        except RunNotFoundError as exc:
            raise RuntimeNotFound(str(exc)) from exc
        adjustments = await self.cost_store.list_cost_adjustments(run_id)
        if not adjustments:
            return snapshot
        return replace(
            snapshot,
            costs=(
                *snapshot.costs,
                *(record.public_payload for record in adjustments),
            ),
        )

    async def read_events(
        self, run_id: str, *, after_sequence: int = 0
    ) -> tuple[StoredRunEvent, ...]:
        """Return the stored chain itself, not a projection of it.

        ``load`` and ``follow`` both hand back reduced or safety-filtered views.
        Auditing the chain — verifying its hashes, or reconstructing which tool
        call moved the workspace tree — needs the stored events verbatim.
        """

        try:
            return await self.journal.read(run_id, after_sequence=after_sequence)
        except RunNotFoundError as exc:
            raise RuntimeNotFound(str(exc)) from exc

    async def _submit_adjust_cost(self, command: AdjustCost) -> RunHandle:
        snapshot = await self.load(command.run_id)
        if snapshot.session_id is None:
            raise RuntimeConflict("run has no Session identity")
        previous = next(
            (
                record
                for record in reversed(snapshot.costs)
                if record.get("record_id") == command.previous_record_id
            ),
            None,
        )
        if previous is None:
            raise RuntimeConflict(
                f"cost record not found: {command.previous_record_id}"
            )
        operation_id = command.operation_id or f"generated:{uuid.uuid4().hex}"
        record_id = command.record_id or deterministic_adjustment_record_id(
            command.run_id,
            operation_id,
        )
        draft = CostAdjustmentDraft(
            record_id=record_id,
            operation_id=operation_id,
            previous_record_id=command.previous_record_id,
            revised_total_micros=command.revised_total_micros,
            note=command.note,
        )
        try:
            appended = await self.cost_store.append_cost_adjustment(
                command.run_id,
                draft,
                previous_record=previous,
            )
        except CostAdjustmentError as exc:
            raise RuntimeConflict(str(exc)) from exc
        return RunHandle(
            run_id=command.run_id,
            session_id=snapshot.session_id,
            execution_id=snapshot.execution_id,
            created=appended.created,
        )

    async def list_session_runs(
        self, session_id: str, *, limit: int = 100
    ) -> tuple[RunSnapshot, ...]:
        run_ids = await self.store.list_runs(session_id, limit=limit)
        snapshots: list[RunSnapshot] = []
        for run_id in run_ids:
            with contextlib.suppress(RuntimeNotFound):
                snapshots.append(await self.load(run_id))
        return tuple(snapshots)

    async def follow(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        live: bool = True,
    ) -> AsyncIterator[RuntimeEvent]:
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if not live:
            # A history read consumes no live deltas. Registering a bus for it
            # would let a client grow this process's state one entry per run id
            # it browses, without ever scheduling work that prunes them.
            try:
                history = await self.journal.read(run_id, after_sequence=after_sequence)
            except RunNotFoundError as exc:
                raise RuntimeNotFound(str(exc)) from exc
            for stored in history:
                yield self._public_event(stored)
            return
        self._forget_finished_runs()
        bus = self._buses.get(run_id)
        if bus is None:
            bus = _LiveBus()
            self._buses[run_id] = bus
        durable_cursor = after_sequence
        # A reconnect has a durable cursor and must not replay transient token
        # deltas. A first subscription (cursor 0) can still drain the short
        # in-memory backlog produced between POST /runs and opening SSE.
        live_cursor = await bus.tail() if after_sequence > 0 else 0
        while True:
            try:
                durable = await self.journal.read(run_id, after_sequence=durable_cursor)
            except RunNotFoundError as exc:
                raise RuntimeNotFound(str(exc)) from exc
            public_durable = tuple(self._public_event(event) for event in durable)
            if durable:
                durable_cursor = durable[-1].sequence
            terminal_seen = bool(durable) and durable[-1].kind in {
                RunEventKind.RUN_COMPLETED,
                RunEventKind.RUN_ABORTED,
            }

            live_events = await bus.read(live_cursor)
            if live_events:
                live_cursor = live_events[-1].live_sequence or live_cursor
            merged = _merge_follow_batch(public_durable, live_events)
            for event in merged:
                yield event
            if terminal_seen:
                return
            await bus.wait(live_cursor, 0.1)
            # Durable polling remains authoritative and also works when the
            # producing Runtime is in another process and NOTIFY is dropped.
            await self.journal.wait(
                run_id,
                after_sequence=durable_cursor,
                timeout_s=0.1,
            )

    async def wait(self, run_id: str, *, timeout_s: float = 130.0) -> RunSnapshot:
        async def terminal() -> RunSnapshot:
            async for _ in self.follow(run_id, live=True):
                pass
            return await self.load(run_id)

        try:
            async with asyncio.timeout(timeout_s):
                return await terminal()
        except TimeoutError:
            return await self.load(run_id)

    async def close(self) -> None:
        # Snapshot writers before cancellation. A task's done callback removes
        # process-local registry entries, and cancellation can interrupt that
        # same task while it is awaiting its finally-block release. Keeping
        # this snapshot preserves close() as the idempotent release backstop.
        writers = tuple(self._writers.values())
        tasks = tuple(task for task in self._tasks.values() if not task.done())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for writer in writers:
            await writer.release()
        await self.agent.registry.close()
        # Every cache below is rebuildable from the journal. Dropping them keeps
        # close() a full release of process-local state, not just of leases.
        self._buses.clear()
        self._results.clear()
        self._task_errors.clear()
        self._workspace_paths.clear()

    async def _reserve_session_request(
        self,
        *,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> object:
        """Reserve a Session owner, healing only a provably terminal stale claim."""

        try:
            return await self.store.reserve_request(
                session_id,
                idempotency_key,
                request_hash,
                proposed_run_id,
            )
        except SessionBusyError as exc:
            try:
                owner = await self.load(exc.active_run_id)
            except RuntimeNotFound:
                # A reservation is write-ahead of run_started. Missing run
                # facts therefore mean an interrupted request, not a stale
                # owner that another logical request may steal.
                raise RuntimeConflict(str(exc)) from exc
            if owner.session_id != session_id or owner.state is not RunState.TERMINAL:
                raise RuntimeConflict(str(exc)) from exc
            await self.store.release_active_run(session_id, exc.active_run_id)
            try:
                return await self.store.reserve_request(
                    session_id,
                    idempotency_key,
                    request_hash,
                    proposed_run_id,
                )
            except SessionBusyError as raced:
                raise RuntimeConflict(str(raced)) from raced

    async def _claim_session_run(self, session_id: str, run_id: str) -> None:
        """Verify-or-claim the Session owner before recovery side effects."""

        try:
            await self.store.claim_active_run(session_id, run_id)
        except SessionBusyError as exc:
            raise RuntimeConflict(str(exc)) from exc

    async def _submit_start(self, command: StartRun) -> RunHandle:
        if not command.prompt.strip():
            raise ValueError("prompt must not be blank")
        session_id = command.session_id or uuid.uuid4().hex
        proposed_run_id = uuid.uuid4().hex
        idempotency_key = command.idempotency_key or f"generated:{proposed_run_id}"
        request_hash = hashlib.sha256(
            canonical_json(
                {
                    "prompt": command.prompt,
                    "agent_revision": self.agent_revision,
                    "tool_manifest_hash": self.agent.tool_manifest_hash,
                }
            ).encode()
        ).hexdigest()
        try:
            raw_reservation = await self._reserve_session_request(
                session_id=session_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                proposed_run_id=proposed_run_id,
            )
        except RequestPayloadConflict:
            raise
        except Exception as exc:
            if "different" in str(exc).casefold() or "conflict" in str(exc).casefold():
                raise RequestPayloadConflict(str(exc)) from exc
            raise
        reservation_value = cast(Any, raw_reservation)
        reservation = RequestReservation(
            str(reservation_value.run_id),
            bool(reservation_value.created),
        )
        repair_snapshot: RunSnapshot | None = None
        repair_start_event: StoredRunEvent | None = None
        if not reservation.created:
            try:
                snapshot = await self.load(reservation.run_id)
            except RuntimeNotFound:
                # A durable reservation intentionally precedes run creation.
                # If the process died in that window, the same request repairs
                # the missing run below using a deterministic first execution.
                pass
            else:
                active = self._tasks.get(reservation.run_id)
                events = await self.journal.read(
                    reservation.run_id, after_sequence=0
                )
                startup_only = all(
                    event.kind is RunEventKind.RUN_STARTED
                    or (
                        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
                        and event.data.get("phase") == "run_start"
                    )
                    for event in events
                )
                workspace_binding_matches = (
                    (snapshot.workspace_anchor is None and self.workspace is None)
                    or (snapshot.workspace_anchor is not None and self.workspace is not None)
                )
                if (
                    snapshot.state is RunState.TERMINAL
                    or (active is not None and not active.done())
                    or not startup_only
                    or not workspace_binding_matches
                ):
                    # Once a model/tool intent exists, retrying Start would hide
                    # provider/tool uncertainty. Recovery must create a new
                    # execution through the explicit Resume matrix instead.
                    return RunHandle(
                        reservation.run_id,
                        session_id,
                        snapshot.execution_id,
                        False,
                    )
                repair_snapshot = snapshot
                repair_start_event = events[0]

        raw_session = await self.store.load_session(session_id)
        session = self._session_record(raw_session)
        execution_id = _initial_execution_id(reservation.run_id)
        history = transcript_from_json(session.transcript)

        workspace_anchor: WorkspaceCheckpoint | None = None
        if self.workspace is not None:
            if repair_snapshot is not None:
                assert repair_snapshot.workspace_anchor is not None
                workspace_anchor = self._workspace_checkpoint_from_data(
                    repair_snapshot.workspace_anchor,
                    session_id,
                    occurred_at=float(repair_snapshot.workspace_anchor["created_at"]),
                )
            else:
                await self._ensure_session_workspace(
                    session_id,
                    current_run_id=reservation.run_id,
                    allow_current_orphan=not reservation.created,
                )
                workspace_anchor = await asyncio.to_thread(
                    self.workspace.checkpoint,
                    session_id,
                )

        writer = _JournalWriter(
            self,
            run_id=reservation.run_id,
            session_id=session_id,
            session_version=session.version,
            execution_id=execution_id,
            snapshot=repair_snapshot,
        )
        start_public_data: dict[str, Any] = {
            "history_items": len(history),
            "agent_revision": self.agent_revision,
            "tool_manifest_hash": self.agent.tool_manifest_hash,
        }
        if workspace_anchor is not None:
            start_public_data["workspace_anchor"] = _workspace_checkpoint_payload(
                workspace_anchor,
                phase="run_start",
                call_key=None,
            )
        synthetic = AgentJournalEvent(
            kind=AgentJournalEventKind.RUN_STARTED,
            run_id=reservation.run_id,
            execution_id=execution_id,
            operation_id="run:started",
            timestamp=time.time(),
            public_data=start_public_data,
            private_data={
                "prompt": command.prompt,
                "history": transcript_to_json(history),
                "transcript": [
                    *transcript_to_json(history),
                    {"role": "user", "content": command.prompt},
                ],
            },
        )
        await writer.record_agent(synthetic)
        if repair_start_event is not None:
            writer._pending_lifecycle_telemetry = repair_start_event
        created_snapshot = await self.load(reservation.run_id)
        if created_snapshot.state is RunState.TERMINAL:
            return RunHandle(
                reservation.run_id,
                session_id,
                created_snapshot.execution_id,
                False,
            )
        handoff = _WriterLeaseHandoff(writer)
        try:
            async with handoff:
                if self.workspace is not None and repair_snapshot is not None:
                    assert workspace_anchor is not None
                    await self._ensure_session_workspace(
                        session_id,
                        current_run_id=reservation.run_id,
                        allow_current_orphan=True,
                    )
                    verification = await asyncio.to_thread(
                        self.workspace.verify,
                        session_id,
                        checkpoint=workspace_anchor,
                    )
                    if (
                        not verification.valid
                        or verification.dirty
                        or verification.current_tree != workspace_anchor.tree_id
                    ):
                        raise RuntimeConflict(
                            "reserved Start workspace differs from its immutable anchor"
                        )
                if self.workspace is not None:
                    async with writer._lock:
                        await writer._workspace_checkpoint_unlocked(
                            phase="run_start",
                            call_key=None,
                            safe=True,
                            checkpoint=workspace_anchor,
                        )
                await writer.project_pending_lifecycle()
                self._schedule(
                    writer,
                    self._execute_new(
                        writer,
                        prompt=command.prompt,
                        history=history,
                        expected_session_version=session.version,
                    ),
                )
                handoff.transfer()
        except RuntimeConflict:
            if handoff.acquired:
                raise
            # Another repairer may have committed the same deterministic first
            # event and fenced this process out. The idempotent Start still
            # succeeds for its caller; only the lease holder may execute it.
            snapshot = await self.load(reservation.run_id)
            return RunHandle(
                reservation.run_id,
                session_id,
                snapshot.execution_id,
                False,
            )
        return RunHandle(reservation.run_id, session_id, execution_id, True)

    async def _submit_resume(self, run_id: str) -> RunHandle:
        async with self._coordination_lock:
            snapshot = await self.load(run_id)
            if snapshot.state is RunState.NEEDS_RECONCILIATION:
                raise ReconciliationRequired("operator reconciliation is required")
            active = self._tasks.get(run_id)
            if active is not None and not active.done():
                assert snapshot.session_id is not None
                return RunHandle(run_id, snapshot.session_id, snapshot.execution_id, False)
            if snapshot.session_id is None:
                raise ResumeRejected("run has no Session identity")
            if snapshot.state is RunState.TERMINAL:
                return RunHandle(run_id, snapshot.session_id, snapshot.execution_id, False)
            if snapshot.agent_revision != self.agent_revision:
                raise ResumeRejected("Agent revision changed; create an explicit Fork")
            if snapshot.tool_manifest_hash != self.agent.tool_manifest_hash:
                raise ResumeRejected("tool manifest changed; create an explicit Fork")
            if snapshot.workspace is not None and self.workspace is None:
                raise ResumeRejected(
                    "run owns a durable workspace; configure its workspace adapter to Resume"
                )
            await self._claim_session_run(snapshot.session_id, run_id)
            durable_history = await self.journal.read(run_id, after_sequence=0)
            first = durable_history[0]
            raw_version = first.data.get("session_version", 0)
            session_version = int(raw_version) if isinstance(raw_version, int) else 0
            execution_id = uuid.uuid4().hex
            writer = _JournalWriter(
                self,
                run_id=run_id,
                session_id=snapshot.session_id,
                session_version=session_version,
                execution_id=execution_id,
                snapshot=snapshot,
            )
            async with _WriterLeaseHandoff(writer) as handoff:
                resume_data: dict[str, Any] = {
                    "resume_reason": self._resume_reason(snapshot, durable_history),
                }
                if snapshot.execution_id is not None:
                    resume_data["previous_execution_id"] = snapshot.execution_id
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.RUN_RESUMED,
                        execution_id=execution_id,
                        data=resume_data,
                    ),
                    operation_id=f"run:resumed:{execution_id}",
                )
                self._schedule(writer, self._execute_resume(writer))
                handoff.transfer()
            return RunHandle(run_id, snapshot.session_id, execution_id, True)

    @classmethod
    def _resume_reason(
        cls,
        snapshot: RunSnapshot,
        durable_history: Sequence[StoredRunEvent],
    ) -> str:
        if any(action.kind.value == "model" for action in snapshot.pending.values()):
            return "model_abandoned"
        if cls._transient_model_failure_step(durable_history) is not None:
            return "model_retry"
        started_tools = tuple(
            tool for tool in snapshot.tools.values() if tool.phase == "started"
        )
        if any(
            tool.resume_policy == ToolResumePolicy.IDEMPOTENT_RETRY.value
            for tool in started_tools
        ):
            return "tool_retry"
        if started_tools or snapshot.state is RunState.NEEDS_RECONCILIATION:
            return "operator_reconciliation"
        return "process_restart"

    async def _submit_fork(self, command: ForkRun) -> RunHandle:
        events = await self.journal.read(command.run_id, after_sequence=0)
        if not events:
            raise RuntimeNotFound(f"run not found: {command.run_id}")
        parent = fold_events(events)
        sequence = command.from_sequence
        if sequence is None:
            if not parent.safe_checkpoint_sequences:
                raise RuntimeConflict("run has no safe checkpoint")
            sequence = parent.safe_checkpoint_sequences[-1]
        if sequence not in parent.safe_checkpoint_sequences:
            raise RuntimeConflict(f"sequence {sequence} is not a safe checkpoint")
        prefix = tuple(event for event in events if event.sequence <= sequence)
        checkpoint = fold_events(prefix)
        session_id = command.session_id or uuid.uuid4().hex
        inherited_prompt = f"Fork from {command.run_id} at durable sequence {sequence}"
        idempotency_key = command.idempotency_key or (
            f"fork:{command.run_id}:{sequence}:{uuid.uuid4().hex}"
        )
        proposed_run_id = uuid.uuid4().hex
        request_hash = hashlib.sha256(
            canonical_json(
                {
                    "parent_run_id": command.run_id,
                    "fork_sequence": sequence,
                    "agent_revision": self.agent_revision,
                    "tool_manifest_hash": self.agent.tool_manifest_hash,
                }
            ).encode()
        ).hexdigest()
        raw_reservation = await self._reserve_session_request(
            session_id=session_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            proposed_run_id=proposed_run_id,
        )
        reservation_value = cast(Any, raw_reservation)
        reservation = RequestReservation(
            str(reservation_value.run_id),
            bool(reservation_value.created),
        )
        repair_snapshot: RunSnapshot | None = None
        repair_start_event: StoredRunEvent | None = None
        if not reservation.created:
            try:
                snapshot = await self.load(reservation.run_id)
            except RuntimeNotFound:
                pass
            else:
                active = self._tasks.get(reservation.run_id)
                child_events = await self.journal.read(
                    reservation.run_id,
                    after_sequence=0,
                )
                startup_only = all(
                    event.kind is RunEventKind.RUN_STARTED
                    or (
                        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
                        and event.data.get("phase") == "fork_start"
                    )
                    for event in child_events
                )
                workspace_binding_matches = (
                    (snapshot.workspace_anchor is None and self.workspace is None)
                    or (snapshot.workspace_anchor is not None and self.workspace is not None)
                )
                first = child_events[0]
                fork_identity_matches = (
                    snapshot.session_id == session_id
                    and first.execution_id == _initial_execution_id(reservation.run_id)
                    and snapshot.parent_run_id == command.run_id
                    and snapshot.fork_sequence == sequence
                    and snapshot.agent_revision == self.agent_revision
                    and snapshot.tool_manifest_hash == self.agent.tool_manifest_hash
                    and first.kind is RunEventKind.RUN_STARTED
                )
                if not fork_identity_matches:
                    raise RuntimeConflict(
                        "reserved Fork child differs from the requested immutable lineage"
                    )
                if (
                    snapshot.state is RunState.TERMINAL
                    or (active is not None and not active.done())
                    or not startup_only
                    or not workspace_binding_matches
                ):
                    # A committed model/tool intent belongs to an execution that
                    # may already have crossed a side-effect boundary. Only the
                    # explicit Resume matrix may decide how to continue it.
                    return RunHandle(
                        reservation.run_id,
                        session_id,
                        snapshot.execution_id,
                        False,
                    )
                repair_snapshot = snapshot
                repair_start_event = first
        raw_session = await self.store.load_session(session_id)
        session = self._session_record(raw_session)
        execution_id = _initial_execution_id(reservation.run_id)
        session_version = session.version
        if repair_start_event is not None:
            stored_session_version = repair_start_event.data.get("session_version")
            if (
                not isinstance(stored_session_version, int)
                or isinstance(stored_session_version, bool)
                or stored_session_version != session.version
            ):
                raise RuntimeConflict(
                    "reserved Fork Session version changed before startup repair"
                )
            session_version = stored_session_version

        workspace_tree: str | None = None
        workspace_anchor: WorkspaceCheckpoint | None = None
        if self.workspace is not None:
            workspace_event = next(
                (
                    event
                    for event in reversed(prefix)
                    if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
                ),
                None,
            )
            if workspace_event is not None:
                workspace_tree = str(workspace_event.data.get("tree_id") or "") or None
                parent_checkpoint = self._workspace_checkpoint_from_event(
                    workspace_event, parent.session_id or ""
                )
                await self._ensure_fork_workspace(
                    parent_checkpoint,
                    session_id,
                    allow_orphan=not reservation.created or repair_snapshot is not None,
                )
                if repair_snapshot is not None:
                    assert repair_snapshot.workspace_anchor is not None
                    workspace_anchor = self._workspace_checkpoint_from_data(
                        repair_snapshot.workspace_anchor,
                        session_id,
                        occurred_at=float(repair_snapshot.workspace_anchor["created_at"]),
                    )
                    verification = await asyncio.to_thread(
                        self.workspace.verify,
                        session_id,
                        checkpoint=workspace_anchor,
                    )
                    if (
                        not verification.valid
                        or verification.dirty
                        or verification.current_tree != workspace_anchor.tree_id
                    ):
                        raise RuntimeConflict(
                            "reserved Fork workspace differs from its immutable anchor"
                        )
                else:
                    workspace_anchor = await asyncio.to_thread(
                        self.workspace.checkpoint,
                        session_id,
                    )
            else:
                raise RuntimeConflict(
                    "selected Fork checkpoint has no durable workspace tree"
                )

        writer = _JournalWriter(
            self,
            run_id=reservation.run_id,
            session_id=session_id,
            session_version=session_version,
            execution_id=execution_id,
            snapshot=repair_snapshot,
        )
        start_data: dict[str, Any] = {
            "status": "running",
            "session_version": session_version,
            "parent_run_id": command.run_id,
            "fork_sequence": sequence,
            "inherited_usage": {
                "input_tokens": checkpoint.usage.input_tokens,
                "output_tokens": checkpoint.usage.output_tokens,
                "total_tokens": checkpoint.usage.total_tokens,
            },
            "inherited_counts": {
                "model_calls": checkpoint.counts.model_calls,
                "tool_calls": checkpoint.counts.tool_calls,
                "tool_executions": checkpoint.counts.tool_executions,
            },
        }
        if workspace_anchor is not None:
            start_data["workspace_anchor"] = _workspace_checkpoint_payload(
                workspace_anchor,
                phase="fork_start",
                call_key=None,
            )
        start_draft = RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            privacy=PrivacyClass.PRIVATE,
            session_id=session_id,
            execution_id=execution_id,
            agent_revision=self.agent_revision,
            tool_manifest_hash=self.agent.tool_manifest_hash,
            data=start_data,
            checkpoint={
                "prompt": inherited_prompt,
                "transcript": _thaw(checkpoint.transcript),
            },
            safe_checkpoint=True,
            usage_delta=checkpoint.usage,
            model_calls_delta=checkpoint.counts.model_calls,
            tool_calls_delta=checkpoint.counts.tool_calls,
            tool_executions_delta=checkpoint.counts.tool_executions,
        )
        await writer.append(start_draft, operation_id="run:started")
        if repair_start_event is not None:
            writer._pending_lifecycle_telemetry = repair_start_event
        await self.store.set_lineage(
            reservation.run_id,
            parent_run_id=command.run_id,
            fork_sequence=sequence,
            workspace_tree=workspace_tree,
        )
        created_snapshot = await self.load(reservation.run_id)
        if created_snapshot.state is RunState.TERMINAL:
            return RunHandle(
                reservation.run_id,
                session_id,
                created_snapshot.execution_id,
                False,
            )
        handoff = _WriterLeaseHandoff(writer)
        try:
            async with handoff:
                if self.workspace is not None:
                    async with writer._lock:
                        await writer._workspace_checkpoint_unlocked(
                            phase="fork_start",
                            call_key=None,
                            safe=True,
                            checkpoint=workspace_anchor,
                        )
                await writer.project_pending_lifecycle()
                self._schedule(
                    writer,
                    self._execute_resume(writer),
                )
                handoff.transfer()
        except RuntimeConflict:
            if handoff.acquired:
                raise
            snapshot = await self.load(reservation.run_id)
            return RunHandle(
                reservation.run_id,
                session_id,
                snapshot.execution_id,
                False,
            )
        return RunHandle(reservation.run_id, session_id, execution_id, True)

    async def _submit_cancel(self, command: CancelRun) -> RunHandle:
        snapshot = await self.load(command.run_id)
        if snapshot.session_id is None:
            raise RuntimeConflict("run has no Session identity")
        if snapshot.state is RunState.TERMINAL:
            return RunHandle(command.run_id, snapshot.session_id, snapshot.execution_id, False)
        writer = self._writers.get(command.run_id)
        task = self._tasks.get(command.run_id)
        owns_writer = (
            writer is None
            or task is None
            or task.done()
            or writer.lease is None
        )
        if owns_writer:
            stale_writer = writer
            if stale_writer is not None:
                # A completed/failed execution may still be visible between
                # its task finishing and the done callback pruning process-
                # local coordination state.  Never append through that old
                # execution after its lease has been released.
                await stale_writer.release()
                if self._writers.get(command.run_id) is stale_writer:
                    self._writers.pop(command.run_id, None)
            if task is not None and task.done() and self._tasks.get(command.run_id) is task:
                self._tasks.pop(command.run_id, None)
            execution_id = uuid.uuid4().hex
            writer = _JournalWriter(
                self,
                run_id=command.run_id,
                session_id=snapshot.session_id,
                session_version=0,
                execution_id=execution_id,
                snapshot=snapshot,
            )
        assert writer is not None

        async def commit_cancellation() -> None:
            async with writer._lock:
                await writer._append_unlocked(
                    RunEventDraft(
                        kind=RunEventKind.RUN_CANCEL_REQUESTED,
                        execution_id=writer.execution_id,
                        data={"reason": command.reason},
                    ),
                    operation_id=f"cancel:{uuid.uuid4().hex}",
                )
                # Keep the writer lock until the terminal fact commits.  The
                # cancelled execution's finally block then waits here instead
                # of releasing the lease between cancel_requested and aborted.
                if task is not None and not task.done():
                    task.cancel()
                await writer._append_unlocked(
                    RunEventDraft(
                        kind=RunEventKind.RUN_ABORTED,
                        privacy=PrivacyClass.PRIVATE,
                        execution_id=writer.execution_id,
                        data={"status": "aborted", "stop_reason": command.reason},
                        checkpoint={"reason": command.reason},
                        safe_checkpoint=True,
                    ),
                    operation_id="run:aborted",
                )
            if task is not None and not task.done():
                await asyncio.gather(task, return_exceptions=True)

        if owns_writer:
            async with _WriterLeaseHandoff(writer):
                await commit_cancellation()
        else:
            await commit_cancellation()
            await writer.release()
        return RunHandle(command.run_id, snapshot.session_id, writer.execution_id, False)

    async def _submit_resolve(self, command: ResolveRun) -> RunHandle:
        snapshot = await self.load(command.run_id)
        if snapshot.session_id is None:
            raise RuntimeConflict("run has no Session identity")
        pending = snapshot.pending.get(command.call_key)
        if pending is None or pending.phase != "reconciliation":
            raise RuntimeConflict("call is not awaiting reconciliation")
        try:
            action = ResolutionAction(command.action)
        except ValueError as exc:
            raise ValueError("action must be retry, use_result, or abort") from exc
        if (
            command.call_key == _WORKSPACE_RECONCILIATION_KEY
            and action is ResolutionAction.USE_RESULT
        ):
            raise RuntimeConflict("workspace reconciliation does not accept a tool result")
        recovery_tool = snapshot.tools.get(command.call_key)
        if (
            action is ResolutionAction.RETRY
            and command.call_key != _WORKSPACE_RECONCILIATION_KEY
            and recovery_tool is not None
            and recovery_tool.resume_policy == ToolResumePolicy.NEVER_RETRY.value
        ):
            raise RuntimeConflict("tool resume policy permanently forbids retry")
        await self._claim_session_run(snapshot.session_id, command.run_id)
        durable_events = await self.journal.read(command.run_id, after_sequence=0)
        if (
            action is ResolutionAction.RETRY
            and self.workspace is not None
            and command.call_key != _WORKSPACE_RECONCILIATION_KEY
            and self._has_completed_parallel_sibling(
                durable_events,
                call_key=command.call_key,
            )
        ):
            raise RuntimeConflict(
                "operator retry cannot restore across a completed parallel sibling"
            )
        raw_session_version = durable_events[0].data.get("session_version", 0)
        session_version = (
            int(raw_session_version)
            if isinstance(raw_session_version, int)
            and not isinstance(raw_session_version, bool)
            else 0
        )
        execution_id = uuid.uuid4().hex
        writer = _JournalWriter(
            self,
            run_id=command.run_id,
            session_id=snapshot.session_id,
            session_version=session_version,
            execution_id=execution_id,
            snapshot=snapshot,
        )
        async with _WriterLeaseHandoff(writer) as handoff:
            resume_data: dict[str, Any] = {
                "resume_reason": "operator_reconciliation",
            }
            if snapshot.execution_id is not None:
                resume_data["previous_execution_id"] = snapshot.execution_id
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.RUN_RESUMED,
                    execution_id=execution_id,
                    data=resume_data,
                ),
                operation_id=f"run:resumed:{execution_id}",
            )
            if action is ResolutionAction.RETRY and self.workspace is not None:
                await self._restore_workspace_for_operator_retry(
                    writer,
                    command.call_key,
                    events=durable_events,
                )
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.RECONCILIATION_RESOLVED,
                    call_key=command.call_key,
                    execution_id=execution_id,
                    data={"action": action.value},
                ),
                operation_id=f"reconcile:{command.call_key}:{uuid.uuid4().hex}",
            )
            if action is ResolutionAction.ABORT:
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.RUN_ABORTED,
                        data={
                            "status": "aborted",
                            "stop_reason": "operator_aborted",
                        },
                        safe_checkpoint=True,
                    ),
                    operation_id="run:aborted",
                )
                return RunHandle(
                    command.run_id, snapshot.session_id, execution_id, False
                )

            if command.call_key == _WORKSPACE_RECONCILIATION_KEY:
                self._schedule(writer, self._execute_resume(writer))
                handoff.transfer()
                return RunHandle(
                    command.run_id, snapshot.session_id, execution_id, True
                )

            tool = snapshot.tools.get(command.call_key)
            if tool is None or tool.call is None:
                raise RuntimeConflict("recovery call payload is unavailable")
            if action is ResolutionAction.USE_RESULT:
                call_id = str(tool.call.get("id", ""))
                tool_name = str(tool.call.get("name", ""))
                content = (
                    command.result
                    if isinstance(command.result, str)
                    else json.dumps(
                        {
                            "ok": True,
                            "data": command.result,
                            "meta": {"operator_supplied": True},
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                message = ToolMessage(
                    call_id=call_id,
                    name=tool_name,
                    content=content,
                    executed=False,
                )
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.TOOL_COMPLETED,
                        privacy=PrivacyClass.PRIVATE,
                        step=tool.step,
                        call_key=command.call_key,
                        execution_id=execution_id,
                        data={
                            "operator_supplied": True,
                            "executed": False,
                            "tool_call_id": call_id,
                            "tool_name": tool_name,
                        },
                        checkpoint={"message": transcript_item_to_json(message)},
                        safe_checkpoint=True,
                    ),
                    operation_id=f"tool:{command.call_key}:operator-result",
                )
            else:
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.TOOL_PLANNED,
                        privacy=PrivacyClass.PRIVATE,
                        step=tool.step,
                        call_key=command.call_key,
                        execution_id=execution_id,
                        data={
                            "resume_policy": tool.resume_policy,
                            "tool_call_id": tool.tool_call_id,
                            "tool_name": tool.tool_name,
                            "operator_retry": True,
                        },
                        checkpoint={"call": dict(tool.call)},
                        safe_checkpoint=True,
                    ),
                    operation_id=(
                        f"tool:{command.call_key}:operator-retry:{uuid.uuid4().hex}"
                    ),
                )
            reconciled = await self.load(command.run_id)
            if reconciled.state is RunState.NEEDS_RECONCILIATION:
                return RunHandle(
                    command.run_id, snapshot.session_id, execution_id, False
                )
            self._schedule(writer, self._execute_resume(writer))
            handoff.transfer()
            return RunHandle(command.run_id, snapshot.session_id, execution_id, True)

    async def _execute_new(
        self,
        writer: _JournalWriter,
        *,
        prompt: str,
        history: Sequence[object],
        expected_session_version: int,
    ) -> None:
        del expected_session_version
        try:
            result = await self.agent.run(
                prompt,
                history=cast(Sequence[Any], history),
                run_id=writer.run_id,
                execution_id=writer.execution_id,
                stream_sink=self._buses.setdefault(writer.run_id, _LiveBus()).publish,
                journal_sink=writer.record_agent,
                side_effect_guard=writer.guard,
                workspace_path=self._workspace_paths.get(writer.session_id),
            )
        finally:
            await self.agent.registry.finalize_execution(
                writer.run_id,
                writer.execution_id,
            )
        await self._finalize(writer, result)

    @staticmethod
    def _workspace_event_call_key(event: StoredRunEvent) -> str | None:
        value = event.data.get("call_key")
        return str(value) if isinstance(value, str) and value else None

    @staticmethod
    def _workspace_checkpoint_event(
        events: Sequence[StoredRunEvent],
        *,
        call_key: str | None = None,
        phase: str | None = None,
    ) -> StoredRunEvent | None:
        for event in reversed(events):
            if event.kind is not RunEventKind.WORKSPACE_CHECKPOINTED:
                continue
            if call_key is not None and AgentRuntime._workspace_event_call_key(event) != call_key:
                continue
            if phase is not None and event.data.get("phase") != phase:
                continue
            return event
        return None

    @staticmethod
    def _has_completed_parallel_sibling(
        events: Sequence[StoredRunEvent],
        *,
        call_key: str,
    ) -> bool:
        """Return whether durable facts prove an overlapping sibling write.

        A completed sibling from the same execution and model step may have
        changed the shared worktree after this call's write-ahead checkpoint.
        Restoring that checkpoint would erase an already committed effect.
        """

        started = next(
            (
                event
                for event in reversed(events)
                if event.call_key == call_key
                and event.kind in {RunEventKind.TOOL_STARTED, RunEventKind.TOOL_CLAIMED}
            ),
            None,
        )
        if started is None:
            return False
        return any(
            event.kind is RunEventKind.TOOL_COMPLETED
            and event.call_key != call_key
            and event.step == started.step
            and event.execution_id == started.execution_id
            and event.sequence > started.sequence
            and event.data.get("executed") is True
            for event in events
        )

    async def _ensure_fork_workspace(
        self,
        parent_checkpoint: WorkspaceCheckpoint,
        session_id: str,
        *,
        allow_orphan: bool,
    ) -> WorkspaceHandle:
        """Create or safely reclaim the child worktree for a reserved Fork."""

        assert self.workspace is not None
        try:
            handle = await asyncio.to_thread(
                self.workspace.fork,
                parent_checkpoint,
                session_id,
            )
            self._workspace_paths[session_id] = handle.path
            return handle
        except WorkspaceConflictError as exc:
            # A process can die after fork() created the isolated worktree but
            # before run_started. Reattach only to the exact immutable parent
            # commit and prove that no content changed in the gap.
            if not allow_orphan:
                raise RuntimeConflict(
                    "Fork target workspace already exists for a new request"
                ) from exc
        try:
            handle = await asyncio.to_thread(
                self.workspace.attach,
                session_id,
                baseline_revision=parent_checkpoint.commit_id,
            )
            verification = await asyncio.to_thread(
                self.workspace.verify,
                session_id,
                checkpoint=parent_checkpoint,
            )
        except WorkspaceError as exc:
            raise RuntimeConflict(
                "reserved Fork workspace could not be reattached safely"
            ) from exc
        if (
            not verification.valid
            or verification.dirty
            or verification.current_tree != parent_checkpoint.tree_id
        ):
            raise RuntimeConflict(
                "reserved Fork workspace differs from its parent checkpoint"
            )
        self._workspace_paths[session_id] = handle.path
        return handle

    async def _ensure_session_workspace(
        self,
        session_id: str,
        *,
        current_run_id: str,
        allow_current_orphan: bool,
    ) -> WorkspaceHandle:
        assert self.workspace is not None
        try:
            handle = await asyncio.to_thread(self.workspace.create, session_id)
            self._workspace_paths[session_id] = handle.path
            return handle
        except WorkspaceConflictError:
            # A Session owns one stable worktree. A fresh Runtime process must
            # reattach it using durable baseline metadata instead of silently
            # treating an arbitrary directory as managed state.
            pass

        run_ids = await self.store.list_runs(session_id, limit=100)
        for previous_run_id in run_ids:
            if previous_run_id == current_run_id:
                continue
            try:
                events = await self.journal.read(previous_run_id, after_sequence=0)
            except RunNotFoundError:
                continue
            checkpoint_event = self._workspace_checkpoint_event(events)
            if checkpoint_event is None:
                continue
            checkpoint = self._workspace_checkpoint_from_event(
                checkpoint_event,
                session_id,
            )
            try:
                handle = await asyncio.to_thread(
                    self.workspace.attach,
                    session_id,
                    baseline_revision=checkpoint.baseline_revision,
                )
            except WorkspaceError as exc:
                raise RuntimeConflict(
                    "durable Session workspace could not be reattached safely"
                ) from exc
            self._workspace_paths[session_id] = handle.path
            return handle

        if not allow_current_orphan:
            raise RuntimeConflict(
                "managed Session workspace exists without a durable checkpoint"
            )

        # The current request may have durably reserved its run id, created a
        # worktree, and then died before run_started. Runtime always creates a
        # new Session workspace at HEAD, so attach resolves that exact baseline
        # and rejects a moved HEAD. Only a clean, otherwise valid worktree can
        # be reclaimed; any edit or ownership ambiguity remains fail-closed.
        try:
            try:
                verification = await asyncio.to_thread(
                    self.workspace.verify,
                    session_id,
                )
                orphan_baseline = verification.baseline_revision
            except WorkspaceError:
                orphan_baseline = "HEAD"
            handle = await asyncio.to_thread(
                self.workspace.attach,
                session_id,
                baseline_revision=orphan_baseline,
            )
            verification = await asyncio.to_thread(self.workspace.verify, session_id)
        except WorkspaceError as exc:
            raise RuntimeConflict(
                "managed Session workspace exists without a recoverable checkpoint"
            ) from exc
        if not verification.valid or verification.dirty:
            raise RuntimeConflict(
                "managed Session workspace orphan is not clean enough to reclaim"
            )
        self._workspace_paths[session_id] = handle.path
        return handle

    async def _restore_workspace_for_operator_retry(
        self,
        writer: _JournalWriter,
        call_key: str,
        *,
        events: Sequence[StoredRunEvent],
    ) -> None:
        """Apply an operator-authorized rollback only inside the managed worktree."""

        assert self.workspace is not None
        checkpoint_event = self._workspace_checkpoint_event(
            events,
            call_key=(None if call_key == _WORKSPACE_RECONCILIATION_KEY else call_key),
            phase=(None if call_key == _WORKSPACE_RECONCILIATION_KEY else "before_tool"),
        )
        if checkpoint_event is None:
            raise RuntimeConflict(
                "operator retry requires a durable workspace checkpoint"
            )
        checkpoint = self._workspace_checkpoint_from_event(
            checkpoint_event,
            writer.session_id,
        )
        try:
            await asyncio.to_thread(
                self.workspace.attach,
                writer.session_id,
                baseline_revision=checkpoint.baseline_revision,
            )
            verification = await asyncio.to_thread(
                self.workspace.verify,
                writer.session_id,
                checkpoint=checkpoint,
            )
            if verification.diverged:
                await self._record_workspace_divergence(
                    writer,
                    checkpoint_event=checkpoint_event,
                    call_key=(
                        None
                        if call_key == _WORKSPACE_RECONCILIATION_KEY
                        else call_key
                    ),
                    reason="operator_retry_restore",
                    verification=verification,
                )
            await asyncio.to_thread(
                self.workspace.restore,
                writer.session_id,
                checkpoint=checkpoint,
            )
            async with writer._lock:
                await writer._workspace_checkpoint_unlocked(
                    phase="operator_retry_restored",
                    call_key=(
                        None
                        if call_key == _WORKSPACE_RECONCILIATION_KEY
                        else call_key
                    ),
                    safe=True,
                )
        except WorkspaceError as exc:
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=checkpoint_event,
                call_key=(
                    None if call_key == _WORKSPACE_RECONCILIATION_KEY else call_key
                ),
                reason="operator_retry_restore_failed",
                error=exc,
            )
            raise RuntimeConflict(
                "operator retry could not restore the managed workspace safely"
            ) from exc

    async def _record_workspace_divergence(
        self,
        writer: _JournalWriter,
        *,
        checkpoint_event: StoredRunEvent | None,
        call_key: str | None,
        reason: str,
        verification: object | None = None,
        error: BaseException | None = None,
    ) -> None:
        data: dict[str, Any] = {
            "phase": "resume_verify",
            "reason": reason,
            "call_key": call_key,
        }
        if checkpoint_event is not None:
            data.update(
                {
                    "checkpoint_id": checkpoint_event.data.get("checkpoint_id"),
                    "baseline_revision": checkpoint_event.data.get("baseline_revision"),
                    "expected_tree": checkpoint_event.data.get("tree_id"),
                }
            )
        if verification is not None:
            value = cast(Any, verification)
            data.update(
                {
                    "dirty": bool(value.dirty),
                    "diverged": bool(value.diverged),
                    "current_tree": str(value.current_tree),
                    "expected_tree": value.expected_tree,
                    "reasons": list(value.reasons),
                    "diff": {
                        "files_changed": int(value.diff.files_changed),
                        "additions": int(value.diff.additions),
                        "deletions": int(value.diff.deletions),
                        "binary_files": int(value.diff.binary_files),
                    },
                }
            )
        if error is not None:
            # Exception text can contain filesystem paths. Persist only its
            # bounded type; operators can correlate the in-process error log.
            data["error_type"] = type(error).__name__
        await writer.append(
            RunEventDraft(
                kind=RunEventKind.WORKSPACE_DIVERGED,
                execution_id=writer.execution_id,
                call_key=call_key,
                data=data,
            ),
            operation_id=(
                f"workspace:diverged:{writer.execution_id}:"
                f"{call_key or 'run'}:{reason}"
            ),
        )

    async def _require_workspace_reconciliation(
        self,
        writer: _JournalWriter,
        *,
        call_keys: Sequence[str],
        reason: str,
    ) -> None:
        keys = tuple(dict.fromkeys(call_keys)) or (_WORKSPACE_RECONCILIATION_KEY,)
        for call_key in keys:
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.RECONCILIATION_REQUIRED,
                    call_key=call_key,
                    execution_id=writer.execution_id,
                    data={
                        "reason": reason,
                        "scope": "workspace",
                    },
                ),
                operation_id=(
                    f"reconcile:{call_key}:{writer.execution_id}:workspace"
                ),
            )

    async def _prepare_workspace_resume(
        self,
        writer: _JournalWriter,
        snapshot: RunSnapshot,
    ) -> bool:
        """Attach and conservatively reconcile one Session worktree.

        Returning ``False`` means durable reconciliation facts were appended
        and this execution must stop before invoking a provider or tool.
        """

        if self.workspace is None:
            return True
        events = await self.journal.read(writer.run_id, after_sequence=0)
        latest_event = self._workspace_checkpoint_event(events)
        started_tools = tuple(
            tool for tool in snapshot.tools.values() if tool.phase == "started"
        )
        started_keys = tuple(tool.call_key for tool in started_tools)
        if latest_event is None:
            anchor_data = snapshot.workspace_anchor
            if anchor_data is not None:
                anchor = self._workspace_checkpoint_from_data(
                    anchor_data,
                    writer.session_id,
                    occurred_at=float(anchor_data["created_at"]),
                )
                try:
                    handle = await asyncio.to_thread(
                        self.workspace.attach,
                        writer.session_id,
                        baseline_revision=anchor.baseline_revision,
                    )
                    self._workspace_paths[writer.session_id] = handle.path
                    verification = await asyncio.to_thread(
                        self.workspace.verify,
                        writer.session_id,
                        checkpoint=anchor,
                    )
                except WorkspaceError as exc:
                    await self._record_workspace_divergence(
                        writer,
                        checkpoint_event=None,
                        call_key=None,
                        reason="workspace_anchor_verify_failed",
                        error=exc,
                    )
                    await self._require_workspace_reconciliation(
                        writer,
                        call_keys=started_keys,
                        reason="workspace_anchor_verify_failed",
                    )
                    await writer.release()
                    return False
                if (
                    not verification.valid
                    or verification.diverged
                    or verification.current_tree != anchor.tree_id
                ):
                    await self._record_workspace_divergence(
                        writer,
                        checkpoint_event=None,
                        call_key=None,
                        reason="workspace_changed_since_run_started",
                        verification=verification,
                    )
                    await self._require_workspace_reconciliation(
                        writer,
                        call_keys=started_keys,
                        reason="workspace_changed_since_run_started",
                    )
                    await writer.release()
                    return False
                initial_execution_id = events[0].execution_id or writer.execution_id
                async with writer._lock:
                    await writer._workspace_checkpoint_unlocked(
                        phase=str(anchor_data.get("phase") or "run_start"),
                        call_key=None,
                        safe=True,
                        checkpoint=anchor,
                        execution_id=initial_execution_id,
                    )
                return True
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=None,
                call_key=(started_keys[0] if len(started_keys) == 1 else None),
                reason="durable_checkpoint_missing",
            )
            await self._require_workspace_reconciliation(
                writer,
                call_keys=started_keys,
                reason="workspace_checkpoint_missing",
            )
            await writer.release()
            return False

        latest = self._workspace_checkpoint_from_event(latest_event, writer.session_id)
        try:
            handle = await asyncio.to_thread(
                self.workspace.attach,
                writer.session_id,
                baseline_revision=latest.baseline_revision,
            )
            self._workspace_paths[writer.session_id] = handle.path
        except WorkspaceError as exc:
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=latest_event,
                call_key=(started_keys[0] if len(started_keys) == 1 else None),
                reason="workspace_attach_failed",
                error=exc,
            )
            await self._require_workspace_reconciliation(
                writer,
                call_keys=started_keys,
                reason="workspace_attach_failed",
            )
            await writer.release()
            return False

        overlapping_completed_sibling = (
            len(started_tools) == 1
            and self._has_completed_parallel_sibling(
                events,
                call_key=started_tools[0].call_key,
            )
        )
        if len(started_tools) > 1 or overlapping_completed_sibling:
            try:
                verification = await asyncio.to_thread(
                    self.workspace.verify,
                    writer.session_id,
                    checkpoint=latest,
                )
            except WorkspaceError as exc:
                await self._record_workspace_divergence(
                    writer,
                    checkpoint_event=latest_event,
                    call_key=None,
                    reason="workspace_verify_failed",
                    error=exc,
                )
                await self._require_workspace_reconciliation(
                    writer,
                    call_keys=started_keys,
                    reason="workspace_verify_failed",
                )
                await writer.release()
                return False
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=latest_event,
                call_key=None,
                reason="ambiguous_parallel_tool_state",
                verification=verification,
            )
            await self._require_workspace_reconciliation(
                writer,
                call_keys=started_keys,
                reason="ambiguous_parallel_tool_state",
            )
            await writer.release()
            return False

        # Exactly one interrupted idempotent tool can be rolled back to its
        # write-ahead checkpoint. Parallel interrupted writers are ambiguous:
        # a later before-tool checkpoint may already contain another tool's
        # partial edits, so they require an operator even when the tree matches.
        if len(started_tools) == 1 and (
            started_tools[0].resume_policy == ToolResumePolicy.IDEMPOTENT_RETRY.value
        ):
            tool = started_tools[0]
            before_event = self._workspace_checkpoint_event(
                events,
                call_key=tool.call_key,
                phase="before_tool",
            )
            if before_event is None:
                await self._record_workspace_divergence(
                    writer,
                    checkpoint_event=latest_event,
                    call_key=tool.call_key,
                    reason="write_ahead_checkpoint_missing",
                )
                await self._require_workspace_reconciliation(
                    writer,
                    call_keys=(tool.call_key,),
                    reason="write_ahead_checkpoint_missing",
                )
                await writer.release()
                return False
            before = self._workspace_checkpoint_from_event(before_event, writer.session_id)
            try:
                verification = await asyncio.to_thread(
                    self.workspace.verify,
                    writer.session_id,
                    checkpoint=before,
                )
                if verification.diverged:
                    await self._record_workspace_divergence(
                        writer,
                        checkpoint_event=before_event,
                        call_key=tool.call_key,
                        reason="interrupted_idempotent_tool",
                        verification=verification,
                    )
                    await asyncio.to_thread(
                        self.workspace.restore,
                        writer.session_id,
                        checkpoint=before,
                    )
                    async with writer._lock:
                        await writer._workspace_checkpoint_unlocked(
                            phase="resume_restored",
                            call_key=tool.call_key,
                            safe=True,
                        )
            except WorkspaceError as exc:
                await self._record_workspace_divergence(
                    writer,
                    checkpoint_event=before_event,
                    call_key=tool.call_key,
                    reason="workspace_restore_failed",
                    error=exc,
                )
                await self._require_workspace_reconciliation(
                    writer,
                    call_keys=(tool.call_key,),
                    reason="workspace_restore_failed",
                )
                await writer.release()
                return False
            return True

        try:
            verification = await asyncio.to_thread(
                self.workspace.verify,
                writer.session_id,
                checkpoint=latest,
            )
        except WorkspaceError as exc:
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=latest_event,
                call_key=None,
                reason="workspace_verify_failed",
                error=exc,
            )
            await self._require_workspace_reconciliation(
                writer,
                call_keys=started_keys,
                reason="workspace_verify_failed",
            )
            await writer.release()
            return False

        if verification.diverged:
            reason = "content_changed_since_checkpoint"
            await self._record_workspace_divergence(
                writer,
                checkpoint_event=latest_event,
                call_key=None,
                reason=reason,
                verification=verification,
            )
            await self._require_workspace_reconciliation(
                writer,
                call_keys=started_keys,
                reason=reason,
            )
            await writer.release()
            return False
        return True

    async def _execute_resume(self, writer: _JournalWriter) -> None:
        snapshot = await self.load(writer.run_id)
        if snapshot.result is not None:
            if not await self._prepare_workspace_resume(writer, snapshot):
                return
            snapshot = await self.load(writer.run_id)
            await self._finalize(writer, self._result_from_snapshot(snapshot))
            return

        # An unfinished model attempt may have incurred unknown cost. Close it
        # explicitly before issuing a fresh attempt in the new execution.
        durable_history = await self.journal.read(writer.run_id, after_sequence=0)
        event_by_sequence = {event.sequence: event for event in durable_history}
        for action in tuple(snapshot.pending.values()):
            if action.kind.value != "model":
                continue
            attempt = int(action.key.rsplit("a", 1)[-1])
            attempt_event = event_by_sequence[action.sequence]
            # Record the unknown ledger entry first. If the process dies before
            # MODEL_ABANDONED, the pending attempt makes the next resume retry
            # this deterministic operation id and then finish abandonment.
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.COST_RECORDED,
                    step=action.step,
                    execution_id=attempt_event.execution_id,
                    data={
                        "record_id": hashlib.sha256(
                            f"{writer.run_id}:{action.key}:abandoned-cost".encode()
                        ).hexdigest()[:32],
                        "operation_id": action.key,
                        "kind": "estimate",
                        "amount_micros": None,
                        "currency": self.pricing.default_currency,
                        "provider": self.provider_name,
                        "model": self.model_name,
                        "source": "catalog_estimate",
                        "catalog_version": self.pricing.version,
                        "price_version": None,
                        "priced_at": datetime.fromtimestamp(
                            attempt_event.occurred_at, tz=UTC
                        ).isoformat(),
                        "estimated": True,
                        "unknown_reason": "provider_completion_not_committed",
                        "usage": {
                            "input_tokens": None,
                            "output_tokens": None,
                            "cached_input_tokens": None,
                            "reasoning_output_tokens": None,
                        },
                    },
                ),
                operation_id=f"cost:{action.operation_id}:abandoned",
            )
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.MODEL_ABANDONED,
                    step=action.step,
                    execution_id=writer.execution_id,
                    data={
                        "attempt": attempt,
                        "cost_unknown": True,
                        "unknown_reason": "provider_completion_not_committed",
                    },
                ),
                operation_id=f"{action.operation_id}:abandoned",
            )
        for draft, operation_id in _interrupted_compression_abandonments(
            durable_history,
            execution_id=writer.execution_id,
        ):
            lifecycle_event = AgentJournalEvent(
                kind=AgentJournalEventKind.CONTEXT_GOVERNED,
                run_id=writer.run_id,
                execution_id=writer.execution_id,
                operation_id=operation_id,
                timestamp=draft.occurred_at or 0.0,
                step=draft.step,
                public_data=draft.data,
            )
            priced_draft, cost_draft = self._unknown_compression_cost_event(
                draft,
                event=lifecycle_event,
            )
            await writer.append_many(
                (
                    (priced_draft, operation_id),
                    (cost_draft, f"cost:{operation_id}"),
                )
            )
        snapshot = await self.load(writer.run_id)

        if not await self._prepare_workspace_resume(writer, snapshot):
            return
        snapshot = await self.load(writer.run_id)

        uncertain = [
            tool
            for tool in snapshot.tools.values()
            if tool.phase == "started"
            and tool.resume_policy not in {ToolResumePolicy.IDEMPOTENT_RETRY.value}
        ]
        if uncertain:
            for tool in uncertain:
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.RECONCILIATION_REQUIRED,
                        step=tool.step,
                        call_key=tool.call_key,
                        execution_id=writer.execution_id,
                        data={
                            "reason": "uncertain_non_idempotent_tool",
                            "resume_policy": tool.resume_policy,
                            "tool_name": tool.tool_name,
                        },
                    ),
                    operation_id=f"reconcile:{tool.call_key}:{writer.execution_id}",
                )
            await writer.release()
            return

        recovered_terminal = self._recover_pre_result_terminal(
            snapshot,
            durable_history,
        )
        if recovered_terminal is not None:
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.RUN_RESULT_READY,
                    privacy=PrivacyClass.PRIVATE,
                    execution_id=writer.execution_id,
                    data={
                        "status": recovered_terminal.status.value,
                        "stop_reason": recovered_terminal.stop_reason.value,
                        "recovered_from_terminal_decision": True,
                    },
                    checkpoint={
                        "output": recovered_terminal.output,
                        "error": recovered_terminal.error,
                        "transcript": transcript_to_json(
                            recovered_terminal.transcript
                        ),
                    },
                    safe_checkpoint=True,
                    tool_calls_delta=max(
                        0,
                        recovered_terminal.tool_calls - snapshot.counts.tool_calls,
                    ),
                ),
                operation_id="run:result_ready",
            )
            await self._finalize(writer, recovered_terminal)
            return

        resume_state = await self._build_resume_state(writer, durable_history)
        if resume_state is None:
            raise ResumeRejected(
                "durable facts do not identify a terminal result or safe continuation"
            )

        try:
            result = await self.agent.run(
                "resume",
                run_id=writer.run_id,
                execution_id=writer.execution_id,
                stream_sink=self._buses.setdefault(writer.run_id, _LiveBus()).publish,
                journal_sink=writer.record_agent,
                resume_state=resume_state,
                side_effect_guard=writer.guard,
                workspace_path=self._workspace_paths.get(writer.session_id),
            )
        finally:
            await self.agent.registry.finalize_execution(
                writer.run_id,
                writer.execution_id,
            )
        await self._finalize(writer, result)

    @staticmethod
    def _transient_model_failure_step(events: Sequence[StoredRunEvent]) -> int | None:
        """Step to retry when the last progress fact is a non-terminal model failure."""

        cause = _last_progress_event(events)
        if (
            cause is None
            or cause.kind is not RunEventKind.MODEL_FAILED
            or cause.data.get("terminal_decision") is not False
        ):
            return None
        return cause.step

    @staticmethod
    def _recover_pre_result_terminal(
        snapshot: RunSnapshot,
        events: Sequence[StoredRunEvent],
    ) -> AgentResult | None:
        """Rebuild a decided result without invoking a provider or tool.

        Only the last orchestration-progress fact may prove a terminal result.
        Lifecycle, workspace, cost, and Resume bookkeeping facts are ignored.
        New journals carry an explicit ``terminal_decision`` marker and an
        exact private checkpoint; the kind-specific branch keeps older logs
        conservatively resumable without guessing every final answer as a
        success.
        """

        cause = _last_progress_event(events)
        if cause is None:
            # A Fork starts a new log at a reducer-approved safe checkpoint.
            # When that checkpoint already contains a non-empty final answer,
            # inherit it without pretending an ordinary start-only run ended.
            if snapshot.parent_run_id is None:
                return None
            inherited_transcript = transcript_from_json(snapshot.transcript)
            inherited_message = (
                inherited_transcript[-1] if inherited_transcript else None
            )
            if (
                not isinstance(inherited_message, AssistantMessage)
                or inherited_message.tool_calls
                or inherited_message.content is None
                or not inherited_message.content.strip()
            ):
                return None
            return AgentResult(
                output=inherited_message.content,
                status=RunStatus.COMPLETED,
                stop_reason=StopReason.COMPLETED,
                run_id=snapshot.run_id,
                model_calls=snapshot.counts.model_calls,
                tool_calls=snapshot.counts.tool_calls,
                tool_executions=snapshot.counts.tool_executions,
                usage=snapshot.usage,
                transcript=inherited_transcript,
                events=(),
            )
        if cause.kind is RunEventKind.RECONCILIATION_REQUIRED:
            return None
        if cause.data.get("terminal_decision") is False:
            # An explicit non-decision (a retried or retry-exhausted transient
            # model failure). The run continues from durable facts.
            return None

        checkpoint = cause.checkpoint or MappingProxyType({})
        transcript = transcript_from_json(snapshot.transcript)
        raw_transcript = checkpoint.get("transcript")
        if isinstance(raw_transcript, (list, tuple)) and all(
            isinstance(item, Mapping) for item in raw_transcript
        ):
            transcript = transcript_from_json(
                tuple(cast(Mapping[str, Any], item) for item in raw_transcript)
            )

        status: RunStatus | None = None
        reason: StopReason | None = None
        output = checkpoint.get("output")
        error = checkpoint.get("error")
        resolved_output = output if isinstance(output, str) else None
        resolved_error = error if isinstance(error, str) else None
        explicit = cause.data.get("terminal_decision") is True

        if explicit:
            raw_status = cause.data.get("status")
            raw_reason = cause.data.get("stop_reason")
            if not isinstance(raw_status, str) or not isinstance(raw_reason, str):
                raise ResumeRejected("terminal decision is missing status metadata")
            try:
                status = RunStatus(raw_status)
                reason = StopReason(raw_reason)
            except ValueError as exc:
                raise ResumeRejected("terminal decision contains an unknown outcome") from exc
        elif cause.kind is RunEventKind.BUDGET_EXHAUSTED:
            budget_outcomes = {
                "context_limit": (RunStatus.PARTIAL, StopReason.CONTEXT_LIMIT),
                "max_tool_calls": (RunStatus.PARTIAL, StopReason.MAX_TOOL_CALLS),
                "max_steps": (RunStatus.PARTIAL, StopReason.MAX_STEPS),
                "wall_time": (RunStatus.TIMED_OUT, StopReason.WALL_TIME),
            }
            raw_budget_reason = cause.data.get("reason")
            if isinstance(raw_budget_reason, str):
                outcome = budget_outcomes.get(raw_budget_reason)
                if outcome is not None:
                    status, reason = outcome
        elif cause.kind is RunEventKind.LOOP_DETECTED:
            status, reason = RunStatus.PARTIAL, StopReason.LOOP_DETECTED
        elif cause.kind is RunEventKind.MODEL_FAILED:
            if cause.data.get("error_type") == "timeout":
                status, reason = RunStatus.TIMED_OUT, StopReason.WALL_TIME
            else:
                status, reason = RunStatus.FAILED, StopReason.MODEL_ERROR
        elif cause.kind is RunEventKind.MODEL_COMPLETED:
            message = next(
                (
                    item
                    for item in reversed(transcript)
                    if isinstance(item, AssistantMessage)
                ),
                None,
            )
            if message is None:
                raise ResumeRejected("model completion has no durable assistant message")
            outcome = cause.data.get("outcome", "completed")
            if outcome == "refused":
                status, reason = RunStatus.FAILED, StopReason.MODEL_REFUSAL
                resolved_output = message.content
            elif outcome == "incomplete":
                status, reason = RunStatus.PARTIAL, StopReason.MODEL_INCOMPLETE
                resolved_output = message.content
            elif outcome == "completed" and not message.tool_calls:
                if message.content is not None and message.content.strip():
                    status, reason = RunStatus.COMPLETED, StopReason.COMPLETED
                    resolved_output = message.content
                else:
                    status, reason = RunStatus.FAILED, StopReason.PROTOCOL_ERROR
                    resolved_error = (
                        "Model returned neither tool calls nor a final answer."
                    )
            elif outcome == "completed":
                call_ids = [call.id for call in message.tool_calls]
                if (
                    any(not call.id or not call.name for call in message.tool_calls)
                    or len(call_ids) != len(set(call_ids))
                ):
                    status, reason = RunStatus.FAILED, StopReason.PROTOCOL_ERROR
                    resolved_error = (
                        "Model returned a malformed or duplicate tool call id/name."
                    )

        if status is None or reason is None:
            return None

        tool_calls = snapshot.counts.tool_calls
        if (
            not explicit
            and cause.kind is RunEventKind.MODEL_COMPLETED
            and cause.data.get("outcome") in {"refused", "incomplete"}
            and cause.tool_calls_delta == 0
        ):
            last_assistant = next(
                (
                    item
                    for item in reversed(transcript)
                    if isinstance(item, AssistantMessage)
                ),
                None,
            )
            if last_assistant is not None:
                tool_calls += len(last_assistant.tool_calls)

        return AgentResult(
            output=resolved_output,
            status=status,
            stop_reason=reason,
            run_id=snapshot.run_id,
            model_calls=snapshot.counts.model_calls,
            tool_calls=tool_calls,
            tool_executions=snapshot.counts.tool_executions,
            usage=snapshot.usage,
            transcript=transcript,
            events=(),
            error=resolved_error,
        )

    async def _build_resume_state(
        self,
        writer: _JournalWriter,
        durable_history: Sequence[StoredRunEvent],
    ) -> AgentResumeState | None:
        snapshot = await self.load(writer.run_id)
        transcript = transcript_from_json(snapshot.transcript)
        retry_step = self._transient_model_failure_step(durable_history)
        completed: list[RecoveredToolCall] = []
        for tool in snapshot.tools.values():
            if tool.call is None or tool.message is None:
                continue
            call = self._tool_call(tool.call)
            message = transcript_item_from_json(tool.message)
            if isinstance(message, ToolMessage):
                if tool.private_payload is not None:
                    message = replace(
                        message,
                        private_payload=cast(
                            Mapping[str, JsonValue],
                            tool.private_payload,
                        ),
                    )
                completed.append(
                    RecoveredToolCall(
                        call,
                        tool.call_key,
                        attempts=tool.attempts,
                        completed=message,
                    )
                )

        model_pending = next(
            (action for action in snapshot.pending.values() if action.kind.value == "model"),
            None,
        )
        if model_pending is not None:
            next_step = model_pending.step or max(1, snapshot.last_step)
            return AgentResumeState(
                transcript,
                next_step,
                snapshot.counts.model_calls,
                snapshot.counts.tool_calls,
                snapshot.counts.tool_executions,
                snapshot.usage,
                completed_tools=tuple(completed),
                action_counts=self._action_counts(snapshot),
            )

        last_assistant_index = next(
            (
                index
                for index in range(len(transcript) - 1, -1, -1)
                if isinstance(transcript[index], AssistantMessage)
            ),
            None,
        )
        if last_assistant_index is None:
            return AgentResumeState(
                transcript,
                max(1, snapshot.last_step or 1),
                snapshot.counts.model_calls,
                snapshot.counts.tool_calls,
                snapshot.counts.tool_executions,
                snapshot.usage,
                completed_tools=tuple(completed),
            )
        assistant = cast(AssistantMessage, transcript[last_assistant_index])
        if not assistant.tool_calls:
            return None
        has_tool_messages = any(
            isinstance(item, ToolMessage) for item in transcript[last_assistant_index + 1 :]
        )
        if has_tool_messages:
            # A transient model failure at step N is retried at step N; only a
            # completed model turn advances the step budget.
            return AgentResumeState(
                transcript,
                retry_step if retry_step is not None else max(1, snapshot.last_step + 1),
                snapshot.counts.model_calls,
                snapshot.counts.tool_calls,
                snapshot.counts.tool_executions,
                snapshot.usage,
                completed_tools=tuple(completed),
                action_counts=self._action_counts(snapshot),
            )

        step = max(
            (tool.step for tool in snapshot.tools.values()),
            default=max(1, snapshot.last_step),
        )
        pending_tools: list[RecoveredToolCall] = []
        completed_by_id = {item.call.id: item for item in completed}
        for index, call in enumerate(assistant.tool_calls):
            call_key = f"s{step}:t{index}"
            recovery = snapshot.tools.get(call_key)
            if recovery is None:
                registered = self.agent.registry.get(call.name)
                policy = (
                    registered.resume_policy.value
                    if registered is not None
                    else ToolResumePolicy.REQUIRE_OPERATOR.value
                )
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.TOOL_PLANNED,
                        privacy=PrivacyClass.PRIVATE,
                        step=step,
                        call_key=call_key,
                        execution_id=writer.execution_id,
                        data={
                            "resume_policy": policy,
                            "tool_call_id": call.id,
                            "tool_name": call.name,
                            "recovered_plan": True,
                        },
                        checkpoint={
                            "call": {
                                "id": call.id,
                                "name": call.name,
                                "arguments": call.arguments,
                            }
                        },
                        safe_checkpoint=True,
                        tool_calls_delta=1,
                    ),
                    operation_id=f"tool:{call_key}:planned",
                )
                attempts = 0
            else:
                attempts = recovery.attempts
            recovered = completed_by_id.get(call.id)
            pending_tools.append(
                RecoveredToolCall(
                    call,
                    call_key,
                    attempts=attempts,
                    completed=(recovered.completed if recovered else None),
                )
            )
        return AgentResumeState(
            transcript,
            step,
            snapshot.counts.model_calls,
            # Reload because recovered TOOL_PLANNED events may have incremented.
            (await self.load(writer.run_id)).counts.tool_calls,
            snapshot.counts.tool_executions,
            snapshot.usage,
            pending_tools=tuple(pending_tools),
            completed_tools=tuple(completed),
            action_counts=self._action_counts(snapshot),
        )

    async def _finalize(self, writer: _JournalWriter, result: AgentResult) -> None:
        if result.stop_reason is StopReason.MODEL_UNAVAILABLE:
            # The Agent ran out of in-execution retries for a transient
            # provider failure. Its last durable fact is a non-terminal
            # model.failed; no run.completed is written so the run stays
            # resumable and the next ResumeRun retries the same step.
            self._results[writer.run_id] = result
            self._forget_finished_runs()
            await writer.release()
            return
        self._results[writer.run_id] = result
        self._forget_finished_runs()
        if result.status is RunStatus.COMPLETED:
            try:
                raw_session = await self.store.commit_session(
                    writer.session_id,
                    expected_version=writer.session_version,
                    transcript=cast(
                        Sequence[Mapping[str, Any]],
                        transcript_to_json(result.transcript),
                    ),
                    operation_id=f"run:{writer.run_id}:session_commit",
                )
            except Exception as exc:
                if not self._is_session_commit_conflict(exc):
                    # Availability and adapter failures leave RESULT_READY
                    # resumable. They must not be rewritten as a durable CAS
                    # loss, which would make a transient outage terminal.
                    raise
                await writer.append(
                    RunEventDraft(
                        kind=RunEventKind.RUN_ABORTED,
                        data={
                            "status": "failed",
                            "stop_reason": "session_version_conflict",
                            "error_type": type(exc).__name__,
                        },
                        safe_checkpoint=True,
                    ),
                    operation_id="run:aborted",
                )
                await writer.release()
                return
            session = self._session_record(raw_session)
            # This append deliberately sits outside the Session CAS error
            # handler. If the journal is unavailable after the Session commit,
            # the execution stops with RUN_RESULT_READY still resumable. A new
            # process repeats the stable commit operation, gets the same
            # Session version, and records this fact without aborting the run.
            await writer.append(
                RunEventDraft(
                    kind=RunEventKind.SESSION_COMMITTED,
                    data={"session_version": session.version},
                    safe_checkpoint=True,
                ),
                operation_id="session:committed",
            )

        await writer.append(
            RunEventDraft(
                kind=RunEventKind.RUN_COMPLETED,
                execution_id=writer.execution_id,
                data={
                    "status": result.status.value,
                    "stop_reason": result.stop_reason.value,
                    "model_calls": result.model_calls,
                    "tool_calls": result.tool_calls,
                    "tool_executions": result.tool_executions,
                    "usage": {
                        "input_tokens": result.usage.input_tokens,
                        "output_tokens": result.usage.output_tokens,
                        "total_tokens": result.usage.total_tokens,
                        "cached_input_tokens": result.usage.cached_input_tokens,
                        "reasoning_output_tokens": result.usage.reasoning_output_tokens,
                        "billable_tokens": result.usage.billable_tokens,
                    },
                },
                safe_checkpoint=True,
            ),
            operation_id="run:completed",
        )
        await writer.release()

    def _schedule(self, writer: _JournalWriter, coroutine: Any) -> None:
        run_id = writer.run_id

        async def execute_owned() -> None:
            try:
                await cast(Any, coroutine)
            finally:
                # A failed/cancelled execution must stop its heartbeat and
                # relinquish the fencing lease. Durable pending facts remain
                # resumable; keeping an orphan heartbeat alive would prevent
                # every future writer from taking over the Run.
                await writer.release()

        owned_coroutine = execute_owned()
        try:
            task = asyncio.create_task(owned_coroutine, name=f"react-run:{run_id}")
        except BaseException:
            # ``execute_owned`` has not started, so closing the wrapper does
            # not close the already-created inner coroutine automatically.
            owned_coroutine.close()
            close = getattr(coroutine, "close", None)
            if callable(close):
                close()
            raise

        def finished(completed: asyncio.Task[None]) -> None:
            if self._tasks.get(run_id) is completed:
                self._tasks.pop(run_id, None)
            if self._writers.get(run_id) is writer:
                self._writers.pop(run_id, None)
            if completed.cancelled():
                if writer._lease_error is not None:
                    self._task_errors[run_id] = writer._lease_error
                self._forget_finished_runs()
                return
            error = completed.exception()
            if error is not None:
                self._task_errors[run_id] = error
            self._forget_finished_runs()

        try:
            self._tasks[run_id] = task
            self._writers[run_id] = writer
            writer.bind_execution_task(task)
            task.add_done_callback(finished)
        except BaseException:
            if self._tasks.get(run_id) is task:
                self._tasks.pop(run_id, None)
            if self._writers.get(run_id) is writer:
                self._writers.pop(run_id, None)
            task.cancel()
            raise

    def _price_model_event(
        self,
        draft: RunEventDraft,
        *,
        event: AgentJournalEvent,
        attempt: int,
    ) -> tuple[RunEventDraft, RunEventDraft]:
        usage = draft.usage_delta
        raw_response_model = event.public_data.get("response_model")
        response_model = (
            raw_response_model
            if isinstance(raw_response_model, str) and raw_response_model.strip()
            else self.model_name
        )
        model_role = (
            "context_compressor"
            if event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
            else "agent"
        )
        cost_operation_id = (
            f"context_compressor:{event.operation_id}"
            if model_role == "context_compressor"
            else f"model:s{event.step}:a{attempt}"
        )
        quote = self.pricing.quote(
            operation_id=cost_operation_id,
            provider=self.provider_name,
            model=self.model_name,
            response_model=response_model,
            usage=UsageBreakdown(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cached_input_tokens=usage.cached_input_tokens or 0,
                reasoning_output_tokens=usage.reasoning_output_tokens or 0,
                billable_tokens=usage.billable_tokens,
            ),
            at=datetime.fromtimestamp(event.timestamp, tz=UTC),
            record_id=hashlib.sha256(
                (
                    f"{event.run_id}:{event.step}:{attempt}:cost"
                    if model_role == "agent"
                    else f"{event.run_id}:{cost_operation_id}:cost"
                ).encode()
            ).hexdigest()[:32],
        )
        cost_data = _cost_payload(quote)
        model_data = dict(draft.data)
        model_data.update(
            {
                "provider": self.provider_name,
                "request_model": self.model_name,
                "response_model": response_model,
                "cost_micros": quote.amount_micros,
                "currency": quote.currency,
            }
        )
        if model_role != "agent":
            model_data["model_role"] = model_role
        return (
            replace(draft, data=model_data),
            RunEventDraft(
                kind=RunEventKind.COST_RECORDED,
                occurred_at=event.timestamp,
                step=event.step,
                execution_id=event.execution_id,
                data=cost_data,
            ),
        )

    def _unknown_compression_cost_event(
        self,
        draft: RunEventDraft,
        *,
        event: AgentJournalEvent,
    ) -> tuple[RunEventDraft, RunEventDraft]:
        """Create an atomic unknown-cost record for an interrupted compressor."""

        usage = draft.usage_delta
        raw_response_model = event.public_data.get("response_model")
        response_model = (
            raw_response_model
            if isinstance(raw_response_model, str) and raw_response_model.strip()
            else self.model_name
        )
        cost_operation_id = f"context_compressor:{event.operation_id}"
        unknown_reason = event.public_data.get("compression_error")
        if not isinstance(unknown_reason, str) or not unknown_reason:
            unknown_reason = "compressor_cost_not_fully_observed"
        model_data = dict(draft.data)
        model_data.update(
            {
                "provider": self.provider_name,
                "request_model": self.model_name,
                "response_model": response_model,
                "model_role": "context_compressor",
                "cost_micros": None,
                "currency": self.pricing.default_currency,
                "cost_unknown": True,
                "unknown_reason": unknown_reason,
            }
        )
        record_id = hashlib.sha256(
            f"{event.run_id}:{cost_operation_id}:unknown-cost".encode()
        ).hexdigest()[:32]
        return (
            replace(draft, data=model_data),
            RunEventDraft(
                kind=RunEventKind.COST_RECORDED,
                occurred_at=event.timestamp,
                step=event.step,
                execution_id=event.execution_id,
                data={
                    "record_id": record_id,
                    "operation_id": cost_operation_id,
                    "kind": "estimate",
                    "amount_micros": None,
                    "currency": self.pricing.default_currency,
                    "provider": self.provider_name,
                    "model": response_model,
                    "request_model": self.model_name,
                    "source": "catalog_estimate",
                    "catalog_version": self.pricing.version,
                    "price_version": None,
                    "priced_at": datetime.fromtimestamp(event.timestamp, tz=UTC).isoformat(),
                    "estimated": True,
                    "unknown_reason": unknown_reason,
                    "usage": {
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cached_input_tokens": usage.cached_input_tokens,
                        "reasoning_output_tokens": usage.reasoning_output_tokens,
                    },
                },
            ),
        )

    async def _project_telemetry(self, event: StoredRunEvent) -> None:
        """Project one committed fact without making observability authoritative."""

        links: tuple[TraceReference, ...] = ()
        if event.kind is RunEventKind.RUN_STARTED and event.execution_id:
            try:
                existing = await self.store.load_trace_reference(
                    event.run_id, event.execution_id
                )
            except Exception:
                existing = None
            if isinstance(existing, TraceReference):
                return
        elif event.kind is RunEventKind.RUN_RESUMED:
            previous_execution_id = event.data.get("previous_execution_id")
            if isinstance(previous_execution_id, str) and previous_execution_id:
                try:
                    previous = await self.store.load_trace_reference(
                        event.run_id, previous_execution_id
                    )
                except Exception:
                    previous = None
                if (
                    isinstance(previous, TraceReference)
                    and previous.run_id == event.run_id
                    and previous.execution_id == previous_execution_id
                ):
                    links = (previous,)

        reference = self._emit_telemetry(event, links=links)
        if reference is None:
            return
        try:
            await self.store.put_trace_reference(reference)
        except Exception:
            # This content-free table is a disposable observability projection.
            # Losing it may lose a Resume link, never a durable Runtime fact.
            pass

    def _emit_telemetry(
        self,
        event: StoredRunEvent,
        *,
        links: tuple[TraceReference, ...] = (),
    ) -> TraceReference | None:
        mapping = {
            RunEventKind.RUN_STARTED: TelemetryEventKind.RUN_STARTED,
            RunEventKind.RUN_RESUMED: TelemetryEventKind.RUN_RESUMED,
            RunEventKind.RUN_COMPLETED: TelemetryEventKind.RUN_COMPLETED,
            RunEventKind.RUN_ABORTED: TelemetryEventKind.RUN_COMPLETED,
            RunEventKind.MODEL_STARTED: TelemetryEventKind.MODEL_STARTED,
            RunEventKind.MODEL_COMPLETED: TelemetryEventKind.MODEL_COMPLETED,
            RunEventKind.MODEL_FAILED: TelemetryEventKind.MODEL_FAILED,
            RunEventKind.TOOL_STARTED: TelemetryEventKind.TOOL_STARTED,
            RunEventKind.TOOL_COMPLETED: TelemetryEventKind.TOOL_COMPLETED,
            RunEventKind.TOOL_REUSED: TelemetryEventKind.TOOL_REUSED,
            RunEventKind.BUDGET_EXHAUSTED: TelemetryEventKind.BUDGET_EXHAUSTED,
            RunEventKind.LOOP_DETECTED: TelemetryEventKind.LOOP_DETECTED,
        }
        kind = mapping.get(event.kind)
        if kind is None:
            return None
        data = event.data
        attributes: dict[str, str | bool | int | float] = {
            "run_id": event.run_id,
            "execution_id": event.execution_id or "",
            "sequence": event.sequence,
            "operation_id": event.operation_id,
            "agent_name": "react_agent",
        }
        if event.step is not None:
            attributes["step"] = event.step
        scalar_keys = {
            "status",
            "stop_reason",
            "model_calls",
            "tool_calls",
            "tool_executions",
            "provider",
            "request_model",
            "response_model",
            "request_id",
            "finish_reason",
            "outcome",
            "error_type",
            "cost_micros",
            "currency",
            "reason",
            "repeat_count",
            "repeat_limit",
            "context_chars",
            "cached",
            "executed",
            "previous_execution_id",
            "resume_reason",
            "attempt",
            "execution_attempt",
            "retryable",
            "retry_exhausted",
            "status_code",
            "error_code",
        }
        for key in scalar_keys:
            value = data.get(key)
            if isinstance(value, (str, bool, int, float)):
                attributes[key] = value
        if event.kind in {
            RunEventKind.MODEL_STARTED,
            RunEventKind.MODEL_COMPLETED,
            RunEventKind.MODEL_FAILED,
        }:
            # Durable model.started intentionally contains no provider request
            # body. The Runtime binding is nevertheless safe metadata and is
            # required when the CLIENT span is named and opened.
            attributes.setdefault("provider", self.provider_name)
            attributes.setdefault("request_model", self.model_name)
        duration_ms = data.get("duration_ms")
        if isinstance(duration_ms, (int, float)):
            attributes["duration_s"] = float(duration_ms) / 1000
        if event.kind is RunEventKind.MODEL_COMPLETED:
            ttfc_ms = data.get("ttfc_ms")
            if (
                isinstance(ttfc_ms, (int, float))
                and not isinstance(ttfc_ms, bool)
            ):
                try:
                    ttfc_ms_number = float(ttfc_ms)
                except OverflowError:
                    pass
                else:
                    if math.isfinite(ttfc_ms_number) and ttfc_ms_number >= 0:
                        attributes["ttfc_s"] = ttfc_ms_number / 1000
            usage_delta = event.usage_delta
            attributes.update(
                {
                    "input_tokens": usage_delta.input_tokens,
                    "output_tokens": usage_delta.output_tokens,
                    "total_tokens": usage_delta.total_tokens,
                }
            )
            for key in (
                "cached_input_tokens",
                "reasoning_output_tokens",
                "billable_tokens",
            ):
                value = getattr(usage_delta, key)
                if value is not None:
                    attributes[key] = value
        usage = data.get("usage")
        if event.kind is not RunEventKind.MODEL_COMPLETED and isinstance(usage, Mapping):
            for key in (
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "cached_input_tokens",
                "reasoning_output_tokens",
                "billable_tokens",
            ):
                value = usage.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    attributes[key] = value
        tool_name = data.get("tool_name")
        if isinstance(tool_name, str):
            attributes["tool_name"] = tool_name
        tool_call_id = data.get("tool_call_id")
        if isinstance(tool_call_id, str):
            attributes["tool_call_id"] = tool_call_id
        if event.call_key is not None:
            attributes["call_key"] = event.call_key
        try:
            return self.telemetry.emit(
                TelemetryEvent(
                    kind,
                    attributes,
                    timestamp_ns=int(event.occurred_at * 1_000_000_000),
                    links=links,
                )
            )
        except Exception:
            # Telemetry is a committed-event observer, never a correctness
            # dependency. Collector/exporter failures must remain fail-open.
            return None

    @staticmethod
    def _public_event(event: StoredRunEvent) -> RuntimeEvent:
        data = cast(dict[str, Any], _thaw(event.data))
        return RuntimeEvent(
            run_id=event.run_id,
            kind=event.kind.value,
            timestamp=event.occurred_at,
            durable_sequence=event.sequence,
            event_id=event.event_id,
            causation_id=event.causation_id,
            execution_id=event.execution_id,
            step=event.step,
            call_key=event.call_key,
            tool_call_id=(
                str(data["tool_call_id"]) if data.get("tool_call_id") is not None else None
            ),
            tool_name=(str(data["tool_name"]) if data.get("tool_name") is not None else None),
            public_data=MappingProxyType(data),
            safe_checkpoint=event.safe_checkpoint,
            terminal=event.kind in {RunEventKind.RUN_COMPLETED, RunEventKind.RUN_ABORTED},
        )

    @staticmethod
    def _session_record(raw: object) -> SessionRecord:
        value = cast(Any, raw)
        raw_transcript = value.transcript
        transcript = tuple(cast(Mapping[str, Any], item) for item in raw_transcript)
        return SessionRecord(
            str(value.session_id),
            int(value.version),
            transcript,
            str(getattr(value, "status", "active")),
        )

    @staticmethod
    def _is_session_commit_conflict(error: BaseException) -> bool:
        if isinstance(error, SessionVersionConflict):
            return True
        # The PostgreSQL adapter deliberately does not depend on this Runtime
        # module, so normalize its narrow domain errors at the internal store
        # seam without importing the optional adapter.
        return type(error).__name__ in {
            "SessionVersionConflictError",
        }

    @staticmethod
    def _tool_call(value: Mapping[str, Any]) -> ToolCall:
        return ToolCall(
            str(value.get("id", "")),
            str(value.get("name", "")),
            str(value.get("arguments", "{}")),
        )

    @staticmethod
    def _action_counts(snapshot: RunSnapshot) -> Mapping[str, int]:
        counts: dict[str, int] = {}
        for tool in snapshot.tools.values():
            if tool.phase not in {"completed", "reused"} or tool.call is None:
                continue
            call = AgentRuntime._tool_call(tool.call)
            fingerprint = tool_action_fingerprint(call)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
        for fingerprint, repeat_count in snapshot.loop_counts.items():
            counts[fingerprint] = max(counts.get(fingerprint, 0), repeat_count)
        return MappingProxyType(counts)

    @staticmethod
    def _result_from_snapshot(snapshot: RunSnapshot) -> AgentResult:
        if snapshot.result is None:
            raise ResumeRejected("run has no durable result checkpoint")
        raw_agent_events = snapshot.result.get("agent_events", ())
        if not isinstance(raw_agent_events, (list, tuple)) or not all(
            isinstance(event, Mapping) for event in raw_agent_events
        ):
            raise ResumeRejected("run result has an invalid Agent event projection")
        try:
            agent_events = agent_events_from_json(
                cast(Sequence[Mapping[str, Any]], raw_agent_events)
            )
        except ValueError as exc:
            raise ResumeRejected("run result has an invalid Agent event projection") from exc
        status = RunStatus(str(snapshot.result.get("status", snapshot.status)))
        reason = StopReason(str(snapshot.result.get("stop_reason", snapshot.stop_reason)))
        raw_context_metrics = snapshot.result.get("context_metrics", {})
        context_metrics = (
            MappingProxyType(dict(cast(Mapping[str, JsonValue], raw_context_metrics)))
            if isinstance(raw_context_metrics, Mapping)
            else MappingProxyType({})
        )
        return AgentResult(
            output=(
                str(snapshot.result["output"])
                if snapshot.result.get("output") is not None
                else None
            ),
            status=status,
            stop_reason=reason,
            run_id=snapshot.run_id,
            model_calls=snapshot.counts.model_calls,
            tool_calls=snapshot.counts.tool_calls,
            tool_executions=snapshot.counts.tool_executions,
            usage=snapshot.usage,
            transcript=transcript_from_json(snapshot.transcript),
            events=agent_events,
            error=(
                str(snapshot.result["error"]) if snapshot.result.get("error") is not None else None
            ),
            context_metrics=context_metrics,
        )

    @staticmethod
    def _workspace_checkpoint_from_data(
        data: Mapping[str, Any],
        session_id: str,
        *,
        occurred_at: float,
    ) -> WorkspaceCheckpoint:
        raw_diff = data.get("diff")
        diff = raw_diff if isinstance(raw_diff, Mapping) else {}
        return WorkspaceCheckpoint(
            checkpoint_id=str(data.get("checkpoint_id", "")),
            session_id=session_id,
            baseline_revision=str(data.get("baseline_revision", "")),
            tree_id=str(data.get("tree_id", "")),
            commit_id=str(data.get("commit_id", "")),
            internal_ref=str(data.get("internal_ref", "")),
            created_at=occurred_at,
            diff=DiffSummary(
                files_changed=int(diff.get("files_changed", 0)),
                additions=int(diff.get("additions", 0)),
                deletions=int(diff.get("deletions", 0)),
                binary_files=int(diff.get("binary_files", 0)),
                paths=tuple(str(path) for path in diff.get("paths", ())),
            ),
        )

    @staticmethod
    def _workspace_checkpoint_from_event(
        event: StoredRunEvent, session_id: str
    ) -> WorkspaceCheckpoint:
        return AgentRuntime._workspace_checkpoint_from_data(
            event.data,
            session_id,
            occurred_at=event.occurred_at,
        )


__all__ = [
    "AgentRuntime",
    "CancelRun",
    "ForkRun",
    "InMemoryRuntimeStore",
    "ReconciliationRequired",
    "RequestPayloadConflict",
    "ResolutionAction",
    "ResolveRun",
    "ResumeRejected",
    "ResumeRun",
    "RunCommand",
    "RunHandle",
    "RuntimeConflict",
    "RuntimeEvent",
    "RuntimeNotFound",
    "SessionVersionConflict",
    "StartRun",
    "agent_event_to_draft",
]
