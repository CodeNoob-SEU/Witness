"""Versioned append-only run events and their pure state reducer."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Self

from .models import Usage

EVENT_SCHEMA_VERSION = 2
GENESIS_HASH = "0" * 64
_HASH_DOMAIN_V1 = b"react-agent-run-event:v1\0"
_HASH_DOMAIN_V2 = b"react-agent-run-event:v2\0"
_EVENT_ID_NAMESPACE = uuid.UUID("d52f059e-3444-54d2-9a65-d38a43e42ad3")


class RunEventKind(StrEnum):
    """Durable facts used for recovery and safe trace projection."""

    RUN_STARTED = "run_started"
    RUN_RESUMED = "run_resumed"
    RUN_FORKED = "run_forked"
    MODEL_STARTED = "model_started"
    MODEL_COMPLETED = "model_completed"
    MODEL_FAILED = "model_failed"
    MODEL_ABANDONED = "model_abandoned"
    TOOL_PLANNED = "tool_planned"
    TOOL_STARTED = "tool_started"
    # Backwards-compatible v1 spelling retained for logs written by the first
    # event-core increment. New Runtime records use TOOL_STARTED.
    TOOL_CLAIMED = "tool_claimed"
    TOOL_COMPLETED = "tool_completed"
    TOOL_REUSED = "tool_reused"
    CHECKPOINT = "checkpoint"
    WORKSPACE_CHECKPOINTED = "workspace_checkpointed"
    WORKSPACE_DIVERGED = "workspace_diverged"
    BUDGET_EXHAUSTED = "budget_exhausted"
    LOOP_DETECTED = "loop_detected"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    RECONCILIATION_RESOLVED = "reconciliation_resolved"
    COST_RECORDED = "cost_recorded"
    COST_ADJUSTED = "cost_adjusted"
    SESSION_COMMITTED = "session_committed"
    RUN_RESULT_READY = "run_result_ready"
    RUN_CANCEL_REQUESTED = "run_cancel_requested"
    RUN_COMPLETED = "run_completed"
    RUN_ABORTED = "run_aborted"


TERMINAL_EVENT_KINDS = frozenset({RunEventKind.RUN_COMPLETED, RunEventKind.RUN_ABORTED})


class PrivacyClass(StrEnum):
    """Most restrictive information class contained by an event."""

    PUBLIC = "public"
    METADATA = "metadata"
    PRIVATE = "private"
    SECRET = "secret"


class RunState(StrEnum):
    """Reducer phase; terminal outcome remains in status and stop_reason."""

    RUNNING = "running"
    WAITING_MODEL = "waiting_model"
    WAITING_TOOL = "waiting_tool"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    TERMINAL = "terminal"


class PendingKind(StrEnum):
    MODEL = "model"
    TOOL_PLANNED = "tool_planned"
    TOOL = "tool"
    RECONCILIATION = "reconciliation"


class EventValidationError(ValueError):
    """A durable event violates its versioned interface."""


class UnsupportedEventVersionError(EventValidationError):
    """The reducer cannot safely interpret an event schema version."""


class EventSequenceError(EventValidationError):
    """A run event chain is missing, duplicated, or out of order."""


class EventHashError(EventValidationError):
    """A run event or its previous-hash link failed verification."""


class EventTerminalError(EventValidationError):
    """A chain contains an event after its terminal fact."""


def _json_ready(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EventValidationError("JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise EventValidationError("JSON object keys must be strings")
            result[key] = _json_ready(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    raise EventValidationError(f"Unsupported canonical JSON value: {type(value).__name__}")


def _freeze_json(value: object) -> object:
    ready = _json_ready(value)
    if isinstance(ready, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in ready.items()})
    if isinstance(ready, list):
        return tuple(_freeze_json(item) for item in ready)
    return ready


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    frozen = _freeze_json(value)
    if not isinstance(frozen, Mapping):  # pragma: no cover - narrowed by the annotation
        raise EventValidationError("event data must be a JSON object")
    return frozen


def _empty_mapping() -> Mapping[str, Any]:
    return MappingProxyType({})


def _empty_int_mapping() -> Mapping[int, int]:
    return MappingProxyType({})


def canonical_json(value: object) -> str:
    """Return deterministic UTF-8 JSON for supported immutable JSON values."""

    return json.dumps(
        _json_ready(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _usage_payload(usage: Usage) -> dict[str, int | None]:
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
        "cached_input_tokens": usage.cached_input_tokens,
        "reasoning_output_tokens": usage.reasoning_output_tokens,
        "billable_tokens": usage.billable_tokens,
    }


def _stable_event_id(run_id: str, sequence: int, operation_id: str) -> str:
    """Derive one retry-stable globally unique fact identity."""

    return str(
        uuid.uuid5(
            _EVENT_ID_NAMESPACE,
            f"{run_id}\0{sequence}\0{operation_id}",
        )
    )


@dataclass(frozen=True, slots=True)
class RunEventDraft:
    """Uncommitted durable fact; sequence and chain hashes are store-owned."""

    kind: RunEventKind
    privacy: PrivacyClass = PrivacyClass.METADATA
    schema_version: int = EVENT_SCHEMA_VERSION
    occurred_at: float | None = None
    step: int | None = None
    call_key: str | None = None
    causation_id: str | None = None
    session_id: str | None = None
    execution_id: str | None = None
    agent_revision: str | None = None
    tool_manifest_hash: str | None = None
    data: Mapping[str, Any] = field(default_factory=_empty_mapping, repr=False)
    checkpoint: Mapping[str, Any] | None = field(default=None, repr=False)
    safe_checkpoint: bool = False
    usage_delta: Usage = field(default_factory=Usage)
    model_calls_delta: int = 0
    tool_calls_delta: int = 0
    tool_executions_delta: int = 0

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_SCHEMA_VERSION:
            raise UnsupportedEventVersionError(
                f"unsupported draft schema version: {self.schema_version}"
            )
        if self.occurred_at is not None and not math.isfinite(self.occurred_at):
            raise EventValidationError("occurred_at must be finite")
        if self.step is not None and self.step < 0:
            raise EventValidationError("step must be non-negative")
        for name in (
            "session_id",
            "execution_id",
            "agent_revision",
            "tool_manifest_hash",
            "call_key",
            "causation_id",
        ):
            value = getattr(self, name)
            if value is not None and not value.strip():
                raise EventValidationError(f"{name} must not be blank")
        if (
            self.kind
            in {
                RunEventKind.TOOL_PLANNED,
                RunEventKind.TOOL_STARTED,
                RunEventKind.TOOL_CLAIMED,
                RunEventKind.TOOL_COMPLETED,
                RunEventKind.TOOL_REUSED,
                RunEventKind.RECONCILIATION_REQUIRED,
                RunEventKind.RECONCILIATION_RESOLVED,
            }
            and self.call_key is None
        ):
            raise EventValidationError(f"{self.kind.value} requires call_key")
        if self.kind in TERMINAL_EVENT_KINDS:
            status = self.data.get("status")
            if not isinstance(status, str) or not status:
                raise EventValidationError("terminal events require data.status")
        if self.checkpoint is not None and self.privacy not in {
            PrivacyClass.PRIVATE,
            PrivacyClass.SECRET,
        }:
            raise EventValidationError("checkpoint events must be classified private or secret")
        deltas = (
            self.model_calls_delta,
            self.tool_calls_delta,
            self.tool_executions_delta,
            self.usage_delta.input_tokens,
            self.usage_delta.output_tokens,
            self.usage_delta.total_tokens,
        )
        if any(delta < 0 for delta in deltas):
            raise EventValidationError("usage and count deltas must be non-negative")
        object.__setattr__(self, "data", _freeze_mapping(self.data))
        if self.checkpoint is not None:
            object.__setattr__(self, "checkpoint", _freeze_mapping(self.checkpoint))

    def payload_hash(self) -> str:
        """Fingerprint caller-controlled content for operation idempotency."""

        encoded = canonical_json(_draft_payload(self)).encode("utf-8")
        return hashlib.sha256(_HASH_DOMAIN_V2 + b"draft\0" + encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class StoredRunEvent:
    """One immutable, committed record in a per-run hash chain."""

    run_id: str
    sequence: int
    operation_id: str
    event_id: str
    kind: RunEventKind
    privacy: PrivacyClass
    schema_version: int
    occurred_at: float
    previous_hash: str
    event_hash: str
    step: int | None = None
    call_key: str | None = None
    causation_id: str | None = None
    session_id: str | None = None
    execution_id: str | None = None
    agent_revision: str | None = None
    tool_manifest_hash: str | None = None
    data: Mapping[str, Any] = field(default_factory=_empty_mapping, repr=False)
    checkpoint: Mapping[str, Any] | None = field(default=None, repr=False)
    safe_checkpoint: bool = False
    usage_delta: Usage = field(default_factory=Usage)
    model_calls_delta: int = 0
    tool_calls_delta: int = 0
    tool_executions_delta: int = 0

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise EventValidationError("run_id must not be blank")
        if not self.operation_id.strip():
            raise EventValidationError("operation_id must not be blank")
        try:
            parsed_event_id = uuid.UUID(self.event_id)
        except (AttributeError, ValueError):
            raise EventValidationError("event_id must be a UUID") from None
        if str(parsed_event_id) != self.event_id:
            raise EventValidationError("event_id must use canonical UUID text")
        if self.causation_id is not None:
            try:
                parsed_causation_id = uuid.UUID(self.causation_id)
            except (AttributeError, ValueError):
                raise EventValidationError("causation_id must be a UUID or None") from None
            if str(parsed_causation_id) != self.causation_id:
                raise EventValidationError("causation_id must use canonical UUID text")
        if self.sequence < 1:
            raise EventSequenceError("stored sequence must be positive")
        if not math.isfinite(self.occurred_at):
            raise EventValidationError("occurred_at must be finite")
        object.__setattr__(self, "data", _freeze_mapping(self.data))
        if self.checkpoint is not None:
            object.__setattr__(self, "checkpoint", _freeze_mapping(self.checkpoint))

    @classmethod
    def from_draft(
        cls,
        draft: RunEventDraft,
        *,
        run_id: str,
        sequence: int,
        operation_id: str,
        previous_hash: str,
        occurred_at: float,
        event_id: str | None = None,
        causation_id: str | None = None,
        session_id: str | None = None,
        execution_id: str | None = None,
        agent_revision: str | None = None,
        tool_manifest_hash: str | None = None,
    ) -> Self:
        event = cls(
            run_id=run_id,
            sequence=sequence,
            operation_id=operation_id,
            event_id=event_id or _stable_event_id(run_id, sequence, operation_id),
            kind=draft.kind,
            privacy=draft.privacy,
            schema_version=draft.schema_version,
            occurred_at=draft.occurred_at if draft.occurred_at is not None else occurred_at,
            previous_hash=previous_hash,
            event_hash="",
            step=draft.step,
            call_key=draft.call_key,
            causation_id=(
                draft.causation_id
                if draft.causation_id is not None
                else causation_id
            ),
            session_id=session_id if session_id is not None else draft.session_id,
            execution_id=execution_id if execution_id is not None else draft.execution_id,
            agent_revision=(agent_revision if agent_revision is not None else draft.agent_revision),
            tool_manifest_hash=(
                tool_manifest_hash if tool_manifest_hash is not None else draft.tool_manifest_hash
            ),
            data=draft.data,
            checkpoint=draft.checkpoint,
            safe_checkpoint=draft.safe_checkpoint,
            usage_delta=draft.usage_delta,
            model_calls_delta=draft.model_calls_delta,
            tool_calls_delta=draft.tool_calls_delta,
            tool_executions_delta=draft.tool_executions_delta,
        )
        return replace(event, event_hash=compute_event_hash(event))


def _draft_payload(draft: RunEventDraft) -> dict[str, object]:
    return {
        "schema_version": draft.schema_version,
        "kind": draft.kind.value,
        "privacy": draft.privacy.value,
        "occurred_at": draft.occurred_at,
        "step": draft.step,
        "call_key": draft.call_key,
        "causation_id": draft.causation_id,
        "session_id": draft.session_id,
        "execution_id": draft.execution_id,
        "agent_revision": draft.agent_revision,
        "tool_manifest_hash": draft.tool_manifest_hash,
        "data": draft.data,
        "checkpoint": draft.checkpoint,
        "safe_checkpoint": draft.safe_checkpoint,
        "usage_delta": _usage_payload(draft.usage_delta),
        "model_calls_delta": draft.model_calls_delta,
        "tool_calls_delta": draft.tool_calls_delta,
        "tool_executions_delta": draft.tool_executions_delta,
    }


def _stored_payload_v1(event: StoredRunEvent) -> dict[str, object]:
    """Return the exact canonical payload committed by schema v1.

    Keep this function version-specific: changing it would invalidate every v1
    hash already stored in a journal. A future schema gets its own payload
    encoder and hash function instead of branching inside this one.
    """

    return {
        "schema_version": event.schema_version,
        "run_id": event.run_id,
        "sequence": event.sequence,
        "operation_id": event.operation_id,
        "kind": event.kind.value,
        "privacy": event.privacy.value,
        "occurred_at": event.occurred_at,
        "previous_hash": event.previous_hash,
        "step": event.step,
        "call_key": event.call_key,
        "session_id": event.session_id,
        "execution_id": event.execution_id,
        "agent_revision": event.agent_revision,
        "tool_manifest_hash": event.tool_manifest_hash,
        "data": event.data,
        "checkpoint": event.checkpoint,
        "safe_checkpoint": event.safe_checkpoint,
        "usage_delta": _usage_payload(event.usage_delta),
        "model_calls_delta": event.model_calls_delta,
        "tool_calls_delta": event.tool_calls_delta,
        "tool_executions_delta": event.tool_executions_delta,
    }


def _compute_event_hash_v1(event: StoredRunEvent) -> str:
    encoded = canonical_json(_stored_payload_v1(event)).encode("utf-8")
    return hashlib.sha256(_HASH_DOMAIN_V1 + encoded).hexdigest()


def _upcast_v1_to_v2(event: StoredRunEvent) -> StoredRunEvent:
    """Add v2 correlation semantics after the original v1 chain is verified."""

    return replace(event, schema_version=2)


def _stored_payload_v2(event: StoredRunEvent) -> dict[str, object]:
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "run_id": event.run_id,
        "sequence": event.sequence,
        "operation_id": event.operation_id,
        "causation_id": event.causation_id,
        "kind": event.kind.value,
        "privacy": event.privacy.value,
        "occurred_at": event.occurred_at,
        "previous_hash": event.previous_hash,
        "step": event.step,
        "call_key": event.call_key,
        "session_id": event.session_id,
        "execution_id": event.execution_id,
        "agent_revision": event.agent_revision,
        "tool_manifest_hash": event.tool_manifest_hash,
        "data": event.data,
        "checkpoint": event.checkpoint,
        "safe_checkpoint": event.safe_checkpoint,
        "usage_delta": _usage_payload(event.usage_delta),
        "model_calls_delta": event.model_calls_delta,
        "tool_calls_delta": event.tool_calls_delta,
        "tool_executions_delta": event.tool_executions_delta,
    }


def _compute_event_hash_v2(event: StoredRunEvent) -> str:
    encoded = canonical_json(_stored_payload_v2(event)).encode("utf-8")
    return hashlib.sha256(_HASH_DOMAIN_V2 + encoded).hexdigest()


def _identity_v2(event: StoredRunEvent) -> StoredRunEvent:
    return event


@dataclass(frozen=True, slots=True)
class _EventSchemaStage:
    """One trusted stored-schema decoder and its semantic upcast step."""

    schema_version: int
    target_version: int
    compute_hash: Callable[[StoredRunEvent], str]
    upcast: Callable[[StoredRunEvent], StoredRunEvent]


# Only schemas that actually existed on disk belong here. There is deliberately
# no synthetic v0 adapter. V1 remains frozen; v2 adds event/causation identity.
_EVENT_SCHEMA_PIPELINE: Mapping[int, _EventSchemaStage] = MappingProxyType(
    {
        1: _EventSchemaStage(
            schema_version=1,
            target_version=2,
            compute_hash=_compute_event_hash_v1,
            upcast=_upcast_v1_to_v2,
        ),
        2: _EventSchemaStage(
            schema_version=2,
            target_version=2,
            compute_hash=_compute_event_hash_v2,
            upcast=_identity_v2,
        ),
    }
)
SUPPORTED_EVENT_SCHEMA_VERSIONS = tuple(_EVENT_SCHEMA_PIPELINE)


def _schema_stage(schema_version: int) -> _EventSchemaStage:
    try:
        return _EVENT_SCHEMA_PIPELINE[schema_version]
    except KeyError:
        raise UnsupportedEventVersionError(
            f"unsupported stored schema version: {schema_version}"
        ) from None


def compute_event_hash(event: StoredRunEvent) -> str:
    """Hash an event using the codec for its original stored schema."""

    return _schema_stage(event.schema_version).compute_hash(event)


def verify_event_chain(events: Sequence[StoredRunEvent]) -> None:
    """Verify the original stored chain before any semantic conversion."""

    if not events:
        return
    run_id = events[0].run_id
    session_id = events[0].session_id
    agent_revision = events[0].agent_revision
    tool_manifest_hash = events[0].tool_manifest_hash
    previous_hash = GENESIS_HASH
    operations: set[str] = set()
    event_ids: set[str] = set()
    terminal_seen = False
    for expected_sequence, event in enumerate(events, start=1):
        stage = _schema_stage(event.schema_version)
        if event.run_id != run_id:
            raise EventSequenceError("one chain cannot contain multiple run ids")
        if event.sequence != expected_sequence:
            raise EventSequenceError(f"expected sequence {expected_sequence}, got {event.sequence}")
        if event.operation_id in operations:
            raise EventSequenceError("operation ids must be unique within a run")
        operations.add(event.operation_id)
        if event.event_id in event_ids:
            raise EventSequenceError("event ids must be unique within a run")
        event_ids.add(event.event_id)
        if event.causation_id == event.event_id:
            raise EventValidationError("an event cannot cause itself")
        if terminal_seen:
            raise EventTerminalError("events cannot follow a terminal event")
        if event.previous_hash != previous_hash:
            raise EventHashError(f"previous hash mismatch at sequence {event.sequence}")
        if event.event_hash != stage.compute_hash(event):
            raise EventHashError(f"event hash mismatch at sequence {event.sequence}")
        if event.session_id != session_id:
            raise EventValidationError("session_id cannot change within a run")
        if event.agent_revision != agent_revision:
            raise EventValidationError("agent_revision cannot change within a run")
        if event.tool_manifest_hash != tool_manifest_hash:
            raise EventValidationError("tool_manifest_hash cannot change within a run")
        terminal_seen = event.kind in TERMINAL_EVENT_KINDS
        previous_hash = event.event_hash


def _upcast_verified_event(event: StoredRunEvent) -> StoredRunEvent:
    """Upcast one already-verified event; never use on untrusted journal rows."""

    current = event
    visited: set[int] = set()
    while True:
        stage = _schema_stage(current.schema_version)
        if stage.schema_version in visited:
            raise RuntimeError("event upcaster pipeline contains a version cycle")
        visited.add(stage.schema_version)
        converted = stage.upcast(current)
        if converted.schema_version != stage.target_version:
            raise RuntimeError(
                "event upcaster returned a schema version other than its declared target"
            )
        for field_name in (
            "run_id",
            "sequence",
            "operation_id",
            "event_id",
            "causation_id",
            "occurred_at",
            "previous_hash",
            "event_hash",
            "session_id",
            "execution_id",
            "agent_revision",
            "tool_manifest_hash",
        ):
            if getattr(converted, field_name) != getattr(current, field_name):
                raise RuntimeError(
                    f"event upcaster changed immutable envelope field {field_name}"
                )
        if converted.schema_version == EVENT_SCHEMA_VERSION:
            return converted
        if converted.schema_version < current.schema_version:
            raise RuntimeError("event upcaster pipeline cannot move backwards")
        current = converted


def upcast_events(events: Sequence[StoredRunEvent]) -> tuple[StoredRunEvent, ...]:
    """Validate raw stored facts, then convert them to current reducer semantics.

    Hashes are always checked with the original schema codec. Upcasters run
    only after the complete raw chain has passed validation, so a transform can
    never repair or conceal a corrupt journal entry.
    """

    verify_event_chain(events)
    return tuple(_upcast_verified_event(event) for event in events)


@dataclass(frozen=True, slots=True)
class RunCounts:
    model_calls: int = 0
    tool_calls: int = 0
    tool_executions: int = 0


@dataclass(frozen=True, slots=True)
class PendingAction:
    key: str
    kind: PendingKind
    sequence: int
    operation_id: str
    step: int | None = None
    call_key: str | None = None
    phase: str | None = None


@dataclass(frozen=True, slots=True)
class ToolRecovery:
    """Reducer-owned recovery record for one stable logical tool call."""

    call_key: str
    step: int
    phase: str
    attempts: int
    tool_call_id: str | None = None
    tool_name: str | None = None
    resume_policy: str | None = None
    call: Mapping[str, Any] | None = field(default=None, repr=False)
    message: Mapping[str, Any] | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Pure projection sufficient for replay and recovery decisions."""

    run_id: str
    session_id: str | None
    execution_id: str | None
    agent_revision: str | None
    tool_manifest_hash: str | None
    state: RunState
    status: str | None
    stop_reason: str | None
    transcript: tuple[Mapping[str, Any], ...] = field(repr=False)
    usage: Usage
    counts: RunCounts
    pending: Mapping[str, PendingAction]
    safe_checkpoint_sequences: tuple[int, ...]
    terminal: StoredRunEvent | None
    last_sequence: int
    last_hash: str
    last_step: int = 0
    executions: tuple[str, ...] = ()
    model_attempts: Mapping[int, int] = field(default_factory=_empty_int_mapping)
    tools: Mapping[str, ToolRecovery] = field(default_factory=_empty_mapping, repr=False)
    loop_counts: Mapping[str, int] = field(default_factory=_empty_mapping)
    costs: tuple[Mapping[str, Any], ...] = field(default=(), repr=False)
    workspace: Mapping[str, Any] | None = field(default=None, repr=False)
    workspace_anchor: Mapping[str, Any] | None = field(default=None, repr=False)
    result: Mapping[str, Any] | None = field(default=None, repr=False)
    session_version: int | None = None
    parent_run_id: str | None = None
    fork_sequence: int | None = None


def _model_pending_key(event: StoredRunEvent) -> str:
    attempt = event.data.get("attempt", 1)
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise EventValidationError("model event attempt must be a positive integer")
    return f"model:s{event.step or 0}:a{attempt}"


def _state_from_pending(pending: Mapping[str, PendingAction]) -> RunState:
    kinds = {action.kind for action in pending.values()}
    if PendingKind.RECONCILIATION in kinds:
        return RunState.NEEDS_RECONCILIATION
    if PendingKind.TOOL in kinds or PendingKind.TOOL_PLANNED in kinds:
        return RunState.WAITING_TOOL
    if PendingKind.MODEL in kinds:
        return RunState.WAITING_MODEL
    return RunState.RUNNING


def _checkpoint_transcript(
    checkpoint: Mapping[str, Any] | None,
    current: tuple[Mapping[str, Any], ...],
) -> tuple[Mapping[str, Any], ...]:
    if checkpoint is None or "transcript" not in checkpoint:
        return current
    raw = checkpoint["transcript"]
    if not isinstance(raw, (list, tuple)):
        raise EventValidationError("checkpoint transcript must be a JSON array")
    result: list[Mapping[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise EventValidationError("checkpoint transcript items must be JSON objects")
        result.append(item)
    return tuple(result)


def _workspace_anchor_from_first(
    first: StoredRunEvent,
) -> Mapping[str, Any] | None:
    raw = first.data.get("workspace_anchor")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise EventValidationError("workspace_anchor must be a JSON object")
    for name in (
        "checkpoint_id",
        "baseline_revision",
        "tree_id",
        "commit_id",
        "internal_ref",
    ):
        value = raw.get(name)
        if not isinstance(value, str) or not value:
            raise EventValidationError(f"workspace_anchor.{name} must be a non-empty string")
    created_at = raw.get("created_at")
    if (
        not isinstance(created_at, (int, float))
        or isinstance(created_at, bool)
        or not math.isfinite(float(created_at))
    ):
        raise EventValidationError("workspace_anchor.created_at must be finite")
    return raw


def fold_events(events: Sequence[StoredRunEvent]) -> RunSnapshot:
    """Purely rebuild a run snapshot; never invokes a model, tool, or adapter."""

    if not events:
        raise EventSequenceError("cannot fold an empty run")
    events = upcast_events(events)
    if events[0].kind is not RunEventKind.RUN_STARTED:
        raise EventSequenceError("the first run event must be run_started")

    first = events[0]
    state = RunState.RUNNING
    status: str | None = None
    stop_reason: str | None = None
    transcript: tuple[Mapping[str, Any], ...] = ()
    usage = Usage()
    counts = RunCounts()
    pending: dict[str, PendingAction] = {}
    safe_checkpoints: list[int] = []
    terminal: StoredRunEvent | None = None
    execution_id = first.execution_id
    executions: list[str] = []
    model_attempts: dict[int, int] = {}
    tools: dict[str, ToolRecovery] = {}
    loop_counts: dict[str, int] = {}
    costs: list[Mapping[str, Any]] = []
    workspace_anchor = _workspace_anchor_from_first(first)
    workspace: Mapping[str, Any] | None = workspace_anchor
    result_payload: Mapping[str, Any] | None = None
    session_version: int | None = None
    last_step = 0
    parent_run_id = first.data.get("parent_run_id")
    fork_sequence = first.data.get("fork_sequence")
    if parent_run_id is not None and not isinstance(parent_run_id, str):
        raise EventValidationError("parent_run_id must be a string")
    if fork_sequence is not None and (
        not isinstance(fork_sequence, int) or isinstance(fork_sequence, bool)
    ):
        raise EventValidationError("fork_sequence must be an integer")

    for event in events:
        if event.execution_id is not None:
            execution_id = event.execution_id
            if event.execution_id not in executions:
                executions.append(event.execution_id)
        if event.step is not None:
            last_step = max(last_step, event.step)
        updates_run_outcome = (
            event.kind
            in {
                RunEventKind.RUN_STARTED,
                RunEventKind.RUN_RESULT_READY,
                RunEventKind.MODEL_FAILED,
                RunEventKind.BUDGET_EXHAUSTED,
                RunEventKind.LOOP_DETECTED,
                *TERMINAL_EVENT_KINDS,
            }
            or event.data.get("terminal_decision") is True
        )
        if updates_run_outcome:
            status_value = event.data.get("status")
            if status_value is not None:
                if not isinstance(status_value, str):
                    raise EventValidationError("run status must be a string")
                status = status_value
            reason_value = event.data.get("stop_reason")
            if reason_value is not None:
                if not isinstance(reason_value, str):
                    raise EventValidationError("run stop_reason must be a string")
                stop_reason = reason_value

        usage = usage + event.usage_delta
        counts = RunCounts(
            model_calls=counts.model_calls + event.model_calls_delta,
            tool_calls=counts.tool_calls + event.tool_calls_delta,
            tool_executions=counts.tool_executions + event.tool_executions_delta,
        )
        transcript = _checkpoint_transcript(event.checkpoint, transcript)
        if event.safe_checkpoint:
            safe_checkpoints.append(event.sequence)

        if event.kind in {
            RunEventKind.WORKSPACE_CHECKPOINTED,
            RunEventKind.WORKSPACE_DIVERGED,
        }:
            workspace = event.data
        elif event.kind in {RunEventKind.COST_RECORDED, RunEventKind.COST_ADJUSTED}:
            costs.append(event.data)
        elif event.kind is RunEventKind.SESSION_COMMITTED:
            raw_version = event.data.get("session_version")
            if not isinstance(raw_version, int) or isinstance(raw_version, bool):
                raise EventValidationError("session commit requires integer session_version")
            session_version = raw_version
        elif event.kind is RunEventKind.RUN_RESULT_READY:
            if event.checkpoint is not None:
                result_payload = event.checkpoint

        if event.kind is RunEventKind.MODEL_STARTED:
            key = _model_pending_key(event)
            attempt = event.data.get("attempt", 1)
            assert isinstance(attempt, int) and not isinstance(attempt, bool)
            model_attempts[event.step or 0] = max(model_attempts.get(event.step or 0, 0), attempt)
            pending[key] = PendingAction(
                key=key,
                kind=PendingKind.MODEL,
                sequence=event.sequence,
                operation_id=event.operation_id,
                step=event.step,
                phase="started",
            )
            state = _state_from_pending(pending)
        elif event.kind in {
            RunEventKind.MODEL_COMPLETED,
            RunEventKind.MODEL_FAILED,
            RunEventKind.MODEL_ABANDONED,
        }:
            pending.pop(_model_pending_key(event), None)
            state = _state_from_pending(pending)
        elif event.kind is RunEventKind.TOOL_PLANNED:
            assert event.call_key is not None
            checkpoint_call = event.checkpoint.get("call") if event.checkpoint is not None else None
            call = checkpoint_call if isinstance(checkpoint_call, Mapping) else None
            pending[event.call_key] = PendingAction(
                key=event.call_key,
                kind=PendingKind.TOOL_PLANNED,
                sequence=event.sequence,
                operation_id=event.operation_id,
                step=event.step,
                call_key=event.call_key,
                phase="planned",
            )
            tools[event.call_key] = ToolRecovery(
                call_key=event.call_key,
                step=event.step or 0,
                phase="planned",
                attempts=0,
                tool_call_id=(
                    str(event.data["tool_call_id"])
                    if event.data.get("tool_call_id") is not None
                    else None
                ),
                tool_name=(
                    str(event.data["tool_name"])
                    if event.data.get("tool_name") is not None
                    else None
                ),
                resume_policy=(
                    str(event.data["resume_policy"])
                    if event.data.get("resume_policy") is not None
                    else None
                ),
                call=call,
            )
            state = _state_from_pending(pending)
        elif event.kind in {RunEventKind.TOOL_STARTED, RunEventKind.TOOL_CLAIMED}:
            assert event.call_key is not None
            previous_tool = tools.get(event.call_key)
            raw_attempt = event.data.get("attempt")
            attempt = (
                raw_attempt
                if isinstance(raw_attempt, int) and not isinstance(raw_attempt, bool)
                else (previous_tool.attempts + 1 if previous_tool is not None else 1)
            )
            if attempt < 1:
                raise EventValidationError("tool attempt must be positive")
            checkpoint_call = event.checkpoint.get("call") if event.checkpoint is not None else None
            call = (
                checkpoint_call
                if isinstance(checkpoint_call, Mapping)
                else (previous_tool.call if previous_tool is not None else None)
            )
            pending[event.call_key] = PendingAction(
                key=event.call_key,
                kind=PendingKind.TOOL,
                sequence=event.sequence,
                operation_id=event.operation_id,
                step=event.step,
                call_key=event.call_key,
                phase="started",
            )
            tools[event.call_key] = ToolRecovery(
                call_key=event.call_key,
                step=event.step or (previous_tool.step if previous_tool else 0),
                phase="started",
                attempts=attempt,
                tool_call_id=(
                    str(event.data["tool_call_id"])
                    if event.data.get("tool_call_id") is not None
                    else (previous_tool.tool_call_id if previous_tool else None)
                ),
                tool_name=(
                    str(event.data["tool_name"])
                    if event.data.get("tool_name") is not None
                    else (previous_tool.tool_name if previous_tool else None)
                ),
                resume_policy=(
                    str(event.data["resume_policy"])
                    if event.data.get("resume_policy") is not None
                    else (previous_tool.resume_policy if previous_tool else None)
                ),
                call=call,
            )
            state = _state_from_pending(pending)
        elif event.kind in {RunEventKind.TOOL_COMPLETED, RunEventKind.TOOL_REUSED}:
            assert event.call_key is not None
            pending.pop(event.call_key, None)
            previous_tool = tools.get(event.call_key)
            checkpoint_message = (
                event.checkpoint.get("message") if event.checkpoint is not None else None
            )
            message = (
                checkpoint_message
                if isinstance(checkpoint_message, Mapping)
                else (previous_tool.message if previous_tool is not None else None)
            )
            tools[event.call_key] = ToolRecovery(
                call_key=event.call_key,
                step=event.step or (previous_tool.step if previous_tool else 0),
                phase=("reused" if event.kind is RunEventKind.TOOL_REUSED else "completed"),
                attempts=(previous_tool.attempts if previous_tool else 0),
                tool_call_id=(previous_tool.tool_call_id if previous_tool else None),
                tool_name=(previous_tool.tool_name if previous_tool else None),
                resume_policy=(previous_tool.resume_policy if previous_tool else None),
                call=(previous_tool.call if previous_tool else None),
                message=message,
            )
            state = _state_from_pending(pending)
        elif event.kind is RunEventKind.RECONCILIATION_REQUIRED:
            assert event.call_key is not None
            pending[event.call_key] = PendingAction(
                key=event.call_key,
                kind=PendingKind.RECONCILIATION,
                sequence=event.sequence,
                operation_id=event.operation_id,
                step=event.step,
                call_key=event.call_key,
                phase="reconciliation",
            )
            previous_tool = tools.get(event.call_key)
            if previous_tool is not None:
                tools[event.call_key] = replace(previous_tool, phase="needs_reconciliation")
            state = RunState.NEEDS_RECONCILIATION
        elif event.kind is RunEventKind.RECONCILIATION_RESOLVED:
            assert event.call_key is not None
            pending.pop(event.call_key, None)
            previous_tool = tools.get(event.call_key)
            if previous_tool is not None:
                tools[event.call_key] = replace(previous_tool, phase="resolved")
            state = _state_from_pending(pending)
        elif event.kind is RunEventKind.LOOP_DETECTED:
            fingerprint = event.data.get("action_fingerprint")
            repeat_count = event.data.get("repeat_count")
            if isinstance(fingerprint, str) and isinstance(repeat_count, int):
                loop_counts[fingerprint] = repeat_count
        elif event.kind in TERMINAL_EVENT_KINDS:
            terminal = event
            state = RunState.TERMINAL

    return RunSnapshot(
        run_id=first.run_id,
        session_id=first.session_id,
        execution_id=execution_id,
        agent_revision=first.agent_revision,
        tool_manifest_hash=first.tool_manifest_hash,
        state=state,
        status=status,
        stop_reason=stop_reason,
        transcript=transcript,
        usage=usage,
        counts=counts,
        pending=MappingProxyType(dict(pending)),
        safe_checkpoint_sequences=tuple(safe_checkpoints),
        terminal=terminal,
        last_sequence=events[-1].sequence,
        last_hash=events[-1].event_hash,
        last_step=last_step,
        executions=tuple(executions),
        model_attempts=MappingProxyType(model_attempts),
        tools=MappingProxyType(tools),
        loop_counts=MappingProxyType(loop_counts),
        costs=tuple(costs),
        workspace=workspace,
        workspace_anchor=workspace_anchor,
        result=result_payload,
        session_version=session_version,
        parent_run_id=parent_run_id,
        fork_sequence=fork_sequence,
    )
