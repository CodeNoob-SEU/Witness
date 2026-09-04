"""Deterministic PR evidence generated from verified runtime-debug journal facts.

This module is deliberately replay-only.  It does not import a model, debugger,
network client, clock, or filesystem API.  The caller supplies the complete run
event chain and receives byte-stable canonical JSON plus fixed-template Markdown.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, cast

from .events import RunEventKind, StoredRunEvent, canonical_json, verify_event_chain
from .models import JsonValue

DEBUG_OBSERVATION_SCHEMA_VERSION = 1
DEBUG_EVIDENCE_SCHEMA_VERSION = 1
DEBUG_EVIDENCE_GENERATOR_REVISION = "debug-evidence-v2"

_DEBUG_TOOL_PREFIX = "python_debug_"
_LIFECYCLE_KINDS = frozenset(
    {
        RunEventKind.TOOL_PLANNED,
        RunEventKind.TOOL_STARTED,
        RunEventKind.TOOL_COMPLETED,
        RunEventKind.TOOL_REUSED,
    }
)
_TERMINAL_KINDS = frozenset({RunEventKind.TOOL_COMPLETED, RunEventKind.TOOL_REUSED})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_ABSOLUTE_PATH = re.compile(r"^[A-Za-z]:[\\/]")


class DebugEvidenceError(ValueError):
    """The verified journal cannot safely produce runtime-debug evidence."""


class DebugObservationError(DebugEvidenceError):
    """A normalized runtime-debug observation is malformed or has a bad digest."""


class DebugLifecycleError(DebugEvidenceError):
    """Debug tool lifecycle events are missing, duplicated, or contradictory."""


@dataclass(frozen=True, slots=True)
class DebugEvidenceArtifacts:
    """Byte-stable evidence payloads produced without external calls."""

    canonical_json: str
    markdown: str
    evidence_payload_sha256: str


@dataclass(frozen=True, slots=True)
class _TerminalResult:
    outcome: str
    call_id: str
    tool_name: str
    observation: dict[str, JsonValue] | None


@dataclass(frozen=True, slots=True)
class _DebugLifecycle:
    call_key: str
    tool_name: str
    planned: StoredRunEvent
    started: StoredRunEvent | None
    terminal: StoredRunEvent
    result: _TerminalResult

    @property
    def action(self) -> str:
        observation = self.result.observation
        if observation is not None:
            action = observation.get("action")
            if isinstance(action, str):
                return action
        return self.tool_name.removeprefix(_DEBUG_TOOL_PREFIX)


@dataclass(frozen=True, slots=True)
class _ObservationRecord:
    event: StoredRunEvent
    observation: dict[str, JsonValue]


def _json_object(value: object, *, label: str) -> dict[str, JsonValue]:
    """Normalize one JSON object through the event-core canonical codec."""

    try:
        decoded = json.loads(canonical_json(value))
    except (TypeError, ValueError) as exc:
        raise DebugObservationError(f"{label} must contain only finite JSON values") from exc
    if not isinstance(decoded, dict):
        raise DebugObservationError(f"{label} must be a JSON object")
    return cast(dict[str, JsonValue], decoded)


def _required_text(value: Mapping[str, JsonValue], key: str, *, label: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise DebugObservationError(f"{label}.{key} must be a non-empty string")
    if any(character in raw for character in ("\x00", "\r", "\n")):
        raise DebugObservationError(f"{label}.{key} must be normalized single-line text")
    return raw


def _optional_stop_id(value: Mapping[str, JsonValue]) -> int | None:
    raw = value.get("stop_id")
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise DebugObservationError("observation.stop_id must be a positive integer")
    return raw


def _normalized_path(raw: JsonValue, *, label: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise DebugObservationError(f"{label} must be a non-empty string")
    if "\\" in raw or "\x00" in raw or "\r" in raw or "\n" in raw:
        raise DebugObservationError(f"{label} must be a normalized workspace-relative path")
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or _WINDOWS_ABSOLUTE_PATH.match(raw):
        raise DebugObservationError(f"{label} must be a normalized workspace-relative path")
    return raw


def _positive_line(raw: JsonValue, *, label: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise DebugObservationError(f"{label} must be a positive integer")
    return raw


def _non_negative_line(raw: JsonValue, *, label: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise DebugObservationError(f"{label} must be a non-negative integer")
    return raw


def _validate_exception(
    raw: JsonValue,
    *,
    require_location: bool = False,
) -> dict[str, JsonValue]:
    if not isinstance(raw, dict):
        raise DebugObservationError("observation.exception must be an object")
    exception = raw
    _required_text(exception, "type", label="observation.exception")
    _required_text(exception, "message", label="observation.exception")
    location_fields = ("path", "line", "function")
    present = [field in exception for field in location_fields]
    if require_location or any(present):
        if not all(present):
            raise DebugObservationError(
                "observation.exception location must include path, line, and function"
            )
        _normalized_path(exception.get("path"), label="observation.exception.path")
        _positive_line(exception.get("line"), label="observation.exception.line")
        _required_text(exception, "function", label="observation.exception")
    return exception


def _validate_frame(
    raw: JsonValue,
    *,
    require_workspace: bool = False,
) -> dict[str, JsonValue]:
    if not isinstance(raw, dict):
        raise DebugObservationError("observation.selected_frame must be an object")
    frame = raw
    frame_index = frame.get("frame_index")
    if isinstance(frame_index, bool) or not isinstance(frame_index, int) or frame_index < 0:
        raise DebugObservationError(
            "observation.selected_frame.frame_index must be a non-negative integer"
        )
    _required_text(frame, "function", label="observation.selected_frame")
    _normalized_path(frame.get("path"), label="observation.selected_frame.path")
    if require_workspace:
        _positive_line(frame.get("line"), label="observation.selected_frame.line")
    else:
        _non_negative_line(frame.get("line"), label="observation.selected_frame.line")
    in_workspace = frame.get("in_workspace")
    if not isinstance(in_workspace, bool):
        raise DebugObservationError(
            "observation.selected_frame.in_workspace must be a boolean"
        )
    if require_workspace and not in_workspace:
        raise DebugObservationError(
            "PR evidence selected_frame must be inside the managed workspace"
        )
    _required_text(
        frame,
        "selection_rule_version",
        label="observation.selected_frame",
    )
    raw_reasons = frame.get("selection_reasons")
    if not isinstance(raw_reasons, list) or not raw_reasons:
        raise DebugObservationError(
            "observation.selected_frame.selection_reasons must be a non-empty array"
        )
    for index, reason in enumerate(raw_reasons):
        if not isinstance(reason, str) or not reason.strip() or any(
            character in reason for character in ("\x00", "\r", "\n")
        ):
            raise DebugObservationError(
                "observation.selected_frame.selection_reasons"
                f"[{index}] must be normalized single-line text"
            )
    return frame


def _validate_variables(raw: JsonValue) -> list[dict[str, JsonValue]]:
    if not isinstance(raw, list):
        raise DebugObservationError("observation.variables must be an array")
    variables: list[dict[str, JsonValue]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise DebugObservationError(f"observation.variables[{index}] must be an object")
        variable = item
        scope = _required_text(variable, "scope", label=f"observation.variables[{index}]")
        if scope not in {"locals", "arguments"}:
            raise DebugObservationError(
                f"observation.variables[{index}].scope must be locals or arguments"
            )
        _required_text(variable, "name", label=f"observation.variables[{index}]")
        _required_text(variable, "type", label=f"observation.variables[{index}]")
        value = variable.get("value")
        if not isinstance(value, str):
            raise DebugObservationError(
                f"observation.variables[{index}].value must be a string"
            )
        for flag in ("redacted", "truncated"):
            if not isinstance(variable.get(flag), bool):
                raise DebugObservationError(
                    f"observation.variables[{index}].{flag} must be a boolean"
                )
        variables.append(variable)
    return variables


def _validate_reproduction(raw: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(raw, dict):
        raise DebugObservationError("observation.reproduction must be an object")
    command = raw.get("command")
    if not isinstance(command, list) or not command:
        raise DebugObservationError("observation.reproduction.command must be a non-empty array")
    for index, argument in enumerate(command):
        if not isinstance(argument, str) or not argument or any(
            character in argument for character in ("\x00", "\r", "\n")
        ):
            raise DebugObservationError(
                f"observation.reproduction.command[{index}] must be normalized text"
            )
    _normalized_path(raw.get("cwd"), label="observation.reproduction.cwd")
    return raw


def _validate_debuggee_exit(raw: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(raw, dict):
        raise DebugObservationError("observation.debuggee_exit must be an object")
    status = _required_text(raw, "status", label="observation.debuggee_exit")
    if status not in {"exited", "terminated"}:
        raise DebugObservationError(
            "observation.debuggee_exit.status must be exited or terminated"
        )
    exit_code = raw.get("exit_code")
    if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
        raise DebugObservationError(
            "observation.debuggee_exit.exit_code must be an integer or null"
        )
    signal = raw.get("signal")
    if signal is not None and (
        not isinstance(signal, str)
        or not signal.strip()
        or any(character in signal for character in ("\x00", "\r", "\n"))
    ):
        raise DebugObservationError(
            "observation.debuggee_exit.signal must be normalized text or null"
        )
    return raw


def _validate_observation_shape(observation: dict[str, JsonValue]) -> None:
    schema_version = observation.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != DEBUG_OBSERVATION_SCHEMA_VERSION
    ):
        raise DebugObservationError(
            f"observation.schema_version must be {DEBUG_OBSERVATION_SCHEMA_VERSION}"
        )
    _required_text(observation, "observation_kind", label="observation")
    _required_text(observation, "action", label="observation")
    _required_text(observation, "debug_session_id", label="observation")
    _required_text(observation, "state", label="observation")
    stop_id = _optional_stop_id(observation)
    if "exception" in observation:
        if stop_id is None:
            raise DebugObservationError("an exception observation requires stop_id")
        _validate_exception(observation["exception"])
    if "selected_frame" in observation:
        if stop_id is None:
            raise DebugObservationError("a selected_frame observation requires stop_id")
        _validate_frame(observation["selected_frame"])
    if "variables" in observation:
        if stop_id is None:
            raise DebugObservationError("a variables observation requires stop_id")
        _validate_variables(observation["variables"])
    if "reproduction" in observation:
        _validate_reproduction(observation["reproduction"])
    if "debuggee_exit" in observation:
        _validate_debuggee_exit(observation["debuggee_exit"])


def seal_debug_observation(observation: Mapping[str, object]) -> dict[str, JsonValue]:
    """Validate and content-seal one normalized debugger observation.

    The digest is SHA-256 over canonical JSON with ``observation_sha256``
    absent.  Supplying an already sealed object is rejected so callers cannot
    accidentally preserve a stale digest after mutating a field.
    """

    normalized = _json_object(observation, label="observation")
    if "observation_sha256" in normalized:
        raise DebugObservationError("seal_debug_observation requires an unsealed observation")
    _validate_observation_shape(normalized)
    digest = hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()
    normalized["observation_sha256"] = digest
    return normalized


def verify_debug_observation(observation: Mapping[str, object]) -> dict[str, JsonValue]:
    """Return a normalized sealed observation, failing closed on digest mismatch."""

    normalized = _json_object(observation, label="observation")
    provided = normalized.pop("observation_sha256", None)
    if not isinstance(provided, str) or _SHA256.fullmatch(provided) is None:
        raise DebugObservationError("observation_sha256 must be a lowercase SHA-256 digest")
    _validate_observation_shape(normalized)
    expected = hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()
    if not hmac.compare_digest(provided, expected):
        raise DebugObservationError("observation_sha256 mismatch")
    normalized["observation_sha256"] = provided
    return normalized


def _checkpoint_object(event: StoredRunEvent, key: str) -> Mapping[str, Any] | None:
    checkpoint = event.checkpoint
    if checkpoint is None:
        return None
    raw = checkpoint.get(key)
    if isinstance(raw, Mapping):
        return cast(Mapping[str, Any], raw)
    return None


def _event_tool_name(event: StoredRunEvent) -> str | None:
    candidates: list[str] = []
    public_name = event.data.get("tool_name")
    if isinstance(public_name, str):
        candidates.append(public_name)
    for key in ("call", "message"):
        payload = _checkpoint_object(event, key)
        if payload is not None and isinstance(payload.get("name"), str):
            candidates.append(cast(str, payload["name"]))
    unique = set(candidates)
    if len(unique) > 1:
        raise DebugLifecycleError(
            f"tool name mismatch at durable sequence {event.sequence}"
        )
    return candidates[0] if candidates else None


def _call_identity(event: StoredRunEvent, key: str) -> tuple[str, str]:
    payload = _checkpoint_object(event, key)
    if payload is None:
        raise DebugLifecycleError(
            f"{event.kind.value} sequence {event.sequence} has no private {key} payload"
        )
    call_id = payload.get("id" if key == "call" else "call_id")
    name = payload.get("name")
    if not isinstance(call_id, str) or not call_id:
        raise DebugLifecycleError(
            f"{event.kind.value} sequence {event.sequence} has no call id"
        )
    if not isinstance(name, str) or not name:
        raise DebugLifecycleError(
            f"{event.kind.value} sequence {event.sequence} has no tool name"
        )
    public_call_id = event.data.get("tool_call_id")
    if public_call_id is not None and public_call_id != call_id:
        raise DebugLifecycleError(
            f"public/private call id mismatch at durable sequence {event.sequence}"
        )
    public_name = event.data.get("tool_name")
    if public_name is not None and public_name != name:
        raise DebugLifecycleError(
            f"public/private tool name mismatch at durable sequence {event.sequence}"
        )
    return call_id, name


def _terminal_result(event: StoredRunEvent) -> _TerminalResult:
    message = _checkpoint_object(event, "message")
    if message is None:
        raise DebugLifecycleError(
            f"{event.kind.value} sequence {event.sequence} has no private ToolMessage"
        )
    call_id, tool_name = _call_identity(event, "message")
    if message.get("role") != "tool":
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} checkpoint is not a ToolMessage"
        )
    content = message.get("content")
    is_error = message.get("is_error")
    if not isinstance(content, str) or not isinstance(is_error, bool):
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} has malformed ToolMessage fields"
        )
    try:
        envelope = json.loads(content)
    except json.JSONDecodeError as exc:
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} has invalid JSON envelope"
        ) from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get("ok"), bool):
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} has malformed tool envelope"
        )
    checkpoint = event.checkpoint or {}
    if "tool_private" in checkpoint and not isinstance(
        checkpoint["tool_private"], Mapping
    ):
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} has malformed private tool evidence"
        )
    private_container = _checkpoint_object(event, "tool_private")
    private_raw = (
        private_container.get("debug_observation")
        if private_container is not None
        else None
    )
    if (
        private_container is not None
        and "debug_observation" in private_container
        and not isinstance(private_raw, Mapping)
    ):
        raise DebugObservationError(
            f"terminal sequence {event.sequence} has malformed private debug observation"
        )
    private_observation = (
        verify_debug_observation(cast(Mapping[str, object], private_raw))
        if isinstance(private_raw, Mapping)
        else None
    )
    if envelope["ok"] is False:
        if not is_error:
            raise DebugLifecycleError(
                f"terminal sequence {event.sequence} error envelope is not marked is_error"
            )
        if private_observation is not None:
            raise DebugLifecycleError(
                f"terminal sequence {event.sequence} has evidence for an error envelope"
            )
        return _TerminalResult("error", call_id, tool_name, None)
    if is_error:
        raise DebugLifecycleError(
            f"terminal sequence {event.sequence} success envelope is marked is_error"
        )
    raw_observation = envelope.get("data")
    projected_observation: dict[str, JsonValue] | None = None
    if isinstance(raw_observation, dict) and "observation_sha256" in raw_observation:
        projected_observation = verify_debug_observation(
            cast(Mapping[str, object], raw_observation)
        )
    if private_observation is None and projected_observation is None:
        raise DebugObservationError(
            f"terminal sequence {event.sequence} has no sealed debug observation"
        )
    if private_observation is not None and projected_observation is None:
        meta = envelope.get("meta")
        if not isinstance(meta, dict) or meta.get("truncated") is not True:
            raise DebugObservationError(
                f"terminal sequence {event.sequence} model projection omitted its observation"
            )
    if (
        private_observation is not None
        and projected_observation is not None
        and private_observation != projected_observation
    ):
        raise DebugObservationError(
            f"terminal sequence {event.sequence} private/projected observations differ"
        )
    observation = private_observation or projected_observation
    assert observation is not None  # narrowed by the fail-closed checks above
    outcome = "reused" if event.kind is RunEventKind.TOOL_REUSED else "completed"
    return _TerminalResult(outcome, call_id, tool_name, observation)


def _debug_lifecycles(events: Sequence[StoredRunEvent]) -> tuple[_DebugLifecycle, ...]:
    grouped: dict[str, list[StoredRunEvent]] = {}
    for event in events:
        if event.kind not in _LIFECYCLE_KINDS:
            continue
        tool_name = _event_tool_name(event)
        if tool_name is None or not tool_name.startswith(_DEBUG_TOOL_PREFIX):
            continue
        if event.call_key is None or not event.call_key:
            raise DebugLifecycleError(
                f"debug lifecycle sequence {event.sequence} has no call_key"
            )
        grouped.setdefault(event.call_key, []).append(event)

    lifecycles: list[_DebugLifecycle] = []
    for call_key, lifecycle_events in grouped.items():
        ordered = sorted(lifecycle_events, key=lambda item: item.sequence)
        planned = [item for item in ordered if item.kind is RunEventKind.TOOL_PLANNED]
        started = [item for item in ordered if item.kind is RunEventKind.TOOL_STARTED]
        terminal = [item for item in ordered if item.kind in _TERMINAL_KINDS]
        if len(planned) != 1:
            raise DebugLifecycleError(f"debug call {call_key!r} must have one planned event")
        if len(terminal) != 1:
            raise DebugLifecycleError(f"debug call {call_key!r} must have one terminal event")
        if len(started) > 1:
            raise DebugLifecycleError(f"debug call {call_key!r} has ambiguous started events")

        planned_call_id, planned_name = _call_identity(planned[0], "call")
        terminal_result = _terminal_result(terminal[0])
        if (terminal_result.call_id, terminal_result.tool_name) != (
            planned_call_id,
            planned_name,
        ):
            raise DebugLifecycleError(f"debug call {call_key!r} changed call identity")
        started_event = started[0] if started else None
        if started_event is not None:
            started_identity = _call_identity(started_event, "call")
            if started_identity != (planned_call_id, planned_name):
                raise DebugLifecycleError(f"debug call {call_key!r} changed call identity")

        if terminal[0].kind is RunEventKind.TOOL_REUSED and started_event is not None:
            raise DebugLifecycleError(f"reused debug call {call_key!r} must not be started")
        if (
            terminal[0].kind is RunEventKind.TOOL_COMPLETED
            and terminal_result.observation is not None
            and started_event is None
        ):
            raise DebugLifecycleError(
                f"successful debug call {call_key!r} must have a started event"
            )
        sequence_order = [planned[0].sequence]
        if started_event is not None:
            sequence_order.append(started_event.sequence)
        sequence_order.append(terminal[0].sequence)
        if sequence_order != sorted(sequence_order) or len(sequence_order) != len(
            set(sequence_order)
        ):
            raise DebugLifecycleError(f"debug call {call_key!r} lifecycle is out of order")

        lifecycles.append(
            _DebugLifecycle(
                call_key=call_key,
                tool_name=planned_name,
                planned=planned[0],
                started=started_event,
                terminal=terminal[0],
                result=terminal_result,
            )
        )
    return tuple(sorted(lifecycles, key=lambda item: item.planned.sequence))


def _matching_stop(
    record: _ObservationRecord,
    *,
    session_id: str,
    stop_id: int,
) -> bool:
    observation = record.observation
    return (
        observation.get("debug_session_id") == session_id
        and observation.get("stop_id") == stop_id
    )


def _primary_runtime_evidence(
    lifecycles: Sequence[_DebugLifecycle],
) -> tuple[
    _ObservationRecord,
    dict[str, JsonValue],
    _ObservationRecord,
    dict[str, JsonValue],
    _ObservationRecord,
    list[dict[str, JsonValue]],
]:
    records = [
        _ObservationRecord(lifecycle.terminal, lifecycle.result.observation)
        for lifecycle in lifecycles
        if lifecycle.result.observation is not None
    ]
    exception_records = [
        record for record in records if isinstance(record.observation.get("exception"), dict)
    ]
    if not exception_records:
        raise DebugEvidenceError("verified debug observations contain no exception evidence")
    exception_record = exception_records[0]
    exception = _validate_exception(
        exception_record.observation["exception"],
        require_location=True,
    )
    session_id = _required_text(
        exception_record.observation,
        "debug_session_id",
        label="observation",
    )
    stop_id = _optional_stop_id(exception_record.observation)
    if stop_id is None:  # already guaranteed by observation validation
        raise DebugEvidenceError("exception evidence has no stop_id")

    same_stop = [
        record
        for record in records
        if _matching_stop(record, session_id=session_id, stop_id=stop_id)
    ]
    frame_records = [
        record for record in same_stop if isinstance(record.observation.get("selected_frame"), dict)
    ]
    if not frame_records:
        raise DebugEvidenceError("exception stop contains no selected workspace frame")
    frame_record = frame_records[-1]
    frame = _validate_frame(
        frame_record.observation["selected_frame"],
        require_workspace=True,
    )

    variable_records = [
        record
        for record in same_stop
        if isinstance(record.observation.get("variables"), list)
        and record.observation.get("selected_frame") == frame
    ]
    if not variable_records:
        raise DebugEvidenceError("selected frame contains no captured variables")
    latest_scope_records: dict[str, _ObservationRecord] = {}
    for record in variable_records:
        captured = _validate_variables(record.observation["variables"])
        for scope in {cast(str, variable["scope"]) for variable in captured}:
            latest_scope_records[scope] = record
    selected_variable_records: list[_ObservationRecord] = []
    for record in latest_scope_records.values():
        if not any(existing is record for existing in selected_variable_records):
            selected_variable_records.append(record)
    selected_variable_records.sort(key=lambda record: record.event.sequence)
    variable_record = selected_variable_records[-1]
    variables: list[dict[str, JsonValue]] = []
    for record in selected_variable_records:
        captured = _validate_variables(record.observation["variables"])
        owned_scopes = {
            scope
            for scope, owner in latest_scope_records.items()
            if owner is record
        }
        variables.extend(
            variable
            for variable in captured
            if cast(str, variable["scope"]) in owned_scopes
        )
    if not variables:
        raise DebugEvidenceError("selected frame variable capture is empty")
    return (
        exception_record,
        exception,
        frame_record,
        frame,
        variable_record,
        variables,
    )


def _execution_evidence(
    lifecycles: Sequence[_DebugLifecycle],
    *,
    session_id: str,
) -> tuple[
    _ObservationRecord,
    dict[str, JsonValue],
    _ObservationRecord,
    dict[str, JsonValue],
]:
    records = [
        _ObservationRecord(lifecycle.terminal, lifecycle.result.observation)
        for lifecycle in lifecycles
        if lifecycle.result.observation is not None
        and lifecycle.result.observation.get("debug_session_id") == session_id
    ]
    reproduction_records = [
        record
        for record in records
        if isinstance(record.observation.get("reproduction"), dict)
    ]
    if not reproduction_records:
        raise DebugEvidenceError(
            "verified debug observations contain no normalized reproduction command"
        )
    reproduction_record = reproduction_records[0]
    reproduction = _validate_reproduction(reproduction_record.observation["reproduction"])

    exit_records = [
        record
        for record in records
        if isinstance(record.observation.get("debuggee_exit"), dict)
    ]
    if not exit_records:
        raise DebugEvidenceError(
            "verified debug observations contain no debuggee exit status"
        )
    exit_record = exit_records[-1]
    debuggee_exit = _validate_debuggee_exit(exit_record.observation["debuggee_exit"])
    return reproduction_record, reproduction, exit_record, debuggee_exit


def _timeline(lifecycles: Sequence[_DebugLifecycle]) -> list[dict[str, JsonValue]]:
    entries: list[dict[str, JsonValue]] = []
    for lifecycle in lifecycles:
        terminal_digest: JsonValue = None
        if lifecycle.result.observation is not None:
            terminal_digest = lifecycle.result.observation["observation_sha256"]
        event_outcomes: list[tuple[StoredRunEvent, str, JsonValue]] = [
            (lifecycle.planned, "planned", None)
        ]
        if lifecycle.started is not None:
            event_outcomes.append((lifecycle.started, "started", None))
        event_outcomes.append(
            (lifecycle.terminal, lifecycle.result.outcome, terminal_digest)
        )
        for event, outcome, digest in event_outcomes:
            entries.append(
                {
                    "sequence": event.sequence,
                    "event_id": event.event_id,
                    "call_key": lifecycle.call_key,
                    "tool_name": lifecycle.tool_name,
                    "action": lifecycle.action,
                    "outcome": outcome,
                    "occurred_at": event.occurred_at,
                    "event_hash": event.event_hash,
                    "observation_sha256": digest,
                }
            )
    return sorted(entries, key=lambda item: cast(int, item["sequence"]))


def _markdown_cell(value: object) -> str:
    text = str(value)
    return (
        html.escape(text, quote=True)
        .replace("|", "&#124;")
        .replace("\r", "&#13;")
        .replace("\n", "&#10;")
    )


def _root_cause_statement(
    failure: Mapping[str, JsonValue],
    frame: Mapping[str, JsonValue],
    variables: Sequence[Mapping[str, JsonValue]],
) -> str:
    rendered_variables = ", ".join(
        f"{variable['name']} = {variable['value']}" for variable in variables
    )
    return (
        f"Observed {failure['type']} at {failure['path']}:{failure['line']} in "
        f"{failure['function']}. Selected frame {frame['function']} at "
        f"{frame['path']}:{frame['line']} recorded {rendered_variables}."
    )


def _render_markdown(payload: Mapping[str, Any]) -> str:
    journal = cast(Mapping[str, Any], payload["journal"])
    failure = cast(Mapping[str, Any], payload["observed_failure"])
    frame = cast(Mapping[str, Any], payload["selected_frame"])
    variables = (
        *cast(Sequence[Mapping[str, Any]], payload["locals"]),
        *cast(Sequence[Mapping[str, Any]], payload["arguments"]),
    )
    root_cause = cast(Mapping[str, Any], payload["root_cause_evidence"])
    reproduction = cast(Mapping[str, Any], payload["reproduction"])
    debuggee_exit = cast(Mapping[str, Any], payload["debuggee_exit"])
    timeline = cast(Sequence[Mapping[str, Any]], payload["timeline"])
    limitations = cast(Sequence[str], payload["limitations"])

    lines = [
        "# Runtime Debugging PR Evidence",
        "",
        "Generated deterministically from journal events whose sequence/hash/digest "
        "consistency was verified; no model summarization was used.",
        "",
        f"- Run ID: `{_markdown_cell(payload['run_id'])}`",
        f"- Journal head sequence: `{_markdown_cell(journal['head_sequence'])}`",
        f"- Journal head hash: `{_markdown_cell(journal['head_hash'])}`",
        f"- Evidence payload SHA-256: `{_markdown_cell(payload['evidence_payload_sha256'])}`",
        "- Report-generation model calls: `0`",
        "- Replay debugger calls: `0`",
        "",
        "## Reproduction",
        "",
        (
            "- Command argv: `"
            + _markdown_cell(
                json.dumps(
                    reproduction["command"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            + "`"
        ),
        f"- Working directory: `{_markdown_cell(reproduction['cwd'])}`",
        "",
        "## Root-cause evidence (recorded facts)",
        "",
        _markdown_cell(root_cause["statement"]),
        "",
        "### Observed failure",
        "",
        (
            f"Observed `{_markdown_cell(failure['type'])}` at "
            f"`{_markdown_cell(failure['path'])}:{_markdown_cell(failure['line'])}` in "
            f"`{_markdown_cell(failure['function'])}`: "
            f"{_markdown_cell(failure['message'])}."
        ),
        "",
        "## Selected suspicious frame",
        "",
        (
            f"Deterministic rule `{_markdown_cell(payload['selection_rule_version'])}` selected "
            f"frame index `{_markdown_cell(frame['frame_index'])}`: "
            f"`{_markdown_cell(frame['path'])}:{_markdown_cell(frame['line'])}` in "
            f"`{_markdown_cell(frame['function'])}`."
        ),
        (
            "Selection reasons: `"
            + _markdown_cell(", ".join(frame["selection_reasons"]))
            + "`."
        ),
        "",
        "## Captured frame variables",
        "",
        "| Scope | Name | Type | Value | Flags |",
        "| --- | --- | --- | --- | --- |",
    ]
    for variable in variables:
        flags = []
        if variable["redacted"]:
            flags.append("redacted")
        if variable["truncated"]:
            flags.append("truncated")
        lines.append(
            "| "
            + " | ".join(
                (
                    _markdown_cell(variable["scope"]),
                    _markdown_cell(variable["name"]),
                    _markdown_cell(variable["type"]),
                    _markdown_cell(variable["value"]),
                    _markdown_cell(", ".join(flags) if flags else "none"),
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Durable evidence timeline",
            "",
            "| Seq | Event ID | Call key | Action | Outcome | Observation SHA-256 | Event hash |",
            "| ---: | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for entry in timeline:
        lines.append(
            "| "
            + " | ".join(
                (
                    _markdown_cell(entry["sequence"]),
                    _markdown_cell(entry["event_id"]),
                    _markdown_cell(entry["call_key"]),
                    _markdown_cell(entry["action"]),
                    _markdown_cell(entry["outcome"]),
                    _markdown_cell(entry["observation_sha256"] or "—"),
                    _markdown_cell(entry["event_hash"]),
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Debuggee termination",
            "",
            f"- Status: `{_markdown_cell(debuggee_exit['status'])}`",
            f"- Exit code: `{_markdown_cell(debuggee_exit['exit_code'])}`",
            f"- Signal: `{_markdown_cell(debuggee_exit['signal'])}`",
            "",
            "## Evidence limitations",
            "",
        ]
    )
    lines.extend(f"- {_markdown_cell(limitation)}" for limitation in limitations)
    lines.append("")
    return "\n".join(lines)


def generate_debug_evidence(events: Sequence[StoredRunEvent]) -> DebugEvidenceArtifacts:
    """Generate deterministic JSON and Markdown from a complete journal chain.

    Chain verification is intentionally the first operation.  Replay then uses
    only immutable event fields and content-sealed private ToolMessage payloads.
    """

    resolved_events = tuple(events)
    verify_event_chain(resolved_events)
    if not resolved_events:
        raise DebugEvidenceError("a complete non-empty event chain is required")

    lifecycles = _debug_lifecycles(resolved_events)
    if not lifecycles:
        raise DebugEvidenceError("journal contains no completed python_debug_* calls")
    (
        exception_record,
        exception,
        frame_record,
        frame,
        variable_record,
        variables,
    ) = _primary_runtime_evidence(lifecycles)
    evidence_session_id = _required_text(
        exception_record.observation,
        "debug_session_id",
        label="observation",
    )
    evidence_lifecycles = tuple(
        lifecycle
        for lifecycle in lifecycles
        if lifecycle.result.observation is not None
        and lifecycle.result.observation.get("debug_session_id") == evidence_session_id
    )
    (
        reproduction_record,
        reproduction,
        exit_record,
        debuggee_exit,
    ) = _execution_evidence(evidence_lifecycles, session_id=evidence_session_id)
    ordered_variables = sorted(
        variables,
        key=lambda item: (
            cast(str, item["scope"]),
            cast(str, item["name"]),
            cast(str, item["type"]),
            cast(str, item["value"]),
        ),
    )
    limitations = [
        "This report states captured runtime facts only; it does not infer why the values arose.",
        (
            "The unkeyed SHA-256 checks establish internal consistency, not provenance or "
            "authenticity; without a trusted external head, a writer able to rewrite the "
            "complete chain can recompute them."
        ),
    ]
    if any(cast(bool, variable["redacted"]) for variable in ordered_variables):
        limitations.append("One or more captured values were redacted before persistence.")
    if any(cast(bool, variable["truncated"]) for variable in ordered_variables):
        limitations.append("One or more captured values were truncated before persistence.")
    local_variables = [
        variable for variable in ordered_variables if variable["scope"] == "locals"
    ]
    argument_variables = [
        variable for variable in ordered_variables if variable["scope"] == "arguments"
    ]

    head = resolved_events[-1]
    base_payload: dict[str, Any] = {
        "schema_version": DEBUG_EVIDENCE_SCHEMA_VERSION,
        "generator_revision": DEBUG_EVIDENCE_GENERATOR_REVISION,
        "run_id": resolved_events[0].run_id,
        "journal": {
            "head_sequence": head.sequence,
            "head_event_id": head.event_id,
            "head_hash": head.event_hash,
        },
        "observed_failure": exception,
        "selected_frame": frame,
        "reproduction": reproduction,
        "debuggee_exit": debuggee_exit,
        "locals": local_variables,
        "arguments": argument_variables,
        "root_cause_evidence": {
            "statement": _root_cause_statement(exception, frame, ordered_variables),
            "fact_only": True,
        },
        "source_sequences": {
            "exception": exception_record.event.sequence,
            "selected_frame": frame_record.event.sequence,
            "variables": variable_record.event.sequence,
            "reproduction": reproduction_record.event.sequence,
            "debuggee_exit": exit_record.event.sequence,
        },
        "selection_rule_version": frame["selection_rule_version"],
        "selection_reasons": frame["selection_reasons"],
        "timeline": _timeline(evidence_lifecycles),
        "limitations": limitations,
        "generation": {"model_calls": 0, "debugger_calls": 0},
    }
    evidence_digest = hashlib.sha256(
        canonical_json(base_payload).encode("utf-8")
    ).hexdigest()
    payload = {**base_payload, "evidence_payload_sha256": evidence_digest}
    return DebugEvidenceArtifacts(
        canonical_json=canonical_json(payload),
        markdown=_render_markdown(payload),
        evidence_payload_sha256=evidence_digest,
    )
