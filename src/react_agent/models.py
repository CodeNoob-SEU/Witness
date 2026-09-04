"""Provider-neutral data models for the agent loop."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, TypeAlias, cast

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None


@dataclass(frozen=True, slots=True)
class Usage:
    """Normalized token usage; providers may leave every field at zero."""

    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None
    billable_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in ("input_tokens", "output_tokens", "total_tokens"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "cached_input_tokens",
            "reasoning_output_tokens",
            "billable_tokens",
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None")
        if self.cached_input_tokens is not None and self.cached_input_tokens > self.input_tokens:
            raise ValueError("cached_input_tokens cannot exceed input_tokens")
        if (
            self.reasoning_output_tokens is not None
            and self.reasoning_output_tokens > self.output_tokens
        ):
            raise ValueError("reasoning_output_tokens cannot exceed output_tokens")

    @staticmethod
    def _add_optional(left: int | None, right: int | None) -> int | None:
        if left is None:
            return right
        if right is None:
            return left
        return left + right

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
            cached_input_tokens=self._add_optional(
                self.cached_input_tokens, other.cached_input_tokens
            ),
            reasoning_output_tokens=self._add_optional(
                self.reasoning_output_tokens, other.reasoning_output_tokens
            ),
            billable_tokens=self._add_optional(self.billable_tokens, other.billable_tokens),
        )


@dataclass(frozen=True, slots=True)
class ToolCall:
    """A structured action requested by the model."""

    id: str
    name: str
    arguments: str = field(repr=False)


def tool_action_fingerprint(call: ToolCall) -> str:
    """Return a stable, non-reversible key for one logical tool action."""

    try:
        parsed = json.loads(call.arguments)
        canonical = json.dumps(
            parsed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    except (json.JSONDecodeError, TypeError):
        canonical = call.arguments
    return hashlib.sha256(f"{call.name}\0{canonical}".encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class UserMessage:
    content: str = field(repr=False)
    role: Literal["user"] = field(default="user", init=False)


@dataclass(frozen=True, slots=True)
class AssistantMessage:
    content: str | None = field(default=None, repr=False)
    tool_calls: tuple[ToolCall, ...] = ()
    # Responses API reasoning/function items must be replayed in stateless mode.
    # Keep them opaque to the core and out of repr/logs.
    raw_items: tuple[Mapping[str, Any], ...] = field(default=(), repr=False, compare=False)
    role: Literal["assistant"] = field(default="assistant", init=False)


@dataclass(frozen=True, slots=True)
class ToolMessage:
    call_id: str
    name: str
    content: str = field(repr=False)
    is_error: bool = False
    executed: bool = False
    cached: bool = False
    duration_ms: float = 0.0
    # The model projection and durable private evidence have different
    # consumers. This payload is never serialized into the transcript; the
    # Agent journal stores it beside ``message`` instead.
    private_payload: Mapping[str, JsonValue] = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )
    role: Literal["tool"] = field(default="tool", init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "private_payload",
            MappingProxyType(dict(self.private_payload)),
        )


TranscriptItem: TypeAlias = UserMessage | AssistantMessage | ToolMessage


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    parameters: Mapping[str, Any]
    strict: bool = True


@dataclass(frozen=True, slots=True)
class ModelRequest:
    transcript: tuple[TranscriptItem, ...]
    tools: tuple[ToolSpec, ...]
    instructions: str
    parallel_tool_calls: bool = True


class ModelOutcome(StrEnum):
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class ModelResponse:
    message: AssistantMessage
    usage: Usage = Usage()
    request_id: str | None = None
    response_model: str | None = None
    finish_reason: str | None = None
    outcome: ModelOutcome = ModelOutcome.COMPLETED
    diagnostic: str | None = None


class ModelStreamEventKind(StrEnum):
    """Provider-neutral model deltas safe to expose to a live UI."""

    TEXT_DELTA = "text_delta"
    REFUSAL_DELTA = "refusal_delta"
    TOOL_CALL_DELTA = "tool_call_delta"


@dataclass(frozen=True, slots=True)
class ModelStreamEvent:
    """One sanitized model delta.

    ``tool_index`` is the stable identifier available on every tool-call delta.
    Providers may reveal the call id or function name only after streaming starts,
    so those two fields are optional.
    """

    kind: ModelStreamEventKind
    delta: str = field(repr=False)
    tool_index: int | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None


ModelStreamSink: TypeAlias = Callable[[ModelStreamEvent], Awaitable[None] | None]


class EventKind(StrEnum):
    RUN_STARTED = "run_started"
    CONTEXT_GOVERNED = "context_governed"
    MODEL_STARTED = "model_started"
    MODEL_COMPLETED = "model_completed"
    MODEL_FAILED = "model_failed"
    TOOL_STARTED = "tool_started"
    TOOL_COMPLETED = "tool_completed"
    TOOL_REUSED = "tool_reused"
    BUDGET_EXHAUSTED = "budget_exhausted"
    LOOP_DETECTED = "loop_detected"
    RUN_COMPLETED = "run_completed"


@dataclass(frozen=True, slots=True)
class AgentEvent:
    """Safe-by-default trace event: raw prompts, arguments and outputs are excluded."""

    kind: EventKind
    run_id: str
    timestamp: float
    step: int | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    data: Mapping[str, JsonValue] = field(default_factory=lambda: MappingProxyType({}))


def agent_event_to_json(event: AgentEvent) -> dict[str, JsonValue]:
    """Encode the safe Agent event projection for durable result recovery."""

    return {
        "kind": event.kind.value,
        "run_id": event.run_id,
        "timestamp": event.timestamp,
        "step": event.step,
        "tool_call_id": event.tool_call_id,
        "tool_name": event.tool_name,
        "data": dict(event.data),
    }


def agent_event_from_json(value: Mapping[str, Any]) -> AgentEvent:
    """Decode one persisted safe Agent event, rejecting malformed facts."""

    raw_kind = value.get("kind")
    run_id = value.get("run_id")
    timestamp = value.get("timestamp")
    step = value.get("step")
    tool_call_id = value.get("tool_call_id")
    tool_name = value.get("tool_name")
    data = value.get("data", {})
    if not isinstance(raw_kind, str):
        raise ValueError("Journal Agent event kind must be a string.")
    try:
        kind = EventKind(raw_kind)
    except ValueError:
        raise ValueError(f"Unsupported journal Agent event kind: {raw_kind!r}.") from None
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("Journal Agent event run_id must be a non-empty string.")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, (int, float))
        or not math.isfinite(float(timestamp))
    ):
        raise ValueError("Journal Agent event timestamp must be finite.")
    if step is not None and (
        isinstance(step, bool) or not isinstance(step, int) or step < 0
    ):
        raise ValueError("Journal Agent event step must be a non-negative integer or null.")
    for name, item in (("tool_call_id", tool_call_id), ("tool_name", tool_name)):
        if item is not None and not isinstance(item, str):
            raise ValueError(f"Journal Agent event {name} must be a string or null.")
    if not isinstance(data, Mapping) or not all(isinstance(key, str) for key in data):
        raise ValueError("Journal Agent event data must be an object with string keys.")
    return AgentEvent(
        kind=kind,
        run_id=run_id,
        timestamp=float(timestamp),
        step=step,
        tool_call_id=tool_call_id,
        tool_name=tool_name,
        data=MappingProxyType(dict(cast(Mapping[str, JsonValue], data))),
    )


def agent_events_from_json(values: Sequence[Mapping[str, Any]]) -> tuple[AgentEvent, ...]:
    """Decode the exact safe event tuple captured in an ``AgentResult``."""

    return tuple(agent_event_from_json(value) for value in values)


class AgentStreamEventKind(StrEnum):
    """Ephemeral, opt-in events for a live Agent debugging workbench."""

    RUN_STARTED = "run_started"
    CONTEXT_GOVERNED = "context_governed"
    MODEL_STARTED = "model_started"
    MODEL_TEXT_DELTA = "model_text_delta"
    MODEL_REFUSAL_DELTA = "model_refusal_delta"
    MODEL_TOOL_CALL_DELTA = "model_tool_call_delta"
    MODEL_TOOL_CALL_READY = "model_tool_call_ready"
    MODEL_COMPLETED = "model_completed"
    MODEL_FAILED = "model_failed"
    TOOL_STARTED = "tool_started"
    TOOL_RESULT = "tool_result"
    TOOL_REUSED = "tool_reused"
    BUDGET_EXHAUSTED = "budget_exhausted"
    LOOP_DETECTED = "loop_detected"
    RUN_COMPLETED = "run_completed"


@dataclass(frozen=True, slots=True)
class AgentStreamEvent:
    """One ordered event delivered only to an explicitly configured stream sink."""

    kind: AgentStreamEventKind
    run_id: str
    sequence: int
    timestamp: float
    step: int | None = None
    call_key: str | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    data: Mapping[str, JsonValue] = field(default_factory=lambda: MappingProxyType({}), repr=False)


class AgentJournalEventKind(StrEnum):
    """Durable orchestration facts emitted before/after external operations.

    Unlike :class:`AgentEvent`, delivery failures for these records are fatal to
    the run.  Unlike :class:`AgentStreamEvent`, they never contain provider
    stream fragments and are suitable for an append-only journal.
    """

    RUN_STARTED = "run.started"
    RUN_RESUMED = "run.resumed"
    CONTEXT_GOVERNED = "context.governed"
    MODEL_STARTED = "model.started"
    MODEL_COMPLETED = "model.completed"
    MODEL_FAILED = "model.failed"
    TOOL_PLANNED = "tool.planned"
    TOOL_STARTED = "tool.started"
    TOOL_COMPLETED = "tool.completed"
    TOOL_REUSED = "tool.reused"
    BUDGET_EXHAUSTED = "budget.exhausted"
    LOOP_DETECTED = "loop.detected"
    RUN_COMPLETED = "run.completed"


@dataclass(frozen=True, slots=True)
class AgentJournalEvent:
    """One unsequenced fact handed to the durable journal seam.

    The journal assigns the authoritative sequence and hash. ``private_data``
    is deliberately separate so public projections and telemetry never need to
    inspect recovery content.
    """

    kind: AgentJournalEventKind
    run_id: str
    execution_id: str
    operation_id: str
    timestamp: float
    step: int | None = None
    call_key: str | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    public_data: Mapping[str, JsonValue] = field(default_factory=lambda: MappingProxyType({}))
    private_data: Mapping[str, JsonValue] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )


class RunStatus(StrEnum):
    COMPLETED = "completed"
    PARTIAL = "partial"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class StopReason(StrEnum):
    COMPLETED = "completed"
    MAX_STEPS = "max_steps"
    MAX_TOOL_CALLS = "max_tool_calls"
    CONTEXT_LIMIT = "context_limit"
    WALL_TIME = "wall_time"
    LOOP_DETECTED = "loop_detected"
    MODEL_ERROR = "model_error"
    MODEL_INCOMPLETE = "model_incomplete"
    MODEL_REFUSAL = "model_refusal"
    PROTOCOL_ERROR = "protocol_error"


@dataclass(frozen=True, slots=True)
class AgentResult:
    output: str | None = field(repr=False)
    status: RunStatus
    stop_reason: StopReason
    run_id: str
    model_calls: int
    tool_calls: int
    tool_executions: int
    usage: Usage
    transcript: tuple[TranscriptItem, ...] = field(repr=False)
    events: tuple[AgentEvent, ...] = field(repr=False)
    error: str | None = None
    context_metrics: Mapping[str, JsonValue] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )


@dataclass(frozen=True, slots=True)
class RecoveredToolCall:
    """One tool call reconstructed from durable facts for conservative resume."""

    call: ToolCall
    call_key: str
    attempts: int = 0
    completed: ToolMessage | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class AgentResumeState:
    """Provider-neutral loop state used only by the durable Runtime.

    It intentionally contains final messages, never raw provider deltas or
    reasoning items.  Callers that only use :meth:`ReActAgent.run` do not need
    to learn this interface.
    """

    transcript: tuple[TranscriptItem, ...] = field(repr=False)
    next_step: int
    model_calls: int = 0
    tool_calls: int = 0
    tool_executions: int = 0
    usage: Usage = Usage()
    pending_tools: tuple[RecoveredToolCall, ...] = field(default=(), repr=False)
    completed_tools: tuple[RecoveredToolCall, ...] = field(default=(), repr=False)
    action_counts: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )

    def __post_init__(self) -> None:
        if self.next_step < 1:
            raise ValueError("next_step must be positive")
        if min(self.model_calls, self.tool_calls, self.tool_executions) < 0:
            raise ValueError("resume counters must be non-negative")
        if any(item.attempts < 0 for item in (*self.pending_tools, *self.completed_tools)):
            raise ValueError("recovered tool attempts must be non-negative")
        object.__setattr__(self, "transcript", tuple(self.transcript))
        object.__setattr__(self, "pending_tools", tuple(self.pending_tools))
        object.__setattr__(self, "completed_tools", tuple(self.completed_tools))
        object.__setattr__(
            self,
            "action_counts",
            MappingProxyType(dict(self.action_counts)),
        )


def transcript_item_to_json(item: TranscriptItem) -> dict[str, JsonValue]:
    """Encode a provider-neutral transcript item for a private journal payload."""

    if isinstance(item, UserMessage):
        return {"role": "user", "content": item.content}
    if isinstance(item, ToolMessage):
        return {
            "role": "tool",
            "call_id": item.call_id,
            "name": item.name,
            "content": item.content,
            "is_error": item.is_error,
            "executed": item.executed,
            "cached": item.cached,
            "duration_ms": item.duration_ms,
        }
    return {
        "role": "assistant",
        "content": item.content,
        "tool_calls": [
            {"id": call.id, "name": call.name, "arguments": call.arguments}
            for call in item.tool_calls
        ],
        # Opaque provider items can contain encrypted reasoning or future
        # provider-specific fields.  They are deliberately excluded from the
        # durable journal; provider-neutral content and tool calls are enough
        # to continue a recovered conversation safely.
        "raw_items": [],
    }


def transcript_item_from_json(value: Mapping[str, Any]) -> TranscriptItem:
    """Decode a journal transcript item, rejecting malformed role payloads."""

    role = value.get("role")
    if role == "user":
        content = value.get("content")
        if not isinstance(content, str):
            raise ValueError("Journal user message content must be a string.")
        return UserMessage(content)
    if role == "tool":
        call_id = value.get("call_id")
        name = value.get("name")
        content = value.get("content")
        if (
            not isinstance(call_id, str)
            or not isinstance(name, str)
            or not isinstance(content, str)
        ):
            raise ValueError("Journal tool message fields must be strings.")
        return ToolMessage(
            call_id=call_id,
            name=name,
            content=content,
            is_error=bool(value.get("is_error", False)),
            executed=bool(value.get("executed", False)),
            cached=bool(value.get("cached", False)),
            duration_ms=float(value.get("duration_ms", 0.0)),
        )
    if role == "assistant":
        raw_calls = value.get("tool_calls", [])
        if not isinstance(raw_calls, (list, tuple)):
            raise ValueError("Journal assistant tool_calls must be an array.")
        calls: list[ToolCall] = []
        for raw_call in raw_calls:
            if not isinstance(raw_call, Mapping):
                raise ValueError("Journal tool call must be an object.")
            call_id = raw_call.get("id")
            name = raw_call.get("name")
            arguments = raw_call.get("arguments")
            if (
                not isinstance(call_id, str)
                or not isinstance(name, str)
                or not isinstance(arguments, str)
            ):
                raise ValueError("Journal tool call fields must be strings.")
            calls.append(ToolCall(call_id, name, arguments))
        raw_items_value = value.get("raw_items", [])
        if not isinstance(raw_items_value, (list, tuple)) or not all(
            isinstance(item, Mapping) for item in raw_items_value
        ):
            raise ValueError("Journal assistant raw_items must be an array of objects.")
        content_value = value.get("content")
        if content_value is not None and not isinstance(content_value, str):
            raise ValueError("Journal assistant content must be a string or null.")
        return AssistantMessage(
            content=content_value,
            tool_calls=tuple(calls),
            raw_items=tuple(cast(Mapping[str, Any], item) for item in raw_items_value),
        )
    raise ValueError(f"Unsupported journal transcript role: {role!r}.")


def transcript_to_json(items: Sequence[TranscriptItem]) -> list[JsonValue]:
    return [transcript_item_to_json(item) for item in items]


def transcript_from_json(items: Sequence[Mapping[str, Any]]) -> tuple[TranscriptItem, ...]:
    return tuple(transcript_item_from_json(item) for item in items)
