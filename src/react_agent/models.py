"""Provider-neutral data models for the agent loop."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, TypeAlias

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None


@dataclass(frozen=True, slots=True)
class Usage:
    """Normalized token usage; providers may leave every field at zero."""

    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
        )


@dataclass(frozen=True, slots=True)
class ToolCall:
    """A structured action requested by the model."""

    id: str
    name: str
    arguments: str = field(repr=False)


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
    role: Literal["tool"] = field(default="tool", init=False)


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


class AgentStreamEventKind(StrEnum):
    """Ephemeral, opt-in events for a live Agent debugging workbench."""

    RUN_STARTED = "run_started"
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
