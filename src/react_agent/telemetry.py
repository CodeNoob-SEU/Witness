"""Safe, optional telemetry adapters for Agent lifecycle events.

The module deliberately accepts a small allowlisted event vocabulary instead
of arbitrary Agent payloads.  OpenTelemetry is loaded dynamically: importing
this package never requires the OpenTelemetry distribution.
"""

from __future__ import annotations

import contextlib
import importlib
import math
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias, runtime_checkable

TelemetryValue: TypeAlias = str | bool | int | float

# GenAI semantic conventions are still Development. Keep the exact contract
# reviewed for this Runtime behind one version label and one golden name table
# so a dependency upgrade cannot silently rename emitted instruments.
GENAI_SEMCONV_REVISION = "development-snapshot-2026-08-20"
GENAI_METRIC_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "model_duration": "gen_ai.client.operation.duration",
        "token_usage": "gen_ai.client.token.usage",
        "agent_duration": "gen_ai.invoke_agent.operation.duration",
        "agent_count": "gen_ai.invoke_agent.operation.count",
        "tool_duration": "gen_ai.execute_tool.operation.duration",
        "tool_count": "gen_ai.execute_tool.operation.count",
    }
)


class TelemetryMode(StrEnum):
    LIVE = "live"
    REPLAY = "replay"


class TelemetryEventKind(StrEnum):
    RUN_STARTED = "run_started"
    RUN_RESUMED = "run_resumed"
    RUN_COMPLETED = "run_completed"
    MODEL_STARTED = "model_started"
    MODEL_COMPLETED = "model_completed"
    MODEL_FAILED = "model_failed"
    TOOL_STARTED = "tool_started"
    TOOL_COMPLETED = "tool_completed"
    TOOL_REUSED = "tool_reused"
    BUDGET_EXHAUSTED = "budget_exhausted"
    LOOP_DETECTED = "loop_detected"


@dataclass(frozen=True, slots=True)
class TraceReference:
    """Content-free durable projection of one execution root span."""

    run_id: str
    execution_id: str
    trace_id: str
    span_id: str
    trace_flags: int = 0

    def __post_init__(self) -> None:
        if not self.run_id.strip() or not self.execution_id.strip():
            raise ValueError("trace reference identities must not be blank")
        for name, value, width in (
            ("trace_id", self.trace_id, 32),
            ("span_id", self.span_id, 16),
        ):
            if (
                len(value) != width
                or value != value.casefold()
                or any(character not in "0123456789abcdef" for character in value)
                or int(value, 16) == 0
            ):
                raise ValueError(f"{name} must be a non-zero lowercase {width}-digit hex id")
        # Persist only the W3C sampled bit. Vendor flags and trace state are
        # intentionally excluded from this content-free recovery projection.
        if isinstance(self.trace_flags, bool) or self.trace_flags not in {0, 1}:
            raise ValueError("trace_flags may contain only the sampled bit")


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    kind: TelemetryEventKind
    attributes: Mapping[str, TelemetryValue] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )
    timestamp_ns: int = field(default_factory=time.time_ns)
    mode: TelemetryMode = TelemetryMode.LIVE
    links: tuple[TraceReference, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        if self.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be non-negative")
        object.__setattr__(self, "links", tuple(self.links))
        if not all(isinstance(link, TraceReference) for link in self.links):
            raise TypeError("telemetry links must be TraceReference values")


@runtime_checkable
class Telemetry(Protocol):
    """One-event interface shared by production and test adapters."""

    def emit(self, event: TelemetryEvent) -> TraceReference | None:
        """Observe one lifecycle event and optionally expose its root context."""
        ...


_COMMON_ATTRIBUTES = frozenset(
    {
        "run_id",
        "execution_id",
        "previous_execution_id",
        "sequence",
        "operation_id",
        "agent_name",
        "step",
        "attempt",
    }
)
_RUN_ATTRIBUTES = frozenset(
    {
        "status",
        "stop_reason",
        "duration_s",
        "model_calls",
        "tool_calls",
        "tool_executions",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
        "billable_tokens",
        "resume_reason",
    }
)
_MODEL_ATTRIBUTES = frozenset(
    {
        "provider",
        "request_model",
        "response_model",
        "request_id",
        "finish_reason",
        "outcome",
        "error_type",
        "duration_s",
        "ttfc_s",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
        "billable_tokens",
        "cost_micros",
        "currency",
    }
)
_TOOL_ATTRIBUTES = frozenset(
    {
        "tool_name",
        "tool_type",
        "tool_call_id",
        "call_key",
        "outcome",
        "error_type",
        "duration_s",
        "cached",
        "executed",
    }
)
_POLICY_ATTRIBUTES = frozenset({"reason", "repeat_count", "repeat_limit", "context_chars"})
_FORBIDDEN_FRAGMENTS = frozenset(
    {
        "api_key",
        "argument",
        "authorization",
        "content",
        "cookie",
        "delta",
        "exception_message",
        "header",
        "password",
        "prompt",
        "raw",
        "reasoning",
        "result",
        "secret",
        "token_value",
    }
)
_SAFE_REASONING_METADATA_ATTRIBUTES = frozenset({"reasoning_output_tokens"})


def _allowed_attributes(kind: TelemetryEventKind) -> frozenset[str]:
    if kind in {
        TelemetryEventKind.RUN_STARTED,
        TelemetryEventKind.RUN_RESUMED,
        TelemetryEventKind.RUN_COMPLETED,
    }:
        return _COMMON_ATTRIBUTES | _RUN_ATTRIBUTES
    if kind in {
        TelemetryEventKind.MODEL_STARTED,
        TelemetryEventKind.MODEL_COMPLETED,
        TelemetryEventKind.MODEL_FAILED,
    }:
        return _COMMON_ATTRIBUTES | _MODEL_ATTRIBUTES
    if kind in {
        TelemetryEventKind.TOOL_STARTED,
        TelemetryEventKind.TOOL_COMPLETED,
        TelemetryEventKind.TOOL_REUSED,
    }:
        return _COMMON_ATTRIBUTES | _TOOL_ATTRIBUTES
    return _COMMON_ATTRIBUTES | _POLICY_ATTRIBUTES


def sanitize_attributes(
    kind: TelemetryEventKind, attributes: Mapping[str, TelemetryValue]
) -> Mapping[str, TelemetryValue]:
    """Return a bounded allowlisted copy suitable for spans and logs."""

    allowed = _allowed_attributes(kind)
    result: dict[str, TelemetryValue] = {}
    for key, value in attributes.items():
        lowered = key.casefold()
        contains_forbidden_fragment = any(
            fragment in lowered for fragment in _FORBIDDEN_FRAGMENTS
        )
        if key not in allowed or (
            contains_forbidden_fragment
            and key not in _SAFE_REASONING_METADATA_ATTRIBUTES
        ):
            continue
        if isinstance(value, str):
            result[key] = value[:256]
        elif isinstance(value, bool) or isinstance(value, int):
            result[key] = value
        elif isinstance(value, float) and math.isfinite(value):
            result[key] = value
    return MappingProxyType(result)


class NoOpTelemetry:
    """Adapter used when telemetry is disabled or its dependency is absent."""

    def emit(self, event: TelemetryEvent) -> TraceReference | None:
        del event
        return None


class RecordingTelemetry:
    """Thread-safe test adapter that records sanitized live events."""

    def __init__(self, *, suppress_replay: bool = True) -> None:
        self._suppress_replay = suppress_replay
        self._events: list[TelemetryEvent] = []
        self._lock = threading.Lock()

    @property
    def events(self) -> tuple[TelemetryEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def emit(self, event: TelemetryEvent) -> TraceReference | None:
        if self._suppress_replay and event.mode is TelemetryMode.REPLAY:
            return None
        recorded = TelemetryEvent(
            event.kind,
            sanitize_attributes(event.kind, event.attributes),
            event.timestamp_ns,
            event.mode,
            event.links,
        )
        with self._lock:
            self._events.append(recorded)
        return None


@dataclass(frozen=True, slots=True)
class MetricCardinalityPolicy:
    """Explicit bounded dimensions for model, tool, and provider metrics."""

    allowed_providers: frozenset[str] = frozenset({"openai", "azure_openai", "openai_compatible"})
    allowed_models: frozenset[str] = frozenset()
    allowed_tools: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_providers", frozenset(self.allowed_providers))
        object.__setattr__(self, "allowed_models", frozenset(self.allowed_models))
        object.__setattr__(self, "allowed_tools", frozenset(self.allowed_tools))


@dataclass(slots=True)
class _OpenSpan:
    span: Any


@dataclass(frozen=True, slots=True)
class _ExecutionReference:
    execution_id: str
    span_key: tuple[str, str, str, str]
    span_context: Any | None = field(default=None, repr=False)
    trace_reference: TraceReference | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class _LinkedSpanContext:
    """Fallback context shape for test/compatible tracer implementations."""

    trace_id: int
    span_id: int
    trace_flags: int
    is_remote: bool = True
    is_valid: bool = True
    trace_state: Any | None = None


class OTelTelemetry:
    """OpenTelemetry adapter; becomes a NoOp when the dependency is unavailable."""

    _INSTRUMENTATION_SCOPE = "react_agent.telemetry"

    def __init__(
        self,
        *,
        tracer: Any | None = None,
        meter: Any | None = None,
        logger: Any | None = None,
        cardinality: MetricCardinalityPolicy | None = None,
    ) -> None:
        self.cardinality = cardinality or MetricCardinalityPolicy()
        self._trace_module: Any | None = None
        self._context_module: Any | None = None
        self._logs_module: Any | None = None
        if tracer is None or meter is None:
            try:
                self._trace_module = importlib.import_module("opentelemetry.trace")
                self._context_module = importlib.import_module("opentelemetry.context")
                metrics_module = importlib.import_module("opentelemetry.metrics")
            except ImportError:
                metrics_module = None
            if tracer is None and self._trace_module is not None:
                tracer = self._trace_module.get_tracer(self._INSTRUMENTATION_SCOPE)
            if meter is None and metrics_module is not None:
                meter = metrics_module.get_meter(self._INSTRUMENTATION_SCOPE)
        else:
            try:
                self._trace_module = importlib.import_module("opentelemetry.trace")
                self._context_module = importlib.import_module("opentelemetry.context")
            except ImportError:
                pass
        if logger is None:
            try:
                self._logs_module = importlib.import_module("opentelemetry._logs")
                logger = self._logs_module.get_logger(self._INSTRUMENTATION_SCOPE)
            except ImportError:
                pass

        self._tracer = tracer
        self._meter = meter
        self._logger = logger
        self.available = tracer is not None and meter is not None
        self._lock = threading.RLock()
        self._spans: dict[tuple[str, str, str, str], _OpenSpan] = {}
        self._execution_references: dict[str, _ExecutionReference] = {}

        self._model_duration = self._create_instrument(
            "create_histogram",
            GENAI_METRIC_NAMES["model_duration"],
            unit="s",
            description="Duration of model operations.",
        )
        self._model_ttfc = self._create_instrument(
            "create_histogram",
            "react_agent.gen_ai.client.time_to_first_content",
            unit="s",
            description="Time from model request start to first content delta.",
        )
        self._token_usage = self._create_instrument(
            "create_histogram",
            GENAI_METRIC_NAMES["token_usage"],
            unit="{token}",
            description="Model token usage by token type.",
        )
        self._model_cost = self._create_instrument(
            "create_histogram",
            "react_agent.gen_ai.cost",
            unit="1",
            description="Estimated model operation cost in the named currency.",
        )
        self._tool_duration = self._create_instrument(
            "create_histogram",
            GENAI_METRIC_NAMES["tool_duration"],
            unit="s",
            description="Duration of tool operations.",
        )
        self._run_duration = self._create_instrument(
            "create_histogram",
            GENAI_METRIC_NAMES["agent_duration"],
            unit="s",
            description="Duration of Agent runs.",
        )
        self._run_count = self._create_instrument(
            "create_counter",
            GENAI_METRIC_NAMES["agent_count"],
            unit="{run}",
            description="Completed Agent runs.",
        )
        self._resume_count = self._create_instrument(
            "create_counter",
            "react_agent.run.resume.count",
            unit="{resume}",
            description="Agent execution resumes after interruption.",
        )
        self._tool_count = self._create_instrument(
            "create_counter",
            GENAI_METRIC_NAMES["tool_count"],
            unit="{call}",
            description="Completed and reused tool calls.",
        )
        self._policy_count = self._create_instrument(
            "create_counter",
            "react_agent.policy.terminal.count",
            unit="{event}",
            description="Budget exhaustion and loop detection events.",
        )

    def _create_instrument(self, method: str, name: str, **kwargs: str) -> Any | None:
        if not self.available:
            return None
        creator = getattr(self._meter, method, None)
        if creator is None:
            return None
        try:
            return creator(name, **kwargs)
        except Exception:
            return None

    @staticmethod
    def _dimension(value: TelemetryValue | None, allowed: frozenset[str]) -> str:
        if not isinstance(value, str):
            return "other"
        return value if value in allowed else "other"

    @staticmethod
    def _bounded_outcome(value: TelemetryValue | None) -> str:
        if value in {
            "completed",
            "failed",
            "partial",
            "timed_out",
            "refused",
            "incomplete",
            "error",
            "reused",
        }:
            return str(value)
        return "other"

    def _model_metric_attributes(
        self, attributes: Mapping[str, TelemetryValue]
    ) -> dict[str, TelemetryValue]:
        return {
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": self._dimension(
                attributes.get("provider"), self.cardinality.allowed_providers
            ),
            "gen_ai.request.model": self._dimension(
                attributes.get("request_model"), self.cardinality.allowed_models
            ),
            "react_agent.outcome": self._bounded_outcome(attributes.get("outcome")),
        }

    def _tool_metric_attributes(
        self, attributes: Mapping[str, TelemetryValue]
    ) -> dict[str, TelemetryValue]:
        return {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": self._dimension(
                attributes.get("tool_name"), self.cardinality.allowed_tools
            ),
            "react_agent.outcome": self._bounded_outcome(attributes.get("outcome")),
            "react_agent.cached": bool(attributes.get("cached", False)),
        }

    @staticmethod
    def _run_metric_attributes(
        attributes: Mapping[str, TelemetryValue],
    ) -> dict[str, TelemetryValue]:
        return {
            "react_agent.status": OTelTelemetry._bounded_outcome(attributes.get("status")),
            "react_agent.stop_reason": str(attributes.get("stop_reason", "other"))[:64],
        }

    @staticmethod
    def _resume_metric_attributes(
        attributes: Mapping[str, TelemetryValue],
    ) -> dict[str, TelemetryValue]:
        reason = attributes.get("resume_reason")
        bounded_reasons = {
            "process_restart",
            "lease_takeover",
            "model_abandoned",
            "tool_retry",
            "operator_reconciliation",
        }
        return {
            "react_agent.resume.reason": (
                str(reason) if reason in bounded_reasons else "other"
            )
        }

    @staticmethod
    def _span_scope(kind: TelemetryEventKind) -> str:
        if kind in {
            TelemetryEventKind.RUN_STARTED,
            TelemetryEventKind.RUN_RESUMED,
            TelemetryEventKind.RUN_COMPLETED,
        }:
            return "run"
        if kind in {
            TelemetryEventKind.MODEL_STARTED,
            TelemetryEventKind.MODEL_COMPLETED,
            TelemetryEventKind.MODEL_FAILED,
        }:
            return "model"
        if kind in {
            TelemetryEventKind.TOOL_STARTED,
            TelemetryEventKind.TOOL_COMPLETED,
            TelemetryEventKind.TOOL_REUSED,
        }:
            return "tool"
        return "run"

    def _span_key(
        self, kind: TelemetryEventKind, attributes: Mapping[str, TelemetryValue]
    ) -> tuple[str, str, str, str]:
        scope = self._span_scope(kind)
        run_id = str(attributes.get("run_id", ""))
        execution_id = str(attributes.get("execution_id", "default"))
        if scope == "run":
            operation_id = "root"
        elif scope == "model" and isinstance(attributes.get("step"), int):
            # Journal operation ids describe individual committed facts and
            # therefore legitimately differ between model.started and its
            # terminal fact. A model operation is instead identified by its
            # durable step and retry attempt within one execution.
            operation_id = (
                f"s{attributes['step']}:a{attributes.get('attempt', 1)}"
            )
        elif scope == "tool" and attributes.get("call_key"):
            # call_key is stable across tool.started/tool.completed and is the
            # Runtime's recovery/idempotency identity for that invocation.
            operation_id = str(attributes["call_key"])
        else:
            operation_id = str(attributes.get("operation_id", attributes.get("step", "unknown")))
        return scope, run_id, execution_id, operation_id

    def _span_kind(self, scope: str) -> Any | None:
        if self._trace_module is None:
            return None
        span_kind = getattr(self._trace_module, "SpanKind", None)
        if span_kind is None:
            return None
        return getattr(span_kind, "CLIENT" if scope == "model" else "INTERNAL", None)

    @staticmethod
    def _span_name(scope: str, attributes: Mapping[str, TelemetryValue]) -> tuple[str, str]:
        if scope == "run":
            suffix = str(attributes.get("agent_name", "")).strip()
            return (f"invoke_agent {suffix}".rstrip(), "invoke_agent")
        if scope == "model":
            suffix = str(attributes.get("request_model", "")).strip()
            return (f"chat {suffix}".rstrip(), "chat")
        suffix = str(attributes.get("tool_name", "")).strip()
        return (f"execute_tool {suffix}".rstrip(), "execute_tool")

    @staticmethod
    def _span_attributes(
        scope: str, attributes: Mapping[str, TelemetryValue], operation_name: str
    ) -> dict[str, TelemetryValue]:
        mapping = {
            "run_id": "react_agent.run.id",
            "execution_id": "react_agent.execution.id",
            "previous_execution_id": "react_agent.execution.previous_id",
            "sequence": "react_agent.event.sequence",
            "operation_id": "react_agent.operation.id",
            "agent_name": "gen_ai.agent.name",
            "step": "react_agent.step",
            "attempt": "react_agent.attempt",
            "provider": "gen_ai.provider.name",
            "request_model": "gen_ai.request.model",
            "response_model": "gen_ai.response.model",
            "request_id": "gen_ai.response.id",
            "finish_reason": "gen_ai.response.finish_reasons",
            "outcome": "react_agent.outcome",
            "error_type": "error.type",
            "input_tokens": "gen_ai.usage.input_tokens",
            "output_tokens": "gen_ai.usage.output_tokens",
            "cached_input_tokens": "react_agent.usage.cached_input_tokens",
            "reasoning_output_tokens": "react_agent.usage.reasoning_output_tokens",
            "billable_tokens": "react_agent.usage.billable_tokens",
            "tool_name": "gen_ai.tool.name",
            "tool_type": "gen_ai.tool.type",
            "tool_call_id": "gen_ai.tool.call.id",
            "call_key": "react_agent.tool.call_key",
            "cached": "react_agent.cached",
            "executed": "react_agent.executed",
            "status": "react_agent.status",
            "stop_reason": "react_agent.stop_reason",
            "reason": "react_agent.reason",
            "resume_reason": "react_agent.resume.reason",
            "ttfc_s": "react_agent.gen_ai.time_to_first_content",
        }
        result: dict[str, TelemetryValue] = {"gen_ai.operation.name": operation_name}
        for source, target in mapping.items():
            value = attributes.get(source)
            if value is not None:
                result[target] = value
        if scope == "run":
            result["react_agent.span.scope"] = "run"
        return result

    @staticmethod
    def _get_span_context(span: Any) -> Any | None:
        try:
            context = span.get_span_context()
        except Exception:
            return None
        if getattr(context, "is_valid", True) is False:
            return None
        return context

    @staticmethod
    def _trace_reference(
        span_context: Any | None,
        attributes: Mapping[str, TelemetryValue],
    ) -> TraceReference | None:
        if span_context is None:
            return None
        try:
            trace_id = int(span_context.trace_id)
            span_id = int(span_context.span_id)
            trace_flags = int(span_context.trace_flags) & 1
            return TraceReference(
                run_id=str(attributes.get("run_id", "")),
                execution_id=str(attributes.get("execution_id", "")),
                trace_id=f"{trace_id:032x}",
                span_id=f"{span_id:016x}",
                trace_flags=trace_flags,
            )
        except (AttributeError, TypeError, ValueError, OverflowError):
            return None

    def _span_context_from_reference(
        self, reference: TraceReference
    ) -> Any | None:
        try:
            trace_id = int(reference.trace_id, 16)
            span_id = int(reference.span_id, 16)
        except ValueError:  # pragma: no cover - TraceReference validates eagerly
            return None
        span_context_type = (
            getattr(self._trace_module, "SpanContext", None)
            if self._trace_module is not None
            else None
        )
        if span_context_type is None:
            return _LinkedSpanContext(
                trace_id=trace_id,
                span_id=span_id,
                trace_flags=reference.trace_flags,
            )
        flags_type = getattr(self._trace_module, "TraceFlags", None)
        flags: Any = reference.trace_flags
        if flags_type is not None:
            try:
                flags = flags_type(reference.trace_flags)
            except Exception:
                return None
        trace_state_type = getattr(self._trace_module, "TraceState", None)
        try:
            trace_state = trace_state_type() if trace_state_type is not None else None
            return span_context_type(
                trace_id=trace_id,
                span_id=span_id,
                is_remote=True,
                trace_flags=flags,
                trace_state=trace_state,
            )
        except Exception:
            return None

    def _empty_parent_context(self) -> Any | None:
        if self._context_module is None:
            return None
        context_type = getattr(self._context_module, "Context", None)
        if context_type is None:
            return None
        try:
            return context_type()
        except Exception:
            return None

    def _span_link(self, span_context: Any | None) -> Any | None:
        if span_context is None:
            return None
        link_type = (
            getattr(self._trace_module, "Link", None)
            if self._trace_module is not None
            else None
        )
        if link_type is None:
            return span_context
        try:
            return link_type(span_context)
        except Exception:
            return None

    def _parent_context_for_child(
        self, key: tuple[str, str, str, str]
    ) -> Any | None:
        """Build an explicit parent without mutating task-local OTel context.

        Runtime lifecycle facts can be emitted by different asyncio tasks: the
        HTTP submit task records RUN_STARTED while the background execution
        records model/tool events and RUN_COMPLETED.  Context attach tokens are
        task-local, so keeping one for the span lifetime is invalid.  A stored
        span is safe to use as an explicit parent context across those tasks.
        """

        if self._trace_module is None:
            return None
        root_key = ("run", key[1], key[2], "root")
        with self._lock:
            root = self._spans.get(root_key)
        if root is None:
            return None
        try:
            empty = self._empty_parent_context()
            if empty is not None:
                return self._trace_module.set_span_in_context(
                    root.span, context=empty
                )
            return self._trace_module.set_span_in_context(root.span)
        except TypeError:
            try:
                return self._trace_module.set_span_in_context(root.span)
            except Exception:
                return None
        except Exception:
            return None

    def _close_previous_execution_for_resume(
        self, event: TelemetryEvent
    ) -> Any | None:
        run_id = str(event.attributes.get("run_id", ""))
        execution_id = str(event.attributes.get("execution_id", "default"))
        with self._lock:
            reference = self._execution_references.get(run_id)
            if reference is None or reference.execution_id == execution_id:
                return None
            self._execution_references.pop(run_id, None)
            previous_spans = [
                (key, state)
                for key, state in self._spans.items()
                if key[1] == run_id and key[2] == reference.execution_id
            ]
            for key, _ in previous_spans:
                self._spans.pop(key, None)
        # Dict insertion order follows nesting in this adapter. Closing it in
        # reverse detaches model/tool contexts before their execution root.
        for _, previous in reversed(previous_spans):
            try:
                previous.span.set_attribute(
                    "react_agent.execution.outcome", "abandoned"
                )
                previous.span.add_event(
                    "execution_abandoned",
                    timestamp=event.timestamp_ns,
                )
            except Exception:
                pass
            try:
                previous.span.end(end_time=event.timestamp_ns)
            except TypeError:
                with contextlib.suppress(Exception):
                    previous.span.end()
            except Exception:
                pass
        return self._span_link(reference.span_context)

    def _start_span(self, event: TelemetryEvent) -> TraceReference | None:
        if not self.available:
            return None
        tracer = self._tracer
        if tracer is None:
            return None
        attributes = event.attributes
        key = self._span_key(event.kind, attributes)
        with self._lock:
            if key in self._spans:
                if key[0] == "run":
                    current = self._execution_references.get(key[1])
                    if current is not None and current.span_key == key:
                        return current.trace_reference
                return None
        local_link = (
            self._close_previous_execution_for_resume(event)
            if event.kind is TelemetryEventKind.RUN_RESUMED
            else None
        )
        scope = key[0]
        name, operation_name = self._span_name(scope, attributes)
        span_attributes = self._span_attributes(scope, attributes, operation_name)
        if scope == "run":
            span_attributes["react_agent.execution.kind"] = (
                "resume"
                if event.kind is TelemetryEventKind.RUN_RESUMED
                else "start"
            )
        start_kwargs: dict[str, Any] = {
            "attributes": span_attributes,
            "start_time": event.timestamp_ns,
        }
        span_kind = self._span_kind(scope)
        if span_kind is not None:
            start_kwargs["kind"] = span_kind
        if scope != "run":
            parent_context = self._parent_context_for_child(key)
            if parent_context is not None:
                start_kwargs["context"] = parent_context
        elif event.kind is TelemetryEventKind.RUN_RESUMED:
            parent_context = self._empty_parent_context()
            if parent_context is not None:
                start_kwargs["context"] = parent_context
            previous_execution_id = str(
                attributes.get("previous_execution_id", "")
            )
            durable_links: list[Any] = []
            for reference in event.links:
                if (
                    reference.run_id != str(attributes.get("run_id", ""))
                    or reference.execution_id != previous_execution_id
                ):
                    continue
                durable_link = self._span_link(
                    self._span_context_from_reference(reference)
                )
                if durable_link is not None:
                    durable_links.append(durable_link)
            if durable_links:
                start_kwargs["links"] = tuple(durable_links)
            elif local_link is not None:
                start_kwargs["links"] = (local_link,)
        try:
            span = tracer.start_span(name, **start_kwargs)
        except TypeError:
            start_kwargs.pop("start_time", None)
            try:
                span = tracer.start_span(name, **start_kwargs)
            except TypeError:
                start_kwargs.pop("context", None)
                start_kwargs.pop("links", None)
                try:
                    span = tracer.start_span(name, **start_kwargs)
                except Exception:
                    return None
            except Exception:
                return None
        except Exception:
            return None
        span_context = self._get_span_context(span)
        trace_reference = (
            self._trace_reference(span_context, attributes)
            if scope == "run"
            else None
        )
        state = _OpenSpan(span)
        with self._lock:
            self._spans[key] = state
            if scope == "run":
                run_id = str(attributes.get("run_id", ""))
                self._execution_references[run_id] = _ExecutionReference(
                    execution_id=str(attributes.get("execution_id", "default")),
                    span_key=key,
                    span_context=span_context,
                    trace_reference=trace_reference,
                )
        return trace_reference

    def _set_error_status(self, span: Any) -> None:
        if self._trace_module is None:
            return
        try:
            status = self._trace_module.Status(self._trace_module.StatusCode.ERROR)
            span.set_status(status)
        except Exception:
            pass

    def _end_span(self, event: TelemetryEvent, *, failed: bool = False) -> None:
        key = self._span_key(event.kind, event.attributes)
        with self._lock:
            state = self._spans.pop(key, None)
        if state is None:
            return
        _, operation_name = self._span_name(key[0], event.attributes)
        for name, value in self._span_attributes(key[0], event.attributes, operation_name).items():
            try:
                state.span.set_attribute(name, value)
            except Exception:
                pass
        if failed:
            self._set_error_status(state.span)
        try:
            state.span.end(end_time=event.timestamp_ns)
        except TypeError:
            state.span.end()
        finally:
            if key[0] == "run":
                run_id = str(event.attributes.get("run_id", ""))
                with self._lock:
                    reference = self._execution_references.get(run_id)
                    if reference is not None and reference.span_key == key:
                        self._execution_references.pop(run_id, None)

    def _add_root_event(self, event: TelemetryEvent) -> None:
        key = self._span_key(TelemetryEventKind.RUN_STARTED, event.attributes)
        with self._lock:
            state = self._spans.get(key)
        if state is None:
            return
        if event.kind in {
            TelemetryEventKind.BUDGET_EXHAUSTED,
            TelemetryEventKind.LOOP_DETECTED,
        }:
            try:
                state.span.set_attribute(
                    "react_agent.policy.trigger", event.kind.value
                )
            except Exception:
                pass
        try:
            state.span.add_event(
                event.kind.value,
                attributes=self._span_attributes("run", event.attributes, "invoke_agent"),
                timestamp=event.timestamp_ns,
            )
        except TypeError:
            state.span.add_event(event.kind.value)

    def _emit_log(self, event: TelemetryEvent) -> None:
        if self._logger is None:
            return
        scope = self._span_scope(event.kind)
        _, operation_name = self._span_name(scope, event.attributes)
        attributes = self._span_attributes(scope, event.attributes, operation_name)
        severity = "ERROR" if event.kind is TelemetryEventKind.MODEL_FAILED else "INFO"
        if event.kind in {
            TelemetryEventKind.BUDGET_EXHAUSTED,
            TelemetryEventKind.LOOP_DETECTED,
        }:
            severity = "WARN"
        try:
            if self._logs_module is None:
                self._logger.emit(
                    {
                        "timestamp": event.timestamp_ns,
                        "severity_text": severity,
                        "body": event.kind.value,
                        "attributes": attributes,
                    }
                )
                return
            severity_number = getattr(self._logs_module.SeverityNumber, severity, None)
            record = self._logs_module.LogRecord(
                timestamp=event.timestamp_ns,
                observed_timestamp=time.time_ns(),
                severity_text=severity,
                severity_number=severity_number,
                body=event.kind.value,
                attributes=attributes,
            )
            self._logger.emit(record)
        except Exception:
            pass

    @staticmethod
    def _record(instrument: Any | None, value: int | float, attributes: Mapping[str, Any]) -> None:
        if instrument is None:
            return
        try:
            instrument.record(value, attributes=attributes)
        except Exception:
            pass

    @staticmethod
    def _add(instrument: Any | None, value: int, attributes: Mapping[str, Any]) -> None:
        if instrument is None:
            return
        try:
            instrument.add(value, attributes=attributes)
        except Exception:
            pass

    def _record_model_metrics(self, event: TelemetryEvent) -> None:
        attributes = event.attributes
        metric_attributes = self._model_metric_attributes(attributes)
        duration = attributes.get("duration_s")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            self._record(self._model_duration, duration, metric_attributes)
        ttfc = attributes.get("ttfc_s")
        if (
            isinstance(ttfc, (int, float))
            and not isinstance(ttfc, bool)
            and ttfc >= 0
        ):
            self._record(self._model_ttfc, ttfc, metric_attributes)
        for token_type, key in (
            ("input", "input_tokens"),
            ("output", "output_tokens"),
            ("cached_input", "cached_input_tokens"),
            ("reasoning_output", "reasoning_output_tokens"),
            ("billable", "billable_tokens"),
        ):
            value = attributes.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                self._record(
                    self._token_usage,
                    value,
                    {**metric_attributes, "gen_ai.token.type": token_type},
                )
        cost_micros = attributes.get("cost_micros")
        if isinstance(cost_micros, int) and not isinstance(cost_micros, bool):
            currency = str(attributes.get("currency", "other"))
            if currency not in {"USD", "EUR", "GBP", "CNY", "JPY"}:
                currency = "other"
            self._record(
                self._model_cost,
                cost_micros / 1_000_000,
                {**metric_attributes, "react_agent.currency": currency},
            )

    def _record_tool_metrics(self, event: TelemetryEvent) -> None:
        attributes = event.attributes
        metric_attributes = self._tool_metric_attributes(attributes)
        duration = attributes.get("duration_s")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            self._record(self._tool_duration, duration, metric_attributes)
        self._add(self._tool_count, 1, metric_attributes)

    def _record_run_metrics(self, event: TelemetryEvent) -> None:
        attributes = event.attributes
        metric_attributes = self._run_metric_attributes(attributes)
        duration = attributes.get("duration_s")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            self._record(self._run_duration, duration, metric_attributes)
        self._add(self._run_count, 1, metric_attributes)

    def emit(self, event: TelemetryEvent) -> TraceReference | None:
        if event.mode is TelemetryMode.REPLAY or not self.available:
            return None
        sanitized = TelemetryEvent(
            event.kind,
            sanitize_attributes(event.kind, event.attributes),
            event.timestamp_ns,
            event.mode,
            event.links,
        )
        kind = sanitized.kind
        if kind in {
            TelemetryEventKind.RUN_STARTED,
            TelemetryEventKind.RUN_RESUMED,
            TelemetryEventKind.MODEL_STARTED,
            TelemetryEventKind.TOOL_STARTED,
        }:
            trace_reference = self._start_span(sanitized)
            if kind is TelemetryEventKind.RUN_RESUMED:
                self._add(
                    self._resume_count,
                    1,
                    self._resume_metric_attributes(sanitized.attributes),
                )
            self._emit_log(sanitized)
            return trace_reference

        self._emit_log(sanitized)
        if kind is TelemetryEventKind.MODEL_COMPLETED:
            self._record_model_metrics(sanitized)
            self._end_span(sanitized)
        elif kind is TelemetryEventKind.MODEL_FAILED:
            self._record_model_metrics(sanitized)
            self._end_span(sanitized, failed=True)
        elif kind is TelemetryEventKind.TOOL_COMPLETED:
            self._record_tool_metrics(sanitized)
            failed = sanitized.attributes.get("outcome") in {"failed", "error"}
            self._end_span(sanitized, failed=failed)
        elif kind is TelemetryEventKind.TOOL_REUSED:
            self._start_span(sanitized)
            self._record_tool_metrics(sanitized)
            self._end_span(sanitized)
        elif kind is TelemetryEventKind.RUN_COMPLETED:
            self._record_run_metrics(sanitized)
            failed = sanitized.attributes.get("status") == "failed"
            self._end_span(sanitized, failed=failed)
        elif kind in {
            TelemetryEventKind.BUDGET_EXHAUSTED,
            TelemetryEventKind.LOOP_DETECTED,
        }:
            self._add(
                self._policy_count,
                1,
                {"react_agent.policy": kind.value},
            )
            self._add_root_event(sanitized)
        return None


def create_telemetry(*, cardinality: MetricCardinalityPolicy | None = None) -> Telemetry:
    """Return an OTel adapter when available, otherwise a NoOp adapter."""

    adapter = OTelTelemetry(cardinality=cardinality)
    return adapter if adapter.available else NoOpTelemetry()
