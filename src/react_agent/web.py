"""Minimal same-origin web chat for the ReAct agent."""

from __future__ import annotations

import ast
import asyncio
import json
import math
import operator
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, Protocol, cast

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from .agent import AgentConfig, ReActAgent
from .context import (
    ContextGovernor,
    ContextStrategy,
    FileContextSummaryStore,
    ModelContextCompressor,
)
from .cost_ledger import MAX_COST_MICROS
from .events import RunSnapshot
from .journal import InMemoryRunJournal, RunJournal
from .models import AgentResult, AgentStreamEvent, RunStatus, StopReason, TranscriptItem
from .postgres_journal import PostgresRunJournal
from .provider import ApiMode, OpenAIModel, ProviderCapabilities
from .runtime import (
    AdjustCost,
    AgentRuntime,
    CancelRun,
    ForkRun,
    ReconciliationRequired,
    ResolveRun,
    ResumeRejected,
    ResumeRun,
    RunCommand,
    RunHandle,
    RuntimeConflict,
    RuntimeEvent,
    RuntimeNotFound,
    StartRun,
)
from .telemetry import MetricCardinalityPolicy, create_telemetry
from .tools import DebugExposure, tool
from .workspace import GitWorktreeWorkspace, WorkspaceCheckpointStore

_STATIC_DIR = Path(__file__).with_name("static")
_INDEX_FILE = _STATIC_DIR / "index.html"
_METRIC_ALLOWED_MODELS_ENV = "REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS"
_METRIC_ALLOWED_TOOLS_ENV = "REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS"
_MAX_METRIC_DIMENSION_VALUES = 64
_MAX_METRIC_DIMENSION_LENGTH = 128


class _AgentExecutionError(Exception):
    """Keep unexpected Agent failures separate from session-store failures."""


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: Annotated[str, Field(min_length=1, max_length=10_000)]
    session_id: uuid.UUID | None = None


class UsageResponse(BaseModel):
    input_tokens: int
    output_tokens: int
    total_tokens: int


class ChatResponse(BaseModel):
    session_id: uuid.UUID
    answer: str | None
    status: str
    stop_reason: str
    model_calls: int
    tool_calls: int
    usage: UsageResponse
    error: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    model: str
    api_mode: ApiMode


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: Annotated[
        str,
        Field(
            min_length=1,
            max_length=10_000,
            validation_alias=AliasChoices("prompt", "message"),
        ),
    ]
    session_id: Annotated[str, Field(min_length=1, max_length=256)] | None = None


class ForkRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_sequence: Annotated[int, Field(ge=1)] | None = None
    session_id: Annotated[str, Field(min_length=1, max_length=256)] | None = None


class CancelRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Annotated[str, Field(min_length=1, max_length=128)] = "user_requested"


class ResolveRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    call_key: Annotated[str, Field(min_length=1, max_length=256)]
    action: Literal["retry", "use_result", "abort"]
    result: object | None = None


class AdjustCostRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    previous_record_id: Annotated[str, Field(min_length=1, max_length=256)]
    revised_total_microunits: Annotated[
        int,
        Field(
            ge=0,
            le=MAX_COST_MICROS,
            validation_alias=AliasChoices(
                "revised_total_microunits",
                "revised_total_micros",
            ),
        ),
    ]
    note: Annotated[str, Field(max_length=2_000)] | None = None
    record_id: Annotated[str, Field(min_length=1, max_length=256)] | None = None


class _RuntimeView(Protocol):
    async def submit(self, command: RunCommand) -> RunHandle: ...

    async def load(self, run_id: str) -> RunSnapshot: ...

    def follow(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        live: bool = True,
    ) -> AsyncIterator[RuntimeEvent]: ...

    async def list_session_runs(self, session_id: str) -> tuple[RunSnapshot, ...]: ...


@dataclass(frozen=True, slots=True)
class _StreamFrame:
    event: str
    data: dict[str, object]
    event_id: str | None = None


_MAX_EVENT_SEQUENCE = 9_223_372_036_854_775_807
_SSE_HEARTBEAT_S = 15.0
_RUNTIME_RESPONSE_TIMEOUT_S = 135.0
_LEGACY_SESSION_NAMESPACE = uuid.UUID("fe8bb06a-85f7-4f0d-9ec5-e6c2719b2d51")
_COST_ADJUSTMENT_OPERATION_NAMESPACE = uuid.UUID(
    "1b8a83db-e248-4a6a-9d04-1109665746b0"
)


@dataclass(slots=True)
class _Session:
    transcript: tuple[TranscriptItem, ...] = ()
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_seen: float = field(default_factory=time.monotonic)
    active_requests: int = 0
    deleting: bool = False


class SessionStore:
    """Small in-memory session store; complete provider transcripts stay server-side."""

    def __init__(self, *, max_sessions: int = 256, ttl_s: float = 3_600.0) -> None:
        if max_sessions < 1:
            raise ValueError("max_sessions must be positive")
        if ttl_s <= 0:
            raise ValueError("ttl_s must be positive")
        self.max_sessions = max_sessions
        self.ttl_s = ttl_s
        self._sessions: dict[uuid.UUID, _Session] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def lease(
        self,
        session_id: uuid.UUID | None,
    ) -> AsyncIterator[tuple[uuid.UUID, _Session]]:
        """Lock one live session and keep it protected from pruning or deletion."""

        now = time.monotonic()
        async with self._lock:
            self._prune(now)
            if session_id is None:
                if len(self._sessions) >= self.max_sessions:
                    self._evict_oldest_unlocked()
                if len(self._sessions) >= self.max_sessions:
                    raise RuntimeError("All chat sessions are currently busy.")
                resolved_id = uuid.uuid4()
                session = _Session(last_seen=now)
                self._sessions[resolved_id] = session
            else:
                resolved_id = session_id
                existing_session = self._sessions.get(resolved_id)
                if existing_session is None or existing_session.deleting:
                    raise KeyError("Chat session was not found or has expired.")
                session = existing_session
            session.last_seen = now
            session.active_requests += 1

        try:
            await session.lock.acquire()
        except BaseException:
            session.active_requests -= 1
            raise

        if self._sessions.get(resolved_id) is not session:
            session.active_requests -= 1
            session.lock.release()
            raise KeyError("Chat session was not found or has expired.")

        try:
            yield resolved_id, session
        finally:
            session.active_requests -= 1
            if self._sessions.get(resolved_id) is session:
                session.last_seen = time.monotonic()
            session.lock.release()

    async def delete(self, session_id: uuid.UUID) -> bool:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None or session.deleting:
                return False
            session.deleting = True
        try:
            await session.lock.acquire()
        except BaseException:
            if self._sessions.get(session_id) is session:
                session.deleting = False
            raise
        try:
            if self._sessions.get(session_id) is not session:
                return False
            self._sessions.pop(session_id)
            return True
        finally:
            session.lock.release()

    def _prune(self, now: float) -> None:
        expired = [
            session_id
            for session_id, session in self._sessions.items()
            if (
                session.active_requests == 0
                and not session.deleting
                and now - session.last_seen > self.ttl_s
            )
        ]
        for session_id in expired:
            self._sessions.pop(session_id, None)

    def _evict_oldest_unlocked(self) -> None:
        candidates = [
            (session.last_seen, session_id)
            for session_id, session in self._sessions.items()
            if session.active_requests == 0 and not session.deleting
        ]
        if candidates:
            _, oldest_id = min(candidates)
            self._sessions.pop(oldest_id, None)


_BINARY_OPERATORS: dict[type[ast.operator], Callable[[float, float], float]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS: dict[type[ast.unaryop], Callable[[float], float]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def _evaluate_expression(expression: str) -> int | float:
    tree = ast.parse(expression, mode="eval")
    if sum(1 for _ in ast.walk(tree)) > 64:
        raise ValueError("Expression is too complex.")

    def evaluate(node: ast.AST) -> int | float:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ValueError("Only numeric constants are allowed.")
            return node.value
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
            return _UNARY_OPERATORS[type(node.op)](evaluate(node.operand))
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 20:
                raise ValueError("Exponent is too large.")
            value = _BINARY_OPERATORS[type(node.op)](left, right)
            if not math.isfinite(float(value)) or abs(value) > 1e100:
                raise ValueError("Result is outside the safe numeric range.")
            return value
        raise ValueError("Unsupported expression.")

    result = evaluate(tree)
    if isinstance(result, float):
        return float(f"{result:.15g}")
    return result


@tool(
    idempotent=True,
    parallel_safe=True,
    timeout_s=2.0,
    debug_exposure=DebugExposure.FULL,
)
def calculate_expression(
    expression: Annotated[
        str,
        Field(
            min_length=1,
            max_length=200,
            description=(
                "Arithmetic expression using numbers, parentheses, "
                "+, -, *, /, //, %, or **."
            ),
        ),
    ],
) -> dict[str, int | float | str]:
    """Safely evaluate a basic arithmetic expression; never use Python eval."""

    return {"expression": expression, "result": _evaluate_expression(expression)}


def _truthy_env(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _metric_dimension_allowlist(
    name: str,
    *,
    defaults: tuple[str, ...],
) -> frozenset[str]:
    """Read one exact, bounded metric dimension allowlist.

    A missing override freezes the finite Runtime configuration. Wildcards are
    deliberately unsupported: accepting one would make a deployment look
    bounded while silently turning arbitrary provider values into labels.
    """

    raw = os.getenv(name)
    values = defaults if raw is None else tuple(part.strip() for part in raw.split(","))
    if raw is not None and (not values or any(not value for value in values)):
        raise RuntimeError(f"{name} must contain one or more comma-separated values.")
    unique_values = frozenset(values)
    if len(unique_values) > _MAX_METRIC_DIMENSION_VALUES:
        raise RuntimeError(
            f"{name} may contain at most {_MAX_METRIC_DIMENSION_VALUES} values."
        )
    for value in unique_values:
        if len(value) > _MAX_METRIC_DIMENSION_LENGTH:
            raise RuntimeError(
                f"Each {name} value must be at most "
                f"{_MAX_METRIC_DIMENSION_LENGTH} characters."
            )
        if "*" in value or any(
            ord(character) < 32 or ord(character) == 127 for character in value
        ):
            raise RuntimeError(f"{name} only accepts exact, printable values.")
    return unique_values


def _build_metric_cardinality_policy(
    agent: ReActAgent,
    model_name: str,
) -> MetricCardinalityPolicy:
    """Freeze bounded model/tool metric labels for this Web Runtime."""

    return MetricCardinalityPolicy(
        allowed_models=_metric_dimension_allowlist(
            _METRIC_ALLOWED_MODELS_ENV,
            defaults=(model_name,),
        ),
        allowed_tools=_metric_dimension_allowlist(
            _METRIC_ALLOWED_TOOLS_ENV,
            defaults=tuple(registered.name for registered in agent.registry.tools),
        ),
    )


def _postgres_dsn_from_env() -> str | None:
    """Resolve the database secret without ever formatting it into diagnostics."""

    raw = os.getenv("REACT_AGENT_POSTGRES_DSN")
    if raw is None:
        raw = os.getenv("DATABASE_URL")
    if raw is None:
        return None
    dsn = raw.strip()
    if not dsn:
        raise RuntimeError("The configured PostgreSQL DSN is blank.")
    return dsn


async def _build_journal_from_env() -> tuple[RunJournal, PostgresRunJournal | None]:
    dsn = _postgres_dsn_from_env()
    if dsn is None:
        return InMemoryRunJournal(), None

    journal: PostgresRunJournal | None = None
    try:
        journal = PostgresRunJournal(dsn)
        await journal.open()
    except Exception:
        if journal is not None:
            with suppress(Exception):
                await journal.close()
        # psycopg exceptions can include connection fields. Replace the entire
        # chain so ASGI startup logs cannot disclose credentials or the DSN.
        raise RuntimeError("Unable to initialize the PostgreSQL journal.") from None
    return journal, journal


def _build_workspace_from_env() -> WorkspaceCheckpointStore | None:
    repository = os.getenv("REACT_AGENT_REPOSITORY")
    managed_root = os.getenv("REACT_AGENT_WORKTREE_ROOT")
    if repository is None and managed_root is None:
        return None
    if not repository or not managed_root:
        raise RuntimeError(
            "REACT_AGENT_REPOSITORY and REACT_AGENT_WORKTREE_ROOT must be set together."
        )
    try:
        return GitWorktreeWorkspace(Path(repository), Path(managed_root))
    except Exception:
        # Git errors can include deployment paths. Keep the startup diagnostic
        # useful without reflecting local layout into logs or HTTP responses.
        raise RuntimeError("Unable to initialize the managed Git workspace.") from None


def _build_agent_from_env() -> tuple[OpenAIModel, ReActAgent, str, ApiMode]:
    model_name = os.getenv("OPENAI_MODEL")
    if not model_name:
        raise RuntimeError("Set OPENAI_MODEL before starting react-agent-web.")
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Set OPENAI_API_KEY before starting react-agent-web.")

    raw_api_mode = os.getenv("OPENAI_API_MODE", "responses")
    if raw_api_mode not in ("responses", "chat_completions"):
        raise RuntimeError("OPENAI_API_MODE must be responses or chat_completions.")
    api_mode = cast(ApiMode, raw_api_mode)
    compat = _truthy_env("OPENAI_COMPAT_MODE")
    capabilities = ProviderCapabilities(
        strict_tools=not compat,
        parallel_tool_calls=not compat,
        store_parameter=not compat,
        encrypted_reasoning_items=not compat,
        chat_stream_usage=not compat,
    )
    model = OpenAIModel(
        model_name,
        api_mode=api_mode,
        base_url=os.getenv("OPENAI_BASE_URL"),
        allow_insecure_http=_truthy_env("OPENAI_ALLOW_INSECURE_HTTP"),
        capabilities=capabilities,
    )
    raw_context_strategy = os.getenv("REACT_AGENT_CONTEXT_STRATEGY", "tiered")
    try:
        context_strategy = ContextStrategy(raw_context_strategy)
    except ValueError:
        raise RuntimeError(
            "REACT_AGENT_CONTEXT_STRATEGY must be tiered, generic, or stop."
        ) from None
    summary_root = os.getenv("REACT_AGENT_CONTEXT_SUMMARY_DIR")
    context_governor = ContextGovernor(
        strategy=context_strategy,
        compressor=(
            None
            if context_strategy is ContextStrategy.STOP
            else ModelContextCompressor(model)
        ),
        store=(FileContextSummaryStore(summary_root) if summary_root else None),
        keep_recent_turns=2,
        max_summary_chars=12_000,
    )
    agent = ReActAgent(
        model,
        [calculate_expression],
        config=AgentConfig(
            max_steps=8,
            max_tool_calls=16,
            max_wall_time_s=120.0,
            max_concurrent_tools=4,
            max_tool_output_chars=8_000,
            max_context_chars=160_000,
            context_strategy=context_strategy,
        ),
        context_governor=context_governor,
    )
    return model, agent, model_name, api_mode


def _public_run_error(status: str, stop_reason: str) -> str | None:
    if status == RunStatus.COMPLETED.value:
        return None
    return {
        "wall_time": "Agent request timed out. Please try again.",
        "model_refusal": "The model declined this request.",
        "model_incomplete": "The model returned an incomplete response.",
        "max_steps": "The Agent reached its step limit.",
        "max_tool_calls": "The Agent reached its tool-call limit.",
        "context_limit": "This conversation is too long to continue.",
        "loop_detected": "The Agent stopped after detecting a repeated action.",
    }.get(stop_reason, "Agent request failed. Please try again.")


def _chat_response(session_id: uuid.UUID, result: AgentResult) -> ChatResponse:
    return ChatResponse(
        session_id=session_id,
        answer=result.output,
        status=result.status.value,
        stop_reason=result.stop_reason.value,
        model_calls=result.model_calls,
        tool_calls=result.tool_calls,
        usage=UsageResponse(
            input_tokens=result.usage.input_tokens,
            output_tokens=result.usage.output_tokens,
            total_tokens=result.usage.total_tokens,
        ),
        error=_public_run_error(result.status.value, result.stop_reason.value),
    )


def _agent_stream_payload(event: AgentStreamEvent) -> dict[str, object]:
    """Serialize only the stable, explicitly public Agent stream contract."""

    return {
        "kind": event.kind.value,
        "run_id": event.run_id,
        "sequence": event.sequence,
        "timestamp": event.timestamp,
        "step": event.step,
        "call_key": event.call_key,
        "tool_call_id": event.tool_call_id,
        "tool_name": event.tool_name,
        "data": dict(event.data),
    }


def _encode_sse(frame: _StreamFrame) -> bytes:
    """Encode one allowlisted event; JSON escaping keeps newlines inside data."""

    lines: list[str] = []
    if frame.event_id is not None:
        lines.append(f"id: {frame.event_id}")
    lines.append(f"event: {frame.event}")
    lines.append(
        "data: "
        + json.dumps(
            frame.data,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def _configured_runtime(request: Request) -> _RuntimeView:
    runtime = cast(
        _RuntimeView | None,
        getattr(request.app.state, "runtime", None),
    )
    if runtime is None:
        raise HTTPException(status_code=503, detail="Durable Runtime is not configured.")
    return runtime


def _runtime_http_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, RuntimeNotFound):
        status_code = 404
        fallback = "Run was not found."
    elif isinstance(
        exc,
        (RuntimeConflict, ResumeRejected, ReconciliationRequired),
    ):
        status_code = 409
        fallback = "Run operation conflicts with the current state."
    else:
        return HTTPException(status_code=502, detail="Durable Runtime request failed.")
    detail = str(exc).strip() or fallback
    return HTTPException(status_code=status_code, detail=detail)


def _idempotency_key(request: Request) -> str | None:
    values = request.headers.getlist("idempotency-key")
    if not values:
        return None
    if len(values) != 1:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key must be supplied at most once.",
        )
    value = values[0]
    if (
        not value
        or len(value) > 256
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key must contain 1-256 visible ASCII characters.",
        )
    return value


def _last_event_sequence(request: Request, query_sequence: int) -> int:
    raw_value = request.headers.get("last-event-id")
    if raw_value is None or not raw_value.strip():
        return query_sequence
    try:
        header_sequence = int(raw_value, 10)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Last-Event-ID must be a non-negative integer.",
        ) from None
    if not 0 <= header_sequence <= _MAX_EVENT_SEQUENCE:
        raise HTTPException(
            status_code=400,
            detail="Last-Event-ID is outside the supported range.",
        )
    return max(query_sequence, header_sequence)


def _enum_text(value: object) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _public_json(value: object) -> object:
    """Copy an explicitly public value into JSON-compatible mutable containers."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("public JSON numbers must be finite")
        return value
    if isinstance(value, Enum):
        return _public_json(value.value)
    if isinstance(value, Mapping):
        copied: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("public JSON object keys must be strings")
            copied[key] = _public_json(item)
        return copied
    if isinstance(value, (list, tuple)):
        return [_public_json(item) for item in value]
    raise ValueError(f"unsupported public JSON value: {type(value).__name__}")


def _run_handle_payload(handle: RunHandle) -> dict[str, object]:
    if not handle.run_id.strip() or not handle.session_id.strip():
        raise ValueError("Runtime returned an invalid run handle")
    if handle.execution_id is not None and not handle.execution_id.strip():
        raise ValueError("Runtime returned an invalid execution id")
    return {
        "run_id": handle.run_id,
        "session_id": handle.session_id,
        "execution_id": handle.execution_id,
        "created": handle.created,
    }


def _snapshot_answer(snapshot: RunSnapshot) -> str | None:
    if snapshot.result is None:
        return None
    output = snapshot.result.get("output")
    return output if isinstance(output, str) else None


def _snapshot_cost_summary(
    snapshot: RunSnapshot,
) -> tuple[int | None, str | None, str | None]:
    """Return a conservative public total for the immutable cost ledger."""

    if not snapshot.costs:
        return None, None, "no_cost_records"
    currencies = {
        str(record["currency"])
        for record in snapshot.costs
        if isinstance(record.get("currency"), str)
    }
    if len(currencies) != 1:
        return None, None, "multiple_or_missing_currencies"
    currency = next(iter(currencies))
    unresolved: dict[str, str] = {}
    total_micros = 0
    for index, record in enumerate(snapshot.costs):
        record_id = record.get("record_id")
        amount = record.get("amount_micros")
        adjusts_record_id = record.get("adjusts_record_id")
        if isinstance(adjusts_record_id, str):
            unresolved.pop(adjusts_record_id, None)
        if amount is None:
            key = record_id if isinstance(record_id, str) else f"record:{index}"
            reason = record.get("unknown_reason")
            unresolved[key] = reason if isinstance(reason, str) else "cost_unavailable"
        elif isinstance(amount, int) and not isinstance(amount, bool):
            total_micros += amount
        else:
            return None, currency, "invalid_cost_record"
    if unresolved:
        return None, currency, next(iter(unresolved.values()))
    return total_micros, currency, None


def _snapshot_payload(snapshot: RunSnapshot) -> dict[str, object]:
    """Project recovery state without exposing transcripts or private checkpoints."""

    pending: dict[str, object] = {}
    for key, action in snapshot.pending.items():
        pending[key] = {
            "key": action.key,
            "kind": _enum_text(action.kind),
            "sequence": action.sequence,
            "step": action.step,
            "call_key": action.call_key,
            "phase": action.phase,
        }

    tools: dict[str, object] = {}
    for key, recovery in snapshot.tools.items():
        # Recovery calls and messages are intentionally private. Only the state
        # needed to render/debug reconciliation is part of the HTTP projection.
        tools[key] = {
            "call_key": recovery.call_key,
            "step": recovery.step,
            "phase": recovery.phase,
            "attempts": recovery.attempts,
            "tool_call_id": recovery.tool_call_id,
            "tool_name": recovery.tool_name,
            "resume_policy": recovery.resume_policy,
        }

    usage = snapshot.usage
    cost_microunits, currency, cost_unknown_reason = _snapshot_cost_summary(snapshot)
    terminal_kind = (
        _enum_text(snapshot.terminal.kind) if snapshot.terminal is not None else None
    )
    return {
        "run_id": snapshot.run_id,
        "session_id": snapshot.session_id,
        "execution_id": snapshot.execution_id,
        "agent_revision": snapshot.agent_revision,
        "tool_manifest_hash": snapshot.tool_manifest_hash,
        "state": _enum_text(snapshot.state),
        "status": snapshot.status,
        "stop_reason": snapshot.stop_reason,
        "usage": {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "total_tokens": usage.total_tokens,
            "cached_input_tokens": usage.cached_input_tokens,
            "reasoning_output_tokens": usage.reasoning_output_tokens,
            "billable_tokens": usage.billable_tokens,
        },
        "counts": {
            "model_calls": snapshot.counts.model_calls,
            "tool_calls": snapshot.counts.tool_calls,
            "tool_executions": snapshot.counts.tool_executions,
        },
        "pending": pending,
        "tools": tools,
        "safe_checkpoint_sequences": list(snapshot.safe_checkpoint_sequences),
        "terminal": snapshot.terminal is not None,
        "terminal_kind": terminal_kind,
        "result_ready": snapshot.result is not None,
        "answer": _snapshot_answer(snapshot),
        "last_sequence": snapshot.last_sequence,
        "last_hash": snapshot.last_hash,
        "last_step": snapshot.last_step,
        "executions": list(snapshot.executions),
        "model_attempts": {
            str(step): attempts for step, attempts in snapshot.model_attempts.items()
        },
        "loop_counts": _public_json(snapshot.loop_counts),
        "costs": _public_json(snapshot.costs),
        "cost_microunits": cost_microunits,
        "currency": currency,
        "cost_unknown_reason": cost_unknown_reason,
        "workspace": _public_json(snapshot.workspace),
        "session_version": snapshot.session_version,
        "parent_run_id": snapshot.parent_run_id,
        "fork_sequence": snapshot.fork_sequence,
    }


def _chat_response_from_snapshot(snapshot: RunSnapshot) -> ChatResponse:
    if snapshot.session_id is None:
        raise ValueError("Runtime snapshot has no session id")
    try:
        session_id = uuid.UUID(snapshot.session_id)
    except ValueError:
        raise ValueError("Runtime returned an invalid legacy session id") from None
    status = snapshot.status or RunStatus.FAILED.value
    stop_reason = snapshot.stop_reason or StopReason.PROTOCOL_ERROR.value
    return ChatResponse(
        session_id=session_id,
        answer=_snapshot_answer(snapshot),
        status=status,
        stop_reason=stop_reason,
        model_calls=snapshot.counts.model_calls,
        tool_calls=snapshot.counts.tool_calls,
        usage=UsageResponse(
            input_tokens=snapshot.usage.input_tokens,
            output_tokens=snapshot.usage.output_tokens,
            total_tokens=snapshot.usage.total_tokens,
        ),
        error=_public_run_error(status, stop_reason),
    )


def _legacy_runtime_result_payload(snapshot: RunSnapshot) -> dict[str, object]:
    result = _chat_response_from_snapshot(snapshot).model_dump(mode="json")
    committed = (
        snapshot.status == RunStatus.COMPLETED.value
        and snapshot.session_version is not None
    )
    pending_side_effect = any(
        action.phase in {"started", "reconciliation"}
        for action in snapshot.pending.values()
    )
    result.update(
        {
            "run_id": snapshot.run_id,
            "tool_executions": snapshot.counts.tool_executions,
            "committed": committed,
            "side_effects_possible": (
                not committed
                and (snapshot.counts.tool_executions > 0 or pending_side_effect)
            ),
        }
    )
    return result


def _legacy_runtime_event_frame(event: RuntimeEvent) -> _StreamFrame:
    sequence = event.live_sequence
    if sequence is None or sequence < 1:
        raise ValueError("legacy Runtime frames require a live sequence")
    public_data = _public_json(event.public_data)
    if not isinstance(public_data, dict):
        raise ValueError("Runtime public event data must be an object")
    return _StreamFrame(
        event=event.kind,
        event_id=str(sequence),
        data={
            "kind": event.kind,
            "run_id": event.run_id,
            "sequence": sequence,
            "timestamp": event.timestamp,
            "step": event.step,
            "call_key": event.call_key,
            "tool_call_id": event.tool_call_id,
            "tool_name": event.tool_name,
            "data": public_data,
        },
    )


def _runtime_event_frame(run_id: str, event: RuntimeEvent) -> _StreamFrame:
    kind = _enum_text(event.kind)
    if not kind or "\n" in kind or "\r" in kind:
        raise ValueError("Runtime returned an invalid event kind")
    durable_sequence = event.durable_sequence
    live_sequence = event.live_sequence
    if durable_sequence is not None and (
        isinstance(durable_sequence, bool)
        or not isinstance(durable_sequence, int)
        or not 1 <= durable_sequence <= _MAX_EVENT_SEQUENCE
    ):
        raise ValueError("Runtime returned an invalid durable sequence")
    if live_sequence is not None and (
        isinstance(live_sequence, bool)
        or not isinstance(live_sequence, int)
        or live_sequence < 1
    ):
        raise ValueError("Runtime returned an invalid live sequence")
    public_data = _public_json(event.public_data)
    if not isinstance(public_data, dict):
        raise ValueError("Runtime public event data must be an object")

    data: dict[str, object] = {
        "run_id": run_id,
        "kind": kind,
        "sequence": durable_sequence,
        "durable_sequence": durable_sequence,
        "live_sequence": live_sequence,
        "safe_checkpoint": event.safe_checkpoint,
        "terminal": event.terminal,
        "data": public_data,
    }
    # These fields are public event metadata when a Runtime implementation
    # supplies them. Never reflect an event object's arbitrary attributes.
    for field_name in (
        "timestamp",
        "occurred_at",
        "step",
        "call_key",
        "tool_call_id",
        "tool_name",
        "event_id",
        "causation_id",
        "execution_id",
    ):
        field_value = getattr(event, field_name, None)
        if field_value is not None:
            data[field_name] = _public_json(field_value)
    return _StreamFrame(
        event=kind,
        data=data,
        event_id=str(durable_sequence) if durable_sequence is not None else None,
    )


def _stream_error_frame(exc: Exception) -> _StreamFrame:
    error = _runtime_http_exception(exc)
    if error.status_code == 404:
        code = "run_not_found"
    elif error.status_code == 409:
        code = "run_conflict"
    else:
        code = "runtime_error"
    return _StreamFrame(
        event="stream_error",
        data={"code": code, "message": str(error.detail)},
    )


async def _close_async_iterator(iterator: AsyncIterator[object]) -> None:
    close = getattr(iterator, "aclose", None)
    if close is not None:
        await cast(Callable[[], Awaitable[None]], close)()


async def _runtime_follower(
    runtime: _RuntimeView,
    run_id: str,
    *,
    after_sequence: int,
    live: bool,
) -> AsyncIterator[RuntimeEvent | None]:
    """Yield events or heartbeat sentinels while retaining one pending read."""

    iterator = runtime.follow(
        run_id,
        after_sequence=after_sequence,
        live=live,
    )
    pending: asyncio.Future[RuntimeEvent] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(anext(iterator))
            done, _ = await asyncio.wait(
                {pending},
                timeout=_SSE_HEARTBEAT_S,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                yield None
                continue
            try:
                event = pending.result()
            except StopAsyncIteration:
                break
            finally:
                pending = None
            yield event
            if event.terminal:
                break
    finally:

        async def close_follower() -> None:
            if pending is not None:
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

        cleanup = asyncio.create_task(close_follower())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise


async def _runtime_event_stream(
    runtime: _RuntimeView,
    run_id: str,
    *,
    after_sequence: int,
    live: bool,
) -> AsyncIterator[bytes]:
    follower = _runtime_follower(
        runtime,
        run_id,
        after_sequence=after_sequence,
        live=live,
    )
    try:
        async for event in follower:
            if event is None:
                yield b": ping\n\n"
                continue
            yield _encode_sse(_runtime_event_frame(run_id, event))
    except Exception as exc:
        yield _encode_sse(_stream_error_frame(exc))
    finally:
        await _close_async_iterator(follower)


async def _start_legacy_runtime_run(
    runtime: _RuntimeView,
    *,
    message: str,
    session_id: uuid.UUID | None,
    idempotency_key: str | None,
) -> RunHandle:
    if session_id is not None:
        try:
            existing = await runtime.list_session_runs(str(session_id))
        except Exception as exc:
            raise _runtime_http_exception(exc) from None
        if not existing:
            raise HTTPException(
                status_code=404,
                detail="Chat session was not found or has expired.",
            )
        resolved_session_id = str(session_id)
    else:
        # Runtime session ids are arbitrary strings, while the compatibility
        # response contract promises a UUID. Choose it before submission.
        resolved_session_id = str(
            uuid.uuid5(_LEGACY_SESSION_NAMESPACE, idempotency_key)
            if idempotency_key is not None
            else uuid.uuid4()
        )
    try:
        handle = await runtime.submit(
            StartRun(
                prompt=message,
                session_id=resolved_session_id,
                idempotency_key=idempotency_key,
            )
        )
        _run_handle_payload(handle)
        try:
            uuid.UUID(handle.session_id)
        except ValueError:
            raise HTTPException(
                status_code=502,
                detail="Durable Runtime returned an invalid legacy session id.",
            ) from None
        return handle
    except HTTPException:
        raise
    except Exception as exc:
        raise _runtime_http_exception(exc) from None


async def _wait_for_runtime_snapshot(
    runtime: _RuntimeView,
    run_id: str,
) -> RunSnapshot:
    follower = runtime.follow(run_id, after_sequence=0, live=True)
    try:
        try:
            async with asyncio.timeout(_RUNTIME_RESPONSE_TIMEOUT_S):
                async for event in follower:
                    if event.terminal:
                        break
        finally:
            await _close_async_iterator(follower)
        snapshot = await runtime.load(run_id)
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Agent request timed out. The Runtime may still be running.",
        ) from None
    except HTTPException:
        raise
    except Exception as exc:
        raise _runtime_http_exception(exc) from None
    if snapshot.terminal is None:
        raise HTTPException(
            status_code=502,
            detail="Durable Runtime ended without a terminal result.",
        )
    return snapshot


async def _legacy_runtime_stream(
    runtime: _RuntimeView,
    handle: RunHandle,
) -> AsyncIterator[bytes]:
    """Project a durable Runtime run onto the original ephemeral SSE shape."""

    yield _encode_sse(
        _StreamFrame(
            event="session",
            data={"session_id": str(uuid.UUID(handle.session_id))},
        )
    )
    follower = _runtime_follower(
        runtime,
        handle.run_id,
        after_sequence=0,
        live=True,
    )
    try:
        async for event in follower:
            if event is None:
                yield b": ping\n\n"
                continue
            if event.live_sequence is not None:
                yield _encode_sse(_legacy_runtime_event_frame(event))
            if not event.terminal:
                continue
            snapshot = await runtime.load(handle.run_id)
            public_result = _legacy_runtime_result_payload(snapshot)
            yield _encode_sse(_StreamFrame(event="result", data=public_result))
            yield _encode_sse(
                _StreamFrame(
                    event="done",
                    data={
                        "run_id": snapshot.run_id,
                        "committed": public_result["committed"],
                        "side_effects_possible": public_result[
                            "side_effects_possible"
                        ],
                    },
                )
            )
            return
        yield _encode_sse(
            _StreamFrame(
                event="stream_error",
                data={
                    "code": "agent_error",
                    "message": "Agent request failed. Please try again.",
                    "committed": False,
                },
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        yield _encode_sse(
            _StreamFrame(
                event="stream_error",
                data={
                    "code": "agent_error",
                    "message": "Agent request failed. Please try again.",
                    "committed": False,
                },
            )
        )
    finally:
        await _close_async_iterator(follower)


async def _submit_runtime_command(
    runtime: _RuntimeView,
    command: RunCommand,
) -> dict[str, object]:
    try:
        handle = await runtime.submit(command)
        return _run_handle_payload(handle)
    except HTTPException:
        raise
    except Exception as exc:
        raise _runtime_http_exception(exc) from None


def create_app(
    *,
    agent: ReActAgent | None = None,
    runtime: _RuntimeView | None = None,
    model_name: str = "test-model",
    api_mode: ApiMode = "chat_completions",
    session_store: SessionStore | None = None,
) -> FastAPI:
    """Create the web app; dependency injection keeps API tests fully offline."""

    store = session_store or SessionStore()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned_model: OpenAIModel | None = None
        owned_runtime: AgentRuntime | None = None
        owned_journal: PostgresRunJournal | None = None
        try:
            if agent is None and runtime is None:
                owned_model, configured_agent, configured_model, configured_mode = (
                    _build_agent_from_env()
                )
                metric_cardinality = _build_metric_cardinality_policy(
                    configured_agent,
                    configured_model,
                )
                journal, owned_journal = await _build_journal_from_env()
                owned_runtime = AgentRuntime(
                    configured_agent,
                    journal,
                    model_name=configured_model,
                    telemetry=create_telemetry(cardinality=metric_cardinality),
                    workspace=_build_workspace_from_env(),
                )
                app.state.agent = configured_agent
                app.state.runtime = owned_runtime
                app.state.model_name = configured_model
                app.state.api_mode = configured_mode
            yield
        finally:
            try:
                if owned_runtime is not None:
                    await owned_runtime.close()
            finally:
                try:
                    if owned_journal is not None:
                        await owned_journal.close()
                finally:
                    if owned_model is not None:
                        await owned_model.aclose()

    app = FastAPI(title="ReAct Agent Chat", version="0.1.0", lifespan=lifespan)
    app.state.sessions = store
    app.state.runtime = runtime
    if agent is not None:
        app.state.agent = agent
        app.state.model_name = model_name
        app.state.api_mode = api_mode
    elif runtime is not None:
        app.state.model_name = model_name
        app.state.api_mode = api_mode

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(
            _INDEX_FILE,
            media_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": (
                    "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                    "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
                    "img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'"
                ),
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.get("/api/health", response_model=HealthResponse)
    async def health(request: Request) -> HealthResponse:
        configured_agent = getattr(request.app.state, "agent", None)
        configured_runtime = getattr(request.app.state, "runtime", None)
        if configured_agent is None and configured_runtime is None:
            raise HTTPException(status_code=503, detail="Agent is not configured.")
        return HealthResponse(
            model=cast(str, request.app.state.model_name),
            api_mode=cast(ApiMode, request.app.state.api_mode),
        )

    @app.post("/api/runs", status_code=202)
    async def start_run(payload: StartRunRequest, request: Request) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        prompt = payload.prompt.strip()
        if not prompt:
            raise HTTPException(status_code=422, detail="Prompt must not be blank.")
        session_id = payload.session_id.strip() if payload.session_id is not None else None
        if session_id == "":
            raise HTTPException(status_code=422, detail="Session id must not be blank.")
        command = StartRun(
            prompt=prompt,
            session_id=session_id,
            idempotency_key=_idempotency_key(request),
        )
        response = await _submit_runtime_command(configured_runtime, command)
        return JSONResponse(status_code=202, content=response)

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str, request: Request) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        try:
            snapshot = await configured_runtime.load(run_id)
            response = _snapshot_payload(snapshot)
        except Exception as exc:
            raise _runtime_http_exception(exc) from None
        return JSONResponse(content=response)

    @app.get("/api/runs/{run_id}/events", response_class=StreamingResponse)
    async def run_events(
        run_id: str,
        request: Request,
        after_sequence: Annotated[
            int,
            Query(ge=0, le=_MAX_EVENT_SEQUENCE),
        ] = 0,
        follow: Annotated[bool, Query()] = True,
    ) -> StreamingResponse:
        configured_runtime = _configured_runtime(request)
        resolved_sequence = _last_event_sequence(request, after_sequence)
        try:
            # Preflight existence before sending SSE headers. Async-generator
            # followers otherwise cannot surface an initial 404 out of band.
            snapshot = await configured_runtime.load(run_id)
        except Exception as exc:
            raise _runtime_http_exception(exc) from None
        live = follow and snapshot.terminal is None
        return StreamingResponse(
            _runtime_event_stream(
                configured_runtime,
                run_id,
                after_sequence=resolved_sequence,
                live=live,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store",
                "X-Accel-Buffering": "no",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.get("/api/sessions/{session_id}/runs")
    async def list_session_runs(session_id: str, request: Request) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        try:
            snapshots = await configured_runtime.list_session_runs(session_id)
            response = {
                "session_id": session_id,
                "runs": [_snapshot_payload(snapshot) for snapshot in snapshots],
            }
        except Exception as exc:
            raise _runtime_http_exception(exc) from None
        return JSONResponse(content=response)

    @app.post("/api/runs/{run_id}/resume", status_code=202)
    async def resume_run(run_id: str, request: Request) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        command = ResumeRun(run_id=run_id)
        response = await _submit_runtime_command(configured_runtime, command)
        return JSONResponse(status_code=202, content=response)

    @app.post("/api/runs/{run_id}/fork", status_code=202)
    async def fork_run(
        run_id: str,
        payload: ForkRunRequest,
        request: Request,
    ) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        session_id = payload.session_id.strip() if payload.session_id is not None else None
        if session_id == "":
            raise HTTPException(status_code=422, detail="Session id must not be blank.")
        command = ForkRun(
            run_id=run_id,
            from_sequence=payload.from_sequence,
            session_id=session_id,
            idempotency_key=_idempotency_key(request),
        )
        response = await _submit_runtime_command(configured_runtime, command)
        return JSONResponse(status_code=202, content=response)

    @app.post("/api/runs/{run_id}/resolve", status_code=202)
    async def resolve_run(
        run_id: str,
        payload: ResolveRunRequest,
        request: Request,
    ) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        call_key = payload.call_key.strip()
        if not call_key:
            raise HTTPException(status_code=422, detail="Call key must not be blank.")
        command = ResolveRun(
            run_id=run_id,
            call_key=call_key,
            action=payload.action,
            result=payload.result,
        )
        response = await _submit_runtime_command(configured_runtime, command)
        return JSONResponse(status_code=202, content=response)

    @app.post("/api/runs/{run_id}/cost-adjustments")
    async def adjust_run_cost(
        run_id: str,
        payload: AdjustCostRequest,
        request: Request,
    ) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        previous_record_id = payload.previous_record_id.strip()
        if not previous_record_id:
            raise HTTPException(
                status_code=422,
                detail="Previous cost record id must not be blank.",
            )
        record_id = payload.record_id.strip() if payload.record_id is not None else None
        if record_id == "":
            raise HTTPException(status_code=422, detail="Cost record id must not be blank.")
        note = payload.note.strip() if payload.note is not None else None
        note = note or None
        idempotency_key = _idempotency_key(request)
        operation_id = (
            "http-cost-adjustment:"
            + uuid.uuid5(
                _COST_ADJUSTMENT_OPERATION_NAMESPACE,
                f"{run_id}\0{idempotency_key}",
            ).hex
            if idempotency_key is not None
            else f"http-cost-adjustment:{uuid.uuid4().hex}"
        )
        handle_payload = await _submit_runtime_command(
            configured_runtime,
            AdjustCost(
                run_id=run_id,
                previous_record_id=previous_record_id,
                revised_total_micros=payload.revised_total_microunits,
                note=note,
                operation_id=operation_id,
                record_id=record_id,
            ),
        )
        try:
            snapshot = await configured_runtime.load(run_id)
        except Exception as exc:
            raise _runtime_http_exception(exc) from None
        response = _snapshot_payload(snapshot)
        adjustment = next(
            (
                item
                for item in reversed(snapshot.costs)
                if item.get("adjustment_operation_id") == operation_id
            ),
            None,
        )
        response["adjustment"] = {
            "created": bool(handle_payload["created"]),
            "record": _public_json(adjustment),
        }
        return JSONResponse(
            status_code=201 if bool(handle_payload["created"]) else 200,
            content=response,
        )

    @app.post("/api/runs/{run_id}/cancel", status_code=202)
    async def cancel_run(
        run_id: str,
        payload: CancelRunRequest,
        request: Request,
    ) -> JSONResponse:
        configured_runtime = _configured_runtime(request)
        reason = payload.reason.strip()
        if not reason:
            raise HTTPException(status_code=422, detail="Cancel reason must not be blank.")
        command = CancelRun(run_id=run_id, reason=reason)
        response = await _submit_runtime_command(configured_runtime, command)
        return JSONResponse(status_code=202, content=response)

    @app.post("/api/chat", response_model=ChatResponse)
    async def chat(payload: ChatRequest, request: Request) -> ChatResponse:
        message = payload.message.strip()
        if not message:
            raise HTTPException(status_code=422, detail="Message must not be blank.")
        configured_runtime = cast(
            _RuntimeView | None,
            getattr(request.app.state, "runtime", None),
        )
        if configured_runtime is not None:
            handle = await _start_legacy_runtime_run(
                configured_runtime,
                message=message,
                session_id=payload.session_id,
                idempotency_key=_idempotency_key(request),
            )
            snapshot = await _wait_for_runtime_snapshot(
                configured_runtime,
                handle.run_id,
            )
            try:
                return _chat_response_from_snapshot(snapshot)
            except (TypeError, ValueError):
                raise HTTPException(
                    status_code=502,
                    detail="Durable Runtime returned an invalid result.",
                ) from None
        configured_agent = cast(ReActAgent | None, getattr(request.app.state, "agent", None))
        if configured_agent is None:
            raise HTTPException(status_code=503, detail="Agent is not configured.")
        resolved_id: uuid.UUID | None = None
        try:
            async with store.lease(payload.session_id) as (resolved_id, session):
                try:
                    result = await configured_agent.run(message, history=session.transcript)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise _AgentExecutionError from None
                if result.status is RunStatus.COMPLETED:
                    session.transcript = result.transcript
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc.args[0])) from None
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except asyncio.CancelledError:
            if payload.session_id is None and resolved_id is not None:
                await asyncio.shield(store.delete(resolved_id))
            raise
        except _AgentExecutionError:
            if payload.session_id is None and resolved_id is not None:
                await store.delete(resolved_id)
            raise HTTPException(
                status_code=502,
                detail="Agent request failed. Please try again.",
            ) from None
        assert resolved_id is not None
        return _chat_response(resolved_id, result)

    @app.post("/api/chat/stream", response_class=StreamingResponse)
    async def chat_stream(payload: ChatRequest, request: Request) -> StreamingResponse:
        """Stream an ephemeral, ordered Agent trace without persisting debug data."""

        message = payload.message.strip()
        if not message:
            raise HTTPException(status_code=422, detail="Message must not be blank.")
        configured_runtime = cast(
            _RuntimeView | None,
            getattr(request.app.state, "runtime", None),
        )
        if configured_runtime is not None:
            handle = await _start_legacy_runtime_run(
                configured_runtime,
                message=message,
                session_id=payload.session_id,
                idempotency_key=_idempotency_key(request),
            )
            return StreamingResponse(
                _legacy_runtime_stream(configured_runtime, handle),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache, no-store",
                    "X-Accel-Buffering": "no",
                    "X-Content-Type-Options": "nosniff",
                },
            )
        configured_agent = cast(ReActAgent | None, getattr(request.app.state, "agent", None))
        if configured_agent is None:
            raise HTTPException(status_code=503, detail="Agent is not configured.")

        # Enter the lease before response headers are sent so an expired session remains
        # a normal HTTP 404 instead of becoming an in-band streaming error.
        lease = store.lease(payload.session_id)
        try:
            resolved_id, session = await lease.__aenter__()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc.args[0])) from None
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        frames: asyncio.Queue[_StreamFrame] = asyncio.Queue(maxsize=256)
        committed = False
        result_available = False
        tool_started_seen = False

        async def publish(event: AgentStreamEvent) -> None:
            nonlocal tool_started_seen
            if event.kind.value == "tool_started":
                tool_started_seen = True
            await frames.put(
                _StreamFrame(
                    event=event.kind.value,
                    data=_agent_stream_payload(event),
                    event_id=str(event.sequence),
                )
            )

        async def produce() -> None:
            nonlocal committed, result_available
            try:
                result = await configured_agent.run(
                    message,
                    history=session.transcript,
                    stream_sink=publish,
                )
                committed = result.status is RunStatus.COMPLETED
                if committed:
                    # Commit once, while the session lease is still held, and only then
                    # publish the authoritative result.
                    session.transcript = result.transcript
                public_result = _chat_response(resolved_id, result).model_dump(mode="json")
                public_result.update(
                    {
                        "run_id": result.run_id,
                        "tool_executions": result.tool_executions,
                        "committed": committed,
                        "side_effects_possible": (
                            not committed
                            and (result.tool_executions > 0 or tool_started_seen)
                        ),
                    }
                )
                result_available = True
                await frames.put(_StreamFrame(event="result", data=public_result))
                await frames.put(
                    _StreamFrame(
                        event="done",
                        data={
                            "run_id": result.run_id,
                            "committed": committed,
                            "side_effects_possible": (
                                not committed
                                and (result.tool_executions > 0 or tool_started_seen)
                            ),
                        },
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await frames.put(
                    _StreamFrame(
                        event="stream_error",
                        data={
                            "code": "agent_error",
                            "message": "Agent request failed. Please try again.",
                            "committed": False,
                        },
                    )
                )

        async def stream_body() -> AsyncIterator[bytes]:
            producer = asyncio.create_task(produce())
            try:
                yield _encode_sse(
                    _StreamFrame(
                        event="session",
                        data={"session_id": str(resolved_id)},
                    )
                )
                while True:
                    if producer.done() and frames.empty():
                        await producer
                        break

                    next_frame = asyncio.create_task(frames.get())
                    done, _ = await asyncio.wait(
                        {next_frame, producer},
                        timeout=15.0,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if next_frame in done:
                        frame = next_frame.result()
                        yield _encode_sse(frame)
                        continue

                    next_frame.cancel()
                    await asyncio.gather(next_frame, return_exceptions=True)
                    if producer in done:
                        await producer
                        if frames.empty():
                            break
                        continue
                    yield b": ping\n\n"

            finally:
                if not producer.done():
                    producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)

                async def release_session() -> None:
                    await lease.__aexit__(None, None, None)
                    if payload.session_id is None and not result_available:
                        await store.delete(resolved_id)

                cleanup = asyncio.create_task(release_session())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Keep releasing the lock/session even if the transport task is
                    # already cancelled. The original cancellation still propagates.
                    await asyncio.shield(cleanup)
                    raise

        return StreamingResponse(
            stream_body(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store",
                "X-Accel-Buffering": "no",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.delete("/api/sessions/{session_id}", status_code=204)
    async def clear_session(session_id: uuid.UUID) -> Response:
        await store.delete(session_id)
        return Response(status_code=204)

    return app


app = create_app()


def main() -> None:
    host = os.getenv("REACT_AGENT_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("REACT_AGENT_WEB_PORT", "8000"))
    uvicorn.run("react_agent.web:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
