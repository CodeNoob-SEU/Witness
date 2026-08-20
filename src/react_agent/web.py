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
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal, cast

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .agent import AgentConfig, ReActAgent
from .models import AgentResult, AgentStreamEvent, RunStatus, TranscriptItem
from .provider import ApiMode, OpenAIModel, ProviderCapabilities
from .tools import DebugExposure, tool

_STATIC_DIR = Path(__file__).with_name("static")
_INDEX_FILE = _STATIC_DIR / "index.html"


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


@dataclass(frozen=True, slots=True)
class _StreamFrame:
    event: str
    data: dict[str, object]
    event_id: str | None = None


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
        ),
    )
    return model, agent, model_name, api_mode


def _chat_response(session_id: uuid.UUID, result: AgentResult) -> ChatResponse:
    public_error = None
    if result.status is not RunStatus.COMPLETED:
        public_error = {
            "wall_time": "Agent request timed out. Please try again.",
            "model_refusal": "The model declined this request.",
            "model_incomplete": "The model returned an incomplete response.",
            "max_steps": "The Agent reached its step limit.",
            "max_tool_calls": "The Agent reached its tool-call limit.",
            "context_limit": "This conversation is too long to continue.",
            "loop_detected": "The Agent stopped after detecting a repeated action.",
        }.get(result.stop_reason.value, "Agent request failed. Please try again.")
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
        error=public_error,
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


def create_app(
    *,
    agent: ReActAgent | None = None,
    model_name: str = "test-model",
    api_mode: ApiMode = "chat_completions",
    session_store: SessionStore | None = None,
) -> FastAPI:
    """Create the web app; dependency injection keeps API tests fully offline."""

    store = session_store or SessionStore()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned_model: OpenAIModel | None = None
        if agent is None:
            owned_model, configured_agent, configured_model, configured_mode = (
                _build_agent_from_env()
            )
            app.state.agent = configured_agent
            app.state.model_name = configured_model
            app.state.api_mode = configured_mode
        try:
            yield
        finally:
            if owned_model is not None:
                await owned_model.aclose()

    app = FastAPI(title="ReAct Agent Chat", version="0.1.0", lifespan=lifespan)
    app.state.sessions = store
    if agent is not None:
        app.state.agent = agent
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
        if configured_agent is None:
            raise HTTPException(status_code=503, detail="Agent is not configured.")
        return HealthResponse(
            model=cast(str, request.app.state.model_name),
            api_mode=cast(ApiMode, request.app.state.api_mode),
        )

    @app.post("/api/chat", response_model=ChatResponse)
    async def chat(payload: ChatRequest, request: Request) -> ChatResponse:
        message = payload.message.strip()
        if not message:
            raise HTTPException(status_code=422, detail="Message must not be blank.")
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
