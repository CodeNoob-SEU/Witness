"""The bounded ReAct-style orchestration loop."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any

from .errors import ConfigurationError, ModelInvocationError
from .models import (
    AgentEvent,
    AgentResult,
    AgentStreamEvent,
    AgentStreamEventKind,
    EventKind,
    JsonValue,
    ModelOutcome,
    ModelRequest,
    ModelStreamEvent,
    ModelStreamEventKind,
    RunStatus,
    StopReason,
    ToolCall,
    ToolMessage,
    ToolSpec,
    TranscriptItem,
    Usage,
    UserMessage,
)
from .prompts import DEFAULT_INSTRUCTIONS
from .provider import Model, StreamingModel
from .tools import ApprovalHandler, DebugExposure, Tool, ToolRegistry

EventSink = Callable[[AgentEvent], Awaitable[None] | None]
StreamSink = Callable[[AgentStreamEvent], Awaitable[None] | None]


_STREAM_KIND_BY_EVENT_KIND: dict[EventKind, AgentStreamEventKind] = {
    EventKind.RUN_STARTED: AgentStreamEventKind.RUN_STARTED,
    EventKind.MODEL_STARTED: AgentStreamEventKind.MODEL_STARTED,
    EventKind.MODEL_COMPLETED: AgentStreamEventKind.MODEL_COMPLETED,
    EventKind.MODEL_FAILED: AgentStreamEventKind.MODEL_FAILED,
    EventKind.TOOL_STARTED: AgentStreamEventKind.TOOL_STARTED,
    EventKind.TOOL_COMPLETED: AgentStreamEventKind.TOOL_RESULT,
    EventKind.TOOL_REUSED: AgentStreamEventKind.TOOL_REUSED,
    EventKind.BUDGET_EXHAUSTED: AgentStreamEventKind.BUDGET_EXHAUSTED,
    EventKind.LOOP_DETECTED: AgentStreamEventKind.LOOP_DETECTED,
    EventKind.RUN_COMPLETED: AgentStreamEventKind.RUN_COMPLETED,
}


@dataclass(frozen=True, slots=True)
class AgentConfig:
    """Run budgets and execution policy for one run."""

    max_steps: int = 8
    max_tool_calls: int = 32
    max_wall_time_s: float = 120.0
    max_concurrent_tools: int = 8
    max_tool_output_chars: int = 20_000
    max_context_chars: int = 200_000
    parallel_tool_calls: bool = True
    repeated_action_limit: int = 3

    def __post_init__(self) -> None:
        if self.max_steps < 1:
            raise ConfigurationError("max_steps must be at least 1")
        if self.max_tool_calls < 0:
            raise ConfigurationError("max_tool_calls must be non-negative")
        if self.max_wall_time_s <= 0:
            raise ConfigurationError("max_wall_time_s must be positive")
        if self.max_concurrent_tools < 1:
            raise ConfigurationError("max_concurrent_tools must be at least 1")
        if self.max_tool_output_chars < 256:
            raise ConfigurationError("max_tool_output_chars must be at least 256")
        if self.max_context_chars < 1:
            raise ConfigurationError("max_context_chars must be positive")
        if self.repeated_action_limit < 2:
            raise ConfigurationError("repeated_action_limit must be at least 2")


def _fingerprint(call: ToolCall) -> str:
    try:
        parsed = json.loads(call.arguments)
        canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        canonical = call.arguments
    return f"{call.name}\0{canonical}"


def _estimate_context_chars(
    transcript: Sequence[TranscriptItem],
    *,
    instructions: str,
    tool_specs: Sequence[ToolSpec],
) -> int:
    total = len(instructions)
    total += sum(
        len(json.dumps(spec.parameters, ensure_ascii=False, default=str))
        + len(spec.name)
        + len(spec.description)
        for spec in tool_specs
    )
    for item in transcript:
        if isinstance(item, UserMessage):
            total += len(item.content)
        elif isinstance(item, ToolMessage):
            total += len(item.call_id) + len(item.name) + len(item.content)
        elif item.raw_items:
            total += sum(
                len(json.dumps(raw_item, ensure_ascii=False, default=str))
                for raw_item in item.raw_items
            )
        else:
            total += len(item.content or "")
            total += sum(
                len(call.id) + len(call.name) + len(call.arguments)
                for call in item.tool_calls
            )
    return total


class ReActAgent:
    """A deep module: callers provide a goal; the loop owns orchestration details."""

    def __init__(
        self,
        model: Model,
        tools: Iterable[Tool] | ToolRegistry = (),
        *,
        instructions: str = DEFAULT_INSTRUCTIONS,
        config: AgentConfig | None = None,
        approval_handler: ApprovalHandler | None = None,
        event_sink: EventSink | None = None,
    ) -> None:
        if not instructions.strip():
            raise ConfigurationError("instructions must not be empty")
        self.model = model
        self.registry = tools if isinstance(tools, ToolRegistry) else ToolRegistry(tools)
        self.instructions = instructions
        self.config = config or AgentConfig()
        self.approval_handler = approval_handler
        self.event_sink = event_sink

    async def run(
        self,
        prompt: str,
        *,
        history: Sequence[TranscriptItem] = (),
        run_id: str | None = None,
        event_sink: EventSink | None = None,
        stream_sink: StreamSink | None = None,
    ) -> AgentResult:
        """Run until a final answer or an explicit budget/protocol terminal state."""

        if not prompt.strip():
            raise ConfigurationError("prompt must not be empty")

        current_run_id = run_id or uuid.uuid4().hex
        transcript: list[TranscriptItem] = [*history, UserMessage(prompt)]
        events: list[AgentEvent] = []
        safe_sink = event_sink or self.event_sink
        started = time.monotonic()
        deadline = started + self.config.max_wall_time_s
        model_calls = 0
        tool_calls = 0
        tool_executions = 0
        usage = Usage()
        executed: dict[str, tuple[str, ToolMessage]] = {}
        action_counts: dict[str, int] = {}
        terminal_call_keys: set[str] = set()
        stream_sequence = 0
        stream_lock = asyncio.Lock()

        async def emit_stream(
            kind: AgentStreamEventKind,
            *,
            step: int | None = None,
            call: ToolCall | None = None,
            call_key: str | None = None,
            data: dict[str, JsonValue] | None = None,
            tool_call_id: str | None = None,
            tool_name: str | None = None,
        ) -> None:
            """Serialize rich observer delivery without retaining the event."""

            nonlocal stream_sequence
            if stream_sink is None:
                return
            async with stream_lock:
                stream_sequence += 1
                event = AgentStreamEvent(
                    kind=kind,
                    run_id=current_run_id,
                    sequence=stream_sequence,
                    timestamp=time.time(),
                    step=step,
                    call_key=call_key,
                    tool_call_id=(call.id if call is not None else tool_call_id),
                    tool_name=(call.name if call is not None else tool_name),
                    data=MappingProxyType(dict(data or {})),
                )
                try:
                    if inspect.iscoroutinefunction(stream_sink):
                        outcome: Any = stream_sink(event)
                    else:
                        outcome = await asyncio.to_thread(stream_sink, event)
                    if inspect.isawaitable(outcome):
                        await outcome
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Debug observers must not change the agent's result.
                    pass

        async def emit(
            kind: EventKind,
            *,
            step: int | None = None,
            call: ToolCall | None = None,
            call_key: str | None = None,
            data: dict[str, JsonValue] | None = None,
            stream_data: dict[str, JsonValue] | None = None,
        ) -> None:
            event = AgentEvent(
                kind=kind,
                run_id=current_run_id,
                timestamp=time.time(),
                step=step,
                tool_call_id=call.id if call else None,
                tool_name=call.name if call else None,
                data=MappingProxyType(dict(data or {})),
            )
            events.append(event)
            if safe_sink is not None:
                remaining = deadline - time.monotonic()
                if remaining > 0:

                    async def deliver() -> None:
                        if inspect.iscoroutinefunction(safe_sink):
                            outcome: Any = safe_sink(event)
                        else:
                            outcome = await asyncio.to_thread(safe_sink, event)
                        if inspect.isawaitable(outcome):
                            await outcome

                    try:
                        async with asyncio.timeout(remaining):
                            await deliver()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
            await emit_stream(
                _STREAM_KIND_BY_EVENT_KIND[kind],
                step=step,
                call=call,
                call_key=call_key,
                data=stream_data if stream_data is not None else data,
            )

        def exposure_for(call: ToolCall) -> DebugExposure:
            registered_tool = self.registry.get(call.name)
            if registered_tool is None:
                return DebugExposure.METADATA
            return registered_tool.debug_exposure

        def bounded_debug_text(value: str) -> tuple[str, bool]:
            limit = self.config.max_tool_output_chars
            if len(value) <= limit:
                return value, False
            return value[:limit], True

        def argument_debug_data(call: ToolCall) -> dict[str, JsonValue]:
            exposure = exposure_for(call)
            data: dict[str, JsonValue] = {
                "status": "ready",
                "exposure": exposure.value,
                "argument_chars": len(call.arguments),
            }
            if exposure is DebugExposure.FULL:
                arguments, truncated = bounded_debug_text(call.arguments)
                data.update({"arguments": arguments, "truncated": truncated})
            return data

        def result_debug_data(
            call: ToolCall,
            message: ToolMessage,
            *,
            cached: bool | None = None,
        ) -> dict[str, JsonValue]:
            exposure = exposure_for(call)
            envelope_truncated = False
            try:
                payload = json.loads(message.content)
                meta = payload.get("meta") if isinstance(payload, dict) else None
                envelope_truncated = bool(
                    isinstance(meta, dict) and meta.get("truncated") is True
                )
            except (json.JSONDecodeError, TypeError):
                pass
            data: dict[str, JsonValue] = {
                "status": "error" if message.is_error else "completed",
                "exposure": exposure.value,
                "is_error": message.is_error,
                "cached": message.cached if cached is None else cached,
                "executed": message.executed,
                "duration_ms": round(message.duration_ms, 3),
                "result_chars": len(message.content),
                "truncated": envelope_truncated,
            }
            if exposure is DebugExposure.FULL:
                content, debug_truncated = bounded_debug_text(message.content)
                data.update(
                    {
                        "result": content,
                        "truncated": envelope_truncated or debug_truncated,
                    }
                )
            return data

        def result(
            status: RunStatus,
            reason: StopReason,
            *,
            output: str | None = None,
            error: str | None = None,
        ) -> AgentResult:
            return AgentResult(
                output=output,
                status=status,
                stop_reason=reason,
                run_id=current_run_id,
                model_calls=model_calls,
                tool_calls=tool_calls,
                tool_executions=tool_executions,
                usage=usage,
                transcript=tuple(transcript),
                events=tuple(events),
                error=error,
            )

        async def finish(final: AgentResult, *, step: int) -> AgentResult:
            await emit(
                EventKind.RUN_COMPLETED,
                step=step,
                data={"status": final.status.value},
                stream_data={
                    "status": final.status.value,
                    "stop_reason": final.stop_reason.value,
                    "model_calls": final.model_calls,
                    "tool_calls": final.tool_calls,
                    "tool_executions": final.tool_executions,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    "usage": {
                        "input_tokens": final.usage.input_tokens,
                        "output_tokens": final.usage.output_tokens,
                        "total_tokens": final.usage.total_tokens,
                    },
                },
            )
            return replace(final, events=tuple(events))

        async def within_deadline(awaitable: Awaitable[Any]) -> Any:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if inspect.iscoroutine(awaitable):
                    awaitable.close()
                raise TimeoutError
            async with asyncio.timeout(remaining):
                return await awaitable

        async def execute_one(call: ToolCall, step: int, tool_index: int) -> ToolMessage:
            nonlocal tool_executions
            call_key = f"s{step}:t{tool_index}"
            fingerprint = _fingerprint(call)
            cached_entry = executed.get(call.id)
            if cached_entry is not None:
                cached_fingerprint, cached_result = cached_entry
                if cached_fingerprint == fingerprint:
                    reused_result = replace(cached_result, cached=True, duration_ms=0.0)
                    await emit(
                        EventKind.TOOL_REUSED,
                        step=step,
                        call=call,
                        call_key=call_key,
                        stream_data=result_debug_data(call, reused_result, cached=True),
                    )
                    terminal_call_keys.add(call_key)
                    return reused_result
                conflict = ToolMessage(
                    call_id=call.id,
                    name=call.name,
                    content=json.dumps(
                        {
                            "ok": False,
                            "error": {
                                "code": "DUPLICATE_CALL_ID",
                                "message": (
                                    "The provider reused a call id with different arguments."
                                ),
                                "retryable": False,
                            },
                        },
                        separators=(",", ":"),
                    ),
                    is_error=True,
                )
                await emit(
                    EventKind.TOOL_COMPLETED,
                    step=step,
                    call=call,
                    call_key=call_key,
                    data={
                        "is_error": True,
                        "cached": False,
                        "duration_ms": 0.0,
                        "output_chars": len(conflict.content),
                    },
                    stream_data=result_debug_data(call, conflict),
                )
                terminal_call_keys.add(call_key)
                return conflict

            await emit(
                EventKind.TOOL_STARTED,
                step=step,
                call=call,
                call_key=call_key,
                stream_data={
                    "status": "running",
                    "exposure": exposure_for(call).value,
                    "argument_chars": len(call.arguments),
                },
            )
            tool_result = await self.registry.execute(
                call,
                run_id=current_run_id,
                approval_handler=self.approval_handler,
                max_output_chars=self.config.max_tool_output_chars,
            )
            if tool_result.executed:
                tool_executions += 1
            executed[call.id] = (fingerprint, tool_result)
            await emit(
                EventKind.TOOL_COMPLETED,
                step=step,
                call=call,
                call_key=call_key,
                data={
                    "is_error": tool_result.is_error,
                    "cached": False,
                    "duration_ms": round(tool_result.duration_ms, 3),
                    "output_chars": len(tool_result.content),
                },
                stream_data=result_debug_data(call, tool_result),
            )
            terminal_call_keys.add(call_key)
            return tool_result

        async def execute_batch(calls: tuple[ToolCall, ...], step: int) -> list[ToolMessage]:
            semaphore = asyncio.Semaphore(self.config.max_concurrent_tools)

            async def limited(call: ToolCall, tool_index: int) -> ToolMessage:
                async with semaphore:
                    return await execute_one(call, step, tool_index)

            can_parallelize = self.config.parallel_tool_calls and all(
                (registered := self.registry.get(call.name)) is None or registered.parallel_safe
                for call in calls
            )
            if can_parallelize:
                # gather preserves request order even when execution completion order differs.
                return list(
                    await asyncio.gather(
                        *(limited(call, tool_index) for tool_index, call in enumerate(calls))
                    )
                )
            return [
                await limited(call, tool_index) for tool_index, call in enumerate(calls)
            ]

        def model_stream_forwarder(
            current_step: int,
        ) -> Callable[[ModelStreamEvent], Awaitable[None]]:
            streamed_tool_names: dict[int, str] = {}
            streamed_tool_call_ids: dict[int, str] = {}
            streamed_argument_chars: dict[int, int] = {}
            exposed_argument_chars: dict[int, int] = {}

            async def forward(event: ModelStreamEvent) -> None:
                if event.kind is ModelStreamEventKind.TEXT_DELTA:
                    await emit_stream(
                        AgentStreamEventKind.MODEL_TEXT_DELTA,
                        step=current_step,
                        data={"delta": event.delta},
                    )
                    return
                if event.kind is ModelStreamEventKind.REFUSAL_DELTA:
                    await emit_stream(
                        AgentStreamEventKind.MODEL_REFUSAL_DELTA,
                        step=current_step,
                        data={"delta": event.delta},
                    )
                    return

                tool_index = event.tool_index if event.tool_index is not None else 0
                if event.tool_name:
                    streamed_tool_names[tool_index] = event.tool_name
                if event.tool_call_id:
                    streamed_tool_call_ids[tool_index] = event.tool_call_id
                tool_name = streamed_tool_names.get(tool_index)
                tool_call_id = streamed_tool_call_ids.get(tool_index)
                total_chars = streamed_argument_chars.get(tool_index, 0) + len(event.delta)
                streamed_argument_chars[tool_index] = total_chars
                registered_tool = self.registry.get(tool_name) if tool_name else None
                exposure = (
                    registered_tool.debug_exposure
                    if registered_tool is not None
                    else DebugExposure.METADATA
                )
                delta_data: dict[str, JsonValue] = {
                    "exposure": exposure.value,
                    "delta_chars": len(event.delta),
                    "argument_chars": total_chars,
                }
                if exposure is DebugExposure.FULL:
                    already_exposed = exposed_argument_chars.get(tool_index, 0)
                    remaining = max(0, self.config.max_tool_output_chars - already_exposed)
                    exposed_delta = event.delta[:remaining]
                    exposed_argument_chars[tool_index] = already_exposed + len(exposed_delta)
                    delta_data.update(
                        {
                            "delta": exposed_delta,
                            "truncated": len(exposed_delta) < len(event.delta),
                        }
                    )
                await emit_stream(
                    AgentStreamEventKind.MODEL_TOOL_CALL_DELTA,
                    step=current_step,
                    call_key=f"s{current_step}:t{tool_index}",
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                    data=delta_data,
                )

            return forward

        await emit(
            EventKind.RUN_STARTED,
            data={"history_items": len(history)},
            stream_data={
                "history_items": len(history),
                "max_steps": self.config.max_steps,
                "max_tool_calls": self.config.max_tool_calls,
                "max_wall_time_s": self.config.max_wall_time_s,
                "max_concurrent_tools": self.config.max_concurrent_tools,
                "max_context_chars": self.config.max_context_chars,
                "parallel_tool_calls": self.config.parallel_tool_calls,
                "repeated_action_limit": self.config.repeated_action_limit,
            },
        )

        for step in range(1, self.config.max_steps + 1):
            tool_specs = self.registry.specs
            context_chars = _estimate_context_chars(
                transcript,
                instructions=self.instructions,
                tool_specs=tool_specs,
            )
            if context_chars > self.config.max_context_chars:
                await emit(
                    EventKind.BUDGET_EXHAUSTED,
                    step=step,
                    data={"reason": "context_limit", "context_chars": context_chars},
                )
                final = result(RunStatus.PARTIAL, StopReason.CONTEXT_LIMIT)
                return await finish(final, step=step)
            model_started_at = time.monotonic()
            await emit(
                EventKind.MODEL_STARTED,
                step=step,
                stream_data={
                    "model_call": model_calls + 1,
                    "context_chars": context_chars,
                    "max_context_chars": self.config.max_context_chars,
                    "remaining_wall_time_ms": round(
                        max(0.0, deadline - time.monotonic()) * 1000, 3
                    ),
                },
            )
            model_calls += 1
            forward_model_event = model_stream_forwarder(step)

            request = ModelRequest(
                transcript=tuple(transcript),
                tools=tool_specs,
                instructions=self.instructions,
                parallel_tool_calls=self.config.parallel_tool_calls,
            )
            try:
                if stream_sink is not None and isinstance(self.model, StreamingModel):
                    model_call = self.model.complete_stream(request, forward_model_event)
                else:
                    model_call = self.model.complete(request)
                response = await within_deadline(model_call)
            except TimeoutError:
                await emit(EventKind.BUDGET_EXHAUSTED, step=step, data={"reason": "wall_time"})
                final = result(RunStatus.TIMED_OUT, StopReason.WALL_TIME)
                return await finish(final, step=step)
            except ModelInvocationError as exc:
                await emit(
                    EventKind.MODEL_FAILED,
                    step=step,
                    data={
                        "error_type": type(exc).__name__,
                        "request_id": exc.request_id,
                        "duration_ms": round(
                            (time.monotonic() - model_started_at) * 1000, 3
                        ),
                    },
                )
                final = result(
                    RunStatus.FAILED,
                    StopReason.MODEL_ERROR,
                    error=str(exc),
                )
                return await finish(final, step=step)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await emit(
                    EventKind.MODEL_FAILED,
                    step=step,
                    data={
                        "error_type": type(exc).__name__,
                        "request_id": None,
                        "duration_ms": round(
                            (time.monotonic() - model_started_at) * 1000, 3
                        ),
                    },
                )
                final = result(
                    RunStatus.FAILED,
                    StopReason.MODEL_ERROR,
                    error=f"Model adapter failed: {type(exc).__name__}",
                )
                return await finish(final, step=step)

            usage = usage + response.usage
            message = response.message
            transcript.append(message)
            await emit(
                EventKind.MODEL_COMPLETED,
                step=step,
                data={
                    "tool_calls": len(message.tool_calls),
                    "output_chars": len(message.content or ""),
                    "request_id": response.request_id,
                    "finish_reason": response.finish_reason,
                    "outcome": response.outcome.value,
                    "duration_ms": round(
                        (time.monotonic() - model_started_at) * 1000, 3
                    ),
                },
                stream_data={
                    "model_call": model_calls,
                    "tool_calls": len(message.tool_calls),
                    "output_chars": len(message.content or ""),
                    "request_id": response.request_id,
                    "finish_reason": response.finish_reason,
                    "outcome": response.outcome.value,
                    "duration_ms": round(
                        (time.monotonic() - model_started_at) * 1000, 3
                    ),
                    "usage": {
                        "input_tokens": response.usage.input_tokens,
                        "output_tokens": response.usage.output_tokens,
                        "total_tokens": response.usage.total_tokens,
                    },
                },
            )

            if response.outcome is not ModelOutcome.COMPLETED:
                if message.tool_calls:
                    tool_calls += len(message.tool_calls)
                    code = (
                        "MODEL_REFUSAL"
                        if response.outcome is ModelOutcome.REFUSED
                        else "MODEL_OUTPUT_INCOMPLETE"
                    )
                    transcript.extend(
                        ToolMessage(
                            call_id=call.id,
                            name=call.name,
                            content=json.dumps(
                                {
                                    "ok": False,
                                    "error": {
                                        "code": code,
                                        "message": (
                                            "The model did not return a complete executable call."
                                        ),
                                        "retryable": False,
                                    },
                                },
                                separators=(",", ":"),
                            ),
                            is_error=True,
                        )
                        for call in message.tool_calls
                    )
                if response.outcome is ModelOutcome.REFUSED:
                    final = result(
                        RunStatus.FAILED,
                        StopReason.MODEL_REFUSAL,
                        output=message.content,
                        error=response.diagnostic,
                    )
                else:
                    final = result(
                        RunStatus.PARTIAL,
                        StopReason.MODEL_INCOMPLETE,
                        output=message.content,
                        error=response.diagnostic,
                    )
                return await finish(final, step=step)

            if not message.tool_calls:
                if message.content is None or not message.content.strip():
                    final = result(
                        RunStatus.FAILED,
                        StopReason.PROTOCOL_ERROR,
                        error="Model returned neither tool calls nor a final answer.",
                    )
                else:
                    final = result(
                        RunStatus.COMPLETED,
                        StopReason.COMPLETED,
                        output=message.content,
                    )
                return await finish(final, step=step)

            ids = [call.id for call in message.tool_calls]
            malformed = any(not call.id or not call.name for call in message.tool_calls)
            if malformed or len(ids) != len(set(ids)):
                final = result(
                    RunStatus.FAILED,
                    StopReason.PROTOCOL_ERROR,
                    error="Model returned a malformed or duplicate tool call id/name.",
                )
                return await finish(final, step=step)

            for tool_index, call in enumerate(message.tool_calls):
                await emit_stream(
                    AgentStreamEventKind.MODEL_TOOL_CALL_READY,
                    step=step,
                    call=call,
                    call_key=f"s{step}:t{tool_index}",
                    data=argument_debug_data(call),
                )

            requested = len(message.tool_calls)
            if tool_calls + requested > self.config.max_tool_calls:
                tool_calls += requested
                budget_results = [
                    ToolMessage(
                        call_id=call.id,
                        name=call.name,
                        content=json.dumps(
                            {
                                "ok": False,
                                "error": {
                                    "code": "TOOL_BUDGET_EXCEEDED",
                                    "message": "The run's tool-call budget was exhausted.",
                                    "retryable": False,
                                },
                            },
                            separators=(",", ":"),
                        ),
                        is_error=True,
                    )
                    for call in message.tool_calls
                ]
                transcript.extend(budget_results)
                await emit(
                    EventKind.BUDGET_EXHAUSTED,
                    step=step,
                    data={"reason": "max_tool_calls"},
                )
                for tool_index, (call, budget_result) in enumerate(
                    zip(message.tool_calls, budget_results, strict=True)
                ):
                    call_key = f"s{step}:t{tool_index}"
                    await emit_stream(
                        AgentStreamEventKind.TOOL_RESULT,
                        step=step,
                        call=call,
                        call_key=call_key,
                        data=result_debug_data(call, budget_result),
                    )
                    terminal_call_keys.add(call_key)
                final = result(RunStatus.PARTIAL, StopReason.MAX_TOOL_CALLS)
                return await finish(final, step=step)

            tool_calls += requested
            try:
                observations = await within_deadline(execute_batch(message.tool_calls, step))
            except TimeoutError:
                # Pair every requested call before returning. Some sync functions running in
                # threads may outlive cancellation; high-risk tools belong in killable workers.
                observations = [
                    ToolMessage(
                        call_id=call.id,
                        name=call.name,
                        content=json.dumps(
                            {
                                "ok": False,
                                "error": {
                                    "code": "RUN_TIMEOUT",
                                    "message": "The run deadline expired during tool execution.",
                                    "retryable": False,
                                },
                            },
                            separators=(",", ":"),
                        ),
                        is_error=True,
                    )
                    for call in message.tool_calls
                ]
                transcript.extend(observations)
                await emit(EventKind.BUDGET_EXHAUSTED, step=step, data={"reason": "wall_time"})
                for tool_index, (call, timeout_result) in enumerate(
                    zip(message.tool_calls, observations, strict=True)
                ):
                    call_key = f"s{step}:t{tool_index}"
                    if call_key in terminal_call_keys:
                        continue
                    await emit_stream(
                        AgentStreamEventKind.TOOL_RESULT,
                        step=step,
                        call=call,
                        call_key=call_key,
                        data=result_debug_data(call, timeout_result),
                    )
                    terminal_call_keys.add(call_key)
                final = result(RunStatus.TIMED_OUT, StopReason.WALL_TIME)
                return await finish(final, step=step)

            transcript.extend(observations)
            for tool_index, call in enumerate(message.tool_calls):
                registered_tool = self.registry.get(call.name)
                if registered_tool is not None and registered_tool.allow_repeated:
                    continue
                signature = _fingerprint(call)
                action_counts[signature] = action_counts.get(signature, 0) + 1
                if action_counts[signature] >= self.config.repeated_action_limit:
                    await emit(
                        EventKind.LOOP_DETECTED,
                        step=step,
                        call=call,
                        call_key=f"s{step}:t{tool_index}",
                        data={
                            "repeat_count": action_counts[signature],
                            "repeat_limit": self.config.repeated_action_limit,
                        },
                    )
                    final = result(RunStatus.PARTIAL, StopReason.LOOP_DETECTED)
                    return await finish(final, step=step)

        await emit(
            EventKind.BUDGET_EXHAUSTED,
            step=self.config.max_steps,
            data={"reason": "max_steps"},
        )
        final = result(RunStatus.PARTIAL, StopReason.MAX_STEPS)
        return await finish(final, step=self.config.max_steps)
