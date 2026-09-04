"""The bounded ReAct-style orchestration loop."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .context import (
    ContextCompressionLifecycleEvent,
    ContextGovernor,
    ContextStrategy,
    ModelContextCompressor,
)
from .errors import ConfigurationError, ModelInvocationError
from .models import (
    AgentEvent,
    AgentJournalEvent,
    AgentJournalEventKind,
    AgentResult,
    AgentResumeState,
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
    TranscriptItem,
    Usage,
    UserMessage,
    agent_event_to_json,
    tool_action_fingerprint,
    transcript_item_to_json,
    transcript_to_json,
)
from .prompts import DEFAULT_INSTRUCTIONS
from .provider import Model, StreamingModel
from .tools import ApprovalHandler, DebugExposure, Tool, ToolRegistry

EventSink = Callable[[AgentEvent], Awaitable[None] | None]
StreamSink = Callable[[AgentStreamEvent], Awaitable[None] | None]
JournalSink = Callable[[AgentJournalEvent], Awaitable[None] | None]
SideEffectGuard = Callable[[], Awaitable[None] | None]


def _usage_data(usage: Usage) -> dict[str, JsonValue]:
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
        "cached_input_tokens": usage.cached_input_tokens,
        "reasoning_output_tokens": usage.reasoning_output_tokens,
        "billable_tokens": usage.billable_tokens,
    }


def _tool_checkpoint(message: ToolMessage) -> dict[str, JsonValue]:
    """Separate the model-facing ToolMessage from journal-only evidence."""

    checkpoint: dict[str, JsonValue] = {
        "message": transcript_item_to_json(message),
    }
    if message.private_payload:
        checkpoint["tool_private"] = dict(message.private_payload)
    return checkpoint


_STREAM_KIND_BY_EVENT_KIND: dict[EventKind, AgentStreamEventKind] = {
    EventKind.RUN_STARTED: AgentStreamEventKind.RUN_STARTED,
    EventKind.CONTEXT_GOVERNED: AgentStreamEventKind.CONTEXT_GOVERNED,
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
    context_strategy: ContextStrategy = ContextStrategy.TIERED
    context_keep_recent_turns: int = 2
    context_summary_max_chars: int = 12_000
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
        if not isinstance(self.context_strategy, ContextStrategy):
            raise ConfigurationError("context_strategy must be a ContextStrategy")
        if self.context_keep_recent_turns < 1:
            raise ConfigurationError("context_keep_recent_turns must be positive")
        if self.context_summary_max_chars < 64:
            raise ConfigurationError("context_summary_max_chars must be at least 64")
        if self.repeated_action_limit < 2:
            raise ConfigurationError("repeated_action_limit must be at least 2")


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
        journal_sink: JournalSink | None = None,
        context_governor: ContextGovernor | None = None,
    ) -> None:
        if not instructions.strip():
            raise ConfigurationError("instructions must not be empty")
        self.model = model
        self.registry = tools if isinstance(tools, ToolRegistry) else ToolRegistry(tools)
        self.instructions = instructions
        self.config = config or AgentConfig()
        self.approval_handler = approval_handler
        self.event_sink = event_sink
        self.journal_sink = journal_sink
        self.context_governor = context_governor or ContextGovernor(
            strategy=self.config.context_strategy,
            compressor=ModelContextCompressor(model),
            keep_recent_turns=self.config.context_keep_recent_turns,
            max_summary_chars=self.config.context_summary_max_chars,
        )

    @property
    def tool_manifest_hash(self) -> str:
        payload = [
            {
                "spec": {
                    "name": registered.spec.name,
                    "description": registered.spec.description,
                    "parameters": registered.spec.parameters,
                    "strict": registered.spec.strict,
                },
                "version": registered.version,
                "resume_policy": registered.resume_policy.value,
                "context_policy": {
                    "effect": registered.context_policy.effect.value,
                    "identity_fields": registered.context_policy.identity_fields,
                },
                "captures_private_result": registered.captures_private_result,
            }
            for registered in self.registry.tools
        ]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()

    @property
    def revision(self) -> str:
        payload = {
            "instructions": self.instructions,
            "config": asdict(self.config),
            "context_governor_revision": self.context_governor.revision,
            "tool_manifest_hash": self.tool_manifest_hash,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()

    async def run(
        self,
        prompt: str,
        *,
        history: Sequence[TranscriptItem] = (),
        run_id: str | None = None,
        execution_id: str | None = None,
        event_sink: EventSink | None = None,
        stream_sink: StreamSink | None = None,
        journal_sink: JournalSink | None = None,
        resume_state: AgentResumeState | None = None,
        side_effect_guard: SideEffectGuard | None = None,
        workspace_path: Path | None = None,
    ) -> AgentResult:
        """Run one fenced execution and always release resource-owning tools."""

        current_run_id = run_id or uuid.uuid4().hex
        current_execution_id = execution_id or uuid.uuid4().hex
        try:
            return await self._run(
                prompt,
                history=history,
                run_id=current_run_id,
                execution_id=current_execution_id,
                event_sink=event_sink,
                stream_sink=stream_sink,
                journal_sink=journal_sink,
                resume_state=resume_state,
                side_effect_guard=side_effect_guard,
                workspace_path=workspace_path,
            )
        finally:
            await self.registry.finalize_execution(
                current_run_id,
                current_execution_id,
            )

    async def _run(
        self,
        prompt: str,
        *,
        history: Sequence[TranscriptItem] = (),
        run_id: str | None = None,
        execution_id: str | None = None,
        event_sink: EventSink | None = None,
        stream_sink: StreamSink | None = None,
        journal_sink: JournalSink | None = None,
        resume_state: AgentResumeState | None = None,
        side_effect_guard: SideEffectGuard | None = None,
        workspace_path: Path | None = None,
    ) -> AgentResult:
        """Run until a final answer or an explicit budget/protocol terminal state."""

        if resume_state is None and not prompt.strip():
            raise ConfigurationError("prompt must not be empty")

        current_run_id = run_id or uuid.uuid4().hex
        current_execution_id = execution_id or uuid.uuid4().hex
        transcript: list[TranscriptItem] = (
            [*history, UserMessage(prompt)]
            if resume_state is None
            else list(resume_state.transcript)
        )
        events: list[AgentEvent] = []
        safe_sink = event_sink or self.event_sink
        durable_sink = journal_sink or self.journal_sink
        started = time.monotonic()
        deadline = started + self.config.max_wall_time_s
        model_calls = resume_state.model_calls if resume_state is not None else 0
        tool_calls = resume_state.tool_calls if resume_state is not None else 0
        tool_executions = resume_state.tool_executions if resume_state is not None else 0
        usage = resume_state.usage if resume_state is not None else Usage()
        executed: dict[str, tuple[str, ToolMessage]] = {}
        if resume_state is not None:
            for recovered in resume_state.completed_tools:
                if recovered.completed is not None:
                    executed[recovered.call.id] = (
                        tool_action_fingerprint(recovered.call),
                        recovered.completed,
                    )
        action_counts: dict[str, int] = (
            dict(resume_state.action_counts) if resume_state is not None else {}
        )
        recovered_attempts = (
            {item.call_key: item.attempts for item in resume_state.pending_tools}
            if resume_state is not None
            else {}
        )
        terminal_call_keys: set[str] = set()
        model_first_delta_at: dict[int, float] = {}
        stream_sequence = 0
        stream_lock = asyncio.Lock()
        context_metrics: dict[str, int] = {
            "projections": 0,
            "peak_input_chars": 0,
            "peak_projected_chars": 0,
            "deterministic_evictions": 0,
            "deterministic_removed_chars": 0,
            "compression_calls": 0,
            "compression_cache_hits": 0,
            "compression_source_chars": 0,
            "hard_fallbacks": 0,
            "hard_dropped_items": 0,
        }

        async def record(
            kind: AgentJournalEventKind,
            operation_id: str,
            *,
            step: int | None = None,
            call: ToolCall | None = None,
            call_key: str | None = None,
            public_data: dict[str, JsonValue] | None = None,
            private_data: dict[str, JsonValue] | None = None,
        ) -> None:
            """Deliver a durable fact; failures deliberately abort orchestration."""

            if durable_sink is None:
                return
            journal_event = AgentJournalEvent(
                kind=kind,
                run_id=current_run_id,
                execution_id=current_execution_id,
                operation_id=operation_id,
                timestamp=time.time(),
                step=step,
                call_key=call_key,
                tool_call_id=call.id if call else None,
                tool_name=call.name if call else None,
                public_data=MappingProxyType(dict(public_data or {})),
                private_data=MappingProxyType(dict(private_data or {})),
            )
            if inspect.iscoroutinefunction(durable_sink):
                outcome: Any = durable_sink(journal_event)
            else:
                outcome = await asyncio.to_thread(durable_sink, journal_event)
            if inspect.isawaitable(outcome):
                await outcome

        async def guard_side_effect() -> None:
            """Fail closed when the Runtime no longer owns the fencing lease."""

            if side_effect_guard is None:
                return
            guard_outcome = side_effect_guard()
            if inspect.isawaitable(guard_outcome):
                await guard_outcome

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
            timestamp: float | None = None,
            step: int | None = None,
            call: ToolCall | None = None,
            call_key: str | None = None,
            data: dict[str, JsonValue] | None = None,
            stream_data: dict[str, JsonValue] | None = None,
        ) -> None:
            event = AgentEvent(
                kind=kind,
                run_id=current_run_id,
                timestamp=time.time() if timestamp is None else timestamp,
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
                envelope_truncated = bool(isinstance(meta, dict) and meta.get("truncated") is True)
            except (json.JSONDecodeError, TypeError):
                pass
            data: dict[str, JsonValue] = {
                "status": "error" if message.is_error else "completed",
                "outcome": "error" if message.is_error else "completed",
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
                context_metrics=MappingProxyType(dict(context_metrics)),
            )

        async def finish(final: AgentResult, *, step: int) -> AgentResult:
            completed_at = time.time()
            completion_event = AgentEvent(
                kind=EventKind.RUN_COMPLETED,
                run_id=current_run_id,
                timestamp=completed_at,
                step=step,
                data=MappingProxyType({"status": final.status.value}),
            )
            await record(
                AgentJournalEventKind.RUN_COMPLETED,
                "run:completed",
                step=step,
                public_data={
                    "status": final.status.value,
                    "stop_reason": final.stop_reason.value,
                    "model_calls": final.model_calls,
                    "tool_calls": final.tool_calls,
                    "tool_executions": final.tool_executions,
                    "usage": _usage_data(final.usage),
                    "context": dict(final.context_metrics),
                },
                private_data={
                    "output": final.output,
                    "error": final.error,
                    "transcript": transcript_to_json(final.transcript),
                    "context_metrics": dict(final.context_metrics),
                    "agent_events": [
                        agent_event_to_json(event)
                        for event in (*final.events, completion_event)
                    ],
                },
            )
            await emit(
                EventKind.RUN_COMPLETED,
                timestamp=completed_at,
                step=step,
                data={"status": final.status.value},
                stream_data={
                    "status": final.status.value,
                    "stop_reason": final.stop_reason.value,
                    "model_calls": final.model_calls,
                    "tool_calls": final.tool_calls,
                    "tool_executions": final.tool_executions,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    "usage": _usage_data(final.usage),
                    "context": dict(final.context_metrics),
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
            attempt = recovered_attempts.get(call_key, 0) + 1
            fingerprint = tool_action_fingerprint(call)
            cached_entry = executed.get(call.id)
            if cached_entry is not None:
                cached_fingerprint, cached_result = cached_entry
                if cached_fingerprint == fingerprint:
                    reused_result = replace(
                        cached_result,
                        cached=True,
                        executed=False,
                        duration_ms=0.0,
                    )
                    reused_public = result_debug_data(
                        call,
                        reused_result,
                        cached=True,
                    )
                    await record(
                        AgentJournalEventKind.TOOL_REUSED,
                        f"tool:{call_key}:reused:{current_execution_id}",
                        step=step,
                        call=call,
                        call_key=call_key,
                        public_data=reused_public,
                        private_data=_tool_checkpoint(reused_result),
                    )
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
                conflict_public = result_debug_data(call, conflict)
                await record(
                    AgentJournalEventKind.TOOL_COMPLETED,
                    f"tool:{call_key}:completed",
                    step=step,
                    call=call,
                    call_key=call_key,
                    public_data=conflict_public,
                    private_data=_tool_checkpoint(conflict),
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

            await record(
                AgentJournalEventKind.TOOL_STARTED,
                f"tool:{call_key}:started:{current_execution_id}",
                step=step,
                call=call,
                call_key=call_key,
                public_data={
                    "resume_policy": (
                        registered.resume_policy.value
                        if (registered := self.registry.get(call.name)) is not None
                        else "require_operator"
                    ),
                    "argument_chars": len(call.arguments),
                    "attempt": attempt,
                },
                private_data={
                    "call": {"id": call.id, "name": call.name, "arguments": call.arguments}
                },
            )
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
                execution_id=current_execution_id,
                approval_handler=self.approval_handler,
                max_output_chars=self.config.max_tool_output_chars,
                call_key=call_key,
                attempt=attempt,
                before_invoke=guard_side_effect,
                workspace_path=workspace_path,
            )
            if tool_result.executed:
                tool_executions += 1
            executed[call.id] = (fingerprint, tool_result)
            completed_public = result_debug_data(call, tool_result)
            await record(
                AgentJournalEventKind.TOOL_COMPLETED,
                f"tool:{call_key}:completed",
                step=step,
                call=call,
                call_key=call_key,
                public_data=completed_public,
                private_data=_tool_checkpoint(tool_result),
            )
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
            return [await limited(call, tool_index) for tool_index, call in enumerate(calls)]

        def model_stream_forwarder(
            current_step: int,
        ) -> Callable[[ModelStreamEvent], Awaitable[None]]:
            streamed_tool_names: dict[int, str] = {}
            streamed_tool_call_ids: dict[int, str] = {}
            streamed_argument_chars: dict[int, int] = {}
            exposed_argument_chars: dict[int, int] = {}

            async def forward(event: ModelStreamEvent) -> None:
                if event.delta and current_step not in model_first_delta_at:
                    model_first_delta_at[current_step] = time.monotonic()
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

        if resume_state is None:
            await record(
                AgentJournalEventKind.RUN_STARTED,
                "run:started",
                public_data={
                    "history_items": len(history),
                    "agent_revision": self.revision,
                    "tool_manifest_hash": self.tool_manifest_hash,
                },
                private_data={
                    "prompt": prompt,
                    "history": transcript_to_json(history),
                    "transcript": transcript_to_json(transcript),
                },
            )
        else:
            await record(
                AgentJournalEventKind.RUN_RESUMED,
                f"run:resumed:{current_execution_id}",
                step=resume_state.next_step,
                public_data={
                    "model_calls": model_calls,
                    "tool_calls": tool_calls,
                    "tool_executions": tool_executions,
                },
                private_data={"transcript": transcript_to_json(transcript)},
            )
        await emit(
            EventKind.RUN_STARTED,
            data={"history_items": len(history), "resumed": resume_state is not None},
            stream_data={
                "history_items": len(history),
                "resumed": resume_state is not None,
                "max_steps": self.config.max_steps,
                "max_tool_calls": self.config.max_tool_calls,
                "max_wall_time_s": self.config.max_wall_time_s,
                "max_concurrent_tools": self.config.max_concurrent_tools,
                "max_context_chars": self.config.max_context_chars,
                "context_strategy": self.config.context_strategy.value,
                "context_keep_recent_turns": self.config.context_keep_recent_turns,
                "context_summary_max_chars": self.config.context_summary_max_chars,
                "parallel_tool_calls": self.config.parallel_tool_calls,
                "repeated_action_limit": self.config.repeated_action_limit,
            },
        )

        start_step = resume_state.next_step if resume_state is not None else 1
        if resume_state is not None and resume_state.pending_tools:
            recovered_calls = tuple(item.call for item in resume_state.pending_tools)
            recovered_step = resume_state.next_step
            try:
                recovered_observations = await within_deadline(
                    execute_batch(recovered_calls, recovered_step)
                )
            except TimeoutError:
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{recovered_step}:wall_time",
                    step=recovered_step,
                    public_data={
                        "reason": "wall_time",
                        "status": RunStatus.TIMED_OUT.value,
                        "stop_reason": StopReason.WALL_TIME.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
                final = result(RunStatus.TIMED_OUT, StopReason.WALL_TIME)
                return await finish(final, step=recovered_step)
            transcript.extend(recovered_observations)
            for recovered in resume_state.pending_tools:
                registered_tool = self.registry.get(recovered.call.name)
                if registered_tool is not None and registered_tool.allow_repeated:
                    continue
                signature = tool_action_fingerprint(recovered.call)
                action_counts[signature] = action_counts.get(signature, 0) + 1
                if action_counts[signature] >= self.config.repeated_action_limit:
                    await record(
                        AgentJournalEventKind.LOOP_DETECTED,
                        f"loop:{recovered.call_key}",
                        step=recovered_step,
                        call=recovered.call,
                        call_key=recovered.call_key,
                        public_data={
                            "action_fingerprint": signature,
                            "repeat_count": action_counts[signature],
                            "repeat_limit": self.config.repeated_action_limit,
                            "status": RunStatus.PARTIAL.value,
                            "stop_reason": StopReason.LOOP_DETECTED.value,
                            "terminal_decision": True,
                        },
                        private_data={"transcript": transcript_to_json(transcript)},
                    )
                    final = result(RunStatus.PARTIAL, StopReason.LOOP_DETECTED)
                    return await finish(final, step=recovered_step)
            start_step = recovered_step + 1

        for step in range(start_step, self.config.max_steps + 1):
            tool_specs = self.registry.specs

            async def record_compression_lifecycle(
                event: ContextCompressionLifecycleEvent,
                lifecycle_step: int = step,
            ) -> None:
                await record(
                    AgentJournalEventKind.CONTEXT_GOVERNED,
                    (
                        f"context:s{lifecycle_step}:compression:{event.phase.value}:"
                        f"{current_execution_id}"
                    ),
                    step=lifecycle_step,
                    public_data={
                        "compression_phase": event.phase.value,
                        "compression_source_chars": event.source_chars,
                        "compressor_revision": event.compressor_revision,
                        "attempted_model_calls": event.attempted_model_calls,
                        "compression_calls": event.attempted_model_calls,
                        "usage": _usage_data(event.usage),
                        "request_id": event.request_id,
                        "response_model": event.response_model,
                        "cost_unknown": event.cost_unknown,
                        "compression_error": event.error,
                    },
                    private_data={
                        "context_compression": {
                            "summary_key": event.summary_key,
                            "source_hash": event.source_hash,
                        }
                    },
                )

            try:
                projection = await within_deadline(
                    self.context_governor.prepare(
                        transcript,
                        instructions=self.instructions,
                        tool_specs=tool_specs,
                        tool_policies={
                            registered.name: registered.context_policy
                            for registered in self.registry.tools
                        },
                        hard_limit=self.config.max_context_chars,
                        compression_event_sink=record_compression_lifecycle,
                    )
                )
            except TimeoutError:
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{step}:wall_time",
                    step=step,
                    public_data={
                        "reason": "wall_time",
                        "status": RunStatus.TIMED_OUT.value,
                        "stop_reason": StopReason.WALL_TIME.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
                await emit(EventKind.BUDGET_EXHAUSTED, step=step, data={"reason": "wall_time"})
                final = result(RunStatus.TIMED_OUT, StopReason.WALL_TIME)
                return await finish(final, step=step)

            report = projection.report
            model_calls += report.compression_calls
            usage = usage + report.compression_usage
            context_metrics["projections"] += 1
            context_metrics["peak_input_chars"] = max(
                context_metrics["peak_input_chars"], report.input_chars
            )
            context_metrics["peak_projected_chars"] = max(
                context_metrics["peak_projected_chars"], report.final_chars
            )
            context_metrics["deterministic_evictions"] += len(report.evictions)
            context_metrics["deterministic_removed_chars"] += (
                report.deterministic_removed_chars
            )
            context_metrics["compression_calls"] += report.compression_calls
            context_metrics["compression_cache_hits"] += int(report.compression_cache_hit)
            context_metrics["compression_source_chars"] += report.compression_source_chars
            context_metrics["hard_fallbacks"] += int(report.hard_fallback)
            context_metrics["hard_dropped_items"] += report.hard_dropped_items
            eviction_reasons: dict[str, JsonValue] = {}
            for eviction in report.evictions:
                key = eviction.reason.value
                current_count = eviction_reasons.get(key, 0)
                eviction_reasons[key] = (
                    current_count + 1 if isinstance(current_count, int) else 1
                )
            governance_changed = bool(
                report.evictions
                or report.summary_key
                or report.hard_fallback
                or report.overflow
                or report.compression_error
            )
            if governance_changed:
                context_public: dict[str, JsonValue] = {
                    "strategy": report.strategy.value,
                    "compression_phase": "projection_completed",
                    "input_chars": report.input_chars,
                    "deterministic_chars": report.deterministic_chars,
                    "context_chars": report.final_chars,
                    "evictions": len(report.evictions),
                    "eviction_reasons": eviction_reasons,
                    "deterministic_removed_chars": report.deterministic_removed_chars,
                    "compression_calls": report.compression_calls,
                    "compression_cache_hit": report.compression_cache_hit,
                    "compression_source_chars": report.compression_source_chars,
                    "hard_fallback": report.hard_fallback,
                    "hard_dropped_items": report.hard_dropped_items,
                    "overflow": report.overflow,
                    "compression_error": report.compression_error,
                    "compression_accounted_in_terminal": bool(report.summary_key),
                    "request_id": report.compression_request_id,
                    "response_model": report.compression_response_model,
                    "usage": _usage_data(report.compression_usage),
                }
                await record(
                    AgentJournalEventKind.CONTEXT_GOVERNED,
                    f"context:s{step}:governed:{current_execution_id}",
                    step=step,
                    public_data=context_public,
                    private_data={
                        "context_projection": transcript_to_json(projection.transcript),
                        **(
                            {
                                "context_compression": {
                                    "summary_key": report.summary_key,
                                }
                            }
                            if report.summary_key is not None
                            else {}
                        ),
                    },
                )
                await emit(
                    EventKind.CONTEXT_GOVERNED,
                    step=step,
                    data={
                        "strategy": report.strategy.value,
                        "input_chars": report.input_chars,
                        "context_chars": report.final_chars,
                        "evictions": len(report.evictions),
                        "compression_calls": report.compression_calls,
                        "hard_fallback": report.hard_fallback,
                        "overflow": report.overflow,
                    },
                    stream_data=context_public,
                )
            context_chars = report.final_chars
            if report.overflow:
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{step}:context_limit",
                    step=step,
                    public_data={
                        "reason": "context_limit",
                        "context_chars": context_chars,
                        "status": RunStatus.PARTIAL.value,
                        "stop_reason": StopReason.CONTEXT_LIMIT.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
                await emit(
                    EventKind.BUDGET_EXHAUSTED,
                    step=step,
                    data={"reason": "context_limit", "context_chars": context_chars},
                )
                final = result(RunStatus.PARTIAL, StopReason.CONTEXT_LIMIT)
                return await finish(final, step=step)
            model_started_at = time.monotonic()
            await record(
                AgentJournalEventKind.MODEL_STARTED,
                f"model:s{step}:started:{current_execution_id}",
                step=step,
                public_data={
                    "model_call": model_calls + 1,
                    "context_chars": context_chars,
                },
                private_data={"transcript": transcript_to_json(transcript)},
            )
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
                transcript=projection.transcript,
                tools=tool_specs,
                instructions=self.instructions,
                parallel_tool_calls=self.config.parallel_tool_calls,
            )
            try:
                await guard_side_effect()
                if stream_sink is not None and isinstance(self.model, StreamingModel):
                    model_call = self.model.complete_stream(request, forward_model_event)
                else:
                    model_call = self.model.complete(request)
                response = await within_deadline(model_call)
            except TimeoutError:
                await record(
                    AgentJournalEventKind.MODEL_FAILED,
                    f"model:s{step}:failed:{current_execution_id}",
                    step=step,
                    public_data={
                        "error_type": "timeout",
                        "request_id": None,
                        "status": RunStatus.TIMED_OUT.value,
                        "stop_reason": StopReason.WALL_TIME.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{step}:wall_time",
                    step=step,
                    public_data={
                        "reason": "wall_time",
                        "status": RunStatus.TIMED_OUT.value,
                        "stop_reason": StopReason.WALL_TIME.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
                await emit(EventKind.BUDGET_EXHAUSTED, step=step, data={"reason": "wall_time"})
                final = result(RunStatus.TIMED_OUT, StopReason.WALL_TIME)
                return await finish(final, step=step)
            except ModelInvocationError as exc:
                await record(
                    AgentJournalEventKind.MODEL_FAILED,
                    f"model:s{step}:failed:{current_execution_id}",
                    step=step,
                    public_data={
                        "error_type": type(exc).__name__,
                        "request_id": exc.request_id,
                        "status": RunStatus.FAILED.value,
                        "stop_reason": StopReason.MODEL_ERROR.value,
                        "terminal_decision": True,
                    },
                    private_data={
                        "error": str(exc),
                        "transcript": transcript_to_json(transcript),
                    },
                )
                await emit(
                    EventKind.MODEL_FAILED,
                    step=step,
                    data={
                        "error_type": type(exc).__name__,
                        "request_id": exc.request_id,
                        "duration_ms": round((time.monotonic() - model_started_at) * 1000, 3),
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
                await record(
                    AgentJournalEventKind.MODEL_FAILED,
                    f"model:s{step}:failed:{current_execution_id}",
                    step=step,
                    public_data={
                        "error_type": type(exc).__name__,
                        "request_id": None,
                        "status": RunStatus.FAILED.value,
                        "stop_reason": StopReason.MODEL_ERROR.value,
                        "terminal_decision": True,
                    },
                    private_data={
                        "error": f"Model adapter failed: {type(exc).__name__}",
                        "transcript": transcript_to_json(transcript),
                    },
                )
                await emit(
                    EventKind.MODEL_FAILED,
                    step=step,
                    data={
                        "error_type": type(exc).__name__,
                        "request_id": None,
                        "duration_ms": round((time.monotonic() - model_started_at) * 1000, 3),
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
            terminal_status: RunStatus | None = None
            terminal_reason: StopReason | None = None
            terminal_output: str | None = None
            terminal_error: str | None = None
            terminal_observations: list[ToolMessage] = []
            ids = [call.id for call in message.tool_calls]
            malformed = any(not call.id or not call.name for call in message.tool_calls)
            if response.outcome is not ModelOutcome.COMPLETED:
                code = (
                    "MODEL_REFUSAL"
                    if response.outcome is ModelOutcome.REFUSED
                    else "MODEL_OUTPUT_INCOMPLETE"
                )
                terminal_observations = [
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
                ]
                terminal_status = (
                    RunStatus.FAILED
                    if response.outcome is ModelOutcome.REFUSED
                    else RunStatus.PARTIAL
                )
                terminal_reason = (
                    StopReason.MODEL_REFUSAL
                    if response.outcome is ModelOutcome.REFUSED
                    else StopReason.MODEL_INCOMPLETE
                )
                terminal_output = message.content
                terminal_error = response.diagnostic
            elif not message.tool_calls:
                if message.content is None or not message.content.strip():
                    terminal_status = RunStatus.FAILED
                    terminal_reason = StopReason.PROTOCOL_ERROR
                    terminal_error = (
                        "Model returned neither tool calls nor a final answer."
                    )
                else:
                    terminal_status = RunStatus.COMPLETED
                    terminal_reason = StopReason.COMPLETED
                    terminal_output = message.content
            elif malformed or len(ids) != len(set(ids)):
                terminal_status = RunStatus.FAILED
                terminal_reason = StopReason.PROTOCOL_ERROR
                terminal_error = (
                    "Model returned a malformed or duplicate tool call id/name."
                )

            first_delta_at = model_first_delta_at.get(step)
            ttfc_ms = (
                round((first_delta_at - model_started_at) * 1000, 3)
                if first_delta_at is not None
                else None
            )
            model_public: dict[str, JsonValue] = {
                "tool_calls": len(message.tool_calls),
                "output_chars": len(message.content or ""),
                # The accumulated provider-neutral answer is the durable
                # correction for an interrupted/reconnected live stream.
                # Raw provider items and reasoning state remain excluded.
                "final_text": message.content,
                "request_id": response.request_id,
                "response_model": response.response_model,
                "finish_reason": response.finish_reason,
                "outcome": response.outcome.value,
                "duration_ms": round((time.monotonic() - model_started_at) * 1000, 3),
                "ttfc_ms": ttfc_ms,
                "usage": _usage_data(response.usage),
            }
            terminal_transcript = [*transcript, *terminal_observations]
            model_private: dict[str, JsonValue] = {
                "message": transcript_item_to_json(message),
                "transcript": transcript_to_json(terminal_transcript),
            }
            if terminal_status is not None and terminal_reason is not None:
                model_public.update(
                    {
                        "status": terminal_status.value,
                        "stop_reason": terminal_reason.value,
                        "terminal_decision": True,
                    }
                )
                model_private.update(
                    {
                        "output": terminal_output,
                        "error": terminal_error,
                    }
                )
            await record(
                AgentJournalEventKind.MODEL_COMPLETED,
                f"model:s{step}:completed",
                step=step,
                public_data=model_public,
                private_data=model_private,
            )
            await emit(
                EventKind.MODEL_COMPLETED,
                step=step,
                data={
                    "tool_calls": len(message.tool_calls),
                    "output_chars": len(message.content or ""),
                    "request_id": response.request_id,
                    "response_model": response.response_model,
                    "finish_reason": response.finish_reason,
                    "outcome": response.outcome.value,
                    "duration_ms": round((time.monotonic() - model_started_at) * 1000, 3),
                    "ttfc_ms": ttfc_ms,
                },
                stream_data={
                    "model_call": model_calls,
                    "tool_calls": len(message.tool_calls),
                    "output_chars": len(message.content or ""),
                    "request_id": response.request_id,
                    "response_model": response.response_model,
                    "finish_reason": response.finish_reason,
                    "outcome": response.outcome.value,
                    "duration_ms": round((time.monotonic() - model_started_at) * 1000, 3),
                    "ttfc_ms": ttfc_ms,
                    "usage": _usage_data(response.usage),
                },
            )

            if terminal_status is not None and terminal_reason is not None:
                if terminal_observations:
                    tool_calls += len(terminal_observations)
                    transcript.extend(terminal_observations)
                final = result(
                    terminal_status,
                    terminal_reason,
                    output=terminal_output,
                    error=terminal_error,
                )
                return await finish(final, step=step)

            for tool_index, call in enumerate(message.tool_calls):
                call_key = f"s{step}:t{tool_index}"
                planned_public = argument_debug_data(call)
                planned_public["resume_policy"] = (
                    registered.resume_policy.value
                    if (registered := self.registry.get(call.name)) is not None
                    else "require_operator"
                )
                await record(
                    AgentJournalEventKind.TOOL_PLANNED,
                    f"tool:{call_key}:planned",
                    step=step,
                    call=call,
                    call_key=call_key,
                    public_data=planned_public,
                    private_data={
                        "call": {
                            "id": call.id,
                            "name": call.name,
                            "arguments": call.arguments,
                        }
                    },
                )
                await emit_stream(
                    AgentStreamEventKind.MODEL_TOOL_CALL_READY,
                    step=step,
                    call=call,
                    call_key=call_key,
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
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{step}:max_tool_calls",
                    step=step,
                    public_data={
                        "reason": "max_tool_calls",
                        "status": RunStatus.PARTIAL.value,
                        "stop_reason": StopReason.MAX_TOOL_CALLS.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
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
                await record(
                    AgentJournalEventKind.BUDGET_EXHAUSTED,
                    f"budget:s{step}:wall_time",
                    step=step,
                    public_data={
                        "reason": "wall_time",
                        "status": RunStatus.TIMED_OUT.value,
                        "stop_reason": StopReason.WALL_TIME.value,
                        "terminal_decision": True,
                    },
                    private_data={"transcript": transcript_to_json(transcript)},
                )
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
                signature = tool_action_fingerprint(call)
                action_counts[signature] = action_counts.get(signature, 0) + 1
                if action_counts[signature] >= self.config.repeated_action_limit:
                    call_key = f"s{step}:t{tool_index}"
                    await record(
                        AgentJournalEventKind.LOOP_DETECTED,
                        f"loop:{call_key}",
                        step=step,
                        call=call,
                        call_key=call_key,
                        public_data={
                            "action_fingerprint": signature,
                            "repeat_count": action_counts[signature],
                            "repeat_limit": self.config.repeated_action_limit,
                            "status": RunStatus.PARTIAL.value,
                            "stop_reason": StopReason.LOOP_DETECTED.value,
                            "terminal_decision": True,
                        },
                        private_data={"transcript": transcript_to_json(transcript)},
                    )
                    await emit(
                        EventKind.LOOP_DETECTED,
                        step=step,
                        call=call,
                        call_key=call_key,
                        data={
                            "repeat_count": action_counts[signature],
                            "repeat_limit": self.config.repeated_action_limit,
                        },
                    )
                    final = result(RunStatus.PARTIAL, StopReason.LOOP_DETECTED)
                    return await finish(final, step=step)

        await record(
            AgentJournalEventKind.BUDGET_EXHAUSTED,
            f"budget:s{self.config.max_steps}:max_steps",
            step=self.config.max_steps,
            public_data={
                "reason": "max_steps",
                "status": RunStatus.PARTIAL.value,
                "stop_reason": StopReason.MAX_STEPS.value,
                "terminal_decision": True,
            },
            private_data={"transcript": transcript_to_json(transcript)},
        )
        await emit(
            EventKind.BUDGET_EXHAUSTED,
            step=self.config.max_steps,
            data={"reason": "max_steps"},
        )
        final = result(RunStatus.PARTIAL, StopReason.MAX_STEPS)
        return await finish(final, step=self.config.max_steps)
