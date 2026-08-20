from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from react_agent.agent import AgentConfig, ReActAgent
from react_agent.errors import ModelInvocationError
from react_agent.events import RunEventDraft, RunEventKind, RunState, StoredRunEvent
from react_agent.journal import InMemoryRunJournal, JournalLease
from react_agent.models import (
    AssistantMessage,
    ModelOutcome,
    ModelRequest,
    ModelResponse,
    ToolCall,
)
from react_agent.postgres_journal import RequestConflictError
from react_agent.runtime import AgentRuntime, InMemoryRuntimeStore, ResumeRun, StartRun
from react_agent.tools import tool


class _SimulatedProcessCrash(BaseException):
    """Stop immediately before the canonical result-ready fact commits."""


class CrashBeforeResultReadyJournal(InMemoryRunJournal):
    def __init__(self) -> None:
        super().__init__()
        self.result_ready_attempted = asyncio.Event()
        self._armed = True

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        if self._armed and operation_id == "run:result_ready":
            self._armed = False
            self.result_ready_attempted.set()
            raise _SimulatedProcessCrash
        return await super().append(
            run_id,
            draft,
            expected_sequence=expected_sequence,
            operation_id=operation_id,
            lease=lease,
        )


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            pytest.fail("terminal recovery must not call the provider")
        return self.responses.popleft()


class FailOnceRequestConflictStore(InMemoryRuntimeStore):
    def __init__(self) -> None:
        super().__init__()
        self.commit_attempted = asyncio.Event()
        self._armed = True

    async def commit_session(
        self,
        session_id: str,
        *,
        expected_version: int,
        transcript: Sequence[Mapping[str, Any]],
        operation_id: str | None = None,
    ) -> object:
        if self._armed:
            self._armed = False
            self.commit_attempted.set()
            raise RequestConflictError("simulated idempotent-content conflict")
        return await super().commit_session(
            session_id,
            expected_version=expected_version,
            transcript=transcript,
            operation_id=operation_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "response",
        "expected_status",
        "expected_reason",
        "expected_output",
        "expected_session_version",
    ),
    (
        (
            ModelResponse(AssistantMessage("done")),
            "completed",
            "completed",
            "done",
            1,
        ),
        (
            ModelResponse(
                AssistantMessage("declined"),
                outcome=ModelOutcome.REFUSED,
                diagnostic="policy",
            ),
            "failed",
            "model_refusal",
            "declined",
            0,
        ),
        (
            ModelResponse(
                AssistantMessage("partial"),
                outcome=ModelOutcome.INCOMPLETE,
                diagnostic="length",
            ),
            "partial",
            "model_incomplete",
            "partial",
            0,
        ),
        (
            ModelResponse(AssistantMessage(None)),
            "failed",
            "protocol_error",
            None,
            0,
        ),
    ),
)
async def test_resume_rebuilds_terminal_model_outcome_without_another_provider_call(
    response: ModelResponse,
    expected_status: str,
    expected_reason: str,
    expected_output: str | None,
    expected_session_version: int,
) -> None:
    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    first_model = ScriptedModel(response)
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(prompt="finish once", session_id="terminal-model", idempotency_key="request")
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(ReActAgent(recovery_model), journal, store=store)
    before_resume = (await recovery_runtime.load(handle.run_id)).last_sequence
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    resumed_events = await journal.read(handle.run_id, after_sequence=before_resume)

    assert completed.state is RunState.TERMINAL
    assert completed.status == expected_status
    assert completed.stop_reason == expected_reason
    assert completed.result is not None
    assert completed.result["output"] == expected_output
    assert len(first_model.requests) == 1
    assert not recovery_model.requests
    assert (await store.load_session("terminal-model")).version == expected_session_version
    assert not any(
        event.kind in {RunEventKind.MODEL_STARTED, RunEventKind.TOOL_STARTED}
        for event in resumed_events
    )
    await recovery_runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_status", "expected_reason", "expected_tool_calls"),
    (
        (
            ModelResponse(
                AssistantMessage(
                    "declined",
                    (ToolCall("call-refused", "side_effect", '{"value":1}'),),
                ),
                outcome=ModelOutcome.REFUSED,
            ),
            "failed",
            "model_refusal",
            1,
        ),
        (
            ModelResponse(
                AssistantMessage(
                    "partial",
                    (ToolCall("call-incomplete", "side_effect", '{"value":1}'),),
                ),
                outcome=ModelOutcome.INCOMPLETE,
            ),
            "partial",
            "model_incomplete",
            1,
        ),
        (
            ModelResponse(
                AssistantMessage(
                    None,
                    (
                        ToolCall("duplicate", "side_effect", '{"value":1}'),
                        ToolCall("duplicate", "side_effect", '{"value":2}'),
                    ),
                )
            ),
            "failed",
            "protocol_error",
            0,
        ),
    ),
)
async def test_terminal_model_tool_calls_are_never_executed_after_resume(
    response: ModelResponse,
    expected_status: str,
    expected_reason: str,
    expected_tool_calls: int,
) -> None:
    invocations = 0

    @tool
    def side_effect(value: int) -> int:
        """Count calls that terminal model outcomes must never execute."""

        nonlocal invocations
        invocations += 1
        return value

    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(response), [side_effect]),
        journal,
        store=store,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="do not execute", session_id="terminal-tool", idempotency_key="request")
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    assert invocations == 0
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [side_effect]),
        journal,
        store=store,
    )
    before_resume = (await recovery_runtime.load(handle.run_id)).last_sequence
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    resumed_events = await journal.read(handle.run_id, after_sequence=before_resume)

    assert completed.state is RunState.TERMINAL
    assert completed.status == expected_status
    assert completed.stop_reason == expected_reason
    assert completed.counts.tool_calls == expected_tool_calls
    assert invocations == 0
    assert not recovery_model.requests
    assert not any(
        event.kind in {
            RunEventKind.TOOL_PLANNED,
            RunEventKind.TOOL_STARTED,
            RunEventKind.TOOL_COMPLETED,
            RunEventKind.TOOL_REUSED,
        }
        for event in resumed_events
    )
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_preserves_max_tool_call_budget_without_executing_planned_tool() -> None:
    invocations = 0

    @tool
    def side_effect(value: int) -> int:
        """Count calls rejected by the run's tool-call budget."""

        nonlocal invocations
        invocations += 1
        return value

    config = AgentConfig(max_tool_calls=0)
    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("budgeted", "side_effect", '{"value":1}'),),
                    )
                )
            ),
            [side_effect],
            config=config,
        ),
        journal,
        store=store,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="budget", session_id="terminal-tool-budget", idempotency_key="request")
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    assert invocations == 0
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [side_effect], config=config),
        journal,
        store=store,
    )
    before_resume = (await recovery_runtime.load(handle.run_id)).last_sequence
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    resumed_events = await journal.read(handle.run_id, after_sequence=before_resume)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "partial"
    assert completed.stop_reason == "max_tool_calls"
    assert invocations == 0
    assert not recovery_model.requests
    assert not any(event.kind is RunEventKind.TOOL_STARTED for event in resumed_events)
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_rebuilds_model_failure_without_retrying_provider() -> None:
    class FailedModel:
        def __init__(self) -> None:
            self.requests = 0

        async def complete(self, request: ModelRequest) -> ModelResponse:
            del request
            self.requests += 1
            raise ModelInvocationError("provider unavailable", request_id="request-1")

    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    first_model = FailedModel()
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(prompt="fail once", session_id="terminal-failure", idempotency_key="request")
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(ReActAgent(recovery_model), journal, store=store)
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "failed"
    assert completed.stop_reason == "model_error"
    assert first_model.requests == 1
    assert not recovery_model.requests
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_rebuilds_budget_terminal_without_starting_model() -> None:
    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    config = AgentConfig(max_context_chars=10)
    first_model = ScriptedModel()
    first_runtime = AgentRuntime(
        ReActAgent(first_model, config=config),
        journal,
        store=store,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="this prompt exceeds ten characters",
            session_id="terminal-budget",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, config=config),
        journal,
        store=store,
    )
    before_resume = (await recovery_runtime.load(handle.run_id)).last_sequence
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    resumed_events = await journal.read(handle.run_id, after_sequence=before_resume)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "partial"
    assert completed.stop_reason == "context_limit"
    assert not first_model.requests
    assert not recovery_model.requests
    assert not any(event.kind is RunEventKind.MODEL_STARTED for event in resumed_events)
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_rebuilds_loop_terminal_without_replaying_tools() -> None:
    invocations = 0

    @tool
    def constant(value: int) -> str:
        """Return one stable value for loop detection."""

        nonlocal invocations
        invocations += 1
        return f"same:{value}"

    config = AgentConfig(repeated_action_limit=2)
    journal = CrashBeforeResultReadyJournal()
    store = InMemoryRuntimeStore()
    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(None, (ToolCall("call-a", "constant", '{"value":1}'),))
        ),
        ModelResponse(
            AssistantMessage(None, (ToolCall("call-b", "constant", '{"value":1}'),))
        ),
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [constant], config=config),
        journal,
        store=store,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="loop", session_id="terminal-loop", idempotency_key="request")
    )
    await asyncio.wait_for(journal.result_ready_attempted.wait(), timeout=1)
    assert invocations == 2
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [constant], config=config),
        journal,
        store=store,
    )
    before_resume = (await recovery_runtime.load(handle.run_id)).last_sequence
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    resumed_events = await journal.read(handle.run_id, after_sequence=before_resume)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "partial"
    assert completed.stop_reason == "loop_detected"
    assert tuple(completed.loop_counts.values()) == (2,)
    assert invocations == 2
    assert not recovery_model.requests
    assert not any(
        event.kind in {
            RunEventKind.MODEL_STARTED,
            RunEventKind.TOOL_STARTED,
            RunEventKind.TOOL_REUSED,
        }
        for event in resumed_events
    )
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_session_operation_conflict_stays_resumable_and_is_not_mislabeled_as_cas() -> None:
    journal = InMemoryRunJournal()
    store = FailOnceRequestConflictStore()
    first_model = ScriptedModel(ModelResponse(AssistantMessage("commit after recovery")))
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(prompt="commit", session_id="session-conflict", idempotency_key="request")
    )
    await asyncio.wait_for(store.commit_attempted.wait(), timeout=1)
    interrupted = await first_runtime.wait(handle.run_id, timeout_s=0.05)

    assert interrupted.result is not None
    assert interrupted.state is not RunState.TERMINAL
    assert not any(
        event.kind is RunEventKind.RUN_ABORTED
        for event in await journal.read(handle.run_id)
    )
    assert (await store.load_session("session-conflict")).version == 0
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(ReActAgent(recovery_model), journal, store=store)
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert not recovery_model.requests
    assert (await store.load_session("session-conflict")).version == 1
    await recovery_runtime.close()
