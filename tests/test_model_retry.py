"""Transient model failures: bounded in-execution retry, then a resumable stop."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from react_agent import (
    AgentConfig,
    AssistantMessage,
    ConfigurationError,
    EventKind,
    ModelInvocationError,
    ModelRequest,
    ModelResponse,
    ReActAgent,
    RunStatus,
    StopReason,
    ToolCall,
    tool,
)
from react_agent.events import RunEventKind, RunState
from react_agent.journal import InMemoryRunJournal
from react_agent.models import AgentJournalEvent, AgentJournalEventKind
from react_agent.runtime import AgentRuntime, InMemoryRuntimeStore, ResumeRun, StartRun

FAST_RETRY = AgentConfig(
    model_retry_limit=2,
    model_retry_backoff_s=0.001,
    model_retry_max_backoff_s=0.002,
)


def transient(status: int = 503) -> ModelInvocationError:
    return ModelInvocationError(
        f"Model request failed (status={status}, code=server_error): InternalServerError",
        request_id=f"req-{status}",
        status_code=status,
        error_code="server_error",
        retryable=True,
    )


def permanent() -> ModelInvocationError:
    return ModelInvocationError(
        "Model request failed (status=400, code=unknown_parameter, param=input[2].x): "
        "BadRequestError",
        request_id="req-400",
        status_code=400,
        error_code="unknown_parameter",
        error_param="input[2].x",
        retryable=False,
    )


class FlakyModel:
    """Raise scripted errors, then serve scripted responses in order."""

    def __init__(self, *turns: ModelInvocationError | ModelResponse) -> None:
        self.turns = deque(turns)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.turns:
            pytest.fail("unexpected provider call")
        turn = self.turns.popleft()
        if isinstance(turn, ModelInvocationError):
            raise turn
        return turn


def answer(text: str = "done") -> ModelResponse:
    return ModelResponse(AssistantMessage(content=text))


@tool(idempotent=True, parallel_safe=True)
def probe(label: str) -> str:
    """Return a stable observation."""

    return f"probe:{label}"


def journal_collector() -> tuple[list[AgentJournalEvent], object]:
    facts: list[AgentJournalEvent] = []

    def sink(event: AgentJournalEvent) -> None:
        facts.append(event)

    return facts, sink


def test_retry_config_is_validated() -> None:
    with pytest.raises(ConfigurationError, match="model_retry_limit"):
        AgentConfig(model_retry_limit=-1)
    with pytest.raises(ConfigurationError, match="model_retry_backoff_s"):
        AgentConfig(model_retry_backoff_s=0)
    with pytest.raises(ConfigurationError, match="model_retry_max_backoff_s"):
        AgentConfig(model_retry_backoff_s=5, model_retry_max_backoff_s=1)
    config = AgentConfig(model_retry_backoff_s=2, model_retry_max_backoff_s=5)
    assert [config.model_retry_backoff(n) for n in (1, 2, 3, 4)] == [2, 4, 5, 5]


@pytest.mark.asyncio
async def test_transient_failure_is_retried_within_the_same_step() -> None:
    model = FlakyModel(transient(429), answer())
    facts, sink = journal_collector()

    result = await ReActAgent(model, config=FAST_RETRY, journal_sink=sink).run("hi")

    assert result.status is RunStatus.COMPLETED
    assert result.output == "done"
    assert result.model_calls == 2
    assert len(model.requests) == 2

    kinds = [(event.kind, event.step) for event in result.events]
    assert kinds[:4] == [
        (EventKind.RUN_STARTED, None),
        (EventKind.MODEL_STARTED, 1),
        (EventKind.MODEL_FAILED, 1),
        (EventKind.MODEL_STARTED, 1),
    ]
    failed = next(event for event in result.events if event.kind is EventKind.MODEL_FAILED)
    assert failed.data["retryable"] is True
    assert failed.data["status_code"] == 429
    assert failed.data["execution_attempt"] == 1
    assert failed.data["retry_in_ms"] == 1.0

    started = [fact for fact in facts if fact.kind is AgentJournalEventKind.MODEL_STARTED]
    assert [fact.step for fact in started] == [1, 1]
    assert len({fact.operation_id for fact in started}) == 2
    failed_fact = next(fact for fact in facts if fact.kind is AgentJournalEventKind.MODEL_FAILED)
    assert failed_fact.public_data["terminal_decision"] is False
    assert "status" not in failed_fact.public_data
    assert failed_fact.public_data["error_code"] == "server_error"
    assert failed_fact.private_data["error"].startswith("Model request failed (status=429")
    assert facts[-1].kind is AgentJournalEventKind.RUN_COMPLETED


@pytest.mark.asyncio
async def test_permanent_failure_is_terminal_with_structural_evidence() -> None:
    model = FlakyModel(permanent())
    facts, sink = journal_collector()

    result = await ReActAgent(model, config=FAST_RETRY, journal_sink=sink).run("hi")

    assert result.status is RunStatus.FAILED
    assert result.stop_reason is StopReason.MODEL_ERROR
    assert result.model_calls == 1
    assert "code=unknown_parameter" in (result.error or "")
    assert result.events[-1].kind is EventKind.RUN_COMPLETED
    failed_fact = next(fact for fact in facts if fact.kind is AgentJournalEventKind.MODEL_FAILED)
    assert failed_fact.public_data["terminal_decision"] is True
    assert failed_fact.public_data["retryable"] is False
    assert failed_fact.public_data["status_code"] == 400
    assert failed_fact.public_data["error_code"] == "unknown_parameter"
    assert failed_fact.private_data["error_param"] == "input[2].x"
    assert facts[-1].kind is AgentJournalEventKind.RUN_COMPLETED


@pytest.mark.asyncio
async def test_exhausted_retries_stop_without_a_terminal_fact() -> None:
    model = FlakyModel(transient(), transient(), transient())
    facts, sink = journal_collector()

    result = await ReActAgent(model, config=FAST_RETRY, journal_sink=sink).run("hi")

    assert result.status is RunStatus.FAILED
    assert result.stop_reason is StopReason.MODEL_UNAVAILABLE
    assert result.model_calls == 3
    assert "resumable" in (result.error or "")
    # The execution's own event stream still closes.
    assert result.events[-1].kind is EventKind.RUN_COMPLETED
    # The durable log does not: the last fact is an explicit non-decision.
    assert facts[-1].kind is AgentJournalEventKind.MODEL_FAILED
    assert facts[-1].public_data["terminal_decision"] is False
    assert facts[-1].public_data["retry_exhausted"] is True
    assert facts[-1].public_data["execution_attempt"] == 3
    assert not any(fact.kind is AgentJournalEventKind.RUN_COMPLETED for fact in facts)


@pytest.mark.asyncio
async def test_retry_limit_zero_disables_in_execution_retry() -> None:
    model = FlakyModel(transient())
    config = AgentConfig(model_retry_limit=0, model_retry_backoff_s=0.001)

    result = await ReActAgent(model, config=config).run("hi")

    assert result.stop_reason is StopReason.MODEL_UNAVAILABLE
    assert result.model_calls == 1


@pytest.mark.asyncio
async def test_backoff_never_outlives_the_wall_clock() -> None:
    model = FlakyModel(transient())
    config = AgentConfig(max_wall_time_s=0.2, model_retry_backoff_s=5.0)

    result = await ReActAgent(model, config=config).run("hi")

    assert result.stop_reason is StopReason.MODEL_UNAVAILABLE
    assert result.model_calls == 1
    failed = next(event for event in result.events if event.kind is EventKind.MODEL_FAILED)
    assert failed.data["retry_in_ms"] is None


async def _wait_idle(runtime: AgentRuntime, run_id: str) -> None:
    for _ in range(200):
        if runtime._is_run_idle(run_id):
            return
        await asyncio.sleep(0.005)
    pytest.fail("execution did not release the run")


@pytest.mark.asyncio
async def test_runtime_releases_a_retry_exhausted_run_and_resumes_the_same_step() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    config = AgentConfig(model_retry_limit=1, model_retry_backoff_s=0.001)
    model = FlakyModel(transient(), transient(), answer("recovered"))
    runtime = AgentRuntime(ReActAgent(model, config=config), journal, store=store)

    handle = await runtime.submit(
        StartRun(prompt="long task", session_id="retry-session", idempotency_key="request")
    )
    await _wait_idle(runtime, handle.run_id)

    suspended = await runtime.load(handle.run_id)
    assert suspended.state is RunState.RUNNING
    assert suspended.terminal is None
    assert suspended.status is None
    assert suspended.counts.model_calls == 2
    events = await journal.read(handle.run_id)
    assert events[-1].kind is RunEventKind.MODEL_FAILED
    assert events[-1].data["terminal_decision"] is False
    assert events[-1].data["retry_exhausted"] is True
    assert [
        event.data["attempt"] for event in events if event.kind is RunEventKind.MODEL_STARTED
    ] == [1, 2]

    resumed = await runtime.submit(ResumeRun(run_id=handle.run_id))
    assert resumed.created
    completed = await runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert completed.counts.model_calls == 3
    events = await journal.read(handle.run_id)
    resumed_event = next(event for event in events if event.kind is RunEventKind.RUN_RESUMED)
    assert resumed_event.data["resume_reason"] == "model_retry"
    third = [event for event in events if event.kind is RunEventKind.MODEL_STARTED][-1]
    assert third.step == 1
    assert third.data["attempt"] == 3
    assert third.execution_id == resumed.execution_id
    assert len(model.requests) == 3
    await runtime.close()


@pytest.mark.asyncio
async def test_resume_after_transient_failure_does_not_burn_a_step() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    config = AgentConfig(max_steps=2, model_retry_limit=0, model_retry_backoff_s=0.001)
    first_model = FlakyModel(
        ModelResponse(
            AssistantMessage(tool_calls=(ToolCall("call-1", "probe", '{"label":"a"}'),))
        ),
        transient(),
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [probe], config=config), journal, store=store
    )
    handle = await first_runtime.submit(
        StartRun(prompt="two steps", session_id="step-session", idempotency_key="request")
    )
    await _wait_idle(first_runtime, handle.run_id)
    await first_runtime.close()

    suspended = await journal.read(handle.run_id)
    assert suspended[-1].kind is RunEventKind.MODEL_FAILED
    assert suspended[-1].step == 2

    second_model = FlakyModel(answer("finished"))
    second_runtime = AgentRuntime(
        ReActAgent(second_model, [probe], config=config), journal, store=store
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await second_runtime.wait(handle.run_id, timeout_s=2)

    # max_steps=2: a burned step would have ended the run as PARTIAL/max_steps.
    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert completed.stop_reason == "completed"
    events = await journal.read(handle.run_id)
    resumed_start = [event for event in events if event.kind is RunEventKind.MODEL_STARTED][-1]
    assert resumed_start.step == 2
    assert resumed_start.data["attempt"] == 2
    assert not any(
        event.kind is RunEventKind.TOOL_STARTED
        and event.execution_id == resumed_start.execution_id
        for event in events
    )
    assert len(second_model.requests) == 1
    await second_runtime.close()


@pytest.mark.asyncio
async def test_legacy_terminal_model_failure_still_recovers_as_terminal() -> None:
    """A permanent failure keeps the pre-existing terminal recovery path."""

    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    model = FlakyModel(permanent())
    runtime = AgentRuntime(ReActAgent(model), journal, store=store)
    handle = await runtime.submit(
        StartRun(prompt="fail", session_id="permanent", idempotency_key="request")
    )
    completed = await runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "failed"
    assert completed.stop_reason == "model_error"
    await runtime.close()
