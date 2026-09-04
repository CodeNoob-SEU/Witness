"""RunSupervisor: orphaned runs are resumed automatically, conservatively."""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path

import httpx
import pytest

from react_agent import (
    AgentConfig,
    AssistantMessage,
    ModelInvocationError,
    ModelRequest,
    ModelResponse,
    ReActAgent,
    ToolCall,
    tool,
)
from react_agent.events import RunState
from react_agent.journal import InMemoryRunJournal
from react_agent.runtime import AgentRuntime, InMemoryRuntimeStore, StartRun
from react_agent.supervisor import RunSupervisor, SupervisedRun
from react_agent.web import create_app


class ScriptedModel:
    def __init__(self, *turns: ModelResponse | ModelInvocationError) -> None:
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


def transient() -> ModelInvocationError:
    return ModelInvocationError(
        "Model request failed (status=503)", status_code=503, retryable=True
    )


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


async def _wait_idle(runtime: AgentRuntime, run_id: str) -> None:
    for _ in range(400):
        if runtime._is_run_idle(run_id):
            return
        await asyncio.sleep(0.005)
    pytest.fail("execution did not release the run")


@pytest.mark.asyncio
async def test_live_leases_are_not_candidates_but_released_runs_are() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()

    @tool(idempotent=True)
    async def hang(label: str) -> str:
        """Stall until the worker is torn down."""

        started.set()
        await asyncio.Event().wait()
        return label

    plan = ModelResponse(AssistantMessage(None, (ToolCall("h-1", "hang", '{"label":"x"}'),)))
    runtime = AgentRuntime(ReActAgent(ScriptedModel(plan), [hang]), journal, store=store)
    handle = await runtime.submit(StartRun(prompt="go", session_id="s", idempotency_key="r"))
    await asyncio.wait_for(started.wait(), timeout=1)

    assert await journal.list_orphaned_runs() == ()
    await runtime.close()
    assert await journal.list_orphaned_runs() == (handle.run_id,)
    with pytest.raises(ValueError):
        await journal.list_orphaned_runs(limit=0)


@pytest.mark.asyncio
async def test_supervisor_resumes_an_orphaned_run_to_completion() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()
    calls: list[int] = []

    @tool(idempotent=True)
    async def note(label: str) -> str:
        """Stall only in the first process."""

        calls.append(1)
        if len(calls) == 1:
            started.set()
            await asyncio.Event().wait()
        return f"noted:{label}"

    plan = ModelResponse(AssistantMessage(None, (ToolCall("n-1", "note", '{"label":"x"}'),)))
    crashed = AgentRuntime(ReActAgent(ScriptedModel(plan), [note]), journal, store=store)
    handle = await crashed.submit(StartRun(prompt="go", session_id="s", idempotency_key="r"))
    await asyncio.wait_for(started.wait(), timeout=1)
    await crashed.close()

    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("done")))
    runtime = AgentRuntime(ReActAgent(recovery_model, [note]), journal, store=store)
    attention: list[SupervisedRun] = []
    supervisor = RunSupervisor(runtime, interval_s=0.01, on_attention=attention.append)

    sweep = await supervisor.sweep()
    assert sweep.candidates == 1
    assert [run.outcome for run in sweep.runs] == ["resumed"]
    assert sweep.runs[0].executions == 2
    assert sweep.resumed == 1 and sweep.attention == ()

    snapshot = await runtime.wait(handle.run_id, timeout_s=2)
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(recovery_model.requests) == 1
    assert attention == []

    # Nothing left to do: the run is terminal and no longer listed.
    again = await supervisor.sweep()
    assert again.candidates == 0
    assert supervisor.sweeps == 2 and supervisor.last_sweep is again
    await runtime.close()


@pytest.mark.asyncio
async def test_supervisor_reports_reconciliation_instead_of_retrying() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()

    @tool
    async def charge(amount: int) -> int:
        """Non-idempotent: a crash mid-call must not be retried blindly."""

        started.set()
        await asyncio.Event().wait()
        return amount

    plan = ModelResponse(AssistantMessage(None, (ToolCall("c-1", "charge", '{"amount":7}'),)))
    crashed = AgentRuntime(ReActAgent(ScriptedModel(plan), [charge]), journal, store=store)
    handle = await crashed.submit(StartRun(prompt="go", session_id="s", idempotency_key="r"))
    await asyncio.wait_for(started.wait(), timeout=1)
    await crashed.close()

    after_restart = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    runtime = AgentRuntime(ReActAgent(after_restart, [charge]), journal, store=store)
    attention: list[SupervisedRun] = []
    supervisor = RunSupervisor(runtime, interval_s=0.01, on_attention=attention.append)

    first = await supervisor.sweep()
    # The first sweep legitimately Resumes; that Resume fails closed into
    # reconciliation and releases the lease again.
    assert [run.outcome for run in first.runs] == ["resumed"]
    await _wait_idle(runtime, handle.run_id)
    second = await supervisor.sweep()
    assert [run.outcome for run in second.runs] == ["needs_reconciliation"]
    assert second.runs[0].state == "needs_reconciliation"
    assert [run.run_id for run in attention] == [handle.run_id]
    assert not after_restart.requests
    await runtime.close()


@pytest.mark.asyncio
async def test_supervisor_backs_off_and_stops_at_the_execution_budget() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    clock = Clock()
    config = AgentConfig(model_retry_limit=0, model_retry_backoff_s=0.001)
    # Every execution gives up on a transient failure, so the run keeps
    # returning to the orphan list without ever becoming terminal.
    model = ScriptedModel(*(transient() for _ in range(6)))
    runtime = AgentRuntime(ReActAgent(model, config=config), journal, store=store)
    handle = await runtime.submit(StartRun(prompt="go", session_id="s", idempotency_key="r"))
    await _wait_idle(runtime, handle.run_id)

    attention: list[SupervisedRun] = []
    supervisor = RunSupervisor(
        runtime,
        interval_s=1.0,
        max_executions_per_run=3,
        on_attention=attention.append,
        clock=clock,
    )

    first = await supervisor.sweep()
    assert [run.outcome for run in first.runs] == ["resumed"]
    await _wait_idle(runtime, handle.run_id)

    immediately = await supervisor.sweep()
    assert [run.outcome for run in immediately.runs] == ["backing_off"]

    clock.now += 60.0
    second = await supervisor.sweep()
    assert [run.outcome for run in second.runs] == ["resumed"]
    await _wait_idle(runtime, handle.run_id)

    clock.now += 600.0
    exhausted = await supervisor.sweep()
    assert [run.outcome for run in exhausted.runs] == ["resume_budget_exhausted"]
    assert exhausted.runs[0].executions == 3
    assert [run.outcome for run in attention] == ["resume_budget_exhausted"]
    snapshot = await runtime.load(handle.run_id)
    assert snapshot.state is RunState.RUNNING and snapshot.terminal is None
    assert len(model.requests) == 3
    await runtime.close()


@pytest.mark.asyncio
async def test_supervisor_serve_loop_starts_and_stops() -> None:
    journal = InMemoryRunJournal()
    runtime = AgentRuntime(ReActAgent(ScriptedModel()), journal, store=InMemoryRuntimeStore())
    supervisor = RunSupervisor(runtime, interval_s=0.01)

    task = supervisor.start()
    assert supervisor.start() is task
    for _ in range(200):
        if supervisor.sweeps >= 2:
            break
        await asyncio.sleep(0.005)
    assert supervisor.sweeps >= 2
    await supervisor.stop()
    assert task.done()
    sweeps = supervisor.sweeps
    await asyncio.sleep(0.03)
    assert supervisor.sweeps == sweeps
    await runtime.close()


def test_supervisor_configuration_is_validated() -> None:
    runtime = AgentRuntime(ReActAgent(ScriptedModel()), InMemoryRunJournal())
    with pytest.raises(ValueError):
        RunSupervisor(runtime, interval_s=0)
    with pytest.raises(ValueError):
        RunSupervisor(runtime, max_executions_per_run=1)
    with pytest.raises(ValueError):
        RunSupervisor(runtime, batch_limit=0)


@pytest.mark.asyncio
async def test_web_exposes_supervisor_status(tmp_path: Path) -> None:
    del tmp_path
    runtime = AgentRuntime(ReActAgent(ScriptedModel()), InMemoryRunJournal())
    app = create_app(runtime=runtime, model_name="offline-runtime")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        disabled = await client.get("/api/supervisor")
        assert disabled.json() == {"enabled": False}
        assert (await client.post("/api/supervisor/sweep")).status_code == 404

        app.state.supervisor = RunSupervisor(runtime, interval_s=0.5)
        status = await client.get("/api/supervisor")
        assert status.json()["enabled"] is True
        assert status.json()["sweeps"] == 0
        sweep = await client.post("/api/supervisor/sweep")
        assert sweep.status_code == 200
        assert sweep.json()["candidates"] == 0
        assert (await client.get("/api/supervisor")).json()["sweeps"] == 1
    await runtime.close()


@pytest.mark.asyncio
async def test_orphan_listing_filters_by_agent_revision_and_lists_freshest_first() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    config = AgentConfig(model_retry_limit=0, model_retry_backoff_s=0.001)

    async def orphan(session: str, agent: ReActAgent) -> str:
        runtime = AgentRuntime(agent, journal, store=store)
        handle = await runtime.submit(
            StartRun(prompt="go", session_id=session, idempotency_key="r")
        )
        await _wait_idle(runtime, handle.run_id)
        await runtime.close()
        return handle.run_id

    agent_a = ReActAgent(ScriptedModel(transient(), transient()), config=config)
    agent_b = ReActAgent(ScriptedModel(transient()), config=config, instructions="other")
    older = await orphan("a1", agent_a)
    foreign = await orphan("b1", agent_b)
    newer = await orphan("a2", agent_a)

    runtime = AgentRuntime(agent_a, journal, store=store)
    binding = runtime.agent_revision
    assert await journal.list_orphaned_runs() == (newer, foreign, older)
    assert await journal.list_orphaned_runs(agent_revision=binding) == (newer, older)
    assert await journal.list_orphaned_runs(agent_revision="unknown") == ()

    # The supervisor never even sees the foreign run.
    sweep = await RunSupervisor(runtime, interval_s=0.01).sweep()
    assert {run.run_id for run in sweep.runs} == {newer, older}
    await _wait_idle(runtime, newer)
    await _wait_idle(runtime, older)
    await runtime.close()


@pytest.mark.asyncio
async def test_unreadable_runs_are_reported_once_and_do_not_stop_the_sweep() -> None:
    class CorruptJournal(InMemoryRunJournal):
        def __init__(self) -> None:
            super().__init__()
            self.corrupt: set[str] = set()

        async def load(self, run_id: str):
            if run_id in self.corrupt:
                raise RuntimeError("event hash mismatch at sequence 1")
            return await super().load(run_id)

    journal = CorruptJournal()
    store = InMemoryRuntimeStore()
    config = AgentConfig(model_retry_limit=0, model_retry_backoff_s=0.001)
    agent = ReActAgent(ScriptedModel(transient(), transient(), transient()), config=config)
    runtime = AgentRuntime(agent, journal, store=store)
    first = await runtime.submit(StartRun(prompt="go", session_id="s1", idempotency_key="r"))
    await _wait_idle(runtime, first.run_id)
    second = await runtime.submit(StartRun(prompt="go", session_id="s2", idempotency_key="r"))
    await _wait_idle(runtime, second.run_id)
    journal.corrupt.add(second.run_id)

    attention: list[SupervisedRun] = []
    supervisor = RunSupervisor(
        runtime, interval_s=1.0, on_attention=attention.append, clock=Clock()
    )
    sweep = await supervisor.sweep()
    by_id = {run.run_id: run for run in sweep.runs}
    assert by_id[second.run_id].outcome == "unreadable"
    assert by_id[second.run_id].detail == "RuntimeError"
    assert by_id[first.run_id].outcome == "resumed"
    assert [run.run_id for run in attention] == [second.run_id]

    await _wait_idle(runtime, first.run_id)
    again = await supervisor.sweep()
    by_id = {run.run_id: run for run in again.runs}
    assert by_id[first.run_id].outcome == "backing_off"
    # Same outcome for the same run is not reported again.
    assert [run.run_id for run in attention] == [second.run_id]
    await runtime.close()
