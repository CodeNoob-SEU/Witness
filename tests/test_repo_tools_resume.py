"""Call-scoped resume policy: a read-only run_command survives a crash by retry."""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path

import pytest

from react_agent import AssistantMessage, ModelRequest, ModelResponse, ReActAgent, ToolCall
from react_agent.events import RunEventKind, RunState
from react_agent.journal import InMemoryRunJournal
from react_agent.repo_tools import CommandResult, create_repository_tools
from react_agent.runtime import AgentRuntime, InMemoryRuntimeStore, ResumeRun, StartRun


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            pytest.fail("unexpected provider call")
        return self.responses.popleft()


class HangingThenRecordingRunner:
    """Hang forever in the first process, record and answer in the second."""

    def __init__(self, *, hang: bool) -> None:
        self.hang = hang
        self.started = asyncio.Event()
        self.commands: list[str] = []

    async def run(self, command: str, *, cwd: Path, timeout_s: float) -> CommandResult:
        del cwd, timeout_s
        self.commands.append(command)
        self.started.set()
        if self.hang:
            await asyncio.Event().wait()
        return CommandResult(0, False, "ok\n", "", 1.0)


async def _wait_for(journal: InMemoryRunJournal, run_id: str, kind: RunEventKind) -> None:
    for _ in range(200):
        if any(event.kind is kind for event in await journal.read(run_id)):
            return
        await asyncio.sleep(0.005)
    pytest.fail(f"{kind.value} was not committed")


def _tools(runner: HangingThenRecordingRunner, root: Path):
    return create_repository_tools(command_runner=runner, root=root)


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [True, False])
async def test_crash_mid_run_command_retries_only_when_declared_read_only(
    tmp_path: Path, read_only: bool
) -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    arguments = f'{{"command":"git status","read_only":{"true" if read_only else "false"}}}'
    plan = ModelResponse(
        AssistantMessage(None, (ToolCall("cmd-1", "run_command", arguments),))
    )

    first_runner = HangingThenRecordingRunner(hang=True)
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(plan), _tools(first_runner, tmp_path)), journal, store=store
    )
    handle = await first_runtime.submit(
        StartRun(prompt="inspect", session_id=f"cmd-{read_only}", idempotency_key="request")
    )
    await asyncio.wait_for(first_runner.started.wait(), timeout=1)
    await first_runtime.close()

    started = [
        event for event in await journal.read(handle.run_id)
        if event.kind is RunEventKind.TOOL_STARTED
    ]
    assert started[0].data["resume_policy"] == (
        "idempotent_retry" if read_only else "require_operator"
    )

    second_runner = HangingThenRecordingRunner(hang=False)
    second_model = ScriptedModel(ModelResponse(AssistantMessage("clean tree")))
    second_runtime = AgentRuntime(
        ReActAgent(second_model, _tools(second_runner, tmp_path)), journal, store=store
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))

    if read_only:
        snapshot = await second_runtime.wait(handle.run_id, timeout_s=2)
        assert snapshot.state is RunState.TERMINAL
        assert snapshot.status == "completed"
        assert second_runner.commands == ["git status"]
        assert len(second_model.requests) == 1
        resumed = next(
            event for event in await journal.read(handle.run_id)
            if event.kind is RunEventKind.RUN_RESUMED
        )
        assert resumed.data["resume_reason"] == "tool_retry"
    else:
        await _wait_for(journal, handle.run_id, RunEventKind.RECONCILIATION_REQUIRED)
        snapshot = await second_runtime.load(handle.run_id)
        assert snapshot.state is RunState.NEEDS_RECONCILIATION
        assert second_runner.commands == []
        assert not second_model.requests
    await second_runtime.close()
