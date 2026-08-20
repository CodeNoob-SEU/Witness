from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path

import pytest

from react_agent.agent import ReActAgent
from react_agent.events import RunEventKind, RunState
from react_agent.journal import InMemoryRunJournal, JournalLease
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse, ToolCall
from react_agent.runtime import (
    AgentRuntime,
    ForkRun,
    InMemoryRuntimeStore,
    ResumeRejected,
    ResumeRun,
    StartRun,
)
from react_agent.tools import ToolExecutionContext, tool
from react_agent.workspace import FakeWorkspaceCheckpointStore


class _SimulatedProcessCrash(BaseException):
    """Stop orchestration between two durable commit points."""


class CrashBeforeFirstWorkspaceEventJournal(InMemoryRunJournal):
    """Crash after run_started is durable but before the first workspace event."""

    def __init__(self) -> None:
        super().__init__()
        self._armed = True

    async def acquire(self, run_id: str, *, owner: str, ttl_s: float) -> JournalLease:
        if self._armed:
            self._armed = False
            raise _SimulatedProcessCrash
        return await super().acquire(run_id, owner=owner, ttl_s=ttl_s)


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            pytest.fail("unexpected provider call")
        return self.responses.popleft()


@pytest.mark.asyncio
async def test_direct_agent_tool_context_has_no_managed_workspace_path_or_schema_field() -> None:
    observed: list[ToolExecutionContext] = []

    @tool
    async def inspect_context(*, context: ToolExecutionContext) -> str:
        """Observe framework metadata without exposing it to the model."""

        observed.append(context)
        return "observed"

    agent = ReActAgent(
        ScriptedModel(
            ModelResponse(
                AssistantMessage(
                    None,
                    (ToolCall("inspect-1", "inspect_context", "{}"),),
                )
            ),
            ModelResponse(AssistantMessage("done")),
        ),
        [inspect_context],
    )

    result = await agent.run("inspect")

    assert result.output == "done"
    assert observed[0].workspace_path is None
    assert "context" not in agent.registry.specs[0].parameters["properties"]
    assert "workspace_path" not in agent.registry.specs[0].parameters["properties"]


@pytest.mark.asyncio
async def test_runtime_start_injects_the_managed_workspace_path_into_tool_context() -> None:
    observed: list[ToolExecutionContext] = []

    @tool
    async def inspect_context(*, context: ToolExecutionContext) -> str:
        """Observe the Runtime-owned workspace path."""

        observed.append(context)
        return "observed"

    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("inspect-1", "inspect_context", "{}"),),
                    )
                ),
                ModelResponse(AssistantMessage("done")),
            ),
            [inspect_context],
        ),
        InMemoryRunJournal(),
        workspace=workspace,
    )

    handle = await runtime.submit(
        StartRun(prompt="inspect", session_id="context-start-session")
    )
    completed = await runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert observed[0].workspace_path == Path(
        "/in-memory-workspaces/context-start-session"
    )
    assert all(
        "workspace_path" not in str(event.data)
        and "in-memory-workspaces" not in str(event.data)
        for event in await runtime.journal.read(handle.run_id)
    )
    await runtime.close()


@pytest.mark.asyncio
async def test_resume_bootstraps_the_exact_clean_workspace_anchor_after_start_crash() -> None:
    journal = CrashBeforeFirstWorkspaceEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await first_runtime.submit(
            StartRun(
                prompt="continue from the immutable workspace anchor",
                session_id="anchor-session",
                idempotency_key="anchor-request",
            )
        )
    run_id = (await store.list_runs("anchor-session"))[0]
    before_resume = await first_runtime.load(run_id)
    initial_events = await journal.read(run_id)

    assert before_resume.state is RunState.RUNNING
    assert before_resume.workspace_anchor is not None
    assert before_resume.workspace == before_resume.workspace_anchor
    assert len(initial_events) == 1
    assert initial_events[0].kind is RunEventKind.RUN_STARTED
    anchor = initial_events[0].data["workspace_anchor"]
    assert anchor == before_resume.workspace_anchor
    assert {
        "checkpoint_id",
        "baseline_revision",
        "tree_id",
        "commit_id",
        "internal_ref",
        "created_at",
    } <= set(anchor)

    model = ScriptedModel(ModelResponse(AssistantMessage("resumed safely")))
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
    )
    resumed = await recovered_runtime.submit(ResumeRun(run_id=run_id))
    completed = await recovered_runtime.wait(resumed.run_id, timeout_s=2)
    events = await journal.read(run_id)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert len(model.requests) == 1
    first_workspace_event = next(
        event for event in events if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
    )
    for field in (
        "checkpoint_id",
        "baseline_revision",
        "tree_id",
        "commit_id",
        "internal_ref",
    ):
        assert first_workspace_event.data[field] == anchor[field]

    await first_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_fork_run_started_anchors_the_child_workspace_before_execution() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent done")))),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-anchor-parent")
    )
    await parent_runtime.wait(parent.run_id, timeout_s=2)
    parent_events = await journal.read(parent.run_id)
    fork_sequence = next(
        event.sequence
        for event in parent_events
        if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "run_start"
    )

    child_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("child done")))),
        journal,
        store=store,
        workspace=workspace,
    )
    child = await child_runtime.submit(
        ForkRun(
            run_id=parent.run_id,
            from_sequence=fork_sequence,
            session_id="fork-anchor-child",
        )
    )
    await child_runtime.wait(child.run_id, timeout_s=2)
    child_snapshot = await child_runtime.load(child.run_id)
    child_events = await journal.read(child.run_id)

    anchor = child_events[0].data["workspace_anchor"]
    assert child_snapshot.workspace_anchor == anchor
    assert anchor["phase"] == "fork_start"
    first_workspace = next(
        event
        for event in child_events
        if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
    )
    assert first_workspace.data["checkpoint_id"] == anchor["checkpoint_id"]
    assert first_workspace.data["tree_id"] == anchor["tree_id"]

    await parent_runtime.close()
    await child_runtime.close()


@pytest.mark.asyncio
async def test_resume_rejects_an_anchored_workspace_without_its_adapter() -> None:
    journal = CrashBeforeFirstWorkspaceEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await first_runtime.submit(
            StartRun(
                prompt="anchored",
                session_id="missing-adapter-session",
                idempotency_key="missing-adapter-request",
            )
        )
    run_id = (await store.list_runs("missing-adapter-session"))[0]
    runtime_without_workspace = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("must not run")))),
        journal,
        store=store,
    )

    with pytest.raises(ResumeRejected, match="workspace adapter"):
        await runtime_without_workspace.submit(ResumeRun(run_id=run_id))

    await first_runtime.close()
    await runtime_without_workspace.close()


@pytest.mark.asyncio
async def test_resume_fails_closed_when_workspace_changed_after_run_started_anchor() -> None:
    journal = CrashBeforeFirstWorkspaceEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await first_runtime.submit(
            StartRun(
                prompt="anchored",
                session_id="diverged-anchor-session",
                idempotency_key="diverged-anchor-request",
            )
        )
    run_id = (await store.list_runs("diverged-anchor-session"))[0]
    workspace.write_file("diverged-anchor-session", "README.md", "changed\n")
    model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
    )

    await recovered_runtime.submit(ResumeRun(run_id=run_id))
    for _ in range(100):
        snapshot = await recovered_runtime.load(run_id)
        if snapshot.state is RunState.NEEDS_RECONCILIATION:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail("workspace divergence never entered reconciliation")
    events = await journal.read(run_id)

    assert not model.requests
    assert any(event.kind is RunEventKind.WORKSPACE_DIVERGED for event in events)
    assert not any(event.kind is RunEventKind.WORKSPACE_CHECKPOINTED for event in events)

    await first_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_runtime_resume_injects_the_reattached_workspace_path() -> None:
    observed: list[ToolExecutionContext] = []

    @tool
    async def inspect_context(*, context: ToolExecutionContext) -> str:
        """Observe the reattached Runtime workspace."""

        observed.append(context)
        return "observed"

    journal = CrashBeforeFirstWorkspaceEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(), [inspect_context]),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await first_runtime.submit(
            StartRun(
                prompt="inspect after resume",
                session_id="context-resume-session",
                idempotency_key="context-resume-request",
            )
        )
    run_id = (await store.list_runs("context-resume-session"))[0]
    recovered_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("inspect-1", "inspect_context", "{}"),),
                    )
                ),
                ModelResponse(AssistantMessage("done")),
            ),
            [inspect_context],
        ),
        journal,
        store=store,
        workspace=workspace,
    )

    await recovered_runtime.submit(ResumeRun(run_id=run_id))
    completed = await recovered_runtime.wait(run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert observed[0].workspace_path == Path(
        "/in-memory-workspaces/context-resume-session"
    )
    await first_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_runtime_fork_injects_the_child_workspace_path() -> None:
    observed: list[ToolExecutionContext] = []

    @tool
    async def inspect_context(*, context: ToolExecutionContext) -> str:
        """Observe the Fork-owned Runtime workspace."""

        observed.append(context)
        return "observed"

    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(ModelResponse(AssistantMessage("parent done"))),
            [inspect_context],
        ),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="context-fork-parent")
    )
    await parent_runtime.wait(parent.run_id, timeout_s=2)
    parent_events = await journal.read(parent.run_id)
    fork_sequence = next(
        event.sequence
        for event in parent_events
        if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "run_start"
    )
    child_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("inspect-1", "inspect_context", "{}"),),
                    )
                ),
                ModelResponse(AssistantMessage("child done")),
            ),
            [inspect_context],
        ),
        journal,
        store=store,
        workspace=workspace,
    )

    child = await child_runtime.submit(
        ForkRun(
            run_id=parent.run_id,
            from_sequence=fork_sequence,
            session_id="context-fork-child",
        )
    )
    completed = await child_runtime.wait(child.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert observed[0].workspace_path == Path(
        "/in-memory-workspaces/context-fork-child"
    )
    await parent_runtime.close()
    await child_runtime.close()
