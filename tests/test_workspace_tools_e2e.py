"""End-to-end: a real Git worktree, a real file-writing tool, a real crash.

Everything else in the suite exercises one layer. This file wires the whole
stack together — Runtime, isolated Git worktree, workspace checkpoints and the
tools that actually mutate files — because that combination is the only thing
that proves the recovery machinery protects real side effects rather than
bookkeeping.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

import pytest

from react_agent.agent import AgentConfig, ReActAgent
from react_agent.events import RunEventKind
from react_agent.journal import InMemoryRunJournal
from react_agent.models import (
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolMessage,
)
from react_agent.runtime import AgentRuntime, StartRun
from react_agent.workspace import GitWorktreeWorkspace
from react_agent.workspace_tools import workspace_tools


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    root = tmp_path / "primary"
    root.mkdir()
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "Workspace Tests")
    _git(root, "config", "user.email", "workspace@example.test")
    (root / "README.md").write_text("baseline\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "--quiet", "-m", "baseline")
    return root


class _WritingModel:
    """Ask for one file write, then answer once the observation comes back."""

    model = "workspace-e2e-model"

    def __init__(self, *, path: str, content: str) -> None:
        self.path = path
        self.content = content
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if any(isinstance(item, ToolMessage) for item in request.transcript):
            return ModelResponse(AssistantMessage("file written"))
        arguments = f'{{"path":"{self.path}","content":"{self.content}"}}'
        return ModelResponse(
            AssistantMessage(
                tool_calls=(ToolCall("call-1", "write_workspace_file", arguments),)
            )
        )


def _runtime(repository: Path, tmp_path: Path, model: object) -> AgentRuntime:
    workspace = GitWorktreeWorkspace(repository, tmp_path / "managed")
    agent = ReActAgent(
        model,  # type: ignore[arg-type]
        workspace_tools(),
        config=AgentConfig(max_steps=4, max_tool_calls=4, parallel_tool_calls=False),
    )
    return AgentRuntime(agent, InMemoryRunJournal(), workspace=workspace)


@pytest.mark.asyncio
async def test_a_tool_write_lands_in_the_worktree_and_never_the_primary_repo(
    repository: Path,
    tmp_path: Path,
) -> None:
    before_head = _git(repository, "rev-parse", "HEAD")
    before_status = _git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    model = _WritingModel(path="notes/agent.md", content="written by the agent")
    runtime = _runtime(repository, tmp_path, model)

    handle = await runtime.submit(StartRun(prompt="write the note", session_id="ws-e2e"))
    snapshot = await runtime.wait(handle.run_id, timeout_s=10)

    assert snapshot.status == "completed"
    assert snapshot.counts.tool_executions == 1

    # The write landed in the Session's isolated worktree...
    worktree = tmp_path / "managed" / "ws-e2e"
    assert (worktree / "notes" / "agent.md").read_text(encoding="utf-8") == (
        "written by the agent"
    )
    # ...and the user's primary repository is byte-for-byte untouched.
    assert _git(repository, "rev-parse", "HEAD") == before_head
    assert _git(repository, "status", "--porcelain=v1", "--untracked-files=all") == (
        before_status
    )
    assert not (repository / "notes").exists()

    await runtime.close()


@pytest.mark.asyncio
async def test_the_runtime_checkpoints_the_worktree_around_a_file_write(
    repository: Path,
    tmp_path: Path,
) -> None:
    model = _WritingModel(path="tracked.txt", content="v1")
    runtime = _runtime(repository, tmp_path, model)

    handle = await runtime.submit(StartRun(prompt="write it", session_id="ws-ckpt"))
    await runtime.wait(handle.run_id, timeout_s=10)
    events = await runtime.journal.read(handle.run_id)

    checkpoints = [
        event for event in events if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
    ]
    phases = [str(event.data.get("phase")) for event in checkpoints]

    # A write-ahead checkpoint before the tool is what makes an interrupted
    # idempotent write recoverable; the after-tool one records what changed.
    assert "run_start" in phases
    assert "before_tool" in phases
    assert "after_tool" in phases

    after_tool = next(
        event for event in checkpoints if event.data.get("phase") == "after_tool"
    )
    diff = after_tool.data["diff"]
    # Durable event payloads are deeply frozen, so this is a mappingproxy.
    assert isinstance(diff, Mapping)
    assert diff["files_changed"] == 1
    assert list(diff["paths"]) == ["tracked.txt"]

    # The durable projection stays content-free: the diff names the path and
    # counts lines, never the bytes that were written.
    assert "v1" not in str(after_tool.data)

    await runtime.close()


@pytest.mark.asyncio
async def test_a_denied_path_never_reaches_the_filesystem_through_the_runtime(
    repository: Path,
    tmp_path: Path,
) -> None:
    escape = tmp_path / "escaped.txt"
    model = _WritingModel(path="../../escaped.txt", content="should not exist")
    runtime = _runtime(repository, tmp_path, model)

    handle = await runtime.submit(StartRun(prompt="try to escape", session_id="ws-deny"))
    snapshot = await runtime.wait(handle.run_id, timeout_s=10)

    # The model is free to ask; the tool is what refuses. The run still
    # completes so the model can see the refusal and choose another path.
    assert snapshot.status == "completed"
    assert not escape.exists()
    assert not (tmp_path / "managed" / "escaped.txt").exists()

    await runtime.close()
