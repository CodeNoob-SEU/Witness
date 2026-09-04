"""Contract tests for the built-in repository tools."""

from __future__ import annotations

import os
from itertools import pairwise
from pathlib import Path

import pytest

from react_agent import ObservationEffect, ReActAgent, ToolResumePolicy
from react_agent.models import ModelRequest, ModelResponse, ToolCall
from react_agent.repo_tools import (
    REPOSITORY_TOOLS_VERSION,
    CommandResult,
    ContainerCommandRunner,
    LocalCommandRunner,
    RepositoryToolError,
    create_repository_tools,
)
from react_agent.tools import Tool, ToolExecutionContext


def _context(workspace: Path | None) -> ToolExecutionContext:
    return ToolExecutionContext(
        run_id="run",
        execution_id="exec",
        call_id="call",
        call_key="s1:t0",
        operation_id="op",
        attempt=1,
        idempotency_key="key",
        workspace_path=workspace,
    )


class _NoModel:
    async def complete(self, request: ModelRequest) -> ModelResponse:
        raise AssertionError("model must not be called")


def _by_name(tools: tuple[Tool, ...]) -> dict[str, Tool]:
    return {registered.name: registered for registered in tools}


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "pkg").mkdir(parents=True)
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (root / "pkg" / "mod.py").write_text(
        "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n"
    )
    (root / "README.md").write_text("# demo\n")
    (root / ".env").write_text("OPENAI_API_KEY=leak\n")
    (root / "image.bin").write_bytes(b"\x00\x01\x02")
    return root


@pytest.mark.asyncio
async def test_workspace_path_from_context_wins_over_fallback_root(
    repo: Path, tmp_path: Path
) -> None:
    tools = _by_name(create_repository_tools(root=tmp_path / "elsewhere"))
    result = await tools["list_dir"].invoke({"path": "."}, context=_context(repo))
    assert result["entries"] == ["README.md", "image.bin", "pkg/"]
    with pytest.raises(RepositoryToolError):
        await tools["list_dir"].invoke({"path": "."}, context=_context(None))


@pytest.mark.asyncio
async def test_listing_hides_git_metadata_and_sensitive_files(repo: Path) -> None:
    tools = _by_name(create_repository_tools())
    result = await tools["list_dir"].invoke({"path": "."}, context=_context(repo))
    assert ".git/" not in result["entries"]
    assert ".env" not in result["entries"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["../outside.txt", "/etc/passwd", "~/x", ".git/HEAD", ".env", "pkg/../../x"],
)
async def test_paths_outside_policy_are_refused(repo: Path, path: str) -> None:
    tools = _by_name(create_repository_tools())
    with pytest.raises(RepositoryToolError):
        await tools["read_file"].invoke(
            {"path": path, "start_line": None, "end_line": None}, context=_context(repo)
        )


@pytest.mark.asyncio
async def test_symlink_escape_is_refused(repo: Path, tmp_path: Path) -> None:
    secret = tmp_path / "secret.txt"
    secret.write_text("outside")
    (repo / "link.txt").symlink_to(secret)
    tools = _by_name(create_repository_tools())
    with pytest.raises(RepositoryToolError):
        await tools["read_file"].invoke(
            {"path": "link.txt", "start_line": None, "end_line": None}, context=_context(repo)
        )
    with pytest.raises(RepositoryToolError):
        await tools["write_file"].invoke(
            {"path": "link.txt", "content": "x"}, context=_context(repo)
        )
    assert secret.read_text() == "outside"


@pytest.mark.asyncio
async def test_read_file_ranges_and_binary_guard(repo: Path) -> None:
    tools = _by_name(create_repository_tools())
    whole = await tools["read_file"].invoke(
        {"path": "pkg/mod.py", "start_line": None, "end_line": None}, context=_context(repo)
    )
    assert whole["total_lines"] == 6
    assert whole["content"].splitlines()[0] == "1| def add(a, b):"
    part = await tools["read_file"].invoke(
        {"path": "pkg/mod.py", "start_line": 5, "end_line": 99}, context=_context(repo)
    )
    assert part["start_line"] == 5 and part["end_line"] == 6
    assert part["content"] == "5| def sub(a, b):\n6|     return a - b"
    with pytest.raises(RepositoryToolError):
        await tools["read_file"].invoke(
            {"path": "image.bin", "start_line": None, "end_line": None}, context=_context(repo)
        )


@pytest.mark.asyncio
async def test_read_limit_is_enforced(repo: Path) -> None:
    tools = _by_name(create_repository_tools(max_read_bytes=8))
    with pytest.raises(RepositoryToolError):
        await tools["read_file"].invoke(
            {"path": "pkg/mod.py", "start_line": None, "end_line": None}, context=_context(repo)
        )


@pytest.mark.asyncio
async def test_search_text_prunes_hidden_directories(repo: Path) -> None:
    (repo / "__pycache__").mkdir()
    (repo / "__pycache__" / "junk.py").write_text("def add(): pass\n")
    tools = _by_name(create_repository_tools())
    result = await tools["search_text"].invoke(
        {"pattern": r"def (add|sub)", "path": ".", "glob": "*.py"}, context=_context(repo)
    )
    assert result["matches"] == ["pkg/mod.py:1: def add(a, b):", "pkg/mod.py:5: def sub(a, b):"]
    assert result["files_scanned"] == 1
    with pytest.raises(RepositoryToolError):
        await tools["search_text"].invoke(
            {"pattern": "(", "path": ".", "glob": "*"}, context=_context(repo)
        )


@pytest.mark.asyncio
async def test_edit_file_is_unique_and_idempotent(repo: Path) -> None:
    tools = _by_name(create_repository_tools())
    edit = {"path": "pkg/mod.py", "old_string": "return a + b", "new_string": "return b + a"}
    first = await tools["edit_file"].invoke(edit, context=_context(repo))
    assert first == {"path": "pkg/mod.py", "already_applied": False}
    second = await tools["edit_file"].invoke(edit, context=_context(repo))
    assert second == {"path": "pkg/mod.py", "already_applied": True}
    assert (repo / "pkg" / "mod.py").read_text().count("return b + a") == 1
    with pytest.raises(RepositoryToolError, match="not found"):
        await tools["edit_file"].invoke(
            {"path": "pkg/mod.py", "old_string": "missing", "new_string": "x"},
            context=_context(repo),
        )
    with pytest.raises(RepositoryToolError, match="occurs 2 times"):
        await tools["edit_file"].invoke(
            {"path": "pkg/mod.py", "old_string": "(a, b)", "new_string": "(x, y)"},
            context=_context(repo),
        )


@pytest.mark.asyncio
async def test_write_file_creates_parents_inside_workspace(repo: Path) -> None:
    tools = _by_name(create_repository_tools())
    result = await tools["write_file"].invoke(
        {"path": "pkg/sub/new.py", "content": "x = 1\n"}, context=_context(repo)
    )
    assert result == {"path": "pkg/sub/new.py", "bytes": 6}
    assert (repo / "pkg" / "sub" / "new.py").read_text() == "x = 1\n"
    with pytest.raises(RepositoryToolError):
        await tools["write_file"].invoke(
            {"path": "secrets.json", "content": "{}"}, context=_context(repo)
        )


@pytest.mark.asyncio
async def test_run_command_uses_a_scrubbed_environment(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-should-not-leak")
    monkeypatch.setenv("REACT_AGENT_POSTGRES_DSN", "postgresql://secret")
    tools = _by_name(create_repository_tools(command_runner=LocalCommandRunner(env={"DEMO": "1"})))
    result = await tools["run_command"].invoke(
        {"command": "env; pwd", "read_only": True}, context=_context(repo)
    )
    assert result["exit_code"] == 0 and result["timed_out"] is False
    assert "OPENAI_API_KEY" not in result["stdout"]
    assert "REACT_AGENT_POSTGRES_DSN" not in result["stdout"]
    assert "DEMO=1" in result["stdout"]
    assert result["stdout"].rstrip().endswith(str(repo))


@pytest.mark.asyncio
async def test_run_command_timeout_kills_the_process_group(repo: Path) -> None:
    tools = _by_name(create_repository_tools(command_timeout_s=0.5))
    result = await tools["run_command"].invoke(
        {"command": "echo started; sleep 30; echo finished", "read_only": False},
        context=_context(repo),
    )
    assert result["timed_out"] is True and result["exit_code"] is None
    assert "started" in result["stdout"] and "finished" not in result["stdout"]


@pytest.mark.asyncio
async def test_run_tests_quotes_model_arguments(repo: Path) -> None:
    captured: list[str] = []

    class Recorder:
        async def run(self, command: str, *, cwd: Path, timeout_s: float) -> CommandResult:
            captured.append(command)
            return CommandResult(0, False, "ok", "", 1.0)

    tools = _by_name(create_repository_tools(command_runner=Recorder(), test_command="pytest -q"))
    result = await tools["run_tests"].invoke(
        {"args": "tests/a.py -k 'x or y'; rm -rf /"}, context=_context(repo)
    )
    assert result["exit_code"] == 0
    assert captured == ["pytest -q tests/a.py -k 'x or y;' rm -rf /"]


def test_container_runner_argv_mounts_workspace_as_current_user(tmp_path: Path) -> None:
    runner = ContainerCommandRunner(
        "example/image:1",
        shell="/bin/bash",
        setup=". /opt/venv/bin/activate",
        extra_args=("--memory", "1g"),
    )
    argv = runner.argv("pytest -q", cwd=tmp_path)
    assert argv[:3] == ("docker", "run", "--rm")
    assert ("--network", "none") == argv[3:5]
    assert f"{tmp_path}:/workspace" in argv
    assert ("--user", f"{os.getuid()}:{os.getgid()}") in pairwise(argv)
    assert argv[-3:] == ("/bin/bash", "-c", ". /opt/venv/bin/activate && pytest -q")
    assert "example/image:1" in argv and "--memory" in argv


def test_declared_policies_and_manifest_are_stable() -> None:
    tools = _by_name(create_repository_tools())
    idempotent = ("list_dir", "read_file", "search_text", "write_file", "edit_file", "run_tests")
    assert set(tools) == {*idempotent, "run_command"}
    for name in idempotent:
        assert tools[name].idempotent
        assert tools[name].resume_policy is ToolResumePolicy.IDEMPOTENT_RETRY
    assert tools["run_command"].idempotent is False
    assert tools["run_command"].resume_policy is ToolResumePolicy.REQUIRE_OPERATOR
    assert tools["run_command"].has_call_resume_policy is True
    assert tools["read_file"].context_policy.effect is ObservationEffect.READ
    assert tools["read_file"].context_policy.identity_fields == ("path", "start_line", "end_line")
    assert tools["edit_file"].context_policy.effect is ObservationEffect.MUTATE
    assert tools["edit_file"].context_policy.identity_fields == ("path",)
    assert tools["run_tests"].context_policy.effect is ObservationEffect.EXECUTE
    assert all(registered.version == REPOSITORY_TOOLS_VERSION for registered in tools.values())
    # Context parameters never leak into the model-facing schema.
    for registered in tools.values():
        assert "context" not in registered.spec.parameters["properties"]
    first = ReActAgent(_NoModel(), create_repository_tools()).tool_manifest_hash
    second = ReActAgent(
        _NoModel(), create_repository_tools(command_timeout_s=5.0)
    ).tool_manifest_hash
    assert first == second, "runtime-only settings must not change the tool manifest"


@pytest.mark.asyncio
async def test_run_command_can_require_approval(repo: Path) -> None:
    tools = _by_name(create_repository_tools(require_command_approval=True))
    assert tools["run_command"].requires_approval is True


@pytest.mark.parametrize(
    ("arguments", "policy"),
    [
        ('{"command":"git status","read_only":true}', ToolResumePolicy.IDEMPOTENT_RETRY),
        ('{"command":"pip install .","read_only":false}', ToolResumePolicy.REQUIRE_OPERATOR),
        ('{"command":"git status"}', ToolResumePolicy.REQUIRE_OPERATOR),
        ("not json", ToolResumePolicy.REQUIRE_OPERATOR),
    ],
)
def test_run_command_resume_policy_is_decided_per_call(
    arguments: str, policy: ToolResumePolicy
) -> None:
    tools = _by_name(create_repository_tools())
    call = ToolCall("call-1", "run_command", arguments)
    assert tools["run_command"].resume_policy_for(call) is policy
    # Other tools keep their static policy regardless of arguments.
    assert (
        tools["run_tests"].resume_policy_for(ToolCall("call-2", "run_tests", "{}"))
        is ToolResumePolicy.IDEMPOTENT_RETRY
    )
