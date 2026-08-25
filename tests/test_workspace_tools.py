"""Safety contract for the tools that can actually change files.

These are the only tools able to mutate a repository, so the tests below focus
on what must be impossible: escaping the managed worktree, touching a denied
path, or writing anywhere at all when no workspace is configured.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from react_agent import ToolCall, ToolRegistry
from react_agent.workspace_tools import (
    MAX_READ_BYTES,
    list_workspace_files,
    read_workspace_file,
    workspace_tools,
    write_workspace_file,
)


def _exists(path: Path) -> bool:
    """Filesystem probe kept out of the async frames ruff guards."""

    return path.exists()


async def _call(
    name: str,
    arguments: dict[str, object],
    *,
    workspace: Path | None,
) -> dict[str, object]:
    registry = ToolRegistry(workspace_tools())
    message = await registry.execute(
        ToolCall(f"call-{name}", name, json.dumps(arguments)),
        run_id="run-1",
        approval_handler=None,
        max_output_chars=100_000,
        call_key="s1:t0",
        workspace_path=workspace,
    )
    payload = json.loads(message.content)
    assert payload["ok"] is True, payload
    result = payload["data"]
    assert isinstance(result, dict)
    return result


@pytest.mark.asyncio
async def test_writes_and_reads_round_trip_inside_the_workspace(tmp_path: Path) -> None:
    written = await _call(
        "write_workspace_file",
        {"path": "src/notes.txt", "content": "hello"},
        workspace=tmp_path,
    )
    assert written["status"] == "ok"
    assert written["created"] is True
    assert (tmp_path / "src" / "notes.txt").read_text(encoding="utf-8") == "hello"

    read = await _call(
        "read_workspace_file", {"path": "src/notes.txt"}, workspace=tmp_path
    )
    assert read["status"] == "ok"
    assert read["content"] == "hello"

    listed = await _call("list_workspace_files", {"subdirectory": ""}, workspace=tmp_path)
    assert listed["status"] == "ok"
    assert listed["files"] == ["src/notes.txt"]


@pytest.mark.asyncio
async def test_rewriting_the_same_content_is_idempotent(tmp_path: Path) -> None:
    first = await _call(
        "write_workspace_file", {"path": "a.txt", "content": "v1"}, workspace=tmp_path
    )
    second = await _call(
        "write_workspace_file", {"path": "a.txt", "content": "v1"}, workspace=tmp_path
    )

    # The declared idempotency is what lets Resume retry a crashed write, so
    # the second attempt must leave exactly the same tree.
    assert first["created"] is True
    assert second["created"] is False
    assert (tmp_path / "a.txt").read_text(encoding="utf-8") == "v1"
    assert write_workspace_file.idempotent is True
    assert write_workspace_file.resume_policy.value == "idempotent_retry"
    # Two writers in one turn could target one file, and the Runtime takes a
    # single before-tool checkpoint per call.
    assert write_workspace_file.parallel_safe is False


@pytest.mark.asyncio
async def test_no_workspace_means_no_file_access_at_all(tmp_path: Path) -> None:
    outside = tmp_path / "should-not-exist.txt"
    stray = Path("should-not-exist.txt")
    assert not _exists(stray)

    denied = await _call(
        "write_workspace_file",
        {"path": "should-not-exist.txt", "content": "nope"},
        workspace=None,
    )

    # Falling back to the server's working directory would escape every
    # isolation guarantee the workspace adapter provides.
    assert denied["status"] == "denied"
    assert "managed Session workspace" in str(denied["reason"])
    assert not _exists(outside)
    assert not _exists(stray)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "../escape.txt",
        "nested/../../escape.txt",
        "/etc/passwd",
        "/tmp/escape.txt",
    ],
)
async def test_traversal_and_absolute_paths_are_refused(tmp_path: Path, path: str) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()

    denied = await _call(
        "write_workspace_file", {"path": path, "content": "x"}, workspace=workspace
    )

    assert denied["status"] == "denied"
    assert not _exists(tmp_path / "escape.txt")


@pytest.mark.asyncio
async def test_a_symlink_pointing_out_of_the_workspace_is_refused(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    secret = tmp_path / "outside.txt"
    secret.write_text("classified", encoding="utf-8")
    try:
        (workspace / "link.txt").symlink_to(secret)
    except (OSError, NotImplementedError) as exc:  # pragma: no cover - platform dependent
        pytest.skip(f"symbolic links are unavailable: {type(exc).__name__}")

    read = await _call("read_workspace_file", {"path": "link.txt"}, workspace=workspace)
    written = await _call(
        "write_workspace_file",
        {"path": "link.txt", "content": "overwritten"},
        workspace=workspace,
    )

    # Resolving the whole link chain is what catches this: the name looks
    # ordinary, only its target escapes.
    assert read["status"] == "denied"
    assert written["status"] == "denied"
    assert secret.read_text(encoding="utf-8") == "classified"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [".env", "id_rsa", "config/credentials.json", "a/.env.prod"])
async def test_sensitive_paths_stay_denied_to_tools(tmp_path: Path, path: str) -> None:
    denied = await _call(
        "write_workspace_file", {"path": path, "content": "secret"}, workspace=tmp_path
    )

    assert denied["status"] == "denied"
    assert "sensitive-path policy" in str(denied["reason"])
    assert not _exists(tmp_path / path)


@pytest.mark.asyncio
async def test_listing_hides_git_internals_and_denied_paths(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("ref: refs/heads/main", encoding="utf-8")
    (tmp_path / ".env").write_text("TOKEN=1", encoding="utf-8")
    (tmp_path / "keep.txt").write_text("ok", encoding="utf-8")

    listed = await _call("list_workspace_files", {"subdirectory": ""}, workspace=tmp_path)

    assert listed["files"] == ["keep.txt"]


@pytest.mark.asyncio
async def test_oversized_and_binary_reads_are_refused(tmp_path: Path) -> None:
    (tmp_path / "big.txt").write_bytes(b"x" * (MAX_READ_BYTES + 1))
    (tmp_path / "binary.txt").write_bytes(b"\xff\xfe\x00\x01")

    big = await _call("read_workspace_file", {"path": "big.txt"}, workspace=tmp_path)
    binary = await _call("read_workspace_file", {"path": "binary.txt"}, workspace=tmp_path)
    missing = await _call("read_workspace_file", {"path": "nope.txt"}, workspace=tmp_path)

    assert big["status"] == "denied"
    assert binary["status"] == "denied"
    assert missing["status"] == "not_found"


def test_file_tools_never_expose_contents_to_the_debug_stream() -> None:
    # File contents can be anything the repository holds, so these tools keep
    # the default metadata-only exposure rather than opting into FULL.
    for registered in workspace_tools():
        assert registered.debug_exposure.value == "metadata"
    assert read_workspace_file.idempotent is True
    assert list_workspace_files.parallel_safe is True
