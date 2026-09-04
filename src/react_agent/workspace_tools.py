"""Tools that read and write inside a Session's isolated Git worktree.

These are the only tools in the package that can change files, so they are
deliberately narrow. Every path is resolved *inside* the managed worktree;
absolute paths, ``..`` traversal, symlinks that leave the tree, and the
workspace module's sensitive-path denylist are all refused. Without a managed
workspace they fail closed rather than falling back to the server's working
directory.

Register them only when a workspace adapter is configured::

    from react_agent.workspace_tools import workspace_tools

    agent = ReActAgent(model, [*workspace_tools(), calculate_expression])

Recoverable problems (missing file, denied path) are returned as structured
results instead of raised, so the model can correct itself and retry rather
than seeing an opaque tool failure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import Field

from .tools import Tool, ToolExecutionContext, tool
from .workspace import _DEFAULT_SENSITIVE_PATTERNS, _is_sensitive, _safe_relative_path

MAX_READ_BYTES = 256 * 1024
MAX_WRITE_CHARS = 200_000
MAX_LISTED_ENTRIES = 400

_DENIED = "denied"
_NOT_FOUND = "not_found"
_OK = "ok"


def _workspace_root(context: ToolExecutionContext) -> Path | None:
    """Return the managed worktree, or ``None`` when there is not one.

    A tool that silently used the process's current directory would escape
    every isolation guarantee the workspace adapter exists to provide.
    """

    root = context.workspace_path
    if root is None:
        return None
    resolved = root.resolve()
    return resolved if resolved.is_dir() else None


def _resolve(context: ToolExecutionContext, path: str) -> tuple[Path, Path] | str:
    """Resolve one workspace-relative path, or return a refusal reason."""

    root = _workspace_root(context)
    if root is None:
        return "this tool requires a managed Session workspace"
    try:
        relative = _safe_relative_path(path)
    except Exception:
        return "path must be workspace-relative and must not contain '..'"
    if _is_sensitive(relative, _DEFAULT_SENSITIVE_PATTERNS):
        return "path is denied by the workspace sensitive-path policy"
    candidate = root / relative
    # resolve() walks the whole symlink chain, so a link pointing outside the
    # worktree is rejected here even though its own name looks harmless.
    resolved = candidate.resolve()
    if resolved != root and not resolved.is_relative_to(root):
        return "path escapes the managed workspace"
    return root, candidate


@tool(
    name="list_workspace_files",
    idempotent=True,
    parallel_safe=True,
    timeout_s=10.0,
)
def list_workspace_files(
    subdirectory: Annotated[
        str,
        Field(max_length=512, description="Workspace-relative directory, or '' for the root."),
    ],
    *,
    context: ToolExecutionContext,
) -> dict[str, object]:
    """List tracked-looking files in the Session workspace; never leaves it."""

    root = _workspace_root(context)
    if root is None:
        return {"status": _DENIED, "reason": "this tool requires a managed Session workspace"}
    if subdirectory.strip():
        resolved = _resolve(context, subdirectory)
        if isinstance(resolved, str):
            return {"status": _DENIED, "reason": resolved}
        _, directory = resolved
    else:
        directory = root
    if not directory.is_dir():
        return {"status": _NOT_FOUND, "reason": "no such directory", "path": subdirectory}

    entries: list[str] = []
    truncated = False
    for item in sorted(directory.rglob("*")):
        if ".git" in item.relative_to(root).parts or not item.is_file():
            continue
        relative = item.relative_to(root).as_posix()
        if _is_sensitive(relative, _DEFAULT_SENSITIVE_PATTERNS):
            continue
        if len(entries) >= MAX_LISTED_ENTRIES:
            truncated = True
            break
        entries.append(relative)
    return {"status": _OK, "files": entries, "truncated": truncated}


@tool(
    name="read_workspace_file",
    idempotent=True,
    parallel_safe=True,
    timeout_s=10.0,
)
def read_workspace_file(
    path: Annotated[
        str,
        Field(min_length=1, max_length=512, description="Workspace-relative file path."),
    ],
    *,
    context: ToolExecutionContext,
) -> dict[str, object]:
    """Read one UTF-8 text file from the Session workspace."""

    resolved = _resolve(context, path)
    if isinstance(resolved, str):
        return {"status": _DENIED, "reason": resolved, "path": path}
    _, target = resolved
    if not target.is_file():
        return {"status": _NOT_FOUND, "reason": "no such file", "path": path}
    size = target.stat().st_size
    if size > MAX_READ_BYTES:
        return {
            "status": _DENIED,
            "reason": f"file exceeds the {MAX_READ_BYTES} byte read limit",
            "path": path,
            "bytes": size,
        }
    try:
        content = target.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return {"status": _DENIED, "reason": "file is not UTF-8 text", "path": path}
    return {"status": _OK, "path": path, "content": content, "bytes": size}


@tool(
    name="write_workspace_file",
    # Writing the same bytes to the same path twice leaves the same tree, so a
    # crashed attempt is safe to retry. That, plus the Runtime's stable
    # idempotency key, is what lets Resume continue without an operator.
    idempotent=True,
    # Two writes in one model turn may target the same file, and the Runtime
    # takes one before-tool workspace checkpoint per call; overlapping writers
    # would make that checkpoint ambiguous.
    parallel_safe=False,
    timeout_s=15.0,
)
def write_workspace_file(
    path: Annotated[
        str,
        Field(min_length=1, max_length=512, description="Workspace-relative file path."),
    ],
    content: Annotated[
        str,
        Field(max_length=MAX_WRITE_CHARS, description="Full UTF-8 contents to write."),
    ],
    *,
    context: ToolExecutionContext,
) -> dict[str, object]:
    """Create or replace one UTF-8 text file inside the Session workspace."""

    resolved = _resolve(context, path)
    if isinstance(resolved, str):
        return {"status": _DENIED, "reason": resolved, "path": path}
    root, target = resolved
    parent = target.parent
    if parent != root and not parent.resolve().is_relative_to(root):
        return {"status": _DENIED, "reason": "path escapes the managed workspace", "path": path}
    parent.mkdir(parents=True, exist_ok=True)
    existed = target.is_file()
    target.write_text(content, encoding="utf-8")
    return {
        "status": _OK,
        "path": path,
        "bytes": len(content.encode("utf-8")),
        "created": not existed,
        # Surfacing the key makes the retry-after-crash story checkable from
        # the transcript alone.
        "idempotency_key": context.idempotency_key,
    }


def workspace_tools() -> tuple[Tool, ...]:
    """Return the workspace tool set in a deterministic order."""

    return (list_workspace_files, read_workspace_file, write_workspace_file)


__all__ = [
    "MAX_LISTED_ENTRIES",
    "MAX_READ_BYTES",
    "MAX_WRITE_CHARS",
    "list_workspace_files",
    "read_workspace_file",
    "workspace_tools",
    "write_workspace_file",
]
