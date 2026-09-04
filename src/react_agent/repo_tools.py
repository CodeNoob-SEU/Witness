"""Built-in repository tools for workspace-bound coding tasks.

The Runtime isolates each Session in a managed Git worktree, but a coding
Agent still needs a small, well-declared vocabulary to explore, edit, and
verify that tree.  ``create_repository_tools`` returns seven typed tools whose
resume and context semantics are declared once, correctly:

* ``list_dir`` / ``read_file`` / ``search_text`` are idempotent reads;
* ``write_file`` / ``edit_file`` are idempotent mutations keyed by ``path``, so
  Tier 1 context governance can retire superseded observations;
* ``run_tests`` is an idempotent execution that resume may retry automatically;
* ``run_command`` is *not* idempotent by default and fails closed into
  operator reconciliation when a worker dies mid-command; a call the model
  declares ``read_only=true`` is planned with ``idempotent_retry`` instead.

Every path is resolved inside the workspace injected through
``ToolExecutionContext``; symlink escapes, ``.git`` internals, and sensitive
files (keys, ``.env``, credentials) are refused.  Commands run through a
``CommandRunner`` seam with a scrubbed environment so the Runtime's own API
keys never reach model-authored shell commands.
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
import re
import shlex
import signal
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .context import ObservationEffect, ToolContextPolicy
from .tools import DebugExposure, Tool, ToolError, ToolExecutionContext, ToolResumePolicy
from .workspace import _DEFAULT_SENSITIVE_PATTERNS, _is_sensitive

REPOSITORY_TOOLS_VERSION = "repo-tools-v2"

_HIDDEN_ENTRIES = frozenset({".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"})
_DEFAULT_ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "TZ")
_MAX_SEARCH_MATCHES = 200
_MAX_DIR_ENTRIES = 500
_BINARY_PROBE_BYTES = 8192


class RepositoryToolError(ToolError):
    """A repository tool refused an argument; the message is written for the model."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Bounded outcome of one shell command."""

    exit_code: int | None
    timed_out: bool
    stdout: str
    stderr: str
    duration_ms: float


class CommandRunner(Protocol):
    """Execute one shell command with the workspace as its working directory."""

    async def run(self, command: str, *, cwd: Path, timeout_s: float) -> CommandResult: ...


def _scrubbed_env(passthrough: Sequence[str], extra: Mapping[str, str] | None) -> dict[str, str]:
    env = {name: os.environ[name] for name in passthrough if name in os.environ}
    env.setdefault("PATH", os.defpath)
    if extra:
        env.update(extra)
    return env


async def _run_argv(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout_s: float,
    max_chars: int,
) -> CommandResult:
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        env=dict(env),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout, stderr = bytearray(), bytearray()

    async def pump(stream: asyncio.StreamReader, buffer: bytearray) -> None:
        while chunk := await stream.read(65_536):
            buffer += chunk

    timed_out = False
    try:
        async with asyncio.timeout(timeout_s):
            await asyncio.gather(pump(process.stdout, stdout), pump(process.stderr, stderr))
            await process.wait()
    except TimeoutError:
        timed_out = True
        _kill_process_group(process)
        await process.wait()
    except asyncio.CancelledError:
        _kill_process_group(process)
        await process.wait()
        raise
    duration_ms = round((time.monotonic() - started) * 1000, 3)
    return CommandResult(
        exit_code=None if timed_out else process.returncode,
        timed_out=timed_out,
        stdout=_tail(bytes(stdout).decode("utf-8", errors="replace"), max_chars),
        stderr=_tail(bytes(stderr).decode("utf-8", errors="replace"), max_chars),
        duration_ms=duration_ms,
    )


def _kill_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _tail(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return "...[truncated]...\n" + text[-max_chars:]


class LocalCommandRunner:
    """Run ``sh -c`` in the workspace with only an allowlisted environment."""

    def __init__(
        self,
        *,
        shell: str = "/bin/sh",
        env: Mapping[str, str] | None = None,
        passthrough: Sequence[str] = _DEFAULT_ENV_PASSTHROUGH,
        max_output_chars: int = 40_000,
    ) -> None:
        self._shell = shell
        self._env = _scrubbed_env(passthrough, env)
        self._max_output_chars = max_output_chars

    @property
    def environment(self) -> Mapping[str, str]:
        return dict(self._env)

    async def run(self, command: str, *, cwd: Path, timeout_s: float) -> CommandResult:
        return await _run_argv(
            (self._shell, "-c", command),
            cwd=cwd,
            env=self._env,
            timeout_s=timeout_s,
            max_chars=self._max_output_chars,
        )


class ContainerCommandRunner:
    """Run commands in a disposable container with the workspace bind-mounted.

    The container runs as the invoking user so it cannot leave root-owned
    files that would break Git checkpoint restore, and it has no network by
    default.  ``setup`` is a shell prefix executed before the command, for
    example activating a virtual environment baked into the image.
    """

    def __init__(
        self,
        image: str,
        *,
        mount_path: str = "/workspace",
        docker: str = "docker",
        shell: str = "/bin/sh",
        setup: str | None = None,
        network: str = "none",
        run_as_current_user: bool = True,
        extra_args: Sequence[str] = (),
        env: Mapping[str, str] | None = None,
        max_output_chars: int = 40_000,
    ) -> None:
        if not image.strip():
            raise ValueError("container image must not be empty")
        if not mount_path.startswith("/"):
            raise ValueError("mount_path must be absolute")
        self._image = image
        self._mount_path = mount_path
        self._docker = docker
        self._shell = shell
        self._setup = setup
        self._network = network
        self._run_as_current_user = run_as_current_user
        self._extra_args = tuple(extra_args)
        self._env = _scrubbed_env((*_DEFAULT_ENV_PASSTHROUGH, "DOCKER_HOST"), env)
        self._max_output_chars = max_output_chars

    def argv(self, command: str, *, cwd: Path) -> tuple[str, ...]:
        script = f"{self._setup} && {command}" if self._setup else command
        argv: list[str] = [
            self._docker,
            "run",
            "--rm",
            "--network",
            self._network,
            "-e",
            "HOME=/tmp",
            "-e",
            "PYTHONDONTWRITEBYTECODE=1",
            "-v",
            f"{cwd}:{self._mount_path}",
            "-w",
            self._mount_path,
        ]
        if self._run_as_current_user and hasattr(os, "getuid"):
            argv += ["--user", f"{os.getuid()}:{os.getgid()}"]
        argv += [*self._extra_args, self._image, self._shell, "-c", script]
        return tuple(argv)

    async def run(self, command: str, *, cwd: Path, timeout_s: float) -> CommandResult:
        return await _run_argv(
            self.argv(command, cwd=cwd),
            cwd=cwd,
            env=self._env,
            timeout_s=timeout_s,
            max_chars=self._max_output_chars,
        )


class _Workspace:
    """Path resolution and policy checks shared by every repository tool."""

    def __init__(
        self,
        *,
        fallback_root: Path | None,
        sensitive_patterns: Sequence[str],
        max_read_bytes: int,
    ) -> None:
        self._fallback_root = fallback_root.resolve() if fallback_root is not None else None
        self._sensitive_patterns = tuple(sensitive_patterns)
        self._max_read_bytes = max_read_bytes

    def root(self, context: ToolExecutionContext) -> Path:
        if context.workspace_path is not None:
            return Path(context.workspace_path).resolve()
        if self._fallback_root is not None:
            return self._fallback_root
        raise RepositoryToolError(
            "No workspace is bound to this run; configure a workspace adapter or a root."
        )

    def resolve(self, context: ToolExecutionContext, path: str, *, for_write: bool = False) -> Path:
        if not path or path.startswith("/") or path.startswith("~"):
            raise RepositoryToolError("path must be relative to the repository root.")
        root = self.root(context)
        candidate = (root / path).resolve()
        if candidate != root and root not in candidate.parents:
            raise RepositoryToolError("path escapes the workspace.")
        relative = candidate.relative_to(root).as_posix()
        if relative == ".git" or relative.startswith(".git/"):
            raise RepositoryToolError("Git metadata is not accessible through tools.")
        if _is_sensitive(relative, self._sensitive_patterns):
            raise RepositoryToolError("path matches a sensitive-file pattern.")
        if for_write:
            for parent in (candidate, *candidate.parents):
                if parent == root:
                    break
                if parent.is_symlink():
                    raise RepositoryToolError("writes through symlinks are not allowed.")
        return candidate

    def relative(self, context: ToolExecutionContext, path: Path) -> str:
        return path.relative_to(self.root(context)).as_posix()

    def read_text(self, target: Path) -> str:
        if not target.is_file():
            raise RepositoryToolError("file not found.")
        size = target.stat().st_size
        if size > self._max_read_bytes:
            raise RepositoryToolError(
                f"file is {size} bytes; the read limit is {self._max_read_bytes} bytes."
            )
        with target.open("rb") as handle:
            probe = handle.read(_BINARY_PROBE_BYTES)
            if b"\x00" in probe:
                raise RepositoryToolError("binary files cannot be read as text.")
            data = probe + handle.read()
        return data.decode("utf-8", errors="replace")

    def is_hidden(self, relative: str) -> bool:
        parts = relative.split("/")
        return any(part in _HIDDEN_ENTRIES for part in parts) or _is_sensitive(
            relative, self._sensitive_patterns
        )


def create_repository_tools(
    *,
    command_runner: CommandRunner | None = None,
    root: Path | None = None,
    test_command: str = "python -m pytest -p no:cacheprovider",
    command_timeout_s: float = 300.0,
    test_timeout_s: float = 900.0,
    require_command_approval: bool = False,
    max_read_bytes: int = 2 * 1024 * 1024,
    sensitive_patterns: Sequence[str] | None = None,
) -> tuple[Tool, ...]:
    """Create the seven repository tools bound to the run's managed workspace.

    ``root`` is only a fallback for callers without a workspace adapter; when
    the Runtime injects ``ToolExecutionContext.workspace_path`` it always wins.
    """

    if command_timeout_s <= 0 or test_timeout_s <= 0:
        raise ValueError("command timeouts must be positive")
    if not test_command.strip():
        raise ValueError("test_command must not be blank")
    runner: CommandRunner = command_runner or LocalCommandRunner()
    workspace = _Workspace(
        fallback_root=root,
        sensitive_patterns=(
            tuple(sensitive_patterns)
            if sensitive_patterns is not None
            else _DEFAULT_SENSITIVE_PATTERNS
        ),
        max_read_bytes=max_read_bytes,
    )

    def list_dir(path: str, *, context: ToolExecutionContext) -> dict[str, Any]:
        """List one directory relative to the repository root ("." for the root).

        Directories end with "/".
        """

        target = workspace.resolve(context, path)
        if not target.is_dir():
            raise RepositoryToolError("not a directory.")
        entries: list[str] = []
        for entry in sorted(target.iterdir(), key=lambda item: item.name):
            relative = workspace.relative(context, entry)
            if workspace.is_hidden(relative):
                continue
            entries.append(entry.name + ("/" if entry.is_dir() else ""))
        return {
            "path": path,
            "entries": entries[:_MAX_DIR_ENTRIES],
            "truncated": len(entries) > _MAX_DIR_ENTRIES,
        }

    def read_file(
        path: str,
        start_line: int | None,
        end_line: int | None,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, Any]:
        """Read a UTF-8 text file.

        Lines are 1-based and inclusive; pass null for both to read the whole
        file. Each returned line is prefixed with its number and "|".
        """

        target = workspace.resolve(context, path)
        lines = workspace.read_text(target).splitlines()
        start = max(1, start_line or 1)
        end = min(len(lines), end_line or len(lines))
        if start_line is not None and start_line < 1:
            raise RepositoryToolError("start_line must be >= 1.")
        if end_line is not None and end_line < start:
            raise RepositoryToolError("end_line must be >= start_line.")
        body = "\n".join(f"{number}| {lines[number - 1]}" for number in range(start, end + 1))
        return {
            "path": path,
            "total_lines": len(lines),
            "start_line": start,
            "end_line": end,
            "content": body,
        }

    def search_text(
        pattern: str,
        path: str,
        glob: str,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, Any]:
        """Search files for a Python regular expression.

        path is a directory relative to the root ("." for everything); glob
        filters file names, e.g. "*.py". Returns up to 200 "file:line: text"
        matches.
        """

        try:
            regex = re.compile(pattern)
        except re.error as exc:
            raise RepositoryToolError(f"invalid regular expression: {exc}") from None
        base = workspace.resolve(context, path)
        if not base.is_dir():
            raise RepositoryToolError("path must be a directory.")
        matches: list[str] = []
        scanned = 0
        for directory, dirnames, filenames in os.walk(base):
            directory_path = Path(directory)
            dirnames[:] = sorted(
                name
                for name in dirnames
                if not workspace.is_hidden(workspace.relative(context, directory_path / name))
                and not (directory_path / name).is_symlink()
            )
            for name in sorted(filenames):
                file = directory_path / name
                relative = workspace.relative(context, file)
                if workspace.is_hidden(relative) or file.is_symlink() or not file.is_file():
                    continue
                if not fnmatch.fnmatchcase(name, glob):
                    continue
                try:
                    text = workspace.read_text(file)
                except (RepositoryToolError, OSError):
                    continue
                scanned += 1
                for number, line in enumerate(text.splitlines(), 1):
                    if regex.search(line):
                        matches.append(f"{relative}:{number}: {line.strip()[:240]}")
                        if len(matches) >= _MAX_SEARCH_MATCHES:
                            return {
                                "matches": matches,
                                "files_scanned": scanned,
                                "truncated": True,
                            }
        return {"matches": matches, "files_scanned": scanned, "truncated": False}

    def write_file(path: str, content: str, *, context: ToolExecutionContext) -> dict[str, Any]:
        """Create or fully overwrite one UTF-8 text file.

        Prefer edit_file for changes to existing files.
        """

        target = workspace.resolve(context, path, for_write=True)
        if target.is_dir():
            raise RepositoryToolError("path is a directory.")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"path": path, "bytes": len(content.encode("utf-8"))}

    def edit_file(
        path: str,
        old_string: str,
        new_string: str,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, Any]:
        """Replace exactly one occurrence of old_string with new_string.

        old_string must be unique in the file; include surrounding lines to
        disambiguate. Re-applying an edit that already took effect reports
        already_applied instead of failing.
        """

        if not old_string:
            raise RepositoryToolError("old_string must not be empty.")
        target = workspace.resolve(context, path, for_write=True)
        text = workspace.read_text(target)
        count = text.count(old_string)
        if count == 0:
            if new_string and new_string in text:
                return {"path": path, "already_applied": True}
            raise RepositoryToolError("old_string was not found in the file.")
        if count > 1:
            raise RepositoryToolError(
                f"old_string occurs {count} times; add context to make it unique."
            )
        target.write_text(text.replace(old_string, new_string, 1), encoding="utf-8")
        return {"path": path, "already_applied": False}

    async def run_command(
        command: str, read_only: bool, *, context: ToolExecutionContext
    ) -> dict[str, Any]:
        """Run one shell command from the repository root.

        Returns the exit code and bounded output. Use it for exploration and
        builds; use edit_file/write_file to change files. Set read_only=true
        only when the command changes nothing (no files, no state, no external
        systems), e.g. git status, ls, cat, grep, python -c "import x": such a
        call is retried automatically if the process crashes while it runs.
        A crash during any other command requires an operator.
        """

        if not command.strip():
            raise RepositoryToolError("command must not be blank.")
        result = await runner.run(command, cwd=workspace.root(context), timeout_s=command_timeout_s)
        return {"read_only": read_only, **_command_payload(result)}

    async def run_tests(args: str, *, context: ToolExecutionContext) -> dict[str, Any]:
        """Run the project's test command with extra arguments.

        Example: "tests/test_x.py -x -q". Returns exit code and bounded output.
        """

        command = " ".join((test_command, *_quoted_test_args(args))).strip()
        result = await runner.run(command, cwd=workspace.root(context), timeout_s=test_timeout_s)
        return {"command": command, **_command_payload(result)}

    read_tools: dict[str, Callable[..., Any]] = {
        "list_dir": list_dir,
        "read_file": read_file,
        "search_text": search_text,
    }
    read_policies = {
        "list_dir": ToolContextPolicy(ObservationEffect.READ, ("path",)),
        "read_file": ToolContextPolicy(ObservationEffect.READ, ("path", "start_line", "end_line")),
        "search_text": ToolContextPolicy(),
    }
    tools: list[Tool] = [
        Tool(
            fn,
            name=name,
            timeout_s=60.0,
            idempotent=True,
            parallel_safe=True,
            allow_repeated=True,
            context_policy=read_policies[name],
            debug_exposure=DebugExposure.METADATA,
            version=REPOSITORY_TOOLS_VERSION,
        )
        for name, fn in read_tools.items()
    ]
    mutate_tools: dict[str, Callable[..., Any]] = {"write_file": write_file, "edit_file": edit_file}
    tools += [
        Tool(
            fn,
            name=name,
            timeout_s=60.0,
            idempotent=True,
            parallel_safe=False,
            context_policy=ToolContextPolicy(ObservationEffect.MUTATE, ("path",)),
            debug_exposure=DebugExposure.METADATA,
            version=REPOSITORY_TOOLS_VERSION,
        )
        for name, fn in mutate_tools.items()
    ]
    tools.append(
        Tool(
            run_tests,
            name="run_tests",
            timeout_s=test_timeout_s + 30.0,
            idempotent=True,
            parallel_safe=False,
            allow_repeated=True,
            context_policy=ToolContextPolicy(ObservationEffect.EXECUTE, ("args",)),
            debug_exposure=DebugExposure.METADATA,
            version=REPOSITORY_TOOLS_VERSION,
        )
    )
    tools.append(
        Tool(
            run_command,
            name="run_command",
            timeout_s=command_timeout_s + 30.0,
            requires_approval=require_command_approval,
            idempotent=False,
            parallel_safe=False,
            allow_repeated=True,
            context_policy=ToolContextPolicy(ObservationEffect.EXECUTE, ("command",)),
            debug_exposure=DebugExposure.METADATA,
            call_resume_policy=_run_command_resume_policy,
            version=REPOSITORY_TOOLS_VERSION,
        )
    )
    return tuple(tools)


def _run_command_resume_policy(arguments: Mapping[str, Any]) -> ToolResumePolicy:
    """Only a command the model declared read-only may be retried after a crash."""

    if arguments.get("read_only") is True:
        return ToolResumePolicy.IDEMPOTENT_RETRY
    return ToolResumePolicy.REQUIRE_OPERATOR


def _quoted_test_args(args: str) -> tuple[str, ...]:
    """Tokenize model-supplied test arguments and re-quote them for the shell.

    The prefix is operator-configured; only literal tokens from the model reach
    the command line, so redirections, pipes, and substitutions are inert.
    """

    try:
        tokens = shlex.split(args)
    except ValueError as exc:
        raise RepositoryToolError(f"args could not be parsed: {exc}") from None
    return tuple(shlex.quote(token) for token in tokens)


def _command_payload(result: CommandResult) -> dict[str, Any]:
    return {
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
        "duration_ms": result.duration_ms,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


__all__ = [
    "REPOSITORY_TOOLS_VERSION",
    "CommandResult",
    "CommandRunner",
    "ContainerCommandRunner",
    "LocalCommandRunner",
    "RepositoryToolError",
    "create_repository_tools",
]
