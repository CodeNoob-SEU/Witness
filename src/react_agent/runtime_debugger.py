"""Stateful, policy-enforcing Python runtime debugger.

This module is the single behavioural seam shared by the in-process Agent
tools and the MCP stdio adapter.  It deliberately exposes a small Python-only
debugging surface while hiding DAP sequencing, stop-scoped references, path
policy, deterministic frame ranking, output bounds, and secret redaction.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import math
import os
import re
import sys
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from .debug_evidence import DebugObservationError, seal_debug_observation
from .debugpy_dap import DAPError, DAPEvent, DAPTimeoutError, DebugpyDAPClient

DEBUG_OBSERVATION_SCHEMA_VERSION = 1
FRAME_SELECTION_RULE_VERSION = "workspace-user-frame-v1"
TOOL_INTERFACE_VERSION = "python-runtime-debug-v1"

_SECRET_NAME = re.compile(
    r"(?:api[_-]?key|authorization|cookie|credential|passwd|password|"
    r"private[_-]?key|secret|(?:access|refresh|id)?[_-]?token)",
    re.IGNORECASE,
)
_TOKENISH_VALUE = re.compile(r"^[A-Za-z0-9_+/=.\-]{24,}$")
_TOKENISH_FRAGMENT = re.compile(r"[A-Za-z0-9_+/=.\-]{24,}")
_WORKSPACE_PATH_FRAGMENT = re.compile(
    r"\$WORKSPACE(?:[\\/][^\s,;\"'}\]]*)?"
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(api[_-]?key|authorization|cookie|credential|passwd|password|"
    r"private[_-]?key|secret|(?:access|refresh|id)?[_-]?token)"
    r"([\"']?\s*[:=]\s*[\"']?)([^\s,;\"'}]+)"
)
_INTERNAL_PARTS = frozenset({"debugpy", "site-packages", "lib-dynload"})


def _single_line(value: str) -> str:
    """Make untrusted debugger text safe for one-line evidence fields."""

    return value.replace("\x00", "\\0").replace("\r", "\\r").replace("\n", "\\n")


class DebugSessionState(StrEnum):
    """Externally observable debugger lifecycle."""

    CREATED = "created"
    INITIALIZING = "initializing"
    CONFIGURING = "configuring"
    RUNNING = "running"
    STOPPED = "stopped"
    EXITED = "exited"


class DebuggerError(RuntimeError):
    """A safe, structured debugger failure suitable for a tool observation."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message
        self.retryable = retryable


class DebugBreakpoint(BaseModel):
    """One source and its complete desired breakpoint line set."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    file: str = Field(min_length=1, max_length=4096)
    lines: list[int] = Field(min_length=0, max_length=256)


class DebugAdapter(Protocol):
    """Internal DAP seam; debugpy and deterministic fakes are adapters."""

    @property
    def closed(self) -> bool: ...

    @property
    def pid(self) -> int | None: ...

    async def initialize(
        self,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    def launch(
        self,
        arguments: Mapping[str, object],
        *,
        timeout_s: float | None = None,
    ) -> asyncio.Task[dict[str, object]]: ...

    async def wait_for_event(self, event: str, *, timeout_s: float | None = None) -> DAPEvent: ...

    async def next_event(self, *, timeout_s: float | None = None) -> DAPEvent: ...

    async def request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def set_breakpoints(
        self,
        source_path: str | os.PathLike[str],
        lines: Sequence[int],
        *,
        source_modified: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def configuration_done(self, *, timeout_s: float | None = None) -> dict[str, object]: ...

    async def threads(self, *, timeout_s: float | None = None) -> dict[str, object]: ...

    async def continue_(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def next(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def step_in(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def step_out(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def pause(
        self, thread_id: int, *, timeout_s: float | None = None
    ) -> dict[str, object]: ...

    async def stack_trace(
        self,
        thread_id: int,
        *,
        start_frame: int = 0,
        levels: int | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def scopes(
        self, frame_id: int, *, timeout_s: float | None = None
    ) -> dict[str, object]: ...

    async def variables(
        self,
        variables_reference: int,
        *,
        start: int = 0,
        count: int | None = None,
        filter_: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def disconnect(
        self,
        *,
        restart: bool = False,
        terminate_debuggee: bool = True,
        suspend_debuggee: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]: ...

    async def close(self) -> None: ...


DebugAdapterFactory = Callable[..., Awaitable[DebugAdapter]]
ObservationRecorder = Callable[[Mapping[str, object]], object]


async def _default_adapter_factory(**kwargs: object) -> DebugAdapter:
    return await DebugpyDAPClient.start(**cast(dict[str, Any], kwargs))


@dataclass(slots=True)
class _DebugSession:
    session_id: str
    owner_id: str
    adapter: DebugAdapter
    program: Path
    cwd: Path
    state: DebugSessionState = DebugSessionState.CREATED
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    stop_id: int = 0
    thread_id: int | None = None
    selected_frame_index: int | None = None
    frames: dict[int, tuple[int, dict[str, object]]] = field(default_factory=dict)
    raw_top_frame: dict[str, object] | None = None
    desired_breakpoints: dict[Path, tuple[int, ...]] = field(default_factory=dict)
    last_exception: dict[str, object] | None = None
    exit_code: int | None = None


def _finite_positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise DebuggerError("INVALID_ARGUMENT", f"{name} must be a positive finite number.")
    return value


def _bounded_int(value: int, name: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise DebuggerError("INVALID_ARGUMENT", f"{name} must be between {minimum} and {maximum}.")
    return value


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: dict[str, int] = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    length = len(value)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())


class PythonRuntimeDebugger:
    """Own all Python runtime-debugging policy behind seven tool operations.

    The workspace root is trusted configuration, never a model argument.  All
    source paths are resolved through it and checked after symlink resolution.
    DAP frame and variable references never cross this interface; callers use
    the monotonically increasing ``stop_id`` plus stable frame indexes.
    """

    def __init__(
        self,
        workspace_root: str | Path,
        *,
        python_executable: str | os.PathLike[str] = sys.executable,
        adapter_factory: DebugAdapterFactory = _default_adapter_factory,
        recorder: ObservationRecorder | None = None,
        dap_timeout_s: float = 10.0,
        default_wait_timeout_s: float = 5.0,
        max_stack_frames: int = 64,
        max_variables: int = 128,
        max_value_chars: int = 2048,
        max_observation_bytes: int = 256 * 1024,
    ) -> None:
        root = Path(workspace_root).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError("workspace_root must be an existing directory")
        self.workspace_root = root
        self.python_executable = os.fspath(python_executable)
        self._adapter_factory = adapter_factory
        self._recorder = recorder
        self._dap_timeout_s = _finite_positive(dap_timeout_s, "dap_timeout_s")
        self._default_wait_timeout_s = _finite_positive(
            default_wait_timeout_s, "default_wait_timeout_s"
        )
        self._max_stack_frames = _bounded_int(
            max_stack_frames, "max_stack_frames", minimum=1, maximum=512
        )
        self._max_variables = _bounded_int(max_variables, "max_variables", minimum=1, maximum=2048)
        self._max_value_chars = _bounded_int(
            max_value_chars, "max_value_chars", minimum=16, maximum=32768
        )
        self._max_observation_bytes = _bounded_int(
            max_observation_bytes,
            "max_observation_bytes",
            minimum=4096,
            maximum=8 * 1024 * 1024,
        )
        self._sessions: dict[str, _DebugSession] = {}
        self._registry_lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> PythonRuntimeDebugger:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.close()

    async def launch(
        self,
        program: str,
        args: list[str],
        breakpoints: list[DebugBreakpoint],
        exception_policy: Literal["uncaught", "raised", "none"],
        stop_on_entry: bool,
        wait_timeout_s: float,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Launch one workspace Python script and wait for its first stop or exit."""

        session_id = uuid.uuid4().hex
        adapter: DebugAdapter | None = None
        adapter_registered = False
        try:
            self._ensure_open()
            owner_id = self._validate_owner_id(_owner_id)
            timeout = self._validate_wait_timeout(wait_timeout_s)
            program_path = self._workspace_path(program, kind="program", require_file=True)
            if program_path.suffix.lower() not in {".py", ".pyw"}:
                raise DebuggerError("INVALID_PROGRAM", "Only Python source programs are allowed.")
            safe_args = self._validate_args(args)
            normalized_breakpoints = self._normalize_breakpoints(breakpoints)
            adapter = await self._adapter_factory(
                python_executable=self.python_executable,
                cwd=self.workspace_root,
                request_timeout_s=self._dap_timeout_s,
            )
            session = _DebugSession(
                session_id=session_id,
                owner_id=owner_id,
                adapter=adapter,
                program=program_path,
                cwd=self.workspace_root,
            )
            async with self._registry_lock:
                self._ensure_open()
                self._sessions[session_id] = session
                adapter_registered = True
            transitions = [DebugSessionState.CREATED.value]
            async with session.lock:
                session.state = DebugSessionState.INITIALIZING
                transitions.append(session.state.value)
                capabilities = await adapter.initialize(timeout_s=self._dap_timeout_s)
                launch_task = adapter.launch(
                    {
                        "program": str(program_path),
                        "cwd": str(self.workspace_root),
                        "args": safe_args,
                        "python": self.python_executable,
                        "justMyCode": False,
                        "stopOnEntry": stop_on_entry,
                        "subProcess": False,
                        "redirectOutput": True,
                    },
                    timeout_s=self._dap_timeout_s * (len(normalized_breakpoints) + 3),
                )
                try:
                    await adapter.wait_for_event("initialized", timeout_s=self._dap_timeout_s)
                    session.state = DebugSessionState.CONFIGURING
                    transitions.append(session.state.value)
                    verified: list[dict[str, object]] = []
                    for source, lines in normalized_breakpoints:
                        verified.extend(await self._install_breakpoints(session, source, lines))
                    if exception_policy != "none":
                        filters = ["uncaught"] if exception_policy == "uncaught" else ["raised"]
                        await adapter.request(
                            "setExceptionBreakpoints",
                            {"filters": filters},
                            timeout_s=self._dap_timeout_s,
                        )
                    if capabilities.get("supportsConfigurationDoneRequest") is True:
                        await adapter.configuration_done(timeout_s=self._dap_timeout_s)
                    await launch_task
                except BaseException:
                    if not launch_task.done():
                        launch_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await launch_task
                    raise
                session.state = DebugSessionState.RUNNING
                transitions.append(session.state.value)
                stopped = await self._wait_until_stopped_or_exited(session, timeout)
                if transitions[-1] != session.state.value:
                    transitions.append(session.state.value)
                payload: dict[str, object] = {
                    "program": self._relative(program_path),
                    "breakpoints": verified,
                    "exception_policy": exception_policy,
                    "reproduction": {
                        "command": [
                            Path(self.python_executable).name,
                            self._relative(program_path),
                            *(self._safe_reproduction_arg(value) for value in safe_args),
                        ],
                        "cwd": ".",
                    },
                    "transitions": transitions,
                    **stopped,
                }
            return await self._success("launch", session, payload)
        except asyncio.CancelledError:
            if adapter is not None and not adapter_registered:
                with suppress(Exception):
                    await adapter.close()
            await self._close_failed_launch(session_id)
            raise
        except (DebuggerError, DAPError, OSError) as exc:
            if adapter is not None and not adapter_registered:
                with suppress(Exception):
                    await adapter.close()
            await self._close_failed_launch(session_id)
            return await self._failure("launch", session_id, exc)

    async def set_breakpoints(
        self,
        debug_session_id: str,
        file: str,
        lines: list[int],
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Replace all breakpoints for one workspace source file."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            self._require_state(
                session,
                {
                    DebugSessionState.CONFIGURING,
                    DebugSessionState.RUNNING,
                    DebugSessionState.STOPPED,
                },
            )
            source = self._workspace_path(file, kind="breakpoint source", require_file=True)
            normalized = self._validate_lines(lines)
            verified = await self._install_breakpoints(session, source, normalized)
            return {"file": self._relative(source), "breakpoints": verified}

        return await self._with_session(
            "set_breakpoints", debug_session_id, operation, owner_id=_owner_id
        )

    async def control(
        self,
        debug_session_id: str,
        action: Literal["continue", "next", "step_in", "step_out", "pause"],
        stop_id: int | None,
        wait_timeout_s: float,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Continue, step, or pause a session and wait for its next stop or exit."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            if action not in {"continue", "next", "step_in", "step_out", "pause"}:
                raise DebuggerError("INVALID_ARGUMENT", "Unsupported debugger control action.")
            timeout = self._validate_wait_timeout(wait_timeout_s)
            if action == "pause":
                self._require_state(session, {DebugSessionState.RUNNING})
                if stop_id is not None:
                    raise DebuggerError("INVALID_ARGUMENT", "pause requires stop_id to be null.")
                queued = await self._wait_until_stopped_or_exited(session, min(timeout, 0.01))
                if session.state is not DebugSessionState.RUNNING:
                    return {"control": action, "already_settled": True, **queued}
                thread_id = await self._running_thread_id(session)
                try:
                    await session.adapter.pause(thread_id, timeout_s=self._dap_timeout_s)
                except (DAPError, OSError):
                    try:
                        queued = await self._wait_until_stopped_or_exited(
                            session, min(timeout, 0.01)
                        )
                    except (DAPError, OSError):
                        queued = {"wait_timed_out": True}
                    if session.state is not DebugSessionState.RUNNING:
                        return {
                            "control": action,
                            "already_settled": True,
                            **queued,
                        }
                    await session.adapter.close()
                    session.state = DebugSessionState.EXITED
                    self._invalidate_suspended_references(session)
                    raise
            else:
                self._require_current_stop(session, stop_id)
                thread_id = self._stopped_thread_id(session)
                self._invalidate_suspended_references(session)
                session.state = DebugSessionState.RUNNING
                try:
                    if action == "continue":
                        await session.adapter.continue_(thread_id, timeout_s=self._dap_timeout_s)
                    elif action == "next":
                        await session.adapter.next(thread_id, timeout_s=self._dap_timeout_s)
                    elif action == "step_in":
                        await session.adapter.step_in(thread_id, timeout_s=self._dap_timeout_s)
                    else:
                        await session.adapter.step_out(thread_id, timeout_s=self._dap_timeout_s)
                except (DAPError, OSError):
                    await session.adapter.close()
                    session.state = DebugSessionState.EXITED
                    self._invalidate_suspended_references(session)
                    raise
            outcome = await self._wait_until_stopped_or_exited(session, timeout)
            return {"control": action, **outcome}

        return await self._with_session("control", debug_session_id, operation, owner_id=_owner_id)

    async def stack(
        self,
        debug_session_id: str,
        stop_id: int,
        levels: int,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Read and deterministically rank frames at the current stop."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            self._require_current_stop(session, stop_id)
            bounded_levels = _bounded_int(
                levels, "levels", minimum=1, maximum=self._max_stack_frames
            )
            frames = await self._load_stack(session, bounded_levels)
            suggested = frames[0]["frame_index"] if frames else None
            return {
                "selection_rule_version": FRAME_SELECTION_RULE_VERSION,
                "suggested_frame_index": suggested,
                "frames": frames,
            }

        return await self._with_session("stack", debug_session_id, operation, owner_id=_owner_id)

    async def select_frame(
        self,
        debug_session_id: str,
        stop_id: int,
        frame_index: int,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Select one current-stop frame without exposing its DAP frame id."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            self._require_current_stop(session, stop_id)
            index = _bounded_int(
                frame_index, "frame_index", minimum=0, maximum=self._max_stack_frames - 1
            )
            if not session.frames:
                await self._load_stack(session, self._max_stack_frames)
            entry = session.frames.get(index)
            if entry is None:
                raise DebuggerError("FRAME_NOT_FOUND", "The requested frame is not available.")
            session.selected_frame_index = index
            return {"selected_frame": entry[1]}

        return await self._with_session(
            "select_frame", debug_session_id, operation, owner_id=_owner_id
        )

    async def variables(
        self,
        debug_session_id: str,
        stop_id: int,
        scope: Literal["locals", "arguments"],
        max_variables: int,
        max_value_chars: int,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Read bounded, redacted variables from the selected current-stop frame."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            self._require_current_stop(session, stop_id)
            count = _bounded_int(
                max_variables, "max_variables", minimum=1, maximum=self._max_variables
            )
            value_limit = _bounded_int(
                max_value_chars,
                "max_value_chars",
                minimum=16,
                maximum=self._max_value_chars,
            )
            if session.selected_frame_index is None:
                raise DebuggerError(
                    "FRAME_NOT_SELECTED", "Select a frame before requesting variables."
                )
            selected_entry = session.frames.get(session.selected_frame_index)
            if selected_entry is None:
                raise DebuggerError(
                    "FRAME_NOT_SELECTED",
                    "Select a current ranked frame before requesting variables.",
                )
            raw_frame_id, selected = selected_entry
            scope_body = await session.adapter.scopes(raw_frame_id, timeout_s=self._dap_timeout_s)
            argument_names: frozenset[str] | None = None
            try:
                scope_entry = self._choose_scope(scope_body, scope)
            except DebuggerError:
                if scope != "arguments":
                    raise
                scope_entry = self._choose_scope(scope_body, "locals")
                argument_names = await asyncio.to_thread(self._argument_names, selected)
            reference = scope_entry.get("variablesReference")
            if not isinstance(reference, int) or isinstance(reference, bool) or reference <= 0:
                raise DebuggerError("SCOPE_UNAVAILABLE", "The requested scope has no variables.")
            fetch_count = self._max_variables if argument_names is not None else count
            variable_body = await session.adapter.variables(
                reference,
                count=fetch_count,
                timeout_s=self._dap_timeout_s,
            )
            normalized = self._normalize_variables(variable_body, fetch_count, value_limit, scope)
            if argument_names is not None:
                normalized = [
                    variable for variable in normalized if variable.get("name") in argument_names
                ][:count]
            return {
                "selected_frame": selected,
                "scope": scope,
                "variables": normalized,
                "returned_variables": len(normalized),
            }

        return await self._with_session(
            "variables", debug_session_id, operation, owner_id=_owner_id
        )

    async def stop(
        self,
        debug_session_id: str,
        *,
        _owner_id: str = "standalone",
    ) -> dict[str, object]:
        """Terminate the debuggee and reap the entire owned process group."""

        async def operation(session: _DebugSession) -> dict[str, object]:
            if session.state is DebugSessionState.RUNNING:
                try:
                    await self._wait_until_stopped_or_exited(session, 0.01)
                except (DAPError, OSError):
                    pass
            previous = session.state
            terminal_status = "exited" if previous is DebugSessionState.EXITED else "terminated"
            try:
                if not session.adapter.closed:
                    await session.adapter.disconnect(
                        terminate_debuggee=True,
                        timeout_s=self._dap_timeout_s,
                    )
                else:
                    await session.adapter.close()
            finally:
                session.state = DebugSessionState.EXITED
                self._invalidate_suspended_references(session)
            return {
                "previous_state": previous.value,
                "process_reaped": True,
                "debuggee_exit": {
                    "status": terminal_status,
                    "exit_code": session.exit_code,
                    "signal": None,
                },
            }

        return await self._with_session("stop", debug_session_id, operation, owner_id=_owner_id)

    async def close_owner(self, owner_id: str) -> None:
        """Reap and forget every session fenced to one Agent execution."""

        normalized = self._validate_owner_id(owner_id)
        async with self._registry_lock:
            sessions = tuple(
                session for session in self._sessions.values() if session.owner_id == normalized
            )
        await self._close_sessions(sessions)

    async def reject(
        self,
        action: str,
        debug_session_id: str,
        error: DebuggerError,
    ) -> dict[str, object]:
        """Create one sealed structured rejection for a thin host adapter."""

        return await self._failure(action, debug_session_id, error)

    async def close(self) -> None:
        """Close every live session. Safe to call repeatedly."""

        async with self._registry_lock:
            self._closed = True
            sessions = tuple(self._sessions.values())
        await self._close_sessions(sessions)

    async def _with_session(
        self,
        action: str,
        session_id: str,
        operation: Callable[[_DebugSession], Awaitable[dict[str, object]]],
        *,
        owner_id: str,
    ) -> dict[str, object]:
        session: _DebugSession | None = None
        try:
            self._ensure_open()
            session = await self._session(session_id, owner_id=owner_id)
            async with session.lock:
                payload = await operation(session)
            return await self._success(action, session, payload)
        except asyncio.CancelledError:
            if session is not None:
                await self._force_close(session)
            raise
        except (DebuggerError, DAPError, OSError) as exc:
            return await self._failure(action, session_id, exc)

    async def _success(
        self,
        action: str,
        session: _DebugSession,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        observation: dict[str, object] = {
            "schema_version": DEBUG_OBSERVATION_SCHEMA_VERSION,
            "observation_kind": "python_runtime_debug",
            "action": action,
            "debug_session_id": session.session_id,
            "state": session.state.value,
            "success": True,
            **payload,
        }
        if session.stop_id > 0 and session.state is DebugSessionState.STOPPED:
            observation["stop_id"] = session.stop_id
        if session.last_exception is not None and session.state is DebugSessionState.STOPPED:
            observation["exception"] = session.last_exception
        sealed = self._bound_and_seal(observation)
        await self._record(sealed)
        return sealed

    async def _failure(
        self,
        action: str,
        session_id: str,
        error: BaseException,
    ) -> dict[str, object]:
        if isinstance(error, DebuggerError):
            code = error.code
            message = error.safe_message
            retryable = error.retryable
        elif isinstance(error, DAPTimeoutError):
            code = "DAP_TIMEOUT"
            message = "The debug adapter did not respond before the deadline."
            retryable = True
        elif isinstance(error, DAPError):
            code = "DAP_ERROR"
            message = "The debug adapter rejected or ended the operation."
            retryable = False
        else:
            code = "DEBUGGER_IO"
            message = "The debugger could not complete a local I/O operation."
            retryable = False
        session = self._sessions.get(session_id)
        state = session.state.value if session is not None else DebugSessionState.EXITED.value
        observation: dict[str, object] = {
            "schema_version": DEBUG_OBSERVATION_SCHEMA_VERSION,
            "observation_kind": "python_runtime_debug",
            "action": action,
            "debug_session_id": _single_line(session_id)[:512] or "invalid-session",
            "state": state,
            "success": False,
            "error": {"code": code, "message": message, "retryable": retryable},
        }
        if session is not None and session.stop_id > 0:
            observation["stop_id"] = session.stop_id
        sealed = self._bound_and_seal(observation)
        await self._record(sealed)
        return sealed

    async def _record(self, observation: Mapping[str, object]) -> None:
        if self._recorder is None:
            return
        outcome = self._recorder(observation)
        if inspect.isawaitable(outcome):
            await cast(Awaitable[object], outcome)

    def _bound_and_seal(self, observation: Mapping[str, object]) -> dict[str, object]:
        encoded = _canonical_json(observation).encode("utf-8")
        if len(encoded) > self._max_observation_bytes:
            raise DebuggerError(
                "OBSERVATION_TOO_LARGE",
                "The normalized debugger observation exceeded its hard byte budget.",
            )
        try:
            return cast(dict[str, object], seal_debug_observation(observation))
        except DebugObservationError as exc:
            raise DebuggerError(
                "INVALID_OBSERVATION",
                "The normalized debugger observation violated the evidence contract.",
            ) from exc

    def _ensure_open(self) -> None:
        if self._closed:
            raise DebuggerError("DEBUGGER_CLOSED", "The runtime debugger is closed.")

    async def _session(self, session_id: str, *, owner_id: str) -> _DebugSession:
        if not session_id.strip():
            raise DebuggerError("INVALID_ARGUMENT", "debug_session_id must not be blank.")
        normalized_owner = self._validate_owner_id(owner_id)
        async with self._registry_lock:
            session = self._sessions.get(session_id)
        if session is None or session.owner_id != normalized_owner:
            raise DebuggerError("SESSION_NOT_FOUND", "The debug session does not exist.")
        return session

    @staticmethod
    def _validate_owner_id(owner_id: str) -> str:
        if not isinstance(owner_id, str) or not owner_id.strip() or len(owner_id) > 512:
            raise DebuggerError("INVALID_OWNER", "The debugger execution owner is invalid.")
        return owner_id

    def _workspace_path(self, raw: str, *, kind: str, require_file: bool) -> Path:
        if not raw.strip() or any(character in raw for character in ("\x00", "\r", "\n")):
            raise DebuggerError("INVALID_PATH", f"The {kind} path is invalid.")
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = self.workspace_root / candidate
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(self.workspace_root)
        except (OSError, ValueError):
            raise DebuggerError(
                "PATH_OUTSIDE_WORKSPACE",
                f"The {kind} must resolve to an existing path inside the workspace.",
            ) from None
        if require_file and not resolved.is_file():
            raise DebuggerError("INVALID_PATH", f"The {kind} must be a file.")
        return resolved

    def _relative(self, path: Path) -> str:
        try:
            return path.resolve(strict=False).relative_to(self.workspace_root).as_posix()
        except ValueError:
            return "<outside-workspace>"

    def _validate_args(self, args: Sequence[str]) -> list[str]:
        if len(args) > 128:
            raise DebuggerError("INVALID_ARGUMENT", "args may contain at most 128 entries.")
        result: list[str] = []
        for value in args:
            if not isinstance(value, str) or "\x00" in value or len(value) > 8192:
                raise DebuggerError("INVALID_ARGUMENT", "Each program argument must be safe text.")
            result.append(value)
        return result

    def _safe_reproduction_arg(self, value: str) -> str:
        redacted_text, redacted, _ = self._redact_text(value)
        if redacted:
            return _single_line(redacted_text)
        candidate = redacted_text.strip("'\"")
        if _TOKENISH_VALUE.fullmatch(candidate) and _entropy(candidate) >= 3.5:
            return "<redacted:high-entropy>"
        return _single_line(redacted_text)

    def _redact_text(self, value: str) -> tuple[str, bool, str | None]:
        """Redact secret assignments and token-like fragments from persisted text."""

        normalized = value.replace(str(self.workspace_root), "$WORKSPACE")
        assignment_redacted = False

        def replace_assignment(match: re.Match[str]) -> str:
            nonlocal assignment_redacted
            assignment_redacted = True
            return f"{match.group(1)}{match.group(2)}<redacted:secret-value>"

        normalized = _SECRET_ASSIGNMENT.sub(replace_assignment, normalized)
        entropy_redacted = False

        def replace_fragment(match: re.Match[str]) -> str:
            nonlocal entropy_redacted
            candidate = match.group(0)
            if _entropy(candidate) < 3.5:
                return candidate
            entropy_redacted = True
            return "<redacted:high-entropy>"

        # Workspace-relative paths are already an allowlisted diagnostic
        # value. Protect each complete normalized path while scanning other
        # fragments so a long filename cannot make `$WORKSPACE` look tokenish.
        workspace_paths: list[str] = []

        def protect_workspace_path(match: re.Match[str]) -> str:
            workspace_paths.append(match.group(0))
            return f"⟦workspace-path-{len(workspace_paths) - 1}⟧"

        normalized = _WORKSPACE_PATH_FRAGMENT.sub(protect_workspace_path, normalized)
        normalized = _TOKENISH_FRAGMENT.sub(replace_fragment, normalized)
        for index, workspace_path in enumerate(workspace_paths):
            normalized = normalized.replace(f"⟦workspace-path-{index}⟧", workspace_path)
        if assignment_redacted:
            return normalized, True, "secret_assignment"
        if entropy_redacted:
            return normalized, True, "high_entropy"
        return normalized, False, None

    def _validate_lines(self, lines: Sequence[int]) -> tuple[int, ...]:
        if len(lines) > 256:
            raise DebuggerError(
                "INVALID_ARGUMENT", "Breakpoint lines may contain at most 256 entries."
            )
        normalized: list[int] = []
        for line in lines:
            checked = _bounded_int(line, "breakpoint line", minimum=1, maximum=10_000_000)
            if checked not in normalized:
                normalized.append(checked)
        return tuple(sorted(normalized))

    def _normalize_breakpoints(
        self, breakpoints: Sequence[DebugBreakpoint]
    ) -> tuple[tuple[Path, tuple[int, ...]], ...]:
        if len(breakpoints) > 64:
            raise DebuggerError(
                "INVALID_ARGUMENT", "breakpoints may contain at most 64 source files."
            )
        result: dict[Path, set[int]] = {}
        for breakpoint in breakpoints:
            source = self._workspace_path(
                breakpoint.file, kind="breakpoint source", require_file=True
            )
            result.setdefault(source, set()).update(self._validate_lines(breakpoint.lines))
        return tuple((path, tuple(sorted(lines))) for path, lines in sorted(result.items()))

    def _validate_wait_timeout(self, value: float) -> float:
        if value == 0:
            return self._default_wait_timeout_s
        timeout = _finite_positive(value, "wait_timeout_s")
        if timeout > 60:
            raise DebuggerError("INVALID_ARGUMENT", "wait_timeout_s cannot exceed 60 seconds.")
        return timeout

    async def _install_breakpoints(
        self,
        session: _DebugSession,
        source: Path,
        lines: Sequence[int],
    ) -> list[dict[str, object]]:
        body = await session.adapter.set_breakpoints(source, lines, timeout_s=self._dap_timeout_s)
        session.desired_breakpoints[source] = tuple(lines)
        raw_breakpoints = body.get("breakpoints")
        response = raw_breakpoints if isinstance(raw_breakpoints, list) else []
        normalized: list[dict[str, object]] = []
        for index, requested in enumerate(lines):
            raw = (
                response[index]
                if index < len(response) and isinstance(response[index], dict)
                else {}
            )
            actual_line = raw.get("line")
            verified = raw.get("verified") is True
            item: dict[str, object] = {
                "file": self._relative(source),
                "requested_line": requested,
                "verified": verified,
                "actual_line": actual_line if isinstance(actual_line, int) else requested,
            }
            message = raw.get("message")
            if isinstance(message, str) and message:
                item["message"] = self._redact_text(message)[0][:512]
            normalized.append(item)
        return normalized

    async def _wait_until_stopped_or_exited(
        self, session: _DebugSession, timeout_s: float
    ) -> dict[str, object]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return {"wait_timed_out": True}
            try:
                event = await session.adapter.next_event(timeout_s=remaining)
            except DAPTimeoutError:
                return {"wait_timed_out": True}
            if event.event == "stopped":
                return await self._accept_stopped(session, event)
            if event.event == "exited":
                raw_code = event.body.get("exitCode")
                session.exit_code = (
                    raw_code
                    if isinstance(raw_code, int) and not isinstance(raw_code, bool)
                    else None
                )
                session.state = DebugSessionState.EXITED
                self._invalidate_suspended_references(session)
                return {"exit_code": session.exit_code}
            if event.event == "terminated":
                session.state = DebugSessionState.EXITED
                self._invalidate_suspended_references(session)
                return {"exit_code": session.exit_code}

    async def _accept_stopped(self, session: _DebugSession, event: DAPEvent) -> dict[str, object]:
        raw_thread = event.body.get("threadId")
        if isinstance(raw_thread, int) and not isinstance(raw_thread, bool) and raw_thread > 0:
            session.thread_id = raw_thread
        else:
            session.thread_id = await self._running_thread_id(session)
        session.stop_id += 1
        session.state = DebugSessionState.STOPPED
        session.frames.clear()
        session.selected_frame_index = None
        session.raw_top_frame = None
        reason = event.body.get("reason")
        safe_reason = _single_line(reason)[:256] if isinstance(reason, str) else "unknown"
        stop: dict[str, object] = {"stop_reason": safe_reason}
        description = event.body.get("description")
        if isinstance(description, str) and description:
            stop["stop_description"] = _single_line(
                self._redact_text(description)[0]
            )[:1024]
        session.last_exception = None
        if safe_reason == "exception" and session.thread_id is not None:
            try:
                info = await session.adapter.request(
                    "exceptionInfo",
                    {"threadId": session.thread_id},
                    timeout_s=self._dap_timeout_s,
                )
            except DAPError:
                info = {}
            session.last_exception = self._normalize_exception(info, event.body)
            await self._load_stack(session, self._max_stack_frames)
            raw_top = session.raw_top_frame
            raw_line = raw_top.get("line") if raw_top is not None else None
            if (
                raw_top is not None
                and isinstance(raw_line, int)
                and not isinstance(raw_line, bool)
                and raw_line > 0
            ):
                session.last_exception.update(
                    {
                        "path": raw_top["path"],
                        "line": raw_line,
                        "function": raw_top["function"],
                    }
                )
            stop["exception"] = session.last_exception
        return stop

    def _normalize_exception(
        self, info: Mapping[str, object], stopped: Mapping[str, object]
    ) -> dict[str, object]:
        exception_id = info.get("exceptionId")
        description = info.get("description") or stopped.get("description") or stopped.get("text")
        break_mode = info.get("breakMode")
        safe_description = (
            _single_line(self._redact_text(description)[0])[:2048]
            if isinstance(description, str)
            else ""
        )
        normalized_type = (
            exception_id.strip().split(maxsplit=1)[0]
            if isinstance(exception_id, str) and exception_id.strip()
            else "unknown"
        )
        result: dict[str, object] = {
            "type": normalized_type,
            "message": safe_description or "Runtime exception",
        }
        if isinstance(break_mode, str):
            result["break_mode"] = break_mode
        details = info.get("details")
        if isinstance(details, Mapping):
            message = details.get("message")
            if isinstance(message, str):
                result["message"] = _single_line(self._redact_text(message)[0])[:2048]
            type_name = details.get("typeName")
            if isinstance(type_name, str) and type_name.strip():
                result["type"] = type_name.strip().split(maxsplit=1)[0]
        return result

    async def _running_thread_id(self, session: _DebugSession) -> int:
        body = await session.adapter.threads(timeout_s=self._dap_timeout_s)
        raw_threads = body.get("threads")
        if not isinstance(raw_threads, list):
            raise DebuggerError("THREAD_NOT_FOUND", "No debuggee thread is available.")
        ids = sorted(
            item["id"]
            for item in raw_threads
            if isinstance(item, dict)
            and isinstance(item.get("id"), int)
            and not isinstance(item.get("id"), bool)
            and cast(int, item["id"]) > 0
        )
        if not ids:
            raise DebuggerError("THREAD_NOT_FOUND", "No debuggee thread is available.")
        return cast(int, ids[0])

    def _stopped_thread_id(self, session: _DebugSession) -> int:
        if session.thread_id is None:
            raise DebuggerError("THREAD_NOT_FOUND", "The stop has no inspectable thread.")
        return session.thread_id

    async def _load_stack(self, session: _DebugSession, levels: int) -> list[dict[str, object]]:
        thread_id = self._stopped_thread_id(session)
        body = await session.adapter.stack_trace(
            thread_id, levels=levels, timeout_s=self._dap_timeout_s
        )
        raw_frames = body.get("stackFrames")
        if not isinstance(raw_frames, list):
            raw_frames = []
        session.raw_top_frame = None
        if raw_frames and isinstance(raw_frames[0], Mapping):
            session.raw_top_frame = self._normalize_frame(raw_frames[0], 0)[0]
        ranked: list[tuple[tuple[object, ...], int, int, dict[str, object]]] = []
        for stack_index, raw in enumerate(raw_frames[:levels]):
            if not isinstance(raw, Mapping):
                continue
            frame_id = raw.get("id")
            if not isinstance(frame_id, int) or isinstance(frame_id, bool) or frame_id <= 0:
                continue
            frame, rank = self._normalize_frame(raw, stack_index)
            ranked.append((rank, stack_index, frame_id, frame))
        ranked.sort(key=lambda item: item[0])
        session.frames.clear()
        session.selected_frame_index = None
        response: list[dict[str, object]] = []
        for suspicious_rank, (_, stack_index, frame_id, frame) in enumerate(ranked):
            normalized = {**frame, "suspicious_rank": suspicious_rank}
            session.frames[stack_index] = (frame_id, normalized)
            response.append(normalized)
        return response

    def _normalize_frame(
        self, raw: Mapping[str, object], stack_index: int
    ) -> tuple[dict[str, object], tuple[object, ...]]:
        source = raw.get("source")
        source_path: Path | None = None
        raw_path = source.get("path") if isinstance(source, Mapping) else None
        if isinstance(raw_path, str) and raw_path:
            source_path = Path(raw_path).expanduser().resolve(strict=False)
        in_workspace = False
        relative = "<unknown>"
        if source_path is not None:
            try:
                relative = source_path.relative_to(self.workspace_root).as_posix()
                if any(character in relative for character in ("\x00", "\r", "\n")):
                    relative = "<invalid-workspace-path>"
                else:
                    in_workspace = True
            except ValueError:
                relative = "<outside-workspace>"
        lower_parts = {part.lower() for part in source_path.parts} if source_path else set()
        internal = bool(lower_parts & _INTERNAL_PARTS)
        if source_path is not None and not in_workspace:
            try:
                internal = internal or source_path.is_relative_to(Path(sys.base_prefix))
            except (OSError, ValueError):
                pass
        line = raw.get("line")
        line_number = line if isinstance(line, int) and not isinstance(line, bool) else 0
        name = raw.get("name")
        function = _single_line(name)[:512] if isinstance(name, str) else "<unknown>"
        reasons: list[str] = []
        if in_workspace:
            reasons.append("workspace_source")
        else:
            reasons.append("outside_workspace")
        if internal:
            reasons.append("runtime_or_dependency_frame")
        else:
            reasons.append("user_frame")
        if stack_index == 0:
            reasons.append("topmost_frame")
        frame: dict[str, object] = {
            "frame_index": stack_index,
            "path": relative,
            "line": line_number,
            "function": function,
            "in_workspace": in_workspace,
            "selection_rule_version": FRAME_SELECTION_RULE_VERSION,
            "selection_reasons": reasons,
        }
        rank = (
            0 if in_workspace else 1,
            1 if internal else 0,
            stack_index,
            relative,
            line_number,
            function,
        )
        return frame, rank

    def _choose_scope(self, body: Mapping[str, object], requested: str) -> Mapping[str, object]:
        raw_scopes = body.get("scopes")
        scopes = (
            [item for item in raw_scopes if isinstance(item, Mapping)]
            if isinstance(raw_scopes, list)
            else []
        )
        aliases = {"locals": {"locals", "local"}, "arguments": {"arguments", "args"}}
        for scope in scopes:
            name = scope.get("name")
            hint = scope.get("presentationHint")
            candidates = {
                value.lower() for value in (name, hint) if isinstance(value, str) and value.strip()
            }
            if candidates & aliases[requested]:
                return scope
        raise DebuggerError(
            "SCOPE_NOT_FOUND", f"The selected frame does not expose an {requested} scope."
        )

    def _argument_names(self, selected_frame: Mapping[str, object]) -> frozenset[str]:
        """Resolve Python parameter names from workspace source without evaluation."""

        path_value = selected_frame.get("path")
        function_value = selected_frame.get("function")
        line_value = selected_frame.get("line")
        if (
            selected_frame.get("in_workspace") is not True
            or not isinstance(path_value, str)
            or not isinstance(function_value, str)
            or isinstance(line_value, bool)
            or not isinstance(line_value, int)
        ):
            raise DebuggerError(
                "ARGUMENT_METADATA_UNAVAILABLE",
                "Arguments can only be resolved for a selected workspace Python frame.",
            )
        source = self._workspace_path(path_value, kind="selected frame source", require_file=True)
        try:
            if source.stat().st_size > 2 * 1024 * 1024:
                raise DebuggerError(
                    "ARGUMENT_METADATA_UNAVAILABLE",
                    "The selected source is too large for safe argument resolution.",
                )
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        except (OSError, UnicodeError, SyntaxError) as exc:
            raise DebuggerError(
                "ARGUMENT_METADATA_UNAVAILABLE",
                "The selected source could not be parsed for argument names.",
            ) from exc
        candidates: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            end_line = node.end_lineno or node.lineno
            if node.name == function_value and node.lineno <= line_value <= end_line:
                candidates.append(node)
        if not candidates:
            raise DebuggerError(
                "ARGUMENT_METADATA_UNAVAILABLE",
                "The selected function arguments were not found in workspace source.",
            )
        function = min(
            candidates,
            key=lambda node: ((node.end_lineno or node.lineno) - node.lineno, node.lineno),
        )
        arguments = function.args
        names = {
            item.arg
            for item in (
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
            )
        }
        if arguments.vararg is not None:
            names.add(arguments.vararg.arg)
        if arguments.kwarg is not None:
            names.add(arguments.kwarg.arg)
        return frozenset(names)

    def _normalize_variables(
        self,
        body: Mapping[str, object],
        count: int,
        max_value_chars: int,
        scope: str,
    ) -> list[dict[str, object]]:
        raw_variables = body.get("variables")
        values = raw_variables if isinstance(raw_variables, list) else []
        normalized: list[dict[str, object]] = []
        for raw in values[:count]:
            if not isinstance(raw, Mapping):
                continue
            name_value = raw.get("name")
            name = (
                _single_line(name_value)[:512]
                if isinstance(name_value, str)
                else "<unnamed>"
            )
            raw_value = raw.get("value")
            value = raw_value if isinstance(raw_value, str) else repr(raw_value)
            value = value.replace(str(self.workspace_root), "$WORKSPACE")
            redacted = False
            reason: str | None = None
            if _SECRET_NAME.search(name):
                value = "<redacted:secret-name>"
                redacted = True
                reason = "secret_name"
            else:
                token_candidate = value.strip("'\"")
                if _TOKENISH_VALUE.fullmatch(token_candidate) and _entropy(token_candidate) >= 3.5:
                    value = "<redacted:high-entropy>"
                    redacted = True
                    reason = "high_entropy"
                else:
                    value, redacted, reason = self._redact_text(value)
            truncated = len(value) > max_value_chars
            if truncated:
                value = value[: max_value_chars - 1] + "…"
            item: dict[str, object] = {
                "scope": scope,
                "name": name,
                "value": value,
                "type": "<unknown>",
                "redacted": redacted,
                "truncated": truncated,
            }
            type_value = raw.get("type")
            if isinstance(type_value, str) and type_value.strip():
                item["type"] = _single_line(type_value.strip())[:256]
            if reason is not None:
                item["redaction_reason"] = reason
            normalized.append(item)
        normalized.sort(key=lambda item: (cast(str, item["name"]), cast(str, item["value"])))
        return normalized

    def _require_state(self, session: _DebugSession, allowed: set[DebugSessionState]) -> None:
        if session.state not in allowed:
            expected = ", ".join(sorted(state.value for state in allowed))
            raise DebuggerError(
                "INVALID_STATE",
                f"Action requires state [{expected}], current state is {session.state.value}.",
            )

    def _require_current_stop(self, session: _DebugSession, stop_id: int | None) -> None:
        self._require_state(session, {DebugSessionState.STOPPED})
        if isinstance(stop_id, bool) or not isinstance(stop_id, int) or stop_id != session.stop_id:
            raise DebuggerError(
                "STALE_STOP",
                "The supplied stop_id is not the session's current suspended state.",
            )

    def _invalidate_suspended_references(self, session: _DebugSession) -> None:
        session.thread_id = None
        session.selected_frame_index = None
        session.frames.clear()
        session.raw_top_frame = None

    async def _close_failed_launch(self, session_id: str) -> None:
        async with self._registry_lock:
            session = self._sessions.get(session_id)
        if session is None:
            return
        await self._force_close(session)
        async with self._registry_lock:
            if self._sessions.get(session_id) is session:
                self._sessions.pop(session_id, None)

    async def _close_sessions(self, sessions: Sequence[_DebugSession]) -> None:
        if not sessions:
            return
        results = await asyncio.gather(
            *(self._force_close(session) for session in sessions),
            return_exceptions=True,
        )
        successful = tuple(
            session
            for session, result in zip(sessions, results, strict=True)
            if not isinstance(result, BaseException)
        )
        async with self._registry_lock:
            for session in successful:
                if self._sessions.get(session.session_id) is session:
                    self._sessions.pop(session.session_id, None)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def _force_close(self, session: _DebugSession) -> None:
        async with session.lock:
            try:
                if not session.adapter.closed:
                    try:
                        await session.adapter.disconnect(
                            terminate_debuggee=True,
                            timeout_s=self._dap_timeout_s,
                        )
                    finally:
                        # Some adapter implementations make disconnect perform
                        # transport cleanup; others do not. Keep close as the
                        # idempotent process-group reap backstop in both cases.
                        await session.adapter.close()
                else:
                    await session.adapter.close()
            finally:
                session.state = DebugSessionState.EXITED
                self._invalidate_suspended_references(session)
