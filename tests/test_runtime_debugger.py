from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from react_agent.debugpy_dap import DAPEvent, DAPRequestError, DAPTimeoutError
from react_agent.runtime_debugger import DebugBreakpoint, PythonRuntimeDebugger

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class FakeDAPAdapter:
    """Programmable DAP boundary used to exercise debugger behavior."""

    def __init__(
        self,
        *,
        events: Sequence[DAPEvent] = (),
        stack_frames: Sequence[Mapping[str, object]] = (),
        scopes: Sequence[Mapping[str, object]] = (),
        variables: Sequence[Mapping[str, object]] = (),
        exception_info: Mapping[str, object] | None = None,
        block_next_event: bool = False,
    ) -> None:
        self.calls: list[str] = []
        self.breakpoint_requests: list[tuple[Path, tuple[int, ...]]] = []
        self.request_arguments: dict[str, Mapping[str, object]] = {}
        self._events: asyncio.Queue[DAPEvent] = asyncio.Queue()
        for event in events:
            self._events.put_nowait(event)
        self.stack_frames = [dict(frame) for frame in stack_frames]
        self.scope_entries = [dict(scope) for scope in scopes]
        self.variable_entries = [dict(variable) for variable in variables]
        self.exception_info = dict(exception_info) if exception_info is not None else None
        self.block_next_event = block_next_event
        self.next_event_entered = asyncio.Event()
        self.close_calls = 0
        self.disconnect_calls = 0
        self.launch_timeout_s: float | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pid(self) -> int | None:
        return 4242

    def push_event(self, event: DAPEvent) -> None:
        self._events.put_nowait(event)

    async def initialize(
        self,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del arguments, timeout_s
        self.calls.append("initialize")
        return {"supportsConfigurationDoneRequest": True}

    def launch(
        self,
        arguments: Mapping[str, object],
        *,
        timeout_s: float | None = None,
    ) -> asyncio.Task[dict[str, object]]:
        self.calls.append("launch")
        self.launch_timeout_s = timeout_s
        self.request_arguments["launch"] = dict(arguments)

        async def completed_response() -> dict[str, object]:
            await asyncio.sleep(0)
            return {}

        return asyncio.create_task(completed_response())

    async def wait_for_event(self, event: str, *, timeout_s: float | None = None) -> DAPEvent:
        del timeout_s
        self.calls.append(f"wait_for_event:{event}")
        assert event == "initialized"
        return DAPEvent(seq=1, event="initialized", body={})

    async def next_event(self, *, timeout_s: float | None = None) -> DAPEvent:
        self.calls.append("next_event")
        self.next_event_entered.set()
        if self.block_next_event:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        try:
            return await asyncio.wait_for(self._events.get(), timeout=timeout_s)
        except TimeoutError as exc:
            raise DAPTimeoutError("fake event timeout") from exc

    async def request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del timeout_s
        self.calls.append(f"request:{command}")
        self.request_arguments[command] = dict(arguments or {})
        if command == "exceptionInfo":
            return self.exception_info or {
                "exceptionId": "ZeroDivisionError",
                "description": "division by zero",
                "breakMode": "always",
            }
        return {}

    async def set_breakpoints(
        self,
        source_path: str | os.PathLike[str],
        lines: Sequence[int],
        *,
        source_modified: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del source_modified, timeout_s
        self.calls.append("set_breakpoints")
        normalized_lines = tuple(lines)
        self.breakpoint_requests.append((Path(source_path), normalized_lines))
        return {"breakpoints": [{"verified": True, "line": line} for line in normalized_lines]}

    async def configuration_done(self, *, timeout_s: float | None = None) -> dict[str, object]:
        del timeout_s
        self.calls.append("configuration_done")
        return {}

    async def threads(self, *, timeout_s: float | None = None) -> dict[str, object]:
        del timeout_s
        self.calls.append("threads")
        return {"threads": [{"id": 7, "name": "main"}]}

    async def continue_(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del single_thread, timeout_s
        self.calls.append(f"continue:{thread_id}")
        return {"allThreadsContinued": True}

    async def next(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del single_thread, granularity, timeout_s
        self.calls.append(f"next:{thread_id}")
        return {}

    async def step_in(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del single_thread, granularity, timeout_s
        self.calls.append(f"step_in:{thread_id}")
        return {}

    async def step_out(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del single_thread, granularity, timeout_s
        self.calls.append(f"step_out:{thread_id}")
        return {}

    async def pause(self, thread_id: int, *, timeout_s: float | None = None) -> dict[str, object]:
        del timeout_s
        self.calls.append(f"pause:{thread_id}")
        return {}

    async def stack_trace(
        self,
        thread_id: int,
        *,
        start_frame: int = 0,
        levels: int | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del start_frame, timeout_s
        self.calls.append(f"stack_trace:{thread_id}")
        frames = self.stack_frames if levels is None else self.stack_frames[:levels]
        return {"stackFrames": frames, "totalFrames": len(self.stack_frames)}

    async def scopes(self, frame_id: int, *, timeout_s: float | None = None) -> dict[str, object]:
        del timeout_s
        self.calls.append(f"scopes:{frame_id}")
        return {"scopes": self.scope_entries}

    async def variables(
        self,
        variables_reference: int,
        *,
        start: int = 0,
        count: int | None = None,
        filter_: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del start, filter_, timeout_s
        self.calls.append(f"variables:{variables_reference}")
        values = self.variable_entries if count is None else self.variable_entries[:count]
        return {"variables": values}

    async def disconnect(
        self,
        *,
        restart: bool = False,
        terminate_debuggee: bool = True,
        suspend_debuggee: bool = False,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del restart, suspend_debuggee, timeout_s
        assert terminate_debuggee is True
        self.calls.append("disconnect")
        self.disconnect_calls += 1
        self._closed = True
        return {}

    async def close(self) -> None:
        self.calls.append("close")
        self.close_calls += 1
        self._closed = True


class FailOnceCloseAdapter(FakeDAPAdapter):
    async def close(self) -> None:
        self.calls.append("close")
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError("transient close failure")
        self._closed = True


class InitializeFailureAdapter(FakeDAPAdapter):
    async def initialize(
        self,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, object]:
        del arguments, timeout_s
        self.calls.append("initialize")
        raise DAPTimeoutError("initialization failed")


class PauseRaceAdapter(FakeDAPAdapter):
    async def pause(self, thread_id: int, *, timeout_s: float | None = None) -> dict[str, object]:
        del timeout_s
        self.calls.append(f"pause:{thread_id}")
        self.push_event(stopped_event(seq=9, reason="pause"))
        raise DAPRequestError("pause", "thread is already stopped")


class FakeAdapterFactory:
    def __init__(self, adapter: FakeDAPAdapter) -> None:
        self.adapter = adapter
        self.calls: list[dict[str, object]] = []

    async def __call__(self, **kwargs: object) -> FakeDAPAdapter:
        self.calls.append(dict(kwargs))
        return self.adapter


def stopped_event(*, seq: int = 2, reason: str = "breakpoint") -> DAPEvent:
    return DAPEvent(seq=seq, event="stopped", body={"threadId": 7, "reason": reason})


def make_workspace(tmp_path: Path) -> tuple[Path, Path]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    program = workspace / "demo.py"
    program.write_text("value = 1\nprint(value)\n", encoding="utf-8")
    return workspace, program


async def marked_processes(marker: str) -> tuple[int, ...]:
    process = await asyncio.create_subprocess_exec(
        "ps",
        "-axo",
        "pid=,command=",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    if process.returncode != 0:
        pytest.skip("process-list inspection is unavailable")
    matches: list[int] = []
    for raw_line in stdout.decode(errors="replace").splitlines():
        pid_text, _, command = raw_line.strip().partition(" ")
        if marker not in command:
            continue
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid != os.getpid():
            matches.append(pid)
    return tuple(matches)


async def wait_for_marked_processes(
    marker: str,
    *,
    present: bool,
    timeout_s: float = 10.0,
) -> tuple[int, ...]:
    deadline = time.monotonic() + timeout_s
    while True:
        matches = await marked_processes(marker)
        if bool(matches) is present:
            return matches
        if time.monotonic() >= deadline:
            pytest.fail(
                f"marked debuggee processes did not become present={present}: {matches!r}"
            )
        await asyncio.sleep(0.05)


async def launch_stopped(
    debugger: PythonRuntimeDebugger,
    *,
    program: str = "demo.py",
    breakpoints: list[DebugBreakpoint] | None = None,
    exception_policy: str = "none",
) -> dict[str, object]:
    return await debugger.launch(
        program=program,
        args=[],
        breakpoints=breakpoints or [],
        exception_policy=exception_policy,  # type: ignore[arg-type]
        stop_on_entry=False,
        wait_timeout_s=1,
    )


@pytest.mark.asyncio
async def test_launch_uses_canonical_dap_handshake_before_observing_stop(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(events=[stopped_event()])
    factory = FakeAdapterFactory(adapter)
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=factory)

    result = await launch_stopped(
        debugger,
        breakpoints=[DebugBreakpoint(file="demo.py", lines=[2, 1, 2])],
        exception_policy="raised",
    )

    assert result["success"] is True
    assert result["state"] == "stopped"
    assert result["stop_id"] == 1
    assert result["transitions"] == [
        "created",
        "initializing",
        "configuring",
        "running",
        "stopped",
    ]
    assert adapter.calls == [
        "initialize",
        "launch",
        "wait_for_event:initialized",
        "set_breakpoints",
        "request:setExceptionBreakpoints",
        "configuration_done",
        "next_event",
    ]
    assert adapter.breakpoint_requests == [(program.resolve(), (1, 2))]
    assert adapter.request_arguments["setExceptionBreakpoints"] == {"filters": ["raised"]}
    assert factory.calls == [
        {
            "python_executable": sys.executable,
            "cwd": workspace.resolve(),
            "request_timeout_s": 10.0,
        }
    ]

    await debugger.close()


@pytest.mark.asyncio
async def test_deferred_launch_timeout_covers_all_breakpoint_configuration_requests(
    tmp_path: Path,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    breakpoints: list[DebugBreakpoint] = []
    for index in range(64):
        source = workspace / f"breakpoint_{index:02d}.py"
        source.write_text("pass\n", encoding="utf-8")
        breakpoints.append(DebugBreakpoint(file=source.name, lines=[]))
    adapter = FakeDAPAdapter(events=[stopped_event()])
    debugger = PythonRuntimeDebugger(
        workspace,
        adapter_factory=FakeAdapterFactory(adapter),
        dap_timeout_s=10,
    )

    result = await launch_stopped(debugger, breakpoints=breakpoints)

    assert result["success"] is True
    assert adapter.launch_timeout_s == 670
    await debugger.close()


@pytest.mark.asyncio
async def test_state_and_stop_generation_reject_invalid_or_stale_control(
    tmp_path: Path,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(events=[stopped_event()])
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])

    invalid_state = await debugger.control(
        session_id, action="pause", stop_id=None, wait_timeout_s=1
    )
    assert invalid_state["success"] is False
    assert invalid_state["error"] == {
        "code": "INVALID_STATE",
        "message": "Action requires state [running], current state is stopped.",
        "retryable": False,
    }

    stale_control = await debugger.control(
        session_id, action="continue", stop_id=0, wait_timeout_s=1
    )
    assert stale_control["success"] is False
    assert stale_control["error"] == {
        "code": "STALE_STOP",
        "message": "The supplied stop_id is not the session's current suspended state.",
        "retryable": False,
    }
    assert not any(call.startswith("continue:") for call in adapter.calls)

    invalid_action = await debugger.control(
        session_id,
        action="bogus",  # type: ignore[arg-type]
        stop_id=1,
        wait_timeout_s=1,
    )
    assert invalid_action["success"] is False
    assert invalid_action["error"] == {
        "code": "INVALID_ARGUMENT",
        "message": "Unsupported debugger control action.",
        "retryable": False,
    }

    adapter.push_event(stopped_event(seq=3, reason="step"))
    resumed = await debugger.control(session_id, action="continue", stop_id=1, wait_timeout_s=1)
    assert resumed["success"] is True
    assert resumed["state"] == "stopped"
    assert resumed["stop_id"] == 2

    stale_stack = await debugger.stack(session_id, stop_id=1, levels=8)
    assert stale_stack["success"] is False
    assert stale_stack["error"] == {
        "code": "STALE_STOP",
        "message": "The supplied stop_id is not the session's current suspended state.",
        "retryable": False,
    }
    await debugger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "adapter_call"),
    (
        ("next", "next:7"),
        ("step_in", "step_in:7"),
        ("step_out", "step_out:7"),
    ),
)
async def test_each_step_control_action_reaches_the_dap_adapter_and_advances_stop(
    tmp_path: Path,
    action: str,
    adapter_call: str,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(events=[stopped_event()])
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])
    adapter.push_event(stopped_event(seq=3, reason="step"))

    result = await debugger.control(
        session_id,
        action=action,  # type: ignore[arg-type]
        stop_id=1,
        wait_timeout_s=1,
    )

    assert result["success"] is True
    assert result["control"] == action
    assert result["state"] == "stopped"
    assert result["stop_id"] == 2
    assert adapter_call in adapter.calls
    await debugger.close()


@pytest.mark.asyncio
async def test_stop_drains_queued_exit_after_a_control_wait_timeout(tmp_path: Path) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(events=[stopped_event()])
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])

    resumed = await debugger.control(
        session_id,
        action="continue",
        stop_id=1,
        wait_timeout_s=0.01,
    )
    assert resumed["state"] == "running"
    assert resumed["wait_timed_out"] is True
    adapter.push_event(DAPEvent(seq=4, event="exited", body={"exitCode": 0}))

    stopped = await debugger.stop(session_id)

    assert stopped["success"] is True
    assert stopped["previous_state"] == "exited"
    assert stopped["debuggee_exit"] == {
        "status": "exited",
        "exit_code": 0,
        "signal": None,
    }
    await debugger.close()


@pytest.mark.asyncio
async def test_pause_rejection_drains_a_racing_stopped_event_without_closing_session(
    tmp_path: Path,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = PauseRaceAdapter()
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await debugger.launch(
        program="demo.py",
        args=[],
        breakpoints=[],
        exception_policy="none",
        stop_on_entry=False,
        wait_timeout_s=0.01,
    )
    session_id = str(launched["debug_session_id"])
    assert launched["state"] == "running"

    result = await debugger.control(
        session_id,
        action="pause",
        stop_id=None,
        wait_timeout_s=1,
    )

    assert result["success"] is True
    assert result["state"] == "stopped"
    assert result["stop_id"] == 1
    assert result["already_settled"] is True
    assert result["stop_reason"] == "pause"
    assert adapter.close_calls == 0
    await debugger.close()


@pytest.mark.asyncio
async def test_stack_ranks_workspace_user_frames_deterministically(tmp_path: Path) -> None:
    workspace, _ = make_workspace(tmp_path)
    second_source = workspace / "second.py"
    second_source.write_text("pass\n", encoding="utf-8")
    outside_user_source = tmp_path / "outside_user.py"
    outside_user_source.write_text("pass\n", encoding="utf-8")
    dependency_source = Path(sys.base_prefix) / "lib" / "site-packages" / "vendor.py"
    adapter = FakeDAPAdapter(
        events=[stopped_event()],
        stack_frames=[
            {
                "id": 11,
                "name": "dependency_wrapper",
                "line": 80,
                "source": {"path": str(dependency_source)},
            },
            {
                "id": 22,
                "name": "workspace_top",
                "line": 2,
                "source": {"path": str(workspace / "demo.py")},
            },
            {
                "id": 33,
                "name": "workspace_caller",
                "line": 1,
                "source": {"path": str(second_source)},
            },
            {
                "id": 44,
                "name": "outside_user",
                "line": 7,
                "source": {"path": str(outside_user_source)},
            },
        ],
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])

    result = await debugger.stack(session_id, stop_id=1, levels=8)

    frames = result["frames"]
    assert isinstance(frames, list)
    assert [frame["frame_index"] for frame in frames] == [1, 2, 3, 0]
    assert [frame["suspicious_rank"] for frame in frames] == [0, 1, 2, 3]
    assert [frame["path"] for frame in frames] == [
        "demo.py",
        "second.py",
        "<outside-workspace>",
        "<outside-workspace>",
    ]
    assert result["suggested_frame_index"] == 1
    selected = await debugger.select_frame(session_id, stop_id=1, frame_index=1)
    assert selected["selected_frame"] == frames[0]
    assert adapter.calls[-1] == "stack_trace:7"

    await debugger.close()


@pytest.mark.asyncio
async def test_exception_location_uses_raw_top_frame_while_stack_suggests_workspace(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    dependency_source = Path(sys.base_prefix) / "lib" / "site-packages" / "vendor.py"
    adapter = FakeDAPAdapter(
        events=[stopped_event(reason="exception")],
        stack_frames=[
            {
                "id": 11,
                "name": "dependency_failure",
                "line": 80,
                "source": {"path": str(dependency_source)},
            },
            {
                "id": 22,
                "name": "workspace_caller",
                "line": 2,
                "source": {"path": str(program)},
            },
        ],
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))

    launched = await launch_stopped(debugger, exception_policy="raised")

    exception = launched["exception"]
    assert isinstance(exception, dict)
    assert exception["path"] == "<outside-workspace>"
    assert exception["line"] == 80
    assert exception["function"] == "dependency_failure"
    session_id = str(launched["debug_session_id"])

    stack = await debugger.stack(session_id, stop_id=1, levels=8)
    assert stack["suggested_frame_index"] == 1
    frames = stack["frames"]
    assert isinstance(frames, list)
    assert [frame["frame_index"] for frame in frames] == [1, 0]
    await debugger.close()


@pytest.mark.asyncio
async def test_multiline_runtime_text_is_normalized_for_sealed_evidence(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(
        events=[stopped_event(reason="exception")],
        stack_frames=[
            {
                "id": 11,
                "name": "failing\nfunction",
                "line": 2,
                "source": {"path": str(program)},
            }
        ],
        exception_info={
            "exceptionId": "RuntimeError",
            "description": "first line\r\nsecond line",
            "breakMode": "always",
        },
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))

    launched = await debugger.launch(
        program="demo.py",
        args=["first argument\nsecond line", str(workspace / "artifact.json")],
        breakpoints=[],
        exception_policy="raised",
        stop_on_entry=False,
        wait_timeout_s=1,
    )

    assert launched["success"] is True
    exception = launched["exception"]
    assert isinstance(exception, dict)
    assert exception["message"] == "first line\\r\\nsecond line"
    assert exception["function"] == "failing\\nfunction"
    reproduction = launched["reproduction"]
    assert isinstance(reproduction, dict)
    assert reproduction["command"][-2:] == [
        "first argument\\nsecond line",
        "$WORKSPACE/artifact.json",
    ]
    await debugger.close()


@pytest.mark.asyncio
async def test_variables_are_bounded_sorted_and_redact_secret_or_high_entropy_values(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(
        events=[stopped_event()],
        stack_frames=[
            {
                "id": 91,
                "name": "calculate",
                "line": 2,
                "source": {"path": str(program)},
            }
        ],
        scopes=[
            {"name": "Locals", "presentationHint": "locals", "variablesReference": 77},
            {
                "name": "Arguments",
                "presentationHint": "arguments",
                "variablesReference": 88,
            },
        ],
        variables=[
            {"name": "plain", "value": "visible", "type": "str"},
            {"name": "api_token", "value": "short-secret", "type": "str"},
            {
                "name": "opaque_blob",
                "value": "'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcd'",
                "type": "str",
            },
            {
                "name": "workspace_path",
                "value": f"'{workspace}/artifact_with_a_descriptive_name.json'",
                "type": "str",
            },
        ],
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])
    await debugger.stack(session_id, stop_id=1, levels=8)
    await debugger.select_frame(session_id, stop_id=1, frame_index=0)

    result = await debugger.variables(
        session_id,
        stop_id=1,
        scope="locals",
        max_variables=4,
        max_value_chars=128,
    )

    assert result["success"] is True
    assert result["returned_variables"] == 4
    assert result["variables"] == [
        {
            "scope": "locals",
            "name": "api_token",
            "value": "<redacted:secret-name>",
            "type": "str",
            "redacted": True,
            "truncated": False,
            "redaction_reason": "secret_name",
        },
        {
            "scope": "locals",
            "name": "opaque_blob",
            "value": "<redacted:high-entropy>",
            "type": "str",
            "redacted": True,
            "truncated": False,
            "redaction_reason": "high_entropy",
        },
        {
            "scope": "locals",
            "name": "plain",
            "value": "visible",
            "type": "str",
            "redacted": False,
            "truncated": False,
        },
        {
            "scope": "locals",
            "name": "workspace_path",
            "value": "'$WORKSPACE/artifact_with_a_descriptive_name.json'",
            "type": "str",
            "redacted": False,
            "truncated": False,
        },
    ]
    assert adapter.calls[-2:] == ["scopes:91", "variables:77"]

    await debugger.close()


@pytest.mark.asyncio
async def test_arguments_scope_uses_the_dap_arguments_reference(tmp_path: Path) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(
        events=[stopped_event()],
        stack_frames=[
            {
                "id": 91,
                "name": "calculate",
                "line": 2,
                "source": {"path": str(program)},
            }
        ],
        scopes=[
            {"name": "Locals", "presentationHint": "locals", "variablesReference": 77},
            {
                "name": "Arguments",
                "presentationHint": "arguments",
                "variablesReference": 88,
            },
        ],
        variables=[
            {"name": "subtotal", "value": "99.0", "type": "float"},
            {"name": "item_count", "value": "0", "type": "int"},
        ],
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])
    await debugger.stack(session_id, stop_id=1, levels=8)
    await debugger.select_frame(session_id, stop_id=1, frame_index=0)

    result = await debugger.variables(
        session_id,
        stop_id=1,
        scope="arguments",
        max_variables=8,
        max_value_chars=128,
    )

    assert result["success"] is True
    assert result["scope"] == "arguments"
    assert result["variables"] == [
        {
            "scope": "arguments",
            "name": "item_count",
            "value": "0",
            "type": "int",
            "redacted": False,
            "truncated": False,
        },
        {
            "scope": "arguments",
            "name": "subtotal",
            "value": "99.0",
            "type": "float",
            "redacted": False,
            "truncated": False,
        },
    ]
    assert adapter.calls[-2:] == ["scopes:91", "variables:88"]
    await debugger.close()


@pytest.mark.asyncio
async def test_variable_values_are_truncated_to_the_requested_character_limit(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(
        events=[stopped_event()],
        stack_frames=[
            {
                "id": 91,
                "name": "calculate",
                "line": 2,
                "source": {"path": str(program)},
            }
        ],
        scopes=[{"name": "Locals", "presentationHint": "locals", "variablesReference": 77}],
        variables=[
            {
                "name": "description",
                "value": "ordinary repository value that exceeds the requested limit",
                "type": "str",
            }
        ],
    )
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])
    await debugger.stack(session_id, stop_id=1, levels=8)
    await debugger.select_frame(session_id, stop_id=1, frame_index=0)

    result = await debugger.variables(
        session_id,
        stop_id=1,
        scope="locals",
        max_variables=1,
        max_value_chars=16,
    )

    assert result["success"] is True
    [variable] = result["variables"]  # type: ignore[misc]
    assert variable["truncated"] is True
    assert variable["redacted"] is False
    assert len(variable["value"]) == 16
    assert variable["value"].endswith("…")
    await debugger.close()


@pytest.mark.asyncio
async def test_variables_fail_closed_when_the_normalized_observation_exceeds_byte_budget(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(
        events=[stopped_event()],
        stack_frames=[
            {
                "id": 91,
                "name": "calculate",
                "line": 2,
                "source": {"path": str(program)},
            }
        ],
        scopes=[{"name": "Locals", "presentationHint": "locals", "variablesReference": 77}],
        variables=[
            {
                "name": f"value_{index:02d}",
                "value": f"ordinary repository value {index:02d} " + ("x" * 96),
                "type": "str",
            }
            for index in range(64)
        ],
    )
    debugger = PythonRuntimeDebugger(
        workspace,
        adapter_factory=FakeAdapterFactory(adapter),
        max_observation_bytes=4096,
    )
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])
    await debugger.stack(session_id, stop_id=1, levels=8)
    await debugger.select_frame(session_id, stop_id=1, frame_index=0)

    result = await debugger.variables(
        session_id,
        stop_id=1,
        scope="locals",
        max_variables=64,
        max_value_chars=128,
    )

    assert result["success"] is False
    assert result["error"] == {
        "code": "OBSERVATION_TOO_LARGE",
        "message": "The normalized debugger observation exceeded its hard byte budget.",
        "retryable": False,
    }
    await debugger.close()


@pytest.mark.asyncio
async def test_launch_rejects_parent_and_symlink_workspace_escape_before_adapter_start(
    tmp_path: Path,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    outside = tmp_path / "outside.py"
    outside.write_text("print('outside')\n", encoding="utf-8")
    symlink = workspace / "escape.py"
    try:
        symlink.symlink_to(outside)
    except (NotImplementedError, OSError):
        pytest.skip("symbolic links are unavailable")
    adapter = FakeDAPAdapter()
    factory = FakeAdapterFactory(adapter)
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=factory)

    parent_escape = await launch_stopped(debugger, program="../outside.py")
    symlink_escape = await launch_stopped(debugger, program="escape.py")

    for result in (parent_escape, symlink_escape):
        assert result["success"] is False
        assert result["error"] == {
            "code": "PATH_OUTSIDE_WORKSPACE",
            "message": "The program must resolve to an existing path inside the workspace.",
            "retryable": False,
        }
    assert factory.calls == []
    await debugger.close()


@pytest.mark.asyncio
async def test_failed_launch_is_removed_from_registry_after_successful_cleanup(
    tmp_path: Path,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = InitializeFailureAdapter()
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))

    result = await launch_stopped(debugger)

    assert result["success"] is False
    assert result["error"]["code"] == "DAP_TIMEOUT"  # type: ignore[index]
    assert debugger._sessions == {}
    assert adapter.close_calls == 1
    await debugger.close()
    assert adapter.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_scope", ["owner", "all"])
async def test_cleanup_failure_remains_registered_and_is_retried(
    tmp_path: Path,
    cleanup_scope: str,
) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = FailOnceCloseAdapter(events=[stopped_event()])
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    launched = await launch_stopped(debugger)
    session_id = str(launched["debug_session_id"])

    async def cleanup() -> None:
        if cleanup_scope == "owner":
            await debugger.close_owner("standalone")
        else:
            await debugger.close()

    with pytest.raises(RuntimeError, match="transient close failure"):
        await cleanup()
    assert debugger._sessions.get(session_id) is not None
    assert adapter.close_calls == 1

    await cleanup()
    assert debugger._sessions == {}
    assert adapter.close_calls == 2
    if cleanup_scope == "owner":
        await debugger.close()


@pytest.mark.asyncio
async def test_cancelling_launch_closes_its_owned_adapter(tmp_path: Path) -> None:
    workspace, _ = make_workspace(tmp_path)
    adapter = FakeDAPAdapter(block_next_event=True)
    debugger = PythonRuntimeDebugger(workspace, adapter_factory=FakeAdapterFactory(adapter))
    task = asyncio.create_task(launch_stopped(debugger))
    await asyncio.wait_for(adapter.next_event_entered.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert adapter.closed is True
    assert adapter.close_calls == 1
    assert debugger._sessions == {}
    await debugger.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    importlib.util.find_spec("debugpy") is None,
    reason="install the debug extra to run the lightweight real-DAP integration",
)
async def test_lightweight_real_debugpy_golden_exception_and_locals() -> None:
    """Run one local script to prove the real debugpy seam, without heavy workloads."""

    debugger = PythonRuntimeDebugger(
        _REPOSITORY_ROOT,
        dap_timeout_s=10,
        default_wait_timeout_s=10,
    )
    session_id: str | None = None
    try:
        launched = await debugger.launch(
            program="examples/runtime_debug_demo/buggy_pricing.py",
            args=[],
            breakpoints=[],
            exception_policy="uncaught",
            stop_on_entry=False,
            wait_timeout_s=10,
        )
        assert launched["success"] is True
        assert launched["state"] == "stopped"
        assert launched["stop_id"] == 1
        exception = launched["exception"]
        assert isinstance(exception, dict)
        assert exception["type"] == "ZeroDivisionError"
        assert exception["path"] == "examples/runtime_debug_demo/buggy_pricing.py"
        session_id = str(launched["debug_session_id"])

        stack = await debugger.stack(session_id, stop_id=1, levels=16)
        assert stack["success"] is True
        frames = stack["frames"]
        assert isinstance(frames, list)
        unit_price_frame = next(
            frame
            for frame in frames
            if frame["function"] == "unit_price"
            and frame["path"] == "examples/runtime_debug_demo/buggy_pricing.py"
        )
        selected = await debugger.select_frame(
            session_id,
            stop_id=1,
            frame_index=int(unit_price_frame["frame_index"]),
        )
        assert selected["success"] is True

        variables = await debugger.variables(
            session_id,
            stop_id=1,
            scope="locals",
            max_variables=32,
            max_value_chars=256,
        )
        assert variables["success"] is True
        values = {variable["name"]: variable["value"] for variable in variables["variables"]}
        assert values["subtotal"] == "99.0"
        assert values["item_count"] == "0"

        stopped = await debugger.stop(session_id)
        assert stopped["success"] is True
        assert stopped["state"] == "exited"
        assert stopped["process_reaped"] is True
    finally:
        await debugger.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.name == "nt" or importlib.util.find_spec("debugpy") is None,
    reason="requires ps and the debug extra for real-process cleanup verification",
)
async def test_real_debugpy_close_reaps_a_stopped_long_running_debuggee(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    program.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    marker = f"debug-close-{os.getpid()}-{time.monotonic_ns()}"
    debugger = PythonRuntimeDebugger(
        workspace,
        dap_timeout_s=10,
        default_wait_timeout_s=10,
    )
    try:
        launched = await debugger.launch(
            program="demo.py",
            args=[marker],
            breakpoints=[],
            exception_policy="none",
            stop_on_entry=True,
            wait_timeout_s=10,
        )
        assert launched["success"] is True
        assert launched["state"] == "stopped"
        assert await wait_for_marked_processes(marker, present=True)

        await debugger.close()

        assert await wait_for_marked_processes(marker, present=False) == ()
    finally:
        await debugger.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.name == "nt" or importlib.util.find_spec("debugpy") is None,
    reason="requires ps and the debug extra for real-process cleanup verification",
)
async def test_cancelling_real_debugpy_launch_reaps_the_running_debuggee(
    tmp_path: Path,
) -> None:
    workspace, program = make_workspace(tmp_path)
    program.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    marker = f"debug-cancel-{os.getpid()}-{time.monotonic_ns()}"
    debugger = PythonRuntimeDebugger(
        workspace,
        dap_timeout_s=10,
        default_wait_timeout_s=30,
    )
    launch_task = asyncio.create_task(
        debugger.launch(
            program="demo.py",
            args=[marker],
            breakpoints=[],
            exception_policy="none",
            stop_on_entry=False,
            wait_timeout_s=30,
        )
    )
    try:
        assert await wait_for_marked_processes(marker, present=True)

        launch_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await launch_task

        assert await wait_for_marked_processes(marker, present=False) == ()
    finally:
        if not launch_task.done():
            launch_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await launch_task
        await debugger.close()
