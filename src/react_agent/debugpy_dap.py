"""Small, defensive async DAP client for the stdio debugpy adapter.

The client deliberately owns only the Debug Adapter Protocol transport.  Path
policy, debugger state, and observation redaction belong in the higher-level
runtime debugger.  Keeping that boundary here also makes the transport usable
without importing debugpy -- debugpy is needed only when :meth:`start` spawns
the adapter.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import subprocess
import sys
from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import NoReturn, Protocol, Self, cast

JsonObject = dict[str, object]

_HEADER_TERMINATOR = b"\r\n\r\n"
_DEFAULT_REQUEST_TIMEOUT_S = 10.0
_DEFAULT_SHUTDOWN_TIMEOUT_S = 2.0
_DEFAULT_MAX_MESSAGE_BYTES = 8 * 1024 * 1024
_DEFAULT_MAX_HEADER_BYTES = 16 * 1024
_DEFAULT_MAX_EVENTS = 1_024
_DEFAULT_MAX_EVENT_BYTES = 16 * 1024 * 1024
_DEFAULT_MAX_STDERR_BYTES = 64 * 1024
_MAX_EXPIRED_REQUESTS = 4_096


class DAPError(Exception):
    """Base class for debug-adapter failures."""


class DAPProtocolError(DAPError):
    """The adapter sent an invalid or unsafe DAP message."""


class DAPClosedError(DAPError):
    """The adapter transport closed before an operation completed."""


class DAPTimeoutError(DAPError, TimeoutError):
    """A DAP operation exceeded its configured deadline."""


class DAPRequestError(DAPError):
    """The adapter returned an unsuccessful response to a request."""

    def __init__(
        self,
        command: str,
        message: str,
        *,
        body: Mapping[str, object] | None = None,
    ) -> None:
        detail = message or "request rejected without a diagnostic"
        super().__init__(f"DAP {command!r} request failed: {detail}")
        self.command = command
        self.message = message
        self.body = dict(body or {})


@dataclass(frozen=True, slots=True)
class DAPEvent:
    """One asynchronous event emitted by the debug adapter."""

    seq: int
    event: str
    body: JsonObject


@dataclass(slots=True)
class _PendingRequest:
    command: str
    future: asyncio.Future[JsonObject]


@dataclass(frozen=True, slots=True)
class _QueuedEvent:
    event: DAPEvent
    encoded_bytes: int


class _DAPWriter(Protocol):
    def write(self, data: bytes) -> None: ...

    async def drain(self) -> None: ...

    def close(self) -> None: ...

    async def wait_closed(self) -> None: ...


def _positive_finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _non_negative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number {value!r} is not permitted")


def _encode_dap_message(message: Mapping[str, object], *, max_message_bytes: int) -> bytes:
    """Encode one DAP message with a byte-accurate Content-Length header."""

    try:
        body = json.dumps(
            dict(message),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DAPProtocolError(f"cannot encode outbound DAP message: {exc}") from exc
    if not body:
        raise DAPProtocolError("outbound DAP message body is empty")
    if len(body) > max_message_bytes:
        raise DAPProtocolError(
            f"outbound DAP message is {len(body)} bytes; limit is {max_message_bytes}"
        )
    return f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body


async def _read_dap_message(
    reader: asyncio.StreamReader,
    *,
    max_header_bytes: int,
    max_message_bytes: int,
) -> JsonObject:
    """Read and validate one Content-Length framed DAP message."""

    try:
        header_bytes = await reader.readuntil(_HEADER_TERMINATOR)
    except asyncio.LimitOverrunError as exc:
        raise DAPProtocolError("DAP header exceeded the stream reader limit") from exc
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise DAPClosedError("debug adapter stdout reached EOF") from exc
        raise DAPProtocolError("debug adapter closed during a DAP header") from exc

    if len(header_bytes) > max_header_bytes:
        raise DAPProtocolError(
            f"DAP header is {len(header_bytes)} bytes; limit is {max_header_bytes}"
        )
    try:
        header_text = header_bytes[: -len(_HEADER_TERMINATOR)].decode("ascii")
    except UnicodeDecodeError as exc:
        raise DAPProtocolError("DAP headers must be ASCII") from exc

    content_length: int | None = None
    for line in header_text.split("\r\n"):
        if not line or ":" not in line:
            raise DAPProtocolError("malformed DAP header line")
        raw_name, raw_value = line.split(":", 1)
        name = raw_name.strip().lower()
        value = raw_value.strip()
        if not name:
            raise DAPProtocolError("DAP header name is empty")
        if name != "content-length":
            continue
        if content_length is not None:
            raise DAPProtocolError("DAP message contains duplicate Content-Length headers")
        if not value.isascii() or not value.isdecimal():
            raise DAPProtocolError("DAP Content-Length must be an unsigned decimal integer")
        try:
            content_length = int(value)
        except ValueError as exc:
            raise DAPProtocolError("DAP Content-Length is too large to parse") from exc

    if content_length is None:
        raise DAPProtocolError("DAP message is missing Content-Length")
    if content_length <= 0:
        raise DAPProtocolError("DAP Content-Length must be greater than zero")
    if content_length > max_message_bytes:
        raise DAPProtocolError(
            f"DAP message is {content_length} bytes; limit is {max_message_bytes}"
        )

    try:
        body = await reader.readexactly(content_length)
    except asyncio.IncompleteReadError as exc:
        raise DAPProtocolError("debug adapter closed during a DAP message body") from exc
    try:
        decoded = json.loads(body.decode("utf-8"), parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DAPProtocolError(f"invalid DAP JSON body: {exc}") from exc
    if not isinstance(decoded, dict):
        raise DAPProtocolError("DAP message body must be a JSON object")
    if not all(isinstance(key, str) for key in decoded):
        raise DAPProtocolError("DAP message keys must be strings")
    return cast(JsonObject, decoded)


class DebugpyDAPClient:
    """Concurrent async DAP client backed by ``python -m debugpy.adapter``.

    ``launch()`` intentionally returns a scheduled task instead of awaiting the
    response.  debugpy may defer that response until after the client receives
    ``initialized``, installs breakpoints, and sends ``configurationDone``.
    Higher layers can therefore use the canonical DAP launch sequence without a
    request/response deadlock.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: _DAPWriter,
        *,
        process: asyncio.subprocess.Process | None,
        request_timeout_s: float,
        shutdown_timeout_s: float,
        max_header_bytes: int,
        max_message_bytes: int,
        max_events: int,
        max_event_bytes: int,
        max_stderr_bytes: int,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._process = process
        self._request_timeout_s = _positive_finite(request_timeout_s, "request_timeout_s")
        self._shutdown_timeout_s = _positive_finite(shutdown_timeout_s, "shutdown_timeout_s")
        self._max_header_bytes = _positive_int(max_header_bytes, "max_header_bytes")
        self._max_message_bytes = _positive_int(max_message_bytes, "max_message_bytes")
        self._max_events = _positive_int(max_events, "max_events")
        self._max_event_bytes = _positive_int(max_event_bytes, "max_event_bytes")
        self._max_stderr_bytes = _positive_int(max_stderr_bytes, "max_stderr_bytes")

        self._next_sequence = 1
        self._pending: dict[int, _PendingRequest] = {}
        self._expired_requests: set[int] = set()
        self._expired_order: deque[int] = deque()
        self._events: deque[_QueuedEvent] = deque()
        self._event_bytes = 0
        self._event_condition = asyncio.Condition()
        self._write_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._termination_lock = asyncio.Lock()

        self._stderr_buffer = bytearray()
        self._stderr_dropped_bytes = 0
        self._closing = False
        self._closed = False
        self._cleanup_complete = False
        self._failure: DAPError | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._process_task: asyncio.Task[None] | None = None

    @classmethod
    async def start(
        cls,
        *,
        python_executable: str | os.PathLike[str] = sys.executable,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        request_timeout_s: float = _DEFAULT_REQUEST_TIMEOUT_S,
        shutdown_timeout_s: float = _DEFAULT_SHUTDOWN_TIMEOUT_S,
        max_header_bytes: int = _DEFAULT_MAX_HEADER_BYTES,
        max_message_bytes: int = _DEFAULT_MAX_MESSAGE_BYTES,
        max_events: int = _DEFAULT_MAX_EVENTS,
        max_event_bytes: int = _DEFAULT_MAX_EVENT_BYTES,
        max_stderr_bytes: int = _DEFAULT_MAX_STDERR_BYTES,
    ) -> Self:
        """Start debugpy's stdio adapter in an isolated process group."""

        executable = os.fspath(python_executable)
        adapter_cwd = os.fspath(cwd) if cwd is not None else None
        adapter_env = dict(env) if env is not None else None
        request_timeout_s = _positive_finite(request_timeout_s, "request_timeout_s")
        shutdown_timeout_s = _positive_finite(shutdown_timeout_s, "shutdown_timeout_s")
        max_header_bytes = _positive_int(max_header_bytes, "max_header_bytes")
        max_message_bytes = _positive_int(max_message_bytes, "max_message_bytes")
        max_events = _positive_int(max_events, "max_events")
        max_event_bytes = _positive_int(max_event_bytes, "max_event_bytes")
        max_stderr_bytes = _positive_int(max_stderr_bytes, "max_stderr_bytes")
        stream_limit = max(64 * 1024, max_header_bytes + 1)

        if os.name == "nt":
            creation_flags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
            process = await asyncio.create_subprocess_exec(
                executable,
                "-m",
                "debugpy.adapter",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=adapter_cwd,
                env=adapter_env,
                limit=stream_limit,
                creationflags=creation_flags,
            )
        else:
            process = await asyncio.create_subprocess_exec(
                executable,
                "-m",
                "debugpy.adapter",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=adapter_cwd,
                env=adapter_env,
                limit=stream_limit,
                start_new_session=True,
            )

        if process.stdin is None or process.stdout is None or process.stderr is None:
            with suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise DAPClosedError("debug adapter subprocess did not expose stdio pipes")

        client = cls(
            process.stdout,
            process.stdin,
            process=process,
            request_timeout_s=request_timeout_s,
            shutdown_timeout_s=shutdown_timeout_s,
            max_header_bytes=max_header_bytes,
            max_message_bytes=max_message_bytes,
            max_events=max_events,
            max_event_bytes=max_event_bytes,
            max_stderr_bytes=max_stderr_bytes,
        )
        client._start_background_tasks(stderr=process.stderr)
        return client

    @classmethod
    def _from_streams(
        cls,
        reader: asyncio.StreamReader,
        writer: _DAPWriter,
        *,
        request_timeout_s: float = _DEFAULT_REQUEST_TIMEOUT_S,
        shutdown_timeout_s: float = _DEFAULT_SHUTDOWN_TIMEOUT_S,
        max_header_bytes: int = _DEFAULT_MAX_HEADER_BYTES,
        max_message_bytes: int = _DEFAULT_MAX_MESSAGE_BYTES,
        max_events: int = _DEFAULT_MAX_EVENTS,
        max_event_bytes: int = _DEFAULT_MAX_EVENT_BYTES,
        max_stderr_bytes: int = _DEFAULT_MAX_STDERR_BYTES,
    ) -> Self:
        """Construct a transport-only client for deterministic offline tests."""

        client = cls(
            reader,
            writer,
            process=None,
            request_timeout_s=request_timeout_s,
            shutdown_timeout_s=shutdown_timeout_s,
            max_header_bytes=max_header_bytes,
            max_message_bytes=max_message_bytes,
            max_events=max_events,
            max_event_bytes=max_event_bytes,
            max_stderr_bytes=max_stderr_bytes,
        )
        client._start_background_tasks(stderr=None)
        return client

    def _start_background_tasks(self, *, stderr: asyncio.StreamReader | None) -> None:
        self._reader_task = asyncio.create_task(self._read_loop(), name="debugpy-dap-reader")
        if stderr is not None:
            self._stderr_task = asyncio.create_task(
                self._capture_stderr(stderr), name="debugpy-dap-stderr"
            )
        if self._process is not None:
            self._process_task = asyncio.create_task(
                self._watch_process(), name="debugpy-dap-process"
            )

    @property
    def closed(self) -> bool:
        """Whether the transport can no longer accept requests."""

        return self._closed or self._closing

    @property
    def returncode(self) -> int | None:
        """Current adapter return code, or ``None`` while it is running."""

        return self._process.returncode if self._process is not None else None

    @property
    def pid(self) -> int | None:
        """Adapter process id, when this client owns a subprocess."""

        return self._process.pid if self._process is not None else None

    @property
    def stderr(self) -> str:
        """Bounded tail of adapter stderr, decoded without raising."""

        return self._stderr_buffer.decode("utf-8", errors="replace")

    @property
    def stderr_dropped_bytes(self) -> int:
        """Number of leading stderr bytes discarded to enforce the bound."""

        return self._stderr_dropped_bytes

    @property
    def failure(self) -> DAPError | None:
        """Terminal transport failure, if closure was not explicitly requested."""

        return self._failure

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.close()

    def start_request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> asyncio.Task[JsonObject]:
        """Schedule a request, allowing DAP events before its response arrives."""

        if not command:
            raise ValueError("command must not be empty")
        return asyncio.create_task(
            self.request(command, arguments, timeout_s=timeout_s),
            name=f"debugpy-dap-{command}",
        )

    async def request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> JsonObject:
        """Send one request and correlate its possibly out-of-order response."""

        if not command:
            raise ValueError("command must not be empty")
        timeout = (
            self._request_timeout_s
            if timeout_s is None
            else _positive_finite(timeout_s, "timeout_s")
        )
        request_arguments = dict(arguments) if arguments is not None else None
        sequence: int | None = None
        pending: _PendingRequest | None = None

        try:
            async with asyncio.timeout(timeout):
                async with self._write_lock:
                    self._raise_if_closed()
                    sequence = self._allocate_sequence()
                    message: JsonObject = {
                        "seq": sequence,
                        "type": "request",
                        "command": command,
                    }
                    if request_arguments is not None:
                        message["arguments"] = request_arguments
                    encoded = _encode_dap_message(
                        message, max_message_bytes=self._max_message_bytes
                    )
                    future = asyncio.get_running_loop().create_future()
                    pending = _PendingRequest(command=command, future=future)
                    self._pending[sequence] = pending
                    try:
                        self._writer.write(encoded)
                        await self._writer.drain()
                    except (BrokenPipeError, ConnectionError, OSError) as exc:
                        closed = DAPClosedError(f"failed to write DAP request: {exc}")
                        self._remove_pending(sequence, pending)
                        pending.future.cancel()
                        await self._mark_closed(closed)
                        raise closed from exc
                return await pending.future
        except TimeoutError as exc:
            self._expire_pending(sequence, pending)
            raise DAPTimeoutError(
                f"DAP {command!r} request exceeded {timeout:.3f} seconds"
            ) from exc
        except asyncio.CancelledError:
            self._expire_pending(sequence, pending)
            raise
        except BaseException:
            self._remove_pending(sequence, pending)
            raise

    async def next_event(self, *, timeout_s: float | None = None) -> DAPEvent:
        """Return the oldest queued adapter event."""

        return await self._wait_for_queued_event(None, timeout_s=timeout_s)

    async def wait_for_event(self, event: str, *, timeout_s: float | None = None) -> DAPEvent:
        """Wait for one named event while preserving every unmatched event."""

        if not event:
            raise ValueError("event must not be empty")
        return await self._wait_for_queued_event(event, timeout_s=timeout_s)

    async def _wait_for_queued_event(
        self,
        event: str | None,
        *,
        timeout_s: float | None,
    ) -> DAPEvent:
        timeout = (
            self._request_timeout_s
            if timeout_s is None
            else _positive_finite(timeout_s, "timeout_s")
        )
        try:
            async with asyncio.timeout(timeout):
                async with self._event_condition:
                    while True:
                        if event is None and self._events:
                            queued = self._events.popleft()
                            self._event_bytes -= queued.encoded_bytes
                            return queued.event
                        if event is not None:
                            for index, candidate in enumerate(self._events):
                                if candidate.event.event == event:
                                    del self._events[index]
                                    self._event_bytes -= candidate.encoded_bytes
                                    return candidate.event
                        if self.closed:
                            self._raise_if_closed()
                        await self._event_condition.wait()
        except TimeoutError as exc:
            label = "next DAP event" if event is None else f"DAP {event!r} event"
            raise DAPTimeoutError(
                f"timed out waiting for {label} after {timeout:.3f} seconds"
            ) from exc

    async def initialize(
        self,
        arguments: Mapping[str, object] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> JsonObject:
        defaults: JsonObject = {
            "clientID": "react-agent-core",
            "clientName": "React Agent Core",
            "adapterID": "python",
            "pathFormat": "path",
            "linesStartAt1": True,
            "columnsStartAt1": True,
            "supportsVariableType": True,
            "supportsVariablePaging": True,
            "supportsRunInTerminalRequest": False,
        }
        if arguments is not None:
            defaults.update(arguments)
        return await self.request("initialize", defaults, timeout_s=timeout_s)

    def launch(
        self,
        arguments: Mapping[str, object],
        *,
        timeout_s: float | None = None,
    ) -> asyncio.Task[JsonObject]:
        """Start a launch request and return its response task immediately."""

        launch_arguments = dict(arguments)
        launch_arguments.setdefault("request", "launch")
        launch_arguments.setdefault("type", "debugpy")
        launch_arguments.setdefault("console", "internalConsole")
        return self.start_request("launch", launch_arguments, timeout_s=timeout_s)

    async def set_breakpoints(
        self,
        source_path: str | os.PathLike[str],
        lines: Sequence[int],
        *,
        source_modified: bool = False,
        timeout_s: float | None = None,
    ) -> JsonObject:
        breakpoints: list[JsonObject] = []
        for line in lines:
            breakpoints.append({"line": _positive_int(line, "breakpoint line")})
        arguments: JsonObject = {
            "source": {"path": os.fspath(source_path)},
            "breakpoints": breakpoints,
            "sourceModified": source_modified,
        }
        return await self.request("setBreakpoints", arguments, timeout_s=timeout_s)

    async def configuration_done(self, *, timeout_s: float | None = None) -> JsonObject:
        return await self.request("configurationDone", {}, timeout_s=timeout_s)

    async def threads(self, *, timeout_s: float | None = None) -> JsonObject:
        return await self.request("threads", {}, timeout_s=timeout_s)

    async def continue_(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        timeout_s: float | None = None,
    ) -> JsonObject:
        arguments = {
            "threadId": _positive_int(thread_id, "thread_id"),
            "singleThread": single_thread,
        }
        return await self.request("continue", arguments, timeout_s=timeout_s)

    async def next(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> JsonObject:
        return await self._step_request(
            "next",
            thread_id,
            single_thread=single_thread,
            granularity=granularity,
            timeout_s=timeout_s,
        )

    async def step_in(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> JsonObject:
        return await self._step_request(
            "stepIn",
            thread_id,
            single_thread=single_thread,
            granularity=granularity,
            timeout_s=timeout_s,
        )

    async def step_out(
        self,
        thread_id: int,
        *,
        single_thread: bool = False,
        granularity: str | None = None,
        timeout_s: float | None = None,
    ) -> JsonObject:
        return await self._step_request(
            "stepOut",
            thread_id,
            single_thread=single_thread,
            granularity=granularity,
            timeout_s=timeout_s,
        )

    async def _step_request(
        self,
        command: str,
        thread_id: int,
        *,
        single_thread: bool,
        granularity: str | None,
        timeout_s: float | None,
    ) -> JsonObject:
        arguments: JsonObject = {
            "threadId": _positive_int(thread_id, "thread_id"),
            "singleThread": single_thread,
        }
        if granularity is not None:
            if granularity not in {"statement", "line", "instruction"}:
                raise ValueError("granularity must be statement, line, or instruction")
            arguments["granularity"] = granularity
        return await self.request(command, arguments, timeout_s=timeout_s)

    async def pause(self, thread_id: int, *, timeout_s: float | None = None) -> JsonObject:
        return await self.request(
            "pause",
            {"threadId": _positive_int(thread_id, "thread_id")},
            timeout_s=timeout_s,
        )

    async def stack_trace(
        self,
        thread_id: int,
        *,
        start_frame: int = 0,
        levels: int | None = None,
        timeout_s: float | None = None,
    ) -> JsonObject:
        arguments: JsonObject = {
            "threadId": _positive_int(thread_id, "thread_id"),
            "startFrame": _non_negative_int(start_frame, "start_frame"),
        }
        if levels is not None:
            arguments["levels"] = _positive_int(levels, "levels")
        return await self.request("stackTrace", arguments, timeout_s=timeout_s)

    async def scopes(self, frame_id: int, *, timeout_s: float | None = None) -> JsonObject:
        return await self.request(
            "scopes",
            {"frameId": _positive_int(frame_id, "frame_id")},
            timeout_s=timeout_s,
        )

    async def variables(
        self,
        variables_reference: int,
        *,
        start: int = 0,
        count: int | None = None,
        filter_: str | None = None,
        timeout_s: float | None = None,
    ) -> JsonObject:
        arguments: JsonObject = {
            "variablesReference": _positive_int(variables_reference, "variables_reference"),
            "start": _non_negative_int(start, "start"),
        }
        if count is not None:
            arguments["count"] = _positive_int(count, "count")
        if filter_ is not None:
            if filter_ not in {"indexed", "named"}:
                raise ValueError("filter_ must be indexed or named")
            arguments["filter"] = filter_
        return await self.request("variables", arguments, timeout_s=timeout_s)

    async def disconnect(
        self,
        *,
        restart: bool = False,
        terminate_debuggee: bool = True,
        suspend_debuggee: bool = False,
        timeout_s: float | None = None,
    ) -> JsonObject:
        """Request a DAP disconnect and always reap the owned process group."""

        if self.closed:
            await self.close()
            return {}
        try:
            return await self.request(
                "disconnect",
                {
                    "restart": restart,
                    "terminateDebuggee": terminate_debuggee,
                    "suspendDebuggee": suspend_debuggee,
                },
                timeout_s=timeout_s,
            )
        finally:
            await self.close()

    async def close(self) -> None:
        """Close pipes, terminate/kill the process group, and await all tasks."""

        async with self._close_lock:
            if self._cleanup_complete:
                return
            self._closing = True
            await self._mark_closed(DAPClosedError("DAP client is closing"))
            self._writer.close()
            with suppress(BrokenPipeError, ConnectionError, OSError, RuntimeError, TimeoutError):
                await asyncio.wait_for(self._writer.wait_closed(), timeout=self._shutdown_timeout_s)
            await self._terminate_process()

            current = asyncio.current_task()
            tasks = tuple(
                task
                for task in (self._reader_task, self._stderr_task, self._process_task)
                if task is not None and task is not current
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._cleanup_complete = True

    def _allocate_sequence(self) -> int:
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def _raise_if_closed(self) -> None:
        if not self.closed:
            return
        if self._failure is not None:
            raise DAPClosedError(str(self._failure)) from self._failure
        raise DAPClosedError("DAP client is closed")

    def _remove_pending(self, sequence: int | None, pending: _PendingRequest | None) -> bool:
        if sequence is None or pending is None:
            return False
        if self._pending.get(sequence) is not pending:
            return False
        del self._pending[sequence]
        return True

    def _expire_pending(self, sequence: int | None, pending: _PendingRequest | None) -> None:
        if sequence is None or not self._remove_pending(sequence, pending):
            return
        self._expired_requests.add(sequence)
        self._expired_order.append(sequence)
        while len(self._expired_order) > _MAX_EXPIRED_REQUESTS:
            oldest = self._expired_order.popleft()
            self._expired_requests.discard(oldest)

    async def _read_loop(self) -> None:
        try:
            while True:
                message = await _read_dap_message(
                    self._reader,
                    max_header_bytes=self._max_header_bytes,
                    max_message_bytes=self._max_message_bytes,
                )
                await self._dispatch_message(message)
        except asyncio.CancelledError:
            raise
        except DAPError as exc:
            await self._mark_closed(exc)
            if not self._closing:
                await self._terminate_process()
        except (ConnectionError, OSError) as exc:
            closed = DAPClosedError(f"debug adapter read failed: {exc}")
            await self._mark_closed(closed)
            if not self._closing:
                await self._terminate_process()
        except Exception as exc:
            protocol = DAPProtocolError(f"unexpected DAP reader failure: {exc}")
            await self._mark_closed(protocol)
            if not self._closing:
                await self._terminate_process()

    async def _dispatch_message(self, message: JsonObject) -> None:
        sequence = self._message_sequence(message)
        message_type = message.get("type")
        if message_type == "response":
            self._handle_response(message)
            return
        if message_type == "event":
            await self._handle_event(sequence, message)
            return
        if message_type == "request":
            await self._reject_reverse_request(message)
            return
        raise DAPProtocolError("DAP message has an unknown or missing type")

    @staticmethod
    def _message_sequence(message: Mapping[str, object]) -> int:
        sequence = message.get("seq")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise DAPProtocolError("DAP message seq must be a non-negative integer")
        return sequence

    def _handle_response(self, message: JsonObject) -> None:
        request_sequence = message.get("request_seq")
        if (
            isinstance(request_sequence, bool)
            or not isinstance(request_sequence, int)
            or request_sequence <= 0
        ):
            raise DAPProtocolError("DAP response request_seq must be a positive integer")
        if request_sequence in self._expired_requests:
            self._expired_requests.discard(request_sequence)
            return
        pending = self._pending.pop(request_sequence, None)
        if pending is None:
            raise DAPProtocolError(f"DAP response refers to unknown request {request_sequence}")
        if pending.future.done():
            return

        command = message.get("command")
        if command != pending.command:
            error = DAPProtocolError(
                f"DAP response command {command!r} does not match {pending.command!r}"
            )
            pending.future.set_exception(error)
            raise error
        success = message.get("success")
        if not isinstance(success, bool):
            error = DAPProtocolError("DAP response success must be a boolean")
            pending.future.set_exception(error)
            raise error
        body_value = message.get("body")
        if body_value is None:
            body: JsonObject = {}
        elif isinstance(body_value, dict) and all(isinstance(key, str) for key in body_value):
            body = cast(JsonObject, body_value)
        else:
            error = DAPProtocolError("DAP response body must be an object when present")
            pending.future.set_exception(error)
            raise error

        if success:
            pending.future.set_result(body)
            return
        raw_message = message.get("message")
        diagnostic = raw_message if isinstance(raw_message, str) else ""
        pending.future.set_exception(DAPRequestError(pending.command, diagnostic, body=body))

    async def _handle_event(self, sequence: int, message: JsonObject) -> None:
        event_name = message.get("event")
        if not isinstance(event_name, str) or not event_name:
            raise DAPProtocolError("DAP event name must be a non-empty string")
        body_value = message.get("body")
        if body_value is None:
            body: JsonObject = {}
        elif isinstance(body_value, dict) and all(isinstance(key, str) for key in body_value):
            body = cast(JsonObject, body_value)
        else:
            raise DAPProtocolError("DAP event body must be an object when present")

        try:
            encoded_bytes = len(
                json.dumps(
                    message,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
        except (TypeError, ValueError, UnicodeError) as exc:
            raise DAPProtocolError(f"cannot account for queued DAP event: {exc}") from exc

        async with self._event_condition:
            if len(self._events) >= self._max_events:
                raise DAPProtocolError(
                    f"DAP event queue exceeded its {self._max_events}-event limit"
                )
            if encoded_bytes > self._max_event_bytes - self._event_bytes:
                raise DAPProtocolError(
                    f"DAP event queue exceeded its {self._max_event_bytes}-byte limit"
                )
            self._events.append(
                _QueuedEvent(
                    event=DAPEvent(seq=sequence, event=event_name, body=body),
                    encoded_bytes=encoded_bytes,
                )
            )
            self._event_bytes += encoded_bytes
            self._event_condition.notify_all()

    async def _reject_reverse_request(self, message: JsonObject) -> None:
        request_sequence = self._message_sequence(message)
        command = message.get("command")
        if not isinstance(command, str) or not command:
            raise DAPProtocolError("reverse DAP request command must be a non-empty string")
        async with self._write_lock:
            if self.closed:
                return
            response: JsonObject = {
                "seq": self._allocate_sequence(),
                "type": "response",
                "request_seq": request_sequence,
                "success": False,
                "command": command,
                "message": "reverse requests are not supported by this client",
            }
            encoded = _encode_dap_message(response, max_message_bytes=self._max_message_bytes)
            try:
                self._writer.write(encoded)
                await self._writer.drain()
            except (BrokenPipeError, ConnectionError, OSError) as exc:
                raise DAPClosedError(f"failed to reject reverse DAP request: {exc}") from exc

    async def _mark_closed(self, error: DAPError) -> None:
        if not self._closed:
            self._closed = True
            if not self._closing:
                self._failure = error
            pending = tuple(self._pending.values())
            self._pending.clear()
            for item in pending:
                if not item.future.done():
                    item.future.set_exception(DAPClosedError(str(error)))
        async with self._event_condition:
            self._event_condition.notify_all()

    async def _capture_stderr(self, stderr: asyncio.StreamReader) -> None:
        try:
            while chunk := await stderr.read(4_096):
                overflow = len(self._stderr_buffer) + len(chunk) - self._max_stderr_bytes
                if overflow > 0:
                    discarded = min(overflow, len(self._stderr_buffer))
                    if discarded:
                        del self._stderr_buffer[:discarded]
                    remaining_overflow = overflow - discarded
                    if remaining_overflow:
                        chunk = chunk[remaining_overflow:]
                    self._stderr_dropped_bytes += overflow
                self._stderr_buffer.extend(chunk)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError):
            # stderr is diagnostic-only; stdout/proc status owns session health.
            return

    async def _watch_process(self) -> None:
        process = self._process
        if process is None:
            return
        try:
            returncode = await process.wait()
        except asyncio.CancelledError:
            raise
        if not self._closing:
            await self._mark_closed(
                DAPClosedError(f"debug adapter exited with status {returncode}")
            )
            # The adapter is the process-group leader. Kill any surviving
            # debuggee children immediately, before its numeric PGID can be
            # reused by an unrelated process group.
            await self._terminate_process()

    async def _terminate_process(self) -> None:
        process = self._process
        if process is None:
            return
        async with self._termination_lock:
            if os.name == "nt":
                await self._terminate_windows_tree(process)
                return
            if process.returncode is not None:
                self._signal_process_group(process, force=True)
                return
            self._signal_process_group(process, force=False)
            try:
                await asyncio.wait_for(
                    asyncio.shield(process.wait()), timeout=self._shutdown_timeout_s
                )
                self._signal_process_group(process, force=True)
                return
            except TimeoutError:
                pass
            self._signal_process_group(process, force=True)
            try:
                await asyncio.wait_for(
                    asyncio.shield(process.wait()), timeout=self._shutdown_timeout_s
                )
            except TimeoutError as exc:
                raise DAPClosedError("debug adapter did not exit after forced termination") from exc

    async def _terminate_windows_tree(self, process: asyncio.subprocess.Process) -> None:
        """Use the Windows tree-aware system terminator for adapter children."""

        terminator: asyncio.subprocess.Process | None = None
        taskkill_failed = False
        cancelled: asyncio.CancelledError | None = None
        terminator_reap_error: DAPClosedError | None = None
        try:
            terminator = await asyncio.create_subprocess_exec(
                "taskkill",
                "/PID",
                str(process.pid),
                "/T",
                "/F",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except asyncio.CancelledError as exc:
            cancelled = exc
            taskkill_failed = True
        except (FileNotFoundError, OSError):
            taskkill_failed = True
        else:
            try:
                returncode = await asyncio.wait_for(
                    asyncio.shield(terminator.wait()),
                    timeout=self._shutdown_timeout_s,
                )
                taskkill_failed = returncode != 0
            except asyncio.CancelledError as exc:
                cancelled = exc
                taskkill_failed = True
            except TimeoutError:
                taskkill_failed = True

        if terminator is not None and terminator.returncode is None and taskkill_failed:
            with suppress(ProcessLookupError, OSError):
                terminator.kill()
            try:
                await asyncio.wait_for(
                    asyncio.shield(terminator.wait()),
                    timeout=self._shutdown_timeout_s,
                )
            except TimeoutError:
                terminator_reap_error = DAPClosedError(
                    "Windows taskkill helper did not exit after forced termination"
                )
            except asyncio.CancelledError as exc:
                cancelled = exc

        if taskkill_failed and process.returncode is None:
            with suppress(ProcessLookupError, OSError):
                process.kill()
        if process.returncode is None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(process.wait()), timeout=self._shutdown_timeout_s
                )
            except TimeoutError:
                with suppress(ProcessLookupError, OSError):
                    process.kill()
                try:
                    await asyncio.wait_for(
                        asyncio.shield(process.wait()),
                        timeout=self._shutdown_timeout_s,
                    )
                except TimeoutError as exc:
                    raise DAPClosedError(
                        "debug adapter tree did not exit after forced termination"
                    ) from exc
        if cancelled is not None:
            raise cancelled
        if terminator_reap_error is not None:
            raise terminator_reap_error

    @staticmethod
    def _signal_process_group(process: asyncio.subprocess.Process, *, force: bool) -> None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
            elif process.returncode is not None:
                return
            elif os.name == "nt" and not force:
                ctrl_break = getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM)
                process.send_signal(ctrl_break)
            elif force:
                process.kill()
            else:
                process.terminate()
        except ProcessLookupError:
            return
        except (PermissionError, OSError):
            with suppress(ProcessLookupError):
                if force:
                    process.kill()
                else:
                    process.terminate()


__all__ = [
    "DAPClosedError",
    "DAPError",
    "DAPEvent",
    "DAPProtocolError",
    "DAPRequestError",
    "DAPTimeoutError",
    "DebugpyDAPClient",
]
