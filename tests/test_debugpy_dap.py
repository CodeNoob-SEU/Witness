from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping

import pytest

from react_agent.debugpy_dap import (
    DAPClosedError,
    DAPProtocolError,
    DAPRequestError,
    DAPTimeoutError,
    DebugpyDAPClient,
    _encode_dap_message,
    _read_dap_message,
)


class MemoryWriter:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[bytes] = asyncio.Queue()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.messages.put_nowait(data)

    async def drain(self) -> None:
        await asyncio.sleep(0)

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        await asyncio.sleep(0)


class FakeProcess:
    def __init__(self, *, pid: int, returncode: int | None = None) -> None:
        self.pid = pid
        self.returncode = returncode
        self.kill_calls = 0
        self._exited = asyncio.Event()
        self.wait_entered = asyncio.Event()
        if returncode is not None:
            self._exited.set()

    async def wait(self) -> int:
        self.wait_entered.set()
        await self._exited.wait()
        assert self.returncode is not None
        return self.returncode

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9
        self._exited.set()


def frame(message: Mapping[str, object], *, limit: int = 1_000_000) -> bytes:
    return _encode_dap_message(message, max_message_bytes=limit)


def decode_frame(encoded: bytes) -> dict[str, object]:
    header, body = encoded.split(b"\r\n\r\n", 1)
    assert header == f"Content-Length: {len(body)}".encode()
    decoded = json.loads(body)
    assert isinstance(decoded, dict)
    return decoded


def response(
    *,
    seq: int,
    request: Mapping[str, object],
    body: Mapping[str, object] | None = None,
    success: bool = True,
    message: str | None = None,
) -> bytes:
    payload: dict[str, object] = {
        "seq": seq,
        "type": "response",
        "request_seq": request["seq"],
        "command": request["command"],
        "success": success,
        "body": dict(body or {}),
    }
    if message is not None:
        payload["message"] = message
    return frame(payload)


@pytest.mark.asyncio
async def test_framing_uses_utf8_byte_length_and_accepts_fragmented_batched_input() -> None:
    first = frame({"seq": 1, "type": "event", "event": "output", "body": {"text": "中文"}})
    second = frame({"seq": 2, "type": "event", "event": "initialized"})
    reader = asyncio.StreamReader()

    for byte in first[:17]:
        reader.feed_data(bytes([byte]))
    reader.feed_data(first[17:] + second)

    first_message = await _read_dap_message(reader, max_header_bytes=1_000, max_message_bytes=1_000)
    second_message = await _read_dap_message(
        reader, max_header_bytes=1_000, max_message_bytes=1_000
    )

    assert first_message["body"] == {"text": "中文"}
    assert second_message["event"] == "initialized"
    first_header, first_body = first.split(b"\r\n\r\n", 1)
    assert first_header == f"Content-Length: {len(first_body)}".encode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b"Content-Length: 100\r\n\r\n",
        b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
        b"Content-Length: nope\r\n\r\n{}",
    ],
)
async def test_framing_rejects_oversized_duplicate_or_invalid_lengths(payload: bytes) -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(payload)

    with pytest.raises(DAPProtocolError):
        await _read_dap_message(reader, max_header_bytes=1_000, max_message_bytes=10)


@pytest.mark.asyncio
async def test_concurrent_requests_correlate_out_of_order_responses_and_queue_events() -> None:
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer)
    try:
        first_task = client.start_request("alpha", {"value": 1})
        second_task = client.start_request("beta", {"value": 2})
        first_request = decode_frame(await writer.messages.get())
        second_request = decode_frame(await writer.messages.get())

        reader.feed_data(
            frame({"seq": 20, "type": "event", "event": "stopped", "body": {"threadId": 7}})
            + response(seq=21, request=second_request, body={"order": 2})
            + response(seq=22, request=first_request, body={"order": 1})
        )

        assert await first_task == {"order": 1}
        assert await second_task == {"order": 2}
        stopped = await client.next_event()
        assert stopped.event == "stopped"
        assert stopped.body == {"threadId": 7}
    finally:
        await client.close()
    assert writer.closed is True


@pytest.mark.asyncio
async def test_wait_for_event_preserves_unmatched_event_order() -> None:
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer)
    try:
        reader.feed_data(
            frame({"seq": 1, "type": "event", "event": "output", "body": {"output": "x"}})
            + frame({"seq": 2, "type": "event", "event": "initialized"})
            + frame({"seq": 3, "type": "event", "event": "terminated"})
        )

        initialized = await client.wait_for_event("initialized")
        assert initialized.seq == 2
        assert (await client.next_event()).event == "output"
        assert (await client.next_event()).event == "terminated"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_event_queue_fails_closed_when_total_byte_budget_is_exceeded() -> None:
    first = {
        "seq": 1,
        "type": "event",
        "event": "output",
        "body": {"output": "x" * 80},
    }
    second = {
        "seq": 2,
        "type": "event",
        "event": "output",
        "body": {"output": "y" * 80},
    }
    budget = max(
        len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        for item in (first, second)
    )
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, max_event_bytes=budget)
    try:
        reader.feed_data(frame(first) + frame(second))
        for _ in range(100):
            if client.closed:
                break
            await asyncio.sleep(0)

        assert client.closed is True
        assert (await client.next_event()).seq == 1
        with pytest.raises(DAPClosedError, match="byte limit"):
            await client.next_event(timeout_s=0.1)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_consuming_an_event_releases_its_queue_byte_budget() -> None:
    payload = {
        "seq": 1,
        "type": "event",
        "event": "output",
        "body": {"output": "z" * 80},
    }
    budget = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode())
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, max_event_bytes=budget)
    try:
        reader.feed_data(frame(payload))
        assert (await client.next_event()).seq == 1

        payload["seq"] = 2
        reader.feed_data(frame(payload))
        assert (await client.next_event()).seq == 2
        assert client.closed is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_timeout_ignores_late_response_without_poisoning_connection() -> None:
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, request_timeout_s=0.01)
    try:
        timed_out = client.start_request("slow")
        slow_request = decode_frame(await writer.messages.get())
        with pytest.raises(DAPTimeoutError):
            await timed_out

        reader.feed_data(response(seq=10, request=slow_request, body={"late": True}))
        healthy = client.start_request("healthy", timeout_s=1)
        healthy_request = decode_frame(await writer.messages.get())
        reader.feed_data(response(seq=11, request=healthy_request, body={"ok": True}))

        assert await healthy == {"ok": True}
        assert client.closed is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_unsuccessful_response_exposes_command_message_and_body() -> None:
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer)
    try:
        task = client.start_request("variables")
        request = decode_frame(await writer.messages.get())
        reader.feed_data(
            response(
                seq=3,
                request=request,
                success=False,
                message="not stopped",
                body={"error": {"id": 42}},
            )
        )

        with pytest.raises(DAPRequestError) as captured:
            await task
        assert captured.value.command == "variables"
        assert captured.value.message == "not stopped"
        assert captured.value.body == {"error": {"id": 42}}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_launch_is_scheduled_without_waiting_for_deferred_response() -> None:
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer)
    try:
        launch_task = client.launch({"program": "/workspace/demo.py"})
        request = decode_frame(await writer.messages.get())

        assert request["command"] == "launch"
        assert request["arguments"] == {
            "program": "/workspace/demo.py",
            "request": "launch",
            "type": "debugpy",
            "console": "internalConsole",
        }
        assert launch_task.done() is False

        reader.feed_data(response(seq=2, request=request, body={}))
        assert await launch_task == {}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_windows_tree_cleanup_falls_back_when_taskkill_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminator = FakeProcess(pid=100, returncode=1)
    adapter = FakeProcess(pid=200)

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> FakeProcess:
        del args, kwargs
        return terminator

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, shutdown_timeout_s=0.01)
    try:
        await client._terminate_windows_tree(adapter)  # type: ignore[arg-type]
    finally:
        await client.close()

    assert terminator.kill_calls == 0
    assert adapter.kill_calls == 1
    assert adapter.returncode == -9


@pytest.mark.asyncio
async def test_windows_tree_cleanup_kills_and_reaps_timed_out_taskkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminator = FakeProcess(pid=300)
    adapter = FakeProcess(pid=400)

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> FakeProcess:
        del args, kwargs
        return terminator

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, shutdown_timeout_s=0.01)
    try:
        await client._terminate_windows_tree(adapter)  # type: ignore[arg-type]
    finally:
        await client.close()

    assert terminator.kill_calls == 1
    assert terminator.returncode == -9
    assert adapter.kill_calls == 1
    assert adapter.returncode == -9


@pytest.mark.asyncio
async def test_windows_tree_cleanup_reaps_taskkill_before_propagating_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminator = FakeProcess(pid=500)
    adapter = FakeProcess(pid=600)

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> FakeProcess:
        del args, kwargs
        return terminator

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    reader = asyncio.StreamReader()
    writer = MemoryWriter()
    client = DebugpyDAPClient._from_streams(reader, writer, shutdown_timeout_s=0.1)
    try:
        cleanup = asyncio.create_task(client._terminate_windows_tree(adapter))  # type: ignore[arg-type]
        await asyncio.wait_for(terminator.wait_entered.wait(), timeout=1)
        cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup
    finally:
        await client.close()

    assert terminator.kill_calls == 1
    assert terminator.returncode == -9
    assert adapter.kill_calls == 1
    assert adapter.returncode == -9
