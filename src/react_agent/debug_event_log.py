"""Private, hash-chained event log for standalone MCP debug sessions."""

from __future__ import annotations

import asyncio
import errno
import json
import os
import stat
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, TypeVar, cast

from .events import (
    EVENT_SCHEMA_VERSION,
    GENESIS_HASH,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    StoredRunEvent,
    canonical_json,
    verify_event_chain,
)
from .models import Usage

R = TypeVar("R")

_AGENT_REVISION = "standalone-debug-mcp-v1"
_TOOL_MANIFEST_HASH = "python-runtime-debug-tools-v1"


def _json_copy(value: object) -> Any:
    return json.loads(canonical_json(value))


def _normalize_log_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    normalized = candidate.parent.resolve() / candidate.name
    try:
        metadata = os.lstat(normalized)
    except FileNotFoundError:
        return normalized
    if stat.S_ISLNK(metadata.st_mode):
        raise ValueError("debug event log must not be a symbolic link")
    return normalized


def _event_payload(event: StoredRunEvent) -> dict[str, object]:
    return {
        "run_id": event.run_id,
        "sequence": event.sequence,
        "operation_id": event.operation_id,
        "event_id": event.event_id,
        "kind": event.kind.value,
        "privacy": event.privacy.value,
        "schema_version": event.schema_version,
        "occurred_at": event.occurred_at,
        "previous_hash": event.previous_hash,
        "event_hash": event.event_hash,
        "step": event.step,
        "call_key": event.call_key,
        "causation_id": event.causation_id,
        "session_id": event.session_id,
        "execution_id": event.execution_id,
        "agent_revision": event.agent_revision,
        "tool_manifest_hash": event.tool_manifest_hash,
        "data": event.data,
        "checkpoint": event.checkpoint,
        "safe_checkpoint": event.safe_checkpoint,
        "usage_delta": {
            "input_tokens": event.usage_delta.input_tokens,
            "output_tokens": event.usage_delta.output_tokens,
            "total_tokens": event.usage_delta.total_tokens,
            "cached_input_tokens": event.usage_delta.cached_input_tokens,
            "reasoning_output_tokens": event.usage_delta.reasoning_output_tokens,
            "billable_tokens": event.usage_delta.billable_tokens,
        },
        "model_calls_delta": event.model_calls_delta,
        "tool_calls_delta": event.tool_calls_delta,
        "tool_executions_delta": event.tool_executions_delta,
    }


def _usage(raw: object) -> Usage:
    if not isinstance(raw, Mapping):
        raise ValueError("debug event usage_delta must be an object")

    def integer(name: str, *, optional: bool = False) -> int | None:
        value = raw.get(name)
        if optional and value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"debug event usage_delta.{name} is invalid")
        return value

    return Usage(
        input_tokens=cast(int, integer("input_tokens")),
        output_tokens=cast(int, integer("output_tokens")),
        total_tokens=cast(int, integer("total_tokens")),
        cached_input_tokens=integer("cached_input_tokens", optional=True),
        reasoning_output_tokens=integer("reasoning_output_tokens", optional=True),
        billable_tokens=integer("billable_tokens", optional=True),
    )


def _optional_text(raw: Mapping[str, object], name: str) -> str | None:
    value = raw.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"debug event {name} must be text or null")
    return value


def _optional_int(raw: Mapping[str, object], name: str) -> int | None:
    value = raw.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"debug event {name} must be an integer or null")
    return value


def _event_from_payload(raw: object) -> StoredRunEvent:
    if not isinstance(raw, Mapping):
        raise ValueError("debug event must be an object")

    def text_value(name: str) -> str:
        value = raw.get(name)
        if not isinstance(value, str):
            raise ValueError(f"debug event {name} must be text")
        return value

    def int_value(name: str) -> int:
        value = raw.get(name)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"debug event {name} must be an integer")
        return value

    occurred_at = raw.get("occurred_at")
    if isinstance(occurred_at, bool) or not isinstance(occurred_at, (int, float)):
        raise ValueError("debug event occurred_at must be numeric")
    data = raw.get("data")
    checkpoint = raw.get("checkpoint")
    if not isinstance(data, Mapping):
        raise ValueError("debug event data must be an object")
    if checkpoint is not None and not isinstance(checkpoint, Mapping):
        raise ValueError("debug event checkpoint must be an object or null")
    safe_checkpoint = raw.get("safe_checkpoint")
    if not isinstance(safe_checkpoint, bool):
        raise ValueError("debug event safe_checkpoint must be boolean")
    return StoredRunEvent(
        run_id=text_value("run_id"),
        sequence=int_value("sequence"),
        operation_id=text_value("operation_id"),
        event_id=text_value("event_id"),
        kind=RunEventKind(text_value("kind")),
        privacy=PrivacyClass(text_value("privacy")),
        schema_version=int_value("schema_version"),
        occurred_at=float(occurred_at),
        previous_hash=text_value("previous_hash"),
        event_hash=text_value("event_hash"),
        step=_optional_int(raw, "step"),
        call_key=_optional_text(raw, "call_key"),
        causation_id=_optional_text(raw, "causation_id"),
        session_id=_optional_text(raw, "session_id"),
        execution_id=_optional_text(raw, "execution_id"),
        agent_revision=_optional_text(raw, "agent_revision"),
        tool_manifest_hash=_optional_text(raw, "tool_manifest_hash"),
        data=cast(Mapping[str, Any], data),
        checkpoint=cast(Mapping[str, Any] | None, checkpoint),
        safe_checkpoint=safe_checkpoint,
        usage_delta=_usage(raw.get("usage_delta")),
        model_calls_delta=int_value("model_calls_delta"),
        tool_calls_delta=int_value("tool_calls_delta"),
        tool_executions_delta=int_value("tool_executions_delta"),
    )


def write_debug_event_log(path: str | Path, events: Sequence[StoredRunEvent]) -> None:
    """Atomically persist a verified private event chain with mode 0600."""

    resolved = _normalize_log_path(path)
    verify_event_chain(events)
    parent_existed = resolved.parent.exists()
    resolved.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not parent_existed:
        try:
            os.chmod(resolved.parent, 0o700)
        except OSError:
            pass
    payload = {
        "format": "react-agent-debug-events-v1",
        "schema_version": EVENT_SCHEMA_VERSION,
        "events": [_event_payload(event) for event in events],
    }
    descriptor, temporary_name = tempfile.mkstemp(
        dir=resolved.parent,
        prefix=f".{resolved.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
        os.chmod(resolved, 0o600)
        if os.name != "nt":
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_descriptor = os.open(resolved.parent, directory_flags)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def load_debug_event_log(path: str | Path) -> tuple[StoredRunEvent, ...]:
    """Load and verify a standalone MCP debug event chain."""

    candidate = _normalize_log_path(path)
    metadata = os.lstat(candidate)
    if stat.S_ISLNK(metadata.st_mode):
        raise ValueError("debug event log must not be a symbolic link")
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("debug event log must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError("debug event log must not be a symbolic link") from None
        raise
    try:
        opened_metadata = os.fstat(descriptor)
        if not stat.S_ISREG(opened_metadata.st_mode):
            raise ValueError("debug event log must be a regular file")
        if os.name != "nt" and opened_metadata.st_mode & 0o077:
            raise ValueError("debug event log permissions are not private")
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            raw = json.load(handle)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(raw, Mapping) or raw.get("format") != "react-agent-debug-events-v1":
        raise ValueError("unsupported debug event log format")
    raw_events = raw.get("events")
    if not isinstance(raw_events, list):
        raise ValueError("debug event log events must be an array")
    events = tuple(_event_from_payload(item) for item in raw_events)
    verify_event_chain(events)
    return events


class MCPDebugEventJournal:
    """Translate standalone MCP calls into the durable Agent lifecycle shape."""

    def __init__(self, path: str | Path) -> None:
        self.path = _normalize_log_path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(
            self.path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(descriptor)
        self.run_id = f"mcp-debug-{uuid.uuid4().hex}"
        self.session_id = f"mcp-session-{uuid.uuid4().hex}"
        self.execution_id = f"mcp-execution-{uuid.uuid4().hex}"
        self._events: list[StoredRunEvent] = []
        self._lock = asyncio.Lock()
        self._call_counter = 0
        self._closed = False
        self._append_unlocked(
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                privacy=PrivacyClass.PRIVATE,
                session_id=self.session_id,
                execution_id=self.execution_id,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
                data={"status": "running", "transport": "mcp_stdio"},
                checkpoint={"transcript": []},
                safe_checkpoint=True,
            ),
            "run:started",
        )
        write_debug_event_log(self.path, self._events)

    @property
    def events(self) -> tuple[StoredRunEvent, ...]:
        return tuple(self._events)

    async def invoke(
        self,
        tool_name: str,
        arguments: Mapping[str, object],
        operation: Callable[[], Awaitable[R]],
    ) -> R:
        """Record planned/started/completed around one MCP tool operation."""

        async with self._lock:
            if self._closed:
                raise RuntimeError("debug event journal is closed")
            self._call_counter += 1
            number = self._call_counter
            call_key = f"mcp:{number}"
            call_id = f"mcp-call-{number}"
            step = number
            call = {
                "id": call_id,
                "name": tool_name,
                "arguments": canonical_json(arguments),
            }
            public = {"tool_call_id": call_id, "tool_name": tool_name}
            self._append_unlocked(
                RunEventDraft(
                    kind=RunEventKind.TOOL_PLANNED,
                    privacy=PrivacyClass.PRIVATE,
                    step=step,
                    call_key=call_key,
                    data=public,
                    checkpoint={"call": call},
                    safe_checkpoint=True,
                    tool_calls_delta=1,
                ),
                f"tool:{call_key}:planned",
            )
            self._append_unlocked(
                RunEventDraft(
                    kind=RunEventKind.TOOL_STARTED,
                    privacy=PrivacyClass.PRIVATE,
                    step=step,
                    call_key=call_key,
                    data=public,
                    checkpoint={"call": call},
                ),
                f"tool:{call_key}:started",
            )
            await asyncio.to_thread(
                write_debug_event_log, self.path, tuple(self._events)
            )
        try:
            result = await operation()
        except asyncio.CancelledError:
            await self._complete_error(
                step, call_key, call_id, tool_name, "CANCELLED", "MCP tool call cancelled."
            )
            raise
        except Exception as exc:
            await self._complete_error(
                step,
                call_key,
                call_id,
                tool_name,
                "MCP_TOOL_ERROR",
                f"MCP tool failed with {type(exc).__name__}.",
            )
            raise
        await self._complete_success(step, call_key, call_id, tool_name, result)
        return result

    async def _complete_success(
        self,
        step: int,
        call_key: str,
        call_id: str,
        tool_name: str,
        result: object,
    ) -> None:
        content = json.dumps(
            {"ok": True, "data": result, "meta": {"truncated": False}},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        await self._append_terminal(step, call_key, call_id, tool_name, content, False)

    async def _complete_error(
        self,
        step: int,
        call_key: str,
        call_id: str,
        tool_name: str,
        code: str,
        message: str,
    ) -> None:
        content = json.dumps(
            {
                "ok": False,
                "error": {"code": code, "message": message, "retryable": False},
            },
            separators=(",", ":"),
        )
        await self._append_terminal(step, call_key, call_id, tool_name, content, True)

    async def _append_terminal(
        self,
        step: int,
        call_key: str,
        call_id: str,
        tool_name: str,
        content: str,
        is_error: bool,
    ) -> None:
        message = {
            "role": "tool",
            "call_id": call_id,
            "name": tool_name,
            "content": content,
            "is_error": is_error,
            "executed": True,
            "cached": False,
            "duration_ms": 0.0,
        }
        async with self._lock:
            self._append_unlocked(
                RunEventDraft(
                    kind=RunEventKind.TOOL_COMPLETED,
                    privacy=PrivacyClass.PRIVATE,
                    step=step,
                    call_key=call_key,
                    data={
                        "tool_call_id": call_id,
                        "tool_name": tool_name,
                        "outcome": "completed",
                        "is_error": is_error,
                        "executed": True,
                    },
                    checkpoint={"message": message},
                    tool_executions_delta=1,
                ),
                f"tool:{call_key}:completed",
            )
            await asyncio.to_thread(
                write_debug_event_log, self.path, tuple(self._events)
            )

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._append_unlocked(
                RunEventDraft(
                    kind=RunEventKind.RUN_COMPLETED,
                    data={"status": "completed", "stop_reason": "completed"},
                    safe_checkpoint=True,
                ),
                "run:completed",
            )
            await asyncio.to_thread(
                write_debug_event_log, self.path, tuple(self._events)
            )

    def _append_unlocked(self, draft: RunEventDraft, operation_id: str) -> None:
        previous = self._events[-1] if self._events else None
        event = StoredRunEvent.from_draft(
            draft,
            run_id=self.run_id,
            sequence=len(self._events) + 1,
            operation_id=operation_id,
            previous_hash=previous.event_hash if previous is not None else GENESIS_HASH,
            occurred_at=time.time(),
            causation_id=previous.event_id if previous is not None else None,
            session_id=self.session_id,
            execution_id=self.execution_id,
            agent_revision=_AGENT_REVISION,
            tool_manifest_hash=_TOOL_MANIFEST_HASH,
        )
        self._events.append(event)
