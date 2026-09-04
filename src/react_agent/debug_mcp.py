"""Official MCP v2 stdio adapter for the Python runtime debugger."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, TypeVar

from mcp.server import MCPServer

from .debug_event_log import MCPDebugEventJournal
from .runtime_debugger import DebugBreakpoint, PythonRuntimeDebugger

R = TypeVar("R")


class _MCPDebugHandlers:
    """Typed MCP adapters; all behaviour remains in PythonRuntimeDebugger."""

    def __init__(
        self,
        debugger: PythonRuntimeDebugger,
        *,
        journal: MCPDebugEventJournal | None,
        allow_execution: bool,
    ) -> None:
        self.debugger = debugger
        self.journal = journal
        self.allow_execution = allow_execution

    async def _invoke(
        self,
        name: str,
        arguments: Mapping[str, object],
        operation: Callable[[], Awaitable[R]],
        *,
        changes_execution: bool,
    ) -> R:
        async def checked() -> R:
            if changes_execution and not self.allow_execution:
                raise PermissionError(
                    "Runtime execution is disabled; start the server with --allow-execution."
                )
            return await operation()

        if self.journal is None:
            return await checked()
        return await self.journal.invoke(name, arguments, checked)

    async def launch(
        self,
        program: str,
        args: list[str],
        breakpoints: list[DebugBreakpoint],
        exception_policy: Literal["uncaught", "raised", "none"],
        stop_on_entry: bool,
        wait_timeout_s: float,
    ) -> dict[str, object]:
        """Launch a workspace Python script and wait for its first stop or exit."""

        if exception_policy not in {"uncaught", "raised", "none"}:
            raise ValueError("exception_policy must be uncaught, raised, or none")
        arguments: dict[str, object] = {
            "program": program,
            "args": args,
            "breakpoints": [item.model_dump(mode="json") for item in breakpoints],
            "exception_policy": exception_policy,
            "stop_on_entry": stop_on_entry,
            "wait_timeout_s": wait_timeout_s,
        }
        return await self._invoke(
            "python_debug_launch",
            arguments,
            lambda: self.debugger.launch(
                program,
                args,
                breakpoints,
                exception_policy,
                stop_on_entry,
                wait_timeout_s,
            ),
            changes_execution=True,
        )

    async def set_breakpoints(
        self,
        debug_session_id: str,
        file: str,
        lines: list[int],
    ) -> dict[str, object]:
        """Replace all breakpoints for one workspace source file."""

        arguments: dict[str, object] = {
            "debug_session_id": debug_session_id,
            "file": file,
            "lines": lines,
        }
        return await self._invoke(
            "python_debug_set_breakpoints",
            arguments,
            lambda: self.debugger.set_breakpoints(debug_session_id, file, lines),
            changes_execution=True,
        )

    async def control(
        self,
        debug_session_id: str,
        action: Literal["continue", "next", "step_in", "step_out", "pause"],
        stop_id: int | None,
        wait_timeout_s: float,
    ) -> dict[str, object]:
        """Continue, step, or pause a live debug session."""

        if action not in {"continue", "next", "step_in", "step_out", "pause"}:
            raise ValueError("unsupported debug control action")
        arguments: dict[str, object] = {
            "debug_session_id": debug_session_id,
            "action": action,
            "stop_id": stop_id,
            "wait_timeout_s": wait_timeout_s,
        }
        return await self._invoke(
            "python_debug_control",
            arguments,
            lambda: self.debugger.control(
                debug_session_id,
                action,
                stop_id,
                wait_timeout_s,
            ),
            changes_execution=True,
        )

    async def stack(
        self,
        debug_session_id: str,
        stop_id: int,
        levels: int,
    ) -> dict[str, object]:
        """Read and deterministically rank frames at the current stop."""

        arguments: dict[str, object] = {
            "debug_session_id": debug_session_id,
            "stop_id": stop_id,
            "levels": levels,
        }
        return await self._invoke(
            "python_debug_stack",
            arguments,
            lambda: self.debugger.stack(debug_session_id, stop_id, levels),
            changes_execution=False,
        )

    async def select_frame(
        self,
        debug_session_id: str,
        stop_id: int,
        frame_index: int,
    ) -> dict[str, object]:
        """Select one ranked frame without exposing its transient DAP id."""

        arguments: dict[str, object] = {
            "debug_session_id": debug_session_id,
            "stop_id": stop_id,
            "frame_index": frame_index,
        }
        return await self._invoke(
            "python_debug_select_frame",
            arguments,
            lambda: self.debugger.select_frame(debug_session_id, stop_id, frame_index),
            changes_execution=False,
        )

    async def variables(
        self,
        debug_session_id: str,
        stop_id: int,
        scope: Literal["locals", "arguments"],
        max_variables: int,
        max_value_chars: int,
    ) -> dict[str, object]:
        """Read bounded, redacted locals or arguments from the selected frame."""

        if scope not in {"locals", "arguments"}:
            raise ValueError("scope must be locals or arguments")
        arguments: dict[str, object] = {
            "debug_session_id": debug_session_id,
            "stop_id": stop_id,
            "scope": scope,
            "max_variables": max_variables,
            "max_value_chars": max_value_chars,
        }
        return await self._invoke(
            "python_debug_variables",
            arguments,
            lambda: self.debugger.variables(
                debug_session_id,
                stop_id,
                scope,
                max_variables,
                max_value_chars,
            ),
            changes_execution=False,
        )

    async def stop(self, debug_session_id: str) -> dict[str, object]:
        """Terminate the debuggee and reap the owned process group."""

        return await self._invoke(
            "python_debug_stop",
            {"debug_session_id": debug_session_id},
            lambda: self.debugger.stop(debug_session_id),
            changes_execution=True,
        )


def build_debug_mcp_server(
    debugger: PythonRuntimeDebugger,
    *,
    event_journal: MCPDebugEventJournal | None = None,
    allow_execution: bool = False,
) -> MCPServer[Any]:
    """Build the official MCP server over one shared debugger core."""

    handlers = _MCPDebugHandlers(
        debugger,
        journal=event_journal,
        allow_execution=allow_execution,
    )

    @asynccontextmanager
    async def lifespan(server: MCPServer[Any]) -> Any:
        del server
        try:
            yield {"debugger": debugger}
        finally:
            await debugger.close()
            if event_journal is not None:
                await event_journal.close()

    server: MCPServer[Any] = MCPServer(
        name="react-agent-python-runtime-debugger",
        title="React Agent Python Runtime Debugger",
        description="Python-only stdio DAP debugger with deterministic evidence logging.",
        version="1.0.0",
        lifespan=lifespan,
    )
    registrations = (
        ("python_debug_launch", handlers.launch),
        ("python_debug_set_breakpoints", handlers.set_breakpoints),
        ("python_debug_control", handlers.control),
        ("python_debug_stack", handlers.stack),
        ("python_debug_select_frame", handlers.select_frame),
        ("python_debug_variables", handlers.variables),
        ("python_debug_stop", handlers.stop),
    )
    for name, function in registrations:
        server.add_tool(function, name=name)
        _harden_registered_tool_schema(server, name)
    return server


def _harden_registered_tool_schema(server: MCPServer[Any], name: str) -> None:
    """Make MCP 2.0's generated argument model strict as well as its schema.

    MCP SDK 2.0 builds function argument models with Pydantic's default
    ``extra='ignore'``.  Advertising ``additionalProperties: false`` without
    changing that model would be cosmetic: a non-conforming client could still
    smuggle unknown fields that the server silently drops.  The SDK does not
    expose an argument-model configuration hook, so this pinned-version adapter
    contains the private access and fails at startup if the expected seam moves.
    """

    manager = getattr(server, "_tool_manager", None)
    registered = manager.get_tool(name) if manager is not None else None
    metadata = getattr(registered, "fn_metadata", None)
    argument_model = getattr(metadata, "arg_model", None)
    if registered is None or argument_model is None:
        raise RuntimeError("MCP 2.0 tool metadata seam is unavailable")
    argument_model.model_config["extra"] = "forbid"
    argument_model.model_rebuild(force=True)
    parameters = argument_model.model_json_schema(by_alias=True)
    if parameters.get("additionalProperties") is not False:
        raise RuntimeError("MCP tool argument schema could not be made fail-closed")
    registered.parameters = parameters


def _truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace",
        default=os.getenv("REACT_AGENT_DEBUG_WORKSPACE", os.getcwd()),
        help="Trusted workspace root (default: current directory).",
    )
    parser.add_argument(
        "--python-executable",
        default=os.getenv("REACT_AGENT_DEBUG_PYTHON", sys.executable),
        help="Trusted Python executable used for debugpy and the debuggee.",
    )
    parser.add_argument(
        "--event-log",
        default=os.getenv("REACT_AGENT_DEBUG_EVENT_LOG"),
        help="Optional private hash-chained event log path.",
    )
    parser.add_argument(
        "--allow-execution",
        action="store_true",
        default=_truthy(os.getenv("REACT_AGENT_DEBUG_ALLOW_EXECUTION")),
        help="Allow MCP calls to launch or control local Python programs.",
    )
    return parser


def main() -> None:
    """Run the MCP v2 server over stdio."""

    arguments = _parser().parse_args()
    workspace = Path(arguments.workspace).expanduser().resolve(strict=True)
    journal = MCPDebugEventJournal(arguments.event_log) if arguments.event_log else None
    debugger = PythonRuntimeDebugger(
        workspace,
        python_executable=arguments.python_executable,
    )
    server = build_debug_mcp_server(
        debugger,
        event_journal=journal,
        allow_execution=bool(arguments.allow_execution),
    )
    server.run("stdio")


if __name__ == "__main__":
    main()
