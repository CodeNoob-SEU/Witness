"""Agent ToolRegistry adapters for the Python runtime debugger."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, cast

from .context import ToolContextPolicy
from .debug_evidence import verify_debug_observation
from .models import JsonValue
from .runtime_debugger import (
    TOOL_INTERFACE_VERSION,
    DebugBreakpoint,
    DebuggerError,
    PythonRuntimeDebugger,
)
from .tools import (
    DebugExposure,
    Tool,
    ToolExecutionContext,
    ToolLifecycle,
    ToolResumePolicy,
)


def _owner_id(debugger: PythonRuntimeDebugger, context: ToolExecutionContext) -> str:
    workspace = context.workspace_path
    if workspace is None:
        raise DebuggerError(
            "DEBUG_WORKSPACE_REQUIRED",
            "Runtime debugging requires a framework-managed workspace.",
        )
    try:
        resolved = Path(workspace).expanduser().resolve(strict=True)
    except OSError:
        raise DebuggerError(
            "DEBUG_WORKSPACE_INVALID",
            "The framework-managed debug workspace is unavailable.",
        ) from None
    if resolved != debugger.workspace_root:
        raise DebuggerError(
            "DEBUG_WORKSPACE_MISMATCH",
            "The tool execution workspace does not match the debugger workspace.",
        )
    return f"agent:{context.run_id}:{context.execution_id}"


def _debug_private_payload(result: Any) -> Mapping[str, JsonValue]:
    """Preserve one sealed observation before the model projection is bounded."""

    if not isinstance(result, Mapping):
        raise TypeError("debug tools must return a sealed observation object")
    observation = verify_debug_observation(cast(Mapping[str, object], result))
    return {"debug_observation": observation}


class _DebugToolLifecycle(ToolLifecycle):
    def __init__(self, debugger: PythonRuntimeDebugger) -> None:
        self.debugger = debugger

    async def finalize_execution(self, run_id: str, execution_id: str) -> None:
        await self.debugger.close_owner(f"agent:{run_id}:{execution_id}")

    async def close(self) -> None:
        await self.debugger.close()


def create_python_debug_tools(debugger: PythonRuntimeDebugger) -> tuple[Tool, ...]:
    """Create seven typed Agent adapters sharing one stateful debugger core."""

    lifecycle = _DebugToolLifecycle(debugger)

    async def launch(
        program: str,
        args: list[str],
        breakpoints: list[DebugBreakpoint],
        exception_policy: Literal["uncaught", "raised", "none"],
        stop_on_entry: bool,
        wait_timeout_s: float,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Launch one managed-workspace Python script under debugpy."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("launch", f"rejected-{context.call_id}", exc)
        return await debugger.launch(
            program,
            args,
            breakpoints,
            exception_policy,
            stop_on_entry,
            wait_timeout_s,
            _owner_id=owner,
        )

    async def set_breakpoints(
        debug_session_id: str,
        file: str,
        lines: list[int],
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Replace all breakpoints for one managed-workspace source file."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("set_breakpoints", debug_session_id, exc)
        return await debugger.set_breakpoints(
            debug_session_id, file, lines, _owner_id=owner
        )

    async def control(
        debug_session_id: str,
        action: Literal["continue", "next", "step_in", "step_out", "pause"],
        stop_id: int | None,
        wait_timeout_s: float,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Continue, step, or pause a fenced debug session."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("control", debug_session_id, exc)
        return await debugger.control(
            debug_session_id,
            action,
            stop_id,
            wait_timeout_s,
            _owner_id=owner,
        )

    async def stack(
        debug_session_id: str,
        stop_id: int,
        levels: int,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Read deterministically ranked frames from the current stop."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("stack", debug_session_id, exc)
        return await debugger.stack(
            debug_session_id, stop_id, levels, _owner_id=owner
        )

    async def select_frame(
        debug_session_id: str,
        stop_id: int,
        frame_index: int,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Select a ranked current-stop frame without exposing a DAP id."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("select_frame", debug_session_id, exc)
        return await debugger.select_frame(
            debug_session_id,
            stop_id,
            frame_index,
            _owner_id=owner,
        )

    async def variables(
        debug_session_id: str,
        stop_id: int,
        scope: Literal["locals", "arguments"],
        max_variables: int,
        max_value_chars: int,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Read bounded, redacted variables from the selected frame."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("variables", debug_session_id, exc)
        return await debugger.variables(
            debug_session_id,
            stop_id,
            scope,
            max_variables,
            max_value_chars,
            _owner_id=owner,
        )

    async def stop(
        debug_session_id: str,
        *,
        context: ToolExecutionContext,
    ) -> dict[str, object]:
        """Terminate and reap one fenced debug session."""

        try:
            owner = _owner_id(debugger, context)
        except DebuggerError as exc:
            return await debugger.reject("stop", debug_session_id, exc)
        return await debugger.stop(debug_session_id, _owner_id=owner)

    functions: dict[str, Callable[..., Any]] = {
        "launch": launch,
        "set_breakpoints": set_breakpoints,
        "control": control,
        "stack": stack,
        "select_frame": select_frame,
        "variables": variables,
        "stop": stop,
    }
    state_changing = {"launch", "set_breakpoints", "control", "stop"}
    order = (
        "launch",
        "set_breakpoints",
        "control",
        "stack",
        "select_frame",
        "variables",
        "stop",
    )
    result: list[Tool] = []
    for name in order:
        changes_state = name in state_changing
        result.append(
            Tool(
                functions[name],
                name=f"python_debug_{name}",
                timeout_s=75.0 if changes_state else 30.0,
                requires_approval=changes_state,
                idempotent=not changes_state,
                parallel_safe=False,
                allow_repeated=True,
                context_policy=ToolContextPolicy(),
                debug_exposure=DebugExposure.METADATA,
                resume_policy=(
                    ToolResumePolicy.NEVER_RETRY
                    if changes_state
                    else ToolResumePolicy.REQUIRE_OPERATOR
                ),
                lifecycle=lifecycle,
                private_result_encoder=_debug_private_payload,
                version=TOOL_INTERFACE_VERSION,
            )
        )
    return tuple(result)
