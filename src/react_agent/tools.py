"""Allowlisted, schema-validated tool registration and execution."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum, StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, ParamSpec, TypeVar, cast, get_type_hints, overload

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from .errors import ConfigurationError
from .models import ToolCall, ToolMessage, ToolSpec

P = ParamSpec("P")
R = TypeVar("R")
ToolFunction = Callable[..., Any]

_TOOL_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    run_id: str
    call_id: str
    tool_name: str
    arguments: Mapping[str, Any]


ApprovalHandler = Callable[[ApprovalRequest], bool | Awaitable[bool]]
SideEffectGuard = Callable[[], Awaitable[None] | None]


class DebugExposure(StrEnum):
    """Controls what an opt-in ephemeral debug stream may reveal for a tool.

    Safe lifecycle events never include arguments or results.  This policy only
    applies to the separate rich debugging stream requested by the caller.
    """

    METADATA = "metadata"
    FULL = "full"


class ToolResumePolicy(StrEnum):
    """How an interrupted tool execution may be handled after a restart."""

    IDEMPOTENT_RETRY = "idempotent_retry"
    REQUIRE_OPERATOR = "require_operator"
    NEVER_RETRY = "never_retry"


@dataclass(frozen=True, slots=True)
class ToolExecutionContext:
    """Framework-owned execution metadata, excluded from the model schema."""

    run_id: str
    call_id: str
    call_key: str
    operation_id: str
    attempt: int
    idempotency_key: str
    workspace_path: Path | None = None


def _strict_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Normalize Pydantic JSON Schema to OpenAI strict function-tool requirements."""

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            node.pop("default", None)
            node.pop("title", None)
            if node.get("type") == "object" or "properties" in node:
                properties = node.setdefault("properties", {})
                node["additionalProperties"] = False
                node["required"] = list(properties)
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(schema)
    return schema


def _json_default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Unsupported tool result type: {type(value).__name__}")


def _json_envelope(payload: Mapping[str, Any], max_chars: int) -> str:
    try:
        encoded = json.dumps(
            payload,
            default=_json_default,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        encoded = json.dumps(
            {
                "ok": False,
                "error": {
                    "code": "OUTPUT_SERIALIZATION",
                    "message": "Tool result could not be serialized safely.",
                    "retryable": False,
                    "type": type(exc).__name__,
                },
            },
            separators=(",", ":"),
        )

    if len(encoded) <= max_chars:
        return encoded

    preview_len = max(0, min(len(encoded), max_chars // 2))
    while True:
        if payload.get("ok") is True:
            shortened: dict[str, Any] = {
                "ok": True,
                "data": {"preview": encoded[:preview_len]},
                "meta": {"truncated": True, "original_chars": len(encoded)},
            }
        else:
            original_error = payload.get("error")
            safe_error = original_error if isinstance(original_error, Mapping) else {}
            shortened = {
                "ok": False,
                "error": {
                    "code": safe_error.get("code", "TOOL_ERROR"),
                    "message": safe_error.get("message", "Tool execution failed."),
                    "retryable": bool(safe_error.get("retryable", False)),
                },
                "meta": {"truncated": True, "original_chars": len(encoded)},
            }
        truncated = json.dumps(shortened, ensure_ascii=False, separators=(",", ":"))
        if len(truncated) <= max_chars or preview_len == 0:
            return truncated
        preview_len //= 2


def _error_message(
    call: ToolCall,
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: Any = None,
    duration_ms: float = 0.0,
    executed: bool = False,
    max_chars: int,
) -> ToolMessage:
    error: dict[str, Any] = {"code": code, "message": message, "retryable": retryable}
    if details is not None:
        error["details"] = details
    return ToolMessage(
        call_id=call.id,
        name=call.name,
        content=_json_envelope({"ok": False, "error": error}, max_chars),
        is_error=True,
        executed=executed,
        duration_ms=duration_ms,
    )


class Tool:
    """A callable hidden behind a validated, allowlisted interface."""

    __slots__ = (
        "_context_parameter",
        "_fn",
        "_input_model",
        "allow_repeated",
        "debug_exposure",
        "description",
        "idempotent",
        "name",
        "parallel_safe",
        "requires_approval",
        "resume_policy",
        "timeout_s",
        "version",
    )

    def __init__(
        self,
        fn: ToolFunction,
        *,
        name: str | None = None,
        description: str | None = None,
        timeout_s: float | None = 30.0,
        requires_approval: bool = False,
        idempotent: bool = False,
        parallel_safe: bool = False,
        allow_repeated: bool = False,
        debug_exposure: DebugExposure = DebugExposure.METADATA,
        resume_policy: ToolResumePolicy | None = None,
        version: str = "1",
    ) -> None:
        self.name = name or fn.__name__
        self.description = description or inspect.getdoc(fn) or ""
        self.timeout_s = timeout_s
        self.requires_approval = requires_approval
        self.idempotent = idempotent
        self.parallel_safe = parallel_safe
        self.allow_repeated = allow_repeated
        self.debug_exposure = debug_exposure
        self.resume_policy = resume_policy or (
            ToolResumePolicy.IDEMPOTENT_RETRY if idempotent else ToolResumePolicy.REQUIRE_OPERATOR
        )
        self.version = version
        self._fn = fn
        self._context_parameter: str | None = None

        if not _TOOL_NAME.fullmatch(self.name):
            raise ConfigurationError(
                f"Invalid tool name {self.name!r}; use 1-64 letters, digits, '_' or '-'."
            )
        if not self.description:
            raise ConfigurationError(f"Tool {self.name!r} needs a useful description or docstring.")
        if timeout_s is not None and timeout_s <= 0:
            raise ConfigurationError("tool timeout_s must be positive or None")
        if not isinstance(debug_exposure, DebugExposure):
            raise ConfigurationError("tool debug_exposure must be a DebugExposure value")
        if not isinstance(self.resume_policy, ToolResumePolicy):
            raise ConfigurationError("tool resume_policy must be a ToolResumePolicy value")
        if self.resume_policy is ToolResumePolicy.IDEMPOTENT_RETRY and not idempotent:
            raise ConfigurationError("idempotent_retry requires idempotent=True")
        if not version.strip() or len(version) > 128:
            raise ConfigurationError("tool version must be 1-128 non-blank characters")

        signature = inspect.signature(fn)
        try:
            type_hints = get_type_hints(fn, include_extras=True)
        except (NameError, TypeError) as exc:
            raise ConfigurationError(
                f"Tool {self.name!r} has an annotation that could not be resolved."
            ) from exc
        fields: dict[str, tuple[Any, Any]] = {}
        for parameter in signature.parameters.values():
            if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
                raise ConfigurationError(f"Tool {self.name!r} cannot use *args or **kwargs.")
            if parameter.kind is parameter.POSITIONAL_ONLY:
                raise ConfigurationError(
                    f"Tool {self.name!r} cannot use positional-only arguments."
                )
            annotation = type_hints.get(parameter.name, parameter.annotation)
            if annotation is ToolExecutionContext:
                if self._context_parameter is not None:
                    raise ConfigurationError(
                        f"Tool {self.name!r} can accept only one ToolExecutionContext."
                    )
                if parameter.kind is not parameter.KEYWORD_ONLY:
                    raise ConfigurationError(
                        "ToolExecutionContext must be declared as a keyword-only parameter."
                    )
                self._context_parameter = parameter.name
                continue
            if annotation is inspect.Parameter.empty:
                raise ConfigurationError(
                    f"Tool {self.name!r} parameter {parameter.name!r} needs a type annotation."
                )
            default = parameter.default if parameter.default is not inspect.Parameter.empty else ...
            fields[parameter.name] = (annotation, default)

        self._input_model = create_model(
            f"{self.name.title().replace('_', '')}Input",
            __config__=ConfigDict(extra="forbid"),
            **cast(dict[str, Any], fields),
        )

    @property
    def spec(self) -> ToolSpec:
        schema = _strict_json_schema(self._input_model.model_json_schema())
        return ToolSpec(
            name=self.name,
            description=self.description,
            parameters=schema,
            strict=True,
        )

    def validate(self, arguments: str) -> dict[str, Any]:
        try:
            raw = json.loads(arguments)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Arguments are not valid JSON: {exc.msg}") from exc
        if not isinstance(raw, dict):
            raise ValueError("Arguments must be a JSON object.")
        validated = self._input_model.model_validate(raw)
        return {
            field_name: getattr(validated, field_name)
            for field_name in self._input_model.model_fields
        }

    async def invoke(
        self,
        arguments: Mapping[str, Any],
        *,
        context: ToolExecutionContext | None = None,
    ) -> Any:
        invocation_arguments = dict(arguments)
        if self._context_parameter is not None:
            if context is None:
                raise RuntimeError("Tool execution context was not provided.")
            invocation_arguments[self._context_parameter] = context
        if inspect.iscoroutinefunction(self._fn):
            invocation = cast(Callable[..., Awaitable[Any]], self._fn)(**invocation_arguments)
        else:
            invocation = asyncio.to_thread(self._fn, **invocation_arguments)
        if self.timeout_s is None:
            return await invocation
        async with asyncio.timeout(self.timeout_s):
            return await invocation


class ToolRegistry:
    """Registry is the only seam through which model-requested code can execute."""

    def __init__(self, tools: Iterable[Tool] = ()) -> None:
        self._tools: dict[str, Tool] = {}
        for registered_tool in tools:
            self.register(registered_tool)

    def register(self, registered_tool: Tool) -> None:
        if registered_tool.name in self._tools:
            raise ConfigurationError(f"Duplicate tool name: {registered_tool.name!r}")
        self._tools[registered_tool.name] = registered_tool

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    @property
    def specs(self) -> tuple[ToolSpec, ...]:
        return tuple(registered_tool.spec for registered_tool in self._tools.values())

    @property
    def tools(self) -> tuple[Tool, ...]:
        """Registered tools in deterministic model-exposure order."""

        return tuple(self._tools.values())

    async def execute(
        self,
        call: ToolCall,
        *,
        run_id: str,
        approval_handler: ApprovalHandler | None,
        max_output_chars: int,
        call_key: str | None = None,
        attempt: int = 1,
        before_invoke: SideEffectGuard | None = None,
        workspace_path: Path | None = None,
    ) -> ToolMessage:
        started = time.monotonic()
        registered_tool = self.get(call.name)
        if registered_tool is None:
            return _error_message(
                call,
                "UNKNOWN_TOOL",
                f"Tool {call.name!r} is not available.",
                max_chars=max_output_chars,
            )

        try:
            arguments = registered_tool.validate(call.arguments)
        except (ValueError, ValidationError) as exc:
            if isinstance(exc, ValidationError):
                details: Any = exc.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                )
                message = "Tool arguments did not match the declared schema."
            else:
                details = str(exc)
                message = "Tool arguments were not valid."
            return _error_message(
                call,
                "INVALID_ARGUMENTS",
                message,
                details=details,
                duration_ms=(time.monotonic() - started) * 1000,
                max_chars=max_output_chars,
            )

        if registered_tool.requires_approval:
            if approval_handler is None:
                approved = False
            else:
                try:
                    approval_arguments = MappingProxyType(copy.deepcopy(arguments))
                    request = ApprovalRequest(
                        run_id=run_id,
                        call_id=call.id,
                        tool_name=call.name,
                        arguments=approval_arguments,
                    )
                    if inspect.iscoroutinefunction(approval_handler):
                        decision = approval_handler(request)
                    else:
                        sync_handler = cast(Callable[[ApprovalRequest], Any], approval_handler)
                        decision = await asyncio.to_thread(sync_handler, request)
                    approved = await decision if inspect.isawaitable(decision) else decision
                except asyncio.CancelledError:
                    raise
                except Exception:
                    return _error_message(
                        call,
                        "APPROVAL_ERROR",
                        "The approval policy failed closed.",
                        duration_ms=(time.monotonic() - started) * 1000,
                        max_chars=max_output_chars,
                    )
            if not approved:
                return _error_message(
                    call,
                    "TOOL_DENIED",
                    "This tool requires approval and was not approved.",
                    duration_ms=(time.monotonic() - started) * 1000,
                    max_chars=max_output_chars,
                )

        stable_call_key = call_key or call.id
        if before_invoke is not None:
            guard_outcome = before_invoke()
            if inspect.isawaitable(guard_outcome):
                await guard_outcome
        try:
            execution_context = ToolExecutionContext(
                run_id=run_id,
                call_id=call.id,
                call_key=stable_call_key,
                operation_id=f"tool:{stable_call_key}",
                attempt=attempt,
                idempotency_key=f"{run_id}:{stable_call_key}",
                workspace_path=workspace_path,
            )
            result = await registered_tool.invoke(arguments, context=execution_context)
        except TimeoutError:
            return _error_message(
                call,
                "TOOL_TIMEOUT",
                "The tool exceeded its execution timeout.",
                retryable=registered_tool.idempotent,
                executed=True,
                duration_ms=(time.monotonic() - started) * 1000,
                max_chars=max_output_chars,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _error_message(
                call,
                "TOOL_EXCEPTION",
                "The tool failed without returning a result.",
                retryable=False,
                details={"type": type(exc).__name__},
                executed=True,
                duration_ms=(time.monotonic() - started) * 1000,
                max_chars=max_output_chars,
            )

        content = _json_envelope(
            {"ok": True, "data": result, "meta": {"truncated": False}},
            max_output_chars,
        )
        serialized_error = '"code":"OUTPUT_SERIALIZATION"' in content
        return ToolMessage(
            call_id=call.id,
            name=call.name,
            content=content,
            is_error=serialized_error,
            executed=True,
            duration_ms=(time.monotonic() - started) * 1000,
        )


@overload
def tool(
    fn: Callable[P, R],
    *,
    name: str | None = None,
    description: str | None = None,
    timeout_s: float | None = 30.0,
    requires_approval: bool = False,
    idempotent: bool = False,
    parallel_safe: bool = False,
    allow_repeated: bool = False,
    debug_exposure: DebugExposure = DebugExposure.METADATA,
    resume_policy: ToolResumePolicy | None = None,
    version: str = "1",
) -> Tool: ...


@overload
def tool(
    fn: None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    timeout_s: float | None = 30.0,
    requires_approval: bool = False,
    idempotent: bool = False,
    parallel_safe: bool = False,
    allow_repeated: bool = False,
    debug_exposure: DebugExposure = DebugExposure.METADATA,
    resume_policy: ToolResumePolicy | None = None,
    version: str = "1",
) -> Callable[[Callable[P, R]], Tool]: ...


def tool(
    fn: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    timeout_s: float | None = 30.0,
    requires_approval: bool = False,
    idempotent: bool = False,
    parallel_safe: bool = False,
    allow_repeated: bool = False,
    debug_exposure: DebugExposure = DebugExposure.METADATA,
    resume_policy: ToolResumePolicy | None = None,
    version: str = "1",
) -> Tool | Callable[[Callable[P, R]], Tool]:
    """Turn a typed callable into a schema-validated Tool."""

    def wrap(function: Callable[P, R]) -> Tool:
        return Tool(
            function,
            name=name,
            description=description,
            timeout_s=timeout_s,
            requires_approval=requires_approval,
            idempotent=idempotent,
            parallel_safe=parallel_safe,
            allow_repeated=allow_repeated,
            debug_exposure=debug_exposure,
            resume_policy=resume_policy,
            version=version,
        )

    return wrap(fn) if fn is not None else wrap
