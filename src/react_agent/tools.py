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
from typing import Any, ParamSpec, Protocol, TypeVar, cast, get_type_hints, overload

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from .context import ToolContextPolicy
from .errors import ConfigurationError
from .models import JsonValue, ToolCall, ToolMessage, ToolSpec

P = ParamSpec("P")
R = TypeVar("R")
ToolFunction = Callable[..., Any]
PrivateResultEncoder = Callable[[Any], Mapping[str, JsonValue]]

_TOOL_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_MAX_PRIVATE_RESULT_BYTES = 512 * 1024
_MAX_TOOL_ERROR_CHARS = 2_000


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    run_id: str
    call_id: str
    tool_name: str
    arguments: Mapping[str, Any]


ApprovalHandler = Callable[[ApprovalRequest], bool | Awaitable[bool]]
SideEffectGuard = Callable[[], Awaitable[None] | None]


class ToolError(Exception):
    """An expected tool failure whose message is safe to show the model.

    Ordinary exceptions are reduced to an opaque ``TOOL_EXCEPTION`` so stack
    traces, paths, and secrets cannot leak into the transcript.  Tool authors
    raise ``ToolError`` only for messages they wrote for the model, such as
    "old_string was not found"; the message is bounded and never includes the
    traceback.
    """

    def __init__(self, message: str, *, code: str = "TOOL_ERROR", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class ToolLifecycle(Protocol):
    """Optional lifecycle seam for tools that own external resources."""

    async def finalize_execution(self, run_id: str, execution_id: str) -> None: ...

    async def close(self) -> None: ...


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
    execution_id: str
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


def _json_envelope(payload: Mapping[str, Any], max_chars: int) -> tuple[str, bool]:
    """Encode one tool envelope and report whether the encoder itself failed.

    The flag is returned structurally rather than recovered by searching the
    encoded text: a tool whose own successful data mentions the fallback code
    must not be reported to the model as a failed call.
    """

    try:
        encoded = json.dumps(
            payload,
            default=_json_default,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        # This fallback stays well below the 256-character floor AgentConfig
        # enforces for max_tool_output_chars, so it never needs truncation.
        return (
            json.dumps(
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
            ),
            True,
        )

    if len(encoded) <= max_chars:
        return encoded, False

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
            return truncated, False
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
    content, _ = _json_envelope({"ok": False, "error": error}, max_chars)
    return ToolMessage(
        call_id=call.id,
        name=call.name,
        content=content,
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
        "_private_result_encoder",
        "allow_repeated",
        "context_policy",
        "debug_exposure",
        "description",
        "idempotent",
        "lifecycle",
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
        context_policy: ToolContextPolicy | None = None,
        debug_exposure: DebugExposure = DebugExposure.METADATA,
        resume_policy: ToolResumePolicy | None = None,
        lifecycle: ToolLifecycle | None = None,
        private_result_encoder: PrivateResultEncoder | None = None,
        version: str = "1",
    ) -> None:
        self.name = name or fn.__name__
        self.description = description or inspect.getdoc(fn) or ""
        self.timeout_s = timeout_s
        self.requires_approval = requires_approval
        self.idempotent = idempotent
        self.parallel_safe = parallel_safe
        self.allow_repeated = allow_repeated
        self.context_policy = context_policy or ToolContextPolicy()
        self.debug_exposure = debug_exposure
        self.resume_policy = resume_policy or (
            ToolResumePolicy.IDEMPOTENT_RETRY if idempotent else ToolResumePolicy.REQUIRE_OPERATOR
        )
        self.lifecycle = lifecycle
        self._private_result_encoder = private_result_encoder
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
        if not isinstance(self.context_policy, ToolContextPolicy):
            raise ConfigurationError("tool context_policy must be a ToolContextPolicy value")
        if not isinstance(self.resume_policy, ToolResumePolicy):
            raise ConfigurationError("tool resume_policy must be a ToolResumePolicy value")
        if private_result_encoder is not None and not callable(private_result_encoder):
            raise ConfigurationError("private_result_encoder must be callable or None")
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

    @property
    def captures_private_result(self) -> bool:
        """Whether execution emits a journal-only payload before projection."""

        return self._private_result_encoder is not None

    def encode_private_result(self, result: Any) -> Mapping[str, JsonValue]:
        """Return normalized journal-only evidence for one raw result.

        The encoder runs before the ordinary result is bounded for the model.
        Round-tripping through JSON prevents mutable or non-JSON objects from
        entering the durable event contract.
        """

        if self._private_result_encoder is None:
            return MappingProxyType({})
        raw = self._private_result_encoder(result)
        if not isinstance(raw, Mapping):
            raise TypeError("private_result_encoder must return a mapping")
        encoded = json.dumps(
            dict(raw),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        if len(encoded.encode("utf-8")) > _MAX_PRIVATE_RESULT_BYTES:
            raise ValueError("private tool evidence exceeded its hard byte budget")
        normalized = json.loads(encoded)
        if not isinstance(normalized, dict):  # pragma: no cover - guarded above
            raise TypeError("private_result_encoder must return a JSON object")
        return MappingProxyType(cast(dict[str, JsonValue], normalized))


class ToolRegistry:
    """Registry is the only seam through which model-requested code can execute."""

    def __init__(self, tools: Iterable[Tool] = ()) -> None:
        self._tools: dict[str, Tool] = {}
        self._lifecycles: dict[int, ToolLifecycle] = {}
        for registered_tool in tools:
            self.register(registered_tool)

    def register(self, registered_tool: Tool) -> None:
        if registered_tool.name in self._tools:
            raise ConfigurationError(f"Duplicate tool name: {registered_tool.name!r}")
        self._tools[registered_tool.name] = registered_tool
        if registered_tool.lifecycle is not None:
            self._lifecycles[id(registered_tool.lifecycle)] = registered_tool.lifecycle

    async def finalize_execution(self, run_id: str, execution_id: str) -> None:
        """Release resources owned by one completed or cancelled execution."""

        for lifecycle in tuple(self._lifecycles.values()):
            await lifecycle.finalize_execution(run_id, execution_id)

    async def close(self) -> None:
        """Release all resource-owning tools. Safe lifecycle adapters are idempotent."""

        for lifecycle in tuple(self._lifecycles.values()):
            await lifecycle.close()

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
        execution_id: str | None = None,
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
                execution_id=execution_id or run_id,
                call_id=call.id,
                call_key=stable_call_key,
                operation_id=f"tool:{stable_call_key}",
                attempt=attempt,
                idempotency_key=f"{run_id}:{stable_call_key}",
                workspace_path=workspace_path,
            )
            result = await registered_tool.invoke(arguments, context=execution_context)
            private_payload = registered_tool.encode_private_result(result)
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
        except ToolError as exc:
            return _error_message(
                call,
                exc.code if _TOOL_NAME.fullmatch(exc.code) else "TOOL_ERROR",
                str(exc)[:_MAX_TOOL_ERROR_CHARS] or "The tool reported a failure.",
                retryable=exc.retryable,
                executed=True,
                duration_ms=(time.monotonic() - started) * 1000,
                max_chars=max_output_chars,
            )
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

        content, serialized_error = _json_envelope(
            {"ok": True, "data": result, "meta": {"truncated": False}},
            max_output_chars,
        )
        return ToolMessage(
            call_id=call.id,
            name=call.name,
            content=content,
            is_error=serialized_error,
            executed=True,
            duration_ms=(time.monotonic() - started) * 1000,
            private_payload=private_payload,
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
    context_policy: ToolContextPolicy | None = None,
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
    context_policy: ToolContextPolicy | None = None,
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
    context_policy: ToolContextPolicy | None = None,
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
            context_policy=context_policy,
            debug_exposure=debug_exposure,
            resume_policy=resume_policy,
            version=version,
        )

    return wrap(fn) if fn is not None else wrap
