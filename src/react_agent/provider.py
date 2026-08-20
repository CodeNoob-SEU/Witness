"""OpenAI and OpenAI-compatible model adapter."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

from openai import AsyncOpenAI, Omit, OpenAIError, omit
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionStreamOptionsParam,
    ChatCompletionToolParam,
)
from openai.types.responses import FunctionToolParam, ResponseIncludable, ResponseInputParam

from .errors import ConfigurationError, ModelInvocationError
from .models import (
    AssistantMessage,
    ModelOutcome,
    ModelRequest,
    ModelResponse,
    ModelStreamEvent,
    ModelStreamEventKind,
    ModelStreamSink,
    ToolCall,
    ToolMessage,
    ToolSpec,
    Usage,
    UserMessage,
)

ApiMode = Literal["responses", "chat_completions"]


class Model(Protocol):
    async def complete(self, request: ModelRequest) -> ModelResponse:
        """Return one provider-neutral assistant turn."""
        ...


@runtime_checkable
class StreamingModel(Model, Protocol):
    async def complete_stream(self, request: ModelRequest, sink: ModelStreamSink) -> ModelResponse:
        """Stream sanitized deltas and return the accumulated final response."""
        ...


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """Disable fields that an incomplete OpenAI-compatible endpoint rejects."""

    strict_tools: bool = True
    parallel_tool_calls: bool = True
    store_parameter: bool = True
    encrypted_reasoning_items: bool = True
    chat_stream_usage: bool = True


def _usage_from_responses(raw: Any) -> Usage:
    if raw is None:
        return Usage()
    input_details = getattr(raw, "input_tokens_details", None)
    output_details = getattr(raw, "output_tokens_details", None)
    input_tokens = int(getattr(raw, "input_tokens", 0) or 0)
    output_tokens = int(getattr(raw, "output_tokens", 0) or 0)
    total_tokens = int(getattr(raw, "total_tokens", 0) or 0)
    return Usage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        cached_input_tokens=(
            int(getattr(input_details, "cached_tokens", 0) or 0)
            if input_details is not None
            else None
        ),
        reasoning_output_tokens=(
            int(getattr(output_details, "reasoning_tokens", 0) or 0)
            if output_details is not None
            else None
        ),
        billable_tokens=total_tokens,
    )


def _usage_from_chat(raw: Any) -> Usage:
    if raw is None:
        return Usage()
    input_details = getattr(raw, "prompt_tokens_details", None)
    output_details = getattr(raw, "completion_tokens_details", None)
    input_tokens = int(getattr(raw, "prompt_tokens", 0) or 0)
    output_tokens = int(getattr(raw, "completion_tokens", 0) or 0)
    total_tokens = int(getattr(raw, "total_tokens", 0) or 0)
    return Usage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        cached_input_tokens=(
            int(getattr(input_details, "cached_tokens", 0) or 0)
            if input_details is not None
            else None
        ),
        reasoning_output_tokens=(
            int(getattr(output_details, "reasoning_tokens", 0) or 0)
            if output_details is not None
            else None
        ),
        billable_tokens=total_tokens,
    )


def _responses_refusal(output: Any) -> str | None:
    refusals: list[str] = []
    for item in output:
        for part in getattr(item, "content", ()) or ():
            if getattr(part, "type", None) == "refusal":
                text = getattr(part, "refusal", None)
                if text:
                    refusals.append(str(text))
    return "\n".join(refusals) or None


def _responses_tool(spec: ToolSpec, *, strict_tools: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "type": "function",
        "name": spec.name,
        "description": spec.description,
        "parameters": dict(spec.parameters),
    }
    if strict_tools:
        result["strict"] = spec.strict
    return result


def _chat_tool(spec: ToolSpec, *, strict_tools: bool) -> dict[str, Any]:
    function: dict[str, Any] = {
        "name": spec.name,
        "description": spec.description,
        "parameters": dict(spec.parameters),
    }
    if strict_tools:
        function["strict"] = spec.strict
    return {"type": "function", "function": function}


def _parse_responses_response(response: Any) -> ModelResponse:
    """Normalize one fully accumulated Responses API response."""

    response_status = getattr(response, "status", None)
    response_error = getattr(response, "error", None)
    if response_error is not None or response_status in {"failed", "cancelled"}:
        error_code = getattr(response_error, "code", None)
        suffix = f" (code={error_code})" if error_code else ""
        raise ModelInvocationError(f"Responses request did not complete{suffix}.")

    raw_items = tuple(
        cast(Mapping[str, Any], item.model_dump(mode="json", exclude_none=True))
        for item in response.output
    )
    calls: list[ToolCall] = []
    has_incomplete_call = False
    for item in response.output:
        if getattr(item, "type", None) == "function_call":
            item_status = getattr(item, "status", None)
            if item_status not in (None, "completed"):
                has_incomplete_call = True
            calls.append(
                ToolCall(
                    id=str(getattr(item, "call_id", "") or ""),
                    name=str(getattr(item, "name", "") or ""),
                    arguments=str(getattr(item, "arguments", "") or ""),
                )
            )
    refusal = _responses_refusal(response.output)
    incomplete = getattr(response, "incomplete_details", None)
    finish_reason = getattr(incomplete, "reason", None) or response_status
    if refusal is not None:
        outcome = ModelOutcome.REFUSED
        diagnostic = "refusal"
    elif response_status == "incomplete" or has_incomplete_call:
        outcome = ModelOutcome.INCOMPLETE
        diagnostic = str(getattr(incomplete, "reason", None) or "incomplete_output")
    elif response_status not in (None, "completed"):
        outcome = ModelOutcome.INCOMPLETE
        diagnostic = f"unexpected_response_status:{response_status}"
    else:
        outcome = ModelOutcome.COMPLETED
        diagnostic = None
    return ModelResponse(
        message=AssistantMessage(
            content=getattr(response, "output_text", None) or refusal,
            tool_calls=tuple(calls),
            raw_items=raw_items,
        ),
        usage=_usage_from_responses(getattr(response, "usage", None)),
        request_id=getattr(response, "_request_id", None),
        response_model=(
            str(getattr(response, "model", "") or "") or None
        ),
        finish_reason=str(finish_reason) if finish_reason is not None else None,
        outcome=outcome,
        diagnostic=diagnostic,
    )


def _parse_chat_response(response: Any) -> ModelResponse:
    """Normalize one fully accumulated Chat Completions response."""

    if not response.choices:
        raise ModelInvocationError("Chat Completions returned no choices.")
    choice = response.choices[0]
    raw_message = choice.message
    refusal = getattr(raw_message, "refusal", None) or None
    finish_reason = choice.finish_reason
    if refusal is not None or finish_reason == "content_filter":
        outcome = ModelOutcome.REFUSED
        diagnostic = "content_filter" if finish_reason == "content_filter" else "refusal"
    elif finish_reason == "length":
        outcome = ModelOutcome.INCOMPLETE
        diagnostic = "length"
    else:
        outcome = ModelOutcome.COMPLETED
        diagnostic = None
    calls = tuple(
        ToolCall(
            id=raw_call.id,
            name=raw_call.function.name,
            arguments=raw_call.function.arguments,
        )
        for raw_call in (raw_message.tool_calls or ())
        if raw_call.type == "function"
    )
    return ModelResponse(
        message=AssistantMessage(content=raw_message.content or refusal, tool_calls=calls),
        usage=_usage_from_chat(response.usage),
        request_id=getattr(response, "_request_id", None),
        response_model=(str(getattr(response, "model", "") or "") or None),
        finish_reason=finish_reason,
        outcome=outcome,
        diagnostic=diagnostic,
    )


def _sanitized_invocation_error(exc: Exception) -> ModelInvocationError:
    if isinstance(exc, OpenAIError):
        request_id = getattr(exc, "request_id", None)
        status = getattr(exc, "status_code", None)
        suffix = f" (status={status})" if status is not None else ""
        return ModelInvocationError(
            f"Model request failed{suffix}: {type(exc).__name__}",
            request_id=request_id,
        )
    if isinstance(exc, (AttributeError, KeyError, TypeError, ValueError)):
        return ModelInvocationError(f"Provider returned an invalid response: {type(exc).__name__}.")
    return ModelInvocationError(f"Model request failed: {type(exc).__name__}.")


async def _emit_stream_event(sink: ModelStreamSink, event: ModelStreamEvent) -> None:
    outcome = sink(event)
    if inspect.isawaitable(outcome):
        await outcome


class OpenAIModel:
    """One small interface over Responses and Chat Completions transports.

    Responses is the default for official OpenAI. Choose ``chat_completions`` for
    compatible providers that do not implement ``/v1/responses``.
    """

    def __init__(
        self,
        model: str,
        *,
        api_mode: ApiMode = "responses",
        api_key: str | None = None,
        base_url: str | None = None,
        allow_insecure_http: bool = False,
        timeout: float = 60.0,
        max_retries: int = 2,
        max_output_tokens: int | None = None,
        temperature: float | None = None,
        store: bool = False,
        capabilities: ProviderCapabilities | None = None,
        extra_body: Mapping[str, Any] | None = None,
        client: AsyncOpenAI | None = None,
    ) -> None:
        if not model.strip():
            raise ConfigurationError("model must not be empty")
        if api_mode not in ("responses", "chat_completions"):
            raise ConfigurationError(f"unsupported api_mode: {api_mode!r}")
        if timeout <= 0:
            raise ConfigurationError("timeout must be positive")
        if max_retries < 0:
            raise ConfigurationError("max_retries must be non-negative")
        if base_url is not None:
            parsed_url = urlsplit(base_url)
            loopback = parsed_url.hostname in {"localhost", "127.0.0.1", "::1"}
            if parsed_url.username is not None or parsed_url.password is not None:
                raise ConfigurationError("base_url must not contain embedded credentials")
            if parsed_url.scheme != "https" and not (
                parsed_url.scheme == "http" and (loopback or allow_insecure_http)
            ):
                raise ConfigurationError(
                    "base_url must use HTTPS; set allow_insecure_http=True only "
                    "for a trusted network"
                )

        self.model = model
        self.api_mode = api_mode
        self.max_output_tokens = max_output_tokens
        self.temperature = temperature
        self.store = store
        self.capabilities = capabilities or ProviderCapabilities()
        self.extra_body = dict(extra_body or {})
        self._owns_client = client is None
        self._client = client or AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
        )

    def __repr__(self) -> str:
        return f"OpenAIModel(model={self.model!r}, api_mode={self.api_mode!r})"

    async def __aenter__(self) -> OpenAIModel:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.close()

    async def complete(self, request: ModelRequest) -> ModelResponse:
        try:
            if self.api_mode == "responses":
                return await self._complete_responses(request)
            return await self._complete_chat(request)
        except asyncio.CancelledError:
            raise
        except ModelInvocationError:
            raise
        except Exception as exc:
            raise _sanitized_invocation_error(exc) from exc

    async def complete_stream(self, request: ModelRequest, sink: ModelStreamSink) -> ModelResponse:
        try:
            if self.api_mode == "responses":
                return await self._complete_responses_stream(request, sink)
            return await self._complete_chat_stream(request, sink)
        except asyncio.CancelledError:
            raise
        except ModelInvocationError:
            raise
        except Exception as exc:
            raise _sanitized_invocation_error(exc) from exc

    def _responses_input(self, request: ModelRequest) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for item in request.transcript:
            if isinstance(item, UserMessage):
                result.append({"role": "user", "content": item.content})
            elif isinstance(item, ToolMessage):
                result.append(
                    {
                        "type": "function_call_output",
                        "call_id": item.call_id,
                        "output": item.content,
                    }
                )
            elif item.raw_items:
                result.extend(dict(raw_item) for raw_item in item.raw_items)
            else:
                if item.content:
                    result.append({"role": "assistant", "content": item.content})
                result.extend(
                    {
                        "type": "function_call",
                        "call_id": tool_call.id,
                        "name": tool_call.name,
                        "arguments": tool_call.arguments,
                    }
                    for tool_call in item.tool_calls
                )
        return result

    async def _complete_responses(self, request: ModelRequest) -> ModelResponse:
        response_tools: list[FunctionToolParam] | Omit = omit
        parallel_tool_calls: bool | Omit = omit
        if request.tools:
            response_tools = [
                cast(
                    FunctionToolParam,
                    _responses_tool(spec, strict_tools=self.capabilities.strict_tools),
                )
                for spec in request.tools
            ]
            if self.capabilities.parallel_tool_calls:
                parallel_tool_calls = request.parallel_tool_calls
        include: list[ResponseIncludable] | Omit = omit
        if not self.store and self.capabilities.encrypted_reasoning_items:
            include = ["reasoning.encrypted_content"]

        response = await self._client.responses.create(
            model=self.model,
            input=cast(ResponseInputParam, self._responses_input(request)),
            instructions=request.instructions,
            store=self.store if self.capabilities.store_parameter else omit,
            include=include,
            tools=response_tools,
            parallel_tool_calls=parallel_tool_calls,
            max_output_tokens=(
                self.max_output_tokens if self.max_output_tokens is not None else omit
            ),
            temperature=self.temperature if self.temperature is not None else omit,
            extra_body=self.extra_body or None,
        )
        return _parse_responses_response(response)

    async def _complete_responses_stream(
        self, request: ModelRequest, sink: ModelStreamSink
    ) -> ModelResponse:
        response_tools: list[FunctionToolParam] | Omit = omit
        parallel_tool_calls: bool | Omit = omit
        if request.tools:
            response_tools = [
                cast(
                    FunctionToolParam,
                    _responses_tool(spec, strict_tools=self.capabilities.strict_tools),
                )
                for spec in request.tools
            ]
            if self.capabilities.parallel_tool_calls:
                parallel_tool_calls = request.parallel_tool_calls
        include: list[ResponseIncludable] | Omit = omit
        if not self.store and self.capabilities.encrypted_reasoning_items:
            include = ["reasoning.encrypted_content"]

        # item id -> (provider-neutral tool index, call id, function name)
        tool_metadata: dict[str, tuple[int, str | None, str | None]] = {}
        async with self._client.responses.stream(
            model=self.model,
            input=cast(ResponseInputParam, self._responses_input(request)),
            instructions=request.instructions,
            store=self.store if self.capabilities.store_parameter else omit,
            include=include,
            tools=response_tools,
            parallel_tool_calls=parallel_tool_calls,
            max_output_tokens=(
                self.max_output_tokens if self.max_output_tokens is not None else omit
            ),
            temperature=self.temperature if self.temperature is not None else omit,
            extra_body=self.extra_body or None,
        ) as stream:
            async for event in stream:
                event_type = getattr(event, "type", None)
                if event_type == "response.output_item.added":
                    item = getattr(event, "item", None)
                    if getattr(item, "type", None) != "function_call":
                        continue
                    item_id = str(getattr(item, "id", "") or "")
                    if item_id and item_id not in tool_metadata:
                        tool_metadata[item_id] = (
                            len(tool_metadata),
                            str(getattr(item, "call_id", "") or "") or None,
                            str(getattr(item, "name", "") or "") or None,
                        )
                elif event_type == "response.output_text.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        await _emit_stream_event(
                            sink,
                            ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, delta),
                        )
                elif event_type == "response.refusal.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        await _emit_stream_event(
                            sink,
                            ModelStreamEvent(ModelStreamEventKind.REFUSAL_DELTA, delta),
                        )
                elif event_type == "response.function_call_arguments.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if not delta:
                        continue
                    item_id = str(getattr(event, "item_id", "") or "")
                    metadata = tool_metadata.get(item_id)
                    if metadata is None:
                        metadata = (len(tool_metadata), None, None)
                        tool_metadata[item_id] = metadata
                    await _emit_stream_event(
                        sink,
                        ModelStreamEvent(
                            ModelStreamEventKind.TOOL_CALL_DELTA,
                            delta,
                            tool_index=metadata[0],
                            tool_call_id=metadata[1],
                            tool_name=metadata[2],
                        ),
                    )
            response = await stream.get_final_response()
        return _parse_responses_response(response)

    def _chat_messages(self, request: ModelRequest) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = [{"role": "system", "content": request.instructions}]
        for item in request.transcript:
            if isinstance(item, UserMessage):
                messages.append({"role": "user", "content": item.content})
            elif isinstance(item, ToolMessage):
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": item.call_id,
                        "content": item.content,
                    }
                )
            else:
                assistant: dict[str, Any] = {"role": "assistant", "content": item.content}
                if item.tool_calls:
                    assistant["tool_calls"] = [
                        {
                            "id": tool_call.id,
                            "type": "function",
                            "function": {
                                "name": tool_call.name,
                                "arguments": tool_call.arguments,
                            },
                        }
                        for tool_call in item.tool_calls
                    ]
                messages.append(assistant)
        return messages

    async def _complete_chat(self, request: ModelRequest) -> ModelResponse:
        chat_tools: list[ChatCompletionToolParam] | Omit = omit
        parallel_tool_calls: bool | Omit = omit
        if request.tools:
            chat_tools = [
                cast(
                    ChatCompletionToolParam,
                    _chat_tool(spec, strict_tools=self.capabilities.strict_tools),
                )
                for spec in request.tools
            ]
            if self.capabilities.parallel_tool_calls:
                parallel_tool_calls = request.parallel_tool_calls

        response = await self._client.chat.completions.create(
            model=self.model,
            messages=cast(list[ChatCompletionMessageParam], self._chat_messages(request)),
            store=self.store if self.capabilities.store_parameter else omit,
            tools=chat_tools,
            parallel_tool_calls=parallel_tool_calls,
            max_completion_tokens=(
                self.max_output_tokens if self.max_output_tokens is not None else omit
            ),
            temperature=self.temperature if self.temperature is not None else omit,
            extra_body=self.extra_body or None,
        )
        return _parse_chat_response(response)

    async def _complete_chat_stream(
        self, request: ModelRequest, sink: ModelStreamSink
    ) -> ModelResponse:
        chat_tools: list[ChatCompletionToolParam] | Omit = omit
        stream_tools: list[ChatCompletionToolParam] | Omit = omit
        parallel_tool_calls: bool | Omit = omit
        if request.tools:
            chat_tools = [
                cast(
                    ChatCompletionToolParam,
                    _chat_tool(spec, strict_tools=self.capabilities.strict_tools),
                )
                for spec in request.tools
            ]
            stream_tools = chat_tools
            if self.capabilities.parallel_tool_calls:
                parallel_tool_calls = request.parallel_tool_calls
        stream_options: ChatCompletionStreamOptionsParam | Omit = omit
        if self.capabilities.chat_stream_usage:
            stream_options = {"include_usage": True}

        stream_extra_body = dict(self.extra_body)
        if request.tools and not self.capabilities.strict_tools:
            # The SDK's high-level accumulator intentionally rejects non-strict tools
            # before making a request because it cannot auto-parse their arguments.
            # Compatible endpoints may not accept `strict`, while the Agent only needs
            # the accumulated raw JSON string and validates it locally. Supplying tools
            # through extra_body keeps the SDK accumulator without enabling auto-parse.
            stream_extra_body["tools"] = chat_tools
            stream_tools = omit

        tool_call_ids: dict[int, str] = {}
        async with self._client.chat.completions.stream(
            model=self.model,
            messages=cast(list[ChatCompletionMessageParam], self._chat_messages(request)),
            store=self.store if self.capabilities.store_parameter else omit,
            tools=stream_tools,
            parallel_tool_calls=parallel_tool_calls,
            max_completion_tokens=(
                self.max_output_tokens if self.max_output_tokens is not None else omit
            ),
            temperature=self.temperature if self.temperature is not None else omit,
            stream_options=stream_options,
            extra_body=stream_extra_body or None,
        ) as stream:
            async for event in stream:
                event_type = getattr(event, "type", None)
                if event_type == "chunk":
                    chunk = getattr(event, "chunk", None)
                    for choice in getattr(chunk, "choices", ()) or ():
                        delta = getattr(choice, "delta", None)
                        for raw_call in getattr(delta, "tool_calls", ()) or ():
                            call_id = str(getattr(raw_call, "id", "") or "")
                            if call_id:
                                tool_call_ids[int(raw_call.index)] = call_id
                elif event_type == "content.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        await _emit_stream_event(
                            sink,
                            ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, delta),
                        )
                elif event_type == "refusal.delta":
                    delta = str(getattr(event, "delta", "") or "")
                    if delta:
                        await _emit_stream_event(
                            sink,
                            ModelStreamEvent(ModelStreamEventKind.REFUSAL_DELTA, delta),
                        )
                elif event_type == "tool_calls.function.arguments.delta":
                    delta = str(getattr(event, "arguments_delta", "") or "")
                    if not delta:
                        continue
                    tool_index = int(getattr(event, "index", 0))
                    await _emit_stream_event(
                        sink,
                        ModelStreamEvent(
                            ModelStreamEventKind.TOOL_CALL_DELTA,
                            delta,
                            tool_index=tool_index,
                            tool_call_id=tool_call_ids.get(tool_index),
                            tool_name=str(getattr(event, "name", "") or "") or None,
                        ),
                    )
            response = await stream.get_final_completion()
        return _parse_chat_response(response)
