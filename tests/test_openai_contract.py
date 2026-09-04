import asyncio
import json

import httpx
import pytest
from openai import AsyncOpenAI

from react_agent import (
    AssistantMessage,
    ConfigurationError,
    ModelInvocationError,
    ModelOutcome,
    ModelRequest,
    ModelStreamEvent,
    ModelStreamEventKind,
    OpenAIModel,
    ProviderCapabilities,
    StreamingModel,
    ToolCall,
    ToolMessage,
    ToolSpec,
    UserMessage,
)


def weather_spec() -> ToolSpec:
    return ToolSpec(
        name="get_weather",
        description="Get weather for one city.",
        parameters={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
            "additionalProperties": False,
        },
    )


def request_with_tool_history() -> ModelRequest:
    return ModelRequest(
        transcript=(
            UserMessage("深圳天气如何?"),
            AssistantMessage(
                tool_calls=(ToolCall("call-previous", "get_weather", '{"city":"深圳"}'),)
            ),
            ToolMessage(
                call_id="call-previous",
                name="get_weather",
                content='{"ok":true,"data":"晴"}',
            ),
        ),
        tools=(weather_spec(),),
        instructions="Use tools when needed.",
        parallel_tool_calls=True,
    )


async def sdk_client(handler) -> AsyncOpenAI:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return AsyncOpenAI(
        api_key="test-api-key",
        base_url="https://provider.test/v1",
        max_retries=0,
        http_client=http_client,
    )


def sse_response(events: list[dict], *, done_marker: bool = False) -> httpx.Response:
    frames = [
        f"event: {event.get('type', 'message')}\ndata: {json.dumps(event, ensure_ascii=False)}"
        for event in events
    ]
    if done_marker:
        frames.append("data: [DONE]")
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream", "x-request-id": "req-stream"},
        content=("\n\n".join(frames) + "\n\n").encode(),
    )


def response_object(status: str, output: list[dict], *, usage: dict | None = None) -> dict:
    return {
        "id": "resp-stream",
        "object": "response",
        "created_at": 1,
        "model": "test-model",
        "status": status,
        "output": output,
        "parallel_tool_calls": True,
        "tool_choice": "auto",
        "tools": [],
        "usage": usage,
    }


@pytest.mark.asyncio
async def test_chat_completions_http_contract_omits_disabled_capability_fields() -> None:
    captured: list[tuple[httpx.Request, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request, json.loads(request.content)))
        return httpx.Response(
            200,
            headers={"content-type": "application/json", "x-request-id": "req-chat"},
            json={
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-next",
                                    "type": "function",
                                    "function": {
                                        "name": "get_weather",
                                        "arguments": '{"city":"广州"}',
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                },
            },
        )

    client = await sdk_client(handler)
    try:
        model = OpenAIModel(
            "test-model",
            api_mode="chat_completions",
            capabilities=ProviderCapabilities(
                strict_tools=False,
                parallel_tool_calls=False,
                store_parameter=False,
                encrypted_reasoning_items=False,
            ),
            client=client,
        )
        response = await model.complete(request_with_tool_history())
    finally:
        await client.close()

    assert len(captured) == 1
    raw_request, payload = captured[0]
    assert raw_request.url.path == "/v1/chat/completions"
    assert payload["model"] == "test-model"
    assert payload["messages"] == [
        {"role": "system", "content": "Use tools when needed."},
        {"role": "user", "content": "深圳天气如何?"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call-previous",
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "arguments": '{"city":"深圳"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call-previous",
            "content": '{"ok":true,"data":"晴"}',
        },
    ]
    assert payload["tools"] == [
        {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get weather for one city.",
                "parameters": weather_spec().parameters,
            },
        }
    ]
    assert "parallel_tool_calls" not in payload
    assert "store" not in payload
    assert "strict" not in payload["tools"][0]["function"]
    assert response.message.tool_calls == (ToolCall("call-next", "get_weather", '{"city":"广州"}'),)
    assert response.response_model == "test-model"
    assert response.usage.total_tokens == 18


@pytest.mark.asyncio
async def test_chat_stream_emits_sanitized_deltas_and_uses_final_accumulator() -> None:
    payloads: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        chunks = [
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "content": "正在查询"},
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-next",
                                    "type": "function",
                                    "function": {
                                        "name": "get_weather",
                                        "arguments": '{"city":',
                                    },
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "function": {"arguments": '"广州"}'},
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "test-model",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
            },
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "test-model",
                "choices": [],
                "usage": {
                    "prompt_tokens": 21,
                    "completion_tokens": 8,
                    "total_tokens": 29,
                },
            },
        ]
        return sse_response(chunks, done_marker=True)

    client = await sdk_client(handler)
    deltas: list[ModelStreamEvent] = []
    try:
        model = OpenAIModel("test-model", api_mode="chat_completions", client=client)
        assert isinstance(model, StreamingModel)
        response = await model.complete_stream(request_with_tool_history(), deltas.append)
    finally:
        await client.close()

    assert payloads[0]["stream"] is True
    assert payloads[0]["stream_options"] == {"include_usage": True}
    assert deltas == [
        ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, "正在查询"),
        ModelStreamEvent(
            ModelStreamEventKind.TOOL_CALL_DELTA,
            '{"city":',
            tool_index=0,
            tool_call_id="call-next",
            tool_name="get_weather",
        ),
        ModelStreamEvent(
            ModelStreamEventKind.TOOL_CALL_DELTA,
            '"广州"}',
            tool_index=0,
            tool_call_id="call-next",
            tool_name="get_weather",
        ),
    ]
    assert response.message.content == "正在查询"
    assert response.message.tool_calls == (ToolCall("call-next", "get_weather", '{"city":"广州"}'),)
    assert response.usage.total_tokens == 29


@pytest.mark.asyncio
async def test_chat_stream_can_omit_usage_option_for_compatible_provider() -> None:
    payloads: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return sse_response(
            [
                {
                    "id": "chatcmpl-stream",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "完成"},
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "chatcmpl-stream",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                },
            ],
            done_marker=True,
        )

    client = await sdk_client(handler)
    try:
        response = await OpenAIModel(
            "test-model",
            api_mode="chat_completions",
            capabilities=ProviderCapabilities(chat_stream_usage=False),
            client=client,
        ).complete_stream(ModelRequest((UserMessage("请求"),), (), "Answer."), lambda _: None)
    finally:
        await client.close()

    assert "stream_options" not in payloads[0]
    assert response.message.content == "完成"


@pytest.mark.asyncio
async def test_chat_stream_supports_compatible_non_strict_tools() -> None:
    payloads: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return sse_response(
            [
                {
                    "id": "chatcmpl-compat-stream",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "完成"},
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "chatcmpl-compat-stream",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {"index": 0, "delta": {}, "finish_reason": "stop"}
                    ],
                },
            ],
            done_marker=True,
        )

    client = await sdk_client(handler)
    try:
        response = await OpenAIModel(
            "test-model",
            api_mode="chat_completions",
            capabilities=ProviderCapabilities(
                strict_tools=False,
                chat_stream_usage=False,
            ),
            client=client,
        ).complete_stream(request_with_tool_history(), lambda _: None)
    finally:
        await client.close()

    assert payloads[0]["tools"][0]["function"]["name"] == "get_weather"
    assert "strict" not in payloads[0]["tools"][0]["function"]
    assert response.message.content == "完成"


@pytest.mark.asyncio
async def test_chat_stream_emits_refusal_delta() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return sse_response(
            [
                {
                    "id": "chatcmpl-refusal",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "refusal": "无法协助。"},
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "chatcmpl-refusal",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                },
                {
                    "id": "chatcmpl-refusal",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 2,
                        "total_tokens": 5,
                    },
                },
            ],
            done_marker=True,
        )

    client = await sdk_client(handler)
    deltas: list[ModelStreamEvent] = []
    try:
        response = await OpenAIModel(
            "test-model", api_mode="chat_completions", client=client
        ).complete_stream(ModelRequest((UserMessage("请求"),), (), "Be safe."), deltas.append)
    finally:
        await client.close()

    assert deltas == [ModelStreamEvent(ModelStreamEventKind.REFUSAL_DELTA, "无法协助。")]
    assert response.outcome is ModelOutcome.REFUSED
    assert response.message.content == "无法协助。"


@pytest.mark.asyncio
async def test_responses_http_contract_omits_disabled_capability_fields() -> None:
    captured: list[tuple[httpx.Request, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request, json.loads(request.content)))
        return httpx.Response(
            200,
            headers={"content-type": "application/json", "x-request-id": "req-responses"},
            json={
                "id": "resp-test",
                "object": "response",
                "created_at": 1,
                "model": "test-model",
                "status": "completed",
                "output": [
                    {
                        "id": "fc-test",
                        "type": "function_call",
                        "call_id": "call-next",
                        "name": "get_weather",
                        "arguments": '{"city":"广州"}',
                        "status": "completed",
                    }
                ],
                "usage": {
                    "input_tokens": 13,
                    "output_tokens": 5,
                    "total_tokens": 18,
                },
            },
        )

    client = await sdk_client(handler)
    try:
        model = OpenAIModel(
            "test-model",
            api_mode="responses",
            capabilities=ProviderCapabilities(
                strict_tools=False,
                parallel_tool_calls=False,
                store_parameter=False,
                encrypted_reasoning_items=False,
            ),
            client=client,
        )
        response = await model.complete(request_with_tool_history())
    finally:
        await client.close()

    assert len(captured) == 1
    raw_request, payload = captured[0]
    assert raw_request.url.path == "/v1/responses"
    assert payload["model"] == "test-model"
    assert payload["instructions"] == "Use tools when needed."
    assert payload["input"] == [
        {"role": "user", "content": "深圳天气如何?"},
        {
            "type": "function_call",
            "call_id": "call-previous",
            "name": "get_weather",
            "arguments": '{"city":"深圳"}',
        },
        {
            "type": "function_call_output",
            "call_id": "call-previous",
            "output": '{"ok":true,"data":"晴"}',
        },
    ]
    assert payload["tools"] == [
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get weather for one city.",
            "parameters": weather_spec().parameters,
        }
    ]
    assert "parallel_tool_calls" not in payload
    assert "store" not in payload
    assert "include" not in payload
    assert "strict" not in payload["tools"][0]
    assert response.message.tool_calls == (ToolCall("call-next", "get_weather", '{"city":"广州"}'),)
    assert response.usage.total_tokens == 18


@pytest.mark.asyncio
async def test_responses_stream_emits_text_and_tool_deltas_from_typed_events() -> None:
    payloads: list[dict] = []
    final_message = {
        "id": "msg-next",
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [
            {
                "type": "output_text",
                "text": "正在查询",
                "annotations": [],
                "logprobs": [],
            }
        ],
    }
    final_call = {
        "id": "fc-next",
        "type": "function_call",
        "call_id": "call-next",
        "name": "get_weather",
        "arguments": '{"city":"广州"}',
        "status": "completed",
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        events = [
            {
                "type": "response.created",
                "sequence_number": 0,
                "response": response_object("in_progress", []),
            },
            {
                "type": "response.output_item.added",
                "sequence_number": 1,
                "output_index": 0,
                "item": {
                    "id": "msg-next",
                    "type": "message",
                    "role": "assistant",
                    "status": "in_progress",
                    "content": [],
                },
            },
            {
                "type": "response.content_part.added",
                "sequence_number": 2,
                "output_index": 0,
                "content_index": 0,
                "item_id": "msg-next",
                "part": {
                    "type": "output_text",
                    "text": "",
                    "annotations": [],
                    "logprobs": [],
                },
            },
            {
                "type": "response.output_text.delta",
                "sequence_number": 3,
                "output_index": 0,
                "content_index": 0,
                "item_id": "msg-next",
                "delta": "正在查询",
                "logprobs": [],
            },
            {
                "type": "response.output_item.added",
                "sequence_number": 4,
                "output_index": 1,
                "item": {
                    **final_call,
                    "arguments": "",
                    "status": "in_progress",
                },
            },
            {
                "type": "response.function_call_arguments.delta",
                "sequence_number": 5,
                "output_index": 1,
                "item_id": "fc-next",
                "delta": '{"city":',
            },
            {
                "type": "response.function_call_arguments.delta",
                "sequence_number": 6,
                "output_index": 1,
                "item_id": "fc-next",
                "delta": '"广州"}',
            },
            {
                "type": "response.completed",
                "sequence_number": 7,
                "response": response_object(
                    "completed",
                    [final_message, final_call],
                    usage={
                        "input_tokens": 18,
                        "output_tokens": 9,
                        "total_tokens": 27,
                    },
                ),
            },
        ]
        return sse_response(events)

    client = await sdk_client(handler)
    deltas: list[ModelStreamEvent] = []
    try:
        response = await OpenAIModel(
            "test-model", api_mode="responses", client=client
        ).complete_stream(request_with_tool_history(), deltas.append)
    finally:
        await client.close()

    assert payloads[0]["stream"] is True
    assert deltas == [
        ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, "正在查询"),
        ModelStreamEvent(
            ModelStreamEventKind.TOOL_CALL_DELTA,
            '{"city":',
            tool_index=0,
            tool_call_id="call-next",
            tool_name="get_weather",
        ),
        ModelStreamEvent(
            ModelStreamEventKind.TOOL_CALL_DELTA,
            '"广州"}',
            tool_index=0,
            tool_call_id="call-next",
            tool_name="get_weather",
        ),
    ]
    assert response.message.content == "正在查询"
    assert response.message.tool_calls == (ToolCall("call-next", "get_weather", '{"city":"广州"}'),)
    assert response.usage.total_tokens == 27
    # Replayed items must be exactly what the provider emitted. The SDK's stream
    # accumulator decorates strict function calls with client-only
    # ``parsed_arguments`` (and text parts with ``parsed``), which providers
    # reject as unknown parameters when the history is sent back.
    assert response.message.raw_items == (final_message, final_call)


@pytest.mark.asyncio
async def test_responses_stream_emits_refusal_delta_and_final_refusal() -> None:
    final_message = {
        "id": "msg-refusal",
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "refusal", "refusal": "无法协助。"}],
    }

    async def handler(_request: httpx.Request) -> httpx.Response:
        return sse_response(
            [
                {
                    "type": "response.created",
                    "sequence_number": 0,
                    "response": response_object("in_progress", []),
                },
                {
                    "type": "response.output_item.added",
                    "sequence_number": 1,
                    "output_index": 0,
                    "item": {
                        "id": "msg-refusal",
                        "type": "message",
                        "role": "assistant",
                        "status": "in_progress",
                        "content": [],
                    },
                },
                {
                    "type": "response.content_part.added",
                    "sequence_number": 2,
                    "output_index": 0,
                    "content_index": 0,
                    "item_id": "msg-refusal",
                    "part": {"type": "refusal", "refusal": ""},
                },
                {
                    "type": "response.refusal.delta",
                    "sequence_number": 3,
                    "output_index": 0,
                    "content_index": 0,
                    "item_id": "msg-refusal",
                    "delta": "无法协助。",
                },
                {
                    "type": "response.completed",
                    "sequence_number": 4,
                    "response": response_object("completed", [final_message]),
                },
            ]
        )

    client = await sdk_client(handler)
    deltas: list[ModelStreamEvent] = []

    async def sink(event: ModelStreamEvent) -> None:
        deltas.append(event)

    try:
        response = await OpenAIModel(
            "test-model", api_mode="responses", client=client
        ).complete_stream(ModelRequest((UserMessage("请求"),), (), "Be safe."), sink)
    finally:
        await client.close()

    assert deltas == [ModelStreamEvent(ModelStreamEventKind.REFUSAL_DELTA, "无法协助。")]
    assert response.outcome is ModelOutcome.REFUSED
    assert response.message.content == "无法协助。"


@pytest.mark.asyncio
async def test_chat_length_finish_marks_tool_call_incomplete() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-truncated",
                "object": "chat.completion",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-truncated",
                                    "type": "function",
                                    "function": {
                                        "name": "get_weather",
                                        "arguments": '{"city":"深圳"}',
                                    },
                                }
                            ],
                        },
                        "finish_reason": "length",
                    }
                ],
            },
        )

    client = await sdk_client(handler)
    try:
        response = await OpenAIModel(
            "test-model",
            api_mode="chat_completions",
            client=client,
        ).complete(request_with_tool_history())
    finally:
        await client.close()

    assert response.outcome is ModelOutcome.INCOMPLETE
    assert response.diagnostic == "length"


@pytest.mark.asyncio
async def test_responses_refusal_is_preserved_as_an_explicit_outcome() -> None:
    payloads: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "resp-refusal",
                "object": "response",
                "created_at": 1,
                "model": "test-model",
                "status": "completed",
                "output": [
                    {
                        "id": "msg-refusal",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "refusal", "refusal": "无法协助该请求。"}],
                    }
                ],
            },
        )

    client = await sdk_client(handler)
    try:
        response = await OpenAIModel(
            "test-model",
            api_mode="responses",
            client=client,
        ).complete(ModelRequest((UserMessage("请求"),), (), "Be safe."))
    finally:
        await client.close()

    assert response.outcome is ModelOutcome.REFUSED
    assert response.message.content == "无法协助该请求。"
    assert payloads[0]["store"] is False
    assert payloads[0]["include"] == ["reasoning.encrypted_content"]


@pytest.mark.asyncio
async def test_stream_cancellation_propagates_and_closes_transport() -> None:
    class BlockingByteStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.closed = False
            self._never = asyncio.Event()

        async def __aiter__(self):
            self.started.set()
            await self._never.wait()
            yield b""

        async def aclose(self) -> None:
            self.closed = True

    body = BlockingByteStream()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=body,
        )

    client = await sdk_client(handler)
    try:
        model = OpenAIModel("test-model", api_mode="chat_completions", client=client)
        task = asyncio.create_task(
            model.complete_stream(
                ModelRequest((UserMessage("请求"),), (), "Answer."), lambda _: None
            )
        )
        await asyncio.wait_for(body.started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert body.closed
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_stream_sink_failures_are_sanitized() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return sse_response(
            [
                {
                    "id": "chatcmpl-stream",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": "delta"},
                            "finish_reason": None,
                        }
                    ],
                }
            ],
            done_marker=True,
        )

    def failing_sink(_event: ModelStreamEvent) -> None:
        raise RuntimeError("secret callback details")

    client = await sdk_client(handler)
    try:
        with pytest.raises(ModelInvocationError) as exc_info:
            await OpenAIModel(
                "test-model", api_mode="chat_completions", client=client
            ).complete_stream(ModelRequest((UserMessage("请求"),), (), "Answer."), failing_sink)
    finally:
        await client.close()

    assert "secret callback details" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_model_repr_never_contains_api_key() -> None:
    secret = "sk-test-do-not-log"
    model = OpenAIModel("test-model", api_key=secret)
    try:
        assert secret not in repr(model)
    finally:
        await model.aclose()


def test_remote_plain_http_base_url_is_rejected_by_default() -> None:
    with pytest.raises(ConfigurationError, match="must use HTTPS"):
        OpenAIModel(
            "test-model",
            api_key="test-key",
            base_url="http://provider.example/v1",
        )
