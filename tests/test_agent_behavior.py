import asyncio
import inspect
import json
from collections import deque

import pytest

from react_agent import (
    AgentConfig,
    AssistantMessage,
    EventKind,
    ModelOutcome,
    ModelRequest,
    ModelResponse,
    ReActAgent,
    RunStatus,
    StopReason,
    ToolCall,
    ToolMessage,
    tool,
)
from react_agent.debug_evidence import seal_debug_observation
from react_agent.models import (
    AgentJournalEvent,
    AgentJournalEventKind,
    AgentStreamEvent,
    AgentStreamEventKind,
    ModelStreamEvent,
    ModelStreamEventKind,
)
from react_agent.tools import DebugExposure, Tool


class ScriptedModel:
    """Deterministic stand-in for the external model boundary."""

    def __init__(self, *responses: ModelResponse) -> None:
        self._responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self._responses:
            pytest.fail("The agent made an unexpected extra model call")
        return self._responses.popleft()


class StreamingScriptedModel(ScriptedModel):
    """A scripted model that exercises the optional streaming seam."""

    def __init__(
        self,
        *turns: tuple[tuple[ModelStreamEvent, ...], ModelResponse],
    ) -> None:
        super().__init__(*(response for _, response in turns))
        self._stream_events = deque(events for events, _ in turns)
        self.stream_calls = 0

    async def complete_stream(self, request, sink) -> ModelResponse:
        self.stream_calls += 1
        events = self._stream_events.popleft()
        for event in events:
            outcome = sink(event)
            if inspect.isawaitable(outcome):
                await outcome
        return await self.complete(request)


def model_turn(
    content: str | None = None,
    *tool_calls: ToolCall,
) -> ModelResponse:
    return ModelResponse(AssistantMessage(content=content, tool_calls=tool_calls))


def observations(result) -> list[ToolMessage]:
    return [item for item in result.transcript if isinstance(item, ToolMessage)]


@pytest.mark.asyncio
async def test_agent_can_complete_without_using_a_tool() -> None:
    model = ScriptedModel(model_turn("直接答案"))

    result = await ReActAgent(model).run("回答问题", run_id="run-direct")

    assert result.output == "直接答案"
    assert result.status is RunStatus.COMPLETED
    assert result.stop_reason is StopReason.COMPLETED
    assert result.model_calls == 1
    assert result.tool_calls == 0
    assert result.tool_executions == 0


@pytest.mark.asyncio
async def test_agent_returns_a_single_tool_observation_to_the_model() -> None:
    @tool
    def double(value: int) -> int:
        """Double an integer."""

        return value * 2

    model = ScriptedModel(
        model_turn(None, ToolCall("call-double", "double", '{"value":21}')),
        model_turn("结果是 42"),
    )

    result = await ReActAgent(model, [double]).run("计算两倍")

    assert result.output == "结果是 42"
    assert result.tool_calls == 1
    assert result.tool_executions == 1
    assert len(model.requests) == 2
    observation = model.requests[1].transcript[-1]
    assert isinstance(observation, ToolMessage)
    assert observation.call_id == "call-double"
    assert observation.name == "double"
    assert json.loads(observation.content) == {
        "ok": True,
        "data": 42,
        "meta": {"truncated": False},
    }


@pytest.mark.asyncio
async def test_parallel_tool_results_keep_the_models_requested_order() -> None:
    both_started = asyncio.Event()
    started: list[str] = []
    finished: list[str] = []

    @tool(parallel_safe=True)
    async def lookup(label: str) -> str:
        """Look up a label concurrently."""

        started.append(label)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=0.5)
        if label == "slow":
            await asyncio.sleep(0.02)
        finished.append(label)
        return f"value:{label}"

    model = ScriptedModel(
        model_turn(
            None,
            ToolCall("call-slow", "lookup", '{"label":"slow"}'),
            ToolCall("call-fast", "lookup", '{"label":"fast"}'),
        ),
        model_turn("完成"),
    )

    result = await ReActAgent(model, [lookup]).run("并行查询")

    assert result.status is RunStatus.COMPLETED
    assert started == ["slow", "fast"]
    assert finished == ["fast", "slow"]
    tool_messages = observations(result)
    assert [message.call_id for message in tool_messages] == ["call-slow", "call-fast"]
    assert [json.loads(message.content)["data"] for message in tool_messages] == [
        "value:slow",
        "value:fast",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arguments", "expected_detail"),
    [
        ("not-json", "Arguments are not valid JSON"),
        ('{"value":"not-an-integer"}', "int_parsing"),
    ],
)
async def test_bad_tool_arguments_are_observed_without_invocation(
    arguments: str,
    expected_detail: str,
) -> None:
    invocations = 0

    @tool
    def square(value: int) -> int:
        """Square an integer."""

        nonlocal invocations
        invocations += 1
        return value * value

    model = ScriptedModel(
        model_turn(None, ToolCall("call-bad", "square", arguments)),
        model_turn("参数无效"),
    )

    result = await ReActAgent(model, [square]).run("计算平方")

    assert result.status is RunStatus.COMPLETED
    assert invocations == 0
    [observation] = observations(result)
    payload = json.loads(observation.content)
    assert observation.is_error is True
    assert payload["ok"] is False
    assert payload["error"]["code"] == "INVALID_ARGUMENTS"
    assert expected_detail in json.dumps(payload["error"].get("details"), ensure_ascii=False)


@pytest.mark.asyncio
async def test_unknown_tool_is_a_recoverable_observation() -> None:
    model = ScriptedModel(
        model_turn(None, ToolCall("call-missing", "missing_tool", "{}")),
        model_turn("工具不存在, 无法继续"),
    )

    result = await ReActAgent(model).run("调用不存在的工具")

    assert result.status is RunStatus.COMPLETED
    assert result.tool_calls == 1
    assert result.tool_executions == 0
    [observation] = observations(result)
    assert observation.is_error is True
    assert json.loads(observation.content)["error"]["code"] == "UNKNOWN_TOOL"


@pytest.mark.asyncio
async def test_denied_approval_never_invokes_the_tool() -> None:
    invocations = 0
    approval_requests = []

    @tool(requires_approval=True)
    def delete_record(record_id: int) -> str:
        """Delete one record."""

        nonlocal invocations
        invocations += 1
        return "deleted"

    def deny(request) -> bool:
        approval_requests.append(request)
        return False

    model = ScriptedModel(
        model_turn(None, ToolCall("call-delete", "delete_record", '{"record_id":7}')),
        model_turn("用户拒绝了删除"),
    )

    result = await ReActAgent(
        model,
        [delete_record],
        approval_handler=deny,
    ).run("删除记录", run_id="approval-run")

    assert result.status is RunStatus.COMPLETED
    assert invocations == 0
    assert len(approval_requests) == 1
    assert approval_requests[0].run_id == "approval-run"
    assert approval_requests[0].arguments == {"record_id": 7}
    [observation] = observations(result)
    assert json.loads(observation.content)["error"]["code"] == "TOOL_DENIED"


@pytest.mark.asyncio
async def test_max_steps_stops_after_the_last_allowed_model_turn() -> None:
    @tool
    def identity(value: int) -> int:
        """Return an integer unchanged."""

        return value

    model = ScriptedModel(
        model_turn(None, ToolCall("call-one", "identity", '{"value":1}')),
    )

    result = await ReActAgent(
        model,
        [identity],
        config=AgentConfig(max_steps=1),
    ).run("不断调用工具")

    assert result.status is RunStatus.PARTIAL
    assert result.stop_reason is StopReason.MAX_STEPS
    assert result.model_calls == 1
    assert result.tool_calls == 1
    assert result.tool_executions == 1


@pytest.mark.asyncio
async def test_max_tool_calls_rejects_the_next_batch_without_executing_it() -> None:
    invoked_values: list[int] = []

    @tool
    def identity(value: int) -> int:
        """Return an integer unchanged."""

        invoked_values.append(value)
        return value

    model = ScriptedModel(
        model_turn(None, ToolCall("call-one", "identity", '{"value":1}')),
        model_turn(None, ToolCall("call-two", "identity", '{"value":2}')),
    )

    result = await ReActAgent(
        model,
        [identity],
        config=AgentConfig(max_tool_calls=1),
    ).run("调用超过预算")

    assert result.status is RunStatus.PARTIAL
    assert result.stop_reason is StopReason.MAX_TOOL_CALLS
    assert result.tool_calls == 2
    assert result.tool_executions == 1
    assert invoked_values == [1]
    assert json.loads(observations(result)[-1].content)["error"]["code"] == (
        "TOOL_BUDGET_EXCEEDED"
    )


@pytest.mark.asyncio
async def test_repeated_call_id_reuses_the_cached_result() -> None:
    invocations = 0

    @tool
    def add(left: int, right: int) -> int:
        """Add two integers."""

        nonlocal invocations
        invocations += 1
        return left + right

    model = ScriptedModel(
        model_turn(None, ToolCall("stable-id", "add", '{"left":1,"right":2}')),
        model_turn(None, ToolCall("stable-id", "add", '{"right":2, "left":1}')),
        model_turn("和为 3"),
    )

    result = await ReActAgent(model, [add]).run("重复同一个调用")

    assert result.status is RunStatus.COMPLETED
    assert result.tool_calls == 2
    assert result.tool_executions == 1
    assert invocations == 1
    first, reused = observations(result)
    assert first.cached is False
    assert reused.cached is True
    assert first.content == reused.content
    assert EventKind.TOOL_REUSED in [event.kind for event in result.events]


@pytest.mark.asyncio
async def test_repeating_the_same_action_and_result_stops_as_a_loop() -> None:
    @tool
    def constant(value: int) -> str:
        """Return a stable result for an integer."""

        return f"same:{value}"

    model = ScriptedModel(
        model_turn(None, ToolCall("call-a", "constant", '{"value":1}')),
        model_turn(None, ToolCall("call-b", "constant", '{"value":1}')),
    )

    result = await ReActAgent(
        model,
        [constant],
        config=AgentConfig(repeated_action_limit=2),
    ).run("陷入循环")

    assert result.status is RunStatus.PARTIAL
    assert result.stop_reason is StopReason.LOOP_DETECTED
    assert result.model_calls == 2
    assert result.tool_executions == 2
    assert EventKind.LOOP_DETECTED in [event.kind for event in result.events]


@pytest.mark.asyncio
async def test_events_do_not_expose_tool_arguments_or_outputs() -> None:
    secret = "super-secret-tool-argument"

    @tool
    def echo(value: str) -> str:
        """Echo one value."""

        return value

    model = ScriptedModel(
        model_turn(None, ToolCall("safe-call-id", "echo", json.dumps({"value": secret}))),
        model_turn("完成"),
    )

    result = await ReActAgent(model, [echo]).run("处理敏感参数")

    serialized_events = json.dumps(
        [
            {
                "kind": event.kind.value,
                "run_id": event.run_id,
                "step": event.step,
                "tool_call_id": event.tool_call_id,
                "tool_name": event.tool_name,
                "data": dict(event.data),
            }
            for event in result.events
        ],
        ensure_ascii=False,
    )
    assert secret not in serialized_events
    assert secret not in repr(result.events)


@pytest.mark.asyncio
async def test_incomplete_model_call_is_never_executed() -> None:
    invoked = False

    @tool
    def transfer(amount: int) -> str:
        """Transfer a validated amount for the test."""

        nonlocal invoked
        invoked = True
        return f"transferred:{amount}"

    model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                content="partial",
                tool_calls=(ToolCall("call-partial", "transfer", '{"amount":100}'),),
            ),
            outcome=ModelOutcome.INCOMPLETE,
            diagnostic="length",
        )
    )

    result = await ReActAgent(model, [transfer]).run("执行操作")

    assert invoked is False
    assert result.status is RunStatus.PARTIAL
    assert result.stop_reason is StopReason.MODEL_INCOMPLETE
    assert result.tool_executions == 0
    assert json.loads(observations(result)[0].content)["error"]["code"] == (
        "MODEL_OUTPUT_INCOMPLETE"
    )


@pytest.mark.asyncio
async def test_unexpected_model_exception_becomes_a_structured_failure() -> None:
    class BrokenModel:
        async def complete(self, _request: ModelRequest) -> ModelResponse:
            raise ValueError("private provider details")

    result = await ReActAgent(BrokenModel()).run("触发 provider 解析错误")

    assert result.status is RunStatus.FAILED
    assert result.stop_reason is StopReason.MODEL_ERROR
    assert result.error == "Model adapter failed: ValueError"
    assert result.events[-1].kind is EventKind.RUN_COMPLETED


@pytest.mark.asyncio
async def test_alternating_actions_are_detected_as_a_loop() -> None:
    @tool
    def stable(label: str) -> str:
        """Return a stable result for loop detection."""

        return label

    model = ScriptedModel(
        model_turn(None, ToolCall("a1", "stable", '{"label":"a"}')),
        model_turn(None, ToolCall("b1", "stable", '{"label":"b"}')),
        model_turn(None, ToolCall("a2", "stable", '{"label":"a"}')),
    )

    result = await ReActAgent(
        model,
        [stable],
        config=AgentConfig(repeated_action_limit=2),
    ).run("交替重复")

    assert result.stop_reason is StopReason.LOOP_DETECTED
    assert result.tool_executions == 3


@pytest.mark.asyncio
async def test_context_budget_stops_before_calling_the_model() -> None:
    model = ScriptedModel(model_turn("should not be used"))

    result = await ReActAgent(
        model,
        config=AgentConfig(max_context_chars=10),
    ).run("这个提示显然超过十个字符")

    assert result.status is RunStatus.PARTIAL
    assert result.stop_reason is StopReason.CONTEXT_LIMIT
    assert result.model_calls == 0
    assert model.requests == []


@pytest.mark.asyncio
async def test_rich_stream_is_ordered_ephemeral_and_closes_the_tool_loop() -> None:
    @tool(debug_exposure=DebugExposure.FULL)
    def add(left: int, right: int) -> int:
        """Add two integers for the debug stream."""

        return left + right

    model = StreamingScriptedModel(
        (
            (
                ModelStreamEvent(
                    ModelStreamEventKind.TOOL_CALL_DELTA,
                    '{"left":',
                    tool_index=0,
                    tool_call_id="call-add",
                    tool_name="add",
                ),
                ModelStreamEvent(
                    ModelStreamEventKind.TOOL_CALL_DELTA,
                    '20,"right":22}',
                    tool_index=0,
                ),
            ),
            model_turn(None, ToolCall("call-add", "add", '{"left":20,"right":22}')),
        ),
        (
            (
                ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, "结果是 "),
                ModelStreamEvent(ModelStreamEventKind.TEXT_DELTA, "42"),
            ),
            model_turn("结果是 42"),
        ),
    )
    streamed: list[AgentStreamEvent] = []

    result = await ReActAgent(model, [add]).run("计算", stream_sink=streamed.append)

    assert result.output == "结果是 42"
    assert model.stream_calls == 2
    assert [event.sequence for event in streamed] == list(range(1, len(streamed) + 1))
    kinds = [event.kind for event in streamed]
    assert kinds[0] is AgentStreamEventKind.RUN_STARTED
    assert kinds[-1] is AgentStreamEventKind.RUN_COMPLETED
    completed_index = kinds.index(AgentStreamEventKind.MODEL_COMPLETED)
    ready_index = kinds.index(AgentStreamEventKind.MODEL_TOOL_CALL_READY)
    started_index = kinds.index(AgentStreamEventKind.TOOL_STARTED)
    result_index = kinds.index(AgentStreamEventKind.TOOL_RESULT)
    assert completed_index < ready_index < started_index < result_index
    ready = streamed[ready_index]
    assert ready.call_key == "s1:t0"
    assert ready.data["arguments"] == '{"left":20,"right":22}'
    tool_result = streamed[result_index]
    assert tool_result.call_key == "s1:t0"
    assert json.loads(str(tool_result.data["result"]))["data"] == 42
    assert all(not isinstance(event, AgentStreamEvent) for event in result.events)
    assert AgentStreamEventKind.MODEL_TEXT_DELTA in kinds


@pytest.mark.asyncio
async def test_metadata_debug_exposure_never_reveals_arguments_or_results() -> None:
    secret = "private-tool-value"

    @tool
    def echo(value: str) -> str:
        """Echo a value without opting in to raw debug exposure."""

        return value

    arguments = json.dumps({"value": secret})
    model = StreamingScriptedModel(
        (
            (
                ModelStreamEvent(
                    ModelStreamEventKind.TOOL_CALL_DELTA,
                    arguments,
                    tool_index=0,
                    tool_call_id="call-echo",
                    tool_name="echo",
                ),
            ),
            model_turn(None, ToolCall("call-echo", "echo", arguments)),
        ),
        ((), model_turn("完成")),
    )
    streamed: list[AgentStreamEvent] = []

    await ReActAgent(model, [echo]).run("处理", stream_sink=streamed.append)

    serialized = json.dumps(
        [
            {
                "kind": event.kind.value,
                "tool_name": event.tool_name,
                "data": dict(event.data),
            }
            for event in streamed
        ],
        ensure_ascii=False,
    )
    assert secret not in serialized
    [delta] = [
        event
        for event in streamed
        if event.kind is AgentStreamEventKind.MODEL_TOOL_CALL_DELTA
    ]
    assert delta.data["delta_chars"] == len(arguments)
    assert "delta" not in delta.data
    [ready] = [
        event
        for event in streamed
        if event.kind is AgentStreamEventKind.MODEL_TOOL_CALL_READY
    ]
    assert "arguments" not in ready.data
    [tool_result] = [
        event for event in streamed if event.kind is AgentStreamEventKind.TOOL_RESULT
    ]
    assert "result" not in tool_result.data


@pytest.mark.asyncio
async def test_durable_debug_projection_replays_final_text_and_opted_in_tool_io() -> None:
    provider_secret = "opaque-encrypted-reasoning"

    @tool(debug_exposure=DebugExposure.FULL)
    def add(left: int, right: int) -> int:
        """Add integers with explicit durable debug exposure."""

        return left + right

    model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                tool_calls=(ToolCall("call-add", "add", '{"left":20,"right":22}'),),
                raw_items=(
                    {
                        "type": "reasoning",
                        "encrypted_content": provider_secret,
                    },
                ),
            )
        ),
        model_turn("最终答案是 42"),
    )
    journaled: list[AgentJournalEvent] = []

    result = await ReActAgent(model, [add]).run(
        "计算",
        journal_sink=journaled.append,
    )

    assert result.output == "最终答案是 42"
    completed_models = [
        event
        for event in journaled
        if event.kind is AgentJournalEventKind.MODEL_COMPLETED
    ]
    assert [event.public_data["final_text"] for event in completed_models] == [
        None,
        "最终答案是 42",
    ]
    [planned] = [
        event for event in journaled if event.kind is AgentJournalEventKind.TOOL_PLANNED
    ]
    assert planned.public_data["arguments"] == '{"left":20,"right":22}'
    [completed_tool] = [
        event
        for event in journaled
        if event.kind is AgentJournalEventKind.TOOL_COMPLETED
    ]
    assert json.loads(str(completed_tool.public_data["result"]))["data"] == 42
    serialized = json.dumps(
        [
            {
                "public": dict(event.public_data),
                "private": dict(event.private_data),
            }
            for event in journaled
        ],
        ensure_ascii=False,
    )
    assert provider_secret not in serialized


@pytest.mark.asyncio
async def test_full_debug_exposure_is_bounded_and_marks_truncation() -> None:
    @tool(debug_exposure=DebugExposure.FULL)
    def echo(value: str) -> str:
        """Echo a value with explicit full debug exposure."""

        return value

    arguments = json.dumps({"value": "x" * 600})
    model = ScriptedModel(
        model_turn(None, ToolCall("large-call", "echo", arguments)),
        model_turn("完成"),
    )
    streamed: list[AgentStreamEvent] = []

    await ReActAgent(
        model,
        [echo],
        config=AgentConfig(max_tool_output_chars=256),
    ).run("处理长结果", stream_sink=streamed.append)

    [ready] = [
        event
        for event in streamed
        if event.kind is AgentStreamEventKind.MODEL_TOOL_CALL_READY
    ]
    assert len(str(ready.data["arguments"])) == 256
    assert ready.data["truncated"] is True
    [tool_result] = [
        event for event in streamed if event.kind is AgentStreamEventKind.TOOL_RESULT
    ]
    assert len(str(tool_result.data["result"])) <= 256
    assert tool_result.data["truncated"] is True


@pytest.mark.asyncio
async def test_agent_journal_preserves_private_result_before_model_truncation() -> None:
    observation = seal_debug_observation(
        {
            "schema_version": 1,
            "observation_kind": "python_runtime_debug",
            "action": "stack",
            "debug_session_id": "large-debug-session",
            "state": "stopped",
            "success": True,
            "stop_id": 1,
            "exception": {
                "type": "RuntimeError",
                "message": "bounded-evidence-" + ("x" * 4_000),
            },
        }
    )

    async def debug_stack() -> dict[str, object]:
        """Return a deliberately large sealed debugger observation."""

        return observation

    registered = Tool(
        debug_stack,
        name="python_debug_stack",
        private_result_encoder=lambda result: {"debug_observation": result},
    )
    journaled: list[AgentJournalEvent] = []
    model = ScriptedModel(
        model_turn(None, ToolCall("large-debug-call", "python_debug_stack", "{}")),
        model_turn("done"),
    )

    await ReActAgent(
        model,
        [registered],
        config=AgentConfig(max_tool_output_chars=256),
    ).run("capture", journal_sink=journaled.append)

    [completed] = [
        event
        for event in journaled
        if event.kind is AgentJournalEventKind.TOOL_COMPLETED
    ]
    projected = json.loads(str(completed.private_data["message"]["content"]))
    private = completed.private_data["tool_private"]

    assert projected["meta"]["truncated"] is True
    assert private["debug_observation"] == observation
    model_observation = model.requests[1].transcript[-1]
    assert isinstance(model_observation, ToolMessage)
    assert json.loads(model_observation.content)["meta"]["truncated"] is True


@pytest.mark.asyncio
async def test_stream_sink_cancellation_propagates_before_model_invocation() -> None:
    model = ScriptedModel(model_turn("不应调用"))

    async def cancel_on_model_start(event: AgentStreamEvent) -> None:
        if event.kind is AgentStreamEventKind.MODEL_STARTED:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await ReActAgent(model).run("取消", stream_sink=cancel_on_model_start)

    assert model.requests == []
