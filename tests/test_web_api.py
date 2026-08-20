import asyncio
import json
import uuid
from collections import deque
from typing import cast

import httpx
import pytest

from react_agent import (
    AgentConfig,
    AgentResult,
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ReActAgent,
    RunStatus,
    StopReason,
    ToolCall,
    ToolMessage,
    Usage,
    UserMessage,
)
from react_agent.web import SessionStore, calculate_expression, create_app


class ScriptedModel:
    """Deterministic model boundary for offline API tests."""

    def __init__(self, *responses: ModelResponse) -> None:
        self._responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self._responses:
            pytest.fail("The web API made an unexpected model call")
        return self._responses.popleft()


class HistoryEchoAgent:
    """Agent double whose answer exposes only observable conversation length."""

    def __init__(self, *, delay_s: float = 0.0) -> None:
        self.delay_s = delay_s
        self.active_runs = 0
        self.max_active_runs = 0

    async def run(self, prompt: str, *, history=()) -> AgentResult:
        self.active_runs += 1
        self.max_active_runs = max(self.max_active_runs, self.active_runs)
        try:
            if self.delay_s:
                await asyncio.sleep(self.delay_s)
            answer = f"history-items:{len(history)}"
            return AgentResult(
                output=answer,
                status=RunStatus.COMPLETED,
                stop_reason=StopReason.COMPLETED,
                run_id=f"echo-{prompt}",
                model_calls=1,
                tool_calls=0,
                tool_executions=0,
                usage=Usage(),
                transcript=(
                    *history,
                    UserMessage(prompt),
                    AssistantMessage(content=answer),
                ),
                events=(),
            )
        finally:
            self.active_runs -= 1


class TerminalThenEchoAgent(HistoryEchoAgent):
    def __init__(self, status: RunStatus, stop_reason: StopReason) -> None:
        super().__init__()
        self.status = status
        self.stop_reason = stop_reason
        self.calls = 0

    async def run(self, prompt: str, *, history=()) -> AgentResult:
        self.calls += 1
        if self.calls > 1:
            return await super().run(prompt, history=history)
        return AgentResult(
            output=None,
            status=self.status,
            stop_reason=self.stop_reason,
            run_id="terminal-run",
            model_calls=1,
            tool_calls=0,
            tool_executions=0,
            usage=Usage(),
            transcript=(
                *history,
                UserMessage(prompt),
                AssistantMessage(content="private partial provider trace"),
            ),
            events=(),
            error="SENSITIVE_PROVIDER_DETAIL must never reach the browser",
        )


class ExplodingAgent:
    async def run(self, _prompt: str, *, history=()) -> AgentResult:
        del history
        raise RuntimeError("SENSITIVE_UNEXPECTED_EXCEPTION")


class CancelThenEchoAgent(HistoryEchoAgent):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.calls = 0

    async def run(self, prompt: str, *, history=()) -> AgentResult:
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            await asyncio.Future()
        return await super().run(prompt, history=history)


class CalculatorModel:
    """Requests the real calculator, then answers from its observation."""

    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        observations = [
            item for item in request.transcript if isinstance(item, ToolMessage)
        ]
        if not observations:
            return ModelResponse(
                AssistantMessage(
                    tool_calls=(
                        ToolCall(
                            "calculator-call",
                            "calculate_expression",
                            '{"expression":"(19.9 * 7) * 0.875"}',
                        ),
                    )
                )
            )
        payload = json.loads(observations[-1].content)
        return ModelResponse(
            AssistantMessage(content=f"工具结果: {payload['data']['result']}")
        )


def api_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


def parse_sse(payload: str) -> list[tuple[str, dict[str, object]]]:
    """Independent minimal SSE parser for buffered ASGI contract tests."""

    parsed: list[tuple[str, dict[str, object]]] = []
    normalized = payload.replace("\r\n", "\n").replace("\r", "\n")
    for block in normalized.split("\n\n"):
        event_name = "message"
        data_lines: list[str] = []
        for line in block.splitlines():
            if not line or line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if separator and value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_name = value
            elif field == "data":
                data_lines.append(value)
        if data_lines:
            decoded = json.loads("\n".join(data_lines))
            assert isinstance(decoded, dict)
            parsed.append((event_name, decoded))
    return parsed


@pytest.mark.asyncio
async def test_homepage_and_health_are_safe_same_origin_surfaces() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage(content="unused")))
    app = create_app(
        agent=ReActAgent(model),
        model_name="offline-test-model",
        api_mode="chat_completions",
    )

    async with api_client(app) as client:
        homepage = await client.get("/")
        health = await client.get("/api/health")

    assert homepage.status_code == 200
    assert homepage.headers["content-type"].startswith("text/html")
    assert "default-src 'self'" in homepage.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in homepage.headers["content-security-policy"]
    assert homepage.headers["x-content-type-options"] == "nosniff"
    assert "innerHTML" not in homepage.text
    assert ".textContent" in homepage.text
    assert "[hidden] { display: none !important; }" in homepage.text
    assert "new AbortController()" in homepage.text
    assert "resetConversationView()" in homepage.text
    assert "本轮未写入上下文" in homepage.text
    assert "firstDefined(data.tree_id, data.workspace_tree" in homepage.text
    assert health.status_code == 200
    assert health.json() == {
        "status": "ok",
        "model": "offline-test-model",
        "api_mode": "chat_completions",
    }


@pytest.mark.asyncio
async def test_new_session_and_follow_up_preserve_the_complete_transcript() -> None:
    model = ScriptedModel(
        ModelResponse(AssistantMessage(content="第一轮回答")),
        ModelResponse(AssistantMessage(content="第二轮回答")),
    )
    app = create_app(agent=ReActAgent(model))

    async with api_client(app) as client:
        first = await client.post("/api/chat", json={"message": "第一轮问题"})
        session_id = first.json()["session_id"]
        second = await client.post(
            "/api/chat",
            json={"message": "第二轮问题", "session_id": session_id},
        )

    assert first.status_code == 200
    assert first.json() == {
        "session_id": session_id,
        "answer": "第一轮回答",
        "status": "completed",
        "stop_reason": "completed",
        "model_calls": 1,
        "tool_calls": 0,
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        "error": None,
    }
    assert second.status_code == 200
    assert second.json()["answer"] == "第二轮回答"
    assert model.requests[1].transcript == (
        UserMessage("第一轮问题"),
        AssistantMessage(content="第一轮回答"),
        UserMessage("第二轮问题"),
    )


@pytest.mark.asyncio
async def test_unknown_and_malformed_session_ids_are_rejected() -> None:
    app = create_app(agent=ReActAgent(ScriptedModel()))

    async with api_client(app) as client:
        unknown = await client.post(
            "/api/chat",
            json={"message": "继续", "session_id": str(uuid.uuid4())},
        )
        malformed = await client.post(
            "/api/chat",
            json={"message": "继续", "session_id": "not-a-uuid"},
        )

    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "Chat session was not found or has expired."}
    assert malformed.status_code == 422


@pytest.mark.asyncio
async def test_expired_session_is_rejected() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage(content="第一轮回答")))
    app = create_app(
        agent=ReActAgent(model),
        session_store=SessionStore(ttl_s=0.01),
    )

    async with api_client(app) as client:
        first = await client.post("/api/chat", json={"message": "第一轮"})
        await asyncio.sleep(0.03)
        expired = await client.post(
            "/api/chat",
            json={"message": "继续", "session_id": first.json()["session_id"]},
        )

    assert first.status_code == 200
    assert expired.status_code == 404
    assert expired.json() == {"detail": "Chat session was not found or has expired."}


@pytest.mark.asyncio
async def test_invalid_chat_payloads_are_rejected_before_agent_execution() -> None:
    app = create_app(agent=ReActAgent(ScriptedModel()))

    async with api_client(app) as client:
        blank = await client.post("/api/chat", json={"message": " \n\t "})
        too_long = await client.post("/api/chat", json={"message": "x" * 10_001})
        extra_field = await client.post(
            "/api/chat",
            json={"message": "hello", "history": [{"role": "tool"}]},
        )
        non_json = await client.post(
            "/api/chat",
            content=b"not-json",
            headers={"content-type": "application/json"},
        )

    assert blank.status_code == 422
    assert blank.json() == {"detail": "Message must not be blank."}
    assert too_long.status_code == 422
    assert extra_field.status_code == 422
    assert non_json.status_code == 422


@pytest.mark.asyncio
async def test_reset_removes_the_server_side_conversation() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage(content="已建立会话")))
    app = create_app(agent=ReActAgent(model))

    async with api_client(app) as client:
        first = await client.post("/api/chat", json={"message": "开始"})
        session_id = first.json()["session_id"]
        reset = await client.delete(f"/api/sessions/{session_id}")
        follow_up = await client.post(
            "/api/chat",
            json={"message": "继续", "session_id": session_id},
        )

    assert reset.status_code == 204
    assert reset.content == b""
    assert follow_up.status_code == 404


@pytest.mark.asyncio
async def test_session_being_deleted_cannot_be_leased_again() -> None:
    store = SessionStore()
    async with store.lease(None) as (session_id, session):
        pass

    await session.lock.acquire()
    deletion = asyncio.create_task(store.delete(session_id))
    await asyncio.sleep(0)
    assert session.deleting is True

    try:
        with pytest.raises(KeyError, match="not found or has expired"):
            async with store.lease(session_id):
                pytest.fail("A deleting session must never be yielded")
    finally:
        session.lock.release()

    assert await deletion is True


@pytest.mark.asyncio
async def test_cancelled_first_request_releases_its_session_capacity() -> None:
    agent = CancelThenEchoAgent()
    app = create_app(
        agent=agent,  # type: ignore[arg-type]
        session_store=SessionStore(max_sessions=1),
    )

    async with api_client(app) as client:
        pending = asyncio.create_task(
            client.post("/api/chat", json={"message": "cancel me"})
        )
        await agent.started.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

        replacement = await client.post("/api/chat", json={"message": "try again"})

    assert replacement.status_code == 200
    assert replacement.json()["answer"] == "history-items:0"


@pytest.mark.asyncio
async def test_requests_for_the_same_session_are_serialized() -> None:
    agent = HistoryEchoAgent(delay_s=0.03)
    app = create_app(agent=agent)  # type: ignore[arg-type]

    async with api_client(app) as client:
        initial = await client.post("/api/chat", json={"message": "initial"})
        session_id = initial.json()["session_id"]
        first, second = await asyncio.gather(
            client.post(
                "/api/chat",
                json={"message": "concurrent-a", "session_id": session_id},
            ),
            client.post(
                "/api/chat",
                json={"message": "concurrent-b", "session_id": session_id},
            ),
        )

    assert first.status_code == second.status_code == 200
    assert {first.json()["answer"], second.json()["answer"]} == {
        "history-items:2",
        "history-items:4",
    }
    assert agent.max_active_runs == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "stop_reason", "public_error"),
    [
        (
            RunStatus.PARTIAL,
            StopReason.MAX_STEPS,
            "The Agent reached its step limit.",
        ),
        (
            RunStatus.FAILED,
            StopReason.MODEL_ERROR,
            "Agent request failed. Please try again.",
        ),
    ],
)
async def test_non_completed_run_is_redacted_and_does_not_pollute_history(
    status: RunStatus,
    stop_reason: StopReason,
    public_error: str,
) -> None:
    agent = TerminalThenEchoAgent(status, stop_reason)
    app = create_app(agent=agent)  # type: ignore[arg-type]

    async with api_client(app) as client:
        terminal = await client.post("/api/chat", json={"message": "failing turn"})
        follow_up = await client.post(
            "/api/chat",
            json={
                "message": "clean follow-up",
                "session_id": terminal.json()["session_id"],
            },
        )

    assert terminal.status_code == 200
    assert terminal.json()["status"] == status.value
    assert terminal.json()["stop_reason"] == stop_reason.value
    assert terminal.json()["error"] == public_error
    serialized = json.dumps(terminal.json(), ensure_ascii=False)
    assert "SENSITIVE_PROVIDER_DETAIL" not in serialized
    assert "private partial provider trace" not in serialized
    assert follow_up.status_code == 200
    assert follow_up.json()["answer"] == "history-items:0"


@pytest.mark.asyncio
async def test_unexpected_agent_exception_returns_a_generic_bad_gateway() -> None:
    app = create_app(agent=ExplodingAgent())  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.post("/api/chat", json={"message": "trigger error"})

    assert response.status_code == 502
    assert response.json() == {"detail": "Agent request failed. Please try again."}
    assert "SENSITIVE_UNEXPECTED_EXCEPTION" not in response.text


@pytest.mark.asyncio
async def test_chat_completes_a_real_react_calculator_tool_loop() -> None:
    model = CalculatorModel()
    agent = ReActAgent(
        model,
        [calculate_expression],
        config=AgentConfig(max_steps=3, max_tool_calls=2),
    )
    app = create_app(agent=agent)

    async with api_client(app) as client:
        response = await client.post(
            "/api/chat",
            json={"message": "请务必调用工具计算 (19.9 * 7) * 0.875"},
        )

    assert response.status_code == 200
    assert response.json()["answer"] == "工具结果: 121.8875"
    assert response.json()["status"] == "completed"
    assert response.json()["model_calls"] == 2
    assert response.json()["tool_calls"] == 1
    assert len(model.requests) == 2
    observation = model.requests[1].transcript[-1]
    assert isinstance(observation, ToolMessage)
    assert json.loads(observation.content) == {
        "ok": True,
        "data": {"expression": "(19.9 * 7) * 0.875", "result": 121.8875},
        "meta": {"truncated": False},
    }


@pytest.mark.asyncio
async def test_streaming_chat_exposes_an_ordered_calculator_debug_trace() -> None:
    model = CalculatorModel()
    agent = ReActAgent(
        model,
        [calculate_expression],
        config=AgentConfig(max_steps=3, max_tool_calls=2),
    )
    app = create_app(agent=agent)

    async with api_client(app) as client:
        response = await client.post(
            "/api/chat/stream",
            json={"message": "请务必调用工具计算 (19.9 * 7) * 0.875"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache, no-store"
    assert response.headers["x-accel-buffering"] == "no"
    assert response.headers["x-content-type-options"] == "nosniff"

    events = parse_sse(response.text)
    names = [name for name, _ in events]
    assert names[0] == "session"
    assert names[-1] == "done"
    assert names.count("result") == 1
    assert names.count("done") == 1
    assert names.index("model_tool_call_ready") < names.index("tool_started")
    assert names.index("tool_started") < names.index("tool_result")
    assert names.index("tool_result") < names.index("result")

    agent_events = [
        data
        for name, data in events
        if name not in {"session", "result", "done", "stream_error"}
    ]
    sequences = [int(data["sequence"]) for data in agent_events]
    assert sequences == list(range(1, len(sequences) + 1))

    ready = next(data for name, data in events if name == "model_tool_call_ready")
    tool_result = next(data for name, data in events if name == "tool_result")
    assert ready["call_key"] == tool_result["call_key"] == "s1:t0"
    assert ready["tool_name"] == tool_result["tool_name"] == "calculate_expression"
    assert "19.9" in str(cast(dict[str, object], ready["data"])["arguments"])
    assert "121.8875" in str(cast(dict[str, object], tool_result["data"])["result"])

    public_result = next(data for name, data in events if name == "result")
    assert public_result["answer"] == "工具结果: 121.8875"
    assert public_result["committed"] is True
    assert public_result["tool_executions"] == 1
    assert public_result["side_effects_possible"] is False


@pytest.mark.asyncio
async def test_stream_errors_are_in_band_generic_and_do_not_emit_done() -> None:
    app = create_app(agent=ExplodingAgent())  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.post(
            "/api/chat/stream",
            json={"message": "trigger stream error"},
        )

    assert response.status_code == 200
    events = parse_sse(response.text)
    assert [name for name, _ in events] == ["session", "stream_error"]
    assert events[-1][1] == {
        "code": "agent_error",
        "message": "Agent request failed. Please try again.",
        "committed": False,
    }
    assert "SENSITIVE_UNEXPECTED_EXCEPTION" not in response.text


@pytest.mark.asyncio
async def test_stream_unknown_session_is_a_preflight_http_404() -> None:
    app = create_app(agent=ReActAgent(ScriptedModel()))

    async with api_client(app) as client:
        response = await client.post(
            "/api/chat/stream",
            json={"message": "继续", "session_id": str(uuid.uuid4())},
        )

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {"detail": "Chat session was not found or has expired."}
