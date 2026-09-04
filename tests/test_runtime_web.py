from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import replace
from typing import ClassVar, cast

import httpx
import pytest

import react_agent.web as web_module
from react_agent.agent import ReActAgent
from react_agent.context import ContextStrategy
from react_agent.events import (
    PendingAction,
    PendingKind,
    RunCounts,
    RunSnapshot,
    RunState,
    ToolRecovery,
)
from react_agent.journal import InMemoryRunJournal
from react_agent.models import (
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ModelStreamEvent,
    ModelStreamEventKind,
    Usage,
)
from react_agent.runtime import (
    AgentRuntime,
    CancelRun,
    ForkRun,
    ReconciliationRequired,
    ResolveRun,
    ResumeRejected,
    ResumeRun,
    RunCommand,
    RunHandle,
    RuntimeConflict,
    RuntimeEvent,
    RuntimeNotFound,
    StartRun,
)
from react_agent.telemetry import MetricCardinalityPolicy, NoOpTelemetry
from react_agent.web import _runtime_event_stream, create_app


class FakePostgresJournal:
    instances: ClassVar[list[FakePostgresJournal]] = []

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.opened = False
        self.closed = False
        self.instances.append(self)

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.closed = True


def snapshot(
    *,
    run_id: str = "run-1",
    session_id: str = "session-1",
) -> RunSnapshot:
    return RunSnapshot(
        run_id=run_id,
        session_id=session_id,
        execution_id="execution-1",
        agent_revision="revision-1",
        tool_manifest_hash="manifest-1",
        state=RunState.RUNNING,
        status=None,
        stop_reason=None,
        transcript=({"content": "PRIVATE_TRANSCRIPT"},),
        usage=Usage(input_tokens=3, output_tokens=2, total_tokens=5),
        counts=RunCounts(model_calls=1, tool_calls=1, tool_executions=1),
        pending={
            "s1:t0": PendingAction(
                key="s1:t0",
                kind=PendingKind.RECONCILIATION,
                sequence=4,
                operation_id="PRIVATE_OPERATION_ID",
                step=1,
                call_key="s1:t0",
                phase="uncertain",
            )
        },
        safe_checkpoint_sequences=(3,),
        terminal=None,
        last_sequence=4,
        last_hash="a" * 64,
        last_step=1,
        executions=("execution-1",),
        model_attempts={1: 1},
        tools={
            "s1:t0": ToolRecovery(
                call_key="s1:t0",
                step=1,
                phase="uncertain",
                attempts=1,
                tool_call_id="tool-1",
                tool_name="calculator",
                resume_policy="require_operator",
                call={"arguments": "PRIVATE_TOOL_ARGUMENTS"},
                message={"content": "PRIVATE_TOOL_RESULT"},
            )
        },
        loop_counts={"fingerprint": 1},
        costs=(
            {
                "record_id": "cost-1",
                "currency": "USD",
                "amount_micros": 10_000,
                "model": "gpt-test",
            },
        ),
        workspace={"tree": "public-tree"},
        result={
            "output": "public final answer",
            "transcript": ["PRIVATE_RESULT_TRANSCRIPT"],
        },
        session_version=2,
    )


class FakeRuntime:
    def __init__(self) -> None:
        self.commands: list[object] = []
        self.snapshots: dict[str, RunSnapshot] = {"run-1": snapshot()}
        self.session_runs: tuple[RunSnapshot, ...] = (snapshot(),)
        self.events: list[RuntimeEvent] = []
        self.follow_calls: list[tuple[str, int, bool]] = []
        self.follower_closed = False
        self.submit_error: Exception | None = None

    async def submit(self, command: RunCommand) -> RunHandle:
        self.commands.append(command)
        if self.submit_error is not None:
            raise self.submit_error
        return RunHandle(
            run_id="run-1",
            session_id="session-1",
            execution_id="execution-1",
            created=True,
        )

    async def load(self, run_id: str) -> RunSnapshot:
        try:
            return self.snapshots[run_id]
        except KeyError:
            raise RuntimeNotFound(f"run not found: {run_id}") from None

    async def list_session_runs(self, session_id: str) -> tuple[RunSnapshot, ...]:
        if session_id == "missing":
            raise RuntimeNotFound(f"session not found: {session_id}")
        return self.session_runs

    async def follow(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        live: bool = True,
    ):
        self.follow_calls.append((run_id, after_sequence, live))
        try:
            for event in self.events:
                yield event
                if event.terminal:
                    return
            if live and self.events:
                await asyncio.Future()
        finally:
            self.follower_closed = True


class GatedStreamingModel:
    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(AssistantMessage(content="流式完成"))

    async def complete_stream(self, request: ModelRequest, sink) -> ModelResponse:
        outcome = sink(
            ModelStreamEvent(
                ModelStreamEventKind.TEXT_DELTA,
                "流式",
            )
        )
        if inspect.isawaitable(outcome):
            await outcome
        await self.release.wait()
        outcome = sink(
            ModelStreamEvent(
                ModelStreamEventKind.TEXT_DELTA,
                "完成",
            )
        )
        if inspect.isawaitable(outcome):
            await outcome
        return await self.complete(request)


class CloseableStreamingModel(GatedStreamingModel):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def api_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


def raw_sse_frames(payload: str) -> list[str]:
    normalized = payload.replace("\r\n", "\n").replace("\r", "\n")
    return [block for block in normalized.split("\n\n") if block and not block.startswith(":")]


def sse_data(frame: str) -> dict[str, object]:
    line = next(line for line in frame.splitlines() if line.startswith("data: "))
    value = json.loads(line.removeprefix("data: "))
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def test_web_metric_allowlist_defaults_to_active_model_and_registered_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS", raising=False)
    monkeypatch.delenv("REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS", raising=False)
    agent = ReActAgent(GatedStreamingModel(), [web_module.calculate_expression])

    policy = web_module._build_metric_cardinality_policy(
        agent,
        "gpt-5.6-terra",
    )

    assert policy.allowed_models == frozenset({"gpt-5.6-terra"})
    assert policy.allowed_tools == frozenset({"calculate_expression"})


def test_web_metric_allowlist_override_is_an_exact_finite_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS",
        "gpt-5.6-terra, gpt-5.6-terra-canary",
    )
    monkeypatch.setenv(
        "REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS",
        "calculate_expression,lookup",
    )

    policy = web_module._build_metric_cardinality_policy(
        ReActAgent(GatedStreamingModel()),
        "ignored-default",
    )

    assert policy.allowed_models == frozenset(
        {"gpt-5.6-terra", "gpt-5.6-terra-canary"}
    )
    assert policy.allowed_tools == frozenset({"calculate_expression", "lookup"})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS", "*"),
        ("REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS", "calculate_expression,*"),
        ("REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS", ""),
        ("REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS", "calculate_expression,"),
    ],
)
def test_web_metric_allowlist_rejects_wildcards_and_empty_entries(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.delenv("REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS", raising=False)
    monkeypatch.delenv("REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS", raising=False)
    monkeypatch.setenv(name, value)

    with pytest.raises(RuntimeError, match=r"exact|one or more"):
        web_module._build_metric_cardinality_policy(
            ReActAgent(GatedStreamingModel(), [web_module.calculate_expression]),
            "gpt-5.6-terra",
        )


def test_web_metric_allowlist_rejects_more_than_the_bounded_maximum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS",
        ",".join(f"model-{index}" for index in range(65)),
    )
    monkeypatch.delenv("REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS", raising=False)

    with pytest.raises(RuntimeError, match="at most 64"):
        web_module._build_metric_cardinality_policy(
            ReActAgent(GatedStreamingModel()),
            "gpt-5.6-terra",
        )


@pytest.mark.asyncio
async def test_owned_web_runtime_passes_bounded_metric_policy_to_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "REACT_AGENT_POSTGRES_DSN",
        "DATABASE_URL",
        "REACT_AGENT_REPOSITORY",
        "REACT_AGENT_WORKTREE_ROOT",
        "REACT_AGENT_OTEL_METRIC_ALLOWED_MODELS",
        "REACT_AGENT_OTEL_METRIC_ALLOWED_TOOLS",
    ):
        monkeypatch.delenv(name, raising=False)
    model = CloseableStreamingModel()
    agent = ReActAgent(model, [web_module.calculate_expression])
    monkeypatch.setattr(
        web_module,
        "_build_agent_from_env",
        lambda **_: (model, agent, "gpt-5.6-terra", "chat_completions"),
    )
    policies: list[MetricCardinalityPolicy] = []

    def fake_create_telemetry(
        *, cardinality: MetricCardinalityPolicy | None = None
    ) -> NoOpTelemetry:
        assert cardinality is not None
        policies.append(cardinality)
        return NoOpTelemetry()

    monkeypatch.setattr(web_module, "create_telemetry", fake_create_telemetry)
    app = create_app()

    async with app.router.lifespan_context(app):
        assert isinstance(app.state.runtime, AgentRuntime)

    [policy] = policies
    assert policy.allowed_models == frozenset({"gpt-5.6-terra"})
    assert policy.allowed_tools == frozenset({"calculate_expression"})
    assert model.closed is True


@pytest.mark.asyncio
async def test_journal_factory_defaults_to_memory_without_a_database_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REACT_AGENT_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    journal, owned_postgres = await web_module._build_journal_from_env()

    assert isinstance(journal, InMemoryRunJournal)
    assert owned_postgres is None


@pytest.mark.asyncio
async def test_database_url_is_supported_as_a_postgres_dsn_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakePostgresJournal.instances.clear()
    monkeypatch.delenv("REACT_AGENT_POSTGRES_DSN", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://platform.internal/agent")
    monkeypatch.setattr(web_module, "PostgresRunJournal", FakePostgresJournal)

    journal, owned_postgres = await web_module._build_journal_from_env()

    assert journal is owned_postgres
    assert owned_postgres is not None
    assert owned_postgres.dsn == "postgresql://platform.internal/agent"
    await owned_postgres.close()


@pytest.mark.asyncio
async def test_explicit_postgres_dsn_wins_and_pool_follows_app_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakePostgresJournal.instances.clear()
    monkeypatch.setenv(
        "REACT_AGENT_POSTGRES_DSN",
        "postgresql://explicit-user:PRIVATE_PASSWORD@db.internal/agent",
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://fallback.invalid/ignored")
    monkeypatch.setattr(web_module, "PostgresRunJournal", FakePostgresJournal)

    model = CloseableStreamingModel()
    agent = ReActAgent(model)
    monkeypatch.setattr(
        web_module,
        "_build_agent_from_env",
        lambda **_: (model, agent, "offline-runtime", "chat_completions"),
    )
    app = create_app()

    async with app.router.lifespan_context(app):
        [journal] = FakePostgresJournal.instances
        assert journal.opened is True
        assert journal.closed is False
        assert journal.dsn.startswith("postgresql://explicit-user:")
        assert isinstance(app.state.runtime, AgentRuntime)

    assert journal.closed is True
    assert model.closed is True


@pytest.mark.asyncio
async def test_postgres_startup_errors_never_echo_the_dsn(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_dsn = "postgresql://user:VERY_PRIVATE_PASSWORD@db.invalid/agent"
    monkeypatch.setenv("REACT_AGENT_POSTGRES_DSN", secret_dsn)

    class ExplodingPostgresJournal(FakePostgresJournal):
        async def open(self) -> None:
            raise RuntimeError(f"driver failed for {self.dsn}")

    monkeypatch.setattr(web_module, "PostgresRunJournal", ExplodingPostgresJournal)

    with pytest.raises(RuntimeError) as captured:
        await web_module._build_journal_from_env()

    output = capsys.readouterr()
    assert str(captured.value) == "Unable to initialize the PostgreSQL journal."
    assert secret_dsn not in str(captured.value)
    assert secret_dsn not in output.out
    assert secret_dsn not in output.err
    [journal] = ExplodingPostgresJournal.instances[-1:]
    assert journal.closed is True


@pytest.mark.asyncio
async def test_start_run_forwards_idempotency_and_runtime_only_app_is_healthy() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime, model_name="runtime-model")  # type: ignore[arg-type]

    async with api_client(app) as client:
        health = await client.get("/api/health")
        response = await client.post(
            "/api/runs",
            headers={"Idempotency-Key": "request-123"},
            json={"message": "  hello  ", "session_id": "session-1"},
        )

    assert health.status_code == 200
    assert health.json()["model"] == "runtime-model"
    assert response.status_code == 202
    assert response.json() == {
        "run_id": "run-1",
        "session_id": "session-1",
        "execution_id": "execution-1",
        "created": True,
    }
    command = runtime.commands[-1]
    assert isinstance(command, StartRun)
    assert command == StartRun(
        prompt="hello",
        session_id="session-1",
        idempotency_key="request-123",
    )


@pytest.mark.asyncio
async def test_real_runtime_streams_model_deltas_and_publishes_final_snapshot() -> None:
    model = GatedStreamingModel()
    runtime = AgentRuntime(ReActAgent(model), InMemoryRunJournal())
    app = create_app(runtime=runtime, model_name="offline-runtime")

    try:
        async with api_client(app) as client:
            started = await client.post(
                "/api/runs",
                json={"message": "请流式回答", "session_id": "integration-session"},
            )
            assert started.status_code == 202
            run_id = started.json()["run_id"]

            following = asyncio.create_task(
                client.get(f"/api/runs/{run_id}/events?follow=true")
            )
            await asyncio.sleep(0.02)
            model.release.set()
            response = await asyncio.wait_for(following, timeout=2.0)
            final_snapshot = await client.get(f"/api/runs/{run_id}")
    finally:
        await runtime.close()

    assert response.status_code == 200
    frames = raw_sse_frames(response.text)
    payloads = [sse_data(frame) for frame in frames]
    streamed_text = "".join(
        str(cast(dict[str, object], payload["data"]).get("delta", ""))
        for payload in payloads
        if payload["kind"] == "model_text_delta"
    )
    assert streamed_text == "流式完成"
    assert any(payload["kind"] == "run_completed" for payload in payloads)
    assert final_snapshot.status_code == 200
    assert final_snapshot.json()["answer"] == "流式完成"
    assert final_snapshot.json()["terminal"] is True


@pytest.mark.asyncio
async def test_legacy_json_chat_uses_runtime_and_preserves_response_contract() -> None:
    model = GatedStreamingModel()
    model.release.set()
    runtime = AgentRuntime(ReActAgent(model), InMemoryRunJournal())
    app = create_app(runtime=runtime, model_name="offline-runtime")

    try:
        async with api_client(app) as client:
            first = await client.post("/api/chat", json={"message": "第一轮"})
            session_id = first.json()["session_id"]
            second = await client.post(
                "/api/chat",
                json={"message": "第二轮", "session_id": session_id},
            )
            unknown = await client.post(
                "/api/chat",
                json={
                    "message": "未知会话",
                    "session_id": "9f5f4eb0-2643-4c12-b038-a76130b1e78e",
                },
            )
    finally:
        await runtime.close()

    assert first.status_code == second.status_code == 200
    assert first.json() == {
        "session_id": session_id,
        "answer": "流式完成",
        "status": "completed",
        "stop_reason": "completed",
        "model_calls": 1,
        "tool_calls": 0,
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        "error": None,
    }
    assert second.json()["session_id"] == session_id
    assert len(model.requests[1].transcript) == 3
    assert unknown.status_code == 404
    assert unknown.json() == {
        "detail": "Chat session was not found or has expired."
    }


@pytest.mark.asyncio
async def test_legacy_new_session_retry_is_idempotent_with_a_header_key() -> None:
    model = GatedStreamingModel()
    model.release.set()
    runtime = AgentRuntime(ReActAgent(model), InMemoryRunJournal())
    app = create_app(runtime=runtime, model_name="offline-runtime")
    headers = {"Idempotency-Key": "legacy-request-1"}

    try:
        async with api_client(app) as client:
            first = await client.post(
                "/api/chat",
                headers=headers,
                json={"message": "只执行一次"},
            )
            retried = await client.post(
                "/api/chat",
                headers=headers,
                json={"message": "只执行一次"},
            )
    finally:
        await runtime.close()

    assert first.status_code == retried.status_code == 200
    assert first.json() == retried.json()
    assert len(model.requests) == 1


@pytest.mark.asyncio
async def test_legacy_stream_chat_is_a_runtime_live_projection() -> None:
    model = GatedStreamingModel()
    model.release.set()
    runtime = AgentRuntime(ReActAgent(model), InMemoryRunJournal())
    app = create_app(runtime=runtime, model_name="offline-runtime")

    try:
        async with api_client(app) as client:
            response = await client.post(
                "/api/chat/stream",
                json={"message": "流式兼容"},
            )
    finally:
        await runtime.close()

    assert response.status_code == 200
    frames = raw_sse_frames(response.text)
    names = [
        next(
            line.removeprefix("event: ")
            for line in frame.splitlines()
            if line.startswith("event: ")
        )
        for frame in frames
    ]
    assert names[0] == "session"
    assert names[-2:] == ["result", "done"]
    live_payloads = [
        sse_data(frame)
        for name, frame in zip(names, frames, strict=True)
        if name not in {"session", "result", "done"}
    ]
    assert [int(payload["sequence"]) for payload in live_payloads] == list(
        range(1, len(live_payloads) + 1)
    )
    assert "".join(
        str(cast(dict[str, object], payload["data"]).get("delta", ""))
        for payload in live_payloads
        if payload["kind"] == "model_text_delta"
    ) == "流式完成"
    result = sse_data(frames[-2])
    assert result["answer"] == "流式完成"
    assert result["committed"] is True


@pytest.mark.asyncio
async def test_invalid_idempotency_key_is_rejected_before_submit() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.post(
            "/api/runs",
            headers={"Idempotency-Key": "contains spaces"},
            json={"prompt": "hello"},
        )

    assert response.status_code == 400
    assert runtime.commands == []


@pytest.mark.asyncio
async def test_cost_adjustment_rejects_values_above_postgres_bigint() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.post(
            "/api/runs/run-1/cost-adjustments",
            json={
                "previous_record_id": "cost-1",
                "revised_total_microunits": 9_223_372_036_854_775_808,
            },
        )

    assert response.status_code == 422
    assert runtime.commands == []


@pytest.mark.asyncio
async def test_snapshot_and_session_history_are_explicitly_redacted() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        run_response = await client.get("/api/runs/run-1")
        history_response = await client.get("/api/sessions/session-1/runs")

    assert run_response.status_code == history_response.status_code == 200
    run = run_response.json()
    assert run["last_sequence"] == 4
    assert run["usage"]["total_tokens"] == 5
    assert run["cost_microunits"] == 10_000
    assert run["currency"] == "USD"
    assert run["cost_unknown_reason"] is None
    assert run["tools"]["s1:t0"]["phase"] == "uncertain"
    assert run["workspace"] == {"tree": "public-tree"}
    assert run["answer"] == "public final answer"
    serialized = run_response.text + history_response.text
    for private_value in (
        "PRIVATE_TRANSCRIPT",
        "PRIVATE_OPERATION_ID",
        "PRIVATE_TOOL_ARGUMENTS",
        "PRIVATE_TOOL_RESULT",
        "PRIVATE_RESULT_TRANSCRIPT",
    ):
        assert private_value not in serialized
    assert '"transcript"' not in serialized
    assert '"checkpoint"' not in serialized


@pytest.mark.asyncio
async def test_snapshot_cost_total_stays_unknown_until_an_adjustment_resolves_it() -> None:
    runtime = FakeRuntime()
    unknown = {
        "record_id": "abandoned-model",
        "currency": "USD",
        "amount_micros": None,
        "unknown_reason": "provider_completion_not_committed",
    }
    known = {
        "record_id": "retry-model",
        "currency": "USD",
        "amount_micros": 250,
    }
    runtime.snapshots["run-1"] = replace(snapshot(), costs=(unknown, known))
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        unresolved = await client.get("/api/runs/run-1")
        runtime.snapshots["run-1"] = replace(
            snapshot(),
            costs=(
                unknown,
                known,
                {
                    "record_id": "invoice-adjustment",
                    "adjusts_record_id": "abandoned-model",
                    "currency": "USD",
                    "amount_micros": 125,
                },
            ),
        )
        resolved = await client.get("/api/runs/run-1")

    assert unresolved.json()["cost_microunits"] is None
    assert (
        unresolved.json()["cost_unknown_reason"]
        == "provider_completion_not_committed"
    )
    assert resolved.json()["cost_microunits"] == 375
    assert resolved.json()["cost_unknown_reason"] is None


@pytest.mark.asyncio
async def test_cost_adjustment_api_refreshes_run_and_session_without_new_run_sequence() -> None:
    model = GatedStreamingModel()
    model.release.set()
    journal = InMemoryRunJournal()
    runtime = AgentRuntime(ReActAgent(model), journal)
    app = create_app(runtime=runtime, model_name="offline-runtime")

    try:
        async with api_client(app) as client:
            started = await client.post(
                "/api/runs",
                headers={"Idempotency-Key": "cost-run-request"},
                json={"prompt": "price this", "session_id": "cost-session"},
            )
            run_id = str(started.json()["run_id"])
            await runtime.wait(run_id, timeout_s=2)
            before = await client.get(f"/api/runs/{run_id}")
            [base_cost] = before.json()["costs"]
            last_sequence = int(before.json()["last_sequence"])
            request_body = {
                "previous_record_id": base_cost["record_id"],
                "revised_total_microunits": 450,
                "note": "provider invoice",
            }
            adjusted = await client.post(
                f"/api/runs/{run_id}/cost-adjustments",
                headers={"Idempotency-Key": "invoice-line-1"},
                json=request_body,
            )
            refreshed = await client.get(f"/api/runs/{run_id}")
            history = await client.get("/api/sessions/cost-session/runs")
            retried = await client.post(
                f"/api/runs/{run_id}/cost-adjustments",
                headers={"Idempotency-Key": "invoice-line-1"},
                json=request_body,
            )
            conflict = await client.post(
                f"/api/runs/{run_id}/cost-adjustments",
                headers={"Idempotency-Key": "invoice-line-1"},
                json={**request_body, "revised_total_microunits": 451},
            )
    finally:
        await runtime.close()

    assert started.status_code == 202
    assert before.json()["terminal"] is True
    assert before.json()["cost_microunits"] is None
    assert adjusted.status_code == 201
    assert adjusted.json()["adjustment"]["created"] is True
    assert adjusted.json()["adjustment"]["record"]["note"] == "provider invoice"
    assert adjusted.json()["last_sequence"] == last_sequence
    assert adjusted.json()["cost_microunits"] == 450
    assert refreshed.json()["costs"] == adjusted.json()["costs"]
    assert refreshed.json()["last_sequence"] == last_sequence
    [history_run] = history.json()["runs"]
    assert history_run["costs"] == adjusted.json()["costs"]
    assert history_run["cost_microunits"] == 450
    assert retried.status_code == 200
    assert retried.json()["adjustment"]["created"] is False
    assert len(retried.json()["costs"]) == 2
    assert conflict.status_code == 409
    durable = await journal.read(run_id)
    assert durable[-1].sequence == last_sequence
    assert all(event.kind.value != "cost_adjusted" for event in durable)


@pytest.mark.asyncio
async def test_sse_uses_max_cursor_and_only_durable_events_receive_ids() -> None:
    runtime = FakeRuntime()
    runtime.events = [
        RuntimeEvent(
            run_id="run-1",
            kind="model_text_delta",
            timestamp=1.0,
            public_data={"delta": "你"},
            live_sequence=8,
            step=1,
        ),
        RuntimeEvent(
            run_id="run-1",
            kind="tool_completed",
            timestamp=2.0,
            public_data={"result": {"value": 42}},
            durable_sequence=9,
            safe_checkpoint=True,
            terminal=True,
            step=1,
            call_key="s1:t0",
        ),
    ]
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.get(
            "/api/runs/run-1/events?after_sequence=4&follow=true",
            headers={"Last-Event-ID": "7"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    frames = raw_sse_frames(response.text)
    assert len(frames) == 2
    assert not any(line.startswith("id:") for line in frames[0].splitlines())
    assert "id: 9" in frames[1].splitlines()
    live = sse_data(frames[0])
    durable = sse_data(frames[1])
    assert live["sequence"] is None
    assert live["live_sequence"] == 8
    assert live["data"] == {"delta": "你"}
    assert durable["durable_sequence"] == 9
    assert durable["safe_checkpoint"] is True
    assert durable["data"] == {"result": {"value": 42}}
    assert runtime.follow_calls == [("run-1", 7, True)]


@pytest.mark.asyncio
async def test_detaching_follower_closes_it_without_submitting_cancel() -> None:
    runtime = FakeRuntime()
    runtime.events = [
        RuntimeEvent(
            run_id="run-1",
            kind="model_text_delta",
            timestamp=1.0,
            public_data={"delta": "partial"},
            live_sequence=1,
        )
    ]
    stream = _runtime_event_stream(
        runtime,  # type: ignore[arg-type]
        "run-1",
        after_sequence=0,
        live=True,
    )

    first = await anext(stream)
    assert b"model_text_delta" in first
    await stream.aclose()

    assert runtime.follower_closed is True
    assert runtime.commands == []


@pytest.mark.asyncio
async def test_heartbeat_keeps_one_pending_follow_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    calls = 0
    closed = False

    class GatedRuntime(FakeRuntime):
        async def follow(self, run_id: str, *, after_sequence: int = 0, live: bool = True):
            nonlocal calls, closed
            del run_id, after_sequence, live
            calls += 1
            try:
                await gate.wait()
                yield RuntimeEvent(
                    run_id="run-1",
                    kind="model_text_delta",
                    timestamp=1.0,
                    public_data={"delta": "ready"},
                    live_sequence=1,
                )
            finally:
                closed = True

    monkeypatch.setattr("react_agent.web._SSE_HEARTBEAT_S", 0.001)
    stream = _runtime_event_stream(
        GatedRuntime(),  # type: ignore[arg-type]
        "run-1",
        after_sequence=0,
        live=True,
    )

    assert await anext(stream) == b": ping\n\n"
    assert calls == 1
    gate.set()
    assert b"ready" in await anext(stream)
    assert calls == 1
    await stream.aclose()
    assert closed is True


@pytest.mark.asyncio
async def test_runtime_action_routes_submit_keyword_commands() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        responses = [
            await client.post("/api/runs/run-1/resume"),
            await client.post(
                "/api/runs/run-1/fork",
                headers={"Idempotency-Key": "fork-1"},
                json={"from_sequence": 3, "session_id": "fork-session"},
            ),
            await client.post(
                "/api/runs/run-1/resolve",
                json={
                    "call_key": "s1:t0",
                    "action": "use_result",
                    "result": {"ok": True},
                },
            ),
            await client.post(
                "/api/runs/run-1/cancel",
                json={"reason": "operator_requested"},
            ),
        ]

    assert all(response.status_code == 202 for response in responses)
    assert runtime.commands == [
        ResumeRun(run_id="run-1"),
        ForkRun(
            run_id="run-1",
            from_sequence=3,
            session_id="fork-session",
            idempotency_key="fork-1",
        ),
        ResolveRun(
            run_id="run-1",
            call_key="s1:t0",
            action="use_result",
            result={"ok": True},
        ),
        CancelRun(run_id="run-1", reason="operator_requested"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status_code"),
    [
        (RuntimeNotFound("missing"), 404),
        (RuntimeConflict("busy"), 409),
        (ResumeRejected("unsafe"), 409),
        (ReconciliationRequired("operator required"), 409),
        (RuntimeError("PRIVATE_RUNTIME_FAILURE"), 502),
    ],
)
async def test_runtime_domain_errors_are_mapped_without_leaking_unexpected_failures(
    error: Exception,
    status_code: int,
) -> None:
    runtime = FakeRuntime()
    runtime.submit_error = error
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.post("/api/runs/run-1/resume")

    assert response.status_code == status_code
    if status_code == 502:
        assert "PRIVATE_RUNTIME_FAILURE" not in response.text


@pytest.mark.asyncio
async def test_missing_run_is_a_preflight_http_404_not_an_sse_error() -> None:
    runtime = FakeRuntime()
    app = create_app(runtime=runtime)  # type: ignore[arg-type]

    async with api_client(app) as client:
        response = await client.get("/api/runs/missing/events")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")


def test_repository_tools_are_registered_only_with_a_managed_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REACT_AGENT_COMMAND_APPROVAL", raising=False)
    names = [t.name for t in web_module._tools_from_env(workspace_enabled=False)]
    assert names == ["calculate_expression"]
    config = web_module._agent_config_from_env(ContextStrategy.TIERED, workspace_enabled=False)
    assert config.max_steps == 8

    monkeypatch.setenv("REACT_AGENT_COMMAND_APPROVAL", "true")
    tools = web_module._tools_from_env(workspace_enabled=True)
    assert [t.name for t in tools] == [
        "calculate_expression",
        "list_dir",
        "read_file",
        "search_text",
        "write_file",
        "edit_file",
        "run_tests",
        "run_command",
    ]
    run_command = next(t for t in tools if t.name == "run_command")
    assert run_command.requires_approval is True
    config = web_module._agent_config_from_env(ContextStrategy.TIERED, workspace_enabled=True)
    assert config.max_steps == 60 and config.max_wall_time_s == 3_600.0
