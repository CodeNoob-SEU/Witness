from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import react_agent.runtime as runtime_module
from react_agent.agent import ReActAgent
from react_agent.cost import Price, PricingCatalog
from react_agent.events import (
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    RunState,
    StoredRunEvent,
)
from react_agent.journal import (
    InMemoryRunJournal,
    JournalLease,
    LeaseConflictError,
    LeaseLostError,
    RunNotFoundError,
)
from react_agent.models import (
    AgentStreamEvent,
    AgentStreamEventKind,
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    Usage,
)
from react_agent.runtime import (
    AgentRuntime,
    CancelRun,
    ForkRun,
    InMemoryRuntimeStore,
    ReconciliationRequired,
    RequestPayloadConflict,
    ResolutionAction,
    ResolveRun,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)
from react_agent.telemetry import (
    RecordingTelemetry,
    TelemetryEvent,
    TelemetryEventKind,
    TraceReference,
)
from react_agent.tools import ToolExecutionContext, ToolResumePolicy, tool
from react_agent.workspace import FakeWorkspaceCheckpointStore


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            pytest.fail("unexpected provider call")
        return self.responses.popleft()


class BlockingModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class TraceCapturingTelemetry:
    def __init__(self, trace_digit: str) -> None:
        self.trace_digit = trace_digit
        self.events: list[TelemetryEvent] = []
        self.references: list[TraceReference] = []

    def emit(self, event: TelemetryEvent) -> TraceReference | None:
        self.events.append(event)
        if event.kind not in {
            TelemetryEventKind.RUN_STARTED,
            TelemetryEventKind.RUN_RESUMED,
        }:
            return None
        reference = TraceReference(
            run_id=str(event.attributes["run_id"]),
            execution_id=str(event.attributes["execution_id"]),
            trace_id=self.trace_digit * 32,
            span_id=f"{len(self.references) + 1:016x}",
            trace_flags=1,
        )
        self.references.append(reference)
        return reference


class FailingTraceProjectionStore(InMemoryRuntimeStore):
    async def load_trace_reference(
        self, run_id: str, execution_id: str
    ) -> TraceReference | None:
        del run_id, execution_id
        raise RuntimeError("trace projection unavailable")


class FailOneAnchoredVerificationWorkspace(FakeWorkspaceCheckpointStore):
    def __init__(self) -> None:
        super().__init__({"README.md": "baseline\n"})
        self.fail_next_anchored_verification = False

    def verify(self, session_id: str, *, checkpoint=None):
        verification = super().verify(session_id, checkpoint=checkpoint)
        if self.fail_next_anchored_verification and checkpoint is not None:
            self.fail_next_anchored_verification = False
            return replace(
                verification,
                valid=False,
                diverged=True,
                reasons=("simulated_preflight_failure",),
            )
        return verification

    async def put_trace_reference(self, reference: TraceReference) -> None:
        del reference
        raise RuntimeError("trace projection unavailable")


class _SimulatedProcessCrash(BaseException):
    """Bypass normal error handling like a kill between two commit points."""


class CrashAfterFirstReservationStore(InMemoryRuntimeStore):
    """Lose the first reservation ACK after its durable state was committed."""

    def __init__(self) -> None:
        super().__init__()
        self._armed = True

    async def reserve_request(
        self,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> object:
        reservation = await super().reserve_request(
            session_id,
            idempotency_key,
            request_hash,
            proposed_run_id,
        )
        if self._armed and reservation.created:
            self._armed = False
            raise _SimulatedProcessCrash
        return reservation


class CrashBeforeFirstLineageStore(InMemoryRuntimeStore):
    """Commit a Fork child run, then die before its registry lineage is stored."""

    def __init__(self) -> None:
        super().__init__()
        self._armed = True

    async def set_lineage(
        self,
        run_id: str,
        *,
        parent_run_id: str | None,
        fork_sequence: int | None,
        workspace_tree: str | None,
    ) -> None:
        if self._armed:
            self._armed = False
            raise _SimulatedProcessCrash
        await super().set_lineage(
            run_id,
            parent_run_id=parent_run_id,
            fork_sequence=fork_sequence,
            workspace_tree=workspace_tree,
        )


class CrashBeforeSessionEventJournal(InMemoryRunJournal):
    def __init__(self) -> None:
        super().__init__()
        self.session_event_attempted = asyncio.Event()
        self._armed = True

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        if self._armed and operation_id == "session:committed":
            self._armed = False
            self.session_event_attempted.set()
            raise _SimulatedProcessCrash
        return await super().append(
            run_id,
            draft,
            expected_sequence=expected_sequence,
            operation_id=operation_id,
            lease=lease,
        )


class SynchronizeMissingRunLoadsJournal(InMemoryRunJournal):
    """Make two recovery writers observe the same reservation/run gap."""

    def __init__(self) -> None:
        super().__init__()
        self._missing_loads = 0
        self._both_missing = asyncio.Event()

    async def load(self, run_id: str):
        try:
            return await super().load(run_id)
        except RunNotFoundError:
            self._missing_loads += 1
            if self._missing_loads == 2:
                self._both_missing.set()
            await asyncio.wait_for(self._both_missing.wait(), timeout=1)
            raise


class CrashNextRunCreateJournal(InMemoryRunJournal):
    """Crash immediately before the next run_started event is committed."""

    def __init__(self, *, armed: bool = True) -> None:
        super().__init__()
        self._armed = armed

    def arm(self) -> None:
        self._armed = True

    async def create(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        operation_id: str,
    ) -> StoredRunEvent:
        if self._armed:
            self._armed = False
            raise _SimulatedProcessCrash
        return await super().create(run_id, draft, operation_id=operation_id)


class CrashBeforeFirstAcquireJournal(InMemoryRunJournal):
    """Commit run_started, then die before any execution can own side effects."""

    def __init__(self, *, armed: bool = True) -> None:
        super().__init__()
        self._armed = armed

    def arm(self) -> None:
        self._armed = True

    async def acquire(
        self, run_id: str, *, owner: str, ttl_s: float
    ) -> JournalLease:
        if self._armed:
            self._armed = False
            raise _SimulatedProcessCrash
        return await super().acquire(run_id, owner=owner, ttl_s=ttl_s)


class PauseAfterAcquireJournal(InMemoryRunJournal):
    """Pause after the adapter owns a lease but before it returns the token."""

    def __init__(self) -> None:
        super().__init__()
        self.acquired = asyncio.Event()
        self.allow_return = asyncio.Event()
        self._armed = True

    async def acquire(
        self, run_id: str, *, owner: str, ttl_s: float
    ) -> JournalLease:
        lease = await super().acquire(run_id, owner=owner, ttl_s=ttl_s)
        if self._armed:
            self._armed = False
            self.acquired.set()
            await self.allow_return.wait()
        return lease


class DropLeaseOnRenewJournal(InMemoryRunJournal):
    def __init__(self) -> None:
        super().__init__()
        self.lease_dropped = asyncio.Event()
        self._armed = True
        self._renewals = 0

    async def renew(self, lease: JournalLease, *, ttl_s: float) -> JournalLease:
        self._renewals += 1
        if self._armed and self._renewals >= 2:
            self._armed = False
            await super().release(lease)
            self.lease_dropped.set()
            raise LeaseLostError("simulated fencing loss")
        return await super().renew(lease, ttl_s=ttl_s)


class FailToolIntentJournal(InMemoryRunJournal):
    def __init__(self) -> None:
        super().__init__()
        self.intent_failed = asyncio.Event()

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        if draft.kind is RunEventKind.TOOL_STARTED:
            self.intent_failed.set()
            raise RuntimeError("simulated tool intent journal outage")
        return await super().append(
            run_id,
            draft,
            expected_sequence=expected_sequence,
            operation_id=operation_id,
            lease=lease,
        )


class FailNextEventJournal(InMemoryRunJournal):
    """Inject one ordinary append failure at a selected durable event kind."""

    def __init__(self) -> None:
        super().__init__()
        self._next_failure: RunEventKind | None = None

    def fail_next(self, kind: RunEventKind) -> None:
        self._next_failure = kind

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        if draft.kind is self._next_failure:
            self._next_failure = None
            raise RuntimeError(f"simulated {draft.kind.value} append outage")
        return await super().append(
            run_id,
            draft,
            expected_sequence=expected_sequence,
            operation_id=operation_id,
            lease=lease,
        )


async def wait_for_kind(
    journal: InMemoryRunJournal,
    run_id: str,
    kind: RunEventKind,
) -> None:
    for _ in range(100):
        if any(event.kind is kind for event in await journal.read(run_id)):
            return
        await asyncio.sleep(0.005)
    pytest.fail(f"event {kind.value} was not committed")


def _stored_event(
    draft: RunEventDraft,
    *,
    run_id: str,
    operation_id: str,
) -> StoredRunEvent:
    return StoredRunEvent.from_draft(
        draft,
        run_id=run_id,
        sequence=1,
        operation_id=operation_id,
        previous_hash="0" * 64,
        occurred_at=1.0,
    )


def test_runtime_projects_committed_resume_ttfc_and_detailed_usage_to_telemetry() -> None:
    telemetry = RecordingTelemetry()
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        InMemoryRunJournal(),
        telemetry=telemetry,
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.RUN_RESUMED,
                execution_id="execution-2",
                data={"resume_reason": "process_restart"},
            ),
            run_id="run-resumed",
            operation_id="run:resumed:execution-2",
        )
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.MODEL_COMPLETED,
                execution_id="execution-2",
                step=1,
                data={
                    "provider": "openai_compatible",
                    "request_model": "gpt-test",
                    "ttfc_ms": 125.5,
                    "usage": {
                        # The canonical Usage delta, rather than a duplicate
                        # public payload, is authoritative for model telemetry.
                        "input_tokens": 999,
                    },
                },
                checkpoint={
                    "prompt": "private prompt must never be projected",
                    "reasoning": "private reasoning must never be projected",
                },
                privacy=PrivacyClass.PRIVATE,
                usage_delta=Usage(
                    input_tokens=10,
                    output_tokens=4,
                    total_tokens=14,
                    cached_input_tokens=3,
                    reasoning_output_tokens=2,
                    billable_tokens=11,
                ),
            ),
            run_id="run-model",
            operation_id="model:s1:completed",
        )
    )

    resumed, completed = telemetry.events
    assert resumed.kind is TelemetryEventKind.RUN_RESUMED
    assert completed.kind is TelemetryEventKind.MODEL_COMPLETED
    assert completed.attributes["ttfc_s"] == pytest.approx(0.1255)
    assert completed.attributes["input_tokens"] == 10
    assert completed.attributes["cached_input_tokens"] == 3
    assert completed.attributes["reasoning_output_tokens"] == 2
    assert completed.attributes["billable_tokens"] == 11
    assert "prompt" not in completed.attributes
    assert "reasoning" not in completed.attributes


def test_runtime_projects_stable_model_and_tool_lifecycle_identity() -> None:
    telemetry = RecordingTelemetry()
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        InMemoryRunJournal(),
        telemetry=telemetry,
        provider_name="openai_compatible",
        model_name="gpt-5.6-terra",
    )
    common = {"execution_id": "execution-1", "step": 3}
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                data={"attempt": 2},
                **common,
            ),
            run_id="run-lifecycle",
            operation_id="model:s3:started:execution-1",
        )
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.MODEL_COMPLETED,
                data={"attempt": 2, "outcome": "completed"},
                **common,
            ),
            run_id="run-lifecycle",
            operation_id="model:s3:completed",
        )
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.TOOL_STARTED,
                call_key="s3:t0",
                data={"tool_name": "calculate"},
                **common,
            ),
            run_id="run-lifecycle",
            operation_id="tool:s3:t0:started:execution-1",
        )
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.TOOL_COMPLETED,
                call_key="s3:t0",
                data={"tool_name": "calculate", "outcome": "completed"},
                **common,
            ),
            run_id="run-lifecycle",
            operation_id="tool:s3:t0:completed",
        )
    )

    model_started, model_completed, tool_started, tool_completed = telemetry.events
    assert model_started.attributes["provider"] == "openai_compatible"
    assert model_started.attributes["request_model"] == "gpt-5.6-terra"
    assert model_started.attributes["attempt"] == 2
    assert model_completed.attributes["attempt"] == 2
    assert tool_started.attributes["call_key"] == "s3:t0"
    assert tool_completed.attributes["call_key"] == "s3:t0"


@pytest.mark.parametrize("ttfc_ms", [True, -1, "12.5"])
def test_runtime_does_not_project_invalid_ttfc(ttfc_ms: object) -> None:
    telemetry = RecordingTelemetry()
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        InMemoryRunJournal(),
        telemetry=telemetry,
    )
    runtime._emit_telemetry(
        _stored_event(
            RunEventDraft(
                kind=RunEventKind.MODEL_COMPLETED,
                data={"ttfc_ms": ttfc_ms},
            ),
            run_id="run-invalid-ttfc",
            operation_id=f"model:invalid:{ttfc_ms!s}",
        )
    )

    assert "ttfc_s" not in telemetry.events[0].attributes


@pytest.mark.asyncio
async def test_runtime_freezes_price_inputs_in_the_durable_cost_event() -> None:
    model = ScriptedModel(
        ModelResponse(
            AssistantMessage("priced answer"),
            usage=Usage(
                input_tokens=1_000,
                output_tokens=500,
                total_tokens=1_500,
                cached_input_tokens=200,
                reasoning_output_tokens=100,
                billable_tokens=1_350,
            ),
        )
    )
    catalog = PricingCatalog(
        "catalog-v1",
        (
            Price(
                "openai",
                "gpt-test",
                "price-v1",
                datetime(2026, 1, 1, tzinfo=UTC),
                Decimal("2.50"),
                Decimal("10.00"),
                cached_input_per_million=Decimal("0.50"),
                reasoning_output_per_million=Decimal("12.00"),
            ),
        ),
    )
    runtime = AgentRuntime(
        ReActAgent(model),
        InMemoryRunJournal(),
        pricing=catalog,
        provider_name="openai",
        model_name="gpt-test",
    )

    handle = await runtime.submit(StartRun(prompt="price me", session_id="priced"))
    snapshot = await runtime.wait(handle.run_id, timeout_s=2)

    [record] = snapshot.costs
    assert record["amount_micros"] == 7_300
    assert record["request_model"] == "gpt-test"
    assert record["response_model"] == "gpt-test"
    assert record["pricing_catalog_version"] == "catalog-v1"
    assert record["price_version"] == "price-v1"
    assert record["price_effective_from"] == "2026-01-01T00:00:00+00:00"
    assert record["unit_prices_per_million"] == {
        "input": "2.50",
        "output": "10.00",
        "cached_input": "0.50",
        "reasoning_output": "12.00",
    }
    assert record["usage"]["billable_tokens"] == 1_350
    await runtime.close()


@pytest.mark.asyncio
async def test_start_is_idempotent_and_replay_never_invokes_provider() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage("durable answer")))
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    telemetry = RecordingTelemetry()
    runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        telemetry=telemetry,
    )

    first = await runtime.submit(
        StartRun(prompt="question", session_id="session", idempotency_key="request")
    )
    duplicate = await runtime.submit(
        StartRun(prompt="question", session_id="session", idempotency_key="request")
    )
    snapshot = await runtime.wait(first.run_id, timeout_s=2)

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.run_id == first.run_id
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert snapshot.counts.model_calls == 1
    assert len(snapshot.costs) == 1
    assert snapshot.costs[0]["amount_micros"] is None
    assert len(model.requests) == 1
    assert sum(
        event.kind is TelemetryEventKind.RUN_STARTED for event in telemetry.events
    ) == 1
    assert sum(
        event.kind is TelemetryEventKind.MODEL_COMPLETED for event in telemetry.events
    ) == 1

    replay = [event async for event in runtime.follow(first.run_id, live=False)]
    assert replay[-1].terminal is True
    assert len(model.requests) == 1
    session = await store.load_session("session")
    assert session.version == 1
    assert session.transcript[-1]["content"] == "durable answer"

    with pytest.raises(RequestPayloadConflict):
        await runtime.submit(
            StartRun(
                prompt="different question",
                session_id="session",
                idempotency_key="request",
            )
        )
    await runtime.close()


@pytest.mark.asyncio
async def test_live_follow_preserves_durable_sequence_when_wall_clock_regresses() -> None:
    journal = InMemoryRunJournal()
    await journal.create(
        "run-clock-regression",
        RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            privacy=PrivacyClass.PRIVATE,
            occurred_at=30.0,
            session_id="session-clock-regression",
            execution_id="execution-clock-regression",
            agent_revision="agent-v1",
            tool_manifest_hash="tools-v1",
            data={"status": "running"},
            checkpoint={"transcript": [{"role": "user", "content": "private"}]},
            safe_checkpoint=True,
        ),
        operation_id="start",
    )
    await journal.append(
        "run-clock-regression",
        RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            occurred_at=20.0,
            step=1,
            model_calls_delta=1,
        ),
        expected_sequence=1,
        operation_id="model-started",
    )
    await journal.append(
        "run-clock-regression",
        RunEventDraft(
            kind=RunEventKind.RUN_ABORTED,
            occurred_at=10.0,
            data={"status": "aborted", "stop_reason": "test"},
            safe_checkpoint=True,
        ),
        expected_sequence=2,
        operation_id="aborted",
    )
    runtime = AgentRuntime(ReActAgent(ScriptedModel()), journal)

    events = [
        event
        async for event in runtime.follow(
            "run-clock-regression",
            after_sequence=0,
            live=True,
        )
    ]

    assert [event.durable_sequence for event in events] == [1, 2, 3]
    assert events[-1].terminal is True
    await runtime.close()


@pytest.mark.asyncio
async def test_live_follow_preserves_both_source_orders_and_emits_terminal_last() -> None:
    run_id = "run-live-interleaving"
    journal = InMemoryRunJournal()
    await journal.create(
        run_id,
        RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            privacy=PrivacyClass.PRIVATE,
            occurred_at=10.0,
            session_id="session-live-interleaving",
            execution_id="execution-live-interleaving",
            agent_revision="agent-v1",
            tool_manifest_hash="tools-v1",
            data={"status": "running"},
            checkpoint={"transcript": [{"role": "user", "content": "private"}]},
            safe_checkpoint=True,
        ),
        operation_id="start",
    )
    await journal.append(
        run_id,
        RunEventDraft(kind=RunEventKind.CHECKPOINT, occurred_at=30.0),
        expected_sequence=1,
        operation_id="checkpoint-1",
    )
    await journal.append(
        run_id,
        # This committed fact is later in sequence despite its earlier wall time.
        RunEventDraft(kind=RunEventKind.CHECKPOINT, occurred_at=20.0),
        expected_sequence=2,
        operation_id="checkpoint-2",
    )
    await journal.append(
        run_id,
        RunEventDraft(
            kind=RunEventKind.RUN_ABORTED,
            occurred_at=5.0,
            data={"status": "aborted", "stop_reason": "test"},
            safe_checkpoint=True,
        ),
        expected_sequence=3,
        operation_id="aborted",
    )
    runtime = AgentRuntime(ReActAgent(ScriptedModel()), journal)
    bus = runtime_module._LiveBus()
    runtime._buses[run_id] = bus
    for source_sequence, timestamp in enumerate((15.0, 35.0, 12.0), start=1):
        await bus.publish(
            AgentStreamEvent(
                kind=(
                    AgentStreamEventKind.RUN_COMPLETED
                    if source_sequence == 3
                    else AgentStreamEventKind.MODEL_TEXT_DELTA
                ),
                run_id=run_id,
                sequence=source_sequence,
                timestamp=timestamp,
            )
        )

    events = [event async for event in runtime.follow(run_id, live=True)]

    assert [
        event.durable_sequence
        for event in events
        if event.durable_sequence is not None
    ] == [1, 2, 3, 4]
    assert [
        event.live_sequence for event in events if event.live_sequence is not None
    ] == [1, 2, 3]
    assert [
        (event.durable_sequence, event.live_sequence) for event in events
    ] == [
        (1, None),
        (None, 1),
        (2, None),
        (3, None),
        (None, 2),
        (None, 3),
        (4, None),
    ]
    assert events[-1].terminal is True
    assert all(not event.terminal for event in events[:-1])
    await runtime.close()


@pytest.mark.asyncio
async def test_durable_result_rebuild_preserves_exact_safe_agent_events() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage("eventful answer")))
    runtime = AgentRuntime(ReActAgent(model), InMemoryRunJournal())

    handle = await runtime.submit(
        StartRun(prompt="capture safe events", session_id="result-events")
    )
    snapshot = await runtime.wait(handle.run_id, timeout_s=2)
    original = runtime._results.pop(handle.run_id)

    rebuilt = AgentRuntime._result_from_snapshot(snapshot)

    assert rebuilt == original
    assert rebuilt.events == original.events
    assert [event.kind.value for event in rebuilt.events] == [
        "run_started",
        "model_started",
        "model_completed",
        "run_completed",
    ]
    await runtime.close()


@pytest.mark.asyncio
async def test_start_repairs_a_reservation_left_before_run_started() -> None:
    journal = InMemoryRunJournal()
    store = CrashAfterFirstReservationStore()
    command = StartRun(
        prompt="recover reserved start",
        session_id="reserved-session",
        idempotency_key="reserved-request",
    )
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    model = ScriptedModel(ModelResponse(AssistantMessage("repaired once")))
    recovered_runtime = AgentRuntime(ReActAgent(model), journal, store=store)
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    duplicate = await recovered_runtime.submit(command)
    events = await journal.read(repaired.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert repaired.created is True
    assert repaired.execution_id is not None
    assert duplicate.run_id == repaired.run_id
    assert duplicate.execution_id == repaired.execution_id
    assert events[0].kind is RunEventKind.RUN_STARTED
    assert events[0].execution_id == repaired.execution_id
    assert len(model.requests) == 1
    await crashed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_start_repairs_run_started_crash_only_after_winning_the_lease() -> None:
    journal = CrashBeforeFirstAcquireJournal()
    store = InMemoryRuntimeStore()
    command = StartRun(
        prompt="recover committed start",
        session_id="post-create-crash-session",
        idempotency_key="post-create-crash-request",
    )
    crashed_telemetry = TraceCapturingTelemetry("3")
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        telemetry=crashed_telemetry,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)
    run_id = (await store.list_runs(command.session_id or ""))[0]
    first_event = (await journal.read(run_id))[0]
    assert first_event.kind is RunEventKind.RUN_STARTED
    assert crashed_telemetry.references == []
    assert first_event.execution_id is not None
    assert (
        await store.load_trace_reference(run_id, first_event.execution_id) is None
    )

    model = ScriptedModel(ModelResponse(AssistantMessage("repaired after create")))
    recovered_telemetry = TraceCapturingTelemetry("4")
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        telemetry=recovered_telemetry,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.status == "completed"
    assert len(model.requests) == 1
    assert len(recovered_telemetry.references) == 1
    assert await store.load_trace_reference(
        run_id, first_event.execution_id
    ) == recovered_telemetry.references[0]
    await crashed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_failed_workspace_preflight_never_opens_a_phantom_start_root() -> None:
    journal = CrashBeforeFirstAcquireJournal()
    store = InMemoryRuntimeStore()
    workspace = FailOneAnchoredVerificationWorkspace()
    command = StartRun(
        prompt="verify before tracing",
        session_id="start-preflight-session",
        idempotency_key="start-preflight-request",
    )
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
        telemetry=TraceCapturingTelemetry("6"),
    )
    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    failed_telemetry = TraceCapturingTelemetry("7")
    workspace.fail_next_anchored_verification = True
    failed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
        telemetry=failed_telemetry,
    )
    with pytest.raises(RuntimeConflict, match="immutable anchor"):
        await failed_runtime.submit(command)
    assert failed_telemetry.references == []

    model = ScriptedModel(ModelResponse(AssistantMessage("preflight recovered")))
    recovered_telemetry = TraceCapturingTelemetry("8")
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
        telemetry=recovered_telemetry,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)

    assert snapshot.status == "completed"
    assert len(model.requests) == 1
    assert len(recovered_telemetry.references) == 1
    await crashed_runtime.close()
    await failed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_duplicate_start_never_replays_a_committed_model_intent() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    command = StartRun(
        prompt="require explicit resume",
        session_id="start-model-intent-session",
        idempotency_key="start-model-intent-request",
    )
    first_runtime = AgentRuntime(ReActAgent(blocked), journal, store=store)
    handle = await first_runtime.submit(command)
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    duplicate_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    duplicate_telemetry = TraceCapturingTelemetry("5")
    duplicate_runtime = AgentRuntime(
        ReActAgent(duplicate_model),
        journal,
        store=store,
        telemetry=duplicate_telemetry,
    )
    duplicate = await duplicate_runtime.submit(command)

    assert duplicate.run_id == handle.run_id
    assert duplicate.created is False
    assert duplicate_model.requests == []
    assert duplicate_telemetry.events == []
    assert any(
        event.kind is RunEventKind.MODEL_STARTED
        for event in await journal.read(handle.run_id)
    )
    await duplicate_runtime.close()


@pytest.mark.asyncio
async def test_failed_resume_preflight_releases_lease_for_immediate_takeover() -> None:
    journal = FailNextEventJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(ReActAgent(blocked), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(
            prompt="interrupt before resume",
            session_id="resume-preflight-lease-session",
            idempotency_key="resume-preflight-lease-request",
        )
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    journal.fail_next(RunEventKind.RUN_RESUMED)
    failed_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    failed_runtime = AgentRuntime(ReActAgent(failed_model), journal, store=store)
    with pytest.raises(RuntimeError, match="run_resumed append outage"):
        await failed_runtime.submit(ResumeRun(run_id=handle.run_id))

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    recovered_runtime = AgentRuntime(ReActAgent(recovered_model), journal, store=store)
    recovered = await recovered_runtime.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await recovered_runtime.wait(handle.run_id, timeout_s=2)

    assert recovered.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert failed_model.requests == []
    assert len(recovered_model.requests) == 1
    await failed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_failed_cancel_preflight_releases_lease_for_immediate_retry() -> None:
    journal = FailNextEventJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(ReActAgent(blocked), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(
            prompt="interrupt before cancellation",
            session_id="cancel-preflight-lease-session",
            idempotency_key="cancel-preflight-lease-request",
        )
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    journal.fail_next(RunEventKind.RUN_CANCEL_REQUESTED)
    failed_runtime = AgentRuntime(ReActAgent(ScriptedModel()), journal, store=store)
    with pytest.raises(RuntimeError, match="run_cancel_requested append outage"):
        await failed_runtime.submit(CancelRun(run_id=handle.run_id))

    recovered_runtime = AgentRuntime(ReActAgent(ScriptedModel()), journal, store=store)
    recovered = await recovered_runtime.submit(CancelRun(run_id=handle.run_id))
    snapshot = await recovered_runtime.load(handle.run_id)

    assert recovered.created is False
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "aborted"
    await failed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_cancel_reacquires_after_same_runtime_execution_failure() -> None:
    journal = FailNextEventJournal()
    journal.fail_next(RunEventKind.MODEL_STARTED)
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("must not complete")))),
        journal,
        store=InMemoryRuntimeStore(),
    )
    handle = await runtime.submit(
        StartRun(
            prompt="fail before provider call",
            session_id="cancel-after-failure-session",
            idempotency_key="cancel-after-failure-request",
        )
    )

    # Observe only the journal seam: once a different owner can acquire and
    # release the fence, the failed background execution has finished its
    # cleanup.  Cancel must not reuse that execution's released writer.
    for _ in range(100):
        try:
            probe = await journal.acquire(handle.run_id, owner="probe", ttl_s=1)
        except LeaseConflictError:
            await asyncio.sleep(0.005)
        else:
            await journal.release(probe)
            break
    else:
        pytest.fail("failed execution did not release its fencing lease")

    cancelled = await runtime.submit(CancelRun(run_id=handle.run_id))
    snapshot = await runtime.load(handle.run_id)

    assert cancelled.created is False
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "aborted"
    await runtime.close()


@pytest.mark.asyncio
async def test_failed_task_scheduling_releases_lease_and_registry_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    runtime = AgentRuntime(ReActAgent(model), journal, store=store)
    command = StartRun(
        prompt="retry failed task handoff",
        session_id="schedule-handoff-session",
        idempotency_key="schedule-handoff-request",
    )
    original_create_task = runtime_module.asyncio.create_task
    fail_run_task = True

    def injected_create_task(coroutine, *, name=None, context=None):
        nonlocal fail_run_task
        if fail_run_task and isinstance(name, str) and name.startswith("react-run:"):
            fail_run_task = False
            raise RuntimeError("simulated task scheduling outage")
        kwargs = {"name": name}
        if context is not None:
            kwargs["context"] = context
        return original_create_task(coroutine, **kwargs)

    monkeypatch.setattr(runtime_module.asyncio, "create_task", injected_create_task)

    with pytest.raises(RuntimeError, match="task scheduling outage"):
        await runtime.submit(command)

    repaired = await runtime.submit(command)
    snapshot = await runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(model.requests) == 1
    await runtime.close()


@pytest.mark.asyncio
async def test_failed_heartbeat_start_releases_acquired_lease_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = InMemoryRunJournal()
    model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=InMemoryRuntimeStore(),
    )
    command = StartRun(
        prompt="retry failed heartbeat startup",
        session_id="heartbeat-start-session",
        idempotency_key="heartbeat-start-request",
    )
    original_create_task = runtime_module.asyncio.create_task
    fail_heartbeat = True

    def injected_create_task(coroutine, *, name=None, context=None):
        nonlocal fail_heartbeat
        if fail_heartbeat and isinstance(name, str) and name.startswith("react-lease:"):
            fail_heartbeat = False
            raise RuntimeError("simulated heartbeat startup outage")
        kwargs = {"name": name}
        if context is not None:
            kwargs["context"] = context
        return original_create_task(coroutine, **kwargs)

    monkeypatch.setattr(runtime_module.asyncio, "create_task", injected_create_task)

    with pytest.raises(RuntimeError, match="heartbeat startup outage"):
        await runtime.submit(command)

    repaired = await runtime.submit(command)
    snapshot = await runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(model.requests) == 1
    await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_acquire_releases_adapter_lease_before_propagating() -> None:
    journal = PauseAfterAcquireJournal()
    store = InMemoryRuntimeStore()
    first_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    command = StartRun(
        prompt="cancel while acquiring",
        session_id="cancelled-acquire-session",
        idempotency_key="cancelled-acquire-request",
    )
    submission = asyncio.create_task(first_runtime.submit(command))
    await asyncio.wait_for(journal.acquired.wait(), timeout=1)

    submission.cancel()
    journal.allow_return.set()
    with pytest.raises(asyncio.CancelledError):
        await submission

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert first_model.requests == []
    assert len(recovered_model.requests) == 1
    await first_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_failed_start_preflight_releases_lease_for_idempotent_repair() -> None:
    journal = FailNextEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    command = StartRun(
        prompt="repair failed start preflight",
        session_id="start-preflight-lease-session",
        idempotency_key="start-preflight-lease-request",
    )
    failed_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    failed_runtime = AgentRuntime(
        ReActAgent(failed_model),
        journal,
        store=store,
        workspace=workspace,
    )
    journal.fail_next(RunEventKind.WORKSPACE_CHECKPOINTED)

    with pytest.raises(RuntimeError, match="workspace_checkpointed append outage"):
        await failed_runtime.submit(command)

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        workspace=workspace,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert failed_model.requests == []
    assert len(recovered_model.requests) == 1
    await failed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_concurrent_start_repairers_share_one_run_and_execution() -> None:
    journal = SynchronizeMissingRunLoadsJournal()
    store = CrashAfterFirstReservationStore()
    command = StartRun(
        prompt="repair concurrently",
        session_id="concurrent-repair-session",
        idempotency_key="concurrent-repair-request",
    )
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    first_model = ScriptedModel(ModelResponse(AssistantMessage("first repairer")))
    second_model = ScriptedModel(ModelResponse(AssistantMessage("second repairer")))
    first_telemetry = TraceCapturingTelemetry("1")
    second_telemetry = TraceCapturingTelemetry("2")
    first_runtime = AgentRuntime(
        ReActAgent(first_model), journal, store=store, telemetry=first_telemetry
    )
    second_runtime = AgentRuntime(
        ReActAgent(second_model), journal, store=store, telemetry=second_telemetry
    )

    outcomes = await asyncio.gather(
        first_runtime.submit(command),
        second_runtime.submit(command),
        return_exceptions=True,
    )

    assert not any(isinstance(item, BaseException) for item in outcomes), outcomes
    first, second = outcomes
    assert not isinstance(first, BaseException)
    assert not isinstance(second, BaseException)
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert sorted((first.created, second.created)) == [False, True]
    snapshot = await first_runtime.wait(first.run_id, timeout_s=2)
    events = await journal.read(first.run_id)
    assert snapshot.state is RunState.TERMINAL
    assert sum(event.kind is RunEventKind.RUN_STARTED for event in events) == 1
    assert len(first_model.requests) + len(second_model.requests) == 1
    emitted_roots = [
        event
        for telemetry in (first_telemetry, second_telemetry)
        for event in telemetry.events
        if event.kind is TelemetryEventKind.RUN_STARTED
    ]
    references = [
        reference
        for telemetry in (first_telemetry, second_telemetry)
        for reference in telemetry.references
    ]
    assert len(emitted_roots) == 1
    assert len(references) == 1
    assert await store.load_trace_reference(first.run_id, first.execution_id or "") == references[0]
    await crashed_runtime.close()
    await first_runtime.close()
    await second_runtime.close()


@pytest.mark.asyncio
async def test_start_reclaims_a_clean_workspace_orphan_before_run_started() -> None:
    journal = CrashNextRunCreateJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    command = StartRun(
        prompt="recover workspace orphan",
        session_id="workspace-orphan-session",
        idempotency_key="workspace-orphan-request",
    )
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)
    assert workspace.verify("workspace-orphan-session").dirty is False

    model = ScriptedModel(ModelResponse(AssistantMessage("workspace repaired")))
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    events = await journal.read(repaired.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert len(model.requests) == 1
    assert sum(event.kind is RunEventKind.RUN_STARTED for event in events) == 1
    assert any(
        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "run_start"
        for event in events
    )
    await crashed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_start_never_reclaims_a_dirty_workspace_orphan() -> None:
    journal = CrashNextRunCreateJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    command = StartRun(
        prompt="reject dirty workspace orphan",
        session_id="dirty-workspace-orphan",
        idempotency_key="dirty-workspace-request",
    )
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)
    workspace.write_file("dirty-workspace-orphan", "README.md", "changed\n")

    model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(RuntimeConflict, match="not clean enough"):
        await recovered_runtime.submit(command)

    assert not model.requests
    await crashed_runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_follower_cancellation_does_not_cancel_background_run() -> None:
    release = asyncio.Event()

    class GatedModel:
        async def complete(self, request: ModelRequest) -> ModelResponse:
            del request
            await release.wait()
            return ModelResponse(AssistantMessage("finished after disconnect"))

    runtime = AgentRuntime(ReActAgent(GatedModel()), InMemoryRunJournal())
    handle = await runtime.submit(StartRun(prompt="keep going", session_id="session"))

    async def follow_one() -> None:
        async for _ in runtime.follow(handle.run_id, live=True):
            return

    follower = asyncio.create_task(follow_one())
    await follower
    release.set()
    snapshot = await runtime.wait(handle.run_id, timeout_s=2)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    await runtime.close()


@pytest.mark.asyncio
async def test_resume_abandons_uncommitted_model_attempt_and_retries_new_execution() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(ReActAgent(blocked), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(prompt="recover me", session_id="session", idempotency_key="request")
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    telemetry = RecordingTelemetry()
    second_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        telemetry=telemetry,
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await second_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.counts.model_calls == 2
    assert len(recovered_model.requests) == 1
    assert sum(event.kind is RunEventKind.MODEL_ABANDONED for event in events) == 1
    assert len({event.execution_id for event in events if event.execution_id}) == 2
    resumed_index = next(
        index for index, event in enumerate(events) if event.kind is RunEventKind.RUN_RESUMED
    )
    abandoned_index = next(
        index
        for index, event in enumerate(events)
        if event.kind is RunEventKind.MODEL_ABANDONED
    )
    assert resumed_index < abandoned_index
    resumed = next(
        event
        for event in telemetry.events
        if event.kind is TelemetryEventKind.RUN_RESUMED
    )
    assert resumed.attributes["previous_execution_id"] == handle.execution_id
    assert resumed.attributes["resume_reason"] == "model_abandoned"
    await second_runtime.close()


@pytest.mark.asyncio
async def test_resume_loads_the_previous_root_from_a_shared_trace_projection() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_telemetry = TraceCapturingTelemetry("a")
    first_runtime = AgentRuntime(
        ReActAgent(blocked),
        journal,
        store=store,
        telemetry=first_telemetry,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="persist trace root",
            session_id="trace-resume-session",
            idempotency_key="trace-resume-request",
        )
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    assert handle.execution_id is not None
    previous_reference = await store.load_trace_reference(
        handle.run_id, handle.execution_id
    )
    assert previous_reference is not None
    assert previous_reference == first_telemetry.references[0]
    await first_runtime.close()

    second_telemetry = TraceCapturingTelemetry("b")
    recovered = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("linked")))),
        journal,
        store=store,
        telemetry=second_telemetry,
    )
    resumed = await recovered.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await recovered.wait(handle.run_id, timeout_s=2)

    assert snapshot.status == "completed"
    resume_event = next(
        event
        for event in second_telemetry.events
        if event.kind is TelemetryEventKind.RUN_RESUMED
    )
    assert resume_event.links == (previous_reference,)
    assert resumed.execution_id is not None
    resumed_reference = await store.load_trace_reference(
        handle.run_id, resumed.execution_id
    )
    assert resumed_reference is not None
    assert resumed_reference == second_telemetry.references[0]
    assert resumed_reference.trace_id != previous_reference.trace_id
    await recovered.close()


@pytest.mark.asyncio
async def test_trace_projection_failure_never_blocks_resume_side_effects() -> None:
    journal = InMemoryRunJournal()
    store = FailingTraceProjectionStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(
        ReActAgent(blocked),
        journal,
        store=store,
        telemetry=TraceCapturingTelemetry("c"),
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="ignore trace store failure",
            session_id="trace-failure-session",
            idempotency_key="trace-failure-request",
        )
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    model = ScriptedModel(ModelResponse(AssistantMessage("still completed")))
    telemetry = TraceCapturingTelemetry("d")
    recovered = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        telemetry=telemetry,
    )
    await recovered.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await recovered.wait(handle.run_id, timeout_s=2)

    assert snapshot.status == "completed"
    assert len(model.requests) == 1
    resume_event = next(
        event
        for event in telemetry.events
        if event.kind is TelemetryEventKind.RUN_RESUMED
    )
    assert resume_event.links == ()
    await recovered.close()


@pytest.mark.asyncio
async def test_interrupted_idempotent_tool_retries_with_stable_key_and_completed_once() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()
    idempotency_keys: list[str] = []

    @tool(idempotent=True, version="v1")
    async def lookup(value: int, *, context: ToolExecutionContext) -> int:
        """Look up a stable value."""

        idempotency_keys.append(context.idempotency_key)
        started.set()
        await asyncio.Event().wait()
        return value

    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(None, (ToolCall("call", "lookup", '{"value":21}'),))
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [lookup]), journal, store=store
    )
    handle = await first_runtime.submit(
        StartRun(prompt="lookup", session_id="session", idempotency_key="request")
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await first_runtime.close()

    @tool(name="lookup", idempotent=True, version="v1")
    async def recovered_lookup(
        value: int, *, context: ToolExecutionContext
    ) -> int:
        """Look up a stable value."""

        idempotency_keys.append(context.idempotency_key)
        return value * 2

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("42")))
    second_runtime = AgentRuntime(
        ReActAgent(recovered_model, [recovered_lookup]), journal, store=store
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await second_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    assert snapshot.counts.tool_calls == 1
    assert snapshot.counts.tool_executions == 1
    assert idempotency_keys[0] == idempotency_keys[1]
    assert sum(event.kind is RunEventKind.TOOL_COMPLETED for event in events) == 1
    assert len(recovered_model.requests) == 1
    await second_runtime.close()


@pytest.mark.asyncio
async def test_uncertain_non_idempotent_tool_fails_closed_for_reconciliation() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()

    @tool(version="v1")
    async def charge(amount: int) -> int:
        """Charge a non-idempotent external account."""

        started.set()
        await asyncio.Event().wait()
        return amount

    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(None, (ToolCall("charge-1", "charge", '{"amount":7}'),))
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [charge]), journal, store=store
    )
    handle = await first_runtime.submit(
        StartRun(prompt="charge", session_id="session", idempotency_key="request")
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await first_runtime.close()

    provider_after_restart = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    second_runtime = AgentRuntime(
        ReActAgent(provider_after_restart, [charge]), journal, store=store
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    await wait_for_kind(journal, handle.run_id, RunEventKind.RECONCILIATION_REQUIRED)
    snapshot = await second_runtime.load(handle.run_id)

    assert snapshot.state is RunState.NEEDS_RECONCILIATION
    assert not provider_after_restart.requests
    with pytest.raises(ReconciliationRequired):
        await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    await second_runtime.close()


@pytest.mark.asyncio
async def test_fork_accepts_only_a_safe_durable_sequence() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage("parent answer")))
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    runtime = AgentRuntime(ReActAgent(model), journal, store=store)
    parent = await runtime.submit(
        StartRun(prompt="parent", session_id="parent-session")
    )
    parent_snapshot = await runtime.wait(parent.run_id, timeout_s=2)
    parent_events = await journal.read(parent.run_id)
    safe_sequence = next(
        event.sequence
        for event in parent_events
        if event.kind is RunEventKind.MODEL_COMPLETED
    )
    unsafe_sequence = next(
        event.sequence
        for event in parent_events
        if event.kind is RunEventKind.MODEL_STARTED
    )

    assert safe_sequence in parent_snapshot.safe_checkpoint_sequences
    assert unsafe_sequence not in parent_snapshot.safe_checkpoint_sequences
    with pytest.raises(RuntimeConflict, match="not a safe checkpoint"):
        await runtime.submit(
            ForkRun(
                run_id=parent.run_id,
                from_sequence=unsafe_sequence,
                session_id="unsafe-fork-session",
            )
        )

    fork = await runtime.submit(
        ForkRun(
            run_id=parent.run_id,
            from_sequence=safe_sequence,
            session_id="safe-fork-session",
            idempotency_key="safe-fork",
        )
    )
    fork_snapshot = await runtime.wait(fork.run_id, timeout_s=2)
    fork_events = await journal.read(fork.run_id)

    assert fork.created is True
    assert fork_snapshot.state is RunState.TERMINAL
    assert fork_snapshot.parent_run_id == parent.run_id
    assert fork_snapshot.fork_sequence == safe_sequence
    assert fork_snapshot.counts == parent_snapshot.counts
    assert fork_events[0].data["parent_run_id"] == parent.run_id
    assert fork_events[0].data["fork_sequence"] == safe_sequence
    assert len(model.requests) == 1, "Fork replay must not call the provider"
    await runtime.close()


@pytest.mark.asyncio
async def test_fork_repairs_a_reservation_left_before_run_started() -> None:
    journal = InMemoryRunJournal()
    parent_store = InMemoryRuntimeStore()
    parent_model = ScriptedModel(ModelResponse(AssistantMessage("parent complete")))
    parent_runtime = AgentRuntime(
        ReActAgent(parent_model),
        journal,
        store=parent_store,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-repair-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    safe_sequence = parent_snapshot.safe_checkpoint_sequences[-1]
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=safe_sequence,
        session_id="fork-repair-child",
        idempotency_key="fork-repair-request",
    )
    child_store = CrashAfterFirstReservationStore()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=child_store,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=child_store,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    duplicate = await recovered_runtime.submit(command)
    events = await journal.read(repaired.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.parent_run_id == parent.run_id
    assert snapshot.fork_sequence == safe_sequence
    assert repaired.created is True
    assert repaired.execution_id is not None
    assert duplicate.run_id == repaired.run_id
    assert duplicate.execution_id == repaired.execution_id
    assert events[0].kind is RunEventKind.RUN_STARTED
    assert events[0].execution_id == repaired.execution_id
    assert not recovered_model.requests
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_fork_repairs_a_child_committed_before_lineage_registration() -> None:
    journal = InMemoryRunJournal()
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=InMemoryRuntimeStore(),
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-post-create-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="fork-post-create-child",
        idempotency_key="fork-post-create-request",
    )
    child_store = CrashBeforeFirstLineageStore()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=child_store,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=child_store,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    duplicate = await recovered_runtime.submit(command)
    events = await journal.read(repaired.run_id)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.parent_run_id == parent.run_id
    assert snapshot.fork_sequence == command.from_sequence
    assert duplicate.run_id == repaired.run_id
    assert duplicate.execution_id == repaired.execution_id
    assert sum(event.kind is RunEventKind.RUN_STARTED for event in events) == 1
    assert recovered_model.requests == []
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_fork_repairs_a_child_left_before_its_first_lease() -> None:
    journal = CrashBeforeFirstAcquireJournal(armed=False)
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=InMemoryRuntimeStore(),
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-pre-lease-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="fork-pre-lease-child",
        idempotency_key="fork-pre-lease-request",
    )
    child_store = InMemoryRuntimeStore()
    journal.arm()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=child_store,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=child_store,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    duplicate = await recovered_runtime.submit(command)
    events = await journal.read(repaired.run_id)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert duplicate.created is False
    assert duplicate.run_id == repaired.run_id
    assert sum(event.kind is RunEventKind.RUN_STARTED for event in events) == 1
    assert recovered_model.requests == []
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_fork_repairs_its_anchored_workspace_after_a_pre_lease_crash() -> None:
    journal = CrashBeforeFirstAcquireJournal(armed=False)
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-anchor-repair-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="fork-anchor-repair-child",
        idempotency_key="fork-anchor-repair-request",
    )
    journal.arm()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        workspace=workspace,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    events = await journal.read(repaired.run_id)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert any(
        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "fork_start"
        for event in events
    )
    assert recovered_model.requests == []
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_failed_fork_preflight_releases_lease_for_idempotent_repair() -> None:
    journal = FailNextEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-preflight-lease-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="fork-preflight-lease-child",
        idempotency_key="fork-preflight-lease-request",
    )
    failed_model = ScriptedModel()
    failed_runtime = AgentRuntime(
        ReActAgent(failed_model),
        journal,
        store=store,
        workspace=workspace,
    )
    journal.fail_next(RunEventKind.WORKSPACE_CHECKPOINTED)

    with pytest.raises(RuntimeError, match="workspace_checkpointed append outage"):
        await failed_runtime.submit(command)

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        workspace=workspace,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)

    assert repaired.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.parent_run_id == parent.run_id
    assert failed_model.requests == []
    assert recovered_model.requests == []
    await failed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_duplicate_fork_never_replays_a_committed_model_intent() -> None:
    journal = InMemoryRunJournal()
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=InMemoryRuntimeStore(),
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="fork-intent-parent")
    )
    parent_events = await journal.read(parent.run_id)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_events[0].sequence,
        session_id="fork-intent-child",
        idempotency_key="fork-intent-request",
    )
    child_store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(ReActAgent(blocked), journal, store=child_store)
    child = await first_runtime.submit(command)
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    duplicate_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    duplicate_telemetry = TraceCapturingTelemetry("9")
    duplicate_runtime = AgentRuntime(
        ReActAgent(duplicate_model),
        journal,
        store=child_store,
        telemetry=duplicate_telemetry,
    )
    duplicate = await duplicate_runtime.submit(command)

    assert duplicate.run_id == child.run_id
    assert duplicate.created is False
    assert duplicate_model.requests == []
    assert duplicate_telemetry.events == []
    assert any(
        event.kind is RunEventKind.MODEL_STARTED
        for event in await journal.read(child.run_id)
    )
    await duplicate_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_concurrent_fork_repairers_share_one_run_and_execution() -> None:
    journal = SynchronizeMissingRunLoadsJournal()
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=InMemoryRuntimeStore(),
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="concurrent-fork-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="concurrent-fork-child",
        idempotency_key="concurrent-fork-request",
    )
    child_store = CrashAfterFirstReservationStore()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=child_store,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)

    first_model = ScriptedModel()
    second_model = ScriptedModel()
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=child_store)
    second_runtime = AgentRuntime(ReActAgent(second_model), journal, store=child_store)
    outcomes = await asyncio.gather(
        first_runtime.submit(command),
        second_runtime.submit(command),
        return_exceptions=True,
    )

    assert not any(isinstance(item, BaseException) for item in outcomes), outcomes
    first, second = outcomes
    assert not isinstance(first, BaseException)
    assert not isinstance(second, BaseException)
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert sorted((first.created, second.created)) == [False, True]
    snapshot = await first_runtime.wait(first.run_id, timeout_s=2)
    events = await journal.read(first.run_id)
    assert snapshot.state is RunState.TERMINAL
    assert sum(event.kind is RunEventKind.RUN_STARTED for event in events) == 1
    assert not first_model.requests
    assert not second_model.requests
    await crashed_runtime.close()
    await first_runtime.close()
    await second_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_fork_reclaims_only_the_matching_workspace_orphan() -> None:
    journal = CrashNextRunCreateJournal(armed=False)
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_model = ScriptedModel(ModelResponse(AssistantMessage("parent complete")))
    parent_runtime = AgentRuntime(
        ReActAgent(parent_model),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="workspace-fork-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="workspace-fork-child",
        idempotency_key="workspace-fork-request",
    )
    journal.arm()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )

    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)
    assert workspace.verify("workspace-fork-child").dirty is False

    recovered_model = ScriptedModel()
    recovered_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        workspace=workspace,
    )
    repaired = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(repaired.run_id, timeout_s=2)
    events = await journal.read(repaired.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.parent_run_id == parent.run_id
    assert not recovered_model.requests
    assert any(
        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "fork_start"
        for event in events
    )
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


@pytest.mark.asyncio
async def test_fork_never_reclaims_a_diverged_workspace_orphan() -> None:
    journal = CrashNextRunCreateJournal(armed=False)
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    parent_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("parent complete")))),
        journal,
        store=store,
        workspace=workspace,
    )
    parent = await parent_runtime.submit(
        StartRun(prompt="parent", session_id="diverged-fork-parent")
    )
    parent_snapshot = await parent_runtime.wait(parent.run_id, timeout_s=2)
    command = ForkRun(
        run_id=parent.run_id,
        from_sequence=parent_snapshot.safe_checkpoint_sequences[-1],
        session_id="diverged-fork-child",
        idempotency_key="diverged-fork-request",
    )
    journal.arm()
    crashed_runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(_SimulatedProcessCrash):
        await crashed_runtime.submit(command)
    workspace.write_file("diverged-fork-child", "README.md", "diverged\n")

    model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    recovered_runtime = AgentRuntime(
        ReActAgent(model),
        journal,
        store=store,
        workspace=workspace,
    )
    with pytest.raises(RuntimeConflict, match="differs from its parent checkpoint"):
        await recovered_runtime.submit(command)

    assert not model.requests
    await crashed_runtime.close()
    await recovered_runtime.close()
    await parent_runtime.close()


async def interrupted_charge_run(
    final_answer: str,
    *,
    journal: InMemoryRunJournal | None = None,
    resume_policy: ToolResumePolicy | None = None,
) -> tuple[
    AgentRuntime,
    InMemoryRunJournal,
    InMemoryRuntimeStore,
    str,
    list[ToolExecutionContext],
    ScriptedModel,
]:
    run_journal = journal or InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    started = asyncio.Event()
    block_first_call = True
    invocations: list[ToolExecutionContext] = []

    @tool(version="v1", resume_policy=resume_policy)
    async def charge(
        amount: int,
        *,
        context: ToolExecutionContext,
    ) -> int:
        """Charge an external account after explicit reconciliation."""

        nonlocal block_first_call
        invocations.append(context)
        if block_first_call:
            block_first_call = False
            started.set()
            await asyncio.Event().wait()
        return amount

    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("charge-1", "charge", '{"amount":7}'),),
            )
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [charge]),
        run_journal,
        store=store,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="charge",
            session_id="reconciliation-session",
            idempotency_key="charge-request",
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await first_runtime.close()

    resumed_model = ScriptedModel(ModelResponse(AssistantMessage(final_answer)))
    resumed_runtime = AgentRuntime(
        ReActAgent(resumed_model, [charge]),
        run_journal,
        store=store,
    )
    await resumed_runtime.submit(ResumeRun(run_id=handle.run_id))
    await wait_for_kind(
        run_journal,
        handle.run_id,
        RunEventKind.RECONCILIATION_REQUIRED,
    )
    return (
        resumed_runtime,
        run_journal,
        store,
        handle.run_id,
        invocations,
        resumed_model,
    )


@pytest.mark.asyncio
async def test_failed_resolve_preflight_releases_lease_for_operator_retry() -> None:
    journal = FailNextEventJournal()
    runtime, _, store, run_id, invocations, model = await interrupted_charge_run(
        "operator result accepted",
        journal=journal,
    )
    command = ResolveRun(
        run_id=run_id,
        call_key="s1:t0",
        action=ResolutionAction.USE_RESULT,
        result={"receipt": "confirmed"},
    )
    journal.fail_next(RunEventKind.RECONCILIATION_RESOLVED)

    with pytest.raises(RuntimeError, match="reconciliation_resolved append outage"):
        await runtime.submit(command)

    recovered_runtime = AgentRuntime(runtime.agent, journal, store=store)
    recovered = await recovered_runtime.submit(command)
    snapshot = await recovered_runtime.wait(run_id, timeout_s=2)

    assert recovered.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(invocations) == 1
    assert len(model.requests) == 1
    await runtime.close()
    await recovered_runtime.close()


@pytest.mark.asyncio
async def test_resolve_rejects_a_run_owned_by_a_different_active_run() -> None:
    runtime, journal, store, run_id, invocations, model = (
        await interrupted_charge_run("must remain unused")
    )
    await store.release_active_run("reconciliation-session", run_id)
    await store.claim_active_run("reconciliation-session", "different-owner-run")

    try:
        with pytest.raises(RuntimeConflict, match="different-owner-run"):
            await runtime.submit(
                ResolveRun(
                    run_id=run_id,
                    call_key="s1:t0",
                    action=ResolutionAction.ABORT,
                )
            )
        events = await journal.read(run_id)
        assert len(invocations) == 1
        assert model.requests == []
        assert not any(
            event.kind in {
                RunEventKind.RECONCILIATION_RESOLVED,
                RunEventKind.RUN_ABORTED,
            }
            for event in events
        )
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_resolve_retry_replays_only_after_explicit_operator_action() -> None:
    runtime, journal, _, run_id, invocations, model = await interrupted_charge_run(
        "retry completed"
    )

    handle = await runtime.submit(
        ResolveRun(
            run_id=run_id,
            call_key="s1:t0",
            action=ResolutionAction.RETRY,
        )
    )
    snapshot = await runtime.wait(run_id, timeout_s=2)
    events = await journal.read(run_id)

    assert handle.created is True
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(invocations) == 2
    assert len(model.requests) == 1
    assert sum(event.kind is RunEventKind.TOOL_STARTED for event in events) == 2
    assert sum(event.kind is RunEventKind.TOOL_COMPLETED for event in events) == 1
    resolved = next(
        event
        for event in events
        if event.kind is RunEventKind.RECONCILIATION_RESOLVED
    )
    assert resolved.data["action"] == ResolutionAction.RETRY.value
    await runtime.close()


@pytest.mark.asyncio
async def test_resolve_retry_honors_never_retry_policy() -> None:
    runtime, journal, _, run_id, invocations, model = await interrupted_charge_run(
        "must remain unused",
        resume_policy=ToolResumePolicy.NEVER_RETRY,
    )

    with pytest.raises(RuntimeConflict, match="permanently forbids retry"):
        await runtime.submit(
            ResolveRun(
                run_id=run_id,
                call_key="s1:t0",
                action=ResolutionAction.RETRY,
            )
        )

    events = await journal.read(run_id)
    assert len(invocations) == 1
    assert model.requests == []
    assert not any(event.kind is RunEventKind.RECONCILIATION_RESOLVED for event in events)
    await runtime.close()


@pytest.mark.asyncio
async def test_resolve_use_result_never_reexecutes_uncertain_tool() -> None:
    runtime, journal, _, run_id, invocations, model = await interrupted_charge_run(
        "operator result accepted"
    )

    await runtime.submit(
        ResolveRun(
            run_id=run_id,
            call_key="s1:t0",
            action=ResolutionAction.USE_RESULT,
            result={"receipt": "confirmed"},
        )
    )
    snapshot = await runtime.wait(run_id, timeout_s=2)
    events = await journal.read(run_id)

    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "completed"
    assert len(invocations) == 1
    assert len(model.requests) == 1
    completed = next(
        event
        for event in events
        if event.kind is RunEventKind.TOOL_COMPLETED
    )
    assert completed.data["operator_supplied"] is True
    assert completed.data["executed"] is False
    assert snapshot.counts.tool_executions == 0
    await runtime.close()


@pytest.mark.asyncio
async def test_resolve_abort_makes_run_terminal_without_more_side_effects() -> None:
    runtime, journal, store, run_id, invocations, model = (
        await interrupted_charge_run("must remain unused")
    )

    handle = await runtime.submit(
        ResolveRun(
            run_id=run_id,
            call_key="s1:t0",
            action=ResolutionAction.ABORT,
        )
    )
    snapshot = await runtime.wait(run_id, timeout_s=2)
    events = await journal.read(run_id)
    session = await store.load_session("reconciliation-session")

    assert handle.created is False
    assert snapshot.state is RunState.TERMINAL
    assert snapshot.status == "aborted"
    assert snapshot.stop_reason == "operator_aborted"
    assert len(invocations) == 1
    assert not model.requests
    assert session.version == 0
    assert events[-1].kind is RunEventKind.RUN_ABORTED
    await runtime.close()


@pytest.mark.asyncio
async def test_resume_after_session_commit_crash_does_not_commit_session_twice() -> None:
    model = ScriptedModel(ModelResponse(AssistantMessage("committed once")))
    journal = CrashBeforeSessionEventJournal()
    store = InMemoryRuntimeStore()
    first_runtime = AgentRuntime(ReActAgent(model), journal, store=store)
    handle = await first_runtime.submit(
        StartRun(
            prompt="commit exactly once",
            session_id="session",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.session_event_attempted.wait(), timeout=1)

    session_after_crash = await store.load_session("session")
    interrupted = await first_runtime.load(handle.run_id)
    assert session_after_crash.version == 1
    assert interrupted.state is not RunState.TERMINAL
    assert interrupted.result is not None
    assert not any(
        event.kind is RunEventKind.SESSION_COMMITTED
        for event in await journal.read(handle.run_id)
    )
    await first_runtime.close()

    recovery_model = ScriptedModel()
    second_runtime = AgentRuntime(
        ReActAgent(recovery_model),
        journal,
        store=store,
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await second_runtime.wait(handle.run_id, timeout_s=2)
    session_after_resume = await store.load_session("session")
    events = await journal.read(handle.run_id)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert session_after_resume.version == 1
    assert session_after_resume.transcript == session_after_crash.transcript
    assert sum(
        event.kind is RunEventKind.SESSION_COMMITTED for event in events
    ) == 1
    assert not recovery_model.requests
    assert len(model.requests) == 1
    await second_runtime.close()


@pytest.mark.asyncio
async def test_result_ready_resume_verifies_workspace_before_terminal_commit() -> None:
    journal = CrashBeforeSessionEventJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"README.md": "baseline\n"})
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("ready")))),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="finish after workspace verification",
            session_id="result-workspace-session",
            idempotency_key="result-workspace-request",
        )
    )
    await asyncio.wait_for(journal.session_event_attempted.wait(), timeout=1)
    interrupted = await first_runtime.load(handle.run_id)
    assert interrupted.result is not None
    assert interrupted.state is not RunState.TERMINAL
    await first_runtime.close()

    workspace.write_file(
        "result-workspace-session",
        "README.md",
        "changed after result checkpoint\n",
    )
    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model),
        journal,
        store=store,
        workspace=workspace,
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    for _ in range(100):
        snapshot = await recovery_runtime.load(handle.run_id)
        if snapshot.state in {RunState.NEEDS_RECONCILIATION, RunState.TERMINAL}:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail("result-ready workspace verification did not settle")

    assert snapshot.state is RunState.NEEDS_RECONCILIATION
    assert not recovery_model.requests
    assert workspace.read_file("result-workspace-session", "README.md") == (
        b"changed after result checkpoint\n"
    )
    assert not any(
        event.kind in {RunEventKind.RUN_COMPLETED, RunEventKind.RUN_ABORTED}
        for event in await journal.read(handle.run_id)
    )
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_restores_half_written_workspace_before_idempotent_retry() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"state.txt": "base\n"})
    started = asyncio.Event()
    observed_before_retry: list[bytes] = []

    @tool(name="edit_file", idempotent=True, version="v1")
    async def interrupted_edit(value: str, *, context: ToolExecutionContext) -> str:
        """Write one deterministic file in the isolated Session workspace."""

        del context
        workspace.write_file("workspace-session", "state.txt", "partial\n")
        started.set()
        await asyncio.Event().wait()
        return value

    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("edit-1", "edit_file", '{"value":"final"}'),),
            )
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [interrupted_edit]),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="edit",
            session_id="workspace-session",
            idempotency_key="edit-request",
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert workspace.read_file("workspace-session", "state.txt") == b"partial\n"
    await first_runtime.close()

    @tool(name="edit_file", idempotent=True, version="v1")
    async def recovered_edit(value: str, *, context: ToolExecutionContext) -> str:
        """Write one deterministic file in the isolated Session workspace."""

        del context
        observed_before_retry.append(
            workspace.read_file("workspace-session", "state.txt")
        )
        workspace.write_file("workspace-session", "state.txt", f"{value}\n")
        return value

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("workspace recovered")))
    second_runtime = AgentRuntime(
        ReActAgent(recovered_model, [recovered_edit]),
        journal,
        store=store,
        workspace=workspace,
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    snapshot = await second_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    assert snapshot.state is RunState.TERMINAL
    assert observed_before_retry == [b"base\n"]
    assert workspace.read_file("workspace-session", "state.txt") == b"final\n"
    assert any(event.kind is RunEventKind.WORKSPACE_DIVERGED for event in events)
    assert any(
        event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "resume_restored"
        for event in events
    )
    await second_runtime.close()


@pytest.mark.asyncio
async def test_resume_fails_closed_when_completed_parallel_sibling_overlaps_pending_tool() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"state.txt": "base\n"})
    interrupted_started = asyncio.Event()
    interrupted_invocations = 0
    completed_invocations = 0

    @tool(name="interrupted_edit", idempotent=True, parallel_safe=True, version="v1")
    async def interrupted_edit(value: str) -> str:
        """Simulate a recoverable write that overlaps a parallel sibling."""

        nonlocal interrupted_invocations
        interrupted_invocations += 1
        if interrupted_invocations == 1:
            workspace.write_file("parallel-session", "state.txt", "partial\n")
            interrupted_started.set()
            await asyncio.Event().wait()
        workspace.write_file("parallel-session", "state.txt", f"{value}\n")
        return value

    @tool(name="completed_edit", idempotent=True, parallel_safe=True, version="v1")
    async def completed_edit(value: str) -> str:
        """Commit a sibling write while the other parallel call is pending."""

        nonlocal completed_invocations
        await interrupted_started.wait()
        completed_invocations += 1
        workspace.write_file("parallel-session", "completed.txt", f"{value}\n")
        return value

    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (
                            ToolCall(
                                "interrupted-1",
                                "interrupted_edit",
                                '{"value":"retried"}',
                            ),
                            ToolCall(
                                "completed-1",
                                "completed_edit",
                                '{"value":"committed"}',
                            ),
                        ),
                    )
                )
            ),
            [interrupted_edit, completed_edit],
        ),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="edit in parallel",
            session_id="parallel-session",
            idempotency_key="parallel-request",
        )
    )
    await wait_for_kind(journal, handle.run_id, RunEventKind.TOOL_COMPLETED)
    assert workspace.read_file("parallel-session", "completed.txt") == b"committed\n"
    await first_runtime.close()
    interrupted_events = await journal.read(handle.run_id)
    overlapping_checkpoint = next(
        event
        for event in interrupted_events
        if event.kind is RunEventKind.WORKSPACE_CHECKPOINTED
        and event.data.get("phase") == "before_tool"
        and event.data.get("call_key") == "s1:t1"
    )
    interrupted_snapshot = await first_runtime.load(handle.run_id)

    assert overlapping_checkpoint.sequence not in (
        interrupted_snapshot.safe_checkpoint_sequences
    )

    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [interrupted_edit, completed_edit]),
        journal,
        store=store,
        workspace=workspace,
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    for _ in range(100):
        snapshot = await recovery_runtime.load(handle.run_id)
        if snapshot.state in {RunState.NEEDS_RECONCILIATION, RunState.TERMINAL}:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail("parallel workspace ambiguity did not settle")
    events = await journal.read(handle.run_id)

    assert snapshot.state is RunState.NEEDS_RECONCILIATION
    assert interrupted_invocations == 1
    assert completed_invocations == 1
    assert not recovery_model.requests
    assert workspace.read_file("parallel-session", "state.txt") == b"partial\n"
    assert workspace.read_file("parallel-session", "completed.txt") == b"committed\n"
    assert any(
        event.kind is RunEventKind.WORKSPACE_DIVERGED
        and event.data.get("reason") == "ambiguous_parallel_tool_state"
        for event in events
    )
    try:
        with pytest.raises(RuntimeConflict, match="parallel sibling"):
            await recovery_runtime.submit(
                ResolveRun(
                    run_id=handle.run_id,
                    call_key="s1:t0",
                    action=ResolutionAction.RETRY,
                )
            )
    finally:
        await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_never_restores_or_replays_diverged_non_idempotent_workspace() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"ledger.txt": "before\n"})
    started = asyncio.Event()

    @tool(name="append_ledger", version="v1")
    async def append_ledger(entry: str) -> str:
        """Append a potentially non-idempotent ledger entry."""

        workspace.write_file("ledger-session", "ledger.txt", f"before\n{entry}\n")
        started.set()
        await asyncio.Event().wait()
        return entry

    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (
                            ToolCall(
                                "ledger-1",
                                "append_ledger",
                                '{"entry":"uncertain"}',
                            ),
                        ),
                    )
                )
            ),
            [append_ledger],
        ),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="append",
            session_id="ledger-session",
            idempotency_key="ledger-request",
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await first_runtime.close()

    provider = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    second_runtime = AgentRuntime(
        ReActAgent(provider, [append_ledger]),
        journal,
        store=store,
        workspace=workspace,
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    await wait_for_kind(journal, handle.run_id, RunEventKind.RECONCILIATION_REQUIRED)
    snapshot = await second_runtime.load(handle.run_id)
    events = await journal.read(handle.run_id)

    assert snapshot.state is RunState.NEEDS_RECONCILIATION
    assert workspace.read_file("ledger-session", "ledger.txt") == b"before\nuncertain\n"
    assert not provider.requests
    assert any(event.kind is RunEventKind.WORKSPACE_DIVERGED for event in events)
    await second_runtime.close()


@pytest.mark.asyncio
async def test_lease_heartbeat_cancels_long_provider_call_after_fencing_loss() -> None:
    journal = DropLeaseOnRenewJournal()
    store = InMemoryRuntimeStore()
    provider_started = asyncio.Event()
    provider_cancelled = asyncio.Event()

    class CancellableModel:
        async def complete(self, request: ModelRequest) -> ModelResponse:
            del request
            provider_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                provider_cancelled.set()
                raise
            raise AssertionError("unreachable")

    first_runtime = AgentRuntime(
        ReActAgent(CancellableModel()),
        journal,
        store=store,
        lease_ttl_s=0.06,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="hold", session_id="lease-session")
    )
    await asyncio.wait_for(provider_started.wait(), timeout=1)
    await asyncio.wait_for(journal.lease_dropped.wait(), timeout=1)
    await asyncio.wait_for(provider_cancelled.wait(), timeout=1)
    interrupted = await first_runtime.load(handle.run_id)

    assert interrupted.state is RunState.WAITING_MODEL
    assert interrupted.counts.model_calls == 1
    assert not any(
        event.kind is RunEventKind.MODEL_COMPLETED
        for event in await journal.read(handle.run_id)
    )
    await first_runtime.close()

    recovered_model = ScriptedModel(ModelResponse(AssistantMessage("new fence completed")))
    second_runtime = AgentRuntime(
        ReActAgent(recovered_model),
        journal,
        store=store,
        lease_ttl_s=0.06,
    )
    await second_runtime.submit(ResumeRun(run_id=handle.run_id))
    recovered = await second_runtime.wait(handle.run_id, timeout_s=2)

    assert recovered.state is RunState.TERMINAL
    assert recovered.counts.model_calls == 2
    assert len(recovered_model.requests) == 1
    await second_runtime.close()


@pytest.mark.asyncio
async def test_tool_is_never_invoked_when_write_ahead_intent_cannot_commit() -> None:
    journal = FailToolIntentJournal()
    invocations = 0

    @tool(idempotent=True)
    async def side_effect(value: int) -> int:
        """Count invocations of a guarded external side effect."""

        nonlocal invocations
        invocations += 1
        return value

    runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("effect-1", "side_effect", '{"value":1}'),),
                    )
                )
            ),
            [side_effect],
        ),
        journal,
    )
    handle = await runtime.submit(
        StartRun(prompt="invoke", session_id="intent-session")
    )
    await asyncio.wait_for(journal.intent_failed.wait(), timeout=1)
    snapshot = await runtime.load(handle.run_id)

    assert invocations == 0
    assert snapshot.counts.tool_executions == 0
    assert not any(
        event.kind is RunEventKind.TOOL_STARTED
        for event in await journal.read(handle.run_id)
    )
    await runtime.close()


@pytest.mark.asyncio
async def test_session_rejects_a_second_run_before_session_commit_race() -> None:
    release = asyncio.Event()
    started = 0

    class ConcurrentModel:
        async def complete(self, request: ModelRequest) -> ModelResponse:
            nonlocal started
            started += 1
            await release.wait()
            prompt = request.transcript[-1]
            return ModelResponse(AssistantMessage(f"answer:{prompt.content}"))

    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    runtime = AgentRuntime(ReActAgent(ConcurrentModel()), journal, store=store)
    first = await runtime.submit(
        StartRun(prompt="one", session_id="shared-session", idempotency_key="one")
    )
    with pytest.raises(RuntimeConflict, match="shared-session"):
        await runtime.submit(
            StartRun(prompt="two", session_id="shared-session", idempotency_key="two")
        )
    release.set()
    first_snapshot = await runtime.wait(first.run_id, timeout_s=2)
    session = await store.load_session("shared-session")

    assert session.version == 1
    assert first_snapshot.status == "completed"
    assert started == 1
    assert len(session.transcript) == 2
    await runtime.close()


@pytest.mark.asyncio
async def test_finished_runs_do_not_grow_process_local_state_without_bound() -> None:
    journal = InMemoryRunJournal()
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel()),
        journal,
        max_retained_runs=2,
    )

    run_ids: list[str] = []
    for index in range(5):
        model = ScriptedModel(ModelResponse(AssistantMessage(f"answer {index}")))
        runtime.agent = ReActAgent(model)
        handle = await runtime.submit(
            StartRun(prompt=f"question {index}", session_id=f"retention-{index}")
        )
        await runtime.wait(handle.run_id, timeout_s=2)
        run_ids.append(handle.run_id)

    # A finished run keeps no live bus, no in-process result and no traceback:
    # every one of those is rebuildable from the durable journal.
    assert len(runtime._buses) <= 2
    assert len(runtime._results) <= 2
    assert len(runtime._task_errors) <= 2

    # Eviction is a cache policy, never a loss of fact. Each run still folds
    # from sequence 1 with its answer intact.
    for index, run_id in enumerate(run_ids):
        snapshot = await runtime.load(run_id)
        assert snapshot.status == "completed"
        assert snapshot.result is not None
        assert snapshot.result["output"] == f"answer {index}"
    await runtime.close()


@pytest.mark.asyncio
async def test_pruning_never_evicts_a_still_executing_run() -> None:
    journal = InMemoryRunJournal()
    blocking = BlockingModel()
    runtime = AgentRuntime(ReActAgent(blocking), journal, max_retained_runs=1)

    running = await runtime.submit(
        StartRun(prompt="stay running", session_id="live-session")
    )
    await asyncio.wait_for(blocking.started.wait(), timeout=2)
    live_bus = runtime._buses.setdefault(running.run_id, runtime_module._LiveBus())

    for index in range(4):
        model = ScriptedModel(ModelResponse(AssistantMessage(f"done {index}")))
        runtime.agent = ReActAgent(model)
        handle = await runtime.submit(
            StartRun(prompt=f"short {index}", session_id=f"short-{index}")
        )
        await runtime.wait(handle.run_id, timeout_s=2)

    # The in-flight producer already bound this bus; evicting it would split it
    # from any follower that attaches afterwards.
    assert runtime._buses.get(running.run_id) is live_bus
    await runtime.close()


@pytest.mark.asyncio
async def test_history_reads_do_not_register_live_buses() -> None:
    journal = InMemoryRunJournal()
    runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("archived")))),
        journal,
    )
    handle = await runtime.submit(StartRun(prompt="archive me", session_id="history"))
    await runtime.wait(handle.run_id, timeout_s=2)
    runtime._buses.clear()

    replayed = [
        event
        async for event in runtime.follow(handle.run_id, live=False)
    ]

    assert replayed[0].kind == "run_started"
    assert replayed[-1].terminal is True
    # Browsing history must not attach process-local state to every run id.
    assert runtime._buses == {}
    await runtime.close()
