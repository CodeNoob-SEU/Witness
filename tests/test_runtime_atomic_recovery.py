from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from react_agent.agent import ReActAgent
from react_agent.cost import Price, PricingCatalog
from react_agent.events import RunEventDraft, RunEventKind, RunState, StoredRunEvent
from react_agent.journal import InMemoryRunJournal, JournalLease
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse, ToolCall, Usage
from react_agent.runtime import (
    AgentRuntime,
    InMemoryRuntimeStore,
    ReconciliationRequired,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)
from react_agent.tools import ToolExecutionContext, tool
from react_agent.workspace import FakeWorkspaceCheckpointStore


class _SimulatedProcessCrash(BaseException):
    """Stop orchestration at a commit point without normal error handling."""


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            pytest.fail("unexpected provider call")
        return self.responses.popleft()


class CrashAfterAtomicBatchCommitJournal(InMemoryRunJournal):
    def __init__(self, crash_on: RunEventKind) -> None:
        super().__init__()
        self.crash_on = crash_on
        self.batch_committed = asyncio.Event()
        self._armed = True

    async def append_many(
        self,
        run_id: str,
        entries: tuple[tuple[RunEventDraft, str], ...],
        *,
        expected_sequence: int,
        lease: JournalLease | None = None,
    ) -> tuple[StoredRunEvent, ...]:
        committed = await super().append_many(
            run_id,
            entries,
            expected_sequence=expected_sequence,
            lease=lease,
        )
        if self._armed and entries[0][0].kind is self.crash_on:
            self._armed = False
            self.batch_committed.set()
            raise _SimulatedProcessCrash
        return committed


class CrashBeforeAtomicBatchCommitJournal(InMemoryRunJournal):
    def __init__(self, crash_on: RunEventKind) -> None:
        super().__init__()
        self.crash_on = crash_on
        self.batch_attempted = asyncio.Event()
        self._armed = True

    async def append_many(
        self,
        run_id: str,
        entries: tuple[tuple[RunEventDraft, str], ...],
        *,
        expected_sequence: int,
        lease: JournalLease | None = None,
    ) -> tuple[StoredRunEvent, ...]:
        if self._armed and entries[0][0].kind is self.crash_on:
            self._armed = False
            self.batch_attempted.set()
            raise _SimulatedProcessCrash
        return await super().append_many(
            run_id,
            entries,
            expected_sequence=expected_sequence,
            lease=lease,
        )


def pricing(version: str, rate: str) -> PricingCatalog:
    return PricingCatalog(
        version,
        (
            Price(
                "openai",
                "gpt-test",
                f"price-{version}",
                datetime(2026, 1, 1, tzinfo=UTC),
                Decimal(rate),
                Decimal(rate),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_model_completion_ack_loss_keeps_one_frozen_cost_and_never_reinvokes_provider(
) -> None:
    journal = CrashAfterAtomicBatchCommitJournal(RunEventKind.MODEL_COMPLETED)
    store = InMemoryRuntimeStore()
    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage("committed before the ACK was lost"),
            usage=Usage(input_tokens=1_000, output_tokens=500, total_tokens=1_500),
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model),
        journal,
        store=store,
        pricing=pricing("v1", "2"),
        provider_name="openai",
        model_name="gpt-test",
    )
    handle = await first_runtime.submit(
        StartRun(prompt="price once", session_id="atomic-model", idempotency_key="request")
    )
    await asyncio.wait_for(journal.batch_committed.wait(), timeout=1)
    interrupted = await first_runtime.load(handle.run_id)

    assert interrupted.state is not RunState.TERMINAL
    assert len(interrupted.costs) == 1
    assert interrupted.costs[0]["amount_micros"] == 3_000
    assert interrupted.costs[0]["pricing_catalog_version"] == "v1"
    await first_runtime.close()

    recovery_model = ScriptedModel()
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model),
        journal,
        store=store,
        pricing=pricing("v2", "200"),
        provider_name="openai",
        model_name="gpt-test",
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)

    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
    assert len(first_model.requests) == 1
    assert not recovery_model.requests
    assert len(completed.costs) == 1
    assert completed.costs[0]["amount_micros"] == 3_000
    assert completed.costs[0]["pricing_catalog_version"] == "v1"
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_failed_execution_releases_its_lease_without_runtime_shutdown() -> None:
    journal = CrashBeforeAtomicBatchCommitJournal(RunEventKind.MODEL_COMPLETED)
    store = InMemoryRuntimeStore()
    first_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(ModelResponse(AssistantMessage("lost before commit")))),
        journal,
        store=store,
        model_name="gpt-test",
        lease_ttl_s=0.03,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="recover", session_id="lease-release", idempotency_key="request")
    )
    await asyncio.wait_for(journal.batch_attempted.wait(), timeout=1)
    await asyncio.sleep(0.08)

    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("recovered")))
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model),
        journal,
        store=store,
        model_name="gpt-test",
        lease_ttl_s=0.03,
    )
    try:
        try:
            await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
        except RuntimeConflict as exc:  # pragma: no cover - assertion gives clearer red output
            pytest.fail(f"failed execution kept renewing its lease: {exc}")
        completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)

        assert completed.state is RunState.TERMINAL
        assert completed.status == "completed"
        assert len(recovery_model.requests) == 1
    finally:
        await first_runtime.close()
        await recovery_runtime.close()


@pytest.mark.asyncio
async def test_tool_completion_ack_loss_reuses_committed_result_without_reexecution() -> None:
    journal = CrashAfterAtomicBatchCommitJournal(RunEventKind.TOOL_COMPLETED)
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"state.txt": "base\n"})
    invocations: list[ToolExecutionContext] = []

    @tool(idempotent=True, version="v1")
    async def write_state(value: str, *, context: ToolExecutionContext) -> str:
        """Write one deterministic value in the managed workspace."""

        invocations.append(context)
        workspace.write_file("atomic-tool", "state.txt", f"{value}\n")
        return value

    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("write-1", "write_state", '{"value":"done"}'),),
                    )
                )
            ),
            [write_state],
        ),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="write", session_id="atomic-tool", idempotency_key="request")
    )
    await asyncio.wait_for(journal.batch_committed.wait(), timeout=1)
    await first_runtime.close()

    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("reused")))
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [write_state]),
        journal,
        store=store,
        workspace=workspace,
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    assert completed.state is RunState.TERMINAL
    assert len(invocations) == 1
    assert sum(event.kind is RunEventKind.TOOL_COMPLETED for event in events) == 1
    assert any(event.kind is RunEventKind.TOOL_REUSED for event in events)
    assert workspace.read_file("atomic-tool", "state.txt") == b"done\n"
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_non_idempotent_tool_batch_failure_requires_reconciliation() -> None:
    journal = CrashBeforeAtomicBatchCommitJournal(RunEventKind.TOOL_COMPLETED)
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"ledger.txt": "before\n"})
    invocations = 0

    @tool(version="v1")
    async def append_ledger(entry: str) -> str:
        """Append an external-style entry that cannot be retried safely."""

        nonlocal invocations
        invocations += 1
        workspace.write_file("atomic-non-idempotent", "ledger.txt", f"before\n{entry}\n")
        return entry

    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("append-1", "append_ledger", '{"entry":"charged"}'),),
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
            session_id="atomic-non-idempotent",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.batch_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("must not run")))
    recovery_runtime = AgentRuntime(
        ReActAgent(recovery_model, [append_ledger]),
        journal,
        store=store,
        workspace=workspace,
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    for _ in range(100):
        snapshot = await recovery_runtime.load(handle.run_id)
        if snapshot.state is RunState.NEEDS_RECONCILIATION:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail("run never entered reconciliation")

    assert invocations == 1
    assert not recovery_model.requests
    assert not any(
        event.kind is RunEventKind.TOOL_COMPLETED
        for event in await journal.read(handle.run_id)
    )
    with pytest.raises(ReconciliationRequired):
        await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    await recovery_runtime.close()
