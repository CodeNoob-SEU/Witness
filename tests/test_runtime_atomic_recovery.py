from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from react_agent.agent import AgentConfig, ReActAgent
from react_agent.context import (
    ContextCompression,
    ContextCompressionRequest,
    ContextGovernor,
    ContextStrategy,
    FileContextSummaryStore,
)
from react_agent.cost import Price, PricingCatalog
from react_agent.events import RunEventDraft, RunEventKind, RunState, StoredRunEvent
from react_agent.journal import InMemoryRunJournal, JournalLease
from react_agent.models import (
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    Usage,
    transcript_to_json,
)
from react_agent.runtime import (
    AgentRuntime,
    InMemoryRuntimeStore,
    ReconciliationRequired,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)
from react_agent.tools import Tool, ToolExecutionContext, tool
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


class BlockingAfterScriptModel(ScriptedModel):
    def __init__(self, *responses: ModelResponse, block_when_exhausted: bool) -> None:
        super().__init__(*responses)
        self.block_when_exhausted = block_when_exhausted
        self.blocked = asyncio.Event()

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if self.responses:
            return self.responses.popleft()
        if not self.block_when_exhausted:
            pytest.fail("unexpected provider call")
        self.blocked.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class CountingContextCompressor:
    revision = "atomic-context-compressor-v1"

    def __init__(self) -> None:
        self.calls = 0

    async def compress(
        self,
        request: ContextCompressionRequest,
    ) -> ContextCompression:
        self.calls += 1
        return ContextCompression(
            "persisted atomic context summary",
            Usage(input_tokens=300, output_tokens=30, total_tokens=330),
            request_id=f"compress-{self.calls}",
            response_model="gpt-test",
        )


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


class BlockAfterReconciliationCommitJournal(CrashBeforeAtomicBatchCommitJournal):
    def __init__(self) -> None:
        super().__init__(RunEventKind.TOOL_COMPLETED)
        self.reconciliation_committed = asyncio.Event()
        self.release_reconciliation = asyncio.Event()
        self._block_reconciliation = True

    async def append(
        self,
        run_id: str,
        draft: RunEventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: JournalLease | None = None,
    ) -> StoredRunEvent:
        committed = await super().append(
            run_id,
            draft,
            expected_sequence=expected_sequence,
            operation_id=operation_id,
            lease=lease,
        )
        if (
            self._block_reconciliation
            and draft.kind is RunEventKind.RECONCILIATION_REQUIRED
        ):
            self._block_reconciliation = False
            self.reconciliation_committed.set()
            await self.release_reconciliation.wait()
        return committed


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


def _compression_agent(
    model: ScriptedModel,
    large_observation: Tool,
    compressor: CountingContextCompressor,
    summary_root: Path,
) -> ReActAgent:
    return ReActAgent(
        model,
        [large_observation],
        config=AgentConfig(
            max_steps=3,
            max_tool_output_chars=16_000,
            max_context_chars=1_800,
            context_strategy=ContextStrategy.GENERIC,
            context_keep_recent_turns=1,
            context_summary_max_chars=512,
        ),
        context_governor=ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=compressor,
            store=FileContextSummaryStore(summary_root),
            keep_recent_turns=1,
            max_summary_chars=512,
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
async def test_context_compression_ack_loss_keeps_one_known_cost_and_reuses_summary(
    tmp_path: Path,
) -> None:
    journal = CrashAfterAtomicBatchCommitJournal(RunEventKind.CONTEXT_GOVERNED)
    runtime_store = InMemoryRuntimeStore()
    summary_root = tmp_path / "summaries"
    tool_invocations = 0

    @tool(idempotent=True, version="v1")
    async def large_observation(label: str) -> str:
        """Return a deterministic repository observation large enough to govern."""

        nonlocal tool_invocations
        tool_invocations += 1
        return f"FACT[{label}] " + ("x" * 12_000)

    first_compressor = CountingContextCompressor()
    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("observe-1", "large_observation", '{"label":"current"}'),),
            )
        )
    )
    first_runtime = AgentRuntime(
        _compression_agent(
            first_model,
            large_observation,
            first_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        pricing=pricing("v1", "2"),
        provider_name="openai",
        model_name="gpt-test",
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="inspect the current repository fact",
            session_id="atomic-context-after",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.batch_committed.wait(), timeout=1)
    await first_runtime.close()

    recovery_compressor = CountingContextCompressor()
    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("FACT[current]")))
    recovery_runtime = AgentRuntime(
        _compression_agent(
            recovery_model,
            large_observation,
            recovery_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        pricing=pricing("v2", "200"),
        provider_name="openai",
        model_name="gpt-test",
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    terminals = [
        event
        for event in events
        if event.kind is RunEventKind.CONTEXT_GOVERNED
        and event.data.get("compression_phase") == "completed"
    ]
    assert len(terminals) == 1
    terminal = terminals[0]
    assert terminal.model_calls_delta == 1
    assert terminal.usage_delta == Usage(input_tokens=300, output_tokens=30, total_tokens=330)
    assert "summary_key" not in terminal.data
    assert "source_hash" not in terminal.data
    assert terminal.checkpoint is not None
    assert "context_compression" in terminal.checkpoint
    private_identity = terminal.checkpoint["context_compression"]
    assert isinstance(private_identity, Mapping)
    public_event_blob = json.dumps(
        [dict(event.data) for event in events],
        default=dict,
        ensure_ascii=False,
        sort_keys=True,
    )
    assert all(
        isinstance(value, str) and value not in public_event_blob
        for value in private_identity.values()
    )

    compressor_costs = [
        event
        for event in events
        if event.kind is RunEventKind.COST_RECORDED
        and event.data.get("operation_id")
        == f"context_compressor:{terminal.operation_id}"
    ]
    assert len(compressor_costs) == 1
    assert compressor_costs[0].sequence == terminal.sequence + 1
    assert compressor_costs[0].data["amount_micros"] == 660
    assert compressor_costs[0].data["pricing_catalog_version"] == "v1"

    projections = [
        event
        for event in events
        if event.kind is RunEventKind.CONTEXT_GOVERNED
        and event.data.get("compression_phase") == "projection_completed"
    ]
    assert len(projections) == 1
    assert projections[0].model_calls_delta == 0
    assert projections[0].usage_delta == Usage()
    assert projections[0].data["compression_cache_hit"] is True

    assert completed.state is RunState.TERMINAL
    assert completed.counts.model_calls == 3
    assert completed.usage == Usage(input_tokens=300, output_tokens=30, total_tokens=330)
    assert first_compressor.calls == 1
    assert recovery_compressor.calls == 0
    assert len(first_model.requests) == 1
    assert len(recovery_model.requests) == 1
    assert tool_invocations == 1
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_context_compression_precommit_crash_records_one_unknown_cost_and_reuses_summary(
    tmp_path: Path,
) -> None:
    journal = CrashBeforeAtomicBatchCommitJournal(RunEventKind.CONTEXT_GOVERNED)
    runtime_store = InMemoryRuntimeStore()
    summary_root = tmp_path / "summaries"
    tool_invocations = 0

    @tool(idempotent=True, version="v1")
    async def large_observation(label: str) -> str:
        """Return a deterministic repository observation large enough to govern."""

        nonlocal tool_invocations
        tool_invocations += 1
        return f"FACT[{label}] " + ("x" * 12_000)

    first_compressor = CountingContextCompressor()
    first_model = ScriptedModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("observe-1", "large_observation", '{"label":"current"}'),),
            )
        )
    )
    first_runtime = AgentRuntime(
        _compression_agent(
            first_model,
            large_observation,
            first_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        pricing=pricing("v1", "2"),
        provider_name="openai",
        model_name="gpt-test",
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="inspect the current repository fact",
            session_id="atomic-context-before",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.batch_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_compressor = CountingContextCompressor()
    recovery_model = ScriptedModel(ModelResponse(AssistantMessage("FACT[current]")))
    recovery_runtime = AgentRuntime(
        _compression_agent(
            recovery_model,
            large_observation,
            recovery_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        pricing=pricing("v2", "200"),
        provider_name="openai",
        model_name="gpt-test",
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    events = await journal.read(handle.run_id)

    assert not any(
        event.kind is RunEventKind.CONTEXT_GOVERNED
        and event.data.get("compression_phase") == "completed"
        for event in events
    )
    abandonments = [
        event
        for event in events
        if event.kind is RunEventKind.CONTEXT_GOVERNED
        and event.data.get("compression_phase") == "abandoned"
    ]
    assert len(abandonments) == 1
    abandoned = abandonments[0]
    assert abandoned.model_calls_delta == 1
    assert abandoned.usage_delta == Usage()
    assert abandoned.data["cost_unknown"] is True
    assert "summary_key" not in abandoned.data
    assert "source_hash" not in abandoned.data

    compressor_costs = [
        event
        for event in events
        if event.kind is RunEventKind.COST_RECORDED
        and event.data.get("operation_id")
        == f"context_compressor:{abandoned.operation_id}"
    ]
    assert len(compressor_costs) == 1
    assert compressor_costs[0].sequence == abandoned.sequence + 1
    assert compressor_costs[0].data["amount_micros"] is None
    assert compressor_costs[0].data["unknown_reason"] == (
        "process_interrupted_before_terminal"
    )

    projections = [
        event
        for event in events
        if event.kind is RunEventKind.CONTEXT_GOVERNED
        and event.data.get("compression_phase") == "projection_completed"
    ]
    assert len(projections) == 1
    assert projections[0].model_calls_delta == 0
    assert projections[0].usage_delta == Usage()
    assert projections[0].data["compression_cache_hit"] is True

    assert completed.state is RunState.TERMINAL
    assert completed.counts.model_calls == 3
    assert completed.usage == Usage()
    assert first_compressor.calls == 1
    assert recovery_compressor.calls == 0
    assert len(first_model.requests) == 1
    assert len(recovery_model.requests) == 1
    assert tool_invocations == 1
    await recovery_runtime.close()


@pytest.mark.asyncio
async def test_snapshot_eviction_replays_identical_active_projection_and_reuses_summary(
    tmp_path: Path,
) -> None:
    journal = InMemoryRunJournal()
    runtime_store = InMemoryRuntimeStore()
    summary_root = tmp_path / "summaries"
    tool_invocations = 0

    @tool(idempotent=True, version="v1")
    async def large_observation(label: str) -> str:
        """Return a deterministic repository observation large enough to govern."""

        nonlocal tool_invocations
        tool_invocations += 1
        return f"FACT[{label}] " + ("x" * 12_000)

    first_compressor = CountingContextCompressor()
    first_model = BlockingAfterScriptModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("observe-1", "large_observation", '{"label":"current"}'),),
            )
        ),
        block_when_exhausted=True,
    )
    first_runtime = AgentRuntime(
        _compression_agent(
            first_model,
            large_observation,
            first_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        provider_name="openai",
        model_name="gpt-test",
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="inspect the current repository fact",
            session_id="context-replay",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(first_model.blocked.wait(), timeout=2)
    assert len(first_model.requests) == 2
    first_projection = json.dumps(
        transcript_to_json(first_model.requests[1].transcript),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert "persisted generated context summary" in first_projection
    await first_runtime.close()

    assert await journal.evict_snapshot(handle.run_id) is True
    rebuilt = await journal.load(handle.run_id)
    assert rebuilt.state is not RunState.TERMINAL

    recovery_compressor = CountingContextCompressor()
    recovery_model = BlockingAfterScriptModel(
        ModelResponse(AssistantMessage("FACT[current]")),
        block_when_exhausted=False,
    )
    recovery_runtime = AgentRuntime(
        _compression_agent(
            recovery_model,
            large_observation,
            recovery_compressor,
            summary_root,
        ),
        journal,
        store=runtime_store,
        provider_name="openai",
        model_name="gpt-test",
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=2)
    assert len(recovery_model.requests) == 1
    replayed_projection = json.dumps(
        transcript_to_json(recovery_model.requests[0].transcript),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    assert replayed_projection == first_projection
    assert first_compressor.calls == 1
    assert recovery_compressor.calls == 0
    assert tool_invocations == 1
    assert completed.state is RunState.TERMINAL
    assert completed.status == "completed"
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


@pytest.mark.asyncio
async def test_persisted_reconciliation_precedes_active_resume_noop() -> None:
    journal = BlockAfterReconciliationCommitJournal()
    store = InMemoryRuntimeStore()
    workspace = FakeWorkspaceCheckpointStore({"ledger.txt": "before\n"})
    invocations = 0

    @tool(version="v1")
    async def charge_account(amount: int) -> str:
        """Perform a non-idempotent external charge."""

        nonlocal invocations
        invocations += 1
        return f"charged:{amount}"

    first_runtime = AgentRuntime(
        ReActAgent(
            ScriptedModel(
                ModelResponse(
                    AssistantMessage(
                        None,
                        (ToolCall("charge-1", "charge_account", '{"amount":7}'),),
                    )
                )
            ),
            [charge_account],
        ),
        journal,
        store=store,
        workspace=workspace,
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="charge",
            session_id="persisted-reconciliation-priority",
            idempotency_key="request",
        )
    )
    await asyncio.wait_for(journal.batch_attempted.wait(), timeout=1)
    await first_runtime.close()

    recovery_runtime = AgentRuntime(
        ReActAgent(ScriptedModel(), [charge_account]),
        journal,
        store=store,
        workspace=workspace,
    )
    try:
        await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
        await asyncio.wait_for(journal.reconciliation_committed.wait(), timeout=1)

        persisted = await recovery_runtime.load(handle.run_id)
        assert persisted.state is RunState.NEEDS_RECONCILIATION
        with pytest.raises(ReconciliationRequired):
            await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    finally:
        journal.release_reconciliation.set()
        await recovery_runtime.close()

    assert invocations == 1
