from __future__ import annotations

import pytest

from react_agent.agent import ReActAgent
from react_agent.events import RunState
from react_agent.journal import InMemoryRunJournal
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse, Usage
from react_agent.runtime import (
    AdjustCost,
    AgentRuntime,
    InMemoryRuntimeStore,
    RuntimeConflict,
    StartRun,
)


class AnswerModel:
    async def complete(self, request: ModelRequest) -> ModelResponse:
        del request
        return ModelResponse(
            AssistantMessage("done"),
            usage=Usage(input_tokens=100, output_tokens=20, total_tokens=120),
        )


@pytest.mark.asyncio
async def test_runtime_merges_post_terminal_adjustments_without_reopening_run_events() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    runtime = AgentRuntime(ReActAgent(AnswerModel()), journal, store=store)
    started = await runtime.submit(
        StartRun(
            prompt="price this",
            session_id="cost-session",
            idempotency_key="cost-run",
        )
    )
    completed = await runtime.wait(started.run_id, timeout_s=2)
    assert completed.state is RunState.TERMINAL
    [unknown] = completed.costs
    assert unknown["amount_micros"] is None
    previous_record_id = str(unknown["record_id"])
    durable_before = await journal.read(started.run_id)

    command = AdjustCost(
        run_id=started.run_id,
        previous_record_id=previous_record_id,
        revised_total_micros=375,
        note="provider invoice",
        operation_id="invoice-line-1",
    )
    first = await runtime.submit(command)
    retried = await runtime.submit(command)
    assert first.created is True
    assert retried.created is False

    adjusted = await runtime.load(started.run_id)
    assert len(adjusted.costs) == 2
    invoice = adjusted.costs[-1]
    assert invoice["kind"] == "adjustment"
    assert invoice["amount_micros"] == 375
    assert invoice["adjusts_record_id"] == previous_record_id
    assert invoice["note"] == "provider invoice"
    assert invoice["ledger_sequence"] == 1

    with pytest.raises(RuntimeConflict, match="different content"):
        await runtime.submit(
            AdjustCost(
                run_id=started.run_id,
                previous_record_id=previous_record_id,
                revised_total_micros=376,
                operation_id="invoice-line-1",
            )
        )
    with pytest.raises(RuntimeConflict, match="latest record"):
        await runtime.submit(
            AdjustCost(
                run_id=started.run_id,
                previous_record_id=previous_record_id,
                revised_total_micros=400,
                operation_id="invoice-line-competing",
            )
        )

    chained = await runtime.submit(
        AdjustCost(
            run_id=started.run_id,
            previous_record_id=str(invoice["record_id"]),
            revised_total_micros=200,
            note="credit applied",
            operation_id="invoice-line-2",
        )
    )
    assert chained.created is True
    latest = await runtime.load(started.run_id)
    assert latest.costs[-1]["amount_micros"] == -175
    assert latest.costs[-1]["operation_total_micros"] == 200
    assert (await runtime.list_session_runs("cost-session"))[0].costs == latest.costs

    # A new Runtime process can rebuild the merged read model from the shared
    # run journal and independent in-memory store.
    restarted = AgentRuntime(ReActAgent(AnswerModel()), journal, store=store)
    assert (await restarted.load(started.run_id)).costs == latest.costs

    durable_after = await journal.read(started.run_id)
    assert durable_after == durable_before
    assert durable_after[-1].sequence == latest.last_sequence
    assert durable_after[-1].event_hash == latest.last_hash
    await runtime.close()
    await restarted.close()
