from __future__ import annotations

import asyncio

import pytest

from react_agent.cost_ledger import (
    CostAdjustmentConflictError,
    CostAdjustmentDraft,
    InMemoryCostAdjustmentStore,
    deterministic_adjustment_record_id,
)
from react_agent.events import RunEventDraft, RunEventKind
from react_agent.journal import InMemoryRunJournal


def unknown_cost() -> dict[str, object]:
    return {
        "record_id": "cost-original",
        "operation_id": "model:step-1:attempt-1",
        "currency": "USD",
        "amount_micros": None,
        "unknown_reason": "provider_completion_not_committed",
        "provider": "compatible",
        "model": "gpt-test",
        "usage": {"input_tokens": 10, "output_tokens": 2},
    }


@pytest.mark.asyncio
async def test_in_memory_cost_adjustments_are_linear_append_only_and_idempotent() -> None:
    store = InMemoryCostAdjustmentStore(clock=lambda: 1_750_000_000.0)
    first_draft = CostAdjustmentDraft(
        record_id="cost-adjustment-1",
        operation_id="invoice-line-1",
        previous_record_id="cost-original",
        revised_total_micros=125,
        note="provider invoice",
    )

    first, retried = await asyncio.gather(
        store.append_cost_adjustment(
            "run-1", first_draft, previous_record=unknown_cost()
        ),
        store.append_cost_adjustment(
            "run-1", first_draft, previous_record=unknown_cost()
        ),
    )
    assert sorted((first.created, retried.created)) == [False, True]
    committed = first.record
    assert committed == retried.record
    assert committed.public_payload["amount_micros"] == 125
    assert committed.public_payload["operation_total_micros"] == 125
    assert committed.public_payload["adjusted_operation_id"] == "model:step-1:attempt-1"
    assert committed.public_payload["adjustment_operation_id"] == "invoice-line-1"
    assert committed.public_payload["note"] == "provider invoice"

    with pytest.raises(CostAdjustmentConflictError, match="different content"):
        await store.append_cost_adjustment(
            "run-1",
            CostAdjustmentDraft(
                record_id="cost-adjustment-1",
                operation_id="invoice-line-1",
                previous_record_id="cost-original",
                revised_total_micros=126,
            ),
            previous_record=unknown_cost(),
        )
    with pytest.raises(CostAdjustmentConflictError, match="latest record"):
        await store.append_cost_adjustment(
            "run-1",
            CostAdjustmentDraft(
                record_id="cost-adjustment-competing",
                operation_id="invoice-line-competing",
                previous_record_id="cost-original",
                revised_total_micros=130,
            ),
            previous_record=unknown_cost(),
        )

    second_draft = CostAdjustmentDraft(
        record_id="cost-adjustment-2",
        operation_id="invoice-line-2",
        previous_record_id="cost-adjustment-1",
        revised_total_micros=75,
        note="credit",
    )
    second = await store.append_cost_adjustment(
        "run-1",
        second_draft,
        # The store resolves an adjustment predecessor authoritatively; this
        # caller projection only needs to retain the requested identity.
        previous_record=committed.public_payload,
    )
    assert second.created is True
    assert second.record.ledger_sequence == 2
    assert second.record.public_payload["amount_micros"] == -50
    assert second.record.public_payload["operation_total_micros"] == 75
    assert tuple(
        item.record_id for item in await store.list_cost_adjustments("run-1")
    ) == ("cost-adjustment-1", "cost-adjustment-2")


def test_adjustment_record_id_is_stable_and_run_scoped() -> None:
    first = deterministic_adjustment_record_id("run-1", "invoice-1")
    assert first == deterministic_adjustment_record_id("run-1", "invoice-1")
    assert first != deterministic_adjustment_record_id("run-2", "invoice-1")


@pytest.mark.asyncio
async def test_adjustment_accepts_nested_immutable_data_from_a_folded_snapshot() -> None:
    journal = InMemoryRunJournal()
    await journal.create(
        "run-folded",
        RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            session_id="session-folded",
            agent_revision="agent-v1",
            tool_manifest_hash="tools-v1",
            data={"status": "running"},
        ),
        operation_id="start",
    )
    await journal.append(
        "run-folded",
        RunEventDraft(
            kind=RunEventKind.COST_RECORDED,
            data={
                **unknown_cost(),
                "unit_prices_per_million": {
                    "input": "1.00",
                    "output": "2.00",
                },
            },
        ),
        expected_sequence=1,
        operation_id="cost",
    )
    snapshot = await journal.load("run-folded")
    [previous] = snapshot.costs
    store = InMemoryCostAdjustmentStore(clock=lambda: 1_750_000_000.0)

    appended = await store.append_cost_adjustment(
        "run-folded",
        CostAdjustmentDraft(
            record_id="folded-adjustment",
            operation_id="folded-invoice",
            previous_record_id="cost-original",
            revised_total_micros=500,
        ),
        previous_record=previous,
    )

    assert appended.record.public_payload["unit_prices_per_million"] == {
        "input": "1.00",
        "output": "2.00",
    }
    assert appended.record.public_payload["usage"] == {
        "input_tokens": 10,
        "output_tokens": 2,
    }
