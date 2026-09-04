"""Tests for the evaluation harness itself.

The point of most of these is negative: a harness that cannot fail a task is
worthless, so several of them deliberately run a model that does the wrong
thing and assert the report says so.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from react_agent.agent import AgentConfig
from react_agent.cost import Price, PricingCatalog
from react_agent.evals import (
    WORKSPACE_SUITE,
    EvalReport,
    EvalTask,
    ReferenceWorkspaceModel,
    TaskCheck,
    TaskOutcome,
    run_suite,
    run_task,
)
from react_agent.models import (
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolMessage,
    Usage,
)

FAST = AgentConfig(max_steps=6, max_tool_calls=8, parallel_tool_calls=False)


class _IdleModel:
    """Answers immediately and never touches a tool."""

    model = "idle-model"

    async def complete(self, request: ModelRequest) -> ModelResponse:
        del request
        return ModelResponse(AssistantMessage("Sure, all done!"))


class _WrongContentModel:
    """Creates the right file with the wrong bytes."""

    model = "wrong-content-model"

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if any(isinstance(item, ToolMessage) for item in request.transcript):
            return ModelResponse(AssistantMessage("Created notes/hello.txt."))
        return ModelResponse(
            AssistantMessage(
                tool_calls=(
                    ToolCall(
                        "call-1",
                        "write_workspace_file",
                        json.dumps({"path": "notes/hello.txt", "content": "x"}),
                    ),
                )
            )
        )


class _CountingModel(ReferenceWorkspaceModel):
    """The reference policy, but reporting token usage so cost can be priced."""

    model = "counting-model"

    async def complete(self, request: ModelRequest) -> ModelResponse:
        response = await super().complete(request)
        return ModelResponse(
            response.message,
            usage=Usage(input_tokens=1_000, output_tokens=500, total_tokens=1_500),
        )


@pytest.mark.asyncio
async def test_the_reference_model_passes_every_built_in_task() -> None:
    report = await run_suite(
        WORKSPACE_SUITE, build_model=ReferenceWorkspaceModel, config=FAST
    )

    assert report.passed == len(WORKSPACE_SUITE)
    assert report.pass_rate == 1.0
    assert [outcome.task for outcome in report.outcomes] == [
        task.name for task in WORKSPACE_SUITE
    ]
    for outcome in report.outcomes:
        assert outcome.status == "completed", outcome
        assert outcome.run_id


@pytest.mark.asyncio
async def test_an_agent_that_does_nothing_fails_the_suite() -> None:
    """A confident final answer must not be mistaken for a completed task."""

    report = await run_suite(WORKSPACE_SUITE, build_model=_IdleModel, config=FAST)

    # Only the refusal task can pass by doing nothing, which is the correct
    # answer there: nothing escaped the workspace.
    failed = {outcome.task for outcome in report.outcomes if not outcome.passed}
    assert failed == {
        "create-file",
        "read-and-report",
        "edit-existing-file",
        "multi-step-edit",
    }
    assert report.pass_rate == pytest.approx(1 / 5)
    detail = next(o.detail for o in report.outcomes if o.task == "create-file")
    assert "never created" in detail
    # Every run still finished cleanly; grading is what caught the difference.
    assert all(outcome.status == "completed" for outcome in report.outcomes)
    assert all(outcome.tool_executions == 0 for outcome in report.outcomes)


@pytest.mark.asyncio
async def test_a_task_is_graded_on_content_not_on_the_files_existence() -> None:
    """Creating the file is not enough; the bytes have to be right."""

    outcome = await run_task(
        WORKSPACE_SUITE[0], build_model=_WrongContentModel, config=FAST
    )

    assert outcome.tool_executions == 1
    assert outcome.status == "completed"
    assert outcome.passed is False
    assert "expected" in outcome.detail


@pytest.mark.asyncio
async def test_a_checker_that_raises_is_a_failure_not_a_pass() -> None:
    def exploding(worktree: Path, answer: str | None) -> TaskCheck:
        del worktree, answer
        raise RuntimeError("checker bug")

    outcome = await run_task(
        EvalTask(name="broken", prompt="do nothing", check=exploding),
        build_model=_IdleModel,
        config=FAST,
    )

    assert outcome.passed is False
    assert "RuntimeError" in outcome.detail


@pytest.mark.asyncio
async def test_cost_is_summed_from_the_durable_ledger_when_priced() -> None:
    catalog = PricingCatalog(
        "eval-test",
        (
            Price(
                provider="openai_compatible",
                model="counting-model",
                version="v1",
                effective_from=datetime(2000, 1, 1, tzinfo=UTC),
                input_per_million=Decimal("1.00"),
                output_per_million=Decimal("2.00"),
            ),
        ),
    )

    outcome = await run_task(
        WORKSPACE_SUITE[0],
        build_model=_CountingModel,
        config=FAST,
        pricing=catalog,
    )

    # Two model calls at 1000 in / 500 out: 2 * (0.001 + 0.001) USD.
    assert outcome.model_calls == 2
    assert outcome.input_tokens == 2_000
    assert outcome.output_tokens == 1_000
    assert outcome.currency == "USD"
    assert outcome.cost_micros == 4_000


@pytest.mark.asyncio
async def test_unpriced_runs_report_unknown_cost_rather_than_zero() -> None:
    outcome = await run_task(
        WORKSPACE_SUITE[0], build_model=ReferenceWorkspaceModel, config=FAST
    )

    assert outcome.cost_micros is None
    report = EvalReport((outcome,))
    assert report.total_cost_micros is None
    assert "unknown" in report.to_markdown()


def test_a_single_unknown_cost_makes_the_total_unknown() -> None:
    def outcome(name: str, cost: int | None) -> TaskOutcome:
        return TaskOutcome(
            task=name,
            passed=True,
            detail="",
            status="completed",
            stop_reason="completed",
            model_calls=1,
            tool_calls=0,
            tool_executions=0,
            input_tokens=10,
            output_tokens=5,
            cost_micros=cost,
            currency="USD" if cost is not None else None,
            duration_s=0.1,
            run_id=f"run-{name}",
        )

    known = EvalReport((outcome("a", 10), outcome("b", 20)))
    partial = EvalReport((outcome("a", 10), outcome("b", None)))

    assert known.total_cost_micros == 30
    # Adding up only the priced half would understate the bill, so the total
    # stays unknown for the same reason a single record never rounds to 0.
    assert partial.total_cost_micros is None
    assert json.loads(json.dumps(partial.to_json()))["total_cost_micros"] is None


def test_report_json_is_serialisable_and_complete() -> None:
    report = EvalReport(())
    payload = report.to_json()

    assert json.dumps(payload)
    assert payload["tasks"] == 0
    assert payload["pass_rate"] == 0.0
