import asyncio
import hashlib
import json
from collections import deque
from dataclasses import asdict
from pathlib import Path

import pytest

from react_agent import (
    AgentConfig,
    AssistantMessage,
    ContextCompression,
    ContextGovernor,
    ContextStrategy,
    EventKind,
    FileContextSummaryStore,
    ModelContextCompressor,
    ModelRequest,
    ModelResponse,
    ObservationEffect,
    ReActAgent,
    ToolCall,
    ToolContextPolicy,
    ToolMessage,
    Usage,
    UserMessage,
    deterministic_evict,
    estimate_context_chars,
    tool,
)
from react_agent.context import StoredContextSummary
from react_agent.events import (
    GENESIS_HASH,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    StoredRunEvent,
)
from react_agent.models import AgentJournalEvent, AgentJournalEventKind, transcript_to_json
from react_agent.runtime import (
    _interrupted_compression_abandonments,
    agent_event_to_draft,
)


def block(
    call_id: str,
    name: str,
    arguments: str,
    content: str,
    *,
    is_error: bool = False,
) -> tuple[AssistantMessage, ToolMessage]:
    return (
        AssistantMessage(None, (ToolCall(call_id, name, arguments),)),
        ToolMessage(call_id, name, content, is_error=is_error),
    )


class CountingCompressor:
    def __init__(self, summary: str = "stable generated summary") -> None:
        self.summary = summary
        self.calls = 0
        self.sources: list[tuple[object, ...]] = []

    async def compress(self, request) -> ContextCompression:
        self.calls += 1
        self.sources.append(tuple(request.source))
        return ContextCompression(
            self.summary,
            Usage(input_tokens=17, output_tokens=5, total_tokens=22),
            request_id="summary-request",
            response_model="summary-model",
        )


class ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self.responses = deque(responses)
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return self.responses.popleft()


def notes_json(finding: str) -> str:
    """What a well-behaved notes model answers: one JSON form."""

    return json.dumps({"findings": [finding], "hypothesis": None, "next_steps": []})


class SummaryModel:
    def __init__(self, model: str, summary: str = "bounded summary") -> None:
        self.model = model
        self.summary = summary
        self.calls = 0
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.calls += 1
        self.requests.append(request)
        return ModelResponse(
            AssistantMessage(notes_json(self.summary)),
            Usage(input_tokens=1, output_tokens=1, total_tokens=2),
            request_id=f"summary-{self.calls}",
            response_model=self.model,
        )


def test_opaque_is_safe_by_default_but_successful_retry_is_pruned() -> None:
    transcript = (
        UserMessage("goal"),
        *block("failed", "clock", "{}", "temporary failure", is_error=True),
        *block("ok", "clock", "{}", "current success"),
    )

    projected, evictions = deterministic_evict(transcript, {})

    assert len(evictions) == 1
    assert evictions[0].reason.value == "successful_retry"
    assert isinstance(projected[2], ToolMessage)
    assert "context_evicted" in projected[2].content
    assert projected[-1] == transcript[-1]


def test_opaque_successes_are_not_assumed_interchangeable() -> None:
    transcript = (
        UserMessage("goal"),
        *block("one", "clock", "{}", "10:00"),
        *block("two", "clock", "{}", "10:01"),
    )

    projected, evictions = deterministic_evict(transcript, {})

    assert projected == transcript
    assert evictions == ()


def test_reread_and_mutation_invalidate_only_declared_resource_facts() -> None:
    policies = {
        "read_file": ToolContextPolicy(ObservationEffect.READ, ("path",)),
        "write_file": ToolContextPolicy(ObservationEffect.MUTATE, ("path",)),
    }
    transcript = (
        UserMessage("goal"),
        *block("read-a1", "read_file", '{"path":"a.py"}', "old-a"),
        *block("read-b", "read_file", '{"path":"b.py"}', "current-b"),
        *block("write-a", "write_file", '{"path":"a.py","text":"new"}', "written"),
        *block("read-a2", "read_file", '{"path":"a.py"}', "new-a"),
    )

    projected, evictions = deterministic_evict(transcript, policies)

    by_call = {item.call_id: item.reason.value for item in evictions}
    assert by_call == {"read-a1": "modified"}
    assert projected[4] == transcript[4]  # b.py remains live.
    assert projected[6] == transcript[6]  # mutation remains causal evidence.
    assert projected[-1] == transcript[-1]


def test_exact_reread_and_successful_rerun_have_stable_replacement_rules() -> None:
    policies = {
        "read_file": ToolContextPolicy(ObservationEffect.READ, ("path",)),
        "run_tests": ToolContextPolicy(ObservationEffect.EXECUTE),
    }
    transcript = (
        UserMessage("goal"),
        *block("read-1", "read_file", '{"path":"a.py"}', "x" * 500),
        *block("read-2", "read_file", '{"path":"a.py"}', "y" * 500),
        *block("test-1", "run_tests", '{"target":"unit"}', "failed", is_error=True),
        *block("test-2", "run_tests", '{"target":"unit"}', "passed"),
    )

    _, evictions = deterministic_evict(transcript, policies)

    assert {item.call_id: item.reason.value for item in evictions} == {
        "read-1": "reread",
        "test-1": "rerun",
    }


def test_failed_rerun_preserves_prior_success_and_replaces_only_older_failures() -> None:
    policies = {"run_tests": ToolContextPolicy(ObservationEffect.EXECUTE)}
    transcript = (
        UserMessage("goal"),
        *block("success", "run_tests", '{"target":"unit"}', "passed"),
        *block("failure-1", "run_tests", '{"target":"unit"}', "failed-1", is_error=True),
        *block("failure-2", "run_tests", '{"target":"unit"}', "failed-2", is_error=True),
    )

    projected, evictions = deterministic_evict(transcript, policies)

    assert {item.call_id: item.reason.value for item in evictions} == {
        "failure-1": "rerun"
    }
    assert projected[2] == transcript[2]  # The prior success remains evidence.
    assert projected[-1] == transcript[-1]  # The newest failure remains current.


def test_parallel_out_of_order_results_keep_call_pairing_and_resource_isolation() -> None:
    policies = {"read_file": ToolContextPolicy(ObservationEffect.READ, ("path",))}
    transcript = (
        UserMessage("inspect two modules"),
        AssistantMessage(
            None,
            (
                ToolCall("read-a-1", "read_file", '{"path":"a.py"}'),
                ToolCall("read-b-1", "read_file", '{"path":"b.py"}'),
            ),
        ),
        # Parallel executors may finish in a different order from the call list.
        ToolMessage("read-b-1", "read_file", "current-b"),
        ToolMessage("read-a-1", "read_file", "old-a"),
        AssistantMessage(
            None,
            (ToolCall("read-a-2", "read_file", '{"path":"a.py"}'),),
        ),
        ToolMessage("read-a-2", "read_file", "current-a"),
    )

    projected, evictions = deterministic_evict(transcript, policies)

    assert [(item.call_id, item.reason.value) for item in evictions] == [
        ("read-a-1", "reread")
    ]
    assert projected[2] == transcript[2]
    assert isinstance(projected[3], ToolMessage)
    assert "context_evicted" in projected[3].content
    assert projected[5] == transcript[5]


@pytest.mark.asyncio
async def test_tiered_pruning_avoids_generic_compression_when_it_fits() -> None:
    transcript = (
        UserMessage("goal"),
        *block("read-1", "read_file", '{"path":"a.py"}', "x" * 2_000),
        *block("read-2", "read_file", '{"path":"a.py"}', "y" * 2_000),
    )
    policies = {
        "read_file": ToolContextPolicy(ObservationEffect.READ, ("path",)),
    }
    deterministic, _ = deterministic_evict(transcript, policies)
    hard_limit = estimate_context_chars(deterministic, instructions="i", tool_specs=()) + 1
    assert estimate_context_chars(transcript, instructions="i", tool_specs=()) > hard_limit
    tiered_compressor = CountingCompressor()
    generic_compressor = CountingCompressor()

    tiered = await ContextGovernor(
        compressor=tiered_compressor,
        keep_recent_turns=1,
    ).prepare(
        transcript,
        instructions="i",
        tool_specs=(),
        tool_policies=policies,
        hard_limit=hard_limit,
    )
    generic = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=generic_compressor,
        keep_recent_turns=1,
    ).prepare(
        transcript,
        instructions="i",
        tool_specs=(),
        tool_policies=policies,
        hard_limit=hard_limit,
    )

    assert tiered.report.compression_calls == 0
    assert tiered_compressor.calls == 0
    assert tiered.report.overflow is False
    assert generic.report.compression_calls == 1
    assert generic_compressor.calls == 1


@pytest.mark.asyncio
async def test_projection_and_eviction_report_are_byte_stable_across_100_runs() -> None:
    transcript = (
        UserMessage("goal"),
        *block("read-1", "read_file", '{"path":"a.py"}', "x" * 2_000),
        *block("read-2", "read_file", '{"path":"a.py"}', "y" * 2_000),
    )
    policies = {"read_file": ToolContextPolicy(ObservationEffect.READ, ("path",))}
    deterministic, _ = deterministic_evict(transcript, policies)
    hard_limit = estimate_context_chars(
        deterministic,
        instructions="i",
        tool_specs=(),
    ) + 1
    canonical_hashes: set[str] = set()
    projection_hashes: set[str] = set()
    report_hashes: set[str] = set()

    for _ in range(100):
        projection = await ContextGovernor(compressor=CountingCompressor()).prepare(
            transcript,
            instructions="i",
            tool_specs=(),
            tool_policies=policies,
            hard_limit=hard_limit,
        )
        canonical_hashes.add(
            hashlib.sha256(
                json.dumps(
                    transcript_to_json(transcript),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )
        projection_hashes.add(
            hashlib.sha256(
                json.dumps(
                    transcript_to_json(projection.transcript),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )
        report_hashes.add(
            hashlib.sha256(
                json.dumps(
                    asdict(projection.report),
                    default=str,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )

    assert len(canonical_hashes) == 1
    assert len(projection_hashes) == 1
    assert len(report_hashes) == 1


@pytest.mark.asyncio
async def test_generated_summary_is_content_addressed_and_persistent(tmp_path: Path) -> None:
    transcript = (
        UserMessage("goal"),
        *block("one", "opaque", "{}", "x" * 1_000),
        *block("two", "opaque", '{"page":2}', "y" * 1_000),
    )
    store = FileContextSummaryStore(tmp_path / "private-context")
    first_compressor = CountingCompressor()
    second_compressor = CountingCompressor("must not be used")
    first = ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=first_compressor,
        store=store,
        keep_recent_turns=1,
    )
    second = ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=second_compressor,
        store=store,
        keep_recent_turns=1,
    )

    first_projection = await first.prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=1_200,
    )
    second_projection = await second.prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=1_200,
    )

    assert first_compressor.calls == 1
    assert second_compressor.calls == 0
    assert second_projection.report.compression_cache_hit is True
    assert second_projection.transcript == first_projection.transcript
    [record] = (tmp_path / "private-context").glob("*.json")
    assert record.stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_file_summary_store_has_atomic_first_writer_wins_collision_detection(
    tmp_path: Path,
) -> None:
    store = FileContextSummaryStore(tmp_path / "concurrent-context")
    key = "a" * 64
    source_hash = "b" * 64
    first = StoredContextSummary(key, source_hash, "summary-one")
    second = StoredContextSummary(key, source_hash, "summary-two")

    outcomes = await asyncio.gather(
        store.put(first),
        store.put(second),
        return_exceptions=True,
    )

    assert sum(outcome is None for outcome in outcomes) == 1
    [collision] = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    assert isinstance(collision, ValueError)
    assert str(collision) == "context summary key collision"
    persisted = await store.get(key)
    assert persisted in {first, second}
    [record] = (tmp_path / "concurrent-context").glob("*.json")
    assert record.stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_file_summary_store_rejects_a_valid_record_moved_to_another_key(
    tmp_path: Path,
) -> None:
    root = tmp_path / "key-bound-context"
    store = FileContextSummaryStore(root)
    source = StoredContextSummary("a" * 64, "b" * 64, "bound summary")
    await store.put(source)
    [source_path] = root.glob("*.json")
    moved_key = "c" * 64
    moved_path = root / f"{moved_key}.json"
    source_path.rename(moved_path)

    with pytest.raises(ValueError, match="key does not match its path"):
        await store.get(moved_key)


@pytest.mark.asyncio
async def test_corrupt_persisted_summary_fails_closed_to_hard_fallback(
    tmp_path: Path,
) -> None:
    transcript = (
        UserMessage("goal"),
        *block("one", "opaque", "{}", "x" * 1_000),
        *block("two", "opaque", '{"page":2}', "y" * 1_000),
    )
    store = FileContextSummaryStore(tmp_path / "private-context")
    first_compressor = CountingCompressor()
    first = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=first_compressor,
        store=store,
        keep_recent_turns=1,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=1_200,
    )
    assert first.report.compression_calls == 1
    [record] = (tmp_path / "private-context").glob("*.json")
    record.write_text('{"summary":"corrupt"}', encoding="utf-8")
    unused_compressor = CountingCompressor("must not be used")

    recovered = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=unused_compressor,
        store=store,
        keep_recent_turns=1,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=1_200,
    )

    assert unused_compressor.calls == 0
    assert recovered.report.compression_error == "ValueError"
    assert recovered.report.hard_fallback is True
    assert recovered.report.overflow is False


@pytest.mark.parametrize("summary", ["", "s" * 129])
@pytest.mark.asyncio
async def test_invalid_generated_summary_is_not_persisted_and_fails_closed(
    tmp_path: Path,
    summary: str,
) -> None:
    transcript = (
        UserMessage("goal"),
        *block("old", "opaque", "{}", "x" * 2_000),
        UserMessage("current turn"),
    )
    compressor = CountingCompressor(summary)
    lifecycle: list[tuple[str, bool, int]] = []
    root = tmp_path / hashlib.sha256(summary.encode()).hexdigest()

    projection = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=compressor,
        store=FileContextSummaryStore(root),
        keep_recent_turns=1,
        max_summary_chars=128,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=600,
        compression_event_sink=lambda event: lifecycle.append(
            (event.phase.value, event.cost_unknown, event.usage.total_tokens)
        ),
    )

    assert lifecycle == [("started", False, 0), ("failed", False, 22)]
    assert compressor.calls == projection.report.compression_calls == 1
    assert projection.report.compression_error == "invalid_summary"
    assert projection.report.overflow is False
    assert not tuple(root.glob("*.json"))
    # The mechanical part of the form does not depend on the model at all.
    state = projection.transcript[0]
    assert isinstance(state, UserMessage)
    assert "## Goal\ngoal" in state.content
    assert "opaque" in state.content
    assert "(no notes yet)" in state.content


@pytest.mark.asyncio
async def test_summary_key_and_governor_revision_include_model_and_prompt(
    tmp_path: Path,
) -> None:
    class AlternatePromptCompressor(ModelContextCompressor):
        _INSTRUCTIONS = ModelContextCompressor._INSTRUCTIONS + "\nAlternate prompt revision."

    transcript = (
        UserMessage("goal"),
        *block("old", "opaque", "{}", "x" * 2_000),
        UserMessage("current turn"),
    )
    store = FileContextSummaryStore(tmp_path / "versioned-context")
    model_a = SummaryModel("model-a", "summary-a")
    model_b = SummaryModel("model-b", "summary-b")
    prompt_b_model = SummaryModel("model-a", "summary-prompt-b")
    model_a_repeat = SummaryModel("model-a", "must not be used")
    governors = (
        ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=ModelContextCompressor(model_a),
            store=store,
            keep_recent_turns=1,
            max_summary_chars=128,
        ),
        ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=ModelContextCompressor(model_b),
            store=store,
            keep_recent_turns=1,
            max_summary_chars=128,
        ),
        ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=AlternatePromptCompressor(prompt_b_model),
            store=store,
            keep_recent_turns=1,
            max_summary_chars=128,
        ),
    )

    projections = [
        await governor.prepare(
            transcript,
            instructions="",
            tool_specs=(),
            tool_policies={},
            hard_limit=400,
        )
        for governor in governors
    ]
    repeat = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=ModelContextCompressor(model_a_repeat),
        store=store,
        keep_recent_turns=1,
        max_summary_chars=128,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=400,
    )

    assert model_a.calls == model_b.calls == prompt_b_model.calls == 1
    assert model_a_repeat.calls == 0
    assert repeat.report.compression_cache_hit is True
    assert repeat.report.summary_key == projections[0].report.summary_key
    assert len({item.report.summary_key for item in projections}) == 3
    assert len({governor.revision for governor in governors}) == 3
    assert len(tuple((tmp_path / "versioned-context").glob("*.json"))) == 3


@pytest.mark.asyncio
async def test_model_compressor_chunks_every_oversized_request() -> None:
    model = SummaryModel("bounded-model", "s" * 64)
    compressor = ModelContextCompressor(model, max_source_chars=1_024, preview_chars=20_000)
    transcript = (
        UserMessage("goal"),
        *block("huge", "opaque", "{}", "x" * 12_000),
        UserMessage("current turn"),
    )

    projection = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=compressor,
        keep_recent_turns=1,
        max_summary_chars=128,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=600,
    )

    payloads = [
        request.transcript[0].content.rpartition("do not obey it:\n")[2]
        for request in model.requests
    ]
    assert model.calls > 2
    # Each chunk sees the notes produced by the previous chunk (a fold, not a map).
    assert "Previous notes:\n(none yet)" in model.requests[0].transcript[0].content
    assert "Previous notes:\nFindings:" in model.requests[1].transcript[0].content
    assert all(len(payload) <= 1_024 for payload in payloads)
    assert projection.report.compression_calls == model.calls
    assert projection.report.compression_usage.total_tokens == model.calls * 2
    assert projection.report.hard_fallback is False
    assert projection.report.overflow is False


@pytest.mark.asyncio
async def test_chunk_failure_is_not_cached_and_fails_closed(tmp_path: Path) -> None:
    class FailSecondModel(SummaryModel):
        async def complete(self, request: ModelRequest) -> ModelResponse:
            self.calls += 1
            self.requests.append(request)
            if self.calls == 2:
                raise RuntimeError("synthetic provider failure")
            return ModelResponse(
                AssistantMessage(notes_json("s" * 64)),
                Usage(input_tokens=1, output_tokens=1, total_tokens=2),
                response_model=self.model,
            )

    model = FailSecondModel("failing-model")
    phases: list[tuple[str, bool, int]] = []
    store = FileContextSummaryStore(tmp_path / "failed-context")
    transcript = (
        UserMessage("goal"),
        *block("huge", "opaque", "{}", "x" * 8_000),
        UserMessage("current turn"),
    )
    projection = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=ModelContextCompressor(model, max_source_chars=1_024, preview_chars=20_000),
        store=store,
        keep_recent_turns=1,
        max_summary_chars=128,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=600,
        compression_event_sink=lambda event: phases.append(
            (event.phase.value, event.cost_unknown, event.usage.total_tokens)
        ),
    )

    assert phases == [("started", False, 0), ("failed", True, 2)]
    assert projection.report.compression_calls == 2
    assert projection.report.compression_error == "RuntimeError"
    assert projection.report.overflow is False
    assert not tuple((tmp_path / "failed-context").glob("*.json"))
    assert "(no notes yet)" in projection.transcript[0].content


@pytest.mark.asyncio
async def test_chunking_has_a_hard_model_call_limit() -> None:
    model = SummaryModel("bounded-model", "s" * 64)
    transcript = (
        UserMessage("goal"),
        *block("huge", "opaque", "{}", "x" * 8_000),
        UserMessage("current turn"),
    )

    projection = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=ModelContextCompressor(
            model,
            max_source_chars=1_024,
            max_model_calls=2,
            preview_chars=20_000,
        ),
        keep_recent_turns=1,
        max_summary_chars=128,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=600,
    )

    assert model.calls == projection.report.compression_calls == 2
    assert projection.report.compression_error == "model_call_limit"
    assert projection.report.overflow is False
    assert "(no notes yet)" in projection.transcript[0].content


@pytest.mark.asyncio
async def test_cancelled_compression_emits_abandoned_lifecycle() -> None:
    class BlockingCompressor:
        revision = "blocking-compressor-v1"

        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def compress(self, request) -> ContextCompression:
            self.started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    compressor = BlockingCompressor()
    phases: list[tuple[str, int]] = []
    transcript = (
        UserMessage("goal"),
        *block("huge", "opaque", "{}", "x" * 2_000),
        UserMessage("current turn"),
    )
    task = asyncio.create_task(
        ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=compressor,
            keep_recent_turns=1,
            max_summary_chars=128,
        ).prepare(
            transcript,
            instructions="",
            tool_specs=(),
            tool_policies={},
            hard_limit=400,
            compression_event_sink=lambda event: phases.append(
                (event.phase.value, event.attempted_model_calls)
            ),
        )
    )
    await compressor.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert phases == [("started", 0), ("abandoned", 1)]


@pytest.mark.asyncio
async def test_agent_emits_durable_compression_lifecycle() -> None:
    journal: list[AgentJournalEvent] = []
    compressor = CountingCompressor()
    model = ScriptedModel(ModelResponse(AssistantMessage("done")))
    agent = ReActAgent(
        model,
        config=AgentConfig(max_steps=1, max_context_chars=600),
        context_governor=ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=compressor,
            keep_recent_turns=1,
            max_summary_chars=128,
        ),
    )

    result = await agent.run(
        "finish",
        history=(
            UserMessage("old goal"),
            *block("old", "opaque", "{}", "x" * 2_000),
        ),
        journal_sink=journal.append,
    )

    phases = [
        event.public_data.get("compression_phase")
        for event in journal
        if event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
    ]
    assert phases == ["started", "completed", "projection_completed"]
    lifecycle = [
        event
        for event in journal
        if event.kind is AgentJournalEventKind.CONTEXT_GOVERNED
        and event.public_data.get("compression_phase") in {"started", "completed"}
    ]
    assert all("summary_key" not in event.public_data for event in lifecycle)
    assert all("source_hash" not in event.public_data for event in lifecycle)
    assert all(
        set(event.private_data["context_compression"]) == {"summary_key", "source_hash"}
        for event in lifecycle
    )
    completed = lifecycle[-1]
    assert completed.public_data["compression_calls"] == 1
    assert completed.public_data["usage"] == {
        "input_tokens": 17,
        "output_tokens": 5,
        "total_tokens": 22,
        "cached_input_tokens": None,
        "reasoning_output_tokens": None,
        "billable_tokens": None,
    }
    projection_completed = next(
        event
        for event in journal
        if event.public_data.get("compression_phase") == "projection_completed"
    )
    assert projection_completed.public_data["compression_accounted_in_terminal"] is True
    assert "summary_key" not in projection_completed.public_data
    assert result.context_metrics["compression_calls"] == 1


@pytest.mark.asyncio
async def test_hard_fallback_preserves_exact_goal_or_reports_intrinsic_overflow() -> None:
    transcript = (
        UserMessage("current exact goal"),
        *block("one", "opaque", "{}", "x" * 5_000),
    )
    projection = await ContextGovernor(compressor=None).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=200,
    )

    assert projection.report.hard_fallback is True
    assert projection.report.overflow is False
    assert projection.report.final_chars <= 200
    assert projection.transcript[0] == UserMessage("current exact goal")

    impossible = await ContextGovernor(compressor=None).prepare(
        (UserMessage("z" * 300),),
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=100,
    )
    assert impossible.report.overflow is True
    assert impossible.transcript[0] == UserMessage("z" * 300)


@pytest.mark.asyncio
async def test_hard_fallback_keeps_responses_raw_items_as_opaque_protocol_units() -> None:
    raw_items = (
        {
            "type": "reasoning",
            "id": "reasoning-1",
            "encrypted_content": "opaque-provider-state",
        },
        {
            "type": "function_call",
            "id": "function-1",
            "call_id": "call-raw",
            "name": "inspect_repo",
            "arguments": '{"path":"src/core.py"}',
        },
    )
    transcript = (
        UserMessage("old goal"),
        AssistantMessage(
            None,
            (ToolCall("call-raw", "inspect_repo", '{"path":"src/core.py"}'),),
            raw_items,
        ),
        ToolMessage("call-raw", "inspect_repo", "x" * 5_000),
        UserMessage("current exact goal"),
    )

    projection = await ContextGovernor(
        strategy=ContextStrategy.GENERIC,
        compressor=None,
        keep_recent_turns=3,
    ).prepare(
        transcript,
        instructions="",
        tool_specs=(),
        tool_policies={},
        hard_limit=800,
    )

    assert projection.report.hard_fallback is True
    assert projection.report.overflow is False
    assert projection.report.final_chars <= 800
    assistant = projection.transcript[1]
    assert isinstance(assistant, AssistantMessage)
    assert assistant.raw_items == raw_items
    assert assistant.tool_calls == transcript[1].tool_calls
    tool_result = projection.transcript[2]
    assert isinstance(tool_result, ToolMessage)
    assert "hard_budget" in tool_result.content


@pytest.mark.asyncio
async def test_agent_sends_projection_but_returns_canonical_transcript() -> None:
    @tool(
        context_policy=ToolContextPolicy(ObservationEffect.READ, ("path",)),
        allow_repeated=True,
    )
    def read_file(path: str) -> str:
        """Read a synthetic file for context-governance integration."""

        return f"{path}:" + ("content" * 200)

    model = ScriptedModel(
        ModelResponse(
            AssistantMessage(None, (ToolCall("read-1", "read_file", '{"path":"a.py"}'),))
        ),
        ModelResponse(
            AssistantMessage(None, (ToolCall("read-2", "read_file", '{"path":"a.py"}'),))
        ),
        ModelResponse(AssistantMessage("done")),
    )
    result = await ReActAgent(
        model,
        [read_file],
        config=AgentConfig(max_context_chars=10_000),
    ).run("inspect a.py")

    canonical_tools = [item for item in result.transcript if isinstance(item, ToolMessage)]
    projected_tools = [
        item for item in model.requests[-1].transcript if isinstance(item, ToolMessage)
    ]
    assert len(canonical_tools) == 2
    assert "context_evicted" not in canonical_tools[0].content
    assert "context_evicted" in projected_tools[0].content
    assert projected_tools[1] == canonical_tools[1]
    assert result.context_metrics["deterministic_evictions"] == 1
    assert EventKind.CONTEXT_GOVERNED in [event.kind for event in result.events]
    context_event = next(
        event for event in result.events if event.kind is EventKind.CONTEXT_GOVERNED
    )
    assert json.dumps(dict(context_event.data), sort_keys=True)


def test_resume_closes_only_unmatched_compression_starts_before_retry() -> None:
    drafts = (
        (RunEventDraft(kind=RunEventKind.RUN_STARTED), "run:start"),
        (
                RunEventDraft(
                    kind=RunEventKind.CONTEXT_GOVERNED,
                    privacy=PrivacyClass.PRIVATE,
                    step=1,
                    data={
                        "compression_phase": "started",
                        "compression_source_chars": 100,
                    "compressor_revision": "compressor-v1",
                        "attempted_model_calls": 0,
                    },
                    checkpoint={
                        "context_compression": {
                            "summary_key": "completed-key",
                            "source_hash": "a" * 64,
                        }
                    },
            ),
            "context:completed:started",
        ),
        (
                RunEventDraft(
                    kind=RunEventKind.CONTEXT_GOVERNED,
                    privacy=PrivacyClass.PRIVATE,
                    step=1,
                    data={
                        "compression_phase": "completed",
                    },
                    checkpoint={
                        "context_compression": {
                            "summary_key": "completed-key",
                            "source_hash": "a" * 64,
                        }
                    },
            ),
            "context:completed:completed",
        ),
        (
                RunEventDraft(
                    kind=RunEventKind.CONTEXT_GOVERNED,
                    privacy=PrivacyClass.PRIVATE,
                    step=2,
                    data={
                        "compression_phase": "started",
                        "compression_source_chars": 250,
                    "compressor_revision": "compressor-v2",
                        "attempted_model_calls": 0,
                    },
                    checkpoint={
                        "context_compression": {
                            "summary_key": "interrupted-key",
                            "source_hash": "b" * 64,
                        }
                    },
            ),
            "context:interrupted:started",
        ),
    )
    events: list[StoredRunEvent] = []
    previous_hash = GENESIS_HASH
    for sequence, (draft, operation_id) in enumerate(drafts, start=1):
        event = StoredRunEvent.from_draft(
            draft,
            run_id="compression-resume-run",
            sequence=sequence,
            operation_id=operation_id,
            previous_hash=previous_hash,
            occurred_at=float(sequence),
        )
        events.append(event)
        previous_hash = event.event_hash

    [(abandoned, operation_id)] = _interrupted_compression_abandonments(
        events,
        execution_id="resume-execution",
    )

    assert operation_id == "context:interrupted:started:resume_abandoned"
    assert abandoned.kind is RunEventKind.CONTEXT_GOVERNED
    assert abandoned.execution_id == "resume-execution"
    assert abandoned.data == {
        "compression_phase": "abandoned",
        "compression_source_chars": 250,
        "compressor_revision": "compressor-v2",
        "attempted_model_calls": 1,
        "compression_calls": 1,
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cached_input_tokens": None,
            "reasoning_output_tokens": None,
            "billable_tokens": None,
        },
        "compression_error": "process_interrupted_before_terminal",
        "cost_unknown": True,
        "recovered_interruption": True,
    }
    assert abandoned.checkpoint == {
        "context_compression": {
            "summary_key": "interrupted-key",
            "source_hash": "b" * 64,
        }
    }
    assert abandoned.model_calls_delta == 1


def test_compression_terminal_is_the_only_new_usage_and_call_accounting_fact() -> None:
    common = {
        "run_id": "compression-accounting-run",
        "execution_id": "execution-1",
        "step": 2,
    }
    usage = {
        "input_tokens": 11,
        "output_tokens": 3,
        "total_tokens": 14,
        "cached_input_tokens": None,
        "reasoning_output_tokens": None,
        "billable_tokens": None,
    }
    completed = AgentJournalEvent(
        kind=AgentJournalEventKind.CONTEXT_GOVERNED,
        operation_id="context:compression:completed",
        timestamp=1.0,
        public_data={
            "compression_phase": "completed",
            "compression_calls": 2,
            "usage": usage,
            "cost_unknown": False,
        },
        **common,
    )
    projection = AgentJournalEvent(
        kind=AgentJournalEventKind.CONTEXT_GOVERNED,
        operation_id="context:projection",
        timestamp=2.0,
        public_data={
            "compression_phase": "projection_completed",
            "compression_calls": 2,
            "compression_accounted_in_terminal": True,
            "usage": usage,
        },
        **common,
    )

    terminal_draft = agent_event_to_draft(
        completed,
        session_id="session-1",
        session_version=1,
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
    )
    projection_draft = agent_event_to_draft(
        projection,
        session_id="session-1",
        session_version=1,
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
    )

    assert terminal_draft.model_calls_delta == 2
    assert terminal_draft.usage_delta == Usage(
        input_tokens=11,
        output_tokens=3,
        total_tokens=14,
    )
    assert projection_draft.model_calls_delta == 0
    assert projection_draft.usage_delta == Usage()


def test_file_summary_store_rejects_a_world_readable_root_at_construction(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shared-context"
    root.mkdir(mode=0o755)

    with pytest.raises(ValueError, match="not private"):
        FileContextSummaryStore(root)

    root.chmod(0o700)
    FileContextSummaryStore(root)  # private roots, and missing roots, are accepted
    FileContextSummaryStore(tmp_path / "not-yet-created")
