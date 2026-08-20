import asyncio
from dataclasses import replace

import pytest

from react_agent.events import (
    EVENT_SCHEMA_VERSION,
    GENESIS_HASH,
    SUPPORTED_EVENT_SCHEMA_VERSIONS,
    EventHashError,
    EventSequenceError,
    EventTerminalError,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    RunState,
    StoredRunEvent,
    UnsupportedEventVersionError,
    canonical_json,
    compute_event_hash,
    fold_events,
    upcast_events,
    verify_event_chain,
)
from react_agent.journal import (
    InMemoryRunJournal,
    LeaseConflictError,
    LeaseLostError,
    OperationConflictError,
    SequenceConflictError,
    TerminalRunError,
)
from react_agent.models import Usage


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def run_started(*, occurred_at: float | None = None) -> RunEventDraft:
    return RunEventDraft(
        kind=RunEventKind.RUN_STARTED,
        privacy=PrivacyClass.PRIVATE,
        occurred_at=occurred_at,
        session_id="session-1",
        execution_id="execution-1",
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
        data={"status": "running"},
        checkpoint={
            "transcript": [{"role": "user", "content": "private prompt"}],
        },
        safe_checkpoint=True,
    )


def fixed_v2_event() -> tuple[RunEventDraft, StoredRunEvent]:
    draft = run_started(occurred_at=1_234.5)
    return draft, StoredRunEvent.from_draft(
        draft,
        run_id="run-1",
        sequence=1,
        operation_id="start",
        previous_hash=GENESIS_HASH,
        occurred_at=9_999.0,
    )


def fixed_v1_event() -> StoredRunEvent:
    event = StoredRunEvent(
        run_id="run-1",
        sequence=1,
        operation_id="start",
        event_id="f18e1128-8e86-5cfa-9885-930bfdf6cd48",
        kind=RunEventKind.RUN_STARTED,
        privacy=PrivacyClass.PRIVATE,
        schema_version=1,
        occurred_at=1_234.5,
        previous_hash=GENESIS_HASH,
        event_hash="",
        session_id="session-1",
        execution_id="execution-1",
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
        data={"status": "running"},
        checkpoint={
            "transcript": [{"role": "user", "content": "private prompt"}],
        },
        safe_checkpoint=True,
    )
    return replace(event, event_hash=compute_event_hash(event))


def test_v1_upcaster_preserves_golden_hash_and_adds_v2_semantics() -> None:
    event = fixed_v1_event()

    assert EVENT_SCHEMA_VERSION == 2
    assert SUPPORTED_EVENT_SCHEMA_VERSIONS == (1, 2)
    # The original v1 hash codec remains frozen even though reducer semantics
    # now include v2 event and causation identities.
    assert event.event_hash == ("1816fb2223a60d05b211f1bb6a2b72f7ca87f2293441c3039379abb380a9f288")
    assert compute_event_hash(event) == event.event_hash

    semantic = upcast_events((event,))
    assert semantic[0].schema_version == 2
    assert semantic[0].event_id == event.event_id
    assert semantic[0].event_hash == event.event_hash
    snapshot = fold_events((event,))
    assert snapshot.last_hash == event.event_hash
    assert snapshot.status == "running"
    assert snapshot.transcript[0]["content"] == "private prompt"


def test_v2_event_and_draft_golden_hashes_include_stable_event_identity() -> None:
    draft, event = fixed_v2_event()

    assert draft.payload_hash() == (
        "f59b52e1ecc9c4eef8e1fe71b423c821ae6095b178d67501e3ba64b7a11abed1"
    )
    assert event.event_id == "48ece370-b407-57bb-94da-63177b534335"
    assert event.event_hash == ("096e68830a0dca6d784b561430a1168329682d8475d7b1483b3ecaa1ddb89c36")
    assert event.causation_id is None


def test_upcaster_validates_original_hash_before_semantic_conversion() -> None:
    event = fixed_v1_event()
    corrupted = replace(event, event_hash="f" * 64)

    with pytest.raises(EventHashError, match="event hash mismatch"):
        upcast_events((corrupted,))


def test_upcaster_rejects_unknown_future_schema_without_guessing() -> None:
    _, event = fixed_v2_event()
    future = replace(event, schema_version=EVENT_SCHEMA_VERSION + 1)

    with pytest.raises(UnsupportedEventVersionError, match="schema version: 3"):
        compute_event_hash(future)
    with pytest.raises(UnsupportedEventVersionError, match="schema version: 3"):
        upcast_events((future,))
    with pytest.raises(UnsupportedEventVersionError, match="schema version: 3"):
        fold_events((future,))


@pytest.mark.asyncio
async def test_canonical_json_and_hash_chain_are_deterministic_and_detect_corruption() -> None:
    wall_clock = FakeClock(1_000.0)
    left = InMemoryRunJournal(wall_clock=wall_clock)
    right = InMemoryRunJournal(wall_clock=wall_clock)

    first_left = await left.create("run-1", run_started(), operation_id="start")
    first_right = await right.create("run-1", run_started(), operation_id="start")
    draft = RunEventDraft(
        kind=RunEventKind.MODEL_STARTED,
        step=1,
        data={"z": [3, 2, 1], "a": "中文"},
        model_calls_delta=1,
    )
    second_left = await left.append("run-1", draft, expected_sequence=1, operation_id="model-1")
    second_right = await right.append("run-1", draft, expected_sequence=1, operation_id="model-1")

    assert canonical_json({"z": 1, "a": "中文"}) == '{"a":"中文","z":1}'
    assert first_left.event_hash == first_right.event_hash
    assert second_left.event_hash == second_right.event_hash
    verify_event_chain((first_left, second_left))

    with pytest.raises(EventSequenceError):
        verify_event_chain((first_left, replace(second_left, sequence=3)))
    with pytest.raises(EventHashError):
        verify_event_chain((first_left, replace(second_left, event_hash="0" * 64)))


@pytest.mark.asyncio
async def test_append_enforces_sequence_and_operation_idempotency() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    draft = RunEventDraft(
        kind=RunEventKind.MODEL_STARTED,
        step=1,
        model_calls_delta=1,
    )

    committed = await journal.append("run-1", draft, expected_sequence=1, operation_id="model-1")
    retried = await journal.append("run-1", draft, expected_sequence=1, operation_id="model-1")

    assert retried is committed
    with pytest.raises(OperationConflictError):
        await journal.append(
            "run-1",
            replace(draft, step=2),
            expected_sequence=2,
            operation_id="model-1",
        )
    with pytest.raises(SequenceConflictError):
        await journal.append(
            "run-1",
            RunEventDraft(kind=RunEventKind.MODEL_FAILED, step=1),
            expected_sequence=1,
            operation_id="model-failed",
        )


@pytest.mark.asyncio
async def test_append_many_is_atomic_retry_stable_and_causally_ordered() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    entries = (
        (
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            "model-started",
        ),
        (
            RunEventDraft(
                kind=RunEventKind.MODEL_FAILED,
                step=1,
                data={"attempt": 1, "error_type": "test"},
            ),
            "model-failed",
        ),
    )

    committed = await journal.append_many(
        "run-1",
        entries,
        expected_sequence=1,
    )
    retried = await journal.append_many(
        "run-1",
        entries,
        expected_sequence=1,
    )

    assert retried == committed
    assert committed[0].sequence == 2
    assert committed[1].sequence == 3
    assert committed[0].causation_id == (await journal.read("run-1"))[0].event_id
    assert committed[1].causation_id == committed[0].event_id

    before = await journal.read("run-1")
    with pytest.raises(EventTerminalError, match="terminal"):
        await journal.append_many(
            "run-1",
            (
                (
                    RunEventDraft(
                        kind=RunEventKind.RUN_COMPLETED,
                        data={"status": "completed"},
                    ),
                    "terminal",
                ),
                (RunEventDraft(kind=RunEventKind.CHECKPOINT), "after-terminal"),
            ),
            expected_sequence=3,
        )
    assert await journal.read("run-1") == before


@pytest.mark.asyncio
async def test_terminal_event_is_unique_but_its_idempotent_retry_is_allowed() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    terminal = RunEventDraft(
        kind=RunEventKind.RUN_COMPLETED,
        data={"status": "completed", "stop_reason": "completed"},
        safe_checkpoint=True,
    )
    committed = await journal.append(
        "run-1", terminal, expected_sequence=1, operation_id="terminal"
    )

    assert (
        await journal.append("run-1", terminal, expected_sequence=1, operation_id="terminal")
        is committed
    )
    with pytest.raises(TerminalRunError):
        await journal.append(
            "run-1",
            RunEventDraft(kind=RunEventKind.MODEL_STARTED, step=2),
            expected_sequence=2,
            operation_id="too-late",
        )


@pytest.mark.asyncio
async def test_tool_lifecycle_status_never_overwrites_the_run_outcome() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.TOOL_PLANNED,
            step=1,
            call_key="s1:t0",
            data={"status": "ready"},
        ),
        expected_sequence=1,
        operation_id="tool-planned",
    )
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.TOOL_STARTED,
            step=1,
            call_key="s1:t0",
            data={"status": "running"},
        ),
        expected_sequence=2,
        operation_id="tool-started",
    )
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.TOOL_COMPLETED,
            step=1,
            call_key="s1:t0",
            data={"status": "completed", "outcome": "completed", "executed": True},
        ),
        expected_sequence=3,
        operation_id="tool-completed",
    )

    snapshot = await journal.load("run-1")
    assert snapshot.state is RunState.RUNNING
    assert snapshot.status == "running"
    assert snapshot.stop_reason is None


@pytest.mark.asyncio
async def test_fold_rebuilds_full_snapshot_after_cached_snapshot_is_deleted() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=1,
            model_calls_delta=1,
        ),
        expected_sequence=1,
        operation_id="model-started",
    )
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.MODEL_COMPLETED,
            privacy=PrivacyClass.PRIVATE,
            step=1,
            usage_delta=Usage(input_tokens=10, output_tokens=2, total_tokens=12),
            checkpoint={
                "transcript": [
                    {"role": "user", "content": "private prompt"},
                    {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "name": "calculator",
                                "arguments": '{"value":21}',
                            }
                        ],
                    },
                ]
            },
            safe_checkpoint=True,
        ),
        expected_sequence=2,
        operation_id="model-completed",
    )
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.TOOL_CLAIMED,
            step=1,
            call_key="s1:t0",
            tool_calls_delta=1,
        ),
        expected_sequence=3,
        operation_id="tool-claimed",
    )
    pending = await journal.load("run-1")
    assert tuple(pending.pending) == ("s1:t0",)
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.TOOL_COMPLETED,
            privacy=PrivacyClass.PRIVATE,
            step=1,
            call_key="s1:t0",
            tool_executions_delta=1,
            checkpoint={
                "transcript": [
                    {"role": "user", "content": "private prompt"},
                    {"role": "assistant", "content": None},
                    {"role": "tool", "content": '{"ok":true,"data":42}'},
                ]
            },
            safe_checkpoint=True,
        ),
        expected_sequence=4,
        operation_id="tool-completed",
    )
    await journal.append(
        "run-1",
        RunEventDraft(
            kind=RunEventKind.RUN_COMPLETED,
            data={"status": "completed", "stop_reason": "completed"},
            safe_checkpoint=True,
        ),
        expected_sequence=5,
        operation_id="terminal",
    )

    before = await journal.load("run-1")
    assert before.state is RunState.TERMINAL
    assert before.session_id == "session-1"
    assert before.execution_id == "execution-1"
    assert before.agent_revision == "agent-v1"
    assert before.tool_manifest_hash == "tools-v1"
    assert before.status == "completed"
    assert before.stop_reason == "completed"
    assert before.usage == Usage(input_tokens=10, output_tokens=2, total_tokens=12)
    assert before.counts.model_calls == 1
    assert before.counts.tool_calls == 1
    assert before.counts.tool_executions == 1
    assert before.pending == {}
    assert before.safe_checkpoint_sequences == (1, 3, 5, 6)
    assert before.terminal is not None
    assert before.transcript[-1]["role"] == "tool"

    assert await journal.evict_snapshot("run-1") is True
    rebuilt = await journal.load("run-1")
    assert rebuilt == before


@pytest.mark.asyncio
async def test_lease_fencing_rejects_stale_and_unleased_writers() -> None:
    clock = FakeClock()
    journal = InMemoryRunJournal(clock=clock)
    await journal.create("run-1", run_started(), operation_id="start")
    first = await journal.acquire("run-1", owner="worker-a", ttl_s=10.0)

    with pytest.raises(LeaseConflictError):
        await journal.acquire("run-1", owner="worker-b", ttl_s=10.0)
    with pytest.raises(LeaseLostError):
        await journal.append(
            "run-1",
            RunEventDraft(kind=RunEventKind.MODEL_STARTED, step=1),
            expected_sequence=1,
            operation_id="unleased",
        )

    await journal.append(
        "run-1",
        RunEventDraft(kind=RunEventKind.MODEL_STARTED, step=1),
        expected_sequence=1,
        operation_id="worker-a-event",
        lease=first,
    )
    clock.advance(11.0)
    second = await journal.acquire("run-1", owner="worker-b", ttl_s=10.0)
    assert second.fence > first.fence

    with pytest.raises(LeaseLostError):
        await journal.append(
            "run-1",
            RunEventDraft(kind=RunEventKind.MODEL_FAILED, step=1),
            expected_sequence=2,
            operation_id="stale-event",
            lease=first,
        )
    with pytest.raises(LeaseLostError):
        await journal.release(first)
    await journal.append(
        "run-1",
        RunEventDraft(kind=RunEventKind.MODEL_FAILED, step=1),
        expected_sequence=2,
        operation_id="worker-b-event",
        lease=second,
    )
    await journal.release(second)


@pytest.mark.asyncio
async def test_wait_and_concurrent_appends_observe_one_committed_sequence() -> None:
    journal = InMemoryRunJournal()
    await journal.create("run-1", run_started(), operation_id="start")
    waiter = asyncio.create_task(journal.wait("run-1", after_sequence=1, timeout_s=1.0))
    await asyncio.sleep(0)

    async def append(operation_id: str):
        return await journal.append(
            "run-1",
            RunEventDraft(kind=RunEventKind.MODEL_STARTED, step=1),
            expected_sequence=1,
            operation_id=operation_id,
        )

    outcomes = await asyncio.gather(
        append("concurrent-a"), append("concurrent-b"), return_exceptions=True
    )
    committed = [outcome for outcome in outcomes if not isinstance(outcome, Exception)]
    conflicts = [outcome for outcome in outcomes if isinstance(outcome, SequenceConflictError)]
    assert len(committed) == 1
    assert len(conflicts) == 1
    assert [event.sequence for event in await waiter] == [2]
