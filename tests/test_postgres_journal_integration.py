import asyncio
import os
import time
import uuid
from dataclasses import replace
from pathlib import Path

import pytest
from psycopg import AsyncConnection, errors, sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from react_agent.agent import ReActAgent
from react_agent.cost_ledger import CostAdjustmentConflictError, CostAdjustmentDraft
from react_agent.events import (
    GENESIS_HASH,
    EventHashError,
    EventTerminalError,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    StoredRunEvent,
    compute_event_hash,
    upcast_events,
    verify_event_chain,
)
from react_agent.journal import (
    LeaseConflictError,
    LeaseLostError,
    OperationConflictError,
    RunMetadataConflictError,
    SequenceConflictError,
    SessionBusyError,
    TerminalRunError,
)
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse, Usage
from react_agent.postgres_journal import (
    PostgresRunJournal,
    RequestConflictError,
    SessionVersionConflictError,
    UnsafeForkError,
)
from react_agent.runtime import (
    AgentRuntime,
    CancelRun,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)
from react_agent.telemetry import TraceReference

POSTGRES_DSN = os.environ.get("TEST_POSTGRES_DSN")
MIGRATION_NAMES = tuple(
    path.name for path in sorted((Path(__file__).resolve().parents[1] / "migrations").glob("*.sql"))
)

pytestmark = pytest.mark.skipif(
    not POSTGRES_DSN,
    reason="set TEST_POSTGRES_DSN to run PostgreSQL integration tests",
)


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def run_started(session_id: str, *, safe: bool = True) -> RunEventDraft:
    return RunEventDraft(
        kind=RunEventKind.RUN_STARTED,
        privacy=PrivacyClass.PRIVATE,
        session_id=session_id,
        execution_id=unique_id("execution"),
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
        data={"status": "running"},
        checkpoint={"transcript": [{"role": "user", "content": "secret"}]},
        safe_checkpoint=safe,
    )


async def open_journal(*, poll_interval_s: float = 0.2) -> PostgresRunJournal:
    assert POSTGRES_DSN is not None
    journal = PostgresRunJournal(
        POSTGRES_DSN,
        min_size=1,
        max_size=8,
        poll_interval_s=poll_interval_s,
    )
    await journal.open()
    await journal.migrate()
    return journal


class _BlockingRuntimeModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _AnswerRuntimeModel:
    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(AssistantMessage("completed"))


@pytest.mark.asyncio
async def test_agent_runtime_enforces_one_active_run_per_session_across_pools() -> None:
    first_journal = await open_journal()
    second_journal = await open_journal()
    blocking_model = _BlockingRuntimeModel()
    answer_model = _AnswerRuntimeModel()
    first_runtime = AgentRuntime(ReActAgent(blocking_model), first_journal)
    second_runtime = AgentRuntime(ReActAgent(answer_model), second_journal)
    session_id = unique_id("runtime-session")
    original = StartRun(
        prompt="first",
        session_id=session_id,
        idempotency_key="first-request",
    )

    try:
        first = await first_runtime.submit(original)
        await asyncio.wait_for(blocking_model.started.wait(), timeout=2)

        duplicate = await second_runtime.submit(original)
        assert duplicate.run_id == first.run_id
        assert duplicate.created is False

        with pytest.raises(RuntimeConflict, match=session_id):
            await second_runtime.submit(
                StartRun(
                    prompt="conflicting",
                    session_id=session_id,
                    idempotency_key="conflicting-request",
                )
            )
        assert answer_model.requests == []

        await first_runtime.submit(CancelRun(run_id=first.run_id))
        successor = await second_runtime.submit(
            StartRun(
                prompt="after abort",
                session_id=session_id,
                idempotency_key="successor-request",
            )
        )
        successor_snapshot = await second_runtime.wait(successor.run_id, timeout_s=3)

        assert successor_snapshot.status == "completed"
        assert len(answer_model.requests) == 1
    finally:
        await first_runtime.close()
        await second_runtime.close()
        await first_journal.close()
        await second_journal.close()


@pytest.mark.asyncio
async def test_postgres_orphan_reservation_keeps_session_busy_until_original_repairs() -> None:
    journal = await open_journal()
    session_id = unique_id("orphan-reservation-session")
    reserved_run_id = unique_id("orphan-reservation-run")
    try:
        first = await journal.reserve_request(
            session_id,
            "original-request",
            "original-hash",
            reserved_run_id,
        )
        duplicate = await journal.reserve_request(
            session_id,
            "original-request",
            "original-hash",
            unique_id("discarded-proposal"),
        )

        assert first.created is True
        assert duplicate.created is False
        assert duplicate.run_id == reserved_run_id
        with pytest.raises(SessionBusyError, match=session_id):
            await journal.reserve_request(
                session_id,
                "different-request",
                "different-hash",
                unique_id("different-run"),
            )
        assert await journal.list_runs(session_id) == ()
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_postgres_resume_claims_missing_owner_and_rejects_legacy_non_owner() -> None:
    first_journal = await open_journal()
    second_journal = await open_journal()
    session_id = unique_id("legacy-resume-session")
    owner_model = _BlockingRuntimeModel()
    owner_runtime = AgentRuntime(ReActAgent(owner_model), first_journal)
    owner = await owner_runtime.submit(
        StartRun(
            prompt="owner",
            session_id=session_id,
            idempotency_key="owner-request",
        )
    )
    await asyncio.wait_for(owner_model.started.wait(), timeout=2)
    await owner_runtime.close()

    try:
        # Simulate pre-007 data whose nonterminal Run has no Session claim.
        await first_journal.release_active_run(session_id, owner.run_id)
        resumed_model = _BlockingRuntimeModel()
        resumed_runtime = AgentRuntime(ReActAgent(resumed_model), first_journal)
        competing_runtime = AgentRuntime(ReActAgent(_AnswerRuntimeModel()), second_journal)
        try:
            await resumed_runtime.submit(ResumeRun(run_id=owner.run_id))
            await asyncio.wait_for(resumed_model.started.wait(), timeout=2)
            with pytest.raises(RuntimeConflict, match=session_id):
                await competing_runtime.submit(
                    StartRun(
                        prompt="must be blocked",
                        session_id=session_id,
                        idempotency_key="competing-request",
                    )
                )
        finally:
            await resumed_runtime.close()
            await competing_runtime.close()

        # Build the legacy ambiguity that migration 007 resolves to one owner.
        await first_journal.release_active_run(session_id, owner.run_id)
        legacy_model = _BlockingRuntimeModel()
        legacy_runtime = AgentRuntime(ReActAgent(legacy_model), second_journal)
        legacy = await legacy_runtime.submit(
            StartRun(
                prompt="legacy non-owner",
                session_id=session_id,
                idempotency_key="legacy-request",
            )
        )
        await asyncio.wait_for(legacy_model.started.wait(), timeout=2)
        await legacy_runtime.close()
        await second_journal.release_active_run(session_id, legacy.run_id)
        await first_journal.claim_active_run(session_id, owner.run_id)

        rejected_model = _AnswerRuntimeModel()
        rejected_runtime = AgentRuntime(ReActAgent(rejected_model), second_journal)
        try:
            with pytest.raises(RuntimeConflict, match=owner.run_id):
                await rejected_runtime.submit(ResumeRun(run_id=legacy.run_id))
            assert rejected_model.requests == []
        finally:
            await rejected_runtime.close()
    finally:
        await first_journal.close()
        await second_journal.close()


@pytest.mark.asyncio
async def test_v2_event_identity_and_causation_round_trip() -> None:
    journal = await open_journal()
    try:
        run_id = unique_id("v2-run")
        created = await journal.create(
            run_id,
            run_started(unique_id("v2-session")),
            operation_id="start",
        )
        appended = await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            expected_sequence=1,
            operation_id="model-started",
        )

        assert created.causation_id is None
        assert appended.causation_id == created.event_id
        replayed = await journal.read(run_id)
        assert tuple(event.event_id for event in replayed) == (
            created.event_id,
            appended.event_id,
        )
        assert replayed[0].causation_id is None
        assert replayed[1].causation_id == replayed[0].event_id
        verify_event_chain(replayed)

        retried = await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            expected_sequence=1,
            operation_id="model-started",
        )
        assert retried == appended
        assert retried.event_id == appended.event_id
        assert len(await journal.read(run_id)) == 2
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_legacy_v1_event_replays_and_accepts_v2_causal_successor() -> None:
    journal = await open_journal()
    run_id = unique_id("legacy-v1-run")
    session_id = unique_id("legacy-v1-session")
    legacy = StoredRunEvent(
        run_id=run_id,
        sequence=1,
        operation_id="legacy-start",
        event_id=str(uuid.uuid4()),
        kind=RunEventKind.RUN_STARTED,
        privacy=PrivacyClass.PRIVATE,
        schema_version=1,
        occurred_at=1_700_000_000.0,
        previous_hash=GENESIS_HASH,
        event_hash="",
        session_id=session_id,
        execution_id=unique_id("legacy-execution"),
        agent_revision="agent-v1",
        tool_manifest_hash="tools-v1",
        data={"status": "running"},
        checkpoint={"transcript": [{"role": "user", "content": "legacy secret"}]},
        safe_checkpoint=True,
    )
    legacy = replace(legacy, event_hash=compute_event_hash(legacy))
    try:
        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO react_agent_sessions (session_id)
                    VALUES (%s)
                    """,
                    (session_id,),
                )
                await connection.execute(
                    """
                    INSERT INTO react_agent_runs (
                        run_id, session_id, agent_revision, tool_manifest_hash,
                        head_sequence, head_hash, terminal
                    ) VALUES (%s, %s, %s, %s, 1, %s, FALSE)
                    """,
                    (
                        run_id,
                        session_id,
                        legacy.agent_revision,
                        legacy.tool_manifest_hash,
                        legacy.event_hash,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO react_agent_run_events (
                        run_id, sequence, event_id, operation_id,
                        operation_payload_hash, schema_version, event_type,
                        privacy_class, occurred_at, session_id, execution_id,
                        agent_revision, tool_manifest_hash, public_payload,
                        private_payload, safe_checkpoint, previous_hash,
                        event_hash
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        legacy.run_id,
                        legacy.sequence,
                        uuid.UUID(legacy.event_id),
                        legacy.operation_id,
                        "0" * 64,
                        legacy.schema_version,
                        legacy.kind.value,
                        legacy.privacy.value,
                        legacy.occurred_at,
                        legacy.session_id,
                        legacy.execution_id,
                        legacy.agent_revision,
                        legacy.tool_manifest_hash,
                        Jsonb({"status": "running"}),
                        Jsonb({"transcript": [{"role": "user", "content": "legacy secret"}]}),
                        legacy.safe_checkpoint,
                        legacy.previous_hash,
                        legacy.event_hash,
                    ),
                )
        finally:
            await connection.close()

        raw = await journal.read(run_id)
        assert raw == (legacy,)
        assert raw[0].causation_id is None
        verify_event_chain(raw)
        semantic = upcast_events(raw)
        assert semantic[0].schema_version == 2
        assert semantic[0].event_id == legacy.event_id
        assert semantic[0].event_hash == legacy.event_hash

        successor = await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            expected_sequence=1,
            operation_id="model-started-v2",
        )
        assert successor.schema_version == 2
        assert successor.previous_hash == legacy.event_hash
        assert successor.causation_id == legacy.event_id

        mixed = await journal.read(run_id)
        assert tuple(event.schema_version for event in mixed) == (1, 2)
        verify_event_chain(mixed)
        assert tuple(event.schema_version for event in upcast_events(mixed)) == (2, 2)
        assert (await journal.load(run_id)).last_sequence == 2
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_append_many_commits_one_causal_chain_and_retries_as_one_batch() -> None:
    journal = await open_journal()
    try:
        run_id = unique_id("batch-run")
        started = await journal.create(
            run_id,
            run_started(unique_id("batch-session")),
            operation_id="start",
        )
        model_started = RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=1,
            data={"attempt": 1},
            model_calls_delta=1,
        )
        model_completed = RunEventDraft(
            kind=RunEventKind.MODEL_COMPLETED,
            privacy=PrivacyClass.PRIVATE,
            step=1,
            data={"attempt": 1, "finish_reason": "stop"},
            checkpoint={
                "transcript": [
                    {"role": "user", "content": "batch"},
                    {"role": "assistant", "content": "done"},
                ]
            },
            safe_checkpoint=True,
            usage_delta=Usage(input_tokens=8, output_tokens=2, total_tokens=10),
        )
        entries = (
            (model_started, "batch:model-started"),
            (model_completed, "batch:model-completed"),
        )

        committed = await journal.append_many(
            run_id,
            entries,
            expected_sequence=1,
        )
        assert tuple(event.sequence for event in committed) == (2, 3)
        assert committed[0].previous_hash == started.event_hash
        assert committed[0].causation_id == started.event_id
        assert committed[1].previous_hash == committed[0].event_hash
        assert committed[1].causation_id == committed[0].event_id

        replayed = await journal.read(run_id)
        assert replayed == (started, *committed)
        verify_event_chain(replayed)
        snapshot = await journal.load(run_id)
        assert snapshot.last_sequence == 3
        assert snapshot.last_hash == committed[-1].event_hash
        assert snapshot.usage.total_tokens == 10

        retried = await journal.append_many(
            run_id,
            entries,
            expected_sequence=1,
        )
        assert retried == committed
        assert tuple(event.event_id for event in retried) == tuple(
            event.event_id for event in committed
        )
        assert len(await journal.read(run_id)) == 3

        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            cursor = await connection.execute(
                """
                SELECT head_sequence, head_hash, terminal,
                    (SELECT count(*) FROM react_agent_run_events
                     WHERE run_id = runs.run_id) AS event_count
                FROM react_agent_runs AS runs WHERE run_id = %s
                """,
                (run_id,),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert int(row[0]) == 3
            assert str(row[1]) == committed[-1].event_hash
            assert row[2] is False
            assert int(row[3]) == 3
        finally:
            await connection.close()
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_append_many_conflicts_fail_closed_without_partial_writes() -> None:
    journal = await open_journal()
    try:
        model_started = RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=1,
            data={"attempt": 1},
            model_calls_delta=1,
        )
        model_completed = RunEventDraft(
            kind=RunEventKind.MODEL_COMPLETED,
            step=1,
            data={"attempt": 1, "finish_reason": "stop"},
        )
        entries = (
            (model_started, "batch:model-started"),
            (model_completed, "batch:model-completed"),
        )

        conflict_run_id = unique_id("batch-payload-conflict")
        await journal.create(
            conflict_run_id,
            run_started(unique_id("batch-conflict-session")),
            operation_id="start",
        )
        committed = await journal.append_many(
            conflict_run_id,
            entries,
            expected_sequence=1,
        )
        conflicting_entries = (
            entries[0],
            (
                replace(
                    model_completed,
                    data={"attempt": 1, "finish_reason": "length"},
                ),
                "batch:model-completed",
            ),
        )
        with pytest.raises(OperationConflictError):
            await journal.append_many(
                conflict_run_id,
                conflicting_entries,
                expected_sequence=1,
            )
        assert (await journal.read(conflict_run_id))[1:] == committed

        partial_run_id = unique_id("batch-partial")
        await journal.create(
            partial_run_id,
            run_started(unique_id("batch-partial-session")),
            operation_id="start",
        )
        existing = await journal.append(
            partial_run_id,
            model_started,
            expected_sequence=1,
            operation_id="batch:model-started",
        )
        with pytest.raises(OperationConflictError, match="partially present"):
            await journal.append_many(
                partial_run_id,
                entries,
                expected_sequence=1,
            )
        assert await journal.read(partial_run_id) == (
            (await journal.read(partial_run_id))[0],
            existing,
        )
        partial_snapshot = await journal.load(partial_run_id)
        assert partial_snapshot.last_sequence == 2
        assert partial_snapshot.last_hash == existing.event_hash
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_append_many_terminal_followed_by_event_rolls_back_entire_batch() -> None:
    journal = await open_journal()
    try:
        run_id = unique_id("batch-terminal-rollback")
        started = await journal.create(
            run_id,
            run_started(unique_id("batch-terminal-session")),
            operation_id="start",
        )
        terminal = RunEventDraft(
            kind=RunEventKind.RUN_COMPLETED,
            data={"status": "completed", "stop_reason": "completed"},
            safe_checkpoint=True,
        )
        successor = RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=2,
            data={"attempt": 1},
            model_calls_delta=1,
        )

        with pytest.raises(EventTerminalError):
            await journal.append_many(
                run_id,
                (
                    (terminal, "batch:terminal"),
                    (successor, "batch:after-terminal"),
                ),
                expected_sequence=1,
            )

        assert await journal.read(run_id) == (started,)
        snapshot = await journal.load(run_id)
        assert snapshot.last_sequence == 1
        assert snapshot.terminal is None

        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            cursor = await connection.execute(
                """
                SELECT head_sequence, head_hash, terminal,
                    (SELECT count(*) FROM react_agent_run_events
                     WHERE run_id = runs.run_id) AS event_count
                FROM react_agent_runs AS runs WHERE run_id = %s
                """,
                (run_id,),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert int(row[0]) == 1
            assert str(row[1]) == started.event_hash
            assert row[2] is False
            assert int(row[3]) == 1
        finally:
            await connection.close()
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_concurrent_create_append_cas_and_operation_idempotency() -> None:
    # A long fallback interval makes this test prove NOTIFY wakes followers;
    # correctness still comes from the durable query after that wake.
    journal = await open_journal(poll_interval_s=5.0)
    try:
        session_id = unique_id("session")
        run_id = unique_id("run")
        started = run_started(session_id)

        created = await asyncio.gather(
            journal.create(run_id, started, operation_id="start"),
            journal.create(run_id, started, operation_id="start"),
        )
        assert created[0] == created[1]
        assert len(await journal.read(run_id)) == 1

        model_started = RunEventDraft(
            kind=RunEventKind.MODEL_STARTED,
            step=1,
            data={"attempt": 1},
            model_calls_delta=1,
        )

        async def append(operation_id: str):
            return await journal.append(
                run_id,
                model_started,
                expected_sequence=1,
                operation_id=operation_id,
            )

        waiter = asyncio.create_task(journal.wait(run_id, after_sequence=1, timeout_s=2.0))
        await asyncio.sleep(0.1)
        append_started_at = time.monotonic()
        outcomes = await asyncio.gather(
            append("model-a"), append("model-b"), return_exceptions=True
        )
        committed = [item for item in outcomes if not isinstance(item, Exception)]
        conflicts = [item for item in outcomes if isinstance(item, SequenceConflictError)]
        assert len(committed) == 1
        assert len(conflicts) == 1
        assert [event.sequence for event in await waiter] == [2]
        assert time.monotonic() - append_started_at < 1.0

        committed_event = committed[0]
        retried = await journal.append(
            run_id,
            model_started,
            expected_sequence=1,
            operation_id=committed_event.operation_id,
        )
        assert retried == committed_event
        assert len(await journal.read(run_id)) == 2

        with pytest.raises(OperationConflictError):
            await journal.append(
                run_id,
                replace(model_started, step=2),
                expected_sequence=2,
                operation_id=committed_event.operation_id,
            )
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_fencing_and_renewal_reject_stale_writers() -> None:
    journal = await open_journal()
    try:
        run_id = unique_id("run")
        await journal.create(run_id, run_started(unique_id("session")), operation_id="start")
        first = await journal.acquire(run_id, owner="worker-a", ttl_s=0.5)
        renewed = await journal.renew(first, ttl_s=0.5)
        assert renewed.fence == first.fence
        assert renewed.expires_at >= first.expires_at

        with pytest.raises(LeaseConflictError):
            await journal.acquire(run_id, owner="worker-b", ttl_s=1.0)
        with pytest.raises(LeaseConflictError):
            await journal.acquire(run_id, owner="worker-a", ttl_s=1.0)
        with pytest.raises(LeaseLostError):
            await journal.append(
                run_id,
                RunEventDraft(
                    kind=RunEventKind.MODEL_STARTED,
                    step=1,
                    data={"attempt": 1},
                ),
                expected_sequence=1,
                operation_id="unleased",
            )

        await asyncio.sleep(0.65)
        with pytest.raises(LeaseLostError):
            await journal.renew(renewed, ttl_s=1.0)
        second = await journal.acquire(run_id, owner="worker-b", ttl_s=2.0)
        assert second.fence > first.fence

        with pytest.raises(LeaseLostError):
            await journal.append(
                run_id,
                RunEventDraft(
                    kind=RunEventKind.MODEL_STARTED,
                    step=1,
                    data={"attempt": 1},
                ),
                expected_sequence=1,
                operation_id="stale-writer",
                lease=renewed,
            )
        committed = await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
            ),
            expected_sequence=1,
            operation_id="current-writer",
            lease=second,
        )
        assert committed.sequence == 2
        with pytest.raises(LeaseLostError):
            await journal.release(renewed)
        await journal.release(second)
        third = await journal.acquire(run_id, owner="worker-c", ttl_s=1.0)
        assert third.fence > second.fence
        await journal.release(third)
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_terminal_rebuild_survives_adapter_restart_and_snapshot_eviction() -> None:
    journal = await open_journal()
    run_id = unique_id("run")
    session_id = unique_id("session")
    try:
        await journal.create(run_id, run_started(session_id), operation_id="start")
        await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            expected_sequence=1,
            operation_id="model-started",
        )
        await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_COMPLETED,
                privacy=PrivacyClass.PRIVATE,
                step=1,
                data={"attempt": 1, "finish_reason": "stop"},
                checkpoint={
                    "transcript": [
                        {"role": "user", "content": "secret"},
                        {"role": "assistant", "content": "done"},
                    ]
                },
                safe_checkpoint=True,
                usage_delta=Usage(input_tokens=10, output_tokens=2, total_tokens=12),
            ),
            expected_sequence=2,
            operation_id="model-completed",
        )
        terminal = RunEventDraft(
            kind=RunEventKind.RUN_COMPLETED,
            data={"status": "completed", "stop_reason": "completed"},
            safe_checkpoint=True,
        )
        committed_terminal = await journal.append(
            run_id,
            terminal,
            expected_sequence=3,
            operation_id="terminal",
        )
        before = await journal.load(run_id)
        assert before.terminal == committed_terminal
        assert before.transcript[-1]["content"] == "done"
        assert (
            await asyncio.wait_for(
                journal.wait(run_id, after_sequence=4, timeout_s=None),
                timeout=0.5,
            )
            == ()
        )

        assert (
            await journal.append(
                run_id,
                terminal,
                expected_sequence=3,
                operation_id="terminal",
            )
            == committed_terminal
        )
        with pytest.raises(TerminalRunError):
            await journal.append(
                run_id,
                RunEventDraft(
                    kind=RunEventKind.MODEL_STARTED,
                    step=2,
                    data={"attempt": 1},
                ),
                expected_sequence=4,
                operation_id="after-terminal",
            )
    finally:
        await journal.close()

    # A new pool represents a fresh Runtime process. Snapshot removal proves
    # correctness still comes from sequence-ordered events, not projections.
    restarted = await open_journal()
    try:
        assert await restarted.load(run_id) == before
        assert await restarted.evict_snapshot(run_id) is True
        assert await restarted.load(run_id) == before
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_post_terminal_cost_adjustment_uses_an_independent_append_only_ledger() -> None:
    journal = await open_journal()
    run_id = unique_id("cost-adjustment-run")
    base_record = {
        "record_id": "cost-original",
        "operation_id": "model:step-1:attempt-1",
        "kind": "estimate",
        "currency": "USD",
        "amount_micros": None,
        "unknown_reason": "provider_completion_not_committed",
        "provider": "compatible",
        "model": "gpt-test",
        "usage": {"input_tokens": 10, "output_tokens": 2},
    }
    try:
        await journal.create(
            run_id,
            run_started(unique_id("cost-adjustment-session")),
            operation_id="start",
        )
        await journal.append(
            run_id,
            RunEventDraft(kind=RunEventKind.COST_RECORDED, data=base_record),
            expected_sequence=1,
            operation_id="base-cost",
        )
        terminal = await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.RUN_COMPLETED,
                data={"status": "completed", "stop_reason": "completed"},
                safe_checkpoint=True,
            ),
            expected_sequence=2,
            operation_id="terminal",
        )
        before = await journal.load(run_id)

        draft = CostAdjustmentDraft(
            record_id="invoice-adjustment",
            operation_id="invoice-line-1",
            previous_record_id="cost-original",
            revised_total_micros=375,
            note="provider invoice",
        )
        committed = await journal.append_cost_adjustment(
            run_id, draft, previous_record=base_record
        )
        retried = await journal.append_cost_adjustment(
            run_id, draft, previous_record=base_record
        )

        assert committed.created is True
        assert retried.created is False
        assert retried.record == committed.record
        assert committed.record.public_payload["amount_micros"] == 375
        assert committed.record.public_payload["ledger_sequence"] == 1
        assert await journal.list_cost_adjustments(run_id) == (committed.record,)
        # The terminal sequence and hash chain are byte-for-byte untouched.
        after = await journal.load(run_id)
        assert after == before
        assert after.last_sequence == terminal.sequence == 3
        assert len(await journal.read(run_id)) == 3

        with pytest.raises(CostAdjustmentConflictError, match="different content"):
            await journal.append_cost_adjustment(
                run_id,
                replace(draft, revised_total_micros=376),
                previous_record=base_record,
            )
        with pytest.raises(CostAdjustmentConflictError, match="latest record"):
            await journal.append_cost_adjustment(
                run_id,
                CostAdjustmentDraft(
                    record_id="competing-adjustment",
                    operation_id="invoice-line-2",
                    previous_record_id="cost-original",
                    revised_total_micros=400,
                ),
                previous_record=base_record,
            )

        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            with pytest.raises(errors.ObjectNotInPrerequisiteState):
                async with connection.transaction():
                    await connection.execute(
                        """
                        UPDATE react_agent_cost_adjustments SET public_payload = '{}'::jsonb
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
        finally:
            await connection.close()
    finally:
        await journal.close()

    restarted = await open_journal()
    try:
        [restored] = await restarted.list_cost_adjustments(run_id)
        assert restored.record_id == "invoice-adjustment"
        assert (await restarted.load(run_id)).last_sequence == 3
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_request_session_cas_safe_lineage_and_run_listing() -> None:
    journal = await open_journal()
    try:
        session_id = unique_id("session")
        proposed = [unique_id("run"), unique_id("run")]

        reservations = await asyncio.gather(
            journal.reserve_request(session_id, "request-1", "hash-a", proposed[0]),
            journal.reserve_request(session_id, "request-1", "hash-a", proposed[1]),
        )
        assert reservations[0].run_id == reservations[1].run_id
        assert sorted(item.created for item in reservations) == [False, True]
        reserved_run_id = reservations[0].run_id
        await journal.create(
            reserved_run_id,
            run_started(session_id),
            operation_id="reserved-run-start",
        )

        with pytest.raises(RequestConflictError):
            await journal.reserve_request(
                session_id, "request-1", "different-hash", unique_id("run")
            )
        with pytest.raises(RequestConflictError):
            await journal.reserve_request(session_id, "request-2", "hash-b", reserved_run_id)

        initial = await journal.load_session(session_id)
        assert initial.version == 0
        assert initial.transcript == ()
        commit_outcomes = await asyncio.gather(
            journal.commit_session(
                session_id,
                expected_version=0,
                transcript=({"role": "user", "content": "a"},),
            ),
            journal.commit_session(
                session_id,
                expected_version=0,
                transcript=({"role": "user", "content": "b"},),
            ),
            return_exceptions=True,
        )
        committed = [item for item in commit_outcomes if not isinstance(item, Exception)]
        conflicts = [
            item for item in commit_outcomes if isinstance(item, SessionVersionConflictError)
        ]
        assert len(committed) == 1
        assert len(conflicts) == 1
        assert (await journal.load_session(session_id)).version == 1

        # The Session worktree cannot be shared with another live Run.  End
        # the reserved Run before exercising a second lineage in this Session.
        await journal.append(
            reserved_run_id,
            RunEventDraft(
                kind=RunEventKind.RUN_COMPLETED,
                data={"status": "completed", "stop_reason": "completed"},
                safe_checkpoint=True,
            ),
            expected_sequence=1,
            operation_id="reserved-run-terminal",
        )

        parent_run_id = unique_id("parent")
        child_run_id = unique_id("child")
        unsafe_child_id = unique_id("child")
        await journal.create(parent_run_id, run_started(session_id), operation_id="parent-start")
        with pytest.raises(RequestConflictError):
            await journal.reserve_request(
                session_id, "request-existing-run", "hash-c", parent_run_id
            )
        await journal.append(
            parent_run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
            ),
            expected_sequence=1,
            operation_id="parent-model",
        )
        await journal.create(
            child_run_id,
            run_started(unique_id("child-session")),
            operation_id="child-start",
        )
        await journal.create(
            unsafe_child_id,
            run_started(unique_id("child-session")),
            operation_id="unsafe-child-start",
        )
        await journal.set_lineage(
            child_run_id,
            parent_run_id=parent_run_id,
            fork_sequence=1,
            workspace_tree="abc123",
        )
        # Exact retries are idempotent; lineage is otherwise immutable.
        await journal.set_lineage(
            child_run_id,
            parent_run_id=parent_run_id,
            fork_sequence=1,
            workspace_tree="abc123",
        )
        with pytest.raises(RunMetadataConflictError):
            await journal.set_lineage(
                child_run_id,
                parent_run_id=parent_run_id,
                fork_sequence=1,
                workspace_tree="different",
            )
        with pytest.raises(UnsafeForkError):
            await journal.set_lineage(
                unsafe_child_id,
                parent_run_id=parent_run_id,
                fork_sequence=2,
                workspace_tree=None,
            )

        cycle_a = unique_id("cycle-a")
        cycle_b = unique_id("cycle-b")
        await journal.create(
            cycle_a,
            run_started(unique_id("cycle-session")),
            operation_id="cycle-a-start",
        )
        await journal.create(
            cycle_b,
            run_started(unique_id("cycle-session")),
            operation_id="cycle-b-start",
        )
        await journal.set_lineage(
            cycle_a,
            parent_run_id=cycle_b,
            fork_sequence=1,
            workspace_tree=None,
        )
        with pytest.raises(RunMetadataConflictError):
            await journal.set_lineage(
                cycle_b,
                parent_run_id=cycle_a,
                fork_sequence=1,
                workspace_tree=None,
            )
        assert parent_run_id in await journal.list_runs(session_id)
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_hash_chain_detects_admin_level_payload_tampering() -> None:
    journal = await open_journal()
    run_id = unique_id("run")
    try:
        await journal.create(run_id, run_started(unique_id("session")), operation_id="start")
        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            with pytest.raises(errors.ObjectNotInPrerequisiteState):
                async with connection.transaction():
                    await connection.execute(
                        """
                        UPDATE react_agent_run_events
                        SET public_payload = '{"status":"blocked"}'::jsonb
                        WHERE run_id = %s AND sequence = 1
                        """,
                        (run_id,),
                    )
            with pytest.raises(errors.ObjectNotInPrerequisiteState):
                async with connection.transaction():
                    await connection.execute("TRUNCATE react_agent_run_events")

            # This deliberately escalates to database-owner behavior to
            # simulate accidental/manual administrator-level corruption.
            async with connection.transaction():
                await connection.execute(
                    """
                    ALTER TABLE react_agent_run_events
                    DISABLE TRIGGER react_agent_reject_event_mutation_trigger
                    """
                )
                await connection.execute(
                    """
                    UPDATE react_agent_run_events
                    SET public_payload = '{"status":"tampered"}'::jsonb
                    WHERE run_id = %s AND sequence = 1
                    """,
                    (run_id,),
                )
                await connection.execute(
                    """
                    ALTER TABLE react_agent_run_events
                    ENABLE TRIGGER react_agent_reject_event_mutation_trigger
                    """
                )
        finally:
            await connection.close()
        with pytest.raises(EventHashError):
            await journal.load(run_id)
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_extended_usage_round_trips_without_changing_event_hash() -> None:
    journal = await open_journal()
    try:
        run_id = unique_id("usage-run")
        await journal.create(run_id, run_started(unique_id("usage-session")), operation_id="start")
        await journal.append(
            run_id,
            RunEventDraft(
                kind=RunEventKind.MODEL_STARTED,
                step=1,
                data={"attempt": 1},
                model_calls_delta=1,
            ),
            expected_sequence=1,
            operation_id="model-started",
        )
        detailed_usage = Usage(
            input_tokens=120,
            output_tokens=30,
            total_tokens=150,
            cached_input_tokens=48,
            reasoning_output_tokens=12,
            billable_tokens=102,
        )
        completion = RunEventDraft(
            kind=RunEventKind.MODEL_COMPLETED,
            privacy=PrivacyClass.PRIVATE,
            step=1,
            data={"attempt": 1, "finish_reason": "stop"},
            checkpoint={
                "transcript": [
                    {"role": "user", "content": "usage"},
                    {"role": "assistant", "content": "complete"},
                ]
            },
            safe_checkpoint=True,
            usage_delta=detailed_usage,
        )
        committed = await journal.append(
            run_id,
            completion,
            expected_sequence=2,
            operation_id="model-completed",
        )

        replayed = (await journal.read(run_id, after_sequence=2))[0]
        assert replayed.usage_delta == detailed_usage
        assert replayed.event_hash == committed.event_hash
        assert compute_event_hash(replayed) == committed.event_hash
        events = await journal.read(run_id)
        verify_event_chain(events)
        snapshot = await journal.load(run_id)
        assert snapshot.usage == detailed_usage
        assert await journal.evict_snapshot(run_id) is True
        assert (await journal.load(run_id)).usage == detailed_usage

        assert (
            await journal.append(
                run_id,
                completion,
                expected_sequence=2,
                operation_id="model-completed",
            )
            == committed
        )
        with pytest.raises(OperationConflictError):
            await journal.append(
                run_id,
                replace(
                    completion,
                    usage_delta=replace(detailed_usage, billable_tokens=101),
                ),
                expected_sequence=3,
                operation_id="model-completed",
            )
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_session_commit_operation_is_idempotent_and_content_addressed() -> None:
    journal = await open_journal()
    try:
        session_id = unique_id("commit-session")
        await journal.reserve_request(
            session_id,
            "create-session",
            "request-hash",
            unique_id("reserved-run"),
        )
        transcript = (
            {
                "role": "user",
                "content": "first",
                "metadata": {"z": 1, "a": 2},
            },
        )
        committed = await journal.commit_session(
            session_id,
            expected_version=0,
            transcript=transcript,
            operation_id="commit-op-1",
        )
        canonical_equivalent = (
            {
                "metadata": {"a": 2, "z": 1},
                "content": "first",
                "role": "user",
            },
        )
        retried = await journal.commit_session(
            session_id,
            expected_version=0,
            transcript=canonical_equivalent,
            operation_id="commit-op-1",
        )
        assert retried == committed
        assert retried.version == 1
        assert (await journal.load_session(session_id)).version == 1

        with pytest.raises(RequestConflictError):
            await journal.commit_session(
                session_id,
                expected_version=0,
                transcript=({"role": "user", "content": "different"},),
                operation_id="commit-op-1",
            )
        with pytest.raises(RequestConflictError):
            await journal.commit_session(
                session_id,
                expected_version=1,
                transcript=canonical_equivalent,
                operation_id="commit-op-1",
            )
        with pytest.raises(ValueError):
            await journal.commit_session(
                session_id,
                expected_version=1,
                transcript=canonical_equivalent,
                operation_id="",
            )

        second = await journal.commit_session(
            session_id,
            expected_version=1,
            transcript=({"role": "user", "content": "second"},),
            operation_id="commit-op-2",
        )
        assert second.version == 2
        # Retrying an old successful operation returns its frozen result even
        # after the Session has advanced again.
        assert (
            await journal.commit_session(
                session_id,
                expected_version=0,
                transcript=canonical_equivalent,
                operation_id="commit-op-1",
            )
            == committed
        )
        with pytest.raises(SessionVersionConflictError):
            await journal.commit_session(
                session_id,
                expected_version=0,
                transcript=({"role": "user", "content": "stale"},),
                operation_id="uncommitted-stale-op",
            )

        concurrent_session = unique_id("concurrent-commit-session")
        await journal.reserve_request(
            concurrent_session,
            "create-session",
            "request-hash",
            unique_id("reserved-run"),
        )
        concurrent_results = await asyncio.gather(
            journal.commit_session(
                concurrent_session,
                expected_version=0,
                transcript=transcript,
                operation_id="same-operation",
            ),
            journal.commit_session(
                concurrent_session,
                expected_version=0,
                transcript=canonical_equivalent,
                operation_id="same-operation",
            ),
        )
        assert concurrent_results[0] == concurrent_results[1]
        assert concurrent_results[0].version == 1

        assert POSTGRES_DSN is not None
        connection = await AsyncConnection.connect(POSTGRES_DSN)
        try:
            cursor = await connection.execute(
                """
                SELECT count(*) FROM react_agent_session_commits
                WHERE session_id = %s
                """,
                (concurrent_session,),
            )
            row = await cursor.fetchone()
            assert row is not None and int(row[0]) == 1
        finally:
            await connection.close()
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_execution_trace_reference_survives_pool_restart_and_is_immutable() -> None:
    journal = await open_journal()
    run_id = unique_id("trace-reference-run")
    draft = run_started(unique_id("trace-reference-session"))
    assert draft.execution_id is not None
    first = TraceReference(
        run_id=run_id,
        execution_id=draft.execution_id,
        trace_id="1" * 32,
        span_id="2" * 16,
        trace_flags=1,
    )
    try:
        await journal.create(run_id, draft, operation_id="start")
        await journal.put_trace_reference(first)
        await journal.put_trace_reference(
            replace(first, trace_id="3" * 32, span_id="4" * 16)
        )
        assert await journal.load_trace_reference(run_id, draft.execution_id) == first
        await journal.put_trace_reference(
            replace(first, execution_id="not-a-committed-execution")
        )
        assert (
            await journal.load_trace_reference(run_id, "not-a-committed-execution")
            is None
        )
    finally:
        await journal.close()

    reopened = await open_journal()
    try:
        assert await reopened.load_trace_reference(run_id, draft.execution_id) == first
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_migrations_001_through_010_apply_in_order_to_empty_schema() -> None:
    assert POSTGRES_DSN is not None
    assert MIGRATION_NAMES[:10] == (
        "001_event_journal.sql",
        "002_runtime_registry.sql",
        "003_journal_hardening.sql",
        "004_usage_details.sql",
        "005_session_commit_idempotency.sql",
        "006_event_causation.sql",
        "007_session_active_run.sql",
        "008_cost_adjustment_ledger.sql",
        "009_execution_trace_references.sql",
        "010_public_event_projection.sql",
    )

    schema_name = f"migration_it_{uuid.uuid4().hex}"
    admin = await AsyncConnection.connect(POSTGRES_DSN, autocommit=True)
    migration_journal: PostgresRunJournal | None = None
    try:
        await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        schema_dsn = make_conninfo(
            POSTGRES_DSN,
            options=f"-csearch_path={schema_name}",
        )
        migration_journal = PostgresRunJournal(schema_dsn)
        await migration_journal.open()
        await migration_journal.migrate()
        # Every migration is intentionally re-runnable.
        await migration_journal.migrate()

        schema_connection = await AsyncConnection.connect(schema_dsn)
        try:
            current_schema_cursor = await schema_connection.execute("SELECT current_schema()")
            current_schema = await current_schema_cursor.fetchone()
            assert current_schema is not None and current_schema[0] == schema_name

            columns_cursor = await schema_connection.execute(
                """
                SELECT table_name, column_name FROM information_schema.columns
                WHERE table_schema = current_schema()
                """
            )
            columns = set(await columns_cursor.fetchall())
            assert {
                ("react_agent_run_events", "cached_input_tokens"),
                ("react_agent_run_events", "reasoning_output_tokens"),
                ("react_agent_run_events", "billable_tokens"),
                ("react_agent_run_events", "causation_id"),
                ("react_agent_session_commits", "operation_id"),
                ("react_agent_session_commits", "transcript_hash"),
                ("react_agent_sessions", "active_run_id"),
                ("react_agent_cost_adjustments", "ledger_sequence"),
                ("react_agent_cost_adjustments", "public_payload"),
                ("react_agent_execution_trace_references", "trace_id"),
                ("react_agent_execution_trace_references", "span_id"),
                ("react_agent_execution_trace_references", "trace_flags"),
            } <= columns

            constraints_cursor = await schema_connection.execute(
                """
                SELECT conname FROM pg_constraint
                WHERE connamespace = current_schema()::regnamespace
                """
            )
            constraints = {str(row[0]) for row in await constraints_cursor.fetchall()}
            assert "react_agent_run_events_usage_details_valid" in constraints
            assert "react_agent_run_events_causation_not_self" in constraints
            assert "react_agent_run_events_causation_fk" in constraints
            assert "react_agent_session_commits_version_unique" in constraints

            view_columns_cursor = await schema_connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = current_schema()
                    AND table_name = 'react_agent_public_run_events'
                """
            )
            public_view_columns = {
                str(row[0]) for row in await view_columns_cursor.fetchall()
            }
            assert "public_payload" in public_view_columns
            assert "private_payload" not in public_view_columns
        finally:
            await schema_connection.close()

        run_id = unique_id("fresh-schema-run")
        await migration_journal.create(
            run_id,
            run_started(unique_id("fresh-schema-session")),
            operation_id="start",
        )
        assert (await migration_journal.load(run_id)).last_sequence == 1
    finally:
        if migration_journal is not None:
            await migration_journal.close()
        await admin.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name))
        )
        await admin.close()
