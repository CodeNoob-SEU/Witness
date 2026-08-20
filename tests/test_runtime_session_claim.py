from __future__ import annotations

import asyncio

import pytest

from react_agent.agent import ReActAgent
from react_agent.journal import InMemoryRunJournal
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse
from react_agent.runtime import (
    AgentRuntime,
    CancelRun,
    ForkRun,
    InMemoryRuntimeStore,
    RequestReservation,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)


class BlockingModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class AnswerModel:
    def __init__(self, answer: str = "done") -> None:
        self.answer = answer
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(AssistantMessage(self.answer))


class GatedAnswerModel:
    def __init__(self, answer: str = "done") -> None:
        self.answer = answer
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.requests: list[ModelRequest] = []

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.started.set()
        await self.release.wait()
        return ModelResponse(AssistantMessage(self.answer))


class FailFirstReleaseStore(InMemoryRuntimeStore):
    def __init__(self) -> None:
        super().__init__()
        self._fail_release = True

    async def release_active_run(self, session_id: str, run_id: str) -> None:
        if self._fail_release:
            self._fail_release = False
            raise RuntimeError("simulated claim release outage")
        await super().release_active_run(session_id, run_id)


class SimulatedCrash(BaseException):
    pass


class CrashAfterReservationStore(InMemoryRuntimeStore):
    def __init__(self) -> None:
        super().__init__()
        self._armed = True

    async def reserve_request(
        self,
        session_id: str,
        idempotency_key: str,
        request_hash: str,
        proposed_run_id: str,
    ) -> RequestReservation:
        reservation = await super().reserve_request(
            session_id,
            idempotency_key,
            request_hash,
            proposed_run_id,
        )
        if self._armed and reservation.created:
            self._armed = False
            raise SimulatedCrash
        return reservation


@pytest.mark.asyncio
async def test_different_start_is_rejected_before_side_effect_while_session_is_active() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    first_model = BlockingModel()
    second_model = AnswerModel("must not run")
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    second_runtime = AgentRuntime(ReActAgent(second_model), journal, store=store)

    try:
        first = await first_runtime.submit(
            StartRun(
                prompt="first",
                session_id="shared-session",
                idempotency_key="first-request",
            )
        )
        await asyncio.wait_for(first_model.started.wait(), timeout=1)

        with pytest.raises(RuntimeConflict, match="shared-session"):
            await second_runtime.submit(
                StartRun(
                    prompt="second",
                    session_id="shared-session",
                    idempotency_key="second-request",
                )
            )

        assert first.created is True
        assert second_model.requests == []
    finally:
        await first_runtime.close()
        await second_runtime.close()


@pytest.mark.asyncio
async def test_session_accepts_a_new_start_after_the_owner_reaches_terminal() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    model = AnswerModel()
    runtime = AgentRuntime(ReActAgent(model), journal, store=store)

    try:
        first = await runtime.submit(
            StartRun(
                prompt="first",
                session_id="reusable-session",
                idempotency_key="first-request",
            )
        )
        await runtime.wait(first.run_id, timeout_s=2)

        second = await runtime.submit(
            StartRun(
                prompt="second",
                session_id="reusable-session",
                idempotency_key="second-request",
            )
        )
        snapshot = await runtime.wait(second.run_id, timeout_s=2)

        assert second.run_id != first.run_id
        assert snapshot.status == "completed"
        assert len(model.requests) == 2
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_new_start_self_heals_a_stale_claim_whose_run_is_terminal() -> None:
    journal = InMemoryRunJournal()
    store = FailFirstReleaseStore()
    model = AnswerModel()
    runtime = AgentRuntime(ReActAgent(model), journal, store=store)

    try:
        first = await runtime.submit(
            StartRun(
                prompt="first",
                session_id="self-heal-session",
                idempotency_key="first-request",
            )
        )
        first_snapshot = await runtime.wait(first.run_id, timeout_s=2)
        assert first_snapshot.status == "completed"

        second = await runtime.submit(
            StartRun(
                prompt="second",
                session_id="self-heal-session",
                idempotency_key="second-request",
            )
        )
        second_snapshot = await runtime.wait(second.run_id, timeout_s=2)

        assert second_snapshot.status == "completed"
        assert len(model.requests) == 2
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_reservation_without_run_blocks_other_requests_but_original_repairs() -> None:
    journal = InMemoryRunJournal()
    store = CrashAfterReservationStore()
    crashed_runtime = AgentRuntime(ReActAgent(AnswerModel()), journal, store=store)
    original = StartRun(
        prompt="original",
        session_id="reserved-session",
        idempotency_key="original-request",
    )

    try:
        with pytest.raises(SimulatedCrash):
            await crashed_runtime.submit(original)

        other_model = AnswerModel("must not run")
        recovery_model = AnswerModel("repaired")
        other_runtime = AgentRuntime(ReActAgent(other_model), journal, store=store)
        recovery_runtime = AgentRuntime(ReActAgent(recovery_model), journal, store=store)
        try:
            with pytest.raises(RuntimeConflict, match="reserved-session"):
                await other_runtime.submit(
                    StartRun(
                        prompt="different",
                        session_id="reserved-session",
                        idempotency_key="different-request",
                    )
                )
            repaired = await recovery_runtime.submit(original)
            snapshot = await recovery_runtime.wait(repaired.run_id, timeout_s=2)

            assert snapshot.status == "completed"
            assert other_model.requests == []
            assert len(recovery_model.requests) == 1
        finally:
            await other_runtime.close()
            await recovery_runtime.close()
    finally:
        await crashed_runtime.close()


@pytest.mark.asyncio
async def test_resume_keeps_the_session_owner_and_releases_it_on_completion() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    interrupted_model = BlockingModel()
    interrupted_runtime = AgentRuntime(
        ReActAgent(interrupted_model),
        journal,
        store=store,
    )

    interrupted = await interrupted_runtime.submit(
        StartRun(
            prompt="interrupt",
            session_id="resume-session",
            idempotency_key="interrupted-request",
        )
    )
    await asyncio.wait_for(interrupted_model.started.wait(), timeout=1)
    await interrupted_runtime.close()

    recovery_model = AnswerModel("resumed")
    recovery_runtime = AgentRuntime(ReActAgent(recovery_model), journal, store=store)
    try:
        resumed = await recovery_runtime.submit(ResumeRun(run_id=interrupted.run_id))
        resumed_snapshot = await recovery_runtime.wait(resumed.run_id, timeout_s=2)
        assert resumed_snapshot.status == "completed"

        next_run = await recovery_runtime.submit(
            StartRun(
                prompt="next",
                session_id="resume-session",
                idempotency_key="next-request",
            )
        )
        next_snapshot = await recovery_runtime.wait(next_run.run_id, timeout_s=2)

        assert next_snapshot.status == "completed"
        assert len(recovery_model.requests) == 2
    finally:
        await recovery_runtime.close()


@pytest.mark.asyncio
async def test_resume_reclaims_a_legacy_missing_session_owner_before_side_effects() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    first_model = BlockingModel()
    first_runtime = AgentRuntime(ReActAgent(first_model), journal, store=store)
    interrupted = await first_runtime.submit(
        StartRun(
            prompt="interrupt",
            session_id="legacy-missing-owner",
            idempotency_key="interrupted-request",
        )
    )
    await asyncio.wait_for(first_model.started.wait(), timeout=1)
    await first_runtime.close()
    await store.release_active_run("legacy-missing-owner", interrupted.run_id)

    resumed_model = GatedAnswerModel("resumed")
    resumed_runtime = AgentRuntime(ReActAgent(resumed_model), journal, store=store)
    competing_model = AnswerModel("must not run")
    competing_runtime = AgentRuntime(ReActAgent(competing_model), journal, store=store)
    try:
        await resumed_runtime.submit(ResumeRun(run_id=interrupted.run_id))
        await asyncio.wait_for(resumed_model.started.wait(), timeout=1)

        with pytest.raises(RuntimeConflict, match="legacy-missing-owner"):
            await competing_runtime.submit(
                StartRun(
                    prompt="competing",
                    session_id="legacy-missing-owner",
                    idempotency_key="competing-request",
                )
            )
        assert competing_model.requests == []

        resumed_model.release.set()
        snapshot = await resumed_runtime.wait(interrupted.run_id, timeout_s=2)
        assert snapshot.status == "completed"
    finally:
        resumed_model.release.set()
        await resumed_runtime.close()
        await competing_runtime.close()


@pytest.mark.asyncio
async def test_resume_rejects_a_legacy_run_owned_by_a_different_active_run() -> None:
    journal = InMemoryRunJournal()
    owner_store = InMemoryRuntimeStore()
    legacy_store = InMemoryRuntimeStore()
    owner_model = BlockingModel()
    legacy_model = BlockingModel()
    owner_runtime = AgentRuntime(ReActAgent(owner_model), journal, store=owner_store)
    legacy_runtime = AgentRuntime(ReActAgent(legacy_model), journal, store=legacy_store)

    owner = await owner_runtime.submit(
        StartRun(
            prompt="owner",
            session_id="legacy-ambiguous-session",
            idempotency_key="owner-request",
        )
    )
    await asyncio.wait_for(owner_model.started.wait(), timeout=1)
    legacy = await legacy_runtime.submit(
        StartRun(
            prompt="legacy non-owner",
            session_id="legacy-ambiguous-session",
            idempotency_key="legacy-request",
        )
    )
    await asyncio.wait_for(legacy_model.started.wait(), timeout=1)
    await owner_runtime.close()
    await legacy_runtime.close()

    resumed_model = AnswerModel("must not run")
    resumed_runtime = AgentRuntime(ReActAgent(resumed_model), journal, store=owner_store)
    try:
        with pytest.raises(RuntimeConflict, match=owner.run_id):
            await resumed_runtime.submit(ResumeRun(run_id=legacy.run_id))
        assert resumed_model.requests == []
    finally:
        await resumed_runtime.close()


@pytest.mark.asyncio
async def test_fork_cannot_enter_a_session_owned_by_an_active_run() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    parent_runtime = AgentRuntime(ReActAgent(AnswerModel("parent")), journal, store=store)
    blocking_model = BlockingModel()
    busy_runtime = AgentRuntime(ReActAgent(blocking_model), journal, store=store)

    try:
        parent = await parent_runtime.submit(
            StartRun(
                prompt="parent",
                session_id="parent-session",
                idempotency_key="parent-request",
            )
        )
        await parent_runtime.wait(parent.run_id, timeout_s=2)
        busy = await busy_runtime.submit(
            StartRun(
                prompt="busy",
                session_id="fork-target-session",
                idempotency_key="busy-request",
            )
        )
        await asyncio.wait_for(blocking_model.started.wait(), timeout=1)

        with pytest.raises(RuntimeConflict, match="fork-target-session"):
            await parent_runtime.submit(
                ForkRun(
                    run_id=parent.run_id,
                    from_sequence=1,
                    session_id="fork-target-session",
                    idempotency_key="fork-request",
                )
            )

        assert busy.created is True
    finally:
        await parent_runtime.close()
        await busy_runtime.close()


@pytest.mark.asyncio
async def test_abort_releases_the_session_for_a_new_start() -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    blocking_model = BlockingModel()
    runtime = AgentRuntime(ReActAgent(blocking_model), journal, store=store)

    try:
        active = await runtime.submit(
            StartRun(
                prompt="block",
                session_id="abort-session",
                idempotency_key="blocking-request",
            )
        )
        await asyncio.wait_for(blocking_model.started.wait(), timeout=1)
        await runtime.submit(CancelRun(run_id=active.run_id))

        next_model = AnswerModel("after abort")
        next_runtime = AgentRuntime(ReActAgent(next_model), journal, store=store)
        try:
            next_run = await next_runtime.submit(
                StartRun(
                    prompt="continue",
                    session_id="abort-session",
                    idempotency_key="next-request",
                )
            )
            snapshot = await next_runtime.wait(next_run.run_id, timeout_s=2)

            assert snapshot.status == "completed"
            assert len(next_model.requests) == 1
        finally:
            await next_runtime.close()
    finally:
        await runtime.close()
