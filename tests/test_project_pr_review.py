from __future__ import annotations

from dataclasses import replace

import pytest

from react_agent.project_pr import (
    InMemoryProjectPRStore,
    MockForge,
    ProjectPRConflict,
    ProjectPREventKind,
    ProjectPRState,
    ProjectPullRequests,
    PublishPR,
    SealRevision,
    StartPR,
    verify_project_pr_events,
)

_BASE_SHA = "a" * 40
_HEAD_SHA = "b" * 40
_CANDIDATE_TREE = "c" * 40
_PATCH_DIGEST = "1" * 64
_VERIFICATION_DIGEST = "2" * 64
_EVIDENCE_DIGEST = "3" * 64


def _start_command(*, goal: str = "Correct the disabled-research pricing path") -> StartPR:
    return StartPR(
        project_key="interview-demo",
        repository="acme/pricing-service",
        pull_request_number=42,
        base_sha=_BASE_SHA,
        head_sha=_HEAD_SHA,
        goal=goal,
        idempotency_key="demo-pr-42",
    )


def _seal_command(
    workflow_id: str,
    *,
    evidence_digest: str = _EVIDENCE_DIGEST,
) -> SealRevision:
    return SealRevision(
        workflow_id=workflow_id,
        expected_revision=1,
        observed_head_sha=_HEAD_SHA,
        candidate_tree=_CANDIDATE_TREE,
        patch_digest=_PATCH_DIGEST,
        verification_digest=_VERIFICATION_DIGEST,
        evidence_digest=evidence_digest,
        operation_id="review:seal:r1",
    )


def _workflows(
    *,
    store: InMemoryProjectPRStore | None = None,
    forge: MockForge | None = None,
) -> tuple[ProjectPullRequests, InMemoryProjectPRStore, MockForge]:
    resolved_store = store or InMemoryProjectPRStore()
    resolved_forge = forge or MockForge(head_sha=_HEAD_SHA)
    return (
        ProjectPullRequests(
            store=resolved_store,
            forge=resolved_forge,
            worker_id="review-worker",
        ),
        resolved_store,
        resolved_forge,
    )


@pytest.mark.asyncio
async def test_start_pr_same_idempotency_key_and_payload_returns_existing_workflow() -> None:
    workflows, _, _ = _workflows()
    command = _start_command()

    created = await workflows.submit(command)
    retried = await workflows.submit(command)
    snapshot = await workflows.load(created.workflow_id)

    assert created.created is True
    assert retried.workflow_id == created.workflow_id
    assert retried.revision == created.revision == 1
    assert retried.created is False
    assert snapshot.last_sequence == 1
    assert snapshot.state is ProjectPRState.ANCHORED


@pytest.mark.asyncio
async def test_start_pr_same_idempotency_key_with_different_payload_conflicts() -> None:
    workflows, _, _ = _workflows()
    await workflows.submit(_start_command())

    with pytest.raises(ProjectPRConflict, match="idempotency key"):
        await workflows.submit(_start_command(goal="A different logical request"))


@pytest.mark.asyncio
async def test_seal_revision_ack_loss_retry_returns_the_same_durable_commit() -> None:
    workflows, store, _ = _workflows()
    started = await workflows.submit(_start_command())
    command = _seal_command(started.workflow_id)

    await workflows.submit(command)  # The first acknowledgement is deliberately discarded.
    before_retry = await workflows.load(started.workflow_id)
    events_before_retry = await store.read(started.workflow_id)

    retried = await workflows.submit(command)
    after_retry = await workflows.load(started.workflow_id)

    assert retried.created is False
    assert retried.workflow_id == started.workflow_id
    assert after_retry == before_retry
    assert await store.read(started.workflow_id) == events_before_retry
    assert after_retry.last_sequence == 2


@pytest.mark.asyncio
async def test_seal_revision_same_operation_with_different_payload_conflicts() -> None:
    workflows, _, _ = _workflows()
    started = await workflows.submit(_start_command())
    command = _seal_command(started.workflow_id)
    await workflows.submit(command)

    with pytest.raises(ProjectPRConflict, match=r"durable candidate|different content"):
        await workflows.submit(replace(command, evidence_digest="4" * 64))


@pytest.mark.asyncio
async def test_sealed_evidence_event_hash_tampering_is_rejected() -> None:
    workflows, store, _ = _workflows()
    started = await workflows.submit(_start_command())
    await workflows.submit(_seal_command(started.workflow_id))
    events = await store.read(started.workflow_id)
    sealed = next(
        event for event in events if event.kind is ProjectPREventKind.REVISION_SEALED
    )
    tampered = replace(sealed, event_hash="0" * 64)

    with pytest.raises(ProjectPRConflict, match="hash"):
        verify_project_pr_events((events[0], tampered))


@pytest.mark.asyncio
async def test_confirmed_publish_retry_never_creates_another_remote_check() -> None:
    workflows, _, forge = _workflows()
    started = await workflows.submit(_start_command())
    await workflows.submit(_seal_command(started.workflow_id))
    command = PublishPR(
        workflow_id=started.workflow_id,
        expected_revision=1,
        operation_id="review:publish:r1",
    )
    first = await workflows.submit(command)
    confirmed = await workflows.load(started.workflow_id)

    retried = await workflows.submit(command)
    after_retry = await workflows.load(started.workflow_id)

    assert first.created is True
    assert retried.created is False
    assert confirmed.state is ProjectPRState.CONFIRMED
    assert after_retry == confirmed
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 1
