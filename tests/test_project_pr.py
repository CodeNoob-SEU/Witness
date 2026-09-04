from __future__ import annotations

import asyncio
import json

import pytest

from react_agent.project_pr import (
    InMemoryProjectPRStore,
    MockForge,
    ProjectPREventKind,
    ProjectPRHeadMoved,
    ProjectPRIntegrityError,
    ProjectPRLeaseLost,
    ProjectPRState,
    ProjectPullRequests,
    PublishPR,
    ResumePR,
    SealRevision,
    StartPR,
)
from react_agent.project_pr_evidence import generate_project_pr_evidence


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.mark.asyncio
async def test_start_pr_anchors_one_idempotent_workflow() -> None:
    workflows = ProjectPullRequests(
        store=InMemoryProjectPRStore(),
        forge=MockForge(),
        worker_id="worker-a",
    )
    command = StartPR(
        project_key="interview-demo",
        repository="acme/pricing-service",
        pull_request_number=42,
        base_sha="a" * 40,
        head_sha="b" * 40,
        goal="Correct the disabled-research pricing path",
        idempotency_key="demo-pr-42",
    )

    created = await workflows.submit(command)
    retried = await workflows.submit(command)
    snapshot = await workflows.load(created.workflow_id)

    assert created.created is True
    assert retried.workflow_id == created.workflow_id
    assert retried.created is False
    assert snapshot.state is ProjectPRState.ANCHORED
    assert snapshot.project_key == "interview-demo"
    assert snapshot.repository == "acme/pricing-service"
    assert snapshot.pull_request_number == 42
    assert snapshot.base_sha == "a" * 40
    assert snapshot.head_sha == "b" * 40
    assert snapshot.revision == 1
    assert snapshot.last_sequence == 1


@pytest.mark.asyncio
async def test_seal_revision_requires_one_complete_reviewable_candidate() -> None:
    workflows = ProjectPullRequests(
        store=InMemoryProjectPRStore(),
        forge=MockForge(),
        worker_id="worker-a",
    )
    started = await workflows.submit(
        StartPR(
            project_key="interview-demo",
            repository="acme/pricing-service",
            pull_request_number=42,
            base_sha="a" * 40,
            head_sha="b" * 40,
            goal="Correct the disabled-research pricing path",
            idempotency_key="demo-pr-42",
        )
    )

    await workflows.submit(
        SealRevision(
            workflow_id=started.workflow_id,
            expected_revision=1,
            observed_head_sha="b" * 40,
            candidate_tree="c" * 40,
            patch_digest="1" * 64,
            verification_digest="2" * 64,
            evidence_digest="3" * 64,
        )
    )
    snapshot = await workflows.load(started.workflow_id)

    assert snapshot.state is ProjectPRState.AWAITING_PUBLISH_APPROVAL
    assert snapshot.candidate_tree == "c" * 40
    assert snapshot.patch_digest == "1" * 64
    assert snapshot.verification_digest == "2" * 64
    assert snapshot.evidence_digest == "3" * 64
    assert snapshot.last_sequence == 2


async def _start_and_seal(workflows: ProjectPullRequests) -> str:
    started = await workflows.submit(
        StartPR(
            project_key="interview-demo",
            repository="acme/pricing-service",
            pull_request_number=42,
            base_sha="a" * 40,
            head_sha="b" * 40,
            goal="Correct the disabled-research pricing path",
            idempotency_key="demo-pr-42",
        )
    )
    await workflows.submit(
        SealRevision(
            workflow_id=started.workflow_id,
            expected_revision=1,
            observed_head_sha="b" * 40,
            candidate_tree="c" * 40,
            patch_digest="1" * 64,
            verification_digest="2" * 64,
            evidence_digest="3" * 64,
        )
    )
    return started.workflow_id


@pytest.mark.asyncio
async def test_publish_durably_records_intent_before_creating_one_check() -> None:
    store = InMemoryProjectPRStore()
    forge = MockForge(app_id=73, pause_before_create=True)
    workflows = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
    )
    workflow_id = await _start_and_seal(workflows)

    publishing = asyncio.create_task(
        workflows.submit(PublishPR(workflow_id=workflow_id, expected_revision=1))
    )
    await asyncio.wait_for(forge.create_entered.wait(), timeout=1)
    in_flight = await workflows.load(workflow_id)

    assert in_flight.state is ProjectPRState.PUBLISHING
    assert in_flight.publish_effect_id == f"{workflow_id}:r1:check"
    assert in_flight.outbound_create_attempts == 1
    assert in_flight.publisher_fence == 1
    assert in_flight.last_sequence == 4
    assert forge.physical_check_run_count == 0

    forge.release_create_response()
    await publishing
    confirmed = await workflows.load(workflow_id)

    assert confirmed.state is ProjectPRState.CONFIRMED
    assert confirmed.check_run_id == "check-1"
    assert confirmed.last_sequence == 5
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 1


@pytest.mark.asyncio
async def test_follow_replays_ordered_durable_workflow_facts() -> None:
    workflows = ProjectPullRequests(
        store=InMemoryProjectPRStore(),
        forge=MockForge(app_id=73),
        worker_id="publisher-a",
    )
    workflow_id = await _start_and_seal(workflows)
    await workflows.submit(PublishPR(workflow_id=workflow_id, expected_revision=1))

    events = [event async for event in workflows.follow(workflow_id)]
    tail = [
        event
        async for event in workflows.follow(workflow_id, after_sequence=3)
    ]

    assert [event.sequence for event in events] == [1, 2, 3, 4, 5]
    assert [event.kind for event in events] == [
        ProjectPREventKind.WORKFLOW_STARTED,
        ProjectPREventKind.REVISION_SEALED,
        ProjectPREventKind.PUBLISH_ENQUEUED,
        ProjectPREventKind.PUBLISH_STARTED,
        ProjectPREventKind.PUBLISH_CONFIRMED,
    ]
    assert [event.sequence for event in tail] == [4, 5]


@pytest.mark.asyncio
async def test_project_pr_evidence_is_byte_stable_and_source_bound() -> None:
    workflows = ProjectPullRequests(
        store=InMemoryProjectPRStore(),
        forge=MockForge(app_id=73),
        worker_id="publisher-a",
    )
    workflow_id = await _start_and_seal(workflows)
    await workflows.submit(PublishPR(workflow_id=workflow_id, expected_revision=1))
    events = tuple([event async for event in workflows.follow(workflow_id)])

    first = generate_project_pr_evidence(events)
    second = generate_project_pr_evidence(events)
    manifest = json.loads(first.json_text)

    assert second.json_text == first.json_text
    assert second.markdown_text == first.markdown_text
    assert second.digest == first.digest
    assert manifest["subject"] == {
        "base_sha": "a" * 40,
        "candidate_tree": "c" * 40,
        "head_sha": "b" * 40,
        "pull_request_number": 42,
        "repository": "acme/pricing-service",
    }
    assert manifest["publication"]["state"] == "confirmed"
    assert manifest["publication"]["outbound_create_attempts"] == 1
    assert manifest["journal"]["event_count"] == 5
    assert manifest["journal"]["head_hash"] == events[-1].event_hash
    assert "does not authenticate origin" in first.markdown_text


@pytest.mark.asyncio
async def test_resume_fails_closed_when_remote_match_is_ambiguous() -> None:
    clock = FakeClock()
    store = InMemoryProjectPRStore(clock=clock)
    forge = MockForge(app_id=73, pause_after_create=True)
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    workflow_id = await _start_and_seal(first_publisher)

    publishing = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_committed.wait(), timeout=1)
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    forge.duplicate_last_check()

    clock.advance(6)
    recovery_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
    )
    with pytest.raises(ProjectPRIntegrityError, match="2 exact check matches"):
        await recovery_publisher.submit(ResumePR(workflow_id=workflow_id))
    failed = await recovery_publisher.load(workflow_id)

    assert failed.state is ProjectPRState.INTEGRITY_FAILED
    assert failed.observed_match_count == 2
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 2


@pytest.mark.asyncio
async def test_resume_safely_stops_when_remote_effect_cannot_be_found() -> None:
    clock = FakeClock()
    store = InMemoryProjectPRStore(clock=clock)
    forge = MockForge(app_id=73, pause_before_create=True)
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    workflow_id = await _start_and_seal(first_publisher)

    publishing = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_entered.wait(), timeout=1)
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert forge.physical_check_run_count == 0

    clock.advance(6)
    recovery_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
        reconciliation_poll_attempts=2,
        reconciliation_poll_interval_s=0,
    )
    await recovery_publisher.submit(ResumePR(workflow_id=workflow_id))
    stopped = await recovery_publisher.load(workflow_id)

    assert stopped.state is ProjectPRState.ACTION_REQUIRED
    assert stopped.observed_match_count == 0
    assert stopped.reconciliation_polls == 2
    assert forge.observe_call_count == 2
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 0


@pytest.mark.asyncio
async def test_resume_adopts_accepted_check_without_another_create() -> None:
    clock = FakeClock()
    store = InMemoryProjectPRStore(clock=clock)
    forge = MockForge(app_id=73, pause_after_create=True)
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    workflow_id = await _start_and_seal(first_publisher)

    publishing = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_committed.wait(), timeout=1)
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert (await first_publisher.load(workflow_id)).state is ProjectPRState.PUBLISHING

    clock.advance(6)
    recovery_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
    )
    await recovery_publisher.submit(ResumePR(workflow_id=workflow_id))
    recovered = await recovery_publisher.load(workflow_id)

    assert recovered.state is ProjectPRState.CONFIRMED
    assert recovered.check_run_id == "check-1"
    assert recovered.remote_check_adopted is True
    assert recovered.observed_match_count == 1
    assert recovered.publisher_fence == 2
    assert recovered.last_sequence == 6
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 1


@pytest.mark.asyncio
async def test_new_publisher_fences_late_confirmation_from_old_worker() -> None:
    clock = FakeClock()
    store = InMemoryProjectPRStore(clock=clock)
    forge = MockForge(app_id=73, pause_after_create=True)
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    workflow_id = await _start_and_seal(first_publisher)

    late_publisher = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_committed.wait(), timeout=1)

    clock.advance(6)
    takeover = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
    )
    await takeover.submit(ResumePR(workflow_id=workflow_id))
    forge.release_create_response()
    with pytest.raises(ProjectPRLeaseLost):
        await late_publisher
    confirmed = await takeover.load(workflow_id)

    assert confirmed.state is ProjectPRState.CONFIRMED
    assert confirmed.publisher_fence == 2
    assert confirmed.check_run_id == "check-1"
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 1


@pytest.mark.asyncio
async def test_publish_rejects_a_stale_pull_request_head_before_remote_write() -> None:
    forge = MockForge(app_id=73, head_sha="d" * 40)
    workflows = ProjectPullRequests(
        store=InMemoryProjectPRStore(),
        forge=forge,
        worker_id="publisher-a",
    )
    workflow_id = await _start_and_seal(workflows)

    with pytest.raises(ProjectPRHeadMoved, match="head changed"):
        await workflows.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    snapshot = await workflows.load(workflow_id)

    assert snapshot.state is ProjectPRState.AWAITING_PUBLISH_APPROVAL
    assert forge.outbound_create_post_count == 0
    assert forge.physical_check_run_count == 0


@pytest.mark.asyncio
async def test_resume_does_not_adopt_an_old_head_check_after_pr_synchronize() -> None:
    clock = FakeClock()
    store = InMemoryProjectPRStore(clock=clock)
    forge = MockForge(app_id=73, pause_after_create=True)
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    workflow_id = await _start_and_seal(first_publisher)
    publishing = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_committed.wait(), timeout=1)
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing

    forge.set_head("d" * 40)
    clock.advance(6)
    recovery_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
    )
    with pytest.raises(ProjectPRHeadMoved, match="head changed"):
        await recovery_publisher.submit(ResumePR(workflow_id=workflow_id))

    assert (await recovery_publisher.load(workflow_id)).state is ProjectPRState.PUBLISHING
    assert forge.outbound_create_post_count == 1
    assert forge.physical_check_run_count == 1
