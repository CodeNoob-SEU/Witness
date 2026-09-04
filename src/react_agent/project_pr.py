"""Crash-consistent project-level pull-request workflow orchestration.

``ProjectPullRequests`` is the external Module above :class:`AgentRuntime`.
Callers submit reviewer-level commands; the Implementation owns revision gates,
append-only facts, publication ordering, and remote reconciliation. Code-host
semantics deliberately do not leak into the Runtime event model.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias, runtime_checkable

from .events import GENESIS_HASH, canonical_json

_WORKFLOW_ID_NAMESPACE = uuid.UUID("7870b508-3a4d-4f99-aaef-218bda9acd13")
_EVENT_ID_NAMESPACE = uuid.UUID("250ea095-416a-4394-80ee-f2177c87e087")
_EVENT_HASH_DOMAIN = b"react-agent-project-pr-event:v1\0"
_TERMINAL_PR_STATES = frozenset(
    {
        "action_required",
        "integrity_failed",
        "confirmed",
    }
)
_TERMINAL_PR_EVENT_KINDS = frozenset(
    {
        "publish_action_required",
        "publish_integrity_failed",
        "publish_confirmed",
    }
)


class ProjectPRError(RuntimeError):
    """Base error raised by the project PR Module."""


class ProjectPRNotFound(ProjectPRError):
    """The requested workflow is not retained by the configured store."""


class ProjectPRConflict(ProjectPRError):
    """A command conflicts with durable workflow identity or state."""


class ProjectPRLeaseConflict(ProjectPRConflict):
    """A different non-expired publisher owns the workflow lease."""


class ProjectPRLeaseLost(ProjectPRConflict):
    """A stale or expired publisher fence attempted a durable write."""


class ProjectPRIntegrityError(ProjectPRConflict):
    """Remote state cannot be reconciled to one unambiguous effect."""


class ProjectPRHeadMoved(ProjectPRConflict):
    """The code-host PR head no longer matches the sealed revision."""


class ProjectPRState(StrEnum):
    """Reviewer-facing workflow state."""

    ANCHORED = "anchored"
    AWAITING_PUBLISH_APPROVAL = "awaiting_publish_approval"
    PUBLISHING = "publishing"
    UNKNOWN = "unknown"
    ACTION_REQUIRED = "action_required"
    INTEGRITY_FAILED = "integrity_failed"
    CONFIRMED = "confirmed"


class ProjectPREventKind(StrEnum):
    """Append-only facts owned by the project PR Module."""

    WORKFLOW_STARTED = "workflow_started"
    REVISION_SEALED = "revision_sealed"
    PUBLISH_ENQUEUED = "publish_enqueued"
    PUBLISH_STARTED = "publish_started"
    PUBLISH_UNKNOWN = "publish_unknown"
    PUBLISH_ACTION_REQUIRED = "publish_action_required"
    PUBLISH_INTEGRITY_FAILED = "publish_integrity_failed"
    PUBLISH_CONFIRMED = "publish_confirmed"


class OutboxState(StrEnum):
    """Durable lifecycle for one externally visible publication effect."""

    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    UNKNOWN = "unknown"
    ACTION_REQUIRED = "action_required"
    FAILED = "failed"
    CONFIRMED = "confirmed"


@dataclass(frozen=True, slots=True, kw_only=True)
class StartPR:
    """Anchor one workflow to an immutable observed pull-request revision."""

    project_key: str
    repository: str
    pull_request_number: int
    base_sha: str
    head_sha: str
    goal: str
    idempotency_key: str


@dataclass(frozen=True, slots=True, kw_only=True)
class SealRevision:
    """Ingest one trusted candidate whose change, tests, and evidence share a tree.

    P0 execution workers call this command only after independently binding the
    three supplied digests to ``candidate_tree``. A later Execution Adapter can
    hide that ingestion step from end users without changing this Module's seam.
    """

    workflow_id: str
    expected_revision: int
    observed_head_sha: str
    candidate_tree: str
    patch_digest: str
    verification_digest: str
    evidence_digest: str
    operation_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishPR:
    """Approve and publish the sealed revision's Runtime Integrity check."""

    workflow_id: str
    expected_revision: int
    operation_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ResumePR:
    """Resume an interrupted publication by observing before any new write."""

    workflow_id: str


PRCommand: TypeAlias = StartPR | SealRevision | PublishPR | ResumePR


@dataclass(frozen=True, slots=True)
class ProjectPRHandle:
    workflow_id: str
    revision: int
    created: bool


@dataclass(frozen=True, slots=True)
class ProjectPRSnapshot:
    workflow_id: str
    state: ProjectPRState
    project_key: str
    repository: str
    pull_request_number: int
    base_sha: str
    head_sha: str
    goal: str
    revision: int
    last_sequence: int
    last_hash: str
    candidate_tree: str | None = None
    patch_digest: str | None = None
    verification_digest: str | None = None
    evidence_digest: str | None = None
    publish_effect_id: str | None = None
    publisher_fence: int | None = None
    outbound_create_attempts: int = 0
    check_run_id: str | None = None
    remote_check_adopted: bool = False
    observed_match_count: int | None = None
    reconciliation_polls: int = 0


@dataclass(frozen=True, slots=True)
class ProjectPREvent:
    """One immutable project-level PR fact."""

    workflow_id: str
    sequence: int
    operation_id: str
    event_id: str
    kind: ProjectPREventKind
    occurred_at: float
    previous_hash: str
    event_hash: str
    data: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True)
class _ProjectPREventDraft:
    kind: ProjectPREventKind
    data: Mapping[str, Any]

    def payload_hash(self) -> str:
        return hashlib.sha256(
            canonical_json({"kind": self.kind.value, "data": self.data}).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class PublisherLease:
    workflow_id: str
    owner: str
    fence: int
    expires_at: float


@dataclass(frozen=True, slots=True)
class ForgeDesiredCheck:
    repository: str
    pull_request_number: int
    head_sha: str
    app_id: int
    name: str
    external_id: str
    summary: str


@dataclass(frozen=True, slots=True)
class ForgeCheckLocator:
    repository: str
    pull_request_number: int
    head_sha: str
    app_id: int
    name: str
    external_id: str


@dataclass(frozen=True, slots=True)
class ForgeCheck:
    check_run_id: str
    locator: ForgeCheckLocator


@dataclass(frozen=True, slots=True)
class OutboxEffect:
    workflow_id: str
    effect_id: str
    revision: int
    state: OutboxState
    desired: ForgeDesiredCheck
    attempts: int = 0
    check_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class _OutboxMutation:
    effect_id: str
    target_state: OutboxState
    desired: ForgeDesiredCheck | None = None
    revision: int | None = None
    expected_state: OutboxState | None = None
    increment_attempts: bool = False
    check_run_id: str | None = None


@runtime_checkable
class ForgePort(Protocol):
    """True external code-host seam; Mock and GitHub Adapters satisfy it."""

    @property
    def app_id(self) -> int: ...

    async def observe_head(self, repository: str, pull_request_number: int) -> str: ...

    async def converge(
        self, desired: ForgeDesiredCheck, *, effect_id: str
    ) -> ForgeCheck: ...

    async def observe(self, locator: ForgeCheckLocator) -> tuple[ForgeCheck, ...]: ...


@runtime_checkable
class ProjectPRStore(Protocol):
    """Persistence seam; it owns atomicity, not workflow transition rules."""

    async def reserve_start(
        self,
        *,
        request_key: tuple[str, str],
        request_hash: str,
        proposed_workflow_id: str,
        draft: _ProjectPREventDraft,
        operation_id: str,
    ) -> tuple[ProjectPREvent, bool]: ...

    async def append(
        self,
        workflow_id: str,
        draft: _ProjectPREventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: PublisherLease | None = None,
        outbox: _OutboxMutation | None = None,
    ) -> tuple[ProjectPREvent, bool]: ...

    async def read(
        self, workflow_id: str, *, after_sequence: int = 0
    ) -> tuple[ProjectPREvent, ...]: ...

    async def load(self, workflow_id: str) -> ProjectPRSnapshot: ...

    async def load_effect(self, workflow_id: str, effect_id: str) -> OutboxEffect: ...

    async def wait(
        self,
        workflow_id: str,
        *,
        after_sequence: int,
        timeout_s: float | None = None,
    ) -> tuple[ProjectPREvent, ...]: ...

    async def acquire(
        self, workflow_id: str, *, owner: str, ttl_s: float
    ) -> PublisherLease: ...

    async def release(self, lease: PublisherLease) -> None: ...


@dataclass(frozen=True, slots=True)
class _CommittedOperation:
    payload_hash: str
    event: ProjectPREvent


@dataclass(slots=True)
class _WorkflowLog:
    events: list[ProjectPREvent]
    operations: dict[str, _CommittedOperation]
    effects: dict[str, OutboxEffect] = field(default_factory=dict)
    fence_counter: int = 0
    lease_owner: str | None = None
    lease_fence: int | None = None
    lease_expires_at: float = 0.0


class InMemoryProjectPRStore:
    """Concurrent reference Adapter with CAS, operation idempotency, and fencing."""

    def __init__(
        self,
        *,
        clock: Any = time.monotonic,
        wall_clock: Any = time.time,
    ) -> None:
        self._clock = clock
        self._wall_clock = wall_clock
        self._condition = asyncio.Condition()
        self._workflows: dict[str, _WorkflowLog] = {}
        self._requests: dict[tuple[str, str], tuple[str, str]] = {}

    async def reserve_start(
        self,
        *,
        request_key: tuple[str, str],
        request_hash: str,
        proposed_workflow_id: str,
        draft: _ProjectPREventDraft,
        operation_id: str,
    ) -> tuple[ProjectPREvent, bool]:
        payload_hash = draft.payload_hash()
        async with self._condition:
            previous = self._requests.get(request_key)
            if previous is not None:
                previous_hash, workflow_id = previous
                if previous_hash != request_hash:
                    raise ProjectPRConflict(
                        "idempotency key was reused with different PR workflow content"
                    )
                return self._workflows[workflow_id].events[0], False
            if proposed_workflow_id in self._workflows:
                raise ProjectPRConflict(
                    f"proposed workflow already exists: {proposed_workflow_id}"
                )
            event = _stored_event(
                draft,
                workflow_id=proposed_workflow_id,
                sequence=1,
                operation_id=operation_id,
                previous_hash=GENESIS_HASH,
                occurred_at=self._wall_clock(),
            )
            self._workflows[proposed_workflow_id] = _WorkflowLog(
                events=[event],
                operations={operation_id: _CommittedOperation(payload_hash, event)},
            )
            self._requests[request_key] = (request_hash, proposed_workflow_id)
            self._condition.notify_all()
            return event, True

    async def append(
        self,
        workflow_id: str,
        draft: _ProjectPREventDraft,
        *,
        expected_sequence: int,
        operation_id: str,
        lease: PublisherLease | None = None,
        outbox: _OutboxMutation | None = None,
    ) -> tuple[ProjectPREvent, bool]:
        payload_hash = _commit_payload_hash(draft, outbox)
        async with self._condition:
            log = self._log(workflow_id)
            committed = log.operations.get(operation_id)
            if committed is not None:
                if committed.payload_hash != payload_hash:
                    raise ProjectPRConflict(
                        f"operation id {operation_id!r} was reused with different content"
                    )
                return committed.event, False
            self._assert_lease(log, workflow_id, lease)
            actual_sequence = log.events[-1].sequence
            if actual_sequence != expected_sequence:
                raise ProjectPRConflict(
                    f"expected sequence {expected_sequence}, got {actual_sequence}"
                )
            updated_effect = self._apply_outbox(log, workflow_id, outbox)
            event = _stored_event(
                draft,
                workflow_id=workflow_id,
                sequence=actual_sequence + 1,
                operation_id=operation_id,
                previous_hash=log.events[-1].event_hash,
                occurred_at=self._wall_clock(),
            )
            log.events.append(event)
            log.operations[operation_id] = _CommittedOperation(payload_hash, event)
            if updated_effect is not None:
                log.effects[updated_effect.effect_id] = updated_effect
            self._condition.notify_all()
            return event, True

    async def read(
        self, workflow_id: str, *, after_sequence: int = 0
    ) -> tuple[ProjectPREvent, ...]:
        async with self._condition:
            return tuple(
                event
                for event in self._log(workflow_id).events
                if event.sequence > after_sequence
            )

    async def load(self, workflow_id: str) -> ProjectPRSnapshot:
        return _fold_events(await self.read(workflow_id))

    async def load_effect(self, workflow_id: str, effect_id: str) -> OutboxEffect:
        async with self._condition:
            log = self._log(workflow_id)
            try:
                return log.effects[effect_id]
            except KeyError:
                raise ProjectPRNotFound(f"outbox effect not found: {effect_id}") from None

    async def wait(
        self,
        workflow_id: str,
        *,
        after_sequence: int,
        timeout_s: float | None = None,
    ) -> tuple[ProjectPREvent, ...]:
        async with self._condition:
            def available() -> tuple[ProjectPREvent, ...]:
                return tuple(
                    event
                    for event in self._log(workflow_id).events
                    if event.sequence > after_sequence
                )

            events = available()
            if events:
                return events
            try:
                if timeout_s is None:
                    while not (events := available()):
                        await self._condition.wait()
                else:
                    async with asyncio.timeout(timeout_s):
                        while not (events := available()):
                            await self._condition.wait()
            except TimeoutError:
                return ()
            return events

    async def acquire(
        self, workflow_id: str, *, owner: str, ttl_s: float
    ) -> PublisherLease:
        if not owner.strip():
            raise ValueError("lease owner must not be blank")
        if ttl_s <= 0:
            raise ValueError("lease ttl_s must be positive")
        async with self._condition:
            log = self._log(workflow_id)
            now = self._clock()
            if log.lease_owner is not None and log.lease_expires_at > now:
                raise ProjectPRLeaseConflict("another publisher owns the workflow lease")
            log.fence_counter += 1
            log.lease_owner = owner
            log.lease_fence = log.fence_counter
            log.lease_expires_at = now + ttl_s
            return PublisherLease(
                workflow_id=workflow_id,
                owner=owner,
                fence=log.fence_counter,
                expires_at=log.lease_expires_at,
            )

    async def release(self, lease: PublisherLease) -> None:
        async with self._condition:
            log = self._log(lease.workflow_id)
            if (
                log.lease_owner == lease.owner
                and log.lease_fence == lease.fence
                and log.lease_expires_at == lease.expires_at
            ):
                log.lease_owner = None
                log.lease_fence = None
                log.lease_expires_at = 0.0

    def _log(self, workflow_id: str) -> _WorkflowLog:
        try:
            return self._workflows[workflow_id]
        except KeyError:
            raise ProjectPRNotFound(f"workflow not found: {workflow_id}") from None

    def _assert_lease(
        self,
        log: _WorkflowLog,
        workflow_id: str,
        lease: PublisherLease | None,
    ) -> None:
        if lease is None:
            if log.lease_owner is not None and log.lease_expires_at > self._clock():
                raise ProjectPRLeaseLost("publisher lease is required for this write")
            return
        if (
            lease.workflow_id != workflow_id
            or lease.owner != log.lease_owner
            or lease.fence != log.lease_fence
            or lease.expires_at != log.lease_expires_at
            or log.lease_expires_at <= self._clock()
        ):
            raise ProjectPRLeaseLost("publisher lease is missing, expired, or stale")

    @staticmethod
    def _apply_outbox(
        log: _WorkflowLog,
        workflow_id: str,
        mutation: _OutboxMutation | None,
    ) -> OutboxEffect | None:
        if mutation is None:
            return None
        current = log.effects.get(mutation.effect_id)
        if mutation.expected_state is None:
            if current is not None:
                raise ProjectPRConflict(f"outbox effect already exists: {mutation.effect_id}")
            if mutation.desired is None or mutation.revision is None:
                raise ProjectPRConflict("new outbox effect requires desired state and revision")
            return OutboxEffect(
                workflow_id=workflow_id,
                effect_id=mutation.effect_id,
                revision=mutation.revision,
                state=mutation.target_state,
                desired=mutation.desired,
                attempts=1 if mutation.increment_attempts else 0,
                check_run_id=mutation.check_run_id,
            )
        if current is None:
            raise ProjectPRNotFound(f"outbox effect not found: {mutation.effect_id}")
        if current.state is not mutation.expected_state:
            raise ProjectPRConflict(
                f"expected outbox state {mutation.expected_state.value}, "
                f"got {current.state.value}"
            )
        return replace(
            current,
            state=mutation.target_state,
            attempts=current.attempts + (1 if mutation.increment_attempts else 0),
            check_run_id=mutation.check_run_id or current.check_run_id,
        )


class MockForge:
    """Controllable code-host Adapter that deliberately performs no deduplication."""

    def __init__(
        self,
        *,
        app_id: int = 1,
        head_sha: str = "b" * 40,
        pause_before_create: bool = False,
        pause_after_create: bool = False,
    ) -> None:
        if app_id < 1:
            raise ValueError("app_id must be positive")
        if pause_before_create and pause_after_create:
            raise ValueError("only one create pause may be enabled")
        _validate_git_id("head_sha", head_sha)
        self._app_id = app_id
        self._head_sha = head_sha
        self._pause_before_create = pause_before_create
        self._pause_after_create = pause_after_create
        self._checks: list[ForgeCheck] = []
        self._outbound_create_post_count = 0
        self._observe_call_count = 0
        self.create_entered = asyncio.Event()
        self.create_committed = asyncio.Event()
        self._create_response_released = asyncio.Event()
        if not pause_before_create and not pause_after_create:
            self._create_response_released.set()

    @property
    def app_id(self) -> int:
        return self._app_id

    @property
    def outbound_create_post_count(self) -> int:
        return self._outbound_create_post_count

    @property
    def physical_check_run_count(self) -> int:
        return len(self._checks)

    @property
    def observe_call_count(self) -> int:
        return self._observe_call_count

    def release_create_response(self) -> None:
        self._create_response_released.set()

    def set_head(self, head_sha: str) -> None:
        _validate_git_id("head_sha", head_sha)
        self._head_sha = head_sha

    def duplicate_last_check(self) -> ForgeCheck:
        """Simulate a conflicting writer without invoking this Adapter's POST path."""

        if not self._checks:
            raise ProjectPRNotFound("mock forge has no check to duplicate")
        duplicate = ForgeCheck(
            check_run_id=f"check-{len(self._checks) + 1}",
            locator=self._checks[-1].locator,
        )
        self._checks.append(duplicate)
        return duplicate

    async def converge(
        self, desired: ForgeDesiredCheck, *, effect_id: str
    ) -> ForgeCheck:
        del effect_id  # The mock intentionally does not treat it as an idempotency key.
        self._outbound_create_post_count += 1
        self.create_entered.set()
        if self._pause_before_create:
            await self._create_response_released.wait()
        locator = ForgeCheckLocator(
            repository=desired.repository,
            pull_request_number=desired.pull_request_number,
            head_sha=desired.head_sha,
            app_id=desired.app_id,
            name=desired.name,
            external_id=desired.external_id,
        )
        check = ForgeCheck(
            check_run_id=f"check-{len(self._checks) + 1}",
            locator=locator,
        )
        self._checks.append(check)
        self.create_committed.set()
        if self._pause_after_create:
            await self._create_response_released.wait()
        return check

    async def observe_head(self, repository: str, pull_request_number: int) -> str:
        del repository, pull_request_number
        return self._head_sha

    async def observe(self, locator: ForgeCheckLocator) -> tuple[ForgeCheck, ...]:
        self._observe_call_count += 1
        return tuple(check for check in self._checks if check.locator == locator)


class ProjectPullRequests:
    """Deep Module coordinating project-level PR workflow facts and effects."""

    def __init__(
        self,
        *,
        store: ProjectPRStore,
        forge: ForgePort,
        worker_id: str,
        check_name: str = "Witness / Runtime Integrity",
        publisher_lease_ttl_s: float = 30.0,
        reconciliation_poll_attempts: int = 3,
        reconciliation_poll_interval_s: float = 0.05,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if not check_name.strip():
            raise ValueError("check_name must not be blank")
        if publisher_lease_ttl_s <= 0:
            raise ValueError("publisher_lease_ttl_s must be positive")
        if (
            isinstance(reconciliation_poll_attempts, bool)
            or reconciliation_poll_attempts < 1
        ):
            raise ValueError("reconciliation_poll_attempts must be positive")
        if reconciliation_poll_interval_s < 0:
            raise ValueError("reconciliation_poll_interval_s must not be negative")
        self._store = store
        self._forge = forge
        self._worker_id = worker_id
        self._check_name = check_name
        self._publisher_lease_ttl_s = publisher_lease_ttl_s
        self._reconciliation_poll_attempts = reconciliation_poll_attempts
        self._reconciliation_poll_interval_s = reconciliation_poll_interval_s

    async def submit(self, command: PRCommand) -> ProjectPRHandle:
        if isinstance(command, StartPR):
            return await self._submit_start(command)
        if isinstance(command, SealRevision):
            return await self._submit_seal(command)
        if isinstance(command, PublishPR):
            return await self._submit_publish(command)
        if isinstance(command, ResumePR):
            return await self._submit_resume(command)
        raise TypeError(f"unsupported PR command: {type(command).__name__}")

    async def load(self, workflow_id: str) -> ProjectPRSnapshot:
        return await self._store.load(workflow_id)

    async def follow(
        self, workflow_id: str, *, after_sequence: int = 0
    ) -> AsyncIterator[ProjectPREvent]:
        if after_sequence < 0:
            raise ValueError("after_sequence must not be negative")
        cursor = after_sequence
        while True:
            events = await self._store.read(workflow_id, after_sequence=cursor)
            if not events:
                snapshot = await self.load(workflow_id)
                if (
                    snapshot.state.value in _TERMINAL_PR_STATES
                    and cursor >= snapshot.last_sequence
                ):
                    return
                events = await self._store.wait(
                    workflow_id,
                    after_sequence=cursor,
                )
            for event in events:
                cursor = event.sequence
                yield event
                if event.kind.value in _TERMINAL_PR_EVENT_KINDS:
                    return

    async def _submit_start(self, command: StartPR) -> ProjectPRHandle:
        _validate_start(command)
        request_hash = _start_hash(command)
        workflow_id = str(
            uuid.uuid5(
                _WORKFLOW_ID_NAMESPACE,
                f"{command.project_key}\0{command.idempotency_key}",
            )
        )
        draft = _ProjectPREventDraft(
            kind=ProjectPREventKind.WORKFLOW_STARTED,
            data={
                "project_key": command.project_key,
                "repository": command.repository,
                "pull_request_number": command.pull_request_number,
                "base_sha": command.base_sha,
                "head_sha": command.head_sha,
                "goal": command.goal,
                "revision": 1,
            },
        )
        _, created = await self._store.reserve_start(
            request_key=(command.project_key, command.idempotency_key),
            request_hash=request_hash,
            proposed_workflow_id=workflow_id,
            draft=draft,
            operation_id="workflow:start",
        )
        return ProjectPRHandle(workflow_id, 1, created)

    async def _submit_seal(self, command: SealRevision) -> ProjectPRHandle:
        _validate_seal(command)
        snapshot = await self.load(command.workflow_id)
        durable_candidate = (
            snapshot.candidate_tree,
            snapshot.patch_digest,
            snapshot.verification_digest,
            snapshot.evidence_digest,
        )
        proposed_candidate = (
            command.candidate_tree,
            command.patch_digest,
            command.verification_digest,
            command.evidence_digest,
        )
        if snapshot.state is ProjectPRState.AWAITING_PUBLISH_APPROVAL:
            if (
                durable_candidate == proposed_candidate
                and snapshot.head_sha == command.observed_head_sha
            ):
                return ProjectPRHandle(snapshot.workflow_id, snapshot.revision, False)
            raise ProjectPRConflict("sealed revision differs from the durable candidate")
        if snapshot.state is not ProjectPRState.ANCHORED:
            raise ProjectPRConflict("revision can only be sealed from anchored state")
        if snapshot.revision != command.expected_revision:
            raise ProjectPRConflict(
                f"expected revision {command.expected_revision}, got {snapshot.revision}"
            )
        if snapshot.head_sha != command.observed_head_sha:
            raise ProjectPRConflict("pull-request head changed before revision seal")
        draft = _ProjectPREventDraft(
            kind=ProjectPREventKind.REVISION_SEALED,
            data={
                "revision": command.expected_revision,
                "observed_head_sha": command.observed_head_sha,
                "candidate_tree": command.candidate_tree,
                "patch_digest": command.patch_digest,
                "verification_digest": command.verification_digest,
                "evidence_digest": command.evidence_digest,
            },
        )
        _, created = await self._store.append(
            command.workflow_id,
            draft,
            expected_sequence=snapshot.last_sequence,
            operation_id=command.operation_id
            or f"revision:{command.expected_revision}:sealed",
        )
        updated = await self.load(command.workflow_id)
        return ProjectPRHandle(updated.workflow_id, updated.revision, created)

    async def _submit_publish(self, command: PublishPR) -> ProjectPRHandle:
        _validate_publish(command)
        snapshot = await self.load(command.workflow_id)
        if snapshot.revision != command.expected_revision:
            raise ProjectPRConflict(
                f"expected revision {command.expected_revision}, got {snapshot.revision}"
            )
        if snapshot.state is ProjectPRState.CONFIRMED:
            return ProjectPRHandle(snapshot.workflow_id, snapshot.revision, False)
        if snapshot.state is not ProjectPRState.AWAITING_PUBLISH_APPROVAL:
            raise ProjectPRConflict("only a sealed revision may be published")
        if (
            snapshot.candidate_tree is None
            or snapshot.verification_digest is None
            or snapshot.evidence_digest is None
        ):
            raise ProjectPRConflict("sealed revision is missing required publication evidence")
        remote_head = await self._forge.observe_head(
            snapshot.repository, snapshot.pull_request_number
        )
        if remote_head != snapshot.head_sha:
            raise ProjectPRHeadMoved(
                f"pull-request head changed from {snapshot.head_sha} to {remote_head}"
            )

        effect_id = f"{snapshot.workflow_id}:r{snapshot.revision}:check"
        desired = ForgeDesiredCheck(
            repository=snapshot.repository,
            pull_request_number=snapshot.pull_request_number,
            head_sha=snapshot.head_sha,
            app_id=self._forge.app_id,
            name=self._check_name,
            external_id=effect_id,
            summary=(
                f"candidate_tree={snapshot.candidate_tree}\n"
                f"verification_sha256={snapshot.verification_digest}\n"
                f"evidence_sha256={snapshot.evidence_digest}"
            ),
        )
        lease = await self._store.acquire(
            snapshot.workflow_id,
            owner=self._worker_id,
            ttl_s=self._publisher_lease_ttl_s,
        )
        operation_root = command.operation_id or f"revision:{snapshot.revision}:publish"
        try:
            await self._store.append(
                snapshot.workflow_id,
                _ProjectPREventDraft(
                    ProjectPREventKind.PUBLISH_ENQUEUED,
                    {
                        "revision": snapshot.revision,
                        "effect_id": effect_id,
                        "head_sha": snapshot.head_sha,
                        "app_id": self._forge.app_id,
                        "name": self._check_name,
                        "external_id": effect_id,
                    },
                ),
                expected_sequence=snapshot.last_sequence,
                operation_id=f"{operation_root}:enqueued",
                lease=lease,
                outbox=_OutboxMutation(
                    effect_id=effect_id,
                    target_state=OutboxState.PENDING,
                    desired=desired,
                    revision=snapshot.revision,
                ),
            )
            enqueued = await self.load(snapshot.workflow_id)
            await self._store.append(
                snapshot.workflow_id,
                _ProjectPREventDraft(
                    ProjectPREventKind.PUBLISH_STARTED,
                    {
                        "revision": snapshot.revision,
                        "effect_id": effect_id,
                        "publisher_fence": lease.fence,
                        "attempt": 1,
                    },
                ),
                expected_sequence=enqueued.last_sequence,
                operation_id=f"{operation_root}:started",
                lease=lease,
                outbox=_OutboxMutation(
                    effect_id=effect_id,
                    expected_state=OutboxState.PENDING,
                    target_state=OutboxState.IN_FLIGHT,
                    increment_attempts=True,
                ),
            )
            check = await self._forge.converge(desired, effect_id=effect_id)
            expected_locator = ForgeCheckLocator(
                repository=desired.repository,
                pull_request_number=desired.pull_request_number,
                head_sha=desired.head_sha,
                app_id=desired.app_id,
                name=desired.name,
                external_id=desired.external_id,
            )
            if check.locator != expected_locator:
                raise ProjectPRConflict("forge receipt does not match the requested check")
            in_flight = await self.load(snapshot.workflow_id)
            await self._store.append(
                snapshot.workflow_id,
                _ProjectPREventDraft(
                    ProjectPREventKind.PUBLISH_CONFIRMED,
                    {
                        "revision": snapshot.revision,
                        "effect_id": effect_id,
                        "check_run_id": check.check_run_id,
                        "adopted": False,
                    },
                ),
                expected_sequence=in_flight.last_sequence,
                operation_id=f"{operation_root}:confirmed",
                lease=lease,
                outbox=_OutboxMutation(
                    effect_id=effect_id,
                    expected_state=OutboxState.IN_FLIGHT,
                    target_state=OutboxState.CONFIRMED,
                    check_run_id=check.check_run_id,
                ),
            )
        except Exception:
            await self._store.release(lease)
            raise
        await self._store.release(lease)
        updated = await self.load(snapshot.workflow_id)
        return ProjectPRHandle(updated.workflow_id, updated.revision, True)

    async def _submit_resume(self, command: ResumePR) -> ProjectPRHandle:
        _validate_resume(command)
        snapshot = await self.load(command.workflow_id)
        if snapshot.state is ProjectPRState.CONFIRMED:
            return ProjectPRHandle(snapshot.workflow_id, snapshot.revision, False)
        if snapshot.state not in {ProjectPRState.PUBLISHING, ProjectPRState.UNKNOWN}:
            raise ProjectPRConflict("workflow has no interrupted publication to resume")
        if snapshot.publish_effect_id is None:
            raise ProjectPRConflict("interrupted publication has no durable effect id")
        remote_head = await self._forge.observe_head(
            snapshot.repository, snapshot.pull_request_number
        )
        if remote_head != snapshot.head_sha:
            raise ProjectPRHeadMoved(
                f"pull-request head changed from {snapshot.head_sha} to {remote_head}"
            )
        effect = await self._store.load_effect(
            snapshot.workflow_id, snapshot.publish_effect_id
        )
        if effect.state not in {OutboxState.IN_FLIGHT, OutboxState.UNKNOWN}:
            raise ProjectPRConflict(
                f"outbox effect cannot be reconciled from {effect.state.value}"
            )
        lease = await self._store.acquire(
            snapshot.workflow_id,
            owner=self._worker_id,
            ttl_s=self._publisher_lease_ttl_s,
        )
        operation_root = f"{effect.effect_id}:reconcile:f{lease.fence}"
        integrity_error: ProjectPRIntegrityError | None = None
        try:
            if effect.state is OutboxState.IN_FLIGHT:
                await self._store.append(
                    snapshot.workflow_id,
                    _ProjectPREventDraft(
                        ProjectPREventKind.PUBLISH_UNKNOWN,
                        {
                            "revision": snapshot.revision,
                            "effect_id": effect.effect_id,
                            "publisher_fence": lease.fence,
                        },
                    ),
                    expected_sequence=snapshot.last_sequence,
                    operation_id=f"{operation_root}:unknown",
                    lease=lease,
                    outbox=_OutboxMutation(
                        effect_id=effect.effect_id,
                        expected_state=OutboxState.IN_FLIGHT,
                        target_state=OutboxState.UNKNOWN,
                    ),
                )
            unknown = await self.load(snapshot.workflow_id)
            locator = ForgeCheckLocator(
                repository=effect.desired.repository,
                pull_request_number=effect.desired.pull_request_number,
                head_sha=effect.desired.head_sha,
                app_id=effect.desired.app_id,
                name=effect.desired.name,
                external_id=effect.desired.external_id,
            )
            matches: tuple[ForgeCheck, ...] = ()
            polls = 0
            for attempt in range(1, self._reconciliation_poll_attempts + 1):
                polls = attempt
                matches = await self._forge.observe(locator)
                if matches:
                    break
                if (
                    attempt < self._reconciliation_poll_attempts
                    and self._reconciliation_poll_interval_s > 0
                ):
                    await asyncio.sleep(self._reconciliation_poll_interval_s)
            if not matches:
                await self._store.append(
                    snapshot.workflow_id,
                    _ProjectPREventDraft(
                        ProjectPREventKind.PUBLISH_ACTION_REQUIRED,
                        {
                            "revision": snapshot.revision,
                            "effect_id": effect.effect_id,
                            "publisher_fence": lease.fence,
                            "observed_match_count": 0,
                            "reconciliation_polls": polls,
                        },
                    ),
                    expected_sequence=unknown.last_sequence,
                    operation_id=f"{operation_root}:action-required",
                    lease=lease,
                    outbox=_OutboxMutation(
                        effect_id=effect.effect_id,
                        expected_state=OutboxState.UNKNOWN,
                        target_state=OutboxState.ACTION_REQUIRED,
                    ),
                )
            elif len(matches) != 1:
                await self._store.append(
                    snapshot.workflow_id,
                    _ProjectPREventDraft(
                        ProjectPREventKind.PUBLISH_INTEGRITY_FAILED,
                        {
                            "revision": snapshot.revision,
                            "effect_id": effect.effect_id,
                            "publisher_fence": lease.fence,
                            "observed_match_count": len(matches),
                            "reconciliation_polls": polls,
                        },
                    ),
                    expected_sequence=unknown.last_sequence,
                    operation_id=f"{operation_root}:integrity-failed",
                    lease=lease,
                    outbox=_OutboxMutation(
                        effect_id=effect.effect_id,
                        expected_state=OutboxState.UNKNOWN,
                        target_state=OutboxState.FAILED,
                    ),
                )
                integrity_error = ProjectPRIntegrityError(
                    f"remote reconciliation found {len(matches)} exact check matches"
                )
            else:
                adopted = matches[0]
                await self._store.append(
                    snapshot.workflow_id,
                    _ProjectPREventDraft(
                        ProjectPREventKind.PUBLISH_CONFIRMED,
                        {
                            "revision": snapshot.revision,
                            "effect_id": effect.effect_id,
                            "check_run_id": adopted.check_run_id,
                            "adopted": True,
                            "observed_match_count": 1,
                            "reconciliation_polls": polls,
                        },
                    ),
                    expected_sequence=unknown.last_sequence,
                    operation_id=f"{operation_root}:adopted",
                    lease=lease,
                    outbox=_OutboxMutation(
                        effect_id=effect.effect_id,
                        expected_state=OutboxState.UNKNOWN,
                        target_state=OutboxState.CONFIRMED,
                        check_run_id=adopted.check_run_id,
                    ),
                )
        except Exception:
            await self._store.release(lease)
            raise
        await self._store.release(lease)
        if integrity_error is not None:
            raise integrity_error
        updated = await self.load(snapshot.workflow_id)
        return ProjectPRHandle(updated.workflow_id, updated.revision, True)


def _stored_event(
    draft: _ProjectPREventDraft,
    *,
    workflow_id: str,
    sequence: int,
    operation_id: str,
    previous_hash: str,
    occurred_at: float,
) -> ProjectPREvent:
    event_id = str(
        uuid.uuid5(
            _EVENT_ID_NAMESPACE,
            f"{workflow_id}\0{sequence}\0{operation_id}",
        )
    )
    data = MappingProxyType(dict(draft.data))
    payload = {
        "workflow_id": workflow_id,
        "sequence": sequence,
        "operation_id": operation_id,
        "event_id": event_id,
        "kind": draft.kind.value,
        "occurred_at": occurred_at,
        "previous_hash": previous_hash,
        "data": data,
    }
    event_hash = hashlib.sha256(
        _EVENT_HASH_DOMAIN + canonical_json(payload).encode()
    ).hexdigest()
    return ProjectPREvent(
        workflow_id=workflow_id,
        sequence=sequence,
        operation_id=operation_id,
        event_id=event_id,
        kind=draft.kind,
        occurred_at=occurred_at,
        previous_hash=previous_hash,
        event_hash=event_hash,
        data=data,
    )


def _commit_payload_hash(
    draft: _ProjectPREventDraft,
    outbox: _OutboxMutation | None,
) -> str:
    outbox_payload: Mapping[str, Any] | None = None
    if outbox is not None:
        outbox_payload = {
            "effect_id": outbox.effect_id,
            "target_state": outbox.target_state.value,
            "desired": (
                _desired_check_payload(outbox.desired)
                if outbox.desired is not None
                else None
            ),
            "revision": outbox.revision,
            "expected_state": (
                outbox.expected_state.value
                if outbox.expected_state is not None
                else None
            ),
            "increment_attempts": outbox.increment_attempts,
            "check_run_id": outbox.check_run_id,
        }
    return hashlib.sha256(
        canonical_json(
            {
                "event": {"kind": draft.kind.value, "data": draft.data},
                "outbox": outbox_payload,
            }
        ).encode()
    ).hexdigest()


def _desired_check_payload(desired: ForgeDesiredCheck) -> Mapping[str, Any]:
    return {
        "repository": desired.repository,
        "pull_request_number": desired.pull_request_number,
        "head_sha": desired.head_sha,
        "app_id": desired.app_id,
        "name": desired.name,
        "external_id": desired.external_id,
        "summary": desired.summary,
    }


def _fold_events(events: Sequence[ProjectPREvent]) -> ProjectPRSnapshot:
    if not events:
        raise ProjectPRNotFound("workflow has no events")
    _verify_event_chain(events)
    first = events[0]
    if first.kind is not ProjectPREventKind.WORKFLOW_STARTED:
        raise ProjectPRConflict("first workflow fact must be workflow_started")
    data = first.data
    snapshot = ProjectPRSnapshot(
        workflow_id=first.workflow_id,
        state=ProjectPRState.ANCHORED,
        project_key=str(data["project_key"]),
        repository=str(data["repository"]),
        pull_request_number=int(data["pull_request_number"]),
        base_sha=str(data["base_sha"]),
        head_sha=str(data["head_sha"]),
        goal=str(data["goal"]),
        revision=int(data["revision"]),
        last_sequence=first.sequence,
        last_hash=first.event_hash,
    )
    for event in events[1:]:
        if event.kind is ProjectPREventKind.REVISION_SEALED:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.AWAITING_PUBLISH_APPROVAL,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                candidate_tree=str(event.data["candidate_tree"]),
                patch_digest=str(event.data["patch_digest"]),
                verification_digest=str(event.data["verification_digest"]),
                evidence_digest=str(event.data["evidence_digest"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_ENQUEUED:
            snapshot = replace(
                snapshot,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_STARTED:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.PUBLISHING,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
                publisher_fence=int(event.data["publisher_fence"]),
                outbound_create_attempts=int(event.data["attempt"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_UNKNOWN:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.UNKNOWN,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
                publisher_fence=int(event.data["publisher_fence"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_ACTION_REQUIRED:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.ACTION_REQUIRED,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
                publisher_fence=int(event.data["publisher_fence"]),
                observed_match_count=int(event.data["observed_match_count"]),
                reconciliation_polls=int(event.data["reconciliation_polls"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_INTEGRITY_FAILED:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.INTEGRITY_FAILED,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
                publisher_fence=int(event.data["publisher_fence"]),
                observed_match_count=int(event.data["observed_match_count"]),
                reconciliation_polls=int(event.data["reconciliation_polls"]),
            )
        elif event.kind is ProjectPREventKind.PUBLISH_CONFIRMED:
            snapshot = replace(
                snapshot,
                state=ProjectPRState.CONFIRMED,
                last_sequence=event.sequence,
                last_hash=event.event_hash,
                publish_effect_id=str(event.data["effect_id"]),
                check_run_id=str(event.data["check_run_id"]),
                remote_check_adopted=bool(event.data.get("adopted", False)),
                observed_match_count=(
                    int(event.data["observed_match_count"])
                    if "observed_match_count" in event.data
                    else snapshot.observed_match_count
                ),
                reconciliation_polls=(
                    int(event.data["reconciliation_polls"])
                    if "reconciliation_polls" in event.data
                    else snapshot.reconciliation_polls
                ),
            )
        else:  # pragma: no cover - all enum members are handled above
            raise ProjectPRConflict(f"unsupported workflow fact: {event.kind.value}")
    return snapshot


def fold_project_pr_events(
    events: Sequence[ProjectPREvent],
) -> ProjectPRSnapshot:
    """Purely rebuild reviewer-facing state from sequence one."""

    return _fold_events(events)


def _verify_event_chain(events: Sequence[ProjectPREvent]) -> None:
    previous_hash = GENESIS_HASH
    workflow_id = events[0].workflow_id
    for expected_sequence, event in enumerate(events, start=1):
        if event.workflow_id != workflow_id:
            raise ProjectPRConflict("workflow event chain mixes workflow identities")
        if event.sequence != expected_sequence:
            raise ProjectPRConflict("workflow event sequence is missing or reordered")
        if event.previous_hash != previous_hash:
            raise ProjectPRConflict("workflow event hash link is inconsistent")
        rebuilt = _stored_event(
            _ProjectPREventDraft(event.kind, event.data),
            workflow_id=event.workflow_id,
            sequence=event.sequence,
            operation_id=event.operation_id,
            previous_hash=event.previous_hash,
            occurred_at=event.occurred_at,
        )
        if rebuilt.event_id != event.event_id or rebuilt.event_hash != event.event_hash:
            raise ProjectPRConflict("workflow event content hash is inconsistent")
        previous_hash = event.event_hash


def verify_project_pr_events(events: Sequence[ProjectPREvent]) -> None:
    """Verify internal sequence, identity, and hash-link consistency."""

    if not events:
        raise ProjectPRConflict("workflow event chain is empty")
    _verify_event_chain(events)


def _validate_start(command: StartPR) -> None:
    string_fields = {
        "project_key": command.project_key,
        "repository": command.repository,
        "goal": command.goal,
        "idempotency_key": command.idempotency_key,
    }
    for name, value in string_fields.items():
        if not value.strip():
            raise ValueError(f"{name} must not be blank")
    if isinstance(command.pull_request_number, bool) or command.pull_request_number < 1:
        raise ValueError("pull_request_number must be positive")
    _validate_git_id("base_sha", command.base_sha)
    _validate_git_id("head_sha", command.head_sha)
    if command.base_sha == command.head_sha:
        raise ValueError("base_sha and head_sha must differ")


def _validate_seal(command: SealRevision) -> None:
    if not command.workflow_id.strip():
        raise ValueError("workflow_id must not be blank")
    if isinstance(command.expected_revision, bool) or command.expected_revision < 1:
        raise ValueError("expected_revision must be positive")
    _validate_git_id("observed_head_sha", command.observed_head_sha)
    _validate_git_id("candidate_tree", command.candidate_tree)
    _validate_digest("patch_digest", command.patch_digest)
    _validate_digest("verification_digest", command.verification_digest)
    _validate_digest("evidence_digest", command.evidence_digest)
    if command.operation_id is not None and not command.operation_id.strip():
        raise ValueError("operation_id must not be blank")


def _validate_publish(command: PublishPR) -> None:
    if not command.workflow_id.strip():
        raise ValueError("workflow_id must not be blank")
    if isinstance(command.expected_revision, bool) or command.expected_revision < 1:
        raise ValueError("expected_revision must be positive")
    if command.operation_id is not None and not command.operation_id.strip():
        raise ValueError("operation_id must not be blank")


def _validate_resume(command: ResumePR) -> None:
    if not command.workflow_id.strip():
        raise ValueError("workflow_id must not be blank")


def _validate_git_id(name: str, value: str) -> None:
    if len(value) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(
            f"{name} must be a lowercase 40- or 64-character Git object id"
        )


def _validate_digest(name: str, value: str) -> None:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _start_hash(command: StartPR) -> str:
    payload = {
        "project_key": command.project_key,
        "repository": command.repository,
        "pull_request_number": command.pull_request_number,
        "base_sha": command.base_sha,
        "head_sha": command.head_sha,
        "goal": command.goal,
    }
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()
