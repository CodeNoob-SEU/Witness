"""Deterministic Crash-to-Proof PR publication demo.

Run with::

    uv run python -m react_agent.project_pr_demo --output output/project_pr_demo
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import subprocess
import sys
import tempfile
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .agent import ReActAgent
from .events import StoredRunEvent, canonical_json
from .journal import InMemoryRunJournal
from .models import AssistantMessage, ModelRequest, ModelResponse, ToolCall
from .project_pr import (
    InMemoryProjectPRStore,
    MockForge,
    ProjectPREvent,
    ProjectPRSnapshot,
    ProjectPullRequests,
    PublishPR,
    ResumePR,
    SealRevision,
    StartPR,
)
from .project_pr_evidence import generate_project_pr_evidence
from .runtime import AgentRuntime, InMemoryRuntimeStore, ResumeRun, StartRun
from .tools import ToolExecutionContext, tool
from .workspace import GitWorktreeWorkspace

_BROKEN_PRICING = """def unit_price(total: float, item_count: int) -> float:
    return total / item_count
"""
_FIXED_PRICING = """def unit_price(total: float, item_count: int) -> float:
    if item_count == 0:
        return total
    return total / item_count
"""
_RUNTIME_SAFE_DATA_KEYS = frozenset(
    {
        "attempt",
        "dirty",
        "diverged",
        "executed",
        "is_error",
        "outcome",
        "phase",
        "previous_execution_id",
        "reason",
        "reasons",
        "resume_policy",
        "resume_reason",
        "status",
        "stop_reason",
        "tool_call_id",
        "tool_name",
    }
)


class _DemoClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _ScriptedModel:
    def __init__(self, *responses: ModelResponse) -> None:
        self._responses = deque(responses)

    async def complete(self, request: ModelRequest) -> ModelResponse:
        del request
        if not self._responses:
            raise RuntimeError("project PR demo model script is exhausted")
        return self._responses.popleft()


@dataclass(frozen=True, slots=True)
class _CodeRecoveryResult:
    base_sha: str
    head_sha: str
    candidate_tree: str
    patch: str
    patch_digest: str
    verification_digest: str
    runtime_evidence_digest: str
    runtime_events_safe_text: str
    runtime_recovery: Mapping[str, Any]
    workspace_restored_before_idempotent_retry: bool
    primary_worktree_unchanged: bool


@dataclass(frozen=True, slots=True)
class ProjectPRDemoResult:
    snapshot: ProjectPRSnapshot
    outbound_create_post_count: int
    physical_check_run_count: int
    publisher_takeovers: int
    code_worker_takeovers: int
    workspace_restored_before_idempotent_retry: bool
    primary_worktree_unchanged: bool
    evidence_digest: str
    output_dir: Path


async def run_project_pr_demo(output_dir: Path) -> ProjectPRDemoResult:
    """Run accepted-before-receipt recovery and write public-safe artifacts."""

    with tempfile.TemporaryDirectory(prefix="witness-project-pr-demo-") as temp_dir:
        code_recovery = await _run_code_recovery(Path(temp_dir))

    clock = _DemoClock()
    store = InMemoryProjectPRStore(clock=clock, wall_clock=clock)
    forge = MockForge(
        app_id=73,
        head_sha=code_recovery.head_sha,
        pause_after_create=True,
    )
    first_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-a",
        publisher_lease_ttl_s=5,
    )
    started = await first_publisher.submit(
        StartPR(
            project_key="interview-demo",
            repository="acme/pricing-service",
            pull_request_number=42,
            base_sha=code_recovery.base_sha,
            head_sha=code_recovery.head_sha,
            goal="Correct the disabled-research pricing path",
            idempotency_key="demo-pr-42",
        )
    )
    await first_publisher.submit(
        SealRevision(
            workflow_id=started.workflow_id,
            expected_revision=1,
            observed_head_sha=code_recovery.head_sha,
            candidate_tree=code_recovery.candidate_tree,
            patch_digest=code_recovery.patch_digest,
            verification_digest=code_recovery.verification_digest,
            evidence_digest=code_recovery.runtime_evidence_digest,
        )
    )

    publishing = asyncio.create_task(
        first_publisher.submit(
            PublishPR(workflow_id=started.workflow_id, expected_revision=1)
        )
    )
    await asyncio.wait_for(forge.create_committed.wait(), timeout=1)
    publishing.cancel()
    try:
        await publishing
    except asyncio.CancelledError:
        pass

    clock.advance(6)
    recovery_publisher = ProjectPullRequests(
        store=store,
        forge=forge,
        worker_id="publisher-b",
        publisher_lease_ttl_s=5,
    )
    await recovery_publisher.submit(ResumePR(workflow_id=started.workflow_id))
    snapshot = await recovery_publisher.load(started.workflow_id)
    events = tuple(
        [event async for event in recovery_publisher.follow(started.workflow_id)]
    )
    evidence = generate_project_pr_evidence(events)
    publisher_takeovers = max((snapshot.publisher_fence or 1) - 1, 0)
    metrics = {
        "schema_version": "witness.project-pr-demo.v1",
        "outcome": "auto_completed",
        "workflow_id": snapshot.workflow_id,
        "revision": snapshot.revision,
        "state": snapshot.state.value,
        "outbound_create_check_post_count": forge.outbound_create_post_count,
        "physical_check_run_count": forge.physical_check_run_count,
        "uncertain_non_idempotent_auto_retries": 0,
        "code_worker_takeovers": 1,
        "workspace_restored_before_idempotent_retry": (
            code_recovery.workspace_restored_before_idempotent_retry
        ),
        "primary_worktree_unchanged": code_recovery.primary_worktree_unchanged,
        "publisher_takeovers": publisher_takeovers,
        "publisher_fence": snapshot.publisher_fence,
        "remote_check_adopted": snapshot.remote_check_adopted,
        "observed_match_count": snapshot.observed_match_count,
        "event_count": len(events),
        "journal_head": snapshot.last_hash,
        "evidence_digest": evidence.digest,
    }

    await asyncio.to_thread(
        _write_demo_artifacts,
        output_dir,
        metrics,
        events,
        evidence.json_text,
        evidence.markdown_text,
        code_recovery,
    )
    return ProjectPRDemoResult(
        snapshot=snapshot,
        outbound_create_post_count=forge.outbound_create_post_count,
        physical_check_run_count=forge.physical_check_run_count,
        publisher_takeovers=publisher_takeovers,
        code_worker_takeovers=1,
        workspace_restored_before_idempotent_retry=(
            code_recovery.workspace_restored_before_idempotent_retry
        ),
        primary_worktree_unchanged=code_recovery.primary_worktree_unchanged,
        evidence_digest=evidence.digest,
        output_dir=output_dir,
    )


def _write_demo_artifacts(
    output_dir: Path,
    metrics: Mapping[str, Any],
    events: tuple[ProjectPREvent, ...],
    evidence_json: str,
    evidence_markdown: str,
    code_recovery: _CodeRecoveryResult,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metrics.json").write_text(
        canonical_json(metrics) + "\n", encoding="utf-8"
    )
    (output_dir / "pr_evidence.json").write_text(evidence_json, encoding="utf-8")
    (output_dir / "pr_evidence.md").write_text(
        evidence_markdown, encoding="utf-8"
    )
    (output_dir / "events.safe.ndjson").write_text(
        "".join(_event_json(event) + "\n" for event in events),
        encoding="utf-8",
    )
    (output_dir / "runtime.events.safe.ndjson").write_text(
        code_recovery.runtime_events_safe_text,
        encoding="utf-8",
    )
    (output_dir / "patch.diff").write_text(code_recovery.patch, encoding="utf-8")
    (output_dir / "runtime_recovery.json").write_text(
        canonical_json(code_recovery.runtime_recovery) + "\n",
        encoding="utf-8",
    )


async def _run_code_recovery(root: Path) -> _CodeRecoveryResult:
    repository = root / "repository"
    managed_root = root / "managed-worktrees"
    repository.mkdir(parents=True)
    _git(repository, "init", "-q")
    (repository / "README.md").write_text("# Pricing fixture\n", encoding="utf-8")
    _git(repository, "add", "README.md")
    _git_commit(repository, "base")
    base_sha = _git(repository, "rev-parse", "HEAD")
    (repository / "pricing.py").write_text(_BROKEN_PRICING, encoding="utf-8")
    _git(repository, "add", "pricing.py")
    _git_commit(repository, "draft-pr-head")
    head_sha = _git(repository, "rev-parse", "HEAD")

    workspace = GitWorktreeWorkspace(repository, managed_root)
    journal = InMemoryRunJournal()
    runtime_store = InMemoryRuntimeStore()
    session_id = "project-pr-code-worker"
    interrupted = asyncio.Event()
    worktree_paths: list[Path] = []
    observed_before_retry: list[str] = []
    invocations = 0

    @tool(name="apply_patch", idempotent=True, version="project-pr-demo-v1")
    async def interrupted_patch(
        content: str,
        *,
        context: ToolExecutionContext,
    ) -> str:
        """Apply the deterministic reviewed patch in the isolated worktree."""

        nonlocal invocations
        invocations += 1
        if context.workspace_path is None:
            raise RuntimeError("managed workspace path was not injected")
        worktree_paths.append(context.workspace_path)
        (context.workspace_path / "pricing.py").write_text(
            "def unit_price(\n", encoding="utf-8"
        )
        interrupted.set()
        await asyncio.Event().wait()
        return content

    first_model = _ScriptedModel(
        ModelResponse(
            AssistantMessage(
                None,
                (ToolCall("patch-1", "apply_patch", '{"content":"fixed"}'),),
            )
        )
    )
    first_runtime = AgentRuntime(
        ReActAgent(first_model, [interrupted_patch]),
        journal,
        store=runtime_store,
        workspace=workspace,
        model_name="project-pr-demo-model",
    )
    handle = await first_runtime.submit(
        StartRun(
            prompt="Apply the reviewed pricing fix",
            session_id=session_id,
            idempotency_key="code-worker-attempt",
        )
    )
    await asyncio.wait_for(interrupted.wait(), timeout=2)
    await first_runtime.close()

    @tool(name="apply_patch", idempotent=True, version="project-pr-demo-v1")
    async def recovered_patch(
        content: str,
        *,
        context: ToolExecutionContext,
    ) -> str:
        """Apply the deterministic reviewed patch in the isolated worktree."""

        nonlocal invocations
        invocations += 1
        if context.workspace_path is None:
            raise RuntimeError("managed workspace path was not injected")
        worktree_paths.append(context.workspace_path)
        pricing_path = context.workspace_path / "pricing.py"
        observed_before_retry.append(pricing_path.read_text(encoding="utf-8"))
        pricing_path.write_text(_FIXED_PRICING, encoding="utf-8")
        return content

    recovery_runtime = AgentRuntime(
        ReActAgent(
            _ScriptedModel(ModelResponse(AssistantMessage("Patch and verification ready"))),
            [recovered_patch],
        ),
        journal,
        store=runtime_store,
        workspace=workspace,
        model_name="project-pr-demo-model",
    )
    await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    # Git checkpoint verification can take a few seconds on a busy laptop.  The
    # demo is deterministic, so give the recovery worker enough time to reach a
    # terminal state instead of turning machine load into a flaky failure.
    completed = await recovery_runtime.wait(handle.run_id, timeout_s=10)
    events = await journal.read(handle.run_id)
    await recovery_runtime.close()

    if completed.status != "completed" or not worktree_paths:
        raise RuntimeError(
            "coding worker recovery did not complete "
            f"(status={completed.status!r}, state={completed.state.value!r})"
        )
    worktree_path = worktree_paths[-1]
    verification = await asyncio.to_thread(_verify_pricing_patch, worktree_path)
    verification_payload = {
        "command": "python -c pricing regression assertions",
        "exit_code": verification.returncode,
        "stdout_sha256": hashlib.sha256(verification.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(verification.stderr.encode()).hexdigest(),
    }
    if verification.returncode != 0:
        raise RuntimeError(f"pricing verification failed: {verification.stderr[:300]}")
    patch = _git(worktree_path, "diff", "--no-ext-diff", "--", "pricing.py")
    if not patch.startswith("diff --git"):
        raise RuntimeError("coding worker produced no reviewable patch")
    checkpoint = workspace.checkpoint(session_id)
    runtime_events_safe_text = "".join(
        _runtime_event_json(event) + "\n" for event in events
    )
    runtime_evidence_digest = hashlib.sha256(
        runtime_events_safe_text.encode()
    ).hexdigest()
    restored = observed_before_retry == [_BROKEN_PRICING]
    primary_unchanged = (
        _git(repository, "status", "--porcelain") == ""
        and _git(repository, "rev-parse", "HEAD") == head_sha
    )
    runtime_recovery = {
        "schema_version": "witness.code-recovery.v1",
        "run_id": handle.run_id,
        "session_id": session_id,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "candidate_tree": checkpoint.tree_id,
        "tool_invocations": invocations,
        "code_worker_takeovers": 1,
        "workspace_restored_before_idempotent_retry": restored,
        "primary_worktree_unchanged": primary_unchanged,
        "verification": verification_payload,
        "runtime_event_count": len(events),
        "runtime_journal_head": events[-1].event_hash,
        "runtime_events_sha256": runtime_evidence_digest,
    }
    workspace.cleanup(session_id)
    return _CodeRecoveryResult(
        base_sha=base_sha,
        head_sha=head_sha,
        candidate_tree=checkpoint.tree_id,
        patch=patch,
        patch_digest=hashlib.sha256(patch.encode()).hexdigest(),
        verification_digest=hashlib.sha256(
            canonical_json(verification_payload).encode()
        ).hexdigest(),
        runtime_evidence_digest=runtime_evidence_digest,
        runtime_events_safe_text=runtime_events_safe_text,
        runtime_recovery=runtime_recovery,
        workspace_restored_before_idempotent_retry=restored,
        primary_worktree_unchanged=primary_unchanged,
    )


def _verify_pricing_patch(worktree_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pricing import unit_price; "
                "assert unit_price(99.0, 0) == 99.0; "
                "assert unit_price(100.0, 4) == 25.0"
            ),
        ],
        cwd=worktree_path,
        check=False,
        capture_output=True,
        text=True,
    )


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(arguments)} failed: {completed.stderr.strip()[:300]}"
        )
    return completed.stdout.strip()


def _git_commit(repository: Path, message: str) -> None:
    _git(
        repository,
        "-c",
        "user.name=Witness Demo",
        "-c",
        "user.email=witness-demo@localhost",
        "commit",
        "-q",
        "-m",
        message,
    )


def _event_json(event: ProjectPREvent) -> str:
    return canonical_json(
        {
            "workflow_id": event.workflow_id,
            "sequence": event.sequence,
            "operation_id": event.operation_id,
            "event_id": event.event_id,
            "kind": event.kind.value,
            "occurred_at": event.occurred_at,
            "previous_hash": event.previous_hash,
            "event_hash": event.event_hash,
            "data": event.data,
        }
    )


def _runtime_event_json(event: StoredRunEvent) -> str:
    """Render a metadata-only projection of the synthetic Runtime fixture."""

    safe_data = {
        key: event.data[key]
        for key in sorted(_RUNTIME_SAFE_DATA_KEYS)
        if key in event.data
    }
    return canonical_json(
        {
            "run_id": event.run_id,
            "sequence": event.sequence,
            "operation_id": event.operation_id,
            "event_id": event.event_id,
            "kind": event.kind.value,
            "occurred_at": event.occurred_at,
            "previous_hash": event.previous_hash,
            "event_hash": event.event_hash,
            "step": event.step,
            "call_key": event.call_key,
            "execution_id": event.execution_id,
            "safe_checkpoint": event.safe_checkpoint,
            "data": safe_data,
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/project_pr_demo"),
        help="Directory for metrics, events, and evidence artifacts.",
    )
    arguments = parser.parse_args(argv)
    result = asyncio.run(run_project_pr_demo(arguments.output))
    print(
        canonical_json(
            {
                "state": result.snapshot.state.value,
                "outbound_create_check_post_count": result.outbound_create_post_count,
                "physical_check_run_count": result.physical_check_run_count,
                "publisher_takeovers": result.publisher_takeovers,
                "evidence_digest": result.evidence_digest,
                "output_dir": str(result.output_dir),
            }
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI smoke test
    raise SystemExit(main())
