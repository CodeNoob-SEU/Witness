"""The demo must be a real run, not a rendering of canned output.

These tests exist to keep that honest: the seeded repository is a real Git
repository, the task drives the real Runtime through a real managed worktree,
and the resulting event chain is verified the same way a production chain is.
If the demo ever degrades into a fixture replay, these fail.
"""

from __future__ import annotations

import asyncio
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from react_agent.demo import (
    DEMO_TASKS_BY_ID,
    DemoTask,
    build_demo_agent,
    demo_pricing,
    seed_demo_repository,
)
from react_agent.events import RunSnapshot, verify_event_chain
from react_agent.journal import InMemoryRunJournal
from react_agent.patch import materialize_run_patch, patch_origins
from react_agent.runtime import AgentRuntime, StartRun


@dataclass(frozen=True, slots=True)
class DemoRun:
    snapshot: RunSnapshot
    worktree: Path
    repository: Path
    journal: InMemoryRunJournal


async def _run_demo_task(task: DemoTask, base: Path) -> DemoRun:
    """Run one demo task exactly the way the console does."""

    from react_agent.workspace import GitWorktreeWorkspace

    repository = base / "repo"
    managed = base / "managed"
    seed_demo_repository(repository)

    session_id = f"demo-{uuid.uuid4().hex[:12]}"
    journal = InMemoryRunJournal()
    runtime = AgentRuntime(
        build_demo_agent(),
        journal,
        model_name="witness-demo-model",
        workspace=GitWorktreeWorkspace(repository, managed),
        pricing=demo_pricing(),
    )
    try:
        handle = await runtime.submit(StartRun(prompt=task.prompt, session_id=session_id))
        snapshot = await runtime.wait(handle.run_id, timeout_s=60.0)
    finally:
        await runtime.close()
    return DemoRun(snapshot, managed / session_id, repository, journal)


async def test_the_session_fix_task_actually_repairs_the_bug() -> None:
    task = DEMO_TASKS_BY_ID["fix-session-refresh"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))

        assert run.snapshot.status == "completed", run.snapshot.stop_reason
        source = (run.worktree / "witness_demo" / "session.py").read_text(encoding="utf-8")
        assert "if token is None:" in source
        assert "self._cache.set(key, token)" in source

        tests = (run.worktree / "tests" / "test_session.py").read_text(encoding="utf-8")
        assert "test_refresh_mints_a_token_when_the_cache_misses" in tests
        # The existing test must survive: a fix that deletes coverage is not a fix.
        assert "test_refresh_returns_a_cached_token" in tests


async def test_the_repaired_worktree_passes_its_own_test_suite() -> None:
    """Grade the demo the way `evals.py` grades: by running the result."""

    task = DEMO_TASKS_BY_ID["fix-session-refresh"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))

        completed = await asyncio.to_thread(
            subprocess.run,
            ["python", "-m", "pytest", "-q", "tests"],
            cwd=run.worktree,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr


async def test_the_demo_chain_verifies_from_the_first_sequence() -> None:
    task = DEMO_TASKS_BY_ID["fix-session-refresh"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))

        events = await run.journal.read(run.snapshot.run_id)
        verify_event_chain(events)  # raises if the history was backfilled
        assert len(events) > 10
        assert events[0].sequence == 1


async def test_the_out_of_bounds_task_writes_nothing_outside_the_worktree() -> None:
    task = DEMO_TASKS_BY_ID["refuse-path-escape"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        base = Path(directory)
        run = await _run_demo_task(task, base)

        assert not (base / "etc").exists()
        assert not (run.worktree / "etc").exists()
        assert run.snapshot.result is not None
        answer = str(run.snapshot.result.get("output") or "")
        # The value of this task is the explanation, not the refusal alone.
        assert "managed" in answer.casefold() or "worktree" in answer.casefold()


async def test_the_coverage_task_adds_a_test_file() -> None:
    task = DEMO_TASKS_BY_ID["cover-cache-eviction"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))

        assert run.snapshot.status == "completed", run.snapshot.stop_reason
        added = run.worktree / "tests" / "test_cache.py"
        assert added.is_file()
        assert "test_evict_is_a_no_op_for_an_unknown_key" in added.read_text(encoding="utf-8")


async def test_every_changed_file_traces_back_to_the_call_that_wrote_it() -> None:
    """The console's core claim: provenance comes from Git, not from prose.

    Attribution here is derived from checkpoint tree ids alone. The tool's
    arguments never reach the durable log (debug exposure defaults to
    ``METADATA``), so nothing in this assertion can be satisfied by a model
    that merely *claims* to have edited a file.
    """

    task = DEMO_TASKS_BY_ID["fix-session-refresh"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))
        events = await run.journal.read(run.snapshot.run_id)

        patch = materialize_run_patch(str(run.repository), events)

        assert {item.path for item in patch.files} == {
            "witness_demo/session.py",
            "tests/test_session.py",
        }
        for item in patch.files:
            assert item.attribution == "exact", item.path
            origin = item.origins[0]
            assert origin.tool_name == "write_workspace_file"
            # Every link in the chain is a real sequence number in this log.
            assert origin.model_sequence is not None
            assert origin.planned_sequence is not None
            assert origin.completed_sequence is not None
            assert origin.planned_sequence < origin.completed_sequence
            assert origin.cost_micros is not None and origin.currency == "USD"

        sequences = [item.origins[0].completed_sequence for item in patch.files]
        assert len(set(sequences)) == 2, "two files, two distinct writes"


async def test_reads_are_not_credited_with_changing_the_workspace() -> None:
    """A call whose before/after trees match must not appear as an origin."""

    task = DEMO_TASKS_BY_ID["fix-session-refresh"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))
        events = await run.journal.read(run.snapshot.run_id)

        origins = patch_origins(events)

        # The run reads three times and writes twice; only the writes moved the tree.
        assert {origin.tool_name for origin in origins} == {"write_workspace_file"}
        assert len(origins) == 2


async def test_a_run_that_changed_nothing_reports_an_empty_patch() -> None:
    task = DEMO_TASKS_BY_ID["refuse-path-escape"]
    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        run = await _run_demo_task(task, Path(directory))
        events = await run.journal.read(run.snapshot.run_id)

        patch = materialize_run_patch(str(run.repository), events)

        assert patch.files == ()
        assert patch.additions == 0 and patch.deletions == 0


def test_seeding_is_idempotent() -> None:
    """The console seeds on every start; a second call must not reset the repo."""

    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        repository = Path(directory) / "repo"
        seed_demo_repository(repository)
        marker = repository / "witness_demo" / "session.py"
        marker.write_text("# local edit\n", encoding="utf-8")

        seed_demo_repository(repository)

        assert marker.read_text(encoding="utf-8") == "# local edit\n"


def test_the_seeded_repository_actually_reproduces_the_bug() -> None:
    """The demo is only worth showing if the bug it fixes is a real one."""

    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        repository = Path(directory) / "repo"
        seed_demo_repository(repository)

        script = (
            "from witness_demo.cache import TokenCache\n"
            "from witness_demo.session import SessionRefresher\n"
            "SessionRefresher(TokenCache()).refresh('nobody')\n"
        )
        completed = subprocess.run(
            ["python", "-c", script],
            cwd=repository,
            capture_output=True,
            text=True,
        )
        assert completed.returncode != 0
        assert "AttributeError" in completed.stderr


@pytest.mark.parametrize("task", DEMO_TASKS_BY_ID.values(), ids=lambda task: task.id)
def test_every_task_states_its_acceptance_criteria(task: DemoTask) -> None:
    """A task without a criterion is a chat prompt wearing a task's clothes."""

    assert task.acceptance
    assert task.title.strip()
    assert task.summary.strip()


# --------------------------------------------------------------------------
# The console over HTTP. Everything above proves the runtime; this proves the
# surface an interviewer actually clicks.
# --------------------------------------------------------------------------


async def test_the_console_serves_a_task_from_dispatch_to_attributed_patch() -> None:
    """One pass through the exact sequence the workspace view performs."""

    import httpx

    from react_agent.runtime import AgentRuntime as _Runtime
    from react_agent.web import DemoEnvironment, create_app
    from react_agent.workspace import GitWorktreeWorkspace

    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        base = Path(directory)
        repository = base / "witness-demo"
        seed_demo_repository(repository)

        runtime = _Runtime(
            build_demo_agent(),
            InMemoryRunJournal(),
            model_name="witness-demo-model",
            workspace=GitWorktreeWorkspace(repository, base / "worktrees"),
            pricing=demo_pricing(),
        )
        app = create_app(
            runtime=runtime,
            model_name="witness-demo-model",
            demo_environment=DemoEnvironment(
                repository=repository,
                managed_root=base / "worktrees",
                session_id="witness-demo",
                tasks=tuple(DEMO_TASKS_BY_ID.values()),
            ),
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            try:
                catalog = await client.get("/api/tasks")
                assert catalog.status_code == 200
                assert {task["id"] for task in catalog.json()["tasks"]} == set(DEMO_TASKS_BY_ID)

                dispatched = await client.post("/api/tasks/fix-session-refresh/runs")
                assert dispatched.status_code == 202
                run_id = dispatched.json()["run_id"]

                await runtime.wait(run_id, timeout_s=60.0)

                integrity = await client.get(f"/api/runs/{run_id}/integrity")
                assert integrity.status_code == 200
                assert integrity.json()["verified"] is True
                assert integrity.json()["first_sequence"] == 1
                assert integrity.json()["resumed"] is False

                patch = await client.get(f"/api/runs/{run_id}/patch")
                assert patch.status_code == 200
                body = patch.json()
                assert body["files_changed"] == 2
                for item in body["files"]:
                    # The claim the console is built to make.
                    assert item["attribution"] == "exact"
                    assert item["origins"][0]["tool_name"] == "write_workspace_file"
                    assert item["hunks"]

                missing = await client.get("/api/runs/does-not-exist/patch")
                assert missing.status_code == 404
            finally:
                await runtime.close()


async def test_a_second_task_in_the_same_session_gets_its_own_patch() -> None:
    """A Session's worktree accumulates; a run's patch must not.

    Without this the second run would be credited with the first run's edits,
    and every provenance claim in the console would be off by one task.
    """

    import httpx

    from react_agent.runtime import AgentRuntime as _Runtime
    from react_agent.web import DemoEnvironment, create_app
    from react_agent.workspace import GitWorktreeWorkspace

    with tempfile.TemporaryDirectory(prefix="witness-demo-") as directory:
        base = Path(directory)
        repository = base / "witness-demo"
        seed_demo_repository(repository)

        runtime = _Runtime(
            build_demo_agent(),
            InMemoryRunJournal(),
            model_name="witness-demo-model",
            workspace=GitWorktreeWorkspace(repository, base / "worktrees"),
            pricing=demo_pricing(),
        )
        app = create_app(
            runtime=runtime,
            model_name="witness-demo-model",
            demo_environment=DemoEnvironment(
                repository=repository,
                managed_root=base / "worktrees",
                session_id="witness-demo",
                tasks=tuple(DEMO_TASKS_BY_ID.values()),
            ),
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            try:
                paths: list[set[str]] = []
                for task_id in ("fix-session-refresh", "cover-cache-eviction"):
                    response = await client.post(f"/api/tasks/{task_id}/runs")
                    run_id = response.json()["run_id"]
                    await runtime.wait(run_id, timeout_s=60.0)
                    patch = (await client.get(f"/api/runs/{run_id}/patch")).json()
                    paths.append({item["path"] for item in patch["files"]})
            finally:
                await runtime.close()

        assert paths[0] == {"witness_demo/session.py", "tests/test_session.py"}
        # The second run touched one new file and must not inherit the first's.
        assert paths[1] == {"tests/test_cache.py"}
