"""Prove a Run survives `kill -9` in the middle of a tool call.

The orchestrator starts a Run in a child worker, waits until PostgreSQL has
committed ``tool_started`` — the exact point where a side effect may already
have happened — and then sends SIGKILL. A second worker resumes the same Run
from durable facts alone and drives it to a final answer. The whole event chain
is re-verified afterwards, so the crash cannot have left a repaired or
back-dated history behind.

    export REACT_AGENT_POSTGRES_DSN=postgresql://user:pass@127.0.0.1:5432/db
    uv run python examples/chaos_resume.py

With ``--supervisor`` worker B is not told which run died. It only runs a
:class:`RunSupervisor`, which lists non-terminal runs whose lease has expired
and Resumes them — the same self-healing loop a long-lived web process runs
with ``REACT_AGENT_SUPERVISOR=true``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from react_agent import (
    AgentConfig,
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ReActAgent,
    ResumeRun,
    StartRun,
    ToolCall,
    ToolExecutionContext,
    ToolMessage,
    tool,
)
from react_agent.events import RunEventKind, StoredRunEvent, verify_event_chain
from react_agent.journal import RunNotFoundError
from react_agent.postgres_journal import PostgresRunJournal
from react_agent.runtime import AgentRuntime, RuntimeConflict
from react_agent.supervisor import RunSupervisor

EFFECTS_ENV = "REACT_AGENT_CHAOS_DIR"
# Short enough that the demo does not idle waiting for a dead worker's lease,
# long enough that a live worker's heartbeat (ttl/3) comfortably renews it.
LEASE_TTL_S = 3.0
FIRST_ATTEMPT_STALL_S = 120.0
RESULT_PREFIX = "CHAOS_RESULT "


def _effects_dir() -> Path:
    return Path(os.environ[EFFECTS_ENV])


@tool(idempotent=True, parallel_safe=True, timeout_s=None)
def append_note(marker: str, *, context: ToolExecutionContext) -> dict[str, str]:
    """Append one auditable note to the demo's effect log."""

    with (_effects_dir() / "attempts.tsv").open("a", encoding="utf-8") as handle:
        handle.write(f"{context.attempt}\t{context.idempotency_key}\t{marker}\n")
    if context.attempt == 1:
        # Only the first attempt stalls, so the orchestrator can kill this
        # worker after tool_started is durable. The retry after resume returns
        # immediately, which is what lets the demo finish.
        time.sleep(FIRST_ATTEMPT_STALL_S)
    return {"marker": marker, "idempotency_key": context.idempotency_key}


class ChaosModel:
    """Deterministic offline model: one tool call, then a final answer.

    ``model`` is a real attribute so both workers derive the same
    ``agent_revision``; Resume refuses to continue a Run whose binding changed.
    """

    model = "chaos-demo-model"

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if any(isinstance(item, ToolMessage) for item in request.transcript):
            return ModelResponse(AssistantMessage("Note recorded. Task complete."))
        return ModelResponse(
            AssistantMessage(
                tool_calls=(
                    ToolCall("call-1", "append_note", '{"marker":"crash-test"}'),
                )
            )
        )


def build_agent() -> ReActAgent:
    """Both workers must build an identical agent or Resume is rejected."""

    return ReActAgent(
        ChaosModel(),
        [append_note],
        config=AgentConfig(max_steps=4, max_tool_calls=4, max_wall_time_s=600.0),
    )


async def _open_journal(dsn: str) -> PostgresRunJournal:
    journal = PostgresRunJournal(dsn)
    await journal.open()
    await journal.migrate()
    return journal


async def run_worker(dsn: str, mode: str, run_id_file: Path, session_id: str) -> int:
    journal = await _open_journal(dsn)
    try:
        runtime = AgentRuntime(build_agent(), journal, lease_ttl_s=LEASE_TTL_S)
        try:
            if mode == "start":
                handle = await runtime.submit(
                    StartRun(
                        prompt="Record one audit note.",
                        session_id=session_id,
                        idempotency_key=f"chaos-{session_id}",
                    )
                )
                await asyncio.to_thread(_write_run_id, run_id_file, handle.run_id)
                await runtime.wait(handle.run_id, timeout_s=600.0)
                return 0

            run_id = await asyncio.to_thread(_read_run_id, run_id_file)
            if mode == "supervise":
                # Worker B does not use run_id to act, only to know when to
                # stop. The supervisor finds the orphan by itself.
                supervisor = RunSupervisor(runtime, interval_s=0.5)
                deadline = time.monotonic() + 60.0
                while time.monotonic() < deadline:
                    sweep = await supervisor.sweep()
                    for supervised in sweep.runs:
                        print(
                            f"   supervisor: run={supervised.run_id[:12]} "
                            f"outcome={supervised.outcome} executions={supervised.executions}"
                        )
                    # A shared database may hold other orphans; only ours ends
                    # the demo. Until its lease expires it is not even listed.
                    if any(
                        supervised.run_id == run_id and supervised.outcome == "resumed"
                        for supervised in sweep.runs
                    ):
                        break
                    await asyncio.sleep(supervisor.interval_s)
                else:
                    raise TimeoutError("the supervisor never resumed the killed run")
                snapshot = await runtime.wait(run_id, timeout_s=120.0)
                answer = ""
                if snapshot.result is not None:
                    answer = str(snapshot.result.get("output") or "")
                print(f"{RESULT_PREFIX}status={snapshot.status} answer={answer}")
                return 0

            deadline = time.monotonic() + 30.0
            while True:
                try:
                    await runtime.submit(ResumeRun(run_id=run_id))
                    break
                except RuntimeConflict as exc:
                    # The dead worker's fencing lease must expire before anyone
                    # else may commit facts for this Run. Waiting here is the
                    # design working, not a failure.
                    if time.monotonic() >= deadline:
                        raise
                    print(f"   waiting for the dead worker's lease to expire ({exc})")
                    await asyncio.sleep(0.5)
            snapshot = await runtime.wait(run_id, timeout_s=120.0)
            answer = ""
            if snapshot.result is not None:
                answer = str(snapshot.result.get("output") or "")
            print(f"{RESULT_PREFIX}status={snapshot.status} answer={answer}")
            return 0
        finally:
            await runtime.close()
    finally:
        await journal.close()


async def _read_events(journal: PostgresRunJournal, run_id: str) -> tuple[StoredRunEvent, ...]:
    try:
        return await journal.read(run_id)
    except RunNotFoundError:
        return ()


async def _await_kind(
    journal: PostgresRunJournal,
    run_id: str,
    kind: RunEventKind,
    *,
    timeout_s: float,
) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if any(event.kind is kind for event in await _read_events(journal, run_id)):
            return True
        await asyncio.sleep(0.1)
    return False


def _write_run_id(run_id_file: Path, run_id: str) -> None:
    run_id_file.write_text(run_id, encoding="utf-8")


def _read_run_id(run_id_file: Path) -> str:
    if not run_id_file.is_file():
        return ""
    return run_id_file.read_text(encoding="utf-8").strip()


def _read_attempts(effects: Path) -> list[str]:
    return (effects / "attempts.tsv").read_text(encoding="utf-8").splitlines()


async def _await_run_id(run_id_file: Path, *, timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = await asyncio.to_thread(_read_run_id, run_id_file)
        if value:
            return value
        await asyncio.sleep(0.1)
    raise TimeoutError("the first worker never reported a run id")


def _spawn_worker(
    mode: str,
    run_id_file: Path,
    session_id: str,
    effects: Path,
) -> subprocess.Popen[str]:
    environment = dict(os.environ)
    environment[EFFECTS_ENV] = str(effects)
    return subprocess.Popen(
        [
            sys.executable,
            os.path.abspath(__file__),
            "--worker",
            mode,
            "--run-id-file",
            str(run_id_file),
            "--session-id",
            session_id,
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _print_chain(events: tuple[StoredRunEvent, ...], *, title: str) -> None:
    print(f"\n{title}")
    for event in events:
        marker = " [safe checkpoint]" if event.safe_checkpoint else ""
        print(f"   {event.sequence:>3}  {event.kind.value:<26}{marker}")


async def orchestrate(dsn: str, effects: Path, *, via_supervisor: bool = False) -> int:
    session_id = f"chaos-{uuid.uuid4().hex[:12]}"
    run_id_file = effects / "run_id.txt"
    journal = await _open_journal(dsn)
    first: subprocess.Popen[str] | None = None
    try:
        print("=" * 68)
        print("STEP 1  start a Run in worker A")
        print("=" * 68)
        first = _spawn_worker("start", run_id_file, session_id, effects)
        run_id = await _await_run_id(run_id_file, timeout_s=60.0)
        print(f"   run_id  = {run_id}")
        print(f"   pid     = {first.pid}")

        print("\n" + "=" * 68)
        print("STEP 2  wait until tool_started is DURABLE, then SIGKILL worker A")
        print("=" * 68)
        if not await _await_kind(
            journal, run_id, RunEventKind.TOOL_STARTED, timeout_s=60.0
        ):
            print("   tool_started never committed; aborting")
            return 1
        print("   tool_started committed -> a side effect may already exist")
        _print_chain(
            await _read_events(journal, run_id),
            title="   durable facts before the crash:",
        )

        os.kill(first.pid, signal.SIGKILL)
        first.wait(timeout=30)
        print(f"\n   worker A killed (exit={first.returncode}, -9 = SIGKILL)")

        snapshot = await journal.load(run_id)
        print(
            f"   durable state now: state={snapshot.state.value} "
            f"pending={list(snapshot.pending)}"
        )

        print("\n" + "=" * 68)
        if via_supervisor:
            print("STEP 3  worker B runs a RunSupervisor; it finds and resumes the orphan")
        else:
            print("STEP 3  worker B resumes from durable facts alone")
        print("=" * 68)
        second = _spawn_worker(
            "supervise" if via_supervisor else "resume", run_id_file, session_id, effects
        )
        output, _ = second.communicate(timeout=180)
        for line in output.splitlines():
            print(f"   {line}")
        if second.returncode != 0:
            print(f"   worker B failed (exit={second.returncode})")
            return 1

        print("\n" + "=" * 68)
        print("STEP 4  verify the crash left an intact, append-only history")
        print("=" * 68)
        events = await _read_events(journal, run_id)
        verify_event_chain(events)
        final = await journal.load(run_id)
        _print_chain(events, title="   full durable chain:")
        print(f"\n   hash chain           : verified over {len(events)} events")
        print(f"   executions           : {len(final.executions)} (crash forced a new one)")
        print(f"   status / stop_reason : {final.status} / {final.stop_reason}")
        answer = str(final.result.get("output")) if final.result else ""
        print(f"   final answer         : {answer}")

        attempts = await asyncio.to_thread(_read_attempts, effects)
        print("\n   tool side effects actually performed:")
        for line in attempts:
            attempt, key, marker = line.split("\t")
            print(f"      attempt={attempt}  idempotency_key={key}  marker={marker}")
        print(
            "\n   Both attempts share one idempotency key, so a real service can\n"
            "   dedupe them. That is what `idempotent=True` buys; a tool without\n"
            "   it would stop here and wait for an operator instead."
        )
        return 0
    finally:
        if first is not None and first.poll() is None:
            first.kill()
        await journal.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", choices=("start", "resume", "supervise"))
    parser.add_argument("--run-id-file")
    parser.add_argument("--session-id")
    parser.add_argument(
        "--supervisor",
        action="store_true",
        help="let a RunSupervisor discover and resume the killed run",
    )
    arguments = parser.parse_args()

    dsn = os.getenv("REACT_AGENT_POSTGRES_DSN") or os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        print(
            "Set REACT_AGENT_POSTGRES_DSN to a PostgreSQL 16+ database first.",
            file=sys.stderr,
        )
        return 2

    if arguments.worker:
        assert arguments.run_id_file and arguments.session_id
        return asyncio.run(
            run_worker(
                dsn,
                arguments.worker,
                Path(arguments.run_id_file),
                arguments.session_id,
            )
        )

    effects = Path(os.environ[EFFECTS_ENV]) if EFFECTS_ENV in os.environ else None
    if effects is None:
        import tempfile

        with tempfile.TemporaryDirectory(prefix="react-agent-chaos-") as directory:
            os.environ[EFFECTS_ENV] = directory
            return asyncio.run(
                orchestrate(dsn, Path(directory), via_supervisor=arguments.supervisor)
            )
    effects.mkdir(parents=True, exist_ok=True)
    return asyncio.run(orchestrate(dsn, effects, via_supervisor=arguments.supervisor))


if __name__ == "__main__":
    raise SystemExit(main())
