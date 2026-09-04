"""Resume orphaned runs automatically.

A worker that dies with ``kill -9``, or an execution that gave up on a
transient provider outage (``model_unavailable``), leaves a run that is not
terminal and whose writer lease has expired. Nothing in the durable design
requires a human to notice: :class:`RunSupervisor` periodically asks the
journal for such runs and submits :class:`ResumeRun` through an ordinary
:class:`AgentRuntime`, so every guarantee of Resume — fencing, idempotent
retry, fail-closed reconciliation, revision checks — applies unchanged.

The supervisor is deliberately conservative:

* it only lists runs recorded under this Agent's revision, so a run some
  other deployment owns is never even a candidate;
* it never touches a run another writer currently leases;
* it never resolves reconciliation, it only reports it — once per outcome;
* a run whose journal cannot be read is reported, not a reason to stop;
* it stops resuming a run after ``max_executions_per_run`` durable executions,
  so a permanently broken run cannot be retried forever;
* between resumes of the same run it backs off exponentially in-process.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .events import RunState
from .runtime import (
    AgentRuntime,
    ReconciliationRequired,
    ResumeRejected,
    ResumeRun,
    RuntimeConflict,
    RuntimeNotFound,
)

logger = logging.getLogger("react_agent.supervisor")

AttentionSink = Callable[["SupervisedRun"], Awaitable[None] | None]

# Outcomes that need a human or a code change; the supervisor reports them
# through ``on_attention`` and does not touch the run again this sweep.
ATTENTION_OUTCOMES = frozenset(
    {"needs_reconciliation", "resume_rejected", "resume_budget_exhausted", "unreadable"}
)


@dataclass(frozen=True, slots=True)
class SupervisedRun:
    """What one sweep decided for one orphaned run."""

    run_id: str
    outcome: str
    executions: int
    state: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class SupervisorSweep:
    """One pass over the journal's orphaned runs."""

    started_at: float
    duration_ms: float
    candidates: int
    runs: tuple[SupervisedRun, ...] = field(default=())

    @property
    def resumed(self) -> int:
        return sum(run.outcome == "resumed" for run in self.runs)

    @property
    def attention(self) -> tuple[SupervisedRun, ...]:
        return tuple(run for run in self.runs if run.outcome in ATTENTION_OUTCOMES)

    def to_json(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "duration_ms": self.duration_ms,
            "candidates": self.candidates,
            "resumed": self.resumed,
            "runs": [
                {
                    "run_id": run.run_id,
                    "outcome": run.outcome,
                    "executions": run.executions,
                    "state": run.state,
                    "detail": run.detail,
                }
                for run in self.runs
            ],
        }


@dataclass(slots=True)
class _RunBackoff:
    resumes: int = 0
    next_eligible_at: float = 0.0


class RunSupervisor:
    """Periodically Resume non-terminal runs whose writer lease has expired."""

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        interval_s: float = 5.0,
        max_executions_per_run: int = 8,
        batch_limit: int = 50,
        on_attention: AttentionSink | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        if max_executions_per_run < 2:
            raise ValueError("max_executions_per_run must allow at least one Resume")
        if batch_limit < 1:
            raise ValueError("batch_limit must be positive")
        self.runtime = runtime
        self.interval_s = interval_s
        self.max_executions_per_run = max_executions_per_run
        self.batch_limit = batch_limit
        self.on_attention = on_attention
        self._clock = clock
        self._backoff: dict[str, _RunBackoff] = {}
        self._reported: dict[str, str] = {}
        self._sweeps = 0
        self._last_sweep: SupervisorSweep | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    @property
    def sweeps(self) -> int:
        return self._sweeps

    @property
    def last_sweep(self) -> SupervisorSweep | None:
        return self._last_sweep

    async def sweep(self) -> SupervisorSweep:
        """Inspect every orphaned run once and Resume the ones that qualify."""

        started_at = time.time()
        started = self._clock()
        candidates = await self.runtime.journal.list_orphaned_runs(
            limit=self.batch_limit,
            agent_revision=self.runtime.agent_revision,
        )
        outcomes: list[SupervisedRun] = []
        for run_id in candidates:
            try:
                outcome = await self._supervise(run_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # One corrupt or unreadable run must not hide the others.
                outcome = SupervisedRun(run_id, "unreadable", 0, detail=type(exc).__name__)
            outcomes.append(outcome)
            if outcome.outcome in ATTENTION_OUTCOMES:
                await self._report(outcome)
        # Forget per-run state for runs the journal no longer lists.
        for cache in (self._backoff, self._reported):
            for run_id in tuple(cache):
                if run_id not in candidates:
                    cache.pop(run_id, None)
        sweep = SupervisorSweep(
            started_at=started_at,
            duration_ms=round((self._clock() - started) * 1000, 3),
            candidates=len(candidates),
            runs=tuple(outcomes),
        )
        self._sweeps += 1
        self._last_sweep = sweep
        if sweep.candidates:
            logger.info(
                "supervisor sweep: candidates=%d resumed=%d attention=%d",
                sweep.candidates,
                sweep.resumed,
                len(sweep.attention),
            )
        return sweep

    async def _supervise(self, run_id: str) -> SupervisedRun:
        try:
            snapshot = await self.runtime.load(run_id)
        except RuntimeNotFound:
            return SupervisedRun(run_id, "not_found", 0)
        executions = len(snapshot.executions)
        state = snapshot.state.value
        if snapshot.state is RunState.TERMINAL:
            return SupervisedRun(run_id, "terminal", executions, state)
        if snapshot.state is RunState.NEEDS_RECONCILIATION:
            return SupervisedRun(
                run_id,
                "needs_reconciliation",
                executions,
                state,
                detail=", ".join(sorted(snapshot.pending)),
            )
        if executions >= self.max_executions_per_run:
            return SupervisedRun(
                run_id,
                "resume_budget_exhausted",
                executions,
                state,
                detail=f"{executions} executions >= {self.max_executions_per_run}",
            )
        backoff = self._backoff.setdefault(run_id, _RunBackoff())
        now = self._clock()
        if now < backoff.next_eligible_at:
            return SupervisedRun(
                run_id,
                "backing_off",
                executions,
                state,
                detail=f"{backoff.next_eligible_at - now:.1f}s",
            )
        try:
            handle = await self.runtime.submit(ResumeRun(run_id=run_id))
        except ReconciliationRequired as exc:
            return SupervisedRun(run_id, "needs_reconciliation", executions, state, str(exc))
        except ResumeRejected as exc:
            return SupervisedRun(run_id, "resume_rejected", executions, state, str(exc))
        except RuntimeConflict as exc:
            # Another worker won the lease between the listing and our Resume.
            return SupervisedRun(run_id, "live_lease", executions, state, str(exc))
        if not handle.created:
            return SupervisedRun(run_id, "already_active", executions, state)
        backoff.resumes += 1
        backoff.next_eligible_at = now + self.interval_s * (2 ** min(backoff.resumes, 8))
        logger.info(
            "supervisor resumed run %s (execution %d, resume #%d)",
            run_id,
            executions + 1,
            backoff.resumes,
        )
        return SupervisedRun(
            run_id,
            "resumed",
            executions + 1,
            state,
            detail=handle.execution_id,
        )

    async def _report(self, run: SupervisedRun) -> None:
        if self._reported.get(run.run_id) == run.outcome:
            return
        self._reported[run.run_id] = run.outcome
        logger.warning(
            "supervisor needs attention: run=%s outcome=%s detail=%s",
            run.run_id,
            run.outcome,
            run.detail,
        )
        if self.on_attention is None:
            return
        try:
            result = self.on_attention(run)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            # A broken alert hook must not stop other runs from being resumed.
            logger.exception("supervisor attention hook failed for run %s", run.run_id)

    async def serve(self) -> None:
        """Sweep until :meth:`stop` is called. Journal outages are retried."""

        self._stop.clear()
        while not self._stop.is_set():
            try:
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("supervisor sweep failed; retrying after interval")
            try:
                async with asyncio.timeout(self.interval_s):
                    await self._stop.wait()
            except TimeoutError:
                continue

    def start(self) -> asyncio.Task[None]:
        if self._task is not None and not self._task.done():
            return self._task
        self._task = asyncio.create_task(self.serve(), name="react-supervisor")
        return self._task

    async def stop(self, *, grace_s: float = 5.0) -> None:
        """Stop the loop; a sweep in progress gets ``grace_s`` to finish."""

        self._stop.set()
        task = self._task
        if task is None:
            return
        self._task = None
        try:
            async with asyncio.timeout(grace_s):
                await asyncio.shield(task)
        except TimeoutError:
            task.cancel()
        except asyncio.CancelledError:
            task.cancel()
            raise
        try:
            await task
        except asyncio.CancelledError:
            pass


__all__ = [
    "ATTENTION_OUTCOMES",
    "RunSupervisor",
    "SupervisedRun",
    "SupervisorSweep",
]
