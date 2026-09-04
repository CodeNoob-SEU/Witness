"""End-to-end SWE-bench driver for the Witness durable runtime.

Subcommands (all read config from environment; see run.sh):

  start     --instance instance.json --session S --key K   -> prints run_id, blocks until terminal
  status    --run-id R                                     -> read-only snapshot summary (separate process)
  resume    --run-id R                                     -> new process takes over after a crash
  supervise [--run-id R]                                   -> RunSupervisor finds and resumes orphans itself
  report    --run-id R --out DIR                           -> journal integrity, replay, cost, patch export
  evaluate  --run-id R --out DIR                           -> SWE-bench FAIL_TO_PASS / PASS_TO_PASS check

Every run-affecting constant (instructions, tools, config, model id, governor)
is pinned below so that a fresh process reproduces the same agent revision and
tool manifest hash; otherwise Resume is rejected by design.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from react_agent import (
    AgentConfig,
    ContainerCommandRunner,
    ContextStrategy,
    OpenAIModel,
    ReActAgent,
    create_repository_tools,
)
from react_agent.context import ContextGovernor, FileContextSummaryStore, ModelContextCompressor
from react_agent.events import RunState, fold_events, verify_event_chain
from react_agent.postgres_journal import PostgresRunJournal
from react_agent.runtime import (
    AgentRuntime,
    ReconciliationRequired,
    ResolutionAction,
    ResolveRun,
    ResumeRejected,
    ResumeRun,
    RuntimeConflict,
    StartRun,
)
from react_agent.supervisor import RunSupervisor
from react_agent.telemetry import MetricCardinalityPolicy, create_telemetry
from react_agent.workspace import GitWorktreeWorkspace

ROOT = Path(os.environ["WITNESS_SWE_ROOT"]).resolve()
REPO = ROOT / "repo"
WORKTREES = ROOT / "worktrees"
SUMMARIES = ROOT / "context-summaries"
IMAGE = os.environ["WITNESS_SWE_IMAGE"]
DSN = os.environ["REACT_AGENT_POSTGRES_DSN"]
MODEL_NAME = os.environ.get("WITNESS_MODEL", "gpt-5.5")
REASONING_EFFORT = os.environ.get("WITNESS_REASONING_EFFORT", "high")
BASE_URL = os.environ["OPENAI_BASE_URL"]
WORKER_ID = os.environ.get("WITNESS_WORKER_ID") or f"harness-{os.getpid()}"
LEASE_TTL_S = 30.0

INSTRUCTIONS = """You are an expert Python software engineer fixing a bug in a real repository.

The repository is checked out in an isolated working tree. Use the tools to explore the code,
make a minimal, correct source change that resolves the issue, and verify it with the test
suite. Rules:
- Only modify source files under src/. Do NOT edit or add tests; hidden tests will judge you.
- Use edit_file for targeted changes; use write_file only for new files.
- Run the relevant existing tests with run_tests before and after your change.
- Use run_command only for exploration. Pass read_only=true for commands that change nothing
  (git status, ls, grep, python -c "import ..."): those are retried automatically after a crash;
  any other command needs an operator if the process dies while it runs.
- When done, reply with a short summary of the root cause and the change you made.
"""

CONFIG = AgentConfig(
    max_steps=60,
    max_tool_calls=200,
    max_wall_time_s=3600.0,
    max_concurrent_tools=4,
    max_tool_output_chars=30_000,
    # WITNESS_MAX_CONTEXT_CHARS=60000 forces Tier 2 generative compression
    # on the same instance (handoff §6.4); the default never triggered it.
    max_context_chars=int(os.environ.get("WITNESS_MAX_CONTEXT_CHARS", "400000")),
    context_strategy=ContextStrategy.TIERED,
    context_keep_recent_turns=3,
    context_summary_max_chars=12_000,
    parallel_tool_calls=True,
    repeated_action_limit=5,
)

# ---------------------------------------------------------------- tools ----
# The seven repository tools come from the runtime itself; the harness only
# decides *where* commands execute: inside the official SWE-bench image, with
# the isolated worktree bind-mounted at /testbed as the invoking user.

TOOLS = create_repository_tools(
    command_runner=ContainerCommandRunner(
        IMAGE,
        mount_path="/testbed",
        shell="/bin/bash",
        setup="source /opt/miniconda3/bin/activate testbed >/dev/null 2>&1",
        network="none",
    ),
    test_command="python -m pytest -p no:cacheprovider",
    command_timeout_s=270.0,
    test_timeout_s=840.0,
)

# ------------------------------------------------------------ composition ----


API_MODE = os.environ.get("WITNESS_API_MODE", "responses")
PROVIDER_LOG = ROOT / "logs" / "provider_http.ndjson"


async def _log_response(response) -> None:
    """Private diagnostic log: request/response metadata plus error bodies."""
    await response.aread()
    record: dict[str, Any] = {
        "ts": time.time(),
        "status": response.status_code,
        "url": str(response.request.url),
        "elapsed_ms": round(response.elapsed.total_seconds() * 1000) if response.elapsed else None,
        "request_bytes": len(response.request.content or b""),
        "response_bytes": len(response.content),
    }
    try:
        body = json.loads(response.request.content)
        record["request_input_types"] = [
            i.get("type") or i.get("role") for i in body.get("input", []) if isinstance(i, dict)
        ] if isinstance(body.get("input"), list) else None
        record["request_message_roles"] = [m.get("role") for m in body.get("messages", [])] if isinstance(body.get("messages"), list) else None
    except Exception:  # noqa: BLE001
        pass
    if response.status_code >= 400:
        record["error_body"] = response.text[:4000]
    else:
        try:
            payload = json.loads(response.content)
            record["response_output_types"] = [o.get("type") for o in payload.get("output", [])]
            record["has_encrypted_reasoning"] = any(
                o.get("type") == "reasoning" and o.get("encrypted_content") for o in payload.get("output", [])
            )
            record["usage"] = payload.get("usage", {}).get("output_tokens_details") or payload.get("usage", {}).get("completion_tokens_details")
        except Exception:  # noqa: BLE001
            pass
    PROVIDER_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PROVIDER_LOG.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def build_model() -> OpenAIModel:
    import httpx
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=os.environ["OPENAI_API_KEY"],
        base_url=BASE_URL,
        timeout=600.0,
        max_retries=2,
        http_client=httpx.AsyncClient(event_hooks={"response": [_log_response]}, timeout=600.0),
    )
    return OpenAIModel(
        MODEL_NAME,
        api_mode=API_MODE,  # type: ignore[arg-type]
        api_key=os.environ["OPENAI_API_KEY"],
        base_url=BASE_URL,
        timeout=600.0,
        max_retries=2,
        extra_body={"reasoning": {"effort": REASONING_EFFORT}} if API_MODE == "responses" else {"reasoning_effort": REASONING_EFFORT},
        client=client,
    )


def build_agent(model: OpenAIModel) -> ReActAgent:
    SUMMARIES.mkdir(parents=True, exist_ok=True)
    governor = ContextGovernor(
        strategy=ContextStrategy.TIERED,
        compressor=ModelContextCompressor(model),
        store=FileContextSummaryStore(SUMMARIES),
        keep_recent_turns=CONFIG.context_keep_recent_turns,
        max_summary_chars=CONFIG.context_summary_max_chars,
    )
    return ReActAgent(
        model, TOOLS, instructions=INSTRUCTIONS, config=CONFIG, context_governor=governor
    )


def build_workspace() -> GitWorktreeWorkspace:
    WORKTREES.mkdir(parents=True, exist_ok=True)
    return GitWorktreeWorkspace(REPO, WORKTREES)


def build_runtime(agent: ReActAgent, journal: PostgresRunJournal) -> AgentRuntime:
    # NoOp unless an OTel SDK/provider was initialised for this process
    # (run.sh does that via opentelemetry-instrument when WITNESS_OTEL=1).
    telemetry = create_telemetry(
        cardinality=MetricCardinalityPolicy(
            allowed_models=frozenset({MODEL_NAME}),
            allowed_tools=frozenset(tool.name for tool in TOOLS),
        )
    )
    return AgentRuntime(
        agent,
        journal,
        telemetry=telemetry,
        model_name=MODEL_NAME,
        workspace=build_workspace(),
        worker_id=WORKER_ID,
        lease_ttl_s=LEASE_TTL_S,
    )


def task_prompt(instance: dict[str, Any]) -> str:
    return (
        "Fix the following issue reported against this repository "
        f"({instance['repo']}, version {instance['version']}).\n\n"
        "<issue>\n" + instance["problem_statement"].strip() + "\n</issue>\n"
    )


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def wait_terminal(runtime: AgentRuntime, run_id: str, deadline_s: float):
    started = time.monotonic()
    while True:
        snapshot = await runtime.wait(run_id, timeout_s=60.0)
        log(
            f"state={snapshot.state.value} status={snapshot.status} seq={snapshot.last_sequence} "
            f"models={snapshot.counts.model_calls} tools={snapshot.counts.tool_executions}"
        )
        if snapshot.state is RunState.TERMINAL:
            return snapshot
        if snapshot.state is RunState.NEEDS_RECONCILIATION:
            return snapshot
        if time.monotonic() - started > deadline_s:
            return snapshot


def print_snapshot(snapshot) -> None:
    payload = {
        "run_id": snapshot.run_id,
        "session_id": snapshot.session_id,
        "execution_id": snapshot.execution_id,
        "state": snapshot.state.value,
        "status": snapshot.status,
        "stop_reason": snapshot.stop_reason,
        "last_sequence": snapshot.last_sequence,
        "last_hash": snapshot.last_hash,
        "last_step": snapshot.last_step,
        "counts": dataclasses.asdict(snapshot.counts),
        "usage": dataclasses.asdict(snapshot.usage),
        "pending": {k: {"kind": v.kind.value, "phase": getattr(v, "phase", None), "tool": getattr(v, "tool_name", None)} for k, v in snapshot.pending.items()},
        "executions": len(snapshot.executions),
        "model_attempts": len(snapshot.model_attempts),
        "safe_checkpoints": len(snapshot.safe_checkpoint_sequences),
        "costs": len(snapshot.costs),
        "output": (snapshot.result or {}).get("output") if snapshot.result else None,
    }
    print(json.dumps(payload, indent=1, default=str), flush=True)


# ------------------------------------------------------------- commands ----


async def cmd_start(args) -> int:
    instance = json.loads(Path(args.instance).read_text())
    model = build_model()
    async with PostgresRunJournal(DSN) as journal:
        await journal.migrate()
        runtime = build_runtime(build_agent(model), journal)
        log(f"agent_revision={runtime.agent_revision} manifest={runtime.agent.tool_manifest_hash}")
        handle = await runtime.submit(
            StartRun(prompt=task_prompt(instance), session_id=args.session, idempotency_key=args.key)
        )
        log(f"RUN_ID={handle.run_id} created={handle.created} execution={handle.execution_id}")
        Path(args.run_id_file).write_text(handle.run_id)
        snapshot = await wait_terminal(runtime, handle.run_id, CONFIG.max_wall_time_s + 300)
        print_snapshot(snapshot)
        await runtime.close()
    await model.aclose()
    return 0


async def cmd_status(args) -> int:
    async with PostgresRunJournal(DSN) as journal:
        snapshot = await journal.load(args.run_id)
        print_snapshot(snapshot)
    return 0


async def cmd_resume(args) -> int:
    model = build_model()
    async with PostgresRunJournal(DSN) as journal:
        runtime = build_runtime(build_agent(model), journal)
        log(f"agent_revision={runtime.agent_revision} manifest={runtime.agent.tool_manifest_hash}")
        before = await journal.load(args.run_id)
        log(f"before resume: state={before.state.value} seq={before.last_sequence} pending={list(before.pending)}")
        deadline = time.monotonic() + LEASE_TTL_S * 3
        attempts = 0
        while True:
            attempts += 1
            try:
                handle = await runtime.submit(ResumeRun(run_id=args.run_id))
                break
            except RuntimeConflict as exc:
                log(f"resume attempt {attempts}: RuntimeConflict: {exc}")
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(5)
            except ReconciliationRequired as exc:
                log(f"ReconciliationRequired: {exc}")
                snap = await journal.load(args.run_id)
                for key, rec in snap.tools.items():
                    log(f"  tool recovery {key}: {rec}")
                if not args.auto_resolve:
                    print_snapshot(snap)
                    return 3
                pending_keys = [k for k, v in snap.pending.items() if v.kind.value == "tool"]
                for key in pending_keys:
                    log(f"operator RETRY for call_key={key}")
                    await runtime.submit(ResolveRun(run_id=args.run_id, call_key=key, action=ResolutionAction.RETRY))
                handle = await runtime.submit(ResumeRun(run_id=args.run_id))
                break
            except ResumeRejected as exc:
                log(f"ResumeRejected: {exc}")
                return 4
        log(f"resumed run={handle.run_id} created={handle.created} execution={handle.execution_id} after {attempts} attempt(s)")
        snapshot = await wait_terminal(runtime, args.run_id, CONFIG.max_wall_time_s + 300)
        print_snapshot(snapshot)
        await runtime.close()
    await model.aclose()
    return 0


async def cmd_supervise(args) -> int:
    """Worker B as a supervisor: it is never told which run died."""

    model = build_model()
    async with PostgresRunJournal(DSN) as journal:
        runtime = build_runtime(build_agent(model), journal)
        log(f"agent_revision={runtime.agent_revision} manifest={runtime.agent.tool_manifest_hash}")
        supervisor = RunSupervisor(runtime, interval_s=5.0, max_executions_per_run=8)
        deadline = time.monotonic() + LEASE_TTL_S * 4
        resumed_run: str | None = None
        while time.monotonic() < deadline:
            sweep = await supervisor.sweep()
            for item in sweep.runs:
                log(f"supervisor: run={item.run_id} outcome={item.outcome} executions={item.executions} detail={item.detail}")
            hits = [item for item in sweep.runs if item.outcome == "resumed"
                    and (args.run_id is None or item.run_id == args.run_id)]
            if hits:
                resumed_run = hits[0].run_id
                break
            await asyncio.sleep(supervisor.interval_s)
        if resumed_run is None:
            log("supervisor found nothing to resume before the deadline")
            await runtime.close()
            await model.aclose()
            return 5
        snapshot = await wait_terminal(runtime, resumed_run, CONFIG.max_wall_time_s + 300)
        print_snapshot(snapshot)
        await runtime.close()
    await model.aclose()
    return 0


def _git(cwd: Path, *argv: str) -> str:
    return subprocess.run(["git", *argv], cwd=cwd, capture_output=True, text=True, check=True).stdout


async def cmd_report(args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"run_id": args.run_id}
    model = build_model()
    async with PostgresRunJournal(DSN) as journal:
        events = await journal.read(args.run_id)
        report["event_count"] = len(events)
        try:
            verify_event_chain(events)
            report["hash_chain_valid"] = True
        except Exception as exc:  # noqa: BLE001
            report["hash_chain_valid"] = False
            report["hash_chain_error"] = repr(exc)
        folded = fold_events(events)
        evicted = await journal.evict_snapshot(args.run_id)
        reloaded = await journal.load(args.run_id)
        report["snapshot_evicted"] = evicted
        report["rebuild_matches_fold"] = (
            reloaded.last_hash == folded.last_hash
            and reloaded.last_sequence == folded.last_sequence
            and reloaded.state == folded.state
            and reloaded.transcript == folded.transcript
        )
        kinds: dict[str, int] = {}
        for e in events:
            kinds[e.kind.value] = kinds.get(e.kind.value, 0) + 1
        report["event_kinds"] = kinds
        report["executions"] = [dict(x) if isinstance(x, dict) else str(x) for x in folded.executions] if folded.executions else []
        report["model_attempts"] = len(folded.model_attempts)
        report["state"] = folded.state.value
        report["status"] = folded.status
        report["stop_reason"] = folded.stop_reason
        report["counts"] = dataclasses.asdict(folded.counts)
        report["usage"] = dataclasses.asdict(folded.usage)
        report["costs"] = [dict(c) for c in folded.costs]
        report["safe_checkpoint_sequences"] = list(folded.safe_checkpoint_sequences)
        report["workspace"] = dict(folded.workspace) if folded.workspace else None
        report["workspace_anchor"] = dict(folded.workspace_anchor) if folded.workspace_anchor else None
        report["output"] = (folded.result or {}).get("output")
        # metadata-only event dump
        with (out / "events.public.ndjson").open("w") as fh:
            for e in events:
                fh.write(json.dumps({
                    "sequence": e.sequence, "kind": e.kind.value, "step": e.step,
                    "execution_id": e.execution_id, "call_key": e.call_key,
                    "safe_checkpoint": e.safe_checkpoint, "previous_hash": e.previous_hash,
                    "event_hash": e.event_hash, "occurred_at": e.occurred_at,
                    "data": {k: v for k, v in dict(e.data).items() if k in {
                        "tool_name", "outcome", "status", "stop_reason", "phase", "attempt",
                        "reason", "resume_reason", "previous_execution_id", "is_error",
                        "dirty", "diverged", "executed", "resume_policy", "tool_call_id",
                        "usage", "reasons", "duration_ms", "output_chars", "arguments_chars"}},
                }, default=str) + "\n")
        # public projection through the runtime follow() API (historical only)
        runtime = build_runtime(build_agent(model), journal)
        public = [ev async for ev in runtime.follow(args.run_id, live=False)]
        report["public_events"] = len(public)
        leaks = [ev.durable_sequence for ev in public if any(
            k in json.dumps(ev.public_data, default=str) for k in ("problem_statement", "old_string", os.environ["OPENAI_API_KEY"]))]
        report["public_projection_leaks"] = leaks
        await runtime.close()
        # workspace
        ws = build_workspace()
        session_id = folded.session_id
        wt = WORKTREES / session_id
        report["worktree_path"] = str(wt)
        report["worktree_exists"] = wt.exists()
        if wt.exists():
            patch = subprocess.run(["git", "diff", "--no-ext-diff"], cwd=wt, capture_output=True, text=True).stdout
            (out / "model_patch.diff").write_text(patch)
            report["patch_files"] = re.findall(r"^diff --git a/(\S+)", patch, re.M)
            report["untracked"] = subprocess.run(["git", "status", "--porcelain"], cwd=wt, capture_output=True, text=True).stdout.splitlines()
            try:
                anchor = folded.workspace_anchor or {}
                handle = ws.attach(session_id, baseline_revision=anchor.get("baseline_revision", "HEAD"))
                summary = ws.diff_summary(session_id)
                report["diff_summary"] = dataclasses.asdict(summary)
            except Exception as exc:  # noqa: BLE001
                report["diff_summary_error"] = repr(exc)
        report["primary_repo_clean"] = subprocess.run(["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True).stdout == ""
        report["primary_repo_head"] = _git(REPO, "rev-parse", "HEAD").strip()
        report["checkpoint_refs"] = _git(REPO, "for-each-ref", "refs/react-agent/").splitlines()
    await model.aclose()
    (out / "report.json").write_text(json.dumps(report, indent=1, default=str))
    print(json.dumps({k: v for k, v in report.items() if k not in {"executions", "costs"}}, indent=1, default=str))
    return 0


async def cmd_evaluate(args) -> int:
    instance = json.loads(Path(args.instance).read_text())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    async with PostgresRunJournal(DSN) as journal:
        snapshot = await journal.load(args.run_id)
    wt = WORKTREES / snapshot.session_id
    eval_dir = ROOT / "eval" / args.run_id
    if eval_dir.exists():
        shutil.rmtree(eval_dir)
    eval_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(wt, eval_dir, ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"))
    test_patch = instance["test_patch"]
    (eval_dir / "_test.patch").write_text(test_patch)
    applied = subprocess.run(["patch", "-p1", "--forward", "-i", "_test.patch"], cwd=eval_dir, capture_output=True, text=True)
    f2p = json.loads(instance["FAIL_TO_PASS"])
    p2p = json.loads(instance["PASS_TO_PASS"])
    files = sorted({t.split("::")[0] for t in f2p + p2p})
    runner = ContainerCommandRunner(
        IMAGE, mount_path="/testbed", shell="/bin/bash",
        setup="source /opt/miniconda3/bin/activate testbed >/dev/null 2>&1", max_output_chars=400_000,
    )
    outcome = await runner.run("python -m pytest -rA -p no:cacheprovider " + " ".join(files), cwd=eval_dir, timeout_s=900)
    result = {"exit_code": outcome.exit_code, "stdout": outcome.stdout, "stderr": outcome.stderr}
    statuses: dict[str, str] = {}
    for line in result["stdout"].splitlines():
        m = re.match(r"^(PASSED|FAILED|ERROR|XFAIL|XPASS|SKIPPED) (\S+)", line)
        if m:
            statuses[m.group(2)] = m.group(1)
    f2p_ok = [t for t in f2p if statuses.get(t) == "PASSED"]
    p2p_ok = [t for t in p2p if statuses.get(t) == "PASSED"]
    verdict = {
        "instance_id": instance["instance_id"],
        "test_patch_applied": applied.returncode == 0,
        "patch_output": applied.stdout[-500:] + applied.stderr[-500:],
        "pytest_exit_code": result["exit_code"],
        "FAIL_TO_PASS": {"passed": len(f2p_ok), "total": len(f2p), "failing": [t for t in f2p if t not in f2p_ok]},
        "PASS_TO_PASS": {"passed": len(p2p_ok), "total": len(p2p), "failing": [t for t in p2p if t not in p2p_ok]},
        "resolved": len(f2p_ok) == len(f2p) and len(p2p_ok) == len(p2p),
    }
    (out / "evaluation.json").write_text(json.dumps(verdict, indent=1))
    (out / "evaluation_pytest.log").write_text(result["stdout"] + "\n--- stderr ---\n" + result["stderr"])
    print(json.dumps(verdict, indent=1))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start"); s.add_argument("--instance", required=True); s.add_argument("--session", required=True); s.add_argument("--key", required=True); s.add_argument("--run-id-file", default=str(ROOT / "current_run_id"))
    s = sub.add_parser("status"); s.add_argument("--run-id", required=True)
    s = sub.add_parser("resume"); s.add_argument("--run-id", required=True); s.add_argument("--auto-resolve", action="store_true")
    s = sub.add_parser("supervise"); s.add_argument("--run-id", default=None)
    s = sub.add_parser("report"); s.add_argument("--run-id", required=True); s.add_argument("--out", required=True)
    s = sub.add_parser("evaluate"); s.add_argument("--run-id", required=True); s.add_argument("--instance", required=True); s.add_argument("--out", required=True)
    args = parser.parse_args()
    fn = {"start": cmd_start, "status": cmd_status, "resume": cmd_resume, "supervise": cmd_supervise, "report": cmd_report, "evaluate": cmd_evaluate}[args.cmd]
    return asyncio.run(fn(args))


if __name__ == "__main__":
    sys.exit(main())
