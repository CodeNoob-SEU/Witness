"""Replay a finished run's per-step transcripts through the context governor.

Reads the `checkpoint(phase=before_model)` facts of one run from the
PostgreSQL journal — each carries the canonical transcript the Agent held at
that step — and runs `ContextGovernor.prepare` on every one of them with a
chosen hard limit, exactly as the live loop would have. This isolates the
governor from the model's own decisions: same inputs, new algorithm.

Two modes:
  --compressor fake   deterministic notes; measures calls, source chars,
                      cache hits, final chars, hard fallbacks (no network)
  --compressor model  a real OpenAI-compatible model for the notes; also
                      measures latency and keeps every rendered form

Usage (inside run.sh's environment):
  python replay_context.py --run-id <id> --hard-limit 60000 --compressor fake --out DIR
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from react_agent import (
    ContextCompression,
    ContextGovernor,
    ContextStrategy,
    OpenAIModel,
    Usage,
    create_repository_tools,
)
from react_agent.context import InMemoryContextSummaryStore, ModelContextCompressor
from react_agent.models import transcript_from_json
from react_agent.postgres_journal import PostgresRunJournal

sys.path.insert(0, str(Path(__file__).resolve().parent))
import swe_harness as harness  # noqa: E402  (module-level env contract)


class FakeCompressor:
    revision = "replay-fake-notes-v1"

    def __init__(self) -> None:
        self.calls = 0

    async def compress(self, request) -> ContextCompression:
        self.calls += 1
        seen = sum(1 for item in request.source)
        previous = request.previous_summary or "Hypothesis: (none yet)"
        summary = previous + f"\n- replay: folded {seen} items"
        return ContextCompression(summary[: request.max_summary_chars], Usage(1, 1, 2))


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--hard-limit", type=int, default=60_000)
    parser.add_argument("--compressor", choices=("fake", "model"), default="fake")
    parser.add_argument("--model", default=os.environ.get("WITNESS_COMPRESSION_MODEL", "gpt-5.4-mini"))
    parser.add_argument("--keep-recent-turns", type=int, default=harness.CONFIG.context_keep_recent_turns)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    tools = create_repository_tools()
    policies = {tool.name: tool.context_policy for tool in tools}
    specs = [tool.spec for tool in tools]

    model = None
    if args.compressor == "model":
        model = OpenAIModel(
            args.model,
            api_mode="responses",
            api_key=os.environ["OPENAI_API_KEY"],
            base_url=harness.BASE_URL,
            timeout=300.0,
            max_retries=2,
        )
        compressor: Any = ModelContextCompressor(model)
    else:
        compressor = FakeCompressor()
    governor = ContextGovernor(
        strategy=ContextStrategy.TIERED,
        compressor=compressor,
        store=InMemoryContextSummaryStore(),
        keep_recent_turns=args.keep_recent_turns,
        max_summary_chars=harness.CONFIG.context_summary_max_chars,
    )

    rows: list[dict[str, Any]] = []
    forms: list[dict[str, Any]] = []
    async with PostgresRunJournal(os.environ["REACT_AGENT_POSTGRES_DSN"]) as journal:
        events = await journal.read(args.run_id)
    checkpoints = [
        event
        for event in events
        if event.kind.value == "checkpoint"
        and event.data.get("phase") == "before_model"
        and event.data.get("attempt") == 1
        and event.checkpoint is not None
        and isinstance(event.checkpoint.get("transcript"), (list, tuple))
    ]
    print(f"run {args.run_id}: {len(checkpoints)} steps, hard_limit={args.hard_limit}", flush=True)
    for event in checkpoints:
        transcript = transcript_from_json(tuple(event.checkpoint["transcript"]))
        started = time.monotonic()
        projection = await governor.prepare(
            transcript,
            instructions=harness.INSTRUCTIONS,
            tool_specs=specs,
            tool_policies=policies,
            hard_limit=args.hard_limit,
        )
        elapsed = time.monotonic() - started
        report = projection.report
        row = {
            "step": event.step,
            "items": len(transcript),
            "input_chars": report.input_chars,
            "deterministic_chars": report.deterministic_chars,
            "final_chars": report.final_chars,
            "evictions": len(report.evictions),
            "compression_calls": report.compression_calls,
            "cache_hit": report.compression_cache_hit,
            "source_chars": report.compression_source_chars,
            "hard_fallback": report.hard_fallback,
            "hard_dropped_items": report.hard_dropped_items,
            "overflow": report.overflow,
            "compression_error": report.compression_error,
            "compression_tokens": report.compression_usage.total_tokens,
            "elapsed_s": round(elapsed, 3),
        }
        rows.append(row)
        first = projection.transcript[0]
        if getattr(first, "content", "").startswith("[working state"):
            forms.append({"step": event.step, "form": first.content})
        print(
            f"step {event.step:>3} input={report.input_chars:>7} final={report.final_chars:>6} "
            f"calls={report.compression_calls} cache={int(report.compression_cache_hit)} "
            f"src={report.compression_source_chars:>6} hard={int(report.hard_fallback)} "
            f"err={report.compression_error} {elapsed:6.1f}s",
            flush=True,
        )
    if model is not None:
        await model.aclose()

    compressing = [row for row in rows if row["compression_calls"] or row["cache_hit"]]
    summary = {
        "run_id": args.run_id,
        "hard_limit": args.hard_limit,
        "compressor": args.compressor,
        "model": args.model if args.compressor == "model" else None,
        "steps": len(rows),
        "steps_over_budget": len(compressing),
        "compression_calls": sum(row["compression_calls"] for row in rows),
        "cache_hits": sum(row["cache_hit"] for row in rows),
        "hard_fallbacks": sum(row["hard_fallback"] for row in rows),
        "overflows": sum(row["overflow"] for row in rows),
        "errors": sum(1 for row in rows if row["compression_error"]),
        "source_chars_max": max((row["source_chars"] for row in rows), default=0),
        "source_chars_mean": round(statistics.mean(row["source_chars"] for row in compressing), 1)
        if compressing
        else 0,
        "final_chars_max": max(row["final_chars"] for row in rows),
        "compression_seconds_total": round(sum(row["elapsed_s"] for row in compressing), 1),
        "compression_seconds_mean": round(statistics.mean(row["elapsed_s"] for row in compressing), 1)
        if compressing
        else 0,
        "compression_tokens_total": sum(row["compression_tokens"] for row in rows),
    }
    (out / "replay_steps.json").write_text(json.dumps(rows, indent=1))
    (out / "replay_summary.json").write_text(json.dumps(summary, indent=1))
    (out / "replay_forms.json").write_text(json.dumps(forms, indent=1, ensure_ascii=False))
    print(json.dumps(summary, indent=1), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
