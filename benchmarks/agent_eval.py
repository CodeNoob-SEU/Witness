"""Run the agent evaluation suite and print a comparable report.

Against a real endpoint (needs `OPENAI_API_KEY` and `OPENAI_MODEL`)::

    uv run python benchmarks/agent_eval.py

Offline, to check the harness itself rather than a model::

    uv run python benchmarks/agent_eval.py --offline

Pricing is optional. Without a catalog every cost is reported as `unknown`
rather than as zero, which is the same rule the cost ledger follows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from react_agent import AgentConfig, OpenAIModel, Price, PricingCatalog
from react_agent.evals import WORKSPACE_SUITE, ReferenceWorkspaceModel, run_suite
from react_agent.provider import Model


def _offline_model_factory() -> Model:
    """The packaged deterministic fixture; proves the harness, not a model."""

    return ReferenceWorkspaceModel()


def _openai_model_factory() -> Model:
    model_name = os.environ["OPENAI_MODEL"]
    raw_mode = os.getenv("OPENAI_API_MODE", "responses")
    if raw_mode not in ("responses", "chat_completions"):
        raise SystemExit("OPENAI_API_MODE must be responses or chat_completions")
    return OpenAIModel(
        model_name,
        api_mode=raw_mode,  # type: ignore[arg-type]
        base_url=os.getenv("OPENAI_BASE_URL"),
    )


def _pricing_from_env(model_name: str) -> PricingCatalog | None:
    """Build a one-entry catalog from explicit per-million rates, if given."""

    input_rate = os.getenv("REACT_AGENT_EVAL_INPUT_PER_MILLION")
    output_rate = os.getenv("REACT_AGENT_EVAL_OUTPUT_PER_MILLION")
    if not input_rate or not output_rate:
        return None
    return PricingCatalog(
        "eval-catalog",
        (
            Price(
                provider="openai_compatible",
                model=model_name,
                version="eval",
                effective_from=datetime(2000, 1, 1, tzinfo=UTC),
                input_per_million=Decimal(input_rate),
                output_per_million=Decimal(output_rate),
            ),
        ),
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="Use the scripted model.")
    parser.add_argument("--json", dest="json_path", help="Also write the report as JSON.")
    parser.add_argument("--max-steps", type=int, default=8)
    arguments = parser.parse_args()

    if arguments.offline:
        factory = _offline_model_factory
        model_name = "offline-scripted"
    else:
        if not os.getenv("OPENAI_API_KEY") or not os.getenv("OPENAI_MODEL"):
            print(
                "Set OPENAI_API_KEY and OPENAI_MODEL, or pass --offline.",
                file=sys.stderr,
            )
            return 2
        factory = _openai_model_factory
        model_name = os.environ["OPENAI_MODEL"]

    report = await run_suite(
        WORKSPACE_SUITE,
        build_model=factory,
        config=AgentConfig(
            max_steps=arguments.max_steps,
            max_tool_calls=12,
            parallel_tool_calls=False,
        ),
        pricing=_pricing_from_env(model_name),
    )

    print(f"model: {model_name}\n")
    print(report.to_markdown())
    print("\nper-task detail:")
    for outcome in report.outcomes:
        print(f"  {outcome.task:<22} {outcome.stop_reason or '-':<18} {outcome.detail}")

    if arguments.json_path:
        _write_json(Path(arguments.json_path), report.to_json())
        print(f"\nwrote {arguments.json_path}")
    return 0 if report.passed == len(report.outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
