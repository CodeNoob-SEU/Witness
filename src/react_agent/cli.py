"""Minimal command-line entrypoint for smoke-testing a configured endpoint."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from .agent import AgentConfig, ReActAgent
from .provider import OpenAIModel, ProviderCapabilities


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the ReAct agent with no application tools.")
    parser.add_argument("prompt", help="User request")
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL"))
    parser.add_argument(
        "--api-mode",
        choices=("responses", "chat_completions"),
        default=os.getenv("OPENAI_API_MODE", "responses"),
    )
    parser.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL"))
    parser.add_argument(
        "--allow-insecure-http",
        action="store_true",
        help="Allow a trusted non-loopback HTTP endpoint (unsafe on untrusted networks).",
    )
    parser.add_argument("--max-steps", type=int, default=8)
    parser.add_argument(
        "--compat",
        action="store_true",
        help="Omit strict/parallel fields for incomplete compatible endpoints.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    if not args.model:
        print("Missing model: pass --model or set OPENAI_MODEL.", file=sys.stderr)
        return 2
    capabilities = ProviderCapabilities(
        strict_tools=not args.compat,
        parallel_tool_calls=not args.compat,
        store_parameter=not args.compat,
        encrypted_reasoning_items=not args.compat,
        chat_stream_usage=not args.compat,
    )
    async with OpenAIModel(
        args.model,
        api_mode=args.api_mode,
        base_url=args.base_url,
        allow_insecure_http=args.allow_insecure_http,
        capabilities=capabilities,
    ) as model:
        agent = ReActAgent(model, config=AgentConfig(max_steps=args.max_steps))
        result = await agent.run(args.prompt)
    if result.output:
        print(result.output)
    if result.status.value != "completed":
        print(
            f"Agent stopped: status={result.status.value}, reason={result.stop_reason.value}",
            file=sys.stderr,
        )
        if result.error:
            print(result.error, file=sys.stderr)
        return 1
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parser().parse_args())))


if __name__ == "__main__":
    main()
