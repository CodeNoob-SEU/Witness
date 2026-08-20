"""Minimal structured ReAct example with a deterministic, offline tool."""

import asyncio
import os
import sys
from typing import Annotated, cast

from pydantic import Field

from react_agent import (
    AgentConfig,
    AgentEvent,
    ApiMode,
    OpenAIModel,
    ReActAgent,
    RunStatus,
    tool,
)


@tool(
    description="在本地计算订单的原价、折扣金额和实付金额; 不访问网络, 也不执行支付。",
    timeout_s=2.0,
    idempotent=True,
    parallel_safe=True,
)
def calculate_order_total(
    unit_price: Annotated[
        float,
        Field(gt=0, le=1_000_000, description="商品单价"),
    ],
    quantity: Annotated[
        int,
        Field(ge=1, le=100_000, description="购买数量"),
    ],
    discount_percent: Annotated[
        float,
        Field(ge=0, le=100, description="百分制折扣率, 例如 12.5 表示减免 12.5%"),
    ],
) -> dict[str, float]:
    """纯本地、确定性金额计算。"""

    subtotal = round(unit_price * quantity, 2)
    discount = round(subtotal * discount_percent / 100, 2)
    total = round(subtotal - discount, 2)
    return {
        "subtotal": subtotal,
        "discount": discount,
        "total": total,
    }


def print_event(event: AgentEvent) -> None:
    """Print safe progress metadata; events exclude prompts and tool arguments."""

    location = f" step={event.step}" if event.step is not None else ""
    tool_name = f" tool={event.tool_name}" if event.tool_name else ""
    print(f"[event] {event.kind.value}{location}{tool_name}", file=sys.stderr)


async def run() -> int:
    api_key = os.getenv("OPENAI_API_KEY")
    model_name = os.getenv("OPENAI_MODEL")
    if not api_key or not model_name:
        print(
            "请先设置 OPENAI_API_KEY 和 OPENAI_MODEL; 可选设置 "
            "OPENAI_BASE_URL、OPENAI_API_MODE。",
            file=sys.stderr,
        )
        return 2

    raw_api_mode = os.getenv("OPENAI_API_MODE", "responses")
    if raw_api_mode not in ("responses", "chat_completions"):
        print(
            "OPENAI_API_MODE 必须是 responses 或 chat_completions。",
            file=sys.stderr,
        )
        return 2
    api_mode = cast(ApiMode, raw_api_mode)

    async with OpenAIModel(
        model_name,
        api_mode=api_mode,
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL"),
        timeout=60.0,
        max_retries=2,
    ) as model:
        agent = ReActAgent(
            model,
            tools=[calculate_order_total],
            config=AgentConfig(
                max_steps=4,
                max_tool_calls=4,
                max_wall_time_s=60.0,
                max_concurrent_tools=2,
                max_tool_output_chars=4_000,
            ),
            event_sink=print_event,
        )
        result = await agent.run(
            "请务必调用 calculate_order_total 工具计算: 商品单价 19.9 元, "
            "购买 7 件, 折扣率 12.5%。最后用中文说明原价、优惠金额和实付金额。"
        )

    if result.output:
        print(result.output)
    else:
        print(
            f"Agent 未生成最终回答: status={result.status.value}, "
            f"reason={result.stop_reason.value}",
            file=sys.stderr,
        )
        if result.error:
            print(result.error, file=sys.stderr)

    print(
        f"[usage] input={result.usage.input_tokens} "
        f"output={result.usage.output_tokens} total={result.usage.total_tokens}",
        file=sys.stderr,
    )
    return 0 if result.status is RunStatus.COMPLETED else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
