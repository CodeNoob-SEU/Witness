import asyncio
import json
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from react_agent import ToolCall, ToolRegistry, tool


class NestedInput(BaseModel):
    label: str
    optional_note: str | None = None


def test_tool_schema_is_strict_at_every_object_level() -> None:
    @tool
    def inspect_record(
        record: NestedInput,
        limit: Annotated[int, Field(ge=1)] = 10,
    ) -> str:
        """Inspect one typed record with a bounded result limit."""

        return f"{record.label}:{limit}"

    schema = inspect_record.spec.parameters

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["record", "limit"]
    assert "default" not in json.dumps(schema)
    assert "title" not in json.dumps(schema)
    nested = schema["$defs"]["NestedInput"]
    assert nested["additionalProperties"] is False
    assert nested["required"] == ["label", "optional_note"]


@pytest.mark.asyncio
async def test_tool_timeout_is_a_recoverable_observation() -> None:
    @tool(timeout_s=0.01, idempotent=True)
    async def slow_lookup(value: str) -> str:
        """Perform a deliberately slow lookup for timeout testing."""

        await asyncio.sleep(1)
        return value

    result = await ToolRegistry([slow_lookup]).execute(
        ToolCall("call-timeout", "slow_lookup", '{"value":"x"}'),
        run_id="run-timeout",
        approval_handler=None,
        max_output_chars=1_000,
    )

    payload = json.loads(result.content)
    assert result.is_error is True
    assert result.executed is True
    assert payload["error"]["code"] == "TOOL_TIMEOUT"
    assert payload["error"]["retryable"] is True


@pytest.mark.asyncio
async def test_tool_output_is_bounded_and_remains_valid_json() -> None:
    @tool
    def large_result() -> str:
        """Return a large local value to exercise observation truncation."""

        return "数据" * 10_000

    result = await ToolRegistry([large_result]).execute(
        ToolCall("call-large", "large_result", "{}"),
        run_id="run-large",
        approval_handler=None,
        max_output_chars=512,
    )

    payload = json.loads(result.content)
    assert len(result.content) <= 512
    assert payload["ok"] is True
    assert payload["meta"]["truncated"] is True


@pytest.mark.asyncio
async def test_approval_policy_exception_fails_closed() -> None:
    invoked = False

    @tool(requires_approval=True)
    def write_value(value: str) -> str:
        """Write one value after explicit approval."""

        nonlocal invoked
        invoked = True
        return value

    def broken_policy(_request) -> bool:
        raise RuntimeError("policy backend unavailable")

    result = await ToolRegistry([write_value]).execute(
        ToolCall("call-write", "write_value", '{"value":"x"}'),
        run_id="run-write",
        approval_handler=broken_policy,
        max_output_chars=1_000,
    )

    assert invoked is False
    assert result.executed is False
    assert json.loads(result.content)["error"]["code"] == "APPROVAL_ERROR"


@pytest.mark.asyncio
async def test_approval_handler_cannot_mutate_executed_arguments() -> None:
    executed: list[dict[str, list[int]]] = []

    @tool(requires_approval=True)
    def write_values(payload: dict[str, list[int]]) -> str:
        """Record nested values after approval."""

        executed.append(payload)
        return "written"

    def approve_and_mutate(request) -> bool:
        request.arguments["payload"]["values"].append(999)
        return True

    result = await ToolRegistry([write_values]).execute(
        ToolCall(
            "call-write-values",
            "write_values",
            '{"payload":{"values":[1]}}',
        ),
        run_id="run-write-values",
        approval_handler=approve_and_mutate,
        max_output_chars=1_000,
    )

    assert result.is_error is False
    assert executed == [{"values": [1]}]
