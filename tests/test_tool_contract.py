import asyncio
import json
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from react_agent import ToolCall, ToolError, ToolRegistry, tool


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


@pytest.mark.asyncio
async def test_tool_error_message_is_passed_through_but_other_exceptions_stay_opaque() -> None:
    @tool(idempotent=True)
    def expected_failure(path: str) -> str:
        """Fail with a message written for the model."""

        raise ToolError(f"{path} was not found.", code="NOT_FOUND")

    @tool(idempotent=True)
    def unexpected_failure(path: str) -> str:
        """Fail with an exception that may carry secrets."""

        raise RuntimeError("psql: password=hunter2 rejected")

    registry = ToolRegistry([expected_failure, unexpected_failure])
    expected = await registry.execute(
        ToolCall("call-1", "expected_failure", '{"path":"a.py"}'),
        run_id="run",
        approval_handler=None,
        max_output_chars=1_000,
    )
    unexpected = await registry.execute(
        ToolCall("call-2", "unexpected_failure", '{"path":"a.py"}'),
        run_id="run",
        approval_handler=None,
        max_output_chars=1_000,
    )

    expected_payload = json.loads(expected.content)
    assert expected.is_error is True and expected.executed is True
    assert expected_payload["error"] == {
        "code": "NOT_FOUND",
        "message": "a.py was not found.",
        "retryable": False,
    }
    unexpected_payload = json.loads(unexpected.content)
    assert unexpected_payload["error"]["code"] == "TOOL_EXCEPTION"
    assert "hunter2" not in unexpected.content


@pytest.mark.asyncio
async def test_tool_data_naming_the_serialization_code_is_not_an_error() -> None:
    @tool
    def describe_error_codes(code: str) -> dict[str, str]:
        """Return documentation for one framework error code."""

        return {"code": code, "meaning": "the encoder could not serialize a result"}

    registry = ToolRegistry([describe_error_codes])

    message = await registry.execute(
        ToolCall("call-1", "describe_error_codes", '{"code":"OUTPUT_SERIALIZATION"}'),
        run_id="run-1",
        approval_handler=None,
        max_output_chars=2_000,
    )

    payload = json.loads(message.content)
    assert payload["ok"] is True
    assert payload["data"]["code"] == "OUTPUT_SERIALIZATION"
    # The envelope succeeded; only the encoder failing may set is_error here.
    assert message.is_error is False
    assert message.executed is True


@pytest.mark.asyncio
async def test_unserializable_tool_result_is_still_reported_as_an_error() -> None:
    @tool
    def return_socket(label: str) -> object:
        """Return a value the JSON encoder cannot represent."""

        return {"label": label, "handle": object()}

    registry = ToolRegistry([return_socket])

    message = await registry.execute(
        ToolCall("call-2", "return_socket", '{"label":"x"}'),
        run_id="run-1",
        approval_handler=None,
        max_output_chars=2_000,
    )

    payload = json.loads(message.content)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "OUTPUT_SERIALIZATION"
    assert message.is_error is True


def test_call_resume_policy_hook_falls_back_to_the_static_policy() -> None:
    from react_agent import ConfigurationError, ToolResumePolicy

    def policy(arguments):
        if arguments["mode"] == "boom":
            raise RuntimeError("hook bug")
        if arguments["mode"] == "wrong-type":
            return "idempotent_retry"
        return (
            ToolResumePolicy.IDEMPOTENT_RETRY
            if arguments["mode"] == "read"
            else ToolResumePolicy.NEVER_RETRY
        )

    @tool(call_resume_policy=policy)
    def shell(mode: str) -> str:
        """Run something whose safety depends on its arguments."""

        return mode

    def decided(arguments: str) -> ToolResumePolicy:
        return shell.resume_policy_for(ToolCall("c", "shell", arguments))

    assert shell.has_call_resume_policy is True
    assert shell.resume_policy is ToolResumePolicy.REQUIRE_OPERATOR
    assert decided('{"mode":"read"}') is ToolResumePolicy.IDEMPOTENT_RETRY
    assert decided('{"mode":"write"}') is ToolResumePolicy.NEVER_RETRY
    # Hook failures, wrong return types and invalid arguments all fail closed.
    assert decided('{"mode":"boom"}') is ToolResumePolicy.REQUIRE_OPERATOR
    assert decided('{"mode":"wrong-type"}') is ToolResumePolicy.REQUIRE_OPERATOR
    assert decided('{"other":1}') is ToolResumePolicy.REQUIRE_OPERATOR
    assert decided("{") is ToolResumePolicy.REQUIRE_OPERATOR

    @tool
    def plain(mode: str) -> str:
        """A tool without a hook keeps its static policy."""

        return mode

    assert plain.has_call_resume_policy is False
    assert plain.resume_policy_for(ToolCall("c", "plain", "{")) is ToolResumePolicy.REQUIRE_OPERATOR

    with pytest.raises(ConfigurationError, match="call_resume_policy"):
        tool(call_resume_policy="not callable")(plain._fn)  # type: ignore[arg-type]
