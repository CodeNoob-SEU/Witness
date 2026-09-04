from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

import pytest

from examples.runtime_debug_demo.buggy_pricing import reproduce
from react_agent.debug_evidence import (
    DebugEvidenceError,
    DebugLifecycleError,
    DebugObservationError,
    generate_debug_evidence,
    seal_debug_observation,
    verify_debug_observation,
)
from react_agent.events import (
    GENESIS_HASH,
    EventHashError,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    StoredRunEvent,
    canonical_json,
)

_RUN_ID = "debug-evidence-run"
_SESSION_ID = "debug-evidence-session"
_EXECUTION_ID = "debug-evidence-execution"
_AGENT_REVISION = "agent-revision"
_TOOL_MANIFEST_HASH = "tool-manifest"


def _launch_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_kind": "session",
        "action": "launch",
        "debug_session_id": "debug-session-1",
        "state": "stopped",
        "reproduction": {
            "command": [
                "python",
                "examples/runtime_debug_demo/buggy_pricing.py",
            ],
            "cwd": ".",
        },
    }


def _stack_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_kind": "stack",
        "action": "stack",
        "debug_session_id": "debug-session-1",
        "state": "stopped",
        "stop_id": 1,
        "exception": {
            "type": "ZeroDivisionError",
            "message": "float division by zero",
            "path": "examples/runtime_debug_demo/buggy_pricing.py",
            "line": 29,
            "function": "unit_price",
        },
    }


def _variables_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_kind": "variables",
        "action": "variables",
        "debug_session_id": "debug-session-1",
        "state": "stopped",
        "stop_id": 1,
        "selected_frame": {
            "frame_index": 1,
            "function": "price_order",
            "path": "examples/runtime_debug_demo/buggy_pricing.py",
            "line": 40,
            "in_workspace": True,
            "selection_rule_version": "workspace-frame-v1",
            "selection_reasons": [
                "workspace_source",
                "user_frame",
            ],
        },
        "variables": [
            {
                "scope": "locals",
                "name": "subtotal",
                "type": "float",
                "value": "99.0",
                "redacted": False,
                "truncated": False,
            },
            {
                "scope": "locals",
                "name": "item_count",
                "type": "int",
                "value": "0",
                "redacted": False,
                "truncated": False,
            },
            {
                "scope": "locals",
                "name": "billable_items",
                "type": "list",
                "value": "[]",
                "redacted": False,
                "truncated": False,
            },
        ],
    }


def _stop_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_kind": "session",
        "action": "stop",
        "debug_session_id": "debug-session-1",
        "state": "terminated",
        "debuggee_exit": {
            "status": "terminated",
            "exit_code": 1,
            "signal": None,
        },
    }


def _message(call_id: str, tool_name: str, observation: Mapping[str, object]) -> dict[str, Any]:
    return {
        "role": "tool",
        "call_id": call_id,
        "name": tool_name,
        "content": json.dumps(
            {
                "ok": True,
                "data": dict(observation),
                "meta": {"truncated": False},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "is_error": False,
        "executed": True,
        "cached": False,
        "duration_ms": 1.0,
    }


def _debug_call_drafts(
    call_number: int,
    tool_name: str,
    observation: Mapping[str, object],
    *,
    include_started: bool = True,
    reused: bool = False,
    truncate_projection: bool = False,
    private_observation: Mapping[str, object] | None = None,
) -> list[tuple[str, RunEventDraft]]:
    call_key = f"s{call_number}:t0"
    call_id = f"call-{call_number}"
    private_call = {"id": call_id, "name": tool_name, "arguments": "{}"}
    public = {"tool_call_id": call_id, "tool_name": tool_name}
    drafts = [
        (
            f"tool:{call_key}:planned",
            RunEventDraft(
                kind=RunEventKind.TOOL_PLANNED,
                privacy=PrivacyClass.PRIVATE,
                step=call_number,
                call_key=call_key,
                data=public,
                checkpoint={"call": private_call},
                safe_checkpoint=True,
                tool_calls_delta=1,
            ),
        )
    ]
    if include_started:
        drafts.append(
            (
                f"tool:{call_key}:started",
                RunEventDraft(
                    kind=RunEventKind.TOOL_STARTED,
                    privacy=PrivacyClass.PRIVATE,
                    step=call_number,
                    call_key=call_key,
                    data=public,
                    checkpoint={"call": private_call},
                ),
            )
        )
    terminal_kind = RunEventKind.TOOL_REUSED if reused else RunEventKind.TOOL_COMPLETED
    terminal_message = _message(call_id, tool_name, observation)
    if truncate_projection:
        original_chars = len(str(terminal_message["content"]))
        terminal_message["content"] = json.dumps(
            {
                "ok": True,
                "data": {"preview": "bounded model projection"},
                "meta": {"truncated": True, "original_chars": original_chars},
            },
            separators=(",", ":"),
        )
    if reused:
        terminal_message["cached"] = True
        terminal_message["executed"] = False
    drafts.append(
        (
            f"tool:{call_key}:{terminal_kind.value}",
            RunEventDraft(
                kind=terminal_kind,
                privacy=PrivacyClass.PRIVATE,
                step=call_number,
                call_key=call_key,
                data={**public, "outcome": "completed", "is_error": False},
                checkpoint={
                    "message": terminal_message,
                    **(
                        {
                            "tool_private": {
                                "debug_observation": dict(private_observation)
                            }
                        }
                        if private_observation is not None
                        else {}
                    ),
                },
                tool_executions_delta=0 if reused else 1,
            ),
        )
    )
    return drafts


def _commit(drafts: Sequence[tuple[str, RunEventDraft]]) -> tuple[StoredRunEvent, ...]:
    events: list[StoredRunEvent] = []
    previous_hash = GENESIS_HASH
    previous_event_id: str | None = None
    for sequence, (operation_id, draft) in enumerate(drafts, start=1):
        event = StoredRunEvent.from_draft(
            draft,
            run_id=_RUN_ID,
            sequence=sequence,
            operation_id=operation_id,
            previous_hash=previous_hash,
            occurred_at=1_700_000_000.0 + sequence,
            causation_id=previous_event_id,
            session_id=_SESSION_ID,
            execution_id=_EXECUTION_ID,
            agent_revision=_AGENT_REVISION,
            tool_manifest_hash=_TOOL_MANIFEST_HASH,
        )
        events.append(event)
        previous_hash = event.event_hash
        previous_event_id = event.event_id
    return tuple(events)


def _golden_chain(
    *,
    launch_observation: Mapping[str, object] | None = None,
    stack_observation: Mapping[str, object] | None = None,
    stop_observation: Mapping[str, object] | None = None,
) -> tuple[StoredRunEvent, ...]:
    drafts: list[tuple[str, RunEventDraft]] = [
        (
            "run:started",
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                privacy=PrivacyClass.PRIVATE,
                session_id=_SESSION_ID,
                execution_id=_EXECUTION_ID,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
                data={"status": "running"},
                checkpoint={"transcript": []},
                safe_checkpoint=True,
            ),
        )
    ]
    sealed_stack = (
        dict(stack_observation)
        if stack_observation is not None
        else seal_debug_observation(_stack_observation())
    )
    drafts.extend(
        _debug_call_drafts(
            1,
            "python_debug_launch",
            (
                dict(launch_observation)
                if launch_observation is not None
                else seal_debug_observation(_launch_observation())
            ),
        )
    )
    drafts.extend(_debug_call_drafts(2, "python_debug_stack", sealed_stack))
    drafts.extend(
        _debug_call_drafts(
            3,
            "python_debug_variables",
            seal_debug_observation(_variables_observation()),
        )
    )
    drafts.extend(
        _debug_call_drafts(
            4,
            "python_debug_stop",
            (
                dict(stop_observation)
                if stop_observation is not None
                else seal_debug_observation(_stop_observation())
            ),
        )
    )
    drafts.append(
        (
            "run:completed",
            RunEventDraft(
                kind=RunEventKind.RUN_COMPLETED,
                data={"status": "completed", "stop_reason": "completed"},
                safe_checkpoint=True,
            ),
        )
    )
    return _commit(drafts)


def test_observation_seal_is_deterministic_and_mutations_fail_verification() -> None:
    left = seal_debug_observation(_stack_observation())
    right = seal_debug_observation(dict(reversed(list(_stack_observation().items()))))

    assert left == right
    assert verify_debug_observation(left) == left
    assert len(str(left["observation_sha256"])) == 64
    with pytest.raises(DebugObservationError, match="unsealed"):
        seal_debug_observation(left)

    changed = dict(left)
    changed_exception = dict(changed["exception"])  # type: ignore[arg-type]
    changed_exception["message"] = "changed after sealing"
    changed["exception"] = changed_exception
    with pytest.raises(DebugObservationError, match="mismatch"):
        verify_debug_observation(changed)

    partial = _stack_observation()
    partial["exception"] = {
        "type": "ZeroDivisionError",
        "message": "location unavailable from adapter",
    }
    assert verify_debug_observation(seal_debug_observation(partial))["exception"] == partial[
        "exception"
    ]


def test_generation_is_byte_stable_and_contains_only_recorded_runtime_facts() -> None:
    events = _golden_chain()

    artifacts = [generate_debug_evidence(events) for _ in range(5)]

    assert len({artifact.canonical_json for artifact in artifacts}) == 1
    assert len({artifact.markdown for artifact in artifacts}) == 1
    payload = json.loads(artifacts[0].canonical_json)
    evidence_digest = payload.pop("evidence_payload_sha256")
    assert hashlib.sha256(canonical_json(payload).encode()).hexdigest() == evidence_digest
    assert artifacts[0].evidence_payload_sha256 == evidence_digest
    assert payload["observed_failure"] == {
        "function": "unit_price",
        "line": 29,
        "message": "float division by zero",
        "path": "examples/runtime_debug_demo/buggy_pricing.py",
        "type": "ZeroDivisionError",
    }
    assert payload["selected_frame"]["function"] == "price_order"
    assert [(item["name"], item["value"]) for item in payload["locals"]] == [
        ("billable_items", "[]"),
        ("item_count", "0"),
        ("subtotal", "99.0"),
    ]
    assert payload["arguments"] == []
    assert payload["root_cause_evidence"]["fact_only"] is True
    assert "item_count = 0" in payload["root_cause_evidence"]["statement"]
    assert payload["reproduction"] == {
        "command": ["python", "examples/runtime_debug_demo/buggy_pricing.py"],
        "cwd": ".",
    }
    assert payload["debuggee_exit"] == {
        "exit_code": 1,
        "signal": None,
        "status": "terminated",
    }
    assert payload["selection_rule_version"] == "workspace-frame-v1"
    assert len(payload["selection_reasons"]) == 2
    assert payload["generation"] == {"debugger_calls": 0, "model_calls": 0}
    assert payload["journal"]["head_hash"] == events[-1].event_hash
    assert len(payload["timeline"]) == 12
    assert all(
        {
            "sequence",
            "event_id",
            "call_key",
            "action",
            "outcome",
            "event_hash",
            "observation_sha256",
        }
        <= set(entry)
        for entry in payload["timeline"]
    )
    assert "no model summarization was used" in artifacts[0].markdown
    assert "Report-generation model calls: `0`" in artifacts[0].markdown
    assert "Replay debugger calls: `0`" in artifacts[0].markdown
    assert "billable_items" in artifacts[0].markdown
    assert "## Reproduction" in artifacts[0].markdown
    assert "## Debuggee termination" in artifacts[0].markdown


def test_stale_event_hash_mismatch_fails_before_evidence_projection() -> None:
    events = _golden_chain()
    corrupted = (*events[:-1], replace(events[-1], event_hash="0" * 64))

    with pytest.raises(EventHashError, match="event hash mismatch"):
        generate_debug_evidence(corrupted)


def test_stale_observation_digest_mismatch_fails_with_valid_outer_chain() -> None:
    stale_digest = seal_debug_observation(_stack_observation())
    stale_digest["observation_sha256"] = "0" * 64
    events = _golden_chain(stack_observation=stale_digest)

    with pytest.raises(DebugObservationError, match="mismatch"):
        generate_debug_evidence(events)


def test_private_observation_replays_when_model_projection_was_truncated() -> None:
    launch = seal_debug_observation(_launch_observation())
    stack = seal_debug_observation(_stack_observation())
    variables = seal_debug_observation(_variables_observation())
    stop = seal_debug_observation(_stop_observation())
    drafts: list[tuple[str, RunEventDraft]] = [
        (
            "run:started",
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                data={"status": "running"},
                session_id=_SESSION_ID,
                execution_id=_EXECUTION_ID,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
            ),
        )
    ]
    for number, name, observation in (
        (1, "python_debug_launch", launch),
        (2, "python_debug_stack", stack),
        (3, "python_debug_variables", variables),
        (4, "python_debug_stop", stop),
    ):
        drafts.extend(
            _debug_call_drafts(
                number,
                name,
                observation,
                truncate_projection=True,
                private_observation=observation,
            )
        )

    artifacts = generate_debug_evidence(_commit(drafts))
    payload = json.loads(artifacts.canonical_json)

    assert payload["observed_failure"]["type"] == "ZeroDivisionError"
    assert payload["locals"][0]["name"] == "billable_items"
    assert sum(bool(entry["observation_sha256"]) for entry in payload["timeline"]) == 4


def test_successful_completion_without_started_event_fails_closed() -> None:
    stack = seal_debug_observation(_stack_observation())
    drafts: list[tuple[str, RunEventDraft]] = [
        (
            "run:started",
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                data={"status": "running"},
                session_id=_SESSION_ID,
                execution_id=_EXECUTION_ID,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
            ),
        )
    ]
    drafts.extend(
        _debug_call_drafts(
            1,
            "python_debug_stack",
            stack,
            include_started=False,
        )
    )
    events = _commit(drafts)

    with pytest.raises(DebugLifecycleError, match="must have a started event"):
        generate_debug_evidence(events)


def test_reused_observation_pairs_without_a_started_event() -> None:
    drafts: list[tuple[str, RunEventDraft]] = [
        (
            "run:started",
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                data={"status": "running"},
                session_id=_SESSION_ID,
                execution_id=_EXECUTION_ID,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
            ),
        )
    ]
    drafts.extend(
        _debug_call_drafts(
            1,
            "python_debug_launch",
            seal_debug_observation(_launch_observation()),
        )
    )
    drafts.extend(
        _debug_call_drafts(
            2,
            "python_debug_stack",
            seal_debug_observation(_stack_observation()),
            include_started=False,
            reused=True,
        )
    )
    drafts.extend(
        _debug_call_drafts(
            3,
            "python_debug_variables",
            seal_debug_observation(_variables_observation()),
        )
    )
    drafts.extend(
        _debug_call_drafts(
            4,
            "python_debug_stop",
            seal_debug_observation(_stop_observation()),
        )
    )
    artifacts = generate_debug_evidence(_commit(drafts))
    payload = json.loads(artifacts.canonical_json)

    assert any(entry["outcome"] == "reused" for entry in payload["timeline"])


def test_missing_runtime_evidence_is_not_replaced_with_an_inference() -> None:
    drafts: list[tuple[str, RunEventDraft]] = [
        (
            "run:started",
            RunEventDraft(
                kind=RunEventKind.RUN_STARTED,
                data={"status": "running"},
                session_id=_SESSION_ID,
                execution_id=_EXECUTION_ID,
                agent_revision=_AGENT_REVISION,
                tool_manifest_hash=_TOOL_MANIFEST_HASH,
            ),
        )
    ]
    drafts.extend(
        _debug_call_drafts(
            1,
            "python_debug_stop",
            seal_debug_observation(_stop_observation()),
        )
    )

    with pytest.raises(DebugEvidenceError, match="no exception evidence"):
        generate_debug_evidence(_commit(drafts))


def test_report_requires_reproduction_command_from_a_sealed_launch_observation() -> None:
    launch = _launch_observation()
    launch.pop("reproduction")
    events = _golden_chain(launch_observation=seal_debug_observation(launch))

    with pytest.raises(DebugEvidenceError, match="no normalized reproduction command"):
        generate_debug_evidence(events)


def test_report_requires_exit_status_from_a_sealed_terminal_observation() -> None:
    stop = _stop_observation()
    stop.pop("debuggee_exit")
    events = _golden_chain(stop_observation=seal_debug_observation(stop))

    with pytest.raises(DebugEvidenceError, match="no debuggee exit status"):
        generate_debug_evidence(events)


def test_golden_target_exposes_the_expected_caller_frame_locals() -> None:
    with pytest.raises(ZeroDivisionError) as caught:
        reproduce()

    traceback = caught.value.__traceback__
    price_frame = None
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == "price_order":
            price_frame = traceback.tb_frame
            break
        traceback = traceback.tb_next

    assert price_frame is not None
    assert price_frame.f_locals["item_count"] == 0
    assert price_frame.f_locals["billable_items"] == []
    assert price_frame.f_locals["subtotal"] == 99.0
