"""Run the real MCP -> DAP -> debugpy golden demo and export verified evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, cast

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult

from react_agent.debug_event_log import load_debug_event_log
from react_agent.debug_evidence import (
    DebugEvidenceArtifacts,
    DebugObservationError,
    generate_debug_evidence,
)
from react_agent.debug_tools import create_python_debug_tools
from react_agent.events import (
    EventHashError,
    RunEventKind,
    StoredRunEvent,
    canonical_json,
    compute_event_hash,
    verify_event_chain,
)
from react_agent.runtime_debugger import PythonRuntimeDebugger

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
TARGET = Path("examples/runtime_debug_demo/buggy_pricing.py")
EXPECTED_TOOLS = (
    "python_debug_launch",
    "python_debug_set_breakpoints",
    "python_debug_control",
    "python_debug_stack",
    "python_debug_select_frame",
    "python_debug_variables",
    "python_debug_stop",
)
EXPECTED_LOCALS = {"billable_items": "[]", "item_count": "0", "subtotal": "99.0"}
EXPECTED_TOOL_FIELDS = {
    "python_debug_launch": {
        "program",
        "args",
        "breakpoints",
        "exception_policy",
        "stop_on_entry",
        "wait_timeout_s",
    },
    "python_debug_set_breakpoints": {"debug_session_id", "file", "lines"},
    "python_debug_control": {
        "debug_session_id",
        "action",
        "stop_id",
        "wait_timeout_s",
    },
    "python_debug_stack": {"debug_session_id", "stop_id", "levels"},
    "python_debug_select_frame": {"debug_session_id", "stop_id", "frame_index"},
    "python_debug_variables": {
        "debug_session_id",
        "stop_id",
        "scope",
        "max_variables",
        "max_value_chars",
    },
    "python_debug_stop": {"debug_session_id"},
}


@dataclass(frozen=True, slots=True)
class GoldenDemoRun:
    """In-memory outputs from one end-to-end demo execution."""

    result: Mapping[str, Any]
    evidence_json: str
    evidence_markdown: str


def _package_version(distribution: str) -> str:
    try:
        return version(distribution)
    except PackageNotFoundError:
        return "unknown"


def _resolve_path(path: Path, *, strict: bool) -> Path:
    return path.expanduser().resolve(strict=strict)


def _breakpoint_line(workspace: Path) -> int:
    source = (workspace / TARGET).read_text(encoding="utf-8").splitlines()
    matches = [
        line_number
        for line_number, line in enumerate(source, start=1)
        if line.strip() == "return unit_price(subtotal, item_count)"
    ]
    if len(matches) != 1:
        raise RuntimeError("golden target must contain one unit_price call site")
    return matches[0]


def _structured(result: object, tool_name: str) -> dict[str, Any]:
    if not isinstance(result, CallToolResult):
        raise RuntimeError(f"{tool_name} returned a non-terminal MCP result")
    if result.is_error:
        diagnostics = [getattr(item, "text", "") for item in result.content]
        raise RuntimeError(f"{tool_name} failed: {' '.join(diagnostics)}")
    content = result.structured_content
    if not isinstance(content, dict):
        raise RuntimeError(f"{tool_name} returned no structuredContent object")
    return cast(dict[str, Any], content)


def _process_snapshot() -> dict[int, str] | None:
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    processes: dict[int, str] = {}
    for line in completed.stdout.splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        pid = int(fields[0])
        if pid != os.getpid():
            processes[pid] = fields[1]
    return processes


def _owned_processes(
    snapshot: Mapping[int, str] | None,
    *,
    token: str,
    baseline: Mapping[int, str] | None,
) -> dict[int, str]:
    if snapshot is None or baseline is None:
        return {}
    markers = (token, "react_agent.debug_mcp", "debugpy.adapter")
    return {
        pid: command
        for pid, command in snapshot.items()
        if pid not in baseline and any(marker in command for marker in markers)
    }


async def _wait_for_no_owned_processes(
    *,
    token: str,
    baseline: Mapping[int, str] | None,
    timeout_s: float = 5.0,
) -> dict[int, str]:
    if baseline is None:
        return {}
    deadline = asyncio.get_running_loop().time() + timeout_s
    remaining: dict[int, str] = {}
    while True:
        remaining = _owned_processes(
            _process_snapshot(),
            token=token,
            baseline=baseline,
        )
        if not remaining or asyncio.get_running_loop().time() >= deadline:
            return remaining
        await asyncio.sleep(0.05)


def _rechain_with_stale_observation_digest(
    events: Sequence[StoredRunEvent],
) -> tuple[StoredRunEvent, ...]:
    """Recompute the outer chain while leaving one inner digest stale."""

    original = list(events)
    target_index: int | None = None
    replacement_checkpoint: dict[str, Any] | None = None
    for index, event in enumerate(original):
        if event.kind is not RunEventKind.TOOL_COMPLETED or event.checkpoint is None:
            continue
        checkpoint = json.loads(canonical_json(event.checkpoint))
        message = checkpoint.get("message")
        if not isinstance(message, dict) or not isinstance(message.get("content"), str):
            continue
        envelope = json.loads(message["content"])
        observation = envelope.get("data") if isinstance(envelope, dict) else None
        if not isinstance(observation, dict) or "observation_sha256" not in observation:
            continue
        observation["observation_sha256"] = "0" * 64
        message["content"] = json.dumps(
            envelope,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        target_index = index
        replacement_checkpoint = checkpoint
        break
    if target_index is None or replacement_checkpoint is None:
        raise RuntimeError("debug chain contains no sealed completion observation")

    rechained = original[:target_index]
    previous_hash = rechained[-1].event_hash if rechained else "0" * 64
    for index in range(target_index, len(original)):
        checkpoint = replacement_checkpoint if index == target_index else original[index].checkpoint
        unhashed = replace(
            original[index],
            checkpoint=checkpoint,
            previous_hash=previous_hash,
            event_hash="",
        )
        event = replace(unhashed, event_hash=compute_event_hash(unhashed))
        rechained.append(event)
        previous_hash = event.event_hash
    verify_event_chain(rechained)
    return tuple(rechained)


def _consistency_mismatch_checks(events: Sequence[StoredRunEvent]) -> dict[str, bool]:
    stale_event_hash_mismatch_detected = False
    corrupted_hash = (*events[:-1], replace(events[-1], event_hash="0" * 64))
    try:
        generate_debug_evidence(corrupted_hash)
    except EventHashError:
        stale_event_hash_mismatch_detected = True

    stale_observation_digest_mismatch_detected = False
    valid_outer_chain = _rechain_with_stale_observation_digest(events)
    try:
        generate_debug_evidence(valid_outer_chain)
    except DebugObservationError:
        stale_observation_digest_mismatch_detected = True
    return {
        "stale_event_hash_mismatch_detected": stale_event_hash_mismatch_detected,
        "stale_observation_digest_mismatch_detected": (
            stale_observation_digest_mismatch_detected
        ),
    }


def _required_evidence_coverage(payload: Mapping[str, Any]) -> float:
    required = (
        bool(payload.get("observed_failure")),
        bool(payload.get("reproduction")),
        bool(payload.get("selected_frame")),
        bool(payload.get("locals")),
        bool(payload.get("debuggee_exit")),
        bool(payload.get("timeline")),
        bool(cast(Mapping[str, Any], payload.get("journal", {})).get("head_hash")),
        bool(payload.get("evidence_payload_sha256")),
    )
    return sum(required) / len(required)


def _schema_without_titles(value: object) -> object:
    """Remove non-semantic Pydantic annotations before adapter comparison."""

    if isinstance(value, dict):
        return {
            key: _schema_without_titles(item)
            for key, item in value.items()
            if key not in {"title", "default"}
        }
    if isinstance(value, list):
        return [_schema_without_titles(item) for item in value]
    return value


def _schema_audit(
    tools: Sequence[object],
    agent_schemas: Mapping[str, Mapping[str, Any]],
) -> tuple[bool, str]:
    serialized: list[dict[str, object]] = []
    advertised_schemas: dict[str, Mapping[str, Any]] = {}
    for raw_tool in tools:
        name = getattr(raw_tool, "name", None)
        schema = getattr(raw_tool, "input_schema", None)
        if not isinstance(name, str) or not isinstance(schema, dict):
            continue
        advertised_schemas[name] = schema
        serialized.append({"name": name, "input_schema": schema})
    digest = hashlib.sha256(canonical_json(serialized).encode()).hexdigest()
    names_match = set(advertised_schemas) == set(EXPECTED_TOOL_FIELDS) == set(agent_schemas)
    schemas_match = names_match and all(
        _schema_without_titles(advertised_schemas[name])
        == _schema_without_titles(agent_schemas[name])
        for name in EXPECTED_TOOL_FIELDS
    )
    return schemas_match, digest


async def _agent_tool_schemas(workspace: Path) -> dict[str, Mapping[str, Any]]:
    """Build the actual Agent-facing contracts used as the MCP parity oracle."""

    debugger = PythonRuntimeDebugger(workspace)
    try:
        return {
            tool.name: tool.spec.parameters
            for tool in create_python_debug_tools(debugger)
        }
    finally:
        await debugger.close()


def _result_markdown(result: Mapping[str, Any]) -> str:
    metrics = cast(Mapping[str, Any], result["metrics"])
    acceptance = cast(Mapping[str, bool], result["acceptance"])
    lines = [
        "# Runtime debugging golden demo",
        "",
        "This result was produced through the official MCP v2 stdio client, the local MCP "
        "server, DAP, and `debugpy==1.8.21` against the real golden target.",
        "",
        "The unkeyed SHA-256 checks below establish internal sequence/hash/digest "
        "consistency only. Without a trusted external head, HMAC, or signature, a writer "
        "able to rewrite the complete journal can recompute the chain; this benchmark does "
        "not test origin authenticity or resistance to such a writer.",
        "",
        "## Result",
        "",
        f"- MCP protocol: `{result['mcp_protocol_version']}`",
        f"- MCP tools listed: `{metrics['mcp_tools_listed']}/7`",
        f"- MCP/Agent schema parity: `{metrics['mcp_schema_parity']}`",
        f"- Breakpoint verified exactly: `{metrics['breakpoint_exact']}`",
        f"- Suspicious frame exact: `{metrics['suspicious_frame_exact']}`",
        f"- Expected locals recall: `{metrics['expected_locals_recall']:.3f}`",
        f"- Required evidence coverage: `{metrics['required_evidence_coverage']:.3f}`",
        f"- Report-generation model calls: `{metrics['report_model_calls']}`",
        f"- Replay debugger calls: `{metrics['replay_debugger_calls']}`",
        f"- Unique JSON hashes across five replays: `{metrics['report_json_hashes_unique']}`",
        f"- Unique Markdown hashes across five replays: "
        f"`{metrics['report_markdown_hashes_unique']}`",
        f"- Stale event-hash mismatch detected: "
        f"`{metrics['stale_event_hash_mismatch_detected']}`",
        f"- Stale observation-digest mismatch detected: "
        f"`{metrics['stale_observation_digest_mismatch_detected']}`",
        f"- Orphan processes after shutdown: `{metrics['orphan_processes']}`",
        "",
        "## Acceptance",
        "",
    ]
    lines.extend(
        f"- `{name}`: **{'PASS' if passed else 'FAIL'}**"
        for name, passed in acceptance.items()
    )
    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            "- `debugging_pr_evidence.json`: canonical replay output.",
            "- `debugging_pr_evidence.md`: reviewer-facing fixed-template report.",
            "- The private raw event log is intentionally not committed.",
            "",
            "Reproduce with:",
            "",
            "```bash",
            "uv run --extra debug python examples/runtime_debug_demo/run_demo.py",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def _write_artifacts(
    output_dir: Path,
    result: Mapping[str, Any],
    evidence: DebugEvidenceArtifacts,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "debugging_demo_results.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "debugging_demo_results.md").write_text(
        _result_markdown(result),
        encoding="utf-8",
    )
    (output_dir / "debugging_pr_evidence.json").write_text(
        evidence.canonical_json,
        encoding="utf-8",
    )
    (output_dir / "debugging_pr_evidence.md").write_text(
        evidence.markdown,
        encoding="utf-8",
    )


async def run_golden_demo(
    *,
    workspace: Path = WORKSPACE_ROOT,
    output_dir: Path | None = None,
) -> GoldenDemoRun:
    """Execute all seven MCP tools and prove deterministic evidence replay."""

    workspace = _resolve_path(workspace, strict=True)
    breakpoint_line = _breakpoint_line(workspace)
    agent_schemas = await _agent_tool_schemas(workspace)
    token = f"witness-golden-{uuid.uuid4().hex}"
    baseline = _process_snapshot()
    process_check_supported = baseline is not None
    started_at = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="witness-debug-demo-") as temporary_dir:
        temporary = Path(temporary_dir)
        event_log = temporary / "events.json"
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr:
            parameters = StdioServerParameters(
                command=sys.executable,
                args=[
                    "-m",
                    "react_agent.debug_mcp",
                    "--workspace",
                    str(workspace),
                    "--python-executable",
                    sys.executable,
                    "--event-log",
                    str(event_log),
                    "--allow-execution",
                ],
                cwd=workspace,
            )
            async with stdio_client(parameters, errlog=stderr) as (read_stream, write_stream):
                async with ClientSession(
                    read_stream,
                    write_stream,
                    read_timeout_seconds=30,
                ) as session:
                    initialized = await session.initialize()
                    listing = await session.list_tools()
                    listed_names = tuple(tool.name for tool in listing.tools)
                    schema_valid, schema_sha256 = _schema_audit(
                        listing.tools,
                        agent_schemas,
                    )

                    launch = _structured(
                        await session.call_tool(
                            "python_debug_launch",
                            {
                                "program": TARGET.as_posix(),
                                "args": [token],
                                "breakpoints": [
                                    {"file": TARGET.as_posix(), "lines": [breakpoint_line]}
                                ],
                                "exception_policy": "uncaught",
                                "stop_on_entry": False,
                                "wait_timeout_s": 15.0,
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_launch",
                    )
                    debug_session_id = cast(str, launch["debug_session_id"])
                    breakpoint_stop_id = cast(int, launch["stop_id"])
                    active_processes = _owned_processes(
                        _process_snapshot(),
                        token=token,
                        baseline=baseline,
                    )

                    set_breakpoints = _structured(
                        await session.call_tool(
                            "python_debug_set_breakpoints",
                            {
                                "debug_session_id": debug_session_id,
                                "file": TARGET.as_posix(),
                                "lines": [breakpoint_line],
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_set_breakpoints",
                    )
                    exception_stop = _structured(
                        await session.call_tool(
                            "python_debug_control",
                            {
                                "debug_session_id": debug_session_id,
                                "action": "continue",
                                "stop_id": breakpoint_stop_id,
                                "wait_timeout_s": 15.0,
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_control",
                    )
                    exception_stop_id = cast(int, exception_stop["stop_id"])
                    stack = _structured(
                        await session.call_tool(
                            "python_debug_stack",
                            {
                                "debug_session_id": debug_session_id,
                                "stop_id": exception_stop_id,
                                "levels": 32,
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_stack",
                    )
                    frames = cast(list[dict[str, Any]], stack["frames"])
                    price_frames = [frame for frame in frames if frame["function"] == "price_order"]
                    if len(price_frames) != 1:
                        raise RuntimeError(
                            "golden stack did not contain exactly one price_order frame"
                        )
                    price_frame = price_frames[0]
                    selected = _structured(
                        await session.call_tool(
                            "python_debug_select_frame",
                            {
                                "debug_session_id": debug_session_id,
                                "stop_id": exception_stop_id,
                                "frame_index": price_frame["frame_index"],
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_select_frame",
                    )
                    variables = _structured(
                        await session.call_tool(
                            "python_debug_variables",
                            {
                                "debug_session_id": debug_session_id,
                                "stop_id": exception_stop_id,
                                "scope": "locals",
                                "max_variables": 64,
                                "max_value_chars": 512,
                            },
                            read_timeout_seconds=30,
                        ),
                        "python_debug_variables",
                    )
                    stopped = _structured(
                        await session.call_tool(
                            "python_debug_stop",
                            {"debug_session_id": debug_session_id},
                            read_timeout_seconds=30,
                        ),
                        "python_debug_stop",
                    )

            stderr.seek(0)
            server_stderr = stderr.read()

        orphaned = await _wait_for_no_owned_processes(token=token, baseline=baseline)
        events = load_debug_event_log(event_log)
        replays = [generate_debug_evidence(events) for _ in range(5)]
        evidence = replays[0]
        evidence_payload = cast(dict[str, Any], json.loads(evidence.canonical_json))
        consistency_checks = _consistency_mismatch_checks(events)

    local_values = {
        item["name"]: item["value"]
        for item in cast(list[dict[str, Any]], variables["variables"])
    }
    expected_locals_found = sum(
        local_values.get(name) == value for name, value in EXPECTED_LOCALS.items()
    )
    launch_breakpoints = cast(list[dict[str, Any]], launch["breakpoints"])
    reset_breakpoints = cast(list[dict[str, Any]], set_breakpoints["breakpoints"])
    breakpoint_exact = bool(
        launch_breakpoints
        and reset_breakpoints
        and launch_breakpoints[0].get("verified") is True
        and reset_breakpoints[0].get("verified") is True
        and launch_breakpoints[0].get("actual_line") == breakpoint_line
        and reset_breakpoints[0].get("actual_line") == breakpoint_line
    )
    selected_frame = cast(dict[str, Any], selected["selected_frame"])
    generation = cast(dict[str, Any], evidence_payload["generation"])
    json_hashes = {hashlib.sha256(item.canonical_json.encode()).hexdigest() for item in replays}
    markdown_hashes = {hashlib.sha256(item.markdown.encode()).hexdigest() for item in replays}
    metrics: dict[str, Any] = {
        "mcp_tools_listed": len(set(listed_names) & set(EXPECTED_TOOLS)),
        "mcp_schema_tool_count": len(listing.tools),
        "mcp_schema_parity": schema_valid,
        "breakpoint_exact": breakpoint_exact,
        "suspicious_frame_exact": (
            selected_frame.get("function") == "price_order"
            and selected_frame.get("in_workspace") is True
        ),
        "expected_locals_recall": expected_locals_found / len(EXPECTED_LOCALS),
        "required_evidence_coverage": _required_evidence_coverage(evidence_payload),
        "report_model_calls": generation["model_calls"],
        "replay_debugger_calls": generation["debugger_calls"],
        "report_json_hashes_unique": len(json_hashes),
        "report_markdown_hashes_unique": len(markdown_hashes),
        "stale_event_hash_mismatch_detected": consistency_checks[
            "stale_event_hash_mismatch_detected"
        ],
        "stale_observation_digest_mismatch_detected": consistency_checks[
            "stale_observation_digest_mismatch_detected"
        ],
        "active_debuggee_processes": len(active_processes),
        "orphan_processes": len(orphaned),
        "process_check_supported": process_check_supported,
        "process_reaped": stopped.get("process_reaped") is True,
        "event_count": len(events),
        "debug_tool_calls": 7,
        "elapsed_ms": round((time.monotonic() - started_at) * 1000, 3),
        "server_stderr_empty": not server_stderr.strip(),
    }
    acceptance = {
        "mcp_initialize_and_7_tools": (
            tuple(sorted(listed_names)) == tuple(sorted(EXPECTED_TOOLS))
        ),
        "mcp_schema_parity": metrics["mcp_schema_parity"],
        "all_7_tools_called": len(events) == 23,
        "verified_breakpoint": metrics["breakpoint_exact"],
        "uncaught_exception_observed": (
            str(cast(dict[str, Any], exception_stop["exception"])["type"]).startswith(
                "ZeroDivisionError"
            )
        ),
        "suspicious_frame_exact": metrics["suspicious_frame_exact"],
        "expected_locals_recall_1": metrics["expected_locals_recall"] == 1.0,
        "required_evidence_coverage_1": metrics["required_evidence_coverage"] == 1.0,
        "zero_report_external_calls": (
            metrics["report_model_calls"] == 0 and metrics["replay_debugger_calls"] == 0
        ),
        "five_replays_byte_identical": (
            metrics["report_json_hashes_unique"] == 1
            and metrics["report_markdown_hashes_unique"] == 1
        ),
        "two_layer_internal_consistency_checks": (
            metrics["stale_event_hash_mismatch_detected"]
            and metrics["stale_observation_digest_mismatch_detected"]
        ),
        "no_orphan_processes": (
            metrics["process_check_supported"]
            and metrics["active_debuggee_processes"] >= 1
            and metrics["orphan_processes"] == 0
            and metrics["process_reaped"]
        ),
        "server_stderr_empty": metrics["server_stderr_empty"],
    }
    result: dict[str, Any] = {
        "benchmark": "runtime-debugging-golden-demo",
        "transport": "official-mcp-v2-stdio",
        "mcp_protocol_version": initialized.protocol_version,
        "mcp_tool_schema_sha256": schema_sha256,
        "hash_check_scope": {
            "mechanism": "unkeyed-sha256",
            "provides": "internal-consistency-mismatch-detection",
            "requires_trusted_external_head_for_authenticity": True,
            "full_chain_rewrite_resistance_tested": False,
        },
        "server": initialized.server_info.model_dump(mode="json"),
        "versions": {
            "python": sys.version.split()[0],
            "mcp": _package_version("mcp"),
            "debugpy": _package_version("debugpy"),
        },
        "target": TARGET.as_posix(),
        "breakpoint_line": breakpoint_line,
        "exception_stop_id": exception_stop_id,
        "evidence_payload_sha256": evidence.evidence_payload_sha256,
        "journal_head_hash": cast(dict[str, Any], evidence_payload["journal"])["head_hash"],
        "metrics": metrics,
        "acceptance": acceptance,
        "all_acceptance_passed": all(acceptance.values()),
    }
    if output_dir is not None:
        _write_artifacts(_resolve_path(output_dir, strict=False), result, evidence)
    return GoldenDemoRun(result, evidence.canonical_json, evidence.markdown)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace",
        type=Path,
        default=WORKSPACE_ROOT,
        help="Trusted repository root.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=WORKSPACE_ROOT / "docs" / "evaluations",
        help="Directory for result and evidence artifacts.",
    )
    return parser


def main() -> None:
    arguments = _parser().parse_args()
    completed = asyncio.run(
        run_golden_demo(workspace=arguments.workspace, output_dir=arguments.output_dir)
    )
    print(json.dumps(completed.result, ensure_ascii=False, indent=2, sort_keys=True))
    if not completed.result["all_acceptance_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
