"""Reproducible synthetic A/B benchmark for context-governance strategies."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import re
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from react_agent import (
    AgentConfig,
    ApprovalHandler,
    AssistantMessage,
    ContextCompression,
    ContextGovernor,
    ContextStrategy,
    ModelRequest,
    ModelResponse,
    ObservationEffect,
    ReActAgent,
    RunStatus,
    Tool,
    ToolCall,
    ToolContextPolicy,
    ToolExecutionContext,
    ToolMessage,
    ToolRegistry,
    Usage,
    UserMessage,
    estimate_context_chars,
    tool,
)
from react_agent.context import ContextCompressionRequest
from react_agent.tools import SideEffectGuard

HARD_LIMIT = 8_000
INSTRUCTIONS = "repository agent instructions" * 12
_FACT = re.compile(r"FACT\[[^\]]+\]")
BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_SEED = "context-ab-scenario-bootstrap-v1"
SCRIPTED_RAW_HARD_LIMIT = 100_000
SCRIPTED_APP_PATH = "app.py"
SCRIPTED_FINAL_ANSWER = "VALUE=42; TESTS=PASS"
SCRIPTED_MODEL_REVISION = "offline-repository-state-machine-v1"
SCRIPTED_INSTRUCTIONS = (
    "Use only the repository tools. Preserve the requested final state and verify it."
)
SCRIPTED_PROMPT = "Set app.py VALUE to 42 and prove the repository test passes."
_SCRIPTED_PADDING = "x" * 3_200
_SCRIPTED_NECESSARY_TOOL_NAMES = frozenset({"run_tests", "write_file"})
_SCRIPTED_READ_ACTION: tuple[str, dict[str, object]] = (
    "read_file",
    {"path": SCRIPTED_APP_PATH},
)
_SCRIPTED_TEST_ACTION: tuple[str, dict[str, object]] = (
    "run_tests",
    {"target": SCRIPTED_APP_PATH},
)
_SCRIPTED_WRITE_ACTION: tuple[str, dict[str, object]] = (
    "write_file",
    {"path": SCRIPTED_APP_PATH, "value": 42},
)
_SCRIPTED_PLAN: tuple[tuple[str, dict[str, object]], ...] = (
    (_SCRIPTED_READ_ACTION,) * 5
    + (
        _SCRIPTED_TEST_ACTION,
        _SCRIPTED_WRITE_ACTION,
        _SCRIPTED_READ_ACTION,
        _SCRIPTED_TEST_ACTION,
    )
)


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    category: str
    transcript: tuple[Any, ...]
    policies: dict[str, ToolContextPolicy]
    live_facts: frozenset[str]


@dataclass(frozen=True, slots=True)
class Result:
    scenario: str
    category: str
    strategy: str
    input_chars: int
    projected_chars: int
    saved_chars: int
    saved_percent: float
    deterministic_evictions: int
    deterministic_removed_chars: int
    recency_masks: int
    compression_calls: int
    compression_source_chars: int
    hard_fallback: bool
    overflow: bool
    live_fact_recall: float
    # Whether deterministic eviction alone met the hard budget (Tier 1 fit),
    # measured directly rather than inferred from later tiers' behaviour.
    tier1_fit: bool = False


class ExtractiveBenchmarkCompressor:
    """Deterministic stand-in that makes compression calls measurable offline."""

    revision = "offline-extractive-compressor-v1"

    async def compress(
        self,
        request: ContextCompressionRequest,
    ) -> ContextCompression:
        text = "\n".join(
            item.content
            for item in request.source
            if isinstance(item, (UserMessage, ToolMessage))
        )
        facts = sorted(set(_FACT.findall(text)))
        summary = "generated-summary: " + ", ".join(facts)
        return ContextCompression(
            summary[: request.max_summary_chars],
            Usage(
                input_tokens=max(1, len(text) // 4),
                output_tokens=max(1, len(summary) // 4),
                total_tokens=max(2, (len(text) + len(summary)) // 4),
            ),
            response_model="offline-extractive-compressor",
        )


def _scripted_app_source(value: int) -> str:
    return f'VALUE = {value}\nPADDING = "{_SCRIPTED_PADDING}"\n'


def _scripted_workspace_file(
    relative_path: str,
    context: ToolExecutionContext,
) -> Path:
    if context.workspace_path is None:
        raise RuntimeError("The scripted repository tool requires a workspace.")
    workspace = context.workspace_path.resolve()
    candidate = (workspace / relative_path).resolve()
    if candidate != workspace / SCRIPTED_APP_PATH:
        raise ValueError("The scripted repository fixture exposes only app.py.")
    return candidate


def _scripted_repository_tools(
    invocations: list[dict[str, object]],
) -> tuple[Tool, ...]:
    @tool(
        allow_repeated=True,
        context_policy=ToolContextPolicy(ObservationEffect.READ, ("path",)),
    )
    def read_file(path: str, *, context: ToolExecutionContext) -> str:
        """Read one UTF-8 file from the scripted repository workspace."""

        invocations.append({"name": "read_file", "path": path})
        return _scripted_workspace_file(path, context).read_text(encoding="utf-8")

    @tool(context_policy=ToolContextPolicy(ObservationEffect.MUTATE, ("path",)))
    def write_file(path: str, value: int, *, context: ToolExecutionContext) -> dict[str, object]:
        """Set VALUE in one Python file in the scripted repository workspace."""

        invocations.append({"name": "write_file", "path": path, "value": value})
        destination = _scripted_workspace_file(path, context)
        destination.write_text(_scripted_app_source(value), encoding="utf-8")
        return {
            "path": path,
            "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
            "value": value,
        }

    @tool(
        allow_repeated=True,
        context_policy=ToolContextPolicy(ObservationEffect.EXECUTE),
    )
    def run_tests(target: str, *, context: ToolExecutionContext) -> dict[str, object]:
        """Compile the scripted Python file and assert its required VALUE."""

        invocations.append({"name": "run_tests", "target": target})
        source_path = _scripted_workspace_file(target, context)
        namespace: dict[str, object] = {}
        exec(compile(source_path.read_text(encoding="utf-8"), target, "exec"), namespace)
        value = namespace.get("VALUE")
        if value != 42:
            raise AssertionError("VALUE is not 42")
        return {"passed": True, "target": target, "value": value}

    return read_file, write_file, run_tests


class RepositoryStateMachineModel:
    """Offline model that advances only when the latest tool fact is visible."""

    revision = SCRIPTED_MODEL_REVISION

    def __init__(self) -> None:
        self.turn = 0
        self.request_context_chars: list[int] = []

    @staticmethod
    def _observation(request: ModelRequest, plan_index: int) -> ToolMessage:
        call_id = f"scripted-call-{plan_index:02d}"
        matches = [
            item
            for item in request.transcript
            if isinstance(item, ToolMessage) and item.call_id == call_id
        ]
        if len(matches) != 1:
            raise RuntimeError(f"Required observation {call_id} is not visible exactly once.")
        return matches[0]

    @staticmethod
    def _validate_observation(observation: ToolMessage, plan_index: int) -> None:
        expected_name, _ = _SCRIPTED_PLAN[plan_index]
        if observation.name != expected_name:
            raise RuntimeError(
                f"Expected {expected_name} observation, received {observation.name}."
            )
        if plan_index == 5:
            if not observation.is_error:
                raise RuntimeError("The pre-edit repository test unexpectedly passed.")
            return
        if observation.is_error:
            raise RuntimeError(f"The {expected_name} tool unexpectedly failed.")
        payload = json.loads(observation.content)
        data = payload.get("data") if isinstance(payload, dict) else None
        if expected_name == "read_file":
            expected_value = 42 if plan_index == 7 else 1
            if not isinstance(data, str) or f"VALUE = {expected_value}" not in data:
                raise RuntimeError("The latest file observation has the wrong VALUE.")
        elif expected_name == "run_tests":
            if not isinstance(data, dict) or data.get("passed") is not True:
                raise RuntimeError("The post-edit repository test did not pass.")
        elif expected_name == "write_file":
            if not isinstance(data, dict) or data.get("value") != 42:
                raise RuntimeError("The write observation did not commit VALUE=42.")

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.request_context_chars.append(
            estimate_context_chars(
                request.transcript,
                instructions=request.instructions,
                tool_specs=request.tools,
            )
        )
        if self.turn:
            self._validate_observation(self._observation(request, self.turn - 1), self.turn - 1)
        if self.turn == len(_SCRIPTED_PLAN):
            self.turn += 1
            return ModelResponse(
                AssistantMessage(SCRIPTED_FINAL_ANSWER),
                response_model=SCRIPTED_MODEL_REVISION,
            )
        if self.turn > len(_SCRIPTED_PLAN):
            raise RuntimeError("The scripted Agent made an unexpected extra model call.")
        name, arguments = _SCRIPTED_PLAN[self.turn]
        call = ToolCall(
            f"scripted-call-{self.turn:02d}",
            name,
            json.dumps(arguments, sort_keys=True, separators=(",", ":")),
        )
        self.turn += 1
        return ModelResponse(
            AssistantMessage(None, (call,)),
            response_model=SCRIPTED_MODEL_REVISION,
        )


class DeterministicBenchmarkToolRegistry(ToolRegistry):
    """Execute real tools but remove nondeterministic duration telemetry."""

    async def execute(
        self,
        call: ToolCall,
        *,
        run_id: str,
        execution_id: str | None = None,
        approval_handler: ApprovalHandler | None,
        max_output_chars: int,
        call_key: str | None = None,
        attempt: int = 1,
        before_invoke: SideEffectGuard | None = None,
        workspace_path: Path | None = None,
    ) -> ToolMessage:
        result = await super().execute(
            call,
            run_id=run_id,
            execution_id=execution_id,
            approval_handler=approval_handler,
            max_output_chars=max_output_chars,
            call_key=call_key,
            attempt=attempt,
            before_invoke=before_invoke,
            workspace_path=workspace_path,
        )
        return replace(result, duration_ms=0.0)


def _scripted_workspace_digest(workspace: Path) -> str:
    digest = hashlib.sha256()
    for candidate in sorted(path for path in workspace.rglob("*") if path.is_file()):
        relative = candidate.relative_to(workspace).as_posix().encode("utf-8")
        content = candidate.read_bytes()
        for value in (relative, content):
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
    return digest.hexdigest()


def _scripted_workspace_file_count(workspace: Path) -> int:
    return sum(1 for path in workspace.rglob("*") if path.is_file())


def _initialize_scripted_workspace(workspace: Path) -> None:
    (workspace / SCRIPTED_APP_PATH).write_text(
        _scripted_app_source(1),
        encoding="utf-8",
    )


async def _run_scripted_repository_arm(
    *,
    name: str,
    workspace: Path,
    strategy: ContextStrategy,
    hard_limit: int,
) -> dict[str, object]:
    invocations: list[dict[str, object]] = []
    tools = _scripted_repository_tools(invocations)
    registry = DeterministicBenchmarkToolRegistry(tools)
    model = RepositoryStateMachineModel()
    compressor = ExtractiveBenchmarkCompressor() if strategy is ContextStrategy.TIERED else None
    governor = ContextGovernor(
        strategy=strategy,
        compressor=compressor,
        keep_recent_turns=2,
        max_summary_chars=1_000,
    )
    agent = ReActAgent(
        model,
        registry,
        instructions=SCRIPTED_INSTRUCTIONS,
        config=AgentConfig(
            max_steps=len(_SCRIPTED_PLAN) + 1,
            max_tool_calls=len(_SCRIPTED_PLAN),
            max_context_chars=hard_limit,
            context_strategy=strategy,
            context_keep_recent_turns=2,
            context_summary_max_chars=1_000,
        ),
        context_governor=governor,
    )
    result = await agent.run(
        SCRIPTED_PROMPT,
        run_id=f"scripted-{name}",
        execution_id=f"scripted-{name}-execution",
        workspace_path=workspace,
    )
    context_metrics: dict[str, int] = {}
    for key, value in result.context_metrics.items():
        if not isinstance(value, int) or isinstance(value, bool):
            raise RuntimeError(f"Context metric {key} is not an integer.")
        context_metrics[key] = value
    necessary = [
        item for item in invocations if item.get("name") in _SCRIPTED_NECESSARY_TOOL_NAMES
    ]
    return {
        "strategy": strategy.value,
        "hard_limit_chars": hard_limit,
        "status": result.status.value,
        "stop_reason": result.stop_reason.value,
        "final_answer": result.output,
        "main_model_calls": len(model.request_context_chars),
        "agent_accounted_model_calls": result.model_calls,
        "tool_calls": result.tool_calls,
        "tool_executions": result.tool_executions,
        "tool_execution_trace": invocations,
        "necessary_stateful_tool_calls": necessary,
        "workspace_digest": _scripted_workspace_digest(workspace),
        "workspace_file_count": _scripted_workspace_file_count(workspace),
        "context": {
            "canonical_chars": estimate_context_chars(
                result.transcript,
                instructions=SCRIPTED_INSTRUCTIONS,
                tool_specs=agent.registry.specs,
            ),
            "request_chars_by_turn": model.request_context_chars,
            "peak_active_request_chars": max(model.request_context_chars),
            **context_metrics,
        },
        "compression": {
            "calls": context_metrics["compression_calls"],
            "cache_hits": context_metrics["compression_cache_hits"],
            "source_chars": context_metrics["compression_source_chars"],
            "usage": asdict(result.usage),
        },
    }


async def _scripted_repository_acceptance() -> dict[str, object]:
    with (
        TemporaryDirectory(prefix="context-ab-raw-") as raw_directory,
        TemporaryDirectory(prefix="context-ab-tiered-") as tiered_directory,
    ):
        raw_workspace = Path(raw_directory)
        tiered_workspace = Path(tiered_directory)
        if raw_workspace == tiered_workspace:
            raise RuntimeError("Scripted benchmark workspaces must be independent.")
        for workspace in (raw_workspace, tiered_workspace):
            _initialize_scripted_workspace(workspace)
        raw = await _run_scripted_repository_arm(
            name="raw-reference",
            workspace=raw_workspace,
            strategy=ContextStrategy.STOP,
            hard_limit=SCRIPTED_RAW_HARD_LIMIT,
        )
        tiered = await _run_scripted_repository_arm(
            name="tiered",
            workspace=tiered_workspace,
            strategy=ContextStrategy.TIERED,
            hard_limit=HARD_LIMIT,
        )

    raw_context = raw["context"]
    tiered_context = tiered["context"]
    assert isinstance(raw_context, dict) and isinstance(tiered_context, dict)
    equivalence = {
        "final_answer_equal": raw["final_answer"] == tiered["final_answer"],
        "necessary_stateful_tool_calls_equal": (
            raw["necessary_stateful_tool_calls"] == tiered["necessary_stateful_tool_calls"]
        ),
        "workspace_digest_equal": raw["workspace_digest"] == tiered["workspace_digest"],
    }
    safety = {
        "arms_completed": (
            raw["status"] == RunStatus.COMPLETED.value
            and tiered["status"] == RunStatus.COMPLETED.value
        ),
        "raw_active_context_exceeds_tiered_budget": (
            int(raw_context["peak_active_request_chars"]) > HARD_LIMIT
        ),
        "tiered_active_context_within_budget": (
            int(tiered_context["peak_active_request_chars"]) <= HARD_LIMIT
        ),
        "tiered_exercised_deterministic_eviction": (
            int(tiered_context["deterministic_evictions"]) > 0
        ),
    }
    raw_peak = int(raw_context["peak_active_request_chars"])
    tiered_peak = int(tiered_context["peak_active_request_chars"])
    return {
        "protocol": {
            "model": SCRIPTED_MODEL_REVISION,
            "live_model_calls": 0,
            "comparison_purpose": "semantic_equivalence_against_high_budget_raw_reference",
            "context_metric_semantics": (
                "counts are cumulative across projections; peak fields are per-run maxima"
            ),
            "tool_duration_normalization": (
                "ToolMessage.duration_ms is set to 0 after each real tool execution"
            ),
            "workspace_count": 2,
            "workspace_paths_distinct": True,
            "raw_reference_hard_limit_chars": SCRIPTED_RAW_HARD_LIMIT,
            "tiered_hard_limit_chars": HARD_LIMIT,
            "necessary_stateful_tool_names": sorted(_SCRIPTED_NECESSARY_TOOL_NAMES),
            "workspace_digest": "sha256-length-prefixed-relative-path-and-file-bytes-v1",
        },
        "arms": {"raw_reference": raw, "tiered": tiered},
        "equivalence": equivalence,
        "safety": safety,
        "comparison": {
            "raw_reference_peak_active_chars": raw_peak,
            "tiered_peak_active_chars": tiered_peak,
            "tiered_peak_active_reduction_percent": round(
                100.0 * (raw_peak - tiered_peak) / raw_peak,
                3,
            ),
        },
        "all_equivalence_passed": all(equivalence.values()),
        "all_safety_passed": all(safety.values()),
    }


def _block(
    sequence: int,
    name: str,
    arguments: dict[str, object],
    fact: str,
    *,
    size: int = 2_400,
    is_error: bool = False,
) -> tuple[AssistantMessage, ToolMessage]:
    call_id = f"call-{sequence}"
    content = f"FACT[{fact}] " + (chr(65 + (sequence % 20)) * size)
    return (
        AssistantMessage(
            None,
            (ToolCall(call_id, name, json.dumps(arguments, sort_keys=True)),),
        ),
        ToolMessage(call_id, name, content, is_error=is_error),
    )


def _scenarios() -> tuple[Scenario, ...]:
    read = ToolContextPolicy(ObservationEffect.READ, ("path",))
    mutate = ToolContextPolicy(ObservationEffect.MUTATE, ("path",))
    execute = ToolContextPolicy(ObservationEffect.EXECUTE)
    scenarios: list[Scenario] = []

    rereads: list[Any] = [UserMessage("inspect the latest a.py")]
    for index in range(12):
        rereads.extend(_block(index, "read_file", {"path": "a.py"}, f"a-v{index}"))
    scenarios.append(
        Scenario(
            "same_file_rereads",
            "replacement_heavy",
            tuple(rereads),
            {"read_file": read},
            frozenset({"FACT[a-v11]"}),
        )
    )

    edits: list[Any] = [UserMessage("edit and verify config.py")]
    sequence = 20
    for version in range(6):
        edits.extend(
            _block(sequence, "read_file", {"path": "config.py"}, f"config-v{version}")
        )
        sequence += 1
        edits.extend(
            _block(
                sequence,
                "write_file",
                {"path": "config.py", "version": version + 1},
                f"write-{version + 1}",
                size=240,
            )
        )
        sequence += 1
    edits.extend(_block(sequence, "read_file", {"path": "config.py"}, "config-v6"))
    scenarios.append(
        Scenario(
            "edit_read_churn",
            "replacement_heavy",
            tuple(edits),
            {"read_file": read, "write_file": mutate},
            frozenset({"FACT[config-v6]", *(f"FACT[write-{i}]" for i in range(1, 7))}),
        )
    )

    reruns: list[Any] = [UserMessage("fix until unit tests pass")]
    for index in range(10):
        reruns.extend(
            _block(
                50 + index,
                "run_tests",
                {"target": "tests/unit"},
                "tests-pass" if index == 9 else f"tests-fail-{index}",
                is_error=index != 9,
            )
        )
    scenarios.append(
        Scenario(
            "test_reruns",
            "replacement_heavy",
            tuple(reruns),
            {"run_tests": execute},
            frozenset({"FACT[tests-pass]"}),
        )
    )

    retries: list[Any] = [UserMessage("retry the stable dependency lookup")]
    for index in range(8):
        retries.extend(
            _block(
                70 + index,
                "dependency_lookup",
                {"package": "core"},
                "lookup-ok" if index == 7 else f"lookup-error-{index}",
                is_error=index != 7,
            )
        )
    scenarios.append(
        Scenario(
            "successful_retry",
            "replacement_heavy",
            tuple(retries),
            {},
            frozenset({"FACT[lookup-ok]"}),
        )
    )

    failed_reruns: list[Any] = [
        UserMessage("retain the last passing run while diagnosing a regression")
    ]
    failed_reruns.extend(
        _block(80, "run_tests", {"target": "tests/regression"}, "tests-pass")
    )
    for index in range(4):
        failed_reruns.extend(
            _block(
                81 + index,
                "run_tests",
                {"target": "tests/regression"},
                f"tests-regression-fail-{index}",
                is_error=True,
            )
        )
    scenarios.append(
        Scenario(
            "success_then_failed_rerun",
            "replacement_heavy",
            tuple(failed_reruns),
            {"run_tests": execute},
            frozenset({"FACT[tests-pass]", "FACT[tests-regression-fail-3]"}),
        )
    )

    unrelated: list[Any] = [UserMessage("inspect twelve independent modules")]
    unrelated_facts: set[str] = set()
    for index in range(12):
        fact = f"module-{index}"
        unrelated_facts.add(f"FACT[{fact}]")
        unrelated.extend(
            _block(90 + index, "read_file", {"path": f"src/m{index}.py"}, fact)
        )
    scenarios.append(
        Scenario(
            "unrelated_reads",
            "non_redundant",
            tuple(unrelated),
            {"read_file": read},
            frozenset(unrelated_facts),
        )
    )

    opaque: list[Any] = [UserMessage("retain append-only audit facts")]
    opaque_facts: set[str] = set()
    for index in range(10):
        fact = f"audit-{index}"
        opaque_facts.add(f"FACT[{fact}]")
        opaque.extend(_block(120 + index, "audit_query", {"cursor": index}, fact))
    scenarios.append(
        Scenario(
            "opaque_append_only",
            "non_redundant",
            tuple(opaque),
            {},
            frozenset(opaque_facts),
        )
    )
    return tuple(scenarios)


def _recency_observation_masking(
    transcript: tuple[Any, ...],
) -> tuple[tuple[Any, ...], int]:
    """Generic baseline: mask oldest tool observations until the budget fits.

    This intentionally has no tool policy, resource identity, or replacement
    semantics. It models common recency-based Observation Masking while keeping
    call/result structure and a content hash for auditability.
    """

    work = list(transcript)
    masked = 0
    for index, item in enumerate(work):
        if (
            estimate_context_chars(
                work,
                instructions=INSTRUCTIONS,
                tool_specs=(),
            )
            <= HARD_LIMIT
        ):
            break
        if not isinstance(item, ToolMessage):
            continue
        marker = json.dumps(
            {
                "observation_masked": "recency",
                "sha256": hashlib.sha256(item.content.encode()).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        work[index] = replace(item, content=marker)
        masked += 1
    return tuple(work), masked


async def _run_one(scenario: Scenario, strategy: str) -> Result:
    transcript = scenario.transcript
    recency_masks = 0
    if strategy == "raw_hard_stop":
        governor = ContextGovernor(strategy=ContextStrategy.STOP, compressor=None)
    elif strategy == "recency_observation_masking":
        transcript, recency_masks = _recency_observation_masking(scenario.transcript)
        governor = ContextGovernor(strategy=ContextStrategy.STOP, compressor=None)
    elif strategy == "generic_summary":
        governor = ContextGovernor(
            strategy=ContextStrategy.GENERIC,
            compressor=ExtractiveBenchmarkCompressor(),
            keep_recent_turns=2,
        )
    elif strategy == "tiered":
        governor = ContextGovernor(
            strategy=ContextStrategy.TIERED,
            compressor=ExtractiveBenchmarkCompressor(),
            keep_recent_turns=2,
        )
    elif strategy == "deterministic_only":
        governor = ContextGovernor(
            strategy=ContextStrategy.TIERED,
            compressor=None,
            keep_recent_turns=2,
        )
    else:  # pragma: no cover - internal benchmark table
        raise ValueError(strategy)
    projection = await governor.prepare(
        transcript,
        instructions=INSTRUCTIONS,
        tool_specs=(),
        tool_policies=scenario.policies,
        hard_limit=HARD_LIMIT,
    )
    rendered = json.dumps(
        [
            item.content
            for item in projection.transcript
            if isinstance(item, (UserMessage, ToolMessage))
        ],
        ensure_ascii=False,
    )
    found = sum(fact in rendered for fact in scenario.live_facts)
    recall = found / len(scenario.live_facts) if scenario.live_facts else 1.0
    report = projection.report
    input_chars = estimate_context_chars(
        scenario.transcript,
        instructions=INSTRUCTIONS,
        tool_specs=(),
    )
    saved = input_chars - report.final_chars
    return Result(
        scenario.name,
        scenario.category,
        strategy,
        input_chars,
        report.final_chars,
        saved,
        round((saved / input_chars) * 100, 3),
        len(report.evictions),
        report.deterministic_removed_chars,
        recency_masks,
        report.compression_calls,
        report.compression_source_chars,
        report.hard_fallback,
        report.overflow,
        round(recall, 4),
        report.deterministic_chars <= HARD_LIMIT,
    )


def _percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _paired_bootstrap_ratio_ci(
    pairs: tuple[tuple[int, int], ...],
) -> tuple[float, float]:
    """Return a cross-version-stable paired percentile interval.

    Every pair is ``(tiered_calls, generic_calls)`` for one scenario. Sampling
    indices come from SHA-256 rather than Python's process-global RNG so the
    archived benchmark remains byte-identical across repeated executions.
    """

    if not pairs or any(generic <= 0 for _, generic in pairs):
        raise ValueError("paired call ratios require positive generic calls")
    ratios: list[float] = []
    for replicate in range(BOOTSTRAP_REPLICATES):
        tiered_total = 0
        generic_total = 0
        for draw in range(len(pairs)):
            digest = hashlib.sha256(
                f"{BOOTSTRAP_SEED}:{replicate}:{draw}".encode()
            ).digest()
            tiered, generic = pairs[int.from_bytes(digest[:8], "big") % len(pairs)]
            tiered_total += tiered
            generic_total += generic
        ratios.append(tiered_total / generic_total)
    return _percentile(ratios, 0.025), _percentile(ratios, 0.975)


def _markdown(payload: dict[str, object]) -> str:
    rows = payload["results"]
    assert isinstance(rows, list)
    lines = [
        "# Context governance synthetic A/B",
        "",
        "Provider-neutral serialized-envelope hard character budget: "
        f"`{HARD_LIMIT}`.",
        "",
        "| Scenario | Category | Strategy | Input | Projected | Saved | Compressor calls | "
        "Evictions | Recency masks | Hard fallback | Overflow | Live-fact recall |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for raw in rows:
        assert isinstance(raw, dict)
        lines.append(
            "| {scenario} | {category} | {strategy} | {input_chars} | "
            "{projected_chars} | {saved_percent:.1f}% | {compression_calls} | "
            "{deterministic_evictions} | {recency_masks} | {hard_fallback} | {overflow} | "
            "{live_fact_recall:.2f} |".format(**raw)
        )
    aggregate = payload["aggregate"]
    assert isinstance(aggregate, dict)
    statistical_protocol = payload["statistical_protocol"]
    assert isinstance(statistical_protocol, dict)
    replacement_pair_count = statistical_protocol["replacement_pair_count"]
    assert isinstance(replacement_pair_count, int)
    tier1_fit_scenarios = statistical_protocol["tier1_fit_replacement_scenarios"]
    assert isinstance(tier1_fit_scenarios, list)
    acceptance = payload["acceptance"]
    assert isinstance(acceptance, dict)
    scripted = payload["scripted_repository_acceptance"]
    assert isinstance(scripted, dict)
    scripted_arms = scripted["arms"]
    scripted_equivalence = scripted["equivalence"]
    scripted_comparison = scripted["comparison"]
    assert isinstance(scripted_arms, dict)
    assert isinstance(scripted_equivalence, dict)
    assert isinstance(scripted_comparison, dict)
    raw_arm = scripted_arms["raw_reference"]
    tiered_arm = scripted_arms["tiered"]
    assert isinstance(raw_arm, dict) and isinstance(tiered_arm, dict)

    def scripted_arm_row(label: str, arm: dict[str, object]) -> str:
        context = arm["context"]
        compression = arm["compression"]
        assert isinstance(context, dict) and isinstance(compression, dict)
        return (
            f"| {label} | {arm['hard_limit_chars']} | {arm['status']} | "
            f"{arm['main_model_calls']} | {arm['tool_executions']} | "
            f"{context['canonical_chars']} | {context['peak_input_chars']} | "
            f"{context['peak_active_request_chars']} | "
            f"{context['deterministic_evictions']} | {compression['calls']} | "
            f"{compression['source_chars']} | {context['hard_fallbacks']} |"
        )

    def pass_fail(value: object) -> str:
        return "PASS" if value else "FAIL"

    call_reduction = aggregate["replacement_compressor_call_reduction_percent"]
    call_ratio_ci = aggregate["replacement_compressor_call_ratio_bootstrap_95ci"]
    assert isinstance(call_ratio_ci, list) and len(call_ratio_ci) == 2
    tiered_saving = aggregate["replacement_tiered_mean_saved_percent"]
    non_redundant_ratio = aggregate["non_redundant_projected_ratio"]
    replacement_recall_gain = aggregate[
        "replacement_tiered_minus_recency_recall_points"
    ]
    non_redundant_recall_gain = aggregate[
        "non_redundant_tiered_minus_recency_recall_points"
    ]
    lines.extend(
        [
            "",
            "## Aggregate",
            "",
            f"- Replacement-heavy compressor-call reduction vs generic: "
            f"**{call_reduction:.1f}%**.",
            "- Tier-1-fit replacement scenarios with strict zero Tiered compressor calls: "
            f"**{', '.join(str(item) for item in tier1_fit_scenarios)}**.",
            f"- Replacement-heavy paired bootstrap tiered/generic call-ratio 95% CI: "
            f"**[{call_ratio_ci[0]:.3f}, {call_ratio_ci[1]:.3f}]** "
            f"(`n={replacement_pair_count}` synthetic scenario pairs, "
            f"`{BOOTSTRAP_REPLICATES}` replicates).",
            f"- Replacement-heavy tiered mean character saving: "
            f"**{tiered_saving:.1f}%**.",
            f"- Non-redundant tiered/generic projected-character ratio: "
            f"**{non_redundant_ratio:.3f}**.",
            f"- Replacement-heavy tiered recall gain vs recency masking: "
            f"**{replacement_recall_gain:.1f} points**.",
            f"- Non-redundant tiered recall gain vs recency masking: "
            f"**{non_redundant_recall_gain:.1f} points**.",
            "",
            "## Scripted repository Raw-reference vs Tiered",
            "",
            "Two independent temporary workspaces run the same deterministic state machine "
            "through real `read_file`, `write_file`, and `run_tests` tools.",
            "Raw intentionally uses a high budget as a semantic reference; this subsection is "
            "an equivalence check, not an equal-budget efficiency comparison.",
            "",
            "| Arm | Hard limit | Status | Main model calls | Tool executions | "
            "Canonical chars | Peak input chars | Peak active chars | Cumulative evictions | "
            "Compressor calls | Compressor source chars | Hard fallbacks |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: | ---: |",
            scripted_arm_row("Raw reference", raw_arm),
            scripted_arm_row("Tiered", tiered_arm),
            "",
            f"- Tiered peak-active-context reduction vs Raw reference: "
            f"**{scripted_comparison['tiered_peak_active_reduction_percent']:.1f}%**.",
            f"- Final answer (both arms): `{raw_arm['final_answer']}`.",
            "- Necessary/stateful tool trace (both arms): `"
            + json.dumps(
                raw_arm["necessary_stateful_tool_calls"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "`.",
            f"- Final workspace tree SHA-256 (both arms): "
            f"`{raw_arm['workspace_digest']}`.",
            f"- `final_answer_equal`: "
            f"**{pass_fail(scripted_equivalence['final_answer_equal'])}**",
            f"- `necessary_stateful_tool_calls_equal`: "
            f"**{pass_fail(scripted_equivalence['necessary_stateful_tool_calls_equal'])}**",
            f"- `workspace_digest_equal`: "
            f"**{pass_fail(scripted_equivalence['workspace_digest_equal'])}**",
            "",
            "> This scripted acceptance makes zero live-model or network calls. It is a "
            "deterministic repository-like integration fixture, not a repository solve-rate "
            "measurement. Real tools execute, but their non-semantic `duration_ms` transcript "
            "field is normalized to zero so archived context counts remain byte-reproducible.",
            "",
            "## Acceptance",
            "",
        ]
    )
    for key, value in acceptance.items():
        lines.append(f"- `{key}`: **{pass_fail(value)}**")
    lines.extend(
        [
            "",
            "## Mechanism-level safety gates",
            "",
            "- Summary content addressing includes algorithm, compressor implementation, "
            "declared compressor revision, model identity/configuration, exact prompt "
            "revision, source hash, and summary limit.",
            "- The model-backed compressor uses bounded hierarchical map/reduce: each "
            "untrusted source payload is at most 64,000 characters by default and a "
            "64-call ceiling fails closed. Partial results are never persisted.",
            "- The Agent durable-journal seam records compression `started`, `completed`, "
            "`failed`, and controlled-cancellation `abandoned` phases before emitting the "
            "final projection fact.",
            "- Runtime resume deterministically reconciles an unmatched compression "
            "`started` fact to `abandoned` before retrying; a completed content-addressed "
            "summary remains reusable. The synthetic A/B itself does not inject crashes.",
            "",
            "> This offline benchmark measures the context mechanism, not model task quality. "
            "The generated-summary arm uses a deterministic extractive stand-in so repeated "
            "runs are byte-comparable. The recency arm masks oldest observations without tool "
            "semantics until the same hard budget is met. The separate live-model trace-QA "
            "is archived in `docs/evaluations/context_live_ab_results.md`; it is still not a "
            "repository patch solve-rate measurement.",
            "",
            "The paired bootstrap unit is one synthetic scenario, not one repository task; "
            "its interval must not be interpreted as a solve-rate confidence interval.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_results(output_dir: Path, payload: dict[str, object]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "context_ab_results.json").write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "context_ab_results.md").write_text(
        _markdown(payload),
        encoding="utf-8",
    )


async def run(output_dir: Path) -> dict[str, object]:
    strategies = (
        "raw_hard_stop",
        "recency_observation_masking",
        "generic_summary",
        "deterministic_only",
        "tiered",
    )
    results = [
        await _run_one(scenario, strategy)
        for scenario in _scenarios()
        for strategy in strategies
    ]
    scripted = await _scripted_repository_acceptance()
    scripted_equivalence = scripted["equivalence"]
    scripted_safety = scripted["safety"]
    assert isinstance(scripted_equivalence, dict)
    assert isinstance(scripted_safety, dict)
    replacement = [item for item in results if item.category == "replacement_heavy"]
    non_redundant = [item for item in results if item.category == "non_redundant"]
    generic_calls = sum(
        item.compression_calls for item in replacement if item.strategy == "generic_summary"
    )
    tiered_calls = sum(
        item.compression_calls for item in replacement if item.strategy == "tiered"
    )
    calls_by_pair = {
        item.scenario: {
            candidate.strategy: candidate.compression_calls
            for candidate in replacement
            if candidate.scenario == item.scenario
            and candidate.strategy in {"generic_summary", "tiered"}
        }
        for item in replacement
    }
    paired_calls = tuple(
        (values["tiered"], values["generic_summary"])
        for _, values in sorted(calls_by_pair.items())
    )
    call_ratio_ci = _paired_bootstrap_ratio_ci(paired_calls)
    reduction = 100.0 * (generic_calls - tiered_calls) / max(1, generic_calls)
    tiered_replacement = [item for item in replacement if item.strategy == "tiered"]
    generic_non_redundant = sum(
        item.projected_chars
        for item in non_redundant
        if item.strategy == "generic_summary"
    )
    tiered_non_redundant = sum(
        item.projected_chars for item in non_redundant if item.strategy == "tiered"
    )
    non_redundant_ratio = tiered_non_redundant / max(1, generic_non_redundant)

    def mean_recall(items: list[Result], strategy: str) -> float:
        selected = [item.live_fact_recall for item in items if item.strategy == strategy]
        return sum(selected) / len(selected)

    replacement_recall_gain = 100.0 * (
        mean_recall(replacement, "tiered")
        - mean_recall(replacement, "recency_observation_masking")
    )
    failed_rerun_tiered = next(
        item
        for item in results
        if item.scenario == "success_then_failed_rerun" and item.strategy == "tiered"
    )
    non_redundant_recall_gain = 100.0 * (
        mean_recall(non_redundant, "tiered")
        - mean_recall(non_redundant, "recency_observation_masking")
    )
    tier1_fit_scenarios = {
        item.scenario
        for item in replacement
        if item.strategy == "deterministic_only" and item.tier1_fit
    }
    tier1_fit_tiered = [
        item
        for item in replacement
        if item.strategy == "tiered" and item.scenario in tier1_fit_scenarios
    ]
    aggregate = {
        "replacement_compressor_call_reduction_percent": round(reduction, 3),
        "replacement_compressor_call_ratio_bootstrap_95ci": [
            round(call_ratio_ci[0], 4),
            round(call_ratio_ci[1], 4),
        ],
        "replacement_tiered_mean_saved_percent": round(
            sum(item.saved_percent for item in tiered_replacement)
            / len(tiered_replacement),
            3,
        ),
        "non_redundant_projected_ratio": round(
            non_redundant_ratio, 4
        ),
        "replacement_tiered_minus_recency_recall_points": round(
            replacement_recall_gain,
            3,
        ),
        "non_redundant_tiered_minus_recency_recall_points": round(
            non_redundant_recall_gain,
            3,
        ),
    }
    acceptance = {
        "tier1_fit_replacement_tiered_zero_compressor_calls": (
            bool(tier1_fit_tiered)
            and all(item.compression_calls == 0 for item in tier1_fit_tiered)
        ),
        "replacement_call_reduction_at_least_50_percent": reduction >= 50.0,
        "replacement_call_ratio_bootstrap_upper_below_1": call_ratio_ci[1] < 1.0,
        "failed_rerun_preserves_prior_success": (
            failed_rerun_tiered.live_fact_recall == 1.0
            and failed_rerun_tiered.deterministic_evictions == 3
            and failed_rerun_tiered.compression_calls == 0
        ),
        "all_tiered_requests_within_hard_budget": all(
            not item.overflow for item in results if item.strategy == "tiered"
        ),
        "all_tiered_live_facts_retained": all(
            item.live_fact_recall == 1.0 for item in results if item.strategy == "tiered"
        ),
        "non_redundant_no_material_regression": non_redundant_ratio <= 1.05,
        "all_recency_requests_within_hard_budget": all(
            not item.overflow
            for item in results
            if item.strategy == "recency_observation_masking"
        ),
        "replacement_tiered_recall_better_than_recency": replacement_recall_gain > 0,
        "non_redundant_tiered_recall_gain_at_least_50_points": (
            non_redundant_recall_gain >= 50.0
        ),
        "scripted_arms_completed": bool(scripted_safety["arms_completed"]),
        "scripted_raw_active_context_exceeds_tiered_budget": bool(
            scripted_safety["raw_active_context_exceeds_tiered_budget"]
        ),
        "scripted_tiered_active_context_within_budget": bool(
            scripted_safety["tiered_active_context_within_budget"]
        ),
        "scripted_tiered_exercised_deterministic_eviction": bool(
            scripted_safety["tiered_exercised_deterministic_eviction"]
        ),
        "scripted_final_answer_equal": bool(scripted_equivalence["final_answer_equal"]),
        "scripted_necessary_stateful_tool_calls_equal": bool(
            scripted_equivalence["necessary_stateful_tool_calls_equal"]
        ),
        "scripted_workspace_digest_equal": bool(
            scripted_equivalence["workspace_digest_equal"]
        ),
    }
    payload: dict[str, object] = {
        "schema_version": 5,
        "benchmark": "context-governance-synthetic-ab",
        "python": platform.python_version(),
        "hard_limit_chars": HARD_LIMIT,
        "budget_metric": "provider-neutral-canonical-json-envelope-characters-v1",
        "statistical_protocol": {
            "paired_unit": "synthetic_scenario",
            "replacement_pair_count": len(paired_calls),
            "bootstrap_replicates": BOOTSTRAP_REPLICATES,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "interval": "paired_percentile_95ci",
            "tier1_fit_replacement_scenarios": sorted(tier1_fit_scenarios),
        },
        "strategies": list(strategies),
        "results": [asdict(item) for item in results],
        "aggregate": aggregate,
        "scripted_repository_acceptance": scripted,
        "acceptance": acceptance,
        "all_acceptance_passed": all(acceptance.values()),
    }
    _write_results(output_dir, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("docs/evaluations"),
    )
    args = parser.parse_args()
    payload = asyncio.run(run(args.output_dir))
    print(json.dumps(payload["aggregate"], sort_keys=True))
    if not payload["all_acceptance_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
