from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from benchmarks.context_live_ab import (
    ArmResult,
    _aggregate,
    _parse_facts,
    _run_arm,
    _source_tree_hash,
    build_instances,
    parse_args,
)
from react_agent import AssistantMessage, ModelOutcome, ModelResponse, Usage


def _result(
    instance_id: str,
    *,
    stratum: str,
    arm: str,
    compression_calls: int,
) -> ArmResult:
    return ArmResult(
        instance_id=instance_id,
        stratum=stratum,
        arm=arm,
        order=1 if arm == "generic" else 2,
        status="completed",
        error_type=None,
        error_code=None,
        requested_model="model",
        response_model="model@revision",
        response_model_verified=True,
        exact_answer=True,
        necessary_fact_recall=1.0,
        expected={"fact": "value"},
        observed={"fact": "value"},
        raw_response_sha256="a" * 64,
        input_chars=20_000,
        deterministic_chars=10_000,
        projected_chars=8_000,
        evictions=4,
        compression_calls=compression_calls,
        compression_source_chars=12_000 if compression_calls else 0,
        compression_cache_hit=False,
        hard_fallback=False,
        overflow=False,
        compression_input_tokens=100 if compression_calls else 0,
        compression_output_tokens=20 if compression_calls else 0,
        compression_total_tokens=120 if compression_calls else 0,
        main_input_tokens=2_000,
        main_output_tokens=20,
        main_total_tokens=2_020,
        projection_latency_ms=10.0,
        main_latency_ms=20.0,
        total_latency_ms=30.0,
    )


def test_live_trace_instances_have_fixed_twenty_pair_design() -> None:
    instances = build_instances(Path.cwd())

    assert len(instances) == 20
    assert sum(item.stratum == "replacement_heavy" for item in instances) == 16
    assert sum(item.stratum == "append_only" for item in instances) == 4
    assert all(len(item.expected) == 4 for item in instances)


def test_exact_answer_rejects_extra_fact_keys() -> None:
    observed, exact, recall = _parse_facts(
        '{"facts":{"fact":"value","extra":"must-fail-exact"}}',
        {"fact": "value"},
    )

    assert observed == {"fact": "value"}
    assert exact is False
    assert recall == 1.0


def test_source_tree_hash_covers_server_helper_and_dockerfiles(tmp_path: Path) -> None:
    for relative in ("benchmarks", "src/react_agent", "tests"):
        (tmp_path / relative).mkdir(parents=True)
    for relative in ("README.md", "pyproject.toml", "uv.lock"):
        (tmp_path / relative).write_text(relative, encoding="utf-8")
    (tmp_path / "benchmarks/context_live_ab.py").write_text("runner-v1", encoding="utf-8")
    server = tmp_path / "benchmarks/context_live_model_server.py"
    server.write_text("server-v1", encoding="utf-8")
    server_dockerfile = tmp_path / "benchmarks/Dockerfile.context-live-server"
    server_dockerfile.write_text("server-image-v1", encoding="utf-8")
    driver_dockerfile = tmp_path / "benchmarks/Dockerfile.context-live-driver"
    driver_dockerfile.write_text("driver-image-v1", encoding="utf-8")
    (tmp_path / "src/react_agent/context.py").write_text("context-v1", encoding="utf-8")
    (tmp_path / "src/react_agent/provider.py").write_text("provider-v1", encoding="utf-8")
    (tmp_path / "src/react_agent/models.py").write_text("models-v1", encoding="utf-8")
    (tmp_path / "src/react_agent/tools.py").write_text("tools-v1", encoding="utf-8")
    (tmp_path / "tests/test_placeholder.py").write_text("test-v1", encoding="utf-8")

    original = _source_tree_hash(tmp_path)
    server.write_text("server-v2", encoding="utf-8")
    server_changed = _source_tree_hash(tmp_path)
    driver_dockerfile.write_text("driver-image-v2", encoding="utf-8")
    dockerfile_changed = _source_tree_hash(tmp_path)

    assert len({original, server_changed, dockerfile_changed}) == 3


def test_aggregate_keeps_failures_in_denominator_and_rejects_model_mismatch() -> None:
    results: list[ArmResult] = []
    for index in range(20):
        stratum = "replacement_heavy" if index < 16 else "append_only"
        instance_id = f"instance-{index:02d}"
        generic = _result(
            instance_id,
            stratum=stratum,
            arm="generic",
            compression_calls=1,
        )
        tiered = _result(
            instance_id,
            stratum=stratum,
            arm="tiered",
            compression_calls=0 if stratum == "replacement_heavy" else 1,
        )
        if index == 0:
            generic = replace(
                generic,
                status="failed",
                error_type="RuntimeError",
                exact_answer=False,
                necessary_fact_recall=0.0,
            )
        if index == 1:
            tiered = replace(
                tiered,
                status="failed",
                error_type="RuntimeError",
                error_code="response_model_mismatch",
                response_model="wrong@revision",
                response_model_verified=False,
                exact_answer=False,
                necessary_fact_recall=0.0,
            )
        results.extend((generic, tiered))

    aggregate, acceptance = _aggregate(results, seed=7, replicates=100)

    assert aggregate["instance_pairs"] == 20
    counts = cast(dict[str, dict[str, int]], aggregate["counts"])
    assert counts["generic"] == {
        "assigned": 20,
        "started": 20,
        "completed": 19,
        "failed": 1,
        "analyzed": 20,
    }
    assert aggregate["generic_exact_accuracy"] == 0.95
    assert aggregate["response_model_mismatch_runs"] == 1
    assert acceptance["twenty_paired_instances_analyzed"] is True
    assert acceptance["all_assigned_runs_completed"] is False
    assert acceptance["all_response_models_match_expected_model"] is False


def test_external_api_provenance_accepts_na_container_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "environment-secret")
    args = parse_args(
        [
            "--base-url",
            "https://example.invalid/v1",
            "--api-key",
            "argv-secret",
            "--provenance-mode",
            "external_api",
            "--model",
            "requested-model-id",
            "--expected-response-model",
            "gpt-5.6-terra",
            "--provider-label",
            "compatible-provider",
            "--endpoint-label",
            "production-east",
            "--git-sha",
            "deadbeef",
            "--git-dirty",
            "false",
            "--network-topology",
            "external HTTPS",
            "--hardware",
            "provider managed",
        ]
    )

    assert args.api_key == "environment-secret"
    assert args.model == "requested-model-id"
    assert args.expected_response_model == "gpt-5.6-terra"
    assert args.provider_label == "compatible-provider"
    assert args.endpoint_label == "production-east"
    assert args.model_revision is None
    assert args.server_image_ref is None
    assert args.driver_image_digest is None


def test_local_docker_expected_response_model_defaults_to_model_at_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    args = parse_args(
        [
            "--base-url",
            "http://model-server:8000/v1",
            "--model",
            "local-model",
            "--model-revision",
            "revision-123",
            "--server-image-ref",
            "server:local",
            "--server-image-digest",
            "sha256:server",
            "--server-base-image-digest",
            "sha256:server-base",
            "--driver-image-ref",
            "driver:local",
            "--driver-image-digest",
            "sha256:driver",
            "--driver-base-image-digest",
            "sha256:driver-base",
            "--git-sha",
            "deadbeef",
            "--git-dirty",
            "false",
            "--server-build-command",
            "docker build server",
            "--driver-build-command",
            "docker build driver",
            "--server-run-command",
            "docker run server",
            "--driver-run-command",
            "docker run driver",
            "--network-topology",
            "internal docker",
            "--hardware",
            "local gpu",
        ]
    )

    assert args.provenance_mode == "local_docker"
    assert args.api_key == "local-no-secret"
    assert args.expected_response_model == "local-model@revision-123"
    assert args.provider_label == "local-openai-compatible"
    assert args.endpoint_label == "unpublished-docker-network"


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--provider-label", "https://user:password@example.invalid"),
        ("--endpoint-label", "prod?api_key=secret"),
    ],
)
def test_provenance_labels_reject_urls_and_credential_shaped_values(
    flag: str,
    value: str,
) -> None:
    argv = [
        "--base-url",
        "https://example.invalid/v1",
        "--provenance-mode",
        "external_api",
        "--expected-response-model",
        "gpt-5.6-terra",
        "--provider-label",
        "provider",
        "--endpoint-label",
        "endpoint",
        "--git-sha",
        "deadbeef",
        "--git-dirty",
        "false",
        "--network-topology",
        "external HTTPS",
        "--hardware",
        "provider managed",
    ]
    argv[argv.index(flag) + 1] = value

    with pytest.raises(SystemExit):
        parse_args(argv)


@pytest.mark.asyncio
async def test_failed_main_call_preserves_completed_compression_metrics() -> None:
    class CompressionThenFailure:
        model = "model"
        api_mode = "chat_completions"
        max_output_tokens = 1_200
        temperature = 0.0

        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, request: object) -> ModelResponse:
            del request
            self.calls += 1
            if self.calls == 1:
                return ModelResponse(
                    message=AssistantMessage("handoff"),
                    usage=Usage(11, 7, 18),
                    response_model="model@revision",
                    outcome=ModelOutcome.COMPLETED,
                )
            raise RuntimeError("main provider failed")

    result = await _run_arm(
        build_instances(Path.cwd())[0],
        arm="generic",
        order=1,
        model=CompressionThenFailure(),  # type: ignore[arg-type]
        expected_response_model="model@revision",
        hard_limit=16_000,
        summary_chars=3_200,
    )

    assert result.status == "failed"
    assert result.compression_calls == 1
    assert result.compression_total_tokens == 18
    assert result.projection_latency_ms > 0


@pytest.mark.asyncio
async def test_returned_usage_above_requested_cap_is_archived_unchanged() -> None:
    instance = build_instances(Path.cwd())[0]

    class CompressionThenCompletion:
        model = "requested-model"
        api_mode = "chat_completions"
        max_output_tokens = 1_200
        temperature = 0.0

        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, request: object) -> ModelResponse:
            del request
            self.calls += 1
            if self.calls == 1:
                return ModelResponse(
                    message=AssistantMessage("handoff"),
                    usage=Usage(11, 7, 18),
                    response_model="gpt-5.6-terra",
                    outcome=ModelOutcome.COMPLETED,
                )
            return ModelResponse(
                message=AssistantMessage(json.dumps({"facts": instance.expected})),
                usage=Usage(2_000, 1_401, 3_401),
                response_model="gpt-5.6-terra",
                outcome=ModelOutcome.COMPLETED,
            )

    result = await _run_arm(
        instance,
        arm="generic",
        order=1,
        model=CompressionThenCompletion(),  # type: ignore[arg-type]
        expected_response_model="gpt-5.6-terra",
        hard_limit=16_000,
        summary_chars=3_200,
    )

    assert result.status == "completed"
    assert result.requested_model == "requested-model"
    assert result.response_model == "gpt-5.6-terra"
    assert result.main_output_tokens == 1_401
    assert result.main_total_tokens == 3_401
