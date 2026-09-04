from pathlib import Path

import pytest

from benchmarks.context_ab import HARD_LIMIT, SCRIPTED_FINAL_ANSWER, run


@pytest.mark.asyncio
async def test_scripted_repository_acceptance_is_non_vacuous_and_reproducible(
    tmp_path: Path,
) -> None:
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"

    first = await run(first_dir)
    scripted = first["scripted_repository_acceptance"]
    assert isinstance(scripted, dict)
    arms = scripted["arms"]
    equivalence = scripted["equivalence"]
    safety = scripted["safety"]
    assert isinstance(arms, dict)
    assert isinstance(equivalence, dict)
    assert isinstance(safety, dict)
    raw = arms["raw_reference"]
    tiered = arms["tiered"]
    assert isinstance(raw, dict) and isinstance(tiered, dict)
    raw_context = raw["context"]
    tiered_context = tiered["context"]
    raw_compression = raw["compression"]
    tiered_compression = tiered["compression"]
    assert isinstance(raw_context, dict) and isinstance(tiered_context, dict)
    assert isinstance(raw_compression, dict) and isinstance(tiered_compression, dict)

    assert first["all_acceptance_passed"] is True
    assert scripted["all_equivalence_passed"] is True
    assert scripted["all_safety_passed"] is True
    assert equivalence == {
        "final_answer_equal": True,
        "necessary_stateful_tool_calls_equal": True,
        "workspace_digest_equal": True,
    }
    assert all(safety.values())
    assert raw["final_answer"] == tiered["final_answer"] == SCRIPTED_FINAL_ANSWER
    assert raw["necessary_stateful_tool_calls"] == tiered["necessary_stateful_tool_calls"]
    assert raw["workspace_digest"] == tiered["workspace_digest"]
    assert raw["tool_executions"] == tiered["tool_executions"] == 9
    assert raw_context["canonical_chars"] == tiered_context["canonical_chars"]
    assert int(raw_context["peak_active_request_chars"]) > HARD_LIMIT
    assert int(tiered_context["peak_active_request_chars"]) <= HARD_LIMIT
    assert int(tiered_context["deterministic_evictions"]) > 0
    assert raw_compression["calls"] == 0
    assert set(tiered_compression) == {"cache_hits", "calls", "source_chars", "usage"}

    await run(second_dir)
    for filename in ("context_ab_results.json", "context_ab_results.md"):
        assert (first_dir / filename).read_bytes() == (second_dir / filename).read_bytes()
