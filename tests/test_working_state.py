"""Form-style Tier 2: mechanical ledger, bounded notes, rolling updates."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from react_agent import (
    AssistantMessage,
    ContextCompression,
    ContextGovernor,
    ContextStrategy,
    FileContextSummaryStore,
    ObservationEffect,
    ToolCall,
    ToolContextPolicy,
    ToolMessage,
    Usage,
    UserMessage,
    estimate_context_chars,
)
from react_agent.context import _hard_fallback
from react_agent.working_state import (
    LedgerObservation,
    WorkingNotes,
    chain_hashes,
    goal_text,
    parse_notes,
    preview_tool_outputs,
    render_ledger,
    render_notes_within,
)

POLICIES = {
    "read_file": ToolContextPolicy(ObservationEffect.READ, ("path", "start_line", "end_line")),
    "edit_file": ToolContextPolicy(ObservationEffect.MUTATE, ("path",)),
    "run_tests": ToolContextPolicy(ObservationEffect.EXECUTE, ("args",)),
}


def ok(data: object) -> str:
    return json.dumps({"ok": True, "data": data, "meta": {"truncated": False}})


def err(code: str) -> str:
    return json.dumps({"ok": False, "error": {"code": code, "message": "m", "retryable": False}})


def turn(call_id: str, name: str, arguments: str, content: str, *, is_error: bool = False):
    return (
        AssistantMessage(None, (ToolCall(call_id, name, arguments),)),
        ToolMessage(call_id, name, content, is_error=is_error),
    )


def observation(name: str, arguments: str, content: str, *, is_error: bool = False):
    call = ToolCall(f"c-{name}-{hash(arguments) & 0xFFFF}", name, arguments)
    policy = POLICIES.get(name, ToolContextPolicy())
    parsed = json.loads(arguments)
    identity = tuple(
        (k, str(parsed[k])) for k in policy.identity_fields if parsed.get(k) is not None
    )
    return LedgerObservation(
        call, ToolMessage(call.id, name, content, is_error=is_error), policy.effect.value, identity
    )


# --- ledger -------------------------------------------------------------------


def test_ledger_groups_reads_edits_and_commands_with_outcomes() -> None:
    ledger = render_ledger(
        (
            observation("read_file", '{"path":"src/a.py","start_line":1,"end_line":80}', ok({})),
            observation("read_file", '{"path":"src/a.py","start_line":200,"end_line":260}', ok({})),
            observation(
                "read_file", '{"path":"src/b.py","start_line":null,"end_line":null}', ok({})
            ),
            observation(
                "edit_file",
                '{"path":"src/a.py","old_string":"x","new_string":"y"}',
                ok({"already_applied": False}),
            ),
            observation(
                "edit_file",
                '{"path":"src/a.py","old_string":"q","new_string":"r"}',
                err("TOOL_ERROR"),
            ),
            observation(
                "run_tests",
                '{"args":"tests/test_a.py -x"}',
                ok({"exit_code": 1, "timed_out": False}),
            ),
            observation(
                "run_tests",
                '{"args":"tests/test_a.py -x"}',
                ok({"exit_code": 0, "timed_out": False}),
            ),
            observation(
                "run_tests", '{"args":"tests -q"}', ok({"exit_code": None, "timed_out": True})
            ),
        ),
        max_chars=4_000,
    )

    assert "Read:" in ledger and "Edited:" in ledger and "Executed:" in ledger
    assert (
        "- read_file src/a.py [start_line=1 end_line=80; start_line=200 end_line=260] x2: ok"
        in ledger
    )
    assert "- read_file src/b.py: ok" in ledger
    assert "- edit_file src/a.py x2: error TOOL_ERROR" in ledger
    assert "- run_tests args=tests/test_a.py -x x2: exit 0" in ledger
    assert "- run_tests args=tests -q: timed out" in ledger
    # No tool output text ever enters the ledger, only outcomes.
    assert '"data"' not in ledger


def test_ledger_is_bounded_and_keeps_the_newest_entries() -> None:
    observations = tuple(
        observation("read_file", f'{{"path":"src/file{i}.py"}}', ok({})) for i in range(200)
    )
    ledger = render_ledger(observations, max_chars=1_500)
    assert len(ledger) <= 1_500
    assert "earlier entries omitted" in ledger
    assert "src/file199.py" in ledger
    assert "src/file0.py" not in ledger


def test_ledger_handles_evicted_markers_and_non_envelope_outputs() -> None:
    marker = json.dumps({"context_evicted": {"v": 1, "reason": "reread"}})
    ledger = render_ledger(
        (
            observation("read_file", '{"path":"src/a.py"}', marker),
            observation("clock", "{}", "10:00"),
            observation("clock", "{}", "boom", is_error=True),
        ),
        max_chars=1_000,
    )
    assert "- read_file src/a.py: ok" in ledger
    assert "- clock x2: error" in ledger


# --- notes ---------------------------------------------------------------------


def test_notes_are_parsed_from_fenced_or_bare_json_and_bounded() -> None:
    fenced = (
        "```json\n"
        + json.dumps(
            {
                "findings": [f"finding {i}" for i in range(40)],
                "hypothesis": "h " * 1_000,
                "next_steps": ["a", "", "a", "b"],
                "open_questions": "one question",
                "extra": "ignored",
            }
        )
        + "\n```"
    )
    notes = parse_notes(fenced)
    assert notes is not None
    assert len(notes.findings) == WorkingNotes.MAX_FINDINGS
    assert len(notes.hypothesis or "") == WorkingNotes.MAX_HYPOTHESIS_CHARS
    assert notes.next_steps == ("a", "b")
    assert notes.open_questions == ("one question",)
    assert parse_notes("Sure! Here is prose without JSON.") is None
    assert parse_notes('{"findings": "single string"}') == WorkingNotes(findings=("single string",))
    assert parse_notes("[1, 2]") is None


def test_notes_render_shrinks_to_fit_and_reports_when_nothing_fits() -> None:
    notes = WorkingNotes(
        findings=("f1", "f2", "f3"),
        hypothesis="h",
        next_steps=("n1",),
        open_questions=("q1", "q2"),
    )
    full = notes.render()
    assert full.startswith(
        "Findings:\n- f1\n- f2\n- f3\nHypothesis: h\nNext steps:\n- n1\nOpen questions:"
    )
    shorter = render_notes_within(notes, len(full) - 1)
    assert shorter is not None and "q2" not in shorter and "f3" in shorter
    assert render_notes_within(notes, 10) is None


# --- chain hashes, goal, previews ----------------------------------------------


def test_chain_hashes_are_prefix_stable() -> None:
    a = (UserMessage("goal"), *turn("1", "t", "{}", "x"), *turn("2", "t", "{}", "y"))
    b = (*a, *turn("3", "t", "{}", "z"))
    boundaries_a = (1, 3, 5)
    boundaries_b = (1, 3, 5, 7)
    assert chain_hashes(a, boundaries_a) == chain_hashes(b, boundaries_b)[:3]
    changed = (UserMessage("goal"), *turn("1", "t", "{}", "DIFFERENT"), *turn("2", "t", "{}", "y"))
    assert chain_hashes(changed, boundaries_a)[0] == chain_hashes(a, boundaries_a)[0]
    assert chain_hashes(changed, boundaries_a)[1] != chain_hashes(a, boundaries_a)[1]


def test_goal_is_verbatim_or_truncated_with_a_marker() -> None:
    assert goal_text((UserMessage("fix it"),), max_chars=100) == "fix it"
    long = goal_text((UserMessage("g" * 500),), max_chars=200)
    assert len(long) <= 200 and "[goal truncated by" in long
    assert goal_text((), max_chars=100) == ""


def test_preview_keeps_head_and_tail_and_marks_the_gap() -> None:
    content = "H" * 1_000 + "M" * 5_000 + "T" * 1_000
    (previewed,) = preview_tool_outputs((ToolMessage("c", "t", content),), max_chars=1_000)
    assert isinstance(previewed, ToolMessage)
    assert previewed.content.startswith("H" * 700)
    assert previewed.content.endswith("T" * 300)
    assert "6000 chars omitted" in previewed.content
    (untouched,) = preview_tool_outputs((ToolMessage("c", "t", "short"),), max_chars=1_000)
    assert untouched.content == "short"


def test_hard_fallback_previews_older_outputs_before_blanking_them() -> None:
    transcript = (
        UserMessage("goal"),
        *turn("1", "read_file", '{"path":"a.py"}', "A" * 6_000),
        *turn("2", "read_file", '{"path":"b.py"}', "B" * 6_000),
        *turn("3", "read_file", '{"path":"c.py"}', "C" * 1_000),
    )
    limit = estimate_context_chars(transcript, instructions="", tool_specs=()) - 6_000
    projected, dropped, overflow = _hard_fallback(
        transcript, instructions="", tool_specs=(), hard_limit=limit, tail_preview_chars=2_000
    )
    assert dropped == 0 and overflow is False
    assert "chars omitted" in projected[2].content and projected[2].content.startswith("A" * 100)
    assert "chars omitted" in projected[4].content
    assert projected[6].content == "C" * 1_000  # newest turn untouched
    assert "hard_budget" not in "".join(
        item.content for item in projected if isinstance(item, ToolMessage)
    )


# --- rolling governor ----------------------------------------------------------


class RecordingCompressor:
    """Answers with notes that name the slice it saw; records every request."""

    revision = "recording-v1"

    def __init__(self) -> None:
        self.requests = []

    async def compress(self, request) -> ContextCompression:
        self.requests.append(request)
        seen = [item.call_id for item in request.source if isinstance(item, ToolMessage)]
        previous = request.previous_summary or ""
        summary = (previous + "\n" if previous else "") + f"saw {','.join(seen)}"
        return ContextCompression(summary, Usage(1, 1, 2), request_id=f"r{len(self.requests)}")


def growing_transcript(steps: int):
    items = [UserMessage("fix the bug")]
    for index in range(1, steps + 1):
        items.extend(
            turn(
                f"t{index}",
                "read_file",
                f'{{"path":"src/f{index}.py"}}',
                ok({"n": index}) + "x" * 900,
            )
        )
    return tuple(items)


@pytest.mark.asyncio
async def test_notes_roll_forward_one_slice_at_a_time(tmp_path: Path) -> None:
    store = FileContextSummaryStore(tmp_path / "state")
    compressor = RecordingCompressor()
    governor = ContextGovernor(
        compressor=compressor, store=store, keep_recent_turns=1, max_summary_chars=4_000
    )
    reports = []
    for steps in range(2, 8):
        projection = await governor.prepare(
            growing_transcript(steps),
            instructions="",
            tool_specs=(),
            tool_policies=POLICIES,
            hard_limit=1_800,
        )
        reports.append(projection.report)
        state = projection.transcript[0]
        assert isinstance(state, UserMessage)
        assert "## Goal\nfix the bug" in state.content
        assert f"src/f{steps - 1}.py" in state.content  # ledger covers the prefix
        assert f"saw t{steps - 1}" in state.content  # notes cover the newest folded turn
        assert len(projection.transcript) == 3  # state + the one kept turn

    # One compression per step, each over exactly the turns since the last notes.
    assert [r.compression_calls for r in reports] == [1] * 6
    assert [r.compression_cache_hit for r in reports] == [False] * 6
    slices = [
        [item.call_id for item in request.source if isinstance(item, ToolMessage)]
        for request in compressor.requests
    ]
    assert slices == [["t1"], ["t2"], ["t3"], ["t4"], ["t5"], ["t6"]]
    assert compressor.requests[0].previous_summary is None
    assert compressor.requests[3].previous_summary == "saw t1\nsaw t2\nsaw t3"
    assert all("read_file src/f" in request.ledger for request in compressor.requests[1:])
    # Source chars stay flat instead of growing with the run.
    sources = [r.compression_source_chars for r in reports]
    assert max(sources) < 1.5 * min(sources)

    # A fresh process with the same store finds the chain: no model call at all.
    fresh = RecordingCompressor()
    replay = await ContextGovernor(
        compressor=fresh, store=store, keep_recent_turns=1, max_summary_chars=4_000
    ).prepare(
        growing_transcript(7),
        instructions="",
        tool_specs=(),
        tool_policies=POLICIES,
        hard_limit=1_800,
    )
    assert replay.report.compression_cache_hit is True
    assert fresh.requests == []
    assert replay.transcript == projection.transcript


@pytest.mark.asyncio
async def test_a_new_process_resumes_the_chain_from_the_newest_persisted_notes(
    tmp_path: Path,
) -> None:
    store = FileContextSummaryStore(tmp_path / "state")
    first = RecordingCompressor()
    governor = ContextGovernor(compressor=first, store=store, keep_recent_turns=1)
    for steps in (2, 3, 4):
        await governor.prepare(
            growing_transcript(steps),
            instructions="",
            tool_specs=(),
            tool_policies=POLICIES,
            hard_limit=1_800,
        )

    # Three more turns happen elsewhere; a new process picks up at step 7.
    second = RecordingCompressor()
    projection = await ContextGovernor(compressor=second, store=store, keep_recent_turns=1).prepare(
        growing_transcript(7),
        instructions="",
        tool_specs=(),
        tool_policies=POLICIES,
        hard_limit=1_800,
    )

    assert len(second.requests) == 1
    assert second.requests[0].previous_summary == "saw t1\nsaw t2\nsaw t3"
    assert [
        item.call_id for item in second.requests[0].source if isinstance(item, ToolMessage)
    ] == ["t4", "t5", "t6"]
    assert "saw t4,t5,t6" in projection.transcript[0].content


@pytest.mark.asyncio
async def test_without_a_compressor_the_mechanical_form_still_replaces_the_prefix() -> None:
    projection = await ContextGovernor(
        strategy=ContextStrategy.TIERED, compressor=None, keep_recent_turns=1
    ).prepare(
        growing_transcript(5),
        instructions="",
        tool_specs=(),
        tool_policies=POLICIES,
        hard_limit=1_800,
    )
    state = projection.transcript[0]
    assert isinstance(state, UserMessage)
    assert "## Ledger (mechanical, exact)" in state.content
    assert "read_file src/f4.py: ok" in state.content
    assert "(no notes yet)" in state.content
    assert projection.report.compression_calls == 0
    assert projection.report.summary_key is None
    assert projection.report.overflow is False
