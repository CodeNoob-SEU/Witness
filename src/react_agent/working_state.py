"""Form-style working state for Tier 2 context compression.

A long repository task does not need a prose recap of everything that happened;
it needs a form that stays true as the run advances:

* the **goal** (the original user request, kept verbatim);
* a **ledger** of what was read, edited and executed, with outcomes — filled
  mechanically from the transcript and the tools' declared context policies, so
  it costs no model call and cannot hallucinate;
* short **notes** (findings, hypothesis, next steps, open questions) that only
  the model can write, updated incrementally from the previous notes and the
  turns since, never regenerated from the whole history.

The pieces are rendered into one bounded message that replaces the compressed
prefix. Everything here is pure and deterministic; the model call lives in
:class:`react_agent.context.ModelContextCompressor`.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .models import (
    ToolCall,
    ToolMessage,
    TranscriptItem,
    UserMessage,
    transcript_to_json,
)

WORKING_STATE_VERSION = "working-state-v1"
_OMITTED = "…"


@dataclass(frozen=True, slots=True)
class LedgerObservation:
    """One paired tool call and result, classified by the caller's policy."""

    call: ToolCall
    message: ToolMessage
    effect: str
    identity: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class WorkingNotes:
    """The model-written part of the form, with hard bounds on every field."""

    findings: tuple[str, ...] = ()
    hypothesis: str | None = None
    next_steps: tuple[str, ...] = ()
    open_questions: tuple[str, ...] = ()

    MAX_FINDINGS = 15
    MAX_STEPS = 8
    MAX_QUESTIONS = 8
    MAX_ITEM_CHARS = 400
    MAX_HYPOTHESIS_CHARS = 600

    def bounded(self) -> WorkingNotes:
        def items(values: Sequence[str], limit: int) -> tuple[str, ...]:
            cleaned: list[str] = []
            for value in values:
                text = " ".join(str(value).split())
                if text and text not in cleaned:
                    cleaned.append(text[: self.MAX_ITEM_CHARS])
                if len(cleaned) >= limit:
                    break
            return tuple(cleaned)

        hypothesis = " ".join(self.hypothesis.split()) if self.hypothesis else None
        return WorkingNotes(
            findings=items(self.findings, self.MAX_FINDINGS),
            hypothesis=(hypothesis[: self.MAX_HYPOTHESIS_CHARS] or None) if hypothesis else None,
            next_steps=items(self.next_steps, self.MAX_STEPS),
            open_questions=items(self.open_questions, self.MAX_QUESTIONS),
        )

    def is_empty(self) -> bool:
        return not (self.findings or self.hypothesis or self.next_steps or self.open_questions)

    def render(self) -> str:
        lines: list[str] = []
        if self.findings:
            lines.append("Findings:")
            lines.extend(f"- {item}" for item in self.findings)
        lines.append(f"Hypothesis: {self.hypothesis or '(none yet)'}")
        if self.next_steps:
            lines.append("Next steps:")
            lines.extend(f"- {item}" for item in self.next_steps)
        if self.open_questions:
            lines.append("Open questions:")
            lines.extend(f"- {item}" for item in self.open_questions)
        return "\n".join(lines)

    def shrink(self) -> WorkingNotes | None:
        """Drop the oldest, least actionable material first; None when nothing is left."""

        if self.open_questions:
            return WorkingNotes(
                self.findings, self.hypothesis, self.next_steps, self.open_questions[:-1]
            )
        if len(self.findings) > 1:
            return WorkingNotes(self.findings[1:], self.hypothesis, self.next_steps, ())
        if self.next_steps:
            return WorkingNotes(self.findings, self.hypothesis, self.next_steps[:-1], ())
        if self.hypothesis and self.findings:
            return WorkingNotes(self.findings, None, (), ())
        if self.findings or self.hypothesis:
            return WorkingNotes((), None, (), ())
        return None


_JSON_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL)


def parse_notes(text: str) -> WorkingNotes | None:
    """Parse a model reply into notes; None when it is not a usable JSON object."""

    candidate = text.strip()
    fenced = _JSON_FENCE.match(candidate)
    if fenced:
        candidate = fenced.group(1)
    start, end = candidate.find("{"), candidate.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        raw = json.loads(candidate[start : end + 1])
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None

    def strings(key: str) -> tuple[str, ...]:
        value = raw.get(key)
        if isinstance(value, str):
            return (value,) if value.strip() else ()
        if not isinstance(value, list):
            return ()
        return tuple(item for item in value if isinstance(item, str) and item.strip())

    hypothesis = raw.get("hypothesis")
    return WorkingNotes(
        findings=strings("findings"),
        hypothesis=hypothesis if isinstance(hypothesis, str) and hypothesis.strip() else None,
        next_steps=strings("next_steps"),
        open_questions=strings("open_questions"),
    ).bounded()


def render_notes_within(notes: WorkingNotes, max_chars: int) -> str | None:
    """Render notes, shrinking them until they fit; None if even one line cannot."""

    current: WorkingNotes | None = notes.bounded()
    while current is not None:
        rendered = current.render()
        if len(rendered) <= max_chars:
            return rendered if not current.is_empty() else None
        current = current.shrink()
    return None


def _envelope(message: ToolMessage) -> tuple[bool, Mapping[str, object], str | None]:
    """Return (ok, data, error_code) from the standard tool JSON envelope."""

    try:
        payload = json.loads(message.content)
    except (json.JSONDecodeError, TypeError):
        return (not message.is_error), {}, None
    if not isinstance(payload, dict):
        return (not message.is_error), {}, None
    if payload.get("context_evicted") is not None:
        return (not message.is_error), {}, None
    ok = payload.get("ok")
    data = payload.get("data")
    error = payload.get("error")
    code = error.get("code") if isinstance(error, dict) else None
    return (
        bool(ok) if isinstance(ok, bool) else not message.is_error,
        data if isinstance(data, dict) else {},
        code if isinstance(code, str) else None,
    )


def _outcome(observation: LedgerObservation) -> str:
    ok, data, code = _envelope(observation.message)
    parts: list[str] = []
    exit_code = data.get("exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool):
        parts.append(f"exit {exit_code}")
    if data.get("timed_out") is True:
        parts.append("timed out")
    if data.get("already_applied") is True:
        parts.append("already applied")
    if data.get("truncated") is True:
        parts.append("truncated")
    if not ok:
        parts.append(f"error {code}" if code else "error")
    elif not parts:
        parts.append("ok")
    return ", ".join(parts)


def _identity_text(observation: LedgerObservation) -> tuple[str, str]:
    """(primary resource, remaining identity detail) for grouping and display."""

    if not observation.identity:
        return "", ""
    (_, primary), *rest = observation.identity
    detail = " ".join(f"{name}={value}" for name, value in rest if value != "")
    return primary, detail


def render_ledger(observations: Sequence[LedgerObservation], *, max_chars: int) -> str:
    """Render the mechanical ledger, newest activity last, bounded by ``max_chars``."""

    reads: OrderedDict[tuple[str, str], list[LedgerObservation]] = OrderedDict()
    mutations: OrderedDict[tuple[str, str], list[LedgerObservation]] = OrderedDict()
    executions: OrderedDict[tuple[str, str], list[LedgerObservation]] = OrderedDict()
    others: OrderedDict[str, list[LedgerObservation]] = OrderedDict()
    for observation in observations:
        primary, _ = _identity_text(observation)
        key = (observation.call.name, primary)
        if observation.effect == "read":
            reads.setdefault(key, []).append(observation)
        elif observation.effect == "mutate":
            mutations.setdefault(key, []).append(observation)
        elif observation.effect == "execute":
            executions.setdefault((observation.call.name, observation.call.arguments), []).append(
                observation
            )
        else:
            others.setdefault(observation.call.name, []).append(observation)

    def summarize_group(name: str, primary: str, items: list[LedgerObservation]) -> str:
        details = []
        for item in items:
            _, detail = _identity_text(item)
            if detail and detail not in details:
                details.append(detail)
        last = _outcome(items[-1])
        count = f" x{len(items)}" if len(items) > 1 else ""
        target = f" {primary}" if primary else ""
        extra = f" [{'; '.join(details[-6:])}]" if details else ""
        return f"- {name}{target}{extra}{count}: {last}"

    sections: list[tuple[str, list[str]]] = []
    def group_lines(groups: OrderedDict[tuple[str, str], list[LedgerObservation]]) -> list[str]:
        return [summarize_group(name, primary, items) for (name, primary), items in groups.items()]

    if reads:
        sections.append(("Read", group_lines(reads)))
    if mutations:
        sections.append(("Edited", group_lines(mutations)))
    if executions:
        lines = []
        for (name, arguments), items in executions.items():
            try:
                parsed = json.loads(arguments)
            except json.JSONDecodeError:
                parsed = None
            shown = (
                " ".join(
                    f"{k}={v}"
                    for k, v in parsed.items()
                    if isinstance(v, (str, int, float, bool))
                )
                if isinstance(parsed, dict)
                else arguments
            )
            count = f" x{len(items)}" if len(items) > 1 else ""
            lines.append(f"- {name} {shown[:200]}{count}: {_outcome(items[-1])}")
        sections.append(("Executed", lines))
    if others:
        sections.append(
            (
                "Other calls",
                [
                    f"- {name} x{len(items)}: {_outcome(items[-1])}"
                    for name, items in others.items()
                ],
            )
        )
    if not sections:
        return ""

    def render(section_lines: list[tuple[str, list[str]]]) -> str:
        blocks = [f"{title}:\n" + "\n".join(lines) for title, lines in section_lines]
        return "\n".join(blocks)

    text = render(sections)
    if len(text) <= max_chars:
        return text
    # Keep the newest lines of every section; older activity is what the
    # deterministic tier has usually already superseded.
    trimmed = [(title, list(lines)) for title, lines in sections]
    while len(render(trimmed)) > max_chars:
        longest = max(trimmed, key=lambda section: len(section[1]))
        if len(longest[1]) <= 1:
            break
        marker = f"- {_OMITTED} earlier entries omitted"
        if longest[1][0] != marker:
            longest[1][0] = marker
        else:
            del longest[1][1]
    text = render(trimmed)
    return text if len(text) <= max_chars else text[: max(0, max_chars - 1)] + _OMITTED


def goal_text(transcript: Sequence[TranscriptItem], *, max_chars: int) -> str:
    """The original request, verbatim; truncated only with an explicit marker."""

    first = next((item for item in transcript if isinstance(item, UserMessage)), None)
    if first is None:
        return ""
    text = first.content
    if len(text) <= max_chars:
        return text
    head = max(0, max_chars - 60)
    return text[:head] + f"\n{_OMITTED}[goal truncated by {len(text) - head} chars]"


def render_working_state(
    *,
    goal: str,
    ledger: str,
    notes: str | None,
    covered_items: int,
    state_hash: str,
) -> str:
    """The single message that replaces the compressed transcript prefix."""

    header = (
        f"[working state; replaces transcript items 0..{covered_items - 1}; "
        f"state_sha256={state_hash}]"
    )
    sections = [header, "## Goal", goal or "(none)"]
    if ledger:
        sections += ["## Ledger (mechanical, exact)", ledger]
    sections += ["## Notes", notes or "(no notes yet)"]
    return "\n".join(sections)


def chain_hashes(
    transcript: Sequence[TranscriptItem],
    boundaries: Sequence[int],
) -> tuple[str, ...]:
    """Hash of the canonical transcript up to each boundary, chained in order.

    ``boundaries`` are increasing item indices (typically turn-group starts
    plus the transcript length). The hash at a boundary depends only on the
    immutable items before it, so any process can locate the newest persisted
    state along the chain without knowing when earlier compressions happened.
    """

    hashes: list[str] = []
    current = hashlib.sha256(WORKING_STATE_VERSION.encode()).hexdigest()
    previous = 0
    for boundary in boundaries:
        slice_hash = hashlib.sha256(
            json.dumps(
                transcript_to_json(transcript[previous:boundary]),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        current = hashlib.sha256(f"{current}\0{slice_hash}".encode()).hexdigest()
        hashes.append(current)
        previous = boundary
    return tuple(hashes)


def preview_tool_outputs(
    transcript: Sequence[TranscriptItem],
    *,
    max_chars: int,
    head_ratio: float = 0.7,
) -> tuple[TranscriptItem, ...]:
    """Shorten long tool outputs to head/tail previews; the ledger keeps the facts."""

    result: list[TranscriptItem] = []
    for item in transcript:
        if isinstance(item, ToolMessage) and len(item.content) > max_chars:
            head = int(max_chars * head_ratio)
            tail = max(0, max_chars - head)
            omitted = len(item.content) - head - tail
            preview = (
                item.content[:head]
                + f"\n{_OMITTED}[{omitted} chars omitted; full output is in the journal]"
                + f"{_OMITTED}\n"
                + (item.content[-tail:] if tail else "")
            )
            result.append(ToolMessage(item.call_id, item.name, preview, is_error=item.is_error))
        else:
            result.append(item)
    return tuple(result)


__all__ = [
    "WORKING_STATE_VERSION",
    "LedgerObservation",
    "WorkingNotes",
    "chain_hashes",
    "goal_text",
    "parse_notes",
    "preview_tool_outputs",
    "render_ledger",
    "render_notes_within",
    "render_working_state",
]
