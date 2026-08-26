"""Materialize a run's patch from Git, and attribute it to durable events.

The durable log is deliberately content-free: a workspace checkpoint records
``tree_id`` / ``commit_id`` / ``baseline_revision`` and a count-only
:class:`~react_agent.workspace.DiffSummary`, never a line of source. That keeps
audit retention independent of code retention — but it means the console has to
rebuild the patch from Git on demand rather than read it out of the log.

Rebuilding it here has a second payoff. Because every tool call is bracketed by
a ``before_tool`` / ``after_tool`` checkpoint, the calls that actually changed
the tree are identifiable *from tree ids alone* — not from what the model said
it did, and not from tool arguments (which the default ``METADATA`` debug
exposure keeps out of the log entirely). Provenance is therefore derived from
Git, which cannot be talked into agreeing with a hallucinated summary.

    patch = materialize_run_patch(repository, events)
    patch.files[0].origins  # -> the tool calls that produced that file
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from .events import RunEventKind, StoredRunEvent

MAX_PATCH_BYTES = 512 * 1024
MAX_PATCH_FILES = 60

ChangeKind = Literal["added", "modified", "deleted", "renamed"]
LineKind = Literal["context", "added", "removed"]

_HUNK_HEADER = re.compile(
    r"^@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? "
    r"\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@(?P<section>.*)$"
)


class PatchUnavailableError(RuntimeError):
    """The patch cannot be rebuilt (no workspace, or Git refuses the range)."""


# --------------------------------------------------------------------------
# Durable projections: what the event log says about the workspace.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkspaceCheckpointRef:
    """One ``workspace_checkpointed`` event, reduced to its identity."""

    sequence: int
    phase: str
    call_key: str | None
    baseline_revision: str
    tree_id: str
    commit_id: str
    paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PatchOrigin:
    """The durable facts behind one tree-changing tool call."""

    call_key: str
    step: int | None
    tool_name: str | None
    before_tree: str
    after_tree: str
    before_commit: str
    after_commit: str
    paths: tuple[str, ...]
    planned_sequence: int | None = None
    started_sequence: int | None = None
    completed_sequence: int | None = None
    model_sequence: int | None = None
    execution_id: str | None = None
    attempts: int = 1
    cost_micros: int | None = None
    currency: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "call_key": self.call_key,
            "step": self.step,
            "tool_name": self.tool_name,
            "before_tree": self.before_tree,
            "after_tree": self.after_tree,
            "before_commit": self.before_commit,
            "after_commit": self.after_commit,
            "paths": list(self.paths),
            "planned_sequence": self.planned_sequence,
            "started_sequence": self.started_sequence,
            "completed_sequence": self.completed_sequence,
            "model_sequence": self.model_sequence,
            "execution_id": self.execution_id,
            "attempts": self.attempts,
            "cost_micros": self.cost_micros,
            "currency": self.currency,
        }


# --------------------------------------------------------------------------
# The patch itself.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchLine:
    kind: LineKind
    old_line: int | None
    new_line: int | None
    text: str

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "old_line": self.old_line,
            "new_line": self.new_line,
            "text": self.text,
        }


@dataclass(frozen=True, slots=True)
class PatchHunk:
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    section: str
    lines: tuple[PatchLine, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "old_start": self.old_start,
            "old_count": self.old_count,
            "new_start": self.new_start,
            "new_count": self.new_count,
            "section": self.section,
            "lines": [line.to_json() for line in self.lines],
        }


@dataclass(frozen=True, slots=True)
class FilePatch:
    path: str
    change: ChangeKind
    additions: int
    deletions: int
    binary: bool = False
    old_path: str | None = None
    hunks: tuple[PatchHunk, ...] = ()
    origins: tuple[PatchOrigin, ...] = ()

    @property
    def attribution(self) -> Literal["exact", "shared", "unattributed"]:
        """How precisely this file's changes map to a single tool call.

        ``exact`` means one call changed the path, so every hunk in it belongs
        to that call. ``shared`` means several calls rewrote the file and the
        console must not pretend to know which one owns a given line.
        """

        if not self.origins:
            return "unattributed"
        return "exact" if len(self.origins) == 1 else "shared"

    def to_json(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "old_path": self.old_path,
            "change": self.change,
            "additions": self.additions,
            "deletions": self.deletions,
            "binary": self.binary,
            "attribution": self.attribution,
            "hunks": [hunk.to_json() for hunk in self.hunks],
            "origins": [origin.to_json() for origin in self.origins],
        }


@dataclass(frozen=True, slots=True)
class RunPatch:
    baseline_revision: str
    head_commit: str | None
    head_tree: str | None
    files: tuple[FilePatch, ...] = ()
    truncated: bool = False
    checkpoints: tuple[WorkspaceCheckpointRef, ...] = field(default=(), repr=False)

    @property
    def additions(self) -> int:
        return sum(item.additions for item in self.files)

    @property
    def deletions(self) -> int:
        return sum(item.deletions for item in self.files)

    def to_json(self) -> dict[str, Any]:
        return {
            "baseline_revision": self.baseline_revision,
            "head_commit": self.head_commit,
            "head_tree": self.head_tree,
            "files_changed": len(self.files),
            "additions": self.additions,
            "deletions": self.deletions,
            "truncated": self.truncated,
            "files": [item.to_json() for item in self.files],
        }


# --------------------------------------------------------------------------
# Projection helpers.
# --------------------------------------------------------------------------


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def workspace_checkpoints(
    events: Sequence[StoredRunEvent],
) -> tuple[WorkspaceCheckpointRef, ...]:
    """Reduce the log to its workspace checkpoints, in sequence order."""

    refs: list[WorkspaceCheckpointRef] = []
    for event in events:
        if event.kind != RunEventKind.WORKSPACE_CHECKPOINTED:
            continue
        data: Mapping[str, Any] = event.data
        tree_id = _text(data.get("tree_id"))
        commit_id = _text(data.get("commit_id"))
        baseline = _text(data.get("baseline_revision"))
        if tree_id is None or commit_id is None or baseline is None:
            continue
        raw_diff = data.get("diff")
        raw_paths = raw_diff.get("paths") if isinstance(raw_diff, Mapping) else None
        paths = tuple(item for item in raw_paths or () if isinstance(item, str))
        refs.append(
            WorkspaceCheckpointRef(
                sequence=event.sequence,
                phase=_text(data.get("phase")) or "unknown",
                call_key=_text(data.get("call_key")),
                baseline_revision=baseline,
                tree_id=tree_id,
                commit_id=commit_id,
                paths=paths,
            )
        )
    return tuple(refs)


def patch_origins(events: Sequence[StoredRunEvent]) -> tuple[PatchOrigin, ...]:
    """Find the tool calls that actually changed the workspace tree.

    A call is included only when its ``after_tool`` tree differs from its
    ``before_tool`` tree. A read that claimed to write, or a write that wrote
    identical bytes, leaves no trace here — which is the point.
    """

    checkpoints = workspace_checkpoints(events)
    before: dict[str, WorkspaceCheckpointRef] = {}
    pairs: list[tuple[WorkspaceCheckpointRef, WorkspaceCheckpointRef]] = []
    for ref in checkpoints:
        if ref.call_key is None:
            continue
        if ref.phase == "before_tool":
            before[ref.call_key] = ref
        elif ref.phase == "after_tool":
            opening = before.pop(ref.call_key, None)
            if opening is not None and opening.tree_id != ref.tree_id:
                pairs.append((opening, ref))

    tool_facts = _tool_facts(events)
    model_sequences = _model_sequences(events)
    costs = _step_costs(events)

    origins: list[PatchOrigin] = []
    for opening, closing in pairs:
        call_key = closing.call_key or ""
        facts = tool_facts.get(call_key, {})
        step = _int(facts.get("step"))
        cost_micros, currency = costs.get(step, (None, None)) if step is not None else (None, None)
        origins.append(
            PatchOrigin(
                call_key=call_key,
                step=step,
                tool_name=_text(facts.get("tool_name")),
                before_tree=opening.tree_id,
                after_tree=closing.tree_id,
                before_commit=opening.commit_id,
                after_commit=closing.commit_id,
                paths=closing.paths,
                planned_sequence=_int(facts.get("planned_sequence")),
                started_sequence=_int(facts.get("started_sequence")),
                completed_sequence=_int(facts.get("completed_sequence")),
                model_sequence=model_sequences.get(step) if step is not None else None,
                execution_id=_text(facts.get("execution_id")),
                attempts=_int(facts.get("attempts")) or 1,
                cost_micros=cost_micros,
                currency=currency,
            )
        )
    return tuple(origins)


def _tool_facts(events: Sequence[StoredRunEvent]) -> dict[str, dict[str, Any]]:
    facts: dict[str, dict[str, Any]] = {}
    sequence_field = {
        RunEventKind.TOOL_PLANNED: "planned_sequence",
        RunEventKind.TOOL_STARTED: "started_sequence",
        RunEventKind.TOOL_CLAIMED: "started_sequence",
        RunEventKind.TOOL_COMPLETED: "completed_sequence",
    }
    for event in events:
        field_name = sequence_field.get(RunEventKind(event.kind))
        if field_name is None or event.call_key is None:
            continue
        entry = facts.setdefault(event.call_key, {"attempts": 0})
        entry.setdefault(field_name, event.sequence)
        entry["step"] = event.step
        entry["execution_id"] = event.execution_id
        name = _text(event.data.get("tool_name"))
        if name is not None:
            entry["tool_name"] = name
        if RunEventKind(event.kind) in {RunEventKind.TOOL_STARTED, RunEventKind.TOOL_CLAIMED}:
            entry["attempts"] = int(entry.get("attempts") or 0) + 1
    return facts


def _model_sequences(events: Sequence[StoredRunEvent]) -> dict[int, int]:
    """The last ``model_completed`` sequence for each step: the call that decided."""

    sequences: dict[int, int] = {}
    for event in events:
        if RunEventKind(event.kind) is RunEventKind.MODEL_COMPLETED and event.step is not None:
            sequences[event.step] = event.sequence
    return sequences


def _step_costs(events: Sequence[StoredRunEvent]) -> dict[int, tuple[int | None, str | None]]:
    costs: dict[int, tuple[int | None, str | None]] = {}
    for event in events:
        if RunEventKind(event.kind) is not RunEventKind.COST_RECORDED or event.step is None:
            continue
        costs[event.step] = (
            _int(event.data.get("amount_micros")),
            _text(event.data.get("currency")),
        )
    return costs


# --------------------------------------------------------------------------
# Git.
# --------------------------------------------------------------------------


def _git_diff(repository: str, old: str, new: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-C",
            repository,
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--find-renames",
            "--unified=3",
            old,
            new,
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        # Git errors quote object ids and can quote deployment paths; neither
        # belongs in an HTTP response.
        raise PatchUnavailableError("Git could not diff the recorded checkpoints.")
    return completed.stdout


def _changed_paths(repository: str, old: str, new: str) -> tuple[str, ...]:
    completed = subprocess.run(
        ["git", "-C", repository, "diff", "--no-color", "--name-only", old, new],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ()
    return tuple(line for line in completed.stdout.splitlines() if line)


def parse_unified_diff(diff: str) -> tuple[tuple[FilePatch, ...], bool]:
    """Parse ``git diff`` output into structured files and hunks."""

    truncated = False
    if len(diff.encode("utf-8")) > MAX_PATCH_BYTES:
        diff = diff.encode("utf-8")[:MAX_PATCH_BYTES].decode("utf-8", "ignore")
        truncated = True

    files: list[FilePatch] = []
    path: str | None = None
    old_path: str | None = None
    change: ChangeKind = "modified"
    binary = False
    hunks: list[PatchHunk] = []
    hunk_lines: list[PatchLine] = []
    header: dict[str, int | str] | None = None
    old_line = 0
    new_line = 0

    def close_hunk() -> None:
        nonlocal hunk_lines, header
        if header is not None:
            hunks.append(
                PatchHunk(
                    old_start=int(header["old_start"]),
                    old_count=int(header["old_count"]),
                    new_start=int(header["new_start"]),
                    new_count=int(header["new_count"]),
                    section=str(header["section"]),
                    lines=tuple(hunk_lines),
                )
            )
        hunk_lines = []
        header = None

    def close_file() -> None:
        nonlocal path, old_path, change, binary, hunks
        close_hunk()
        if path is not None:
            additions = sum(
                1 for hunk in hunks for line in hunk.lines if line.kind == "added"
            )
            deletions = sum(
                1 for hunk in hunks for line in hunk.lines if line.kind == "removed"
            )
            files.append(
                FilePatch(
                    path=path,
                    change=change,
                    additions=additions,
                    deletions=deletions,
                    binary=binary,
                    old_path=old_path if old_path != path else None,
                    hunks=tuple(hunks),
                )
            )
        path = None
        old_path = None
        change = "modified"
        binary = False
        hunks = []

    for raw in diff.splitlines():
        if raw.startswith("diff --git "):
            close_file()
            parts = raw.split(" b/", 1)
            if len(parts) == 2:
                old_path = parts[0][len("diff --git a/") :]
                path = parts[1]
            continue
        if path is None:
            continue
        if raw.startswith("new file mode"):
            change = "added"
            continue
        if raw.startswith("deleted file mode"):
            change = "deleted"
            continue
        if raw.startswith("rename from ") or raw.startswith("rename to "):
            change = "renamed"
            continue
        if raw.startswith("Binary files "):
            binary = True
            continue
        if raw.startswith("@@"):
            close_hunk()
            match = _HUNK_HEADER.match(raw)
            if match is None:
                continue
            old_line = int(match.group("old_start"))
            new_line = int(match.group("new_start"))
            header = {
                "old_start": old_line,
                "old_count": int(match.group("old_count") or 1),
                "new_start": new_line,
                "new_count": int(match.group("new_count") or 1),
                "section": match.group("section").strip(),
            }
            continue
        if header is None:
            continue
        if raw.startswith("+"):
            hunk_lines.append(PatchLine("added", None, new_line, raw[1:]))
            new_line += 1
        elif raw.startswith("-"):
            hunk_lines.append(PatchLine("removed", old_line, None, raw[1:]))
            old_line += 1
        elif raw.startswith(" "):
            hunk_lines.append(PatchLine("context", old_line, new_line, raw[1:]))
            old_line += 1
            new_line += 1
        elif raw.startswith("\\"):
            # "\ No newline at end of file" annotates the previous line.
            continue

    close_file()
    if len(files) > MAX_PATCH_FILES:
        files = files[:MAX_PATCH_FILES]
        truncated = True
    return tuple(files), truncated


def materialize_run_patch(
    repository: str,
    events: Sequence[StoredRunEvent],
) -> RunPatch:
    """Rebuild the run's patch from Git and attach its durable provenance."""

    checkpoints = workspace_checkpoints(events)
    if not checkpoints:
        raise PatchUnavailableError("This run has no workspace checkpoints.")

    # Diff this run's own opening checkpoint, not the Session baseline. A
    # Session's worktree accumulates across runs, so anchoring on the baseline
    # would credit each run with everything its predecessors in the same Session
    # had already changed.
    start = checkpoints[0]
    head = checkpoints[-1]
    if head.tree_id == start.tree_id:
        return RunPatch(
            baseline_revision=start.baseline_revision,
            head_commit=head.commit_id,
            head_tree=head.tree_id,
            checkpoints=checkpoints,
        )

    files, truncated = parse_unified_diff(
        _git_diff(repository, start.commit_id, head.commit_id)
    )

    # Attribution is by path, not by line. A file touched by exactly one call
    # maps every hunk to that call; a file rewritten several times lists them in
    # order rather than guessing which rewrite owns a given line.
    by_path: dict[str, list[PatchOrigin]] = {}
    for origin in patch_origins(events):
        touched = _changed_paths(repository, origin.before_commit, origin.after_commit)
        for changed in touched or origin.paths:
            by_path.setdefault(changed, []).append(origin)

    attributed = tuple(
        FilePatch(
            path=item.path,
            change=item.change,
            additions=item.additions,
            deletions=item.deletions,
            binary=item.binary,
            old_path=item.old_path,
            hunks=item.hunks,
            origins=tuple(by_path.get(item.path, ())),
        )
        for item in files
    )
    return RunPatch(
        baseline_revision=start.baseline_revision,
        head_commit=head.commit_id,
        head_tree=head.tree_id,
        files=attributed,
        truncated=truncated,
        checkpoints=checkpoints,
    )


__all__ = [
    "MAX_PATCH_BYTES",
    "MAX_PATCH_FILES",
    "ChangeKind",
    "FilePatch",
    "PatchHunk",
    "PatchLine",
    "PatchOrigin",
    "PatchUnavailableError",
    "RunPatch",
    "WorkspaceCheckpointRef",
    "materialize_run_patch",
    "parse_unified_diff",
    "patch_origins",
    "workspace_checkpoints",
]
