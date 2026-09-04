"""Deterministic, zero-side-effect renderer for project PR evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .events import canonical_json
from .project_pr import (
    ProjectPREvent,
    ProjectPREventKind,
    ProjectPRState,
    fold_project_pr_events,
)


class ProjectPREvidenceError(ValueError):
    """The supplied workflow facts cannot form a reviewable evidence bundle."""


@dataclass(frozen=True, slots=True)
class ProjectPREvidenceArtifacts:
    json_text: str
    markdown_text: str
    digest: str


def generate_project_pr_evidence(
    events: Sequence[ProjectPREvent],
) -> ProjectPREvidenceArtifacts:
    """Render canonical JSON and Markdown without model, Forge, or workspace calls."""

    snapshot = fold_project_pr_events(events)
    if snapshot.state not in {
        ProjectPRState.CONFIRMED,
        ProjectPRState.ACTION_REQUIRED,
        ProjectPRState.INTEGRITY_FAILED,
    }:
        raise ProjectPREvidenceError(
            f"workflow evidence is not sealed from state {snapshot.state.value}"
        )
    if (
        snapshot.candidate_tree is None
        or snapshot.patch_digest is None
        or snapshot.verification_digest is None
        or snapshot.evidence_digest is None
    ):
        raise ProjectPREvidenceError("workflow is missing sealed revision evidence")

    recovery_events = tuple(
        event
        for event in events
        if event.kind
        in {
            ProjectPREventKind.PUBLISH_UNKNOWN,
            ProjectPREventKind.PUBLISH_ACTION_REQUIRED,
            ProjectPREventKind.PUBLISH_INTEGRITY_FAILED,
        }
    )
    manifest = {
        "schema_version": "witness.pr-evidence.v1",
        "workflow": {
            "workflow_id": snapshot.workflow_id,
            "project_key": snapshot.project_key,
            "revision": snapshot.revision,
            "goal": snapshot.goal,
        },
        "subject": {
            "repository": snapshot.repository,
            "pull_request_number": snapshot.pull_request_number,
            "base_sha": snapshot.base_sha,
            "head_sha": snapshot.head_sha,
            "candidate_tree": snapshot.candidate_tree,
        },
        "change_and_verification": {
            "patch_sha256": snapshot.patch_digest,
            "verification_sha256": snapshot.verification_digest,
            "source_evidence_sha256": snapshot.evidence_digest,
        },
        "publication": {
            "state": snapshot.state.value,
            "effect_id": snapshot.publish_effect_id,
            "check_run_id": snapshot.check_run_id,
            "outbound_create_attempts": snapshot.outbound_create_attempts,
            "remote_check_adopted": snapshot.remote_check_adopted,
            "observed_match_count": snapshot.observed_match_count,
            "publisher_fence": snapshot.publisher_fence,
            "reconciliation_polls": snapshot.reconciliation_polls,
        },
        "recovery": {
            "reconciliation_event_count": len(recovery_events),
            "event_ids": [event.event_id for event in recovery_events],
        },
        "journal": {
            "first_sequence": events[0].sequence,
            "last_sequence": snapshot.last_sequence,
            "event_count": len(events),
            "head_hash": snapshot.last_hash,
            "event_ids": [event.event_id for event in events],
        },
        "limitations": [
            "The unkeyed SHA-256 chain checks internal consistency but does not "
            "authenticate origin or resist an authorized full-log rewrite."
        ],
    }
    json_text = canonical_json(manifest) + "\n"
    digest = hashlib.sha256(json_text.encode()).hexdigest()
    markdown_text = _render_markdown(manifest, bundle_digest=digest)
    return ProjectPREvidenceArtifacts(
        json_text=json_text,
        markdown_text=markdown_text,
        digest=digest,
    )


def _render_markdown(manifest: Mapping[str, object], *, bundle_digest: str) -> str:
    workflow = manifest["workflow"]
    subject = manifest["subject"]
    change = manifest["change_and_verification"]
    publication = manifest["publication"]
    journal = manifest["journal"]
    assert isinstance(workflow, dict)
    assert isinstance(subject, dict)
    assert isinstance(change, dict)
    assert isinstance(publication, dict)
    assert isinstance(journal, dict)
    return "\n".join(
        (
            "# Witness PR Evidence",
            "",
            f"- Workflow: `{workflow['workflow_id']}` revision {workflow['revision']}",
            f"- Subject: `{subject['repository']}#{subject['pull_request_number']}`",
            f"- Base/head: `{subject['base_sha']}` → `{subject['head_sha']}`",
            f"- Candidate tree: `{subject['candidate_tree']}`",
            f"- Patch digest: `{change['patch_sha256']}`",
            f"- Verification digest: `{change['verification_sha256']}`",
            f"- Publication state: `{publication['state']}`",
            f"- Check Run: `{publication['check_run_id']}`",
            f"- Outbound create attempts: {publication['outbound_create_attempts']}",
            f"- Remote check adopted: {publication['remote_check_adopted']}",
            f"- Journal head: `{journal['head_hash']}` ({journal['event_count']} facts)",
            f"- Evidence JSON digest: `{bundle_digest}`",
            "",
            "## Integrity limitation",
            "",
            "The unkeyed SHA-256 chain detects internal inconsistency but does not "
            "authenticate origin or resist an authorized full-log rewrite.",
            "",
        )
    )
