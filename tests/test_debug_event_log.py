from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

import react_agent.debug_event_log as debug_event_log
from react_agent.debug_event_log import (
    MCPDebugEventJournal,
    load_debug_event_log,
    write_debug_event_log,
)
from react_agent.events import (
    GENESIS_HASH,
    PrivacyClass,
    RunEventDraft,
    RunEventKind,
    StoredRunEvent,
)


def event_chain() -> tuple[StoredRunEvent, ...]:
    event = StoredRunEvent.from_draft(
        RunEventDraft(
            kind=RunEventKind.RUN_STARTED,
            privacy=PrivacyClass.PRIVATE,
            data={"status": "running"},
        ),
        run_id="debug-log-test-run",
        sequence=1,
        operation_id="run:started",
        previous_hash=GENESIS_HASH,
        occurred_at=1.0,
    )
    return (event,)


@pytest.mark.skipif(os.name == "nt", reason="directory fsync is a POSIX durability seam")
def test_atomic_write_fsyncs_the_parent_directory_after_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations: list[str] = []
    original_fsync = debug_event_log.os.fsync
    original_replace = debug_event_log.os.replace

    def recording_fsync(descriptor: int) -> None:
        metadata = os.fstat(descriptor)
        target = "directory" if stat.S_ISDIR(metadata.st_mode) else "file"
        operations.append(f"fsync:{target}")
        original_fsync(descriptor)

    def recording_replace(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        operations.append("replace")
        original_replace(source, target)

    monkeypatch.setattr(debug_event_log.os, "fsync", recording_fsync)
    monkeypatch.setattr(debug_event_log.os, "replace", recording_replace)

    write_debug_event_log(tmp_path / "private-events.json", event_chain())

    assert operations == ["fsync:file", "replace", "fsync:directory"]


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_load_rejects_a_symlink_even_when_its_target_is_a_valid_private_log(
    tmp_path: Path,
) -> None:
    target = tmp_path / "private-events.json"
    write_debug_event_log(target, event_chain())
    link = tmp_path / "linked-events.json"
    link.symlink_to(target.name)

    with pytest.raises(ValueError, match="symbolic link"):
        load_debug_event_log(link)


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_write_rejects_a_final_symlink_without_modifying_its_target(tmp_path: Path) -> None:
    target = tmp_path / "protected-target.txt"
    target.write_text("protected content", encoding="utf-8")
    target.chmod(0o600)
    link = tmp_path / "private-events.json"
    link.symlink_to(target.name)

    with pytest.raises(ValueError, match="symbolic link"):
        write_debug_event_log(link, event_chain())

    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == "protected content"


@pytest.mark.skipif(os.name == "nt", reason="symlink creation is not reliably available on Windows")
def test_journal_construction_rejects_a_dangling_final_symlink_without_creating_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "must-not-be-created.json"
    link = tmp_path / "private-events.json"
    link.symlink_to(target.name)

    with pytest.raises(ValueError, match="symbolic link"):
        MCPDebugEventJournal(link)

    assert link.is_symlink()
    assert not target.exists()


def test_load_rejects_non_regular_files(tmp_path: Path) -> None:
    directory = tmp_path / "events-directory"
    directory.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="regular file"):
        load_debug_event_log(directory)


@pytest.mark.skipif(os.name == "nt", reason="POSIX private mode bits are unavailable on Windows")
def test_load_rejects_a_log_readable_by_group_or_other(tmp_path: Path) -> None:
    path = tmp_path / "public-events.json"
    write_debug_event_log(path, event_chain())
    path.chmod(0o644)

    with pytest.raises(ValueError, match="permissions are not private"):
        load_debug_event_log(path)
