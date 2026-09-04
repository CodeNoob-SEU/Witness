from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from react_agent.workspace import (
    FakeWorkspaceCheckpointStore,
    GitWorktreeWorkspace,
    InMemoryWorkspaceCheckpointStore,
    WorkspaceCheckpointStore,
    WorkspaceSafetyError,
)


def git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    root = tmp_path / "primary"
    root.mkdir()
    git(root, "init", "--quiet")
    git(root, "config", "user.name", "Workspace Tests")
    git(root, "config", "user.email", "workspace@example.test")
    (root / ".gitignore").write_text("ignored.txt\nignored-dir/\n", encoding="utf-8")
    (root / "README.md").write_text("baseline\n", encoding="utf-8")
    git(root, "add", ".gitignore", "README.md")
    git(root, "commit", "--quiet", "-m", "baseline")
    return root


def primary_state(repository: Path) -> tuple[str, str, bytes, str]:
    return (
        git(repository, "rev-parse", "HEAD"),
        git(repository, "status", "--porcelain=v1", "--untracked-files=all"),
        (repository / "README.md").read_bytes(),
        git(repository, "write-tree"),
    )


def test_git_adapter_checkpoints_without_mutating_primary_worktree(
    repository: Path,
    tmp_path: Path,
) -> None:
    (repository / "README.md").write_text("primary dirty content\n", encoding="utf-8")
    (repository / "primary-staged.txt").write_text("staged\n", encoding="utf-8")
    git(repository, "add", "primary-staged.txt")
    (repository / "primary-untracked.txt").write_text("untracked\n", encoding="utf-8")
    before = primary_state(repository)
    store = GitWorktreeWorkspace(repository, tmp_path / "managed")

    workspace = store.create("session-one")
    (workspace.path / "README.md").write_text("baseline\nchanged\n", encoding="utf-8")
    (workspace.path / "notes.txt").write_text("new\n", encoding="utf-8")
    workspace_status = git(
        workspace.path,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    checkpoint = store.checkpoint("session-one")

    assert isinstance(store, WorkspaceCheckpointStore)
    assert checkpoint.baseline_revision == before[0]
    assert checkpoint.diff.files_changed == 2
    assert checkpoint.diff.paths == ("README.md", "notes.txt")
    assert checkpoint.diff.additions == 2
    assert git(repository, "cat-file", "-t", checkpoint.internal_ref) == "commit"
    assert git(repository, "rev-parse", f"{checkpoint.internal_ref}^{{tree}}") == checkpoint.tree_id
    assert (
        git(workspace.path, "status", "--porcelain=v1", "--untracked-files=all")
        == workspace_status
    )
    assert primary_state(repository) == before

    assert store.cleanup("session-one") is True
    assert not workspace.path.exists()
    assert primary_state(repository) == before


def test_checkpoint_fork_is_clean_and_content_identical(
    repository: Path,
    tmp_path: Path,
) -> None:
    store = GitWorktreeWorkspace(repository, tmp_path / "managed")
    source = store.create("source")
    (source.path / "README.md").write_text("forked content\n", encoding="utf-8")
    (source.path / "src.txt").write_text("isolated\n", encoding="utf-8")

    checkpoint = store.checkpoint("source")
    forked = store.fork(checkpoint, "forked")

    assert (forked.path / "README.md").read_text(encoding="utf-8") == "forked content\n"
    assert (forked.path / "src.txt").read_text(encoding="utf-8") == "isolated\n"
    assert git(forked.path, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert git(forked.path, "rev-parse", "HEAD") == checkpoint.commit_id
    verification = store.verify("forked", checkpoint=checkpoint)
    assert verification.valid is True
    assert verification.dirty is False
    assert verification.diverged is False
    assert verification.current_tree == checkpoint.tree_id


def test_git_adapter_reattaches_durable_checkpoint_after_process_restart(
    repository: Path,
    tmp_path: Path,
) -> None:
    managed = tmp_path / "managed"
    first_process = GitWorktreeWorkspace(repository, managed)
    handle = first_process.create("resumable")
    (handle.path / "README.md").write_text("durable edit\n", encoding="utf-8")
    checkpoint = first_process.checkpoint("resumable")
    primary_before = primary_state(repository)

    second_process = GitWorktreeWorkspace(repository, managed)
    attached = second_process.attach(
        "resumable",
        baseline_revision=checkpoint.baseline_revision,
    )
    verification = second_process.verify("resumable", checkpoint=checkpoint)

    assert attached.path == handle.path
    assert verification.valid is True
    assert verification.current_tree == checkpoint.tree_id
    assert (attached.path / "README.md").read_text(encoding="utf-8") == "durable edit\n"
    assert primary_state(repository) == primary_before


def test_verify_distinguishes_dirty_content_from_checkpoint_divergence(
    repository: Path,
    tmp_path: Path,
) -> None:
    store = GitWorktreeWorkspace(repository, tmp_path / "managed")
    workspace = store.create("editing")
    (workspace.path / "draft.txt").write_text("one\n", encoding="utf-8")

    dirty = store.verify("editing")
    assert dirty.valid is True
    assert dirty.dirty is True
    assert dirty.diverged is False

    checkpoint = store.checkpoint("editing")
    matching = store.verify("editing", checkpoint=checkpoint)
    assert matching.valid is True
    assert matching.dirty is True
    assert matching.diverged is False

    (workspace.path / "draft.txt").write_text("one\ntwo\n", encoding="utf-8")
    diverged = store.verify("editing", checkpoint=checkpoint)
    assert diverged.valid is False
    assert diverged.dirty is True
    assert diverged.diverged is True
    assert "content_changed_since_checkpoint" in diverged.reasons

    git(workspace.path, "add", "draft.txt")
    git(workspace.path, "commit", "--quiet", "-m", "detached advance")
    moved = store.verify("editing")
    assert moved.valid is False
    assert moved.dirty is False
    assert moved.diverged is True
    assert "head_moved_from_baseline" in moved.reasons


def test_ignored_files_are_not_added_to_checkpoint(
    repository: Path,
    tmp_path: Path,
) -> None:
    store = GitWorktreeWorkspace(repository, tmp_path / "managed")
    workspace = store.create("ignored")
    (workspace.path / "ignored.txt").write_text("not captured\n", encoding="utf-8")
    ignored_directory = workspace.path / "ignored-dir"
    ignored_directory.mkdir()
    (ignored_directory / "large.bin").write_bytes(b"x" * (6 * 1024 * 1024))

    checkpoint = store.checkpoint("ignored")

    assert checkpoint.diff.is_empty
    tree_paths = git(repository, "ls-tree", "-r", "--name-only", checkpoint.tree_id).splitlines()
    assert "ignored.txt" not in tree_paths
    assert "ignored-dir/large.bin" not in tree_paths


def test_sensitive_large_and_escaping_symlink_content_is_rejected(
    repository: Path,
    tmp_path: Path,
) -> None:
    store = GitWorktreeWorkspace(
        repository,
        tmp_path / "managed",
        max_file_bytes=64,
    )
    workspace = store.create("unsafe")

    secret = workspace.path / ".env"
    secret.write_text("TOKEN=sensitive\n", encoding="utf-8")
    with pytest.raises(WorkspaceSafetyError, match="sensitive path"):
        store.checkpoint("unsafe")
    secret.unlink()

    large = workspace.path / "large.txt"
    large.write_bytes(b"x" * 65)
    with pytest.raises(WorkspaceSafetyError, match="exceeds 64 bytes"):
        store.checkpoint("unsafe")
    large.unlink()

    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    link = workspace.path / "escape-link"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {type(exc).__name__}")
    with pytest.raises(WorkspaceSafetyError, match="symbolic link escapes"):
        store.checkpoint("unsafe")


def test_allowed_roots_and_cleanup_never_remove_unmanaged_paths(
    repository: Path,
    tmp_path: Path,
) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside-managed"
    with pytest.raises(WorkspaceSafetyError, match="outside the configured allowed roots"):
        GitWorktreeWorkspace(repository, outside, allowed_roots=(allowed,))

    managed = allowed / "worktrees"
    store = GitWorktreeWorkspace(repository, managed, allowed_roots=(allowed,))
    workspace = store.create("owned")
    unmanaged = managed / "not-a-session"
    unmanaged.mkdir()
    sentinel = unmanaged / "keep.txt"
    sentinel.write_text("keep\n", encoding="utf-8")

    assert store.cleanup("missing") is False
    assert sentinel.exists()
    assert store.cleanup("owned") is True
    assert not workspace.path.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep\n"


def test_in_memory_and_fake_adapters_satisfy_the_same_interface() -> None:
    for store in (
        InMemoryWorkspaceCheckpointStore({"README.md": "base\n"}),
        FakeWorkspaceCheckpointStore({"README.md": "base\n"}),
    ):
        assert isinstance(store, WorkspaceCheckpointStore)
        store.create("memory")
        store.write_file("memory", "README.md", "changed\n")
        checkpoint = store.checkpoint("memory")
        forked = store.fork(checkpoint, "forked")

        assert checkpoint.diff.files_changed == 1
        assert forked.baseline_revision == checkpoint.commit_id
        assert store.verify("forked", checkpoint=checkpoint).valid is True
        assert store.cleanup("memory") is True
        assert store.cleanup("forked") is True


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symbolic links are unavailable")
def test_baseline_with_escaping_symlink_is_rejected_without_touching_primary(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "primary"
    repository.mkdir()
    git(repository, "init", "--quiet")
    git(repository, "config", "user.name", "Workspace Tests")
    git(repository, "config", "user.email", "workspace@example.test")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    try:
        (repository / "escape").symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {type(exc).__name__}")
    git(repository, "add", "escape")
    git(repository, "commit", "--quiet", "-m", "symlink baseline")
    before = (
        git(repository, "rev-parse", "HEAD"),
        git(repository, "status", "--porcelain=v1", "--untracked-files=all"),
        os.readlink(repository / "escape"),
    )
    store = GitWorktreeWorkspace(repository, tmp_path / "managed")

    with pytest.raises(WorkspaceSafetyError, match="symbolic link escapes"):
        store.create("unsafe-baseline")

    assert (
        git(repository, "rev-parse", "HEAD"),
        git(repository, "status", "--porcelain=v1", "--untracked-files=all"),
        os.readlink(repository / "escape"),
    ) == before


def test_seed_paths_and_command_restore_ignored_environment(
    repository: Path, tmp_path: Path
) -> None:
    (repository / "ignored.txt").write_text("generated 1.2.3\n", encoding="utf-8")
    (repository / "ignored-dir").mkdir()
    (repository / "ignored-dir" / "data.bin").write_bytes(b"\x00\x01")
    store = GitWorktreeWorkspace(
        repository,
        tmp_path / "managed",
        seed_paths=("ignored.txt", "ignored-dir"),
        seed_command="printf seeded > seed-marker.txt",
    )

    handle = store.create("seeded")
    assert (handle.path / "ignored.txt").read_text(encoding="utf-8") == "generated 1.2.3\n"
    assert (handle.path / "ignored-dir" / "data.bin").read_bytes() == b"\x00\x01"
    assert (handle.path / "seed-marker.txt").read_text(encoding="utf-8") == "seeded"

    # Seeded ignored content is environment, not a change: it never reaches a
    # checkpoint. The seed command's untracked output does, like any edit.
    checkpoint = store.checkpoint("seeded")
    assert "ignored.txt" not in checkpoint.diff.paths
    assert "seed-marker.txt" in checkpoint.diff.paths

    forked = store.fork(checkpoint, "seeded-fork")
    assert (forked.path / "ignored.txt").exists()
    assert (forked.path / "ignored-dir" / "data.bin").exists()


def test_seed_refuses_tracked_missing_sensitive_or_failing_inputs(
    repository: Path, tmp_path: Path
) -> None:
    with pytest.raises(WorkspaceSafetyError, match="unsafe workspace-relative"):
        GitWorktreeWorkspace(repository, tmp_path / "m0", seed_paths=("../outside",))
    with pytest.raises(ValueError):
        GitWorktreeWorkspace(repository, tmp_path / "m0", seed_command="   ")

    # A tracked file is already in the worktree; copying over it is refused.
    tracked = GitWorktreeWorkspace(repository, tmp_path / "m1", seed_paths=("README.md",))
    with pytest.raises(WorkspaceSafetyError, match="not ignored"):
        tracked.create("s1")
    assert not (tmp_path / "m1" / "s1").exists()

    missing = GitWorktreeWorkspace(repository, tmp_path / "m2", seed_paths=("ignored.txt",))
    with pytest.raises(WorkspaceSafetyError, match="missing"):
        missing.create("s2")

    (repository / ".env").write_text("SECRET=1\n", encoding="utf-8")
    (repository / ".gitignore").write_text("ignored.txt\nignored-dir/\n.env\n", encoding="utf-8")
    git(repository, "commit", "--quiet", "-am", "ignore env")
    secret = GitWorktreeWorkspace(repository, tmp_path / "m3", seed_paths=(".env",))
    with pytest.raises(WorkspaceSafetyError, match="sensitive"):
        secret.create("s3")

    failing = GitWorktreeWorkspace(repository, tmp_path / "m4", seed_command="exit 3")
    with pytest.raises(Exception, match="exit code 3"):
        failing.create("s4")
    # A failed seed discards the half-built worktree entirely.
    assert not (tmp_path / "m4" / "s4").exists()
    assert "s4" not in git(repository, "worktree", "list")
