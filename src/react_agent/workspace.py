"""Safe, isolated workspace checkpoints backed by Git worktrees.

The module keeps Git plumbing, path validation, snapshot refs, and cleanup behind
one small interface.  Callers never need to mutate the user's primary worktree.
"""

from __future__ import annotations

import difflib
import fnmatch
import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

_SESSION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DEFAULT_MAX_FILE_BYTES = 5 * 1024 * 1024
_DEFAULT_SENSITIVE_PATTERNS = (
    ".env",
    ".env.*",
    "*.key",
    "*.pem",
    "*.p12",
    "*.pfx",
    "id_rsa",
    "id_rsa.*",
    "id_ed25519",
    "id_ed25519.*",
    "credentials.json",
    "secrets.json",
    "secret.*",
    ".netrc",
    ".npmrc",
    ".pypirc",
    ".ssh",
    ".ssh/*",
    ".aws/credentials",
    ".config/gcloud/*",
)
_SAFE_TEMPLATE_SUFFIXES = (".example", ".sample", ".template")
_UNTRUSTED_GIT_ENV = (
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_DIR",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_WORK_TREE",
)


class WorkspaceError(RuntimeError):
    """Base error raised by the workspace module."""


class WorkspaceSafetyError(WorkspaceError):
    """A requested operation failed a filesystem or repository safety invariant."""


class WorkspaceConflictError(WorkspaceError):
    """A session, path, or checkpoint conflicts with managed state."""


class WorkspaceNotFoundError(WorkspaceError):
    """A requested managed workspace or checkpoint does not exist."""


@dataclass(frozen=True, slots=True)
class WorkspaceHandle:
    """A session-owned isolated workspace."""

    session_id: str
    path: Path
    baseline_revision: str


@dataclass(frozen=True, slots=True)
class DiffSummary:
    """A compact, content-free summary between two workspace trees."""

    files_changed: int
    additions: int
    deletions: int
    binary_files: int
    paths: tuple[str, ...]

    @property
    def is_empty(self) -> bool:
        return self.files_changed == 0


@dataclass(frozen=True, slots=True)
class WorkspaceCheckpoint:
    """An immutable Git tree protected by an internal ref."""

    checkpoint_id: str
    session_id: str
    baseline_revision: str
    tree_id: str
    commit_id: str
    internal_ref: str
    created_at: float
    diff: DiffSummary


@dataclass(frozen=True, slots=True)
class WorkspaceVerification:
    """Verification of current content against its baseline and an optional checkpoint."""

    valid: bool
    dirty: bool
    diverged: bool
    baseline_revision: str
    head_revision: str
    current_tree: str
    expected_tree: str | None
    diff: DiffSummary
    reasons: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return self.valid


@runtime_checkable
class WorkspaceCheckpointStore(Protocol):
    """Interface for isolated workspaces and immutable checkpoints.

    Implementations must never mutate a caller's primary workspace.  A session ID
    owns exactly one workspace until ``cleanup`` succeeds.
    """

    def create(self, session_id: str, *, baseline_revision: str = "HEAD") -> WorkspaceHandle:
        """Create a session-specific workspace at an immutable baseline revision."""

    def attach(self, session_id: str, *, baseline_revision: str) -> WorkspaceHandle:
        """Reattach a verified workspace after a Runtime process restart."""

    def checkpoint(self, session_id: str) -> WorkspaceCheckpoint:
        """Capture allowed workspace content without changing its index or HEAD."""

    def diff_summary(
        self,
        session_id: str,
        *,
        against: WorkspaceCheckpoint | str | None = None,
    ) -> DiffSummary:
        """Summarize current content against a checkpoint, revision, or baseline."""

    def verify(
        self,
        session_id: str,
        *,
        checkpoint: WorkspaceCheckpoint | None = None,
    ) -> WorkspaceVerification:
        """Check repository ownership, dirty state, and checkpoint divergence."""

    def restore(
        self, session_id: str, *, checkpoint: WorkspaceCheckpoint
    ) -> WorkspaceVerification:
        """Restore only the isolated workspace to a verified checkpoint tree."""

    def fork(self, checkpoint: WorkspaceCheckpoint, new_session_id: str) -> WorkspaceHandle:
        """Create a clean isolated workspace from an immutable checkpoint."""

    def cleanup(self, session_id: str) -> bool:
        """Remove only a workspace previously created by this adapter."""


def _validate_session_id(session_id: str) -> None:
    if not _SESSION_ID.fullmatch(session_id) or session_id in {".", ".."}:
        raise WorkspaceSafetyError(
            "session_id must use 1-128 letters, digits, '.', '_' or '-' and may not be a path"
        )


def _is_within(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts or path == PurePosixPath("."):
        raise WorkspaceSafetyError(f"unsafe workspace-relative path: {value!r}")
    return path.as_posix()


def _is_sensitive(path: str, patterns: Sequence[str]) -> bool:
    lowered = path.casefold()
    name = PurePosixPath(lowered).name
    if any(name.endswith(suffix) for suffix in _SAFE_TEMPLATE_SUFFIXES):
        return False
    return any(
        fnmatch.fnmatchcase(lowered, pattern.casefold())
        or fnmatch.fnmatchcase(name, pattern.casefold())
        for pattern in patterns
    )


def _decode_path(value: bytes) -> str:
    return os.fsdecode(value)


def _git_error(stderr: bytes) -> str:
    message = stderr.decode("utf-8", errors="replace").strip().replace("\n", " ")
    return message[:500] or "Git command failed"


def _run_git(
    cwd: Path,
    *arguments: str,
    input_data: bytes | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> bytes:
    environment = os.environ.copy()
    for name in _UNTRUSTED_GIT_ENV:
        environment.pop(name, None)
    if extra_env:
        environment.update(extra_env)
    try:
        completed: subprocess.CompletedProcess[bytes] = subprocess.run(
            [
                "git",
                "-c",
                f"core.hooksPath={os.devnull}",
                "-c",
                "core.fsmonitor=false",
                "-C",
                str(cwd),
                *arguments,
            ],
            input=input_data,
            capture_output=True,
            check=False,
            env=environment,
        )
    except OSError as exc:
        raise WorkspaceError(f"Git could not be executed: {type(exc).__name__}") from exc
    if completed.returncode != 0:
        raise WorkspaceError(_git_error(completed.stderr))
    return completed.stdout


def _git_text(
    cwd: Path,
    *arguments: str,
    input_data: bytes | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> str:
    return _run_git(
        cwd,
        *arguments,
        input_data=input_data,
        extra_env=extra_env,
    ).decode("utf-8", errors="strict").strip()


def _summarize_mapping_diff(
    old: Mapping[str, bytes],
    new: Mapping[str, bytes],
) -> DiffSummary:
    paths = tuple(
        sorted(path for path in old.keys() | new.keys() if old.get(path) != new.get(path))
    )
    additions = 0
    deletions = 0
    binary_files = 0
    for path in paths:
        before = old.get(path, b"")
        after = new.get(path, b"")
        if b"\0" in before or b"\0" in after:
            binary_files += 1
            continue
        before_lines = before.decode("utf-8", errors="replace").splitlines()
        after_lines = after.decode("utf-8", errors="replace").splitlines()
        for line in difflib.ndiff(before_lines, after_lines):
            if line.startswith("+ "):
                additions += 1
            elif line.startswith("- "):
                deletions += 1
    return DiffSummary(
        files_changed=len(paths),
        additions=additions,
        deletions=deletions,
        binary_files=binary_files,
        paths=paths,
    )


class GitWorktreeWorkspace:
    """Git adapter that owns detached worktrees beneath one managed directory."""

    def __init__(
        self,
        repository: Path,
        managed_root: Path,
        *,
        allowed_roots: Sequence[Path] | None = None,
        max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
        sensitive_patterns: Sequence[str] | None = None,
        seed_paths: Sequence[str] = (),
        seed_command: str | None = None,
        seed_timeout_s: float = 300.0,
    ) -> None:
        """``seed_paths`` / ``seed_command`` restore environment a worktree lacks.

        ``git worktree add`` materializes tracked files only. Generated,
        gitignored inputs the task needs (a ``_version.py`` from
        setuptools_scm, a vendored data file) can be listed in ``seed_paths``
        as repository-relative paths and are copied from the primary
        repository into every new worktree. ``seed_command`` runs once in the
        new worktree afterwards (e.g. ``pip install -e .``). Seeded content is
        ignored by Git, so it never enters a checkpoint or a patch; a seed
        that produces sensitive or escaping files fails creation closed.
        """

        if max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be positive")
        if seed_timeout_s <= 0:
            raise ValueError("seed_timeout_s must be positive")
        if seed_command is not None and not seed_command.strip():
            raise ValueError("seed_command must not be blank")

        try:
            repository_candidate = repository.expanduser().resolve(strict=True)
        except OSError as exc:
            raise WorkspaceSafetyError("repository path does not exist or is inaccessible") from exc
        try:
            top_level = Path(
                _git_text(repository_candidate, "rev-parse", "--show-toplevel")
            ).resolve(strict=True)
        except WorkspaceError as exc:
            raise WorkspaceSafetyError("repository must be a non-bare Git worktree") from exc

        managed_candidate = managed_root.expanduser().absolute()
        if managed_candidate.exists() and managed_candidate.is_symlink():
            raise WorkspaceSafetyError("managed_root may not be a symbolic link")
        managed_resolved = managed_candidate.resolve(strict=False)
        roots = tuple(
            root.expanduser().resolve(strict=False)
            for root in (allowed_roots if allowed_roots is not None else (managed_resolved,))
        )
        if not roots or not any(_is_within(managed_resolved, root) for root in roots):
            raise WorkspaceSafetyError("managed_root is outside the configured allowed roots")
        if _is_within(managed_resolved, top_level) or _is_within(top_level, managed_resolved):
            raise WorkspaceSafetyError("managed_root and the primary repository must be disjoint")

        try:
            managed_resolved.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise WorkspaceSafetyError("managed_root could not be created safely") from exc
        if managed_resolved.is_symlink() or not managed_resolved.is_dir():
            raise WorkspaceSafetyError("managed_root must be a real directory")

        common_value = _git_text(top_level, "rev-parse", "--git-common-dir")
        common_path = Path(common_value)
        if not common_path.is_absolute():
            common_path = top_level / common_path

        self._repository = top_level
        self._managed_root = managed_resolved
        self._allowed_roots = roots
        self._common_git_dir = common_path.resolve(strict=True)
        self._max_file_bytes = max_file_bytes
        self._seed_paths = tuple(_safe_relative_path(value) for value in seed_paths)
        self._seed_command = seed_command
        self._seed_timeout_s = seed_timeout_s
        self._sensitive_patterns = tuple(
            sensitive_patterns
            if sensitive_patterns is not None
            else _DEFAULT_SENSITIVE_PATTERNS
        )
        self._workspaces: dict[str, WorkspaceHandle] = {}
        self._checkpoints: dict[str, WorkspaceCheckpoint] = {}
        self._lock = threading.RLock()

    @property
    def repository(self) -> Path:
        return self._repository

    @property
    def managed_root(self) -> Path:
        return self._managed_root

    def create(self, session_id: str, *, baseline_revision: str = "HEAD") -> WorkspaceHandle:
        _validate_session_id(session_id)
        with self._lock:
            if session_id in self._workspaces:
                raise WorkspaceConflictError(f"workspace already exists for session {session_id!r}")
            baseline = self._resolve_commit(baseline_revision)
            target = self._managed_root / session_id
            self._assert_managed_target(target)
            if target.exists() or target.is_symlink():
                raise WorkspaceConflictError(f"managed workspace path already exists: {target}")

            try:
                _run_git(
                    self._repository,
                    "worktree",
                    "add",
                    "--detach",
                    str(target),
                    baseline,
                )
                handle = WorkspaceHandle(
                    session_id=session_id,
                    path=target,
                    baseline_revision=baseline,
                )
                self._seed_worktree(target)
                self._validate_worktree(handle)
            except Exception:
                self._discard_unregistered_worktree(target)
                raise
            self._workspaces[session_id] = handle
            return handle

    def attach(self, session_id: str, *, baseline_revision: str) -> WorkspaceHandle:
        _validate_session_id(session_id)
        with self._lock:
            existing = self._workspaces.get(session_id)
            resolved_baseline = self._resolve_commit(baseline_revision)
            if existing is not None:
                if existing.baseline_revision != resolved_baseline:
                    raise WorkspaceConflictError(
                        "attached workspace baseline differs from durable metadata"
                    )
                self._validate_worktree(existing)
                return existing
            target = self._managed_root / session_id
            self._assert_managed_target(target)
            handle = WorkspaceHandle(
                session_id=session_id,
                path=target,
                baseline_revision=resolved_baseline,
            )
            self._validate_worktree(handle)
            head_revision = self._resolve_commit("HEAD", cwd=target)
            if head_revision != resolved_baseline:
                raise WorkspaceConflictError(
                    "managed worktree HEAD differs from durable baseline"
                )
            self._workspaces[session_id] = handle
            return handle

    def checkpoint(self, session_id: str) -> WorkspaceCheckpoint:
        with self._lock:
            handle = self._get_workspace(session_id)
            tree_id = self._snapshot_tree(handle)
            baseline_tree = self._resolve_tree(handle.baseline_revision)
            summary = self._diff_trees(baseline_tree, tree_id)
            checkpoint_id = uuid.uuid4().hex
            session_key = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:24]
            internal_ref = f"refs/react-agent/checkpoints/{session_key}/{checkpoint_id}"
            identity = {
                "GIT_AUTHOR_NAME": "ReAct Agent Workspace",
                "GIT_AUTHOR_EMAIL": "workspace@localhost",
                "GIT_COMMITTER_NAME": "ReAct Agent Workspace",
                "GIT_COMMITTER_EMAIL": "workspace@localhost",
            }
            commit_id = _git_text(
                self._repository,
                "commit-tree",
                tree_id,
                "-p",
                handle.baseline_revision,
                input_data=f"workspace checkpoint {checkpoint_id}\n".encode(),
                extra_env=identity,
            )
            _run_git(
                self._repository,
                "update-ref",
                internal_ref,
                commit_id,
            )
            checkpoint = WorkspaceCheckpoint(
                checkpoint_id=checkpoint_id,
                session_id=session_id,
                baseline_revision=handle.baseline_revision,
                tree_id=tree_id,
                commit_id=commit_id,
                internal_ref=internal_ref,
                created_at=time.time(),
                diff=summary,
            )
            self._checkpoints[checkpoint_id] = checkpoint
            return checkpoint

    def diff_summary(
        self,
        session_id: str,
        *,
        against: WorkspaceCheckpoint | str | None = None,
    ) -> DiffSummary:
        with self._lock:
            handle = self._get_workspace(session_id)
            current_tree = self._snapshot_tree(handle)
            if isinstance(against, WorkspaceCheckpoint):
                self._require_checkpoint(against)
                old_tree = against.tree_id
            elif isinstance(against, str):
                old_tree = self._resolve_tree(against)
            else:
                old_tree = self._resolve_tree(handle.baseline_revision)
            return self._diff_trees(old_tree, current_tree)

    def verify(
        self,
        session_id: str,
        *,
        checkpoint: WorkspaceCheckpoint | None = None,
    ) -> WorkspaceVerification:
        with self._lock:
            handle = self._get_workspace(session_id)
            self._validate_worktree(handle)
            head_revision = self._resolve_commit("HEAD", cwd=handle.path)
            head_tree = self._resolve_tree(head_revision)
            current_tree = self._snapshot_tree(handle)
            expected_tree: str | None = None
            reasons: list[str] = []

            if head_revision != handle.baseline_revision:
                reasons.append("head_moved_from_baseline")
            if checkpoint is not None:
                self._require_checkpoint(checkpoint)
                expected_tree = checkpoint.tree_id
                if not self._checkpoint_ref_is_intact(checkpoint):
                    reasons.append("checkpoint_ref_mismatch")
                if current_tree != checkpoint.tree_id:
                    reasons.append("content_changed_since_checkpoint")

            dirty = current_tree != head_tree
            diverged = bool(reasons)
            baseline_tree = self._resolve_tree(handle.baseline_revision)
            return WorkspaceVerification(
                valid=not diverged,
                dirty=dirty,
                diverged=diverged,
                baseline_revision=handle.baseline_revision,
                head_revision=head_revision,
                current_tree=current_tree,
                expected_tree=expected_tree,
                diff=self._diff_trees(baseline_tree, current_tree),
                reasons=tuple(reasons),
            )

    def restore(
        self, session_id: str, *, checkpoint: WorkspaceCheckpoint
    ) -> WorkspaceVerification:
        with self._lock:
            handle = self._get_workspace(session_id)
            self._require_checkpoint(checkpoint)
            if checkpoint.session_id != session_id:
                raise WorkspaceConflictError("checkpoint belongs to another session")
            if not self._checkpoint_ref_is_intact(checkpoint):
                raise WorkspaceSafetyError(
                    "checkpoint internal ref no longer matches its immutable tree"
                )
            # Update only the isolated worktree/index. HEAD remains at the
            # Session baseline, so the primary repository and its index are
            # never reset or overwritten.
            _run_git(
                handle.path,
                "read-tree",
                "--reset",
                "-u",
                checkpoint.tree_id,
            )
            _run_git(handle.path, "clean", "-f", "-d", "--")
            verification = self.verify(session_id, checkpoint=checkpoint)
            if not verification.valid:
                raise WorkspaceConflictError(
                    "workspace did not match the checkpoint after restore"
                )
            return verification

    def fork(self, checkpoint: WorkspaceCheckpoint, new_session_id: str) -> WorkspaceHandle:
        _validate_session_id(new_session_id)
        with self._lock:
            self._require_checkpoint(checkpoint)
            if not self._checkpoint_ref_is_intact(checkpoint):
                raise WorkspaceSafetyError("checkpoint internal ref no longer matches its tree")
            if new_session_id in self._workspaces:
                raise WorkspaceConflictError(
                    f"workspace already exists for session {new_session_id!r}"
                )
            target = self._managed_root / new_session_id
            self._assert_managed_target(target)
            if target.exists() or target.is_symlink():
                raise WorkspaceConflictError(f"managed workspace path already exists: {target}")
            try:
                _run_git(
                    self._repository,
                    "worktree",
                    "add",
                    "--detach",
                    str(target),
                    checkpoint.internal_ref,
                )
                handle = WorkspaceHandle(
                    session_id=new_session_id,
                    path=target,
                    baseline_revision=checkpoint.commit_id,
                )
                self._seed_worktree(target)
                self._validate_worktree(handle)
            except Exception:
                self._discard_unregistered_worktree(target)
                raise
            self._workspaces[new_session_id] = handle
            return handle

    def cleanup(self, session_id: str) -> bool:
        _validate_session_id(session_id)
        with self._lock:
            handle = self._workspaces.get(session_id)
            if handle is None:
                return False
            self._assert_managed_target(handle.path)
            if handle.path.is_symlink():
                raise WorkspaceSafetyError("refusing to clean a workspace replaced by a symlink")
            if handle.path.exists():
                try:
                    _run_git(
                        self._repository,
                        "worktree",
                        "remove",
                        "--force",
                        str(handle.path),
                    )
                except WorkspaceError:
                    self._assert_managed_target(handle.path)
                    shutil.rmtree(handle.path)
            _run_git(self._repository, "worktree", "prune")
            self._workspaces.pop(session_id, None)
            return True

    def _resolve_commit(self, revision: str, *, cwd: Path | None = None) -> str:
        if not revision or "\0" in revision:
            raise WorkspaceSafetyError("baseline revision must not be empty")
        try:
            return _git_text(
                cwd or self._repository,
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{revision}^{{commit}}",
            )
        except WorkspaceError as exc:
            raise WorkspaceNotFoundError(f"unknown commit revision: {revision!r}") from exc

    def _resolve_tree(self, revision: str) -> str:
        try:
            return _git_text(
                self._repository,
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{revision}^{{tree}}",
            )
        except WorkspaceError as exc:
            raise WorkspaceNotFoundError(f"unknown tree or revision: {revision!r}") from exc

    def _get_workspace(self, session_id: str) -> WorkspaceHandle:
        _validate_session_id(session_id)
        try:
            return self._workspaces[session_id]
        except KeyError as exc:
            raise WorkspaceNotFoundError(f"workspace not found for session {session_id!r}") from exc

    def _require_checkpoint(self, checkpoint: WorkspaceCheckpoint) -> None:
        known = self._checkpoints.get(checkpoint.checkpoint_id)
        if known is not None:
            if (
                known.session_id == checkpoint.session_id
                and known.baseline_revision == checkpoint.baseline_revision
                and known.tree_id == checkpoint.tree_id
                and known.commit_id == checkpoint.commit_id
                and known.internal_ref == checkpoint.internal_ref
            ):
                # Database timestamp precision and a recomputed public diff may
                # differ after restart; immutable Git identities are the
                # authority for checkpoint ownership.
                return
            raise WorkspaceNotFoundError("checkpoint is not managed by this adapter")
        session_key = hashlib.sha256(checkpoint.session_id.encode("utf-8")).hexdigest()[:24]
        expected_prefix = f"refs/react-agent/checkpoints/{session_key}/"
        if (
            not checkpoint.internal_ref.startswith(expected_prefix)
            or checkpoint.internal_ref != expected_prefix + checkpoint.checkpoint_id
            or not self._checkpoint_ref_is_intact(checkpoint)
        ):
            raise WorkspaceNotFoundError("checkpoint is not managed by this adapter")
        # A fresh process may reconstruct this immutable descriptor from the
        # durable journal. The ref/tree verification above is the authority.
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def _assert_managed_target(self, target: Path) -> None:
        lexical = target.absolute()
        if lexical.parent != self._managed_root:
            raise WorkspaceSafetyError("workspace path is not a direct child of managed_root")
        try:
            resolved = lexical.resolve(strict=False)
        except (OSError, RuntimeError) as exc:
            raise WorkspaceSafetyError("workspace path cannot be resolved safely") from exc
        if not _is_within(resolved, self._managed_root):
            raise WorkspaceSafetyError("workspace path escapes managed_root")
        if not any(_is_within(resolved, root) for root in self._allowed_roots):
            raise WorkspaceSafetyError("workspace path escapes configured allowed roots")

    def _validate_worktree(self, handle: WorkspaceHandle) -> None:
        self._assert_managed_target(handle.path)
        if not handle.path.exists() or not handle.path.is_dir() or handle.path.is_symlink():
            raise WorkspaceSafetyError("managed worktree is missing or is not a real directory")
        common_value = _git_text(handle.path, "rev-parse", "--git-common-dir")
        common_path = Path(common_value)
        if not common_path.is_absolute():
            common_path = handle.path / common_path
        if common_path.resolve(strict=True) != self._common_git_dir:
            raise WorkspaceSafetyError("worktree belongs to a different Git repository")
        self._validate_contents(handle.path)

    def _seed_worktree(self, target: Path) -> None:
        """Copy declared ignored inputs, then run the seed command, in a new worktree."""

        for relative in self._seed_paths:
            source = self._repository / relative
            if source.is_symlink() or not source.exists():
                raise WorkspaceSafetyError(f"seed path is missing or a symlink: {relative}")
            if _is_sensitive(relative, self._sensitive_patterns):
                raise WorkspaceSafetyError(f"seed path matches a sensitive pattern: {relative}")
            # A directory pattern such as "build/" only matches with the slash.
            if not self._is_ignored(target, relative + "/" if source.is_dir() else relative):
                # Tracked content is already in the worktree; copying the
                # primary repository's copy over it would smuggle in edits.
                raise WorkspaceSafetyError(f"seed path is not ignored by Git: {relative}")
            destination = target / relative
            if destination.exists() or destination.is_symlink():
                raise WorkspaceSafetyError(f"seed path already exists in the worktree: {relative}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                if source.is_dir():
                    shutil.copytree(source, destination, symlinks=True)
                else:
                    shutil.copy2(source, destination)
            except OSError as exc:
                raise WorkspaceError(
                    f"seed path could not be copied: {relative} ({type(exc).__name__})"
                ) from exc
        if self._seed_command is None:
            return
        environment = os.environ.copy()
        for name in _UNTRUSTED_GIT_ENV:
            environment.pop(name, None)
        try:
            completed = subprocess.run(
                self._seed_command,
                shell=True,
                cwd=str(target),
                env=environment,
                capture_output=True,
                check=False,
                timeout=self._seed_timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            raise WorkspaceError("seed command timed out") from exc
        except OSError as exc:
            raise WorkspaceError(f"seed command could not run: {type(exc).__name__}") from exc
        if completed.returncode != 0:
            raise WorkspaceError(f"seed command failed with exit code {completed.returncode}")

    @staticmethod
    def _is_ignored(worktree: Path, relative: str) -> bool:
        try:
            _run_git(worktree, "check-ignore", "-q", "--", relative)
        except WorkspaceError:
            return False
        return True

    def _validate_contents(self, root: Path) -> None:
        root_resolved = root.resolve(strict=True)
        ignored_raw = _run_git(
            root,
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        )
        ignored = {
            _decode_path(item)
            for item in ignored_raw.split(b"\0")
            if item
        }

        for directory, directory_names, file_names in os.walk(root, followlinks=False):
            directory_path = Path(directory)
            if directory_path == root:
                directory_names[:] = [name for name in directory_names if name != ".git"]
                file_names = [name for name in file_names if name != ".git"]
            for name in [*directory_names, *file_names]:
                candidate = directory_path / name
                relative = candidate.relative_to(root).as_posix()
                if _is_sensitive(relative, self._sensitive_patterns):
                    raise WorkspaceSafetyError(
                        f"sensitive path is not allowed in a checkpoint workspace: {relative}"
                    )
                if candidate.is_symlink():
                    try:
                        resolved_target = candidate.resolve(strict=False)
                    except (OSError, RuntimeError) as exc:
                        raise WorkspaceSafetyError(
                            f"symbolic link cannot be resolved safely: {relative}"
                        ) from exc
                    if not _is_within(resolved_target, root_resolved):
                        raise WorkspaceSafetyError(
                            f"symbolic link escapes the managed workspace: {relative}"
                        )
                    continue
                if candidate.is_file() and relative not in ignored:
                    try:
                        size = candidate.stat().st_size
                    except OSError as exc:
                        raise WorkspaceSafetyError(
                            f"workspace file could not be inspected: {relative}"
                        ) from exc
                    if size > self._max_file_bytes:
                        raise WorkspaceSafetyError(
                            f"workspace file exceeds {self._max_file_bytes} bytes: {relative}"
                        )

    def _snapshot_tree(self, handle: WorkspaceHandle) -> str:
        self._validate_worktree(handle)
        temporary_directory = Path(
            tempfile.mkdtemp(prefix=".checkpoint-index-", dir=self._managed_root)
        )
        index_path = temporary_directory / "index"
        environment = {"GIT_INDEX_FILE": str(index_path)}
        try:
            _run_git(
                handle.path,
                "read-tree",
                handle.baseline_revision,
                extra_env=environment,
            )
            _run_git(
                handle.path,
                "add",
                "-A",
                "--",
                ".",
                extra_env=environment,
            )
            return _git_text(handle.path, "write-tree", extra_env=environment)
        finally:
            shutil.rmtree(temporary_directory, ignore_errors=True)

    def _diff_trees(self, old_tree: str, new_tree: str) -> DiffSummary:
        paths_raw = _run_git(
            self._repository,
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--name-only",
            "-z",
            old_tree,
            new_tree,
            "--",
        )
        paths = tuple(
            sorted(_decode_path(item) for item in paths_raw.split(b"\0") if item)
        )
        numstat_raw = _run_git(
            self._repository,
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--numstat",
            "-z",
            old_tree,
            new_tree,
            "--",
        )
        additions = 0
        deletions = 0
        binary_files = 0
        for record in numstat_raw.split(b"\0"):
            if not record:
                continue
            fields = record.split(b"\t", 2)
            if len(fields) != 3:
                raise WorkspaceError("Git returned an invalid numstat record")
            if fields[0] == b"-" or fields[1] == b"-":
                binary_files += 1
            else:
                additions += int(fields[0])
                deletions += int(fields[1])
        return DiffSummary(
            files_changed=len(paths),
            additions=additions,
            deletions=deletions,
            binary_files=binary_files,
            paths=paths,
        )

    def _checkpoint_ref_is_intact(self, checkpoint: WorkspaceCheckpoint) -> bool:
        try:
            commit_id = self._resolve_commit(checkpoint.internal_ref)
            tree_id = self._resolve_tree(checkpoint.internal_ref)
        except WorkspaceError:
            return False
        return commit_id == checkpoint.commit_id and tree_id == checkpoint.tree_id

    def _discard_unregistered_worktree(self, target: Path) -> None:
        try:
            self._assert_managed_target(target)
        except WorkspaceSafetyError:
            return
        if target.is_symlink():
            return
        try:
            _run_git(
                self._repository,
                "worktree",
                "remove",
                "--force",
                str(target),
            )
        except WorkspaceError:
            if target.exists() and target.is_dir():
                shutil.rmtree(target)
        try:
            _run_git(self._repository, "worktree", "prune")
        except WorkspaceError:
            pass


@dataclass(slots=True)
class _MemoryWorkspace:
    handle: WorkspaceHandle
    files: dict[str, bytes]
    head_revision: str


class InMemoryWorkspaceCheckpointStore:
    """Deterministic in-memory adapter for callers and interface-level tests."""

    def __init__(
        self,
        baseline_files: Mapping[str, bytes | str] | None = None,
        *,
        max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
        sensitive_patterns: Sequence[str] | None = None,
    ) -> None:
        if max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be positive")
        normalized = {
            _safe_relative_path(path): value.encode() if isinstance(value, str) else bytes(value)
            for path, value in (baseline_files or {}).items()
        }
        self._max_file_bytes = max_file_bytes
        self._sensitive_patterns = tuple(
            sensitive_patterns
            if sensitive_patterns is not None
            else _DEFAULT_SENSITIVE_PATTERNS
        )
        self._validate_files(normalized)
        root_tree = self._tree_id(normalized)
        self._root_revision = f"memory-{root_tree}"
        self._revisions: dict[str, dict[str, bytes]] = {
            self._root_revision: dict(normalized)
        }
        self._workspaces: dict[str, _MemoryWorkspace] = {}
        self._checkpoints: dict[str, WorkspaceCheckpoint] = {}
        self._lock = threading.RLock()

    def create(self, session_id: str, *, baseline_revision: str = "HEAD") -> WorkspaceHandle:
        _validate_session_id(session_id)
        with self._lock:
            if session_id in self._workspaces:
                raise WorkspaceConflictError(f"workspace already exists for session {session_id!r}")
            revision = self._root_revision if baseline_revision == "HEAD" else baseline_revision
            try:
                files = dict(self._revisions[revision])
            except KeyError as exc:
                raise WorkspaceNotFoundError(f"unknown memory revision: {revision!r}") from exc
            handle = WorkspaceHandle(
                session_id=session_id,
                path=Path("/in-memory-workspaces") / session_id,
                baseline_revision=revision,
            )
            self._workspaces[session_id] = _MemoryWorkspace(handle, files, revision)
            return handle

    def attach(self, session_id: str, *, baseline_revision: str) -> WorkspaceHandle:
        _validate_session_id(session_id)
        with self._lock:
            workspace = self._get_workspace(session_id)
            if workspace.handle.baseline_revision != baseline_revision:
                raise WorkspaceConflictError(
                    "attached workspace baseline differs from durable metadata"
                )
            return workspace.handle

    def checkpoint(self, session_id: str) -> WorkspaceCheckpoint:
        with self._lock:
            workspace = self._get_workspace(session_id)
            self._validate_files(workspace.files)
            baseline = self._revisions[workspace.handle.baseline_revision]
            tree_id = self._tree_id(workspace.files)
            checkpoint_id = uuid.uuid4().hex
            commit_id = hashlib.sha256(
                f"{workspace.handle.baseline_revision}:{tree_id}:{checkpoint_id}".encode()
            ).hexdigest()
            internal_ref = f"memory://checkpoints/{checkpoint_id}"
            checkpoint = WorkspaceCheckpoint(
                checkpoint_id=checkpoint_id,
                session_id=session_id,
                baseline_revision=workspace.handle.baseline_revision,
                tree_id=tree_id,
                commit_id=commit_id,
                internal_ref=internal_ref,
                created_at=time.time(),
                diff=_summarize_mapping_diff(baseline, workspace.files),
            )
            self._checkpoints[checkpoint_id] = checkpoint
            self._revisions[commit_id] = dict(workspace.files)
            return checkpoint

    def diff_summary(
        self,
        session_id: str,
        *,
        against: WorkspaceCheckpoint | str | None = None,
    ) -> DiffSummary:
        with self._lock:
            workspace = self._get_workspace(session_id)
            self._validate_files(workspace.files)
            if isinstance(against, WorkspaceCheckpoint):
                self._require_checkpoint(against)
                revision = against.commit_id
            elif isinstance(against, str):
                revision = against
            else:
                revision = workspace.handle.baseline_revision
            try:
                old = self._revisions[revision]
            except KeyError as exc:
                raise WorkspaceNotFoundError(f"unknown memory revision: {revision!r}") from exc
            return _summarize_mapping_diff(old, workspace.files)

    def verify(
        self,
        session_id: str,
        *,
        checkpoint: WorkspaceCheckpoint | None = None,
    ) -> WorkspaceVerification:
        with self._lock:
            workspace = self._get_workspace(session_id)
            self._validate_files(workspace.files)
            baseline = self._revisions[workspace.handle.baseline_revision]
            head_files = self._revisions[workspace.head_revision]
            current_tree = self._tree_id(workspace.files)
            expected_tree: str | None = None
            reasons: list[str] = []
            if workspace.head_revision != workspace.handle.baseline_revision:
                reasons.append("head_moved_from_baseline")
            if checkpoint is not None:
                self._require_checkpoint(checkpoint)
                expected_tree = checkpoint.tree_id
                if current_tree != checkpoint.tree_id:
                    reasons.append("content_changed_since_checkpoint")
            return WorkspaceVerification(
                valid=not reasons,
                dirty=workspace.files != head_files,
                diverged=bool(reasons),
                baseline_revision=workspace.handle.baseline_revision,
                head_revision=workspace.head_revision,
                current_tree=current_tree,
                expected_tree=expected_tree,
                diff=_summarize_mapping_diff(baseline, workspace.files),
                reasons=tuple(reasons),
            )

    def restore(
        self, session_id: str, *, checkpoint: WorkspaceCheckpoint
    ) -> WorkspaceVerification:
        with self._lock:
            workspace = self._get_workspace(session_id)
            self._require_checkpoint(checkpoint)
            if checkpoint.session_id != session_id:
                raise WorkspaceConflictError("checkpoint belongs to another session")
            try:
                workspace.files = dict(self._revisions[checkpoint.commit_id])
            except KeyError as exc:
                raise WorkspaceNotFoundError(
                    "checkpoint content is not retained by this adapter"
                ) from exc
            verification = self.verify(session_id, checkpoint=checkpoint)
            if not verification.valid:
                raise WorkspaceConflictError(
                    "workspace did not match the checkpoint after restore"
                )
            return verification

    def fork(self, checkpoint: WorkspaceCheckpoint, new_session_id: str) -> WorkspaceHandle:
        _validate_session_id(new_session_id)
        with self._lock:
            self._require_checkpoint(checkpoint)
            return self.create(new_session_id, baseline_revision=checkpoint.commit_id)

    def cleanup(self, session_id: str) -> bool:
        _validate_session_id(session_id)
        with self._lock:
            return self._workspaces.pop(session_id, None) is not None

    def write_file(self, session_id: str, path: str, content: bytes | str) -> None:
        """Adapter-specific helper for tests that need to model a workspace edit."""

        with self._lock:
            workspace = self._get_workspace(session_id)
            normalized = _safe_relative_path(path)
            value = content.encode() if isinstance(content, str) else bytes(content)
            candidate = dict(workspace.files)
            candidate[normalized] = value
            self._validate_files(candidate)
            workspace.files = candidate

    def remove_file(self, session_id: str, path: str) -> None:
        """Adapter-specific helper for tests that need to model a deletion."""

        with self._lock:
            workspace = self._get_workspace(session_id)
            workspace.files.pop(_safe_relative_path(path), None)

    def read_file(self, session_id: str, path: str) -> bytes:
        """Adapter-specific helper for deterministic Runtime/tool tests."""

        with self._lock:
            workspace = self._get_workspace(session_id)
            normalized = _safe_relative_path(path)
            try:
                return bytes(workspace.files[normalized])
            except KeyError as exc:
                raise WorkspaceNotFoundError(
                    f"workspace file not found: {normalized!r}"
                ) from exc

    def _get_workspace(self, session_id: str) -> _MemoryWorkspace:
        _validate_session_id(session_id)
        try:
            return self._workspaces[session_id]
        except KeyError as exc:
            raise WorkspaceNotFoundError(f"workspace not found for session {session_id!r}") from exc

    def _require_checkpoint(self, checkpoint: WorkspaceCheckpoint) -> None:
        known = self._checkpoints.get(checkpoint.checkpoint_id)
        if known is None or not (
            known.session_id == checkpoint.session_id
            and known.baseline_revision == checkpoint.baseline_revision
            and known.tree_id == checkpoint.tree_id
            and known.commit_id == checkpoint.commit_id
            and known.internal_ref == checkpoint.internal_ref
        ):
            raise WorkspaceNotFoundError("checkpoint is not managed by this adapter")

    def _validate_files(self, files: Mapping[str, bytes]) -> None:
        for path, content in files.items():
            normalized = _safe_relative_path(path)
            if _is_sensitive(normalized, self._sensitive_patterns):
                raise WorkspaceSafetyError(
                    f"sensitive path is not allowed in a checkpoint workspace: {normalized}"
                )
            if len(content) > self._max_file_bytes:
                raise WorkspaceSafetyError(
                    f"workspace file exceeds {self._max_file_bytes} bytes: {normalized}"
                )

    @staticmethod
    def _tree_id(files: Mapping[str, bytes]) -> str:
        digest = hashlib.sha256()
        for path in sorted(files):
            encoded_path = path.encode("utf-8")
            content = files[path]
            digest.update(len(encoded_path).to_bytes(8, "big"))
            digest.update(encoded_path)
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
        return digest.hexdigest()


class FakeWorkspaceCheckpointStore(InMemoryWorkspaceCheckpointStore):
    """Explicit fake adapter name for dependency injection in higher-level tests."""
