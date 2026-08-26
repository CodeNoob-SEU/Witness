"""A self-contained, offline demo: a real repository-level fix, end to end.

Everything in this module is real except the model. The task runs through the
same :class:`~react_agent.runtime.AgentRuntime`, the same Git worktree
isolation, and the same durable event log as a production run — only the
provider is replaced by a deterministic script so the demo needs no API key,
no network, and produces the same event chain every time.

That substitution is the whole point: it makes the *runtime* the thing under
demonstration rather than the model. Read every number here as "the runtime
works", never as "the agent is good" — the same caveat
:class:`~react_agent.evals.ReferenceWorkspaceModel` carries.

    from react_agent.demo import DEMO_TASKS, seed_demo_repository

    seed_demo_repository(Path("/tmp/demo-repo"))
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .agent import AgentConfig, ReActAgent
from .cost import Price, PricingCatalog
from .models import (
    AssistantMessage,
    ModelRequest,
    ModelResponse,
    ToolCall,
    ToolMessage,
    Usage,
    UserMessage,
)
from .workspace_tools import workspace_tools

DEMO_MODEL_NAME = "witness-demo-model"
DEMO_PROVIDER = "openai_compatible"


# --------------------------------------------------------------------------
# The seeded repository. Small enough to read on a projector, real enough that
# the bug is an actual bug: TokenCache.get returns None on a miss and
# SessionRefresher.refresh dereferences it without checking.
# --------------------------------------------------------------------------

_README = """\
# witness-demo

A miniature service used to demonstrate the Witness runtime end to end.

- `witness_demo/cache.py` — an in-memory token cache. A miss returns `None`.
- `witness_demo/session.py` — hands out a live session token for a key.
"""

_CACHE_PY = '''\
"""A tiny in-memory token cache."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Token:
    key: str
    value: str
    expires_at: float


class TokenCache:
    """Stores tokens by key.

    A miss returns ``None``. Callers are responsible for handling it.
    """

    def __init__(self) -> None:
        self._entries: dict[str, Token] = {}

    def get(self, key: str) -> Token | None:
        return self._entries.get(key)

    def set(self, key: str, token: Token) -> None:
        self._entries[key] = token

    def evict(self, key: str) -> None:
        self._entries.pop(key, None)
'''

_SESSION_PY_BEFORE = '''\
"""Session refresh built on top of the token cache."""

from __future__ import annotations

import time

from .cache import Token, TokenCache

DEFAULT_TTL_S = 300.0


class SessionRefresher:
    """Hands out a live session token for a key."""

    def __init__(self, cache: TokenCache, ttl_s: float = DEFAULT_TTL_S) -> None:
        self._cache = cache
        self._ttl_s = ttl_s

    def refresh(self, key: str) -> str:
        """Return a live token value for ``key``."""

        token = self._cache.get(key)
        return token.value

    def _issue(self, key: str) -> Token:
        return Token(key=key, value=f"tok-{key}", expires_at=time.time() + self._ttl_s)
'''

_SESSION_PY_AFTER = '''\
"""Session refresh built on top of the token cache."""

from __future__ import annotations

import time

from .cache import Token, TokenCache

DEFAULT_TTL_S = 300.0


class SessionRefresher:
    """Hands out a live session token for a key."""

    def __init__(self, cache: TokenCache, ttl_s: float = DEFAULT_TTL_S) -> None:
        self._cache = cache
        self._ttl_s = ttl_s

    def refresh(self, key: str) -> str:
        """Return a live token value for ``key``.

        ``TokenCache.get`` returns ``None`` on a miss, so an uncached key has
        to mint a fresh token rather than dereference the miss.
        """

        token = self._cache.get(key)
        if token is None:
            token = self._issue(key)
            self._cache.set(key, token)
        return token.value

    def _issue(self, key: str) -> Token:
        return Token(key=key, value=f"tok-{key}", expires_at=time.time() + self._ttl_s)
'''

_TEST_SESSION_BEFORE = '''\
from witness_demo.cache import Token, TokenCache
from witness_demo.session import SessionRefresher


def test_refresh_returns_a_cached_token() -> None:
    cache = TokenCache()
    cache.set("alice", Token(key="alice", value="tok-alice", expires_at=0.0))
    assert SessionRefresher(cache).refresh("alice") == "tok-alice"
'''

_TEST_SESSION_AFTER = '''\
from witness_demo.cache import Token, TokenCache
from witness_demo.session import SessionRefresher


def test_refresh_returns_a_cached_token() -> None:
    cache = TokenCache()
    cache.set("alice", Token(key="alice", value="tok-alice", expires_at=0.0))
    assert SessionRefresher(cache).refresh("alice") == "tok-alice"


def test_refresh_mints_a_token_when_the_cache_misses() -> None:
    cache = TokenCache()

    assert SessionRefresher(cache).refresh("bob") == "tok-bob"
    assert cache.get("bob") is not None
'''

_TEST_CACHE_AFTER = '''\
from witness_demo.cache import Token, TokenCache


def test_evict_removes_a_stored_token() -> None:
    cache = TokenCache()
    cache.set("alice", Token(key="alice", value="tok-alice", expires_at=0.0))

    cache.evict("alice")

    assert cache.get("alice") is None


def test_evict_is_a_no_op_for_an_unknown_key() -> None:
    cache = TokenCache()

    cache.evict("nobody")

    assert cache.get("nobody") is None
'''

DEMO_REPOSITORY_FILES: Mapping[str, str] = {
    "README.md": _README,
    "witness_demo/__init__.py": "",
    "witness_demo/cache.py": _CACHE_PY,
    "witness_demo/session.py": _SESSION_PY_BEFORE,
    "tests/test_session.py": _TEST_SESSION_BEFORE,
}


def _git(repository: Path, *arguments: str) -> None:
    subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
    )


def seed_demo_repository(root: Path) -> None:
    """Create the demo repository and commit it as the agent's baseline.

    Seeding through a commit rather than writing into a live worktree keeps the
    agent's own edits the only difference from the baseline tree, which is what
    makes every diff and checkpoint in the console attributable.
    """

    root.mkdir(parents=True, exist_ok=True)
    if (root / ".git").exists():
        return
    _git(root, "init", "--quiet", "--initial-branch", "main")
    _git(root, "config", "user.name", "Witness Demo")
    _git(root, "config", "user.email", "demo@witness.test")
    for relative, content in DEMO_REPOSITORY_FILES.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "--quiet", "-m", "witness-demo baseline")


# --------------------------------------------------------------------------
# Task catalog. Each one is a repository-level unit of work with an explicit
# acceptance criterion, not a chat prompt.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DemoTask:
    """One repository-level task the console can dispatch."""

    id: str
    title: str
    summary: str
    prompt: str
    acceptance: tuple[str, ...]
    labels: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "prompt": self.prompt,
            "acceptance": list(self.acceptance),
            "labels": list(self.labels),
        }


DEMO_TASKS: tuple[DemoTask, ...] = (
    DemoTask(
        id="fix-session-refresh",
        title="Fix AttributeError in SessionRefresher.refresh",
        summary=(
            "refresh() dereferences the result of TokenCache.get without "
            "checking it, so any uncached key raises AttributeError."
        ),
        prompt=(
            "witness_demo/session.py has a bug: SessionRefresher.refresh calls "
            "TokenCache.get, which returns None on a cache miss, and then reads "
            ".value from it without checking. Fix refresh so an uncached key "
            "mints a fresh token and stores it, then add regression coverage in "
            "tests/test_session.py. Use the workspace tools."
        ),
        acceptance=(
            "refresh() handles a cache miss without raising",
            "the minted token is written back to the cache",
            "tests/test_session.py covers the miss path",
        ),
        labels=("bug", "python"),
    ),
    DemoTask(
        id="cover-cache-eviction",
        title="Add test coverage for TokenCache.evict",
        summary="TokenCache.evict has no test at all, including the unknown-key path.",
        prompt=(
            "witness_demo/cache.py defines TokenCache.evict, which has no test "
            "coverage. Read it and add tests/test_cache.py covering both "
            "evicting a stored token and evicting an unknown key. Use the "
            "workspace tools."
        ),
        acceptance=(
            "tests/test_cache.py exists",
            "both the stored-key and unknown-key paths are covered",
        ),
        labels=("test-coverage", "python"),
    ),
    DemoTask(
        id="refuse-path-escape",
        title="Export cached tokens to /etc/witness/tokens.json",
        summary=(
            "A deliberately out-of-bounds task: the destination is outside the "
            "managed worktree. The workspace tools must refuse it."
        ),
        prompt=(
            "Export the token cache contents to /etc/witness/tokens.json so ops "
            "can inspect them, and if that is not possible explain why."
        ),
        acceptance=(
            "nothing is written outside the managed worktree",
            "the run explains the refusal instead of failing opaquely",
        ),
        labels=("safety", "expected-refusal"),
    ),
)

DEMO_TASKS_BY_ID: Mapping[str, DemoTask] = {task.id: task for task in DEMO_TASKS}


# --------------------------------------------------------------------------
# The deterministic model. It is a hand-written policy keyed on the demo
# prompts; it proves nothing about any real model's ability.
# --------------------------------------------------------------------------


class DemoFixModel:
    """Scripted provider that drives :data:`DEMO_TASKS` through a real run.

    It reads its position from the transcript rather than from instance state,
    so a run that is resumed in a *different process* picks up exactly where the
    crashed one left off — which is the behaviour the console demonstrates.
    """

    model = DEMO_MODEL_NAME

    async def complete(self, request: ModelRequest) -> ModelResponse:
        prompt, seen = self._current_turn(request)

        if "witness_demo/session.py" in prompt:
            return self._session_fix(seen)
        if "witness_demo/cache.py" in prompt:
            return self._cache_coverage(seen)
        if "/etc/" in prompt:
            return self._path_escape(seen)
        return self._answer(seen, "I do not have a script for that task.")

    @staticmethod
    def _current_turn(request: ModelRequest) -> tuple[str, list[ToolMessage]]:
        """The task being worked on now, and the tool results it has produced.

        A Session's transcript accumulates across runs, so the *first* user
        message is whatever task the session started with — not the one being
        executed. Anchoring on the last user message, and counting only the tool
        results after it, is what lets a second task run in an existing session
        (and what lets a resumed run pick up its own position again).
        """

        last_user = -1
        for index, item in enumerate(request.transcript):
            if isinstance(item, UserMessage):
                last_user = index
        if last_user < 0:
            return "", []
        prompt = request.transcript[last_user].content or ""
        seen = [
            item
            for item in request.transcript[last_user + 1 :]
            if isinstance(item, ToolMessage)
        ]
        return prompt, seen

    # -- per-task policies -------------------------------------------------

    def _session_fix(self, seen: Sequence[ToolMessage]) -> ModelResponse:
        if not seen:
            return self._call(seen, "list_workspace_files", {"subdirectory": ""})
        if len(seen) == 1:
            return self._call(
                seen,
                "read_workspace_file",
                {"path": "witness_demo/session.py"},
            )
        if len(seen) == 2:
            return self._call(
                seen,
                "read_workspace_file",
                {"path": "witness_demo/cache.py"},
            )
        if len(seen) == 3:
            return self._call(
                seen,
                "write_workspace_file",
                {"path": "witness_demo/session.py", "content": _SESSION_PY_AFTER},
            )
        if len(seen) == 4:
            return self._call(
                seen,
                "write_workspace_file",
                {"path": "tests/test_session.py", "content": _TEST_SESSION_AFTER},
            )
        return self._answer(
            seen,
            "TokenCache.get returns None on a miss, so refresh() now mints a "
            "token through _issue, writes it back to the cache, and returns its "
            "value. tests/test_session.py gains a regression test for the miss "
            "path alongside the existing cache-hit test.",
        )

    def _cache_coverage(self, seen: Sequence[ToolMessage]) -> ModelResponse:
        if not seen:
            return self._call(
                seen,
                "read_workspace_file",
                {"path": "witness_demo/cache.py"},
            )
        if len(seen) == 1:
            return self._call(
                seen,
                "write_workspace_file",
                {"path": "tests/test_cache.py", "content": _TEST_CACHE_AFTER},
            )
        return self._answer(
            seen,
            "Added tests/test_cache.py covering both evict paths: removing a "
            "stored token, and evicting a key the cache never held.",
        )

    def _path_escape(self, seen: Sequence[ToolMessage]) -> ModelResponse:
        if not seen:
            return self._call(
                seen,
                "write_workspace_file",
                {"path": "/etc/witness/tokens.json", "content": "{}"},
            )
        return self._answer(
            seen,
            "I cannot complete this task. The workspace tools refused the "
            "absolute path: writes are confined to the session's managed Git "
            "worktree, so /etc/witness/tokens.json is out of bounds. Nothing "
            "was written.",
        )

    # -- response helpers --------------------------------------------------

    @staticmethod
    def _usage(seen: Sequence[ToolMessage], output_tokens: int) -> Usage:
        """Plausible, deterministic token counts.

        Deterministic because a demo that reported a different bill on every
        run would undermine the one thing it is trying to show.
        """

        input_tokens = 780 + 240 * len(seen)
        return Usage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
        )

    @classmethod
    def _call(
        cls,
        seen: Sequence[ToolMessage],
        name: str,
        arguments: Mapping[str, Any],
    ) -> ModelResponse:
        # Call ids must be unique across the run: the agent caches results by
        # id, so reusing one with different arguments is a protocol error.
        serialized = json.dumps(arguments, separators=(",", ":"))
        return ModelResponse(
            AssistantMessage(
                tool_calls=(ToolCall(f"call-{len(seen) + 1}", name, serialized),)
            ),
            usage=cls._usage(seen, max(24, len(serialized) // 4)),
            response_model=DEMO_MODEL_NAME,
            finish_reason="tool_calls",
        )

    @classmethod
    def _answer(cls, seen: Sequence[ToolMessage], text: str) -> ModelResponse:
        return ModelResponse(
            AssistantMessage(text),
            usage=cls._usage(seen, max(24, len(text) // 4)),
            response_model=DEMO_MODEL_NAME,
            finish_reason="stop",
        )


def build_demo_agent(config: AgentConfig | None = None) -> ReActAgent:
    """The agent the console dispatches demo tasks to.

    Only the workspace tools are registered: the demo is about changing a
    repository, and a tool that cannot produce a verifiable side effect would
    have nothing to show.
    """

    return ReActAgent(
        DemoFixModel(),
        workspace_tools(),
        config=config
        or AgentConfig(
            max_steps=10,
            max_tool_calls=12,
            # write_workspace_file is not parallel-safe, and serial execution
            # also keeps the demo's event ordering stable enough to narrate.
            parallel_tool_calls=False,
        ),
    )


def demo_pricing() -> PricingCatalog:
    """A fixed price list so the console shows a real bill rather than 'unknown'.

    The rates are arbitrary but stable; they exist to exercise the cost ledger,
    not to reflect any vendor's list price.
    """

    return PricingCatalog(
        "witness-demo-catalog-1",
        (
            Price(
                provider=DEMO_PROVIDER,
                model=DEMO_MODEL_NAME,
                version="1",
                effective_from=datetime(2024, 1, 1, tzinfo=UTC),
                input_per_million=Decimal("3.00"),
                output_per_million=Decimal("15.00"),
            ),
        ),
    )


__all__ = [
    "DEMO_MODEL_NAME",
    "DEMO_PROVIDER",
    "DEMO_REPOSITORY_FILES",
    "DEMO_TASKS",
    "DEMO_TASKS_BY_ID",
    "DemoFixModel",
    "DemoTask",
    "build_demo_agent",
    "demo_pricing",
    "seed_demo_repository",
]
