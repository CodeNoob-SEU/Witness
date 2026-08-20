from __future__ import annotations

import asyncio

import pytest

from react_agent.agent import ReActAgent
from react_agent.journal import InMemoryRunJournal
from react_agent.models import AssistantMessage, ModelRequest, ModelResponse
from react_agent.runtime import (
    AgentRuntime,
    InMemoryRuntimeStore,
    ResumeRejected,
    ResumeRun,
    StartRun,
)


class BlockingModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def complete(self, request: ModelRequest) -> ModelResponse:
        del request
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class NoCallModel:
    async def complete(self, request: ModelRequest) -> ModelResponse:
        del request
        pytest.fail("revision mismatch must be rejected before a provider call")
        return ModelResponse(AssistantMessage("unreachable"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first_provider", "first_model", "next_provider", "next_model"),
    (
        ("provider-a", "model-a", "provider-a", "model-b"),
        ("provider-a", "model-a", "provider-b", "model-a"),
    ),
)
async def test_resume_rejects_changed_provider_or_request_model_revision(
    first_provider: str,
    first_model: str,
    next_provider: str,
    next_model: str,
) -> None:
    journal = InMemoryRunJournal()
    store = InMemoryRuntimeStore()
    blocked = BlockingModel()
    first_runtime = AgentRuntime(
        ReActAgent(blocked),
        journal,
        store=store,
        provider_name=first_provider,
        model_name=first_model,
    )
    handle = await first_runtime.submit(
        StartRun(prompt="interrupt", session_id="revision", idempotency_key="request")
    )
    await asyncio.wait_for(blocked.started.wait(), timeout=1)
    await first_runtime.close()

    recovery_runtime = AgentRuntime(
        ReActAgent(NoCallModel()),
        journal,
        store=store,
        provider_name=next_provider,
        model_name=next_model,
    )
    with pytest.raises(ResumeRejected, match="revision changed"):
        await recovery_runtime.submit(ResumeRun(run_id=handle.run_id))
    await recovery_runtime.close()
