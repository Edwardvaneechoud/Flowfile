"""Fixtures for the semantic-model tests: a world of real Delta tables, a signed-in client and a recording provider.

Only the model provider is replaced (``RecordingProvider``); SQL runs through the catalog SQL path on the session's
worker, the ledger is the test database's own.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import semantic_routes
from flowfile_core.ai.providers.base import ChatResponse
from flowfile_core.auth.jwt import get_current_active_user, get_user_or_internal_service
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from tests.semantic.world import World, build_world, drop_world, reset_process_state


class RecordingProvider:
    """A provider that answers each call with ``answer(messages)`` and keeps every prompt it was sent."""

    name = "fake"
    model = "fake-model"

    def __init__(self, answer: Callable[[list[Any]], Any]) -> None:
        self.answer = answer
        self.calls: list[list[Any]] = []

    async def chat(self, messages, tools=None, max_tokens=None, response_format=None, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(list(messages))
        reply = self.answer(list(messages))
        return ChatResponse(model=self.model, content=reply if isinstance(reply, str) else json.dumps(reply))

    @property
    def sent(self) -> str:
        """Every message of every call but the fixed system prompts (their examples hold made-up values)."""
        return "\n".join(str(m.content) for call in self.calls for m in call if m.role != "system")


@pytest.fixture(autouse=True)
def _clean_process_state() -> Iterator[None]:
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(True)
    reset_process_state()
    yield
    reset_process_state()
    FEATURE_FLAG_AI.set(original)


@pytest.fixture
def world() -> Iterator[World]:
    w = build_world(owner_id=1)
    yield w
    drop_world(w)


@pytest.fixture
def client() -> Iterator[TestClient]:
    user = PydanticUser(id=1, username="local_user")
    for dependency in (get_current_active_user, get_user_or_internal_service):
        main.app.dependency_overrides[dependency] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        for dependency in (get_current_active_user, get_user_or_internal_service):
            main.app.dependency_overrides.pop(dependency, None)


@pytest.fixture
def use_provider(monkeypatch: pytest.MonkeyPatch) -> Callable[[RecordingProvider], RecordingProvider]:
    """Make the semantic AI routes answer with ``provider``, whatever the request names."""

    def _use(provider: RecordingProvider) -> RecordingProvider:
        monkeypatch.setattr(semantic_routes, "resolve_semantic_provider", lambda *_a, **_k: provider)
        return provider

    return _use
