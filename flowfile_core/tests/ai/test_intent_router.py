"""chat → agent auto-promotion classifier tests.

The classifier was simplified in round 2 to a single LLM call with
conversation history (no heuristic regexes). These tests cover the
remaining surface:

* ``test_classify_intent_calls_llm_with_user_message`` — the LLM sees the
  user's text and the system prompt; provider call uses
  ``response_format={"type": "json_object"}`` and ``tools=None``.
* ``test_classify_intent_history_is_forwarded_to_llm`` — recent chat
  turns are prepended between the system prompt and the user message
  (oldest first) so context-dependent messages like *"can you implement?"*
  classify against the prior assistant suggestion.
* ``test_classify_intent_history_is_bounded`` — only the last
  :data:`DEFAULT_HISTORY_TURNS` turns are passed; system messages are
  dropped; per-turn payload is clipped to
  :data:`DEFAULT_HISTORY_CHARS_PER_TURN`.
* ``test_classify_intent_promotes_build_with_history`` — a stubbed LLM
  call with realistic history returns ``kind="build"`` and the verdict
  layer flips to ``"agent"``.
* ``test_classify_intent_falls_back_on_timeout`` — slow provider exceeds
  the timeout → ``kind="chat"`` confidence 0.
* ``test_classify_intent_falls_back_on_provider_error`` — provider raises
  → ``kind="chat"`` confidence 0.
* ``test_classify_intent_falls_back_on_malformed_json`` — provider
  returns non-JSON → ``kind="chat"`` confidence 0.
* ``test_classify_intent_strips_markdown_fences`` — provider wraps JSON
  in ```` ```json … ``` ```` → still parses.
* ``test_classify_intent_handles_empty_message`` — whitespace-only input
  short-circuits to chat without calling the LLM.
* ``test_classify_intent_no_provider_falls_back`` — ``provider=None`` →
  chat fallback (no exception).
* ``test_verdict_for_promotes_only_high_confidence_build`` — verdict
  layer applies the promotion threshold.
* ``test_lazy_litellm_contract_for_intent_router`` — importing the module
  doesn't pull ``litellm`` into ``sys.modules``.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from typing import Any

import pytest

from flowfile_core.ai import intent_router
from flowfile_core.ai.intent_router import (
    DEFAULT_HISTORY_CHARS_PER_TURN,
    DEFAULT_HISTORY_TURNS,
    PROMOTION_CONFIDENCE_THRESHOLD,
    IntentClassification,
    classify_intent,
    verdict_for,
)
from flowfile_core.ai.providers.base import ChatResponse, Message, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler


# --------------------------------------------------------------------------- #
# Fakes #
# --------------------------------------------------------------------------- #


class _FakeProvider:
    """Provider stub for the LLM call. Records ``last_call_kwargs`` so tests
    can inspect ``messages`` (history forwarding) and ``response_format``."""

    name: str = "fake"
    model: str = "fake-haiku"
    supports_tools: bool = True
    supports_streaming: bool = True

    def __init__(
        self,
        *,
        content: str | None = None,
        sleep_before_response: float = 0.0,
        raise_exc: BaseException | None = None,
    ) -> None:
        self._content = content
        self._sleep = sleep_before_response
        self._raise = raise_exc
        self.last_call_kwargs: dict[str, Any] = {}

    async def chat(
        self,
        messages: list[Any],
        tools: list[Any] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> ChatResponse:
        self.last_call_kwargs = {
            "messages": messages,
            "tools": tools,
            "max_tokens": max_tokens,
            "response_format": response_format,
        }
        if self._sleep > 0:
            await asyncio.sleep(self._sleep)
        if self._raise is not None:
            raise self._raise
        return ChatResponse(
            model=self.model,
            content=self._content,
            tool_calls=[],
            finish_reason="stop",
            usage=Usage(),
        )

    def stream(self, *_a: Any, **_kw: Any):
        raise AssertionError("stream() should not be called by intent_router")


def _scheduler() -> RateLimitScheduler:
    """Fresh scheduler whose clock is monotonic-zero so the RPM window never
    blocks tests; ``sleep`` is a no-op for the same reason."""

    return RateLimitScheduler(
        time_source=lambda: 0.0,
        sleep=lambda *_a, **_k: asyncio.sleep(0),
    )


def _payload(kind: str, confidence: float, reason: str) -> str:
    return json.dumps({"kind": kind, "confidence": confidence, "reason": reason})


# --------------------------------------------------------------------------- #
# LLM dispatch + history #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_classify_intent_calls_llm_with_user_message() -> None:
    provider = _FakeProvider(content=_payload("chat", 0.9, "asks about lineage"))
    classification = await classify_intent(
        "what does this flow do",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "chat"
    assert classification.confidence == pytest.approx(0.9)

    # Wire shape: system + user; tools=None; JSON response_format.
    messages = provider.last_call_kwargs["messages"]
    assert provider.last_call_kwargs["tools"] is None
    assert provider.last_call_kwargs["response_format"] == {"type": "json_object"}
    assert messages[0].role == "system"
    assert "intent classifier" in messages[0].content.lower()
    assert messages[-1].role == "user"
    assert messages[-1].content == "what does this flow do"


@pytest.mark.asyncio
async def test_classify_intent_history_is_forwarded_to_llm() -> None:
    """Recent chat turns are inserted between the system prompt and the user
    message (oldest first) so the LLM can use prior context."""
    provider = _FakeProvider(content=_payload("build", 0.85, "executes prior suggestion"))
    history = [
        Message(role="user", content="how do I get the number of customers per city?"),
        Message(role="assistant", content="Here's how you'd do it: group_by('city') ..."),
    ]
    await classify_intent(
        "can you implement?",
        history=history,
        provider=provider,
        scheduler=_scheduler(),
    )

    sent = provider.last_call_kwargs["messages"]
    assert sent[0].role == "system"
    assert sent[1].role == "user"
    assert "customers per city" in sent[1].content
    assert sent[2].role == "assistant"
    assert sent[2].content.startswith("Here's how")
    assert sent[3].role == "user"
    assert sent[3].content == "can you implement?"


@pytest.mark.asyncio
async def test_classify_intent_history_is_bounded() -> None:
    """Only the last ``DEFAULT_HISTORY_TURNS`` user/assistant turns are
    forwarded; system messages are dropped; per-turn payload is clipped at
    ``DEFAULT_HISTORY_CHARS_PER_TURN``."""
    long_content = "x" * (DEFAULT_HISTORY_CHARS_PER_TURN + 500)
    history = [
        Message(role="system", content="stale system message"),
        # 6 user/assistant turns — only the last DEFAULT_HISTORY_TURNS (4) survive.
        Message(role="user", content="msg-1"),
        Message(role="assistant", content="resp-1"),
        Message(role="user", content="msg-2"),
        Message(role="assistant", content="resp-2"),
        Message(role="user", content="msg-3"),
        Message(role="assistant", content=long_content),  # gets clipped
    ]
    provider = _FakeProvider(content=_payload("chat", 0.7, "..."))

    await classify_intent(
        "follow-up",
        history=history,
        provider=provider,
        scheduler=_scheduler(),
    )

    sent = provider.last_call_kwargs["messages"]
    # system (classifier's own) + DEFAULT_HISTORY_TURNS history + 1 user = 6
    assert len(sent) == 1 + DEFAULT_HISTORY_TURNS + 1
    # First message after system is "msg-2" (oldest of the trailing 4); the
    # earlier "msg-1" / "resp-1" + the stale system entry were dropped.
    assert sent[1].content == "msg-2"
    assert sent[2].content == "resp-2"
    assert sent[3].content == "msg-3"
    # Long assistant content was clipped to the per-turn cap.
    assert len(sent[4].content) <= DEFAULT_HISTORY_CHARS_PER_TURN
    assert sent[4].content.endswith("...")
    assert sent[-1].content == "follow-up"


@pytest.mark.asyncio
async def test_classify_intent_promotes_build_with_history() -> None:
    """End-to-end: a context-dependent message gets ``kind=build`` from the
    LLM and the verdict layer flips it to agent."""
    provider = _FakeProvider(content=_payload("build", 0.85, "follow-up to a suggestion"))
    classification = await classify_intent(
        "can you implement?",
        history=[
            Message(role="user", content="how do I count customers per city?"),
            Message(role="assistant", content="Add a group_by node grouping on city ..."),
        ],
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "build"
    assert verdict_for(classification) == "agent"


# --------------------------------------------------------------------------- #
# Failure modes #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_classify_intent_falls_back_on_timeout() -> None:
    provider = _FakeProvider(
        content=_payload("build", 0.9, "ignored"),
        sleep_before_response=1.0,
    )
    classification = await classify_intent(
        "anything",
        provider=provider,
        scheduler=_scheduler(),
        timeout=0.05,
    )
    assert classification.kind == "chat"
    assert classification.confidence == 0.0
    assert "timed out" in classification.reason


@pytest.mark.asyncio
async def test_classify_intent_falls_back_on_provider_error() -> None:
    provider = _FakeProvider(raise_exc=RuntimeError("boom"))
    classification = await classify_intent(
        "anything",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "chat"
    assert classification.confidence == 0.0
    assert "failed" in classification.reason.lower()


@pytest.mark.asyncio
async def test_classify_intent_falls_back_on_malformed_json() -> None:
    provider = _FakeProvider(content="not json at all")
    classification = await classify_intent(
        "anything",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "chat"
    assert classification.confidence == 0.0
    assert "malformed" in classification.reason.lower()


@pytest.mark.asyncio
async def test_classify_intent_strips_markdown_fences() -> None:
    fenced = "```json\n" + _payload("chat", 0.7, "asks about lineage") + "\n```"
    provider = _FakeProvider(content=fenced)
    classification = await classify_intent(
        "what does this flow do",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "chat"
    assert classification.confidence == pytest.approx(0.7)


@pytest.mark.asyncio
async def test_classify_intent_handles_empty_message() -> None:
    """Whitespace-only input short-circuits to chat without calling the LLM."""
    provider = _FakeProvider(content=_payload("build", 0.99, "ignored"))
    classification = await classify_intent(
        "   \n\t",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert classification.kind == "chat"
    assert classification.confidence == 1.0
    assert provider.last_call_kwargs == {}, "provider.chat must not be called for empty input"


@pytest.mark.asyncio
async def test_classify_intent_no_provider_falls_back() -> None:
    classification = await classify_intent("anything", provider=None)
    assert classification.kind == "chat"
    assert classification.confidence == 0.0
    assert "no provider" in classification.reason.lower()


# --------------------------------------------------------------------------- #
# Verdict mapping #
# --------------------------------------------------------------------------- #


def test_verdict_for_promotes_only_high_confidence_build() -> None:
    above = IntentClassification(
        kind="build",
        confidence=PROMOTION_CONFIDENCE_THRESHOLD + 0.1,
        reason="…",
    )
    at = IntentClassification(
        kind="build",
        confidence=PROMOTION_CONFIDENCE_THRESHOLD,
        reason="…",
    )
    below = IntentClassification(
        kind="build",
        confidence=PROMOTION_CONFIDENCE_THRESHOLD - 0.1,
        reason="…",
    )
    chat = IntentClassification(kind="chat", confidence=0.99, reason="…")
    ambiguous = IntentClassification(kind="ambiguous", confidence=0.95, reason="…")

    assert verdict_for(above) == "agent"
    assert verdict_for(at) == "agent"
    assert verdict_for(below) == "chat"
    assert verdict_for(chat) == "chat"
    assert verdict_for(ambiguous) == "chat"


# --------------------------------------------------------------------------- #
# Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract_for_intent_router() -> None:
    """Importing ``flowfile_core.ai.intent_router`` must not import ``litellm``."""
    sys.modules.pop("litellm", None)
    importlib.reload(intent_router)
    assert "litellm" not in sys.modules


# --------------------------------------------------------------------------- #
# Surface key — round 4 #
# --------------------------------------------------------------------------- #


def test_intent_classifier_has_dedicated_surface_key() -> None:
    """The classifier owns its own surface key — not borrowed from's
    ``settings_autocomplete``. Audit-log filtering and per-surface model
    tuning treat the two as independent.
    """
    assert intent_router.SURFACE == "intent_classifier"


def test_intent_classifier_surface_in_lockstep_across_providers() -> None:
    """Every provider's ``surface_models`` carries an ``intent_classifier``
    entry pointing at a Haiku-class (or equivalent fast/cheap) model. Drift
    guard so adding a future provider without this entry fails the test
    suite at PR time."""
    from flowfile_core.ai.context import builder as ctx_builder
    from flowfile_core.ai.context import budget as ctx_budget
    from flowfile_core.ai.providers import (
        AnthropicProvider,
        GoogleProvider,
        GroqProvider,
        OllamaProvider,
        OpenAIProvider,
        OpenRouterProvider,
    )
    from flowfile_core.ai.tools import registry as tool_registry

    # 1. SurfaceLiteral coverage in both modules that declare it.
    assert "intent_classifier" in ctx_builder.SURFACE_TO_LEVEL
    assert "intent_classifier" in tool_registry.get_args(tool_registry.SurfaceLiteral)

    # 2. SURFACE_PRESETS — empty frozenset because the classifier never
    # invokes tools (single-shot strict-JSON judgement).
    assert tool_registry.SURFACE_PRESETS["intent_classifier"] == frozenset()

    # 3. _check_preset_coverage stays consistent.
    tool_registry._check_preset_coverage()

    # 4. Every provider class carries an entry.
    for provider_cls in (
        AnthropicProvider,
        OpenAIProvider,
        GoogleProvider,
        GroqProvider,
        OpenRouterProvider,
        OllamaProvider,
    ):
        assert "intent_classifier" in provider_cls.surface_models, (
            f"{provider_cls.__name__} missing intent_classifier in surface_models"
        )

    # 5. Budget table has an entry — strictly smaller than ``cmd_k`` because
    # the classifier prompt is tighter (system + ≤4 turns + current message).
    classifier_budget = ctx_budget.surface_budget("intent_classifier")
    cmd_k_budget = ctx_budget.surface_budget("cmd_k")
    assert classifier_budget[0] <= cmd_k_budget[0]
    assert classifier_budget[1] <= cmd_k_budget[1]
