"""OpenAI adapter — GPT-* with structured outputs and tool use.

Owned by W11. ``gpt-4.1-mini`` is the cost/quality default; users override per
surface via ``provider_factory(..., model=...)``. Strict structured outputs are
litellm-handled; we surface them as plain ``ChatResponse`` content.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class OpenAIProvider(LiteLLMProvider):
    name: ClassVar[str] = "openai"
    default_model: ClassVar[str] = "gpt-4.1-mini"
    model_prefix: ClassVar[str] = ""  # litellm routes GPT-* models without a prefix
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "gpt-4.1-mini",
        "ghost_node": "gpt-4.1-mini",
        "explain": "gpt-4.1",
        "agent": "gpt-4.1",
        "agent_complex": "gpt-4.1",
        # W71 — agent_staged stays on the mini tier: each LLM round sees
        # one tool with a tight enum, so the cheap model is sufficient
        # and the four-round cycle stays well within rate limits.
        "agent_staged": "gpt-4.1-mini",
        "docgen": "gpt-4.1",
        "settings_autocomplete": "gpt-4.1-mini",
        "lineage": "gpt-4.1",
        "intent_classifier": "gpt-4.1-mini",
    }
