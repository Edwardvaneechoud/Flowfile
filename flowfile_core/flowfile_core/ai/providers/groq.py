"""Groq adapter — fast streaming for low-TTFB surfaces.

Owned by W11. ~30 RPM free-tier limit (plan §8.2) makes Groq a good fit for
``cmd_k`` and ghost-node surfaces, where one user keystroke = one call.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class GroqProvider(LiteLLMProvider):
    name: ClassVar[str] = "groq"
    default_model: ClassVar[str] = "llama-3.3-70b-versatile"
    model_prefix: ClassVar[str] = "groq/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "llama-3.3-70b-versatile",
        "ghost_node": "llama-3.3-70b-versatile",
        "explain": "llama-3.3-70b-versatile",
        "agent_complex": "llama-3.3-70b-versatile",
        # W71 — Groq's llama-3.3-70b is the canonical small-model target
        # for agent_staged. The function-calling failures we saw on
        # agent / agent_complex (text-JSON-in-content) go away when the
        # tools array has exactly one entry per round.
        "agent_staged": "llama-3.3-70b-versatile",
        "docgen": "llama-3.3-70b-versatile",
        "settings_autocomplete": "llama-3.3-70b-versatile",
        "lineage": "llama-3.3-70b-versatile",
        "intent_classifier": "llama-3.3-70b-versatile",
    }
