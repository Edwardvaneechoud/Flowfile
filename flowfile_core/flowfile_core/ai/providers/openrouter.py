"""OpenRouter adapter — many free models behind one BYOK key.

Owned by W11. Capability flags reflect a *typical* OpenRouter-served model
(plan §8.4 calls these "per-model" — tool-use varies). Surface presets pick
broadly capable defaults; users can override per surface via the model field.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class OpenRouterProvider(LiteLLMProvider):
    name: ClassVar[str] = "openrouter"
    default_model: ClassVar[str] = "anthropic/claude-sonnet-4.5"
    model_prefix: ClassVar[str] = "openrouter/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "anthropic/claude-haiku-4.5",
        "ghost_node": "anthropic/claude-haiku-4.5",
        "explain": "anthropic/claude-sonnet-4.5",
        "agent": "anthropic/claude-sonnet-4.5",
        "agent_complex": "anthropic/claude-opus-4.5",
        "docgen": "anthropic/claude-sonnet-4.5",
        "settings_autocomplete": "anthropic/claude-haiku-4.5",
        "lineage": "anthropic/claude-sonnet-4.5",
    }
