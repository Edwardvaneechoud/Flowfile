"""Anthropic adapter (Claude Opus 4.7, Sonnet 4.6, Haiku 4.5).

Owned by W11. D010 maps each surface to a default model from this provider:

* ``cmd_k`` / ``ghost_node`` → Haiku 4.5 (sub-1s TTFB target).
* ``explain`` / ``agent`` / ``docgen`` → Sonnet 4.6.
* ``agent_complex`` → Opus 4.7.

Tool use and streaming-with-tools are both first-class on Anthropic.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class AnthropicProvider(LiteLLMProvider):
    name: ClassVar[str] = "anthropic"
    default_model: ClassVar[str] = "claude-sonnet-4-6"
    model_prefix: ClassVar[str] = "anthropic/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "claude-haiku-4-5",
        "ghost_node": "claude-haiku-4-5",
        "explain": "claude-sonnet-4-6",
        "agent": "claude-sonnet-4-6",
        "agent_complex": "claude-opus-4-7",
        "docgen": "claude-sonnet-4-6",
        "settings_autocomplete": "claude-haiku-4-5",
        "lineage": "claude-sonnet-4-6",
    }
