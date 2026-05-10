"""Groq adapter — fast streaming for low-TTFB surfaces.

~30 RPM free-tier limit makes Groq a good fit for ``cmd_k`` and
ghost-node surfaces, where one user keystroke = one call.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class GroqProvider(LiteLLMProvider):
    name: ClassVar[str] = "groq"
    default_model: ClassVar[str] = "qwen/qwen3-32b"
    model_prefix: ClassVar[str] = "groq/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "qwen/qwen3-32b",
        "ghost_node": "qwen/qwen3-coder-30b-a3b-instruct",
        "explain": "qwen/qwen3-coder-30b-a3b-instruct",
        "agent_complex": "qwen/qwen3-coder-30b-a3b-instruct",
        # Qwen3 Coder 30B is the canonical small-model target
        # for agent_staged. The function-calling failures seen on
        # full-catalog surfaces (text-JSON-in-content) go away when
        # the tools array has exactly one entry per round.
        "agent_staged": "qwen/qwen3-coder-30b-a3b-instruct",
        "docgen": "qwen/qwen3-coder-30b-a3b-instruct",
        "settings_autocomplete": "qwen/qwen3-coder-30b-a3b-instruct",
        "lineage": "qwen/qwen3-coder-30b-a3b-instruct",
        "intent_classifier": "qwen/qwen3-coder-30b-a3b-instruct",
    }
