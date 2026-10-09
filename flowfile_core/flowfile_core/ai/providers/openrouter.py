"""OpenRouter adapter — many hosted models behind one BYOK key.

Capability flags reflect a *typical* OpenRouter-served model
(per-model — tool-use varies). Surface presets pick broadly capable
defaults; users can override per surface via the model field. The
Claude surfaces mirror the direct Anthropic adapter (Sonnet 5.5 for the
thinking paths, Haiku 4.5 for the quick ones, Opus 4.7 for
``agent_complex`` until the Opus 5.5 tool-turn check passes); the
open-weights default is Qwen3.6 35B-A3B.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class OpenRouterProvider(LiteLLMProvider):
    name: ClassVar[str] = "openrouter"
    default_model: ClassVar[str] = "qwen/qwen3.6-35b-a3b"
    model_prefix: ClassVar[str] = "openrouter/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "anthropic/claude-haiku-4.5",
        "ghost_node": "anthropic/claude-haiku-4.5",
        "explain": "anthropic/claude-sonnet-5.5",
        "agent_complex": "anthropic/claude-opus-4.7",
        # ``agent_staged`` is the surface built specifically to make
        # smaller open-weights models viable: one tool per LLM round,
        # so the function-calling compliance failures seen on the
        # full-catalog surfaces go away. Qwen3.6 35B-A3B is a cheap,
        # tool-capable open-weights default (the previous
        # ``llama-3.3-70b-instruct`` was billed too — OpenRouter's
        # ``:free`` variant of it no longer exists); users can override
        # via ``model=``.
        "agent_staged": "qwen/qwen3.6-35b-a3b",
        "docgen": "anthropic/claude-sonnet-5.5",
        "settings_autocomplete": "anthropic/claude-haiku-4.5",
        "lineage": "anthropic/claude-sonnet-5.5",
        "intent_classifier": "anthropic/claude-haiku-4.5",
        "cron": "anthropic/claude-haiku-4.5",
    }
