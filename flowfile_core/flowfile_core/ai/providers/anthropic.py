"""Anthropic adapter (Claude Opus 4.7, Sonnet 4.6, Haiku 4.5).

Surface → default model mapping:

* ``cmd_k`` / ``ghost_node`` → Haiku 4.5 (sub-1s TTFB target).
* ``explain`` / ``docgen`` → Sonnet 4.6.
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
        "agent_complex": "claude-opus-4-7",
        # ``agent_staged`` exposes one tool per stage so each round is
        # a tightly-scoped decision. Haiku is plenty for stages 0/1/2
        # (enum picks); stage 3 (settings JSON) is the only stage
        # where Sonnet would help and Haiku 4.5 still gets the JSON
        # shape right when the per-type ``Example payload`` is in the
        # prompt. Cost dominates for the 4×-per-node-add fan-out —
        # defaulting to Haiku saves ~5× over Sonnet. Users who
        # explicitly want Sonnet can override via the model picker
        # (``model=`` on the request).
        "agent_staged": "claude-haiku-4-5",
        "docgen": "claude-sonnet-4-6",
        "settings_autocomplete": "claude-haiku-4-5",
        "lineage": "claude-sonnet-4-6",
        "intent_classifier": "claude-haiku-4-5",
        "cron": "claude-haiku-4-5",
    }
