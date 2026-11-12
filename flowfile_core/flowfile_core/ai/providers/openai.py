"""OpenAI adapter — GPT-* with structured outputs and tool use.

``gpt-6-luna`` is the default; users override per
surface via ``provider_factory(..., model=...)``. Strict structured
outputs are litellm-handled; we surface them as plain
``ChatResponse`` content.
"""

from typing import Any, ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMKwargs, LiteLLMProvider
from flowfile_core.ai.providers.base import Message, ToolSpec


class OpenAIProvider(LiteLLMProvider):
    name: ClassVar[str] = "openai"
    default_model: ClassVar[str] = "gpt-6-luna"
    model_prefix: ClassVar[str] = ""
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "gpt-6-luna",
        "ghost_node": "gpt-6-luna",
        "explain": "gpt-6-luna",
        "agent_complex": "gpt-6-luna",
        "agent_staged": "gpt-6-luna",
        "docgen": "gpt-6-luna",
        "settings_autocomplete": "gpt-6-luna",
        "lineage": "gpt-6-luna",
        "intent_classifier": "gpt-6-luna",
        "cron": "gpt-6-luna",
    }

    def _build_kwargs(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None,
        max_tokens: int | None,
        *,
        stream: bool,
        response_format: dict[str, Any] | None = None,
    ) -> LiteLLMKwargs:
        kwargs = super()._build_kwargs(messages, tools, max_tokens, stream=stream, response_format=response_format)
        if self.model == "gpt-6-luna":
            # The bundled litellm model map predates this model.
            kwargs["custom_llm_provider"] = "openai"
            kwargs["allowed_openai_params"] = ["reasoning_effort"]
            # Chat Completions tool calls require reasoning to be disabled.
            kwargs["reasoning_effort"] = "none"
            if max_tokens is not None:
                kwargs["max_completion_tokens"] = kwargs.pop("max_tokens")
        return kwargs
