"""Ollama adapter — local / fully-offline path.

No BYOK; talks to the user's local Ollama server (default
``http://localhost:11434``). Defaults to Qwen3.5 9B (Qwen3.6 35B-A3B
for ``agent_complex``), both tool-capable; older or non-instruct
models fall back to JSON-mode + Pydantic-repair handled at the
executor layer. Thinking is switched off on every request
(``extra_params`` → litellm ``reasoning_effort="none"`` → Ollama
``think: false``): the quick surfaces cap output at ~100 tokens, which
a reasoning pass would consume before the answer.
"""

from typing import Any, ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class OllamaProvider(LiteLLMProvider):
    name: ClassVar[str] = "ollama"
    default_model: ClassVar[str] = "qwen3.5:9b"
    model_prefix: ClassVar[str] = "ollama_chat/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    default_api_base: ClassVar[str | None] = "http://localhost:11434"
    extra_params: ClassVar[dict[str, Any]] = {"reasoning_effort": "none"}
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "qwen3.5:9b",
        "ghost_node": "qwen3.5:9b",
        "explain": "qwen3.5:9b",
        "agent_complex": "qwen3.6:35b-a3b",
        "docgen": "qwen3.5:9b",
        "settings_autocomplete": "qwen3.5:9b",
        "lineage": "qwen3.5:9b",
        "intent_classifier": "qwen3.5:9b",
        "cron": "qwen3.5:9b",
    }
