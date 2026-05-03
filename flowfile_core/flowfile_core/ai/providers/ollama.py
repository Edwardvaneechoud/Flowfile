"""Ollama adapter — local / fully-offline path (plan §11.3 demo).

Owned by W11. No BYOK; talks to the user's local Ollama server (default
``http://localhost:11434``). Tool-use works on Llama 3.1+; older models fall
back to JSON-mode + Pydantic-repair (W31's responsibility).
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class OllamaProvider(LiteLLMProvider):
    name: ClassVar[str] = "ollama"
    default_model: ClassVar[str] = "llama3.1:8b"
    model_prefix: ClassVar[str] = "ollama_chat/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    default_api_base: ClassVar[str | None] = "http://localhost:11434"
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "llama3.1:8b",
        "ghost_node": "llama3.1:8b",
        "explain": "llama3.1:8b",
        "agent": "llama3.1:70b",
        "agent_complex": "llama3.1:70b",
        "docgen": "llama3.1:8b",
        "settings_autocomplete": "llama3.1:8b",
        "lineage": "llama3.1:8b",
    }
