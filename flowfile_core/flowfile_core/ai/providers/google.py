"""Google Gemini adapter — free-tier default per plan §8.2.

Owned by W11. Gemini 2.x Flash is the bundled first-run demo model: 250–1000
req/day, no card required, native tool-use and streaming. ``provider_factory``
is the entry point; W12 will plug the BYOK key.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class GoogleProvider(LiteLLMProvider):
    name: ClassVar[str] = "google"
    default_model: ClassVar[str] = "gemini-2.5-flash"
    model_prefix: ClassVar[str] = "gemini/"
    supports_tools: ClassVar[bool] = True
    supports_streaming: ClassVar[bool] = True
    surface_models: ClassVar[dict[str, str]] = {
        "cmd_k": "gemini-2.5-flash",
        "ghost_node": "gemini-2.5-flash",
        "explain": "gemini-2.5-flash",
        "agent_complex": "gemini-2.5-pro",
        "docgen": "gemini-2.5-flash",
        "settings_autocomplete": "gemini-2.5-flash",
        "lineage": "gemini-2.5-flash",
        "intent_classifier": "gemini-2.5-flash",
    }
