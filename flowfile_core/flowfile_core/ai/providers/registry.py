"""Provider factory and supported-provider registry.

Owned by W11. ``provider_factory(name, *, model, surface, api_key, api_base)``
returns an instantiated ``Provider`` ready for ``chat()`` / ``stream()`` calls.
The BYOK key-loading layer lives in :mod:`flowfile_core.ai.byok` (W12) — call
``flowfile_core.ai.byok.get_configured_provider(db, user_id, provider, ...)``
when you need a provider configured against a user's encrypted credentials.

Surface→model resolution follows D010 placeholders: explicit ``model=`` wins,
otherwise ``surface_models[surface]`` if recognised, otherwise the provider's
``default_model``. The litellm vendor prefix is applied automatically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowfile_core.ai.providers.anthropic import AnthropicProvider
from flowfile_core.ai.providers.google import GoogleProvider
from flowfile_core.ai.providers.groq import GroqProvider
from flowfile_core.ai.providers.ollama import OllamaProvider
from flowfile_core.ai.providers.openai import OpenAIProvider
from flowfile_core.ai.providers.openrouter import OpenRouterProvider

if TYPE_CHECKING:
    from flowfile_core.ai.providers._litellm_base import LiteLLMProvider
    from flowfile_core.ai.providers.base import Provider


PROVIDERS: dict[str, type[LiteLLMProvider]] = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "google": GoogleProvider,
    "groq": GroqProvider,
    "openrouter": OpenRouterProvider,
    "ollama": OllamaProvider,
}


class UnknownProviderError(KeyError):
    """Raised when ``provider_factory`` receives an unrecognised provider name."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.name = name
        self.supported = sorted(PROVIDERS.keys())

    def __str__(self) -> str:
        return f"Unknown provider {self.name!r}; supported: {self.supported}"


def provider_factory(
    name: str,
    *,
    model: str | None = None,
    surface: str | None = None,
    api_key: str | None = None,
    api_base: str | None = None,
) -> Provider:
    """Return a configured ``Provider`` instance.

    Resolution order for the model: explicit ``model=`` arg > ``surface_models``
    lookup keyed by ``surface`` > ``default_model`` for the provider class.
    """
    cls = PROVIDERS.get(name)
    if cls is None:
        raise UnknownProviderError(name)

    resolved_model = model
    if resolved_model is None and surface is not None:
        resolved_model = cls.surface_models.get(surface, cls.default_model)

    return cls(model=resolved_model, api_key=api_key, api_base=api_base)


def list_supported_providers() -> list[str]:
    """Names of all wired providers (in registration order)."""
    return list(PROVIDERS.keys())
