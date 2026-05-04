"""BYOK runtime seam — load credentials and instantiate a Provider (W12).

Owned by W12. This is the module downstream callers use when they need a
configured :class:`~flowfile_core.ai.providers.base.Provider` for a given
``(user, provider, surface)`` triple. Kept separate from
:mod:`flowfile_core.ai.credentials` so the credential CRUD layer stays
``litellm``-import-free (``import flowfile_core.ai.credentials`` doesn't
trigger any provider-class import; this module does).

Resolution order for the model passed to ``provider_factory`` (W29 widens
W12's original 4-step order with two list-aware steps so OpenRouter /
Groq users can curate several free models per credential and still get
sensible per-surface defaults):

1. Explicit ``model=`` argument (caller knows best — chat-drawer per-request
   overrides land here).
2. The credential row's ``default_model`` (user-configured override).
3. The provider class' ``surface_models[surface]`` if it appears in the
   user's curated ``models`` list (so per-surface routing still wins when the
   user has explicitly opted into the routed model).
4. Otherwise, the first entry of the user's curated ``models`` list (so a
   surface call doesn't silently fall back to the class default when the
   user has expressed a preference).
5. The provider class' ``surface_models[surface]`` (existing fallback).
6. The provider class' ``default_model`` (terminal fallback).

When no credential row exists for the user, ``get_configured_provider``
falls through to ``provider_factory(name)`` with no ``api_key`` /
``api_base``. ``litellm`` then picks up env vars like ``ANTHROPIC_API_KEY``
on its own — :func:`detect_env_fallback` exists so the
``GET /ai/providers`` route can surface that distinction in the UI.
"""

from __future__ import annotations

import os

from sqlalchemy.orm import Session

from flowfile_core.ai.credentials import (
    decode_models,
    decrypt_api_key,
    get_provider_credential,
)
from flowfile_core.ai.providers import PROVIDERS, Provider, provider_factory

# Per-provider env-var names that ``litellm`` reads when no api_key is passed.
# Ollama is always considered "available" (no key required) — surface it as
# ``configured`` only when the user has saved a row (otherwise ``unconfigured``
# pending the user pointing us at their local server).
_PROVIDER_ENV_VARS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
    "groq": ("GROQ_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
    "ollama": (),  # no key required; falls into ``unconfigured`` until row saved.
}


class ProviderNotConfiguredError(RuntimeError):
    """Raised when no credential row exists and no env-var fallback is available."""

    def __init__(self, provider: str) -> None:
        super().__init__(
            f"Provider {provider!r} is not configured. Save an API key via "
            f"POST /ai/providers/{provider} or set the appropriate env var."
        )
        self.provider = provider


def detect_env_fallback(provider: str) -> bool:
    """True iff a recognised env var is set for ``provider``.

    Used by ``GET /ai/providers`` to distinguish ``env_fallback`` from
    ``unconfigured`` so the UI can show the user "we'll fall back to your
    ANTHROPIC_API_KEY env var if you don't save a key here".
    """
    env_vars = _PROVIDER_ENV_VARS.get(provider, ())
    return any(os.environ.get(name) for name in env_vars)


def get_configured_provider(
    db: Session,
    user_id: int,
    provider: str,
    *,
    surface: str | None = None,
    model: str | None = None,
) -> Provider:
    """Load BYOK credentials and return a configured :class:`Provider`.

    Falls back to ``provider_factory(provider)`` (no key, no base) if no
    credential row exists — ``litellm`` will pick up env vars like
    ``ANTHROPIC_API_KEY`` on its own. Ollama needs no key but typically
    needs an ``api_base``; if neither a row nor a default base is set on
    the class, the call will fail when ``litellm`` actually dials out.

    Raises :class:`ProviderNotConfiguredError` only when the provider is
    completely unset — no row, no env var, and not Ollama. The vast
    majority of well-configured calls return cleanly without ever raising.
    """
    cred = get_provider_credential(db, user_id, provider)

    api_key: str | None = None
    api_base: str | None = None
    resolved_model = model

    cls = PROVIDERS.get(provider)

    if cred is not None:
        api_key = decrypt_api_key(db, cred)
        api_base = cred.api_base
        if resolved_model is None and cred.default_model is not None:
            resolved_model = cred.default_model

        # W29 — consult the user's curated models list when neither an
        # explicit ``model=`` nor a stored ``default_model`` won. Step 3:
        # if a per-surface routing target appears in the user's list,
        # honour it. Step 4: otherwise, take the first listed model as
        # the per-credential default.
        if resolved_model is None:
            curated = decode_models(cred)
            if curated:
                surface_route = cls.surface_models.get(surface) if (cls is not None and surface is not None) else None
                if surface_route is not None and surface_route in curated:
                    resolved_model = surface_route
                else:
                    resolved_model = curated[0]

    has_env_fallback = detect_env_fallback(provider)
    has_default_base = cls is not None and cls.default_api_base is not None
    if cred is None and not has_env_fallback and not has_default_base and provider != "ollama":
        raise ProviderNotConfiguredError(provider)

    # When ``resolved_model`` is set we pass it explicitly; otherwise let
    # ``provider_factory`` resolve via the surface map.
    surface_for_factory = surface if resolved_model is None else None

    return provider_factory(
        provider,
        model=resolved_model,
        surface=surface_for_factory,
        api_key=api_key,
        api_base=api_base,
    )


__all__ = [
    "ProviderNotConfiguredError",
    "detect_env_fallback",
    "get_configured_provider",
]
