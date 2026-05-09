"""HTTP routes for BYOK provider credentials.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. All
endpoints require an authenticated user via
``Depends(get_current_active_user)``. The feature-flag dependency
sits on the parent ``ai_router``; it's intentionally not enforced
per-route here so each route module stays independently testable.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.ai.byok import detect_env_fallback, get_configured_provider
from flowfile_core.ai.credentials import (
    ProviderCredentialInput,
    ProviderCredentialPublic,
    ProviderListItem,
    ProviderTestResult,
    delete_provider_credential,
    get_provider_credential,
    list_provider_credentials,
    to_public,
    update_test_status,
    upsert_provider_credential,
)
from flowfile_core.ai.providers import PROVIDERS, Message, UnknownProviderError, list_supported_providers
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


@router.get("/providers", response_model=list[ProviderListItem], tags=["ai"])
def get_providers(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[ProviderListItem]:
    """List all supported providers, enriched with the caller's BYOK state.

    Each entry carries class-level metadata (default model, surface map,
    capability flags) plus a ``status`` of ``configured`` (row + key, or
    Ollama row), ``env_fallback`` (no row but a recognised env var is set),
    or ``unconfigured`` (neither). The credential snapshot is included
    when a row exists.
    """
    user_creds = {c.provider: c for c in list_provider_credentials(db, current_user.id)}
    items: list[ProviderListItem] = []
    for name in list_supported_providers():
        cls = PROVIDERS[name]
        cred = user_creds.get(name)
        if cred is not None and (cred.api_key_secret_id is not None or name == "ollama"):
            status = "configured"
        elif cred is not None and cred.api_key_secret_id is None and detect_env_fallback(name):
            status = "env_fallback"
        elif cred is None and detect_env_fallback(name):
            status = "env_fallback"
        else:
            status = "unconfigured"
        items.append(
            ProviderListItem(
                provider=name,
                supports_tools=cls.supports_tools,
                supports_streaming=cls.supports_streaming,
                default_model=cls.default_model,
                surfaces=dict(cls.surface_models),
                status=status,
                credential=to_public(cred) if cred is not None else None,
            )
        )
    return items


@router.post("/providers/{name}", response_model=ProviderCredentialPublic, tags=["ai"])
def upsert_provider(
    name: str,
    payload: ProviderCredentialInput,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> ProviderCredentialPublic:
    """Create or update the caller's credential for ``name``.

    Idempotent on the natural key ``(user_id, provider)``. The Pydantic
    validator already rejects ``api_key`` + ``clear_api_key`` together;
    this handler doesn't need to re-check.
    """
    _ensure_known_provider(name)
    cred = upsert_provider_credential(db, current_user.id, name, payload)
    return to_public(cred)


@router.delete("/providers/{name}", status_code=204, tags=["ai"])
def remove_provider(
    name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete the caller's credential and any owned secret atomically.

    Returns 404 if no row exists. Does not affect env-var fallback —
    deleting a credential whose key was unset just removes the row.
    """
    _ensure_known_provider(name)
    delete_provider_credential(db, current_user.id, name)


@router.post("/providers/{name}/test", response_model=ProviderTestResult, tags=["ai"])
async def test_provider(
    name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> ProviderTestResult:
    """Live-test the caller's credential by issuing a 1-token chat ping.

    On success persists ``last_tested_at + last_test_status="ok"``; on
    failure persists ``last_test_status="error"`` with the exception message
    (truncated). Non-2xx HTTP statuses are still returned as ``200`` with
    ``ok=false`` so the UI can render the error inline rather than handling
    fetch errors specially.
    """
    _ensure_known_provider(name)
    try:
        provider = get_configured_provider(db, current_user.id, name)
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 — surface any setup failure to the UI
        result = ProviderTestResult(ok=False, error=str(exc))
        cred = get_provider_credential(db, current_user.id, name)
        if cred is not None:
            update_test_status(db, cred.id, ok=False, error=result.error)
        return result

    try:
        await provider.chat(
            messages=[Message(role="user", content="ping")],
            max_tokens=1,
        )
        ok, error = True, None
    except Exception as exc:  # noqa: BLE001 — provider-side errors surfaced to UI
        ok, error = False, str(exc)[:512]

    cred = get_provider_credential(db, current_user.id, name)
    if cred is not None:
        update_test_status(db, cred.id, ok=ok, error=error)
    return ProviderTestResult(ok=ok, error=error)


__all__ = ["router"]
