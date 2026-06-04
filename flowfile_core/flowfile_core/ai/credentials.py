"""AI BYOK credentials — DB CRUD + Pydantic schemas.

Keeps the credential layer **provider-import-free** so importing
``flowfile_core.ai.credentials`` doesn't pull in ``litellm``
(verified by a snapshot test). The runtime seam — loading
credentials and instantiating a
:class:`~flowfile_core.ai.providers.base.Provider` — lives in
:mod:`flowfile_core.ai.byok`. The HTTP routes live in
:mod:`flowfile_core.ai.byok_routes`.

Design notes:

* **Secret rotation in place.** When ``upsert_provider_credential``
  receives a new ``api_key`` and a credential row with an existing
  ``api_key_secret_id``, we mutate the ``Secret.encrypted_value`` of
  the referenced row rather than creating a new ``Secret`` row.
  Matches the
  ``database_connection_manager.db_connections.update_database_connection``
  precedent and keeps secret ids stable for any future audit
  references.
* **Secret name convention.**
  ``ai:{provider}:api_key:{user_id}:{credential_id}`` — descriptive
  only; the FK ``ai_provider_credentials.api_key_secret_id`` is the
  unambiguous link. Avoids name clashes with other features that
  share the ``secrets`` table.
* **Explicit clear semantics.** The Pydantic input has a separate
  ``clear_api_key: bool`` flag (and ``clear_models``) so the user
  can rotate to "no key" / "no curated model list" without deleting
  and recreating the whole row. ``api_key=None`` / ``models=None``
  mean "keep existing"; the clear flag is mutually exclusive with
  the corresponding value (raises 422).
* **Models as JSON-in-Text.** ``AiProviderCredential.models`` is a
  nullable ``Text`` column holding a JSON-encoded ``list[str]`` so
  the schema works on SQLite and PG identically (matches the
  pattern used by ``flow_runs.node_results_json``, ``configs.tags``
  etc.). All encode/decode happens here so callers see
  ``list[str] | None``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field, SecretStr, model_validator
from sqlalchemy.orm import Session

from flowfile_core.auth.models import SecretInput
from flowfile_core.database.models import AiProviderCredential, Secret
from flowfile_core.secret_manager.secret_manager import (
    decrypt_secret,
    encrypt_secret,
    store_secret,
)

# ---------- Pydantic schemas ----------


class ProviderCredentialInput(BaseModel):
    """Body for ``POST /ai/providers/{name}``.

    Field semantics:

    * ``api_key=None`` (default) → keep the existing secret untouched.
    * ``api_key="sk-..."`` → store/rotate the secret in place.
    * ``clear_api_key=True`` → drop the existing secret (FK nulled, row deleted).
    * ``api_base`` / ``default_model`` overwrite when provided (``None`` keeps).
    * ``models=None`` (default) → keep the existing curated model list.
    * ``models=["a", "b"]`` → replace the curated list verbatim, in order.
    * ``models=[]`` → equivalent to ``clear_models=True`` (the user picked
      "no curated list"). Stored as ``NULL`` so reads behave consistently.
    * ``clear_models=True`` → null the curated list. Mutually exclusive with a
      non-empty ``models``.
    """

    api_key: SecretStr | None = None
    clear_api_key: bool = False
    api_base: str | None = None
    default_model: str | None = None
    models: list[str] | None = None
    clear_models: bool = False

    @model_validator(mode="after")
    def _check_api_key_and_clear_mutually_exclusive(self) -> ProviderCredentialInput:
        if self.api_key is not None and self.clear_api_key:
            raise ValueError("api_key and clear_api_key are mutually exclusive")
        if self.models is not None and len(self.models) > 0 and self.clear_models:
            raise ValueError("models and clear_models are mutually exclusive")
        return self


class ProviderCredentialPublic(BaseModel):
    """Read-only view returned by every BYOK route. Never includes the key."""

    provider: str
    has_key: bool
    api_base: str | None = None
    default_model: str | None = None
    models: list[str] | None = None
    last_tested_at: datetime | None = None
    last_test_status: Literal["ok", "error"] | None = None
    last_test_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


ProviderStatus = Literal["configured", "env_fallback", "unconfigured"]


class ProviderListItem(BaseModel):
    """Per-provider entry in ``GET /ai/providers``.

    Enriches :func:`flowfile_core.ai.providers.list_supported_providers` with
    class-level metadata (default model, surface map, capability flags) and
    the user's per-provider state (``status``, optional ``credential``).
    """

    provider: str
    supports_tools: bool
    supports_streaming: bool
    default_model: str
    surfaces: dict[str, str] = Field(default_factory=dict)
    status: ProviderStatus
    credential: ProviderCredentialPublic | None = None


class ProviderTestResult(BaseModel):
    """Body returned by ``POST /ai/providers/{name}/test``."""

    ok: bool
    error: str | None = None


# ---------- DB CRUD ----------


def get_provider_credential(db: Session, user_id: int, provider: str) -> AiProviderCredential | None:
    """Return the (user, provider) credential row, or ``None``."""
    return (
        db.query(AiProviderCredential)
        .filter(
            AiProviderCredential.user_id == user_id,
            AiProviderCredential.provider == provider,
        )
        .first()
    )


def list_provider_credentials(db: Session, user_id: int) -> list[AiProviderCredential]:
    """Return all credential rows for a user (no decryption)."""
    return (
        db.query(AiProviderCredential)
        .filter(AiProviderCredential.user_id == user_id)
        .order_by(AiProviderCredential.provider.asc())
        .all()
    )


def upsert_provider_credential(
    db: Session,
    user_id: int,
    provider: str,
    payload: ProviderCredentialInput,
) -> AiProviderCredential:
    """Create-or-update the (user, provider) credential.

    See :class:`ProviderCredentialInput` for field semantics. The 422 for the
    ``api_key`` + ``clear_api_key`` collision is raised by the Pydantic
    validator before we get here, but route handlers should still translate
    ``ValidationError`` → ``HTTPException(422)``.
    """
    cred = get_provider_credential(db, user_id, provider)
    is_new = cred is None
    if cred is None:
        cred = AiProviderCredential(user_id=user_id, provider=provider)
        db.add(cred)
        db.flush()  # so we have cred.id for the secret name

    if payload.api_base is not None:
        cred.api_base = payload.api_base or None
    if payload.default_model is not None:
        cred.default_model = payload.default_model or None

    if payload.clear_api_key:
        _clear_api_key_secret(db, cred)
    elif payload.api_key is not None:
        _store_or_rotate_api_key(db, cred, user_id, payload.api_key.get_secret_value())

    if payload.clear_models or (payload.models is not None and len(payload.models) == 0):
        cred.models = None
    elif payload.models is not None:
        cred.models = json.dumps(list(payload.models))

    db.flush()
    if is_new:
        db.commit()
        db.refresh(cred)
    else:
        db.commit()
        db.refresh(cred)
    return cred


def delete_provider_credential(db: Session, user_id: int, provider: str) -> None:
    """Delete the credential and its secret (if any) atomically.

    Raises ``HTTPException(404)`` if no row matches.
    """
    cred = get_provider_credential(db, user_id, provider)
    if cred is None:
        raise HTTPException(status_code=404, detail="Provider credential not found")
    _clear_api_key_secret(db, cred)
    db.delete(cred)
    db.commit()


def update_test_status(
    db: Session,
    credential_id: int,
    ok: bool,
    error: str | None = None,
) -> None:
    """Record a ``/test`` outcome on the credential row."""
    cred = db.query(AiProviderCredential).filter(AiProviderCredential.id == credential_id).first()
    if cred is None:
        return
    cred.last_tested_at = datetime.utcnow()
    cred.last_test_status = "ok" if ok else "error"
    cred.last_test_error = None if ok else (error or "unknown error")
    db.commit()


# ---------- Helpers (private) ----------


def _store_or_rotate_api_key(
    db: Session,
    cred: AiProviderCredential,
    user_id: int,
    plaintext_key: str,
) -> None:
    """Mutate the existing ``Secret.encrypted_value`` if present, else create.

    Mirrors the cloud-connection precedent in
    ``db_connections.update_database_connection`` so secret ids stay stable
    across rotations.
    """
    secret_name = f"ai:{cred.provider}:api_key:{user_id}:{cred.id}"
    if cred.api_key_secret_id is not None:
        existing = db.query(Secret).filter(Secret.id == cred.api_key_secret_id).first()
        if existing is not None:
            existing.encrypted_value = encrypt_secret(plaintext_key, user_id)
            existing.name = secret_name
            return
        # FK pointed at a vanished secret — fall through and create a fresh one.
    new_secret = store_secret(db, SecretInput(name=secret_name, value=SecretStr(plaintext_key)), user_id)
    cred.api_key_secret_id = new_secret.id


def _clear_api_key_secret(db: Session, cred: AiProviderCredential) -> None:
    """Drop the referenced ``Secret`` row (if any) and null the FK."""
    if cred.api_key_secret_id is None:
        return
    secret = db.query(Secret).filter(Secret.id == cred.api_key_secret_id).first()
    if secret is not None:
        db.delete(secret)
    cred.api_key_secret_id = None


def decrypt_api_key(db: Session, cred: AiProviderCredential) -> str | None:
    """Return the plaintext api_key for a credential, or ``None`` if unset."""
    if cred.api_key_secret_id is None:
        return None
    secret = db.query(Secret).filter(Secret.id == cred.api_key_secret_id).first()
    if secret is None:
        return None
    return decrypt_secret(secret.encrypted_value, cred.user_id).get_secret_value()


def decode_models(cred: AiProviderCredential) -> list[str] | None:
    """Decode the JSON-encoded ``models`` column to a ``list[str]`` or ``None``.

    Returns ``None`` for missing / empty / malformed payloads so downstream
    callers can treat "no curated list" uniformly. Bad JSON in the DB means
    the column was hand-edited or migration-corrupted; logging is left to the
    caller because the recovery action is "fall through to ``default_model``"
    either way.
    """
    raw = cred.models
    if raw is None:
        return None
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, list) or not decoded:
        return None
    return [str(item) for item in decoded]


def to_public(cred: AiProviderCredential) -> ProviderCredentialPublic:
    """Project an ORM row to its public Pydantic shape."""
    return ProviderCredentialPublic(
        provider=cred.provider,
        has_key=cred.api_key_secret_id is not None,
        api_base=cred.api_base,
        default_model=cred.default_model,
        models=decode_models(cred),
        last_tested_at=cred.last_tested_at,
        last_test_status=cred.last_test_status,  # type: ignore[arg-type]
        last_test_error=cred.last_test_error,
        created_at=cred.created_at,
        updated_at=cred.updated_at,
    )


__all__ = [
    "ProviderCredentialInput",
    "ProviderCredentialPublic",
    "ProviderListItem",
    "ProviderStatus",
    "ProviderTestResult",
    "decode_models",
    "decrypt_api_key",
    "delete_provider_credential",
    "get_provider_credential",
    "list_provider_credentials",
    "to_public",
    "update_test_status",
    "upsert_provider_credential",
]
