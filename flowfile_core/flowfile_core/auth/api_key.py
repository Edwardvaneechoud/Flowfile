"""API-key generation and verification for published flow endpoints.

Tokens are random and high-entropy, so they are stored as a one-way SHA-256
hash (never recoverable). The raw token is shown to the user exactly once, at
creation time.
"""

import datetime
import hashlib
import secrets as _secrets

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

_TOKEN_PREFIX = "ffk_"
_PREFIX_DISPLAY_LEN = 12


def hash_api_key(raw_token: str) -> str:
    """One-way SHA-256 hash of an API token (hex digest)."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """Return ``(raw_token, key_hash, key_prefix)`` for a fresh API key."""
    raw = _TOKEN_PREFIX + _secrets.token_urlsafe(32)
    return raw, hash_api_key(raw), raw[:_PREFIX_DISPLAY_LEN]


def _is_expired(expires_at: datetime.datetime | None) -> bool:
    if expires_at is None:
        return False
    now = datetime.datetime.now(datetime.timezone.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
    return expires_at < now


def verify_api_key(
    slug: str,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> db_models.FlowApiEndpoint:
    """FastAPI dependency: authenticate a request to ``/api/data/{slug}``.

    Validates the ``X-API-Key`` header against an enabled, unexpired key whose
    endpoint matches ``slug`` and is itself enabled, updates ``last_used_at`` and
    returns the resolved endpoint. Raises 401 on any failure.
    """
    unauthorized = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "ApiKey"},
    )
    if not x_api_key:
        raise unauthorized

    key_hash = hash_api_key(x_api_key)
    key = (
        db.query(db_models.FlowApiKey)
        .filter(
            db_models.FlowApiKey.key_hash == key_hash,
            db_models.FlowApiKey.enabled.is_(True),
        )
        .first()
    )
    if key is None or not _secrets.compare_digest(key.key_hash, key_hash) or _is_expired(key.expires_at):
        raise unauthorized

    endpoint = db.get(db_models.FlowApiEndpoint, key.endpoint_id)
    # Wrong/unknown slug stays a 401 so we don't confirm endpoint existence to a
    # key that isn't scoped to it. A disabled endpoint is reported clearly, since
    # the caller already proved they hold a valid key for it.
    if endpoint is None or endpoint.slug != slug:
        raise unauthorized
    if not endpoint.enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This API endpoint is disabled",
        )

    key.last_used_at = datetime.datetime.now(datetime.timezone.utc)
    db.commit()
    return endpoint
