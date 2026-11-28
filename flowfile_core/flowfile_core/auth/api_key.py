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

from flowfile_core.auth.jwt import active_db_user
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
    now = datetime.datetime.now(datetime.UTC)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=datetime.UTC)
    return expires_at < now


def consumer_for_token(db: Session, raw_token: str | None) -> db_models.ApiConsumer | None:
    """The enabled consumer an enabled, unexpired key belongs to, or ``None``; stamps the key's ``last_used_at``.

    What a Delta Sharing recipient signs in with: access is then decided by the consumer's share grants.
    """
    if not raw_token:
        return None
    key = (
        db.query(db_models.FlowApiKey)
        .filter(db_models.FlowApiKey.key_hash == hash_api_key(raw_token), db_models.FlowApiKey.enabled.is_(True))
        .first()
    )
    if key is None or _is_expired(key.expires_at) or key.consumer_id is None:
        return None
    consumer = db.get(db_models.ApiConsumer, key.consumer_id)
    if consumer is None or not consumer.enabled or active_db_user(db, consumer.owner_id) is None:
        return None
    key.last_used_at = datetime.datetime.now(datetime.UTC)
    db.commit()
    return consumer


def verify_api_key(
    slug: str,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> db_models.FlowApiEndpoint:
    """FastAPI dependency: authenticate a request to ``/api/data/{slug}``.

    Resolves the ``X-API-Key`` header to an enabled, unexpired key, then to its
    ``ApiConsumer``, and authorizes the request only if that consumer is enabled and
    holds a grant for the endpoint named ``slug`` (which must itself be enabled, and
    whose owner must exist and not be disabled).
    Updates ``last_used_at`` and returns the resolved endpoint. The grant check is
    the sole authorization gate — there is no per-key endpoint fallback — so a single
    key can call every endpoint its consumer is granted. Raises 401 on any failure.
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
    # Security rests on the high-entropy token plus the hashed-column lookup: the
    # WHERE clause above already requires an exact key_hash match, so a further
    # constant-time compare here would always be True and adds nothing.
    if key is None or _is_expired(key.expires_at) or key.consumer_id is None:
        raise unauthorized

    consumer = db.get(db_models.ApiConsumer, key.consumer_id)
    if consumer is None or not consumer.enabled:
        raise unauthorized

    # Resolve the requested endpoint, then require a grant linking this consumer to
    # it. An unknown slug or a missing grant stays a 401 so we never confirm endpoint
    # existence to a key that isn't authorized for it. A disabled endpoint is reported
    # clearly (403), since the caller has proven they hold a granted key for it.
    endpoint = db.query(db_models.FlowApiEndpoint).filter(db_models.FlowApiEndpoint.slug == slug).first()
    if endpoint is None:
        raise unauthorized
    grant = (
        db.query(db_models.ApiConsumerEndpoint)
        .filter(
            db_models.ApiConsumerEndpoint.consumer_id == consumer.id,
            db_models.ApiConsumerEndpoint.endpoint_id == endpoint.id,
        )
        .first()
    )
    if grant is None:
        raise unauthorized
    # The flow runs as the endpoint's owner, so a disabled or deleted owner's API stops answering.
    if active_db_user(db, endpoint.owner_id) is None:
        raise unauthorized
    if not endpoint.enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This API endpoint is disabled",
        )

    key.last_used_at = datetime.datetime.now(datetime.UTC)
    db.commit()
    return endpoint
