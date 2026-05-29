"""Service layer for API consumers (reusable API clients / service accounts).

A consumer holds one or more rotatable ``FlowApiKey`` tokens and is granted access
to one or more published ``FlowApiEndpoint``s via ``ApiConsumerEndpoint`` rows. A
single key can therefore call every endpoint its consumer is granted — this is what
lets one key be shared across multiple flows.

These functions are intentionally low-level data operations: they assume the caller
(the router) has already resolved ownership. They raise ``LookupError`` for a missing
row (router → 404) and ``ValueError`` for a uniqueness conflict (router → 409). The
two helpers the per-flow "Create key" button relies on — ``get_or_create_implicit_consumer``
— keep that path on the same consumer-based auth path as everything else.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from flowfile_core.auth.api_key import generate_api_key
from flowfile_core.database import models as db_models


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


def _scoped(db: Session, owner_id: int | None):
    q = db.query(db_models.ApiConsumer)
    if owner_id is not None:
        q = q.filter(db_models.ApiConsumer.owner_id == owner_id)
    return q


def list_consumers(db: Session, owner_id: int | None) -> list[db_models.ApiConsumer]:
    """List explicit (non-implicit) consumers, newest first. ``owner_id=None`` = all (admin)."""
    return (
        _scoped(db, owner_id)
        .filter(db_models.ApiConsumer.is_implicit.is_(False))
        .order_by(db_models.ApiConsumer.created_at.desc())
        .all()
    )


def get_consumer(
    db: Session, consumer_id: int, owner_id: int | None, *, allow_implicit: bool = False
) -> db_models.ApiConsumer:
    consumer = _scoped(db, owner_id).filter(db_models.ApiConsumer.id == consumer_id).first()
    if consumer is None or (consumer.is_implicit and not allow_implicit):
        raise LookupError("Consumer not found")
    return consumer


def create_consumer(
    db: Session, owner_id: int, *, name: str, description: str | None = None, enabled: bool = True
) -> db_models.ApiConsumer:
    consumer = db_models.ApiConsumer(
        name=name.strip(), description=description, owner_id=owner_id, enabled=enabled, is_implicit=False
    )
    db.add(consumer)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("A consumer with that name already exists") from exc
    db.refresh(consumer)
    return consumer


def update_consumer(
    db: Session,
    consumer: db_models.ApiConsumer,
    *,
    name: str | None = None,
    description: str | None = None,
    enabled: bool | None = None,
) -> db_models.ApiConsumer:
    if name is not None:
        consumer.name = name.strip()
    if description is not None:
        consumer.description = description
    if enabled is not None:
        consumer.enabled = enabled
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("A consumer with that name already exists") from exc
    db.refresh(consumer)
    return consumer


def delete_consumer(db: Session, consumer: db_models.ApiConsumer) -> None:
    """Delete a consumer and its keys + grants (explicit deletes; SQLite FK is off)."""
    db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.consumer_id == consumer.id).delete(
        synchronize_session=False
    )
    db.query(db_models.ApiConsumerEndpoint).filter(
        db_models.ApiConsumerEndpoint.consumer_id == consumer.id
    ).delete(synchronize_session=False)
    db.delete(consumer)
    db.commit()


# ---------------------------------------------------------------------------
# Grants (consumer <-> endpoint)
# ---------------------------------------------------------------------------


def grant_endpoint(
    db: Session, consumer: db_models.ApiConsumer, endpoint: db_models.FlowApiEndpoint
) -> db_models.ApiConsumerEndpoint:
    grant = db_models.ApiConsumerEndpoint(consumer_id=consumer.id, endpoint_id=endpoint.id)
    db.add(grant)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("Consumer already has access to that endpoint") from exc
    db.refresh(grant)
    return grant


def revoke_endpoint(db: Session, consumer: db_models.ApiConsumer, endpoint_id: int) -> None:
    db.query(db_models.ApiConsumerEndpoint).filter(
        db_models.ApiConsumerEndpoint.consumer_id == consumer.id,
        db_models.ApiConsumerEndpoint.endpoint_id == endpoint_id,
    ).delete(synchronize_session=False)
    db.commit()


def list_granted_endpoints(db: Session, consumer: db_models.ApiConsumer) -> list[db_models.FlowApiEndpoint]:
    return (
        db.query(db_models.FlowApiEndpoint)
        .join(db_models.ApiConsumerEndpoint, db_models.ApiConsumerEndpoint.endpoint_id == db_models.FlowApiEndpoint.id)
        .filter(db_models.ApiConsumerEndpoint.consumer_id == consumer.id)
        .all()
    )


def list_consumers_for_endpoint(
    db: Session, endpoint_id: int, *, include_implicit: bool = False
) -> list[db_models.ApiConsumer]:
    q = (
        db.query(db_models.ApiConsumer)
        .join(db_models.ApiConsumerEndpoint, db_models.ApiConsumerEndpoint.consumer_id == db_models.ApiConsumer.id)
        .filter(db_models.ApiConsumerEndpoint.endpoint_id == endpoint_id)
    )
    if not include_implicit:
        q = q.filter(db_models.ApiConsumer.is_implicit.is_(False))
    return q.all()


def count_endpoints(db: Session, consumer_id: int) -> int:
    return (
        db.query(db_models.ApiConsumerEndpoint)
        .filter(db_models.ApiConsumerEndpoint.consumer_id == consumer_id)
        .count()
    )


def count_keys(db: Session, consumer_id: int) -> int:
    return db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.consumer_id == consumer_id).count()


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


def create_key(
    db: Session,
    consumer: db_models.ApiConsumer,
    *,
    name: str,
    expires_at=None,
    endpoint_id: int | None = None,
) -> tuple[db_models.FlowApiKey, str]:
    """Mint a key for ``consumer``. Returns ``(key, raw_token)`` — the raw token once."""
    raw_token, key_hash, key_prefix = generate_api_key()
    key = db_models.FlowApiKey(
        consumer_id=consumer.id,
        endpoint_id=endpoint_id,
        owner_id=consumer.owner_id,
        name=name,
        key_hash=key_hash,
        key_prefix=key_prefix,
        expires_at=expires_at,
    )
    db.add(key)
    db.commit()
    db.refresh(key)
    return key, raw_token


def list_keys(db: Session, consumer: db_models.ApiConsumer) -> list[db_models.FlowApiKey]:
    return db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.consumer_id == consumer.id).all()


def get_key(db: Session, consumer: db_models.ApiConsumer, key_id: int) -> db_models.FlowApiKey:
    key = db.get(db_models.FlowApiKey, key_id)
    if key is None or key.consumer_id != consumer.id:
        raise LookupError("Key not found")
    return key


def update_key(
    db: Session, key: db_models.FlowApiKey, *, name: str | None = None, enabled: bool | None = None
) -> db_models.FlowApiKey:
    if name is not None:
        key.name = name
    if enabled is not None:
        key.enabled = enabled
    db.commit()
    db.refresh(key)
    return key


def delete_key(db: Session, key: db_models.FlowApiKey) -> None:
    db.delete(key)
    db.commit()


# ---------------------------------------------------------------------------
# Implicit consumer (backs the per-flow "Create key" button)
# ---------------------------------------------------------------------------


def get_or_create_implicit_consumer(
    db: Session, endpoint: db_models.FlowApiEndpoint
) -> db_models.ApiConsumer:
    """Return the implicit, single-endpoint consumer for ``endpoint``, creating it if needed.

    A per-flow key created from the flow's API panel belongs to this consumer, so it
    flows through the same consumer + grant auth path as a shared consumer's keys.
    Flushed (not committed) so the caller can commit the consumer, grant and key in
    one transaction. Resolved by an existing grant first, so renaming the endpoint's
    slug never spawns a duplicate implicit consumer.
    """
    existing = (
        db.query(db_models.ApiConsumer)
        .join(db_models.ApiConsumerEndpoint, db_models.ApiConsumerEndpoint.consumer_id == db_models.ApiConsumer.id)
        .filter(
            db_models.ApiConsumerEndpoint.endpoint_id == endpoint.id,
            db_models.ApiConsumer.is_implicit.is_(True),
        )
        .first()
    )
    if existing is not None:
        return existing

    name = f"endpoint:{endpoint.slug}"
    consumer = (
        db.query(db_models.ApiConsumer).filter_by(owner_id=endpoint.owner_id, name=name).first()
    )
    if consumer is None:
        consumer = db_models.ApiConsumer(
            name=name,
            description=f"Auto-created for endpoint {endpoint.slug}",
            owner_id=endpoint.owner_id,
            enabled=True,
            is_implicit=True,
        )
        db.add(consumer)
        db.flush()
    if (
        db.query(db_models.ApiConsumerEndpoint)
        .filter_by(consumer_id=consumer.id, endpoint_id=endpoint.id)
        .first()
        is None
    ):
        db.add(db_models.ApiConsumerEndpoint(consumer_id=consumer.id, endpoint_id=endpoint.id))
        db.flush()
    return consumer
