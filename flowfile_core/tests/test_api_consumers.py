"""Tests for API consumers (reusable API clients) and consumer-based auth.

Covers:
- consumer CRUD + owner scoping (admin sees all; implicit consumers hidden),
- grant / revoke and the foreign-endpoint / duplicate-grant guards,
- the headline behaviour: ONE key authorizes EVERY endpoint its consumer is granted,
  and an un-granted endpoint stays 401 (no privilege escalation),
- disabled consumer -> 401; delete cascades keys + grants,
- the per-flow "Create key" button creating an implicit consumer on the one auth path,
- migration 018's backfill linking pre-existing keys to an implicit consumer.

Routes are called directly with an in-memory SQLite session (no HTTP), mirroring
test_flow_api.py.
"""

import importlib.util
import pathlib
from types import SimpleNamespace
from uuid import uuid4

import alembic.op
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import flowfile_core
from flowfile_core.auth import api_key as api_key_mod
from flowfile_core.database import models as db_models
from flowfile_core.flowfile import api_consumer_manager
from flowfile_core.routes import api_consumers, flow_api
from flowfile_core.schemas.flow_api_schema import (
    ApiConsumerCreate,
    ApiConsumerGrant,
    ApiConsumerUpdate,
    ApiKeyCreate,
    ApiKeyUpdate,
)

# Distinct ids so owner-scoping is observable. local_user (Electron) is id=1 + admin.
ADMIN = SimpleNamespace(id=1, is_admin=True)
USER1 = SimpleNamespace(id=1, is_admin=False)
USER2 = SimpleNamespace(id=2, is_admin=False)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    db_models.Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


def _endpoint(db, *, slug, owner_id=1, enabled=True):
    """Register a flow and publish a bare endpoint for it (no consumer/key)."""
    reg = db_models.FlowRegistration(
        flow_uuid=str(uuid4()), name=f"flow-{slug}", flow_path=f"/tmp/{slug}.yaml", owner_id=owner_id
    )
    db.add(reg)
    db.commit()
    db.refresh(reg)
    ep = db_models.FlowApiEndpoint(registration_id=reg.id, owner_id=owner_id, slug=slug, enabled=enabled)
    db.add(ep)
    db.commit()
    db.refresh(ep)
    return ep


def _consumer_granted_to(db, *, slug, name="prod", owner=USER1):
    """Helper: explicit consumer + endpoint + grant + key. Returns (consumer_out, ep, raw)."""
    ep = _endpoint(db, slug=slug, owner_id=owner.id)
    c = api_consumers.create_consumer(ApiConsumerCreate(name=name), current_user=owner, db=db)
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep.id), current_user=owner, db=db)
    created = api_consumers.create_key(c.id, ApiKeyCreate(name="k"), current_user=owner, db=db)
    return c, ep, created.api_key


# Consumer CRUD + scoping


def test_create_list_get_consumer(db_session):
    out = api_consumers.create_consumer(
        ApiConsumerCreate(name="prod", description="d"), current_user=USER1, db=db_session
    )
    assert out.id and out.name == "prod" and out.is_implicit is False
    assert out.endpoint_count == 0 and out.key_count == 0

    listed = api_consumers.list_consumers(current_user=USER1, db=db_session)
    assert [c.name for c in listed] == ["prod"]

    got = api_consumers.get_consumer(out.id, current_user=USER1, db=db_session)
    assert got.id == out.id


def test_duplicate_consumer_name_is_409(db_session):
    api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    with pytest.raises(HTTPException) as exc:
        api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    assert exc.value.status_code == 409


def test_list_excludes_implicit_and_scopes_by_owner(db_session):
    api_consumers.create_consumer(ApiConsumerCreate(name="mine"), current_user=USER1, db=db_session)
    api_consumers.create_consumer(ApiConsumerCreate(name="theirs"), current_user=USER2, db=db_session)
    # an implicit consumer (created by the per-flow button path) must stay hidden
    ep = _endpoint(db_session, slug="sales", owner_id=1)
    api_consumer_manager.get_or_create_implicit_consumer(db_session, ep)
    db_session.commit()

    u1 = api_consumers.list_consumers(current_user=USER1, db=db_session)
    assert [c.name for c in u1] == ["mine"]  # USER2's hidden, implicit hidden

    all_admin = api_consumers.list_consumers(current_user=ADMIN, db=db_session)
    assert sorted(c.name for c in all_admin) == ["mine", "theirs"]  # admin sees all explicit, no implicit


def test_get_foreign_consumer_is_404(db_session):
    out = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    with pytest.raises(HTTPException) as exc:
        api_consumers.get_consumer(out.id, current_user=USER2, db=db_session)
    assert exc.value.status_code == 404


def test_update_consumer(db_session):
    out = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    upd = api_consumers.update_consumer(
        out.id, ApiConsumerUpdate(enabled=False, description="x"), current_user=USER1, db=db_session
    )
    assert upd.enabled is False and upd.description == "x"


# Grants


def test_grant_and_revoke(db_session):
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    ep1 = _endpoint(db_session, slug="a")
    ep2 = _endpoint(db_session, slug="b")
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep1.id), current_user=USER1, db=db_session)
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep2.id), current_user=USER1, db=db_session)
    granted = api_consumers.list_granted_endpoints(c.id, current_user=USER1, db=db_session)
    assert sorted(e.slug for e in granted) == ["a", "b"]

    # duplicate grant -> 409
    with pytest.raises(HTTPException) as exc:
        api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep1.id), current_user=USER1, db=db_session)
    assert exc.value.status_code == 409

    api_consumers.revoke_endpoint(c.id, ep1.id, current_user=USER1, db=db_session)
    granted = api_consumers.list_granted_endpoints(c.id, current_user=USER1, db=db_session)
    assert [e.slug for e in granted] == ["b"]


def test_grant_foreign_endpoint_is_404(db_session):
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    ep_other = _endpoint(db_session, slug="z", owner_id=2)  # owned by USER2
    with pytest.raises(HTTPException) as exc:
        api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep_other.id), current_user=USER1, db=db_session)
    assert exc.value.status_code == 404


def test_available_endpoints_lists_only_owner_endpoints(db_session):
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    _endpoint(db_session, slug="a", owner_id=1)
    _endpoint(db_session, slug="b", owner_id=1)
    _endpoint(db_session, slug="z", owner_id=2)
    avail = api_consumers.list_available_endpoints(c.id, current_user=USER1, db=db_session)
    assert sorted(e.slug for e in avail) == ["a", "b"]


# Consumer-based auth: one key, many endpoints


def test_one_key_authorizes_every_granted_endpoint(db_session):
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    ep1 = _endpoint(db_session, slug="a")
    ep2 = _endpoint(db_session, slug="b")
    _endpoint(db_session, slug="c")  # published but NOT granted
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep1.id), current_user=USER1, db=db_session)
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep2.id), current_user=USER1, db=db_session)
    raw = api_consumers.create_key(c.id, ApiKeyCreate(name="k"), current_user=USER1, db=db_session).api_key

    assert api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session).id == ep1.id
    assert api_key_mod.verify_api_key(slug="b", x_api_key=raw, db=db_session).id == ep2.id
    # The same valid key must NOT reach an endpoint it wasn't granted (escalation guard).
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="c", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401


def test_disabled_consumer_rejects_its_keys(db_session):
    c, ep, raw = _consumer_granted_to(db_session, slug="a")
    assert api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session).id == ep.id

    api_consumers.update_consumer(c.id, ApiConsumerUpdate(enabled=False), current_user=USER1, db=db_session)
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401


def test_disabled_endpoint_with_granted_key_is_403(db_session):
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    ep = _endpoint(db_session, slug="a", enabled=False)
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep.id), current_user=USER1, db=db_session)
    raw = api_consumers.create_key(c.id, ApiKeyCreate(name="k"), current_user=USER1, db=db_session).api_key
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 403
    assert "disabled" in exc.value.detail.lower()


# Keys


def test_key_rename_disable_and_delete(db_session):
    c, ep, raw = _consumer_granted_to(db_session, slug="a")
    key = api_consumers.list_keys(c.id, current_user=USER1, db=db_session)[0]

    upd = api_consumers.update_key(
        c.id, key.id, ApiKeyUpdate(enabled=False, name="renamed"), current_user=USER1, db=db_session
    )
    assert upd.enabled is False and upd.name == "renamed"
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401

    api_consumers.delete_key(c.id, key.id, current_user=USER1, db=db_session)
    assert api_consumers.list_keys(c.id, current_user=USER1, db=db_session) == []


def test_delete_consumer_removes_keys_and_grants(db_session):
    c, ep, raw = _consumer_granted_to(db_session, slug="a")
    cid = c.id
    api_consumers.delete_consumer(cid, current_user=USER1, db=db_session)

    assert db_session.get(db_models.ApiConsumer, cid) is None
    assert db_session.query(db_models.FlowApiKey).filter_by(consumer_id=cid).count() == 0
    assert db_session.query(db_models.ApiConsumerEndpoint).filter_by(consumer_id=cid).count() == 0
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="a", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401


# Per-flow "Create key" button -> implicit consumer (single auth path)


def test_per_flow_key_creates_implicit_consumer(db_session):
    ep = _endpoint(db_session, slug="sales")
    created = flow_api.create_key(ep.id, ApiKeyCreate(name="prod"), current_user=USER1, db=db_session)
    raw = created.api_key

    assert created.consumer_id is not None
    consumer = db_session.get(db_models.ApiConsumer, created.consumer_id)
    assert consumer.is_implicit is True and consumer.name == "endpoint:sales"
    assert (
        db_session.query(db_models.ApiConsumerEndpoint)
        .filter_by(consumer_id=consumer.id, endpoint_id=ep.id)
        .count()
        == 1
    )
    # The key authenticates through the consumer + grant path.
    assert api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session).id == ep.id
    # Implicit consumer is hidden from the central list, but its key shows on the endpoint.
    assert api_consumers.list_consumers(current_user=USER1, db=db_session) == []
    assert len(flow_api.list_keys(ep.id, current_user=USER1, db=db_session)) == 1


def test_per_flow_keys_share_one_implicit_consumer(db_session):
    ep = _endpoint(db_session, slug="sales")
    k1 = flow_api.create_key(ep.id, ApiKeyCreate(name="a"), current_user=USER1, db=db_session)
    k2 = flow_api.create_key(ep.id, ApiKeyCreate(name="b"), current_user=USER1, db=db_session)
    assert k1.consumer_id == k2.consumer_id  # same implicit consumer, not one per key


def test_endpoint_consumers_excludes_implicit(db_session):
    ep = _endpoint(db_session, slug="sales")
    flow_api.create_key(ep.id, ApiKeyCreate(name="k"), current_user=USER1, db=db_session)  # implicit consumer
    c = api_consumers.create_consumer(ApiConsumerCreate(name="prod"), current_user=USER1, db=db_session)
    api_consumers.grant_endpoint(c.id, ApiConsumerGrant(endpoint_id=ep.id), current_user=USER1, db=db_session)

    listed = flow_api.list_endpoint_consumers(ep.id, current_user=USER1, db=db_session)
    assert [c.name for c in listed] == ["prod"]  # implicit excluded


# Migration 018 backfill


def _load_migration_018():
    path = pathlib.Path(flowfile_core.__file__).parent / "alembic" / "versions" / "018_api_consumers.py"
    spec = importlib.util.spec_from_file_location("migration_018", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_018_backfills_implicit_consumer(db_session, monkeypatch):
    """Pre-existing keys (consumer_id NULL) get one shared implicit consumer + grant + link."""
    ep = _endpoint(db_session, slug="sales", owner_id=7)
    for _ in range(2):
        _raw, key_hash, prefix = api_key_mod.generate_api_key()
        db_session.add(
            db_models.FlowApiKey(endpoint_id=ep.id, owner_id=7, name="k", key_hash=key_hash, key_prefix=prefix)
        )
    db_session.commit()

    # Run the migration's backfill against this session's connection (same transaction).
    monkeypatch.setattr(alembic.op, "get_bind", lambda: db_session.connection())
    _load_migration_018()._backfill_consumers()
    db_session.expire_all()

    keys = db_session.query(db_models.FlowApiKey).all()
    assert all(k.consumer_id is not None for k in keys)
    consumer_ids = {k.consumer_id for k in keys}
    assert len(consumer_ids) == 1  # both keys share one implicit consumer

    consumer = db_session.get(db_models.ApiConsumer, consumer_ids.pop())
    assert consumer.is_implicit is True and consumer.owner_id == 7 and consumer.name == "endpoint:sales"
    assert db_session.query(db_models.ApiConsumerEndpoint).filter_by(endpoint_id=ep.id).count() == 1
