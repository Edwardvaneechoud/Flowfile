"""Lifecycle & data-integrity tests for the 'host flows as HTTP data APIs' feature.

Covers the P3 review findings:
- #6  ``delete_flow`` cleans up ``flow_api_endpoints`` / ``flow_api_keys`` (the
      orphaned key no longer authenticates and the slug is freed for republish),
- #8  ``sync_api_compatibility`` disables an already-published endpoint when the
      flow loses (or duplicates) its ``api_response`` node,
- #13 the ``enabled`` columns carry a DB ``server_default`` so a DB-level insert
      that omits ``enabled`` still defaults to enabled.
"""

import datetime
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from flowfile_core.auth import api_key as api_key_mod
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database import models as db_models
from flowfile_core.flowfile import api_consumer_manager, catalog_helpers

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    db_models.Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def helpers_db(db_session, monkeypatch):
    """Make ``sync_api_compatibility`` use the in-memory test session.

    ``catalog_helpers`` opens its own session via ``get_db_context``; patch it to
    yield (and never close) the test session so assertions can run afterwards.
    """

    @contextmanager
    def _ctx():
        yield db_session

    monkeypatch.setattr(catalog_helpers, "get_db_context", _ctx)
    return db_session


def _register_flow(db, *, name="api_flow", flow_path="/tmp/api_flow.yaml", is_api_compatible=True):
    reg = db_models.FlowRegistration(
        flow_uuid=str(uuid4()),
        name=name,
        flow_path=flow_path,
        owner_id=1,
        is_api_compatible=is_api_compatible,
    )
    db.add(reg)
    db.commit()
    db.refresh(reg)
    return reg


def _publish_with_key(db, reg, *, slug="sales", enabled=True):
    """Create a FlowApiEndpoint + its implicit consumer + grant + a FlowApiKey.

    Auth resolves key -> consumer -> grant -> endpoint, so the consumer + grant
    (created via the same helper the per-flow 'Create key' button uses) are what
    make the key authenticate. Returns (endpoint, key, raw_token).
    """
    ep = db_models.FlowApiEndpoint(registration_id=reg.id, owner_id=1, slug=slug, enabled=enabled)
    db.add(ep)
    db.commit()
    db.refresh(ep)
    consumer = api_consumer_manager.get_or_create_implicit_consumer(db, ep)
    raw, key_hash, prefix = api_key_mod.generate_api_key()
    key = db_models.FlowApiKey(
        consumer_id=consumer.id, endpoint_id=ep.id, owner_id=1, name="k", key_hash=key_hash, key_prefix=prefix
    )
    db.add(key)
    db.commit()
    db.refresh(key)
    return ep, key, raw


def _fake_flow(flow_path, *, api_response_count):
    """A minimal stand-in for a FlowGraph that ``sync_api_compatibility`` can read."""
    nodes = [SimpleNamespace(node_type="api_response") for _ in range(api_response_count)]
    nodes.append(SimpleNamespace(node_type="manual_input"))
    return SimpleNamespace(flow_settings=SimpleNamespace(path=flow_path, save_location=None), nodes=nodes)


# ---------------------------------------------------------------------------
# #6 — delete_flow cleans up endpoints + keys
# ---------------------------------------------------------------------------


def test_delete_flow_removes_endpoint_and_keys(db_session):
    reg = _register_flow(db_session)
    ep, key, raw = _publish_with_key(db_session, reg, slug="sales")
    reg_id, ep_id, key_id = reg.id, ep.id, key.id

    # The key authenticates before deletion.
    assert api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session).id == ep_id

    SQLAlchemyCatalogRepository(db_session).delete_flow(reg_id)

    # Registration, endpoint and key are all gone (no orphans).
    assert db_session.get(db_models.FlowRegistration, reg_id) is None
    assert db_session.get(db_models.FlowApiEndpoint, ep_id) is None
    assert db_session.get(db_models.FlowApiKey, key_id) is None

    # The orphaned key no longer authenticates — the revocation gap is closed.
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401

    # The slug is freed, so a brand-new flow can claim it (republish unblocked).
    reg2 = _register_flow(db_session, name="api_flow_2", flow_path="/tmp/api_flow_2.yaml")
    db_session.add(db_models.FlowApiEndpoint(registration_id=reg2.id, owner_id=1, slug="sales"))
    db_session.commit()  # must not raise a UNIQUE-constraint violation
    assert db_session.query(db_models.FlowApiEndpoint).filter_by(slug="sales").count() == 1


def test_delete_flow_leaves_other_flows_endpoints_intact(db_session):
    reg_a = _register_flow(db_session, name="a", flow_path="/tmp/a.yaml")
    reg_b = _register_flow(db_session, name="b", flow_path="/tmp/b.yaml")
    ep_a, key_a, _ = _publish_with_key(db_session, reg_a, slug="aaa")
    ep_b, key_b, raw_b = _publish_with_key(db_session, reg_b, slug="bbb")
    ep_a_id, key_a_id, ep_b_id, key_b_id = ep_a.id, key_a.id, ep_b.id, key_b.id

    SQLAlchemyCatalogRepository(db_session).delete_flow(reg_a.id)

    # Flow A's endpoint + key are gone...
    assert db_session.get(db_models.FlowApiEndpoint, ep_a_id) is None
    assert db_session.get(db_models.FlowApiKey, key_a_id) is None
    # ...but flow B's are untouched and B still authenticates.
    assert db_session.get(db_models.FlowApiEndpoint, ep_b_id) is not None
    assert db_session.get(db_models.FlowApiKey, key_b_id) is not None
    assert api_key_mod.verify_api_key(slug="bbb", x_api_key=raw_b, db=db_session).id == ep_b_id


def test_delete_flow_without_endpoint_is_clean(db_session):
    """Deleting a flow that was never published must not error on the new cleanup."""
    reg = _register_flow(db_session, is_api_compatible=False)
    reg_id = reg.id
    SQLAlchemyCatalogRepository(db_session).delete_flow(reg_id)
    assert db_session.get(db_models.FlowRegistration, reg_id) is None


# ---------------------------------------------------------------------------
# #8 — sync_api_compatibility disables an already-published endpoint
# ---------------------------------------------------------------------------


def test_sync_api_compatibility_disables_published_endpoint(helpers_db):
    flow_path = "/tmp/incompat.yaml"
    reg = _register_flow(helpers_db, flow_path=flow_path, is_api_compatible=True)
    ep, key, raw = _publish_with_key(helpers_db, reg, slug="sales", enabled=True)

    # The flow lost its api_response node → no longer API-compatible.
    catalog_helpers.sync_api_compatibility(_fake_flow(flow_path, api_response_count=0))

    helpers_db.refresh(reg)
    helpers_db.refresh(ep)
    assert reg.is_api_compatible is False
    assert ep.enabled is False

    # A still-valid key now gets a clean 403 (disabled endpoint) instead of a 500.
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=helpers_db)
    assert exc.value.status_code == 403


def test_sync_api_compatibility_disables_when_api_node_duplicated(helpers_db):
    """Two api_response nodes is also incompatible (==1 required), so disable too."""
    flow_path = "/tmp/dup.yaml"
    reg = _register_flow(helpers_db, flow_path=flow_path, is_api_compatible=True)
    ep, _key, _raw = _publish_with_key(helpers_db, reg, slug="dup", enabled=True)

    catalog_helpers.sync_api_compatibility(_fake_flow(flow_path, api_response_count=2))

    helpers_db.refresh(reg)
    helpers_db.refresh(ep)
    assert reg.is_api_compatible is False
    assert ep.enabled is False


def test_sync_api_compatibility_keeps_compatible_endpoint_enabled(helpers_db):
    """Positive control: a still-compatible flow leaves its endpoint enabled."""
    flow_path = "/tmp/compat.yaml"
    reg = _register_flow(helpers_db, flow_path=flow_path, is_api_compatible=True)
    ep, _key, raw = _publish_with_key(helpers_db, reg, slug="sales", enabled=True)

    catalog_helpers.sync_api_compatibility(_fake_flow(flow_path, api_response_count=1))

    helpers_db.refresh(reg)
    helpers_db.refresh(ep)
    assert reg.is_api_compatible is True
    assert ep.enabled is True
    # And it still authenticates.
    assert api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=helpers_db).id == ep.id


# ---------------------------------------------------------------------------
# #13 — enabled columns carry a server_default
# ---------------------------------------------------------------------------


def test_enabled_columns_have_server_default(db_session):
    """A DB-level insert that omits ``enabled`` must fall back to the server_default.

    Without ``server_default`` on the model the column DDL is ``NOT NULL`` with no
    DEFAULT, so this raw insert would raise an IntegrityError instead.
    """
    reg = _register_flow(db_session)
    ts = datetime.datetime(2024, 1, 1)

    db_session.execute(
        text(
            "INSERT INTO flow_api_endpoints (registration_id, owner_id, slug, created_at, updated_at) "
            "VALUES (:rid, 1, 'srvdef', :ts, :ts)"
        ),
        {"rid": reg.id, "ts": ts},
    )
    db_session.commit()
    ep = db_session.query(db_models.FlowApiEndpoint).filter_by(slug="srvdef").one()
    assert ep.enabled is True

    db_session.execute(
        text(
            "INSERT INTO flow_api_keys (endpoint_id, owner_id, name, key_hash, key_prefix, created_at) "
            "VALUES (:eid, 1, 'k', 'hash_srvdef', 'ffk_srvdef', :ts)"
        ),
        {"eid": ep.id, "ts": ts},
    )
    db_session.commit()
    key = db_session.query(db_models.FlowApiKey).filter_by(key_hash="hash_srvdef").one()
    assert key.enabled is True
