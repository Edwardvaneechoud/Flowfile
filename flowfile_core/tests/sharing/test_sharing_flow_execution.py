"""Flow-execution read path honors the private-by-default catalog boundary.

The REST catalog endpoints enforce access, but a flow's catalog_reader / sql_query
nodes resolve tables at add-time via module-level helpers in ``flow_graph``. These
must authorize the node's (server-stamped) ``user_id`` so a flow can't be used to
read another user's unshared table — the gap behind the original High finding.
"""

import json
from pathlib import Path

import pytest

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph import (
    _accessible_catalog_table_ids,
    _resolve_catalog_sql_tables,
    _resolve_catalog_table_info,
)
from flowfile_core.schemas import input_schema
from shared.storage_config import storage


@pytest.fixture
def alice_table(users, resource_factory):
    """A private namespace owned by alice with a physical table and a virtual table."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="ExecPrivate", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="exec_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    table_id = resource_factory(
        db_models.CatalogTable,
        name="alice_exec_table",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
        file_path="/tmp/alice_exec_table",  # non-existent: resolution stops at the auth gate
    )
    vtable_id = resource_factory(
        db_models.CatalogTable,
        name="alice_virtual_table",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
        table_type="virtual",
    )
    return {"catalog": catalog_id, "schema": schema_id, "table": table_id, "vtable": vtable_id}


@pytest.fixture
def team(users, group_factory):
    return group_factory("exec-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


def _reader(user_id, table_id):
    return input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=table_id, user_id=user_id)


# ---------- single-table reader (_resolve_catalog_table_info) ----------


def test_reader_denies_unshared_table_for_non_owner(users, alice_table):
    """bob, with no grant, cannot resolve alice's table → fail closed (authorized=False)."""
    info = _resolve_catalog_table_info(_reader(users["bob"].id, alice_table["table"]))
    assert info.authorized is False
    assert info.file_path is None
    assert info.serialized_lf is None


def test_reader_allows_owner(users, alice_table):
    info = _resolve_catalog_table_info(_reader(users["alice"].id, alice_table["table"]))
    assert info.authorized is True
    assert info.file_path == "/tmp/alice_exec_table"


def test_reader_allows_after_grant(users, alice_table, team, grant_factory):
    grant_factory("catalog_table", alice_table["table"], team, permission="use", granted_by=users["alice"].id)
    info = _resolve_catalog_table_info(_reader(users["bob"].id, alice_table["table"]))
    assert info.authorized is True
    assert info.file_path == "/tmp/alice_exec_table"


def test_reader_admin_bypasses(users, alice_table):
    info = _resolve_catalog_table_info(_reader(users["admin"].id, alice_table["table"]))
    assert info.authorized is True


def test_reader_internal_run_unrestricted(users, alice_table):
    """user_id=None (scheduler/CLI/electron) keeps today's unrestricted behavior."""
    info = _resolve_catalog_table_info(_reader(None, alice_table["table"]))
    assert info.authorized is True
    assert info.file_path == "/tmp/alice_exec_table"


def test_reader_denies_nonexistent_table_for_restricted_user(users):
    """A restricted user pointing at a missing table id is denied, never served."""
    info = _resolve_catalog_table_info(_reader(users["bob"].id, 999_999))
    assert info.authorized is False


# ---------- SQL reader table filtering (_resolve_catalog_sql_tables) ----------
# A virtual table appears in ``virtual_tables`` regardless of on-disk state, so it
# cleanly exercises the accessibility filter (without it, it WOULD be registered).


def test_sql_resolver_excludes_unshared_table(users, alice_table):
    resolved = _resolve_catalog_sql_tables(node_id=1, user_id=users["bob"].id)
    assert "alice_virtual_table" not in resolved.virtual_tables


def test_sql_resolver_includes_owned_table(users, alice_table):
    resolved = _resolve_catalog_sql_tables(node_id=1, user_id=users["alice"].id)
    assert "alice_virtual_table" in resolved.virtual_tables


def test_sql_resolver_includes_granted_table(users, alice_table, team, grant_factory):
    grant_factory("catalog_table", alice_table["vtable"], team, permission="use", granted_by=users["alice"].id)
    resolved = _resolve_catalog_sql_tables(node_id=1, user_id=users["bob"].id)
    assert "alice_virtual_table" in resolved.virtual_tables


def test_sql_resolver_internal_run_sees_all(users, alice_table):
    resolved = _resolve_catalog_sql_tables(node_id=1, user_id=None)
    assert "alice_virtual_table" in resolved.virtual_tables


# ---------- accessible-id helper ----------


def test_accessible_ids_none_for_internal_and_admin(users, alice_table):
    with get_db_context() as db:
        assert _accessible_catalog_table_ids(db, None) is None
        assert _accessible_catalog_table_ids(db, users["admin"].id) is None


def test_accessible_ids_set_for_restricted_user(users, alice_table):
    with get_db_context() as db:
        bob_ids = _accessible_catalog_table_ids(db, users["bob"].id)
        assert isinstance(bob_ids, set)
        assert alice_table["table"] not in bob_ids
        alice_ids = _accessible_catalog_table_ids(db, users["alice"].id)
        assert alice_table["table"] in alice_ids


def test_accessible_ids_stale_user_denies_all(users):
    """A user id that no longer resolves must deny everything (no access widening)."""
    with get_db_context() as db:
        assert _accessible_catalog_table_ids(db, 10_000_000) == set()


# ---------- save_sql_query_as_flow drops inaccessible tables ----------


def _save_as_flow(db, user, **kwargs) -> int:
    svc = CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, user))
    return svc.save_sql_query_as_flow(**kwargs)


def _reader_table_ids(flow_path: str) -> list[int]:
    data = json.loads(Path(flow_path).read_text())
    return [n["setting_input"].get("catalog_table_id") for n in data["nodes"] if n["type"] == "catalog_reader"]


def test_save_as_flow_drops_unshared_table(monkeypatch, tmp_path, users, alice_table):
    """A restricted user saving a query over alice's table embeds no reader for it."""
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path)
    with get_db_context() as db:
        bob = db.get(db_models.User, users["bob"].id)
        reg_id = _save_as_flow(
            db,
            bob,
            query="SELECT * FROM alice_exec_table",
            name="bob_steal",
            owner_id=users["bob"].id,
            namespace_id=alice_table["schema"],
            used_tables=["alice_exec_table"],
        )
        flow_path = db.get(db_models.FlowRegistration, reg_id).flow_path

    try:
        # The only referenced table was inaccessible → dropped entirely.
        assert _reader_table_ids(flow_path) == []
    finally:
        with get_db_context() as db:
            row = db.get(db_models.FlowRegistration, reg_id)
            if row is not None:
                db.delete(row)
                db.commit()


def test_save_as_flow_keeps_accessible_table(monkeypatch, tmp_path, users, alice_table, team, grant_factory):
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path)
    grant_factory("catalog_table", alice_table["table"], team, permission="use", granted_by=users["alice"].id)
    with get_db_context() as db:
        bob = db.get(db_models.User, users["bob"].id)
        reg_id = _save_as_flow(
            db,
            bob,
            query="SELECT * FROM alice_exec_table",
            name="bob_shared_query",
            owner_id=users["bob"].id,
            namespace_id=alice_table["schema"],
            used_tables=["alice_exec_table"],
        )
        flow_path = db.get(db_models.FlowRegistration, reg_id).flow_path

    try:
        assert alice_table["table"] in _reader_table_ids(flow_path)
    finally:
        with get_db_context() as db:
            row = db.get(db_models.FlowRegistration, reg_id)
            if row is not None:
                db.delete(row)
                db.commit()


def test_save_as_flow_owner_keeps_table(monkeypatch, tmp_path, users, alice_table):
    """Sanity: the owner's own table is always embedded."""
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path)
    with get_db_context() as db:
        alice = db.get(db_models.User, users["alice"].id)
        reg_id = _save_as_flow(
            db,
            alice,
            query="SELECT * FROM alice_exec_table",
            name="alice_own_query",
            owner_id=users["alice"].id,
            namespace_id=alice_table["schema"],
            used_tables=["alice_exec_table"],
        )
        flow_path = db.get(db_models.FlowRegistration, reg_id).flow_path

    try:
        assert alice_table["table"] in _reader_table_ids(flow_path)
    finally:
        with get_db_context() as db:
            row = db.get(db_models.FlowRegistration, reg_id)
            if row is not None:
                db.delete(row)
                db.commit()
