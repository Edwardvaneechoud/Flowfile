"""Manage-gating for the catalog change-tracking routes in docker mode."""

import polars as pl
import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is outside the group."""
    return group_factory("cdc-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_table(users, resource_factory, tmp_path):
    """A real Delta table owned by alice, in her private schema."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="AliceCdcCat", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="alice_cdc_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    path = tmp_path / "alice_cdc_table"
    pl.DataFrame({"a": [1, 2]}).write_delta(str(path))
    pl.DataFrame({"a": [3]}).write_delta(str(path), mode="append")
    return resource_factory(
        db_models.CatalogTable,
        name="alice_cdc_table",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
        file_path=str(path),
        storage_format="delta",
    )


def test_read_requires_access(users, client_for, alice_table, team, grant_factory):
    bob = client_for("bob")
    assert bob.get(f"/catalog/tables/{alice_table}/cdc").status_code == 403
    grant_factory("catalog_table", alice_table, team, granted_by=users["alice"].id)
    assert bob.get(f"/catalog/tables/{alice_table}/cdc").status_code == 200


def test_enable_requires_manage(users, client_for, alice_table, team, grant_factory):
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.post(f"/catalog/tables/{alice_table}/cdc/enable").status_code == 403
    # The owner may enable it.
    alice = client_for("alice")
    assert alice.post(f"/catalog/tables/{alice_table}/cdc/enable").status_code == 200


def test_manage_grantee_may_enable(users, client_for, alice_table, team, grant_factory):
    grant_factory("catalog_table", alice_table, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    response = bob.post(f"/catalog/tables/{alice_table}/cdc/enable")
    assert response.status_code == 200, response.text
    assert response.json()["cdc_enabled"] is True


def test_cursor_owner_may_reset_without_manage(users, client_for, alice_table, team, grant_factory, resource_factory):
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    resource_factory(
        db_models.CatalogCdcCursor,
        table_id=alice_table,
        consumer_key="name:bobs_cursor",
        owner_id=users["bob"].id,
        last_version=0,
    )
    bob = client_for("bob")
    response = bob.post(
        f"/catalog/tables/{alice_table}/cdc/cursors/reset",
        json={"consumer_key": "name:bobs_cursor", "to": 1},
    )
    assert response.status_code == 200, response.text
    assert response.json()["last_version"] == 1


def test_stranger_may_not_touch_a_cursor(users, client_for, alice_table, resource_factory):
    resource_factory(
        db_models.CatalogCdcCursor,
        table_id=alice_table,
        consumer_key="name:alices_cursor",
        owner_id=users["alice"].id,
        last_version=0,
    )
    carol = client_for("carol")
    assert (
        carol.post(
            f"/catalog/tables/{alice_table}/cdc/cursors/reset",
            json={"consumer_key": "name:alices_cursor", "to": 1},
        ).status_code
        == 403
    )
    assert carol.delete(f"/catalog/tables/{alice_table}/cdc/cursors/name:alices_cursor").status_code == 403


def test_use_grantee_may_not_delete_another_users_cursor(
    users, client_for, alice_table, team, grant_factory, resource_factory
):
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    resource_factory(
        db_models.CatalogCdcCursor,
        table_id=alice_table,
        consumer_key="name:alices_cursor",
        owner_id=users["alice"].id,
        last_version=0,
    )
    bob = client_for("bob")
    assert bob.delete(f"/catalog/tables/{alice_table}/cdc/cursors/name:alices_cursor").status_code == 403


def test_a_commit_by_another_reader_leaves_the_cursor_its_owner(
    users, client_for, alice_table, team, grant_factory, resource_factory
):
    """A reader's run may advance a shared cursor; it does not become the one who may reset or delete it."""
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    resource_factory(
        db_models.CatalogCdcCursor,
        table_id=alice_table,
        consumer_key="name:alices_cursor",
        owner_id=users["alice"].id,
        last_version=0,
    )
    bob = client_for("bob")

    committed = bob.post(
        f"/catalog/tables/{alice_table}/cdc/cursors", json={"consumer_key": "name:alices_cursor", "last_version": 1}
    )

    assert committed.status_code == 200, committed.text
    assert committed.json()["last_version"] == 1
    assert committed.json()["owner_id"] == users["alice"].id
    assert bob.delete(f"/catalog/tables/{alice_table}/cdc/cursors/name:alices_cursor").status_code == 403


def test_a_commit_that_creates_a_cursor_makes_the_caller_its_owner(users, client_for, alice_table, team, grant_factory):
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")

    committed = bob.post(
        f"/catalog/tables/{alice_table}/cdc/cursors", json={"consumer_key": "name:bobs_cursor", "last_version": 1}
    )

    assert committed.status_code == 200, committed.text
    assert committed.json()["owner_id"] == users["bob"].id
    assert bob.delete(f"/catalog/tables/{alice_table}/cdc/cursors/name:bobs_cursor").status_code == 204


@pytest.fixture
def alice_flow_cursor(users, resource_factory, alice_table) -> tuple[int, str]:
    """A flow alice registered in a private schema, and its reader's cursor on her table: ``(flow id, key)``."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="AliceFlowCat", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="alice_flow_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    flow_uuid = "0b7d1c1e-5f43-4a55-9a0e-1c7a51f0c0de"
    flow_id = resource_factory(
        db_models.FlowRegistration,
        name="alice_cdc_flow",
        flow_path="/tmp/alice_cdc_flow.flowfile",
        flow_uuid=flow_uuid,
        namespace_id=schema_id,
        owner_id=users["alice"].id,
    )
    key = f"flow:{flow_uuid}:node:3"
    resource_factory(
        db_models.CatalogCdcCursor, table_id=alice_table, consumer_key=key, owner_id=users["alice"].id, last_version=0
    )
    return flow_id, key


def _position(table_id: int, key: str) -> int:
    with get_db_context() as db:
        return db.query(db_models.CatalogCdcCursor).filter_by(table_id=table_id, consumer_key=key).one().last_version


def test_a_reader_of_the_table_may_not_advance_the_cursor_of_a_flow_they_cannot_use(
    users, client_for, alice_table, alice_flow_cursor, team, grant_factory
):
    """An advanced cursor makes the flow's next run skip the changes in between, so it takes the flow, not the table."""
    _, key = alice_flow_cursor
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")

    refused = bob.post(f"/catalog/tables/{alice_table}/cdc/cursors", json={"consumer_key": key, "last_version": 1})

    assert refused.status_code == 403, refused.text
    assert _position(alice_table, key) == 0


def test_whoever_may_use_a_flow_advances_its_cursor(
    users, client_for, alice_table, alice_flow_cursor, team, grant_factory
):
    """A flow has one cursor whoever runs it: another person's run advances it and leaves it its owner."""
    flow_id, key = alice_flow_cursor
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    grant_factory("flow", flow_id, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")

    committed = bob.post(f"/catalog/tables/{alice_table}/cdc/cursors", json={"consumer_key": key, "last_version": 1})

    assert committed.status_code == 200, committed.text
    assert (committed.json()["last_version"], committed.json()["owner_id"]) == (1, users["alice"].id)


def test_the_cursor_of_a_flow_this_catalog_does_not_hold_is_advanced_by_its_owner_or_a_manager_of_the_table(
    users, client_for, alice_table, group_factory, grant_factory
):
    readers = group_factory("cdc-readers", users["admin"].id, {users["bob"].id: "member", users["carol"].id: "member"})
    grant_factory("catalog_table", alice_table, readers, permission="use", granted_by=users["alice"].id)
    key = "flow:5e1f3c52-6d0b-4c57-8d0a-3a1b7b6f2d11:node:1"
    route = f"/catalog/tables/{alice_table}/cdc/cursors"
    bob, carol, alice = client_for("bob"), client_for("carol"), client_for("alice")

    assert bob.post(route, json={"consumer_key": key, "last_version": 0}).json()["owner_id"] == users["bob"].id
    assert carol.post(route, json={"consumer_key": key, "last_version": 1}).status_code == 403
    assert _position(alice_table, key) == 0
    assert bob.post(route, json={"consumer_key": key, "last_version": 1}).status_code == 200
    assert alice.post(route, json={"consumer_key": key, "last_version": 2}).json()["owner_id"] == users["bob"].id
    assert bob.delete(f"{route}/{key}").status_code == 204


def test_manage_grantee_may_delete_any_cursor(
    users, client_for, alice_table, team, grant_factory, resource_factory
):
    grant_factory("catalog_table", alice_table, team, permission="manage", granted_by=users["alice"].id)
    resource_factory(
        db_models.CatalogCdcCursor,
        table_id=alice_table,
        consumer_key="name:alices_cursor",
        owner_id=users["alice"].id,
        last_version=0,
    )
    bob = client_for("bob")
    assert bob.delete(f"/catalog/tables/{alice_table}/cdc/cursors/name:alices_cursor").status_code == 204


def test_admin_bypasses(users, client_for, alice_table):
    admin = client_for("admin")
    assert admin.get(f"/catalog/tables/{alice_table}/cdc").status_code == 200
    assert admin.post(f"/catalog/tables/{alice_table}/cdc/enable").status_code == 200
