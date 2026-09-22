"""Manage-gating for the catalog change-tracking routes in docker mode."""

import polars as pl
import pytest

from flowfile_core.database import models as db_models


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
