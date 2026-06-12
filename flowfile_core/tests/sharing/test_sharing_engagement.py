"""Favorites/follows honor the private-by-default catalog boundary.

Favoriting/following is an engagement action, but it returned an enriched DTO
for any resource id — letting a non-owner read another user's private flow/table
metadata (name, paths, schema, SQL, plan) by favoriting it. Adds must require
``use`` on the target; lists must filter to accessible resources.
"""

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def alice_catalog(users, resource_factory):
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="EngPrivate", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="eng_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    flow_id = resource_factory(
        db_models.FlowRegistration,
        name="alice_eng_flow",
        flow_path="/tmp/alice_eng_flow.flowfile",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
    )
    table_id = resource_factory(
        db_models.CatalogTable, name="alice_eng_table", namespace_id=schema_id, owner_id=users["alice"].id
    )
    return {"schema": schema_id, "flow": flow_id, "table": table_id}


@pytest.fixture
def team(users, group_factory):
    return group_factory("eng-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


# ---------- favorites ----------


def test_favorite_private_flow_denied(client_for, alice_catalog):
    bob = client_for("bob")
    assert bob.post(f"/catalog/flows/{alice_catalog['flow']}/favorite").status_code == 403
    # And it leaks nothing through the list.
    assert alice_catalog["flow"] not in [f["id"] for f in bob.get("/catalog/favorites").json()]


def test_favorite_after_grant_allowed(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("flow", alice_catalog["flow"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.post(f"/catalog/flows/{alice_catalog['flow']}/favorite").status_code == 201
    listed = bob.get("/catalog/favorites").json()
    fav = next((f for f in listed if f["id"] == alice_catalog["flow"]), None)
    assert fav is not None
    assert fav["access"]["access_level"] == "use"


def test_list_favorites_filters_inaccessible_rows(users, client_for, alice_catalog):
    """A favorite row that predates a revoked grant must not leak the resource."""
    with get_db_context() as db:
        db.query(db_models.FlowFavorite).filter_by(
            user_id=users["bob"].id, registration_id=alice_catalog["flow"]
        ).delete()
        db.add(db_models.FlowFavorite(user_id=users["bob"].id, registration_id=alice_catalog["flow"]))
        db.commit()
    try:
        bob = client_for("bob")
        assert alice_catalog["flow"] not in [f["id"] for f in bob.get("/catalog/favorites").json()]
    finally:
        with get_db_context() as db:
            db.query(db_models.FlowFavorite).filter_by(
                user_id=users["bob"].id, registration_id=alice_catalog["flow"]
            ).delete()
            db.commit()


# ---------- follows ----------


def test_follow_private_flow_denied(client_for, alice_catalog):
    bob = client_for("bob")
    assert bob.post(f"/catalog/flows/{alice_catalog['flow']}/follow").status_code == 403
    assert alice_catalog["flow"] not in [f["id"] for f in bob.get("/catalog/following").json()]


def test_follow_after_grant_allowed(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("flow", alice_catalog["flow"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.post(f"/catalog/flows/{alice_catalog['flow']}/follow").status_code == 201
    assert alice_catalog["flow"] in [f["id"] for f in bob.get("/catalog/following").json()]


def test_list_following_filters_inaccessible_rows(users, client_for, alice_catalog):
    with get_db_context() as db:
        db.query(db_models.FlowFollow).filter_by(
            user_id=users["bob"].id, registration_id=alice_catalog["flow"]
        ).delete()
        db.add(db_models.FlowFollow(user_id=users["bob"].id, registration_id=alice_catalog["flow"]))
        db.commit()
    try:
        bob = client_for("bob")
        assert alice_catalog["flow"] not in [f["id"] for f in bob.get("/catalog/following").json()]
    finally:
        with get_db_context() as db:
            db.query(db_models.FlowFollow).filter_by(
                user_id=users["bob"].id, registration_id=alice_catalog["flow"]
            ).delete()
            db.commit()


# ---------- table favorites ----------


def test_table_favorite_private_denied(client_for, alice_catalog):
    bob = client_for("bob")
    assert bob.post(f"/catalog/tables/{alice_catalog['table']}/favorite").status_code == 403
    assert alice_catalog["table"] not in [t["id"] for t in bob.get("/catalog/table-favorites").json()]


def test_table_favorite_after_grant_allowed(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("catalog_table", alice_catalog["table"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.post(f"/catalog/tables/{alice_catalog['table']}/favorite").status_code == 201
    listed = bob.get("/catalog/table-favorites").json()
    fav = next((t for t in listed if t["id"] == alice_catalog["table"]), None)
    assert fav is not None
    assert fav["access"]["access_level"] == "use"


def test_list_table_favorites_filters_inaccessible_rows(users, client_for, alice_catalog):
    with get_db_context() as db:
        db.query(db_models.TableFavorite).filter_by(
            user_id=users["bob"].id, table_id=alice_catalog["table"]
        ).delete()
        db.add(db_models.TableFavorite(user_id=users["bob"].id, table_id=alice_catalog["table"]))
        db.commit()
    try:
        bob = client_for("bob")
        assert alice_catalog["table"] not in [t["id"] for t in bob.get("/catalog/table-favorites").json()]
    finally:
        with get_db_context() as db:
            db.query(db_models.TableFavorite).filter_by(
                user_id=users["bob"].id, table_id=alice_catalog["table"]
            ).delete()
            db.commit()


# ---------- owner + admin still work ----------


def test_owner_can_favorite_own_flow(users, client_for, alice_catalog):
    alice = client_for("alice")
    assert alice.post(f"/catalog/flows/{alice_catalog['flow']}/favorite").status_code == 201
    assert alice_catalog["flow"] in [f["id"] for f in alice.get("/catalog/favorites").json()]


def test_admin_can_favorite_any_flow(users, client_for, alice_catalog):
    admin = client_for("admin")
    assert admin.post(f"/catalog/flows/{alice_catalog['flow']}/favorite").status_code == 201
