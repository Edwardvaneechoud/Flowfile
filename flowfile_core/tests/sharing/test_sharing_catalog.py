"""Catalog private-by-default + sharing: visibility filtering, by-id guards, manage checks."""

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is outside the group."""
    return group_factory("cat-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_catalog(users, resource_factory):
    """A private namespace tree owned by alice with one flow and one table."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="AlicePrivate", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="alice_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    flow_id = resource_factory(
        db_models.FlowRegistration,
        name="alice_flow",
        flow_path="/tmp/alice_flow.flowfile",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
    )
    table_id = resource_factory(
        db_models.CatalogTable, name="alice_table", namespace_id=schema_id, owner_id=users["alice"].id
    )
    return {"catalog": catalog_id, "schema": schema_id, "flow": flow_id, "table": table_id}


def _share(client, resource_type, resource_id, group_id, permission="use"):
    response = client.post(
        "/shares",
        json={
            "resource_type": resource_type,
            "resource_id": resource_id,
            "group_id": group_id,
            "permission": permission,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_private_by_default_list_and_by_id(users, client_for, alice_catalog):
    bob = client_for("bob")
    # Lists exclude alice's private resources.
    flow_names = [f["name"] for f in bob.get("/catalog/flows").json()]
    assert "alice_flow" not in flow_names
    table_names = [t["name"] for t in bob.get("/catalog/tables").json()]
    assert "alice_table" not in table_names
    ns_names = [n["name"] for n in bob.get("/catalog/namespaces").json()]
    assert "AlicePrivate" not in ns_names

    # By-id reads are forbidden (403), not silently served.
    assert bob.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 403
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 403

    # The owner still sees everything.
    alice = client_for("alice")
    assert alice.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 200
    assert "alice_flow" in [f["name"] for f in alice.get("/catalog/flows").json()]


def test_admin_sees_all(users, client_for, alice_catalog):
    admin = client_for("admin")
    assert admin.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 200
    assert admin.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 200


def test_direct_table_grant(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("catalog_table", alice_catalog["table"], team, granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 200
    table_names = [t["name"] for t in bob.get("/catalog/tables").json()]
    assert "alice_table" in table_names
    # The flow in the same namespace is still private (table grant doesn't leak the flow).
    assert bob.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 403


def test_namespace_inheritance(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, granted_by=users["alice"].id)
    bob = client_for("bob")
    # The granted namespace + its children's flows/tables become visible.
    assert bob.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 200
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 200
    assert "alice_flow" in [f["name"] for f in bob.get("/catalog/flows").json()]
    assert "AlicePrivate" in [n["name"] for n in bob.get("/catalog/namespaces").json()]


def test_manage_checks_on_mutations(users, client_for, alice_catalog, team, grant_factory):
    bob = client_for("bob")
    # use grant: read yes, mutate no.
    grant_factory("flow", alice_catalog["flow"], team, permission="use", granted_by=users["alice"].id)
    grant_factory("catalog_table", alice_catalog["table"], team, permission="use", granted_by=users["alice"].id)
    assert bob.put(f"/catalog/flows/{alice_catalog['flow']}", json={"description": "x"}).status_code == 403
    assert bob.delete(f"/catalog/tables/{alice_catalog['table']}").status_code == 403


def test_manage_grant_allows_mutation(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("flow", alice_catalog["flow"], team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    response = bob.put(f"/catalog/flows/{alice_catalog['flow']}", json={"description": "managed by bob"})
    assert response.status_code == 200, response.text
    assert response.json()["description"] == "managed by bob"


def test_public_namespace_visible_to_all(users, client_for):
    # The seeded General catalog is a public container.
    bob = client_for("bob")
    ns_names = [n["name"] for n in bob.get("/catalog/namespaces").json()]
    assert "General" in ns_names


def test_namespace_tree_filtering(users, client_for, alice_catalog, team, grant_factory):
    bob = client_for("bob")
    tree = bob.get("/catalog/namespaces/tree").json()
    names = {n["name"] for n in tree}
    # Alice's private catalog is absent; the public General container is present.
    assert "AlicePrivate" not in names
    assert "General" in names

    # After a namespace grant it appears with alice's flow nested inside.
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, granted_by=users["alice"].id)
    tree = bob.get("/catalog/namespaces/tree").json()
    alice_node = next((n for n in tree if n["name"] == "AlicePrivate"), None)
    assert alice_node is not None
    nested_flows = [f["name"] for child in alice_node["children"] for f in child["flows"]]
    assert "alice_flow" in nested_flows


def test_revoked_grant_loses_access(users, client_for, alice_catalog, team, client_for_admin=None):
    alice = client_for("alice")
    share = _share(alice, "catalog_table", alice_catalog["table"], team)
    bob = client_for("bob")
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 200
    assert alice.delete(f"/shares/{share['id']}").status_code == 204
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 403


def test_flow_delete_cleans_grants(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("flow", alice_catalog["flow"], team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.delete(f"/catalog/flows/{alice_catalog['flow']}").status_code == 204
    with get_db_context() as db:
        grants = (
            db.query(db_models.ResourceGrant)
            .filter_by(resource_type="flow", resource_id=alice_catalog["flow"])
            .count()
        )
    assert grants == 0


def test_create_in_invisible_namespace_rejected(users, client_for, alice_catalog):
    bob = client_for("bob")
    response = bob.post(
        "/catalog/namespaces",
        json={"name": "bob_child", "parent_id": alice_catalog["catalog"]},
    )
    assert response.status_code == 403


def test_create_in_public_namespace_allowed(users, client_for):
    bob = client_for("bob")
    with get_db_context() as db:
        general_id = (
            db.query(db_models.CatalogNamespace).filter_by(name="General", parent_id=None).first().id
        )
    response = bob.post("/catalog/namespaces", json={"name": "bob_public_child", "parent_id": general_id})
    assert response.status_code == 201, response.text
    # cleanup
    with get_db_context() as db:
        row = db.query(db_models.CatalogNamespace).filter_by(name="bob_public_child").first()
        if row:
            db.delete(row)
            db.commit()
