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


def test_public_namespace_visible_to_all(users, client_for, resource_factory):
    # A public namespace is a container visible to everyone (self-contained: the
    # seeded 'General' namespace may have been wiped by a prior test suite).
    resource_factory(
        db_models.CatalogNamespace,
        name="PublicShared",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    bob = client_for("bob")
    ns_names = [n["name"] for n in bob.get("/catalog/namespaces").json()]
    assert "PublicShared" in ns_names


def test_namespace_tree_filtering(users, client_for, alice_catalog, team, grant_factory, resource_factory):
    public_id = resource_factory(
        db_models.CatalogNamespace,
        name="PublicTree",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    bob = client_for("bob")
    tree = bob.get("/catalog/namespaces/tree").json()
    names = {n["name"] for n in tree}
    # Alice's private catalog is absent; the public container is present.
    assert "AlicePrivate" not in names
    assert "PublicTree" in names

    # After a namespace grant it appears with alice's flow nested inside.
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, granted_by=users["alice"].id)
    tree = bob.get("/catalog/namespaces/tree").json()
    alice_node = next((n for n in tree if n["name"] == "AlicePrivate"), None)
    assert alice_node is not None
    nested_flows = [f["name"] for child in alice_node["children"] for f in child["flows"]]
    assert "alice_flow" in nested_flows
    assert public_id  # silence unused


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
            db.query(db_models.ResourceGrant).filter_by(resource_type="flow", resource_id=alice_catalog["flow"]).count()
        )
    assert grants == 0


def test_create_in_invisible_namespace_rejected(users, client_for, alice_catalog):
    bob = client_for("bob")
    response = bob.post(
        "/catalog/namespaces",
        json={"name": "bob_child", "parent_id": alice_catalog["catalog"]},
    )
    assert response.status_code == 403


# ---------- access field on DTOs ----------


def test_access_field_on_table_and_flow(users, client_for, alice_catalog, team, grant_factory):
    grant_factory("catalog_table", alice_catalog["table"], team, permission="use", granted_by=users["alice"].id)
    grant_factory("flow", alice_catalog["flow"], team, permission="manage", granted_by=users["alice"].id)

    alice = client_for("alice")
    own_table = alice.get(f"/catalog/tables/{alice_catalog['table']}").json()
    assert own_table["access"] == {"is_owner": True, "access_level": "owner", "shared_by": None}

    bob = client_for("bob")
    shared_table = bob.get(f"/catalog/tables/{alice_catalog['table']}").json()
    assert shared_table["access"] == {"is_owner": False, "access_level": "use", "shared_by": "share_alice"}
    shared_flow = bob.get(f"/catalog/flows/{alice_catalog['flow']}").json()
    assert shared_flow["access"]["access_level"] == "manage"
    assert shared_flow["access"]["is_owner"] is False


# ---------- visualizations ----------


@pytest.fixture
def alice_viz(users, resource_factory, alice_catalog):
    return resource_factory(
        db_models.CatalogVisualization,
        name="alice_viz",
        spec_json="[]",
        source_type="table",
        catalog_table_id=alice_catalog["table"],
        namespace_id=alice_catalog["schema"],
        created_by=users["alice"].id,
    )


def test_visualization_private_then_shared(users, client_for, alice_viz, team, grant_factory):
    bob = client_for("bob")
    assert alice_viz not in [v["id"] for v in bob.get("/catalog/visualizations").json()]
    assert bob.get(f"/catalog/visualizations/{alice_viz}").status_code == 403

    grant_factory("visualization", alice_viz, team, permission="use", granted_by=users["alice"].id)
    listed = bob.get("/catalog/visualizations").json()
    shared = next((v for v in listed if v["id"] == alice_viz), None)
    assert shared is not None
    assert shared["access"]["access_level"] == "use"
    by_id = bob.get(f"/catalog/visualizations/{alice_viz}")
    assert by_id.status_code == 200
    # use grant cannot delete
    assert bob.delete(f"/catalog/visualizations/{alice_viz}").status_code == 403


def test_visualization_manage_grant_can_delete_and_cleans_grants(users, client_for, alice_viz, team, grant_factory):
    grant_factory("visualization", alice_viz, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.delete(f"/catalog/visualizations/{alice_viz}").status_code == 204
    with get_db_context() as db:
        assert (
            db.query(db_models.ResourceGrant).filter_by(resource_type="visualization", resource_id=alice_viz).count()
            == 0
        )


# ---------- dashboards ----------


@pytest.fixture
def alice_dashboard(users, resource_factory, alice_catalog):
    return resource_factory(
        db_models.CatalogDashboard,
        name="alice_dashboard",
        layout_json="{}",
        layout_version=1,
        namespace_id=alice_catalog["schema"],
        created_by=users["alice"].id,
    )


def test_dashboard_private_then_shared(users, client_for, alice_dashboard, team, grant_factory):
    bob = client_for("bob")
    assert alice_dashboard not in [d["id"] for d in bob.get("/catalog/dashboards").json()]
    assert bob.get(f"/catalog/dashboards/{alice_dashboard}").status_code == 403

    grant_factory("dashboard", alice_dashboard, team, permission="use", granted_by=users["alice"].id)
    listed = bob.get("/catalog/dashboards").json()
    shared = next((d for d in listed if d["id"] == alice_dashboard), None)
    assert shared is not None and shared["access"]["access_level"] == "use"
    assert bob.get(f"/catalog/dashboards/{alice_dashboard}").status_code == 200
    assert bob.delete(f"/catalog/dashboards/{alice_dashboard}").status_code == 403


# ---------- models (artifacts) + namespace cascade ----------


@pytest.fixture
def alice_artifact(users, resource_factory, alice_catalog):
    return resource_factory(
        db_models.GlobalArtifact,
        name="alice_model",
        version=1,
        status="active",
        serialization_format="joblib",
        namespace_id=alice_catalog["schema"],
        owner_id=users["alice"].id,
        source_registration_id=alice_catalog["flow"],
    )


def test_artifact_shows_in_tree_when_granted(users, client_for, alice_artifact, team, grant_factory):
    bob = client_for("bob")

    def _artifact_names(tree):
        names = []
        for node in tree:
            names += [a["name"] for a in node.get("artifacts", [])]
            names += _artifact_names(node.get("children", []))
        return names

    assert "alice_model" not in _artifact_names(bob.get("/catalog/namespaces/tree").json())

    grant_factory("global_artifact", alice_artifact, team, permission="use", granted_by=users["alice"].id)
    assert "alice_model" in _artifact_names(bob.get("/catalog/namespaces/tree").json())


def test_namespace_grant_cascades_to_content(
    users, client_for, alice_catalog, alice_viz, alice_dashboard, alice_artifact, team, grant_factory
):
    # One namespace grant reveals tables, flows, visualizations, dashboards, and models inside it.
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.get(f"/catalog/tables/{alice_catalog['table']}").status_code == 200
    assert bob.get(f"/catalog/flows/{alice_catalog['flow']}").status_code == 200
    assert bob.get(f"/catalog/visualizations/{alice_viz}").status_code == 200
    assert bob.get(f"/catalog/dashboards/{alice_dashboard}").status_code == 200
    assert alice_dashboard in [d["id"] for d in bob.get("/catalog/dashboards").json()]


def test_query_virtual_table_loses_access_on_revoke(users, resource_factory, alice_catalog, team, grant_factory):
    """A saved query virtual table must re-check the creator's access to its source
    tables at resolve time — revoking the grant immediately blocks it."""
    from flowfile_core.catalog import CatalogService, NotAuthorizedError, SQLAlchemyCatalogRepository

    # alice owns a source table; bob builds a query virtual table on top of it.
    src_id = resource_factory(
        db_models.CatalogTable, name="shared_src_tbl", namespace_id=alice_catalog["schema"], owner_id=users["alice"].id
    )
    vt_id = resource_factory(
        db_models.CatalogTable,
        name="bob_query_vt",
        namespace_id=alice_catalog["schema"],
        owner_id=users["bob"].id,
        table_type="virtual",
        sql_query="SELECT * FROM shared_src_tbl",
    )
    grant_factory("catalog_table", src_id, team, permission="use", granted_by=users["alice"].id)

    def _resolve():
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            return svc.resolve_query_virtual_table(vt_id, user_id=users["bob"].id)

    # With the grant the auth check passes (resolution then fails on missing data,
    # not on authorization).
    with pytest.raises(Exception) as exc_info:
        _resolve()
    assert not isinstance(exc_info.value, NotAuthorizedError)

    # Revoke the grant on the source table → the virtual table can no longer read it.
    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(resource_type="catalog_table", resource_id=src_id).delete()
        db.commit()
    with pytest.raises(NotAuthorizedError):
        _resolve()


def test_use_grant_does_not_allow_namespace_writes(users, client_for, alice_catalog, team, grant_factory):
    """A use-level namespace grant is read-only: creating inside it is denied;
    upgrading the grant to manage allows it."""
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    denied = bob.post("/catalog/namespaces", json={"name": "bob_sub", "parent_id": alice_catalog["catalog"]})
    assert denied.status_code == 403, denied.text

    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_namespace", resource_id=alice_catalog["catalog"]
        ).update({"permission": "manage"})
        db.commit()
    allowed = bob.post("/catalog/namespaces", json={"name": "bob_sub", "parent_id": alice_catalog["catalog"]})
    assert allowed.status_code == 201, allowed.text
    with get_db_context() as db:
        row = db.query(db_models.CatalogNamespace).filter_by(name="bob_sub").first()
        if row:
            db.delete(row)
            db.commit()


def test_create_visualization_requires_table_access(users, client_for, alice_catalog):
    bob = client_for("bob")
    response = bob.post(
        "/catalog/visualizations",
        json={"name": "bob_viz", "spec": [{}], "source_type": "table", "catalog_table_id": alice_catalog["table"]},
    )
    assert response.status_code == 403, response.text


def test_create_dashboard_requires_writable_namespace(users, client_for, alice_catalog):
    bob = client_for("bob")
    response = bob.post("/catalog/dashboards", json={"name": "bob_dash", "namespace_id": alice_catalog["schema"]})
    assert response.status_code == 403, response.text


def test_create_in_public_namespace_allowed(users, client_for, resource_factory):
    public_id = resource_factory(
        db_models.CatalogNamespace,
        name="PublicCreate",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    bob = client_for("bob")
    response = bob.post("/catalog/namespaces", json={"name": "bob_public_child", "parent_id": public_id})
    assert response.status_code == 201, response.text
    # cleanup
    with get_db_context() as db:
        row = db.query(db_models.CatalogNamespace).filter_by(name="bob_public_child").first()
        if row:
            db.delete(row)
            db.commit()
