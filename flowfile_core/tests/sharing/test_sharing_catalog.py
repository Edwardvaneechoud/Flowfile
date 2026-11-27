"""Catalog private-by-default + sharing: visibility filtering, by-id guards, manage checks."""

import json

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


@pytest.fixture
def alice_notebook(users, alice_catalog, resource_factory):
    """A notebook owned by alice, living in her private schema."""
    return resource_factory(
        db_models.CatalogNotebook,
        name="alice_nb",
        namespace_id=alice_catalog["schema"],
        owner_id=users["alice"].id,
    )


def test_notebook_private_by_default(users, client_for, alice_notebook):
    bob = client_for("bob")
    assert "alice_nb" not in [n["name"] for n in bob.get("/catalog/notebooks").json()]
    assert bob.get(f"/catalog/notebooks/{alice_notebook}").status_code == 403
    alice = client_for("alice")
    assert alice.get(f"/catalog/notebooks/{alice_notebook}").status_code == 200
    assert "alice_nb" in [n["name"] for n in alice.get("/catalog/notebooks").json()]


def test_notebook_direct_grant(users, client_for, alice_notebook, team, grant_factory):
    grant_factory("catalog_notebook", alice_notebook, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.get(f"/catalog/notebooks/{alice_notebook}").status_code == 200
    assert "alice_nb" in [n["name"] for n in bob.get("/catalog/notebooks").json()]


def test_notebook_namespace_inheritance(users, client_for, alice_catalog, alice_notebook, team, grant_factory):
    grant_factory("catalog_namespace", alice_catalog["catalog"], team, granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.get(f"/catalog/notebooks/{alice_notebook}").status_code == 200


def test_notebook_use_grant_denies_mutation(users, client_for, alice_notebook, team, grant_factory):
    grant_factory("catalog_notebook", alice_notebook, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.put(f"/catalog/notebooks/{alice_notebook}", json={"description": "x"}).status_code == 403
    assert bob.delete(f"/catalog/notebooks/{alice_notebook}").status_code == 403


def test_notebook_manage_grant_allows_mutation(users, client_for, alice_notebook, team, grant_factory):
    grant_factory("catalog_notebook", alice_notebook, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    resp = bob.put(f"/catalog/notebooks/{alice_notebook}", json={"description": "managed"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["description"] == "managed"


def test_notebook_manage_grant_can_delete_and_cleans_grants(users, client_for, alice_notebook, team, grant_factory):
    grant_factory("catalog_notebook", alice_notebook, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.delete(f"/catalog/notebooks/{alice_notebook}").status_code == 204
    with get_db_context() as db:
        assert (
            db.query(db_models.ResourceGrant)
            .filter_by(resource_type="catalog_notebook", resource_id=alice_notebook)
            .count()
            == 0
        )


def test_create_notebook_in_private_namespace_rejected(users, client_for, alice_catalog):
    bob = client_for("bob")
    response = bob.post("/catalog/notebooks", json={"name": "bob_nb", "namespace_id": alice_catalog["schema"]})
    assert response.status_code == 403, response.text


def test_create_notebook_in_public_namespace_allowed(users, client_for, resource_factory):
    public_id = resource_factory(
        db_models.CatalogNamespace,
        name="PublicNbCreate",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    bob = client_for("bob")
    response = bob.post("/catalog/notebooks", json={"name": "bob_public_nb", "namespace_id": public_id})
    assert response.status_code == 201, response.text
    # cleanup: API-created notebooks aren't tracked by resource_factory, and a leftover
    # row would collide with a reused namespace rowid in a later test.
    with get_db_context() as db:
        row = db.get(db_models.CatalogNotebook, response.json()["id"])
        if row:
            db.delete(row)
            db.commit()


def test_manage_grantee_cannot_move_notebook_into_private_namespace(
    users, client_for, alice_catalog, alice_notebook, team, grant_factory, resource_factory
):
    # bob can manage his own notebook in a public namespace...
    public_id = resource_factory(
        db_models.CatalogNamespace,
        name="PublicNbMove",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    bob_nb = resource_factory(
        db_models.CatalogNotebook,
        name="bob_movable_nb",
        namespace_id=public_id,
        owner_id=users["bob"].id,
    )
    bob = client_for("bob")
    # ...but cannot move it into alice's private namespace.
    response = bob.put(f"/catalog/notebooks/{bob_nb}", json={"namespace_id": alice_catalog["schema"]})
    assert response.status_code == 403, response.text


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


def _tree_nodes(client) -> dict[int, dict]:
    nodes: dict[int, dict] = {}

    def walk(level):
        for node in level:
            nodes[node["id"]] = node
            walk(node["children"])

    walk(client.get("/catalog/namespaces/tree").json())
    return nodes


def test_the_tree_says_who_may_manage_each_namespace(
    users, client_for, alice_catalog, team, grant_factory, resource_factory
):
    """The owner or a manage grant on the namespace itself, as core's changes require; public grants writing only."""
    alice_id = users["alice"].id
    public_id = resource_factory(
        db_models.CatalogNamespace, name="PublicManaged", parent_id=None, level=0, owner_id=alice_id, is_public=True
    )
    public_schema = resource_factory(
        db_models.CatalogNamespace, name="open", parent_id=public_id, level=1, owner_id=alice_id, is_public=True
    )
    carol = client_for("carol")
    seen = _tree_nodes(carol)
    assert (seen[public_id]["can_manage"], seen[public_schema]["can_manage"]) == (False, False)
    assert (seen[public_id]["can_write"], seen[public_schema]["can_write"]) == (True, True)
    assert carol.delete(f"/catalog/namespaces/{public_schema}").status_code == 403
    created = carol.post("/catalog/namespaces", json={"name": "carols", "parent_id": public_id})
    assert created.status_code == 201, "a public catalog stopped being writable"
    assert _tree_nodes(carol)[created.json()["id"]]["can_manage"] is True
    assert carol.delete(f"/catalog/namespaces/{created.json()['id']}").status_code == 204

    owner = _tree_nodes(client_for("alice"))
    ids = (public_id, public_schema, alice_catalog["catalog"], alice_catalog["schema"])
    assert all(owner[i]["can_manage"] and owner[i]["can_write"] for i in ids)
    admin = _tree_nodes(client_for("admin"))[public_schema]
    assert (admin["can_manage"], admin["can_write"]) == (True, True)

    grant_factory("catalog_namespace", alice_catalog["catalog"], team, permission="use", granted_by=alice_id)
    grant_factory("catalog_namespace", alice_catalog["schema"], team, permission="manage", granted_by=alice_id)
    bob = _tree_nodes(client_for("bob"))
    catalog, schema = bob[alice_catalog["catalog"]], bob[alice_catalog["schema"]]
    assert (catalog["can_manage"], schema["can_manage"]) == (False, True)
    assert (catalog["can_write"], schema["can_write"]) == (False, True)


def test_a_breadcrumb_of_a_granted_table_is_neither_writable_nor_managed(
    users, client_for, alice_catalog, team, grant_factory
):
    grant_factory("catalog_table", alice_catalog["table"], team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    seen = _tree_nodes(bob)
    for ns_id in (alice_catalog["catalog"], alice_catalog["schema"]):
        assert (seen[ns_id]["can_write"], seen[ns_id]["can_manage"]) == (False, False)
    refused = bob.post("/catalog/namespaces", json={"name": "bobs", "parent_id": alice_catalog["catalog"]})
    assert refused.status_code == 403, "the tree called writable a namespace core refuses writes to"


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


def test_a_chart_is_made_for_a_dashboard_only_by_who_manages_it(
    users, client_for, alice_catalog, alice_dashboard, team, grant_factory
):
    grant_factory("catalog_table", alice_catalog["table"], team, granted_by=users["alice"].id)
    bob = client_for("bob")
    body = {
        "name": "bobs_chart",
        "spec": [{}],
        "source_type": "table",
        "catalog_table_id": alice_catalog["table"],
        "dashboard_id": alice_dashboard,
    }
    assert bob.post("/catalog/visualizations", json=body).status_code == 403

    grant_factory("dashboard", alice_dashboard, team, permission="manage", granted_by=users["alice"].id)
    created = bob.post("/catalog/visualizations", json=body)
    assert created.status_code == 201, created.text
    assert created.json()["dashboard_id"] == alice_dashboard
    # Whoever manages the dashboard may keep its chart, and the dashboard's delete takes the rest.
    kept = bob.post(f"/catalog/visualizations/{created.json()['id']}/detach")
    assert kept.status_code == 200 and kept.json()["dashboard_id"] is None
    assert bob.delete(f"/catalog/visualizations/{created.json()['id']}").status_code == 204


@pytest.fixture
def alice_dashboard_chart(users, resource_factory, alice_catalog, alice_dashboard):
    """A chart on alice's table that belongs to her dashboard."""
    return resource_factory(
        db_models.CatalogVisualization,
        name="alice_dashboard_chart",
        spec_json="[]",
        source_type="table",
        catalog_table_id=alice_catalog["table"],
        namespace_id=alice_catalog["schema"],
        dashboard_id=alice_dashboard,
        created_by=users["alice"].id,
    )


def test_a_chart_names_its_dashboard_only_to_who_may_read_the_dashboard(
    users, client_for, alice_catalog, alice_dashboard, alice_dashboard_chart, team, grant_factory
):
    def as_seen_by(client) -> list[dict]:
        """The chart from each route that answers it: the library, its table's charts and the chart itself."""
        table_charts = client.get(f"/catalog/tables/{alice_catalog['table']}/visualizations").json()
        return [
            next(v for v in client.get("/catalog/visualizations").json() if v["id"] == alice_dashboard_chart),
            next(v for v in table_charts if v["id"] == alice_dashboard_chart),
            client.get(f"/catalog/visualizations/{alice_dashboard_chart}").json(),
        ]

    # Reading the table shows bob the chart and that a dashboard owns it, never which dashboard by name.
    grant_factory("catalog_table", alice_catalog["table"], team, granted_by=users["alice"].id)
    bob = client_for("bob")
    assert alice_dashboard not in [d["id"] for d in bob.get("/catalog/dashboards").json()]
    for chart in as_seen_by(bob):
        assert chart["dashboard_id"] == alice_dashboard
        assert chart["dashboard_name"] is None

    for chart in as_seen_by(client_for("alice")):
        assert chart["dashboard_name"] == "alice_dashboard"
    for chart in as_seen_by(client_for("admin")):
        assert chart["dashboard_name"] == "alice_dashboard"

    grant_factory("dashboard", alice_dashboard, team, permission="use", granted_by=users["alice"].id)
    for chart in as_seen_by(bob):
        assert chart["dashboard_name"] == "alice_dashboard"


def test_a_dashboards_manager_detaches_a_chart_that_is_not_theirs(
    users, client_for, alice_dashboard, alice_dashboard_chart, team, group_factory, grant_factory
):
    bob = client_for("bob")
    detach = f"/catalog/visualizations/{alice_dashboard_chart}/detach"
    assert bob.post(detach).status_code == 403

    # Reading the dashboard is not managing it.
    readers = group_factory("cat-readers", users["admin"].id, {users["bob"].id: "member"})
    grant_factory("dashboard", alice_dashboard, readers, permission="use", granted_by=users["alice"].id)
    assert bob.post(detach).status_code == 403

    grant_factory("dashboard", alice_dashboard, team, permission="manage", granted_by=users["alice"].id)
    kept = bob.post(detach)
    assert kept.status_code == 200, kept.text
    assert kept.json()["dashboard_id"] is None
    with get_db_context() as db:
        assert db.get(db_models.CatalogVisualization, alice_dashboard_chart).dashboard_id is None

    # The chart is alice's own now: managing her dashboard gave bob no right over it, nor a second detach.
    assert bob.delete(f"/catalog/visualizations/{alice_dashboard_chart}").status_code == 403
    assert bob.post(detach).status_code == 403


@pytest.fixture
def alice_sql_viz(users, resource_factory, alice_catalog):
    return resource_factory(
        db_models.CatalogVisualization,
        name="alice_sql_viz",
        spec_json="[]",
        source_type="sql",
        sql_query="SELECT 1 AS region",
        namespace_id=alice_catalog["schema"],
        created_by=users["alice"].id,
    )


def _filter_layout(*bindings: dict) -> dict:
    filters = [{"id": f"f{i}", "field_name": "region", "kind": "categorical", **b} for i, b in enumerate(bindings)]
    return {"tiles": [], "filters": filters}


def _delete_dashboards_named(name: str) -> None:
    with get_db_context() as db:
        db.query(db_models.CatalogDashboard).filter_by(name=name).delete()
        db.commit()


def test_a_filter_source_the_saver_cannot_read_reads_as_unknown(
    users, client_for, alice_catalog, alice_viz, alice_sql_viz
):
    bob = client_for("bob")
    for binding in (
        {"datasource_id": alice_catalog["table"]},
        {"datasource_viz_id": alice_viz},
        {"datasource_viz_id": alice_sql_viz},
    ):
        resp = bob.post("/catalog/dashboards", json={"name": "bob_probe", "layout": _filter_layout(binding)})
        assert resp.status_code == 422, resp.text
        assert "references unknown" in resp.text
        assert "reads a catalog table" not in resp.text
    _delete_dashboards_named("bob_probe")


def test_a_filter_source_the_saver_can_read_is_checked_as_before(
    users, client_for, alice_viz, alice_sql_viz, team, grant_factory
):
    grant_factory("visualization", alice_viz, team, permission="use", granted_by=users["alice"].id)
    grant_factory("visualization", alice_sql_viz, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    try:
        table_chart = bob.post(
            "/catalog/dashboards",
            json={"name": "bob_bound", "layout": _filter_layout({"datasource_viz_id": alice_viz})},
        )
        assert table_chart.status_code == 422
        assert "reads a catalog table" in table_chart.text
        sql_chart = bob.post(
            "/catalog/dashboards",
            json={"name": "bob_bound", "layout": _filter_layout({"datasource_viz_id": alice_sql_viz})},
        )
        assert sql_chart.status_code == 201, sql_chart.text
    finally:
        _delete_dashboards_named("bob_bound")


def test_a_manage_grantee_keeps_the_owners_filter_sources(
    users, client_for, alice_catalog, alice_sql_viz, resource_factory, team, grant_factory
):
    owner_layout = _filter_layout({"datasource_id": alice_catalog["table"]}, {"datasource_viz_id": alice_sql_viz})
    dashboard_id = resource_factory(
        db_models.CatalogDashboard,
        name="alice_filtered",
        layout_json=json.dumps(owner_layout),
        layout_version=1,
        created_by=users["alice"].id,
    )
    grant_factory("dashboard", dashboard_id, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    kept = bob.put(f"/catalog/dashboards/{dashboard_id}", json={"layout": owner_layout, "description": "edited"})
    assert kept.status_code == 200, kept.text
    other_table = resource_factory(
        db_models.CatalogTable, name="alice_other", namespace_id=alice_catalog["schema"], owner_id=users["alice"].id
    )
    added = _filter_layout(
        {"datasource_id": alice_catalog["table"]},
        {"datasource_viz_id": alice_sql_viz},
        {"datasource_id": other_table},
    )
    resp = bob.put(f"/catalog/dashboards/{dashboard_id}", json={"layout": added})
    assert resp.status_code == 422
    assert f"unknown catalog_table_id={other_table}" in resp.text


def test_write_responses_stamp_owner_access(users, client_for, alice_viz, alice_dashboard):
    alice = client_for("alice")
    viz_resp = alice.put(f"/catalog/visualizations/{alice_viz}", json={"description": "mine"})
    assert viz_resp.status_code == 200, viz_resp.text
    assert viz_resp.json()["access"] == {"is_owner": True, "access_level": "owner", "shared_by": None}
    dash_resp = alice.put(f"/catalog/dashboards/{alice_dashboard}", json={"description": "mine"})
    assert dash_resp.status_code == 200, dash_resp.text
    assert dash_resp.json()["access"] == {"is_owner": True, "access_level": "owner", "shared_by": None}


def test_write_responses_stamp_manage_grantee_access(users, client_for, alice_viz, alice_dashboard, team, grant_factory):
    grant_factory("visualization", alice_viz, team, permission="manage", granted_by=users["alice"].id)
    grant_factory("dashboard", alice_dashboard, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    viz_resp = bob.put(f"/catalog/visualizations/{alice_viz}", json={"description": "managed"})
    assert viz_resp.status_code == 200, viz_resp.text
    assert viz_resp.json()["access"]["access_level"] == "manage"
    assert viz_resp.json()["access"]["is_owner"] is False
    dash_resp = bob.put(f"/catalog/dashboards/{alice_dashboard}", json={"description": "managed"})
    assert dash_resp.status_code == 200, dash_resp.text
    assert dash_resp.json()["access"]["access_level"] == "manage"


def test_create_responses_stamp_owner_access(users, client_for):
    bob = client_for("bob")
    viz_resp = bob.post(
        "/catalog/visualizations",
        json={"name": "bob_cas_viz", "spec": [{}], "source_type": "sql", "sql_query": "SELECT 1"},
    )
    assert viz_resp.status_code == 201, viz_resp.text
    assert viz_resp.json()["access"] == {"is_owner": True, "access_level": "owner", "shared_by": None}
    dash_resp = bob.post("/catalog/dashboards", json={"name": "bob_cas_dash"})
    assert dash_resp.status_code == 201, dash_resp.text
    assert dash_resp.json()["access"] == {"is_owner": True, "access_level": "owner", "shared_by": None}
    # cleanup: API-created rows aren't tracked by resource_factory.
    with get_db_context() as db:
        for model, row_id in (
            (db_models.CatalogVisualization, viz_resp.json()["id"]),
            (db_models.CatalogDashboard, dash_resp.json()["id"]),
        ):
            row = db.get(model, row_id)
            if row:
                db.delete(row)
        db.commit()


def test_use_grant_stale_token_yields_403_not_409(users, client_for, alice_viz, alice_dashboard, team, grant_factory):
    """Authorization precedes the CAS check: a read-only grantee never learns write-token state."""
    grant_factory("visualization", alice_viz, team, permission="use", granted_by=users["alice"].id)
    grant_factory("dashboard", alice_dashboard, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    stale_viz = bob.put(
        f"/catalog/visualizations/{alice_viz}",
        json={"description": "x", "expected_updated_at": "2000-01-01T00:00:00"},
    )
    assert stale_viz.status_code == 403, stale_viz.text
    stale_dash = bob.put(
        f"/catalog/dashboards/{alice_dashboard}",
        json={"description": "x", "expected_layout_version": 999},
    )
    assert stale_dash.status_code == 403, stale_dash.text


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


def test_global_runs_list_filters_to_accessible_flows(users, client_for, alice_catalog, resource_factory):
    """Restricted global list: own runs + accessible flows' runs (was a 500 on ``.runs``)."""
    from datetime import datetime, timezone

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    alice_run = resource_factory(
        db_models.FlowRun,
        registration_id=alice_catalog["flow"],
        flow_name="alice_flow",
        user_id=users["alice"].id,
        started_at=started,
        run_type="manual",
    )
    bob_run = resource_factory(
        db_models.FlowRun, registration_id=None, flow_name="bob_adhoc", user_id=users["bob"].id, started_at=started
    )

    response = client_for("bob").get("/catalog/runs")
    assert response.status_code == 200, response.text
    ids = [r["id"] for r in response.json()["items"]]
    assert bob_run in ids
    assert alice_run not in ids

    ids = [r["id"] for r in client_for("alice").get("/catalog/runs").json()["items"]]
    assert alice_run in ids
