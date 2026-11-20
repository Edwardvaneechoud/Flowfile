"""The global search shows a user exactly what the catalog's list endpoints show them (multi-user mode)."""

import json
import uuid

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def marker():
    return "zs" + uuid.uuid4().hex[:8]


@pytest.fixture
def team(users, group_factory):
    return group_factory("search-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_items(users, resource_factory, marker):
    """A private catalog of alice's holding a table (with a column), a flow, a schedule, a notebook, a chart, a
    dashboard and an artifact, plus a kernel of hers."""
    alice = users["alice"].id
    catalog = resource_factory(db_models.CatalogNamespace, name=f"{marker}_private", level=0, owner_id=alice)
    schema = resource_factory(db_models.CatalogNamespace, name="team", parent_id=catalog, level=1, owner_id=alice)
    table = resource_factory(
        db_models.CatalogTable,
        name="ledger",
        namespace_id=schema,
        owner_id=alice,
        schema_json=json.dumps([{"name": f"{marker}_amount", "dtype": "Float64"}]),
    )
    flow = resource_factory(
        db_models.FlowRegistration,
        name="ledger_flow",
        flow_path="/tmp/ledger.yaml",
        namespace_id=schema,
        owner_id=alice,
    )
    schedule = resource_factory(
        db_models.FlowSchedule, registration_id=flow, owner_id=alice, schedule_type="interval", interval_seconds=3600
    )
    notebook = resource_factory(db_models.CatalogNotebook, name="ledger_notes", namespace_id=schema, owner_id=alice)
    chart = resource_factory(
        db_models.CatalogVisualization, name="ledger_chart", spec_json="{}", catalog_table_id=table, created_by=alice
    )
    board = resource_factory(
        db_models.CatalogDashboard, name="ledger_board", layout_json="{}", namespace_id=schema, created_by=alice
    )
    artifact = resource_factory(
        db_models.GlobalArtifact,
        name="ledger_model",
        namespace_id=schema,
        status="active",
        owner_id=alice,
        source_registration_id=flow,
        serialization_format="joblib",
    )
    kernel = resource_factory(db_models.Kernel, id=f"{marker}-alice", name=f"{marker} alice kernel", user_id=alice)
    return {
        "namespace": {catalog, schema},
        "table": {table},
        "column": {f"{table}:{marker}_amount"},
        "flow": {flow},
        "schedule": {schedule},
        "notebook": {notebook},
        "visualization": {chart},
        "dashboard": {board},
        "artifact": {artifact},
        "kernel": {kernel},
    }


def _found(client, q) -> dict[str, set]:
    response = client.get("/search", params={"q": q, "limit_per_kind": 100})
    assert response.status_code == 200, response.text
    found: dict[str, set] = {}
    for hit in response.json()["hits"]:
        found.setdefault(hit["kind"], set()).add(hit["id"])
    return found


def test_owner_finds_everything_of_theirs(client_for, alice_items, marker):
    assert _found(client_for("alice"), marker) == alice_items
    assert _found(client_for("alice"), f"{marker} amount")["column"] == alice_items["column"]


def test_another_user_finds_none_of_it(client_for, alice_items, marker):
    assert _found(client_for("bob"), marker) == {}
    assert _found(client_for("bob"), f"{marker} amount") == {}


def test_admin_finds_the_catalog_but_not_another_users_kernels(client_for, alice_items, marker):
    found = _found(client_for("admin"), marker)
    assert found == {k: v for k, v in alice_items.items() if k != "kernel"}


def test_a_table_grant_makes_the_table_its_column_and_its_chart_findable(
    users, client_for, alice_items, team, grant_factory, marker
):
    (table_id,) = alice_items["table"]
    grant_factory("catalog_table", table_id, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    found = _found(bob, f"{marker} ledger")
    assert found == {k: alice_items[k] for k in ("table", "column", "visualization")}
    assert _found(bob, f"{marker} amount") == {"column": alice_items["column"]}
    assert {t["id"] for t in bob.get("/catalog/tables").json()} >= alice_items["table"]


def test_a_flow_grant_makes_the_flow_and_its_schedule_findable(
    users, client_for, alice_items, team, grant_factory, marker
):
    (flow_id,) = alice_items["flow"]
    grant_factory("flow", flow_id, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    found = _found(bob, f"{marker} ledger")
    assert found == {"flow": alice_items["flow"], "schedule": alice_items["schedule"]}
    assert {s["id"] for s in bob.get("/catalog/schedules").json()} >= alice_items["schedule"]


def test_a_namespace_grant_opens_what_is_inside(users, client_for, alice_items, team, grant_factory, marker):
    catalog_id = min(alice_items["namespace"])
    grant_factory("catalog_namespace", catalog_id, team, granted_by=users["alice"].id)
    found = _found(client_for("bob"), marker)
    assert found == {k: v for k, v in alice_items.items() if k != "kernel"}


def test_search_hits_match_the_list_endpoints(users, client_for, alice_items, team, grant_factory, marker):
    """Whatever the grants, a hit of a listed kind is in that kind's list endpoint for the same user."""
    (table_id,) = alice_items["table"]
    (flow_id,) = alice_items["flow"]
    grant_factory("catalog_table", table_id, team, granted_by=users["alice"].id)
    grant_factory("flow", flow_id, team, granted_by=users["alice"].id)
    for who in ("alice", "bob", "carol", "admin"):
        client = client_for(who)
        found = _found(client, marker)
        lists = {
            "table": {t["id"] for t in client.get("/catalog/tables").json()},
            "flow": {f["id"] for f in client.get("/catalog/flows").json()},
            "schedule": {s["id"] for s in client.get("/catalog/schedules").json()},
            "notebook": {n["id"] for n in client.get("/catalog/notebooks").json()},
            "visualization": {v["id"] for v in client.get("/catalog/visualizations").json()},
            "dashboard": {d["id"] for d in client.get("/catalog/dashboards").json()},
        }
        for kind, listed in lists.items():
            assert found.get(kind, set()) <= listed, (who, kind)


def _hits(client, q, kind) -> list[dict]:
    response = client.get("/search", params={"q": q, "kinds": kind, "limit_per_kind": 100})
    assert response.status_code == 200, response.text
    return response.json()["hits"]


def test_a_schedule_on_an_unreadable_flow_names_no_flow(users, client_for, alice_items, resource_factory, marker):
    """bob owns a schedule on alice's flow without a grant on it: he finds his schedule, not the flow's name."""
    (flow_id,) = alice_items["flow"]
    schedule = resource_factory(
        db_models.FlowSchedule,
        name=f"{marker} nightly",
        registration_id=flow_id,
        owner_id=users["bob"].id,
        schedule_type="interval",
        interval_seconds=86400,
    )
    bob = client_for("bob")
    (hit,) = _hits(bob, marker, "schedule")
    assert hit["id"] == schedule
    assert hit["parent"] is None and hit["path"] == [] and hit["namespace_id"] is None
    assert "ledger" not in json.dumps(hit)
    assert _hits(bob, f"{marker} ledger_flow", "schedule") == []


def test_granted_notebooks_dashboards_and_artifacts_are_findable_alone(
    users, client_for, alice_items, team, grant_factory, marker
):
    for resource_type, kind in (
        ("catalog_notebook", "notebook"),
        ("dashboard", "dashboard"),
        ("global_artifact", "artifact"),
    ):
        (item_id,) = alice_items[kind]
        grant_factory(resource_type, item_id, team, granted_by=users["alice"].id)
    found = _found(client_for("bob"), marker)
    assert found == {k: alice_items[k] for k in ("notebook", "dashboard", "artifact")}


def test_an_artifact_names_its_source_flow_only_to_readers_of_the_flow(
    users, client_for, alice_items, team, grant_factory, marker
):
    (artifact_id,) = alice_items["artifact"]
    (flow_id,) = alice_items["flow"]
    grant_factory("global_artifact", artifact_id, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    (hit,) = _hits(bob, marker, "artifact")
    assert hit["parent"] is None and "ledger_flow" not in json.dumps(hit)
    grant_factory("flow", flow_id, team, granted_by=users["alice"].id)
    (hit,) = _hits(bob, marker, "artifact")
    assert hit["parent"]["name"] == "ledger_flow"


def test_a_chart_names_its_table_only_to_readers_of_the_table(
    users, client_for, alice_items, team, grant_factory, marker
):
    (chart_id,) = alice_items["visualization"]
    (table_id,) = alice_items["table"]
    grant_factory("visualization", chart_id, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    hits = [hit for hit in _hits(bob, "ledger_chart", "visualization") if hit["id"] == chart_id]
    assert len(hits) == 1 and hits[0]["parent"] is None and "on ledger" not in (hits[0]["subtitle"] or "")
    assert chart_id not in {hit["id"] for hit in _hits(bob, f"{marker} ledger", "visualization")}
    grant_factory("catalog_table", table_id, team, granted_by=users["alice"].id)
    (hit,) = [hit for hit in _hits(bob, "ledger_chart", "visualization") if hit["id"] == chart_id]
    assert hit["parent"]["id"] == table_id


def test_a_public_namespace_is_findable_by_everyone(users, client_for, resource_factory, marker):
    public = resource_factory(
        db_models.CatalogNamespace, name=f"{marker}_open", level=0, owner_id=users["alice"].id, is_public=True
    )
    assert _found(client_for("bob"), marker) == {"namespace": {public}}


def test_a_catalog_grant_shows_its_schemas_as_the_tree_does(
    users, client_for, alice_items, team, grant_factory, resource_factory, marker
):
    """A covered schema holding something readable is a hit without its description; an empty one is no hit."""
    alice = users["alice"].id
    catalog_id = min(alice_items["namespace"])
    schema_id = max(alice_items["namespace"])
    empty = resource_factory(
        db_models.CatalogNamespace,
        name=f"{marker}_falcon",
        parent_id=catalog_id,
        level=1,
        owner_id=alice,
        description="Project Falcon target list",
    )
    with get_db_context() as db:
        db.get(db_models.CatalogNamespace, schema_id).description = "Team ledgers"
        db.commit()
    grant_factory("catalog_namespace", catalog_id, team, granted_by=alice)
    bob = client_for("bob")
    hits = {hit["id"]: hit for hit in _hits(bob, marker, "namespace")}
    assert set(hits) == {catalog_id, schema_id}
    assert hits[schema_id]["description"] is None
    assert empty not in hits
    assert "Falcon" not in json.dumps(_hits(bob, "falcon", "namespace"))


def test_a_dashboard_shared_alone_names_no_catalog_the_caller_cannot_see(
    users, client_for, alice_items, team, grant_factory, marker
):
    (board_id,) = alice_items["dashboard"]
    grant_factory("dashboard", board_id, team, granted_by=users["alice"].id)
    bob = client_for("bob")
    (hit,) = [hit for hit in _hits(bob, "ledger_board", "dashboard") if hit["id"] == board_id]
    assert hit["path"] == [] and hit["namespace_id"] is None and marker not in json.dumps(hit)
    assert _found(bob, f"{marker}_private") == {}


def test_an_unnamed_schedule_on_an_unreadable_flow_is_not_found_by_its_placeholder_name(
    users, client_for, alice_items, resource_factory, marker
):
    (flow_id,) = alice_items["flow"]
    schedule = resource_factory(
        db_models.FlowSchedule,
        registration_id=flow_id,
        owner_id=users["bob"].id,
        schedule_type="cron",
        cron_expression=f"0 {int(marker[2:4], 16) % 24} * * *",
    )
    bob = client_for("bob")
    assert schedule not in {hit["id"] for hit in _hits(bob, "schedule", "schedule")}
