"""``GET /search`` over real catalog rows (single-user mode).

Every row a test makes carries a fresh marker in its catalog's or its own name, and every query includes the marker,
so rows other tests leave in the shared database never reach an assertion.
"""

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.node_designer.state import EnvironmentState, NodeManifest
from flowfile_core.search import SearchKind, SearchRequest, search_catalog


@pytest.fixture(scope="module")
def client():
    with TestClient(main.app) as plain:
        token = plain.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    yield authed
    authed.close()


@pytest.fixture(scope="module")
def me(client):
    return client.get("/auth/users/me").json()["id"]


@pytest.fixture
def make():
    """Insert ORM rows and remove them (newest first) after the test."""
    created: list[tuple[type, object]] = []

    def _make(model, **kwargs):
        with get_db_context() as db:
            row = model(**kwargs)
            db.add(row)
            db.commit()
            db.refresh(row)
            created.append((model, row.id))
            return row.id

    yield _make
    with get_db_context() as db:
        for model, row_id in reversed(created):
            row = db.get(model, row_id)
            if row is not None:
                db.delete(row)
        db.commit()


@pytest.fixture
def marker():
    return "zq" + uuid.uuid4().hex[:8]


@pytest.fixture
def catalog(make, me, marker):
    """``<marker>_finance.sales`` holding a table with columns, a flow with a cron schedule, a notebook, a chart, a
    dashboard and two versions of an artifact; plus a kernel of the caller's and one of another user's."""
    cat = make(db_models.CatalogNamespace, name=f"{marker}_finance", level=0, owner_id=me)
    schema = make(db_models.CatalogNamespace, name="sales", parent_id=cat, level=1, owner_id=me)
    schema_json = json.dumps([{"name": "customer_id", "dtype": "Int64"}, {"name": "amount", "dtype": "Float64"}])
    orders = make(
        db_models.CatalogTable,
        name="orders",
        namespace_id=schema,
        owner_id=me,
        description="All orders",
        schema_json=schema_json,
        row_count=1200,
        column_count=2,
    )
    make(db_models.CatalogTable, name="orders_archive", namespace_id=schema, owner_id=me)
    make(db_models.CatalogTable, name="raw_orders", namespace_id=schema, owner_id=me)
    flow = make(
        db_models.FlowRegistration,
        name="load_orders",
        flow_path=f"/tmp/{marker}/load_orders.yaml",
        namespace_id=schema,
        owner_id=me,
    )
    schedule = make(
        db_models.FlowSchedule,
        registration_id=flow,
        owner_id=me,
        schedule_type="cron",
        cron_expression="0 6 * * *",
        cron_timezone="Europe/Amsterdam",
    )
    notebook = make(db_models.CatalogNotebook, name="revenue_notebook", namespace_id=schema, owner_id=me)
    chart = make(
        db_models.CatalogVisualization,
        name="revenue_chart",
        chart_type="bar",
        spec_json="{}",
        catalog_table_id=orders,
        created_by=me,
    )
    board = make(db_models.CatalogDashboard, name="revenue_board", layout_json="{}", namespace_id=schema, created_by=me)
    first = make(
        db_models.GlobalArtifact,
        name="churn_model",
        namespace_id=schema,
        version=1,
        status="active",
        owner_id=me,
        source_registration_id=flow,
        python_type="sklearn.ensemble.RandomForestClassifier",
        serialization_format="joblib",
        tags=json.dumps(["ml"]),
    )
    latest = make(
        db_models.GlobalArtifact,
        name="churn_model",
        namespace_id=schema,
        version=2,
        status="active",
        owner_id=me,
        source_registration_id=flow,
        python_type="sklearn.ensemble.RandomForestClassifier",
        serialization_format="joblib",
        tags=json.dumps(["ml"]),
    )
    other_user = make(db_models.User, username=f"{marker}_other", email=f"{marker}@example.com", hashed_password="x")
    kernel = make(db_models.Kernel, id=f"{marker}-k", name=f"{marker} kernel", user_id=me, packages='["xgboost"]')
    make(db_models.Kernel, id=f"{marker}-other", name=f"{marker} theirs", user_id=other_user, packages='["xgboost"]')
    return SimpleNamespace(
        catalog=cat,
        schema=schema,
        orders=orders,
        flow=flow,
        schedule=schedule,
        notebook=notebook,
        chart=chart,
        board=board,
        artifact_versions=(first, latest),
        kernel=kernel,
    )


def _search(client, q, **params):
    response = client.get("/search", params={"q": q, **params})
    assert response.status_code == 200, response.text
    return response.json()


def _of(body, kind):
    return [hit for hit in body["hits"] if hit["kind"] == kind]


def test_table_by_name_ranks_the_exact_name_first(client, catalog, marker):
    body = _search(client, f"{marker} orders", kinds="table")
    names = [hit["name"] for hit in _of(body, "table")]
    assert names[0] == "orders"
    assert set(names) == {"orders", "orders_archive", "raw_orders"}
    top = _of(body, "table")[0]
    assert top["qualified_name"] == f"{marker}_finance.sales.orders"
    assert top["path"] == [f"{marker}_finance", "sales"]
    assert top["uri"] == f"flowfile://catalog/tables/{catalog.orders}"
    assert top["subtitle"] == "Delta table · 1,200 rows · 2 columns"
    assert top["details"]["row_count"] == 1200
    assert {"field": "name", "value": "orders"} in top["matches"]


def test_a_dotted_name_finds_the_table(client, catalog, marker):
    body = _search(client, f"{marker}_finance.sales.orders", kinds="table")
    assert _of(body, "table")[0]["id"] == catalog.orders


def test_column_hits_need_the_column_name_to_match(client, catalog, marker):
    body = _search(client, f"{marker} customer", kinds="column")
    columns = _of(body, "column")
    assert [hit["name"] for hit in columns] == ["customer_id"]
    hit = columns[0]
    assert hit["id"] == f"{catalog.orders}:customer_id"
    assert hit["qualified_name"] == f"{marker}_finance.sales.orders.customer_id"
    assert hit["parent"] == {
        "kind": "table",
        "id": catalog.orders,
        "name": "orders",
        "uri": f"flowfile://catalog/tables/{catalog.orders}",
    }
    assert hit["uri"] == f"flowfile://catalog/tables/{catalog.orders}/columns/customer_id"
    assert hit["subtitle"] == "Int64"


def test_a_column_needs_a_word_in_its_name_not_only_in_its_type(client, catalog, marker):
    assert _of(_search(client, f"{marker} int64", kinds="column"), "column") == []
    body = _search(client, f"{marker} customer int64", kinds="column")
    assert [hit["name"] for hit in _of(body, "column")] == ["customer_id"]


def test_dashes_and_underscores_are_interchangeable(client, catalog, marker):
    body = _search(client, f"{marker}-finance raw-orders", kinds="table")
    assert [hit["name"] for hit in _of(body, "table")] == ["raw_orders"]


def test_every_kind_in_the_catalog_is_found_through_its_path(client, catalog, marker):
    body = _search(client, marker, limit_per_kind=50)
    assert {hit["id"] for hit in _of(body, "namespace")} == {catalog.catalog, catalog.schema}
    assert catalog.orders in {hit["id"] for hit in _of(body, "table")}
    assert [hit["id"] for hit in _of(body, "flow")] == [catalog.flow]
    assert [hit["id"] for hit in _of(body, "schedule")] == [catalog.schedule]
    assert [hit["id"] for hit in _of(body, "notebook")] == [catalog.notebook]
    assert [hit["id"] for hit in _of(body, "visualization")] == [catalog.chart]
    assert [hit["id"] for hit in _of(body, "dashboard")] == [catalog.board]
    assert [hit["id"] for hit in _of(body, "artifact")] == [catalog.artifact_versions[1]]
    assert [hit["id"] for hit in _of(body, "kernel")] == [catalog.kernel]
    assert _of(body, "column") == []


def test_schedule_without_a_name_is_named_after_its_flow(client, catalog, marker):
    hit = _of(_search(client, f"{marker} cron", kinds="schedule"), "schedule")[0]
    assert hit["name"] == "load_orders"
    assert hit["subtitle"] == "Cron 0 6 * * * (Europe/Amsterdam)"
    assert hit["parent"]["id"] == catalog.flow
    assert hit["details"]["registration_id"] == catalog.flow


def test_artifact_versions_collapse_to_the_latest(client, catalog, marker):
    hits = _of(_search(client, f"{marker} randomforest", kinds="artifact"), "artifact")
    assert len(hits) == 1
    hit = hits[0]
    assert hit["id"] == catalog.artifact_versions[1]
    assert hit["details"]["version"] == 2
    assert hit["details"]["versions"] == 2
    assert hit["parent"] == {
        "kind": "flow",
        "id": catalog.flow,
        "name": "load_orders",
        "uri": f"flowfile://catalog/flows/{catalog.flow}",
    }
    assert any(match["field"] == "python_type" for match in hit["matches"])


def test_kernels_are_found_by_package_and_only_the_callers(client, catalog, marker):
    hits = _of(_search(client, f"{marker} xgboost", kinds="kernel"), "kernel")
    assert [hit["id"] for hit in hits] == [catalog.kernel]
    assert {"field": "package", "value": "xgboost"} in hits[0]["matches"]
    assert hits[0]["uri"] == f"flowfile://kernels/{marker}-k"


def test_visualization_points_at_its_table(client, catalog, marker):
    hit = _of(_search(client, f"{marker} revenue chart", kinds="visualization"), "visualization")[0]
    assert hit["parent"]["id"] == catalog.orders
    assert hit["path"] == [f"{marker}_finance", "sales"]
    assert hit["subtitle"] == "Bar · on orders"


def test_kinds_filter_and_limit_per_kind(client, catalog, marker):
    body = _search(client, f"{marker} orders", kinds="table", limit_per_kind=1)
    assert body["kinds"] == ["table"]
    assert body["counts"] == {"table": 3}
    assert len(body["hits"]) == 1


def test_hits_are_sorted_by_score_across_kinds(client, catalog, marker):
    hits = _search(client, f"{marker} orders")["hits"]
    scores = [hit["score"] for hit in hits]
    assert scores == sorted(scores, reverse=True)


def test_like_wildcards_match_literally(client, catalog, make, me, marker):
    percent = make(db_models.CatalogTable, name=f"{marker}_50%_off", namespace_id=catalog.schema, owner_id=me)
    make(db_models.CatalogTable, name=f"{marker}_500ff", namespace_id=catalog.schema, owner_id=me)
    body = _search(client, f"{marker} 50%", kinds="table")
    assert [hit["id"] for hit in body["hits"]] == [percent]
    assert _search(client, f"{marker} 50%_o", kinds="table")["hits"][0]["id"] == percent


def test_blank_and_missing_queries_are_refused(client):
    assert client.get("/search", params={"q": "   "}).status_code == 422
    assert client.get("/search").status_code == 422
    assert client.get("/search", params={"q": "x", "kinds": "nope"}).status_code == 422


def test_search_needs_a_signed_in_user():
    with TestClient(main.app) as anonymous:
        assert anonymous.get("/search", params={"q": "orders"}).status_code == 401


def _custom_node_entry(marker, file_name, *, load_error=None):
    manifest = NodeManifest(
        node_name=f"{marker} Fuzzy Dedupe",
        node_category="Cleaning",
        intro="Removes near-duplicate rows",
        tags=["dedupe"],
        environment=EnvironmentState(kind="kernel", dependencies=["rapidfuzz>=3"]),
    )
    return SimpleNamespace(
        node_key=f"{marker}_fuzzy_dedupe",
        file_name=file_name,
        file_path=Path(f"/tmp/{file_name}"),
        manifest=manifest,
        mtime=0,
        mount_path=None,
        load_error=load_error,
    )


def _install_entries(monkeypatch, entries, holder):
    from flowfile_core.flowfile.user_defined.registry import registry as node_registry

    monkeypatch.setattr(node_registry, "all", lambda include_broken=True: entries)
    monkeypatch.setattr(node_registry, "get", lambda key: holder if key == holder.node_key else None)


def test_custom_nodes_come_from_the_registry_scan(client, monkeypatch, marker):
    entry = _custom_node_entry(marker, f"{marker}_fuzzy_dedupe.py")
    _install_entries(monkeypatch, [entry], entry)
    hits = _of(_search(client, f"{marker} rapidfuzz", kinds="custom_node"), "custom_node")
    assert [hit["id"] for hit in hits] == [f"{marker}_fuzzy_dedupe"]
    assert hits[0]["subtitle"] == "Custom node · Cleaning · runs on a kernel"
    assert hits[0]["details"]["environment"] == "kernel"


def test_a_duplicate_custom_node_file_gets_its_own_id_and_private_files_are_hidden(client, monkeypatch, marker):
    holder = _custom_node_entry(marker, f"{marker}_fuzzy_dedupe.py")
    duplicate = _custom_node_entry(marker, f"{marker}_copy.py", load_error="Duplicate node name")
    private = _custom_node_entry(marker, f"_{marker}_helper.py")
    _install_entries(monkeypatch, [private, holder, duplicate], holder)
    hits = _of(_search(client, f"{marker} dedupe", kinds="custom_node"), "custom_node")
    ids = sorted(hit["id"] for hit in hits)
    assert ids == sorted([f"{marker}_fuzzy_dedupe", f"file:{marker}_copy.py"])
    assert len({hit["uri"] for hit in hits}) == 2


def test_a_kind_with_more_candidates_than_examined_reports_truncated(catalog, me, marker):
    with get_db_context() as db:
        user = db.get(db_models.User, me)
        request = SearchRequest(query=f"{marker} orders", kinds=[SearchKind.TABLE])
        body = search_catalog(db, user, request, max_candidates=1)
    assert body.truncated is True
    assert body.truncated_kinds == [SearchKind.TABLE]
    assert body.counts[SearchKind.TABLE] == 1


def test_artifact_versions_collapse_before_the_candidate_cap(catalog, me, marker):
    with get_db_context() as db:
        user = db.get(db_models.User, me)
        request = SearchRequest(query=f"{marker} churn", kinds=[SearchKind.ARTIFACT])
        body = search_catalog(db, user, request, max_candidates=1)
    assert body.truncated is False
    assert [hit.id for hit in body.hits] == [catalog.artifact_versions[1]]
    assert body.hits[0].details["versions"] == 2


def test_a_word_spanning_catalog_and_schema_finds_what_is_inside(client, catalog, marker):
    body = _search(client, f"{marker}_finance-sales orders", kinds="table")
    assert {hit["name"] for hit in _of(body, "table")} == {"orders", "orders_archive", "raw_orders"}


def test_a_word_spanning_schema_and_table_finds_a_column(client, catalog, marker):
    body = _search(client, f"{marker} sales_orders customer", kinds="column")
    assert [hit["name"] for hit in _of(body, "column")] == ["customer_id"]


def test_a_column_whose_name_holds_the_word_mid_word_is_still_a_hit(client, make, me, marker):
    """A table with no namespace has its bare name as the column path, which can outscore a mid-word name match."""
    columns = [{"name": f"pre{marker}", "dtype": "Int64"}, {"name": f"{marker}_total", "dtype": "Int64"}]
    make(db_models.CatalogTable, name=marker, namespace_id=None, owner_id=me, schema_json=json.dumps(columns))
    body = _search(client, marker, kinds="column")
    assert {hit["name"] for hit in _of(body, "column")} == {f"pre{marker}", f"{marker}_total"}


def test_tables_without_a_matching_column_do_not_fill_the_column_cap(make, me, marker):
    """Every stored schema holds the JSON key "name", so the SQL filter passes all of them for the word "name"."""
    schema = make(db_models.CatalogNamespace, name=f"{marker}_cols", level=0, owner_id=me)
    make(
        db_models.CatalogTable,
        name="people",
        namespace_id=schema,
        owner_id=me,
        schema_json=json.dumps([{"name": "customer_name", "dtype": "String"}]),
    )
    for index in range(3):
        make(
            db_models.CatalogTable,
            name=f"other_{index}",
            namespace_id=schema,
            owner_id=me,
            schema_json=json.dumps([{"name": "amount", "dtype": "Int64"}]),
        )
    with get_db_context() as db:
        user = db.get(db_models.User, me)
        request = SearchRequest(query=f"{marker} name", kinds=[SearchKind.COLUMN])
        body = search_catalog(db, user, request, max_candidates=1)
    assert [hit.name for hit in body.hits] == ["customer_name"]
    assert body.truncated is False
