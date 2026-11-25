"""A catalog kept elsewhere, without any catalog running: what the API refuses before asking one, the migration, and
that nothing can be saved into such a catalog's schemas by any save path.

The rows of a catalog kept elsewhere are made directly in the database here; ``test_external_catalog_rest.py`` adds
real ones through the API against Apache Polaris and Lakekeeper.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect

from flowfile_core import flow_file_handler, main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    ExternalCatalog,
    FlowRegistration,
    Secret,
)
from shared.storage_config import storage

CLOSED = "http://127.0.0.1:9/api/catalog"
SECRET = "client-secret-for-tests-only"


def _client() -> TestClient:
    client = TestClient(main.app)
    token = client.post("/auth/token").json()["access_token"]
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _client()


def _details(response) -> str:
    detail = response.json().get("detail")
    if isinstance(detail, list):
        return " ".join(item.get("msg", "") for item in detail)
    return str(detail)


@pytest.fixture
def kept(request):
    """A catalog kept elsewhere and one of its schemas, as rows; removed after the test."""
    with get_db_context() as db:
        catalog = CatalogNamespace(name="elsewhere_rows", level=0, owner_id=1, kept_elsewhere=True)
        db.add(catalog)
        db.commit()
        schema = CatalogNamespace(name="sales", parent_id=catalog.id, level=1, owner_id=1, kept_elsewhere=True)
        db.add(schema)
        db.add(ExternalCatalog(namespace_id=catalog.id, kind="iceberg_rest", uri=CLOSED, sign_in="none"))
        db.commit()
        ids = {"catalog": catalog.id, "schema": schema.id}
    yield ids
    with get_db_context() as db:
        rows = [row.id for row in db.query(CatalogTable).filter(CatalogTable.namespace_id.in_(ids.values()))]
        db.query(CatalogTableReadLink).filter(CatalogTableReadLink.table_id.in_(rows)).delete(
            synchronize_session=False
        )
        db.query(CatalogTable).filter(CatalogTable.id.in_(rows)).delete(synchronize_session=False)
        db.query(FlowRegistration).filter(FlowRegistration.namespace_id.in_(ids.values())).delete(
            synchronize_session=False
        )
        db.query(ExternalCatalog).filter_by(namespace_id=ids["catalog"]).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.parent_id == ids["catalog"]).delete()
        db.query(CatalogNamespace).filter_by(id=ids["catalog"]).delete()
        db.commit()


def _body(**overrides) -> dict:
    return {
        "name": "unreachable",
        "uri": CLOSED,
        "warehouse": "signed",
        "sign_in": "client_credentials",
        "client_id": "flowfile",
        "client_secret": SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
        **overrides,
    }


def test_a_body_with_a_storage_setting_is_refused():
    added = client.post("/catalog/external", json=_body(**{"s3.access-key-id": "AKIA0000"}))
    tested = client.post("/catalog/external/test", json=_body(**{"s3.access-key-id": "AKIA0000"}))

    assert (added.status_code, tested.status_code) == (422, 422)
    refused = [item["msg"] for item in added.json()["detail"] if "s3.access-key-id" in item["loc"]]
    assert refused == ["Extra inputs are not permitted"]
    echoed = SECRET in added.text or SECRET in tested.text
    assert not echoed


@pytest.mark.parametrize(
    ("overrides", "said"),
    [
        ({"uri": "ftp://catalog"}, "The address starts with http:// or https://."),
        ({"token_endpoint": "catalog/token"}, "The token endpoint starts with http:// or https://."),
        ({"sign_in": "none"}, "With no sign-in, leave the token, client id, secret, scope and token endpoint empty."),
        ({"sign_in": "token", "token": "a-token"}, "A token sign-in takes only the token."),
        ({"client_id": ""}, "A client id and secret sign-in needs the client id."),
        ({"client_id": "realm:client"}, "A client id cannot hold ':'."),
        ({"client_secret": ""}, "Enter the client secret."),
        ({"sign_in": "token", "client_id": None, "client_secret": None, "scope": None}, "Enter the token."),
    ],
    ids=["address", "token endpoint", "none takes nothing", "token takes only it", "client id needed",
         "client id colon", "secret needed", "token needed"],
)
def test_each_rule_of_the_form_is_refused_with_its_sentence(overrides, said):
    response = client.post("/catalog/external", json=_body(**overrides))

    assert response.status_code == 422
    found = said in _details(response)
    assert found, "the form's sentence is not in the answer"
    echoed = SECRET in response.text
    assert not echoed


def test_a_catalog_that_cannot_be_reached_is_not_added():
    response = client.post("/catalog/external", json=_body())

    assert response.status_code == 422
    assert response.json()["detail"] == f"No catalog answers at {CLOSED}."
    with get_db_context() as db:
        namespaces = db.query(CatalogNamespace).filter_by(name="unreachable").count()
        secrets = db.query(Secret).filter_by(name="unreachable__catalog_sign_in").count()
        rows = db.query(ExternalCatalog).filter_by(uri=CLOSED).count()
    assert (namespaces, secrets, rows) == (0, 0, 0)


def test_a_failed_test_is_an_answer_and_never_a_401():
    response = client.post("/catalog/external/test", json={k: v for k, v in _body().items() if k != "name"})

    assert response.status_code == 200
    assert response.json() == {"ok": False, "message": f"No catalog answers at {CLOSED}.", "schemas": None}


def test_the_migration_adds_the_table_and_the_two_columns():
    with get_db_context() as db:
        inspector = inspect(db.get_bind())
        columns = {column["name"] for column in inspector.get_columns("catalog_namespaces")}
        table = {column["name"] for column in inspector.get_columns("external_catalogs")}
        revision = db.execute(__import__("sqlalchemy").text("SELECT version_num FROM alembic_version")).scalar()

    assert {"kept_elsewhere", "gone_since"} <= columns
    assert {"namespace_id", "uri", "warehouse", "sign_in", "client_id", "scope", "token_endpoint", "secret_id",
            "synced_at", "sync_error", "not_shown"} <= table
    assert revision >= "046"


def _flow(stem: str) -> int:
    storage.flows_directory.mkdir(parents=True, exist_ok=True)
    path = storage.flows_directory / f"{stem}.yaml"
    path.unlink(missing_ok=True)
    response = client.post("/editor/create_flow/", params={"flow_path": str(path)})
    assert response.status_code == 200, response.text
    return response.json()


def _save(route: str, ids: dict, tmp_path) -> object:
    schema = ids["schema"]
    if route == "create schema":
        return client.post("/catalog/namespaces", json={"name": "added_here", "parent_id": ids["catalog"]})
    if route == "register flow":
        path = storage.flows_directory / "kept_elsewhere_flow.yaml"
        return client.post("/catalog/flows", json={"name": "f", "flow_path": str(path), "namespace_id": schema})
    if route == "register table":
        source = storage.user_data_directory / "kept_elsewhere_table.csv"
        source.write_text("a\n1\n")
        return client.post("/catalog/tables", json={"name": "t", "file_path": str(source), "namespace_id": schema})
    if route == "create virtual table":
        return client.post(
            "/catalog/virtual-tables", json={"name": "v", "namespace_id": schema, "producer_registration_id": 1}
        )
    if route == "create notebook":
        return client.post("/catalog/notebooks", json={"name": "n", "namespace_id": schema})
    if route == "save chart":
        body = {"name": "c", "spec": [], "source_type": "sql", "sql_query": "select 1", "namespace_id": schema}
        return client.post("/catalog/visualizations", json=body)
    raise AssertionError(route)


@pytest.mark.parametrize(
    "route",
    ["create schema", "register flow", "register table", "create virtual table", "create notebook", "save chart"],
)
def test_nothing_can_be_saved_into_a_schema_kept_elsewhere(kept, route, tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path)
    response = _save(route, kept, tmp_path)

    assert response.status_code == 422, response.text
    detail = _details(response)
    refused = "is kept by another catalog: Flowfile shows it and does not" in detail
    assert refused, detail


def _writer_settings(**settings) -> list:
    """The answers to saving a Catalog Writer's settings once per ``settings``, each a fresh node on a fresh flow."""
    answers = []
    for index, given in enumerate(settings.values()):
        flow_id = _flow(f"kept_elsewhere_writer_{index}")
        try:
            client.post(
                "/editor/add_node",
                params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
            )
            payload = {"flow_id": flow_id, "node_id": 1, "catalog_write_settings": {"table_name": "w", **given}}
            answers.append(client.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"}))
        finally:
            flow_file_handler.delete_flow(flow_id)
    return answers


def test_a_catalog_writer_on_a_schema_kept_elsewhere_saves_overwrite_and_append_and_refuses_upsert(kept):
    schema = kept["schema"]
    overwrite, append, by_name, upsert, on_the_catalog = _writer_settings(
        overwrite={"namespace_id": schema},
        append={"namespace_id": schema, "write_mode": "append"},
        by_name={"namespace_id": None, "namespace_full_name": "elsewhere_rows.sales", "source": "local",
                 "write_mode": "append", "track_changes": False, "partition_by": [], "merge_keys": []},
        upsert={"namespace_id": schema, "write_mode": "upsert", "merge_keys": ["a"]},
        on_the_catalog={"namespace_id": kept["catalog"]},
    )

    assert (overwrite.status_code, append.status_code, by_name.status_code) == (200, 200, 200), overwrite.text
    assert upsert.status_code == 422
    assert _details(upsert) == (
        "elsewhere_rows keeps this table. Flowfile can Append to it or Overwrite it; Upsert is not available there."
    )
    assert on_the_catalog.status_code == 422
    assert "is kept by another catalog: Flowfile shows it and does not" in _details(on_the_catalog)


def test_a_mode_changed_on_an_unchanged_target_is_still_refused(kept):
    """The drawer re-posts an unchanged target; a refused mode on it is refused all the same."""
    flow_id = _flow("kept_elsewhere_writer_again")
    try:
        client.post(
            "/editor/add_node",
            params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
        )
        base = {"flow_id": flow_id, "node_id": 1}
        first = client.post("/update_settings/", params={"node_type": "catalog_writer"}, json={
            **base, "catalog_write_settings": {"table_name": "w", "namespace_id": kept["schema"]},
        })
        second = client.post("/update_settings/", params={"node_type": "catalog_writer"}, json={
            **base, "catalog_write_settings": {"table_name": "w", "namespace_id": kept["schema"], "write_mode": "error"},
        })
    finally:
        flow_file_handler.delete_flow(flow_id)

    assert (first.status_code, second.status_code) == (200, 422)
    assert _details(second).endswith("; Error if exists is not available there.")


def test_a_catalog_or_a_schema_still_listed_is_not_removed_by_the_ordinary_delete(kept):
    catalog = client.delete(f"/catalog/namespaces/{kept['catalog']}")
    schema = client.delete(f"/catalog/namespaces/{kept['schema']}")

    assert (catalog.status_code, schema.status_code) == (422, 422)
    assert "remove the catalog from Flowfile instead" in catalog.json()["detail"]


def test_a_schema_kept_elsewhere_is_not_renamed_and_its_catalog_takes_no_storage(kept):
    renamed = client.put(f"/catalog/namespaces/{kept['schema']}", json={"name": "other"})
    stored = client.put(f"/catalog/namespaces/{kept['catalog']}", json={"storage_uri": "s3://bucket/x"})
    catalog_renamed = client.put(f"/catalog/namespaces/{kept['catalog']}", json={"description": "said here"})

    assert (renamed.status_code, stored.status_code, catalog_renamed.status_code) == (422, 422, 200)


def test_the_tree_marks_the_catalog_and_its_schemas(kept):
    tree = client.get("/catalog/namespaces/tree").json()

    catalog = next(node for node in tree if node["id"] == kept["catalog"])
    assert catalog["kept_elsewhere"] is True
    assert catalog["external"]["uri"] == CLOSED and catalog["external"]["sign_in"] == "none"
    secret_keys = [key for key in catalog["external"] if "secret" in key or key == "token"]
    assert secret_keys == []
    assert [child["kept_elsewhere"] for child in catalog["children"]] == [True]


def test_a_write_drops_the_kept_table_list_of_that_schema_only():
    from flowfile_core.catalog.services import external_catalogs

    lists = external_catalogs._TABLE_LISTS
    saved = dict(lists)
    try:
        lists.clear()
        lists[(1, "sales")] = (0.0, external_catalogs._now(), ["orders"])
        lists[(2, "sales")] = (0.0, external_catalogs._now(), ["orders"])
        lists[(1, "finance")] = (0.0, external_catalogs._now(), ["invoices"])
        external_catalogs.forget_schema_tables("sales")
        assert list(lists) == [(1, "finance")]
    finally:
        lists.clear()
        lists.update(saved)


def _described(ids: dict, table: str, **kwargs) -> int:
    """The row a table of the schema ``sales`` gets once its catalog described it for a node; its id."""
    from flowfile_core.catalog.services.external_catalogs import ensure_table_row, outside_table

    with get_db_context() as db:
        row = ensure_table_row(db, outside_table(db, f"elsewhere_rows.sales.{table}", None, None, source="local"), **kwargs)
        return row.id


def _reader_registration(name: str) -> int:
    with get_db_context() as db:
        registration = FlowRegistration(name=name, flow_path=f"/tmp/{name}.yaml", owner_id=1)
        db.add(registration)
        db.commit()
        return registration.id


def _drop_registration(registration_id: int) -> None:
    from flowfile_core.catalog import SQLAlchemyCatalogRepository

    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_flow(registration_id)


def test_a_described_table_gets_one_row_of_the_catalogs_owner_with_no_path_and_no_count(kept):
    first = _described(kept, "orders_once")
    again = _described(kept, "orders_once")

    with get_db_context() as db:
        rows = db.query(CatalogTable).filter_by(namespace_id=kept["schema"], name="orders_once").all()
        row = rows[0]
        fields = (row.storage_format, row.file_path, row.row_count, row.size_bytes, row.owner_id, row.table_type)
    assert first == again and len(rows) == 1
    assert fields == ("iceberg", None, None, None, 1, "physical")


def test_two_requests_that_describe_a_table_at_once_make_one_row(kept):
    import threading

    barrier = threading.Barrier(4)
    made: list[int] = []
    failed: list[BaseException] = []

    def describe() -> None:
        try:
            barrier.wait()
            made.append(_described(kept, "orders_raced"))
        except BaseException as exc:  # noqa: BLE001
            failed.append(exc)

    threads = [threading.Thread(target=describe) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failed == []
    with get_db_context() as db:
        rows = db.query(CatalogTable).filter_by(namespace_id=kept["schema"], name="orders_raced").count()
    assert rows == 1 and len(set(made)) == 1 and len(made) == 4


def test_a_write_names_its_flow_on_the_row(kept):
    registration = _reader_registration("writes_elsewhere")
    try:
        row_id = _described(kept, "orders_written")
        written = _described(kept, "orders_written", written_by=registration)
        with get_db_context() as db:
            source = db.get(CatalogTable, written).source_registration_id
        assert written == row_id and source == registration
    finally:
        _drop_registration(registration)


def test_such_a_row_is_in_no_listing_of_tables(kept):
    from flowfile_core.flowfile.flow_graph.catalog_resolution import _resolve_catalog_sql_tables

    name = "orders_unlisted_f3c1"
    before = client.get("/catalog/stats").json()["total_tables"]
    row_id = _described(kept, name)

    listed = [table["id"] for table in client.get("/catalog/tables").json()]
    in_schema = client.get("/catalog/tables", params={"namespace_id": kept["schema"]}).json()
    overview = [table["id"] for table in client.get("/catalog/overview").json()["tables"]]
    catalog = next(node for node in client.get("/catalog/namespaces/tree").json() if node["id"] == kept["catalog"])
    tree_tables = [table["id"] for child in catalog["children"] for table in child["tables"]] + [
        table["id"] for table in catalog["tables"]
    ]
    resolved = client.get("/catalog/tables/resolve", params={"q": f"elsewhere_rows.sales.{name}"})
    bare = client.get("/catalog/tables/resolve", params={"q": name})
    sql = _resolve_catalog_sql_tables(1)
    searched = client.get("/search", params={"q": name}).json()["hits"]
    after = client.get("/catalog/stats").json()["total_tables"]

    assert row_id not in listed and in_schema == [] and row_id not in overview
    assert tree_tables == []
    assert (resolved.status_code, bare.status_code) == (404, 404)
    assert name not in sql.table_paths and name not in sql.virtual_tables
    assert [hit for hit in searched if hit["kind"] in ("table", "column")] == []
    assert after == before


def test_removing_the_catalog_removes_its_table_rows_and_their_links(kept):
    from flowfile_core.catalog import SQLAlchemyCatalogRepository

    registration = _reader_registration("reads_a_removed_catalog")
    try:
        row_id = _described(kept, "orders_removed")
        with get_db_context() as db:
            SQLAlchemyCatalogRepository(db).replace_read_links(registration, [row_id])

        removed = client.delete(f"/catalog/external/{kept['catalog']}")

        assert removed.status_code == 204, removed.text
        with get_db_context() as db:
            row = db.get(CatalogTable, row_id)
            links = db.query(CatalogTableReadLink).filter_by(registration_id=registration).count()
            namespaces = db.query(CatalogNamespace).filter(CatalogNamespace.id.in_(kept.values())).count()
        assert (row, links, namespaces) == (None, 0, 0)
    finally:
        _drop_registration(registration)


def test_removing_a_gone_schema_removes_its_table_rows_and_their_links(kept):
    from datetime import datetime

    from flowfile_core.catalog import SQLAlchemyCatalogRepository

    registration = _reader_registration("reads_a_gone_schema")
    try:
        row_id = _described(kept, "orders_gone")
        with get_db_context() as db:
            SQLAlchemyCatalogRepository(db).replace_read_links(registration, [row_id])
            db.get(CatalogNamespace, kept["schema"]).gone_since = datetime(2026, 11, 1)
            db.commit()

        removed = client.delete(f"/catalog/namespaces/{kept['schema']}")

        assert removed.status_code == 204, removed.text
        with get_db_context() as db:
            row = db.get(CatalogTable, row_id)
            links = db.query(CatalogTableReadLink).filter_by(registration_id=registration).count()
            schema = db.get(CatalogNamespace, kept["schema"])
        assert (row, links, schema) == (None, 0, None)
    finally:
        _drop_registration(registration)


def test_a_refused_removal_of_a_gone_schema_keeps_its_table_rows(kept):
    """The schema is counted before anything is removed: a flow still filed there refuses it and every row stays."""
    from datetime import datetime

    row_id = _described(kept, "orders_kept_on_refusal")
    with get_db_context() as db:
        db.add(FlowRegistration(name="filed_in_a_gone_schema", flow_path="/tmp/filed_in_a_gone_schema.yaml",
                                owner_id=1, namespace_id=kept["schema"]))
        db.get(CatalogNamespace, kept["schema"]).gone_since = datetime(2026, 11, 1)
        db.commit()

    refused = client.delete(f"/catalog/namespaces/{kept['schema']}")

    assert refused.status_code == 422, refused.text
    with get_db_context() as db:
        assert db.get(CatalogTable, row_id) is not None


def test_a_writer_whose_schema_resolves_nowhere_is_refused_with_the_runs_sentence():
    (gone,) = _writer_settings(
        gone={"namespace_id": None, "namespace_full_name": "polaris_gone.sales", "source": "local",
              "write_mode": "append"},
    )

    assert gone.status_code == 422
    assert _details(gone) == "There is no catalog schema called polaris_gone.sales here; pick the schema again."


def test_a_table_name_with_a_dot_is_refused_for_a_schema_kept_elsewhere_as_for_a_delta_table(kept):
    """The other catalog would take ``eu.orders`` as a nested namespace Flowfile does not show."""
    from flowfile_core.catalog.validators import reject_dot_in_name
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.schemas import input_schema
    from tests.flowfile.conftest import add_test_manual_input
    from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph

    with pytest.raises(ValueError) as delta:
        reject_dot_in_name("eu.orders", "Table")
    flow_id = _flow("kept_elsewhere_dotted")
    try:
        client.post("/editor/add_node",
                    params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0})
        saved = client.post("/update_settings/", params={"node_type": "catalog_writer"}, json={
            "flow_id": flow_id, "node_id": 1, "catalog_write_settings": {
                "table_name": "eu.orders", "namespace_full_name": "elsewhere_rows.sales", "write_mode": "append"}})
    finally:
        flow_file_handler.delete_flow(flow_id)
    graph = create_graph(execution_location="local")
    add_test_manual_input(graph, [{"id": 1}], node_id=1)
    add_node_promise_on_type(graph, "catalog_writer", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_catalog_writer(input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id, node_id=2, user_id=1, depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name="eu.orders", namespace_full_name="elsewhere_rows.sales", write_mode="append"),
    ))
    (step,) = [step for step in graph.run_graph().node_step_result if step.node_id == 2]

    assert (saved.status_code, _details(saved)) == (422, str(delta.value))
    assert (step.success, step.error) == (False, str(delta.value))


def test_a_hub_writer_saves_overwrite_and_upsert_on_a_changed_target_as_before(kept):
    """A writer on the hub names a schema of the hub: one this Flowfile has no row of, or one whose name a catalog kept
    elsewhere here shares. Neither is refused for this Flowfile's catalogs, as before catalogs kept elsewhere."""
    answers = _writer_settings(
        overwrite={"source": "hub", "namespace_full_name": "hubcat.sales"},
        upsert={"source": "hub", "namespace_full_name": "hubcat.sales", "write_mode": "upsert", "merge_keys": ["a"]},
        named_like_kept={"source": "hub", "namespace_full_name": "elsewhere_rows.sales"},
        upsert_named_like_kept={
            "source": "hub", "namespace_full_name": "elsewhere_rows.sales", "write_mode": "upsert", "merge_keys": ["a"]
        },
    )

    assert [answer.status_code for answer in answers] == [200, 200, 200, 200], [answer.text for answer in answers]


def test_a_hub_reader_named_like_a_catalog_kept_elsewhere_here_is_a_hub_reader(kept, tmp_path):
    """The hub's ``elsewhere_rows.sales.orders`` is not this Flowfile's lineage row of the same name: the reader resolves
    to no table kept elsewhere and a save names the flow as the reader of none of this Flowfile's rows."""
    from flowfile_core.flowfile.flow_graph.catalog_resolution import _resolve_catalog_table_info
    from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
    from flowfile_core.schemas import input_schema
    from tests.flowfile.execution.graph_helpers import add_promise, make_graph

    _described(kept, "orders")
    path = tmp_path / "reads_the_hub.yaml"
    with get_db_context() as db:
        registration = FlowRegistration(name="reads_the_hub", flow_path=str(path), owner_id=1)
        db.add(registration)
        db.commit()
        registration_id = registration.id
    reader = input_schema.NodeCatalogReader(
        flow_id=8471, node_id=1, user_id=1, source="hub", catalog_full_table_name="elsewhere_rows.sales.orders"
    )
    graph = make_graph(8471)
    graph.flow_settings.path = str(path)
    graph.flow_settings.source_registration_id = registration_id
    token = schema_prefetch_blocked.set(True)
    try:
        add_promise(graph, 1, "catalog_reader")
        graph.add_catalog_reader(reader)
    finally:
        schema_prefetch_blocked.reset(token)
    try:
        graph._sync_catalog_read_links()
        with get_db_context() as db:
            links = db.query(CatalogTableReadLink).filter_by(registration_id=registration_id).count()

        assert links == 0
        assert _resolve_catalog_table_info(reader).kept_elsewhere is None
        assert graph.flow_settings.source_registration_id == registration_id
    finally:
        _drop_registration(registration_id)
