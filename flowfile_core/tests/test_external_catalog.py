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
    if route == "catalog writer settings":
        flow_id = _flow("kept_elsewhere_writer")
        try:
            client.post(
                "/editor/add_node",
                params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
            )
            payload = {"flow_id": flow_id, "node_id": 1, "catalog_write_settings": {"table_name": "w", "namespace_id": schema}}
            return client.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
        finally:
            flow_file_handler.delete_flow(flow_id)
    raise AssertionError(route)


@pytest.mark.parametrize(
    "route",
    ["create schema", "register flow", "register table", "create virtual table", "create notebook", "save chart",
     "catalog writer settings"],
)
def test_nothing_can_be_saved_into_a_schema_kept_elsewhere(kept, route, tmp_path):
    response = _save(route, kept, tmp_path)

    assert response.status_code == 422, response.text
    detail = _details(response)
    refused = "is kept by another catalog: Flowfile shows it and does not" in detail
    assert refused, detail


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
