"""A catalog kept elsewhere, added over the API: Apache Polaris with a client id and secret, Lakekeeper with none.

Adding one stores one secret and makes the other catalog's namespaces schema rows; a sync adds, marks gone and brings
back schemas as the same rows; its tables are listed live and kept a minute; a table's columns come from the catalog's
description and no storage key is asked for. No answer holds the sign-in.

Every test skips unless both catalogs run: ``python -m test_utils.iceberg_rest start``.
"""

import json
import uuid

import polars as pl
import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, ExternalCatalog, ResourceGrant, Secret, UserGroup
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared import iceberg_utils
from shared.iceberg_utils import storage_properties, write_table
from shared.tests.iceberg.table_child import rows_frame
from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import clean_aws, polaris_body, signed

pytestmark = pytest.mark.iceberg_rest

SIGNED = fixtures.SIGNED_CATALOG_URI


@pytest.fixture(scope="module")
def catalogs_up():
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    if not fixtures.catalog_ready():
        pytest.skip(fixtures.SKIP_REASON)


@pytest.fixture(scope="module")
def principal(catalogs_up):
    made = fixtures.new_principal()
    yield made
    fixtures.drop_principal(made.name)


class _Answers(list):
    """Every response body of a test, searched for the sign-in; its ``repr`` holds none of them."""

    def __repr__(self) -> str:
        return f"<{len(self)} answers>"


@pytest.fixture
def api():
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {client.post('/auth/token').json()['access_token']}"}
    client.answers = _Answers()
    added: list[int] = []

    def call(method: str, path: str, **kwargs):
        response = client.request(method, path, **kwargs)
        client.answers.append(response.text)
        if method == "POST" and path == "/catalog/external" and response.status_code == 201:
            added.append(response.json()["id"])
        return response

    client.call = call
    yield client
    for namespace_id in added:
        client.delete(f"/catalog/external/{namespace_id}")


@pytest.fixture
def made_namespaces(signed):
    """Names of namespaces made on Polaris during a test; dropped (with their tables and children) after it."""
    made: list[tuple] = []
    yield made
    for name in reversed(made):
        for child in signed.list_namespaces(name) if signed.namespace_exists(name) else []:
            signed.drop_namespace(child)
        for identifier in signed.list_tables(name) if signed.namespace_exists(name) else []:
            signed.drop_table(identifier)
        if signed.namespace_exists(name):
            signed.drop_namespace(name)


def _namespace(signed, made, name: str | None = None) -> str:
    name = name or f"ns_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    made.append((name,))
    return name


def _leaks(principal, answers, *tokens) -> list[int]:
    forms = [principal.client_secret, principal.credential, *tokens]
    return [index for index, text in enumerate(answers) if any(form in text for form in forms)]


def _children(namespace_id: int) -> dict[str, CatalogNamespace]:
    with get_db_context() as db:
        rows = db.query(CatalogNamespace).filter_by(parent_id=namespace_id).all()
        db.expunge_all()
        return {row.name: row for row in rows}


def _tree_node(api, namespace_id: int) -> dict:
    return next(node for node in api.call("GET", "/catalog/namespaces/tree").json() if node["id"] == namespace_id)


def test_adding_a_signed_catalog_makes_its_schemas_rows_and_stores_one_secret(api, principal, signed, made_namespaces):
    first, second = _namespace(signed, made_namespaces), _namespace(signed, made_namespaces)

    added = api.call("POST", "/catalog/external", json=polaris_body(principal))

    assert added.status_code == 201, added.json().get("detail")
    answer = added.json()
    assert answer["kept_elsewhere"] is True
    external = answer["external"]
    assert (external["uri"], external["sign_in"], external["client_id"]) == (
        SIGNED,
        "client_credentials",
        principal.client_id,
    )
    secret_keys = sorted(key for key in external if "secret" in key or key == "token")
    assert secret_keys == []
    node = _tree_node(api, answer["id"])
    names = {child["name"] for child in node["children"]}
    assert {first, second} <= names
    assert all(child["kept_elsewhere"] for child in node["children"])
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=answer["id"]).one()
        secrets = db.query(Secret).filter_by(name=f"{answer['name']}__catalog_sign_in").all()
        ciphertext = secrets[0].encrypted_value
        plain = json.loads(decrypt_secret(ciphertext).get_secret_value())
    assert len(secrets) == 1 and secrets[0].id == row.secret_id
    assert ciphertext.startswith("$ffsec$1$")
    stored_is_the_pair = (plain["client_id"], plain["client_secret"]) == (principal.client_id, principal.client_secret)
    assert stored_is_the_pair
    assert sorted(plain) == ["client_id", "client_secret", "scope"]
    token, _ = fixtures.fetch_token(principal)
    assert _leaks(principal, api.answers, token) == []


def test_the_open_catalog_is_added_with_no_sign_in_and_stores_no_secret(api, catalogs_up):
    body = {"name": f"lake_{uuid.uuid4().hex[:6]}", "uri": fixtures.CATALOG_URI, "warehouse": fixtures.WAREHOUSE}

    added = api.call("POST", "/catalog/external", json=body)

    assert added.status_code == 201, added.json().get("detail")
    assert added.json()["external"]["sign_in"] == "none"
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=added.json()["id"]).one()
        secrets = db.query(Secret).filter_by(name=f"{body['name']}__catalog_sign_in").count()
    assert (row.secret_id, secrets) == (None, 0)


def test_test_says_signed_in_or_why_not(api, principal):
    def tried(**overrides) -> dict:
        body = {key: value for key, value in polaris_body(principal, **overrides).items() if key != "name"}
        response = api.call("POST", "/catalog/external/test", json=body)
        assert response.status_code == 200
        return response.json()

    right = tried()
    wrong = tried(client_secret="not-the-secret")
    unsigned = tried(sign_in="none", client_id=None, client_secret=None, scope=None)
    nowhere = tried(warehouse="nope")

    assert right["ok"] and right["message"] == f"Signed in. {right['schemas']} schemas."
    said = [wrong["message"], unsigned["message"], nowhere["message"]]
    assert said == [
        f"The catalog at {SIGNED} refused the client id and secret.",
        f"The catalog at {SIGNED} asks for a sign-in.",
        f"The catalog at {SIGNED} has no warehouse nope.",
    ]
    assert (wrong["ok"], unsigned["ok"], nowhere["ok"]) == (False, False, False)
    assert _leaks(principal, api.answers) == []


@pytest.fixture
def polaris(api, principal):
    """A Polaris catalog added through the API; its namespace id."""
    added = api.call("POST", "/catalog/external", json=polaris_body(principal))
    assert added.status_code == 201, added.json().get("detail")
    return added.json()["id"]


def _sync(api, namespace_id: int, force: bool = True) -> dict:
    response = api.call("POST", f"/catalog/external/{namespace_id}/sync", params={"force": force})
    assert response.status_code == 200, response.json().get("detail")
    return response.json()


def test_a_sync_adds_a_new_schema_marks_a_dropped_one_gone_and_brings_it_back_as_the_same_row(
    api, signed, polaris, made_namespaces
):
    name = _namespace(signed, made_namespaces)
    added = _sync(api, polaris)
    row_id = _children(polaris)[name].id

    signed.drop_namespace(name)
    gone = _sync(api, polaris)
    after_drop = _children(polaris)[name]
    signed.create_namespace(name)
    back = _sync(api, polaris)
    returned = _children(polaris)[name]

    assert name in added["added"]
    assert gone["gone"] == [name] and after_drop.gone_since is not None and after_drop.id == row_id
    assert back["back"] == [name] and returned.gone_since is None and returned.id == row_id
    node = _tree_node(api, polaris)
    assert all(child["unavailable_reason"] is None for child in node["children"] if child["name"] == name)


def test_a_second_sync_within_half_a_minute_asks_nothing_unless_forced(api, polaris):
    first = _sync(api, polaris, force=False)
    forced = _sync(api, polaris, force=True)

    assert first["skipped"] is True
    assert forced["skipped"] is False and forced["error"] is None


def test_nested_and_dotted_namespaces_are_counted_and_not_shown(api, signed, polaris, made_namespaces):
    before = _sync(api, polaris)["not_shown"]
    base = _namespace(signed, made_namespaces)
    dotted = _namespace(signed, made_namespaces, f"{base}.dot")
    signed.create_namespace((base, "inner"))

    after = _sync(api, polaris)

    assert after["not_shown"] - before == 2
    names = set(_children(polaris))
    assert base in names and dotted not in names
    node = _tree_node(api, polaris)
    assert node["external"]["not_shown"] == after["not_shown"]


def test_a_catalog_that_stopped_answering_keeps_its_schemas_and_says_since_when(api, polaris):
    before = set(_children(polaris))
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=polaris).one()
        synced_at, row.uri = row.synced_at, "http://127.0.0.1:9/api/catalog"
        db.commit()

    result = _sync(api, polaris)

    assert result["error"] == "No catalog answers at http://127.0.0.1:9/api/catalog."
    assert set(_children(polaris)) == before
    external = _tree_node(api, polaris)["external"]
    assert external["sync_error"] == result["error"]
    assert external["synced_at"] == synced_at.isoformat()


def test_changing_the_address_asks_for_the_sign_in_again(api, principal, polaris):
    moved = api.call("PUT", f"/catalog/external/{polaris}", json={"uri": "https://elsewhere.example/api/catalog"})
    endpoint = api.call("PUT", f"/catalog/external/{polaris}", json={"token_endpoint": "https://idp.example/token"})
    kind = api.call("PUT", f"/catalog/external/{polaris}", json={"sign_in": "token"})

    said = [response.json()["detail"] for response in (moved, endpoint, kind)]
    assert said == ["Enter the sign-in again to change where it is sent."] * 3
    unchanged = _tree_node(api, polaris)["external"]["uri"]
    assert unchanged == SIGNED


def test_changing_the_scope_keeps_the_stored_secret(api, principal, polaris):
    with get_db_context() as db:
        before = db.query(ExternalCatalog).filter_by(namespace_id=polaris).one().secret_id
    scope = f"PRINCIPAL_ROLE:{principal.name}_role"

    changed = api.call("PUT", f"/catalog/external/{polaris}", json={"scope": scope})
    retested = api.call(
        "POST",
        "/catalog/external/test",
        json={
            "uri": SIGNED,
            "warehouse": fixtures.SIGNED_WAREHOUSE,
            "sign_in": "client_credentials",
            "client_id": principal.client_id,
            "scope": scope,
            "namespace_id": polaris,
        },
    )

    assert changed.status_code == 200, changed.json().get("detail")
    assert changed.json()["external"]["scope"] == scope
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=polaris).one()
        plain = json.loads(decrypt_secret(db.get(Secret, row.secret_id).encrypted_value).get_secret_value())
    assert row.secret_id == before
    kept = plain["client_secret"] == principal.client_secret
    assert kept and plain["scope"] == scope
    assert retested.json()["ok"] is True
    assert _leaks(principal, api.answers) == []


def test_removing_the_catalog_removes_its_rows_its_grants_and_its_secret(api, principal, polaris):
    with get_db_context() as db:
        group = UserGroup(name=f"g_{uuid.uuid4().hex[:6]}", created_by=1)
        db.add(group)
        db.commit()
        schema_ids = [row.id for row in db.query(CatalogNamespace).filter_by(parent_id=polaris)]
        for namespace_id in [polaris, *schema_ids[:1]]:
            db.add(ResourceGrant(resource_type="catalog_namespace", resource_id=namespace_id, group_id=group.id,
                                 permission="use", granted_by=1))
        db.commit()
        secret_id = db.query(ExternalCatalog).filter_by(namespace_id=polaris).one().secret_id
        group_id = group.id

    removed = api.call("DELETE", f"/catalog/external/{polaris}")

    assert removed.status_code == 204
    ids = [polaris, *schema_ids]
    with get_db_context() as db:
        rows = db.query(CatalogNamespace).filter(CatalogNamespace.id.in_(ids)).count()
        external = db.query(ExternalCatalog).filter_by(namespace_id=polaris).count()
        secret = db.get(Secret, secret_id)
        grants = db.query(ResourceGrant).filter(ResourceGrant.group_id == group_id).count()
        db.query(UserGroup).filter_by(id=group_id).delete()
        db.commit()
    assert (rows, external, secret, grants) == (0, 0, None, 0)


def _schema_id(namespace_id: int, name: str) -> int:
    return _children(namespace_id)[name].id


def _seed(signed, identifier: str, rows: int) -> None:
    frame = rows_frame(rows)
    table = signed.create_table(identifier, schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema)
    if rows:
        write_table(table, frame, "append")


def test_tables_are_listed_from_the_other_catalog_and_kept_for_a_minute(api, signed, polaris, made_namespaces):
    name = _namespace(signed, made_namespaces)
    signed.create_table(f"{name}.first", schema=pa.schema([("id", pa.int64())]))
    _sync(api, polaris)
    path = f"/catalog/external/{polaris}/schemas/{_schema_id(polaris, name)}/tables"

    listed = api.call("GET", path).json()
    signed.create_table(f"{name}.second", schema=pa.schema([("id", pa.int64())]))
    kept = api.call("GET", path).json()
    refreshed = api.call("GET", path, params={"refresh": True}).json()

    assert ([t["name"] for t in listed["tables"]], listed["cached"]) == (["first"], False)
    assert ([t["name"] for t in kept["tables"]], kept["cached"]) == (["first"], True)
    assert [t["name"] for t in refreshed["tables"]] == ["first", "second"]
    catalog_name = _tree_node(api, polaris)["name"]
    assert refreshed["tables"][0]["reference"] == f"{catalog_name}.{name}.first"


def test_a_tables_columns_come_from_its_description_and_no_count_is_given(api, signed, polaris, made_namespaces):
    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 100)
    _sync(api, polaris)
    path = f"/catalog/external/{polaris}/schemas/{_schema_id(polaris, name)}/table"

    before = api.call("GET", path, params={"name": "orders"}).json()
    signed.load_table(f"{name}.orders").delete("id < 10")
    after = api.call("GET", path, params={"name": "orders"}).json()

    assert [(c["name"], c["dtype"]) for c in before["columns"]] == [
        ("id", "Int64"),
        ("grp", "Int64"),
        ("val", "Float64"),
        ("name", "String"),
    ]
    assert before["table"] == f"{name}.orders" and before["format_version"] == 2
    assert before["location"].startswith(fixtures.SIGNED_LOCATION)
    counts = [key for key in before if key in ("row_count", "rows", "summary", "records")]
    assert counts == []
    assert after["columns"] == before["columns"]
    assert after["updated_at"] >= before["updated_at"]


def test_a_tables_detail_says_when_a_write_arrived_and_gives_no_count_of_rows(api, signed, polaris, made_namespaces):
    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 0)
    _sync(api, polaris)
    path = f"/catalog/external/{polaris}/schemas/{_schema_id(polaris, name)}/table"

    empty = api.call("GET", path, params={"name": "orders"}).json()
    write_table(signed.load_table(f"{name}.orders"), rows_frame(10), "append")
    appended = api.call("GET", path, params={"name": "orders"}).json()

    keys = ("last_commit_at", "last_operation", "snapshot_count")
    assert [empty[key] for key in keys] == [None, None, 0]
    assert (appended["last_operation"], appended["snapshot_count"]) == ("append", 1)
    assert appended["last_commit_at"] is not None and "updated_at" in appended
    counts = [key for key in appended if key in ("row_count", "rows", "summary", "records", "total_records")]
    assert counts == []


def test_listing_and_describing_ask_the_catalog_for_no_storage_key(api, signed, polaris, made_namespaces, monkeypatch):
    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    loaded, asked = [], []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys", True))
        catalog = build(*args, **kwargs)
        load = catalog.load_table

        def kept(identifier):
            table = load(identifier)
            loaded.append(table)
            return table

        catalog.load_table = kept
        return catalog

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)
    schema_id = _schema_id(polaris, name)
    listed = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/tables", params={"refresh": True})
    described = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"})

    assert (listed.status_code, described.status_code) == (200, 200)
    assert asked == [False, False]
    loads = len(loaded)
    assert loads == 1
    handed = sorted(key for key in storage_properties(loaded[0]) if key in ("s3.access-key-id", "s3.session-token"))
    assert handed == []


def test_a_schema_gone_on_the_other_side_lists_nothing_and_says_why(api, signed, polaris, made_namespaces):
    name = _namespace(signed, made_namespaces)
    _sync(api, polaris)
    schema_id = _schema_id(polaris, name)
    signed.drop_namespace(name)
    _sync(api, polaris)

    listed = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/tables")
    removed = api.call("DELETE", f"/catalog/namespaces/{schema_id}")

    assert listed.status_code == 422
    catalog_name = _tree_node(api, polaris)["name"]
    assert listed.json()["detail"].startswith(f"No longer in {catalog_name}'s own catalog since ")
    assert removed.status_code == 204
    assert name not in _children(polaris)


@pytest.fixture
def reader_flow(tmp_path):
    """A registered flow of this test's own, open in the handler: ``(graph, registration id, name, path)``."""
    from flowfile_core import flow_file_handler
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.database.models import FlowRegistration

    name = f"outside_reader_{uuid.uuid4().hex[:6]}"
    path = tmp_path / f"{name}.yaml"
    with get_db_context() as db:
        registration = FlowRegistration(name=name, flow_path=str(path), owner_id=1)
        db.add(registration)
        db.commit()
        registration_id = registration.id
    internal_id = flow_file_handler.add_flow(name=name, flow_path=str(path), user_id=1)
    graph = flow_file_handler.get_flow(internal_id)
    graph.flow_settings.source_registration_id = registration_id
    yield graph, registration_id, name, path
    flow_file_handler.delete_flow(internal_id)
    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_flow(registration_id)


def test_a_saved_flow_reading_an_outside_table_lists_it_and_the_tables_detail_names_the_flow(
    api, signed, polaris, made_namespaces, reader_flow
):
    import re

    from flowfile_core.schemas import input_schema

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    catalog, schema_id = _tree_node(api, polaris)["name"], _schema_id(polaris, name)
    graph, registration_id, flow_name, path = reader_flow
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader"))
    graph.add_catalog_reader(input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=1, catalog_full_table_name=f"{catalog}.{name}.orders",
    ))

    columns = [column.column_name for column in graph.get_node(1).schema]
    graph.save_flow(str(path))
    flow = api.call("GET", f"/catalog/flows/{registration_id}").json()
    detail = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"}).json()

    assert columns == ["id", "grp", "val", "name"]
    assert [(table["name"], table["namespace_id"]) for table in flow["tables_read"]] == [("orders", schema_id)]
    assert detail["read_by_flows"] == [{"id": registration_id, "name": flow_name}]
    assert detail["written_by"] is None
    assert graph.get_node(1).setting_input.catalog_table_id is None
    assert re.search(r"catalog_table_id: \d", path.read_text()) is None
    listed = [table["name"] for table in api.call("GET", "/catalog/tables").json() if table["namespace_id"] == schema_id]
    assert listed == []

    graph.delete_node(1)
    graph.save_flow(str(path))
    after = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"}).json()
    assert api.call("GET", f"/catalog/flows/{registration_id}").json()["tables_read"] == []
    assert after["read_by_flows"] == []


def test_a_tables_detail_names_the_flow_that_wrote_it(api, signed, polaris, made_namespaces, reader_flow):
    from flowfile_core.catalog.services.external_catalogs import ensure_table_row, outside_table

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    catalog, schema_id = _tree_node(api, polaris)["name"], _schema_id(polaris, name)
    _, registration_id, flow_name, _ = reader_flow
    with get_db_context() as db:
        ensure_table_row(db, outside_table(db, f"{catalog}.{name}.orders", None, None, source="local"), written_by=registration_id)

    detail = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"}).json()

    assert detail["written_by"] == {"id": registration_id, "name": flow_name}
    assert detail["read_by_flows"] == []
    counts = [key for key in detail if key in ("row_count", "rows", "summary", "records")]
    assert counts == []


def test_a_table_no_node_was_shown_has_no_row(api, signed, polaris, made_namespaces):
    from flowfile_core.database.models import CatalogTable

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    schema_id = _schema_id(polaris, name)

    described = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"})

    assert described.status_code == 200
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(namespace_id=schema_id).count() == 0


@pytest.mark.parametrize("execution_location", ["local", "remote"])
def test_a_saved_flows_first_run_names_it_as_the_tables_reader_without_another_save(
    api, signed, polaris, made_namespaces, reader_flow, execution_location
):
    """Saved before any node was shown the table, the flow had no read link; the run that makes the row adds it."""
    from flowfile_core.schemas import input_schema

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    catalog, schema_id = _tree_node(api, polaris)["name"], _schema_id(polaris, name)
    graph, registration_id, flow_name, path = reader_flow
    graph.flow_settings.execution_location = execution_location
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader"))
    graph.add_catalog_reader(input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=1, catalog_full_table_name=f"{catalog}.{name}.orders",
    ))
    graph.save_flow(str(path))
    saved = api.call("GET", f"/catalog/flows/{registration_id}").json()["tables_read"]

    run = graph.run_graph()
    flow = api.call("GET", f"/catalog/flows/{registration_id}").json()
    detail = api.call("GET", f"/catalog/external/{polaris}/schemas/{schema_id}/table", params={"name": "orders"}).json()

    assert run.success, [step.error for step in run.node_step_result if not step.success]
    assert saved == []
    assert [(table["name"], table["namespace_id"]) for table in flow["tables_read"]] == [("orders", schema_id)]
    assert detail["read_by_flows"] == [{"id": registration_id, "name": flow_name}]
    assert graph.flow_settings.source_registration_id == registration_id


def test_two_outside_readers_of_one_run_both_name_the_flow_as_their_tables_reader(
    api, signed, polaris, made_namespaces, reader_flow
):
    """Each reader adds its own link as it runs; neither run takes the other's away."""
    from flowfile_core.schemas import input_schema

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _seed(signed, f"{name}.customers", 3)
    _sync(api, polaris)
    catalog = _tree_node(api, polaris)["name"]
    graph, registration_id, _, path = reader_flow
    graph.flow_settings.execution_location = "remote"
    for node_id, table in ((1, "orders"), (2, "customers")):
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
        )
        graph.add_catalog_reader(input_schema.NodeCatalogReader(
            flow_id=graph.flow_id, node_id=node_id, user_id=1, catalog_full_table_name=f"{catalog}.{name}.{table}",
        ))
    graph.save_flow(str(path))

    run = graph.run_graph()
    flow = api.call("GET", f"/catalog/flows/{registration_id}").json()

    assert run.success, [step.error for step in run.node_step_result if not step.success]
    assert sorted(table["name"] for table in flow["tables_read"]) == ["customers", "orders"]


def test_a_run_of_a_flow_that_does_not_own_its_registration_keeps_the_id_and_links_nothing(
    api, signed, polaris, made_namespaces, reader_flow, tmp_path
):
    """A copied flow file carries another flow's id until it is saved; its run neither clears the id nor names the
    other flow as the table's reader."""
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.database.models import FlowRegistration
    from flowfile_core.schemas import input_schema

    name = _namespace(signed, made_namespaces)
    _seed(signed, f"{name}.orders", 5)
    _sync(api, polaris)
    catalog = _tree_node(api, polaris)["name"]
    graph, _, _, path = reader_flow
    with get_db_context() as db:
        other = FlowRegistration(name=f"original_{uuid.uuid4().hex[:6]}", flow_path=str(tmp_path / "o.yaml"), owner_id=1)
        db.add(other)
        db.commit()
        other_id = other.id
    try:
        graph.flow_settings.source_registration_id = other_id
        graph.flow_settings.path = str(path)
        graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader"))
        graph.add_catalog_reader(input_schema.NodeCatalogReader(
            flow_id=graph.flow_id, node_id=1, user_id=1, catalog_full_table_name=f"{catalog}.{name}.orders",
        ))

        run = graph.run_graph()
        flow = api.call("GET", f"/catalog/flows/{other_id}").json()

        assert run.success, [step.error for step in run.node_step_result if not step.success]
        assert graph.flow_settings.source_registration_id == other_id
        assert flow["tables_read"] == []
    finally:
        with get_db_context() as db:
            SQLAlchemyCatalogRepository(db).delete_flow(other_id)


def _secret_of(namespace_id: int) -> tuple[str, int]:
    """The catalog's sign-in secret: its name and id."""
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=namespace_id).one()
        return db.get(Secret, row.secret_id).name, row.secret_id


def _ref_token(namespace_id: int) -> str | None:
    """The sign-in ``table_ref_for_run`` would put in a run's ``IcebergTableRef`` for one of the catalog's tables."""
    from flowfile_core.catalog.services.external_catalogs import table_ref_for_run

    with get_db_context() as db:
        name = db.get(CatalogNamespace, namespace_id).name
        return table_ref_for_run(db, name, "any_schema.any_table", None, writes=False).token



def _sign_in_again(namespace_id: int) -> str:
    with get_db_context() as db:
        name = db.get(CatalogNamespace, namespace_id).name
    return f"Flowfile no longer holds the sign-in for {name}; enter it again with Edit on the catalog."


def test_a_catalogs_sign_in_is_not_deleted_as_a_secret_and_the_secrets_list_shows_it(api, polaris):
    from fastapi import HTTPException

    from flowfile_core.secret_manager.secret_manager import delete_secret

    name, secret_id = _secret_of(polaris)
    with get_db_context() as db:
        catalog = db.get(CatalogNamespace, polaris).name
    said = (
        f"This secret is the sign-in of the catalog {catalog}; change the sign-in on the catalog or remove the "
        "catalog from Flowfile instead."
    )

    listed = [secret["name"] for secret in api.call("GET", "/secrets/secrets").json()]
    refused = api.call("DELETE", f"/secrets/secrets/{name}")
    with get_db_context() as db, pytest.raises(HTTPException) as direct:
        delete_secret(db, name, 1)

    assert name in listed
    assert (refused.status_code, refused.json()["detail"]) == (400, said)
    assert (direct.value.status_code, direct.value.detail) == (400, said)
    assert _secret_of(polaris) == (name, secret_id)


def test_a_sign_in_id_another_secret_took_is_never_sent_overwritten_or_deleted(api, principal, polaris):
    """An install that deleted the sign-in before deleting one was refused holds a dangling id, which SQLite gives to
    the next secret stored: that secret is no sign-in of the catalog, so nothing of it is sent, changed or removed."""
    from flowfile_core.catalog.services.external_catalogs import sign_in_name

    name, secret_id = _secret_of(polaris)
    with get_db_context() as db:
        db.delete(db.get(Secret, secret_id))
        db.commit()
    stored = api.call("POST", "/secrets/secrets", json={"name": f"mine_{uuid.uuid4().hex[:6]}", "value": "not-it"})
    other_id = stored.json()["id"]
    if other_id != secret_id:
        api.call("DELETE", f"/secrets/secrets/{stored.json()['name']}")
        pytest.skip("this catalog database gave the next secret a new id, so no id is left dangling to take")
    with get_db_context() as db:
        other_ciphertext = db.get(Secret, other_id).encrypted_value
    said = _sign_in_again(polaris)
    test_body = {
        "uri": SIGNED,
        "warehouse": fixtures.SIGNED_WAREHOUSE,
        "sign_in": "client_credentials",
        "client_id": principal.client_id,
        "scope": fixtures.SIGNED_SCOPE,
        "namespace_id": polaris,
    }

    with pytest.raises(ValueError) as refused:
        _ref_token(polaris)
    synced = _sync(api, polaris)
    tested = api.call("POST", "/catalog/external/test", json=test_body)
    rescoped = api.call("PUT", f"/catalog/external/{polaris}", json={"scope": fixtures.SIGNED_SCOPE})
    resigned = api.call("PUT", f"/catalog/external/{polaris}", json={"client_secret": principal.client_secret})
    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=polaris).one()
        fresh = db.get(Secret, row.secret_id)
        fresh_id, fresh_name, fresh_ciphertext = fresh.id, fresh.name, fresh.encrypted_value
        catalog = db.get(CatalogNamespace, polaris).name
        unchanged = db.get(Secret, other_id).encrypted_value == other_ciphertext
    sent_now = _ref_token(polaris) == fresh_ciphertext
    removed = api.call("DELETE", f"/catalog/external/{polaris}")
    with get_db_context() as db:
        survived = db.get(Secret, other_id) is not None
        fresh_gone = db.get(Secret, fresh_id) is None
        if survived:
            db.delete(db.get(Secret, other_id))
            db.commit()

    assert str(refused.value) == said and synced["error"] == said
    assert (tested.status_code, tested.json()["ok"], tested.json()["message"]) == (200, False, said)
    assert (rescoped.status_code, rescoped.json()["detail"]) == (422, said)
    assert resigned.status_code == 200, resigned.json().get("detail")
    assert fresh_id != other_id and fresh_name == sign_in_name(catalog) and name == fresh_name
    assert unchanged is True and sent_now is True
    assert removed.status_code == 204 and survived and fresh_gone
    assert _leaks(principal, api.answers) == []


def test_a_renamed_catalog_keeps_its_sign_in(api, polaris):
    name, secret_id = _secret_of(polaris)
    renamed = api.call("PUT", f"/catalog/namespaces/{polaris}", json={"name": f"renamed_{uuid.uuid4().hex[:6]}"})

    synced = _sync(api, polaris)
    refused = api.call("DELETE", f"/secrets/secrets/{name}")

    assert renamed.status_code == 200, renamed.text
    assert synced["error"] is None
    assert refused.status_code == 400 and _secret_of(polaris) == (name, secret_id)
