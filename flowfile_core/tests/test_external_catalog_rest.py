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

pytestmark = pytest.mark.iceberg_rest

SIGNED = fixtures.SIGNED_CATALOG_URI


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    from test_utils.s3.aws_profiles import isolate_aws

    aws_dir = tmp_path / "aws"
    aws_dir.mkdir()
    isolate_aws(monkeypatch, aws_dir)


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


@pytest.fixture(scope="module")
def signed(principal):
    return fixtures.signed_catalog(principal)


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


def _signed_body(principal, name: str | None = None, **overrides) -> dict:
    return {
        "name": name or f"polaris_{uuid.uuid4().hex[:6]}",
        "uri": SIGNED,
        "warehouse": fixtures.SIGNED_WAREHOUSE,
        "sign_in": "client_credentials",
        "client_id": principal.client_id,
        "client_secret": principal.client_secret,
        "scope": fixtures.SIGNED_SCOPE,
        **overrides,
    }


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

    added = api.call("POST", "/catalog/external", json=_signed_body(principal))

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
        body = {key: value for key, value in _signed_body(principal, **overrides).items() if key != "name"}
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
    added = api.call("POST", "/catalog/external", json=_signed_body(principal))
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
