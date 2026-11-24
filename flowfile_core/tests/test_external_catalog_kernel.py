"""A table of a catalog kept elsewhere (Apache Polaris) read by a notebook kernel through core's catalog routes.

A kernel names such a table as a catalog page's cell does: ``catalog.schema.table``, or the table's name with its
schema's row id. ``GET /catalog/tables/resolve`` answers it, for a kernel only, as a view with the columns its catalog
describes, and gives it its hidden row; ``POST /catalog/tables/{id}/materialize`` has the session's worker ask that
catalog for the keys and write a snapshot the kernel scans. No answer to the kernel holds the sign-in, its
ciphertext, a token or a storage key.

Every test skips unless Polaris runs (``python -m test_utils.iceberg_rest start``) and the session's worker answers
(``SKIP_WORKER_TESTS=0``). Each test makes its own Polaris namespace and kernel row and drops them after.
"""

import contextlib
import json
import re
import uuid
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import create_kernel_token
from flowfile_core.catalog.constants import KEPT_ELSEWHERE_FORMAT
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, Kernel
from shared.storage_config import storage
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

SIGNED = fixtures.SIGNED_CATALOG_URI
COLUMNS = [("id", "Int64"), ("grp", "Int64"), ("val", "Float64"), ("name", "String")]
SECRET_SHAPES = {
    "ciphertext": re.compile(r"\$ffsec\$"),
    "storage key name": re.compile(r"s3\.(secret-access-key|session-token|access-key-id)"),
    "AWS-style access key": re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b"),
    "bearer header": re.compile(r"Bearer\s+[A-Za-z0-9._-]{20,}"),
    "client secret field": re.compile(r"client_secret"),
}


class _Values(list):
    """Secret values a test searches for; its ``repr`` holds none of them."""

    def __repr__(self) -> str:
        return f"<{len(self)} secret values>"


def _findings(text: str, values: list[str]) -> list[str]:
    """Names of the secret shapes and indexes of the secret values found in ``text``; never a value."""
    found = [name for name, shape in SECRET_SHAPES.items() if shape.search(text)]
    return found + [f"value #{index}" for index, value in enumerate(values) if value and value in text]


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    from test_utils.s3.aws_profiles import isolate_aws

    aws_dir = tmp_path / "aws"
    aws_dir.mkdir()
    isolate_aws(monkeypatch, aws_dir)


@pytest.fixture(scope="module")
def principal():
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    made = fixtures.new_principal()
    yield made
    fixtures.drop_principal(made.name)


@pytest.fixture(scope="module")
def signed(principal):
    return fixtures.signed_catalog(principal)


def _orders(rows: int, start: int = 0) -> pl.LazyFrame:
    return (
        pl.LazyFrame()
        .select(pl.int_range(start, start + rows, dtype=pl.Int64).alias("id"))
        .with_columns(
            grp=pl.col("id") % 10, val=pl.col("id").cast(pl.Float64) * 0.5, name=pl.format("name-{}", pl.col("id"))
        )
    )


@pytest.fixture
def schema(signed):
    """A namespace of this test's own on Polaris holding ``orders``, 1000 rows PyIceberg wrote; dropped after."""
    from shared.iceberg_utils import write_table

    name = f"kernel_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    frame = _orders(1000)
    made = signed.create_table(
        f"{name}.orders", schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema
    )
    write_table(made, frame, "append")
    yield name
    for identifier in signed.list_tables(name):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)
    with contextlib.suppress(Exception):
        signed.drop_namespace(name)


def _add_catalog(principal, owner_id: int = 1) -> tuple[str, int]:
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    name = f"polaris_{uuid.uuid4().hex[:6]}"
    body = ExternalCatalogCreate(
        name=name,
        uri=SIGNED,
        warehouse=fixtures.SIGNED_WAREHOUSE,
        sign_in="client_credentials",
        client_id=principal.client_id,
        client_secret=principal.client_secret,
        scope=fixtures.SIGNED_SCOPE,
    )
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(body, owner_id)
        catalog_id = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).one().id
    return name, catalog_id


def _remove_catalog(name: str) -> None:
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository

    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        if row is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


@pytest.fixture
def polaris(principal, schema):
    """Polaris added to Flowfile's catalog by user 1 with a client id and secret: ``(name, catalog id, schema id)``."""
    name, catalog_id = _add_catalog(principal)
    with get_db_context() as db:
        schema_id = db.query(CatalogNamespace).filter_by(parent_id=catalog_id, name=schema).one().id
    yield name, catalog_id, schema_id
    _remove_catalog(name)


@pytest.fixture
def kernel():
    """A TestClient signed as a running kernel of user 1, with its own token; every answer it got is in ``answers``."""
    kernel_id, token_id = f"ext-kernel-{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
    with get_db_context() as db:
        db.add(Kernel(id=kernel_id, name="Outside catalog", user_id=1, token_id=token_id))
        db.commit()
    client = TestClient(main.app)
    client.headers = {"X-Internal-Token": create_kernel_token(1, kernel_id, token_id), "X-Kernel-Id": kernel_id}
    client.answers = []
    request = client.request

    def recording(method, url, **kwargs):
        response = request(method, url, **kwargs)
        client.answers.append(response.text)
        return response

    client.request = recording
    yield client
    with get_db_context() as db:
        db.query(Kernel).filter(Kernel.id == kernel_id).delete()
        db.commit()


@pytest.fixture
def person():
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {client.post('/auth/token').json()['access_token']}"}
    return client


def _kept_rows(schema_id: int, name: str) -> list[int]:
    with get_db_context() as db:
        rows = db.query(CatalogTable).filter_by(namespace_id=schema_id, name=name).all()
        return [row.id for row in rows if row.storage_format == KEPT_ELSEWHERE_FORMAT]


def _resolve(client, q: str, namespace_id: int | None = None, **params):
    query = {"q": q, **params}
    if namespace_id is not None:
        query["namespace_id"] = namespace_id
    return client.get("/catalog/tables/resolve", params=query)


def test_both_ways_of_naming_the_table_resolve_to_its_row_answered_as_a_view_with_its_columns(
    kernel, polaris, schema
):
    name, _, schema_id = polaris

    by_name = _resolve(kernel, f"{name}.{schema}.orders")
    in_schema = _resolve(kernel, "orders", schema_id, strict=True)

    assert (by_name.status_code, in_schema.status_code) == (200, 200), (by_name.text, in_schema.text)
    named, found = by_name.json()["table"], in_schema.json()["table"]
    assert named["id"] == found["id"]
    assert _kept_rows(schema_id, "orders") == [named["id"]]
    assert named["table_type"] == found["table_type"] == "virtual"
    assert [(column["name"], column["dtype"]) for column in named["schema_columns"]] == COLUMNS
    assert (named["name"], named["namespace_id"], named["qualified_name"]) == (
        "orders",
        schema_id,
        f"{name}.{schema}.orders",
    )
    assert (named["file_path"], named["row_count"], named["polars_plan"]) == (None, None, None)


def test_materialize_writes_every_row_and_another_engines_append_under_one_file_name(
    kernel, polaris, schema, signed, running_worker
):
    from shared.iceberg_utils import write_table

    name, _, _ = polaris
    table_id = _resolve(kernel, f"{name}.{schema}.orders").json()["table"]["id"]

    first = kernel.post(f"/catalog/tables/{table_id}/materialize")
    assert first.status_code == 200, first.text
    first_path = first.json()["path"]
    first_ids = pl.read_ipc(first_path)["id"].sort().to_list()
    write_table(signed.load_table(f"{schema}.orders"), _orders(1000, 1000), "append")
    second = kernel.post(f"/catalog/tables/{table_id}/materialize")

    assert second.status_code == 200, second.text
    assert (first.json()["row_count"], second.json()["row_count"]) == (1000, 2000)
    assert first_ids == list(range(1000))
    assert pl.read_ipc(second.json()["path"])["id"].sort().to_list() == list(range(2000))
    assert second.json()["path"] == first_path
    assert Path(first_path).parent == storage.shared_virtual_results_directory
    assert Path(first_path).name.startswith(f"fvt-{table_id}-")


def test_the_row_is_in_no_listing(kernel, person, polaris, schema):
    name, _, schema_id = polaris
    table_id = _resolve(kernel, f"{name}.{schema}.orders").json()["table"]["id"]

    listings = [
        kernel.get("/catalog/tables"),
        kernel.get("/catalog/tables", params={"namespace_id": schema_id}),
        person.get("/catalog/tables"),
        person.get("/catalog/tables", params={"namespace_id": schema_id}),
    ]

    assert [listing.status_code for listing in listings] == [200] * 4
    assert [[row["id"] for row in listing.json() if row["id"] == table_id] for listing in listings] == [[]] * 4
    tree = person.get("/catalog/namespaces/tree").json()
    catalog = next(node for node in tree if node["name"] == name)
    shown = [table["id"] for child in catalog["children"] for table in child.get("tables", [])]
    assert table_id not in shown


def test_a_mistyped_table_is_a_422_with_a_sentence_and_makes_no_row(kernel, polaris, schema):
    name, _, schema_id = polaris

    by_name = _resolve(kernel, f"{name}.{schema}.ordrs")
    in_schema = _resolve(kernel, "ordrs", schema_id, strict=True)

    said = f"The catalog at {SIGNED} has no table {schema}.ordrs."
    assert (by_name.status_code, by_name.json()["detail"]) == (422, said)
    assert (in_schema.status_code, in_schema.json()["detail"]) == (422, said)
    assert _kept_rows(schema_id, "ordrs") == []


def test_a_persons_own_sign_in_still_gets_404_and_makes_no_row(person, polaris, schema):
    name, _, schema_id = polaris

    by_name = _resolve(person, f"{name}.{schema}.orders")
    in_schema = _resolve(person, "orders", schema_id)

    assert (by_name.status_code, in_schema.status_code) == (404, 404)
    assert _kept_rows(schema_id, "orders") == []


def test_no_sign_in_ciphertext_token_or_storage_key_reaches_the_kernel(
    kernel, polaris, schema, signed, principal, running_worker
):
    from shared.iceberg_utils import storage_properties

    name, _, schema_id = polaris
    table_id = _resolve(kernel, f"{name}.{schema}.orders").json()["table"]["id"]
    _resolve(kernel, "orders", schema_id)
    _resolve(kernel, f"{name}.{schema}.ordrs")
    materialized = kernel.post(f"/catalog/tables/{table_id}/materialize")
    assert materialized.status_code == 200, materialized.text

    keys = fixtures.key_values(storage_properties(signed.load_table(f"{schema}.orders")))
    token, _ = fixtures.fetch_token(principal)
    values = _Values([principal.client_secret, principal.credential, token, *keys, fixtures.MINIO_ROOT_PASSWORD])
    snapshot = Path(materialized.json()["path"]).read_bytes().decode("latin-1")
    places = {"answers": "\n".join(kernel.answers), "snapshot file": snapshot}
    assert len(kernel.answers) == 4
    assert {place: _findings(text, values) for place, text in places.items()} == {place: [] for place in places}


def test_what_the_worker_is_refused_is_a_422_with_its_sentence(kernel, signed, schema, running_worker):
    """A principal that may read a table's description and not its data: core describes the table, the worker's ask
    for the keys is refused, and the kernel gets that refusal's sentence."""
    reader = fixtures.new_principal(privilege="TABLE_READ_PROPERTIES")
    try:
        for privilege in ("NAMESPACE_LIST", "TABLE_LIST"):
            fixtures._manage(
                "PUT",
                f"/catalogs/{fixtures.SIGNED_WAREHOUSE}/catalog-roles/{reader.name}_cat/grants",
                fixtures._root_headers(),
                {"grant": {"type": "catalog", "privilege": privilege}},
            )
        name, _ = _add_catalog(reader)
        try:
            resolved = _resolve(kernel, f"{name}.{schema}.orders")
            assert resolved.status_code == 200, resolved.text
            table_id = resolved.json()["table"]["id"]

            refused = kernel.post(f"/catalog/tables/{table_id}/materialize")

            assert refused.status_code == 422, refused.text
            assert refused.json()["detail"] == f"The catalog at {SIGNED} knows this sign-in and does not allow it this."
            assert list(storage.shared_virtual_results_directory.glob(f"fvt-{table_id}-*.arrow")) == []
        finally:
            _remove_catalog(name)
    finally:
        fixtures.drop_principal(reader.name)


CHILD_FAILED = "resolve_virtual_table child failed: "


def _worker_error(body: str) -> RuntimeError:
    """The error ``trigger_resolve_virtual_table`` raises for a worker that answered ``body`` with a failure."""
    return RuntimeError(f"Worker resolve_virtual_table failed: {body}")


def test_only_a_childs_refusal_is_a_sentence_with_its_paths_scrubbed_and_its_address_kept():
    """Called directly with the worker's own texts: no failure of these can be provoked in this suite without killing
    the worker's child, refusing core's token or putting a proxy in front of the worker. Anything but the child's
    refusal is ``None``, which ``materialize_virtual_table`` raises as ``WorkerUnavailableError`` (a 503 with the
    fixed detail) as before."""
    from flowfile_core.catalog.services.virtual_tables import _worker_refusal

    others = [
        '{"detail":"resolve_virtual_table child exited without result (exitcode=-9)"}',
        '{"detail":"Invalid or missing internal token"}',
        "<html><head><title>502 Bad Gateway</title></head><body><h1>502 Bad Gateway</h1></body></html>",
        '{"detail":"[Errno 13] Permission denied: \'/srv/flowfile/shared/catalog_virtual_results/fvt-7-ab.arrow\'"}',
        '{"detail":[{"type":"missing","loc":["body","plan_bytes"]}]}',
    ]
    refusal = json.dumps(
        {"detail": f"{CHILD_FAILED}The catalog at {SIGNED} could not read /srv/flowfile/storage/tables/orders.parquet."}
    )

    assert [_worker_refusal(_worker_error(body)) for body in others] == [None] * len(others)
    assert _worker_refusal(_worker_error(refusal)) == f"The catalog at {SIGNED} could not read orders.parquet."


def test_the_row_and_its_snapshot_file_go_with_the_catalog(kernel, person, principal, schema, running_worker):
    name, catalog_id = _add_catalog(principal)
    try:
        table_id = _resolve(kernel, f"{name}.{schema}.orders").json()["table"]["id"]
        materialized = kernel.post(f"/catalog/tables/{table_id}/materialize")
        assert materialized.status_code == 200, materialized.text
        snapshot = Path(materialized.json()["path"])
        assert snapshot.exists()

        removed = person.delete(f"/catalog/external/{catalog_id}")

        assert removed.status_code == 204, removed.text
        with get_db_context() as db:
            assert db.get(CatalogTable, table_id) is None
        assert not snapshot.exists()
        assert list(storage.shared_virtual_results_directory.glob(f"fvt-{table_id}-*.arrow")) == []
    finally:
        _remove_catalog(name)
