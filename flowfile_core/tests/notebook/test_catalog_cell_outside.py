"""A catalog page's notebook cell, in process: what ``ff.read_catalog_table`` and ``ff.write_catalog_table`` say there.

Such a cell runs in a notebook kernel with a negative flow id and no kernel session: the runtime installs the
``metadata_lookup`` hook over its own transport, which asks core's catalog routes as the kernel
(``kernel_runtime/catalog_lookup.py``). Here the hook is installed with ``flowfile_frame._metadata.installed`` over a
transport that makes the runtime's calls (``GET /catalog/tables/resolve``, ``POST .../materialize``) against core's
real routes with a kernel's own token, raising as the runtime does (an ``httpx`` status error for a refusal). The
cell's own work runs under ``IN_KERNEL_OP``, so ``kernel_db_opens`` lists any catalog connection the kernel would
open; core's answers run in a fresh context and are not counted.

Every test skips unless Polaris runs (``python -m test_utils.iceberg_rest start``); the read of rows also needs the
session's worker (``SKIP_WORKER_TESTS=0``).
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import secrets
import uuid
from collections.abc import Iterator
from typing import Any

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, User
from flowfile_frame import _metadata
from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import added_catalog, clean_aws, kernel_of, namespace_on, principal, signed
from tests.notebook.conftest import IN_KERNEL_OP

pytestmark = pytest.mark.iceberg_rest

CATALOG_CELL_FLOW_ID = -123
WRITE_SENTENCE = (
    "A catalog table is not written from this notebook with ff.write_catalog_table: a table of Flowfile's own is "
    "written with flowfile_ctx.write_catalog_table(frame.collect(), ...), and a table kept by another catalog is "
    "written by a flow, with a Write to Catalog node."
)


@pytest.fixture
def schema(signed):
    """A namespace of this test's own on Polaris holding ``orders``, 1000 rows PyIceberg wrote; dropped after."""
    from shared.iceberg_utils import write_table

    with namespace_on(signed, "cell") as name:
        frame = pl.LazyFrame({"id": list(range(1000))}, schema={"id": pl.Int64})
        made = signed.create_table(
            f"{name}.orders", schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema
        )
        write_table(made, frame, "append")
        yield name


@contextlib.contextmanager
def _added(principal, schema: str) -> Iterator[str]:
    """Polaris added to Flowfile's catalog by user 1 with ``principal``'s client id and secret: ``catalog.schema``."""
    with added_catalog(principal) as (name, _, _):
        yield f"{name}.{schema}"


@pytest.fixture
def polaris(principal, schema):
    """Polaris added to Flowfile's catalog by user 1 with a client id and secret: ``catalog.schema``."""
    with _added(principal, schema) as where:
        yield where


@pytest.fixture
def described_only(signed, schema):
    """Polaris added with a principal that may read a table's description and not its data: ``catalog.schema``."""
    reader = fixtures.new_principal(privilege="TABLE_READ_PROPERTIES")
    try:
        for privilege in ("NAMESPACE_LIST", "TABLE_LIST"):
            fixtures._manage(
                "PUT",
                f"/catalogs/{fixtures.SIGNED_WAREHOUSE}/catalog-roles/{reader.name}_cat/grants",
                fixtures._root_headers(),
                {"grant": {"type": "catalog", "privilege": privilege}},
            )
        with _added(reader, schema) as where:
            yield where
    finally:
        fixtures.drop_principal(reader.name)


def _runtime_transport(client: TestClient, between=None):
    """The catalog-cell lookup of ``kernel_runtime/catalog_lookup.py`` for ``catalog_table``, the namespaces and
    ``tables``, over ``client``.

    A resolve's 404 is ``None`` and then a ``LookupError``; any other refusal is ``raise_for_status``'s
    ``httpx.HTTPStatusError``, as ``flowfile_client._resolve_catalog_table`` raises it. Each call runs in a fresh
    context, as core's answer to a kernel runs in another process. ``between`` runs after the resolve and before
    the materialize, for what changes in that moment.
    """

    def get(path: str, **params) -> Any:
        resp = client.get(f"/catalog/{path}", params={k: v for k, v in params.items() if v is not None})
        resp.raise_for_status()
        return resp.json()

    def namespaces(parent_id=None) -> list[dict]:
        return [{key: row[key] for key in ("id", "name", "parent_id")} for row in get("namespaces", parent_id=parent_id)]

    def ask(payload: dict[str, Any]) -> dict[str, Any]:
        kind, args = payload["kind"], payload.get("args", {})
        if kind == "namespaces":
            return {"result": namespaces(args.get("parent_id"))}
        if kind == "namespace_by_name":
            rows = namespaces(args.get("parent_id"))
            return {"result": next((row for row in rows if row["name"] == args["name"]), None)}
        if kind == "tables":
            return {"result": get("tables", namespace_id=args["namespace_id"])}
        if kind != "catalog_table":
            raise NotImplementedError(f"Catalog-cell metadata lookup {kind!r} is not supported")
        reference = args.get("catalog_full_table_name") or args.get("catalog_table_name")
        params: dict[str, Any] = {"q": reference, "strict": "true"}
        if args.get("catalog_namespace_id") is not None:
            params["namespace_id"] = args["catalog_namespace_id"]
        resp = client.get("/catalog/tables/resolve", params=params)
        if resp.status_code == 404:
            raise LookupError(f"Catalog table {reference!r} not found")
        resp.raise_for_status()
        row = resp.json()["table"]
        result = {
            "file_path": row.get("file_path") if row["table_type"] != "virtual" else None,
            "table_type": row["table_type"],
            "producer_fingerprint": None,
            "is_optimized": False,
            "source_table_versions": row.get("source_table_versions"),
            "authorized": True,
            "table_id": row["id"],
            "namespace_id": row.get("namespace_id"),
            "table_name": row["name"],
            "scd2_config": row.get("scd2"),
            "schema_json": json.dumps(row.get("schema_columns", [])),
            "cdc_enabled": row.get("cdc_enabled", False),
            "cdc_enabled_version": row.get("cdc_enabled_version"),
        }
        if row["table_type"] == "virtual":
            if between is not None:
                between()
            made = client.post(f"/catalog/tables/{row['id']}/materialize", timeout=600.0)
            if made.status_code >= 400:
                raise RuntimeError(f"Materializing catalog view '{row['name']}' failed: {made.text}")
            result["materialized"] = {"path": made.json()["path"], "format": made.json().get("format", "ipc")}
        return {"result": result}

    return lambda payload: contextvars.Context().run(ask, payload)


@contextlib.contextmanager
def _catalog_cell(client: TestClient, between=None) -> Iterator[None]:
    """A catalog page's cell: the hook over the runtime's transport, the cell's work marked as the kernel's."""
    marked = IN_KERNEL_OP.set(True)
    try:
        with _metadata.installed(CATALOG_CELL_FLOW_ID, _runtime_transport(client, between)):
            yield
    finally:
        IN_KERNEL_OP.reset(marked)


def test_a_catalog_cell_reads_an_outside_table(kernel_of, polaris, kernel_db_opens, running_worker):
    import flowfile as ff

    with _catalog_cell(kernel_of(1)):
        height = ff.read_catalog_table("orders", namespace_full_name=polaris).collect().height

    assert height == 1000
    assert kernel_db_opens == []


def test_a_catalog_cell_lists_an_outside_schemas_tables_as_the_other_catalog_has_them(
    kernel_of, polaris, signed, kernel_db_opens
):
    import pyarrow as pa

    import flowfile as ff

    catalog, schema = polaris.split(".", 1)
    kernel = kernel_of(1)

    def cell(listing):
        with _catalog_cell(kernel):
            return [t.qualified_name for t in listing()]

    first = cell(lambda: ff.get_catalog(catalog).get_schema(schema).list_tables())
    signed.create_table(f"{schema}.later", schema=pa.schema([("id", pa.int64())]))
    second = cell(lambda: ff.get_catalog(catalog).get_schema(schema).list_tables())
    flat = cell(lambda: ff.get_catalog(catalog).list_tables())

    assert first == [f"{polaris}.orders"]
    assert second == [f"{polaris}.later", f"{polaris}.orders"]
    assert set(second) <= set(flat)
    assert kernel_db_opens == []


def _says_no_status_line(message: str) -> bool:
    return not any(text in message for text in ("422", "Client error", "Unprocessable", "/catalog/tables"))


def test_a_catalog_cell_listing_a_schema_it_has_no_use_on_raises_the_access_sentence(
    kernel_of, polaris, kernel_db_opens, monkeypatch
):
    """The catalog and its schema are public, so the stranger sees them, and Use is still not theirs."""
    import flowfile as ff

    catalog, schema = polaris.split(".", 1)
    with get_db_context() as db:
        catalog_row = db.query(CatalogNamespace).filter_by(name=catalog, parent_id=None).one()
        catalog_row.is_public = True
        db.query(CatalogNamespace).filter_by(parent_id=catalog_row.id, name=schema).one().is_public = True
        user = User(username=f"cell_stranger_{uuid.uuid4().hex[:6]}", email=f"{uuid.uuid4().hex[:8]}@example.invalid",
                    hashed_password="not-a-login", disabled=False, is_admin=False)
        db.add(user)
        db.commit()
        stranger_id = user.id
    try:
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_urlsafe(48))
        with _catalog_cell(kernel_of(stranger_id)), pytest.raises(ValueError) as refused:
            ff.get_catalog(catalog).get_schema(schema).list_tables()
    finally:
        with get_db_context() as db:
            db.delete(db.get(User, stranger_id))
            db.commit()

    assert str(refused.value) == f"You have no access to {polaris}; ask its owner to share it."
    assert _says_no_status_line(str(refused.value))
    assert kernel_db_opens == []


def test_a_catalog_cell_listing_a_catalog_that_cannot_be_reached_raises_its_sentence(
    kernel_of, polaris, kernel_db_opens
):
    import flowfile as ff
    from flowfile_core.database.models import ExternalCatalog
    from tests.conftest import closed_url

    catalog, schema = polaris.split(".", 1)
    closed = f"{closed_url()}/api/catalog"
    with get_db_context() as db:
        catalog_id = db.query(CatalogNamespace).filter_by(name=catalog, parent_id=None).one().id
        db.query(ExternalCatalog).filter_by(namespace_id=catalog_id).one().uri = closed
        db.commit()

    with _catalog_cell(kernel_of(1)), pytest.raises(ValueError) as refused:
        ff.get_catalog(catalog).get_schema(schema).list_tables()

    assert str(refused.value) == f"No catalog answers at {closed}."
    assert _says_no_status_line(str(refused.value))
    assert kernel_db_opens == []


def test_a_mistyped_table_is_core_s_sentence_not_a_status_line(kernel_of, polaris, kernel_db_opens):
    import flowfile as ff

    schema = polaris.split(".", 1)[1]
    with _catalog_cell(kernel_of(1)), pytest.raises(ValueError) as refused:
        ff.read_catalog_table("ordrs", namespace_full_name=polaris)

    assert str(refused.value) == f"The catalog at {fixtures.SIGNED_CATALOG_URI} has no table {schema}.ordrs."
    assert kernel_db_opens == []


def test_someone_without_use_gets_the_access_sentence(kernel_of, polaris, kernel_db_opens, monkeypatch):
    import flowfile as ff

    with get_db_context() as db:
        user = User(username=f"cell_stranger_{uuid.uuid4().hex[:6]}", email=f"{uuid.uuid4().hex[:8]}@example.invalid",
                    hashed_password="not-a-login", disabled=False, is_admin=False)
        db.add(user)
        db.commit()
        stranger_id = user.id
    try:
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_urlsafe(48))
        with _catalog_cell(kernel_of(stranger_id)), pytest.raises(ValueError) as refused:
            ff.read_catalog_table("orders", namespace_full_name=polaris)
    finally:
        with get_db_context() as db:
            db.delete(db.get(User, stranger_id))
            db.commit()

    assert str(refused.value) == f"You have no access to {polaris}; ask its owner to share it."
    assert kernel_db_opens == []


def test_what_the_worker_is_refused_while_writing_the_snapshot_is_the_plain_sentence(
    kernel_of, described_only, kernel_db_opens, running_worker
):
    """The catalog describes the table to core, then refuses the worker the keys: the cell says that refusal."""
    import flowfile as ff

    with _catalog_cell(kernel_of(1)), pytest.raises(ValueError) as refused:
        ff.read_catalog_table("orders", namespace_full_name=described_only)

    said = f"The catalog at {fixtures.SIGNED_CATALOG_URI} knows this sign-in and does not allow it this."
    assert str(refused.value) == said
    assert kernel_db_opens == []


def test_a_table_gone_between_the_resolve_and_the_snapshot_is_core_s_sentence(
    kernel_of, polaris, signed, kernel_db_opens
):
    """Core resolves the table, then the other catalog drops it before the snapshot: core's materialize refuses with
    its sentence, which the cell says plainly instead of the runtime's status line."""
    import flowfile as ff

    schema = polaris.split(".", 1)[1]
    with _catalog_cell(kernel_of(1), between=lambda: signed.drop_table(f"{schema}.orders")), pytest.raises(
        ValueError
    ) as refused:
        ff.read_catalog_table("orders", namespace_full_name=polaris)

    assert str(refused.value) == f"The catalog at {fixtures.SIGNED_CATALOG_URI} has no table {schema}.orders."
    assert kernel_db_opens == []


def test_any_other_failure_of_the_snapshot_is_left_as_the_runtime_raised_it():
    from flowfile_core.flowfile.flow_graph.catalog_resolution import _core_refusal

    others = [
        RuntimeError("Materializing catalog view 'orders' failed: Internal Server Error"),
        RuntimeError('Materializing catalog view \'orders\' failed: {"detail":[{"msg":"field required"}]}'),
        RuntimeError("Unsupported materialization format 'csv' for catalog view 'orders'"),
        PermissionError("Not authorized to read catalog view 'orders'"),
    ]

    assert [_core_refusal(error) for error in others] == [None] * len(others)


def test_a_table_nowhere_stays_a_lookup_error(kernel_of, kernel_db_opens):
    import flowfile as ff

    missing = f"nowhere_{uuid.uuid4().hex[:6]}"
    with _catalog_cell(kernel_of(1)), pytest.raises(LookupError) as refused:
        ff.read_catalog_table(missing, namespace_full_name="General.default")

    assert str(refused.value) == f"Catalog table 'General.default.{missing}' not found"
    assert kernel_db_opens == []


def test_a_write_to_an_outside_table_says_where_it_is_written_and_writes_nothing(
    kernel_of, polaris, signed, kernel_db_opens
):
    import flowfile as ff

    schema = polaris.split(".", 1)[1]
    with _catalog_cell(kernel_of(1)), pytest.raises(ValueError) as refused:
        frame = ff.from_dict({"id": [1, 2, 3]})
        ff.write_catalog_table(frame, "orders_m", namespace_full_name=polaris, write_mode="append")

    assert str(refused.value) == WRITE_SENTENCE
    assert not signed.table_exists(f"{schema}.orders_m")
    assert kernel_db_opens == []


@pytest.mark.parametrize("write_mode", ["append", "virtual"])
def test_a_write_to_a_table_of_flowfiles_own_says_the_same(kernel_of, kernel_db_opens, write_mode):
    import flowfile as ff

    table = f"own_{uuid.uuid4().hex[:6]}"
    with _catalog_cell(kernel_of(1)), pytest.raises(ValueError) as refused:
        frame = ff.from_dict({"id": [1, 2, 3]})
        ff.write_catalog_table(frame, table, namespace_full_name="General.default", write_mode=write_mode)

    assert str(refused.value) == WRITE_SENTENCE
    assert "outside" not in str(refused.value)
    assert kernel_db_opens == []
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(name=table).count() == 0
