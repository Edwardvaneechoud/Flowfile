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

from flowfile_core import main
from flowfile_core.auth.jwt import create_kernel_token
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, Kernel, User
from flowfile_frame import _metadata
from test_utils.iceberg_rest import fixtures
from tests.notebook.conftest import IN_KERNEL_OP

pytestmark = pytest.mark.iceberg_rest

CATALOG_CELL_FLOW_ID = -123
WRITE_SENTENCE = (
    "A catalog table is not written from this notebook with ff.write_catalog_table: a table of Flowfile's own is "
    "written with flowfile_ctx.write_catalog_table(frame.collect(), ...), and a table kept by another catalog is "
    "written by a flow, with a Write to Catalog node."
)


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


@pytest.fixture
def schema(signed):
    """A namespace of this test's own on Polaris holding ``orders``, 1000 rows PyIceberg wrote; dropped after."""
    from shared.iceberg_utils import write_table

    name = f"cell_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    frame = pl.LazyFrame({"id": list(range(1000))}, schema={"id": pl.Int64})
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


@contextlib.contextmanager
def _added(principal, schema: str) -> Iterator[str]:
    """Polaris added to Flowfile's catalog by user 1 with ``principal``'s client id and secret: ``catalog.schema``."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    name = f"polaris_{uuid.uuid4().hex[:6]}"
    body = ExternalCatalogCreate(
        name=name,
        uri=fixtures.SIGNED_CATALOG_URI,
        warehouse=fixtures.SIGNED_WAREHOUSE,
        sign_in="client_credentials",
        client_id=principal.client_id,
        client_secret=principal.client_secret,
        scope=fixtures.SIGNED_SCOPE,
    )
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(body, 1)
    try:
        yield f"{name}.{schema}"
    finally:
        with get_db_context() as db:
            row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
            if row is not None:
                CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


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


@pytest.fixture
def kernel_of():
    """A TestClient signed as a running kernel of a user, with that kernel's own token; the rows go after."""
    made: list[str] = []

    def _client(user_id: int) -> TestClient:
        kernel_id, token_id = f"cell-kernel-{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
        with get_db_context() as db:
            db.add(Kernel(id=kernel_id, name="Catalog cell", user_id=user_id, token_id=token_id))
            db.commit()
        made.append(kernel_id)
        client = TestClient(main.app)
        client.headers = {"X-Internal-Token": create_kernel_token(user_id, kernel_id, token_id), "X-Kernel-Id": kernel_id}
        return client

    yield _client
    with get_db_context() as db:
        db.query(Kernel).filter(Kernel.id.in_(made)).delete(synchronize_session=False)
        db.commit()


def _runtime_transport(client: TestClient, between=None):
    """The catalog-cell lookup of ``kernel_runtime/catalog_lookup.py`` for ``catalog_table``, over ``client``.

    A resolve's 404 is ``None`` and then a ``LookupError``; any other refusal is ``raise_for_status``'s
    ``httpx.HTTPStatusError``, as ``flowfile_client._resolve_catalog_table`` raises it. Each call runs in a fresh
    context, as core's answer to a kernel runs in another process. ``between`` runs after the resolve and before
    the materialize, for what changes in that moment.
    """

    def ask(payload: dict[str, Any]) -> dict[str, Any]:
        kind, args = payload["kind"], payload.get("args", {})
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
