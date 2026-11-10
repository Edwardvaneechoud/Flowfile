"""A catalog table's storage is resolved through one function, and the preview's data functions take a row and a target.

``resolve_for_table`` answers by the table's own path: a table stays where it was written. The preview and the Delta
history are read from a table's row, in core's own schema, and its target, without the catalog database. The MinIO
tests read the object store ``test_utils.s3.fixtures`` names (``TEST_MINIO_PORT``).
"""

import uuid

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.previews import TablePreviewService
from flowfile_core.catalog.storage_backend import join_catalog_uri, resolve_for_namespace, resolve_for_table
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.catalog_schema import CatalogTableOut
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.delta_utils import write_delta
from shared.storage_config import storage
from test_utils.s3.fixtures import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT_URL,
    MINIO_SECRET_KEY,
    get_minio_client,
    is_docker_available,
)
from tests.flowfile.conftest import catalog_cleanup, create_test_graph, run_test_graph

_CONNECTION_NAME = "catalog-table-target-minio"
_BUCKET = "flowfile-test"
LOCAL = ExecutionContext(registry.LOCAL_BACKEND_ID, parallelism=1, preview_rows=None)
WORKER = ExecutionContext(registry.WORKER_BACKEND_ID, parallelism=1, preview_rows=None)


def _minio_available() -> bool:
    if not is_docker_available():
        return False
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
    catalog_cleanup()
    yield
    catalog_cleanup()


def _ensure_connection() -> None:
    connection = FullCloudStorageConnection(
        connection_name=_CONNECTION_NAME,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=SecretStr(MINIO_SECRET_KEY),
        aws_region="us-east-1",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_allow_unsafe_html=True,
    )
    with get_db_context() as db:
        try:
            store_cloud_connection(db, connection, user_id=1)
            db.commit()
        except ValueError as e:
            if "already exists" not in str(e):
                raise
            db.rollback()


def _schema(storage_uri: str | None = None) -> int:
    """A catalog, on object storage at ``storage_uri`` or on local disk, with one schema; the schema's id."""
    with get_db_context() as db:
        namespaces = NamespaceService(SQLAlchemyCatalogRepository(db))
        catalog = namespaces.create_namespace(
            f"target_cat_{uuid.uuid4().hex[:8]}",
            owner_id=1,
            storage_uri=storage_uri,
            storage_connection_name=_CONNECTION_NAME if storage_uri else None,
        )
        return namespaces.create_namespace(f"target_sch_{uuid.uuid4().hex[:8]}", owner_id=1, parent_id=catalog.id).id


def _table(namespace_id: int, name: str, file_path: str, row_count: int | None = None) -> int:
    with get_db_context() as db:
        table = db_models.CatalogTable(
            name=name,
            namespace_id=namespace_id,
            owner_id=1,
            file_path=file_path,
            table_type="physical",
            row_count=row_count,
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id


def _two_versions(path: str, storage_options: dict | None = None) -> None:
    """A Delta table at ``path`` of three rows at version 0 and five at version 1."""
    write_delta(pl.DataFrame({"id": [1, 2, 3]}), path, mode="overwrite", storage_options=storage_options)
    write_delta(pl.DataFrame({"id": [1, 2, 3, 4, 5]}), path, mode="overwrite", storage_options=storage_options)


def _by_id_and_by_row(table_id: int):
    """The latest preview, the preview of version 0 and the history of a table: as the service reads them by id, and
    as the data functions read them from the table's ``CatalogTableOut`` and target on a service with no repository."""
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        row = service.get_table(table_id)
        by_id = (
            service.get_table_preview(table_id, limit=10),
            service.get_table_preview(table_id, limit=10, version=0),
            service.get_table_history(table_id),
        )
    assert isinstance(row, CatalogTableOut)
    target = resolve_for_table(row.file_path, row.namespace_id)
    previews = TablePreviewService(repo=None, tables=None, virtual_tables=None)
    by_row = (
        previews.physical_table_preview(row, target, 10, None),
        previews.physical_table_preview(row, target, 10, 0),
        previews.table_history(row, target),
    )
    return by_id, by_row


def test_a_tables_own_path_decides_its_target():
    _ensure_connection()
    uri = f"s3://{_BUCKET}/target_{uuid.uuid4().hex[:8]}"
    on_object_storage, on_disk = _schema(uri), _schema()

    stayed_local = resolve_for_table(str(storage.catalog_tables_directory / "t_ab12"), on_object_storage)
    assert stayed_local.is_cloud is False
    assert stayed_local.base == str(storage.catalog_tables_directory)
    assert stayed_local.to_worker_payload() is None
    assert stayed_local.storage_options == {}
    assert stayed_local.scan_kwargs(1) == {} and stayed_local.scan_kwargs(None) == {}
    assert resolve_for_table(None, on_object_storage) == stayed_local

    cloud = resolve_for_table(f"{uri}/t_ab12", on_object_storage)
    of_namespace = resolve_for_namespace(on_object_storage)
    assert cloud.is_cloud is True
    assert (cloud.base, cloud.connection_name) == (uri, _CONNECTION_NAME)
    assert cloud.storage_options == of_namespace.storage_options
    assert cloud.credential_ref == of_namespace.credential_ref
    assert cloud.to_worker_payload()["base_uri"] == uri

    assert resolve_for_table(f"{uri}/t_ab12", on_disk) == resolve_for_namespace(on_disk)


def test_a_local_table_is_previewed_from_its_row_and_target_without_the_database(tmp_path):
    path = str(tmp_path / "people")
    _two_versions(path)
    table_id = _table(_schema(), "people", path, row_count=5)

    with use_context(LOCAL):
        by_id, by_row = _by_id_and_by_row(table_id)

    assert by_row == by_id
    latest, first, history = by_row
    assert (len(latest.rows), latest.total_rows, len(first.rows)) == (5, 5, 3)
    assert history.current_version == 1 and len(history.history) == 2


@requires_minio
@pytest.mark.parametrize("context", [LOCAL, WORKER], ids=["read in this process", "read by the worker"])
def test_an_object_storage_table_is_previewed_from_its_row_and_target(context, request):
    if context is WORKER:
        request.getfixturevalue("running_worker")
    _ensure_connection()
    uri = f"s3://{_BUCKET}/target_{uuid.uuid4().hex[:8]}"
    schema_id = _schema(uri)
    target = resolve_for_namespace(schema_id)
    path = join_catalog_uri(target.base, f"people_{uuid.uuid4().hex[:6]}")
    _two_versions(path, target.storage_options)
    table_id = _table(schema_id, "people", path, row_count=5)

    with use_context(context):
        by_id, by_row = _by_id_and_by_row(table_id)

    assert by_row == by_id
    latest, first, history = by_row
    assert (len(latest.rows), latest.total_rows, len(first.rows)) == (5, 5, 3)
    assert history.current_version == 1 and len(history.history) == 2


def _read(reader_settings: dict) -> pl.DataFrame:
    graph = create_test_graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader"))
    graph.add_catalog_reader(input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, **reader_settings))
    run_test_graph(graph)
    return graph.get_node(1).get_resulting_data().collect()


@requires_minio
def test_a_catalog_reader_scans_each_table_of_one_schema_where_it_was_written(tmp_path):
    _ensure_connection()
    uri = f"s3://{_BUCKET}/target_{uuid.uuid4().hex[:8]}"
    schema_id = _schema(uri)
    target = resolve_for_namespace(schema_id)
    cloud_path = join_catalog_uri(target.base, f"orders_{uuid.uuid4().hex[:6]}")
    write_delta(
        pl.DataFrame({"customer_id": [1, 2, 1], "amount": [100, 200, 150]}),
        cloud_path,
        storage_options=target.storage_options,
    )
    local_path = str(tmp_path / "customers")
    write_delta(pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}), local_path)
    orders_id = _table(schema_id, "target_orders", cloud_path)
    customers_id = _table(schema_id, "target_customers", local_path)

    assert _read({"catalog_table_id": orders_id}).sort("amount")["amount"].to_list() == [100, 150, 200]
    assert _read({"catalog_table_id": customers_id}).sort("id")["name"].to_list() == ["Alice", "Bob"]
    joined = _read(
        {
            "sql_query": (
                "SELECT c.name, SUM(o.amount) AS total FROM target_customers c "
                "JOIN target_orders o ON c.id = o.customer_id GROUP BY c.name"
            )
        }
    )
    assert dict(zip(joined["name"].to_list(), joined["total"].to_list(), strict=True)) == {"Alice": 250, "Bob": 200}
