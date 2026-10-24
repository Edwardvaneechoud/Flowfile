"""The catalog's compute: the catalog writer's Delta write runs on the execution context's backend, and the catalog
services send their table I/O to the worker exactly when that backend is not the worker embedded in this process, a
worker they cannot reach answering ``ComputeUnavailable`` and never a read in this process.

The writer's expected reports are what its in-process Delta write returned before the write moved behind the
backend, recorded from that code on the same inputs; only ``size_bytes`` is measured here, the way that code
measured it, since a file's byte size belongs to the Parquet writer of the day.
"""

import dataclasses
import datetime
import shutil
import types
import uuid
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog import compute as catalog_compute
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import previews as previews_module
from flowfile_core.catalog.services import tables as tables_module
from flowfile_core.catalog.services.previews import TablePreviewService
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.storage_backend import resolve_for_namespace
from flowfile_core.configs import settings
from flowfile_core.configs.utils import MutableBool
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.execution.payloads import (
    PAYLOAD_KEY,
    CatalogTableIdentity,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import JobHandle, OperationKind, OpResult
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_graph.catalog_write import (
    CatalogDeltaWrite,
    _catalog_delta_op,
    _catalog_delta_written,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.catalog_schema import CatalogTablePreview
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.delta_utils import get_delta_size_bytes
from shared.storage_config import storage
from tests.flowfile.conftest import (
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
    run_test_graph,
)
from tests.flowfile.execution.test_results_exists_unreachable import _closed_url
from tests.test_catalog_cloud_virtual import (
    _BUCKET,
    _create_cloud_schema,
    _ensure_connection,
    get_minio_client,
    requires_minio,
)

pytestmark = pytest.mark.core

LOCAL = ExecutionContext(registry.LOCAL_BACKEND_ID, parallelism=1, preview_rows=None)
WORKER = ExecutionContext(registry.WORKER_BACKEND_ID, parallelism=1, preview_rows=None)
UTC = datetime.timezone.utc
RUN_TIMESTAMP = "2024-01-01T00:00:00+00:00"
SCD2_CONFIG = {
    "business_keys": ["id"],
    "surrogate_key_column": "sk",
    "valid_from_column": "valid_from",
    "valid_to_column": "valid_to",
    "is_current_column": "is_current",
    "compare_columns": ["val"],
    "full_snapshot": False,
    "partition_on_current": True,
}
SCD2_TABLE_SCHEMA = [
    {"name": "id", "dtype": "Int64"},
    {"name": "val", "dtype": "String"},
    {"name": "qty", "dtype": "Int64"},
    {"name": "sk", "dtype": "String"},
    {"name": "valid_from", "dtype": "Datetime(time_unit='us', time_zone='UTC')"},
    {"name": "valid_to", "dtype": "Datetime(time_unit='us', time_zone='UTC')"},
    {"name": "is_current", "dtype": "Boolean"},
]
PEOPLE_SCHEMA = [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]


def make_graph(flow_id: int, execution_location: str) -> FlowGraph:
    return FlowGraph(
        schemas.FlowSettings(
            flow_id=flow_id, name=f"catalog_compute_{flow_id}", path=".", execution_location=execution_location
        )
    )


def writer_settings(write_mode: str, **fields) -> input_schema.CatalogWriteSettings:
    return input_schema.CatalogWriteSettings(table_name="t", write_mode=write_mode, **fields)


def s3_connection() -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        connection_name="catalog-compute",
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="key-id",
        aws_secret_access_key="plain-s3-secret",
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
    )


# The decision the catalog services make


def test_the_core_backend_keeps_catalog_io_in_core():
    with use_context(LOCAL):
        assert catalog_compute.uses_worker() is False


def test_the_worker_backend_sends_catalog_io_to_the_worker():
    with use_context(WORKER):
        assert catalog_compute.uses_worker() is True


@pytest.mark.parametrize("configured, expected", [("local", False), ("worker", True)])
def test_without_a_context_the_configured_default_backend_decides(monkeypatch, configured, expected):
    monkeypatch.setenv(registry.BACKEND_ENV_VAR, configured)
    monkeypatch.setattr(settings, "OFFLOAD_TO_WORKER", MutableBool(not expected))

    assert catalog_compute.uses_worker() is expected


@pytest.mark.parametrize("offload", [True, False])
def test_without_a_context_or_a_configured_backend_the_offload_setting_decides(monkeypatch, offload):
    monkeypatch.delenv(registry.BACKEND_ENV_VAR, raising=False)
    monkeypatch.setattr(settings, "OFFLOAD_TO_WORKER", MutableBool(offload))

    assert catalog_compute.uses_worker() is offload


def test_a_local_flow_keeps_catalog_io_in_core_while_offload_is_on(monkeypatch):
    monkeypatch.delenv(registry.BACKEND_ENV_VAR, raising=False)
    monkeypatch.setattr(settings, "OFFLOAD_TO_WORKER", MutableBool(True))

    with use_context(make_graph(9301, "local").execution_context):
        assert catalog_compute.uses_worker() is False
    assert catalog_compute.uses_worker() is True


@pytest.fixture
def probe_backend():
    """A registered backend that is neither core's own nor the worker."""
    registry.reset()
    registry.register_backend("probe", lambda: types.SimpleNamespace(id="probe"))
    yield ExecutionContext("probe", parallelism=1, preview_rows=None)
    registry.reset()


def test_any_backend_but_the_core_one_sends_catalog_io_out(probe_backend):
    with use_context(probe_backend):
        assert catalog_compute.uses_worker() is True


@pytest.fixture
def delta_table():
    """A Delta table under the managed catalog directory, whose bare name the worker's catalog routes resolve."""
    path = storage.catalog_tables_directory / f"people_{uuid.uuid4().hex[:8]}"
    pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]}).write_delta(str(path))
    yield str(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def external_delta_table(tmp_path) -> str:
    """A Delta table registered where it lies, outside the managed catalog directory."""
    path = tmp_path / "people"
    pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]}).write_delta(str(path))
    return str(path)


def test_a_metadata_read_runs_where_the_context_says(delta_table, monkeypatch):
    asked = []

    def worker_read(table_name, storage=None, *, worker_url):
        asked.append((table_name, storage, worker_url))
        return {
            "column_schema": [{"name": "id", "dtype": "Int64"}],
            "row_count": 99,
            "column_count": 1,
            "size_bytes": 7,
        }

    monkeypatch.setattr(tables_module, "trigger_read_table_metadata", worker_read)

    with use_context(LOCAL):
        in_core = TableService._read_table_metadata(delta_table, "delta")
    with use_context(WORKER):
        on_worker = TableService._read_table_metadata(delta_table, "delta")

    assert in_core[1:3] == (3, 2)
    assert on_worker == ([{"name": "id", "dtype": "Int64"}], 99, 1, 7)
    assert asked == [(Path(delta_table).name, None, registry.worker_url())]


def test_a_preview_runs_where_the_context_says(delta_table, monkeypatch):
    asked = []

    def worker_preview(table_name, n_rows, storage=None, *, worker_url):
        asked.append((table_name, n_rows, worker_url))
        return CatalogTablePreview(columns=["id"], dtypes=["Int64"], rows=[[42]], total_rows=1)

    monkeypatch.setattr(previews_module, "trigger_delta_preview", worker_preview)
    service = TablePreviewService(repo=None, tables=None, virtual_tables=None)

    with use_context(LOCAL):
        in_core = service._get_delta_preview(delta_table, 2)
    with use_context(WORKER):
        on_worker = service._get_delta_preview(delta_table, 2)

    assert in_core.rows == [[1, "ada"], [2, "grace"]]
    assert on_worker.rows == [[42]]
    assert asked == [(Path(delta_table).name, 2, registry.worker_url())]


def previews_of(table_path: str) -> TablePreviewService:
    """The preview service over a repository that holds one local table at ``table_path``."""
    table = types.SimpleNamespace(file_path=table_path, namespace_id=None)
    return TablePreviewService(repo=types.SimpleNamespace(get_table=lambda _: table), tables=None, virtual_tables=None)


CATALOG_READS = {
    "metadata": lambda path: TableService._read_table_metadata(path, "delta"),
    "preview": lambda path: previews_of(path)._get_delta_preview(path, 2),
    "version preview": lambda path: previews_of(path)._get_delta_version_preview(path, 0, 2),
    "history": lambda path: previews_of(path).get_table_history(1),
}


@pytest.fixture(params=["refused at the handshake", "refused once the handshake passed"])
def stopped_worker(request, monkeypatch) -> str:
    """The registry's worker pointed at a port nothing listens on; returns its URL.

    Refused by the handshake that checks a worker before the first call to it, or by the call itself while an earlier
    verdict on it stands.
    """
    url = _closed_url()
    monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", url)
    if request.param == "refused once the handshake passed":
        monkeypatch.setattr(subprocess_operations, "ensure_handshake", lambda *args, **kwargs: None)
    return url


@pytest.mark.parametrize("read", CATALOG_READS)
def test_a_catalog_read_on_a_stopped_worker_is_compute_unavailable_and_never_read_in_core(
    read, delta_table, stopped_worker
):
    with use_context(WORKER), pytest.raises(ComputeUnavailable) as excinfo:
        CATALOG_READS[read](delta_table)

    assert str(excinfo.value).startswith(f"Could not reach the worker at {stopped_worker}")


@pytest.mark.parametrize("read", ["metadata", "version preview", "history"])
def test_an_external_table_is_read_in_this_process_whatever_the_context(read, external_delta_table, monkeypatch):
    def refused(*args, **kwargs):
        pytest.fail("the worker was asked for a table outside the managed catalog directory")

    monkeypatch.setattr(tables_module, "trigger_read_table_metadata", refused)
    monkeypatch.setattr(previews_module, "trigger_delta_version_preview", refused)
    monkeypatch.setattr(previews_module, "trigger_delta_history", refused)

    with use_context(WORKER):
        on_worker_context = CATALOG_READS[read](external_delta_table)
    with use_context(LOCAL):
        on_local_context = CATALOG_READS[read](external_delta_table)

    assert on_worker_context == on_local_context


@pytest.fixture
def catalog_api(monkeypatch):
    """A client of the app whose catalog I/O goes to the worker, and a namespace to register tables in."""
    monkeypatch.delenv(registry.BACKEND_ENV_VAR, raising=False)
    monkeypatch.setattr(settings, "OFFLOAD_TO_WORKER", MutableBool(True))
    catalog_cleanup()
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {client.post('/auth/token').json()['access_token']}"}
    yield client, create_test_namespace()
    catalog_cleanup()


def register_physical(namespace_id: int, file_path: str) -> int:
    table = CatalogTable(name="people", namespace_id=namespace_id, owner_id=1, file_path=file_path)
    with get_db_context() as db:
        return SQLAlchemyCatalogRepository(db).create_table(table).id


@pytest.mark.parametrize("route", ["/history", "/preview?version=0"])
def test_a_catalog_route_on_a_stopped_worker_answers_503_naming_no_address(
    route, catalog_api, delta_table, stopped_worker, caplog, monkeypatch
):
    from flowfile_core.configs import logger

    monkeypatch.setattr(logger, "propagate", True)
    client, namespace_id = catalog_api
    table_id = register_physical(namespace_id, delta_table)

    response = client.get(f"/catalog/tables/{table_id}{route}")

    assert response.status_code == 503
    assert response.json() == {"detail": "Compute unavailable"}
    assert stopped_worker.split("://", 1)[1] not in response.text
    assert f"Could not reach the worker at {stopped_worker}" in caplog.text


# The catalog writer's operation and payload


def payload_fields(payload) -> dict:
    """The payload's fields but ``plan``, compared by identity instead: a LazyFrame has no equality."""
    return {f.name: getattr(payload, f.name) for f in dataclasses.fields(payload) if f.name != "plan"}


@pytest.mark.parametrize("write_mode", ["overwrite", "append", "error"])
def test_a_plain_write_mode_is_a_delta_write(write_mode):
    plan = pl.LazyFrame({"id": [1]})
    settings_ = writer_settings(write_mode, partition_by=["id"], track_changes=write_mode == "append")
    connection = s3_connection().get_worker_interface(1)

    operation, write = _catalog_delta_op(
        write_mode,
        settings_,
        plan=plan,
        path="s3://bucket/catalog/t_1",
        connection=connection,
        scd2_config=None,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    assert operation is OperationKind.delta_write
    assert write.plan is plan
    assert payload_fields(write) == payload_fields(
        DeltaWritePayload(
            plan=plan,
            path="s3://bucket/catalog/t_1",
            mode=write_mode,
            partition_by=["id"],
            enable_cdf=write_mode == "append",
            connection=connection,
            flow_id=4,
            node_id=2,
            job_id=write.job_id,
        )
    )
    assert write.connection.aws_secret_access_key.startswith("$ffsec$")


@pytest.mark.parametrize("write_mode", ["upsert", "update", "delete"])
def test_a_merge_mode_is_a_delta_merge_by_the_merge_keys(write_mode):
    plan = pl.LazyFrame({"id": [1]})

    operation, write = _catalog_delta_op(
        write_mode,
        writer_settings(write_mode, merge_keys=["id"], track_changes=True),
        plan=plan,
        path="/catalog/t_1",
        connection=None,
        scd2_config=None,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    assert operation is OperationKind.delta_merge
    assert write.plan is plan
    assert payload_fields(write) == payload_fields(
        DeltaMergePayload(
            plan=plan,
            path="/catalog/t_1",
            merge_mode=write_mode,
            merge_keys=["id"],
            partition_by=[],
            enable_cdf=True,
            connection=None,
            flow_id=4,
            node_id=2,
            job_id=write.job_id,
        )
    )


def test_an_scd2_write_carries_the_resolved_config_as_typed_fields():
    plan = pl.LazyFrame({"id": [1], "val": ["a"]})
    config = {**SCD2_CONFIG, "full_snapshot": True, "partition_on_current": False}

    operation, write = _catalog_delta_op(
        "scd2",
        writer_settings("scd2", merge_keys=["id"], partition_by=["val"]),
        plan=plan,
        path="/catalog/t_1",
        connection=None,
        scd2_config=config,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    assert operation is OperationKind.delta_scd2
    assert write.plan is plan
    assert payload_fields(write) == payload_fields(
        DeltaScd2Payload(
            plan=plan,
            path="/catalog/t_1",
            business_keys=["id"],
            compare_columns=["val"],
            full_snapshot=True,
            surrogate_key_column="sk",
            valid_from_column="valid_from",
            valid_to_column="valid_to",
            is_current_column="is_current",
            run_timestamp=RUN_TIMESTAMP,
            partition_by=["val"],
            partition_on_current=False,
            connection=None,
            flow_id=4,
            node_id=2,
            job_id=write.job_id,
        )
    )


def test_an_scd2_config_without_the_creation_knob_partitions_on_the_current_flag():
    persisted_shape = {key: value for key, value in SCD2_CONFIG.items() if key != "partition_on_current"}

    _, write = _catalog_delta_op(
        "scd2",
        writer_settings("scd2", merge_keys=["id"]),
        plan=pl.LazyFrame({"id": [1], "val": ["a"]}),
        path="/catalog/t_1",
        connection=None,
        scd2_config=persisted_shape,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    assert write.partition_on_current is True


def test_the_embedded_worker_opens_a_cloud_table_with_the_options_the_worker_resolves(monkeypatch):
    from flowfile_worker.external_sources.s3_source.models import FullCloudStorageConnection as WorkerConnection

    received = []
    monkeypatch.setattr(
        "shared.delta_utils.write_delta", lambda *args, **kwargs: received.append(kwargs["storage_options"]) or False
    )
    connection = s3_connection()
    _, write = _catalog_delta_op(
        "overwrite",
        writer_settings("overwrite"),
        plan=pl.LazyFrame({"id": [1]}),
        path="s3://bucket/catalog/t_1",
        connection=connection.get_worker_interface(1),
        scd2_config=None,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    EmbeddedWorkerBackend().run_op(OperationKind.delta_write, op_payload(write))

    sent = WorkerConnection.model_validate(connection.get_worker_interface(1).model_dump())
    assert received == [sent.get_storage_options()]
    assert received[0]["aws_secret_access_key"] == "plain-s3-secret"


def test_every_write_gets_its_own_job_id():
    def job_id() -> str:
        _, write = _catalog_delta_op(
            "overwrite",
            writer_settings("overwrite"),
            plan=pl.LazyFrame({"id": [1]}),
            path="/catalog/t_1",
            connection=None,
            scd2_config=None,
            run_timestamp=RUN_TIMESTAMP,
            flow_id=4,
            node_id=2,
        )
        return write.job_id

    assert job_id() != job_id()


# What the writer reads from a report


def test_a_worker_scd2_report_becomes_the_table_metadata_and_version():
    report = {
        "table_path": "/catalog/t_1",
        "schema": SCD2_TABLE_SCHEMA,
        "row_count": 5,
        "column_count": 7,
        "size_bytes": 4254,
        "version": 1,
        "scd2_metrics": {"rows_inserted": 2, "rows_closed": 1, "rows_total": 5, "rows_current": 4, "created": False},
    }

    assert _catalog_delta_written(report) == CatalogDeltaWrite(
        {
            "schema": SCD2_TABLE_SCHEMA,
            "row_count": 5,
            "column_count": 7,
            "size_bytes": 4254,
            "scd2_metrics": {
                "rows_inserted": 2,
                "rows_closed": 1,
                "rows_total": 5,
                "rows_current": 4,
                "created": False,
            },
        },
        1,
    )


def test_a_worker_write_report_carries_no_version():
    report = {"table_path": "/catalog/t_1", "schema": PEOPLE_SCHEMA, "row_count": 3, "column_count": 2, "size_bytes": 9}

    assert _catalog_delta_written(report) == CatalogDeltaWrite(
        {"schema": PEOPLE_SCHEMA, "row_count": 3, "column_count": 2, "size_bytes": 9}, None
    )


@pytest.mark.parametrize("report, version", [({"skipped": True}, None), ({"skipped": True, "version": 3}, 3)])
def test_a_skipped_report_registers_nothing_and_keeps_its_version(report, version):
    assert _catalog_delta_written(report) == CatalogDeltaWrite(None, version)


# The writer's write on the embedded worker reads as it did before


def run_locally(delta_mode, settings_, plan, path, scd2_config=None, run_timestamp=RUN_TIMESTAMP):
    operation, write = _catalog_delta_op(
        delta_mode,
        settings_,
        plan=plan,
        path=path,
        connection=None,
        scd2_config=scd2_config,
        run_timestamp=run_timestamp,
        flow_id=1,
        node_id=2,
    )
    return _catalog_delta_written(EmbeddedWorkerBackend().run_op(operation, op_payload(write)).metadata)


def test_an_scd2_write_in_core_reads_as_before(tmp_path):
    path = str(tmp_path / "scd2")
    scd2 = writer_settings("scd2", merge_keys=["id"])
    changed = pl.LazyFrame({"id": [1, 2, 4], "val": ["A", "b", "d"], "qty": [11, 20, 40]})

    first = run_locally(
        "scd2",
        scd2,
        pl.LazyFrame({"id": [1, 2, 3], "val": ["a", "b", "c"], "qty": [10, 20, 30]}),
        path,
        SCD2_CONFIG,
        "2024-01-01T00:00:00+00:00",
    )
    first_size = get_delta_size_bytes(path)
    second = run_locally("scd2", scd2, changed, path, SCD2_CONFIG, "2024-02-01T00:00:00+00:00")
    second_size = get_delta_size_bytes(path)
    unchanged = run_locally("scd2", scd2, changed, path, SCD2_CONFIG, "2024-03-01T00:00:00+00:00")

    assert first == CatalogDeltaWrite(
        meta={
            "schema": SCD2_TABLE_SCHEMA,
            "row_count": 3,
            "column_count": 7,
            "size_bytes": first_size,
            "scd2_metrics": {"rows_inserted": 3, "rows_closed": 0, "rows_total": 3, "rows_current": 3, "created": True},
        },
        scd2_version=0,
    )
    assert second == CatalogDeltaWrite(
        meta={
            "schema": SCD2_TABLE_SCHEMA,
            "row_count": 5,
            "column_count": 7,
            "size_bytes": second_size,
            "scd2_metrics": {
                "rows_inserted": 2,
                "rows_closed": 1,
                "rows_total": 5,
                "rows_current": 4,
                "created": False,
            },
        },
        scd2_version=1,
    )
    assert unchanged == CatalogDeltaWrite(meta=None, scd2_version=1)
    jan, feb = datetime.datetime(2024, 1, 1, tzinfo=UTC), datetime.datetime(2024, 2, 1, tzinfo=UTC)
    assert pl.read_delta(path).sort("id", "valid_from").rows() == [
        (1, "a", 10, "086436db5d6237d8f53e55ebf5bafe19", jan, feb, False),
        (1, "A", 11, "8a33fe6e8900cddb5e995de3f3912ec3", feb, None, True),
        (2, "b", 20, "a4d2b5f0652fef0de72c82802a95ade3", jan, None, True),
        (3, "c", 30, "ec4f31d51c39fec0ce47aecc38b5bf5c", jan, None, True),
        (4, "d", 40, "b45a6c225243cc60905eb98c9803620e", feb, None, True),
    ]


def test_a_plain_write_and_a_merge_in_core_read_as_before(tmp_path):
    path = str(tmp_path / "plain")
    empty = pl.LazyFrame({"id": [], "name": []}, schema={"id": pl.Int64, "name": pl.String})

    overwritten = run_locally(
        "overwrite",
        writer_settings("overwrite"),
        pl.LazyFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]}),
        path,
    )
    overwritten_size = get_delta_size_bytes(path)
    upserted = run_locally(
        "upsert",
        writer_settings("upsert", merge_keys=["id"]),
        pl.LazyFrame({"id": [3, 4], "name": ["dijkstra", "barbara"]}),
        path,
    )
    upserted_size = get_delta_size_bytes(path)
    appended_nothing = run_locally("append", writer_settings("append"), empty, path)
    upserted_nothing = run_locally("upsert", writer_settings("upsert", merge_keys=["id"]), empty, path)

    assert overwritten == CatalogDeltaWrite(
        meta={"schema": PEOPLE_SCHEMA, "row_count": 3, "column_count": 2, "size_bytes": overwritten_size},
        scd2_version=None,
    )
    assert upserted == CatalogDeltaWrite(
        meta={"schema": PEOPLE_SCHEMA, "row_count": 4, "column_count": 2, "size_bytes": upserted_size},
        scd2_version=None,
    ), "the worker reports a merge by the table after it"
    assert appended_nothing == CatalogDeltaWrite(meta=None, scd2_version=None)
    assert upserted_nothing == CatalogDeltaWrite(
        meta={"schema": PEOPLE_SCHEMA, "row_count": 4, "column_count": 2, "size_bytes": upserted_size},
        scd2_version=None,
    )
    assert pl.read_delta(path).sort("id").rows() == [(1, "ada"), (2, "grace"), (3, "dijkstra"), (4, "barbara")]


def test_an_append_in_core_reports_the_appended_rows(tmp_path):
    path = str(tmp_path / "plain")
    run_locally("overwrite", writer_settings("overwrite"), pl.LazyFrame({"x": [1, 2]}), path)

    appended = run_locally("append", writer_settings("append"), pl.LazyFrame({"x": [3, 4, 5]}), path)

    assert appended == CatalogDeltaWrite(
        meta={
            "schema": [{"name": "x", "dtype": "Int64"}],
            "row_count": 3,
            "column_count": 1,
            "size_bytes": get_delta_size_bytes(path),
        },
        scd2_version=None,
    )
    assert pl.read_delta(path).height == 5


# The catalog writer node, end to end


@pytest.fixture
def clean_catalog():
    catalog_cleanup()
    yield
    catalog_cleanup()


class Writes:
    """The ``run_op`` calls either backend ran, with the job the watched writer node held while each ran."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, OperationKind, object, JobHandle | None]] = []
        self.writer = None

    def take_one(self, backend_id: str, op: OperationKind, payload_type: type):
        """Assert exactly one write ran since the last take: ``op`` on ``backend_id``, cancellable through the node.

        Returns the write's payload.
        """
        (call,) = self.calls
        self.calls.clear()
        ran_on, ran, payload, held = call
        assert (ran_on, ran, type(payload)) == (backend_id, op, payload_type)
        assert held == JobHandle(backend_id, payload.job_id)
        return payload


@pytest.fixture
def writes(monkeypatch) -> Writes:
    recorder = Writes()
    for backend_type in (EmbeddedWorkerBackend, WorkerBackend):

        def spy(self, op, payload, _run_op=backend_type.run_op):
            held = recorder.writer._submit_in_flight if recorder.writer is not None else None
            recorder.calls.append((self.id, op, payload.get(PAYLOAD_KEY), held))
            return _run_op(self, op, payload)

        monkeypatch.setattr(backend_type, "run_op", spy)
    return recorder


def write_flow(writes, flow_id, execution_location, namespace_id, rows, write_mode="overwrite", **writer):
    graph = create_test_graph(flow_id=flow_id, execution_location=execution_location)
    add_test_manual_input(graph, rows, node_id=1)
    add_test_catalog_writer(
        graph,
        node_id=2,
        depending_on_id=1,
        table_name="dim",
        namespace_id=namespace_id,
        write_mode=write_mode,
        **writer,
    )
    writes.writer = graph.get_node(2)
    return graph


def registered(namespace_id):
    with get_db_context() as db:
        (table,) = SQLAlchemyCatalogRepository(db).list_tables(namespace_id=namespace_id)
        return table


def test_the_catalog_writer_overwrites_on_the_flows_backend(execution_location, clean_catalog, writes):
    namespace_id = create_test_namespace()
    rows = [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}]
    first = write_flow(writes, 9311, execution_location, namespace_id, rows)
    backend_id = first.execution_context.backend_id

    run_test_graph(first)
    writes.take_one(backend_id, OperationKind.delta_write, DeltaWritePayload)
    created = registered(namespace_id)
    run_test_graph(write_flow(writes, 9312, execution_location, namespace_id, [{"id": 3, "name": "edsger"}]))
    writes.take_one(backend_id, OperationKind.delta_write, DeltaWritePayload)
    replaced = registered(namespace_id)

    assert (created.row_count, created.column_count) == (2, 2)
    assert (replaced.id, replaced.row_count) == (created.id, 1)
    assert replaced.size_bytes == get_delta_size_bytes(replaced.file_path)
    assert pl.read_delta(replaced.file_path).rows() == [(3, "edsger")]


def test_the_catalog_writer_upserts_on_the_flows_backend(execution_location, clean_catalog, writes):
    namespace_id = create_test_namespace()
    seed = write_flow(
        writes, 9321, execution_location, namespace_id, [{"id": 1, "name": "ada"}, {"id": 2, "name": "x"}]
    )
    backend_id = seed.execution_context.backend_id
    run_test_graph(seed)
    writes.take_one(backend_id, OperationKind.delta_write, DeltaWritePayload)

    changes = [{"id": 2, "name": "hopper"}, {"id": 3, "name": "edsger"}]
    upsert = write_flow(writes, 9322, execution_location, namespace_id, changes, write_mode="upsert", merge_keys=["id"])
    run_test_graph(upsert)

    writes.take_one(backend_id, OperationKind.delta_merge, DeltaMergePayload)
    table = registered(namespace_id)
    assert pl.read_delta(table.file_path).sort("id").rows() == [(1, "ada"), (2, "hopper"), (3, "edsger")]
    # The worker reports a merge by the table's rows, embedded in this process or beside it.
    assert table.row_count == 3
    assert table.size_bytes == get_delta_size_bytes(table.file_path)


def test_the_catalog_writer_versions_an_scd2_table_on_the_flows_backend(execution_location, clean_catalog, writes):
    namespace_id = create_test_namespace()
    scd2 = {"write_mode": "scd2", "merge_keys": ["id"], "scd2": input_schema.Scd2Settings()}
    loads = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    first = write_flow(writes, 9331, execution_location, namespace_id, loads, **scd2)
    backend_id = first.execution_context.backend_id

    run_test_graph(first)
    writes.take_one(backend_id, OperationKind.delta_scd2, DeltaScd2Payload)
    loaded = registered(namespace_id)
    changes = [{"id": 1, "val": "A"}, {"id": 2, "val": "b"}]
    second = write_flow(writes, 9332, execution_location, namespace_id, changes, **scd2)
    run_test_graph(second)
    writes.take_one(backend_id, OperationKind.delta_scd2, DeltaScd2Payload)
    versioned = registered(namespace_id)

    assert (loaded.row_count, versioned.row_count, versioned.column_count) == (2, 3, 6)
    assert versioned.scd2_config is not None
    assert versioned.size_bytes == get_delta_size_bytes(versioned.file_path)
    history = pl.read_delta(versioned.file_path).sort("id", "valid_from")
    assert history.select("id", "val", "is_current").rows() == [(1, "a", False), (1, "A", True), (2, "b", True)]
    emitted = second.get_node(2).get_resulting_data().collect()
    assert emitted.columns == ["id", "val", "sk", "valid_from", "valid_to", "is_current"]
    assert emitted.sort("id").select("id", "val", "is_current").rows() == [(1, "A", True), (2, "b", True)]


# The catalog writer names its table by identity (FLOWFILE_COMPUTE_PROTOCOL on)


@pytest.fixture(params=[False, True], ids=["protocol-off", "protocol-on"])
def protocol(request, monkeypatch) -> bool:
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture
def worker_kwargs(monkeypatch) -> list[dict]:
    """The keyword arguments each worker Delta task was sent with, in order; the tasks still run."""
    sent: list[dict] = []
    delta_task = WorkerBackend._delta_task

    def spy(self, write, operation_type, kwargs):
        sent.append(kwargs)
        return delta_task(self, write, operation_type, kwargs)

    monkeypatch.setattr(WorkerBackend, "_delta_task", spy)
    return sent


@pytest.fixture
def s3_namespace(clean_catalog):
    """A schema under a catalog stored in MinIO: its id, the catalog's URI and the storage options core reads with."""
    _ensure_connection()
    try:
        get_minio_client().create_bucket(Bucket=_BUCKET)
    except Exception:
        pass
    uri = f"s3://{_BUCKET}/identity_{uuid.uuid4().hex[:8]}"
    namespace_id = _create_cloud_schema(uri)
    return namespace_id, uri, resolve_for_namespace(namespace_id).storage_options


def dir_name(file_path: str) -> str:
    """The last segment of a path or URI, whichever separator the platform registered it with."""
    return file_path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def assert_addressed(protocol, payloads, worker_sent, file_path, table_ids, base_uri=None):
    """Off, every write names ``file_path``; on, each names the table by identity and no write carries a path."""
    if protocol:
        assert [(payload.path, payload.table) for payload in payloads] == [
            (None, CatalogTableIdentity(table_id=table_id, table_name=dir_name(file_path), base_uri=base_uri))
            for table_id in table_ids
        ]
        for kwargs in worker_sent:
            assert "output_path" not in kwargs
            assert kwargs["table_name"] == dir_name(file_path)
            assert (kwargs["storage_payload"] or {}).get("base_uri") == base_uri
    else:
        assert [(payload.path, payload.table) for payload in payloads] == [(file_path, None)] * len(payloads)
        assert [kwargs["output_path"] for kwargs in worker_sent] == [file_path] * len(worker_sent)


@pytest.mark.parametrize("delta_mode", ["overwrite", "upsert", "scd2"])
def test_a_write_by_identity_carries_no_path(delta_mode):
    table = CatalogTableIdentity(table_id=5, table_name="t_1", base_uri="s3://bucket/catalog")

    _, write = _catalog_delta_op(
        delta_mode,
        writer_settings(delta_mode, merge_keys=["id"]),
        plan=pl.LazyFrame({"id": [1], "val": ["a"]}),
        path=None,
        table=table,
        connection=None,
        scd2_config=SCD2_CONFIG if delta_mode == "scd2" else None,
        run_timestamp=RUN_TIMESTAMP,
        flow_id=4,
        node_id=2,
    )

    assert (write.path, write.table) == (None, table)


def test_the_catalog_writer_names_a_local_table_by_identity_when_the_flag_is_on(
    execution_location, protocol, clean_catalog, writes, worker_kwargs
):
    namespace_id = create_test_namespace()
    first = write_flow(writes, 9341, execution_location, namespace_id, [{"id": 1, "name": "ada"}])
    backend_id = first.execution_context.backend_id
    run_test_graph(first)
    created_by = writes.take_one(backend_id, OperationKind.delta_write, DeltaWritePayload)
    created = registered(namespace_id)
    upsert = write_flow(
        writes, 9342, execution_location, namespace_id, [{"id": 2, "name": "grace"}], "upsert", merge_keys=["id"]
    )
    run_test_graph(upsert)
    merged_by = writes.take_one(backend_id, OperationKind.delta_merge, DeltaMergePayload)
    merged = registered(namespace_id)

    assert_addressed(protocol, [created_by, merged_by], worker_kwargs, created.file_path, [None, created.id])
    assert len(worker_kwargs) == (2 if backend_id == registry.WORKER_BACKEND_ID else 0)
    assert merged.file_path == created.file_path
    assert pl.read_delta(merged.file_path).sort("id").rows() == [(1, "ada"), (2, "grace")]


def test_core_stores_and_reads_the_table_where_the_compute_answered(clean_catalog, monkeypatch, tmp_path):
    """The location the compute answers, not the one core resolved, is what core registers and reads back."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    run_op = EmbeddedWorkerBackend.run_op

    def moved(self, op, payload):
        result = run_op(self, op, payload)
        answered = result.metadata.get("table_path")
        if not answered:
            return result
        elsewhere = tmp_path / "elsewhere" / dir_name(answered)
        elsewhere.parent.mkdir(exist_ok=True)
        shutil.move(answered, elsewhere)
        return OpResult(refs=result.refs, metadata={**result.metadata, "table_path": str(elsewhere)})

    monkeypatch.setattr(EmbeddedWorkerBackend, "run_op", moved)
    namespace_id = create_test_namespace()
    graph = create_test_graph(flow_id=9351, execution_location="local")
    add_test_manual_input(graph, [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}], node_id=1)
    add_test_catalog_writer(
        graph,
        node_id=2,
        depending_on_id=1,
        table_name="dim",
        namespace_id=namespace_id,
        write_mode="scd2",
        merge_keys=["id"],
        scd2=input_schema.Scd2Settings(),
    )
    run_test_graph(graph)

    table = registered(namespace_id)
    assert table.file_path.startswith(str(tmp_path / "elsewhere"))
    assert pl.read_delta(table.file_path).height == 2
    emitted = graph.get_node(2).get_resulting_data().collect()
    assert emitted.sort("id").select("id", "val", "is_current").rows() == [(1, "a", True), (2, "b", True)]


def test_a_table_outside_its_catalog_root_is_refused_by_identity(protocol, clean_catalog, tmp_path):
    external = str(tmp_path / "external_dim")
    pl.DataFrame({"id": [1], "name": ["ada"]}).write_delta(external)
    namespace_id = create_test_namespace()
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).register_table_from_data(
            name="dim", table_path=external, owner_id=1, namespace_id=namespace_id, storage_format="delta"
        )
    graph = create_test_graph(flow_id=9361, execution_location="local")
    add_test_manual_input(graph, [{"id": 2, "name": "grace"}], node_id=1)
    add_test_catalog_writer(
        graph, node_id=2, depending_on_id=1, table_name="dim", namespace_id=namespace_id, write_mode="append"
    )

    run = graph.run_graph()

    if protocol:
        assert not run.success
        assert "not a table directory under its catalog's storage root" in str(run.node_step_result[-1].error)
        assert pl.read_delta(external).height == 1
    else:
        assert run.success
        assert pl.read_delta(external).height == 2


@pytest.mark.parametrize("same_place", [True, False], ids=["same-place", "another-place"])
def test_an_existing_table_stays_where_its_record_puts_it(protocol, same_place, clean_catalog, monkeypatch, tmp_path):
    """A compute that resolves the table under a catalog directory of its own never moves or deletes the table: the
    same directory spelled through a symlink keeps the record's location, another directory fails the write."""
    namespace_id = create_test_namespace()

    def append(flow_id, rows):
        graph = create_test_graph(flow_id=flow_id, execution_location="local")
        add_test_manual_input(graph, rows, node_id=1)
        add_test_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="dim", namespace_id=namespace_id, write_mode="append"
        )
        return graph.run_graph()

    assert append(9391, [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}]).success
    stored = registered(namespace_id).file_path
    compute_root = tmp_path / "compute_root"
    if same_place:
        compute_root.symlink_to(Path(stored).parent, target_is_directory=True)
    else:
        compute_root.mkdir()
    monkeypatch.setattr("flowfile_worker.funcs.storage", types.SimpleNamespace(catalog_tables_directory=compute_root))

    run = append(9392, [{"id": 3, "name": "edsger"}])

    refused = protocol and not same_place
    assert run.success is not refused
    assert registered(namespace_id).file_path == stored
    assert pl.read_delta(stored).height == (2 if refused else 3)
    if refused:
        assert "which is another location" in str(run.node_step_result[-1].error)


def test_a_table_stored_through_another_spelling_of_its_catalog_root_is_written_in_place(
    protocol, clean_catalog, tmp_path
):
    namespace_id = create_test_namespace()
    root_link = tmp_path / "catalog_link"
    root_link.symlink_to(resolve_for_namespace(namespace_id).base, target_is_directory=True)
    stored = str(root_link / f"dim_{uuid.uuid4().hex[:8]}")
    pl.DataFrame({"id": [1], "name": ["ada"]}).write_delta(stored)
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).register_table_from_data(
            name="dim", table_path=stored, owner_id=1, namespace_id=namespace_id, storage_format="delta"
        )
    graph = create_test_graph(flow_id=9393, execution_location="local")
    add_test_manual_input(graph, [{"id": 2, "name": "grace"}], node_id=1)
    add_test_catalog_writer(
        graph, node_id=2, depending_on_id=1, table_name="dim", namespace_id=namespace_id, write_mode="append"
    )

    run_test_graph(graph)

    assert registered(namespace_id).file_path == stored
    assert pl.read_delta(stored).sort("id").rows() == [(1, "ada"), (2, "grace")]


@requires_minio
def test_the_catalog_writer_writes_an_object_storage_table_on_the_flows_backend(
    execution_location, protocol, writes, worker_kwargs, s3_namespace
):
    namespace_id, uri, options = s3_namespace
    first = write_flow(writes, 9371, execution_location, namespace_id, [{"id": 1, "name": "ada"}])
    backend_id = first.execution_context.backend_id
    run_test_graph(first)
    created_by = writes.take_one(backend_id, OperationKind.delta_write, DeltaWritePayload)
    created = registered(namespace_id)
    upsert = write_flow(
        writes, 9372, execution_location, namespace_id, [{"id": 2, "name": "grace"}], "upsert", merge_keys=["id"]
    )
    run_test_graph(upsert)
    merged_by = writes.take_one(backend_id, OperationKind.delta_merge, DeltaMergePayload)
    merged = registered(namespace_id)

    assert created.file_path.startswith(uri + "/") and merged.file_path == created.file_path
    assert_addressed(protocol, [created_by, merged_by], worker_kwargs, created.file_path, [None, created.id], uri)
    assert pl.read_delta(merged.file_path, storage_options=options).sort("id").rows() == [(1, "ada"), (2, "grace")]
    assert merged.size_bytes == get_delta_size_bytes(merged.file_path, storage_options=options)


@requires_minio
def test_the_catalog_writer_versions_an_object_storage_scd2_table_on_the_flows_backend(
    execution_location, protocol, writes, worker_kwargs, s3_namespace
):
    namespace_id, uri, options = s3_namespace
    scd2 = {"write_mode": "scd2", "merge_keys": ["id"], "scd2": input_schema.Scd2Settings()}
    first = write_flow(writes, 9381, execution_location, namespace_id, [{"id": 1, "val": "a"}], **scd2)
    backend_id = first.execution_context.backend_id
    run_test_graph(first)
    loaded_by = writes.take_one(backend_id, OperationKind.delta_scd2, DeltaScd2Payload)
    loaded = registered(namespace_id)
    second = write_flow(writes, 9382, execution_location, namespace_id, [{"id": 1, "val": "A"}], **scd2)
    run_test_graph(second)
    versioned_by = writes.take_one(backend_id, OperationKind.delta_scd2, DeltaScd2Payload)
    versioned = registered(namespace_id)

    assert loaded.file_path.startswith(uri + "/") and versioned.file_path == loaded.file_path
    assert_addressed(protocol, [loaded_by, versioned_by], worker_kwargs, loaded.file_path, [None, loaded.id], uri)
    assert versioned.size_bytes == get_delta_size_bytes(versioned.file_path, storage_options=options)
    history = pl.read_delta(versioned.file_path, storage_options=options).sort("valid_from")
    assert history.select("id", "val", "is_current").rows() == [(1, "a", False), (1, "A", True)]
    emitted = second.get_node(2).get_resulting_data().collect()
    assert emitted.select("id", "val", "is_current").rows() == [(1, "A", True)]
