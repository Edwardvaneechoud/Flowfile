"""End-to-end: a saved flow that uses catalog-on-S3 tables, run via a MANUAL
(non-editor) run through the ``flowfile run flow`` CLI path.

This closes the coverage gap the existing tests leave open:
- ``test_catalog_flow_graph.py::test_cli_runner_records_producer_lineage`` proves
  save -> ``flowfile.__main__.run_flow`` works, but only against a LOCAL catalog.
- ``test_catalog_cloud_virtual.py`` proves S3-backed catalogs work, but runs the
  flow in-process (``run_graph``) and only exercises virtual writes.

Neither runs a *stored* flow against an S3 catalog through the manual-run path.
These tests do: build a flow with catalog reader/writer nodes targeting an
S3-backed catalog, ``save_flow`` to disk, then execute the saved file via
``run_flow`` (the same entry point manual/scheduled runs use) and assert the
Delta bytes actually landed in object storage with correct rows and lineage.

Cloud catalog writes follow ``execution_location`` like every other writer node: a remote
run offloads to the worker (the session-scoped ``flowfile_worker`` fixture provides it), a
local run — which the CLI/scheduler force — writes object storage in-core via resolved
``storage_options``. A cloud destination never forces the worker; that is what the
``..._runs_in_core_for_local_execution`` tests pin down with an offload sentinel (the session
worker would otherwise mask a regression). Cloud *reads* are always in-core
(``pl.scan_delta`` with resolved ``storage_options``). Gated on the shared MinIO mock via
``requires_minio``.
"""

import os
import tempfile
import uuid
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import resolve_for_namespace
from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import flow_graph as fg
from flowfile_core.schemas import input_schema
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA as SAMPLE_DATA,
)
from tests.flowfile.conftest import (
    add_test_catalog_writer as _add_catalog_writer,
)
from tests.flowfile.conftest import (
    add_test_manual_input as _add_manual_input,
)
from tests.flowfile.conftest import (
    catalog_cleanup as _cleanup,
)
from tests.flowfile.conftest import (
    create_test_flow_registration as _create_flow_registration,
)
from tests.flowfile.conftest import (
    create_test_graph as _create_graph,
)
from tests.flowfile.conftest import (
    run_test_graph as _run_graph,
)
from tests.test_catalog_cloud_virtual import (
    _BUCKET,
    _add_age_filter,
    _create_cloud_schema,
    _ensure_connection,
    get_minio_client,
    requires_minio,
)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
    _cleanup()
    yield
    _cleanup()


def _run_saved_flow(resolved_path: str) -> int:
    """Run a saved flow via the manual/scheduled-run CLI entry point, restoring the
    process-global offload flag + env afterwards (run_flow mutates them)."""
    prev_offload = OFFLOAD_TO_WORKER.value
    prev_env = {k: os.environ.get(k) for k in ("FLOWFILE_SINGLE_FILE_MODE", "FLOWFILE_WORKER_PORT")}
    try:
        from flowfile.__main__ import run_flow

        return run_flow(resolved_path, run_id=None)
    finally:
        OFFLOAD_TO_WORKER.set(prev_offload)
        for key, value in prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _assert_delta_objects_in_s3(file_path: str) -> None:
    """The catalog table's bytes really exist as a Delta table in object storage."""
    assert file_path.startswith(f"s3://{_BUCKET}/"), file_path
    prefix = file_path[len(f"s3://{_BUCKET}/") :]
    resp = get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    assert keys, f"no objects under s3://{_BUCKET}/{prefix}"
    assert any("_delta_log" in k for k in keys), f"no _delta_log under {prefix}: {keys}"


def _offload_forbidden(*args, **kwargs):
    """Sentinel for ``_write_catalog_delta_remote``: a local-execution run must write a cloud
    catalog in-core, never offloading to a worker (which a standalone CLI/scheduler run lacks)."""
    raise AssertionError("cloud catalog write offloaded to the worker during local execution")


def _add_upsert_writer(graph, node_id, depending_on_id, table_name, namespace_id, merge_keys, user_id=1):
    """A catalog writer in upsert mode (the conftest helper exposes no merge_keys)."""
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_writer")
    )
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=table_name,
                namespace_id=namespace_id,
                write_mode="upsert",
                merge_keys=merge_keys,
            ),
            user_id=user_id,
        )
    )
    fg.add_connection(
        graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id)
    )


@requires_minio
class TestCloudCatalogCliRunE2E:
    """Store a flow that targets an S3-backed catalog, then run it from disk via the
    manual-run CLI path — proving runtime storage-backend selection, owner-keyed
    cloud-credential resolution, flow persistence and non-editor execution compose."""

    def test_saved_flow_writes_physical_catalog_table_to_s3_via_cli_run(self):
        """manual_input -> catalog_writer(physical, S3): saved to disk, run via the CLI,
        the new Delta table must land in object storage with producer lineage."""
        _ensure_connection()
        try:
            get_minio_client().create_bucket(Bucket=_BUCKET)
        except Exception:
            pass

        uri = f"s3://{_BUCKET}/clirun_{uuid.uuid4().hex[:8]}"
        schema_id = _create_cloud_schema(uri)
        assert resolve_for_namespace(schema_id).is_cloud is True

        graph = _create_graph(execution_location="local")
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="cli_cloud_table",
            namespace_id=schema_id,
            write_mode="overwrite",
        )

        with tempfile.TemporaryDirectory() as tmp:
            flow_path = Path(tmp) / "cloud_writer_flow.yaml"
            graph.save_flow(str(flow_path))
            resolved_path = str(flow_path.resolve())
            reg_id = _create_flow_registration(schema_id, name="cloud_writer_flow", path=resolved_path)

            exit_code = _run_saved_flow(resolved_path)

        assert exit_code == 0

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=schema_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.name == "cli_cloud_table"
            assert table.row_count == 3
            assert table.source_registration_id == reg_id
            table_file_path = table.file_path

        _assert_delta_objects_in_s3(table_file_path)
        target = resolve_for_namespace(schema_id)
        df = pl.scan_delta(table_file_path, storage_options=target.storage_options).collect()
        assert df.height == 3
        assert set(df["name"].to_list()) == {"Alice", "Bob", "Charlie"}

    def test_saved_flow_reads_and_writes_s3_catalog_via_cli_run(self):
        """catalog_reader(S3) -> filter -> catalog_writer(physical, S3): a saved flow run
        via the CLI must read the cloud source in-core and write the filtered result back
        to object storage (full round trip through the manual-run path)."""
        from shared.delta_utils import write_delta

        _ensure_connection()
        try:
            get_minio_client().create_bucket(Bucket=_BUCKET)
        except Exception:
            pass

        uri = f"s3://{_BUCKET}/clirt_{uuid.uuid4().hex[:8]}"
        schema_id = _create_cloud_schema(uri)
        target = resolve_for_namespace(schema_id)
        assert target.is_cloud is True

        # Seed a physical source table in S3 and register it in the catalog.
        source_dest = f"{uri}/source_physical"
        assert (
            write_delta(pl.DataFrame(SAMPLE_DATA), source_dest, mode="overwrite", storage_options=target.storage_options)
            is True
        )
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            phys = svc.register_table_from_data(
                name="cloud_source",
                table_path=source_dest,
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[
                    {"name": "name", "dtype": "Utf8"},
                    {"name": "age", "dtype": "Int64"},
                    {"name": "city", "dtype": "Utf8"},
                ],
                row_count=3,
                column_count=3,
                size_bytes=100,
            )
            phys_id = phys.id

        graph = _create_graph(execution_location="local")
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        )
        graph.add_catalog_reader(
            input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, catalog_table_id=phys_id)
        )
        _add_age_filter(graph, node_id=2, depending_on_id=1)
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="cloud_filtered",
            namespace_id=schema_id,
            write_mode="overwrite",
        )

        with tempfile.TemporaryDirectory() as tmp:
            flow_path = Path(tmp) / "cloud_rw_flow.yaml"
            graph.save_flow(str(flow_path))
            resolved_path = str(flow_path.resolve())
            _create_flow_registration(schema_id, name="cloud_rw_flow", path=resolved_path)

            exit_code = _run_saved_flow(resolved_path)

        assert exit_code == 0

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            out = next(t for t in repo.list_tables(namespace_id=schema_id) if t.name == "cloud_filtered")
            assert out.row_count == 2
            filtered_path = out.file_path

        _assert_delta_objects_in_s3(filtered_path)
        df = pl.scan_delta(filtered_path, storage_options=target.storage_options).collect()
        assert df.height == 2
        assert set(df["name"].to_list()) == {"Alice", "Charlie"}

    def test_cloud_physical_write_runs_in_core_for_local_execution(self, monkeypatch):
        """Regression for the scheduler/CLI failure: a physical *cloud* catalog write under
        local execution must run IN-CORE via storage_options, never offloading to a worker
        that a standalone run does not have. The sentinel re-raises if the worker branch is
        taken — the session ``flowfile_worker`` fixture would otherwise hide the regression."""
        _ensure_connection()
        try:
            get_minio_client().create_bucket(Bucket=_BUCKET)
        except Exception:
            pass

        uri = f"s3://{_BUCKET}/incore_{uuid.uuid4().hex[:8]}"
        schema_id = _create_cloud_schema(uri)
        assert resolve_for_namespace(schema_id).is_cloud is True

        monkeypatch.setattr(fg, "_write_catalog_delta_remote", _offload_forbidden)

        graph = _create_graph(execution_location="local")
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="incore_cloud_table",
            namespace_id=schema_id,
            write_mode="overwrite",
        )
        _run_graph(graph)

        with get_db_context() as db:
            tables = SQLAlchemyCatalogRepository(db).list_tables(namespace_id=schema_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.name == "incore_cloud_table"
            assert table.row_count == 3
            # Guards get_delta_size_bytes(storage_options=...): without it, cloud size silently records 0.
            assert table.size_bytes and table.size_bytes > 0
            table_file_path = table.file_path

        _assert_delta_objects_in_s3(table_file_path)
        target = resolve_for_namespace(schema_id)
        df = pl.scan_delta(table_file_path, storage_options=target.storage_options).collect()
        assert df.height == 3
        assert set(df["name"].to_list()) == {"Alice", "Bob", "Charlie"}

    def test_cloud_upsert_runs_in_core_for_local_execution(self, monkeypatch):
        """The merge branch of the in-core path: a cloud *upsert* (merge_into_delta with
        storage_options) under local execution. Seed the table in-core, then upsert into it."""
        _ensure_connection()
        try:
            get_minio_client().create_bucket(Bucket=_BUCKET)
        except Exception:
            pass

        uri = f"s3://{_BUCKET}/upsert_{uuid.uuid4().hex[:8]}"
        schema_id = _create_cloud_schema(uri)
        target = resolve_for_namespace(schema_id)
        assert target.is_cloud is True

        monkeypatch.setattr(fg, "_write_catalog_delta_remote", _offload_forbidden)

        seed = _create_graph(execution_location="local")
        _add_manual_input(seed, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            seed,
            node_id=2,
            depending_on_id=1,
            table_name="upsert_table",
            namespace_id=schema_id,
            write_mode="overwrite",
        )
        _run_graph(seed)

        upserts = [
            {"name": "Alice", "age": 99, "city": "Amsterdam"},  # update existing
            {"name": "Dave", "age": 40, "city": "Dublin"},  # insert new
        ]
        merge = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(merge, upserts, node_id=1)
        _add_upsert_writer(
            merge,
            node_id=2,
            depending_on_id=1,
            table_name="upsert_table",
            namespace_id=schema_id,
            merge_keys=["name"],
        )
        _run_graph(merge)

        with get_db_context() as db:
            table = next(
                t
                for t in SQLAlchemyCatalogRepository(db).list_tables(namespace_id=schema_id)
                if t.name == "upsert_table"
            )
            table_file_path = table.file_path

        df = pl.scan_delta(table_file_path, storage_options=target.storage_options).collect()
        assert df.height == 4
        assert set(df["name"].to_list()) == {"Alice", "Bob", "Charlie", "Dave"}
        assert df.filter(pl.col("name") == "Alice")["age"].item() == 99
