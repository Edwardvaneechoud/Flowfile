"""Object-storage catalog + virtual tables.

A virtual table built over an S3-backed source must NOT cache a serialized Polars plan:
``LazyFrame.serialize`` embeds the source's ``storage_options`` (decrypted cloud
credentials) inline, so a cached cloud plan would freeze those secrets in the metadata DB
(stale on rotation, a secret-at-rest leak). Cloud-sourced virtual tables therefore re-run
the producer flow on read, resolving credentials fresh.

- Infra-free unit tests cover the serialized-frame cloud sniffer and the read-side guard.
- A MinIO-gated end-to-end test proves the real write path leaves ``serialized_lazy_frame``
  empty and the table still reads back correctly through producer re-execution.
"""

import io
import tempfile
import uuid
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.storage_backend import resolve_for_namespace, serialized_frame_uses_cloud
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.flowfile.flow_graph import _resolve_virtual_table, add_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput
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
    create_test_namespace as _create_namespace,
)
from tests.flowfile.conftest import (
    run_test_graph as _run_graph,
)

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

_CONNECTION_NAME = "catalog-cloudvt-minio"
_BUCKET = "flowfile-test"


def _minio_available() -> bool:
    """True only when the shared MinIO mock is reachable (never starts/stops it)."""
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
    _cleanup()
    yield
    _cleanup()


def _serialize_cloud_scan(secret: str = "frozen-secret") -> bytes:
    """A serialized LazyFrame that scans S3 — exactly the blob we refuse to cache/replay."""
    lf = pl.scan_parquet(
        "s3://example-bucket/frozen/part.parquet",
        storage_options={"aws_access_key_id": "AKIAEXAMPLE", "aws_secret_access_key": secret},
    )
    buf = io.BytesIO()
    lf.filter(pl.col("x") > 1).select("x", "y").serialize(buf)
    return buf.getvalue()


def _add_age_filter(graph, node_id: int, depending_on_id: int) -> None:
    """Add a lazy ``age > 28`` filter (keeps Alice=30, Charlie=35)."""
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="filter")
    )
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="age", operator="greater_than", value="28"),
            ),
        )
    )
    add_connection(
        graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id)
    )


# ---- Infra-free: serialized-frame cloud sniffer ---------------------------- #


class TestSerializedFrameUsesCloud:
    def test_local_frame_is_not_cloud(self):
        buf = io.BytesIO()
        pl.LazyFrame({"x": [1, 2, 3]}).serialize(buf)
        assert serialized_frame_uses_cloud(buf.getvalue()) is False

    def test_s3_scan_is_cloud_and_embeds_credentials(self):
        blob = _serialize_cloud_scan()
        # The reason we refuse it: the serialized plan carries the source's decrypted secret.
        assert b"frozen-secret" in blob
        assert serialized_frame_uses_cloud(blob) is True

    def test_gcs_scan_is_cloud(self):
        buf = io.BytesIO()
        pl.scan_parquet("gs://example-bucket/part.parquet").serialize(buf)
        assert serialized_frame_uses_cloud(buf.getvalue()) is True

    def test_none_and_empty_are_not_cloud(self):
        assert serialized_frame_uses_cloud(None) is False
        assert serialized_frame_uses_cloud(b"") is False


# ---- Infra-free: read-side guard refuses a cloud-tainted cache ------------- #


class TestCloudCacheReadGuard:
    def test_resolve_virtual_table_skips_cloud_blob(self):
        """``_resolve_virtual_table`` must not deserialize a cloud-tainted cache (that would
        replay frozen creds) — it falls back to ``resolve_virtual_flow_table``."""
        blob = _serialize_cloud_scan()
        expected = pl.LazyFrame({"ok": [1]})
        with patch("flowfile_core.flowfile.flow_graph.get_db_context") as mock_ctx:
            mock_db = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch("flowfile_core.catalog.CatalogService") as MockSvc:
                inst = MagicMock()
                inst.resolve_virtual_flow_table.return_value = expected
                MockSvc.return_value = inst
                result = _resolve_virtual_table(is_optimized=True, serialized_lf=blob, catalog_table_id=7)
        inst.resolve_virtual_flow_table.assert_called_once()
        assert result is expected

    def test_resolve_virtual_table_uses_local_blob(self):
        """A local serialized plan is still deserialized directly (no regression)."""
        buf = io.BytesIO()
        pl.LazyFrame({"x": [1, 2, 3]}).serialize(buf)
        result = _resolve_virtual_table(
            is_optimized=True, serialized_lf=buf.getvalue(), catalog_table_id=-1, run_location="local"
        )
        assert result.collect()["x"].to_list() == [1, 2, 3]


def _build_local_optimized_virtual(ns_id: int, table_name: str) -> int:
    """Run manual_input → age-filter → virtual writer, save the flow, return the new table id.

    Produces an optimized LOCAL virtual table (lazy upstream, no cloud source)."""
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        flow_path = f.name
    reg_id = _create_flow_registration(ns_id, name=f"prod_{table_name}", path=flow_path)
    graph = _create_graph(source_registration_id=reg_id)
    _add_manual_input(graph, SAMPLE_DATA, node_id=1)
    _add_age_filter(graph, node_id=2, depending_on_id=1)
    _add_catalog_writer(
        graph, node_id=3, depending_on_id=2, table_name=table_name, namespace_id=ns_id, write_mode="virtual"
    )
    _run_graph(graph)
    graph.save_flow(flow_path)
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        return next(t.id for t in repo.list_tables(namespace_id=ns_id) if t.name == table_name)


class TestServiceRefusesCloudCache:
    def test_resolve_refuses_cloud_tainted_blob_and_reexecutes(self):
        """If a virtual-table row carries a cloud-tainted serialized plan (e.g. written before
        this fix), ``resolve_virtual_flow_table`` ignores it and re-runs the producer flow."""
        ns_id = _create_namespace()
        vt_id = _build_local_optimized_virtual(ns_id, "tainted_vt")

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            row = repo.get_table(vt_id)
            assert row.is_optimized is True
            assert row.serialized_lazy_frame is not None  # a real local cache exists
            row.serialized_lazy_frame = _serialize_cloud_scan()  # simulate a cloud-tainted cache
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            df = svc.resolve_virtual_flow_table(vt_id, run_location="local").collect()

        # The guard refused the cloud blob and re-ran the producer (age > 28 → 2 rows),
        # rather than deserializing a plan that points at s3://example-bucket.
        assert df.height == 2
        assert set(df["name"].to_list()) == {"Alice", "Charlie"}


# ---- MinIO-gated: real cloud source leaves no cached plan ------------------ #


def _ensure_connection() -> None:
    conn = FullCloudStorageConnection(
        connection_name=_CONNECTION_NAME,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )
    with get_db_context() as db:
        try:
            store_cloud_connection(db, conn, user_id=1)
            db.commit()
        except ValueError as e:
            if "already exists" not in str(e):
                raise
            db.rollback()


def _create_cloud_schema(uri: str) -> int:
    """Create a cloud catalog (level 0, storage at *uri*) + child schema; return the schema id."""
    with get_db_context() as db:
        cat = NamespaceService(SQLAlchemyCatalogRepository(db)).create_namespace(
            f"cloudcat_{uuid.uuid4().hex[:8]}",
            owner_id=1,
            storage_uri=uri,
            storage_connection_name=_CONNECTION_NAME,
        )
        cat_id = cat.id
    with get_db_context() as db:
        schema = NamespaceService(SQLAlchemyCatalogRepository(db)).create_namespace(
            f"cloudsch_{uuid.uuid4().hex[:8]}", owner_id=1, parent_id=cat_id
        )
        return schema.id


@requires_minio
class TestCloudSourcedVirtualWriteE2E:
    def test_cloud_source_virtual_write_is_not_cached_and_reads_back(self):
        from shared.delta_utils import write_delta

        _ensure_connection()
        try:
            get_minio_client().create_bucket(Bucket=_BUCKET)
        except Exception:
            pass

        uri = f"s3://{_BUCKET}/cloudvt_{uuid.uuid4().hex[:8]}"
        schema_id = _create_cloud_schema(uri)

        # Materialize a physical Delta table in object storage and register it.
        target = resolve_for_namespace(schema_id)
        assert target.is_cloud is True
        dest = f"{uri}/source_physical"
        assert write_delta(pl.DataFrame(SAMPLE_DATA), dest, mode="overwrite", storage_options=target.storage_options) is True

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            phys = svc.register_table_from_data(
                name="cloud_source",
                table_path=dest,
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

        # Producer flow: read the cloud table → lazy filter → virtual write.
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            flow_path = f.name
        reg_id = _create_flow_registration(schema_id, name="cloud_producer", path=flow_path)
        graph = _create_graph(source_registration_id=reg_id)
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        )
        graph.add_catalog_reader(
            input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, catalog_table_id=phys_id)
        )
        _add_age_filter(graph, node_id=2, depending_on_id=1)
        _add_catalog_writer(
            graph, node_id=3, depending_on_id=2, table_name="cloud_vt", namespace_id=schema_id, write_mode="virtual"
        )
        _run_graph(graph)
        graph.save_flow(flow_path)

        # The cloud-sourced virtual table must carry NO serialized plan (no frozen creds).
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            vt = next(t for t in repo.list_tables(namespace_id=schema_id) if t.name == "cloud_vt")
            assert vt.table_type == "virtual"
            assert vt.is_optimized is False
            assert vt.serialized_lazy_frame is None
            vt_id = vt.id

        # Reading it re-runs the producer flow, resolving cloud credentials fresh — this is
        # exactly what makes credential rotation safe (nothing was frozen at write time).
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.resolve_virtual_flow_table(vt_id, run_location="local").collect()
        assert out.height == 2
        assert set(out["name"].to_list()) == {"Alice", "Charlie"}
