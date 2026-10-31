"""Object-storage catalog + virtual tables.

A virtual table caches no plan: every read runs its producer flow, so a cloud source's
credentials resolve fresh where the plan runs and a producer edit shows on the next read.

- Infra-free unit tests cover the serialized-frame cloud sniffer (the Explore Data node's
  guard) and a read that follows its producer flow.
- A MinIO-gated end-to-end test writes a virtual table over a cloud source and reads it back.
"""

import io
import tempfile
import uuid
from pathlib import Path

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.storage_backend import resolve_for_namespace, serialized_frame_uses_cloud
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.flowfile.flow_graph import _resolve_catalog_table_info, add_connection
from flowfile_core.flowfile.manage.io_flowfile import open_flow
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


class TestOptimizedVirtualReadsThroughItsProducer:
    def test_a_producer_edit_shows_on_the_next_read(self):
        """No plan is cached: editing the producer flow after the write changes what the table reads."""
        ns_id = _create_namespace()
        vt_id = _build_local_optimized_virtual(ns_id, "follows_producer")
        with get_db_context() as db:
            row = SQLAlchemyCatalogRepository(db).get_table(vt_id)
            assert row.is_optimized is True
            flow_path = SQLAlchemyCatalogRepository(db).get_flow(row.producer_registration_id).flow_path

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            assert svc.resolve_virtual_flow_table(vt_id, run_location="local").collect().height == 2
        reader = input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=vt_id)
        fingerprint = _resolve_catalog_table_info(reader).producer_fingerprint
        assert fingerprint is not None

        producer = open_flow(Path(flow_path))
        producer.add_filter(
            input_schema.NodeFilter(
                flow_id=producer.flow_id,
                node_id=2,
                depending_on_id=1,
                filter_input=FilterInput(
                    mode="basic", basic_filter=BasicFilter(field="age", operator="greater_than", value="0")
                ),
            )
        )
        producer.save_flow(flow_path)

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            assert svc.resolve_virtual_flow_table(vt_id, run_location="local").collect().height == 3
        assert _resolve_catalog_table_info(reader).producer_fingerprint not in (None, fingerprint)


# ---- MinIO-gated: a virtual table over a cloud source reads back ----------- #


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
    def test_cloud_source_virtual_write_reads_back(self):
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

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            vt = next(t for t in repo.list_tables(namespace_id=schema_id) if t.name == "cloud_vt")
            assert vt.table_type == "virtual"
            assert vt.is_optimized is True
            vt_id = vt.id

        # Reading it re-runs the producer flow, resolving cloud credentials fresh.
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.resolve_virtual_flow_table(vt_id, run_location="local").collect()
        assert out.height == 2
        assert set(out["name"].to_list()) == {"Alice", "Charlie"}
