"""Cloud Delta writer merge modes + change tracking, and the cloud reader's change feed.

The writer and reader accept a plain filesystem path in ``resource_path`` (auth ``auto`` with no
connection fabricates an env-var S3 connection whose options deltalake ignores locally), so the
behaviour is proven here on local Delta tables; emulator-gated tests repeat it on ``s3://`` and ``az://``.
"""

import datetime
import time
import uuid

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import flow_graph as fg
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import cloud_storage_schemas, input_schema
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.delta_utils import (
    get_change_data_feed_floor,
    get_delta_head_version,
    is_change_data_feed_enabled,
    write_delta,
)
from test_utils.azurite.fixtures import (
    AZURITE_ACCOUNT_KEY,
    AZURITE_ACCOUNT_NAME,
    AZURITE_BLOB_PORT,
    AZURITE_HOST,
    is_azurite_available,
)
from test_utils.s3.fixtures import get_minio_client, is_docker_available
from tests.flowfile.conftest import add_test_manual_input, create_test_graph, run_test_graph

V0 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
V1 = [{"id": 2, "name": "Bobby"}, {"id": 3, "name": "Carol"}]
V2 = [{"id": 1, "name": "Alice"}]
CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

_BUCKET = "flowfile-test"
_MINIO_CONNECTION = FullCloudStorageConnection(
    connection_name="cloud-delta-features-minio",
    storage_type="s3",
    auth_method="access_key",
    aws_access_key_id="minioadmin",
    aws_secret_access_key=SecretStr("minioadmin"),
    aws_region="us-east-1",
    endpoint_url="http://localhost:9000",
    aws_allow_unsafe_html=True,
)
_AZURITE_CONNECTION = FullCloudStorageConnection(
    connection_name="cloud-delta-features-azurite",
    storage_type="adls",
    auth_method="access_key",
    azure_account_name=AZURITE_ACCOUNT_NAME,
    azure_account_key=SecretStr(AZURITE_ACCOUNT_KEY),
    endpoint_url=f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
)


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


def _offload_forbidden(*args, **kwargs):
    raise AssertionError("cloud Delta write offloaded to the worker during local execution")


@pytest.fixture
def local_only(monkeypatch):
    monkeypatch.setattr(fg, "_write_cloud_delta_remote", _offload_forbidden)


def _write(path, data, write_mode, *, flow_id, connection_name=None, execution_location="local", track_changes=True):
    graph = create_test_graph(flow_id=flow_id, execution_location=execution_location)
    add_test_manual_input(graph, data, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="cloud_storage_writer"))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter(
            flow_id=flow_id,
            node_id=2,
            user_id=1,
            depending_on_id=1,
            cloud_storage_settings=cloud_storage_schemas.CloudStorageWriteSettings(
                resource_path=path,
                connection_name=connection_name,
                file_format="delta",
                write_mode=write_mode,
                merge_keys=["id"] if write_mode in ("upsert", "update", "delete") else [],
                track_changes=track_changes,
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
    run_test_graph(graph)


def _reader_graph(path, *, flow_id, connection_name=None, execution_location="local", **cdc):
    graph = create_test_graph(flow_id=flow_id, execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="cloud_storage_reader"))
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=flow_id,
            node_id=1,
            user_id=1,
            cloud_storage_settings=cloud_storage_schemas.CloudStorageReadSettings(
                resource_path=path, connection_name=connection_name, file_format="delta", **cdc
            ),
        )
    )
    return graph


def _read(graph) -> pl.DataFrame:
    run_test_graph(graph)
    return graph.get_node(1).get_resulting_data().collect().sort("id", "_change_type")


def _seed(path, **kw) -> str:
    """Create (v0), upsert a changed + a new row (v1), delete one (v2); return an instant between v0 and v1."""
    _write(path, V0, "upsert", flow_id=1, **kw)
    time.sleep(0.01)
    instant = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _write(path, V1, "upsert", flow_id=2, **kw)
    _write(path, V2, "delete", flow_id=3, **kw)
    return instant


def _assert_seeded_changes(df: pl.DataFrame) -> None:
    assert df.select("id", "name", "_change_type", "_commit_version").rows() == [
        (1, "Alice", "delete", 2),
        (2, "Bobby", "update_postimage", 1),
        (3, "Carol", "insert", 1),
    ]


class TestCloudDeltaWriter:
    def test_upsert_then_delete_with_change_tracking(self, tmp_path, local_only):
        path = str(tmp_path / "tracked")
        _write(path, V0, "upsert", flow_id=1)
        assert is_change_data_feed_enabled(path)
        assert get_change_data_feed_floor(path) == 0

        _write(path, V1, "upsert", flow_id=2)
        _write(path, V2, "delete", flow_id=3)
        assert pl.scan_delta(path).collect().sort("id").rows() == [(2, "Bobby"), (3, "Carol")]
        assert get_delta_head_version(path) == 2

    def test_track_changes_enables_the_feed_on_an_existing_table(self, tmp_path, local_only):
        path = str(tmp_path / "untracked")
        write_delta(pl.DataFrame(V0), path)
        assert get_change_data_feed_floor(path) is None

        _write(path, V1, "append", flow_id=1)
        assert get_change_data_feed_floor(path) == 1
        assert get_delta_head_version(path) == 2


class TestCloudDeltaChangeReader:
    def test_since_version_returns_the_upsert_and_delete_rows(self, tmp_path, local_only):
        path = str(tmp_path / "tracked")
        _seed(path)

        graph = _reader_graph(path, flow_id=10, cdc_mode="since_version", cdc_from_version=0)
        assert [c.column_name for c in graph.get_node(1).schema] == ["id", "name", *CDF_COLUMNS]
        _assert_seeded_changes(_read(graph))

        with_preimage = _read(
            _reader_graph(path, flow_id=11, cdc_mode="since_version", cdc_from_version=0, cdc_include_preimage=True)
        )
        assert with_preimage.filter(pl.col("_change_type") == "update_preimage").select("id", "name").rows() == [
            (2, "Bob")
        ]
        assert with_preimage.height == 4

    def test_since_timestamp_future_is_empty_and_param_resolves(self, tmp_path, local_only):
        path = str(tmp_path / "tracked")
        instant = _seed(path)

        future = _read(
            _reader_graph(path, flow_id=10, cdc_mode="since_timestamp", cdc_from_timestamp="2999-01-01T00:00:00+00:00")
        )
        assert future.height == 0
        assert future.columns == ["id", "name", *CDF_COLUMNS]

        graph = _reader_graph(path, flow_id=11, cdc_mode="since_timestamp", cdc_from_timestamp="${since}")
        graph.flow_settings.parameters = [FlowParameter(name="since", default_value=instant, type="string")]
        _assert_seeded_changes(_read(graph))

    def test_untracked_table_is_an_actionable_error(self, tmp_path):
        path = str(tmp_path / "untracked")
        write_delta(pl.DataFrame(V0), path)

        run_info = _reader_graph(path, flow_id=10, cdc_mode="since_version", cdc_from_version=0).run_graph()
        assert not run_info.success
        assert "Change tracking is not enabled" in str(run_info.node_step_result[0].error)


def _ensure_connection(conn: FullCloudStorageConnection) -> None:
    with get_db_context() as db:
        try:
            store_cloud_connection(db, conn, user_id=1)
            db.commit()
        except ValueError as e:
            if "already exists" not in str(e):
                raise
            db.rollback()


@requires_minio
def test_s3_upsert_delete_and_change_read(execution_location, monkeypatch):
    if execution_location == "local":
        monkeypatch.setattr(fg, "_write_cloud_delta_remote", _offload_forbidden)
    try:
        get_minio_client().create_bucket(Bucket=_BUCKET)
    except Exception:
        pass
    _ensure_connection(_MINIO_CONNECTION)
    path = f"s3://{_BUCKET}/cloud_delta_{uuid.uuid4().hex[:8]}"
    kw = {"connection_name": _MINIO_CONNECTION.connection_name, "execution_location": execution_location}
    _seed(path, **kw)

    opts = fg.CloudStorageReader.get_storage_options(_MINIO_CONNECTION)
    assert get_change_data_feed_floor(path, opts) == 0
    assert get_delta_head_version(path, opts) == 2
    assert pl.scan_delta(path, storage_options=opts).collect().sort("id").rows() == [(2, "Bobby"), (3, "Carol")]

    _assert_seeded_changes(_read(_reader_graph(path, flow_id=10, cdc_mode="since_version", cdc_from_version=0, **kw)))


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite emulator not available")
def test_adls_upsert_and_change_read(local_only):
    """``az://`` paths reach delta-rs as ``abfss://`` on both the merge write and the change read."""
    _ensure_connection(_AZURITE_CONNECTION)
    path = f"az://{_BUCKET}/cloud_delta_{uuid.uuid4().hex[:8]}"
    kw = {"connection_name": _AZURITE_CONNECTION.connection_name}
    _seed(path, **kw)

    _assert_seeded_changes(_read(_reader_graph(path, flow_id=10, cdc_mode="since_version", cdc_from_version=0, **kw)))
