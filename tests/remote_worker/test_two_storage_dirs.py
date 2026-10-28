"""Core and a worker that share no storage directory run a flow with previews.

Each service starts in a working directory of its own with a relative ``FLOWFILE_STORAGE_DIR`` of its own, so a
path under the worker's storage names, in core, a file that is not there, as it would on another host. The two share
the catalog database and the secure store, as a deployment's core and worker do. The worker reads the flow's CSV files
on this host, so it declares local files (``FLOWFILE_WORKER_LOCAL_FILES``). With the compute protocol flag on,
every node's preview, schema and record count comes from the worker and core's cache directory stays empty; with it
off, core opens the worker's files and the flow fails, which shows the services share no storage. A Train Model node
and an Apply Model node run the same way with the flag on: the model stays on the worker and Apply Model scores with it
by reference, and a model published to MinIO as the artifact store, behind a self-signed TLS forwarder, is applied from
the catalog. A catalog table on MinIO, under a connection that takes its keys from the environment, is read with
the flag on by a worker that has the keys and a core that has none, so core resolves no credential to learn its schema.
So are a cloud read with no saved connection and a cloud read of a directory under that connection, the worker's
environment being the only route to MinIO. Nothing here imports flowfile_core; the services start through
``test_utils.services``, as in ``tests/cloud_e2e``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest

from test_utils import tls
from test_utils.s3 import fixtures as s3
from test_utils.services import Services, failed_nodes, flow_document, running_services

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the services are managed as POSIX process groups")

FLOW_ID = 7381
ML_FLOW_ID = 7382
CATALOG_FLOW_ID = 7383
CLOUD_FLOW_ID = 7384
MINIO_BUCKET = "worker-test-bucket"

CUSTOMERS = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
SEGMENTS = [{"id": i, "segment": f"s{i % 3}"} for i in range(1, 7)]
KEPT = [row for row in CUSTOMERS if row["amount"] > 10]
JOINED = [{**row, "segment": f"s{row['id'] % 3}", "id_right": row["id"]} for row in KEPT]
TOTALS = [{"region": "eu", "total": 80}, {"region": "us", "total": 120}]
TRAIN_ROWS = [{"x1": float(i), "x2": float(i % 3), "y": 2.0 * i + 3.0 * (i % 3) + 1.0} for i in range(1, 9)]
CUSTOMER_COLUMNS = [("id", "Int64"), ("name", "String"), ("region", "String"), ("amount", "Int64")]
EXPECTED = {
    1: (CUSTOMER_COLUMNS, CUSTOMERS),
    2: (CUSTOMER_COLUMNS, KEPT),
    3: ([("id", "Int64"), ("segment", "String")], SEGMENTS),
    4: ([*CUSTOMER_COLUMNS, ("segment", "String"), ("id_right", "Int64")], JOINED),
    5: ([("region", "String"), ("total", "Int64")], TOTALS),
}
MINIO_OPTIONS = {
    "aws_region": "us-east-1",
    "endpoint_url": s3.MINIO_ENDPOINT_URL,
    "aws_allow_http": "true",
    "aws_access_key_id": s3.MINIO_ACCESS_KEY,
    "aws_secret_access_key": s3.MINIO_SECRET_KEY,
}
ENVIRONMENT_CONNECTION = "minio from the environment"


def _running(root: Path, protocol: str, *, worker_env: dict[str, str] | None = None, **env: str):
    """Core and a worker with storage directories of their own; ``env`` goes to both, ``worker_env`` to the worker."""
    shared = {"FLOWFILE_COMPUTE_PROTOCOL": protocol, "FLOWFILE_WORKER_LOCAL_FILES": "1", **env}
    return running_services(root, "electron", shared, worker_env=worker_env, own_storage=True)


@pytest.fixture
def minio_prefix() -> Iterator[str]:
    """A fresh prefix in ``MINIO_BUCKET``, emptied afterwards; the test skips when Docker cannot give MinIO."""
    if not s3.minio_available():
        pytest.skip("MinIO (Docker) is not available")
    prefix = f"two-dirs-{uuid.uuid4().hex}"
    yield prefix
    minio = s3.get_minio_client()
    for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", []):
        minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])


def _environment_connection(services: Services) -> None:
    """An S3 connection to MinIO named ``ENVIRONMENT_CONNECTION`` that takes its keys from the environment."""
    connection = {
        "connection_name": ENVIRONMENT_CONNECTION,
        "storage_type": "s3",
        "auth_method": "env_vars",
        "aws_region": "us-east-1",
        "endpoint_url": s3.MINIO_ENDPOINT_URL,
        "aws_allow_unsafe_html": True,
    }
    services.post("/cloud_connections/cloud_connection", connection)


def _read(node_id: int, path: Path, outputs: list[int]) -> dict:
    received_file = {
        "name": path.name,
        "path": str(path),
        "file_type": "csv",
        "table_settings": {"file_type": "csv", "delimiter": ",", "has_headers": True, "encoding": "utf-8"},
    }
    return {
        "id": node_id,
        "type": "read",
        "is_start_node": True,
        "outputs": outputs,
        "setting_input": {"received_file": received_file},
    }


def _flow(root: Path) -> dict:
    """Read 1 → filter 2 → join 4 with read 3 → group_by 5, in Development mode on the worker."""
    data = root / "data"
    data.mkdir()
    pl.DataFrame(CUSTOMERS).write_csv(data / "customers.csv")
    pl.DataFrame(SEGMENTS).write_csv(data / "segments.csv")
    join_input = {
        "join_mapping": [{"left_col": "id", "right_col": "id"}],
        "left_select": {"select": []},
        "right_select": {"select": []},
        "how": "inner",
    }
    aggregations = [{"old_name": "region", "agg": "groupby"}, {"old_name": "amount", "agg": "sum", "new_name": "total"}]
    return flow_document(
        FLOW_ID,
        "two storage dirs",
        [
            _read(1, data / "customers.csv", [2]),
            {
                "id": 2,
                "type": "filter",
                "input_ids": [1],
                "outputs": [4],
                "setting_input": {"filter_input": {"mode": "advanced", "advanced_filter": "[amount] > 10"}},
            },
            _read(3, data / "segments.csv", [4]),
            {
                "id": 4,
                "type": "join",
                "input_ids": [2],
                "right_input_id": 3,
                "outputs": [5],
                "setting_input": {"join_input": join_input},
            },
            {
                "id": 5,
                "type": "group_by",
                "input_ids": [4],
                "setting_input": {"groupby_input": {"agg_cols": aggregations}},
            },
        ],
    )


def _ml_flow(root: Path, flow_id: int, *, published: str | None = None) -> dict:
    """Read 1 → Train Model 2 → Apply Model 3 into ``pred``, in Development mode on the worker.

    Apply Model scores with the model Train Model holds; with ``published`` Train Model publishes it to the catalog
    under that name and Apply Model reads it from there.
    """
    data = root / "data"
    data.mkdir()
    pl.DataFrame(TRAIN_ROWS).write_csv(data / "train.csv")
    train_input = {
        "target_column": "y",
        "feature_columns": ["x1", "x2"],
        "model_type": "linear_regression",
        "params": {"add_bias": True},
    }
    apply_input = {"source": "upstream", "upstream_node_id": 2, "output_column": "pred"}
    if published is not None:
        train_input.update(publish_to_catalog=True, model_name=published)
        apply_input = {"source": "catalog", "model_name": published, "output_column": "pred"}
    return flow_document(
        flow_id,
        "ml two storage dirs",
        [
            _read(1, data / "train.csv", [2]),
            {
                "id": 2,
                "type": "train_model",
                "input_ids": [1],
                "outputs": [3],
                "setting_input": {"train_input": train_input},
            },
            {"id": 3, "type": "apply_model", "input_ids": [2], "setting_input": {"apply_input": apply_input}},
        ],
    )


def _predictions(services: Services, flow_id: int) -> list[float]:
    """Apply Model's ``pred`` in its preview, rounded, in the order of ``TRAIN_ROWS``."""
    preview = services.get("/node/data", flow_id=flow_id, node_id=3)
    by_x1 = {row["x1"]: row["pred"] for row in preview["data"]}
    return [round(by_x1[row["x1"]], 6) for row in TRAIN_ROWS]


def _key(row: dict) -> tuple:
    return tuple(row.values())


def _assert_preview(
    services: Services, flow_id: int, node_id: int, columns: list = CUSTOMER_COLUMNS, rows: list = CUSTOMERS
) -> None:
    """The node's preview has these columns and these rows, in any order, and counts them."""
    preview = services.get("/node/data", flow_id=flow_id, node_id=node_id)
    assert [(column["name"], column["data_type"]) for column in preview["table_schema"]] == columns, node_id
    assert sorted(preview["data"], key=_key) == sorted(rows, key=_key), node_id
    assert preview["number_of_records"] == len(rows), node_id


def test_core_takes_every_preview_schema_and_count_from_the_worker(tmp_path):
    with _running(tmp_path, protocol="1") as services:
        flow_id = services.import_flow(_flow(tmp_path))
        run = services.run(flow_id)

        assert run["success"] is True and not failed_nodes(run), failed_nodes(run)
        for node_id, (columns, rows) in EXPECTED.items():
            _assert_preview(services, flow_id, node_id, columns, rows)
        assert any(path.is_file() for path in (services.storage["worker"] / "cache" / str(flow_id)).iterdir())
        assert list((services.storage["core"] / "cache").iterdir()) == []


def test_with_the_flag_off_core_opens_the_workers_files_and_the_flow_fails(tmp_path):
    """Guards the harness: the task routes need one disk, which these two services do not share."""
    with _running(tmp_path, protocol="0") as services:
        run = services.run(services.import_flow(_flow(tmp_path)))

        assert run["success"] is False
        assert any("No such file or directory" in error for error in failed_nodes(run).values()), failed_nodes(run)


@pytest.mark.parametrize("protocol", ["1", "0"])
def test_a_catalog_table_on_object_storage_is_read_with_the_workers_credentials_only(tmp_path, protocol, minio_prefix):
    """Only the worker holds MinIO's keys: with the flag off core opens the table itself and the read fails."""
    storage_uri = f"s3://{MINIO_BUCKET}/{minio_prefix}/catalog"
    table_path = f"{storage_uri}/customers"
    pl.DataFrame(CUSTOMERS).write_delta(table_path, storage_options=MINIO_OPTIONS)
    keys = {"AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY}
    with _running(tmp_path, protocol=protocol, worker_env=keys, AWS_EC2_METADATA_DISABLED="true") as services:
        _environment_connection(services)
        catalog = services.post(
            "/catalog/namespaces",
            {"name": "minio", "storage_uri": storage_uri, "storage_connection_name": ENVIRONMENT_CONNECTION},
        )
        schema = services.post("/catalog/namespaces", {"name": "sales", "parent_id": catalog["id"]})
        table = services.post(
            "/catalog/tables/from-data",
            {
                "name": "customers",
                "table_path": table_path,
                "namespace_id": schema["id"],
                "schema_columns": [{"name": name, "dtype": dtype} for name, dtype in CUSTOMER_COLUMNS],
                "row_count": len(CUSTOMERS),
                "column_count": len(CUSTOMER_COLUMNS),
                "size_bytes": 1,
            },
        )
        reader = {"catalog_table_id": table["id"]}
        nodes = [{"id": 1, "type": "catalog_reader", "is_start_node": True, "setting_input": reader}]
        flow_id = services.import_flow(flow_document(CATALOG_FLOW_ID, "catalog read two storage dirs", nodes))
        run = services.run(flow_id)

        if protocol == "1":
            assert run["success"] is True and not failed_nodes(run), failed_nodes(run)
            _assert_preview(services, flow_id, 1)
            assert list((services.storage["core"] / "cache").iterdir()) == []
        else:
            assert run["success"] is False
            assert 1 in failed_nodes(run), failed_nodes(run)


@pytest.mark.parametrize("protocol", ["1", "0"])
def test_cloud_reads_core_cannot_resolve_use_the_workers_identity_only(tmp_path, protocol, minio_prefix):
    """A read with no saved connection and a directory read under an ``env_vars`` connection; only the worker has keys.

    With the flag off core builds both reads itself: boto3's default chain finds no credentials for the first, and the
    directory is read with none.
    """
    base = f"s3://{MINIO_BUCKET}/{minio_prefix}"
    customers = pl.DataFrame(CUSTOMERS)
    customers.write_parquet(f"{base}/customers.parquet", storage_options=MINIO_OPTIONS)
    customers.head(3).write_parquet(f"{base}/parts/part0.parquet", storage_options=MINIO_OPTIONS)
    customers.tail(3).write_parquet(f"{base}/parts/part1.parquet", storage_options=MINIO_OPTIONS)
    worker_env = {
        "AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": s3.MINIO_ENDPOINT_URL,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
    }
    with _running(tmp_path, protocol=protocol, worker_env=worker_env, AWS_EC2_METADATA_DISABLED="true") as services:
        _environment_connection(services)
        readers = {
            1: {"resource_path": f"{base}/customers.parquet", "auth_mode": "aws-cli", "connection_name": None},
            2: {
                "resource_path": f"{base}/parts/",
                "scan_mode": "directory",
                "connection_name": ENVIRONMENT_CONNECTION,
            },
        }
        nodes = [
            {
                "id": node_id,
                "type": "cloud_storage_reader",
                "is_start_node": True,
                "setting_input": {"cloud_storage_settings": {"file_format": "parquet", **read}},
            }
            for node_id, read in readers.items()
        ]
        flow_id = services.import_flow(flow_document(CLOUD_FLOW_ID, "cloud read two storage dirs", nodes))
        run = services.run(flow_id)

        if protocol == "1":
            assert run["success"] is True and not failed_nodes(run), failed_nodes(run)
            for node_id in readers:
                _assert_preview(services, flow_id, node_id)
            assert list((services.storage["core"] / "cache").iterdir()) == []
        else:
            assert run["success"] is False
            assert set(readers) <= set(failed_nodes(run)), failed_nodes(run)


def test_a_model_trained_on_the_worker_scores_the_next_node_with_no_shared_disk(tmp_path):
    with _running(tmp_path, protocol="1") as services:
        flow_id = services.import_flow(_ml_flow(tmp_path, ML_FLOW_ID))
        run = services.run(flow_id)

        assert run["success"] is True and not failed_nodes(run), failed_nodes(run)
        assert _predictions(services, flow_id) == [row["y"] for row in TRAIN_ROWS]
        assert list((services.storage["core"] / "cache").iterdir()) == []
        assert list((services.storage["worker"] / "cache").rglob("*.blob"))


@pytest.fixture(scope="module")
def minio_over_tls(tmp_path_factory):
    """MinIO behind a self-signed HTTPS forwarder: the forwarder's URL and the certificate's path."""
    with tls.minio_over_tls(tmp_path_factory.mktemp("tls")) as endpoint:
        yield endpoint


def test_a_model_published_to_minio_is_applied_from_the_catalog_with_no_shared_disk(
    tmp_path, minio_over_tls, minio_prefix
):
    """Both services trust the forwarder's certificate: core's boto3 finalises the version, the worker moves the bytes."""
    endpoint, certificate = minio_over_tls
    prefix = f"{minio_prefix}/"
    store = {
        "FLOWFILE_ARTIFACT_STORAGE": "s3",
        "FLOWFILE_S3_BUCKET": MINIO_BUCKET,
        "FLOWFILE_S3_PREFIX": prefix,
        "FLOWFILE_S3_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY,
        "AWS_CA_BUNDLE": str(certificate),
        "REQUESTS_CA_BUNDLE": str(certificate),
    }
    flow = _ml_flow(tmp_path, ML_FLOW_ID, published=f"model_{uuid.uuid4().hex[:12]}")
    with _running(tmp_path, protocol="1", **store) as services:
        flow_id = services.import_flow(flow)
        run = services.run(flow_id)

        assert run["success"] is True and not failed_nodes(run), failed_nodes(run)
        assert _predictions(services, flow_id) == [row["y"] for row in TRAIN_ROWS]
        objects = s3.get_minio_client().list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", [])
        assert len(objects) == 1
        assert list((services.storage["core"] / "cache").iterdir()) == []
