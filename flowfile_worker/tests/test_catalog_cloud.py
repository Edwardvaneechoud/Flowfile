"""Object-storage coverage for the worker catalog read/write path (MinIO, Docker-gated).

Validates the core→worker credential hand-off (``_resolve_storage_options`` decrypts
the owner-encrypted connection), the cloud branch of ``open_catalog_table`` /
``read_table_metadata``, and that a Delta table actually lands in object storage.
"""

import polars as pl
import pytest

from flowfile_worker import funcs
from flowfile_worker.catalog_reader import open_catalog_table
from shared.delta_utils import write_delta

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available, managed_minio
    from tests.utils import cloud_storage_connection_settings  # noqa: F401  (pytest fixture)
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available, managed_minio
    from utils import cloud_storage_connection_settings  # noqa: F401

requires_docker = pytest.mark.skipif(not is_docker_available(), reason="Docker required for MinIO")

_BASE_URI = "s3://worker-test-bucket/catalog"


def _catalog_payload(conn) -> dict:
    """Build a ``CatalogStorageInterface`` payload as core sends it to the worker.

    The connection secrets are the *encrypted* tokens (plain strings, as the core
    worker-interface model_dumps them), which the worker decrypts itself.
    """
    return {
        "base_uri": _BASE_URI,
        "connection": {
            "storage_type": "s3",
            "auth_method": "access_key",
            "connection_name": "minio-test",
            "aws_region": "us-east-1",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": conn.aws_secret_access_key.get_secret_value(),
            "endpoint_url": "http://localhost:9000",
            "aws_allow_unsafe_html": True,
        },
    }


def test_resolve_storage_options_none_is_local():
    assert funcs._resolve_storage_options(None) is None
    assert funcs._resolve_storage_options({}) is None


@requires_docker
def test_storage_options_decrypt_roundtrip(cloud_storage_connection_settings):
    """The worker decrypts the owner-encrypted connection back to usable options."""
    payload = _catalog_payload(cloud_storage_connection_settings)
    opts = funcs._resolve_storage_options(payload)
    assert opts["aws_access_key_id"] == "minioadmin"
    assert opts["aws_secret_access_key"] == "minioadmin"
    assert opts["endpoint_url"] == "http://localhost:9000"
    assert opts["aws_allow_http"] == "true"


@requires_docker
def test_worker_catalog_cloud_write_read_metadata(cloud_storage_connection_settings):
    with managed_minio():
        payload = _catalog_payload(cloud_storage_connection_settings)
        opts = funcs._resolve_storage_options(payload)
        name = "wtbl"
        uri = _BASE_URI + "/" + name

        write_delta(pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), uri, mode="overwrite", storage_options=opts)

        # Worker reader returns rows from object storage.
        lf = open_catalog_table(name, base_uri=_BASE_URI, storage_options=opts)
        assert lf.collect().height == 3

        # Worker metadata reader reports the cloud table's shape/size.
        meta = funcs.read_table_metadata(name, base_uri=_BASE_URI, storage_options=opts)
        assert meta["row_count"] == 3
        assert meta["column_count"] == 2
        assert meta["size_bytes"] > 0

        # The Delta log actually exists in object storage.
        client = get_minio_client()
        listed = client.list_objects_v2(Bucket="worker-test-bucket", Prefix="catalog/wtbl/_delta_log/")
        assert listed.get("KeyCount", 0) > 0
