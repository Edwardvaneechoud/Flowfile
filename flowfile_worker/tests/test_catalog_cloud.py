"""Object-storage coverage for the worker catalog read/write path (MinIO).

Connects to the MinIO mock the test job already started (``poetry run start_minio``);
it must NOT manage that container's lifecycle. Validates the core→worker credential
hand-off (``_resolve_storage_options`` decrypts the owner-encrypted connection), the
cloud branch of ``open_catalog_table`` / ``read_table_metadata``, and that a Delta
table actually lands in object storage.
"""

import polars as pl
import pytest

from flowfile_worker import funcs
from flowfile_worker.catalog_reader import open_catalog_table
from flowfile_worker.secrets import encrypt_secret
from shared.delta_utils import write_delta

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

_BUCKET = "worker-test-bucket"
_BASE_URI = f"s3://{_BUCKET}/catalog"


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


def _ensure_bucket() -> None:
    try:
        get_minio_client().create_bucket(Bucket=_BUCKET)
    except Exception:
        pass  # already exists


def _catalog_payload() -> dict:
    """Build a ``CatalogStorageInterface`` payload exactly as core sends it to the worker.

    ``aws_secret_access_key`` is an owner-encrypted ``$ffsec$…`` token (built here via
    ``encrypt_secret``, mirroring core's worker-interface), so the test exercises the
    real HKDF decrypt inside ``_resolve_storage_options`` rather than passing plaintext.
    """
    encrypted_secret = encrypt_secret("minioadmin")
    assert encrypted_secret.startswith("$ffsec$")
    return {
        "base_uri": _BASE_URI,
        "connection": {
            "storage_type": "s3",
            "auth_method": "access_key",
            "connection_name": "minio-test",
            "aws_region": "us-east-1",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": encrypted_secret,
            "endpoint_url": "http://localhost:9000",
            "aws_allow_unsafe_html": True,
        },
    }


def test_resolve_storage_options_none_is_local():
    assert funcs._resolve_storage_options(None) is None
    assert funcs._resolve_storage_options({}) is None


@requires_minio
def test_storage_options_decrypt_roundtrip():
    """The worker decrypts the owner-encrypted connection back to usable options."""
    opts = funcs._resolve_storage_options(_catalog_payload())
    assert opts["aws_access_key_id"] == "minioadmin"
    assert opts["aws_secret_access_key"] == "minioadmin"
    assert opts["endpoint_url"] == "http://localhost:9000"
    assert opts["aws_allow_http"] == "true"


@requires_minio
def test_worker_catalog_cloud_write_read_metadata():
    _ensure_bucket()
    opts = funcs._resolve_storage_options(_catalog_payload())
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
    listed = get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix="catalog/wtbl/_delta_log/")
    assert listed.get("KeyCount", 0) > 0
