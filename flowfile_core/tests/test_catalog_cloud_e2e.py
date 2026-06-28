"""End-to-end coverage of the catalog object-storage credential bridge (core side).

Creates a real ``CloudStorageConnection``, points the catalog storage config at it,
and asserts ``resolve_catalog_storage`` produces a cloud target whose decrypted
``storage_options`` are usable in-process and whose ``to_worker_payload`` carries the
secret re-encrypted (owner-keyed) for the worker. The actual S3 byte I/O is covered by
the Docker-gated shared/worker cloud suites.
"""

import pytest
from pydantic import SecretStr

from flowfile_core.catalog.storage_backend import resolve_catalog_storage
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

_CONNECTION_NAME = "catalog-minio-e2e"
_CATALOG_URI = "s3://flowfile-test/catalog"


def _make_minio_connection() -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        connection_name=_CONNECTION_NAME,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )


@pytest.fixture
def _stored_connection():
    with get_db_context() as db:
        try:
            store_cloud_connection(db, _make_minio_connection(), user_id=1)
            db.commit()
        except ValueError as e:
            # Only tolerate the "already exists" collision when a prior test in the same
            # DB session stored it; any other validation error is a real failure.
            if "already exists" not in str(e):
                raise
            db.rollback()


def test_resolve_returns_cloud_target_with_decrypted_options(monkeypatch, _stored_connection):
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", _CATALOG_URI)
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", _CONNECTION_NAME)

    target = resolve_catalog_storage(1)

    assert target.is_cloud is True
    assert target.base == _CATALOG_URI
    assert target.connection_name == _CONNECTION_NAME
    # Decrypted, ready for core's own in-process scan_delta.
    assert target.storage_options["aws_access_key_id"] == "minioadmin"
    assert target.storage_options["aws_secret_access_key"] == "minioadmin"
    assert target.storage_options["endpoint_url"] == "http://localhost:9000"
    assert target.storage_options["aws_allow_http"] == "true"


def test_worker_payload_carries_encrypted_secret(monkeypatch, _stored_connection):
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", _CATALOG_URI)
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", _CONNECTION_NAME)

    payload = resolve_catalog_storage(1).to_worker_payload()

    assert payload is not None
    assert payload["base_uri"] == _CATALOG_URI
    conn = payload["connection"]
    assert conn["aws_access_key_id"] == "minioadmin"
    secret = conn["aws_secret_access_key"]
    # Encrypted for the worker (owner-keyed), never plaintext or masked.
    assert secret.startswith("$ffsec$")
    assert "minioadmin" not in secret


def test_resolve_local_when_unset(monkeypatch):
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
    target = resolve_catalog_storage(1)
    assert target.is_cloud is False
    assert target.to_worker_payload() is None
