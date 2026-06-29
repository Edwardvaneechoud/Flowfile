"""End-to-end coverage of the per-catalog object-storage credential bridge (core side).

Creates a real ``CloudStorageConnection`` and a level-0 catalog whose
``storage_uri`` / ``storage_connection_name`` point at it, then asserts
``resolve_for_namespace`` produces a cloud target whose decrypted ``storage_options``
are usable in-process and whose ``to_worker_payload`` carries the secret re-encrypted
(owner-keyed) for the worker. The actual S3 byte I/O is covered by the Docker-gated
shared/worker cloud suites (and by ``test_catalog_namespace_storage``'s MinIO round-trip).
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.storage_backend import resolve_for_namespace
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

_CONNECTION_NAME = "catalog-minio-e2e"
_CATALOG_URI = "s3://flowfile-test/catalog"


@pytest.fixture(autouse=True)
def _no_env_catalog_storage(monkeypatch):
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)


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
def _cloud_catalog_id() -> int:
    with get_db_context() as db:
        try:
            store_cloud_connection(db, _make_minio_connection(), user_id=1)
            db.commit()
        except ValueError as e:
            # Only tolerate the "already exists" collision from a prior test in the same DB.
            if "already exists" not in str(e):
                raise
            db.rollback()
    with get_db_context() as db:
        svc = NamespaceService(SQLAlchemyCatalogRepository(db))
        ns = svc.create_namespace(
            f"e2ecat_{uuid.uuid4().hex[:8]}",
            owner_id=1,
            storage_uri=_CATALOG_URI,
            storage_connection_name=_CONNECTION_NAME,
        )
        return ns.id


def test_resolve_returns_cloud_target_with_decrypted_options(_cloud_catalog_id):
    target = resolve_for_namespace(_cloud_catalog_id)

    assert target.is_cloud is True
    assert target.base == _CATALOG_URI
    assert target.connection_name == _CONNECTION_NAME
    assert target.storage_options["aws_access_key_id"] == "minioadmin"
    assert target.storage_options["aws_secret_access_key"] == "minioadmin"
    assert target.storage_options["endpoint_url"] == "http://localhost:9000"


def test_worker_payload_carries_encrypted_secret(_cloud_catalog_id):
    payload = resolve_for_namespace(_cloud_catalog_id).to_worker_payload()

    assert payload is not None
    assert payload["base_uri"] == _CATALOG_URI
    conn = payload["connection"]
    assert conn["aws_access_key_id"] == "minioadmin"
    secret = conn["aws_secret_access_key"]
    assert secret.startswith("$ffsec$")
    assert "minioadmin" not in secret


def test_resolve_local_when_namespace_has_no_storage():
    with get_db_context() as db:
        svc = NamespaceService(SQLAlchemyCatalogRepository(db))
        local_id = svc.create_namespace(f"e2elocal_{uuid.uuid4().hex[:8]}", owner_id=1).id
    target = resolve_for_namespace(local_id)
    assert target.is_cloud is False
    assert target.to_worker_payload() is None
