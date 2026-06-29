"""Unit tests for the per-catalog storage resolver (``catalog/storage_backend.py``).

Storage resolves from the level-0 catalog's ``storage_uri`` / ``storage_connection_name``
columns; schemas inherit from their root catalog (one namespace <-> one storage). These cover
the local default (the byte-for-byte zero-regression guard), the cloud credential bridge,
inheritance, and the resolver's defensive error branches — no object-storage I/O required
(credential decryption only).
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.storage_backend import (
    CatalogStorageTarget,
    _is_cloud_uri,
    join_catalog_uri,
    resolve_catalog_storage,
    resolve_for_namespace,
)
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.storage_config import storage

_CONNECTION_NAME = "catalog-sb-minio"
_CATALOG_URI = "s3://flowfile-test/catalog"


@pytest.fixture(autouse=True)
def _no_env_catalog_storage(monkeypatch):
    """Keep the creation-time env default out of these resolver tests."""
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)


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


def _make_namespace(*, storage_uri=None, storage_connection_name=None, parent_id=None) -> int:
    name = f"sb_{uuid.uuid4().hex[:8]}"
    with get_db_context() as db:
        svc = NamespaceService(SQLAlchemyCatalogRepository(db))
        ns = svc.create_namespace(
            name,
            owner_id=1,
            parent_id=parent_id,
            storage_uri=storage_uri,
            storage_connection_name=storage_connection_name,
        )
        return ns.id


def _insert_raw_catalog(*, storage_uri, storage_connection_name) -> int:
    """Insert a level-0 catalog directly (bypassing service validation) to exercise the
    resolver's defensive error branches for manually-corrupted rows."""
    name = f"sbraw_{uuid.uuid4().hex[:8]}"
    with get_db_context() as db:
        ns = db_models.CatalogNamespace(
            name=name,
            parent_id=None,
            level=0,
            owner_id=1,
            storage_uri=storage_uri,
            storage_connection_name=storage_connection_name,
        )
        db.add(ns)
        db.commit()
        db.refresh(ns)
        return ns.id


# ---- Pure helpers (no DB) -------------------------------------------------- #


def test_is_cloud_uri():
    assert _is_cloud_uri("s3://bucket/catalog")
    assert _is_cloud_uri("gs://bucket/catalog")
    assert _is_cloud_uri("abfss://container/catalog")
    assert not _is_cloud_uri("/home/user/.flowfile/catalog_tables")
    assert not _is_cloud_uri("relative/path")


def test_join_catalog_uri_cloud_keeps_scheme():
    assert join_catalog_uri("s3://bucket/catalog", "t_ab12") == "s3://bucket/catalog/t_ab12"
    assert join_catalog_uri("s3://bucket/catalog/", "t_ab12") == "s3://bucket/catalog/t_ab12"


def test_join_catalog_uri_local_is_filesystem_path():
    assert join_catalog_uri("/a/b", "t_ab12") == "/a/b/t_ab12"


def test_local_target_to_worker_payload_is_none():
    target = CatalogStorageTarget(is_cloud=False, base="/tmp/catalog")
    assert target.to_worker_payload() is None


# ---- Namespace-driven resolution ------------------------------------------- #


def test_resolve_namespace_none_is_local():
    target = resolve_for_namespace(None)
    assert target.is_cloud is False
    assert target.base == str(storage.catalog_tables_directory)
    assert target.to_worker_payload() is None


def test_resolve_catalog_without_storage_is_local():
    cat_id = _make_namespace()
    target = resolve_for_namespace(cat_id)
    assert target.is_cloud is False
    assert target.base == str(storage.catalog_tables_directory)
    assert target.storage_options == {}
    assert target.connection_name is None


def test_resolve_catalog_with_storage_is_cloud():
    _ensure_connection()
    cat_id = _make_namespace(storage_uri=_CATALOG_URI, storage_connection_name=_CONNECTION_NAME)
    target = resolve_for_namespace(cat_id)
    assert target.is_cloud is True
    assert target.base == _CATALOG_URI
    assert target.connection_name == _CONNECTION_NAME
    # Decrypted, ready for core's own in-process scan_delta.
    assert target.storage_options["aws_access_key_id"] == "minioadmin"
    assert target.storage_options["aws_secret_access_key"] == "minioadmin"
    assert target.storage_options["endpoint_url"] == "http://localhost:9000"


def test_schema_inherits_root_catalog_storage():
    _ensure_connection()
    cat_id = _make_namespace(storage_uri=_CATALOG_URI, storage_connection_name=_CONNECTION_NAME)
    schema_id = _make_namespace(parent_id=cat_id)
    target = resolve_for_namespace(schema_id)
    assert target.is_cloud is True
    assert target.base == _CATALOG_URI


def test_worker_payload_owner_encrypted():
    _ensure_connection()
    cat_id = _make_namespace(storage_uri=_CATALOG_URI, storage_connection_name=_CONNECTION_NAME)
    payload = resolve_for_namespace(cat_id).to_worker_payload()
    assert payload is not None
    assert payload["base_uri"] == _CATALOG_URI
    secret = payload["connection"]["aws_secret_access_key"]
    # Owner-keyed for the worker (owner_id=1 embedded), never plaintext.
    assert secret.startswith("$ffsec$1$1$")
    assert "minioadmin" not in secret


def test_shim_resolves_by_namespace_ignoring_user():
    _ensure_connection()
    cat_id = _make_namespace(storage_uri=_CATALOG_URI, storage_connection_name=_CONNECTION_NAME)
    # resolve_catalog_storage keeps its user_id arg for back-compat but ignores it.
    target = resolve_catalog_storage(999, namespace_id=cat_id)
    assert target.is_cloud is True
    assert target.base == _CATALOG_URI


def test_resolve_storage_uri_without_connection_raises():
    cat_id = _insert_raw_catalog(storage_uri=_CATALOG_URI, storage_connection_name=None)
    with pytest.raises(ValueError, match="storage_connection_name"):
        resolve_for_namespace(cat_id)


def test_resolve_missing_connection_raises():
    cat_id = _insert_raw_catalog(storage_uri=_CATALOG_URI, storage_connection_name="does-not-exist")
    with pytest.raises(ValueError, match="does-not-exist"):
        resolve_for_namespace(cat_id)
