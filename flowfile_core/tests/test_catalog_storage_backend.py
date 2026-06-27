"""Unit tests for the catalog storage resolver (``catalog/storage_backend.py``).

These exercise the config-driven resolution seam without object storage: the
local default (the byte-for-byte zero-regression guard) and the error paths when
the cloud URI is set without a usable connection.
"""

import pytest

from flowfile_core.catalog.storage_backend import (
    CatalogStorageTarget,
    _is_cloud_uri,
    join_catalog_uri,
    resolve_catalog_storage,
)
from shared.storage_config import storage


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


def test_resolve_unset_returns_local_target(monkeypatch):
    """Unset config ⇒ the local target, identical to today's behavior."""
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)

    target = resolve_catalog_storage(1)
    assert target.is_cloud is False
    assert target.base == str(storage.catalog_tables_directory)
    assert target.storage_options == {}
    assert target.connection_name is None
    assert target.worker_interface is None
    assert target.to_worker_payload() is None


def test_resolve_uri_without_connection_raises(monkeypatch):
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", "s3://flowfile-test/catalog")
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)

    with pytest.raises(ValueError, match="FLOWFILE_CATALOG_STORAGE_CONNECTION"):
        resolve_catalog_storage(1)


def test_resolve_missing_connection_raises(monkeypatch):
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", "s3://flowfile-test/catalog")
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", "does-not-exist")

    with pytest.raises(ValueError, match="does-not-exist"):
        resolve_catalog_storage(1)


def test_local_target_to_worker_payload_is_none():
    target = CatalogStorageTarget(is_cloud=False, base="/tmp/catalog")
    assert target.to_worker_payload() is None
