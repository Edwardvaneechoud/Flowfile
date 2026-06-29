"""Per-catalog (namespace) object storage: create/update validation, the
one-namespace<->one-storage immutability invariant, root inheritance, the creation-time
env default, mixed-backend resolution, and (MinIO-gated) a real S3 round-trip proving a
schema inherits its catalog's object storage.
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.catalog.exceptions import InvalidNamespaceStorageError, NamespaceStorageLockedError
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.storage_backend import join_catalog_uri, resolve_for_namespace
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

_CONNECTION_NAME = "catalog-ns-minio"
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
def _no_env_catalog_storage(monkeypatch):
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


def _svc(db) -> NamespaceService:
    return NamespaceService(SQLAlchemyCatalogRepository(db))


def _create_catalog(*, storage_uri=None, storage_connection_name=None) -> int:
    with get_db_context() as db:
        return _svc(db).create_namespace(
            f"nscat_{uuid.uuid4().hex[:8]}",
            owner_id=1,
            storage_uri=storage_uri,
            storage_connection_name=storage_connection_name,
        ).id


def _create_schema(parent_id: int) -> int:
    with get_db_context() as db:
        return _svc(db).create_namespace(f"nssch_{uuid.uuid4().hex[:8]}", owner_id=1, parent_id=parent_id).id


def _add_physical_table(namespace_id: int, file_path: str) -> None:
    with get_db_context() as db:
        db.add(
            db_models.CatalogTable(
                name=f"t_{uuid.uuid4().hex[:8]}",
                namespace_id=namespace_id,
                owner_id=1,
                file_path=file_path,
                table_type="physical",
            )
        )
        db.commit()


# ---- Create-time validation ------------------------------------------------ #


def test_storage_rejected_on_schema_level():
    _ensure_connection()
    cat_id = _create_catalog()
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).create_namespace(
            f"child_{uuid.uuid4().hex[:6]}",
            owner_id=1,
            parent_id=cat_id,
            storage_uri="s3://flowfile-test/x",
            storage_connection_name=_CONNECTION_NAME,
        )


def test_non_cloud_storage_uri_rejected():
    _ensure_connection()
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).create_namespace(
            f"bad_{uuid.uuid4().hex[:6]}",
            owner_id=1,
            storage_uri="/local/path",
            storage_connection_name=_CONNECTION_NAME,
        )


def test_storage_uri_without_connection_rejected():
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).create_namespace(f"bad_{uuid.uuid4().hex[:6]}", owner_id=1, storage_uri="s3://flowfile-test/x")


def test_connection_without_uri_rejected():
    _ensure_connection()
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).create_namespace(
            f"bad_{uuid.uuid4().hex[:6]}", owner_id=1, storage_connection_name=_CONNECTION_NAME
        )


def test_unknown_connection_rejected():
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).create_namespace(
            f"bad_{uuid.uuid4().hex[:6]}",
            owner_id=1,
            storage_uri="s3://flowfile-test/x",
            storage_connection_name="nope-not-real",
        )


# ---- Immutability invariant ------------------------------------------------ #


def test_repoint_empty_catalog_allowed():
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/a", storage_connection_name=_CONNECTION_NAME)
    with get_db_context() as db:
        updated = _svc(db).update_namespace(
            cat_id, storage_uri="s3://flowfile-test/b", storage_connection_name=_CONNECTION_NAME
        )
        assert updated.storage_uri == "s3://flowfile-test/b"


def test_repoint_populated_catalog_locked():
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/a", storage_connection_name=_CONNECTION_NAME)
    schema_id = _create_schema(cat_id)
    _add_physical_table(schema_id, "s3://flowfile-test/a/t1")
    with get_db_context() as db, pytest.raises(NamespaceStorageLockedError):
        _svc(db).update_namespace(
            cat_id, storage_uri="s3://flowfile-test/b", storage_connection_name=_CONNECTION_NAME
        )


def test_noop_storage_update_on_populated_catalog_allowed():
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/a", storage_connection_name=_CONNECTION_NAME)
    _add_physical_table(cat_id, "s3://flowfile-test/a/t1")
    with get_db_context() as db:
        # Same values ⇒ not a change ⇒ no lock.
        updated = _svc(db).update_namespace(
            cat_id, storage_uri="s3://flowfile-test/a", storage_connection_name=_CONNECTION_NAME
        )
        assert updated.storage_uri == "s3://flowfile-test/a"


def test_clear_storage_on_empty_catalog_allowed():
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/a", storage_connection_name=_CONNECTION_NAME)
    with get_db_context() as db:
        updated = _svc(db).update_namespace(cat_id, storage_uri=None, storage_connection_name=None)
        assert updated.storage_uri is None
        assert updated.storage_connection_name is None


def test_storage_update_rejected_on_schema():
    cat_id = _create_catalog()
    schema_id = _create_schema(cat_id)
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        _svc(db).update_namespace(
            schema_id, storage_uri="s3://flowfile-test/x", storage_connection_name=_CONNECTION_NAME
        )


# ---- Effective storage walk + mixed backends ------------------------------- #


def test_effective_storage_inherited_by_schema():
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/eff", storage_connection_name=_CONNECTION_NAME)
    schema_id = _create_schema(cat_id)
    with get_db_context() as db:
        eff = _svc(db).get_effective_storage(schema_id)
    assert eff is not None
    uri, conn, owner = eff
    assert uri == "s3://flowfile-test/eff"
    assert conn == _CONNECTION_NAME
    assert owner == 1


def test_effective_storage_none_when_unset():
    cat_id = _create_catalog()
    with get_db_context() as db:
        assert _svc(db).get_effective_storage(cat_id) is None
        assert _svc(db).get_effective_storage(None) is None


def test_mixed_backend_distinct_targets():
    _ensure_connection()
    cloud_cat = _create_catalog(storage_uri="s3://flowfile-test/mixed", storage_connection_name=_CONNECTION_NAME)
    local_cat = _create_catalog()
    cloud_target = resolve_for_namespace(cloud_cat)
    local_target = resolve_for_namespace(local_cat)
    assert cloud_target.is_cloud is True
    assert local_target.is_cloud is False
    assert cloud_target.base != local_target.base


def test_env_default_snapshotted_at_create(monkeypatch):
    _ensure_connection()
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", "s3://flowfile-test/envdefault")
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", _CONNECTION_NAME)
    with get_db_context() as db:
        ns = _svc(db).create_namespace(f"envcat_{uuid.uuid4().hex[:8]}", owner_id=1)
    assert ns.storage_uri == "s3://flowfile-test/envdefault"
    assert ns.storage_connection_name == _CONNECTION_NAME


# ---- Real S3 round-trip (against the already-running MinIO mock) ------------ #


@requires_minio
def test_schema_inherits_catalog_object_storage_roundtrip():
    """A schema inherits its catalog's object storage; a real Delta write lands in S3."""
    _ensure_connection()
    try:
        get_minio_client().create_bucket(Bucket=_BUCKET)
    except Exception:
        pass  # already exists
    prefix = f"s3://{_BUCKET}/nsroundtrip_{uuid.uuid4().hex[:8]}"
    cat_id = _create_catalog(storage_uri=prefix, storage_connection_name=_CONNECTION_NAME)
    schema_id = _create_schema(cat_id)

    target = resolve_for_namespace(schema_id)
    assert target.is_cloud is True
    assert target.base == prefix

    import polars as pl
    from shared.delta_utils import write_delta

    dest = join_catalog_uri(target.base, f"t_{uuid.uuid4().hex[:6]}")
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert write_delta(df, dest, mode="overwrite", storage_options=target.storage_options) is True
    assert pl.scan_delta(dest, storage_options=target.storage_options).collect().height == 3

    key_prefix = dest.replace(f"s3://{_BUCKET}/", "") + "/_delta_log/"
    listed = get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix=key_prefix)
    assert listed.get("KeyCount", 0) > 0
