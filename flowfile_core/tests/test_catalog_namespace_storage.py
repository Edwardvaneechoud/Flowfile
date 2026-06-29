"""Per-catalog (namespace) object storage: create/update validation, the
one-namespace<->one-storage immutability invariant, root inheritance, the creation-time
env default, mixed-backend resolution, and (MinIO-gated) a real S3 round-trip proving a
schema inherits its catalog's object storage.
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.catalog.exceptions import (
    InvalidNamespaceStorageError,
    NamespaceStorageLockedError,
    NotAuthorizedError,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.service import CatalogService
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


def _add_physical_table(namespace_id: int, file_path: str, name: str | None = None) -> int:
    with get_db_context() as db:
        t = db_models.CatalogTable(
            name=name or f"t_{uuid.uuid4().hex[:8]}",
            namespace_id=namespace_id,
            owner_id=1,
            file_path=file_path,
            table_type="physical",
        )
        db.add(t)
        db.commit()
        db.refresh(t)
        return t.id


def _add_virtual_table(namespace_id: int) -> int:
    with get_db_context() as db:
        t = db_models.CatalogTable(
            name=f"v_{uuid.uuid4().hex[:8]}",
            namespace_id=namespace_id,
            owner_id=1,
            file_path=None,
            table_type="virtual",
        )
        db.add(t)
        db.commit()
        db.refresh(t)
        return t.id


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


def test_virtual_table_does_not_lock_storage():
    """Only PHYSICAL tables freeze a catalog's storage; a virtual table must not."""
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/v", storage_connection_name=_CONNECTION_NAME)
    _add_virtual_table(cat_id)
    with get_db_context() as db:
        updated = _svc(db).update_namespace(
            cat_id, storage_uri="s3://flowfile-test/v2", storage_connection_name=_CONNECTION_NAME
        )
        assert updated.storage_uri == "s3://flowfile-test/v2"


def test_clear_only_uri_clears_connection_too():
    """Nulling only storage_uri (connection omitted) clears both, not a rejected uri-less connection."""
    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/pc", storage_connection_name=_CONNECTION_NAME)
    with get_db_context() as db:
        updated = _svc(db).update_namespace(cat_id, storage_uri=None)
        assert updated.storage_uri is None
        assert updated.storage_connection_name is None


# ---- Creation-time env default: best-effort, never a live override ---------- #


def test_env_default_unusable_connection_falls_back_to_local(monkeypatch):
    """A configured env default whose connection the creator can't access must NOT block catalog
    creation — it falls back to local."""
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", "s3://flowfile-test/envbad")
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", "totally-not-a-real-connection")
    with get_db_context() as db:
        ns = _svc(db).create_namespace(f"envbad_{uuid.uuid4().hex[:8]}", owner_id=1)
    assert ns.storage_uri is None
    assert ns.storage_connection_name is None


def test_env_is_not_a_live_resolution_override(monkeypatch):
    """The env vars seed storage only at create time; resolution never consults them afterwards."""
    cat_id = _create_catalog()  # created with env unset (autouse) ⇒ local
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_URI", "s3://flowfile-test/live")
    monkeypatch.setenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", _CONNECTION_NAME)
    assert resolve_for_namespace(cat_id).is_cloud is False


# ---- Reparenting cannot split a catalog across backends -------------------- #


def test_reparent_across_backends_rejected():
    _ensure_connection()
    local_cat = _create_catalog()
    local_schema = _create_schema(local_cat)
    cloud_cat = _create_catalog(storage_uri="s3://flowfile-test/reparent", storage_connection_name=_CONNECTION_NAME)
    table_id = _add_physical_table(local_schema, "/var/flowfile/catalog_tables/t_local")
    with get_db_context() as db, pytest.raises(InvalidNamespaceStorageError):
        CatalogService(SQLAlchemyCatalogRepository(db)).update_table(table_id, namespace_id=cloud_cat)


def test_reparent_within_same_backend_allowed():
    local_cat = _create_catalog()
    schema_a = _create_schema(local_cat)
    schema_b = _create_schema(local_cat)
    table_id = _add_physical_table(schema_a, "/var/flowfile/catalog_tables/t_move")
    with get_db_context() as db:
        out = CatalogService(SQLAlchemyCatalogRepository(db)).update_table(table_id, namespace_id=schema_b)
        assert out.namespace_id == schema_b


# ---- Owner-only storage governance (anti-repoint) -------------------------- #


class _StubAccess:
    """Minimal restricted AccessResolver stand-in: manage is granted, but the caller is not the owner."""

    def __init__(self, user_id: int) -> None:
        self.restricted = True
        self.user_id = user_id

    def require_manage(self, *args, **kwargs) -> None:
        return None

    def require_use(self, *args, **kwargs) -> None:
        return None


def test_manage_grantee_cannot_change_storage():
    _ensure_connection()
    cat_id = _create_catalog()  # owner_id=1
    with get_db_context() as db, pytest.raises(NotAuthorizedError):
        svc = CatalogService(SQLAlchemyCatalogRepository(db), access=_StubAccess(user_id=999))
        svc.update_namespace(cat_id, storage_uri="s3://flowfile-test/x", storage_connection_name=_CONNECTION_NAME)


def test_owner_can_change_storage_when_restricted():
    _ensure_connection()
    cat_id = _create_catalog()  # owner_id=1
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db), access=_StubAccess(user_id=1))
        updated = svc.update_namespace(
            cat_id, storage_uri="s3://flowfile-test/owned", storage_connection_name=_CONNECTION_NAME
        )
        assert updated.storage_uri == "s3://flowfile-test/owned"


# ---- Mixed-backend SQL: per-table namespace is recorded -------------------- #


def test_resolve_catalog_sql_tables_records_per_table_namespace():
    """A SQL query spanning two cloud catalogs must carry each table's own namespace_id so the reader
    resolves per-catalog storage (the mixed-backend memoization keys off this)."""
    from flowfile_core.flowfile.flow_graph import _resolve_catalog_sql_tables

    _ensure_connection()
    cat_id = _create_catalog(storage_uri="s3://flowfile-test/sqlmix", storage_connection_name=_CONNECTION_NAME)
    schema_a = _create_schema(cat_id)
    schema_b = _create_schema(cat_id)
    name_a = f"sqlmix_a_{uuid.uuid4().hex[:8]}"
    name_b = f"sqlmix_b_{uuid.uuid4().hex[:8]}"
    _add_physical_table(schema_a, "s3://flowfile-test/sqlmix/t_a", name=name_a)
    _add_physical_table(schema_b, "s3://flowfile-test/sqlmix/t_b", name=name_b)

    resolved = _resolve_catalog_sql_tables(node_id=-1, user_id=None)
    assert resolved.table_paths.get(name_a) == "s3://flowfile-test/sqlmix/t_a"
    assert resolved.table_namespaces.get(name_a) == schema_a
    assert resolved.table_namespaces.get(name_b) == schema_b


# ---- Route-level omit-vs-clear (model_fields_set) -------------------------- #


@pytest.fixture
def authed_client():
    from fastapi.testclient import TestClient

    from flowfile_core import main

    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
        c.headers = {"Authorization": f"Bearer {token}"}
        yield c


def test_update_route_omit_preserves_storage_explicit_null_clears(authed_client):
    _ensure_connection()
    resp = authed_client.post(
        "/catalog/namespaces",
        json={
            "name": f"rt_{uuid.uuid4().hex[:8]}",
            "storage_uri": "s3://flowfile-test/route",
            "storage_connection_name": _CONNECTION_NAME,
        },
    )
    assert resp.status_code == 201, resp.text
    ns_id = resp.json()["id"]
    assert resp.json()["storage_uri"] == "s3://flowfile-test/route"

    # name/description-only update must NOT wipe storage (field omitted ⇒ no change).
    r2 = authed_client.put(f"/catalog/namespaces/{ns_id}", json={"description": "renamed"})
    assert r2.status_code == 200, r2.text
    assert r2.json()["storage_uri"] == "s3://flowfile-test/route"

    # explicit nulls clear it (empty catalog).
    r3 = authed_client.put(
        f"/catalog/namespaces/{ns_id}", json={"storage_uri": None, "storage_connection_name": None}
    )
    assert r3.status_code == 200, r3.text
    assert r3.json()["storage_uri"] is None
    assert r3.json()["storage_connection_name"] is None


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
