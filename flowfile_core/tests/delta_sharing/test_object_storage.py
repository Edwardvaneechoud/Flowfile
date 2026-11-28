"""Tables in an object-storage catalog: presigned S3 URLs from the catalog's connection, read like any client would."""

from __future__ import annotations

import json
import uuid

import httpx
import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

MINIO = "http://localhost:9000"
BUCKET_URI = "s3://flowfile-test/delta-sharing"
MINIO_OPTIONS = {
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "aws_region": "us-east-1",
    "endpoint_url": MINIO,
    "aws_allow_http": "true",
}


def _minio_up() -> bool:
    try:
        return httpx.get(f"{MINIO}/minio/health/live", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


@pytest.fixture(autouse=True)
def _no_env_catalog_storage(monkeypatch):
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)


def _connection(name: str, auth_method: str) -> FullCloudStorageConnection:
    keys = (
        {"aws_access_key_id": "minioadmin", "aws_secret_access_key": SecretStr("minioadmin")}
        if auth_method == "access_key"
        else {}
    )
    return FullCloudStorageConnection(
        connection_name=name,
        storage_type="s3",
        auth_method=auth_method,
        aws_region="us-east-1",
        endpoint_url=MINIO,
        aws_allow_unsafe_html=True,
        **keys,
    )


@pytest.fixture
def cloud_schema(catalog):
    """``cloud_schema(auth_method)``: a schema of a catalog on S3 behind a connection signing in that way."""
    made: list[tuple[int, str]] = []

    def _make(auth_method: str) -> int:
        name = f"ds-conn-{uuid.uuid4().hex[:6]}"
        with get_db_context() as db:
            store_cloud_connection(db, _connection(name, auth_method), user_id=catalog.owner_id)
            db.commit()
            root = db_models.CatalogNamespace(
                name=f"DSCloud_{uuid.uuid4().hex[:6]}",
                level=0,
                owner_id=catalog.owner_id,
                storage_uri=BUCKET_URI,
                storage_connection_name=name,
            )
            db.add(root)
            db.commit()
            schema = db_models.CatalogNamespace(name="lake", level=1, parent_id=root.id, owner_id=catalog.owner_id)
            db.add(schema)
            db.commit()
            made.append((root.id, name))
            made.append((schema.id, name))
            return schema.id

    yield _make
    with get_db_context() as db:
        for ns_id, name in reversed(made):
            row = db.get(db_models.CatalogNamespace, ns_id)
            if row is not None:
                db.delete(row)
            db.query(db_models.CloudStorageConnection).filter_by(connection_name=name).delete()
        db.commit()


def _register(catalog, schema_id: int, name: str, path: str) -> int:
    with get_db_context() as db:
        row = db_models.CatalogTable(
            name=name,
            namespace_id=schema_id,
            owner_id=catalog.owner_id,
            file_path=path,
            storage_format="delta",
            table_type="physical",
        )
        db.add(row)
        db.commit()
        catalog.table_ids.append(row.id)
        return row.id


def test_a_connection_without_keys_of_its_own_cannot_share(designer, catalog, cloud_schema, cleanup_sharing):
    schema_id = cloud_schema("env_vars")
    table_id = _register(catalog, schema_id, "keyless", f"{BUCKET_URI}/never_written")
    share_id = designer.post("/delta-sharing/manage/shares", json={"name": "keyless"}).json()["id"]
    refused = designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": table_id})
    assert refused.status_code == 422
    assert "no access key of its own" in refused.json()["detail"]
    eligible = {t["table_id"]: t for t in designer.get("/delta-sharing/manage/eligible-tables").json()}
    assert "no access key of its own" in eligible[table_id]["refusal"]


@pytest.mark.skipif(not _minio_up(), reason="MinIO is not running on localhost:9000 (poetry run start_minio)")
def test_an_s3_table_is_read_through_presigned_urls(anonymous, catalog, cloud_schema, share_tables):
    schema_id = cloud_schema("access_key")
    path = f"{BUCKET_URI}/orders_{uuid.uuid4().hex[:8]}"
    frame = pl.DataFrame({"id": [1, 2, 3], "region": ["eu", "us", "eu"]})
    frame.write_delta(path, storage_options=MINIO_OPTIONS, delta_write_options={"partition_by": ["region"]})
    table_id = _register(catalog, schema_id, "orders", path)
    shared = share_tables(table_id)

    response = anonymous.post(
        "/delta-sharing/shares/retail/schemas/lake/tables/orders/query", json={}, headers=shared.headers
    )
    assert response.status_code == 200, response.text
    files = [json.loads(line)["file"] for line in response.text.splitlines() if '"file"' in line]
    assert len(files) == 2
    assert all(f["url"].startswith(f"{MINIO}/flowfile-test/delta-sharing/") for f in files)
    assert all("X-Amz-Signature=" in f["url"] for f in files)
    parts = []
    for f in files:
        body = httpx.get(f["url"], timeout=10)
        assert body.status_code == 200
        part = pl.read_parquet(body.content)
        parts.append(part.with_columns(pl.lit(f["partitionValues"]["region"]).alias("region")))
    got = pl.concat(parts).select("id", "region").sort("id")
    assert got.equals(frame)

    unsigned = files[0]["url"].split("?", 1)[0]
    assert httpx.get(unsigned, timeout=10).status_code in (401, 403)
