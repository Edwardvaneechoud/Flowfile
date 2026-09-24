"""Routes for a Delta table at a bare object-storage path (``/cloud_storage/delta/*``) against MinIO."""

import uuid

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from shared.cloud_storage.storage_options import build_storage_options
from shared.delta_utils import write_delta

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

from test_utils.s3.aws_profiles import isolate_aws

_BUCKET = "flowfile-test"
_CONNECTION_NAME = "cloud_delta_routes_minio"


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


@pytest.fixture(scope="module")
def client() -> TestClient:
    try:
        get_minio_client().create_bucket(Bucket=_BUCKET)
    except Exception:
        pass
    with get_db_context() as db:
        try:
            delete_cloud_connection(db, _CONNECTION_NAME, 1)
        except Exception:
            pass
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=_CONNECTION_NAME,
                storage_type="s3",
                auth_method="access_key",
                aws_region="us-east-1",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                aws_allow_unsafe_html=True,
                endpoint_url="http://localhost:9000",
            ),
            1,
        )
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    yield authed
    with get_db_context() as db:
        delete_cloud_connection(db, _CONNECTION_NAME, 1)


def _post(client: TestClient, route: str, path: str, **extra):
    body = {"connection_name": _CONNECTION_NAME, "resource_path": path, **extra}
    response = client.post(f"/cloud_storage/delta{route}", json=body)
    assert response.status_code == 200, response.text
    return response.json()


@requires_minio
def test_info_enable_and_history_on_a_bare_path(client):
    path = f"s3://{_BUCKET}/cloud_delta_routes_{uuid.uuid4().hex[:8]}"

    assert _post(client, "/info", path) == {
        "exists": False,
        "current_version": None,
        "partition_columns": [],
        "columns": [],
        "cdc_enabled": False,
        "cdc_enabled_version": None,
    }

    storage_options = build_storage_options(
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )
    write_delta(pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}), path, storage_options=storage_options)

    info = _post(client, "/info", path)
    assert info["exists"] is True
    assert info["current_version"] == 0
    assert info["columns"] == [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]
    assert info["cdc_enabled"] is False

    enabled = _post(client, "/cdc/enable", path)
    assert (enabled["cdc_enabled"], enabled["cdc_enabled_version"], enabled["current_version"]) == (True, 1, 1)
    assert _post(client, "/info", path) == enabled

    history = _post(client, "/history", path)
    assert [entry["version"] for entry in history] == [1, 0]
    assert history[0]["operation"] == "SET TBLPROPERTIES"


def test_aws_cli_without_local_credentials_is_a_400(monkeypatch, tmp_path):
    """"No connection" resolves to aws-cli; with no local AWS credentials that is a caller error, not a 500."""
    isolate_aws(monkeypatch, tmp_path, endpoint=None)
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]

    response = TestClient(main.app).post(
        "/cloud_storage/delta/info",
        json={"connection_name": None, "auth_mode": "aws-cli", "resource_path": f"s3://{_BUCKET}/no-credentials"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert detail["error_code"] == "DELTA_ERROR"
    assert detail["message"].startswith("No AWS credentials found in the local AWS profile or environment.")
