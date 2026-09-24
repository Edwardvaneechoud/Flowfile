"""Saved cloud connections' AWS profile and session token, end to end through the routes against MinIO.

Every test runs with hermetic AWS config files and ``AWS_ENDPOINT_URL`` pointing at a dead local port, so only
an endpoint stored on the connection can reach MinIO and nothing reaches real AWS.
"""

import os
import uuid

import boto3
import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    get_cloud_connection_schema,
    store_cloud_connection,
    update_cloud_connection,
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

try:
    from test_utils.s3.fixtures import (
        MINIO_ACCESS_KEY,
        MINIO_ENDPOINT_URL,
        MINIO_SECRET_KEY,
        get_minio_client,
        is_docker_available,
    )
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import (
        MINIO_ACCESS_KEY,
        MINIO_ENDPOINT_URL,
        MINIO_SECRET_KEY,
        get_minio_client,
        is_docker_available,
    )

_BUCKET = "flowfile-test"
_CONNECTION_NAME = f"credentials lane {uuid.uuid4().hex[:6]}"


def _minio_available() -> bool:
    if not is_docker_available():
        return False
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


@pytest.fixture
def hermetic_aws(monkeypatch, tmp_path):
    """A default profile with keys MinIO rejects and a ``minio`` profile with the real ones."""
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    (tmp_path / "credentials").write_text(
        "[default]\naws_access_key_id = AKIDNOTMINIO\naws_secret_access_key = wrong-secret\n"
        f"[minio]\naws_access_key_id = {MINIO_ACCESS_KEY}\naws_secret_access_key = {MINIO_SECRET_KEY}\n"
    )
    (tmp_path / "config").write_text("[default]\nregion = us-east-1\n[profile minio]\nregion = us-east-1\n")
    (tmp_path / "boto.cfg").write_text("")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("BOTO_CONFIG", str(tmp_path / "boto.cfg"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")


@pytest.fixture
def client(hermetic_aws):
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    yield authed
    with get_db_context() as db:
        delete_cloud_connection(db, _CONNECTION_NAME, 1)


@pytest.fixture
def delta_table_path():
    """A one-version Delta table under a unique MinIO prefix, deleted afterwards."""
    client = get_minio_client()
    try:
        client.create_bucket(Bucket=_BUCKET)
    except Exception:
        pass
    prefix = f"credentials_lane_{uuid.uuid4().hex[:8]}"
    options = {
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "aws_region": "us-east-1",
        "endpoint_url": MINIO_ENDPOINT_URL,
        "aws_allow_http": "true",
    }
    path = f"s3://{_BUCKET}/{prefix}/table"
    pl.DataFrame({"id": [1, 2]}).write_delta(path, storage_options=options)
    yield path
    listed = client.list_objects_v2(Bucket=_BUCKET, Prefix=f"{prefix}/")
    keys = [{"Key": obj["Key"]} for obj in listed.get("Contents", [])]
    if keys:
        client.delete_objects(Bucket=_BUCKET, Delete={"Objects": keys})


def _save(client: TestClient, method: str, **fields):
    body = {
        "connection_name": _CONNECTION_NAME,
        "storage_type": "s3",
        "aws_region": "us-east-1",
        "endpoint_url": MINIO_ENDPOINT_URL,
        "aws_allow_unsafe_html": True,
        **fields,
    }
    response = client.request(method, "/cloud_connections/cloud_connection", json=body)
    assert response.status_code == 200, response.text


def _listed(client: TestClient) -> dict:
    response = client.get("/cloud_connections/cloud_connections")
    assert response.status_code == 200, response.text
    return next(c for c in response.json() if c["connection_name"] == _CONNECTION_NAME)


def _delta_info(client: TestClient, path: str):
    return client.post("/cloud_storage/delta/info", json={"connection_name": _CONNECTION_NAME, "resource_path": path})


@requires_minio
def test_aws_cli_connection_uses_its_profile_and_endpoint(client, delta_table_path):
    """The connection name is not a profile; the explicit one is, and the connection's endpoint is honoured."""
    _save(client, "POST", auth_method="aws-cli", aws_profile="minio")
    assert _listed(client)["aws_profile"] == "minio"

    info = _delta_info(client, delta_table_path)
    assert info.status_code == 200, info.text
    assert (info.json()["exists"], info.json()["current_version"]) == (True, 0)

    _save(client, "PUT", auth_method="aws-cli", aws_profile="missing")
    info = _delta_info(client, delta_table_path)
    assert info.status_code == 400
    assert "AWS profile 'missing' was not found" in info.text


@requires_minio
def test_access_key_connection_forwards_its_session_token(client, delta_table_path, monkeypatch):
    """Temporary STS keys only work together with their token, so a 200 proves the token was stored and sent."""
    sts = boto3.client(
        "sts",
        endpoint_url=MINIO_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    credentials = sts.assume_role(
        RoleArn="arn:aws:iam::123456789012:role/flowfile-test", RoleSessionName="credentials-lane"
    )["Credentials"]
    keys = {
        "auth_method": "access_key",
        "aws_access_key_id": credentials["AccessKeyId"],
        "aws_secret_access_key": credentials["SecretAccessKey"],
    }

    _save(client, "POST", **keys)
    assert _delta_info(client, delta_table_path).status_code != 200

    _save(client, "PUT", **keys, aws_session_token=credentials["SessionToken"])
    info = _delta_info(client, delta_table_path)
    assert info.status_code == 200, info.text
    assert info.json()["exists"] is True
    assert "aws_session_token" not in _listed(client)


@requires_minio
def test_rotating_from_temporary_to_permanent_keys_keeps_the_connection_working(client, delta_table_path):
    """Leaving the token blank while entering permanent keys must not send the expired token along with them."""
    sts = boto3.client(
        "sts",
        endpoint_url=MINIO_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    credentials = sts.assume_role(
        RoleArn="arn:aws:iam::123456789012:role/flowfile-test", RoleSessionName="credentials-rotation"
    )["Credentials"]
    _save(
        client,
        "POST",
        auth_method="access_key",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    assert _delta_info(client, delta_table_path).status_code == 200

    _save(
        client,
        "PUT",
        auth_method="access_key",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        aws_session_token="",
    )
    info = _delta_info(client, delta_table_path)
    assert info.status_code == 200, info.text


def _stored_token() -> str | None:
    with get_db_context() as db:
        token = get_cloud_connection_schema(db, _CONNECTION_NAME, 1).aws_session_token
        orphans = db.query(Secret).filter(Secret.name == f"{_CONNECTION_NAME}_aws_session_token").count()
    assert orphans == (1 if token else 0), "a dropped token must not leave its secret behind"
    return token.get_secret_value() if token else None


def test_session_token_lives_and_dies_with_its_key_pair(hermetic_aws):
    """A token is only valid with the keys it was issued with, so it never outlives them."""

    def _connection(**fields) -> FullCloudStorageConnection:
        return FullCloudStorageConnection(connection_name=_CONNECTION_NAME, storage_type="s3", **fields)

    try:
        with get_db_context() as db:
            store_cloud_connection(
                db,
                _connection(
                    auth_method="access_key",
                    aws_access_key_id="ASIATEMP1",
                    aws_secret_access_key="temp-secret",
                    aws_session_token="token-1",
                ),
                1,
            )
        assert _stored_token() == "token-1"

        with get_db_context() as db:
            update_cloud_connection(db, _connection(auth_method="access_key", aws_access_key_id="ASIATEMP1"), 1)
        assert _stored_token() == "token-1", "re-saving without touching the keys keeps the token"

        with get_db_context() as db:
            update_cloud_connection(
                db,
                _connection(auth_method="access_key", aws_access_key_id="AKIAPERM", aws_secret_access_key="perm"),
                1,
            )
        assert _stored_token() is None, "rotating to permanent keys drops the stale token"

        with get_db_context() as db:
            update_cloud_connection(
                db,
                _connection(
                    auth_method="access_key",
                    aws_access_key_id="ASIATEMP2",
                    aws_secret_access_key="temp-secret-2",
                    aws_session_token="token-2",
                ),
                1,
            )
        assert _stored_token() == "token-2"

        with get_db_context() as db:
            update_cloud_connection(db, _connection(auth_method="access_key", aws_access_key_id="ASIATEMP2"), 1)
            update_cloud_connection(db, _connection(auth_method="access_key", aws_secret_access_key="new"), 1)
        assert _stored_token() is None, "a new secret (or a changed key ID) without a token drops it"

        with get_db_context() as db:
            update_cloud_connection(
                db,
                _connection(auth_method="access_key", aws_access_key_id="ASIATEMP3", aws_session_token="token-3"),
                1,
            )
            update_cloud_connection(db, _connection(auth_method="aws-cli", aws_session_token="ignored"), 1)
        assert _stored_token() is None, "a token is never kept, or stored, for another auth method"
    finally:
        with get_db_context() as db:
            delete_cloud_connection(db, _CONNECTION_NAME, 1)


def test_updating_without_a_profile_keeps_the_stored_one(hermetic_aws):
    """An API client that omits aws_profile must not switch an aws-cli connection to the default chain."""
    try:
        with get_db_context() as db:
            store_cloud_connection(
                db,
                FullCloudStorageConnection(
                    connection_name=_CONNECTION_NAME, storage_type="s3", auth_method="aws-cli", aws_profile="analytics"
                ),
                1,
            )
            update_cloud_connection(
                db,
                FullCloudStorageConnection(
                    connection_name=_CONNECTION_NAME, storage_type="s3", auth_method="aws-cli", aws_region="eu-west-1"
                ),
                1,
            )
            assert get_cloud_connection_schema(db, _CONNECTION_NAME, 1).aws_profile == "analytics"

            update_cloud_connection(
                db,
                FullCloudStorageConnection(
                    connection_name=_CONNECTION_NAME, storage_type="s3", auth_method="aws-cli", aws_profile=""
                ),
                1,
            )
            assert get_cloud_connection_schema(db, _CONNECTION_NAME, 1).aws_profile is None
    finally:
        with get_db_context() as db:
            delete_cloud_connection(db, _CONNECTION_NAME, 1)
