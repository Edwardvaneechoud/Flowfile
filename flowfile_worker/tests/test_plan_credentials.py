"""The worker decrypts the cloud credentials core encrypts into a query plan, or resolves the connection it names.

Importing ``flowfile_worker.utils`` registers the worker's own ``$ffsec$`` decryption, so no core is needed.
"""

import io
import json
import os
import sqlite3
import subprocess
import sys
import uuid

import polars as pl
import pytest

from flowfile_worker import utils
from flowfile_worker.secrets import encrypt_secret
from shared import cloud_credential_provider as cp
from shared.cloud_credential_provider import CredentialRefProvider, EncryptedCredentialProvider

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

_BUCKET = "flowfile-test"
_NON_SECRET = {"aws_region": "us-east-1", "endpoint_url": MINIO_ENDPOINT_URL, "aws_allow_http": "true"}
_CATALOG_DDL = """
CREATE TABLE secrets (id INTEGER PRIMARY KEY, encrypted_value TEXT);
CREATE TABLE cloud_storage_connections (
  id INTEGER PRIMARY KEY, connection_name TEXT, storage_type TEXT, auth_method TEXT, aws_region TEXT,
  aws_access_key_id TEXT, aws_secret_access_key_id INTEGER, aws_session_token_id INTEGER, aws_role_arn TEXT,
  aws_allow_unsafe_html BOOLEAN, aws_profile TEXT, azure_account_name TEXT, azure_account_key_id INTEGER,
  azure_tenant_id TEXT, azure_client_id TEXT, azure_client_secret_id INTEGER, azure_sas_token_id INTEGER,
  gcs_service_account_key_id INTEGER, gcs_project_id TEXT, endpoint_url TEXT, verify_ssl BOOLEAN);
"""


def _minio_available() -> bool:
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


def _provider(credentials: dict, user_id: int = 7) -> EncryptedCredentialProvider:
    return EncryptedCredentialProvider(encrypt_secret(json.dumps(credentials), user_id=user_id))


@pytest.fixture
def worker_decryptor():
    """Install the worker's decryptor for one test.

    The registry is process-global and the last import wins; a test that imported flowfile_core replaced it.
    """
    previous = cp._decrypt
    cp.register_secret_decryptor(utils._decrypt_plan_credentials)
    yield
    cp._decrypt = previous


@pytest.mark.worker
def test_a_fresh_worker_process_registers_its_own_decryptor():
    """What a spawned child does: importing flowfile_worker.utils registers the worker's decryptor."""
    code = (
        "import flowfile_worker.utils as u, shared.cloud_credential_provider as cp, sys\n"
        "assert cp._decrypt is u._decrypt_plan_credentials\n"
        "assert 'flowfile_core' not in sys.modules\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr


@pytest.mark.worker
def test_worker_decryptor_round_trips_worker_encrypted_credentials(worker_decryptor):
    assert _provider({"aws_secret_access_key": "s3cr3t"})() == ({"aws_secret_access_key": "s3cr3t"}, None)


@pytest.mark.worker
@pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")
def test_worker_collects_a_plan_whose_credentials_are_encrypted(monkeypatch, worker_decryptor):
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    path = f"s3://{_BUCKET}/worker-plan-credentials-{uuid.uuid4().hex[:8]}/data.parquet"
    credentials = {"aws_access_key_id": MINIO_ACCESS_KEY, "aws_secret_access_key": MINIO_SECRET_KEY}
    pl.DataFrame({"a": [1, 2, 3]}).write_parquet(path, storage_options={**_NON_SECRET, **credentials})
    try:
        plan = pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=_provider(credentials))
        blob = plan.serialize()
        assert MINIO_SECRET_KEY.encode() not in blob

        assert pl.LazyFrame.deserialize(io.BytesIO(blob)).collect().height == 3
    finally:
        client = get_minio_client()
        client.delete_object(Bucket=_BUCKET, Key=path.removeprefix(f"s3://{_BUCKET}/"))


@pytest.mark.worker
@pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")
def test_a_fresh_worker_process_resolves_a_connection_reference(tmp_path):
    """A spawned child reads the connection from its catalog and decrypts it with the worker's own master key."""
    database = tmp_path / "catalog.db"
    with sqlite3.connect(database) as con:
        con.executescript(_CATALOG_DDL)
        con.execute("INSERT INTO secrets VALUES (1, ?)", (encrypt_secret(MINIO_SECRET_KEY, user_id=7),))
        con.execute(
            "INSERT INTO cloud_storage_connections (id, storage_type, auth_method, aws_region, aws_access_key_id,"
            " aws_secret_access_key_id, aws_allow_unsafe_html, endpoint_url, verify_ssl)"
            " VALUES (3, 's3', 'access_key', 'us-east-1', ?, 1, 1, ?, 1)",
            (MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL),
        )
    path = f"s3://{_BUCKET}/worker-plan-credentials-{uuid.uuid4().hex[:8]}/data.parquet"
    credentials = {"aws_access_key_id": MINIO_ACCESS_KEY, "aws_secret_access_key": MINIO_SECRET_KEY}
    pl.DataFrame({"a": [1, 2, 3]}).write_parquet(path, storage_options={**_NON_SECRET, **credentials})
    plan_file = tmp_path / "plan.bin"
    provider = CredentialRefProvider(3, 7)
    plan_file.write_bytes(pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=provider).serialize())
    assert MINIO_SECRET_KEY.encode() not in plan_file.read_bytes()
    code = (
        "import io, flowfile_worker.funcs, polars as pl\n"
        f"print(pl.LazyFrame.deserialize(io.BytesIO(open({str(plan_file)!r}, 'rb').read())).collect().height)\n"
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    env.update(AWS_EC2_METADATA_DISABLED="true", FLOWFILE_DATABASE_URL=f"sqlite:///{database}", TEST_MODE="1")
    try:
        ran = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip().splitlines()[-1] == "3"
    finally:
        get_minio_client().delete_object(Bucket=_BUCKET, Key=path.removeprefix(f"s3://{_BUCKET}/"))
