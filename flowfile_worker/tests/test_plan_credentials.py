"""The worker decrypts the cloud credentials core encrypts into a query plan.

Importing ``flowfile_worker.utils`` registers the worker's own ``$ffsec$`` decryption, so no core is needed.
"""

import io
import json
import os
import subprocess
import sys
import uuid

import polars as pl
import pytest

from flowfile_worker import utils
from flowfile_worker.secrets import encrypt_secret
from shared import cloud_credential_provider as cp
from shared.cloud_credential_provider import EncryptedCredentialProvider

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

_BUCKET = "flowfile-test"
_NON_SECRET = {"aws_region": "us-east-1", "endpoint_url": MINIO_ENDPOINT_URL, "aws_allow_http": "true"}


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

        assert utils.collect_lazy_frame(pl.LazyFrame.deserialize(io.BytesIO(blob))).height == 3
    finally:
        client = get_minio_client()
        client.delete_object(Bucket=_BUCKET, Key=path.removeprefix(f"s3://{_BUCKET}/"))
