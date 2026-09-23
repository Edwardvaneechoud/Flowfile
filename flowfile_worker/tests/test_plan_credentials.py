"""The worker decrypts the cloud credentials core encrypts into a query plan.

Core ships cloud reads as plans whose credentials ride in an ``EncryptedCredentialProvider``.
``flowfile_worker.utils`` (imported by every child that collects a plan) registers the worker's own
``$ffsec$`` decryption, so the provider resolves here without core.
"""

import io
import json
import os
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


@pytest.mark.worker
def test_worker_registers_its_own_decryptor():
    assert cp._decrypt is utils._decrypt_plan_credentials
    assert _provider({"aws_secret_access_key": "s3cr3t"})() == ({"aws_secret_access_key": "s3cr3t"}, None)


@pytest.mark.worker
@pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")
def test_worker_collects_a_plan_whose_credentials_are_encrypted(monkeypatch):
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
