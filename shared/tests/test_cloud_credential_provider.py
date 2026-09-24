"""EncryptedCredentialProvider keeps cloud credentials out of serialized Polars plans.

Polars inlines ``storage_options`` into a serialized LazyFrame, so a scan built with decrypted
credentials carries them wherever the plan goes. These tests pin that a provider-based scan keeps
only ciphertext in the plan and still reads the data with the same pushdown. Encryption is the
real ``$ffsec$`` scheme under the test master key.
"""

import json
import os
import pickle
import sys
import uuid

import polars as pl
import pytest
from cryptography.fernet import Fernet

from shared import cloud_credential_provider as cp
from shared.cloud_credential_provider import (
    CredentialDecryptionError,
    EncryptedCredentialProvider,
    split_credentials,
)
from shared.delta_utils import scan_delta_changes
from shared.notifications import crypto

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

_BUCKET = "flowfile-test"
SENTINEL = "SENTINEL-SECRET-7f3a91"
_NON_SECRET = {"aws_region": "us-east-1", "endpoint_url": MINIO_ENDPOINT_URL, "aws_allow_http": "true"}
_MINIO_CREDENTIALS = {
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_session_token": "",
}


def _minio_available() -> bool:
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


@pytest.fixture
def ffsec(monkeypatch):
    """Encrypt with the real ``$ffsec$`` scheme and register it as this process's decryptor."""
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setattr(cp, "_decrypt", crypto.decrypt_secret)

    def _provider(credentials: dict) -> EncryptedCredentialProvider:
        return EncryptedCredentialProvider(crypto.encrypt_secret(json.dumps(credentials), user_id=1))

    return _provider


@pytest.fixture
def aws_free_env(monkeypatch):
    """No ambient AWS credentials or profile, so only the provider can authenticate."""
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    monkeypatch.setenv("AWS_CONFIG_FILE", os.devnull)


@pytest.fixture
def minio_prefix():
    prefix = f"credprov-{uuid.uuid4().hex[:8]}"
    yield prefix
    client = get_minio_client()
    for obj in client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix).get("Contents", []):
        client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


class TestSplitCredentials:
    def test_s3_credentials_move_to_the_provider(self):
        full = {**_NON_SECRET, "aws_access_key_id": "AKID", "aws_secret_access_key": SENTINEL, "aws_session_token": ""}
        options, credentials = split_credentials(full)
        assert options == _NON_SECRET
        assert credentials == {"aws_access_key_id": "AKID", "aws_secret_access_key": SENTINEL, "aws_session_token": ""}

    def test_azure_key_and_sas_move_but_account_name_stays(self):
        options, credentials = split_credentials({"account_name": "acct", "account_key": "k", "sas_token": "s"})
        assert options == {"account_name": "acct"}
        assert credentials == {"account_key": "k", "sas_token": "s"}

    def test_secrets_a_polars_provider_cannot_carry_stay_in_the_options(self):
        # Polars providers accept only the S3 keys and Azure account_key / sas_token / bearer_token.
        options, credentials = split_credentials({"client_id": "c", "client_secret": "x", "token": "{json}"})
        assert credentials == {}
        assert options == {"client_id": "c", "client_secret": "x", "token": "{json}"}


class TestEncryptedCredentialProvider:
    def test_pickles_as_ciphertext_only(self, ffsec):
        provider = ffsec({"aws_access_key_id": "AKID", "aws_secret_access_key": SENTINEL})

        blob = pickle.dumps(provider)

        assert SENTINEL.encode() not in blob
        assert provider.encrypted_credentials.startswith("$ffsec$1$1$")
        assert pickle.loads(blob)() == ({"aws_access_key_id": "AKID", "aws_secret_access_key": SENTINEL}, None)
        assert SENTINEL not in repr(provider)

    def test_no_registered_decryptor_is_a_clear_error(self, ffsec, monkeypatch):
        provider = ffsec({"aws_secret_access_key": SENTINEL})
        monkeypatch.setattr(cp, "_decrypt", None)
        with pytest.raises(CredentialDecryptionError, match="no secret decryptor is registered"):
            provider()

    def test_a_different_master_key_is_a_clear_error(self, ffsec, monkeypatch):
        provider = ffsec({"aws_secret_access_key": SENTINEL})
        # The executing side resolves another master key: docker mode with a different FLOWFILE_MASTER_KEY.
        monkeypatch.delenv("TEST_MODE")
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())

        with pytest.raises(CredentialDecryptionError, match="must share the same master key") as info:
            provider()
        assert "InvalidToken" in str(info.value)
        assert info.value.__cause__ is None
        assert SENTINEL not in str(info.value)


@requires_minio
class TestScansAgainstMinio:
    def _put_parquet(self, prefix: str) -> str:
        frame = pl.DataFrame({"a": list(range(1000)), "c": [i % 7 for i in range(1000)]})
        path = f"s3://{_BUCKET}/{prefix}/data.parquet"
        frame.write_parquet(path, storage_options={**_NON_SECRET, **_MINIO_CREDENTIALS}, row_group_size=100)
        return path

    def test_parquet_plan_has_no_credentials_and_keeps_its_pushdown(self, ffsec, aws_free_env, minio_prefix):
        path = self._put_parquet(minio_prefix)

        def query(**storage):
            return pl.scan_parquet(path, **storage).filter(pl.col("c") == 3).select("a")

        plain = query(storage_options={**_NON_SECRET, **_MINIO_CREDENTIALS})
        protected = query(storage_options=_NON_SECRET, credential_provider=ffsec(_MINIO_CREDENTIALS))

        assert MINIO_SECRET_KEY.encode() in plain.serialize()
        assert MINIO_SECRET_KEY.encode() not in protected.serialize()
        assert protected.explain() == plain.explain()
        assert protected.collect().equals(plain.collect())

    def test_an_ambient_session_token_does_not_mix_in(self, ffsec, aws_free_env, minio_prefix, monkeypatch):
        path = self._put_parquet(minio_prefix)
        monkeypatch.setenv("AWS_SESSION_TOKEN", "bogus")
        credentials = {k: v for k, v in _MINIO_CREDENTIALS.items() if k != "aws_session_token"}
        lf = pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=ffsec(credentials))
        assert lf.select(pl.len()).collect().item() == 1000

    def test_change_feed_plan_has_no_credentials(self, ffsec, aws_free_env, minio_prefix):
        from shared.delta_utils import get_delta_head_version, write_delta

        full = {**_NON_SECRET, **_MINIO_CREDENTIALS}
        path = f"s3://{_BUCKET}/{minio_prefix}/cdf"
        write_delta(pl.DataFrame({"k": [1, 2]}), path, mode="overwrite", storage_options=full, enable_cdf=True)
        write_delta(pl.DataFrame({"k": [3]}), path, mode="append", storage_options=full)
        head = get_delta_head_version(path, storage_options=full)

        plain = scan_delta_changes(path, 0, head, storage_options=full)
        protected = scan_delta_changes(
            path, 0, head, storage_options=_NON_SECRET, credential_provider=ffsec(_MINIO_CREDENTIALS)
        )

        assert MINIO_SECRET_KEY.encode() in plain.serialize()
        assert MINIO_SECRET_KEY.encode() not in protected.serialize()
        assert sorted(protected.collect()["k"].to_list()) == [1, 2, 3]
