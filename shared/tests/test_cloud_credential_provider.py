"""The credential providers keep cloud credentials out of serialized Polars plans.

A provider-based scan keeps only ciphertext, or only a stored connection's id, in the plan, reads with the same
pushdown, and decrypts or resolves where it executes.
"""

import json
import os
import pickle
import sqlite3
import subprocess
import sys
import textwrap
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from cryptography.fernet import Fernet

from shared import cloud_credential_provider as cp
from shared.cloud_credential_provider import (
    CredentialDecryptionError,
    CredentialRefProvider,
    CredentialResolutionError,
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

from test_utils.s3.aws_profiles import isolate_aws

REPO_ROOT = Path(__file__).resolve().parents[2]
_BUCKET = "flowfile-test"
SENTINEL = "SENTINEL-SECRET-7f3a91"
_NON_SECRET = {"aws_region": "us-east-1", "endpoint_url": MINIO_ENDPOINT_URL, "aws_allow_http": "true"}
_MINIO_CREDENTIALS = {
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_session_token": "",
}
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
def catalog(tmp_path, monkeypatch, ffsec):
    """A catalog database holding only the two tables a reference resolves from; returns a store function.

    The store function encrypts each secret with the real ``$ffsec$`` scheme and answers the connection's id.
    """
    path = tmp_path / "catalog.db"
    with sqlite3.connect(path) as con:
        con.executescript(_CATALOG_DDL)
    monkeypatch.setenv("FLOWFILE_DATABASE_URL", f"sqlite:///{path}")

    def _store(secrets: dict[str, str] | None = None, **fields) -> int:
        with sqlite3.connect(path) as con:
            columns = {"verify_ssl": True, **fields}
            for name, value in (secrets or {}).items():
                cur = con.execute(
                    "INSERT INTO secrets (encrypted_value) VALUES (?)", (crypto.encrypt_secret(value, 1),)
                )
                columns[f"{name}_id"] = cur.lastrowid
            placeholders = ", ".join("?" * len(columns))
            cur = con.execute(
                f"INSERT INTO cloud_storage_connections ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(columns.values()),
            )
            return cur.lastrowid

    return _store


def _minio_connection(catalog) -> int:
    return catalog(
        {"aws_secret_access_key": MINIO_SECRET_KEY},
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_region="us-east-1",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_allow_unsafe_html=True,
    )


@pytest.fixture
def aws_free_env(monkeypatch, tmp_path):
    """No ambient AWS credentials or profile, so only the provider can authenticate."""
    isolate_aws(monkeypatch, tmp_path, endpoint=None)


@pytest.fixture
def minio_prefix():
    prefix = f"credprov-{uuid.uuid4().hex[:8]}"
    yield prefix
    client = get_minio_client()
    for obj in client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix).get("Contents", []):
        client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


def _run_in_fresh_process(plan: bytes, tmp_path: Path, register_decryptor: bool) -> subprocess.CompletedProcess:
    """Deserialize and collect *plan* in a new interpreter, as the worker does with a core-built plan."""
    plan_file = tmp_path / "plan.bin"
    plan_file.write_bytes(plan)
    register = (
        "from shared.cloud_credential_provider import register_secret_decryptor\n"
        "from shared.notifications.crypto import decrypt_secret\n"
        "register_secret_decryptor(decrypt_secret)\n"
        if register_decryptor
        else ""
    )
    code = register + textwrap.dedent(
        f"""
        import io
        import polars as pl
        print(pl.LazyFrame.deserialize(io.BytesIO(open({str(plan_file)!r}, "rb").read())).collect().height)
        """
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    env.update(TEST_MODE="1", AWS_EC2_METADATA_DISABLED="true", PYTHONPATH=str(REPO_ROOT))
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=tmp_path)


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

    def test_plan_decrypts_in_the_executing_process(self, ffsec, aws_free_env, minio_prefix, tmp_path):
        path = self._put_parquet(minio_prefix)
        plan = pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=ffsec(_MINIO_CREDENTIALS))

        ran = _run_in_fresh_process(plan.serialize(), tmp_path, register_decryptor=True)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip() == "1000"

        refused = _run_in_fresh_process(plan.serialize(), tmp_path, register_decryptor=False)
        assert refused.returncode != 0
        assert "no secret decryptor is registered" in refused.stderr

    def test_change_feed_plan_has_no_credentials(self, ffsec, aws_free_env, minio_prefix, tmp_path):
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
        ran = _run_in_fresh_process(protected.serialize(), tmp_path, register_decryptor=True)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip() == "3"


class TestCredentialRefProvider:
    def test_pickles_as_the_reference_and_a_per_plan_nonce(self):
        provider = CredentialRefProvider(7, 3)

        restored = pickle.loads(pickle.dumps(provider))

        assert vars(restored) == {"connection_id": 7, "user_id": 3, "nonce": provider.nonce}
        assert pickle.dumps(CredentialRefProvider(7, 3)) != pickle.dumps(provider)
        assert repr(provider) == "CredentialRefProvider(connection_id=7, user_id=3)"

    def test_an_s3_access_key_resolves_from_the_catalog(self, catalog):
        connection_id = catalog(
            {"aws_secret_access_key": SENTINEL, "aws_session_token": "tok"},
            connection_name="s3",
            storage_type="s3",
            auth_method="access_key",
            aws_access_key_id="AKID",
            endpoint_url="http://example",
        )
        assert CredentialRefProvider(connection_id, 1)() == (
            {"aws_access_key_id": "AKID", "aws_secret_access_key": SENTINEL, "aws_session_token": "tok"},
            None,
        )

    @pytest.mark.parametrize(
        ("method", "secret", "key"),
        [("access_key", "azure_account_key", "account_key"), ("sas_token", "azure_sas_token", "sas_token")],
    )
    def test_an_azure_key_or_sas_resolves_from_the_catalog(self, catalog, method, secret, key):
        connection_id = catalog({secret: SENTINEL}, storage_type="adls", auth_method=method, azure_account_name="acct")
        assert CredentialRefProvider(connection_id, 1)() == ({key: SENTINEL}, None)

    def test_a_service_principal_becomes_a_bearer_token(self, catalog, monkeypatch):
        import httpx

        sent = {}

        def _post(url, data, timeout):
            sent.update(url=url, data=data)
            return httpx.Response(200, json={"access_token": "bearer", "expires_in": 3599})

        monkeypatch.setattr(httpx, "post", _post)
        connection_id = catalog(
            {"azure_client_secret": SENTINEL},
            storage_type="adls",
            auth_method="service_principal",
            azure_tenant_id="tenant",
            azure_client_id="client",
        )

        credentials, expiry = CredentialRefProvider(connection_id, 1)()

        assert credentials == {"bearer_token": "bearer"}
        assert abs(expiry - (time.time() + 3599)) < 60
        assert sent["url"] == "https://login.microsoftonline.com/tenant/oauth2/v2.0/token"
        assert sent["data"] == {
            "grant_type": "client_credentials",
            "client_id": "client",
            "client_secret": SENTINEL,
            "scope": "https://storage.azure.com/.default",
        }

    def test_a_rejected_service_principal_is_a_clear_error(self, catalog, monkeypatch):
        import httpx

        monkeypatch.setattr(httpx, "post", lambda url, data, timeout: httpx.Response(401, text=SENTINEL))
        connection_id = catalog(
            {"azure_client_secret": SENTINEL},
            storage_type="adls",
            auth_method="service_principal",
            azure_tenant_id="tenant",
            azure_client_id="client",
        )
        with pytest.raises(CredentialResolutionError, match="rejected the service principal client") as info:
            CredentialRefProvider(connection_id, 1)()
        assert SENTINEL not in str(info.value)

    def test_a_service_account_key_becomes_a_bearer_token(self, catalog, monkeypatch):
        from google.oauth2 import service_account

        seen = {}

        class _Credentials:
            token = "bearer"
            expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)

            def refresh(self, request):
                seen["refreshed"] = True

        def _from_info(info, scopes):
            seen.update(info=info, scopes=scopes)
            return _Credentials()

        monkeypatch.setattr(service_account.Credentials, "from_service_account_info", staticmethod(_from_info))
        key = json.dumps({"type": "service_account", "private_key": SENTINEL})
        connection_id = catalog({"gcs_service_account_key": key}, storage_type="gcs", auth_method="service_account")

        credentials, expiry = CredentialRefProvider(connection_id, 1)()

        assert credentials == {"bearer_token": "bearer"}
        assert abs(expiry - (time.time() + 3600)) < 60
        assert seen == {
            "info": json.loads(key),
            "scopes": ["https://www.googleapis.com/auth/devstorage.read_only"],
            "refreshed": True,
        }

    def test_a_connection_missing_from_the_catalog_is_a_clear_error(self, catalog):
        with pytest.raises(CredentialResolutionError, match=r"has no cloud connection 99 \(user 4\)"):
            CredentialRefProvider(99, 4)()

    def test_secrets_this_process_cannot_decrypt_are_a_clear_error(self, catalog, monkeypatch):
        connection_id = catalog(
            {"aws_secret_access_key": SENTINEL}, storage_type="s3", auth_method="access_key", aws_access_key_id="A"
        )
        monkeypatch.delenv("TEST_MODE")
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())

        with pytest.raises(CredentialDecryptionError, match="must share the same master key") as info:
            CredentialRefProvider(connection_id, 1)()
        assert "cloud connection" in str(info.value)
        assert SENTINEL not in str(info.value)

    @requires_minio
    def test_iam_role_assumes_the_role_with_the_resolving_process_identity(self, catalog, monkeypatch, tmp_path):
        isolate_aws(
            monkeypatch,
            tmp_path,
            endpoint=None,
            AWS_ACCESS_KEY_ID=MINIO_ACCESS_KEY,
            AWS_SECRET_ACCESS_KEY=MINIO_SECRET_KEY,
            AWS_ENDPOINT_URL_STS=MINIO_ENDPOINT_URL,
        )
        connection_id = catalog(
            storage_type="s3",
            auth_method="iam_role",
            aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test",
            aws_region="us-east-1",
        )

        credentials, _ = CredentialRefProvider(connection_id, 1)()

        assert set(credentials) == {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}
        assert credentials["aws_access_key_id"] != MINIO_ACCESS_KEY

    @requires_minio
    def test_a_reference_plan_resolves_in_the_executing_process(self, catalog, aws_free_env, minio_prefix, tmp_path):
        path = f"s3://{_BUCKET}/{minio_prefix}/ref.parquet"
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(path, storage_options={**_NON_SECRET, **_MINIO_CREDENTIALS})
        provider = CredentialRefProvider(_minio_connection(catalog), 1)

        plan = pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=provider).serialize()

        assert MINIO_SECRET_KEY.encode() not in plan
        assert b"$ffsec$" not in plan
        ran = _run_in_fresh_process(plan, tmp_path, register_decryptor=True)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip() == "3"

    @requires_minio
    def test_a_later_plan_resolves_the_connection_again(self, catalog, aws_free_env, minio_prefix):
        """Polars caches resolved credentials per pickled provider: each plan resolves the connection as it is then."""
        path = f"s3://{_BUCKET}/{minio_prefix}/again.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(path, storage_options={**_NON_SECRET, **_MINIO_CREDENTIALS})
        connection_id = _minio_connection(catalog)

        def _plan() -> pl.LazyFrame:
            provider = CredentialRefProvider(connection_id, 1)
            return pl.scan_parquet(path, storage_options=_NON_SECRET, credential_provider=provider)

        assert _plan().collect().height == 2
        database = os.environ["FLOWFILE_DATABASE_URL"].removeprefix("sqlite:///")
        with sqlite3.connect(database) as con:
            con.execute("UPDATE secrets SET encrypted_value = ?", (crypto.encrypt_secret("not-the-secret", 1),))
        with pytest.raises(OSError):
            _plan().collect()
