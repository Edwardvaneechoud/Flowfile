import os
import uuid

import polars as pl
import pytest
from deltalake import DeltaTable

from shared.cloud_storage.directory import get_first_file_from_s3_dir
from shared.cloud_storage.storage_options import build_s3_storage_options, build_storage_options
from shared.cloud_storage.utils import ensure_path_has_wildcard_pattern, validate_cloud_resource_path

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

_BUCKET = "flowfile-test"
# Discard port: anything that ignores the connection's endpoint and falls back to the environment fails fast
# here instead of sending the test credentials to real AWS.
_DEAD_ENDPOINT = "http://127.0.0.1:9"


def _minio_available() -> bool:
    """True only when the shared MinIO mock is reachable (never starts/stops it)."""
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


@pytest.fixture
def hermetic_aws(monkeypatch, tmp_path):
    """Point boto3 at empty temp AWS files with every ambient AWS_* variable removed.

    ``AWS_ENDPOINT_URL`` is set to a dead local port, so only an endpoint taken from the connection can
    reach MinIO and nothing reaches real AWS. Returns a writer that adds a static-key profile.
    """
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    credentials, config, boto_config = tmp_path / "credentials", tmp_path / "config", tmp_path / "boto.cfg"
    for path in (credentials, config, boto_config):
        path.write_text("")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("BOTO_CONFIG", str(boto_config))
    monkeypatch.setenv("AWS_ENDPOINT_URL", _DEAD_ENDPOINT)

    def write_static_profile(
        access_key: str, secret_key: str, region: str = "us-east-1", profile: str = "default"
    ) -> None:
        with credentials.open("a") as handle:
            handle.write(f"[{profile}]\naws_access_key_id = {access_key}\naws_secret_access_key = {secret_key}\n")
        section = "default" if profile == "default" else f"profile {profile}"
        with config.open("a") as handle:
            handle.write(f"[{section}]\nregion = {region}\n")

    return write_static_profile


def _minio_connection(**overrides) -> dict:
    """Connection fields for MinIO: the endpoint and plain-HTTP flag come from the connection itself."""
    return {"aws_region": "us-east-1", "endpoint_url": MINIO_ENDPOINT_URL, "aws_allow_unsafe_html": True, **overrides}


@pytest.fixture
def minio_prefix():
    """A unique prefix under the shared test bucket, deleted afterwards."""
    client = get_minio_client()
    try:
        client.create_bucket(Bucket=_BUCKET)
    except Exception:
        pass
    prefix = f"shared-cloud-options-{uuid.uuid4().hex[:8]}"
    yield f"s3://{_BUCKET}/{prefix}"
    listed = client.list_objects_v2(Bucket=_BUCKET, Prefix=f"{prefix}/")
    keys = [{"Key": obj["Key"]} for obj in listed.get("Contents", [])]
    if keys:
        client.delete_objects(Bucket=_BUCKET, Delete={"Objects": keys})


class TestAwsProfileSelection:
    """aws-cli credentials come from the explicit ``aws_profile``, never from the connection's display name."""

    def test_named_profile_is_used(self, hermetic_aws):
        hermetic_aws("AKIDDEFAULT", "default-secret")
        hermetic_aws("AKIDNAMED", "named-secret", region="eu-west-1", profile="analytics")
        options = build_storage_options(
            "s3", "aws-cli", connection_name="minio connection", aws_profile="analytics"
        )
        assert options["aws_access_key_id"] == "AKIDNAMED"
        assert options["aws_secret_access_key"] == "named-secret"
        assert options["aws_region"] == "eu-west-1"

    @pytest.mark.parametrize("profile", [None, "", "   "])
    def test_blank_profile_uses_the_default_chain(self, hermetic_aws, profile):
        hermetic_aws("AKIDDEFAULT", "default-secret")
        hermetic_aws("AKIDNAMED", "named-secret", profile="analytics")
        options = build_storage_options("s3", "aws-cli", aws_profile=profile)
        assert options["aws_access_key_id"] == "AKIDDEFAULT"

    def test_connection_name_is_never_used_as_a_profile(self, hermetic_aws):
        hermetic_aws("AKIDDEFAULT", "default-secret")
        hermetic_aws("AKIDNAMED", "named-secret", profile="minio connection")
        options = build_storage_options("s3", "aws-cli", connection_name="minio connection")
        assert options["aws_access_key_id"] == "AKIDDEFAULT"

    def test_unknown_profile_raises_an_actionable_error(self, hermetic_aws):
        hermetic_aws("AKIDDEFAULT", "default-secret")
        with pytest.raises(ValueError, match="AWS profile 'missing' was not found") as excinfo:
            build_storage_options("s3", "aws-cli", aws_profile="missing")
        assert "leave it blank" in str(excinfo.value)

    def test_connection_region_overrides_the_profile_region(self, hermetic_aws):
        hermetic_aws("AKIDDEFAULT", "default-secret", region="eu-west-1")
        options = build_storage_options("s3", "aws-cli", aws_region="us-east-2")
        assert options["aws_region"] == "us-east-2"


class TestAwsCliCredentials:
    """Real boto3 credential resolution against hermetic temp AWS files."""

    def test_static_key_profile_emits_no_none_values(self, hermetic_aws):
        hermetic_aws("AKIDSTATIC", "static-secret-value")
        options = build_storage_options(storage_type="s3", auth_method="aws-cli", connection_name=None)
        assert options == {
            "aws_access_key_id": "AKIDSTATIC",
            "aws_secret_access_key": "static-secret-value",
            "aws_session_token": "",
            "aws_region": "us-east-1",
        }

    def test_temporary_env_credentials_keep_their_token(self, hermetic_aws, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ASIATEMP")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "temp-secret")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "temp-token")
        options = build_storage_options("s3", "aws-cli", aws_region="us-east-1")
        assert options["aws_session_token"] == "temp-token"

    def test_no_credentials_raises_an_actionable_error(self, hermetic_aws):
        with pytest.raises(ValueError, match="No AWS credentials found") as excinfo:
            build_storage_options(storage_type="s3", auth_method="aws-cli", connection_name=None)
        assert "Select a cloud storage connection" in str(excinfo.value)

    def test_connection_endpoint_http_and_tls_options_are_merged(self, hermetic_aws):
        hermetic_aws("AKIDSTATIC", "static-secret-value")
        options = build_storage_options(
            "s3",
            "aws-cli",
            endpoint_url="https://minio.internal:9000",
            aws_allow_unsafe_html=True,
            verify_ssl=False,
        )
        assert options["aws_access_key_id"] == "AKIDSTATIC"
        assert options["endpoint_url"] == "https://minio.internal:9000"
        assert options["aws_allow_http"] == "true"
        assert options["verify"] == "False"


class TestAccessKeyValidation:
    @pytest.mark.parametrize(
        ("key_id", "secret", "missing"),
        [
            ("AKID", None, "aws_secret_access_key"),
            (None, "secret", "aws_access_key_id"),
            ("", "", "aws_access_key_id and aws_secret_access_key"),
        ],
    )
    def test_missing_key_material_raises_naming_the_field(self, key_id, secret, missing):
        with pytest.raises(ValueError, match=f"'minio' uses access_key auth but is missing {missing}\\."):
            build_s3_storage_options(
                "access_key",
                connection_name="minio",
                aws_access_key_id=key_id,
                aws_secret_access_key=secret,
            )

    def test_complete_key_pair_blanks_the_session_token(self):
        options = build_s3_storage_options("access_key", aws_access_key_id="AKID", aws_secret_access_key="secret")
        assert options["aws_session_token"] == ""
        assert None not in options.values()



_AUTH_METHODS = (
    "access_key",
    "iam_role",
    "aws-cli",
    "env_vars",
    "service_principal",
    "managed_identity",
    "sas_token",
    "service_account",
    "auto",
)


@pytest.fixture
def every_connection_field(hermetic_aws, monkeypatch):
    """Every connection field set; iam_role assumes its role against MinIO's STS, aws-cli reads a temp profile."""
    hermetic_aws(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    monkeypatch.setenv("AWS_ENDPOINT_URL_STS", MINIO_ENDPOINT_URL)
    return {
        "connection_name": "every field",
        "aws_region": "us-east-1",
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "aws_session_token": None,
        "aws_role_arn": "arn:aws:iam::123456789012:role/flowfile-test",
        "aws_allow_unsafe_html": True,
        "aws_profile": None,
        "azure_account_name": "devstoreaccount1",
        "azure_account_key": "a2V5",
        "azure_tenant_id": "tenant",
        "azure_client_id": "client",
        "azure_client_secret": "client-secret",
        "azure_sas_token": "sv=1",
        "gcs_service_account_key": '{"type": "service_account"}',
        "gcs_project_id": "project",
        "endpoint_url": MINIO_ENDPOINT_URL,
        "verify_ssl": False,
    }


@pytest.mark.parametrize("storage_type", ["s3", "adls", "gcs"])
@pytest.mark.parametrize("auth_method", _AUTH_METHODS)
def test_every_auth_method_and_storage_type_returns_only_strings(storage_type, auth_method, every_connection_field):
    if storage_type == "s3" and auth_method == "iam_role" and not _minio_available():
        pytest.skip("iam_role assumes its role against MinIO's STS")
    options = build_storage_options(storage_type, auth_method, **every_connection_field)
    assert isinstance(options, dict)
    assert all(isinstance(key, str) and isinstance(value, str) for key, value in options.items()), options


@pytest.mark.parametrize("storage_type", ["s3", "adls", "gcs"])
@pytest.mark.parametrize("auth_method", ["env_vars", "managed_identity", "service_account", "auto"])
def test_sparse_connections_return_a_dict_without_none(storage_type, auth_method):
    options = build_storage_options(storage_type, auth_method, verify_ssl=True)
    assert isinstance(options, dict)
    assert all(isinstance(value, str) for value in options.values()), options


@requires_minio
@pytest.mark.parametrize("auth_method", ["access_key", "aws-cli", "aws-cli-named-profile", "env_vars", "iam_role"])
def test_s3_options_work_against_minio_for_every_auth_method(auth_method, hermetic_aws, monkeypatch, minio_prefix):
    """Every S3 auth method builds options polars, deltalake and boto3 accept, with the endpoint from the connection.

    ``AWS_ENDPOINT_URL`` points at a dead port, so a builder that dropped the connection's endpoint fails here.
    """
    connection = _minio_connection(connection_name="minio connection")
    if auth_method == "access_key":
        connection.update(aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
    elif auth_method == "aws-cli":
        hermetic_aws(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    elif auth_method == "aws-cli-named-profile":
        hermetic_aws("AKIDNOTMINIO", "wrong-secret")
        hermetic_aws(MINIO_ACCESS_KEY, MINIO_SECRET_KEY, profile="minio")
        auth_method, connection["aws_profile"] = "aws-cli", "minio"
    elif auth_method == "env_vars":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
    elif auth_method == "iam_role":
        hermetic_aws(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        monkeypatch.setenv("AWS_ENDPOINT_URL_STS", MINIO_ENDPOINT_URL)
        connection["aws_role_arn"] = "arn:aws:iam::123456789012:role/flowfile-test"

    options = build_storage_options("s3", auth_method, **connection)
    assert all(isinstance(value, str) for value in options.values())

    frame = pl.DataFrame({"id": [1, 2, 3], "category": ["a", None, "b"]})
    frame.write_parquet(f"{minio_prefix}/file.parquet", storage_options=options)
    assert pl.scan_parquet(f"{minio_prefix}/file.parquet", storage_options=options).collect().equals(frame)
    frame.write_delta(f"{minio_prefix}/table", mode="overwrite", storage_options=options)
    assert DeltaTable(f"{minio_prefix}/table", storage_options=options).version() == 0
    assert get_first_file_from_s3_dir(f"{minio_prefix}/**/*.parquet", options).startswith(minio_prefix)


class TestResourcePathGuard:
    @pytest.mark.parametrize("path", ["", "   ", None])
    def test_empty_writer_path(self, path):
        with pytest.raises(ValueError) as excinfo:
            validate_cloud_resource_path(path, role="writer")
        assert str(excinfo.value) == (
            "Cloud storage writer has no target path. Enter an object-storage URI such as s3://bucket/folder/table."
        )

    def test_empty_reader_path(self):
        with pytest.raises(ValueError) as excinfo:
            validate_cloud_resource_path("", role="reader")
        assert str(excinfo.value) == (
            "Cloud storage reader has no source path. "
            "Enter an object-storage URI such as s3://bucket/folder/file.parquet."
        )

    @pytest.mark.parametrize("path", ["output", "folder/table", "./out.parquet", "~/data.csv", "http://host/x"])
    def test_relative_or_non_cloud_path(self, path):
        with pytest.raises(ValueError) as excinfo:
            validate_cloud_resource_path(path, role="writer")
        assert str(excinfo.value) == (
            f"Cloud storage path '{path}' is not a URI. Use a path starting with s3://, az://, abfss:// or gs://."
        )

    @pytest.mark.parametrize("path", ["s3://bucket/table", "az://c/p", "abfss://c/p", "gs://b/k.parquet"])
    def test_cloud_uris_pass(self, path):
        assert validate_cloud_resource_path(path, role="reader") == path

    def test_absolute_local_path_passes(self, tmp_path):
        path = str(tmp_path / "table")
        assert validate_cloud_resource_path(path, role="writer") == path

    def test_absolute_local_path_refused_when_local_paths_are_off(self, tmp_path):
        with pytest.raises(ValueError, match="is a local path, which this server does not allow"):
            validate_cloud_resource_path(str(tmp_path / "table"), role="writer", allow_local_paths=False)
        assert validate_cloud_resource_path("s3://b/t", role="writer", allow_local_paths=False) == "s3://b/t"


class TestWildcardPattern:
    @pytest.mark.parametrize("path", ["", "  ", "/", "///"])
    def test_never_produces_a_filesystem_root_glob(self, path):
        with pytest.raises(ValueError):
            ensure_path_has_wildcard_pattern(path, "parquet")

    def test_directory_gets_a_recursive_pattern(self):
        assert ensure_path_has_wildcard_pattern("s3://b/dir/", "csv") == "s3://b/dir/**/*.csv"
        assert ensure_path_has_wildcard_pattern("s3://b/dir/*.csv", "csv") == "s3://b/dir/*.csv"
