import asyncio
import datetime
import ipaddress
import os
import ssl
import threading
import uuid

import polars as pl
import pytest
from deltalake import DeltaTable

from shared.cloud_storage.browse import list_cloud_uri
from shared.cloud_storage.directory import get_first_file_from_s3_dir
from shared.cloud_storage.storage_options import build_s3_client, build_s3_storage_options, build_storage_options
from shared.cloud_storage.utils import (
    create_storage_options_from_boto_credentials,
    ensure_path_has_wildcard_pattern,
    session_token_option,
    validate_cloud_resource_path,
)

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

_BUCKET = "flowfile-test"
# Discard port: anything that falls back to the environment fails fast here instead of reaching real AWS.
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

    ``AWS_ENDPOINT_URL`` is a dead local port, so only a connection's endpoint reaches MinIO. Returns a profile writer.
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
        assert options["allow_invalid_certificates"] == "true"


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


class TestSessionTokenRule:
    """One helper owns "blank, never absent": both credential producers go through it."""

    @pytest.mark.parametrize(("token", "expected"), [(None, ""), ("", ""), ("tok", "tok")])
    def test_helper_blanks_a_missing_token(self, token, expected):
        assert session_token_option(token) == expected

    def test_static_profile_credentials_carry_a_blank_token(self, hermetic_aws):
        hermetic_aws("AKIDSTATIC", "static-secret-value")
        assert create_storage_options_from_boto_credentials(None)["aws_session_token"] == ""

    def test_finalize_no_longer_special_cases_the_token(self):
        # A builder that emits None would now drop the key; the producers, not _finalize, own the rule.
        from shared.cloud_storage.storage_options import _finalize

        assert _finalize({"aws_session_token": None, "aws_allow_http": True}) == {"aws_allow_http": "true"}


class TestBuildS3Client:
    """The one boto3 client builder; browse.py adds timeouts + path style, directory.py takes the defaults."""

    @pytest.fixture
    def captured_kwargs(self, monkeypatch):
        """Capture what build_s3_client hands boto3, without constructing a real client."""
        import boto3

        captured = {}

        def _fake_client(service, **kwargs):
            captured["service"] = service
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(boto3, "client", _fake_client)
        return captured

    def test_allow_invalid_certificates_becomes_a_boolean_verify(self, captured_kwargs):
        # build_s3_storage_options emits the object_store key as the *string* "true"; boto3 needs a bool.
        build_s3_client({"aws_access_key_id": "k", "aws_secret_access_key": "s", "allow_invalid_certificates": "true"})
        assert captured_kwargs["verify"] is False

    def test_verification_stays_on_by_default(self, captured_kwargs):
        build_s3_client({"aws_access_key_id": "k", "allow_invalid_certificates": "false"})
        assert "verify" not in captured_kwargs

    def test_aws_region_is_translated_to_region_name(self, captured_kwargs):
        build_s3_client({"aws_access_key_id": "k", "aws_region": "eu-west-1"})
        assert captured_kwargs["region_name"] == "eu-west-1"
        assert "aws_region" not in captured_kwargs

    def test_object_store_only_keys_are_dropped_not_forwarded(self, captured_kwargs):
        # boto3.client() raises on unknown kwargs, so forwarding these would break every listing and read.
        build_s3_client(
            {
                "aws_access_key_id": "k",
                "aws_secret_access_key": "s",
                "aws_allow_http": "true",
                "aws_virtual_hosted_style_request": "false",
            }
        )
        assert set(captured_kwargs) == {"service", "aws_access_key_id", "aws_secret_access_key", "config"}

    def test_blank_session_token_is_not_forwarded(self, captured_kwargs):
        # The builders blank aws_session_token to defeat ambient env tokens; boto3 must not see "".
        build_s3_client({"aws_access_key_id": "k", "aws_secret_access_key": "s", "aws_session_token": ""})
        assert "aws_session_token" not in captured_kwargs

    def test_no_options_keeps_botocore_defaults(self, captured_kwargs):
        build_s3_client(None)
        assert captured_kwargs == {"service": "s3", "config": None}

    def test_timeouts_bound_the_client_and_cap_retries(self, captured_kwargs):
        build_s3_client({"aws_access_key_id": "k"}, timeouts=(5, 15))
        config = captured_kwargs["config"]
        assert (config.connect_timeout, config.read_timeout) == (5, 15)
        assert config.retries == {"max_attempts": 2, "mode": "standard"}

    def test_path_style_applies_to_a_custom_endpoint(self, captured_kwargs):
        build_s3_client({"aws_access_key_id": "k", "endpoint_url": "http://localhost:9000"}, path_style=True)
        assert captured_kwargs["endpoint_url"] == "http://localhost:9000"
        assert captured_kwargs["config"].s3["addressing_style"] == "path"

    def test_aws_endpoint_keeps_default_addressing(self, captured_kwargs):
        build_s3_client({"aws_access_key_id": "k"}, path_style=True)
        assert captured_kwargs["config"] is None

    def test_path_style_merges_into_the_timeout_config(self, captured_kwargs):
        build_s3_client(
            {"aws_access_key_id": "k", "endpoint_url": "http://localhost:9000"}, timeouts=(5, 15), path_style=True
        )
        config = captured_kwargs["config"]
        assert config.connect_timeout == 5
        assert config.s3["addressing_style"] == "path"

    def test_a_stored_session_token_is_forwarded(self):
        options = build_storage_options(
            "s3", "access_key", aws_access_key_id="ASIA", aws_secret_access_key="s", aws_session_token="tok"
        )
        assert options["aws_session_token"] == "tok"


class TestTlsVerificationMapping:
    """``verify_ssl=False`` must reach object_store as ``allow_invalid_certificates``; ``verify`` is ignored there."""

    @pytest.mark.parametrize(
        ("storage_type", "auth_method", "extra"),
        [
            ("s3", "access_key", {"aws_access_key_id": "AKID", "aws_secret_access_key": "s"}),
            ("s3", "env_vars", {}),
            ("adls", "access_key", {"azure_account_name": "acct", "azure_account_key": "a2V5"}),
            ("adls", "sas_token", {"azure_account_name": "acct", "azure_sas_token": "sv=1"}),
        ],
    )
    def test_verify_ssl_false_allows_invalid_certificates(self, storage_type, auth_method, extra):
        insecure = build_storage_options(storage_type, auth_method, verify_ssl=False, **extra)
        secure = build_storage_options(storage_type, auth_method, verify_ssl=True, **extra)
        assert insecure["allow_invalid_certificates"] == "true"
        assert "allow_invalid_certificates" not in secure
        assert "verify" not in insecure and "verify" not in secure

    def test_aws_cli_maps_verify_ssl_too(self, hermetic_aws):
        hermetic_aws("AKIDSTATIC", "static-secret-value")
        assert build_storage_options("s3", "aws-cli", verify_ssl=False)["allow_invalid_certificates"] == "true"

    def test_gcs_has_no_tls_option(self):
        options = build_storage_options("gcs", "service_account", gcs_service_account_key="{}", verify_ssl=False)
        assert "allow_invalid_certificates" not in options and "verify" not in options


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
    """Every S3 auth method builds options polars, deltalake and boto3 accept, with the endpoint from the connection."""
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


@requires_minio
@pytest.mark.parametrize("ambient_token", [None, "bogus-ambient-token"], ids=["no_env_token", "env_token"])
def test_aws_cli_connection_round_trip_on_minio(hermetic_aws, monkeypatch, minio_prefix, ambient_token):
    """A saved aws-cli connection pointed at MinIO stays on MinIO for every polars/deltalake consumer.

    The ambient token variant proves the blank token shadows a stray ``AWS_SESSION_TOKEN``.
    """
    hermetic_aws(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    if ambient_token:
        monkeypatch.setenv("AWS_SESSION_TOKEN", ambient_token)
    options = build_storage_options("s3", "aws-cli", **_minio_connection(connection_name="minio connection"))
    assert options["aws_session_token"] == ""
    assert options["endpoint_url"] == MINIO_ENDPOINT_URL

    frame = pl.DataFrame({"id": [1, 2, 3], "category": ["a", None, "b"], "output_field": ["test"] * 3})
    for _ in range(2):
        frame.lazy().sink_delta(
            f"{minio_prefix}/partitioned",
            mode="append",
            storage_options=options,
            delta_write_options={"partition_by": ["output_field"]},
        )
    table = DeltaTable(f"{minio_prefix}/partitioned", storage_options=options)
    assert table.version() == 1
    assert table.metadata().partition_columns == ["output_field"]
    assert pl.scan_delta(f"{minio_prefix}/partitioned", storage_options=options).collect().height == 6

    frame.write_delta(f"{minio_prefix}/plain", mode="overwrite", storage_options=options)
    assert DeltaTable(f"{minio_prefix}/plain", storage_options=options).version() == 0

    frame.write_parquet(f"{minio_prefix}/file.parquet", storage_options=options)
    assert pl.scan_parquet(f"{minio_prefix}/file.parquet", storage_options=options).collect().equals(frame)


@pytest.fixture
def minio_behind_self_signed_tls(tmp_path):
    """An HTTPS endpoint with a self-signed certificate that forwards to MinIO; yields its URL."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (tmp_path / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (tmp_path / "key.pem").write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "cert.pem", tmp_path / "key.pem")
    upstream_host, upstream_port = MINIO_ENDPOINT_URL.removeprefix("http://").split(":")

    async def pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def forward(client_reader, client_writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, int(upstream_port))
        await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, _context: None)  # rejected handshakes are the point of the test
    server = loop.run_until_complete(asyncio.start_server(forward, "127.0.0.1", 0, ssl=context))
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{server.sockets[0].getsockname()[1]}"
    finally:
        loop.call_soon_threadsafe(server.close)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


@requires_minio
@pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")
def test_verify_ssl_false_reaches_every_s3_client(hermetic_aws, minio_prefix, minio_behind_self_signed_tls):
    """Against a self-signed endpoint, verify_ssl=False works for polars, deltalake and both boto3 clients.

    The secure options and the old ``verify: "False"`` key fail on it, proving the certificate is rejected.
    """
    connection = _minio_connection(
        endpoint_url=minio_behind_self_signed_tls,
        aws_allow_unsafe_html=False,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    insecure = build_storage_options("s3", "access_key", **connection, verify_ssl=False)
    secure = build_storage_options("s3", "access_key", **connection, verify_ssl=True)

    frame = pl.DataFrame({"id": [1, 2]})
    frame.write_parquet(f"{minio_prefix}/file.parquet", storage_options=insecure)
    assert pl.scan_parquet(f"{minio_prefix}/file.parquet", storage_options=insecure).collect().height == 2
    frame.write_delta(f"{minio_prefix}/table", storage_options=insecure)
    assert DeltaTable(f"{minio_prefix}/table", storage_options=insecure).version() == 0
    assert get_first_file_from_s3_dir(f"{minio_prefix}/**/*.parquet", insecure).startswith(minio_prefix)
    listed = list_cloud_uri("s3", f"{minio_prefix}/", insecure)
    assert {entry.name for entry in listed.entries} >= {"file.parquet", "table"}

    for rejected in (secure, {**secure, "verify": "False"}):
        with pytest.raises(OSError):
            pl.scan_parquet(f"{minio_prefix}/file.parquet", storage_options=rejected).collect()
        with pytest.raises(OSError):
            DeltaTable(f"{minio_prefix}/table", storage_options=rejected)


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


class TestWildcardPattern:
    @pytest.mark.parametrize("path", ["", "  ", "/", "///"])
    def test_never_produces_a_filesystem_root_glob(self, path):
        with pytest.raises(ValueError):
            ensure_path_has_wildcard_pattern(path, "parquet")

    def test_directory_gets_a_recursive_pattern(self):
        assert ensure_path_has_wildcard_pattern("s3://b/dir/", "csv") == "s3://b/dir/**/*.csv"
        assert ensure_path_has_wildcard_pattern("s3://b/dir/*.csv", "csv") == "s3://b/dir/*.csv"
