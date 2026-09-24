"""Pure-logic storage options builders for cloud storage providers.

These functions accept plain string values (callers are responsible for
extracting/decrypting secrets before calling). This allows both flowfile_core
and flowfile_worker to share the same logic despite different secret
management approaches.
"""

from __future__ import annotations

from typing import Any

from shared.cloud_storage.utils import create_storage_options_from_boto_credentials, session_token_option

# object_store's client option for skipping TLS verification; polars and deltalake silently ignore "verify".
ALLOW_INVALID_CERTIFICATES = "allow_invalid_certificates"
# Object-store option keys boto3 accepts as client kwargs; anything else (aws_allow_http, ...) would raise.
_S3_CLIENT_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token", "endpoint_url")


def tls_verification_disabled(storage_options: dict[str, Any] | None) -> bool:
    """Whether built storage options ask to skip TLS certificate verification (for boto3/Azure SDK clients)."""
    return str((storage_options or {}).get(ALLOW_INVALID_CERTIFICATES)).lower() == "true"


def build_s3_client(
    storage_options: dict[str, Any] | None,
    *,
    timeouts: tuple[float, float] | None = None,
    path_style: bool = False,
):
    """A boto3 S3 client from Polars-shaped storage options; only the keys boto3 understands are forwarded.

    *timeouts* is ``(connect, read)`` seconds and also caps retries at 2, for interactive listings.
    *path_style* addresses a custom endpoint path-style: S3-compatible stores do not serve virtual-hosted URLs.
    """
    import boto3
    from botocore.config import Config

    options = storage_options or {}
    kwargs: dict[str, Any] = {key: options[key] for key in _S3_CLIENT_KEYS if options.get(key)}
    region = options.get("aws_region") or options.get("region_name")
    if region:
        kwargs["region_name"] = region
    if tls_verification_disabled(options):
        kwargs["verify"] = False  # a bool: boto3 reads a string verify as a CA-bundle path

    config = None
    if timeouts:
        connect_timeout, read_timeout = timeouts
        config = Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": 2, "mode": "standard"},
        )
    if path_style and kwargs.get("endpoint_url"):
        path_config = Config(s3={"addressing_style": "path"})
        config = path_config if config is None else config.merge(path_config)
    return boto3.client("s3", config=config, **kwargs)


def build_storage_options(
    storage_type: str,
    auth_method: str,
    *,
    # S3 fields
    connection_name: str | None = None,
    aws_region: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_role_arn: str | None = None,
    aws_allow_unsafe_html: bool | None = None,
    aws_session_token: str | None = None,
    aws_profile: str | None = None,
    # ADLS fields
    azure_account_name: str | None = None,
    azure_account_key: str | None = None,
    azure_tenant_id: str | None = None,
    azure_client_id: str | None = None,
    azure_client_secret: str | None = None,
    azure_sas_token: str | None = None,
    # GCS fields
    gcs_service_account_key: str | None = None,
    gcs_project_id: str | None = None,
    # Common
    endpoint_url: str | None = None,
    verify_ssl: bool = True,
) -> dict[str, str]:
    """Build storage options dict based on the storage type and auth method.

    All secret values must be provided as plain strings (already decrypted); the result is all strings (``_finalize``).
    """
    if storage_type == "s3":
        options = build_s3_storage_options(
            auth_method=auth_method,
            connection_name=connection_name,
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_role_arn=aws_role_arn,
            aws_allow_unsafe_html=aws_allow_unsafe_html,
            aws_profile=aws_profile,
            endpoint_url=endpoint_url,
            verify_ssl=verify_ssl,
        )
    elif storage_type == "adls":
        options = build_adls_storage_options(
            auth_method=auth_method,
            azure_account_name=azure_account_name,
            azure_account_key=azure_account_key,
            azure_tenant_id=azure_tenant_id,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            azure_sas_token=azure_sas_token,
            endpoint_url=endpoint_url,
            verify_ssl=verify_ssl,
        )
    elif storage_type == "gcs":
        options = build_gcs_storage_options(
            auth_method=auth_method,
            gcs_service_account_key=gcs_service_account_key,
            gcs_project_id=gcs_project_id,
            endpoint_url=endpoint_url,
        )
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
    return _finalize(options)


def _finalize(options: dict[str, Any] | None) -> dict[str, str]:
    """Coerce builder output to the all-string dict that polars, deltalake and object_store accept.

    ``None`` is dropped; bools become ``"true"``/``"false"``.
    """
    finalized: dict[str, str] = {}
    for key, value in (options or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            finalized[key] = "true" if value else "false"
        elif isinstance(value, int | float):
            finalized[key] = str(value)
        else:
            finalized[key] = value
    return finalized


def build_s3_storage_options(
    auth_method: str,
    *,
    connection_name: str | None = None,
    aws_region: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    aws_role_arn: str | None = None,
    aws_allow_unsafe_html: bool | None = None,
    aws_profile: str | None = None,
    endpoint_url: str | None = None,
    verify_ssl: bool = True,
) -> dict[str, Any]:
    """Build S3-specific storage options.

    The auth method decides only the credentials (aws-cli via boto3 from ``aws_profile``, never the
    connection name); the endpoint, plain-HTTP and TLS options apply to every method.
    """
    import boto3

    storage_options: dict[str, Any] = {}
    if aws_region:
        storage_options["aws_region"] = aws_region

    if auth_method == "aws-cli":
        storage_options.update(
            create_storage_options_from_boto_credentials(profile_name=aws_profile, region_name=aws_region)
        )

    elif auth_method == "access_key":
        missing = [
            field
            for field, value in (
                ("aws_access_key_id", aws_access_key_id),
                ("aws_secret_access_key", aws_secret_access_key),
            )
            if not value
        ]
        if missing:
            label = f" '{connection_name}'" if connection_name not in (None, "", "None") else ""
            raise ValueError(
                f"Cloud connection{label} uses access_key auth but is missing {' and '.join(missing)}. "
                "Edit the connection and enter both the access key ID and the secret access key."
            )
        storage_options["aws_access_key_id"] = aws_access_key_id
        storage_options["aws_secret_access_key"] = aws_secret_access_key
        storage_options["aws_session_token"] = session_token_option(aws_session_token)

    elif auth_method == "iam_role":
        sts_client = boto3.client("sts", region_name=aws_region)
        assumed_role_object = sts_client.assume_role(
            RoleArn=aws_role_arn,
            RoleSessionName="PolarsCloudStorageReaderSession",
        )
        credentials = assumed_role_object["Credentials"]
        storage_options["aws_access_key_id"] = credentials["AccessKeyId"]
        storage_options["aws_secret_access_key"] = credentials["SecretAccessKey"]
        storage_options["aws_session_token"] = credentials["SessionToken"]

    if endpoint_url:
        storage_options["endpoint_url"] = endpoint_url
    if aws_allow_unsafe_html:
        storage_options["aws_allow_http"] = "true"
    if not verify_ssl:
        storage_options[ALLOW_INVALID_CERTIFICATES] = "true"

    return storage_options


def build_adls_storage_options(
    auth_method: str,
    *,
    azure_account_name: str | None = None,
    azure_account_key: str | None = None,
    azure_tenant_id: str | None = None,
    azure_client_id: str | None = None,
    azure_client_secret: str | None = None,
    azure_sas_token: str | None = None,
    endpoint_url: str | None = None,
    verify_ssl: bool = True,
) -> dict[str, Any]:
    """Build Azure ADLS-specific storage options."""
    storage_options: dict[str, Any] = {}
    if not verify_ssl:
        storage_options[ALLOW_INVALID_CERTIFICATES] = "true"

    if auth_method == "access_key":
        if azure_account_name:
            storage_options["account_name"] = azure_account_name
        if azure_account_key:
            storage_options["account_key"] = azure_account_key

    elif auth_method == "service_principal":
        if azure_tenant_id:
            storage_options["tenant_id"] = azure_tenant_id
        if azure_client_id:
            storage_options["client_id"] = azure_client_id
        if azure_client_secret:
            storage_options["client_secret"] = azure_client_secret

    elif auth_method == "sas_token":
        if azure_account_name:
            storage_options["account_name"] = azure_account_name
        if azure_sas_token:
            storage_options["sas_token"] = azure_sas_token

    elif auth_method == "managed_identity":
        if azure_account_name:
            storage_options["account_name"] = azure_account_name
        storage_options["use_azure_cli"] = "true"

    if endpoint_url:
        if endpoint_url.startswith("http://"):
            # Emulator mode (e.g. Azurite): use path-style URLs and allow HTTP
            storage_options["azure_storage_use_emulator"] = "true"
            storage_options["azure_storage_allow_http"] = "true"
            # Build emulator endpoint with account name in path
            account = azure_account_name or "devstoreaccount1"
            storage_options["azure_storage_endpoint"] = f"{endpoint_url.rstrip('/')}/{account}"
        else:
            storage_options["azure_storage_endpoint"] = endpoint_url

    return storage_options


def build_gcs_storage_options(
    auth_method: str,
    *,
    gcs_service_account_key: str | None = None,
    gcs_project_id: str | None = None,
    endpoint_url: str | None = None,
) -> dict[str, Any]:
    """Build GCS-specific storage options (fsspec/gcsfs-compatible); ``verify_ssl`` is not applied, gcsfs has none."""
    storage_options: dict[str, Any] = {}

    if auth_method == "service_account" and gcs_service_account_key:
        storage_options["token"] = gcs_service_account_key
    elif endpoint_url:
        # Emulator (e.g. fake-gcs-server): anonymous auth via gcsfs
        storage_options["token"] = "anon"

    if gcs_project_id:
        storage_options["project"] = gcs_project_id

    if endpoint_url:
        storage_options["endpoint_url"] = endpoint_url

    return storage_options
