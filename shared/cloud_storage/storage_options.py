"""Pure-logic storage options builders for cloud storage providers.

These functions accept plain string values (callers are responsible for
extracting/decrypting secrets before calling). This allows both flowfile_core
and flowfile_worker to share the same logic despite different secret
management approaches.
"""

from __future__ import annotations

from typing import Any

from shared.cloud_storage.utils import create_storage_options_from_boto_credentials


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
) -> dict[str, Any]:
    """Build storage options dict based on the storage type and auth method.

    All secret values must be provided as plain strings (already decrypted).
    """
    if storage_type == "s3":
        return build_s3_storage_options(
            auth_method=auth_method,
            connection_name=connection_name,
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_role_arn=aws_role_arn,
            aws_allow_unsafe_html=aws_allow_unsafe_html,
            endpoint_url=endpoint_url,
            verify_ssl=verify_ssl,
        )
    elif storage_type == "adls":
        return build_adls_storage_options(
            auth_method=auth_method,
            azure_account_name=azure_account_name,
            azure_account_key=azure_account_key,
            azure_tenant_id=azure_tenant_id,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            azure_sas_token=azure_sas_token,
            endpoint_url=endpoint_url,
        )
    elif storage_type == "gcs":
        return build_gcs_storage_options(
            auth_method=auth_method,
            gcs_service_account_key=gcs_service_account_key,
            gcs_project_id=gcs_project_id,
            endpoint_url=endpoint_url,
        )
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")


def build_s3_storage_options(
    auth_method: str,
    *,
    connection_name: str | None = None,
    aws_region: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_role_arn: str | None = None,
    aws_allow_unsafe_html: bool | None = None,
    endpoint_url: str | None = None,
    verify_ssl: bool = True,
) -> dict[str, Any]:
    """Build S3-specific storage options.

    For iam_role auth, this function assumes the role via boto3 STS.
    """
    import boto3

    if auth_method == "aws-cli":
        return create_storage_options_from_boto_credentials(
            profile_name=connection_name, region_name=aws_region
        )

    storage_options: dict[str, Any] = {}
    if aws_region:
        storage_options["aws_region"] = aws_region
    if endpoint_url:
        storage_options["endpoint_url"] = endpoint_url
    if not verify_ssl:
        storage_options["verify"] = "False"
    if aws_allow_unsafe_html:
        storage_options["aws_allow_http"] = "true"

    if auth_method == "access_key":
        storage_options["aws_access_key_id"] = aws_access_key_id
        storage_options["aws_secret_access_key"] = aws_secret_access_key
        # Explicitly clear any session token from the environment
        storage_options["aws_session_token"] = ""

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
) -> dict[str, Any]:
    """Build Azure ADLS-specific storage options."""
    storage_options: dict[str, Any] = {}

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
    """Build GCS-specific storage options (fsspec/gcsfs-compatible)."""
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
