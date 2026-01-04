"""Cloud storage connection schemas for S3, ADLS, and other cloud providers."""

import base64
from typing import Literal

import polars as pl
from pydantic import BaseModel, SecretStr, field_validator

from flowfile_core.secret_manager.secret_manager import encrypt_secret

CloudStorageType = Literal["s3", "adls", "gcs"]
AuthMethod = Literal[
    "access_key", "iam_role", "service_principal", "managed_identity", "sas_token", "aws-cli", "env_vars"
]


def encrypt_for_worker(secret_value: SecretStr | None) -> str | None:
    """
    Encrypts a secret value for use in worker contexts.
    This is a placeholder function that simulates encryption.
    In practice, you would use a secure encryption method.
    """
    if secret_value is not None:
        return encrypt_secret(secret_value.get_secret_value())


class AuthSettingsInput(BaseModel):
    """
    The information needed for the user to provide the details that are needed to provide how to connect to the
     Cloud provider
    """

    storage_type: CloudStorageType
    auth_method: AuthMethod
    connection_name: str | None = "None"  # This is the reference to the item we will fetch that contains the data


class FullCloudStorageConnectionWorkerInterface(AuthSettingsInput):
    """Internal model with decrypted secrets"""

    # AWS S3
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_role_arn: str | None = None
    aws_allow_unsafe_html: bool | None = None
    aws_session_token: str | None = None

    # Azure ADLS
    azure_account_name: str | None = None
    azure_account_key: str | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None

    # Common
    endpoint_url: str | None = None
    verify_ssl: bool = True


class FullCloudStorageConnection(AuthSettingsInput):
    """Internal model with decrypted secrets"""

    # AWS S3
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_role_arn: str | None = None
    aws_allow_unsafe_html: bool | None = None
    aws_session_token: SecretStr | None = None

    # Azure ADLS
    azure_account_name: str | None = None
    azure_account_key: SecretStr | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: SecretStr | None = None

    # Common
    endpoint_url: str | None = None
    verify_ssl: bool = True

    def get_worker_interface(self) -> "FullCloudStorageConnectionWorkerInterface":
        """
        Convert to a public interface model without secrets.
        """
        return FullCloudStorageConnectionWorkerInterface(
            storage_type=self.storage_type,
            auth_method=self.auth_method,
            connection_name=self.connection_name,
            aws_allow_unsafe_html=self.aws_allow_unsafe_html,
            aws_secret_access_key=encrypt_for_worker(self.aws_secret_access_key),
            aws_region=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_role_arn=self.aws_role_arn,
            aws_session_token=encrypt_for_worker(self.aws_session_token),
            azure_account_name=self.azure_account_name,
            azure_tenant_id=self.azure_tenant_id,
            azure_account_key=encrypt_for_worker(self.azure_account_key),
            azure_client_id=self.azure_client_id,
            azure_client_secret=encrypt_for_worker(self.azure_client_secret),
            endpoint_url=self.endpoint_url,
            verify_ssl=self.verify_ssl,
        )


class FullCloudStorageConnectionInterface(AuthSettingsInput):
    """API response model - no secrets exposed"""

    # Public fields only
    aws_allow_unsafe_html: bool | None = None
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_role_arn: str | None = None
    azure_account_name: str | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    endpoint_url: str | None = None
    verify_ssl: bool = True


class CloudStorageSettings(BaseModel):
    """Settings for cloud storage nodes in the visual designer"""

    auth_mode: AuthMethod = "auto"
    connection_name: str | None = None  # Required only for 'reference' mode
    resource_path: str  # s3://bucket/path/to/file.csv

    @field_validator("auth_mode", mode="after")
    def validate_auth_requirements(cls, v, values):
        data = values.data
        if v == "reference" and not data.get("connection_name"):
            raise ValueError("connection_name required when using reference mode")
        return v


class CloudStorageReadSettings(CloudStorageSettings):
    """Settings for reading from cloud storage"""

    scan_mode: Literal["single_file", "directory"] = "single_file"
    file_format: Literal["csv", "parquet", "json", "delta", "iceberg"] = "parquet"
    csv_has_header: bool | None = True
    csv_delimiter: str | None = ","
    csv_encoding: str | None = "utf8"
    delta_version: int | None = None


class CloudStorageReadSettingsInternal(BaseModel):
    read_settings: CloudStorageReadSettings
    connection: FullCloudStorageConnection


class WriteSettingsWorkerInterface(BaseModel):
    """Settings for writing to cloud storage"""

    resource_path: str  # s3://bucket/path/to/file.csv

    write_mode: Literal["overwrite", "append"] = "overwrite"
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet"

    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy"

    csv_delimiter: str = ","
    csv_encoding: str = "utf8"


class CloudStorageWriteSettings(CloudStorageSettings, WriteSettingsWorkerInterface):
    """Settings for writing to cloud storage"""

    pass

    def get_write_setting_worker_interface(self) -> WriteSettingsWorkerInterface:
        """
        Convert to a worker interface model without secrets.
        """
        return WriteSettingsWorkerInterface(
            resource_path=self.resource_path,
            write_mode=self.write_mode,
            file_format=self.file_format,
            parquet_compression=self.parquet_compression,
            csv_delimiter=self.csv_delimiter,
            csv_encoding=self.csv_encoding,
        )


class CloudStorageWriteSettingsInternal(BaseModel):
    write_settings: CloudStorageWriteSettings
    connection: FullCloudStorageConnection


class CloudStorageWriteSettingsWorkerInterface(BaseModel):
    """Settings for writing to cloud storage in worker context"""

    operation: str
    write_settings: WriteSettingsWorkerInterface
    connection: FullCloudStorageConnectionWorkerInterface
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1


def get_cloud_storage_write_settings_worker_interface(
    write_settings: CloudStorageWriteSettings,
    connection: FullCloudStorageConnection,
    lf: pl.LazyFrame,
    flowfile_flow_id: int = 1,
    flowfile_node_id: int | str = -1,
) -> CloudStorageWriteSettingsWorkerInterface:
    """
    Convert to a worker interface model with hashed secrets.
    """
    operation = base64.b64encode(lf.serialize()).decode()

    return CloudStorageWriteSettingsWorkerInterface(
        operation=operation,
        write_settings=write_settings.get_write_setting_worker_interface(),
        connection=connection.get_worker_interface(),
        flowfile_flow_id=flowfile_flow_id,  # Default value, can be overridden
        flowfile_node_id=flowfile_node_id,  # Default value, can be overridden
    )
