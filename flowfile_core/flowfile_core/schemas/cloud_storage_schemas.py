"""Cloud storage connection schemas for S3, ADLS, and other cloud providers."""

from typing import Optional, Literal
import polars as pl
import base64

from pydantic import BaseModel, SecretStr, field_validator, Field

from flowfile_core.secret_manager.secret_manager import encrypt_secret

CloudStorageType = Literal["s3", "adls", "gcs"]
AuthMethod = Literal["access_key", "iam_role", "service_principal", "managed_identity", "sas_token", "aws-cli", "env_vars"]


def encrypt_for_worker(secret_value: SecretStr|None) -> str|None:
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
    connection_name: Optional[str] = "None"  # This is the reference to the item we will fetch that contains the data


class FullCloudStorageConnectionWorkerInterface(AuthSettingsInput):
    """Internal model with decrypted secrets"""

    # AWS S3
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_role_arn: Optional[str] = None
    aws_allow_unsafe_html: Optional[bool] = None
    aws_session_token: Optional[str] = None

    # Azure ADLS
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None

    # Common
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True


class FullCloudStorageConnection(AuthSettingsInput):
    """Internal model with decrypted secrets"""

    # AWS S3
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[SecretStr] = None
    aws_role_arn: Optional[str] = None
    aws_allow_unsafe_html: Optional[bool] = None
    aws_session_token: Optional[SecretStr] = None

    # Azure ADLS
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[SecretStr] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[SecretStr] = None

    # Common
    endpoint_url: Optional[str] = None
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
            verify_ssl=self.verify_ssl
        )


class FullCloudStorageConnectionInterface(AuthSettingsInput):
    """API response model - no secrets exposed"""

    # Public fields only
    aws_allow_unsafe_html: Optional[bool] = None
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_role_arn: Optional[str] = None
    azure_account_name: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True


class CloudStorageSettings(BaseModel):
    """Settings for cloud storage nodes in the visual designer"""

    auth_mode: AuthMethod = "auto"
    connection_name: Optional[str] = None  # Required only for 'reference' mode
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
    csv_has_header: Optional[bool] = True
    csv_delimiter: Optional[str] = ","
    csv_encoding: Optional[str] = "utf8"
    delta_version: Optional[int] = None


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
            csv_encoding=self.csv_encoding
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
        flowfile_node_id=flowfile_node_id  # Default value, can be overridden
    )