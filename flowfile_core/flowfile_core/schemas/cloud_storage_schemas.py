"""Cloud storage connection schemas for S3, ADLS, and other cloud providers."""

from typing import Optional, Literal

from pydantic import BaseModel, SecretStr, field_validator, Field

from flowfile_core.schemas.schemas import SecretRef

CloudStorageType = Literal["s3", "adls", "gcs"]
AuthMethod = Literal["access_key", "iam_role", "service_principal", "managed_identity", "sas_token", "aws-cli", "env_vars"]


class AuthSettingsInput(BaseModel):
    """
    The information needed for the user to provide the details that are needed to provide how to connect to the
     Cloud provider
    """
    storage_type: CloudStorageType
    auth_method: AuthMethod
    connection_name: Optional[str] = "None"  # This is the reference to the item we will fetch that contains the data


class FullCloudStorageConnection(AuthSettingsInput):
    """Internal model with decrypted secrets"""

    # AWS S3
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[SecretStr] = None
    aws_role_arn: Optional[str] = None
    aws_allow_unsafe_html: Optional[bool] = None

    # Azure ADLS
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[SecretStr] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[SecretStr] = None

    # Common
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True


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
    # CSV specific options
    csv_has_header: Optional[bool] = True
    csv_delimiter: Optional[str] = ","
    csv_encoding: Optional[str] = "utf8"
    # Deltalake specific settings
    delta_version: Optional[int] = None


class CloudStorageReadSettingsInternal(BaseModel):
    read_settings: CloudStorageReadSettings
    connection: FullCloudStorageConnection


class CloudStorageWriteSettings(CloudStorageSettings):
    """Settings for writing to cloud storage"""

    write_mode: Literal["overwrite", "append"] = "overwrite"
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet"

    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy"

    csv_delimiter: str = ","
    csv_encoding: str = "utf8"


class CloudStorageWriteSettingsInternal(BaseModel):
    write_settings: CloudStorageWriteSettings
    connection: FullCloudStorageConnection
