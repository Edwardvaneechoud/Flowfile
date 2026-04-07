"""Cloud storage connection schemas for S3, ADLS, and other cloud providers."""

from typing import Any, Literal

from pydantic import BaseModel, SecretStr

from flowfile_worker.secrets import decrypt_secret
from shared.cloud_storage.gcs import use_pyarrow_for_gcs as _use_pyarrow_for_gcs
from shared.cloud_storage.storage_options import build_storage_options

CloudStorageType = Literal["s3", "adls", "gcs"]
AuthMethod = Literal[
    "access_key",
    "iam_role",
    "service_principal",
    "managed_identity",
    "sas_token",
    "aws-cli",
    "env_vars",
    "service_account",
]


class FullCloudStorageConnection(BaseModel):
    """Internal model with decrypted secrets"""

    storage_type: CloudStorageType
    auth_method: AuthMethod
    connection_name: str | None = "None"  # This is the reference to the item we will fetch that contains the data

    # AWS S3
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_role_arn: str | None = None
    aws_allow_unsafe_html: bool | None = None

    # Azure ADLS
    azure_account_name: str | None = None
    azure_account_key: SecretStr | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: SecretStr | None = None
    azure_sas_token: SecretStr | None = None

    # Google Cloud Storage
    gcs_service_account_key: SecretStr | None = None
    gcs_project_id: str | None = None

    # Common
    endpoint_url: str | None = None
    verify_ssl: bool = True

    def _extract_secret(self, field: SecretStr | None) -> str | None:
        """Extract and decrypt a secret value."""
        if field is None:
            return None
        return decrypt_secret(field.get_secret_value()).get_secret_value()

    def get_storage_options(self) -> dict[str, Any]:
        """Build storage options dict, decrypting secrets as needed."""
        return build_storage_options(
            storage_type=self.storage_type,
            auth_method=self.auth_method,
            connection_name=self.connection_name,
            aws_region=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self._extract_secret(self.aws_secret_access_key),
            aws_role_arn=self.aws_role_arn,
            aws_allow_unsafe_html=self.aws_allow_unsafe_html,
            azure_account_name=self.azure_account_name,
            azure_account_key=self._extract_secret(self.azure_account_key),
            azure_tenant_id=self.azure_tenant_id,
            azure_client_id=self.azure_client_id,
            azure_client_secret=self._extract_secret(self.azure_client_secret),
            azure_sas_token=self._extract_secret(self.azure_sas_token),
            gcs_service_account_key=self._extract_secret(self.gcs_service_account_key),
            gcs_project_id=self.gcs_project_id,
            endpoint_url=self.endpoint_url,
            verify_ssl=self.verify_ssl,
        )

    def should_use_pyarrow_for_gcs(self) -> bool:
        """Whether to use PyArrow/gcsfs backend for GCS writes."""
        return _use_pyarrow_for_gcs(self.storage_type, self.endpoint_url)


class WriteSettings(BaseModel):
    """Settings for writing to cloud storage"""

    resource_path: str  # s3://bucket/path/to/file.csv

    write_mode: Literal["overwrite", "append"] = "overwrite"
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet"

    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy"

    csv_delimiter: str = ","
    csv_encoding: str = "utf8"


class CloudStorageWriteSettings(BaseModel):
    write_settings: WriteSettings
    connection: FullCloudStorageConnection
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
