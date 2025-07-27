"""Cloud storage connection schemas for S3, ADLS, and other cloud providers."""

from typing import Optional, Literal, Dict, Any
import boto3
from pydantic import BaseModel, SecretStr
from flowfile_worker.secrets import decrypt_secret

CloudStorageType = Literal["s3", "adls", "gcs"]
AuthMethod = Literal["access_key", "iam_role", "service_principal", "managed_identity", "sas_token", "aws-cli", "env_vars"]


def create_storage_options_from_boto_credentials(profile_name: Optional[str],
                                                 region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a storage options dictionary from AWS credentials using a boto3 profile.
    This is the most robust way to handle profile-based authentication as it
    bypasses Polars' internal credential provider chain, avoiding conflicts.

    Parameters
    ----------
    profile_name
        The name of the AWS profile in ~/.aws/credentials.
    region_name
        The AWS region to use.

    Returns
    -------
    Dict[str, Any]
        A storage options dictionary for Polars with explicit credentials.
    """
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    credentials = session.get_credentials()
    frozen_creds = credentials.get_frozen_credentials()

    storage_options = {
        "aws_access_key_id": frozen_creds.access_key,
        "aws_secret_access_key": frozen_creds.secret_key,
        "aws_session_token": frozen_creds.token,
    }
    # Use the session's region if one was resolved, otherwise use the provided one
    if session.region_name:
        storage_options["aws_region"] = session.region_name

    print("Boto3: Successfully created storage options with explicit credentials.")
    return storage_options


class FullCloudStorageConnection(BaseModel):
    """Internal model with decrypted secrets"""
    storage_type: CloudStorageType
    auth_method: AuthMethod
    connection_name: Optional[str] = "None"  # This is the reference to the item we will fetch that contains the data

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

    def get_storage_options(self) -> Dict[str, Any]:
        """
        Build storage options dict based on the connection type and auth method.

        Returns:
            Dict containing appropriate storage options for the provider
        """
        if self.storage_type == "s3":
            return self._get_s3_storage_options()

    def _get_s3_storage_options(self) -> Dict[str, Any]:
        """Build S3-specific storage options."""
        auth_method = self.auth_method
        print(f"Building S3 storage options for auth_method: '{auth_method}'")

        if auth_method == "aws-cli":
            return create_storage_options_from_boto_credentials(
                profile_name=self.connection_name,
                region_name=self.aws_region
            )

        storage_options = {}
        if self.aws_region:
            storage_options["aws_region"] = self.aws_region
        if self.endpoint_url:
            storage_options["endpoint_url"] = self.endpoint_url
        if not self.verify_ssl:
            storage_options["verify"] = "False"
        if self.aws_allow_unsafe_html:  # Note: Polars uses aws_allow_http
            storage_options["aws_allow_http"] = "true"

        if auth_method == "access_key":
            storage_options["aws_access_key_id"] = self.aws_access_key_id
            storage_options["aws_secret_access_key"] = decrypt_secret(
                self.aws_secret_access_key.get_secret_value()).get_secret_value()
            # Explicitly clear any session token from the environment
            storage_options["aws_session_token"] = ""

        elif auth_method == "iam_role":
            # Correctly implement IAM role assumption using boto3 STS client.
            sts_client = boto3.client('sts', region_name=self.aws_region)
            assumed_role_object = sts_client.assume_role(
                RoleArn=self.aws_role_arn,
                RoleSessionName="PolarsCloudStorageReaderSession"  # A descriptive session name
            )
            credentials = assumed_role_object['Credentials']
            storage_options["aws_access_key_id"] = credentials['AccessKeyId']
            storage_options["aws_secret_access_key"] = decrypt_secret(credentials['SecretAccessKey']).get_secret_value()
            storage_options["aws_session_token"] = decrypt_secret(credentials['SessionToken']).get_secret_value()

        return storage_options


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
