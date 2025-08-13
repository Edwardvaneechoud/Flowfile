import boto3
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, Callable, Literal

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection


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


class CloudStorageReader:
    """Helper class to handle different cloud storage authentication methods and read operations."""

    @staticmethod
    def get_storage_options(connection: FullCloudStorageConnection) -> Dict[str, Any]:
        """
        Build storage options dict based on the connection type and auth method.

        Args:
            connection: Full connection details with decrypted secrets

        Returns:
            Dict containing appropriate storage options for the provider
        """
        if connection.storage_type == "s3":
            return CloudStorageReader._get_s3_storage_options(connection)
        elif connection.storage_type == "adls":
            return CloudStorageReader._get_adls_storage_options(connection)
        elif connection.storage_type == "gcs":
            return CloudStorageReader._get_gcs_storage_options(connection)
        else:
            raise ValueError(f"Unsupported storage type: {connection.storage_type}")

    @staticmethod
    def _get_s3_storage_options(connection: 'FullCloudStorageConnection') -> Dict[str, Any]:
        """Build S3-specific storage options."""
        auth_method = connection.auth_method
        if auth_method == "aws-cli":
            return create_storage_options_from_boto_credentials(
                profile_name=connection.connection_name,
                region_name=connection.aws_region
            )

        storage_options = {}
        if connection.aws_region:
            storage_options["aws_region"] = connection.aws_region
        if connection.endpoint_url:
            storage_options["endpoint_url"] = connection.endpoint_url
        if not connection.verify_ssl:
            storage_options["verify"] = "False"
        if connection.aws_allow_unsafe_html: # Note: Polars uses aws_allow_http
            storage_options["aws_allow_http"] = "true"

        if auth_method == "access_key":
            storage_options["aws_access_key_id"] = connection.aws_access_key_id
            storage_options["aws_secret_access_key"] = connection.aws_secret_access_key.get_secret_value()
            # Explicitly clear any session token from the environment
            storage_options["aws_session_token"] = ""

        elif auth_method == "iam_role":
            # Correctly implement IAM role assumption using boto3 STS client.
            sts_client = boto3.client('sts', region_name=connection.aws_region)
            assumed_role_object = sts_client.assume_role(
                RoleArn=connection.aws_role_arn,
                RoleSessionName="PolarsCloudStorageReaderSession" # A descriptive session name
            )
            credentials = assumed_role_object['Credentials']
            storage_options["aws_access_key_id"] = credentials['AccessKeyId']
            storage_options["aws_secret_access_key"] = credentials['SecretAccessKey']
            storage_options["aws_session_token"] = credentials['SessionToken']

        return storage_options

    @staticmethod
    def _get_adls_storage_options(connection: 'FullCloudStorageConnection') -> Dict[str, Any]:
        """Build Azure ADLS-specific storage options."""
        storage_options = {}

        if connection.auth_method == "access_key":
            # Account key authentication
            if connection.azure_account_name:
                storage_options["account_name"] = connection.azure_account_name
            if connection.azure_account_key:
                storage_options["account_key"] = connection.azure_account_key.get_secret_value()

        elif connection.auth_method == "service_principal":
            # Service principal authentication
            if connection.azure_tenant_id:
                storage_options["tenant_id"] = connection.azure_tenant_id
            if connection.azure_client_id:
                storage_options["client_id"] = connection.azure_client_id
            if connection.azure_client_secret:
                storage_options["client_secret"] = connection.azure_client_secret.get_secret_value()

        elif connection.auth_method == "sas_token":
            # SAS token authentication
            if connection.azure_sas_token:
                storage_options["sas_token"] = connection.azure_sas_token.get_secret_value()

        return storage_options

    @staticmethod
    def _get_gcs_storage_options(connection: 'FullCloudStorageConnection') -> Dict[str, Any]:
        """Build GCS-specific storage options."""
        # GCS typically uses service account authentication
        # Implementation would depend on how credentials are stored
        return {}

    @staticmethod
    def get_credential_provider(connection: 'FullCloudStorageConnection') -> Optional[Callable]:
        """
        Get a credential provider function if needed for the authentication method.

        Args:
            connection: Full connection details

        Returns:
            Credential provider function or None
        """
        if connection.storage_type == "s3" and connection.auth_method == "iam_role":
            # For IAM role, create a credential provider
            def aws_credential_provider():
                # This would typically use boto3 to assume the role
                # For now, returning a placeholder
                return {
                    "aws_access_key_id": "...",
                    "aws_secret_access_key": "...",
                    "aws_session_token": "...",
                }, None  # expiry

            return aws_credential_provider
        return None


def get_first_file_from_s3_dir(source: str, storage_options: Dict[str, Any] = None) -> str:
    """
    Get the first parquet file from an S3 directory path.

    Parameters
    ----------
    source : str
        S3 path with wildcards (e.g., 's3://bucket/prefix/**/*/*.parquet')

    storage_options: FullCloudStorageConnection

    Returns
    -------
    str
        S3 URI of the first parquet file found

    Raises
    ------
    ValueError
        If source path is invalid or no parquet files found
    ClientError
        If S3 access fails
    """
    if not source.startswith('s3://'):
        raise ValueError("Source must be a valid S3 URI starting with 's3://'")
    bucket_name, prefix = _parse_s3_path(source)
    file_extension = _get_file_extension(source)
    base_prefix = _remove_wildcards_from_prefix(prefix)
    s3_client = _create_s3_client(storage_options)

    # Get parquet files
    first_file = _get_first_file(s3_client, bucket_name, base_prefix, file_extension)

    # Return first file URI
    return f"s3://{bucket_name}/{first_file['Key']}"


def _get_file_extension(source: str) -> str:
    parts = source.split(".")
    if len(parts) == 1:
        raise ValueError("Source path does not contain a file extension")
    return parts[-1].lower()


def _parse_s3_path(source: str) -> tuple[str, str]:
    """Parse S3 URI into bucket name and prefix."""
    path_parts = source[5:].split('/', 1)  # Remove 's3://'
    bucket_name = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ''
    return bucket_name, prefix


def _remove_wildcards_from_prefix(prefix: str) -> str:
    """Remove wildcard patterns from S3 prefix."""
    return prefix.split('*')[0]


def _create_s3_client(storage_options: Optional[Dict[str, Any]]):
    """Create boto3 S3 client with optional credentials."""
    if storage_options is None:
        return boto3.client('s3')

    # Handle both 'aws_region' and 'region_name' keys
    client_options = storage_options.copy()
    if 'aws_region' in client_options:
        client_options['region_name'] = client_options.pop('aws_region')

    return boto3.client('s3', **{k: v for k, v in client_options.items() if k != "aws_allow_http"})


def _get_first_file(s3_client, bucket_name: str, base_prefix: str, file_extension: str) -> Dict[Any, Any]:
    """List all parquet files in S3 bucket with given prefix."""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=base_prefix)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith(f".{file_extension}"):
                        return obj
            else:
                raise ValueError(f"No objects found in s3://{bucket_name}/{base_prefix}")
        raise ValueError(f"No {file_extension} files found in s3://{bucket_name}/{base_prefix}")
    except ClientError as e:
        raise ValueError(f"Failed to list files in s3://{bucket_name}/{base_prefix}: {e}")


def ensure_path_has_wildcard_pattern(resource_path: str, file_format: Literal["csv", "parquet", "json"]):
    if not resource_path.endswith(f"*.{file_format}"):
        resource_path = resource_path.rstrip("/") + f"/**/*.{file_format}"
    return resource_path
