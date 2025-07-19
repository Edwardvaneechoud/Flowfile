import boto3

from typing import Dict, Optional, Any, Callable
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection


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
        storage_options = {}
        if not connection.aws_region:
            try:
                connection.aws_region = boto3.Session().region_name
            except Exception:
                pass

        if connection.auth_method == "access_key":
            if connection.aws_access_key_id:
                storage_options["aws_access_key_id"] = connection.aws_access_key_id
            if connection.aws_secret_access_key:
                storage_options["aws_secret_access_key"] = connection.aws_secret_access_key.get_secret_value()
            if connection.aws_region:
                storage_options["aws_region"] = connection.aws_region

        elif connection.auth_method == "iam_role":
            # IAM role authentication
            if connection.aws_role_arn:
                # For IAM role, we might need to use a credential provider
                # This will be handled by the credential_provider parameter
                pass
        else:
            storage_options['aws_region'] = connection.aws_region

        # Add endpoint URL if provided (for S3-compatible services)
        if connection.endpoint_url:
            storage_options["endpoint_url"] = connection.endpoint_url

        # SSL verification
        if not connection.verify_ssl:
            storage_options["verify"] = False

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

        elif connection.storage_type == "s3" and connection.auth_method == "aws-cli":
            # Use AWS CLI credentials
            # Polars should automatically pick these up, so we don't need a custom provider
            return None
        return None
