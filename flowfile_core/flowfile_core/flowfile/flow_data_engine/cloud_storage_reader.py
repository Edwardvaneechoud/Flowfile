"""Cloud storage reader helpers for FlowDataEngine.

Provides the CloudStorageReader class that translates FullCloudStorageConnection
objects into storage_options dicts and credential providers, delegating the
actual options building to shared.cloud_storage.storage_options.
"""

from collections.abc import Callable
from typing import Any

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

# Re-export from shared for backward compatibility of callers that import from here
from shared.cloud_storage.directory import get_first_file_from_cloud_dir  # noqa: F401
from shared.cloud_storage.gcs import use_pyarrow_for_gcs as _use_pyarrow_for_gcs
from shared.cloud_storage.storage_options import build_storage_options
from shared.cloud_storage.utils import ensure_path_has_wildcard_pattern  # noqa: F401


class CloudStorageReader:
    """Helper class to handle different cloud storage authentication methods and read operations."""

    @staticmethod
    def get_storage_options(connection: FullCloudStorageConnection) -> dict[str, Any]:
        """Build storage options dict, extracting secrets from SecretStr fields.

        Args:
            connection: Full connection details with decrypted secrets

        Returns:
            Dict containing appropriate storage options for the provider
        """
        return build_storage_options(
            storage_type=connection.storage_type,
            auth_method=connection.auth_method,
            connection_name=connection.connection_name,
            aws_region=connection.aws_region,
            aws_access_key_id=connection.aws_access_key_id,
            aws_secret_access_key=(
                connection.aws_secret_access_key.get_secret_value()
                if connection.aws_secret_access_key else None
            ),
            aws_role_arn=connection.aws_role_arn,
            aws_allow_unsafe_html=connection.aws_allow_unsafe_html,
            azure_account_name=connection.azure_account_name,
            azure_account_key=(
                connection.azure_account_key.get_secret_value()
                if connection.azure_account_key else None
            ),
            azure_tenant_id=connection.azure_tenant_id,
            azure_client_id=connection.azure_client_id,
            azure_client_secret=(
                connection.azure_client_secret.get_secret_value()
                if connection.azure_client_secret else None
            ),
            azure_sas_token=(
                connection.azure_sas_token.get_secret_value()
                if connection.azure_sas_token else None
            ),
            gcs_service_account_key=(
                connection.gcs_service_account_key.get_secret_value()
                if connection.gcs_service_account_key else None
            ),
            gcs_project_id=connection.gcs_project_id,
            endpoint_url=connection.endpoint_url,
            verify_ssl=connection.verify_ssl,
        )

    @staticmethod
    def use_pyarrow_for_gcs(connection: "FullCloudStorageConnection") -> bool:
        """Whether to use PyArrow backend for GCS reads (required for gcsfs/fsspec)."""
        return _use_pyarrow_for_gcs(connection.storage_type, connection.endpoint_url)

    @staticmethod
    def get_credential_provider(connection: "FullCloudStorageConnection") -> Callable | None:
        """Get a credential provider function if needed for the authentication method.

        Args:
            connection: Full connection details

        Returns:
            Credential provider function or None
        """
        if connection.storage_type == "s3" and connection.auth_method == "iam_role":
            def aws_credential_provider():
                return {
                    "aws_access_key_id": "...",
                    "aws_secret_access_key": "...",
                    "aws_session_token": "...",
                }, None  # expiry

            return aws_credential_provider

        return None
