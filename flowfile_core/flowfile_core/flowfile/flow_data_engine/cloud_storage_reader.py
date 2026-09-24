"""Cloud storage reader helpers for FlowDataEngine.

Provides the CloudStorageReader class that translates FullCloudStorageConnection
objects into storage_options dicts and credential providers, delegating the
actual options building to shared.cloud_storage.storage_options.

Importing this module registers core's secret decryptor, so plans carrying an
``EncryptedCredentialProvider`` can execute in core (local runs, previews).
"""

import json
from typing import Any

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.secret_manager.secret_manager import _encrypt_with_master_key, decrypt_secret, encrypt_secret
from shared.cloud_credential_provider import (
    EncryptedCredentialProvider,
    register_secret_decryptor,
    split_credentials,
)

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
                connection.aws_secret_access_key.get_secret_value() if connection.aws_secret_access_key else None
            ),
            aws_role_arn=connection.aws_role_arn,
            aws_allow_unsafe_html=connection.aws_allow_unsafe_html,
            aws_profile=connection.aws_profile,
            azure_account_name=connection.azure_account_name,
            azure_account_key=(
                connection.azure_account_key.get_secret_value() if connection.azure_account_key else None
            ),
            azure_tenant_id=connection.azure_tenant_id,
            azure_client_id=connection.azure_client_id,
            azure_client_secret=(
                connection.azure_client_secret.get_secret_value() if connection.azure_client_secret else None
            ),
            azure_sas_token=(connection.azure_sas_token.get_secret_value() if connection.azure_sas_token else None),
            gcs_service_account_key=(
                connection.gcs_service_account_key.get_secret_value() if connection.gcs_service_account_key else None
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
    def get_credential_provider(
        storage_options: dict[str, Any] | None, user_id: int | None
    ) -> EncryptedCredentialProvider | None:
        """A provider carrying the credentials in *storage_options* encrypted, or None when there are none.

        The credentials are re-encrypted under *user_id* (the principal running the flow); without one
        they use the legacy master-key format, which core and the worker also decrypt.
        """
        if not storage_options:
            return None
        credentials = split_credentials(storage_options)[1]
        if not credentials:
            return None
        payload = json.dumps(credentials)
        encrypted = encrypt_secret(payload, user_id) if user_id is not None else _encrypt_with_master_key(payload)
        return EncryptedCredentialProvider(encrypted)

    @staticmethod
    def get_scan_kwargs(
        storage_options: dict[str, Any] | None, credential_provider: EncryptedCredentialProvider | None
    ) -> dict[str, Any]:
        """``storage_options``/``credential_provider`` kwargs for a lazy scan, with no credential in the options."""
        kwargs: dict[str, Any] = {}
        if storage_options:
            kwargs["storage_options"] = (
                split_credentials(storage_options)[0] if credential_provider is not None else storage_options
            )
        if credential_provider is not None:
            kwargs["credential_provider"] = credential_provider
        return kwargs

    @staticmethod
    def get_secure_scan_kwargs(storage_options: dict[str, Any] | None, user_id: int | None) -> dict[str, Any]:
        """Scan kwargs for a plan that may be serialized: the credentials travel only encrypted.

        Polars inlines ``storage_options`` into a serialized plan, and core ships plans to the worker.
        """
        return CloudStorageReader.get_scan_kwargs(
            storage_options, CloudStorageReader.get_credential_provider(storage_options, user_id)
        )


def _decrypt_plan_credentials(encrypted: str) -> str:
    return decrypt_secret(encrypted).get_secret_value()


register_secret_decryptor(_decrypt_plan_credentials)
