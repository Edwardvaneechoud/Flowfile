"""Cloud storage reader helpers for FlowDataEngine.

Provides the CloudStorageReader class that translates FullCloudStorageConnection
objects into storage_options dicts and credential providers, delegating the
actual options building to shared.cloud_storage.storage_options. With the compute
protocol flag on, a read of a stored connection names it by reference instead, and
whichever process runs the plan resolves it.

Importing it registers core's secret decryptor, so provider-carrying plans can execute in core.
"""

import json
from typing import Any

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.protocol import CredentialRef
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.secret_manager.secret_manager import _encrypt_with_master_key, decrypt_secret, encrypt_secret
from shared.cloud_credential_provider import (
    CredentialRefProvider,
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
            aws_session_token=(
                connection.aws_session_token.get_secret_value() if connection.aws_session_token else None
            ),
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

        Encrypted under *user_id*, else in the legacy master-key format the worker also decrypts.
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
    def credential_ref(connection: FullCloudStorageConnection, user_id: int | None) -> CredentialRef | None:
        """The catalog's reference to *connection* as resolved for *user_id*, or None for one the catalog does not hold.

        Looked up by the rule the connection was resolved by: the user's own first, then the lowest-id group grant.
        """
        if user_id is None or not connection.connection_name:
            return None
        from flowfile_core.database.connection import get_db_context
        from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection

        with get_db_context() as db:
            row = get_cloud_connection(db, connection.connection_name, user_id)
            return CredentialRef(connection_id=row.id, user_id=user_id) if row is not None else None

    @staticmethod
    def get_plan_storage_options(connection: FullCloudStorageConnection) -> dict[str, str]:
        """The options a read's plan carries when the executor resolves its credentials: no secret, nothing resolved.

        The connection's region, endpoint, plain-HTTP, TLS, account and project settings; S3 is built as ``env_vars``,
        whose branch resolves no credential, and the Azure and GCS builders add a credential only when given one. A
        GCS endpoint is reached through object_store's ``gcs_base_url``, which only a service-account key carries: a
        key with no key material and OAuth off, so the plan sends the provider's bearer or, with none, no token.
        """
        if CloudStorageReader.use_pyarrow_for_gcs(connection):
            stub = {
                "gcs_base_url": connection.endpoint_url,
                "disable_oauth": True,
                "client_email": "",
                "private_key": "",
                "private_key_id": "",
            }
            return {"service_account_key": json.dumps(stub)}
        return build_storage_options(
            storage_type=connection.storage_type,
            auth_method="env_vars" if connection.storage_type == "s3" else connection.auth_method,
            connection_name=connection.connection_name,
            aws_region=connection.aws_region,
            aws_allow_unsafe_html=connection.aws_allow_unsafe_html,
            azure_account_name=connection.azure_account_name,
            gcs_project_id=connection.gcs_project_id,
            endpoint_url=connection.endpoint_url,
            verify_ssl=connection.verify_ssl,
        )

    @staticmethod
    def get_read_credentials(
        connection: FullCloudStorageConnection, user_id: int | None
    ) -> tuple[dict[str, Any], EncryptedCredentialProvider | CredentialRefProvider | None]:
        """The storage options and credential provider of a read's plan.

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, a Polars scan of a stored connection whose method holds a credential
        carries a ``CredentialRefProvider`` and options with no secret, so whichever process executes the plan resolves
        the connection; ``env_vars`` and managed identity carry no provider, and a named connection the catalog does
        not hold for *user_id* is refused, never resolved in core. Otherwise, the flag off or a read with no saved
        connection, core resolves the options and the plan carries their credentials encrypted.

        Raises:
            ValueError: With the flag on, the connection is named but not stored for *user_id*, or *user_id* is None.
        """
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            if connection.auth_method in ("env_vars", "managed_identity"):
                return CloudStorageReader.get_plan_storage_options(connection), None
            ref = CloudStorageReader.credential_ref(connection, user_id)
            if ref is not None:
                provider = CredentialRefProvider(ref.connection_id, ref.user_id)
                return CloudStorageReader.get_plan_storage_options(connection), provider
            if connection.connection_name:
                reason = "is read with no user" if user_id is None else f"is not stored for user {user_id}"
                raise ValueError(
                    f'Cloud connection "{connection.connection_name}" {reason}: '
                    "a read names a connection stored for the user it runs as"
                )
        storage_options = CloudStorageReader.get_storage_options(connection)
        return storage_options, CloudStorageReader.get_credential_provider(storage_options, user_id)

    @staticmethod
    def get_scan_kwargs(
        storage_options: dict[str, Any] | None,
        credential_provider: EncryptedCredentialProvider | CredentialRefProvider | None,
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
