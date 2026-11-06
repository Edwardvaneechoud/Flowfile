"""Keep decrypted cloud credentials out of serialized Polars query plans.

Polars inlines ``storage_options`` into a serialized plan, so a plan carries its credentials through a provider
instead. ``EncryptedCredentialProvider`` holds a ``$ffsec$`` ciphertext and decrypts through whatever the executing
process registered with :func:`register_secret_decryptor`. ``CredentialRefProvider`` holds only a stored
connection's id and user, resolved against the executing process's own catalog. Stdlib-only at import and outside
``shared.cloud_storage`` (whose ``__init__`` loads ``shared.delta_utils``): every spawned worker child imports this
module.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from datetime import UTC
from typing import Any

PROVIDER_CREDENTIAL_KEYS = frozenset(
    {"aws_access_key_id", "aws_secret_access_key", "aws_session_token", "account_key", "sas_token"}
)

_decrypt: Callable[[str], str] | None = None


class CredentialDecryptionError(RuntimeError):
    """The credentials embedded in a query plan could not be decrypted in this process."""


def register_secret_decryptor(decrypt: Callable[[str], str]) -> None:
    """Set how this process decrypts a ``$ffsec$`` string into plaintext."""
    global _decrypt
    _decrypt = decrypt


def split_credentials(storage_options: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Split storage options into (options safe to serialize, credentials for a provider)."""
    options: dict[str, Any] = {}
    credentials: dict[str, str] = {}
    for key, value in storage_options.items():
        if key in PROVIDER_CREDENTIAL_KEYS and value is not None:
            credentials[key] = value
        else:
            options[key] = value
    return options, credentials


class EncryptedCredentialProvider:
    """A Polars ``credential_provider`` whose only state is the ciphertext; it decrypts where the plan executes."""

    def __init__(self, encrypted_credentials: str):
        self.encrypted_credentials = encrypted_credentials

    def __call__(self) -> tuple[dict[str, str], None]:
        if _decrypt is None:
            raise CredentialDecryptionError(
                "This query plan carries encrypted cloud storage credentials, but no secret decryptor is "
                "registered in this process. Plans that read cloud storage run only in Flowfile core or "
                "the Flowfile worker."
            )
        try:
            credentials = json.loads(_decrypt(self.encrypted_credentials))
        except Exception as e:
            reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            # from None: a chained JSONDecodeError would keep the decrypted text on its .doc attribute.
            raise CredentialDecryptionError(
                "Could not decrypt the cloud storage credentials embedded in this query plan. Core and "
                f"the worker must share the same master key. ({reason})"
            ) from None
        return credentials, None

    def __repr__(self) -> str:
        return "EncryptedCredentialProvider(<encrypted>)"


_AZURE_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"
_GCS_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"


class CredentialResolutionError(RuntimeError):
    """A query plan's cloud connection reference could not be resolved in this process."""


class CredentialRefProvider:
    """A Polars ``credential_provider`` whose only state is a stored connection's id and the user it runs for.

    Called where the plan executes: that process reads the connection from its catalog database, decrypts its
    secrets with its own master key and resolves them (an AWS profile or role, an Azure service principal, a GCS
    service account) into the credentials a Polars scan takes.
    """

    def __init__(self, connection_id: int, user_id: int):
        self.connection_id = connection_id
        self.user_id = user_id
        # Polars caches credentials by the pickled provider; the nonce keeps one entry per plan.
        self.nonce = os.urandom(8).hex()

    def __call__(self) -> tuple[dict[str, str], int | None]:
        return resolve_credential_ref(self.connection_id, self.user_id)

    def __repr__(self) -> str:
        return f"CredentialRefProvider(connection_id={self.connection_id}, user_id={self.user_id})"


def resolve_credential_ref(connection_id: int, user_id: int) -> tuple[dict[str, str], int | None]:
    """The Polars credentials of stored cloud connection *connection_id* and their expiry, resolved in this process.

    Reads the connection from this process's catalog database and decrypts its secrets with this process's master
    key. An ``aws-cli`` profile or ``iam_role`` role therefore resolves with this process's AWS identity.
    """
    from shared.cloud_storage.storage_options import build_storage_options
    from shared.database import cloud_connection_record

    label = f"cloud connection {connection_id} (user {user_id})"
    try:
        record = cloud_connection_record(connection_id)
    except Exception as e:
        raise CredentialResolutionError(
            f"Could not read {label} from the catalog database: {type(e).__name__}"
        ) from None
    if record is None:
        raise CredentialResolutionError(
            f"The catalog database this process reads has no {label}. A worker resolves a plan's cloud "
            "connection against the catalog core writes, so it must use core's catalog database."
        )
    fields, ciphertexts = record
    secrets = {name: _decrypt_stored(value, label) for name, value in ciphertexts.items() if value}
    options = build_storage_options(**fields, **secrets)
    return _provider_credentials(fields["storage_type"], options)


AMBIENT_AUTH_METHODS = frozenset({"env_vars", "managed_identity"})
"""Auth methods whose connection holds no credential: a sink or scan of one takes none and resolves ambiently."""


def credential_ref_kwargs(connection_id: int, user_id: int) -> dict[str, Any]:
    """What a sink or scan of a URI takes for stored connection *connection_id*: its provider and its plain options.

    The provider resolves the credentials where the sink or scan runs. The options (region, endpoint, plain HTTP,
    TLS, account, project) are read here with no secret decrypted, so an S3-compatible store is reached at its own
    endpoint. A connection in ``AMBIENT_AUTH_METHODS`` gets no provider, so the executing process's default chain
    applies. A connection the catalog lacks gets the provider alone, which raises ``CredentialResolutionError`` if
    Polars calls it.

    Raises:
        CredentialResolutionError: The catalog database could not be read.
    """
    from shared.cloud_storage.storage_options import build_storage_options
    from shared.database import cloud_connection_record

    provider = CredentialRefProvider(connection_id, user_id)
    try:
        record = cloud_connection_record(connection_id)
    except Exception as e:
        raise CredentialResolutionError(
            f"Could not read cloud connection {connection_id} (user {user_id}) from the catalog database: "
            f"{type(e).__name__}"
        ) from None
    if record is None:
        return {"credential_provider": provider}
    fields = record[0]
    storage_type = fields["storage_type"]
    kwargs: dict[str, Any] = {}
    if fields["auth_method"] not in AMBIENT_AUTH_METHODS:
        kwargs["credential_provider"] = provider
    kwargs["storage_options"] = build_storage_options(
        storage_type=storage_type,
        auth_method="env_vars" if storage_type == "s3" else fields["auth_method"],
        connection_name=fields["connection_name"],
        aws_region=fields["aws_region"],
        aws_allow_unsafe_html=fields["aws_allow_unsafe_html"],
        azure_account_name=fields["azure_account_name"],
        gcs_project_id=fields["gcs_project_id"],
        endpoint_url=fields["endpoint_url"],
        verify_ssl=fields["verify_ssl"],
    )
    return kwargs


def _decrypt_stored(ciphertext: str, label: str) -> str:
    if _decrypt is None:
        raise CredentialDecryptionError(f"No secret decryptor is registered in this process to resolve {label}.")
    try:
        return _decrypt(ciphertext)
    except Exception as e:
        reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        raise CredentialDecryptionError(
            f"Could not decrypt the secrets of {label}. Core and the worker must share the same master key. ({reason})"
        ) from None


def _provider_credentials(storage_type: str, options: Mapping[str, str]) -> tuple[dict[str, str], int | None]:
    """What a Polars provider may return for resolved *options*: S3 keys, an Azure key, SAS or bearer, a GCS bearer."""
    if storage_type == "s3":
        keys = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")
        # Keep the builder's "" session token: it keeps an ambient AWS_SESSION_TOKEN out.
        return {key: options[key] for key in keys if options.get(key) is not None}, None
    if storage_type == "adls":
        if options.get("client_secret"):
            return _azure_bearer_token(options["tenant_id"], options["client_id"], options["client_secret"])
        return {key: options[key] for key in ("account_key", "sas_token") if options.get(key)}, None
    if storage_type == "gcs" and options.get("token") not in (None, "anon"):
        return _gcs_bearer_token(options["token"])
    return {}, None


def _azure_bearer_token(tenant_id: str, client_id: str, client_secret: str) -> tuple[dict[str, str], int]:
    """A storage-scoped token for an Azure service principal, by the client-credentials grant object_store makes."""
    import time

    import httpx

    response = httpx.post(
        _AZURE_TOKEN_URL.format(tenant_id=tenant_id),
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": _AZURE_STORAGE_SCOPE,
        },
        timeout=30,
    )
    if response.status_code != 200:
        raise CredentialResolutionError(
            f"Azure rejected the service principal {client_id} of tenant {tenant_id} ({response.status_code})."
        )
    token = response.json()
    return {"bearer_token": token["access_token"]}, int(time.time()) + int(token.get("expires_in", 3600))


def _gcs_bearer_token(service_account_key: str) -> tuple[dict[str, str], int | None]:
    """A read-only token for a GCS service-account key; google-auth answers the expiry as a naive UTC datetime."""

    import google.auth.transport.requests
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_key), scopes=[_GCS_READ_SCOPE]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    if credentials.expiry is None:
        return {"bearer_token": credentials.token}, None
    return {"bearer_token": credentials.token}, int(credentials.expiry.replace(tzinfo=UTC).timestamp())
