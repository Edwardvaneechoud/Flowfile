"""Keep decrypted cloud credentials out of serialized Polars query plans.

Polars inlines ``storage_options`` into a LazyFrame's serialized plan. A cloud scan built in core
with decrypted credentials would therefore carry them in plaintext wherever the plan goes: the
worker, logs, caches, stored virtual-table plans. ``EncryptedCredentialProvider`` carries the
credentials only as a ``$ffsec$`` ciphertext and hands them to Polars through its
``credential_provider`` hook, decrypting on the side that executes the plan.

Core and the worker resolve the master key independently, so each process that executes plans
registers its own decryptor with :func:`register_secret_decryptor`. The decryption logic itself
stays in core's secret manager and the worker's ``secrets`` module; nothing here knows the key.

Polars accepts provider credentials only for S3 (access key, secret, session token) and Azure
(``account_key``, ``sas_token``, ``bearer_token``). Other secrets, such as an Azure client secret or a
GCS service-account key, stay in the storage options.

Stdlib-only and deliberately outside ``shared.cloud_storage``: that package's ``__init__`` loads
``shared.delta_utils``, and every spawned worker child imports this module to register its decryptor.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
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
    """A Polars ``credential_provider`` whose only state is the encrypted credentials.

    Pickles, and therefore serializes into a plan, as the ciphertext alone. Polars calls it when
    the plan executes; the decrypted credentials then live only in that process's memory.
    """

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
