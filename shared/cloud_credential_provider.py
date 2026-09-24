"""Keep decrypted cloud credentials out of serialized Polars query plans.

Polars inlines ``storage_options`` into a serialized plan; ``EncryptedCredentialProvider`` carries the
credentials as a ``$ffsec$`` ciphertext instead and decrypts through whatever the executing process
registered with :func:`register_secret_decryptor`. Stdlib-only and outside ``shared.cloud_storage``
(whose ``__init__`` loads ``shared.delta_utils``): every spawned worker child imports this module.
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
