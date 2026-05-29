"""Shared cryptographic primitives for Flowfile secret storage.

Both ``flowfile_core`` and ``flowfile_worker`` need to encrypt/decrypt secrets
using the same envelope format. This package owns the single source of truth
for that logic — services fetch the master key via their own backend
(``flowfile_core.auth.secrets`` or ``flowfile_worker.secrets``) and then call
into this module for the actual crypto.
"""

from shared.crypto.envelope import (
    KEY_DERIVATION_VERSION,
    SECRET_FORMAT_PREFIX,
    decrypt_secret_envelope,
    derive_user_key,
    encrypt_secret_envelope,
    parse_v1_envelope,
)
from shared.crypto.master_key import normalize_master_key

__all__ = [
    "KEY_DERIVATION_VERSION",
    "SECRET_FORMAT_PREFIX",
    "decrypt_secret_envelope",
    "derive_user_key",
    "encrypt_secret_envelope",
    "normalize_master_key",
    "parse_v1_envelope",
]
