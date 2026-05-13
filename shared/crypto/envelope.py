"""Cryptographic envelope for Flowfile secrets.

Owns the single source of truth for how secrets are derived, encrypted, and
parsed across ``flowfile_core`` and ``flowfile_worker``. Both services fetch
the master key through their own backend, then delegate the actual crypto
operations to this module.

Envelope format v1: ``$ffsec$1$<user_id>$<fernet_token>``

The user_id is embedded so a downstream consumer (e.g. the worker) can decrypt
without separately being told who the secret belongs to. The leading
``$ffsec$1$`` prefix reserves the version digit for forward-compatible envelope
changes — a future v2 envelope can be detected by the same dispatch.

Changing ``KEY_DERIVATION_VERSION`` invalidates every existing secret because
it is the HKDF salt. Do not edit without a migration path.
"""

import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from pydantic import SecretStr

KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"

SECRET_FORMAT_PREFIX = "$ffsec$1$"


def derive_user_key(master_key: bytes, user_id: int) -> bytes:
    """Derive a per-user 32-byte Fernet key from the master key using HKDF.

    Deterministic: the same ``master_key`` + ``user_id`` always produces the
    same key, which is what lets secrets remain decryptable across process
    restarts without persisting per-user keys.
    """
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=KEY_DERIVATION_VERSION,
        info=f"user-{user_id}".encode(),
    )
    derived = hkdf.derive(master_key)
    return base64.urlsafe_b64encode(derived)


def parse_v1_envelope(encrypted_value: str) -> tuple[int, str]:
    """Split a v1 envelope into ``(user_id, fernet_token)``.

    Raises ``ValueError`` if ``encrypted_value`` isn't a well-formed v1 envelope.
    """
    if not encrypted_value.startswith(SECRET_FORMAT_PREFIX):
        raise ValueError("Not a v1 envelope")
    remainder = encrypted_value[len(SECRET_FORMAT_PREFIX) :]
    parts = remainder.split("$", 1)
    if len(parts) != 2:
        raise ValueError("Invalid encrypted secret format")
    try:
        user_id = int(parts[0])
    except ValueError as e:
        raise ValueError("Invalid encrypted secret format") from e
    return user_id, parts[1]


def encrypt_secret_envelope(master_key: bytes, plaintext: str, user_id: int) -> str:
    """Encrypt ``plaintext`` as a v1 envelope: ``$ffsec$1$<user_id>$<token>``."""
    key = derive_user_key(master_key, user_id)
    token = Fernet(key).encrypt(plaintext.encode()).decode()
    return f"{SECRET_FORMAT_PREFIX}{user_id}${token}"


def decrypt_secret_envelope(
    master_key: bytes,
    encrypted_value: str,
    user_id: int | None = None,
) -> SecretStr:
    """Decrypt a v1 envelope or a legacy raw Fernet token.

    For v1 envelopes (``$ffsec$1$...``), ``user_id`` is extracted from the blob
    and the caller-supplied ``user_id`` argument is ignored.

    For legacy raw tokens (no envelope prefix), the optional ``user_id`` selects
    a derived key; passing ``None`` falls back to decrypting with the master key
    directly — which is how the earliest version of the secret store encrypted.
    """
    if encrypted_value.startswith(SECRET_FORMAT_PREFIX):
        embedded_user_id, token = parse_v1_envelope(encrypted_value)
        key = derive_user_key(master_key, embedded_user_id)
        return SecretStr(Fernet(key).decrypt(token.encode()).decode())

    if user_id is not None:
        key = derive_user_key(master_key, user_id)
        return SecretStr(Fernet(key).decrypt(encrypted_value.encode()).decode())

    return SecretStr(Fernet(master_key).decrypt(encrypted_value.encode()).decode())
