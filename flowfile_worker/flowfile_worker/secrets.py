"""
Simplified secure storage module for FlowFile worker to read credentials and secrets.

The crypto envelope itself lives in :mod:`shared.crypto.envelope` so the
worker and core agree byte-for-byte on encoding. This module owns the
worker-side master-key lookup (Docker secret / env / local file) and re-exports
the shared envelope functions for backward compatibility with existing callers
and tests.
"""

import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet
from pydantic import SecretStr

from flowfile_worker.configs import TEST_MODE
from shared.crypto.envelope import (
    KEY_DERIVATION_VERSION,
    SECRET_FORMAT_PREFIX,
    decrypt_secret_envelope,
    encrypt_secret_envelope,
)
from shared.crypto.envelope import (
    derive_user_key as _derive_user_key,
)

logger = logging.getLogger(__name__)

__all__ = [
    "KEY_DERIVATION_VERSION",
    "SECRET_FORMAT_PREFIX",
    "SecureStorage",
    "decrypt_secret",
    "derive_user_key",
    "encrypt_secret",
    "get_docker_secret_key",
    "get_master_key",
    "get_password",
]


class SecureStorage:
    """A secure local storage mechanism for reading secrets using Fernet encryption."""

    def __init__(self):
        app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
        self.storage_path = Path(app_data) / "flowfile"
        logger.debug(f"Using storage path: {self.storage_path}")
        self.key_path = self.storage_path / ".secret_key"

    def _get_store_path(self, service_name):
        """Get the path to the encrypted store file for a service."""
        return self.storage_path / f"{service_name}.json.enc"

    def _read_store(self, service_name):
        """Read and decrypt the store file for a service."""
        path = self._get_store_path(service_name)
        if not path.exists():
            return {}

        try:
            with open(self.key_path, "rb") as f:
                key = f.read()
            with open(path, "rb") as f:
                data = f.read()

            return json.loads(Fernet(key).decrypt(data).decode())
        except Exception as e:
            logger.debug(f"Error reading from encrypted store: {e}")
            return {}

    def get_password(self, service_name, username):
        """Retrieve a password from secure storage."""
        store = self._read_store(service_name)
        return store.get(username)


_storage = SecureStorage()

# Cache for the auto-generated test master key. We generate one per-process when
# TEST_MODE is on and no explicit key is supplied via env, so an encrypt/decrypt
# round-trip in a single test process stays consistent without committing a key
# to source control.
_TEST_MASTER_KEY: str | None = None


def get_password(service_name, username):
    """
    Retrieve a password from secure storage.

    Args:
        service_name: The name of the service
        username: The username or key

    Returns:
        The stored password or None if not found
    """
    return _storage.get_password(service_name, username)


def get_docker_secret_key() -> str | None:
    """
    Get the master key from Docker secret or environment variable.

    Returns:
        str: The master key if successfully read, None if not configured.

    Raises:
        RuntimeError: If the secret file exists but cannot be read, or key is invalid.
    """
    env_key = os.environ.get("FLOWFILE_MASTER_KEY")
    if env_key:
        try:
            Fernet(env_key.encode())
            return env_key
        except Exception:
            logger.error("FLOWFILE_MASTER_KEY environment variable is not a valid Fernet key")
            raise RuntimeError("FLOWFILE_MASTER_KEY is not a valid Fernet key") from None

    secret_path = "/run/secrets/flowfile_master_key"
    if os.path.exists(secret_path):
        try:
            with open(secret_path) as f:
                key = f.read().strip()
                Fernet(key.encode())
                return key
        except Exception as e:
            logger.error(f"Failed to read or validate master key from Docker secret: {e}")
            raise RuntimeError("Failed to read master key from Docker secret") from e

    return None


def get_master_key() -> str:
    """
    Get the master encryption key.

    Resolution order:
    1. If ``TEST_MODE`` is on, prefer ``FLOWFILE_MASTER_KEY`` (so core+worker
       integration tests can share a key); otherwise generate a fresh per-process
       key so worker-only round-trip tests have a usable key without storing a
       value in source.
    2. If running in Docker, read the key from the Docker secret or env var.
    3. Otherwise read the key from the local SecureStorage.

    Returns:
        str: The master encryption key

    Raises:
        RuntimeError: If in Docker mode and no key is configured.
        ValueError: If the master key is not found in storage.
    """
    if TEST_MODE:
        explicit = os.environ.get("FLOWFILE_MASTER_KEY")
        if explicit:
            return explicit
        global _TEST_MASTER_KEY
        if _TEST_MASTER_KEY is None:
            _TEST_MASTER_KEY = Fernet.generate_key().decode()
        return _TEST_MASTER_KEY

    if os.environ.get("FLOWFILE_MODE") == "docker":
        key = get_docker_secret_key()
        if key is None:
            raise RuntimeError(
                "Master key not configured. Set FLOWFILE_MASTER_KEY environment variable "
                "or mount the flowfile_master_key Docker secret."
            )
        return key

    key = get_password("flowfile", "master_key")
    if not key:
        raise ValueError("Master key not found in storage.")
    return key


def derive_user_key(user_id: int) -> bytes:
    """Derive a per-user Fernet key from the current master key."""
    return _derive_user_key(get_master_key().encode(), user_id)


def decrypt_secret(encrypted_value: str) -> SecretStr:
    """Decrypt a v1 envelope (``$ffsec$1$...``) or a legacy raw Fernet token."""
    return decrypt_secret_envelope(get_master_key().encode(), encrypted_value)


def encrypt_secret(secret_value: str, user_id: int | None = None) -> str:
    """Encrypt a secret value.

    With ``user_id`` set, emits a v1 envelope (``$ffsec$1$<user_id>$<token>``).
    Without ``user_id``, emits a legacy raw Fernet token encrypted with the
    master key — kept for backward compatibility with existing worker tests.
    """
    master_key = get_master_key().encode()
    if user_id is not None:
        return encrypt_secret_envelope(master_key, secret_value, user_id)
    return Fernet(master_key).encrypt(secret_value.encode()).decode()
