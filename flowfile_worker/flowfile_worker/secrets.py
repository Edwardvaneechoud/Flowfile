"""
Simplified secure storage module for FlowFile worker to read credentials and secrets.
"""

import base64
import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from pydantic import SecretStr

from flowfile_worker.configs import TEST_MODE

# Set up logging
logger = logging.getLogger(__name__)

# Version identifier for key derivation scheme (must match flowfile_core)
KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"

# Encrypted secret format: $ffsec$1${user_id}${fernet_token}
SECRET_FORMAT_PREFIX = "$ffsec$1$"


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
    # First, check for environment variable (allows runtime configuration)
    env_key = os.environ.get("FLOWFILE_MASTER_KEY")
    if env_key:
        # Validate it's a proper Fernet key
        try:
            Fernet(env_key.encode())
            return env_key
        except Exception:
            logger.error("FLOWFILE_MASTER_KEY environment variable is not a valid Fernet key")
            raise RuntimeError("FLOWFILE_MASTER_KEY is not a valid Fernet key")

    # Then, check for Docker secret file
    secret_path = "/run/secrets/flowfile_master_key"
    if os.path.exists(secret_path):
        try:
            with open(secret_path) as f:
                key = f.read().strip()
                # Validate the key
                Fernet(key.encode())
                return key
        except Exception as e:
            logger.error(f"Failed to read or validate master key from Docker secret: {e}")
            raise RuntimeError("Failed to read master key from Docker secret")

    # No key configured
    return None


def get_master_key() -> str:
    """
    Get the master encryption key.

    If in TEST_MODE, returns a test key.
    If running in Docker, retrieves the key from Docker secrets or environment.
    Otherwise, retrieves the key from secure storage.

    Returns:
        str: The master encryption key

    Raises:
        RuntimeError: If in Docker mode and no key is configured.
        ValueError: If the master key is not found in storage.
    """
    # First check for test mode
    if TEST_MODE:
        return b"06t640eu3AG2FmglZS0n0zrEdqadoT7lYDwgSmKyxE4=".decode()

    # Next check if running in Docker
    if os.environ.get("FLOWFILE_MODE") == "docker":
        key = get_docker_secret_key()
        if key is None:
            raise RuntimeError(
                "Master key not configured. Set FLOWFILE_MASTER_KEY environment variable "
                "or mount the flowfile_master_key Docker secret."
            )
        return key

    # Otherwise read from local storage
    key = get_password("flowfile", "master_key")
    if not key:
        raise ValueError("Master key not found in storage.")
    return key


def derive_user_key(user_id: int) -> bytes:
    """
    Derive a user-specific encryption key from the master key using HKDF.

    This provides cryptographic isolation between users - each user's secrets
    are encrypted with a unique key derived from the master key.

    Args:
        user_id: The unique identifier for the user

    Returns:
        bytes: A 32-byte URL-safe base64-encoded key suitable for Fernet
    """
    master_key = get_master_key().encode()

    # Use HKDF to derive a user-specific key
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,  # Fernet requires 32 bytes
        salt=KEY_DERIVATION_VERSION,  # Static salt is fine for key derivation
        info=f"user-{user_id}".encode(),  # User-specific context
    )

    # Derive raw key material and encode for Fernet
    derived_key = hkdf.derive(master_key)
    return base64.urlsafe_b64encode(derived_key)


def decrypt_secret(encrypted_value: str) -> SecretStr:
    """
    Decrypt an encrypted value.

    Supports both new format (with embedded user_id) and legacy format.
    - New format: $ffsec$1${user_id}${fernet_token} - user_id extracted automatically
    - Legacy format: raw Fernet token - uses master key directly

    Args:
        encrypted_value: The encrypted value as a string

    Returns:
        SecretStr: The decrypted value as a SecretStr
    """
    # Check for new versioned format with embedded user_id
    if encrypted_value.startswith(SECRET_FORMAT_PREFIX):
        # Parse: $ffsec$1${user_id}${fernet_token}
        remainder = encrypted_value[len(SECRET_FORMAT_PREFIX):]
        parts = remainder.split("$", 1)
        if len(parts) != 2:
            raise ValueError("Invalid encrypted secret format")

        embedded_user_id = int(parts[0])
        fernet_token = parts[1]

        key = derive_user_key(embedded_user_id)
        f = Fernet(key)
        return SecretStr(f.decrypt(fernet_token.encode()).decode())

    # Legacy format - use master key directly
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def encrypt_secret(secret_value: str, user_id: int | None = None) -> str:
    """
    Encrypt a secret value.

    If user_id is provided, uses per-user key derivation with embedded user_id format.
    Otherwise, uses legacy master key encryption (for backward compatibility in tests).

    Args:
        secret_value: The secret value to encrypt
        user_id: Optional user ID for per-user key derivation

    Returns:
        str: The encrypted value as a string
    """
    if user_id is not None:
        key = derive_user_key(user_id)
        f = Fernet(key)
        fernet_token = f.encrypt(secret_value.encode()).decode()
        return f"{SECRET_FORMAT_PREFIX}{user_id}${fernet_token}"

    # Legacy format for backward compatibility
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()
