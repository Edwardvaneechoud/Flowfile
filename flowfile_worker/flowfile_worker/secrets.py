"""
Simplified secure storage module for FlowFile worker to read credentials and secrets.
"""
from cryptography.fernet import Fernet
import os
from pathlib import Path
import json
import logging
from pydantic import SecretStr
from flowfile_worker.configs import TEST_MODE

# Set up logging
logger = logging.getLogger(__name__)


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


def get_docker_secret_key():
    """
    Get the master key from Docker secret.

    Returns:
        str: The master key if successfully read from Docker secret.

    Raises:
        RuntimeError: If running in Docker but unable to access the secret.
    """
    secret_path = "/run/secrets/flowfile_master_key"
    if os.path.exists(secret_path):
        try:
            with open(secret_path, "r") as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Failed to read master key from Docker secret: {e}")
            raise RuntimeError("Failed to read master key from Docker secret")
    else:
        logger.critical("Running in Docker but flowfile_master_key secret is not mounted!")
        raise RuntimeError("Docker secret 'flowfile_master_key' is not mounted")


def get_master_key() -> str:
    """
    Get the master encryption key.

    If in TEST_MODE, returns a test key.
    If running in Docker, retrieves the key from Docker secrets.
    Otherwise, retrieves the key from secure storage.

    Returns:
        str: The master encryption key

    Raises:
        ValueError: If the master key is not found in storage.
    """
    # First check for test mode
    if TEST_MODE:
        return b'06t640eu3AG2FmglZS0n0zrEdqadoT7lYDwgSmKyxE4='.decode()

    # Next check if running in Docker
    if os.environ.get("RUNNING_IN_DOCKER") == "true":
        return get_docker_secret_key()

    # Otherwise read from local storage
    key = get_password("flowfile", "master_key")
    if not key:
        raise ValueError("Master key not found in storage.")
    return key


def decrypt_secret(encrypted_value) -> SecretStr:
    """
    Decrypt an encrypted value using the master key.

    Args:
        encrypted_value: The encrypted value as a string

    Returns:
        SecretStr: The decrypted value as a SecretStr
    """
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def encrypt_secret(secret_value):
    """
    Encrypt a secret value using the master key.

    Args:
        secret_value: The secret value to encrypt

    Returns:
        str: The encrypted value as a string
    """
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()
