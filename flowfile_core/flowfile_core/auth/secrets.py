"""
Secure storage module for FlowFile credentials and secrets.
"""
from cryptography.fernet import Fernet
import os
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


class SecureStorage:
    """A secure local storage mechanism for secrets using Fernet encryption."""

    def __init__(self):
        env = os.environ.get("FLOWFILE_MODE")
        logger.debug(f'Using secure storage in {env} mode')
        if os.environ.get("FLOWFILE_MODE") == "electron":
            app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
            self.storage_path = Path(app_data) / "flowfile"
        else:
            self.storage_path = Path(os.environ.get("SECURE_STORAGE_PATH", "/tmp/.flowfile"))
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Using SECURE_STORAGE_PATH: {self.storage_path}")
        try:
            os.chmod(self.storage_path, 0o700)
        except Exception as e:
            logger.debug(f"Could not set permissions on storage directory: {e}")

        self.key_path = self.storage_path / ".secret_key"
        if not self.key_path.exists():
            with open(self.key_path, "wb") as f:
                f.write(Fernet.generate_key())
            try:
                os.chmod(self.key_path, 0o600)
            except Exception as e:
                logger.debug(f"Could not set permissions on key file: {e}")

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

    def _write_store(self, service_name, data):
        """Encrypt and write data to the store file for a service."""
        try:
            with open(self.key_path, "rb") as f:
                key = f.read()

            encrypted = Fernet(key).encrypt(json.dumps(data).encode())
            path = self._get_store_path(service_name)

            with open(path, "wb") as f:
                f.write(encrypted)
            try:
                os.chmod(path, 0o600)
            except Exception as e:
                logger.debug(f"Could not set permissions on store file: {e}")
        except Exception as e:
            logger.error(f"Failed to write to secure store: {e}")

    def get_password(self, service_name, username):
        """Retrieve a password from secure storage."""
        store = self._read_store(service_name)
        return store.get(username)

    def set_password(self, service_name, username, password):
        """Store a password in secure storage."""
        store = self._read_store(service_name)
        store[username] = password
        self._write_store(service_name, store)

    def delete_password(self, service_name, username):
        """Delete a password from secure storage."""
        store = self._read_store(service_name)
        if username in store:
            del store[username]
            self._write_store(service_name, store)


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


def set_password(service_name, username, password):
    """
    Store a password in secure storage.

    Args:
        service_name: The name of the service
        username: The username or key
        password: The password or secret to store
    """
    _storage.set_password(service_name, username, password)


def delete_password(service_name, username):
    """
    Delete a password from secure storage.

    Args:
        service_name: The name of the service
        username: The username or key to delete
    """
    _storage.delete_password(service_name, username)


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


def get_master_key():
    """
    Get or generate the master encryption key.

    If running in Docker, retrieves the key from Docker secrets.
    Otherwise, retrieves or generates a key using the secure storage.

    Returns:
        str: The master encryption key
    """
    if os.environ.get("RUNNING_IN_DOCKER") == "true":
        return get_docker_secret_key()

    key = get_password("flowfile", "master_key")
    if not key:
        key = Fernet.generate_key().decode()
        set_password("flowfile", "master_key", key)
    return key
