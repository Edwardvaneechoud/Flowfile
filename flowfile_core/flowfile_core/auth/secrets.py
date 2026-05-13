"""
Secure storage module for FlowFile credentials and secrets.
"""

import hashlib
import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


def _key_fingerprint(key: str) -> str:
    """Short, stable, non-reversible identifier for a Fernet key.

    The first 8 hex chars of SHA-256 are enough for an operator to
    visually compare a backed-up key against the live one without
    exposing the raw key material in logs or status responses.
    """
    return hashlib.sha256(key.encode()).hexdigest()[:8]


class SecureStorage:
    """A secure local storage mechanism for secrets using Fernet encryption."""

    def __init__(self):
        env = os.environ.get("FLOWFILE_MODE")
        logger.debug(f"Using secure storage in {env} mode")
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
        env_key = env_key.strip().strip('"').strip("'")
        try:
            Fernet(env_key.encode())
            return env_key
        except Exception:
            raise RuntimeError("FLOWFILE_MASTER_KEY is not a valid Fernet key") from None

    secret_path = "/run/secrets/flowfile_master_key"
    if os.path.isfile(secret_path):
        try:
            with open(secret_path) as f:
                key = f.read().strip()
                Fernet(key.encode())
                return key
        except Exception:
            raise RuntimeError("Failed to read master key from Docker secret") from None

    return None


def is_master_key_configured() -> bool:
    """
    Check if the master key is properly configured.

    Returns:
        bool: True if master key is configured and valid, False otherwise.
    """
    try:
        if os.environ.get("FLOWFILE_MODE") == "docker":
            return get_docker_secret_key() is not None
        return True
    except RuntimeError:
        return False


def generate_master_key() -> str:
    """
    Generate a new Fernet master key.

    Returns:
        str: A new valid Fernet encryption key.
    """
    return Fernet.generate_key().decode()


def get_master_key():
    """
    Get or generate the master encryption key.

    If running in Docker, retrieves the key from Docker secrets or environment.
    Otherwise, retrieves or generates a key using the secure storage.

    Returns:
        str: The master encryption key

    Raises:
        RuntimeError: If in Docker mode and no key is configured.
    """
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
        key = Fernet.generate_key().decode()
        set_password("flowfile", "master_key", key)
        # Loud one-shot banner. This branch only fires when the secure store
        # had no master key — i.e. the *very first* call on a fresh install.
        # The absence of the key file is itself the trigger, so no sentinel is
        # needed. Subsequent runs read the cached key and skip this entirely.
        storage_dir = _storage.storage_path
        logger.warning(
            "FLOWFILE MASTER KEY GENERATED — BACK UP THESE FILES IMMEDIATELY.\n"
            "  fingerprint: %s\n"
            "  file 1 (store key):  %s\n"
            "  file 2 (encrypted secrets store): %s\n"
            "If either file is lost, every secret encrypted by this Flowfile "
            "instance becomes permanently unrecoverable. There is no recovery path.",
            _key_fingerprint(key),
            storage_dir / ".secret_key",
            storage_dir / "flowfile.json.enc",
        )
    return key
