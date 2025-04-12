# Simplified auth/secrets.py that only reads from location
import os
from pathlib import Path
import json
import logging
from cryptography.fernet import Fernet
from pydantic import SecretStr

# Set up logging
logger = logging.getLogger(__name__)


class KeyringFallback:
    """A simple fallback for reading keys from storage"""

    def __init__(self):

        app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
        self.storage_path = Path(app_data) / "flowfile"
        self.key_path = self.storage_path / ".secret_key"

    def _get_store_path(self, service_name):
        return self.storage_path / f"{service_name}.json.enc"

    def _read_store(self, service_name):
        path = self._get_store_path(service_name)
        if not path.exists():
            return {}

        try:
            with open(self.key_path, "rb") as f:
                key = f.read()
            with open(path, "rb") as f:
                data = f.read()

            return json.loads(Fernet(key).decrypt(data).decode())
        except Exception as _e:
            logger.debug(f"Error reading from encrypted store: {_e}")
            return {}

    def get_password(self, service_name, username):
        store = self._read_store(service_name)
        return store.get(username)


def get_password(service_name, username):
    """Read password from local storage"""
    fallback = KeyringFallback()
    return fallback.get_password(service_name, username)


def get_master_key():
    """Get the master encryption key."""
    key = get_password("flowfile", "master_key")
    if not key:
        raise ValueError("Master key not found in storage.")
    return key


def decrypt_secret(encrypted_value) -> SecretStr:
    """Decrypt an encrypted value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def encrypt_secret(secret_value):
    """Encrypt a secret value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()
