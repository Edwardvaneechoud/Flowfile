# auth/secrets.py

from cryptography.fernet import Fernet
import os
from pathlib import Path
import json
import logging


# Set up logging
logger = logging.getLogger(__name__)

try:
    import keyring

    # Test if keyring actually works
    test_key = f"_test_{os.getpid()}"
    keyring.set_password("flowfile", test_key, "test")
    test_value = keyring.get_password("flowfile", test_key)
    if test_value == "test":
        keyring.delete_password("flowfile", test_key)
        KEYRING_AVAILABLE = True
    else:
        KEYRING_AVAILABLE = False
except Exception as main_e:
    logger.debug(f"Keyring not available: {main_e}")
    KEYRING_AVAILABLE = False


class KeyringFallback:
    """A simple fallback when keyring is not available"""

    def __init__(self):
        if os.environ.get("FLOWFILE_MODE") == "electron":
            app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
            self.storage_path = Path(app_data) / "flowfile"
        else:
            self.storage_path = Path(os.environ.get("SECURE_STORAGE_PATH", "/tmp/.flowfile"))

        self.storage_path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.storage_path, 0o700)
        except Exception as _e:
            logger.debug(f"Could not set permissions on storage directory: {_e}")

        self.key_path = self.storage_path / ".secret_key"
        if not self.key_path.exists():
            with open(self.key_path, "wb") as f:
                f.write(Fernet.generate_key())
            try:
                os.chmod(self.key_path, 0o600)
            except Exception as _e:
                logger.debug(f"Could not set permissions on key file: {_e}")

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

    def _write_store(self, service_name, data):
        try:
            with open(self.key_path, "rb") as f:
                key = f.read()

            encrypted = Fernet(key).encrypt(json.dumps(data).encode())
            path = self._get_store_path(service_name)

            with open(path, "wb") as f:
                f.write(encrypted)
            try:
                os.chmod(path, 0o600)
            except Exception as __e:
                logger.debug(f"Could not set permissions on store file: {__e}")
        except Exception as _e:
            logger.error(f"Failed to write to secure store: {_e}")

    def get_password(self, service_name, username):
        store = self._read_store(service_name)
        return store.get(username)

    def set_password(self, service_name, username, password):
        store = self._read_store(service_name)
        store[username] = password
        self._write_store(service_name, store)

    def delete_password(self, service_name, username):
        store = self._read_store(service_name)
        if username in store:
            del store[username]
            self._write_store(service_name, store)


# Initialize fallback if needed
_fallback = None if KEYRING_AVAILABLE else KeyringFallback()


def get_password(service_name, username):
    """Direct wrapper around keyring with fallback"""
    if KEYRING_AVAILABLE:
        try:
            return keyring.get_password(service_name, username)
        except Exception as _e:
            logger.debug(f"Keyring get_password failed: {_e}")

    if _fallback:
        return _fallback.get_password(service_name, username)
    return None


def set_password(service_name, username, password):
    """Direct wrapper around keyring with fallback"""
    if KEYRING_AVAILABLE:
        try:
            keyring.set_password(service_name, username, password)
            return
        except Exception as _e:
            logger.debug(f"Keyring set_password failed: {_e}")

    if _fallback:
        _fallback.set_password(service_name, username, password)


def delete_password(service_name, username):
    """Direct wrapper around keyring with fallback"""
    if KEYRING_AVAILABLE:
        try:
            keyring.delete_password(service_name, username)
            return
        except Exception as e:
            logger.debug(f"Keyring delete_password failed: {e}")

    if _fallback:
        _fallback.delete_password(service_name, username)


# Original functions using the secure storage

def get_master_key():
    """Get or generate the master encryption key."""
    key = get_password("flowfile", "master_key")
    if not key:
        key = Fernet.generate_key().decode()
        set_password("flowfile", "master_key", key)
    return key



