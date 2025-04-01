from cryptography.fernet import Fernet
import os
import json
import base64
import tempfile
import subprocess

from pathlib import Path
from typing import Optional
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging
from cryptography.fernet import InvalidToken

# Setup logging
logger = logging.getLogger(__name__)


class FileSecureStorage:
    """File-based secure storage as a fallback when keyring is not available"""

    def __init__(self, app_name: str, storage_dir: Optional[str] = None):
        """Initialize the file-based secure storage"""
        self.app_name = app_name
        self.salt = b'flowfile_static_salt_value_0123456789'

        if storage_dir:
            self.storage_dir = Path(storage_dir)
        else:
            # Use platform-specific config directory
            if os.name == 'nt':  # Windows
                base_dir = os.environ.get('APPDATA', '')
            elif os.name == 'posix':  # Linux/macOS
                base_dir = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
            else:
                base_dir = os.path.expanduser('~/.config')

            self.storage_dir = Path(base_dir) / app_name

        # Create directory if it doesn't exist
        os.makedirs(self.storage_dir, exist_ok=True)

        self.storage_path = self.storage_dir / f"{app_name}_secrets.enc"
        self._master_key = None
        self._secrets = {}

        # Load existing secrets if available
        self._load_secrets()

    def _derive_key_from_device(self) -> bytes:
        """Derive a key based on system-specific information"""
        # Get system information for key derivation
        system_info = []

        # Add machine ID if available
        machine_id_paths = ['/etc/machine-id', '/var/lib/dbus/machine-id']
        for path in machine_id_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        system_info.append(f.read().strip())
                    break
                except (IOError, PermissionError) as e:
                    logger.debug(f"Could not read machine ID from {path}: {e}")

        # Add additional system identifiers
        try:
            if os.name == 'nt':  # Windows
                result = subprocess.run(['wmic', 'csproduct', 'get', 'uuid'],
                                        capture_output=True, text=True, check=False)
                if result.returncode == 0:
                    uuid = result.stdout.split('\n')[1].strip()
                    system_info.append(uuid)
            elif os.name == 'posix':  # Linux/macOS
                # Get hostname
                if os.path.exists('/etc/hostname'):
                    try:
                        with open('/etc/hostname', 'r') as f:
                            system_info.append(f.read().strip())
                    except (IOError, PermissionError) as e:
                        logger.debug(f"Could not read hostname: {e}")

                # For macOS, try to get system UUID
                if os.path.exists('/usr/sbin/system_profiler'):
                    try:
                        result = subprocess.run(['system_profiler', 'SPHardwareDataType'],
                                                capture_output=True, text=True, check=False)
                        if result.returncode == 0:
                            system_info.append(result.stdout)
                    except (subprocess.SubprocessError, OSError) as e:
                        logger.debug(f"Could not run system_profiler: {e}")
        except (ImportError, subprocess.SubprocessError, OSError) as e:
            logger.debug(f"Error collecting system identifiers: {e}")

        # If we couldn't get any system info, use the username and home directory
        if not system_info:
            system_info.append(os.environ.get('USERNAME', os.environ.get('USER', 'unknown')))
            system_info.append(os.path.expanduser('~'))

        # Create a unique device identifier
        device_id = ''.join(system_info).encode()

        # Derive key using PBKDF2
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.salt,
            iterations=100000,
        )

        key = base64.urlsafe_b64encode(kdf.derive(device_id))
        return key

    def _get_master_key(self) -> bytes:
        """Get or generate the master encryption key"""
        if self._master_key:
            return self._master_key

        # First check environment variable
        env_key = os.environ.get(f"MASTER_KEY") or os.environ.get(f"{self.app_name.upper()}_MASTER_KEY")
        if env_key:
            try:
                # Validate that it's a proper Fernet key
                Fernet(env_key.encode())
                self._master_key = env_key.encode()
                return self._master_key
            except ValueError as e:
                logger.warning(f"Invalid master key format in environment variable: {e}")
                logger.warning("Falling back to device-derived key")

        # Use key derived from device info
        self._master_key = self._derive_key_from_device()
        return self._master_key

    def _load_secrets(self) -> None:
        """Load and decrypt the secrets file if it exists"""
        if not self.storage_path.exists():
            self._secrets = {}
            return

        try:
            with open(self.storage_path, 'rb') as f:
                encrypted_data = f.read()

            f = Fernet(self._get_master_key())
            decrypted_data = f.decrypt(encrypted_data)
            self._secrets = json.loads(decrypted_data.decode())
        except (IOError, PermissionError) as e:
            logger.warning(f"Failed to read secrets file: {e}")
            self._secrets = {}
        except InvalidToken as e:
            logger.warning(f"Failed to decrypt secrets (invalid key): {e}")
            self._secrets = {}
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse secrets JSON: {e}")
            self._secrets = {}
        except Exception as e:
            logger.warning(f"Unexpected error loading secrets: {e}")
            self._secrets = {}

    def _save_secrets(self) -> None:
        """Encrypt and save the secrets file"""
        try:
            f = Fernet(self._get_master_key())
            encrypted_data = f.encrypt(json.dumps(self._secrets).encode())

            # Write to a temporary file first, then move to the final location
            with tempfile.NamedTemporaryFile(delete=False, dir=self.storage_dir) as temp:
                temp.write(encrypted_data)
                temp_name = temp.name

            os.replace(temp_name, self.storage_path)
        except (IOError, PermissionError) as e:
            logger.error(f"Failed to write secrets file: {e}")
            raise
        except ValueError as e:
            logger.error(f"Encryption error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error saving secrets: {e}")
            raise

    def get_password(self, service: str, username: str) -> Optional[str]:
        """Keyring-compatible get_password method"""
        key = f"{service}:{username}"
        return self._secrets.get(key)

    def set_password(self, service: str, username: str, password: str) -> None:
        """Keyring-compatible set_password method"""
        key = f"{service}:{username}"
        self._secrets[key] = password
        self._save_secrets()


def get_master_key() -> str:
    """
    Get the master key for encryption/decryption.

    Tries multiple methods in order:
    1. Environment variable (for Docker mode)
    2. Keyring if available (for Electron mode)
    3. File-based storage with device-derived key (fallback)

    Returns:
        str: The master key as a string
    """
    # Try environment variable first if not in Electron mode
    if os.environ.get("FLOWFILE_MODE") != "electron":
        key = get_key_from_env()
        if key:
            return key

    # Try keyring next
    key = get_key_from_keyring()
    if key:
        return key

    # Fall back to file storage
    return get_key_from_file()


def get_key_from_env() -> str:
    """
    Get master key from environment variable.

    Returns:
        str: The master key if found and valid, empty string otherwise
    """
    env_key = os.environ.get("MASTER_KEY")
    if not env_key:
        logger.info("No MASTER_KEY environment variable found")
        return ""

    try:
        # Validate the key format
        Fernet(env_key.encode())
        return env_key
    except ValueError as e:
        logger.warning(f"Invalid MASTER_KEY environment variable: {e}")
        return ""


def get_key_from_keyring() -> str:
    """
    Get master key from the system keyring.

    Returns:
        str: The master key if keyring works, empty string otherwise
    """
    try:
        import keyring
        try:
            # Test that keyring is working
            key = keyring.get_password("flowfile", "master_key")
            if not key:
                key = Fernet.generate_key().decode()
                keyring.set_password("flowfile", "master_key", key)
            return key
        except Exception as e:
            logger.warning(f"Keyring is available but not working: {e}")
            return ""
    except ImportError:
        logger.info("Keyring package not installed")
        return ""


def get_key_from_file() -> str:
    """
    Get master key from file-based secure storage.
    This is the fallback method when other methods fail.

    Returns:
        str: The master key, either existing or newly generated
    """
    logger.info("Using file-based secure storage fallback")
    storage = FileSecureStorage("flowfile")
    key = storage.get_password("flowfile", "master_key")
    if not key:
        key = Fernet.generate_key().decode()
        storage.set_password("flowfile", "master_key", key)
    return key


def generate_new_key() -> str:
    """
    Generate a new Fernet key.

    Returns:
        str: A new Fernet key as a string
    """
    return Fernet.generate_key().decode()

def encrypt_secret(secret_value: str) -> str:
    """
    Encrypt a secret value.

    Args:
        secret_value: The string to encrypt

    Returns:
        str: The encrypted value as a string

    Raises:
        ValueError: If the input is invalid or encryption fails
    """
    if not isinstance(secret_value, str):
        raise ValueError("Secret value must be a string")

    try:
        key = get_master_key().encode()
        f = Fernet(key)
        return f.encrypt(secret_value.encode()).decode()
    except (ValueError, TypeError) as e:
        logger.error(f"Encryption error: {e}")
        raise ValueError(f"Failed to encrypt secret: {e}") from e


def decrypt_secret(encrypted_value: str) -> str:
    """
    Decrypt a secret value.

    Args:
        encrypted_value: The encrypted string to decrypt

    Returns:
        str: The decrypted value as a string

    Raises:
        ValueError: If the input is invalid
        cryptography.fernet.InvalidToken: If decryption fails (e.g., tampered data or wrong key)
    """
    if not isinstance(encrypted_value, str):
        raise ValueError("Encrypted value must be a string")

    try:
        key = get_master_key().encode()
        f = Fernet(key)
        return f.decrypt(encrypted_value.encode()).decode()
    except InvalidToken as e:
        logger.error(f"Failed to decrypt (invalid token): {e}")
        raise
    except (ValueError, TypeError) as e:
        logger.error(f"Decryption error: {e}")
        raise ValueError(f"Failed to decrypt secret: {e}") from e
