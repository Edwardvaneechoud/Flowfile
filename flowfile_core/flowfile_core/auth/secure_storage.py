import os
import json
import base64
import tempfile
from pathlib import Path
from typing import Optional, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class SecureStorage:
    """
    A platform-agnostic secure storage solution for managing secrets.
    Works across Windows, Linux, and macOS without external dependencies like keyring.
    """

    def __init__(self, app_name: str, storage_dir: Optional[str] = None, salt: Optional[bytes] = None):
        """
        Initialize the secure storage.

        Args:
            app_name: Name of the application (used for the storage file name)
            storage_dir: Optional directory to store the encrypted secrets
                         (defaults to platform-specific user config directory)
            salt: Optional salt for key derivation (defaults to a fixed value)
        """
        self.app_name = app_name
        self.salt = salt or b'flowfile_static_salt_value_0123456789'

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
        """
        Derive a key based on system-specific information.
        This creates a device-specific key without requiring user input.
        """
        # Get system information for key derivation
        system_info = []

        # Add machine ID if available (generally stable across reboots)
        machine_id_paths = ['/etc/machine-id', '/var/lib/dbus/machine-id']
        for path in machine_id_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        system_info.append(f.read().strip())
                    break
                except Exception:
                    pass

        # Add additional system identifiers
        try:
            if os.name == 'nt':  # Windows
                import subprocess
                result = subprocess.run(['wmic', 'csproduct', 'get', 'uuid'],
                                        capture_output=True, text=True, check=False)
                if result.returncode == 0:
                    uuid = result.stdout.split('\n')[1].strip()
                    system_info.append(uuid)
            elif os.name == 'posix':  # Linux/macOS
                # Get hostname
                if os.path.exists('/etc/hostname'):
                    with open('/etc/hostname', 'r') as f:
                        system_info.append(f.read().strip())

                # For macOS, try to get system UUID
                if os.path.exists('/usr/sbin/system_profiler'):
                    import subprocess
                    result = subprocess.run(['system_profiler', 'SPHardwareDataType'],
                                            capture_output=True, text=True, check=False)
                    if result.returncode == 0:
                        system_info.append(result.stdout)
        except Exception:
            # Fall back to a basic identifier if we can't get system-specific info
            pass

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
        """Get or generate the master encryption key."""
        if self._master_key:
            return self._master_key

        # First check environment variable
        env_key = os.environ.get(f"{self.app_name.upper()}_MASTER_KEY")
        if env_key:
            try:
                # Validate that it's a proper Fernet key
                Fernet(env_key.encode())
                self._master_key = env_key.encode()
                return self._master_key
            except Exception:
                pass  # Invalid key format, fall back to device-derived key

        # Use key derived from device info
        self._master_key = self._derive_key_from_device()
        return self._master_key

    def _load_secrets(self) -> None:
        """Load and decrypt the secrets file if it exists."""
        if not self.storage_path.exists():
            self._secrets = {}
            return

        try:
            with open(self.storage_path, 'rb') as f:
                encrypted_data = f.read()

            f = Fernet(self._get_master_key())
            decrypted_data = f.decrypt(encrypted_data)
            self._secrets = json.loads(decrypted_data.decode())
        except Exception:
            # If decryption fails (possibly due to changed machine ID),
            # we'll start with an empty secrets store
            self._secrets = {}

    def _save_secrets(self) -> None:
        """Encrypt and save the secrets file."""
        f = Fernet(self._get_master_key())
        encrypted_data = f.encrypt(json.dumps(self._secrets).encode())

        # Write to a temporary file first, then move to the final location
        # for atomic writes (prevents corruption if the process is interrupted)
        with tempfile.NamedTemporaryFile(delete=False, dir=self.storage_dir) as temp:
            temp.write(encrypted_data)
            temp_name = temp.name

        os.replace(temp_name, self.storage_path)

    def get_secret(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a secret by key.

        Args:
            key: The key to look up
            default: Value to return if key doesn't exist

        Returns:
            The stored secret value or the default value
        """
        return self._secrets.get(key, default)

    def set_secret(self, key: str, value: Any) -> None:
        """
        Store a secret value.

        Args:
            key: The key to store the value under
            value: The value to store (must be JSON serializable)
        """
        self._secrets[key] = value
        self._save_secrets()

    def delete_secret(self, key: str) -> bool:
        """
        Delete a secret by key.

        Args:
            key: The key to delete

        Returns:
            True if the key existed and was deleted, False otherwise
        """
        if key in self._secrets:
            del self._secrets[key]
            self._save_secrets()
            return True
        return False

    def clear_all(self) -> None:
        """Remove all stored secrets."""
        self._secrets = {}
        self._save_secrets()

    def encrypt_value(self, value: str) -> str:
        """
        Encrypt a single value using the master key.

        Args:
            value: The string value to encrypt

        Returns:
            The encrypted value as a string
        """
        f = Fernet(self._get_master_key())
        return f.encrypt(value.encode()).decode()

    def decrypt_value(self, encrypted_value: str) -> str:
        """
        Decrypt a single value using the master key.

        Args:
            encrypted_value: The encrypted string value

        Returns:
            The decrypted value as a string
        """
        f = Fernet(self._get_master_key())
        return f.decrypt(encrypted_value.encode()).decode()


# Example usage for drop-in replacement of the original functions
def get_master_key():
    """
    Get the master key for encryption/decryption.
    Compatible with the original API but uses SecureStorage internally.
    """
    storage = SecureStorage("flowfile")
    master_key = storage.get_secret("master_key")
    if not master_key:
        # Generate a new key
        master_key = Fernet.generate_key().decode()
        storage.set_secret("master_key", master_key)
    return master_key


def encrypt_secret(secret_value):
    """
    Encrypt a secret value.
    Compatible with the original API but uses SecureStorage internally.
    """
    storage = SecureStorage("flowfile")
    return storage.encrypt_value(secret_value)


def decrypt_secret(encrypted_value):
    """
    Decrypt a secret value.
    Compatible with the original API but uses SecureStorage internally.
    """
    storage = SecureStorage("flowfile")
    return storage.decrypt_value(encrypted_value)


def get_secure_storage():
    # Try to use keyring first if available
    try:
        import keyring
        # Test keyring with a simple operation
        keyring.get_password("test", "test")
        return KeyringStorage("flowfile")  # A wrapper class for keyring
    except (ImportError, Exception) as e:
        # Fall back to custom implementation if keyring isn't available/working
        return SecureStorage("flowfile")