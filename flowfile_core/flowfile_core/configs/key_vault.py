from cryptography.fernet import Fernet
import threading


class SimpleKeyVault:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SimpleKeyVault, cls).__new__(cls)
                # Put any initialization here
                cls._instance.key = Fernet.generate_key()
                cls._instance.cipher_suite = Fernet(cls._instance.key)
                cls._instance.vault = {}
        return cls._instance

    def encrypt(self, message):
        """Encrypts a message."""
        if isinstance(message, str):
            message = message.encode()  # Ensure message is bytes
        encrypted_message = self.cipher_suite.encrypt(message)
        return encrypted_message

    def decrypt(self, encrypted_message):
        """Decrypts an encrypted message."""
        decrypted_message = self.cipher_suite.decrypt(encrypted_message)
        return decrypted_message.decode()  # Convert bytes back to string

    def set_value(self, key, value):
        """Encrypt and store a value in the vault."""
        self.vault[key] = self.encrypt(value)

    def get_value(self, key):
        """Retrieve and decrypt a value from the vault."""
        encrypted_value = self.vault.get(key)
        if encrypted_value:
            return self.decrypt(encrypted_value)
        return None
