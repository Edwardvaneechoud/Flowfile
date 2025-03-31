
from cryptography.fernet import Fernet
import keyring
import os


def get_master_key():
    if os.environ.get("FLOWFILE_MODE") == "electron" or 1==1:
        key = keyring.get_password("flowfile", "master_key")
        if not key:
            key = Fernet.generate_key().decode()
            keyring.set_password("flowfile", "master_key", key)
        return key
    else:
        key = os.environ.get("MASTER_KEY")
        if not key:
            raise Exception("MASTER_KEY environment variable must be set in Docker mode")
        return key


def encrypt_secret(secret_value):
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()


def decrypt_secret(encrypted_value):
    key = get_master_key().encode()
    f = Fernet(key)
    return f.decrypt(encrypted_value.encode()).decode()