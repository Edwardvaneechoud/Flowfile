
from cryptography.fernet import Fernet
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.auth.secrets import get_master_key
from pydantic import SecretStr


def encrypt_secret(secret_value):
    """Encrypt a secret value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()


def decrypt_secret(encrypted_value) -> SecretStr:
    """Decrypt an encrypted value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def get_encrypted_secret(current_user_id: int, secret_name: str) -> str|None:
    with get_db_context() as db:
        user_id = current_user_id
        # Get secrets from database
        db_secret = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id and
                                                      db_models.Secret.name == secret_name).first()
        if db_secret:
            return db_secret.encrypted_value
        else:
            return None
