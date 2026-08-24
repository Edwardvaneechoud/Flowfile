"""Read-only mirror of core's ``$ffsec$`` secret scheme.

Deliberately duplicated (like ``flowfile_worker.secrets``) rather than imported:
the scheduler tick and the CLI subprocess must decrypt a channel's webhook URL
without pulling in flowfile_core. The format, the HKDF parameters and the master-key
resolution order (TEST_MODE → docker-only env/secret → secure store) must stay in
lockstep with ``flowfile_core/flowfile_core/secret_manager``,
``flowfile_core/auth/secrets.py`` and ``flowfile_worker/secrets.py``.
Nothing here writes: a missing key file is an error, never a freshly generated key.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

logger = logging.getLogger("flowfile.notifications")

# Must match flowfile_core / flowfile_worker.
KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"
SECRET_FORMAT_PREFIX = "$ffsec$1$"

TEST_MASTER_KEY = "06t640eu3AG2FmglZS0n0zrEdqadoT7lYDwgSmKyxE4="

DOCKER_SECRET_PATH = "/run/secrets/flowfile_master_key"
SECURE_STORAGE_SERVICE = "flowfile"
SECURE_STORAGE_ENTRY = "master_key"


def _secure_storage_path() -> Path:
    """Where core's ``SecureStorage`` put the store — resolved, never created."""
    if os.environ.get("FLOWFILE_MODE") == "electron":
        override = os.environ.get("FLOWFILE_SECURE_STORAGE_PATH")
        if override:
            return Path(override)
        app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
        return Path(app_data) / "flowfile"
    return Path(os.environ.get("SECURE_STORAGE_PATH", "/tmp/.flowfile"))


def _read_secure_storage_key() -> str | None:
    storage_path = _secure_storage_path()
    key_path = storage_path / ".secret_key"
    store_path = storage_path / f"{SECURE_STORAGE_SERVICE}.json.enc"
    if not key_path.exists() or not store_path.exists():
        return None
    try:
        store = json.loads(Fernet(key_path.read_bytes()).decrypt(store_path.read_bytes()).decode())
    except Exception:
        logger.debug("Could not read the secure store at %s", store_path, exc_info=True)
        return None
    return store.get(SECURE_STORAGE_ENTRY)


def _docker_master_key() -> str | None:
    """The docker-mode key: env var first, then the mounted secret. None if neither is set."""
    env_key = os.environ.get("FLOWFILE_MASTER_KEY")
    if env_key:
        try:
            Fernet(env_key.encode())
            return env_key
        except Exception:
            raise RuntimeError("FLOWFILE_MASTER_KEY is not a valid Fernet key") from None

    if os.path.exists(DOCKER_SECRET_PATH):
        try:
            key = Path(DOCKER_SECRET_PATH).read_text().strip()
            Fernet(key.encode())
            return key
        except Exception as e:
            raise RuntimeError("Failed to read master key from Docker secret") from e

    return None


def get_master_key() -> str:
    """Resolve the Fernet master key: TEST_MODE → (docker only) env/docker secret → secure store.

    The env var and the docker secret are consulted **only** in docker mode, exactly as
    core and the worker do: outside docker they encrypt with the secure-store key, so
    honouring a stray FLOWFILE_MASTER_KEY here would make every decrypt fail.
    """
    # Env is read per call rather than pinned at import so a test can set it late.
    if "TEST_MODE" in os.environ:
        return TEST_MASTER_KEY

    if os.environ.get("FLOWFILE_MODE") == "docker":
        key = _docker_master_key()
        if key is None:
            raise RuntimeError(
                "Master key not configured. Set FLOWFILE_MASTER_KEY environment variable "
                "or mount the flowfile_master_key Docker secret."
            )
        return key

    key = _read_secure_storage_key()
    if not key:
        raise ValueError("Master key not found in storage.")
    return key


def derive_user_key(user_id: int) -> bytes:
    """HKDF-derive the per-user Fernet key from the master key."""
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=KEY_DERIVATION_VERSION,
        info=f"user-{user_id}".encode(),
    )
    return base64.urlsafe_b64encode(hkdf.derive(get_master_key().encode()))


def decrypt_secret(encrypted: str) -> str:
    """Decrypt a ``$ffsec$1$<user_id>$<token>`` value, or a legacy raw Fernet token."""
    if encrypted.startswith(SECRET_FORMAT_PREFIX):
        remainder = encrypted[len(SECRET_FORMAT_PREFIX) :]
        parts = remainder.split("$", 1)
        if len(parts) != 2:
            raise ValueError("Invalid encrypted secret format")
        return Fernet(derive_user_key(int(parts[0]))).decrypt(parts[1].encode()).decode()

    return Fernet(get_master_key().encode()).decrypt(encrypted.encode()).decode()


def encrypt_secret(value: str, user_id: int | None = None) -> str:
    """Encrypt in the same format core writes, so a channel URL round-trips here."""
    if user_id is not None:
        token = Fernet(derive_user_key(user_id)).encrypt(value.encode()).decode()
        return f"{SECRET_FORMAT_PREFIX}{user_id}${token}"
    return Fernet(get_master_key().encode()).encrypt(value.encode()).decode()
