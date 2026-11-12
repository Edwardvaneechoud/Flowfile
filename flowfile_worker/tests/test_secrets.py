"""Tests for secrets module."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from flowfile_worker.secrets import (
    KEY_DERIVATION_VERSION,
    SECRET_FORMAT_PREFIX,
    SecureStorage,
    decrypt_secret,
    derive_user_key,
    encrypt_secret,
    get_docker_secret_key,
    get_master_key,
)


class TestSecureStorage:
    """Test SecureStorage class."""

    def test_init_storage_path(self):
        storage = SecureStorage()
        assert storage.storage_path is not None
        assert "flowfile" in str(storage.storage_path)

    def test_get_store_path(self):
        storage = SecureStorage()
        path = storage._get_store_path("test_service")
        assert str(path).endswith("test_service.json.enc")

    def test_read_store_nonexistent(self):
        storage = SecureStorage()
        result = storage._read_store("nonexistent_service_12345")
        assert result == {}

    def test_get_password_nonexistent(self):
        storage = SecureStorage()
        result = storage.get_password("nonexistent_service_12345", "user")
        assert result is None


class TestConstants:
    """Test module constants."""

    def test_key_derivation_version(self):
        assert KEY_DERIVATION_VERSION == b"flowfile-secrets-v1"

    def test_secret_format_prefix(self):
        assert SECRET_FORMAT_PREFIX == "$ffsec$1$"


class TestGetMasterKey:
    """Test get_master_key function (TEST_MODE is enabled by conftest)."""

    def test_returns_test_key(self):
        key = get_master_key()
        assert isinstance(key, str)
        assert len(key) > 0


class TestGetDockerSecretKey:
    """Test get_docker_secret_key function."""

    def test_no_key_configured(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)
        result = get_docker_secret_key()
        assert result is None

    def test_valid_env_key(self, monkeypatch):
        from cryptography.fernet import Fernet
        valid_key = Fernet.generate_key().decode()
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", valid_key)
        result = get_docker_secret_key()
        assert result == valid_key

    def test_invalid_env_key_raises(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", "not-a-valid-key")
        with pytest.raises(RuntimeError, match="not a valid Fernet key"):
            get_docker_secret_key()


class TestDeriveUserKey:
    """Test derive_user_key function."""

    def test_returns_bytes(self):
        key = derive_user_key(1)
        assert isinstance(key, bytes)

    def test_different_users_different_keys(self):
        key1 = derive_user_key(1)
        key2 = derive_user_key(2)
        assert key1 != key2

    def test_deterministic(self):
        key1 = derive_user_key(42)
        key2 = derive_user_key(42)
        assert key1 == key2

    def test_key_suitable_for_fernet(self):
        from cryptography.fernet import Fernet
        key = derive_user_key(1)
        # Should not raise
        Fernet(key)


class TestEncryptDecryptSecret:
    """Test encrypt_secret and decrypt_secret functions."""

    def test_round_trip_with_user_id(self):
        original = "my_secret_password"
        encrypted = encrypt_secret(original, user_id=1)
        decrypted = decrypt_secret(encrypted)
        assert decrypted.get_secret_value() == original

    def test_round_trip_legacy(self):
        original = "legacy_secret"
        encrypted = encrypt_secret(original)
        decrypted = decrypt_secret(encrypted)
        assert decrypted.get_secret_value() == original

    def test_encrypted_format_with_user_id(self):
        encrypted = encrypt_secret("test", user_id=42)
        assert encrypted.startswith("$ffsec$1$42$")

    def test_encrypted_format_legacy(self):
        encrypted = encrypt_secret("test")
        assert not encrypted.startswith("$ffsec$")

    def test_different_user_ids_different_ciphertext(self):
        secret = "same_secret"
        enc1 = encrypt_secret(secret, user_id=1)
        enc2 = encrypt_secret(secret, user_id=2)
        assert enc1 != enc2

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid encrypted secret format"):
            decrypt_secret("$ffsec$1$invalid_no_dollar_separator")

    def test_decrypt_returns_secret_str(self):
        from pydantic import SecretStr
        encrypted = encrypt_secret("test_value", user_id=1)
        result = decrypt_secret(encrypted)
        assert isinstance(result, SecretStr)


def test_a_plain_secret_decrypts_without_loading_the_sealed_opener(tmp_path):
    """``shared.sealed`` loads only for a sealed secret, never for a ``$ffsec$`` or legacy one, so every worker child
    that decrypts a secret is spared it and the server extension that opens one. In a fresh process, since the test
    session has long imported the module."""
    repo_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import sys",
            "from flowfile_worker.secrets import decrypt_secret, encrypt_secret",
            'assert decrypt_secret(encrypt_secret("mine", user_id=1)).get_secret_value() == "mine"',
            'assert decrypt_secret(encrypt_secret("legacy")).get_secret_value() == "legacy"',
            'print("shared.sealed" in sys.modules)',
        ]
    )
    env = {
        **os.environ,
        "TEST_MODE": "1",
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "PYTHONPATH": os.pathsep.join(
            [str(repo_root / "flowfile_worker"), str(repo_root), os.environ.get("PYTHONPATH", "")]
        ),
    }
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"
