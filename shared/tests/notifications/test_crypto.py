"""The read-only $ffsec$ mirror must round-trip what core writes."""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from shared.notifications import crypto


@pytest.fixture(autouse=True)
def master_key(monkeypatch):
    """Pin the shared test key — outside docker the env var is deliberately ignored."""
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    return crypto.TEST_MASTER_KEY


def test_per_user_round_trip():
    token = crypto.encrypt_secret("https://hooks.example.com/abc", 7)
    assert token.startswith("$ffsec$1$7$")
    assert crypto.decrypt_secret(token) == "https://hooks.example.com/abc"


def test_legacy_raw_token_round_trip():
    token = crypto.encrypt_secret("https://hooks.example.com/abc")
    assert not token.startswith("$ffsec$")
    assert crypto.decrypt_secret(token) == "https://hooks.example.com/abc"


def test_another_users_key_cannot_read_it():
    token = crypto.encrypt_secret("secret-url", 7)
    forged = token.replace("$ffsec$1$7$", "$ffsec$1$8$", 1)
    with pytest.raises(Exception):
        crypto.decrypt_secret(forged)


def test_derived_keys_differ_per_user():
    assert crypto.derive_user_key(1) != crypto.derive_user_key(2)


def test_malformed_token_is_rejected():
    with pytest.raises(ValueError):
        crypto.decrypt_secret("$ffsec$1$no-user-separator")


def test_invalid_master_key_is_reported(monkeypatch):
    monkeypatch.delenv("TEST_MODE", raising=False)
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", "not-a-fernet-key")
    with pytest.raises(RuntimeError):
        crypto.get_master_key()


def test_test_mode_pins_the_shared_test_key(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "1")
    assert crypto.get_master_key() == crypto.TEST_MASTER_KEY


def test_env_master_key_is_ignored_outside_docker(monkeypatch, tmp_path):
    """Core encrypts with the secure-store key when not in docker; honouring the env var
    here would decrypt every channel URL with the wrong key."""
    monkeypatch.delenv("TEST_MODE", raising=False)
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    # Point the store at an empty dir so the machine's real /tmp/.flowfile cannot answer.
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(tmp_path / "store"))

    with pytest.raises(ValueError, match="Master key not found in storage."):
        crypto.get_master_key()


def test_env_master_key_is_used_in_docker_mode(monkeypatch):
    monkeypatch.delenv("TEST_MODE", raising=False)
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", key)

    assert crypto.get_master_key() == key
