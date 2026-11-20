"""Server-mode security gates: ``docker`` and ``hub`` both require a configured master key and seed the admin
user from the environment; ``electron`` and ``package`` keep generating a local key and seed no admin. Without a token,
a server's ``GET /health/status`` names the Flowfile version it runs and says what the core can do.

``FLOWFILE_MODE`` is set per test through ``monkeypatch.setenv``, since these gates read it per call.
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

import flowfile_core.auth.secrets as secrets_module
from flowfile_core import main
from flowfile_core.auth.password import verify_password
from flowfile_core.auth.secrets import SecureStorage, get_master_key, is_master_key_configured
from flowfile_core.configs.settings import is_docker_mode, is_server_mode
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import create_docker_admin_user
from shared._version import __version__

SERVER_MODES = ["docker", "server"]
LOCAL_MODES = ["electron", "package"]


@pytest.fixture
def local_store(tmp_path, monkeypatch):
    """A secure store in a temp dir, so a local-mode key never lands in the shared test store."""

    def _make(mode: str) -> SecureStorage:
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / "store"))
        monkeypatch.setenv("SECURE_STORAGE_PATH", str(tmp_path / "store"))
        store = SecureStorage()
        monkeypatch.setattr(secrets_module, "_storage", store)
        return store

    return _make


def _delete_user(username: str) -> None:
    with get_db_context() as db:
        user = db.query(db_models.User).filter(db_models.User.username == username).first()
        if user:
            db.delete(user)
            db.commit()


class TestIsServerMode:
    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_modes(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        assert is_server_mode() is True

    @pytest.mark.parametrize("mode", LOCAL_MODES)
    def test_local_modes(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        assert is_server_mode() is False

    def test_unset_is_not_a_server(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        assert is_server_mode() is False

    def test_hub_is_not_docker(self, monkeypatch):
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "server")
        assert is_docker_mode() is False


class TestMasterKeyGate:
    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_refuses_without_master_key(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)

        assert is_master_key_configured() is False
        with pytest.raises(RuntimeError, match="Master key not configured"):
            get_master_key()

    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_reports_setup_required_without_master_key(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)

        with TestClient(main.app) as client:
            response = client.get("/health/status")

        assert response.status_code == 200
        data = response.json()
        assert data["setup_required"] is True
        assert data["master_key_configured"] is False
        assert data["mode"] == mode

    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_uses_environment_master_key(self, monkeypatch, mode):
        key = Fernet.generate_key().decode()
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", key)

        assert is_master_key_configured() is True
        assert get_master_key() == key

    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_rejects_invalid_master_key(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", "not-a-fernet-key")

        assert is_master_key_configured() is False
        with pytest.raises(RuntimeError, match="not a valid Fernet key"):
            get_master_key()

    @pytest.mark.parametrize("mode", LOCAL_MODES)
    def test_local_mode_generates_and_keeps_a_key(self, local_store, monkeypatch, mode):
        store = local_store(mode)
        monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)

        assert is_master_key_configured() is True
        key = get_master_key()
        Fernet(key.encode())
        assert store.get_password("flowfile", "master_key") == key
        assert get_master_key() == key

    @pytest.mark.parametrize("mode", LOCAL_MODES)
    def test_local_mode_ignores_environment_master_key(self, local_store, monkeypatch, mode):
        local_store(mode)
        env_key = Fernet.generate_key().decode()
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", env_key)

        assert get_master_key() != env_key


class TestHealthStatus:
    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_names_the_version_without_a_token(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)

        with TestClient(main.app) as client:
            response = client.get("/health/status")
            refused = client.get("/auth/users/me")

        assert response.status_code == 200
        assert response.json()["version"] == __version__
        assert refused.status_code == 401


class TestAdminSeeding:
    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_seeds_admin_from_environment(self, monkeypatch, mode):
        username = f"{mode}_seeded_admin"
        password = "seededpass123"
        _delete_user(username)
        try:
            monkeypatch.setenv("FLOWFILE_MODE", mode)
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", password)

            with get_db_context() as db:
                assert create_docker_admin_user(db) is True
                user = db.query(db_models.User).filter(db_models.User.username == username).first()
                assert user is not None
                assert user.is_admin is True
                assert user.must_change_password is True
                assert verify_password(password, user.hashed_password)
        finally:
            _delete_user(username)

    @pytest.mark.parametrize("mode", SERVER_MODES)
    def test_server_mode_without_admin_env_seeds_nothing(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.delenv("FLOWFILE_ADMIN_USER", raising=False)
        monkeypatch.delenv("FLOWFILE_ADMIN_PASSWORD", raising=False)

        with get_db_context() as db:
            assert create_docker_admin_user(db) is False

    @pytest.mark.parametrize("mode", LOCAL_MODES)
    def test_local_mode_does_not_seed_admin(self, monkeypatch, mode):
        username = f"{mode}_unseeded_admin"
        _delete_user(username)
        try:
            monkeypatch.setenv("FLOWFILE_MODE", mode)
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", "somepass123")

            with get_db_context() as db:
                assert create_docker_admin_user(db) is False
                assert db.query(db_models.User).filter(db_models.User.username == username).first() is None
        finally:
            _delete_user(username)
