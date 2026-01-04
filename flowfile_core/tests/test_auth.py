"""Tests for authentication functionality in both Docker and Electron modes."""

import os
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database import models as db_models
from flowfile_core.database.init_db import create_docker_admin_user
from flowfile_core.auth.password import verify_password, get_password_hash


@pytest.fixture
def test_user_credentials():
    """Fixture providing test user credentials."""
    return {
        "username": "testuser",
        "password": "testpassword123",
        "email": "testuser@flowfile.app"
    }


@pytest.fixture
def create_test_user(test_user_credentials):
    """Fixture to create a test user in the database."""
    with get_db_context() as db:
        # Clean up any existing test user
        existing_user = db.query(db_models.User).filter(
            db_models.User.username == test_user_credentials["username"]
        ).first()
        if existing_user:
            db.delete(existing_user)
            db.commit()

        # Create new test user
        hashed_password = get_password_hash(test_user_credentials["password"])
        test_user = db_models.User(
            username=test_user_credentials["username"],
            email=test_user_credentials["email"],
            full_name="Test User",
            hashed_password=hashed_password,
            disabled=False
        )
        db.add(test_user)
        db.commit()
        db.refresh(test_user)
        user_id = test_user.id

    yield user_id

    # Cleanup after test
    with get_db_context() as db:
        user = db.query(db_models.User).filter(db_models.User.id == user_id).first()
        if user:
            db.delete(user)
            db.commit()


class TestPasswordUtilities:
    """Test password hashing and verification utilities."""

    def test_hash_password(self):
        """Test that password hashing works."""
        password = "mysecretpassword"
        hashed = get_password_hash(password)

        assert hashed is not None
        assert hashed != password
        assert len(hashed) > 0
        # Bcrypt hashes start with $2b$
        assert hashed.startswith("$2b$")

    def test_verify_correct_password(self):
        """Test that correct password verification works."""
        password = "mysecretpassword"
        hashed = get_password_hash(password)

        assert verify_password(password, hashed) is True

    def test_verify_incorrect_password(self):
        """Test that incorrect password verification fails."""
        password = "mysecretpassword"
        wrong_password = "wrongpassword"
        hashed = get_password_hash(password)

        assert verify_password(wrong_password, hashed) is False

    def test_different_hashes_for_same_password(self):
        """Test that the same password produces different hashes (salt verification)."""
        password = "mysecretpassword"
        hash1 = get_password_hash(password)
        hash2 = get_password_hash(password)

        # Hashes should be different due to salt
        assert hash1 != hash2
        # But both should verify correctly
        assert verify_password(password, hash1) is True
        assert verify_password(password, hash2) is True


class TestElectronModeAuth:
    """Test authentication in Electron mode."""

    @pytest.fixture(autouse=True)
    def setup_electron_mode(self, monkeypatch):
        """Set up Electron mode for these tests."""
        monkeypatch.setenv("FLOWFILE_MODE", "electron")

    def test_electron_mode_auto_authenticate(self):
        """Test that Electron mode auto-authenticates without credentials."""
        with TestClient(main.app) as client:
            # Post to /auth/token without any credentials
            response = client.post("/auth/token")

            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data
            assert data["token_type"] == "bearer"
            assert len(data["access_token"]) > 0

    def test_electron_mode_ignores_credentials(self):
        """Test that Electron mode ignores provided credentials."""
        with TestClient(main.app) as client:
            # Even with wrong credentials, should auto-authenticate
            response = client.post(
                "/auth/token",
                data={"username": "wronguser", "password": "wrongpass"}
            )

            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data
            assert data["token_type"] == "bearer"


class TestDockerModeAuth:
    """Test authentication in Docker mode."""

    @pytest.fixture(autouse=True)
    def setup_docker_mode(self, monkeypatch):
        """Set up Docker mode for these tests."""
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-unit-tests")

    def test_docker_mode_login_with_valid_credentials(self, create_test_user, test_user_credentials):
        """Test successful login with valid credentials in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post(
                "/auth/token",
                data={
                    "username": test_user_credentials["username"],
                    "password": test_user_credentials["password"]
                }
            )

            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data
            assert data["token_type"] == "bearer"
            assert len(data["access_token"]) > 0

    def test_docker_mode_login_with_invalid_password(self, create_test_user, test_user_credentials):
        """Test login fails with invalid password in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post(
                "/auth/token",
                data={
                    "username": test_user_credentials["username"],
                    "password": "wrongpassword"
                }
            )

            assert response.status_code == 401
            data = response.json()
            assert data["detail"] == "Incorrect username or password"

    def test_docker_mode_login_with_invalid_username(self, test_user_credentials):
        """Test login fails with non-existent username in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post(
                "/auth/token",
                data={
                    "username": "nonexistentuser",
                    "password": test_user_credentials["password"]
                }
            )

            assert response.status_code == 401
            data = response.json()
            assert data["detail"] == "Incorrect username or password"

    def test_docker_mode_login_without_username(self):
        """Test login fails without username in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post(
                "/auth/token",
                data={"password": "somepassword"}
            )

            assert response.status_code == 401
            data = response.json()
            assert data["detail"] == "Incorrect username or password"

    def test_docker_mode_login_without_password(self, test_user_credentials):
        """Test login fails without password in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post(
                "/auth/token",
                data={"username": test_user_credentials["username"]}
            )

            assert response.status_code == 401
            data = response.json()
            assert data["detail"] == "Incorrect username or password"

    def test_docker_mode_login_without_credentials(self):
        """Test login fails without any credentials in Docker mode."""
        with TestClient(main.app) as client:
            response = client.post("/auth/token")

            assert response.status_code == 401
            data = response.json()
            assert data["detail"] == "Incorrect username or password"


class TestDockerAdminUserCreation:
    """Test admin user creation from environment variables."""

    def cleanup_admin_user(self, username):
        """Helper to clean up admin user."""
        with get_db_context() as db:
            user = db.query(db_models.User).filter(
                db_models.User.username == username
            ).first()
            if user:
                db.delete(user)
                db.commit()

    def test_create_admin_user_with_env_vars(self, monkeypatch):
        """Test admin user creation when environment variables are set."""
        admin_username = "dockeradmin"
        admin_password = "dockerpass123"

        # Clean up first
        self.cleanup_admin_user(admin_username)

        try:
            monkeypatch.setenv("FLOWFILE_MODE", "docker")
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", admin_username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", admin_password)

            with get_db_context() as db:
                result = create_docker_admin_user(db)

                assert result is True

                # Verify user was created
                user = db.query(db_models.User).filter(
                    db_models.User.username == admin_username
                ).first()

                assert user is not None
                assert user.username == admin_username
                assert user.email == f"{admin_username}@flowfile.app"
                assert user.full_name == "Admin User"
                assert verify_password(admin_password, user.hashed_password)
        finally:
            self.cleanup_admin_user(admin_username)

    def test_admin_user_not_created_in_electron_mode(self, monkeypatch):
        """Test admin user is not created in Electron mode."""
        admin_username = "electronuser"

        # Clean up first
        self.cleanup_admin_user(admin_username)

        try:
            monkeypatch.setenv("FLOWFILE_MODE", "electron")
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", admin_username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", "somepass")

            with get_db_context() as db:
                result = create_docker_admin_user(db)

                assert result is False

                # Verify user was not created
                user = db.query(db_models.User).filter(
                    db_models.User.username == admin_username
                ).first()

                assert user is None
        finally:
            self.cleanup_admin_user(admin_username)

    def test_admin_user_not_created_without_username(self, monkeypatch):
        """Test admin user is not created when username env var is missing."""
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.delenv("FLOWFILE_ADMIN_USER", raising=False)
        monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", "somepass")

        with get_db_context() as db:
            result = create_docker_admin_user(db)
            assert result is False

    def test_admin_user_not_created_without_password(self, monkeypatch):
        """Test admin user is not created when password env var is missing."""
        admin_username = "testadmin"

        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("FLOWFILE_ADMIN_USER", admin_username)
        monkeypatch.delenv("FLOWFILE_ADMIN_PASSWORD", raising=False)

        with get_db_context() as db:
            result = create_docker_admin_user(db)
            assert result is False

    def test_admin_user_already_exists(self, monkeypatch):
        """Test that existing admin user is not overwritten."""
        admin_username = "existingadmin"
        original_password = "originalpass"

        # Clean up first
        self.cleanup_admin_user(admin_username)

        try:
            # Create user manually
            with get_db_context() as db:
                original_hash = get_password_hash(original_password)
                user = db_models.User(
                    username=admin_username,
                    email=f"{admin_username}@original.com",
                    full_name="Original User",
                    hashed_password=original_hash
                )
                db.add(user)
                db.commit()

            # Try to create admin user with same username
            monkeypatch.setenv("FLOWFILE_MODE", "docker")
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", admin_username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", "newpassword")

            with get_db_context() as db:
                result = create_docker_admin_user(db)

                assert result is False

                # Verify original user is unchanged
                user = db.query(db_models.User).filter(
                    db_models.User.username == admin_username
                ).first()

                assert user is not None
                assert user.email == f"{admin_username}@original.com"
                assert user.full_name == "Original User"
                # Password should still be the original
                assert verify_password(original_password, user.hashed_password)
                assert not verify_password("newpassword", user.hashed_password)
        finally:
            self.cleanup_admin_user(admin_username)

    def test_login_with_created_admin_user(self, monkeypatch):
        """Test that we can login with the created admin user."""
        admin_username = "loginadmin"
        admin_password = "loginpass123"

        # Clean up first
        self.cleanup_admin_user(admin_username)

        try:
            # Create admin user
            monkeypatch.setenv("FLOWFILE_MODE", "docker")
            monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-unit-tests")
            monkeypatch.setenv("FLOWFILE_ADMIN_USER", admin_username)
            monkeypatch.setenv("FLOWFILE_ADMIN_PASSWORD", admin_password)

            with get_db_context() as db:
                create_docker_admin_user(db)

            # Try to login
            with TestClient(main.app) as client:
                response = client.post(
                    "/auth/token",
                    data={
                        "username": admin_username,
                        "password": admin_password
                    }
                )

                assert response.status_code == 200
                data = response.json()
                assert "access_token" in data
                assert data["token_type"] == "bearer"
        finally:
            self.cleanup_admin_user(admin_username)
