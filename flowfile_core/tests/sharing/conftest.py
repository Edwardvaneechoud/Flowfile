"""Fixtures for multi-user (docker-mode) sharing tests.

In-process, no Docker: every sharing gate reads FLOWFILE_MODE from os.environ per
call, so monkeypatch.setenv flips the mode for just these tests. Module-level
TestClients elsewhere in the suite mint electron tokens at import time — never
flip the mode process-wide.
"""

from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.password import get_password_hash
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context

TEST_PASSWORD = "sharing-test-password-123"
USERNAMES = {
    "admin": "share_admin",
    "alice": "share_alice",
    "bob": "share_bob",
    "carol": "share_carol",
}


@pytest.fixture(autouse=True)
def multi_user_mode(monkeypatch):
    """Docker mode + the env docker mode requires (JWT secret, valid Fernet master key)."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("JWT_SECRET_KEY", "sharing-test-secret-key")
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())


@pytest.fixture
def users():
    """One admin + three regular users; teardown removes them and ALL sharing rows."""
    created_ids = []
    out = {}
    with get_db_context() as db:
        for key, username in USERNAMES.items():
            existing = db.query(db_models.User).filter(db_models.User.username == username).first()
            if existing:
                db.delete(existing)
                db.commit()
            user = db_models.User(
                username=username,
                email=f"{username}@flowfile.app",
                full_name=username,
                hashed_password=get_password_hash(TEST_PASSWORD),
                disabled=False,
                is_admin=(key == "admin"),
                must_change_password=False,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            created_ids.append(user.id)
            out[key] = SimpleNamespace(id=user.id, username=username, is_admin=(key == "admin"))
    yield out
    with get_db_context() as db:
        db.query(db_models.ResourceGrant).delete()
        db.query(db_models.UserGroupMembership).delete()
        db.query(db_models.UserGroup).delete()
        for model in (
            db_models.DatabaseConnection,
            db_models.CloudStorageConnection,
            db_models.KafkaConnection,
            db_models.GoogleAnalyticsConnection,
            db_models.Secret,
        ):
            db.query(model).filter(model.user_id.in_(created_ids)).delete(synchronize_session=False)
        for uid in created_ids:
            user = db.get(db_models.User, uid)
            if user is not None:
                db.delete(user)
        db.commit()


@pytest.fixture
def group_factory():
    """Create a group with members via ORM: group_factory(name, created_by, {user_id: role})."""

    def _create(name: str, created_by: int, members: dict[int, str]) -> int:
        with get_db_context() as db:
            group = db_models.UserGroup(name=name, created_by=created_by)
            db.add(group)
            db.commit()
            db.refresh(group)
            for user_id, role in members.items():
                db.add(db_models.UserGroupMembership(group_id=group.id, user_id=user_id, role=role))
            db.commit()
            return group.id

    return _create


@pytest.fixture
def grant_factory():
    def _grant(resource_type: str, resource_id: int, group_id: int, permission: str = "use", granted_by: int = 1):
        with get_db_context() as db:
            db.add(
                db_models.ResourceGrant(
                    resource_type=resource_type,
                    resource_id=resource_id,
                    group_id=group_id,
                    permission=permission,
                    granted_by=granted_by,
                )
            )
            db.commit()

    return _grant


@pytest.fixture
def resource_factory():
    """Create arbitrary ORM rows and delete them (reverse order) on teardown."""
    created: list[tuple[type, int]] = []

    def _create(model, **kwargs) -> int:
        with get_db_context() as db:
            row = model(**kwargs)
            db.add(row)
            db.commit()
            db.refresh(row)
            created.append((model, row.id))
            return row.id

    yield _create
    with get_db_context() as db:
        for model, row_id in reversed(created):
            obj = db.get(model, row_id)
            if obj is not None:
                db.delete(obj)
        db.commit()


@pytest.fixture
def client_for(users):
    """Authenticated TestClient for a user key from the `users` fixture ('admin', 'alice', ...)."""
    clients = []

    def _client(key: str) -> TestClient:
        client = TestClient(main.app)
        response = client.post("/auth/token", data={"username": USERNAMES[key], "password": TEST_PASSWORD})
        assert response.status_code == 200, response.text
        client.headers["Authorization"] = f"Bearer {response.json()['access_token']}"
        clients.append(client)
        return client

    yield _client
    for client in clients:
        client.close()
