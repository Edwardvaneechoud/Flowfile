"""Fixtures for workspace (project export/import) tests.

All tests run against the shared session test DB but isolate themselves with a
fresh user per test (cleaned up on teardown), and redirect the ``storage``
singleton's roots to a temp dir so flow-file writes never touch ``~/.flowfile``.
"""

from __future__ import annotations

import uuid

import pytest
from cryptography.fernet import Fernet

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture(autouse=True)
def _master_key(monkeypatch):
    """store_secret / encrypt_secret need a valid Fernet master key in env."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())


@pytest.fixture
def storage_tmp(monkeypatch, tmp_path):
    """Redirect storage roots to a temp dir (flows/outputs/etc.) for isolation.

    ``base_directory`` caches in ``_base_dir``; setting it (plus ``_user_data_dir``)
    redirects every path property computed from them. The DB engine was already
    bound at import, so the catalog DB is unaffected.
    """
    from shared.storage_config import storage

    base = tmp_path / "storage"
    base.mkdir()
    monkeypatch.setattr(storage, "_base_dir", base)
    monkeypatch.setattr(storage, "_user_data_dir", base)
    return base


@pytest.fixture
def project_root(tmp_path):
    return str(tmp_path / "project")


@pytest.fixture
def ws_user():
    """A throwaway user; teardown removes the user and all its workspace rows."""
    with get_db_context() as db:
        username = f"ws_{uuid.uuid4().hex[:10]}"
        user = db_models.User(
            username=username,
            email=f"{username}@flowfile.test",
            full_name=username,
            hashed_password="x",
            disabled=False,
            is_admin=False,
            must_change_password=False,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = user.id

    yield uid

    with get_db_context() as db:
        db.query(db_models.FlowSchedule).filter(db_models.FlowSchedule.owner_id == uid).delete()
        db.query(db_models.FlowRegistration).filter(db_models.FlowRegistration.owner_id == uid).delete()
        for model in (
            db_models.DatabaseConnection,
            db_models.CloudStorageConnection,
            db_models.KafkaConnection,
            db_models.GoogleAnalyticsConnection,
            db_models.Secret,
        ):
            db.query(model).filter(model.user_id == uid).delete(synchronize_session=False)
        db.query(db_models.WorkspaceProject).filter(db_models.WorkspaceProject.owner_id == uid).delete()
        user = db.get(db_models.User, uid)
        if user is not None:
            db.delete(user)
        db.commit()
