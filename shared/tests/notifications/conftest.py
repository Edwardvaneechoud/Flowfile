"""Throwaway-DB + isolated-storage fixtures for the notification tests."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared import run_completion
from shared.models import Base
from shared.notifications import processor
from shared.storage_config import storage


@pytest.fixture(autouse=True)
def _isolate_storage(tmp_path, monkeypatch):
    """payload.build_run_event_payload probes ``storage.logs_directory``."""
    monkeypatch.setattr(storage, "_base_dir", tmp_path / "storage")
    storage.logs_directory.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def session_factory(tmp_path, monkeypatch):
    """Session factory over a temp catalog DB, with both get_database_url seams redirected."""
    url = f"sqlite:///{tmp_path / 'notify.db'}"
    # TEST_MODE pins the shared test key: outside docker the env var is ignored, and the
    # developer's real secure store must not be what these tests encrypt against.
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.delenv("FLOWFILE_MASTER_KEY", raising=False)
    monkeypatch.delenv("FLOWFILE_NOTIFY_ALLOW_PRIVATE_HOSTS", raising=False)
    monkeypatch.setattr(processor, "get_database_url", lambda: url)
    monkeypatch.setattr(run_completion, "get_database_url", lambda: url)

    engine = create_engine(url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
