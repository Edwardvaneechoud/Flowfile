"""Tests for the /system/db_backups admin endpoints.

Auth fixtures mirror tests/test_system_worker.py (dependency overrides).
``FLOWFILE_DB_PATH`` points the route at a throwaway tmp_path database — it
beats ``TESTING=True`` in ``get_database_url``, which ``get_database_path``
resolves per call.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from tests.test_migration import create_legacy_db


@pytest.fixture
def admin_client() -> Iterator[TestClient]:
    user = PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def non_admin_client() -> Iterator[TestClient]:
    user = PydanticUser(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def catalog_db(tmp_path, monkeypatch) -> Path:
    db_path = tmp_path / "flowfile_catalog.db"
    create_legacy_db(
        db_path,
        {
            "db_info": {
                "columns": ["id", "app_version", "updated_at"],
                "col_types": {"id": "INTEGER PRIMARY KEY", "app_version": "TEXT NOT NULL"},
                "rows": [(1, "0.16.0", "2026-01-01T00:00:00")],
            },
        },
    )
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(db_path))
    monkeypatch.delenv("FLOWFILE_DB_BACKUP_KEEP", raising=False)
    return db_path


def test_admin_lists_backups(admin_client: TestClient, catalog_db: Path) -> None:
    response = admin_client.get("/system/db_backups")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["directory"] == str(catalog_db.parent / "db_backups")
    assert body["keep"] == 10
    assert body["enabled"] is True
    assert body["backups"] == []


def test_post_creates_a_backup_that_the_next_get_lists(admin_client: TestClient, catalog_db: Path) -> None:
    created = admin_client.post("/system/db_backups", json={"reason": "pre_update"})

    assert created.status_code == 200, created.text
    body = created.json()
    assert body["kind"] == "pre_update"
    assert body["app_version"] == "0.16.0"
    assert Path(body["path"]).exists()

    listed = admin_client.get("/system/db_backups").json()["backups"]
    assert [entry["file_name"] for entry in listed] == [body["file_name"]]
    assert listed[0]["kind"] == "pre_update"


def test_non_admin_gets_403(non_admin_client: TestClient, catalog_db: Path) -> None:
    assert non_admin_client.get("/system/db_backups").status_code == 403
    assert non_admin_client.post("/system/db_backups", json={"reason": "manual"}).status_code == 403


def test_disabled_retention_returns_409(admin_client: TestClient, catalog_db: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_DB_BACKUP_KEEP", "0")

    listed = admin_client.get("/system/db_backups")
    assert listed.status_code == 200, listed.text
    assert listed.json()["enabled"] is False

    response = admin_client.post("/system/db_backups", json={"reason": "manual"})
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["error_code"] == "BACKUPS_DISABLED"


def test_busy_database_returns_503(admin_client: TestClient, catalog_db: Path, monkeypatch) -> None:
    import flowfile_core.database.backup as backup_mod

    monkeypatch.setattr(backup_mod, "_BACKUP_TIMEOUT_SECONDS", 0.5)
    locker = sqlite3.connect(catalog_db)
    locker.execute("BEGIN EXCLUSIVE")
    try:
        response = admin_client.post("/system/db_backups", json={"reason": "manual"})
    finally:
        locker.rollback()
        locker.close()

    assert response.status_code == 503, response.text
    assert response.json()["detail"]["error_code"] == "BACKUP_FAILED"
