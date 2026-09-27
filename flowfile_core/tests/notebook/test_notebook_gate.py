"""The canvas-notebook flag, the session mode policy and the ``GET /notebook/status`` route."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings
from flowfile_core.notebook.gate import is_canvas_notebook_enabled, notebook_sessions_allowed

ADMIN = SimpleNamespace(id=1, is_admin=True)
MEMBER = SimpleNamespace(id=2, is_admin=False)


@pytest.fixture
def flag():
    before = bool(settings.FEATURE_FLAG_CANVAS_NOTEBOOK)
    yield settings.FEATURE_FLAG_CANVAS_NOTEBOOK
    settings.FEATURE_FLAG_CANVAS_NOTEBOOK.set(before)


def _client(is_admin: bool):
    user = PydanticUser(username="nb_user", id=1 if is_admin else 2, disabled=False, is_admin=is_admin)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    return TestClient(main.app)


@pytest.fixture
def admin_client():
    yield _client(is_admin=True)
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def member_client():
    yield _client(is_admin=False)
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


def test_flag_defaults_off_without_the_env_var():
    import os

    if "FEATURE_FLAG_CANVAS_NOTEBOOK" in os.environ:
        pytest.skip("FEATURE_FLAG_CANVAS_NOTEBOOK is set in this environment")
    assert type(settings.FEATURE_FLAG_CANVAS_NOTEBOOK).__name__ == "MutableBool"
    assert is_canvas_notebook_enabled() is False


def test_flag_flips_live(flag):
    flag.set(False)
    assert is_canvas_notebook_enabled() is False
    flag.set(True)
    assert is_canvas_notebook_enabled() is True


def test_status_route_503_when_flag_off(flag, admin_client):
    flag.set(False)
    response = admin_client.get("/notebook/status")
    assert response.status_code == 503
    assert "FEATURE_FLAG_CANVAS_NOTEBOOK" in response.json()["detail"]


def test_status_route_200_when_flag_on(flag, admin_client, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    flag.set(True)
    response = admin_client.get("/notebook/status")
    assert response.status_code == 200
    assert response.json() == {"canvas_notebook": True, "sessions": True}


def test_status_route_reports_sessions_off_for_a_member_in_docker(flag, member_client, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", "admin")
    flag.set(True)
    response = member_client.get("/notebook/status")
    assert response.status_code == 200
    assert response.json() == {"canvas_notebook": True, "sessions": False}


def test_status_route_requires_auth(flag):
    flag.set(True)
    assert TestClient(main.app).get("/notebook/status").status_code == 401


def test_sessions_allowed_for_every_user_in_electron(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.delenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", raising=False)
    assert notebook_sessions_allowed(ADMIN) and notebook_sessions_allowed(MEMBER)


def test_sessions_default_to_electron_when_mode_unset(monkeypatch):
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    assert notebook_sessions_allowed(MEMBER)


@pytest.mark.parametrize("mode", ["docker", "package"])
def test_sessions_off_by_default_in_multi_user_modes(monkeypatch, mode):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    monkeypatch.delenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", raising=False)
    assert not notebook_sessions_allowed(ADMIN)
    assert not notebook_sessions_allowed(MEMBER)


@pytest.mark.parametrize("mode", ["docker", "package"])
def test_sessions_admin_only_when_opted_in(monkeypatch, mode):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", "admin")
    assert notebook_sessions_allowed(ADMIN)
    assert not notebook_sessions_allowed(MEMBER)


@pytest.mark.parametrize("value", ["on", "true", "all", ""])
def test_sessions_opt_in_accepts_only_admin(monkeypatch, value):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", value)
    assert not notebook_sessions_allowed(ADMIN)
