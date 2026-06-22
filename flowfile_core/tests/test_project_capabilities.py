"""Capability surfacing for the project feature: the /health/status flags the frontend reads to
decide whether to show the Projects UI / lock the folder picker, and the GET /project/root endpoint
that hands the picker its confined base.

settings.FLOWFILE_MODE is a module-level constant bound at import time, so we patch it with
monkeypatch.setattr (setenv would not change the already-bound constant).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from flowfile_core.configs import settings as s


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,enable,exp_enabled,exp_confined",
    [
        ("electron", False, True, False),
        ("package", False, True, True),
        ("docker", False, False, True),
        ("docker", True, True, True),
    ],
)
async def test_health_status_project_flags(monkeypatch, mode, enable, exp_enabled, exp_confined):
    from flowfile_core.routes.public import get_setup_status

    monkeypatch.setattr(s, "FLOWFILE_MODE", mode)
    s.FLOWFILE_ENABLE_PROJECTS.set(enable)
    try:
        status = await get_setup_status()
        assert status.projects_enabled == exp_enabled
        assert status.projects_confined == exp_confined
    finally:
        s.FLOWFILE_ENABLE_PROJECTS.set(False)


def test_get_project_root_creates_confined_base(tmp_path, monkeypatch):
    from flowfile_core.routes import project as route

    base = tmp_path / "projects" / "5"
    monkeypatch.setattr(route, "project_root_base", lambda owner_id: base.resolve())
    out = route.get_project_root(SimpleNamespace(id=5, is_admin=True))

    assert out.root == str(base.resolve())
    assert base.exists(), "project root base must be created on demand so the picker can list it"


def test_get_project_root_electron_returns_user_data(monkeypatch):
    from flowfile_core.routes import project as route

    monkeypatch.setattr(route, "project_root_base", lambda owner_id: None)
    out = route.get_project_root(SimpleNamespace(id=1, is_admin=True))

    assert out.root == str(route.storage.user_data_directory)
