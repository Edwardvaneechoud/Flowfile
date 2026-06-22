"""Admin-gate tests for the /project router (I5-adjacent hardening).

In docker mode the git-tracking router is admin-only: non-admins must not even see it (404, matching
require_projects_enabled's no-disclosure style). electron/package are single-user → unrestricted.

The router-gate dependencies are unit-callable; we exercise them directly like TestMP3_DockerRouterGate.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from flowfile_core.configs import settings as s
from flowfile_core.routes.project import require_projects_admin, require_projects_enabled

_ADMIN = SimpleNamespace(id=1, username="admin", is_admin=True)
_NON_ADMIN = SimpleNamespace(id=2, username="alice", is_admin=False)


class TestProjectAdminGate:
    def test_docker_flag_on_non_admin_is_blocked(self, monkeypatch):
        monkeypatch.setattr(s, "FLOWFILE_MODE", "docker")
        s.FLOWFILE_ENABLE_PROJECTS.set(True)
        try:
            require_projects_enabled()  # flag on → not the blocker
            with pytest.raises(HTTPException) as exc_info:
                require_projects_admin(_NON_ADMIN)
            assert exc_info.value.status_code == 404
        finally:
            s.FLOWFILE_ENABLE_PROJECTS.set(False)

    def test_docker_flag_on_admin_is_reachable(self, monkeypatch):
        monkeypatch.setattr(s, "FLOWFILE_MODE", "docker")
        s.FLOWFILE_ENABLE_PROJECTS.set(True)
        try:
            require_projects_enabled()
            require_projects_admin(_ADMIN)  # must not raise
        finally:
            s.FLOWFILE_ENABLE_PROJECTS.set(False)

    def test_docker_flag_off_blocks_regardless_of_admin(self, monkeypatch):
        monkeypatch.setattr(s, "FLOWFILE_MODE", "docker")
        s.FLOWFILE_ENABLE_PROJECTS.set(False)
        try:
            with pytest.raises(HTTPException) as exc_info:
                require_projects_enabled()
            assert exc_info.value.status_code == 404
            require_projects_admin(_ADMIN)  # admin gate alone does not block; the flag gate already 404s
        finally:
            s.FLOWFILE_ENABLE_PROJECTS.set(False)

    def test_electron_non_admin_is_unrestricted(self, monkeypatch):
        monkeypatch.setattr(s, "FLOWFILE_MODE", "electron")
        require_projects_enabled()  # must not raise
        require_projects_admin(_NON_ADMIN)  # must not raise

    def test_package_non_admin_is_unrestricted(self, monkeypatch):
        monkeypatch.setattr(s, "FLOWFILE_MODE", "package")
        require_projects_enabled()  # must not raise
        require_projects_admin(_NON_ADMIN)  # must not raise


class TestI5FolderPathRelativized:
    """I5 — POST /init|/open and GET /active must not echo the absolute server path in docker mode."""

    def _project(self, tmp_path, owner_id=7):
        from flowfile_core.project.models import ActiveProject

        root = tmp_path / "projects" / str(owner_id) / "myproj"
        root.mkdir(parents=True)
        return ActiveProject(
            id=1, name="myproj", root=root, owner_id=owner_id, track_data_artifacts=True
        )

    def test_docker_payload_is_relative_to_owner_base(self, tmp_path, monkeypatch):
        from flowfile_core.routes import project as route

        project = self._project(tmp_path)
        base = (tmp_path / "projects" / str(project.owner_id)).resolve()
        monkeypatch.setattr(route, "project_root_base", lambda owner_id: base)

        payload = route._payload(project)
        assert payload.folder_path == "myproj"
        assert str(tmp_path) not in payload.folder_path

    def test_electron_payload_is_absolute(self, tmp_path, monkeypatch):
        from flowfile_core.routes import project as route

        project = self._project(tmp_path)
        monkeypatch.setattr(route, "project_root_base", lambda owner_id: None)

        payload = route._payload(project)
        assert payload.folder_path == str(project.root)
