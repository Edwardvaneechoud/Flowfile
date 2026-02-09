"""Tests for the File Manager API endpoints.

Covers file listing, upload (with extension/size validation), deletion,
Docker-mode gating, and path traversal prevention.
"""

import io
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.routes import file_manager as fm_module

# Keep a reference to the real check before any patching.
_real_check_docker_mode = fm_module._check_docker_mode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


def _get_test_client() -> TestClient:
    token = _get_auth_token()
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _get_test_client()

PREFIX = "/file_manager"


def _upload_test_file(name: str = "test.csv", content: bytes = b"a,b\n1,2\n") -> dict:
    """Helper to upload a file and return the JSON response."""
    resp = client.post(
        f"{PREFIX}/upload",
        files={"file": (name, io.BytesIO(content), "application/octet-stream")},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _docker_mode_with_tmp_uploads(tmp_path):
    """Patch Docker-mode check to pass + redirect uploads to a temp directory.

    The ``storage`` singleton caches its directories at import time (in
    electron mode), so we cannot simply flip ``FLOWFILE_MODE``.  Instead we:

    1. No-op the ``_check_docker_mode`` guard so endpoints are reachable.
    2. Monkey-patch ``storage.uploads_directory`` to return a fresh temp dir
       so files written during tests never touch real user data.
    """
    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()

    with (
        patch.object(fm_module, "_check_docker_mode", return_value=None),
        patch.object(
            type(fm_module.storage),
            "uploads_directory",
            new=property(lambda self: uploads_dir),
        ),
    ):
        yield uploads_dir

    # tmp_path cleanup is automatic


# ---------------------------------------------------------------------------
# Docker-mode gating
# ---------------------------------------------------------------------------


class TestDockerModeGating:
    """Endpoints must return 403 when not in Docker mode."""

    @pytest.fixture(autouse=True)
    def _restore_real_check(self, monkeypatch):
        """Re-install the real ``_check_docker_mode`` (undoing the autouse
        no-op) and ensure FLOWFILE_MODE is *not* docker."""
        monkeypatch.setattr(fm_module, "_check_docker_mode", _real_check_docker_mode)
        monkeypatch.setenv("FLOWFILE_MODE", "electron")

    def test_list_files_blocked_in_electron_mode(self):
        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 403
        assert "Docker mode" in resp.json()["detail"]

    def test_upload_blocked_in_electron_mode(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("test.csv", io.BytesIO(b"data"), "text/csv")},
        )
        assert resp.status_code == 403

    def test_delete_blocked_in_electron_mode(self):
        resp = client.delete(f"{PREFIX}/files/test.csv")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# List files
# ---------------------------------------------------------------------------


class TestListFiles:
    def test_list_empty_directory(self):
        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_list_shows_uploaded_file(self):
        _upload_test_file("test_list.csv")
        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 200
        names = [f["name"] for f in resp.json()]
        assert "test_list.csv" in names

    def test_list_only_shows_allowed_types(self, _docker_mode_with_tmp_uploads):
        """Even if a non-allowed file sneaks in, listing filters by extension."""
        uploads_dir = _docker_mode_with_tmp_uploads
        sneaky = uploads_dir / "test_sneaky.py"
        sneaky.write_text("import os")
        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 200
        names = [f["name"] for f in resp.json()]
        assert "test_sneaky.py" not in names


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


class TestUpload:
    def test_upload_csv(self):
        data = _upload_test_file("test_upload.csv", b"col1,col2\na,b\n")
        assert data["filename"] == "test_upload.csv"
        assert data["size"] > 0

    def test_upload_parquet(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={
                "file": (
                    "test_upload.parquet",
                    io.BytesIO(b"PAR1" + b"\x00" * 100),
                    "application/octet-stream",
                )
            },
        )
        assert resp.status_code == 200
        assert resp.json()["filename"] == "test_upload.parquet"

    def test_upload_json(self):
        data = _upload_test_file("test_upload.json", b'{"key": "value"}')
        assert data["filename"] == "test_upload.json"

    def test_upload_txt(self):
        data = _upload_test_file("test_upload.txt", b"hello world")
        assert data["filename"] == "test_upload.txt"

    def test_upload_tsv(self):
        data = _upload_test_file("test_upload.tsv", b"a\tb\n1\t2\n")
        assert data["filename"] == "test_upload.tsv"

    def test_upload_xlsx(self):
        data = _upload_test_file("test_upload.xlsx", b"\x00" * 50)
        assert data["filename"] == "test_upload.xlsx"

    def test_upload_xls(self):
        data = _upload_test_file("test_upload.xls", b"\x00" * 50)
        assert data["filename"] == "test_upload.xls"

    def test_upload_rejected_for_disallowed_extension(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("test_bad.py", io.BytesIO(b"import os"), "text/plain")},
        )
        assert resp.status_code == 400
        assert "not allowed" in resp.json()["detail"]

    def test_upload_rejected_for_exe(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("test_bad.exe", io.BytesIO(b"\x00"), "application/octet-stream")},
        )
        assert resp.status_code == 400

    def test_upload_rejected_for_no_extension(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("testfile", io.BytesIO(b"data"), "application/octet-stream")},
        )
        assert resp.status_code == 400

    def test_upload_sanitizes_path_traversal_in_filename(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("../../etc/test_traversal.csv", io.BytesIO(b"a,b"), "text/csv")},
        )
        # Should either reject or sanitize to just the basename
        if resp.status_code == 200:
            assert resp.json()["filename"] == "test_traversal.csv"

    def test_upload_overwrites_existing_file(self):
        _upload_test_file("test_overwrite.csv", b"old")
        data = _upload_test_file("test_overwrite.csv", b"new,data")
        assert data["filename"] == "test_overwrite.csv"
        assert data["size"] == len(b"new,data")

    def test_upload_returns_file_size(self):
        content = b"a,b,c\n1,2,3\n4,5,6\n"
        data = _upload_test_file("test_size.csv", content)
        assert data["size"] == len(content)


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_existing_file(self):
        _upload_test_file("test_delete.csv")
        resp = client.delete(f"{PREFIX}/files/test_delete.csv")
        assert resp.status_code == 200
        assert "deleted" in resp.json()["message"]

    def test_delete_nonexistent_file(self):
        resp = client.delete(f"{PREFIX}/files/test_no_such_file.csv")
        assert resp.status_code == 404

    def test_delete_file_actually_removed(self):
        _upload_test_file("test_gone.csv")
        client.delete(f"{PREFIX}/files/test_gone.csv")
        resp = client.get(f"{PREFIX}/files")
        names = [f["name"] for f in resp.json()]
        assert "test_gone.csv" not in names

    def test_delete_rejects_path_traversal(self):
        resp = client.delete(f"{PREFIX}/files/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 404)

    def test_delete_rejects_slash_in_filename(self):
        resp = client.delete(f"{PREFIX}/files/sub/test.csv")
        # FastAPI path param won't match or the handler rejects it
        assert resp.status_code in (400, 404, 422)

    def test_delete_rejects_directory(self, _docker_mode_with_tmp_uploads):
        """Cannot delete a directory via the delete endpoint."""
        uploads_dir = _docker_mode_with_tmp_uploads
        subdir = uploads_dir / "test_subdir"
        subdir.mkdir(exist_ok=True)
        resp = client.delete(f"{PREFIX}/files/test_subdir")
        assert resp.status_code == 400
        assert "directories" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Integration: upload → list → delete round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_upload_list_delete_cycle(self):
        # Upload
        _upload_test_file("test_roundtrip.csv", b"x,y\n1,2\n")

        # Verify in list
        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 200
        names = [f["name"] for f in resp.json()]
        assert "test_roundtrip.csv" in names

        # Delete
        resp = client.delete(f"{PREFIX}/files/test_roundtrip.csv")
        assert resp.status_code == 200

        # Verify removed from list
        resp = client.get(f"{PREFIX}/files")
        names = [f["name"] for f in resp.json()]
        assert "test_roundtrip.csv" not in names

    def test_upload_multiple_files(self):
        _upload_test_file("test_multi_a.csv", b"a\n1\n")
        _upload_test_file("test_multi_b.json", b'{"k":1}')

        resp = client.get(f"{PREFIX}/files")
        names = [f["name"] for f in resp.json()]
        assert "test_multi_a.csv" in names
        assert "test_multi_b.json" in names
