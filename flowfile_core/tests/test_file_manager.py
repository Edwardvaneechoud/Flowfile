"""Tests for the File Manager API endpoints.

Covers file listing, upload (with extension/size validation, unique-name
collision handling and streaming size limits), deletion, Docker-mode gating,
and path traversal prevention.
"""

import io
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.routes import file_manager as fm_module

# Keep a reference to the real check before any patching.
_real_check_docker_mode = fm_module._check_docker_mode


# Helpers


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


def _upload_test_file(name: str = "test.csv", content: bytes = b"a,b\n1,2\n", **params) -> dict:
    """Helper to upload a file and return the JSON response."""
    resp = client.post(
        f"{PREFIX}/upload",
        files={"file": (name, io.BytesIO(content), "application/octet-stream")},
        params=params,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# Fixtures


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


# Docker-mode gating


class TestDockerModeGating:
    """Listing and deletion are Docker-only; upload and limits are not."""

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

    def test_upload_allowed_in_electron_mode(self):
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("test_electron_upload.csv", io.BytesIO(b"data"), "text/csv")},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["filename"] == "test_electron_upload.csv"

    def test_limits_allowed_in_electron_mode(self):
        resp = client.get(f"{PREFIX}/limits")
        assert resp.status_code == 200
        assert resp.json()["max_file_size_bytes"] == fm_module.MAX_FILE_SIZE

    def test_delete_blocked_in_electron_mode(self):
        resp = client.delete(f"{PREFIX}/files/test.csv")
        assert resp.status_code == 403


# Upload limits


class TestLimits:
    def test_limits_reports_max_size_and_extensions(self):
        resp = client.get(f"{PREFIX}/limits")
        assert resp.status_code == 200
        data = resp.json()
        assert data["max_file_size_bytes"] == fm_module.MAX_FILE_SIZE
        expected = {"csv", "tsv", "txt", "parquet", "xlsx", "xls", "ipc", "arrow", "feather", "ndjson", "jsonl", "avro"}
        assert expected <= set(data["allowed_extensions"])
        assert data["allowed_extensions"] == sorted(data["allowed_extensions"])


# List files


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


# Upload


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

    @pytest.mark.parametrize("ext", ["ipc", "arrow", "feather", "ndjson", "jsonl", "avro", "tsv"])
    def test_upload_accepts_data_extension(self, ext):
        data = _upload_test_file(f"test_ext.{ext}", b"\x00" * 32)
        assert data["filename"] == f"test_ext.{ext}"

    def test_upload_returns_absolute_filepath(self):
        data = _upload_test_file("test_abs.csv")
        assert os.path.isabs(data["filepath"])
        assert os.path.basename(data["filepath"]) == data["filename"]


class TestUniqueUpload:
    def test_unique_suffixes_collisions(self, _docker_mode_with_tmp_uploads):
        uploads_dir = _docker_mode_with_tmp_uploads
        names = [_upload_test_file("u.csv", f"body-{i}".encode(), unique=True)["filename"] for i in range(3)]
        assert names == ["u.csv", "u (1).csv", "u (2).csv"]
        for i, name in enumerate(names):
            assert (uploads_dir / name).read_bytes() == f"body-{i}".encode()

    def test_without_unique_overwrites(self, _docker_mode_with_tmp_uploads):
        uploads_dir = _docker_mode_with_tmp_uploads
        _upload_test_file("o.csv", b"old")
        data = _upload_test_file("o.csv", b"new")
        assert data["filename"] == "o.csv"
        assert (uploads_dir / "o.csv").read_bytes() == b"new"
        assert not (uploads_dir / "o (1).csv").exists()


class TestUploadSizeLimit:
    def test_oversized_upload_rejected_and_leaves_no_file(self, monkeypatch, _docker_mode_with_tmp_uploads):
        monkeypatch.setattr(fm_module, "MAX_FILE_SIZE", 16)
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("test_big.csv", io.BytesIO(b"x" * 1024), "text/csv")},
        )
        assert resp.status_code == 400
        assert "too large" in resp.json()["detail"].lower()
        assert not (_docker_mode_with_tmp_uploads / "test_big.csv").exists()


class TestOverwriteDurability:
    """A failed overwrite must leave the pre-existing file untouched (no truncate/delete)."""

    @staticmethod
    def _assert_only_keep_csv_intact(uploads_dir):
        assert (uploads_dir / "keep.csv").read_bytes() == b"precious"
        assert [p.name for p in uploads_dir.iterdir()] == ["keep.csv"]

    def test_oversized_upload_preserves_existing_file(self, monkeypatch, _docker_mode_with_tmp_uploads):
        uploads_dir = _docker_mode_with_tmp_uploads
        _upload_test_file("keep.csv", b"precious")
        monkeypatch.setattr(fm_module, "MAX_FILE_SIZE", 4)
        resp = client.post(
            f"{PREFIX}/upload",
            files={"file": ("keep.csv", io.BytesIO(b"x" * 1024), "text/csv")},
        )
        assert resp.status_code == 400
        self._assert_only_keep_csv_intact(uploads_dir)

    def test_failed_write_preserves_existing_file(self, monkeypatch, _docker_mode_with_tmp_uploads):
        uploads_dir = _docker_mode_with_tmp_uploads
        _upload_test_file("keep.csv", b"precious")

        async def _boom(file, fd):
            os.close(fd)
            raise OSError("disk full")

        monkeypatch.setattr(fm_module, "_write_upload", _boom)
        with pytest.raises(OSError):
            client.post(
                f"{PREFIX}/upload",
                files={"file": ("keep.csv", io.BytesIO(b"new bytes"), "text/csv")},
            )
        self._assert_only_keep_csv_intact(uploads_dir)


# Delete


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


# Integration: upload → list → delete round-trip


class TestRoundTrip:
    def test_upload_list_delete_cycle(self):
        _upload_test_file("test_roundtrip.csv", b"x,y\n1,2\n")

        resp = client.get(f"{PREFIX}/files")
        assert resp.status_code == 200
        names = [f["name"] for f in resp.json()]
        assert "test_roundtrip.csv" in names

        resp = client.delete(f"{PREFIX}/files/test_roundtrip.csv")
        assert resp.status_code == 200

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
