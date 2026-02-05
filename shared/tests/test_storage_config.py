"""Tests for shared_directory and user_files_directory in storage_config."""

import os
from pathlib import Path

import pytest


class TestSharedDirectory:
    def test_shared_directory_default(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        """Without env vars, shared_directory is under base_directory."""
        monkeypatch.delenv("FLOWFILE_SHARED_DIR", raising=False)
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        monkeypatch.delenv("FLOWFILE_STORAGE_DIR", raising=False)

        from shared.storage_config import FlowfileStorage

        monkeypatch.setenv("FLOWFILE_STORAGE_DIR", str(tmp_path / "base"))
        storage = FlowfileStorage()
        assert storage.shared_directory == tmp_path / "base" / "shared"

    def test_shared_directory_from_env_var(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        """FLOWFILE_SHARED_DIR env var overrides the default."""
        custom_shared = tmp_path / "custom_shared"
        monkeypatch.setenv("FLOWFILE_SHARED_DIR", str(custom_shared))

        from shared.storage_config import FlowfileStorage

        storage = FlowfileStorage()
        assert storage.shared_directory == custom_shared

    def test_shared_directory_docker_mode(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        """In Docker mode without env var, shared_directory is /shared."""
        monkeypatch.delenv("FLOWFILE_SHARED_DIR", raising=False)
        monkeypatch.setenv("FLOWFILE_MODE", "docker")

        from shared.storage_config import FlowfileStorage

        storage = FlowfileStorage()
        assert storage.shared_directory == Path("/shared")

    def test_user_files_directory(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        """user_files_directory is always shared_directory / 'user_files'."""
        custom_shared = tmp_path / "custom_shared"
        monkeypatch.setenv("FLOWFILE_SHARED_DIR", str(custom_shared))

        from shared.storage_config import FlowfileStorage

        storage = FlowfileStorage()
        assert storage.user_files_directory == custom_shared / "user_files"

    def test_ensure_directories_creates_shared(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        """_ensure_directories creates the shared and user_files directories."""
        custom_shared = tmp_path / "custom_shared"
        monkeypatch.setenv("FLOWFILE_SHARED_DIR", str(custom_shared))

        from shared.storage_config import FlowfileStorage

        storage = FlowfileStorage()
        assert custom_shared.exists()
        assert (custom_shared / "user_files").exists()
