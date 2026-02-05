"""Tests for _translate_host_path_to_container in flowfile_client."""

import pytest

from kernel_runtime.flowfile_client import _translate_host_path_to_container


class TestTranslateHostPathToContainer:
    def test_translate_when_env_var_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        """When FLOWFILE_HOST_SHARED_DIR is set, paths under it are translated."""
        host_shared = tmp_path / "host_shared"
        host_shared.mkdir()
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", str(host_shared))

        host_path = str(host_shared / "user_files" / "export.csv")
        result = _translate_host_path_to_container(host_path)
        assert result == "/shared/user_files/export.csv"

    def test_translate_when_env_var_not_set(self, monkeypatch: pytest.MonkeyPatch):
        """When FLOWFILE_HOST_SHARED_DIR is not set, the path is returned as-is."""
        monkeypatch.delenv("FLOWFILE_HOST_SHARED_DIR", raising=False)

        path = "/some/random/path/file.csv"
        result = _translate_host_path_to_container(path)
        assert result == path

    def test_translate_path_not_under_shared_dir(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        """When the path is not under the shared dir, it's returned as-is."""
        host_shared = tmp_path / "host_shared"
        host_shared.mkdir()
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", str(host_shared))

        other_path = "/completely/different/path/file.csv"
        result = _translate_host_path_to_container(other_path)
        assert result == other_path

    def test_translate_nested_path(self, monkeypatch: pytest.MonkeyPatch, tmp_path):
        """Nested paths within the shared dir are correctly translated."""
        host_shared = tmp_path / "host_shared"
        host_shared.mkdir()
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", str(host_shared))

        host_path = str(host_shared / "global_artifacts" / "1" / "model.joblib")
        result = _translate_host_path_to_container(host_path)
        assert result == "/shared/global_artifacts/1/model.joblib"
