"""Tests for global artifact client functions.

Covers:
- publish_global
- get_global
- list_global_artifacts
- delete_global_artifact
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kernel_runtime.flowfile_client import (
    _CORE_URL,
    delete_global_artifact,
    get_global,
    list_global_artifacts,
    publish_global,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_httpx_client():
    """Mock httpx.Client for testing without actual HTTP calls."""
    with patch("kernel_runtime.flowfile_client.httpx.Client") as mock:
        yield mock


@pytest.fixture
def mock_prepare_response():
    """Standard prepare-upload response."""
    return {
        "artifact_id": 1,
        "version": 1,
        "method": "file",
        "path": "/tmp/staging/1_test.pkl",
        "storage_key": "1/test.pkl",
    }


@pytest.fixture
def mock_artifact_response():
    """Standard artifact metadata response."""
    return {
        "id": 1,
        "name": "test_artifact",
        "namespace_id": None,
        "version": 1,
        "status": "active",
        "owner_id": 1,
        "python_type": "builtins.dict",
        "serialization_format": "pickle",
        "download_source": {
            "method": "file",
            "path": "/tmp/artifacts/1/test.pkl",
        },
        "sha256": "abc123",
    }


# ---------------------------------------------------------------------------
# publish_global Tests
# ---------------------------------------------------------------------------


class TestPublishGlobal:
    """Tests for publish_global function."""

    def test_publish_dict_object(self, mock_httpx_client, tmp_path):
        """Should publish a dict object successfully."""
        # Setup mock
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        # Mock prepare response
        prepare_response = MagicMock()
        prepare_response.json.return_value = {
            "artifact_id": 1,
            "version": 1,
            "method": "file",
            "path": str(tmp_path / "staging" / "1_test.pkl"),
            "storage_key": "1/test.pkl",
        }
        prepare_response.raise_for_status = MagicMock()

        # Mock finalize response
        finalize_response = MagicMock()
        finalize_response.raise_for_status = MagicMock()

        mock_client.post.side_effect = [prepare_response, finalize_response]

        # Create staging dir
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        # Test
        obj = {"key": "value", "number": 42}
        artifact_id = publish_global("test_artifact", obj)

        assert artifact_id == 1

        # Verify prepare-upload was called
        calls = mock_client.post.call_args_list
        assert len(calls) == 2

        # Check prepare call
        prepare_call = calls[0]
        assert "prepare-upload" in prepare_call[0][0]
        prepare_json = prepare_call[1]["json"]
        assert prepare_json["name"] == "test_artifact"
        assert prepare_json["serialization_format"] == "pickle"

        # Check finalize call
        finalize_call = calls[1]
        assert "finalize" in finalize_call[0][0]
        finalize_json = finalize_call[1]["json"]
        assert finalize_json["artifact_id"] == 1
        assert finalize_json["storage_key"] == "1/test.pkl"
        assert len(finalize_json["sha256"]) == 64

    def test_publish_with_metadata(self, mock_httpx_client, tmp_path):
        """Should include description and tags in publish."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        prepare_response = MagicMock()
        prepare_response.json.return_value = {
            "artifact_id": 1,
            "version": 1,
            "method": "file",
            "path": str(tmp_path / "1_test.pkl"),
            "storage_key": "1/test.pkl",
        }
        prepare_response.raise_for_status = MagicMock()

        finalize_response = MagicMock()
        finalize_response.raise_for_status = MagicMock()

        mock_client.post.side_effect = [prepare_response, finalize_response]

        (tmp_path).mkdir(parents=True, exist_ok=True)

        publish_global(
            "test",
            {"data": 1},
            description="A test artifact",
            tags=["ml", "production"],
            namespace_id=5,
        )

        prepare_call = mock_client.post.call_args_list[0]
        prepare_json = prepare_call[1]["json"]

        assert prepare_json["description"] == "A test artifact"
        assert prepare_json["tags"] == ["ml", "production"]
        assert prepare_json["namespace_id"] == 5

    def test_publish_stores_python_type(self, mock_httpx_client, tmp_path):
        """Should capture Python type information."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        prepare_response = MagicMock()
        prepare_response.json.return_value = {
            "artifact_id": 1,
            "version": 1,
            "method": "file",
            "path": str(tmp_path / "1_test.pkl"),
            "storage_key": "1/test.pkl",
        }
        prepare_response.raise_for_status = MagicMock()

        finalize_response = MagicMock()
        finalize_response.raise_for_status = MagicMock()

        mock_client.post.side_effect = [prepare_response, finalize_response]

        (tmp_path).mkdir(parents=True, exist_ok=True)

        class CustomClass:
            pass

        obj = CustomClass()
        publish_global("custom", obj)

        prepare_call = mock_client.post.call_args_list[0]
        prepare_json = prepare_call[1]["json"]

        assert "CustomClass" in prepare_json["python_type"]


# ---------------------------------------------------------------------------
# get_global Tests
# ---------------------------------------------------------------------------


class TestGetGlobal:
    """Tests for get_global function."""

    def test_get_artifact_by_name(self, mock_httpx_client, tmp_path):
        """Should retrieve artifact by name."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        # Create test artifact file
        artifact_path = tmp_path / "artifacts" / "1" / "test.pkl"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

        import pickle
        with open(artifact_path, "wb") as f:
            pickle.dump({"key": "value"}, f)

        # Mock response
        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "id": 1,
            "name": "test_artifact",
            "serialization_format": "pickle",
            "download_source": {
                "method": "file",
                "path": str(artifact_path),
            },
        }
        get_response.raise_for_status = MagicMock()

        mock_client.get.return_value = get_response

        result = get_global("test_artifact")

        assert result == {"key": "value"}

        # Verify correct endpoint called
        get_call = mock_client.get.call_args
        assert "by-name/test_artifact" in get_call[0][0]

    def test_get_specific_version(self, mock_httpx_client, tmp_path):
        """Should request specific version when provided."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        artifact_path = tmp_path / "test.pkl"
        import pickle
        with open(artifact_path, "wb") as f:
            pickle.dump("v1 data", f)

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "id": 1,
            "name": "test",
            "version": 1,
            "serialization_format": "pickle",
            "download_source": {"method": "file", "path": str(artifact_path)},
        }
        get_response.raise_for_status = MagicMock()

        mock_client.get.return_value = get_response

        get_global("test", version=1)

        get_call = mock_client.get.call_args
        assert get_call[1]["params"]["version"] == 1

    def test_get_not_found_raises_key_error(self, mock_httpx_client):
        """Should raise KeyError when artifact not found."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        get_response = MagicMock()
        get_response.status_code = 404

        mock_client.get.return_value = get_response

        with pytest.raises(KeyError, match="not found"):
            get_global("nonexistent")

    def test_get_with_namespace_filter(self, mock_httpx_client, tmp_path):
        """Should include namespace_id in request params."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        artifact_path = tmp_path / "test.pkl"
        import pickle
        with open(artifact_path, "wb") as f:
            pickle.dump({}, f)

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "id": 1,
            "serialization_format": "pickle",
            "download_source": {"method": "file", "path": str(artifact_path)},
        }
        get_response.raise_for_status = MagicMock()

        mock_client.get.return_value = get_response

        get_global("test", namespace_id=5)

        get_call = mock_client.get.call_args
        assert get_call[1]["params"]["namespace_id"] == 5


# ---------------------------------------------------------------------------
# list_global_artifacts Tests
# ---------------------------------------------------------------------------


class TestListGlobalArtifacts:
    """Tests for list_global_artifacts function."""

    def test_list_all_artifacts(self, mock_httpx_client):
        """Should list all artifacts."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        list_response = MagicMock()
        list_response.json.return_value = [
            {"id": 1, "name": "artifact1", "version": 1},
            {"id": 2, "name": "artifact2", "version": 1},
        ]
        list_response.raise_for_status = MagicMock()

        mock_client.get.return_value = list_response

        result = list_global_artifacts()

        assert len(result) == 2
        assert result[0]["name"] == "artifact1"
        assert result[1]["name"] == "artifact2"

    def test_list_with_namespace_filter(self, mock_httpx_client):
        """Should filter by namespace."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        list_response = MagicMock()
        list_response.json.return_value = [{"id": 1, "name": "ns_artifact"}]
        list_response.raise_for_status = MagicMock()

        mock_client.get.return_value = list_response

        list_global_artifacts(namespace_id=5)

        get_call = mock_client.get.call_args
        assert get_call[1]["params"]["namespace_id"] == 5

    def test_list_with_tags_filter(self, mock_httpx_client):
        """Should filter by tags."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        list_response = MagicMock()
        list_response.json.return_value = []
        list_response.raise_for_status = MagicMock()

        mock_client.get.return_value = list_response

        list_global_artifacts(tags=["ml", "production"])

        get_call = mock_client.get.call_args
        assert get_call[1]["params"]["tags"] == ["ml", "production"]


# ---------------------------------------------------------------------------
# delete_global_artifact Tests
# ---------------------------------------------------------------------------


class TestDeleteGlobalArtifact:
    """Tests for delete_global_artifact function."""

    def test_delete_all_versions_by_name(self, mock_httpx_client):
        """Should delete all versions when version not specified."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        delete_response = MagicMock()
        delete_response.status_code = 200
        delete_response.raise_for_status = MagicMock()

        mock_client.delete.return_value = delete_response

        delete_global_artifact("test_artifact")

        delete_call = mock_client.delete.call_args
        assert "by-name/test_artifact" in delete_call[0][0]

    def test_delete_specific_version(self, mock_httpx_client):
        """Should delete specific version when specified."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        # Mock get to retrieve artifact ID
        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {"id": 42}
        get_response.raise_for_status = MagicMock()

        # Mock delete
        delete_response = MagicMock()
        delete_response.raise_for_status = MagicMock()

        mock_client.get.return_value = get_response
        mock_client.delete.return_value = delete_response

        delete_global_artifact("test", version=1)

        # Should get artifact to find ID first
        get_call = mock_client.get.call_args
        assert get_call[1]["params"]["version"] == 1

        # Then delete by ID
        delete_call = mock_client.delete.call_args
        assert "/42" in delete_call[0][0]

    def test_delete_not_found_raises_key_error(self, mock_httpx_client):
        """Should raise KeyError when artifact not found."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        delete_response = MagicMock()
        delete_response.status_code = 404

        mock_client.delete.return_value = delete_response

        with pytest.raises(KeyError, match="not found"):
            delete_global_artifact("nonexistent")


# ---------------------------------------------------------------------------
# Integration Tests (with actual serialization)
# ---------------------------------------------------------------------------


class TestGlobalArtifactIntegration:
    """Integration tests using actual serialization but mocked HTTP."""

    @pytest.fixture
    def integration_setup(self, mock_httpx_client, tmp_path):
        """Setup for integration tests."""
        mock_client = MagicMock()
        mock_httpx_client.return_value.__enter__.return_value = mock_client

        staging_dir = tmp_path / "staging"
        artifacts_dir = tmp_path / "artifacts"
        staging_dir.mkdir(parents=True, exist_ok=True)
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        return {
            "mock_client": mock_client,
            "staging_dir": staging_dir,
            "artifacts_dir": artifacts_dir,
        }

    def test_publish_and_retrieve_roundtrip(self, integration_setup, tmp_path):
        """Should successfully publish and retrieve same object."""
        mock_client = integration_setup["mock_client"]
        staging_dir = integration_setup["staging_dir"]
        artifacts_dir = integration_setup["artifacts_dir"]

        artifact_file = artifacts_dir / "1" / "test.pkl"

        # Setup prepare response
        prepare_response = MagicMock()
        prepare_response.json.return_value = {
            "artifact_id": 1,
            "version": 1,
            "method": "file",
            "path": str(staging_dir / "1_test.pkl"),
            "storage_key": "1/test.pkl",
        }
        prepare_response.raise_for_status = MagicMock()

        # Setup finalize response
        finalize_response = MagicMock()
        finalize_response.raise_for_status = MagicMock()

        mock_client.post.side_effect = [prepare_response, finalize_response]

        # Publish
        original_obj = {"nested": {"data": [1, 2, 3]}, "value": 42}
        artifact_id = publish_global("test_roundtrip", original_obj)

        assert artifact_id == 1

        # Move file to artifacts dir (simulating finalize)
        staging_file = staging_dir / "1_test.pkl"
        if staging_file.exists():
            artifact_file.parent.mkdir(parents=True, exist_ok=True)
            staging_file.rename(artifact_file)

        # Setup get response
        get_response = MagicMock()
        get_response.status_code = 200
        get_response.json.return_value = {
            "id": 1,
            "name": "test_roundtrip",
            "serialization_format": "pickle",
            "download_source": {
                "method": "file",
                "path": str(artifact_file),
            },
        }
        get_response.raise_for_status = MagicMock()

        mock_client.get.return_value = get_response

        # Retrieve
        retrieved = get_global("test_roundtrip")

        assert retrieved == original_obj
