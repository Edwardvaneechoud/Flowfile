"""Tests for the Global Artifacts API endpoints.

Covers prepare-upload / finalize workflow, artifact lookup by name,
versioning, listing with filters, and deletion.
"""

import hashlib
import json
import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import GlobalArtifact, User


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


def _cleanup_artifacts():
    """Remove all artifact rows so tests start clean."""
    with get_db_context() as db:
        db.query(GlobalArtifact).delete()
        db.commit()


def _write_blob_to_staging(path: str, content: bytes) -> str:
    """Write content to the staging path and return its SHA-256."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    return hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_artifacts():
    """Ensure a clean artifact state for every test."""
    _cleanup_artifacts()
    yield
    _cleanup_artifacts()


# ---------------------------------------------------------------------------
# Prepare Upload Tests
# ---------------------------------------------------------------------------


class TestPrepareUpload:
    def test_prepare_upload_returns_target(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "test_model",
                "serialization_format": "pickle",
                "description": "A test artifact",
                "tags": ["test"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["artifact_id"] > 0
        assert data["version"] == 1
        assert data["method"] == "file"
        assert data["storage_key"] is not None
        assert data["path"] is not None

    def test_prepare_upload_increments_version(self):
        # First upload
        resp1 = client.post(
            "/artifacts/prepare-upload",
            json={"name": "versioned", "serialization_format": "pickle"},
        )
        assert resp1.status_code == 200
        v1 = resp1.json()
        assert v1["version"] == 1

        # Finalize first one to make it active
        blob = b"version1"
        sha256 = _write_blob_to_staging(v1["path"], blob)
        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": v1["artifact_id"],
                "storage_key": v1["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )

        # Second upload of same name
        resp2 = client.post(
            "/artifacts/prepare-upload",
            json={"name": "versioned", "serialization_format": "pickle"},
        )
        assert resp2.status_code == 200
        assert resp2.json()["version"] == 2

    def test_prepare_upload_parquet_extension(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "my_df", "serialization_format": "parquet"},
        )
        data = resp.json()
        assert data["storage_key"].endswith(".parquet")

    def test_prepare_upload_joblib_extension(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "my_model", "serialization_format": "joblib"},
        )
        data = resp.json()
        assert data["storage_key"].endswith(".joblib")


# ---------------------------------------------------------------------------
# Finalize Upload Tests
# ---------------------------------------------------------------------------


class TestFinalizeUpload:
    def test_finalize_activates_artifact(self):
        # Prepare
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "finalize_test", "serialization_format": "pickle"},
        )
        target = resp.json()

        # Write blob to staging
        blob = b"test data for finalization"
        sha256 = _write_blob_to_staging(target["path"], blob)

        # Finalize
        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["artifact_id"] == target["artifact_id"]

        # Verify it's active in DB
        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, target["artifact_id"])
            assert artifact.status == "active"
            assert artifact.sha256 == sha256

    def test_finalize_rejects_sha256_mismatch(self):
        # Prepare
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "bad_hash", "serialization_format": "pickle"},
        )
        target = resp.json()

        # Write blob
        blob = b"actual data"
        _write_blob_to_staging(target["path"], blob)

        # Finalize with wrong hash
        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "size_bytes": len(blob),
            },
        )
        assert resp.status_code == 400
        assert "SHA-256 mismatch" in resp.json()["detail"]

    def test_finalize_rejects_missing_blob(self):
        # Prepare but don't write anything
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "missing_blob", "serialization_format": "pickle"},
        )
        target = resp.json()

        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": "abc123",
                "size_bytes": 100,
            },
        )
        assert resp.status_code == 400

    def test_finalize_rejects_non_pending(self):
        # Prepare and finalize first
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "double_finalize", "serialization_format": "pickle"},
        )
        target = resp.json()
        blob = b"data"
        sha256 = _write_blob_to_staging(target["path"], blob)
        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )

        # Try to finalize again
        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Get by Name Tests
# ---------------------------------------------------------------------------


def _create_active_artifact(name: str, blob: bytes = b"test", **kwargs) -> dict:
    """Helper to create a fully active artifact."""
    resp = client.post(
        "/artifacts/prepare-upload",
        json={"name": name, "serialization_format": "pickle", **kwargs},
    )
    target = resp.json()
    sha256 = _write_blob_to_staging(target["path"], blob)
    client.post(
        "/artifacts/finalize",
        json={
            "artifact_id": target["artifact_id"],
            "storage_key": target["storage_key"],
            "sha256": sha256,
            "size_bytes": len(blob),
        },
    )
    return target


class TestGetByName:
    def test_get_latest_version(self):
        _create_active_artifact("lookup_test", b"v1")
        _create_active_artifact("lookup_test", b"v2")

        resp = client.get("/artifacts/by-name/lookup_test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "lookup_test"
        assert data["version"] == 2
        assert data["download_source"] is not None
        assert data["download_source"]["method"] == "file"

    def test_get_specific_version(self):
        _create_active_artifact("version_test", b"v1")
        _create_active_artifact("version_test", b"v2")

        resp = client.get("/artifacts/by-name/version_test", params={"version": 1})
        assert resp.status_code == 200
        assert resp.json()["version"] == 1

    def test_get_not_found(self):
        resp = client.get("/artifacts/by-name/nonexistent")
        assert resp.status_code == 404

    def test_get_returns_tags(self):
        _create_active_artifact("tagged", tags=["ml", "prod"])
        resp = client.get("/artifacts/by-name/tagged")
        assert resp.status_code == 200
        assert resp.json()["tags"] == ["ml", "prod"]


# ---------------------------------------------------------------------------
# List Artifacts Tests
# ---------------------------------------------------------------------------


class TestListArtifacts:
    def test_list_empty(self):
        resp = client.get("/artifacts/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_returns_active_only(self):
        _create_active_artifact("active_one", b"data1")
        _create_active_artifact("active_two", b"data2")

        # Create a pending one (not finalized)
        client.post(
            "/artifacts/prepare-upload",
            json={"name": "pending_one", "serialization_format": "pickle"},
        )

        resp = client.get("/artifacts/")
        assert resp.status_code == 200
        names = [a["name"] for a in resp.json()]
        assert "active_one" in names
        assert "active_two" in names
        assert "pending_one" not in names

    def test_list_filter_by_tags(self):
        _create_active_artifact("ml_model", tags=["ml", "production"])
        _create_active_artifact("data_file", tags=["data"])

        resp = client.get("/artifacts/", params={"tags": ["ml"]})
        assert resp.status_code == 200
        names = [a["name"] for a in resp.json()]
        assert "ml_model" in names
        assert "data_file" not in names

    def test_list_pagination(self):
        for i in range(5):
            _create_active_artifact(f"item_{i:02d}", f"data{i}".encode())

        resp = client.get("/artifacts/", params={"limit": 2, "offset": 0})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/artifacts/", params={"limit": 2, "offset": 4})
        assert resp.status_code == 200
        assert len(resp.json()) == 1


# ---------------------------------------------------------------------------
# Delete Tests
# ---------------------------------------------------------------------------


class TestDeleteArtifact:
    def test_delete_artifact(self):
        target = _create_active_artifact("delete_me")

        resp = client.delete(f"/artifacts/{target['artifact_id']}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "deleted"

        # Verify it's soft-deleted
        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, target["artifact_id"])
            assert artifact.status == "deleted"

        # Verify it doesn't appear in list
        resp = client.get("/artifacts/")
        names = [a["name"] for a in resp.json()]
        assert "delete_me" not in names

    def test_delete_not_found(self):
        resp = client.delete("/artifacts/99999")
        assert resp.status_code == 404

    def test_delete_removes_from_name_lookup(self):
        target = _create_active_artifact("gone_model")
        client.delete(f"/artifacts/{target['artifact_id']}")

        resp = client.get("/artifacts/by-name/gone_model")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Metadata Tests
# ---------------------------------------------------------------------------


class TestArtifactMetadata:
    def test_description_and_tags_stored(self):
        _create_active_artifact(
            "described",
            description="A well-described artifact",
            tags=["ml", "v2"],
        )
        resp = client.get("/artifacts/by-name/described")
        data = resp.json()
        assert data["description"] == "A well-described artifact"
        assert data["tags"] == ["ml", "v2"]

    def test_python_type_stored(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "typed",
                "serialization_format": "joblib",
                "python_type": "sklearn.ensemble.RandomForestClassifier",
                "python_module": "sklearn.ensemble",
            },
        )
        target = resp.json()
        blob = b"model bytes"
        sha256 = _write_blob_to_staging(target["path"], blob)
        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )

        resp = client.get("/artifacts/by-name/typed")
        data = resp.json()
        assert data["python_type"] == "sklearn.ensemble.RandomForestClassifier"
        assert data["python_module"] == "sklearn.ensemble"

    def test_lineage_stored(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "lineage_test",
                "serialization_format": "pickle",
                "source_flow_id": 42,
                "source_node_id": 7,
                "source_kernel_id": "kernel-abc-123",
            },
        )
        target = resp.json()
        blob = b"lineage data"
        sha256 = _write_blob_to_staging(target["path"], blob)
        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": len(blob),
            },
        )

        resp = client.get("/artifacts/by-name/lineage_test")
        data = resp.json()
        assert data["source_flow_id"] == 42
        assert data["source_node_id"] == 7
        assert data["source_kernel_id"] == "kernel-abc-123"
