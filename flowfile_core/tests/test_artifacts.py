"""Integration tests for the Global Artifacts API endpoints.

Covers:
- Upload workflow (prepare + finalize)
- Retrieval by name and ID
- Versioning
- Listing and filtering
- Deletion
- Error handling
"""

import json
import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, GlobalArtifact, User


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
    """Remove all global artifact rows so tests start clean."""
    with get_db_context() as db:
        db.query(GlobalArtifact).delete()
        db.commit()


def _cleanup_namespaces():
    """Remove test namespaces."""
    with get_db_context() as db:
        db.query(CatalogNamespace).filter(
            CatalogNamespace.name.like("ArtifactTest%")
        ).delete(synchronize_session=False)
        db.commit()


def _get_local_user_id() -> int:
    """Get the local user ID for testing."""
    with get_db_context() as db:
        user = db.query(User).filter_by(username="local_user").first()
        if user:
            return user.id
    return 1


def _create_test_namespace() -> int:
    """Create a test namespace and return its ID."""
    # Create catalog
    cat_resp = client.post(
        "/catalog/namespaces",
        json={"name": "ArtifactTestCatalog", "description": "Test catalog for artifacts"},
    )
    if cat_resp.status_code != 201:
        # May already exist, try to find it
        with get_db_context() as db:
            cat = db.query(CatalogNamespace).filter_by(
                name="ArtifactTestCatalog", parent_id=None
            ).first()
            if cat:
                schema = db.query(CatalogNamespace).filter_by(
                    name="ArtifactTestSchema", parent_id=cat.id
                ).first()
                if schema:
                    return schema.id
        raise Exception(f"Failed to create catalog: {cat_resp.text}")

    cat_id = cat_resp.json()["id"]

    # Create schema
    schema_resp = client.post(
        "/catalog/namespaces",
        json={"name": "ArtifactTestSchema", "parent_id": cat_id},
    )
    if schema_resp.status_code != 201:
        raise Exception(f"Failed to create schema: {schema_resp.text}")

    return schema_resp.json()["id"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_artifacts():
    """Ensure clean artifact state for every test."""
    _cleanup_artifacts()
    yield
    _cleanup_artifacts()


@pytest.fixture
def test_namespace() -> int:
    """Create a test namespace for artifact tests."""
    _cleanup_namespaces()
    ns_id = _create_test_namespace()
    yield ns_id
    _cleanup_namespaces()


@pytest.fixture
def staging_dir(tmp_path):
    """Create a temporary staging directory."""
    staging = tmp_path / "artifact_staging"
    staging.mkdir(parents=True, exist_ok=True)
    return staging


@pytest.fixture
def artifacts_dir(tmp_path):
    """Create a temporary artifacts directory."""
    artifacts = tmp_path / "global_artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    return artifacts


# ---------------------------------------------------------------------------
# Upload Workflow Tests
# ---------------------------------------------------------------------------


class TestPrepareUpload:
    """Tests for the /artifacts/prepare-upload endpoint."""

    def test_prepare_upload_creates_pending_artifact(self):
        """Prepare upload should create an artifact in pending status."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "test_model",
                "serialization_format": "pickle",
                "description": "A test artifact",
                "tags": ["test", "model"],
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["artifact_id"] > 0
        assert data["version"] == 1
        assert data["method"] == "file"
        assert data["storage_key"].startswith(str(data["artifact_id"]))

        # Verify DB record
        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, data["artifact_id"])
            assert artifact is not None
            assert artifact.status == "pending"
            assert artifact.name == "test_model"

    def test_prepare_upload_increments_version(self):
        """Each upload to same name should increment version."""
        # First upload
        resp1 = client.post(
            "/artifacts/prepare-upload",
            json={"name": "versioned_model", "serialization_format": "pickle"},
        )
        assert resp1.status_code == 201
        assert resp1.json()["version"] == 1

        # Finalize first upload to make it active
        self._finalize_artifact(resp1.json())

        # Second upload - should be version 2
        resp2 = client.post(
            "/artifacts/prepare-upload",
            json={"name": "versioned_model", "serialization_format": "pickle"},
        )
        assert resp2.status_code == 201
        assert resp2.json()["version"] == 2

    def test_prepare_upload_with_namespace(self, test_namespace):
        """Upload with namespace_id should associate artifact with namespace."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "namespaced_model",
                "serialization_format": "joblib",
                "namespace_id": test_namespace,
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.namespace_id == test_namespace

    def test_prepare_upload_invalid_namespace(self):
        """Upload with nonexistent namespace should return 404."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "invalid_ns_model",
                "serialization_format": "pickle",
                "namespace_id": 999999,
            },
        )
        assert resp.status_code == 404

    def test_prepare_upload_stores_python_type_info(self):
        """Python type and module info should be stored."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "typed_model",
                "serialization_format": "joblib",
                "python_type": "sklearn.ensemble.RandomForestClassifier",
                "python_module": "sklearn.ensemble",
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.python_type == "sklearn.ensemble.RandomForestClassifier"
            assert artifact.python_module == "sklearn.ensemble"

    def _finalize_artifact(self, prepare_response: dict):
        """Helper to finalize an artifact upload."""
        # Create a dummy file for testing
        path = Path(prepare_response["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test artifact data"
        path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prepare_response["artifact_id"],
                "storage_key": prepare_response["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )


class TestFinalizeUpload:
    """Tests for the /artifacts/finalize endpoint."""

    def test_finalize_activates_artifact(self, tmp_path):
        """Finalize should activate the artifact and store metadata."""
        # Prepare upload
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "finalize_test", "serialization_format": "pickle"},
        )
        assert prep_resp.status_code == 201
        prep_data = prep_resp.json()

        # Write test data to staging path
        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test artifact content for finalization"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        # Finalize
        fin_resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )
        assert fin_resp.status_code == 200
        fin_data = fin_resp.json()
        assert fin_data["status"] == "ok"
        assert fin_data["artifact_id"] == prep_data["artifact_id"]
        assert fin_data["version"] == 1

        # Verify DB record updated
        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, prep_data["artifact_id"])
            assert artifact.status == "active"
            assert artifact.sha256 == sha256
            assert artifact.size_bytes > 0

    def test_finalize_nonexistent_artifact(self):
        """Finalize for nonexistent artifact should return 404."""
        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": 999999,
                "storage_key": "fake/key",
                "sha256": "abc123",
                "size_bytes": 100,
            },
        )
        assert resp.status_code == 404

    def test_finalize_already_active_artifact(self, tmp_path):
        """Finalize on already active artifact should return 400."""
        # Create and finalize an artifact
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "double_finalize", "serialization_format": "pickle"},
        )
        prep_data = prep_resp.json()

        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        # First finalize - should succeed
        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )

        # Second finalize - should fail
        resp = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Retrieval Tests
# ---------------------------------------------------------------------------


class TestRetrieveArtifact:
    """Tests for artifact retrieval endpoints."""

    def _create_active_artifact(
        self,
        name: str,
        namespace_id: int | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """Helper to create an active artifact for testing."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "serialization_format": "pickle",
                "namespace_id": namespace_id,
                "tags": tags or [],
                "python_type": "builtins.dict",
            },
        )
        prep_data = prep_resp.json()

        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test artifact data"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )

        return prep_data["artifact_id"]

    def test_get_artifact_by_name(self):
        """Should retrieve artifact by name."""
        self._create_active_artifact("retrieve_by_name")

        resp = client.get("/artifacts/by-name/retrieve_by_name")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "retrieve_by_name"
        assert data["status"] == "active"
        assert data["download_source"] is not None
        assert data["download_source"]["method"] == "file"

    def test_get_artifact_by_name_not_found(self):
        """Should return 404 for nonexistent artifact."""
        resp = client.get("/artifacts/by-name/nonexistent_artifact")
        assert resp.status_code == 404

    def test_get_artifact_by_id(self):
        """Should retrieve artifact by ID."""
        artifact_id = self._create_active_artifact("retrieve_by_id")

        resp = client.get(f"/artifacts/{artifact_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == artifact_id
        assert data["name"] == "retrieve_by_id"

    def test_get_artifact_by_id_not_found(self):
        """Should return 404 for nonexistent artifact ID."""
        resp = client.get("/artifacts/999999")
        assert resp.status_code == 404

    def test_get_specific_version(self):
        """Should retrieve specific version of artifact."""
        # Create v1
        self._create_active_artifact("versioned_retrieve")
        # Create v2
        self._create_active_artifact("versioned_retrieve")

        # Get v1
        resp = client.get("/artifacts/by-name/versioned_retrieve", params={"version": 1})
        assert resp.status_code == 200
        assert resp.json()["version"] == 1

        # Get v2
        resp = client.get("/artifacts/by-name/versioned_retrieve", params={"version": 2})
        assert resp.status_code == 200
        assert resp.json()["version"] == 2

        # Get latest (should be v2)
        resp = client.get("/artifacts/by-name/versioned_retrieve")
        assert resp.status_code == 200
        assert resp.json()["version"] == 2

    def test_get_artifact_with_namespace_filter(self, test_namespace):
        """Should filter by namespace."""
        # Create artifact in namespace
        self._create_active_artifact("ns_filtered", namespace_id=test_namespace)
        # Create artifact without namespace
        self._create_active_artifact("ns_filtered")

        # Get with namespace filter - should find namespaced one
        resp = client.get(
            "/artifacts/by-name/ns_filtered",
            params={"namespace_id": test_namespace},
        )
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == test_namespace

        # Get without namespace filter - should find non-namespaced one
        resp = client.get("/artifacts/by-name/ns_filtered")
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] is None

    def test_get_artifact_versions(self):
        """Should retrieve artifact with all versions."""
        # Create multiple versions
        self._create_active_artifact("multi_version")
        self._create_active_artifact("multi_version")
        self._create_active_artifact("multi_version")

        resp = client.get("/artifacts/by-name/multi_version/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 3  # Latest version
        assert len(data["all_versions"]) == 3
        versions = [v["version"] for v in data["all_versions"]]
        assert sorted(versions, reverse=True) == [3, 2, 1]


# ---------------------------------------------------------------------------
# Listing Tests
# ---------------------------------------------------------------------------


class TestListArtifacts:
    """Tests for artifact listing endpoints."""

    def _create_active_artifact(
        self,
        name: str,
        namespace_id: int | None = None,
        tags: list[str] | None = None,
        python_type: str = "builtins.dict",
    ) -> int:
        """Helper to create an active artifact."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "serialization_format": "pickle",
                "namespace_id": namespace_id,
                "tags": tags or [],
                "python_type": python_type,
            },
        )
        prep_data = prep_resp.json()

        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )
        return prep_data["artifact_id"]

    def test_list_all_artifacts(self):
        """Should list all active artifacts."""
        self._create_active_artifact("list_test_1")
        self._create_active_artifact("list_test_2")

        resp = client.get("/artifacts/")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 2
        names = [a["name"] for a in data]
        assert "list_test_1" in names
        assert "list_test_2" in names

    def test_list_artifacts_with_namespace_filter(self, test_namespace):
        """Should filter by namespace."""
        self._create_active_artifact("ns_list_1", namespace_id=test_namespace)
        self._create_active_artifact("ns_list_2")

        resp = client.get("/artifacts/", params={"namespace_id": test_namespace})
        assert resp.status_code == 200
        data = resp.json()
        assert all(a["namespace_id"] == test_namespace for a in data)

    def test_list_artifacts_with_tag_filter(self):
        """Should filter by tags."""
        self._create_active_artifact("tagged_1", tags=["ml", "production"])
        self._create_active_artifact("tagged_2", tags=["ml", "dev"])
        self._create_active_artifact("tagged_3", tags=["other"])

        # Filter by single tag
        resp = client.get("/artifacts/", params={"tags": ["ml"]})
        assert resp.status_code == 200
        data = resp.json()
        names = [a["name"] for a in data]
        assert "tagged_1" in names
        assert "tagged_2" in names
        assert "tagged_3" not in names

    def test_list_artifacts_with_name_filter(self):
        """Should filter by name substring."""
        self._create_active_artifact("model_alpha")
        self._create_active_artifact("model_beta")
        self._create_active_artifact("config_gamma")

        resp = client.get("/artifacts/", params={"name_contains": "model"})
        assert resp.status_code == 200
        data = resp.json()
        names = [a["name"] for a in data]
        assert "model_alpha" in names
        assert "model_beta" in names
        assert "config_gamma" not in names

    def test_list_artifacts_with_python_type_filter(self):
        """Should filter by Python type."""
        self._create_active_artifact(
            "sklearn_model",
            python_type="sklearn.ensemble.RandomForestClassifier",
        )
        self._create_active_artifact("dict_config", python_type="builtins.dict")

        resp = client.get("/artifacts/", params={"python_type_contains": "sklearn"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert all("sklearn" in (a.get("python_type") or "") for a in data)

    def test_list_artifacts_pagination(self):
        """Should support pagination."""
        for i in range(5):
            self._create_active_artifact(f"paginated_{i}")

        # Get first page
        resp = client.get("/artifacts/", params={"limit": 2, "offset": 0})
        assert resp.status_code == 200
        page1 = resp.json()
        assert len(page1) == 2

        # Get second page
        resp = client.get("/artifacts/", params={"limit": 2, "offset": 2})
        assert resp.status_code == 200
        page2 = resp.json()
        assert len(page2) == 2

        # Pages should be different
        page1_ids = {a["id"] for a in page1}
        page2_ids = {a["id"] for a in page2}
        assert page1_ids.isdisjoint(page2_ids)

    def test_list_artifact_names(self):
        """Should list unique artifact names."""
        self._create_active_artifact("unique_name_1")
        self._create_active_artifact("unique_name_2")
        self._create_active_artifact("unique_name_2")  # Create v2

        resp = client.get("/artifacts/names")
        assert resp.status_code == 200
        names = resp.json()
        assert "unique_name_1" in names
        assert "unique_name_2" in names
        # Should be unique - no duplicates
        assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# Deletion Tests
# ---------------------------------------------------------------------------


class TestDeleteArtifact:
    """Tests for artifact deletion endpoints."""

    def _create_active_artifact(self, name: str) -> int:
        """Helper to create an active artifact."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": name, "serialization_format": "pickle"},
        )
        prep_data = prep_resp.json()

        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

        client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": prep_data["artifact_id"],
                "storage_key": prep_data["storage_key"],
                "sha256": sha256,
                "size_bytes": len(test_data),
            },
        )
        return prep_data["artifact_id"]

    def test_delete_artifact_by_id(self):
        """Should delete specific artifact version by ID."""
        artifact_id = self._create_active_artifact("delete_by_id")

        resp = client.delete(f"/artifacts/{artifact_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"
        assert data["artifact_id"] == artifact_id

        # Verify it's deleted (soft delete)
        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, artifact_id)
            assert artifact.status == "deleted"

        # Should not be retrievable
        resp = client.get(f"/artifacts/{artifact_id}")
        assert resp.status_code == 404

    def test_delete_artifact_by_name(self):
        """Should delete all versions of artifact by name."""
        self._create_active_artifact("delete_all_versions")
        self._create_active_artifact("delete_all_versions")

        resp = client.delete("/artifacts/by-name/delete_all_versions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"
        assert data["versions_deleted"] == 2

        # Should not be retrievable
        resp = client.get("/artifacts/by-name/delete_all_versions")
        assert resp.status_code == 404

    def test_delete_nonexistent_artifact(self):
        """Should return 404 for nonexistent artifact."""
        resp = client.delete("/artifacts/999999")
        assert resp.status_code == 404

    def test_delete_by_name_not_found(self):
        """Should return 404 for nonexistent artifact name."""
        resp = client.delete("/artifacts/by-name/nonexistent_delete")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Edge Cases and Error Handling
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_artifact_with_special_characters_in_name(self):
        """Should handle special characters in artifact name."""
        # Note: URL encoding is handled by TestClient
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "model-v1.2_final", "serialization_format": "pickle"},
        )
        assert resp.status_code == 201

    def test_artifact_with_empty_tags(self):
        """Should handle empty tags list."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={"name": "no_tags", "serialization_format": "pickle", "tags": []},
        )
        assert resp.status_code == 201

    def test_artifact_with_long_description(self):
        """Should handle long descriptions."""
        long_desc = "A" * 1000
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "long_desc",
                "serialization_format": "pickle",
                "description": long_desc,
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.description == long_desc

    def test_list_with_invalid_limit(self):
        """Should reject invalid limit values."""
        resp = client.get("/artifacts/", params={"limit": 0})
        assert resp.status_code == 422

        resp = client.get("/artifacts/", params={"limit": 1000})
        assert resp.status_code == 422

    def test_list_with_negative_offset(self):
        """Should reject negative offset."""
        resp = client.get("/artifacts/", params={"offset": -1})
        assert resp.status_code == 422
