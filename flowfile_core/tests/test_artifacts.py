"""Integration tests for the Global Artifacts API endpoints.

Covers:
- Upload workflow (prepare + finalize)
- Retrieval by name and ID
- Versioning
- Listing and filtering
- Deletion
- Source registration linking
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
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact, User


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


def _cleanup_registrations():
    """Remove test flow registrations and their orphaned artifacts."""
    with get_db_context() as db:
        reg_ids = [
            r.id
            for r in db.query(FlowRegistration)
            .filter(FlowRegistration.name.like("ArtifactTest%"))
            .all()
        ]
        if reg_ids:
            db.query(GlobalArtifact).filter(
                GlobalArtifact.source_registration_id.in_(reg_ids)
            ).delete(synchronize_session=False)
            db.query(FlowRegistration).filter(
                FlowRegistration.id.in_(reg_ids)
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

    schema_resp = client.post(
        "/catalog/namespaces",
        json={"name": "ArtifactTestSchema", "parent_id": cat_id},
    )
    if schema_resp.status_code != 201:
        raise Exception(f"Failed to create schema: {schema_resp.text}")

    return schema_resp.json()["id"]


def _create_test_registration(
    namespace_id: int | None = None,
    name: str = "ArtifactTestFlow",
) -> int:
    """Create a test flow registration and return its ID."""
    resp = client.post(
        "/catalog/flows",
        json={
            "name": name,
            "flow_path": f"/tmp/{name}.flow",
            "namespace_id": namespace_id,
        },
    )
    assert resp.status_code == 201, f"Failed to create registration: {resp.text}"
    return resp.json()["id"]


def _publish(
    name: str,
    reg_id: int,
    namespace_id: int | None = None,
    tags: list[str] | None = None,
    python_type: str = "builtins.dict",
    payload: bytes = b"test artifact data",
) -> dict:
    """prepare + finalize one version; returns the prepare response."""
    import hashlib

    prep_resp = client.post(
        "/artifacts/prepare-upload",
        json={
            "name": name,
            "source_registration_id": reg_id,
            "serialization_format": "pickle",
            "namespace_id": namespace_id,
            "tags": tags or [],
            "python_type": python_type,
        },
    )
    assert prep_resp.status_code == 201, prep_resp.text
    prep_data = prep_resp.json()

    staging_path = Path(prep_data["path"])
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_bytes(payload)

    fin_resp = client.post(
        "/artifacts/finalize",
        json={
            "artifact_id": prep_data["artifact_id"],
            "storage_key": prep_data["storage_key"],
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
        },
    )
    assert fin_resp.status_code == 200, fin_resp.text
    return prep_data


def _blob_exists(storage_key: str) -> bool:
    from flowfile_core.artifacts import get_storage_backend

    return get_storage_backend().exists(storage_key)


def _internal_headers() -> dict:
    from flowfile_core.auth.jwt import get_internal_token

    return {"X-Internal-Token": get_internal_token()}


# Fixtures


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
def test_registration() -> int:
    """Create a test flow registration for artifact tests."""
    _cleanup_registrations()
    reg_id = _create_test_registration()
    yield reg_id
    _cleanup_artifacts()
    _cleanup_registrations()


@pytest.fixture
def test_registration_with_namespace(test_namespace) -> tuple[int, int]:
    """Create a flow registration under a namespace. Returns (reg_id, ns_id)."""
    _cleanup_registrations()
    reg_id = _create_test_registration(
        namespace_id=test_namespace,
        name="ArtifactTestFlowNS",
    )
    yield reg_id, test_namespace
    _cleanup_artifacts()
    _cleanup_registrations()


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


# Upload Workflow Tests


class TestPrepareUpload:
    """Tests for the /artifacts/prepare-upload endpoint."""

    def test_prepare_upload_creates_pending_artifact(self, test_registration):
        """Prepare upload should create an artifact in pending status."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "test_model",
                "source_registration_id": test_registration,
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

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, data["artifact_id"])
            assert artifact is not None
            assert artifact.status == "pending"
            assert artifact.name == "test_model"
            assert artifact.source_registration_id == test_registration

    def test_prepare_upload_increments_version(self, test_registration):
        """Each upload to same name should increment version."""
        resp1 = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "versioned_model",
                "source_registration_id": test_registration,
                "serialization_format": "pickle",
            },
        )
        assert resp1.status_code == 201
        assert resp1.json()["version"] == 1

        self._finalize_artifact(resp1.json())

        resp2 = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "versioned_model",
                "source_registration_id": test_registration,
                "serialization_format": "pickle",
            },
        )
        assert resp2.status_code == 201
        assert resp2.json()["version"] == 2

    def test_prepare_upload_with_namespace(self, test_registration_with_namespace):
        """Upload with namespace_id should associate artifact with namespace."""
        reg_id, ns_id = test_registration_with_namespace
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "namespaced_model",
                "source_registration_id": reg_id,
                "serialization_format": "joblib",
                "namespace_id": ns_id,
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.namespace_id == ns_id

    def test_prepare_upload_invalid_namespace(self, test_registration):
        """Upload with nonexistent namespace should return 404."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "invalid_ns_model",
                "source_registration_id": test_registration,
                "serialization_format": "pickle",
                "namespace_id": 999999,
            },
        )
        assert resp.status_code == 404

    def test_prepare_upload_stores_python_type_info(self, test_registration):
        """Python type and module info should be stored."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "typed_model",
                "source_registration_id": test_registration,
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

    def test_prepare_upload_without_source_registration_id(self):
        """Prepare upload without source_registration_id should return 422."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "no_reg_model",
                "serialization_format": "pickle",
            },
        )
        assert resp.status_code == 422

    def test_prepare_upload_with_invalid_source_registration_id(self):
        """Prepare upload with nonexistent source_registration_id should return 404."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "bad_reg_model",
                "source_registration_id": 999999,
                "serialization_format": "pickle",
            },
        )
        assert resp.status_code == 404

    def test_namespace_inherited_from_registration(self, test_registration_with_namespace):
        """namespace_id should be inherited from registration when not provided."""
        reg_id, ns_id = test_registration_with_namespace
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "inherited_ns_model",
                "source_registration_id": reg_id,
                "serialization_format": "pickle",
                # namespace_id intentionally omitted
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.namespace_id == ns_id

    def test_explicit_namespace_overrides_registration(self, test_registration_with_namespace, test_namespace):
        """Explicit namespace_id should override the registration's namespace."""
        reg_id, reg_ns_id = test_registration_with_namespace

        cat_resp = client.post(
            "/catalog/namespaces",
            json={"name": "ArtifactTestCatalog2", "description": "Second test catalog"},
        )
        if cat_resp.status_code == 201:
            cat_id = cat_resp.json()["id"]
            schema_resp = client.post(
                "/catalog/namespaces",
                json={"name": "ArtifactTestSchema2", "parent_id": cat_id},
            )
            override_ns_id = schema_resp.json()["id"]
        else:
            with get_db_context() as db:
                cat = db.query(CatalogNamespace).filter_by(
                    name="ArtifactTestCatalog2", parent_id=None
                ).first()
                schema = db.query(CatalogNamespace).filter_by(
                    name="ArtifactTestSchema2", parent_id=cat.id
                ).first()
                override_ns_id = schema.id

        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "override_ns_model",
                "source_registration_id": reg_id,
                "serialization_format": "pickle",
                "namespace_id": override_ns_id,
            },
        )
        assert resp.status_code == 201

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, resp.json()["artifact_id"])
            assert artifact.namespace_id == override_ns_id
            assert artifact.namespace_id != reg_ns_id

        with get_db_context() as db:
            db.query(CatalogNamespace).filter(
                CatalogNamespace.name.like("ArtifactTestCatalog2%")
                | CatalogNamespace.name.like("ArtifactTestSchema2%")
            ).delete(synchronize_session=False)
            db.commit()

    def _finalize_artifact(self, prepare_response: dict):
        """Helper to finalize an artifact upload."""
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

    def test_finalize_activates_artifact(self, test_registration):
        """Finalize should activate the artifact and store metadata."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "finalize_test",
                "source_registration_id": test_registration,
                "serialization_format": "pickle",
            },
        )
        assert prep_resp.status_code == 201
        prep_data = prep_resp.json()

        staging_path = Path(prep_data["path"])
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        test_data = b"test artifact content for finalization"
        staging_path.write_bytes(test_data)

        import hashlib
        sha256 = hashlib.sha256(test_data).hexdigest()

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

    def test_finalize_already_active_artifact(self, test_registration):
        """Finalize on already active artifact should return 400."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "double_finalize",
                "source_registration_id": test_registration,
                "serialization_format": "pickle",
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


# Retrieval Tests


class TestRetrieveArtifact:
    """Tests for artifact retrieval endpoints."""

    @pytest.fixture(autouse=True)
    def _setup_registration(self, test_registration):
        self._reg_id = test_registration

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
                "source_registration_id": self._reg_id,
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
        assert data["source_registration_id"] == self._reg_id
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
        self._create_active_artifact("versioned_retrieve")
        self._create_active_artifact("versioned_retrieve")

        resp = client.get("/artifacts/by-name/versioned_retrieve", params={"version": 1})
        assert resp.status_code == 200
        assert resp.json()["version"] == 1

        resp = client.get("/artifacts/by-name/versioned_retrieve", params={"version": 2})
        assert resp.status_code == 200
        assert resp.json()["version"] == 2

        resp = client.get("/artifacts/by-name/versioned_retrieve")
        assert resp.status_code == 200
        assert resp.json()["version"] == 2

    def test_get_artifact_with_namespace_filter(self, test_namespace):
        """Filtering by namespace works; a name-only lookup that spans multiple
        namespaces is rejected as ambiguous (409)."""
        self._create_active_artifact("ns_filtered", namespace_id=test_namespace)
        self._create_active_artifact("ns_filtered")

        resp = client.get(
            "/artifacts/by-name/ns_filtered",
            params={"namespace_id": test_namespace},
        )
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == test_namespace

        # Same name in two namespaces + no namespace_id -> ambiguous.
        resp = client.get("/artifacts/by-name/ns_filtered")
        assert resp.status_code == 409
        assert resp.json()["detail"]["error_code"] == "AMBIGUOUS_ARTIFACT"
        assert "namespace_id" in resp.json()["detail"]["message"]

    def test_get_artifact_by_namespace_name(self, test_namespace):
        """Lookup by namespace *name* resolves to the same artifact as namespace_id."""
        self._create_active_artifact("ns_named", namespace_id=test_namespace)

        resp = client.get(
            "/artifacts/by-name/ns_named",
            params={"namespace": "ArtifactTestSchema"},
        )
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == test_namespace

        # Unknown namespace name -> 404.
        resp = client.get(
            "/artifacts/by-name/ns_named",
            params={"namespace": "does_not_exist"},
        )
        assert resp.status_code == 404

    def test_get_artifact_versions(self):
        """Should retrieve artifact with all versions."""
        self._create_active_artifact("multi_version")
        self._create_active_artifact("multi_version")
        self._create_active_artifact("multi_version")

        resp = client.get("/artifacts/by-name/multi_version/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 3
        assert len(data["all_versions"]) == 3
        versions = [v["version"] for v in data["all_versions"]]
        assert sorted(versions, reverse=True) == [3, 2, 1]


# Listing Tests


class TestListArtifacts:
    """Tests for artifact listing endpoints."""

    @pytest.fixture(autouse=True)
    def _setup_registration(self, test_registration):
        self._reg_id = test_registration

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
                "source_registration_id": self._reg_id,
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

        resp = client.get("/artifacts/", params={"limit": 2, "offset": 0})
        assert resp.status_code == 200
        page1 = resp.json()
        assert len(page1) == 2

        resp = client.get("/artifacts/", params={"limit": 2, "offset": 2})
        assert resp.status_code == 200
        page2 = resp.json()
        assert len(page2) == 2

        page1_ids = {a["id"] for a in page1}
        page2_ids = {a["id"] for a in page2}
        assert page1_ids.isdisjoint(page2_ids)

    def test_list_latest_only_collapses_versions(self):
        """latest_only returns one row per (name, namespace) — the newest version."""
        for _ in range(3):
            self._create_active_artifact("collapse_me")
        self._create_active_artifact("other_one")

        resp = client.get("/artifacts/", params={"latest_only": True})
        assert resp.status_code == 200
        data = resp.json()
        rows = [a for a in data if a["name"] == "collapse_me"]
        assert len(rows) == 1
        assert rows[0]["version"] == 3
        assert rows[0]["version_count"] == 3
        assert len([a for a in data if a["name"] == "other_one"]) == 1

    def test_list_latest_only_keeps_namespaces_apart(self, test_namespace):
        """The same name in two namespaces stays two rows, each with its namespace path."""
        self._create_active_artifact("same_name", namespace_id=test_namespace)
        self._create_active_artifact("same_name", namespace_id=test_namespace)
        self._create_active_artifact("same_name")

        resp = client.get("/artifacts/", params={"latest_only": True, "name_contains": "same_name"})
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 2
        by_ns = {a["namespace_id"]: a for a in rows}
        assert set(by_ns) == {test_namespace, None}
        assert by_ns[test_namespace]["version"] == 2
        assert by_ns[test_namespace]["version_count"] == 2
        assert by_ns[test_namespace]["namespace_path"] == "ArtifactTestCatalog.ArtifactTestSchema"
        assert by_ns[None]["namespace_path"] is None
        assert by_ns[None]["version_count"] == 1

    def test_list_latest_only_composes_with_filters(self, test_namespace):
        """latest_only composes with namespace/tag/name/type filters."""
        self._create_active_artifact("filtered_model", namespace_id=test_namespace, tags=["ml"])
        self._create_active_artifact("filtered_model", namespace_id=test_namespace, tags=["ml"])
        self._create_active_artifact("filtered_other", namespace_id=test_namespace, tags=["ops"])
        self._create_active_artifact("filtered_model")

        resp = client.get(
            "/artifacts/",
            params={
                "latest_only": True,
                "namespace_id": test_namespace,
                "tags": ["ml"],
                "name_contains": "filtered",
                "python_type_contains": "dict",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()
        assert [(a["name"], a["version"]) for a in rows] == [("filtered_model", 2)]

    def test_list_latest_only_pagination_is_disjoint(self):
        """Pagination counts artifacts, not versions."""
        for i in range(5):
            self._create_active_artifact(f"page_art_{i}")
            self._create_active_artifact(f"page_art_{i}")

        page1 = client.get(
            "/artifacts/", params={"latest_only": True, "name_contains": "page_art_", "limit": 2, "offset": 0}
        ).json()
        page2 = client.get(
            "/artifacts/", params={"latest_only": True, "name_contains": "page_art_", "limit": 2, "offset": 2}
        ).json()
        assert len(page1) == 2 and len(page2) == 2
        assert {a["name"] for a in page1}.isdisjoint({a["name"] for a in page2})
        assert all(a["version"] == 2 for a in page1 + page2)

    def test_list_without_latest_only_is_unchanged(self):
        """The default still returns one item per version row."""
        self._create_active_artifact("all_rows")
        self._create_active_artifact("all_rows")

        rows = client.get("/artifacts/", params={"name_contains": "all_rows"}).json()
        assert sorted(a["version"] for a in rows) == [1, 2]

    def test_list_artifact_names(self):
        """Should list unique artifact names."""
        self._create_active_artifact("unique_name_1")
        self._create_active_artifact("unique_name_2")
        self._create_active_artifact("unique_name_2")

        resp = client.get("/artifacts/names")
        assert resp.status_code == 200
        names = resp.json()
        assert "unique_name_1" in names
        assert "unique_name_2" in names
        assert len(names) == len(set(names))

    def test_namespace_path_survives_a_dangling_parent(self, test_namespace):
        """SQLite does not enforce FKs, so the parent walk must not loop on a missing row."""
        with get_db_context() as db:
            ns = db.get(CatalogNamespace, test_namespace)
            ns.parent_id = 999_999
            db.commit()

        self._create_active_artifact("orphan_ns", namespace_id=test_namespace)

        resp = client.get("/artifacts/", params={"name_contains": "orphan_ns"})
        assert resp.status_code == 200
        assert resp.json()[0]["namespace_path"] == "ArtifactTestSchema"

    def test_tree_version_count_falls_back_to_the_group_size(self, test_namespace):
        """A failed publish keeps its row count; only active artifacts count actives."""
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.services.namespaces import bulk_enrich_artifacts

        self._create_active_artifact("healthy", namespace_id=test_namespace)
        self._create_active_artifact("healthy", namespace_id=test_namespace)
        with get_db_context() as db:
            for version in (1, 2, 3):
                db.add(
                    GlobalArtifact(
                        name="failed_publish",
                        namespace_id=test_namespace,
                        version=version,
                        status="failed",
                        owner_id=_get_local_user_id(),
                        source_registration_id=self._reg_id,
                        serialization_format="pickle",
                    )
                )
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            enriched = bulk_enrich_artifacts(
                repo.list_artifacts_for_namespace(test_namespace),
                version_counts=repo.artifact_version_counts_for_namespace(test_namespace),
            )

        counts = {a.name: a.version_count for a in enriched}
        assert counts["healthy"] == 2
        assert counts["failed_publish"] == 3


# Deletion Tests


class TestDeleteArtifact:
    """Tests for artifact deletion endpoints."""

    @pytest.fixture(autouse=True)
    def _setup_registration(self, test_registration):
        self._reg_id = test_registration

    def _create_active_artifact(self, name: str) -> int:
        """Helper to create an active artifact."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": self._reg_id,
                "serialization_format": "pickle",
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

    def test_delete_artifact_by_id(self):
        """Should delete a specific (non-latest) artifact version by ID."""
        artifact_id = self._create_active_artifact("delete_by_id")
        self._create_active_artifact("delete_by_id")

        resp = client.delete(f"/artifacts/{artifact_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"
        assert data["artifact_id"] == artifact_id

        with get_db_context() as db:
            artifact = db.get(GlobalArtifact, artifact_id)
            assert artifact.status == "deleted"

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


# Flow Deletion Cascade Tests


class TestFlowDeletionWithArtifacts:
    """Tests for flow deletion when artifacts exist."""

    def _create_active_artifact(self, name: str, reg_id: int) -> int:
        """Helper to create an active artifact for a given registration."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": reg_id,
                "serialization_format": "pickle",
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

    def test_delete_flow_with_active_artifacts_blocked(self):
        """Deleting a flow with active artifacts should be blocked (409)."""
        _cleanup_registrations()
        reg_id = _create_test_registration(name="ArtifactTestFlowBlock")
        self._create_active_artifact("blocking_artifact", reg_id)

        resp = client.delete(f"/catalog/flows/{reg_id}")
        assert resp.status_code == 409

        _cleanup_artifacts()
        _cleanup_registrations()

    def test_delete_flow_after_artifacts_deleted(self):
        """Deleting a flow should succeed after all its artifacts are deleted."""
        _cleanup_registrations()
        reg_id = _create_test_registration(name="ArtifactTestFlowAllow")
        self._create_active_artifact("deletable_artifact", reg_id)

        # The only version of an artifact is removed by name, not per-version.
        assert client.delete("/artifacts/by-name/deletable_artifact").status_code == 200

        resp = client.delete(f"/catalog/flows/{reg_id}")
        assert resp.status_code == 204

        _cleanup_registrations()

    def test_delete_flow_without_artifacts_succeeds(self):
        """Deleting a flow with no artifacts should succeed."""
        _cleanup_registrations()
        reg_id = _create_test_registration(name="ArtifactTestFlowEmpty")

        resp = client.delete(f"/catalog/flows/{reg_id}")
        assert resp.status_code == 204

        _cleanup_registrations()


class TestFlowArtifactsBlobExists:
    """Tests for blob_exists on the flow-artifacts listing."""

    def _create_active_artifact(self, name: str, reg_id: int) -> str:
        """Create an active artifact and return its storage_key."""
        prep_resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": reg_id,
                "serialization_format": "pickle",
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
        return prep_data["storage_key"]

    def test_flow_artifacts_flag_missing_blob(self, test_registration):
        """blob_exists should flip to False when the backing blob is removed."""
        storage_key = self._create_active_artifact("blob_check", test_registration)

        resp = client.get(f"/catalog/flows/{test_registration}/artifacts")
        assert resp.status_code == 200
        assert resp.json()[0]["blob_exists"] is True

        from flowfile_core.artifacts import get_storage_backend
        get_storage_backend().delete(storage_key)

        resp = client.get(f"/catalog/flows/{test_registration}/artifacts")
        assert resp.status_code == 200
        assert resp.json()[0]["blob_exists"] is False


# Edge Cases and Error Handling


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.fixture(autouse=True)
    def _setup_registration(self, test_registration):
        self._reg_id = test_registration

    def test_artifact_with_special_characters_in_name(self):
        """Should handle special characters in artifact name."""
        # Note: URL encoding is handled by TestClient
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "model-v1.2_final",
                "source_registration_id": self._reg_id,
                "serialization_format": "pickle",
            },
        )
        assert resp.status_code == 201

    def test_artifact_with_empty_tags(self):
        """Should handle empty tags list."""
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "no_tags",
                "source_registration_id": self._reg_id,
                "serialization_format": "pickle",
                "tags": [],
            },
        )
        assert resp.status_code == 201

    def test_artifact_with_long_description(self):
        """Should handle long descriptions."""
        long_desc = "A" * 1000
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "long_desc",
                "source_registration_id": self._reg_id,
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


# Qualified references


class TestQualifiedReference:
    """`catalog.schema::name` references on every by-name route."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration_with_namespace):
        self._reg_id, self._ns_id = test_registration_with_namespace
        self._plain_reg_id = _create_test_registration(name="ArtifactTestFlowPlain")
        self._path = "ArtifactTestCatalog.ArtifactTestSchema"

    def test_qualified_reference_resolves(self):
        _publish("qual_model", self._reg_id, namespace_id=self._ns_id)

        resp = client.get(f"/artifacts/by-name/{self._path}::qual_model")
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == self._ns_id

    def test_qualified_reference_disambiguates_a_collision(self):
        _publish("collide", self._reg_id, namespace_id=self._ns_id)
        _publish("collide", self._plain_reg_id)

        # Bare name still 409s (unchanged contract), in the router's one 409 shape ...
        collision = client.get("/artifacts/by-name/collide")
        assert collision.status_code == 409
        assert collision.json()["detail"]["error_code"] == "AMBIGUOUS_ARTIFACT"
        assert "collide" in collision.json()["detail"]["message"]
        # ... while the qualified reference resolves.
        resp = client.get(f"/artifacts/by-name/{self._path}::collide")
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == self._ns_id

    def test_versions_and_delete_accept_the_same_reference(self):
        _publish("qual_versions", self._reg_id, namespace_id=self._ns_id)
        _publish("qual_versions", self._reg_id, namespace_id=self._ns_id)
        _publish("qual_versions", self._plain_reg_id)

        resp = client.get(f"/artifacts/by-name/{self._path}::qual_versions/versions")
        assert resp.status_code == 200
        assert resp.json()["total_versions"] == 2

        resp = client.delete(f"/artifacts/by-name/{self._path}::qual_versions")
        assert resp.status_code == 200
        assert resp.json()["versions_deleted"] == 2
        # The other namespace's copy survives, and is now unambiguous.
        assert client.get("/artifacts/by-name/qual_versions").json()["namespace_id"] is None

    def test_duplicate_schema_name_resolves_by_path(self):
        """A schema name reused under another catalog is ambiguous by name, not by path."""
        cat_b = client.post("/catalog/namespaces", json={"name": "ArtifactTestCatalogB"})
        assert cat_b.status_code == 201, cat_b.text
        schema_b = client.post(
            "/catalog/namespaces",
            json={"name": "ArtifactTestSchema", "parent_id": cat_b.json()["id"]},
        )
        assert schema_b.status_code == 201, schema_b.text
        ns_b = schema_b.json()["id"]

        reg_b = _create_test_registration(namespace_id=ns_b, name="ArtifactTestFlowB")
        _publish("dup_schema", self._reg_id, namespace_id=self._ns_id)
        _publish("dup_schema", reg_b, namespace_id=ns_b)

        # The flat namespace-name lookup cannot disambiguate...
        flat = client.get("/artifacts/by-name/dup_schema", params={"namespace": "ArtifactTestSchema"})
        assert flat.status_code == 409
        # ...the path form can.
        resp = client.get("/artifacts/by-name/ArtifactTestCatalogB.ArtifactTestSchema::dup_schema")
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == ns_b

    def test_unknown_namespace_path_falls_back_to_an_unambiguous_name(self):
        """A renamed catalog must not break an already-saved qualified value."""
        _publish("renamed_ns", self._reg_id, namespace_id=self._ns_id)

        resp = client.get("/artifacts/by-name/Gone.Missing::renamed_ns")
        assert resp.status_code == 200
        assert resp.json()["namespace_id"] == self._ns_id

    def test_unknown_namespace_path_with_ambiguous_name_is_404(self):
        _publish("renamed_dup", self._reg_id, namespace_id=self._ns_id)
        _publish("renamed_dup", self._plain_reg_id)

        assert client.get("/artifacts/by-name/Gone.Missing::renamed_dup").status_code == 404

    def test_reference_splits_on_the_last_separator(self):
        """Splitting on the first separator would look for a name of 'B::target'."""
        _publish("target", self._reg_id, namespace_id=self._ns_id)

        resp = client.get("/artifacts/by-name/A::B::target")
        assert resp.status_code == 200
        assert resp.json()["name"] == "target"

    def test_prepare_upload_rejects_separator_in_name(self):
        resp = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": "General.models::bad",
                "source_registration_id": self._reg_id,
                "serialization_format": "pickle",
            },
        )
        assert resp.status_code == 422


# Per-version management


class TestDeleteArtifactVersion:
    """Per-version delete guards and blast radius."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration):
        self._reg_id = test_registration

    def _five_versions(self, name: str = "five") -> list[dict]:
        return [_publish(name, self._reg_id, payload=f"payload-{i}".encode()) for i in range(5)]

    def test_delete_middle_version_leaves_siblings(self):
        versions = self._five_versions()
        target = versions[2]

        resp = client.delete(f"/artifacts/{target['artifact_id']}")
        assert resp.status_code == 200

        with get_db_context() as db:
            rows = db.query(GlobalArtifact).filter_by(name="five").all()
            statuses = {r.version: r.status for r in rows}
        assert statuses[3] == "deleted"
        assert [statuses[v] for v in (1, 2, 4, 5)] == ["active"] * 4
        assert not _blob_exists(target["storage_key"])
        assert all(_blob_exists(v["storage_key"]) for v in versions if v is not target)

    def test_delete_version_drops_its_grants(self):
        from flowfile_core.database.models import ResourceGrant

        versions = self._five_versions("granted")
        target_id = versions[1]["artifact_id"]
        keeper_id = versions[0]["artifact_id"]
        with get_db_context() as db:
            for resource_id in (target_id, keeper_id):
                db.add(
                    ResourceGrant(
                        resource_type="global_artifact",
                        resource_id=resource_id,
                        group_id=1,
                        permission="use",
                        granted_by=1,
                    )
                )
            db.commit()

        assert client.delete(f"/artifacts/{target_id}").status_code == 200

        with get_db_context() as db:
            remaining = {
                g.resource_id
                for g in db.query(ResourceGrant).filter_by(resource_type="global_artifact").all()
            }
            db.query(ResourceGrant).filter_by(resource_type="global_artifact").delete()
            db.commit()
        assert target_id not in remaining
        assert keeper_id in remaining

    def test_delete_latest_version_is_refused_without_force(self):
        versions = self._five_versions("latest_guard")
        latest_id = versions[-1]["artifact_id"]

        resp = client.delete(f"/artifacts/{latest_id}")
        assert resp.status_code == 409
        assert resp.json()["detail"]["error_code"] == "CANNOT_DELETE_LATEST"

        resp = client.delete(f"/artifacts/{latest_id}", params={"force": True})
        assert resp.status_code == 200
        assert client.get("/artifacts/by-name/latest_guard").json()["version"] == 4

    def test_delete_last_remaining_version_is_refused(self):
        only = _publish("solo", self._reg_id)

        resp = client.delete(f"/artifacts/{only['artifact_id']}")
        assert resp.status_code == 409
        assert resp.json()["detail"]["error_code"] == "CANNOT_DELETE_LAST_VERSION"
        assert "by-name" in resp.json()["detail"]["message"]

        # force only overrides the latest-version guard, never the last-version one.
        resp = client.delete(f"/artifacts/{only['artifact_id']}", params={"force": True})
        assert resp.status_code == 409
        assert resp.json()["detail"]["error_code"] == "CANNOT_DELETE_LAST_VERSION"
        with get_db_context() as db:
            assert db.get(GlobalArtifact, only["artifact_id"]).status == "active"

    def test_internal_service_principal_bypasses_the_guards(self):
        """Released kernel images delete 'the latest version' by design."""
        only = _publish("kernel_delete", self._reg_id)

        resp = client.delete(f"/artifacts/{only['artifact_id']}", headers=_internal_headers())
        assert resp.status_code == 200
        with get_db_context() as db:
            assert db.get(GlobalArtifact, only["artifact_id"]).status == "deleted"

    def test_republish_after_delete_does_not_reuse_the_version_number(self):
        versions = self._five_versions("reuse")
        assert client.delete(f"/artifacts/{versions[2]['artifact_id']}").status_code == 200

        republished = _publish("reuse", self._reg_id)
        with get_db_context() as db:
            assert db.get(GlobalArtifact, republished["artifact_id"]).version == 6


class TestPromoteArtifactVersion:
    """Copy-forward restore."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration_with_namespace):
        self._reg_id, self._ns_id = test_registration_with_namespace

    def test_promote_creates_a_new_latest_from_an_old_version(self):
        v1 = _publish("promote_me", self._reg_id, namespace_id=self._ns_id, payload=b"v1-bytes")
        _publish("promote_me", self._reg_id, namespace_id=self._ns_id, payload=b"v2-bytes")
        _publish("promote_me", self._reg_id, namespace_id=self._ns_id, payload=b"v3-bytes")

        resp = client.post(f"/artifacts/{v1['artifact_id']}/promote")
        assert resp.status_code == 200
        promoted = resp.json()
        assert promoted["version"] == 4
        assert promoted["status"] == "active"
        assert promoted["storage_key"] != v1["storage_key"]
        assert _blob_exists(promoted["storage_key"])

        with get_db_context() as db:
            source = db.get(GlobalArtifact, v1["artifact_id"])
            assert source.status == "active" and source.version == 1
            assert promoted["sha256"] == source.sha256
            assert promoted["owner_id"] == source.owner_id
            assert promoted["namespace_id"] == source.namespace_id

        latest = client.get("/artifacts/by-name/promote_me", params={"namespace_id": self._ns_id}).json()
        assert latest["id"] == promoted["id"]

    def test_promote_keeps_the_source_namespace(self):
        v1 = _publish("promote_ns", self._reg_id, namespace_id=self._ns_id)
        _publish("promote_ns", self._reg_id, namespace_id=self._ns_id)

        promoted = client.post(f"/artifacts/{v1['artifact_id']}/promote").json()
        assert promoted["namespace_id"] == self._ns_id

    def test_promote_missing_or_deleted_version_is_404(self):
        v1 = _publish("promote_gone", self._reg_id, namespace_id=self._ns_id)
        _publish("promote_gone", self._reg_id, namespace_id=self._ns_id)
        assert client.delete(f"/artifacts/{v1['artifact_id']}").status_code == 200

        assert client.post(f"/artifacts/{v1['artifact_id']}/promote").status_code == 404
        assert client.post("/artifacts/999999/promote").status_code == 404

    def test_promote_with_a_missing_blob_is_404(self):
        """The storage copy is what discovers the gone blob (no pre-flight probe)."""
        from flowfile_core.artifacts import get_storage_backend

        v1 = _publish("promote_noblob", self._reg_id, namespace_id=self._ns_id)
        _publish("promote_noblob", self._reg_id, namespace_id=self._ns_id)
        get_storage_backend().delete(v1["storage_key"])

        assert client.post(f"/artifacts/{v1['artifact_id']}/promote").status_code == 404
        with get_db_context() as db:
            assert db.query(GlobalArtifact).filter_by(name="promote_noblob").count() == 2

    def _service(self, db):
        from flowfile_core.artifacts import get_storage_backend
        from flowfile_core.artifacts.service import ArtifactService

        return ArtifactService(db, get_storage_backend())

    def test_promote_retries_a_lost_version_race(self):
        """A concurrent publish taking the version number must not surface as a 500."""
        from sqlalchemy.exc import IntegrityError

        v1 = _publish("promote_race", self._reg_id, namespace_id=self._ns_id)
        _publish("promote_race", self._reg_id, namespace_id=self._ns_id)

        with get_db_context() as db:
            service = self._service(db)
            real_flush = db.flush
            attempts = []

            def flaky_flush(*args, **kwargs):
                attempts.append(1)
                if len(attempts) == 1:
                    raise IntegrityError("INSERT", {}, Exception("UNIQUE constraint failed"))
                return real_flush(*args, **kwargs)

            db.flush = flaky_flush
            promoted = service.promote_version(v1["artifact_id"])

        assert len(attempts) >= 2, "the lost race was not retried"
        assert promoted.version == 3
        assert _blob_exists(promoted.storage_key)

    def test_promote_cleans_up_its_blob_when_every_attempt_conflicts(self, monkeypatch):
        from sqlalchemy.exc import IntegrityError

        from flowfile_core.artifacts.exceptions import ArtifactUploadError

        v1 = _publish("promote_race_lost", self._reg_id, namespace_id=self._ns_id)
        _publish("promote_race_lost", self._reg_id, namespace_id=self._ns_id)

        with get_db_context() as db:
            service = self._service(db)
            copied: list[str] = []
            real_copy = service.storage.copy

            def spy_copy(src_key, dst_key):
                copied.append(dst_key)
                return real_copy(src_key, dst_key)

            monkeypatch.setattr(service.storage, "copy", spy_copy)
            db.commit = lambda *a, **k: (_ for _ in ()).throw(
                IntegrityError("INSERT", {}, Exception("UNIQUE constraint failed"))
            )
            with pytest.raises(ArtifactUploadError):
                service.promote_version(v1["artifact_id"], _max_retries=2)

        assert len(copied) == 2
        assert not any(_blob_exists(key) for key in copied)


class TestPruneVersions:
    """Retention: keep the newest N, never the current latest."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration):
        self._reg_id = test_registration

    def _versions(self, name: str, count: int) -> list[dict]:
        return [_publish(name, self._reg_id, payload=f"prune-{i}".encode()) for i in range(count)]

    def test_dry_run_reports_without_deleting(self):
        self._versions("prune_dry", 8)

        resp = client.post("/artifacts/by-name/prune_dry/prune", params={"keep": 5, "dry_run": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == 0
        assert [c["version"] for c in data["would_delete"]] == [3, 2, 1]
        assert data["freed_bytes"] == sum(c["size_bytes"] for c in data["would_delete"])

        with get_db_context() as db:
            assert db.query(GlobalArtifact).filter_by(name="prune_dry", status="active").count() == 8

    def test_prune_keeps_the_newest_versions_including_latest(self):
        versions = self._versions("prune_real", 8)

        resp = client.post("/artifacts/by-name/prune_real/prune", params={"keep": 5, "dry_run": False})
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 3

        with get_db_context() as db:
            active = sorted(
                v.version for v in db.query(GlobalArtifact).filter_by(name="prune_real", status="active").all()
            )
        assert active == [4, 5, 6, 7, 8]
        assert not _blob_exists(versions[0]["storage_key"])
        assert _blob_exists(versions[-1]["storage_key"])

    def test_prune_with_keep_above_version_count_is_a_no_op(self):
        self._versions("prune_noop", 3)

        resp = client.post("/artifacts/by-name/prune_noop/prune", params={"keep": 10, "dry_run": False})
        assert resp.status_code == 200
        assert resp.json() == {"would_delete": [], "freed_bytes": 0, "deleted": 0}

    def test_prune_unknown_artifact_is_404(self):
        assert client.post("/artifacts/by-name/never_published/prune").status_code == 404


class TestArtifactVersionsPagination:
    """Server-side pagination of the versions endpoint."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration):
        self._reg_id = test_registration
        for i in range(5):
            self._last = _publish("paged", self._reg_id, payload=f"paged-{i}".encode())

    def test_pages_are_disjoint_and_ordered(self):
        page1 = client.get("/artifacts/by-name/paged/versions", params={"limit": 2, "offset": 0}).json()
        page2 = client.get("/artifacts/by-name/paged/versions", params={"limit": 2, "offset": 2}).json()

        assert page1["total_versions"] == 5 and page2["total_versions"] == 5
        assert [v["version"] for v in page1["all_versions"]] == [5, 4]
        assert [v["version"] for v in page2["all_versions"]] == [3, 2]

    def test_version_rows_carry_type_and_blob_state(self):
        from flowfile_core.artifacts import get_storage_backend

        get_storage_backend().delete(self._last["storage_key"])
        data = client.get("/artifacts/by-name/paged/versions", params={"limit": 2}).json()
        newest = data["all_versions"][0]
        assert newest["python_type"] == "builtins.dict"
        assert newest["blob_exists"] is False
        assert data["all_versions"][1]["blob_exists"] is True

    def test_default_returns_every_version(self):
        data = client.get("/artifacts/by-name/paged/versions").json()
        assert len(data["all_versions"]) == 5
        assert data["total_versions"] == 5


class TestArtifactAuthz:
    """Every artifact endpoint authenticates; download_source is not public."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_registration):
        self._reg_id = test_registration

    def test_reads_and_finalize_require_authentication(self):
        published = _publish("authz_model", self._reg_id)
        anonymous = TestClient(main.app)

        for path in (
            "/artifacts/",
            "/artifacts/names",
            "/artifacts/by-name/authz_model",
            "/artifacts/by-name/authz_model/versions",
            f"/artifacts/{published['artifact_id']}",
        ):
            assert anonymous.get(path).status_code == 401, path

        resp = anonymous.post(
            "/artifacts/finalize",
            json={
                "artifact_id": published["artifact_id"],
                "storage_key": published["storage_key"],
                "sha256": "deadbeef",
                "size_bytes": 1,
            },
        )
        assert resp.status_code == 401
        anonymous.close()

    def test_mutations_require_authentication(self):
        """Promote / prune / per-version delete must never become anonymous."""
        published = _publish("authz_mutations", self._reg_id)
        _publish("authz_mutations", self._reg_id, payload=b"v2")
        artifact_id = published["artifact_id"]
        anonymous = TestClient(main.app)

        assert anonymous.post(f"/artifacts/{artifact_id}/promote").status_code == 401
        assert anonymous.post("/artifacts/by-name/authz_mutations/prune").status_code == 401
        assert anonymous.delete(f"/artifacts/{artifact_id}").status_code == 401
        assert anonymous.delete("/artifacts/by-name/authz_mutations").status_code == 401

        with get_db_context() as db:
            assert db.query(GlobalArtifact).filter_by(name="authz_mutations", status="active").count() == 2
        anonymous.close()

    def test_internal_service_principal_can_read(self):
        _publish("authz_kernel", self._reg_id)
        kernel = TestClient(main.app)

        resp = kernel.get("/artifacts/by-name/authz_kernel", headers=_internal_headers())
        assert resp.status_code == 200
        assert resp.json()["download_source"] is not None
        kernel.close()
