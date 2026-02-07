"""Shared fixtures for kernel_runtime tests."""

import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kernel_runtime.artifact_persistence import ArtifactPersistence
from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.main import app, artifact_store


@pytest.fixture()
def store() -> ArtifactStore:
    """Fresh ArtifactStore for each test."""
    return ArtifactStore()


@pytest.fixture(autouse=True)
def _clear_global_state():
    """Reset the global artifact_store and persistence state between tests."""
    from kernel_runtime import main
    from kernel_runtime.artifact_persistence import RecoveryMode

    artifact_store.clear()
    # Reset persistence state
    main._persistence = None
    main._recovery_mode = RecoveryMode.LAZY
    main._recovery_status = {"status": "pending", "recovered": [], "errors": []}
    main._kernel_id = "default"
    main._persistence_path = "/shared/artifacts"
    # Detach persistence from artifact store
    artifact_store._persistence = None
    artifact_store._lazy_index.clear()
    artifact_store._loading_locks.clear()
    artifact_store._persist_pending.clear()

    yield

    artifact_store.clear()
    main._persistence = None
    main._recovery_mode = RecoveryMode.LAZY
    main._recovery_status = {"status": "pending", "recovered": [], "errors": []}
    main._kernel_id = "default"
    main._persistence_path = "/shared/artifacts"
    artifact_store._persistence = None
    artifact_store._lazy_index.clear()
    artifact_store._loading_locks.clear()
    artifact_store._persist_pending.clear()


@pytest.fixture()
def client(tmp_path: Path) -> Generator[TestClient, None, None]:
    """FastAPI TestClient for the kernel runtime app.

    Sets PERSISTENCE_PATH to a temp directory so persistence tests work
    in CI environments without /shared.
    """
    # Set env vars before TestClient triggers lifespan
    old_path = os.environ.get("PERSISTENCE_PATH")
    os.environ["PERSISTENCE_PATH"] = str(tmp_path / "artifacts")

    with TestClient(app) as c:
        yield c

    # Restore original env var
    if old_path is None:
        os.environ.pop("PERSISTENCE_PATH", None)
    else:
        os.environ["PERSISTENCE_PATH"] = old_path


@pytest.fixture()
def tmp_dir() -> Generator[Path, None, None]:
    """Temporary directory cleaned up after each test."""
    with tempfile.TemporaryDirectory(prefix="kernel_test_") as d:
        yield Path(d)


@pytest.fixture()
def persistence(tmp_dir: Path) -> ArtifactPersistence:
    """Fresh ArtifactPersistence backed by a temporary directory."""
    return ArtifactPersistence(tmp_dir / "artifacts")


@pytest.fixture()
def store_with_persistence(persistence: ArtifactPersistence) -> ArtifactStore:
    """ArtifactStore with persistence enabled."""
    s = ArtifactStore()
    s.enable_persistence(persistence)
    return s
