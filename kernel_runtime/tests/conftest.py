"""Shared fixtures for kernel_runtime tests."""

import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kernel_runtime.artifact_store import ArtifactStore, RecoveryMode
from kernel_runtime.main import app, artifact_store


@pytest.fixture()
def store() -> ArtifactStore:
    """Fresh ArtifactStore for each test (no persistence)."""
    return ArtifactStore()


@pytest.fixture(autouse=True)
def _clear_global_artifacts():
    """Reset the global artifact_store between tests."""
    artifact_store.clear()
    yield
    artifact_store.clear()


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    """FastAPI TestClient for the kernel runtime app."""
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def tmp_dir() -> Generator[Path, None, None]:
    """Temporary directory cleaned up after each test."""
    with tempfile.TemporaryDirectory(prefix="kernel_test_") as d:
        yield Path(d)


# ------------------------------------------------------------------
# Persistence fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def persistence_manager(tmp_path):
    """PersistenceManager using a temporary directory."""
    from kernel_runtime.persistence import PersistenceManager

    return PersistenceManager(str(tmp_path), kernel_id="test-kernel")


@pytest.fixture()
def persistent_store_lazy(persistence_manager):
    """ArtifactStore with persistence in LAZY mode."""
    return ArtifactStore(
        persistence=persistence_manager,
        recovery_mode=RecoveryMode.LAZY,
    )


@pytest.fixture()
def persistent_store_eager(tmp_path):
    """ArtifactStore with persistence in EAGER mode."""
    from kernel_runtime.persistence import PersistenceManager

    pm = PersistenceManager(str(tmp_path), kernel_id="eager-kernel")
    return ArtifactStore(
        persistence=pm,
        recovery_mode=RecoveryMode.EAGER,
    )
