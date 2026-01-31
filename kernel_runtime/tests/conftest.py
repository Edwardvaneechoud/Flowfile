"""Shared fixtures for kernel_runtime tests."""

import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.main import app, artifact_store


@pytest.fixture()
def store() -> ArtifactStore:
    """Fresh ArtifactStore for each test."""
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
