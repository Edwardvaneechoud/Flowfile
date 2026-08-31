"""Fixtures for the standalone telemetry collector tests (no flowfile imports, no network)."""

import pytest
from fastapi.testclient import TestClient

from tools.telemetry_collector.app import app


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEMETRY_DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def client():
    return TestClient(app)
