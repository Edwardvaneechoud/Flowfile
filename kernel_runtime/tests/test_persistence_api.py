"""Tests for the persistence-related API endpoints in the kernel runtime."""

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


class TestHealthEndpoint:
    def test_health_includes_persistence_info(self, client: TestClient):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "persistence" in data
        assert "recovery_mode" in data


class TestRecoveryStatusEndpoint:
    def test_recovery_status(self, client: TestClient):
        response = client.get("/recovery-status")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestPersistenceEndpoint:
    def test_persistence_info(self, client: TestClient):
        response = client.get("/persistence")
        assert response.status_code == 200
        data = response.json()
        assert "enabled" in data
        assert "recovery_mode" in data


class TestRecoverEndpoint:
    def test_recover_returns_status(self, client: TestClient):
        response = client.post("/recover")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestCleanupEndpoint:
    def test_cleanup_with_max_age(self, client: TestClient):
        response = client.post("/cleanup", json={"max_age_hours": 24})
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_cleanup_with_empty_request(self, client: TestClient):
        response = client.post("/cleanup", json={})
        assert response.status_code == 200
