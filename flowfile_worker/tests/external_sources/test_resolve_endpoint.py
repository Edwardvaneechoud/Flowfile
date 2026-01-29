"""Tests for resolve_endpoint_for_docker in s3_source models."""

import pytest

from flowfile_worker.external_sources.s3_source.models import resolve_endpoint_for_docker


@pytest.fixture
def docker_mode(monkeypatch):
    """Set FLOWFILE_MODE=docker for the duration of the test."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")


@pytest.fixture
def non_docker_mode(monkeypatch):
    """Ensure FLOWFILE_MODE is not set to docker."""
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)


class TestResolveEndpointForDocker:
    """Tests for localhost -> host.docker.internal translation in Docker mode."""

    def test_localhost_translated_in_docker_mode(self, docker_mode):
        result = resolve_endpoint_for_docker("http://localhost:9000")
        assert result == "http://host.docker.internal:9000"

    def test_127_0_0_1_translated_in_docker_mode(self, docker_mode):
        result = resolve_endpoint_for_docker("http://127.0.0.1:9000")
        assert result == "http://host.docker.internal:9000"

    def test_0_0_0_0_translated_in_docker_mode(self, docker_mode):
        result = resolve_endpoint_for_docker("http://0.0.0.0:9000")
        assert result == "http://host.docker.internal:9000"

    def test_https_localhost_translated(self, docker_mode):
        result = resolve_endpoint_for_docker("https://localhost:9000")
        assert result == "https://host.docker.internal:9000"

    def test_non_localhost_not_translated(self, docker_mode):
        result = resolve_endpoint_for_docker("http://minio.example.com:9000")
        assert result == "http://minio.example.com:9000"

    def test_no_port_localhost_translated(self, docker_mode):
        result = resolve_endpoint_for_docker("http://localhost")
        assert result == "http://host.docker.internal"

    def test_localhost_with_path_translated(self, docker_mode):
        result = resolve_endpoint_for_docker("http://localhost:9000/bucket/key")
        assert result == "http://host.docker.internal:9000/bucket/key"

    def test_not_translated_outside_docker(self, non_docker_mode):
        result = resolve_endpoint_for_docker("http://localhost:9000")
        assert result == "http://localhost:9000"

    def test_not_translated_in_electron_mode(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        result = resolve_endpoint_for_docker("http://localhost:9000")
        assert result == "http://localhost:9000"
