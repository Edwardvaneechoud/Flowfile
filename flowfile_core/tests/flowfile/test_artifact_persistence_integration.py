"""
Unit-level integration tests for artifact persistence models and
KernelManager proxy methods.

These tests do NOT require Docker — they use mocked HTTP responses.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from flowfile_core.kernel.models import (
    ArtifactPersistenceInfo,
    CleanupRequest,
    CleanupResult,
    RecoveryMode,
    RecoveryStatus,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    """Run an async coroutine from sync test code."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _make_manager(kernel_id: str = "test-kernel", port: int = 19000):
    """Create a KernelManager with a single IDLE kernel, patching Docker."""
    from flowfile_core.kernel.models import KernelInfo, KernelState

    kernel = KernelInfo(
        id=kernel_id,
        name="Test Kernel",
        state=KernelState.IDLE,
        port=port,
        container_id="fake-container-id",
    )

    with patch("flowfile_core.kernel.manager.docker"):
        from flowfile_core.kernel.manager import KernelManager

        with patch.object(KernelManager, "_restore_kernels_from_db"):
            with patch.object(KernelManager, "_reclaim_running_containers"):
                manager = KernelManager(shared_volume_path="/tmp/test_shared")

    manager._kernels[kernel_id] = kernel
    manager._kernel_owners[kernel_id] = 1
    return manager


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestPersistenceModels:
    """Tests for the new persistence-related Pydantic models."""

    def test_recovery_mode_enum_values(self):
        assert RecoveryMode.LAZY == "lazy"
        assert RecoveryMode.EAGER == "eager"
        assert RecoveryMode.NONE == "none"

    def test_recovery_mode_from_string(self):
        assert RecoveryMode("lazy") == RecoveryMode.LAZY
        assert RecoveryMode("eager") == RecoveryMode.EAGER
        assert RecoveryMode("none") == RecoveryMode.NONE

    def test_recovery_status_defaults(self):
        status = RecoveryStatus(status="pending")
        assert status.status == "pending"
        assert status.mode is None
        assert status.recovered == []
        assert status.errors == []
        assert status.indexed is None

    def test_recovery_status_full(self):
        status = RecoveryStatus(
            status="completed",
            mode="eager",
            recovered=["model", "encoder"],
            errors=[],
        )
        assert len(status.recovered) == 2

    def test_cleanup_request_empty(self):
        req = CleanupRequest()
        assert req.max_age_hours is None
        assert req.artifact_names is None

    def test_cleanup_request_with_age(self):
        req = CleanupRequest(max_age_hours=24.0)
        assert req.max_age_hours == 24.0

    def test_cleanup_request_with_names(self):
        req = CleanupRequest(artifact_names=[{"flow_id": 0, "name": "model"}])
        assert len(req.artifact_names) == 1

    def test_cleanup_result(self):
        result = CleanupResult(status="cleaned", removed_count=5)
        assert result.removed_count == 5

    def test_persistence_info_disabled(self):
        info = ArtifactPersistenceInfo(enabled=False)
        assert info.enabled is False
        assert info.persisted_count == 0
        assert info.disk_usage_bytes == 0

    def test_persistence_info_enabled(self):
        info = ArtifactPersistenceInfo(
            enabled=True,
            recovery_mode="lazy",
            kernel_id="my-kernel",
            persistence_path="/shared/artifacts/my-kernel",
            persisted_count=3,
            in_memory_count=2,
            disk_usage_bytes=1024000,
            artifacts={"model": {"persisted": True, "in_memory": True}},
        )
        assert info.persisted_count == 3
        assert info.artifacts["model"]["persisted"] is True

    def test_persistence_info_serialization(self):
        info = ArtifactPersistenceInfo(
            enabled=True,
            kernel_id="k1",
            persisted_count=1,
        )
        d = info.model_dump()
        assert d["enabled"] is True
        assert d["kernel_id"] == "k1"
        # Should round-trip through JSON
        info2 = ArtifactPersistenceInfo(**d)
        assert info2 == info


# ---------------------------------------------------------------------------
# KernelManager proxy method tests (mocked HTTP)
# ---------------------------------------------------------------------------


class TestKernelManagerRecoverArtifacts:
    """Tests for KernelManager.recover_artifacts() proxy method."""

    def test_recover_artifacts_success(self):
        manager = _make_manager()
        response_data = {
            "status": "completed",
            "mode": "manual",
            "recovered": ["model", "encoder"],
            "errors": [],
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = _run(manager.recover_artifacts("test-kernel"))

        assert isinstance(result, RecoveryStatus)
        assert result.status == "completed"
        assert result.recovered == ["model", "encoder"]

    def test_recover_artifacts_kernel_not_running(self):
        manager = _make_manager()
        manager._kernels["test-kernel"].state = MagicMock(value="stopped")
        # Set state to STOPPED
        from flowfile_core.kernel.models import KernelState
        manager._kernels["test-kernel"].state = KernelState.STOPPED

        with pytest.raises(RuntimeError, match="not running"):
            _run(manager.recover_artifacts("test-kernel"))

    def test_recover_artifacts_kernel_not_found(self):
        manager = _make_manager()
        with pytest.raises(KeyError, match="not found"):
            _run(manager.recover_artifacts("nonexistent"))


class TestKernelManagerRecoveryStatus:
    """Tests for KernelManager.get_recovery_status() proxy method."""

    def test_get_recovery_status(self):
        manager = _make_manager()
        response_data = {
            "status": "completed",
            "mode": "lazy",
            "indexed": 5,
            "recovered": [],
            "errors": [],
        }

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = _run(manager.get_recovery_status("test-kernel"))

        assert isinstance(result, RecoveryStatus)
        assert result.status == "completed"
        assert result.indexed == 5


class TestKernelManagerCleanupArtifacts:
    """Tests for KernelManager.cleanup_artifacts() proxy method."""

    def test_cleanup_by_age(self):
        manager = _make_manager()
        response_data = {"status": "cleaned", "removed_count": 3}

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            request = CleanupRequest(max_age_hours=24)
            result = _run(manager.cleanup_artifacts("test-kernel", request))

        assert isinstance(result, CleanupResult)
        assert result.removed_count == 3

    def test_cleanup_by_name(self):
        manager = _make_manager()
        response_data = {"status": "cleaned", "removed_count": 1}

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            request = CleanupRequest(
                artifact_names=[{"flow_id": 0, "name": "old_model"}],
            )
            result = _run(manager.cleanup_artifacts("test-kernel", request))

        assert result.removed_count == 1


class TestKernelManagerPersistenceInfo:
    """Tests for KernelManager.get_persistence_info() proxy method."""

    def test_get_persistence_info_enabled(self):
        manager = _make_manager()
        response_data = {
            "enabled": True,
            "recovery_mode": "lazy",
            "kernel_id": "test-kernel",
            "persistence_path": "/shared/artifacts/test-kernel",
            "persisted_count": 2,
            "in_memory_count": 2,
            "disk_usage_bytes": 51200,
            "artifacts": {
                "model": {"flow_id": 0, "persisted": True, "in_memory": True},
                "encoder": {"flow_id": 0, "persisted": True, "in_memory": False},
            },
        }

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = _run(manager.get_persistence_info("test-kernel"))

        assert isinstance(result, ArtifactPersistenceInfo)
        assert result.enabled is True
        assert result.persisted_count == 2
        assert result.disk_usage_bytes == 51200
        assert "model" in result.artifacts
        assert "encoder" in result.artifacts

    def test_get_persistence_info_disabled(self):
        manager = _make_manager()
        response_data = {
            "enabled": False,
            "recovery_mode": "lazy",
            "persisted_count": 0,
            "disk_usage_bytes": 0,
        }

        mock_response = MagicMock()
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = _run(manager.get_persistence_info("test-kernel"))

        assert result.enabled is False
        assert result.persisted_count == 0


# ---------------------------------------------------------------------------
# Docker environment variable injection tests
# ---------------------------------------------------------------------------


class TestKernelStartupEnvironment:
    """Verify that persistence env vars are injected when starting a kernel."""

    def test_start_kernel_passes_persistence_env_vars(self):
        """start_kernel should pass KERNEL_ID, PERSISTENCE_ENABLED, etc."""
        from flowfile_core.kernel.models import KernelConfig, KernelState

        with patch("flowfile_core.kernel.manager.docker") as mock_docker:
            from flowfile_core.kernel.manager import KernelManager

            with patch.object(KernelManager, "_restore_kernels_from_db"):
                with patch.object(KernelManager, "_reclaim_running_containers"):
                    manager = KernelManager(shared_volume_path="/tmp/test")

            # Create a kernel
            config = KernelConfig(id="env-test", name="Env Test")
            _run(manager.create_kernel(config, user_id=1))

            # Mock the Docker image check and container run
            mock_docker.from_env.return_value.images.get.return_value = MagicMock()
            mock_container = MagicMock()
            mock_container.id = "fake-id"
            mock_docker.from_env.return_value.containers.run.return_value = mock_container

            # Mock health check
            with patch.object(manager, "_wait_for_healthy", new_callable=AsyncMock):
                _run(manager.start_kernel("env-test"))

            # Verify containers.run was called with persistence env vars
            call_args = mock_docker.from_env.return_value.containers.run.call_args
            environment = call_args[1]["environment"]

            assert environment["KERNEL_ID"] == "env-test"
            assert environment["PERSISTENCE_ENABLED"] == "true"
            assert environment["PERSISTENCE_PATH"] == "/shared/artifacts"
            assert environment["RECOVERY_MODE"] == "lazy"
