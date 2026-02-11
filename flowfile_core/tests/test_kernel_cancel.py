"""Tests for kernel execution cancellation support."""

from unittest.mock import MagicMock, patch

import pytest

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import KernelInfo, KernelState


class TestKernelManagerInterrupt:
    """Tests for KernelManager.interrupt_execution_sync."""

    def _make_manager_with_kernel(self, kernel_id="k1", state=KernelState.EXECUTING, container_id="abc123"):
        """Create a KernelManager with a mocked Docker client and a pre-registered kernel."""
        with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
            manager = KernelManager.__new__(KernelManager)
            manager._docker = MagicMock()
            manager._kernels = {}
            manager._kernel_owners = {}
            manager._shared_volume = "/tmp/test"
            manager._docker_network = None
            manager._kernel_volume = None
            manager._kernel_volume_type = None
            manager._kernel_mount_target = None

        kernel = KernelInfo(id=kernel_id, name="test-kernel", state=state, container_id=container_id)
        manager._kernels[kernel_id] = kernel
        return manager, kernel

    def test_interrupt_sends_sigusr1(self):
        manager, kernel = self._make_manager_with_kernel()
        mock_container = MagicMock()
        manager._docker.containers.get.return_value = mock_container

        result = manager.interrupt_execution_sync("k1")

        assert result is True
        manager._docker.containers.get.assert_called_once_with("abc123")
        mock_container.kill.assert_called_once_with(signal="SIGUSR1")

    def test_interrupt_kernel_not_found(self):
        manager, _ = self._make_manager_with_kernel()

        result = manager.interrupt_execution_sync("nonexistent")

        assert result is False

    def test_interrupt_kernel_not_executing(self):
        manager, kernel = self._make_manager_with_kernel(state=KernelState.IDLE)

        result = manager.interrupt_execution_sync("k1")

        assert result is False
        manager._docker.containers.get.assert_not_called()

    def test_interrupt_no_container_id(self):
        manager, kernel = self._make_manager_with_kernel(container_id=None)

        result = manager.interrupt_execution_sync("k1")

        assert result is False

    def test_interrupt_docker_error(self):
        import docker.errors

        manager, kernel = self._make_manager_with_kernel()
        manager._docker.containers.get.side_effect = docker.errors.NotFound("gone")

        result = manager.interrupt_execution_sync("k1")

        assert result is False


class TestFlowNodeCancelWithKernel:
    """Tests for FlowNode.cancel() with kernel cancel context."""

    def _make_node(self):
        """Create a minimal FlowNode for cancel testing."""
        from flowfile_core.flowfile.flow_node.flow_node import FlowNode

        setting_input = MagicMock()
        setting_input.is_setup = False
        setting_input.cache_results = False

        node = FlowNode(
            node_id=1,
            function=lambda: None,
            parent_uuid="test-uuid",
            setting_input=setting_input,
            name="test_node",
            node_type="python_script",
        )
        return node

    def test_cancel_with_kernel_context_calls_interrupt(self):
        node = self._make_node()
        mock_manager = MagicMock()
        node._kernel_cancel_context = ("k1", mock_manager)

        node.cancel()

        mock_manager.interrupt_execution_sync.assert_called_once_with("k1")
        assert node.node_stats.is_canceled is True

    def test_cancel_without_context_logs_warning(self):
        node = self._make_node()
        node._fetch_cached_df = None
        node._kernel_cancel_context = None

        node.cancel()

        assert node.node_stats.is_canceled is True

    def test_cancel_prefers_fetch_cached_df_over_kernel(self):
        """When _fetch_cached_df is set, it should be cancelled (not the kernel)."""
        node = self._make_node()
        mock_fetcher = MagicMock()
        mock_manager = MagicMock()
        node._fetch_cached_df = mock_fetcher
        node._kernel_cancel_context = ("k1", mock_manager)

        node.cancel()

        mock_fetcher.cancel.assert_called_once()
        mock_manager.interrupt_execution_sync.assert_not_called()
        assert node.node_stats.is_canceled is True

    def test_cancel_kernel_interrupt_exception_handled(self):
        """Even if interrupt_execution_sync raises, cancel should not crash."""
        node = self._make_node()
        mock_manager = MagicMock()
        mock_manager.interrupt_execution_sync.side_effect = RuntimeError("Docker unavailable")
        node._kernel_cancel_context = ("k1", mock_manager)

        node.cancel()  # Should not raise

        assert node.node_stats.is_canceled is True
