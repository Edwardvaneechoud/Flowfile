"""Tests for kernel execution cancellation support."""

from unittest.mock import MagicMock, patch

import docker.errors

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import KernelInfo, KernelState

import pytest

pytestmark = pytest.mark.kernel


def _make_manager(kernel_id="k1", state=KernelState.EXECUTING, container_id="abc123"):
    """Build a KernelManager with a mocked Docker client and one kernel."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._shared_volume = "/tmp/test"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None

    kernel = KernelInfo(id=kernel_id, name="test-kernel", state=state, container_id=container_id)
    mgr._kernels[kernel_id] = kernel
    return mgr


def _make_node():
    """Build a minimal FlowNode for cancel testing."""
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    setting_input = MagicMock()
    setting_input.is_setup = False
    setting_input.cache_results = False

    return FlowNode(
        node_id=1,
        function=lambda: None,
        parent_uuid="test-uuid",
        setting_input=setting_input,
        name="test_node",
        node_type="python_script",
    )


# -- KernelManager.interrupt_execution_sync -----------------------------------


class TestKernelManagerInterrupt:
    def test_sends_sigusr1(self):
        mgr = _make_manager()
        container = MagicMock()
        mgr._docker.containers.get.return_value = container

        assert mgr.interrupt_execution_sync("k1") is True
        container.kill.assert_called_once_with(signal="SIGUSR1")

    def test_unknown_kernel(self):
        mgr = _make_manager()
        assert mgr.interrupt_execution_sync("nonexistent") is False

    def test_kernel_not_executing(self):
        mgr = _make_manager(state=KernelState.IDLE)
        assert mgr.interrupt_execution_sync("k1") is False
        mgr._docker.containers.get.assert_not_called()

    def test_no_container_id(self):
        mgr = _make_manager(container_id=None)
        assert mgr.interrupt_execution_sync("k1") is False

    def test_docker_not_found(self):
        mgr = _make_manager()
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("gone")
        assert mgr.interrupt_execution_sync("k1") is False


# -- FlowNode.cancel with kernel context --------------------------------------


class TestFlowNodeCancelWithKernel:
    def test_cancel_calls_interrupt(self):
        node = _make_node()
        mock_mgr = MagicMock()
        node._kernel_cancel_context = ("k1", mock_mgr)

        node.cancel()

        mock_mgr.interrupt_execution_sync.assert_called_once_with("k1")
        assert node.node_stats.is_canceled is True

    def test_cancel_without_context(self):
        node = _make_node()
        node.cancel()
        assert node.node_stats.is_canceled is True


    def test_worker_fetcher_takes_priority(self):
        node = _make_node()
        fetcher = MagicMock()
        mock_mgr = MagicMock()
        node._fetch_cached_df = fetcher
        node._kernel_cancel_context = ("k1", mock_mgr)

        node.cancel()

        fetcher.cancel.assert_called_once()
        mock_mgr.interrupt_execution_sync.assert_not_called()
        assert node.node_stats.is_canceled is True

    def test_interrupt_exception_does_not_crash(self):
        node = _make_node()
        mock_mgr = MagicMock()
        mock_mgr.interrupt_execution_sync.side_effect = RuntimeError("Docker unavailable")
        node._kernel_cancel_context = ("k1", mock_mgr)

        node.cancel()  # must not raise
        assert node.node_stats.is_canceled is True
