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
        mgr._scratch_flow_ids = {}
        mgr._shared_volume = "/tmp/test"
        mgr._catalog_tables_dir = "/__catalog_tables_unused__"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
        mgr._catalog_volume = None
        mgr._catalog_volume_type = None
        mgr._catalog_mount_target = None
        mgr._active_execs = {}

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

    def test_token_of_the_running_cell_is_honoured(self):
        mgr = _make_manager()
        mgr._active_execs["k1"] = "tok-running"
        container = MagicMock()
        mgr._docker.containers.get.return_value = container

        assert mgr.interrupt_execution_sync("k1", "tok-running") is True
        container.kill.assert_called_once_with(signal="SIGUSR1")

    def test_token_of_another_execution_interrupts_nothing(self):
        """The reported bug: cancelling a flow queued behind another flow on the
        same kernel must not touch the cell that flow is running."""
        mgr = _make_manager()
        mgr._active_execs["k1"] = "tok-flow-a"

        assert mgr.interrupt_execution_sync("k1", "tok-flow-b") is False
        mgr._docker.containers.get.assert_not_called()

    def test_token_of_a_finished_execution_interrupts_nothing(self):
        """A cell that already released the kernel cannot cancel its successor."""
        mgr = _make_manager()  # nothing active

        assert mgr.interrupt_execution_sync("k1", "tok-finished") is False
        mgr._docker.containers.get.assert_not_called()

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
    def test_cancel_interrupts_its_own_execution(self):
        node = _make_node()
        mock_mgr = MagicMock()
        node._kernel_cancel_context = ("k1", mock_mgr, "tok-1")

        node.cancel()

        mock_mgr.interrupt_execution_sync.assert_called_once_with("k1", "tok-1")
        assert node.node_stats.is_canceled is True

    def test_cancel_never_interrupts_untargeted(self):
        """A bare kernel-wide interrupt would hit whichever flow owns the kernel."""
        node = _make_node()
        mock_mgr = MagicMock()
        node._kernel_cancel_context = ("k1", mock_mgr, "tok-1")

        node.cancel()

        kernel_id, exec_token = mock_mgr.interrupt_execution_sync.call_args[0]
        assert kernel_id == "k1"
        assert exec_token

    def test_cancel_without_context(self):
        node = _make_node()
        node.cancel()
        assert node.node_stats.is_canceled is True


    def test_worker_fetcher_takes_priority(self):
        node = _make_node()
        fetcher = MagicMock()
        mock_mgr = MagicMock()
        node._fetch_cached_df = fetcher
        node._kernel_cancel_context = ("k1", mock_mgr, "tok-1")

        node.cancel()

        fetcher.cancel.assert_called_once()
        mock_mgr.interrupt_execution_sync.assert_not_called()
        assert node.node_stats.is_canceled is True

    def test_interrupt_exception_does_not_crash(self):
        node = _make_node()
        mock_mgr = MagicMock()
        mock_mgr.interrupt_execution_sync.side_effect = RuntimeError("Docker unavailable")
        node._kernel_cancel_context = ("k1", mock_mgr, "tok-1")

        node.cancel()  # must not raise
        assert node.node_stats.is_canceled is True


# -- Cross-flow isolation on a shared kernel ----------------------------------


class TestCrossFlowCancellation:
    """Two flows, one kernel: cancelling either must only stop its own cell."""

    def test_queued_flow_cancel_spares_the_running_flow(self):
        mgr = _make_manager()
        mgr._active_execs["k1"] = "tok-flow-a"  # flow A holds the kernel

        node_b = _make_node()
        node_b._kernel_cancel_context = ("k1", mgr, "tok-flow-b")  # still queued
        node_b.cancel()

        # No interrupt reached the kernel at all — flow A's cell keeps running.
        mgr._docker.containers.get.assert_not_called()
        assert node_b.node_stats.is_canceled is True

    def test_running_flow_cancel_still_interrupts_its_own_cell(self):
        mgr = _make_manager()
        mgr._active_execs["k1"] = "tok-flow-a"
        container = MagicMock()
        mgr._docker.containers.get.return_value = container

        node_a = _make_node()
        node_a._kernel_cancel_context = ("k1", mgr, "tok-flow-a")
        node_a.cancel()

        container.kill.assert_called_once_with(signal="SIGUSR1")
        assert node_a.node_stats.is_canceled is True

    def test_cancel_sets_the_event_even_when_the_interrupt_is_suppressed(self):
        """A queued node must still unblock — it just must not interrupt anyone."""
        import threading

        mgr = _make_manager()
        mgr._active_execs["k1"] = "tok-flow-a"

        node_b = _make_node()
        event = threading.Event()
        node_b._kernel_cancel_context = ("k1", mgr, "tok-flow-b")
        node_b._kernel_cancel_event = event

        node_b.cancel()

        assert event.is_set()
        mgr._docker.containers.get.assert_not_called()
        assert node_b.node_stats.is_canceled is True
