"""Tests for resolve_node_paths and FlowSettings.show_edge_labels."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest
from flowfile_core.schemas.schemas import FlowSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(shared_volume: str) -> KernelManager:
    """Build a KernelManager with a mocked Docker client pointing at *shared_volume*."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._shared_volume = shared_volume
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
    return mgr


def _create_inputs(tmp_path: Path, flow_id: int, node_id: int, filenames: list[str]) -> Path:
    """Create empty parquet stub files and return the input directory."""
    input_dir = tmp_path / str(flow_id) / str(node_id) / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        (input_dir / name).write_bytes(b"")
    return input_dir


# ---------------------------------------------------------------------------
# resolve_node_paths
# ---------------------------------------------------------------------------


class TestResolveNodePaths:
    """Unit tests for KernelManager.resolve_node_paths."""

    def test_named_inputs_parsed(self, tmp_path: Path):
        """Files named {name}_{index}.parquet are grouped under their name key."""
        _create_inputs(tmp_path, 1, 2, ["clients_1.parquet", "orders_0.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert "orders" in req.input_paths
        assert "clients" in req.input_paths
        assert "main" in req.input_paths
        assert len(req.input_paths["orders"]) == 1
        assert len(req.input_paths["clients"]) == 1
        # "main" is the backward-compat alias containing all paths
        assert len(req.input_paths["main"]) == 2

    def test_main_files_grouped(self, tmp_path: Path):
        """Files named main_{idx}.parquet stay under 'main' with no duplicate alias."""
        _create_inputs(tmp_path, 1, 2, ["main_0.parquet", "main_1.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert list(req.input_paths.keys()) == ["main"]
        assert len(req.input_paths["main"]) == 2

    def test_noop_when_input_paths_set(self, tmp_path: Path):
        """If input_paths is already populated, resolve_node_paths is a no-op."""
        _create_inputs(tmp_path, 1, 2, ["orders_0.parquet"])
        mgr = _make_manager(str(tmp_path))

        existing = {"custom": ["/some/path.parquet"]}
        req = ExecuteRequest(node_id=2, code="", flow_id=1, input_paths=existing)
        mgr.resolve_node_paths(req)

        assert req.input_paths == existing

    def test_noop_when_no_flow_id(self, tmp_path: Path):
        """Without flow_id the method is a no-op."""
        mgr = _make_manager(str(tmp_path))
        req = ExecuteRequest(node_id=2, code="", flow_id=0)
        mgr.resolve_node_paths(req)

        assert req.input_paths == {}

    def test_noop_when_no_node_id(self, tmp_path: Path):
        """Without node_id the method is a no-op."""
        mgr = _make_manager(str(tmp_path))
        req = ExecuteRequest(node_id=0, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert req.input_paths == {}

    def test_empty_input_dir(self, tmp_path: Path):
        """An empty input directory results in no input_paths."""
        _create_inputs(tmp_path, 1, 2, [])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert req.input_paths == {}

    def test_no_input_dir(self, tmp_path: Path):
        """If the input directory doesn't exist, input_paths stays empty."""
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert req.input_paths == {}

    def test_output_dir_always_set(self, tmp_path: Path):
        """output_dir is set regardless of whether input files exist."""
        _create_inputs(tmp_path, 1, 2, [])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        expected_suffix = os.path.join("1", "2", "outputs")
        assert req.output_dir.endswith(expected_suffix)

    def test_underscore_in_name(self, tmp_path: Path):
        """A name with underscores (my_orders_0.parquet) groups under 'my_orders'."""
        _create_inputs(tmp_path, 1, 2, ["my_orders_0.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert "my_orders" in req.input_paths
        assert len(req.input_paths["my_orders"]) == 1

    def test_multiple_files_same_name(self, tmp_path: Path):
        """Multiple files with the same name prefix are grouped together."""
        _create_inputs(tmp_path, 1, 2, ["orders_0.parquet", "orders_1.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert len(req.input_paths["orders"]) == 2
        assert "main" in req.input_paths
        assert len(req.input_paths["main"]) == 2

    def test_file_without_index(self, tmp_path: Path):
        """A file without the _index pattern falls back to the 'main' key."""
        _create_inputs(tmp_path, 1, 2, ["noindex.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert list(req.input_paths.keys()) == ["main"]
        assert len(req.input_paths["main"]) == 1

    def test_path_translation_local_mode(self, tmp_path: Path):
        """In local mode, paths are translated from shared_volume to /shared."""
        _create_inputs(tmp_path, 1, 2, ["orders_0.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        # In local mode to_kernel_path replaces _shared_volume with /shared
        for paths in req.input_paths.values():
            for p in paths:
                assert p.startswith("/shared/")
        assert req.output_dir.startswith("/shared/")


# ---------------------------------------------------------------------------
# FlowSettings.show_edge_labels
# ---------------------------------------------------------------------------


class TestFlowSettingsShowEdgeLabels:
    """Tests for the show_edge_labels field on FlowSettings."""

    def test_defaults_true(self):
        settings = FlowSettings(flow_id=1, name="test", path=".")
        assert settings.show_edge_labels is True

    def test_explicit_false(self):
        settings = FlowSettings(flow_id=1, name="test", path=".", show_edge_labels=False)
        assert settings.show_edge_labels is False

    def test_roundtrip(self):
        """model_dump -> model_validate preserves the value."""
        settings = FlowSettings(flow_id=1, name="test", path=".", show_edge_labels=False)
        restored = FlowSettings.model_validate(settings.model_dump())
        assert restored.show_edge_labels is False

    def test_backward_compat_missing_field(self):
        """Old data without show_edge_labels defaults to True."""
        data = {"flow_id": 1, "name": "test", "path": "."}
        settings = FlowSettings.model_validate(data)
        assert settings.show_edge_labels is True
