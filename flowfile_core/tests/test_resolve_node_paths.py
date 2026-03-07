"""Tests for resolve_node_paths, write_inputs_to_parquet, and FlowSettings.show_edge_labels."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flowfile_core.kernel.execution import write_inputs_to_parquet
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest
from flowfile_core.schemas import input_schema
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

    def test_defaults_false(self):
        settings = FlowSettings(flow_id=1, name="test", path=".")
        assert settings.show_edge_labels is False

    def test_explicit_false(self):
        settings = FlowSettings(flow_id=1, name="test", path=".", show_edge_labels=False)
        assert settings.show_edge_labels is False

    def test_roundtrip(self):
        """model_dump -> model_validate preserves the value."""
        settings = FlowSettings(flow_id=1, name="test", path=".", show_edge_labels=False)
        restored = FlowSettings.model_validate(settings.model_dump())
        assert restored.show_edge_labels is False

    def test_backward_compat_missing_field(self):
        """Old data without show_edge_labels defaults to False."""
        data = {"flow_id": 1, "name": "test", "path": "."}
        settings = FlowSettings.model_validate(data)
        assert settings.show_edge_labels is False


# ---------------------------------------------------------------------------
# write_inputs_to_parquet
# ---------------------------------------------------------------------------


def _mock_fetcher(has_error=False, error_description=None):
    """Return a mock ExternalDfFetcher that succeeds or fails."""
    fetcher = MagicMock()
    fetcher.has_error = has_error
    fetcher.error_description = error_description
    return fetcher


class TestWriteInputsToParquet:
    """Unit tests for kernel.execution.write_inputs_to_parquet."""

    def test_unnamed_inputs_all_under_main(self, tmp_path: Path):
        """Without input_names, all files are grouped under 'main'."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        ft1, ft2 = MagicMock(), MagicMock()
        with patch(
            "flowfile_core.kernel.execution.ExternalDfFetcher",
            side_effect=lambda **kw: _mock_fetcher(),
        ):
            result = write_inputs_to_parquet((ft1, ft2), mgr, input_dir, 1, 2)

        assert list(result.keys()) == ["main"]
        assert len(result["main"]) == 2
        # Paths should be kernel-translated
        for p in result["main"]:
            assert p.startswith("/shared/")

    def test_named_inputs_grouped_by_name(self, tmp_path: Path):
        """With input_names, each table gets its own key plus a 'main' alias."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        ft1, ft2 = MagicMock(), MagicMock()
        with patch(
            "flowfile_core.kernel.execution.ExternalDfFetcher",
            side_effect=lambda **kw: _mock_fetcher(),
        ):
            result = write_inputs_to_parquet((ft1, ft2), mgr, input_dir, 1, 2, input_names=["orders", "clients"])

        assert "orders" in result
        assert "clients" in result
        assert "main" in result
        assert len(result["orders"]) == 1
        assert len(result["clients"]) == 1
        assert len(result["main"]) == 2

    def test_named_inputs_main_no_duplicate(self, tmp_path: Path):
        """When one input is named 'main', no extra 'main' alias is added."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        ft1 = MagicMock()
        with patch(
            "flowfile_core.kernel.execution.ExternalDfFetcher",
            side_effect=lambda **kw: _mock_fetcher(),
        ):
            result = write_inputs_to_parquet((ft1,), mgr, input_dir, 1, 2, input_names=["main"])

        assert list(result.keys()) == ["main"]
        assert len(result["main"]) == 1

    def test_unnamed_fetcher_error_raises(self, tmp_path: Path):
        """An error in ExternalDfFetcher raises RuntimeError (unnamed path)."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        ft1 = MagicMock()
        with patch(
            "flowfile_core.kernel.execution.ExternalDfFetcher",
            side_effect=lambda **kw: _mock_fetcher(has_error=True, error_description="disk full"),
        ):
            with pytest.raises(RuntimeError, match="Failed to write parquet"):
                write_inputs_to_parquet((ft1,), mgr, input_dir, 1, 2)

    def test_named_fetcher_error_raises(self, tmp_path: Path):
        """An error in ExternalDfFetcher raises RuntimeError (named path)."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        ft1 = MagicMock()
        with patch(
            "flowfile_core.kernel.execution.ExternalDfFetcher",
            side_effect=lambda **kw: _mock_fetcher(has_error=True, error_description="disk full"),
        ):
            with pytest.raises(RuntimeError, match="orders"):
                write_inputs_to_parquet((ft1,), mgr, input_dir, 1, 2, input_names=["orders"])

    def test_empty_tuple_returns_empty_main(self, tmp_path: Path):
        """An empty tuple of tables returns {"main": []}."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        result = write_inputs_to_parquet((), mgr, input_dir, 1, 2)
        assert result == {"main": []}


# ---------------------------------------------------------------------------
# NodePythonScript / UserDefinedNode output_names
# ---------------------------------------------------------------------------


class TestOutputNamesSchema:
    """Tests for the output_names field on NodePythonScript and UserDefinedNode."""

    def test_python_script_defaults_to_main(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1)
        assert node.output_names == ["main"]

    def test_python_script_custom_output_names(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, output_names=["result", "errors"])
        assert node.output_names == ["result", "errors"]

    def test_python_script_roundtrip(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, output_names=["a", "b"])
        restored = input_schema.NodePythonScript.model_validate(node.model_dump())
        assert restored.output_names == ["a", "b"]

    def test_user_defined_node_defaults_to_main(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1, settings={}, kernel_id=None)
        assert node.output_names == ["main"]

    def test_user_defined_node_custom_output_names(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1, settings={}, output_names=["out1", "out2"])
        assert node.output_names == ["out1", "out2"]


# ---------------------------------------------------------------------------
# NodeBase.node_reference validator
# ---------------------------------------------------------------------------


class TestNodeReferenceValidator:
    """Tests for the node_reference field_validator on NodeBase."""

    def test_none_stays_none(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference=None)
        assert node.node_reference is None

    def test_empty_string_becomes_none(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="")
        assert node.node_reference is None

    def test_valid_lowercase(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="my_ref")
        assert node.node_reference == "my_ref"

    def test_rejects_uppercase(self):
        with pytest.raises(Exception, match="lowercase"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="MyRef")

    def test_rejects_spaces(self):
        with pytest.raises(Exception, match="spaces"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="my ref")

    def test_rejects_path_traversal(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="../../etc/passwd")

    def test_rejects_slashes(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="foo/bar")

    def test_rejects_dots(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="foo.bar")

    def test_rejects_starts_with_digit(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, node_reference="1abc")


class TestOutputNamesValidator:
    """Tests for the output_names field_validator on NodePythonScript and UserDefinedNode."""

    def test_rejects_unsafe_chars(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, output_names=["../evil"])

    def test_rejects_duplicates(self):
        with pytest.raises(Exception, match="unique"):
            input_schema.NodePythonScript(flow_id=1, node_id=1, output_names=["out", "out"])

    def test_valid_names_pass(self):
        node = input_schema.NodePythonScript(flow_id=1, node_id=1, output_names=["clean", "rejected"])
        assert node.output_names == ["clean", "rejected"]

    def test_user_defined_node_rejects_unsafe(self):
        with pytest.raises(Exception, match="lowercase letters, digits, and underscores"):
            input_schema.UserDefinedNode(flow_id=1, node_id=1, settings={}, output_names=["foo/bar"])
