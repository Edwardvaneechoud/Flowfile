"""Tests for resolve_node_paths, to_kernel_path, write_inputs_to_parquet, read_kernel_outputs,
and FlowSettings.show_edge_labels."""

import os
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, TaskFailed
from flowfile_core.flowfile.execution.protocol import OperationKind, OpResult
from flowfile_core.flowfile.execution.payloads import FileWritePayload, unpack
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.kernel.execution import (
    clear_stale_parquets,
    read_kernel_outputs,
    write_inputs_to_parquet,
    write_parquet_for_kernel,
)
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import FlowSettings

# Helpers


def _make_manager(shared_volume: str) -> KernelManager:
    """Build a KernelManager with a mocked Docker client pointing at *shared_volume*."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._scratch_flow_ids = {}
        mgr._shared_volume = shared_volume
        # Catalog tables dir is irrelevant for resolve_node_paths tests but
        # ``to_kernel_path`` references it; point at a sentinel outside the
        # shared tree so the prefix-match branch never accidentally fires.
        mgr._catalog_tables_dir = "/__catalog_tables_unused__"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
        mgr._catalog_volume = None
        mgr._catalog_volume_type = None
        mgr._catalog_mount_target = None
    return mgr


def _create_inputs(tmp_path: Path, flow_id: int, node_id: int, filenames: list[str]) -> Path:
    """Create empty parquet stub files and return the input directory."""
    input_dir = tmp_path / str(flow_id) / str(node_id) / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        (input_dir / name).write_bytes(b"")
    return input_dir


# resolve_node_paths


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
        """output_dir is set regardless of whether input files exist, as a pure-POSIX path."""
        _create_inputs(tmp_path, 1, 2, [])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert req.output_dir == "/shared/1/2/outputs"

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

        for paths in req.input_paths.values():
            for p in paths:
                assert p.startswith("/shared/")
        assert req.output_dir.startswith("/shared/")


# to_kernel_path — Windows host paths must yield pure-POSIX container paths


_WIN_SHARED = r"C:\Users\u\.flowfile\temp\kernel_shared"
_WIN_CATALOG = r"C:\Users\u\.flowfile\catalog_tables"


class TestToKernelPathWindows:
    """Regression tests for the Windows mixed-separator path bug (kernel exchange)."""

    def test_windows_output_dir_pure_posix(self):
        mgr = _make_manager(_WIN_SHARED)
        result = mgr.to_kernel_path(_WIN_SHARED + r"\1\5\outputs")
        assert result == "/shared/1/5/outputs"
        assert "\\" not in result

    def test_windows_input_file(self):
        mgr = _make_manager(_WIN_SHARED)
        result = mgr.to_kernel_path(_WIN_SHARED + r"\1\5\inputs\orders_0.parquet")
        assert result == "/shared/1/5/inputs/orders_0.parquet"

    def test_windows_persistence_path(self):
        """PERSISTENCE_PATH used to come out as /shared\\artifacts on Windows."""
        mgr = _make_manager(_WIN_SHARED)
        assert mgr.to_kernel_path(_WIN_SHARED + r"\artifacts") == "/shared/artifacts"

    def test_windows_exact_prefix(self):
        mgr = _make_manager(_WIN_SHARED)
        assert mgr.to_kernel_path(_WIN_SHARED) == "/shared"

    def test_windows_catalog_prefix_wins(self):
        mgr = _make_manager(_WIN_SHARED)
        mgr._catalog_tables_dir = _WIN_CATALOG
        assert mgr.to_kernel_path(_WIN_CATALOG + r"\orders_ab12") == "/catalog_tables/orders_ab12"
        assert mgr.to_kernel_path(_WIN_CATALOG) == "/catalog_tables"

    def test_windows_unknown_prefix_passthrough(self):
        mgr = _make_manager(_WIN_SHARED)
        other = r"D:\elsewhere\data.parquet"
        assert mgr.to_kernel_path(other) == other

    def test_posix_paths_unchanged(self):
        mgr = _make_manager("/home/u/.flowfile/temp/kernel_shared")
        result = mgr.to_kernel_path("/home/u/.flowfile/temp/kernel_shared/1/5/outputs")
        assert result == "/shared/1/5/outputs"

    def test_posix_prefix_no_partial_match(self):
        """A sibling dir sharing the prefix string must not be rebased."""
        mgr = _make_manager("/home/u/shared")
        assert mgr.to_kernel_path("/home/u/shared_other/x.parquet") == "/home/u/shared_other/x.parquet"

    def test_dind_identity(self):
        mgr = _make_manager(_WIN_SHARED)
        mgr._kernel_volume = "flowfile-internal-storage"
        path = "/app/internal_storage/temp/kernel_shared/1/5/outputs"
        assert mgr.to_kernel_path(path) == path

    def test_resolve_node_paths_windows_shared_volume(self, tmp_path: Path):
        """End-to-end: resolve_node_paths on a real dir but with Windows-style prefix registered."""
        _create_inputs(tmp_path, 1, 2, ["orders_0.parquet"])
        mgr = _make_manager(str(tmp_path))

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        for paths in req.input_paths.values():
            for p in paths:
                assert "\\" not in p
        assert "\\" not in req.output_dir


# read_kernel_outputs


class _FakeNode:
    def __init__(self):
        self._named_outputs = {}


def _execute_result(output_paths: list[str]) -> ExecuteResult:
    return ExecuteResult(success=True, output_paths=output_paths)


class TestReadKernelOutputs:
    """Unit tests for kernel.execution.read_kernel_outputs."""

    def test_reads_primary_output(self, tmp_path: Path):
        pl.DataFrame({"a": [1, 2]}).write_parquet(tmp_path / "main.parquet")
        node = _FakeNode()

        result = read_kernel_outputs(
            output_dir=str(tmp_path),
            output_names=["main"],
            result=_execute_result([str(tmp_path / "main.parquet")]),
            node=node,
        )

        assert isinstance(result, FlowDataEngine)
        assert "output-0" in node._named_outputs
        assert result.data_frame.collect().shape == (2, 1)

    def test_multiple_named_outputs(self, tmp_path: Path):
        pl.DataFrame({"a": [1]}).write_parquet(tmp_path / "clean.parquet")
        pl.DataFrame({"b": [2]}).write_parquet(tmp_path / "rejected.parquet")
        node = _FakeNode()

        result = read_kernel_outputs(
            output_dir=str(tmp_path),
            output_names=["clean", "rejected"],
            result=_execute_result(["clean.parquet", "rejected.parquet"]),
            node=node,
        )

        assert set(node._named_outputs) == {"output-0", "output-1"}
        assert result.data_frame.collect().columns == ["a"]

    def test_none_when_nothing_published(self, tmp_path: Path):
        """Empty kernel output_paths -> None (caller's input-passthrough fallback)."""
        result = read_kernel_outputs(
            output_dir=str(tmp_path),
            output_names=["main"],
            result=_execute_result([]),
            node=_FakeNode(),
        )
        assert result is None

    def test_raises_on_translation_mismatch(self, tmp_path: Path):
        """Kernel reported outputs but none exist locally -> loud error, not ghost data."""
        with pytest.raises(RuntimeError, match="mount mismatch"):
            read_kernel_outputs(
                output_dir=str(tmp_path),
                output_names=["main"],
                result=_execute_result(["/shared\\1\\5\\outputs/main.parquet"]),
                node=_FakeNode(),
            )

    def test_raises_on_output_name_mismatch(self, tmp_path: Path):
        """publish_output under a name not in output_names -> naming error, not a mount error."""
        with pytest.raises(RuntimeError, match="expects outputs named"):
            read_kernel_outputs(
                output_dir=str(tmp_path),
                output_names=["main"],
                result=_execute_result(["/shared/1/5/outputs/results.parquet"]),
                node=_FakeNode(),
            )

    def test_no_raise_when_some_outputs_found(self, tmp_path: Path):
        """A partial match (only a secondary output) does not trip the guard."""
        pl.DataFrame({"b": [2]}).write_parquet(tmp_path / "rejected.parquet")
        node = _FakeNode()

        result = read_kernel_outputs(
            output_dir=str(tmp_path),
            output_names=["clean", "rejected"],
            result=_execute_result(["rejected.parquet"]),
            node=node,
        )

        assert result is None
        assert set(node._named_outputs) == {"output-1"}

    def test_node_none_supported(self, tmp_path: Path):
        pl.DataFrame({"a": [1]}).write_parquet(tmp_path / "main.parquet")
        result = read_kernel_outputs(
            output_dir=str(tmp_path),
            output_names=["main"],
            result=_execute_result(["main.parquet"]),
            node=None,
        )
        assert isinstance(result, FlowDataEngine)


# FlowSettings.show_edge_labels


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


# write_inputs_to_parquet: the kernel parquet hand-off


class _HandoffRecorder:
    """Stands in for the execution context's compute backend: keeps every operation, failing it when told to."""

    id = "handoff-recorder"

    def __init__(self) -> None:
        self.operations: list[tuple[OperationKind, FileWritePayload]] = []
        self.error: Exception | None = None

    def run_op(self, op, payload):
        self.operations.append((op, unpack(payload, FileWritePayload)))
        if self.error is not None:
            raise self.error
        return OpResult(refs={})


class _PathManager:
    def to_kernel_path(self, path: str) -> str:
        return path


@pytest.fixture
def recorder(monkeypatch) -> _HandoffRecorder:
    """The default compute backend for the test: a recorder, so a call without a run's context or a graph reaches it."""
    backend = _HandoffRecorder()
    registry.reset()
    registry.register_backend(backend.id, lambda: backend)
    monkeypatch.setenv(registry.BACKEND_ENV_VAR, backend.id)
    yield backend
    registry.reset()


@pytest.fixture
def in_core():
    """A run on core's own backend."""
    with use_context(ExecutionContext(registry.LOCAL_BACKEND_ID, parallelism=1, preview_rows=None)) as context:
        yield context


def _tables() -> tuple[FlowDataEngine, FlowDataEngine]:
    return FlowDataEngine(pl.LazyFrame({"a": [1, 2]})), FlowDataEngine(pl.LazyFrame({"b": ["x"]}))


class TestWriteInputsToParquet:
    """Unit tests for kernel.execution.write_inputs_to_parquet."""

    def test_unnamed_inputs_all_under_main(self, tmp_path: Path, recorder):
        """Without input_names, all files are grouped under 'main', each written by a file_write."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)
        ft1, ft2 = _tables()

        result = write_inputs_to_parquet((ft1, ft2), mgr, input_dir, 1, 2)

        assert list(result.keys()) == ["main"]
        assert len(result["main"]) == 2
        for p in result["main"]:
            assert p.startswith("/shared/")
        assert [op for op, _ in recorder.operations] == [OperationKind.file_write] * 2
        assert [write.settings.abs_file_path for _, write in recorder.operations] == [
            os.path.join(input_dir, "main_0.parquet"),
            os.path.join(input_dir, "main_1.parquet"),
        ]
        assert [write.plan for _, write in recorder.operations] == [ft1.data_frame, ft2.data_frame]

    def test_named_inputs_grouped_by_name(self, tmp_path: Path, recorder):
        """With input_names, each table gets its own key plus a 'main' alias."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        result = write_inputs_to_parquet(_tables(), mgr, input_dir, 1, 2, input_names=["orders", "clients"])

        assert "orders" in result
        assert "clients" in result
        assert "main" in result
        assert len(result["orders"]) == 1
        assert len(result["clients"]) == 1
        assert len(result["main"]) == 2
        assert [os.path.basename(write.settings.abs_file_path) for _, write in recorder.operations] == [
            "orders_0.parquet",
            "clients_1.parquet",
        ]

    def test_named_input_main_refused(self, tmp_path: Path, recorder):
        """An input named 'main' is refused: that key is the positional list of every input."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        with pytest.raises(ValueError, match="'main' is reserved"):
            write_inputs_to_parquet(_tables()[:1], mgr, input_dir, 1, 2, input_names=["main"])
        assert recorder.operations == []

    def test_a_write_is_a_parquet_file_write_at_the_exact_path(self, tmp_path: Path, recorder):
        """What the backend gets: the rows as a plan, a parquet file at the path as given, and the flow and node."""
        input_dir = tmp_path / "flow" / "7" / "inputs"
        frame = pl.DataFrame({"a": [1, 2]})

        write_inputs_to_parquet((FlowDataEngine(frame),), _PathManager(), str(input_dir), 7, 9, input_names=["orders"])

        ((op, write),) = recorder.operations
        assert op is OperationKind.file_write
        assert write.settings.abs_file_path == str(input_dir / "orders_0.parquet")
        assert (write.settings.file_type, write.settings.write_mode, write.settings.compression) == (
            "parquet",
            "overwrite",
            "zstd",
        )
        assert (write.flow_id, write.node_id) == (7, 9)
        assert isinstance(write.plan, pl.LazyFrame)
        assert write.plan.collect().equals(frame)
        assert input_dir.is_dir(), "the folder exists before the backend writes into it"

    @pytest.mark.parametrize("input_names", [None, ["orders"]], ids=["unnamed", "named"])
    def test_a_compute_error_raises_a_runtime_error_naming_the_input(self, tmp_path: Path, recorder, input_names):
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        recorder.error = TaskFailed("disk full")
        expected = "input 0 (orders)" if input_names else "input 0"

        with pytest.raises(RuntimeError, match=rf"Failed to write parquet for {re.escape(expected)}: disk full"):
            write_inputs_to_parquet(_tables()[:1], mgr, input_dir, 1, 2, input_names=input_names)

    def test_an_unreachable_compute_raises_a_runtime_error_too(self, tmp_path: Path, recorder):
        recorder.error = ComputeUnavailable("Could not reach the worker")

        with pytest.raises(RuntimeError, match="Failed to write parquet for input 0: Could not reach the worker"):
            write_inputs_to_parquet(_tables()[:1], _PathManager(), str(tmp_path / "inputs"), 1, 2)

    def test_another_error_propagates_as_raised(self, tmp_path: Path, recorder):
        recorder.error = OSError("disk full")

        with pytest.raises(OSError, match="disk full"):
            write_inputs_to_parquet(_tables()[:1], _PathManager(), str(tmp_path / "inputs"), 1, 2)

    def test_empty_tuple_returns_empty_main(self, tmp_path: Path, recorder):
        """An empty tuple of tables returns {"main": []}."""
        mgr = _make_manager(str(tmp_path))
        input_dir = str(tmp_path / "inputs")
        os.makedirs(input_dir, exist_ok=True)

        result = write_inputs_to_parquet((), mgr, input_dir, 1, 2)
        assert result == {"main": []}
        assert recorder.operations == []

    def test_a_run_in_core_writes_the_files_in_process(self, tmp_path: Path, recorder, in_core):
        """In a run on core's own backend the files are written here, whatever the default backend is."""
        input_dir = str(tmp_path / "inputs")
        ft1, ft2 = _tables()

        result = write_inputs_to_parquet((ft1, ft2), _PathManager(), input_dir, 1, 2, input_names=["orders", "clients"])

        assert recorder.operations == []
        assert pl.read_parquet(result["orders"][0]).to_dict(as_series=False) == {"a": [1, 2]}
        assert pl.read_parquet(result["clients"][0]).to_dict(as_series=False) == {"b": ["x"]}
        assert result["main"] == result["orders"] + result["clients"]

    @pytest.mark.parametrize(
        ("execution_location", "expected_writes"), [("local", 0), ("remote", 2)], ids=["local", "remote"]
    )
    def test_outside_a_run_the_graph_names_the_backend(
        self, tmp_path: Path, recorder, execution_location, expected_writes
    ):
        """A local graph writes in core; a remote one on the default backend, here the recorder."""
        from tests.flowfile.conftest import create_test_graph

        graph = create_test_graph(flow_id=8801, execution_location=execution_location)
        input_dir = str(tmp_path / "inputs")

        result = write_inputs_to_parquet(_tables(), _PathManager(), input_dir, 1, 2, graph=graph)

        assert len(recorder.operations) == expected_writes
        assert all(os.path.isfile(path) for path in result["main"]) == (execution_location == "local")

    def test_a_run_context_wins_over_the_graph(self, tmp_path: Path, recorder, in_core):
        from tests.flowfile.conftest import create_test_graph

        graph = create_test_graph(flow_id=8802, execution_location="remote")

        write_inputs_to_parquet(_tables(), _PathManager(), str(tmp_path / "inputs"), 1, 2, graph=graph)

        assert recorder.operations == []


class TestWriteParquetForKernel:
    """write_parquet_for_kernel on core's own backend: a complete parquet file when it returns."""

    def test_writes_valid_parquet(self, tmp_path: Path, in_core):
        """Creates a valid parquet file from a LazyFrame."""
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        output_path = str(tmp_path / "sub" / "test.parquet")
        write_parquet_for_kernel(lf, output_path, flow_id=1, node_id=2, what="input 0")

        df = pl.read_parquet(output_path)
        assert df.shape == (3, 2)
        assert df.columns == ["a", "b"]

    def test_accepts_dataframe(self, tmp_path: Path, in_core):
        """Accepts a DataFrame directly."""
        df_in = pl.DataFrame({"x": [10, 20]})
        output_path = str(tmp_path / "test.parquet")
        write_parquet_for_kernel(df_in, output_path, flow_id=1, node_id=2, what="input 0")

        df_out = pl.read_parquet(output_path)
        assert df_out.shape == (2, 1)
        assert df_out["x"].to_list() == [10, 20]

    def test_creates_parent_dirs(self, tmp_path: Path, in_core):
        """Creates parent directories if they don't exist."""
        lf = pl.LazyFrame({"a": [1]})
        output_path = str(tmp_path / "deep" / "nested" / "dir" / "test.parquet")
        write_parquet_for_kernel(lf, output_path, flow_id=1, node_id=2, what="input 0")

        assert os.path.exists(output_path)

    def test_the_file_is_synced_to_disk_before_it_returns(self, tmp_path: Path, in_core, monkeypatch):
        """Another process (a kernel container) reads the file next, so it is fsync'd, not only closed."""
        synced = []
        real_fsync = os.fsync

        def recording_fsync(descriptor):
            synced.append(os.fstat(descriptor).st_ino)
            real_fsync(descriptor)

        monkeypatch.setattr(os, "fsync", recording_fsync)
        output_path = tmp_path / "test.parquet"

        rows = pl.LazyFrame({"a": list(range(1000))})
        write_parquet_for_kernel(rows, str(output_path), flow_id=1, node_id=2, what="x")

        assert synced == [output_path.stat().st_ino]
        assert pl.read_parquet(output_path)["a"].to_list() == list(range(1000))

    def test_overwrites_the_previous_file(self, tmp_path: Path, in_core):
        output_path = str(tmp_path / "test.parquet")
        write_parquet_for_kernel(pl.LazyFrame({"a": [1, 2, 3]}), output_path, flow_id=1, node_id=2, what="x")

        write_parquet_for_kernel(pl.LazyFrame({"a": [9]}), output_path, flow_id=1, node_id=2, what="x")

        assert pl.read_parquet(output_path)["a"].to_list() == [9]


# NodePythonScript / UserDefinedNode output_names


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


# NodeBase.node_reference validator


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


class TestClearStaleParquets:
    """Unit tests for kernel.execution.clear_stale_parquets."""

    def test_removes_stale_parquets(self, tmp_path: Path):
        (tmp_path / "old_input_0.parquet").write_bytes(b"stale")
        (tmp_path / "main.parquet").write_bytes(b"stale")

        clear_stale_parquets(str(tmp_path))

        assert list(tmp_path.iterdir()) == []

    def test_leaves_non_parquet_files(self, tmp_path: Path):
        (tmp_path / "main.parquet").write_bytes(b"stale")
        (tmp_path / "notes.txt").write_text("keep me")

        clear_stale_parquets(str(tmp_path))

        assert [p.name for p in tmp_path.iterdir()] == ["notes.txt"]

    def test_missing_dir_is_noop(self, tmp_path: Path):
        clear_stale_parquets(str(tmp_path / "does_not_exist"))
