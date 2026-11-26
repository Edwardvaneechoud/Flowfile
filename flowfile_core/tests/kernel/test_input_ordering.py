"""Kernel input ordering and the reserved positional ``"main"`` alias (no Docker)."""

from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.registry import LOCAL_BACKEND_ID
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.kernel.execution import _assert_safe_name, write_inputs_to_parquet
from flowfile_core.kernel.manager import KernelManager, ordered_input_files
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult
from tests.flowfile.execution.graph_helpers import add_manual_input, add_promise, connect, make_graph


def _local_manager(shared_volume: Path) -> KernelManager:
    """A KernelManager in local-volume mode, without touching Docker."""
    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(shared_volume)
    mgr._catalog_tables_dir = "/__catalog_tables_unused__"
    mgr._kernel_volume = None
    return mgr


class _AnsweringManager(KernelManager):
    """A local-volume manager whose kernel answers every cell with success and no output, keeping the requests."""

    def execute_sync(self, kernel_id, request, flow_logger=None, cancel_event=None):
        self.requests.append(request)
        return ExecuteResult(success=True)


def _answering_manager(shared_volume: Path) -> _AnsweringManager:
    mgr = _AnsweringManager.__new__(_AnsweringManager)
    mgr._shared_volume = str(shared_volume)
    mgr._catalog_tables_dir = "/__catalog_tables_unused__"
    mgr._kernel_volume = None
    mgr._docker_network = None
    mgr.requests = []
    return mgr


@pytest.fixture
def in_core():
    """The inputs are written by core's own backend, as in a local flow's run."""
    with use_context(ExecutionContext(LOCAL_BACKEND_ID, parallelism=1, preview_rows=None)) as context:
        yield context


def _input_dir(shared_volume: Path, flow_id: int, node_id: int) -> Path:
    path = shared_volume / str(flow_id) / str(node_id) / "inputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


class TestOrderedInputFiles:
    def test_orders_by_index_not_lexicographically(self):
        assert sorted(["df_10_1.parquet", "df_9_0.parquet"]) == ["df_10_1.parquet", "df_9_0.parquet"]
        assert ordered_input_files(["df_10_1.parquet", "df_9_0.parquet"]) == ["df_9_0.parquet", "df_10_1.parquet"]

    def test_eleven_inputs_follow_wiring_order(self):
        names = [f"df_{i + 1}_{i}.parquet" for i in range(11)]
        assert ordered_input_files(sorted(names)) == names

    def test_mixed_names(self):
        names = ["noindex.parquet", "orders_10.parquet", "my_clients_2.parquet", "main_0.parquet", "b_x.parquet"]
        assert ordered_input_files(names) == [
            "main_0.parquet",
            "my_clients_2.parquet",
            "orders_10.parquet",
            "b_x.parquet",
            "noindex.parquet",
        ]

    def test_empty(self):
        assert ordered_input_files([]) == []


class TestInteractiveRequestMain:
    def test_main_is_every_input_in_index_order(self, tmp_path: Path):
        input_dir = _input_dir(tmp_path, 1, 2)
        names = [f"df_{i + 1}_{i}.parquet" for i in range(11)]
        for name in names:
            (input_dir / name).write_bytes(b"")

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        _local_manager(tmp_path).resolve_node_paths(req)

        assert req.input_paths["main"] == [f"/shared/1/2/inputs/{name}" for name in names]
        assert req.input_paths["df_11"] == ["/shared/1/2/inputs/df_11_10.parquet"]

    def test_main_overwrites_a_name_grouped_under_main(self, tmp_path: Path):
        input_dir = _input_dir(tmp_path, 1, 2)
        for name in ["noindex.parquet", "orders_0.parquet"]:
            (input_dir / name).write_bytes(b"")

        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        _local_manager(tmp_path).resolve_node_paths(req)

        assert req.input_paths["main"] == [
            "/shared/1/2/inputs/orders_0.parquet",
            "/shared/1/2/inputs/noindex.parquet",
        ]
        assert req.input_paths["orders"] == ["/shared/1/2/inputs/orders_0.parquet"]

    def test_interactive_request_matches_flow_run_inputs(self, tmp_path: Path, in_core):
        """The designer's rebuilt request lists the same files, in the same order, as the flow run."""
        mgr = _local_manager(tmp_path)
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = tuple(FlowDataEngine(pl.LazyFrame({"position": [i]})) for i in range(11))
        names = [f"df_{i + 1}" for i in range(11)]

        written = write_inputs_to_parquet(tables, mgr, str(input_dir), 1, 2, input_names=names)
        req = ExecuteRequest(node_id=2, code="", flow_id=1)
        mgr.resolve_node_paths(req)

        assert req.input_paths == written
        local_files = [tmp_path / p.removeprefix("/shared/") for p in req.input_paths["main"]]
        assert [pl.read_parquet(f)["position"][0] for f in local_files] == list(range(11))


class TestReservedMainName:
    def test_assert_safe_name_refuses_main(self):
        with pytest.raises(ValueError, match="node_reference"):
            _assert_safe_name("main")

    def test_similar_names_are_allowed(self):
        _assert_safe_name("main_orders")
        _assert_safe_name("mains")

    def test_write_inputs_refuses_an_input_named_main(self, tmp_path: Path, in_core):
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = (FlowDataEngine(pl.LazyFrame({"a": [1]})), FlowDataEngine(pl.LazyFrame({"b": [2]})))

        with pytest.raises(ValueError, match="'main' is reserved"):
            write_inputs_to_parquet(
                tables, _local_manager(tmp_path), str(input_dir), 1, 2, input_names=["orders", "main"]
            )

    def test_unnamed_inputs_still_use_main(self, tmp_path: Path, in_core):
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = (FlowDataEngine(pl.LazyFrame({"a": [1]})), FlowDataEngine(pl.LazyFrame({"b": [2]})))

        result = write_inputs_to_parquet(tables, _local_manager(tmp_path), str(input_dir), 1, 2)

        assert result == {"main": ["/shared/1/2/inputs/main_0.parquet", "/shared/1/2/inputs/main_1.parquet"]}


class TestExchangeDirs:
    def test_a_nodes_folders_are_under_the_shared_folder(self, tmp_path: Path):
        inputs, outputs = _local_manager(tmp_path).exchange_dirs("k1", 1, 2)

        assert Path(inputs) == tmp_path / "1" / "2" / "inputs"
        assert Path(outputs) == tmp_path / "1" / "2" / "outputs"


class TestPositionalInputs:
    """``_execute_on_kernel(named_inputs=False)``: inputs by position only, whatever their sources are called."""

    TABLES = (FlowDataEngine(pl.LazyFrame({"a": [1]})), FlowDataEngine(pl.LazyFrame({"b": [2]})))

    def _run(self, flow_id: int, manager: KernelManager, monkeypatch, **kwargs):
        """Kernel node 3 over two sources, the first with the reference ``main``."""
        graph = make_graph(flow_id, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}], node_reference="main")
        add_manual_input(graph, 2, [{"b": 2}])
        add_promise(graph, 3, "python_script")
        connect(graph, 1, 3)
        connect(graph, 2, 3)
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)
        return graph._execute_on_kernel(
            node_id=3, kernel_id="k1", code="", output_names=["main"], flow_data_engine=self.TABLES, **kwargs
        )

    def test_a_source_called_main_is_refused_when_inputs_are_named(self, tmp_path: Path, in_core, monkeypatch):
        manager = _answering_manager(tmp_path)

        with pytest.raises(ValueError, match="'main' is reserved"):
            self._run(91431, manager, monkeypatch)

        assert manager.requests == []

    def test_positional_inputs_go_under_main_in_wiring_order(self, tmp_path: Path, in_core, monkeypatch):
        manager = _answering_manager(tmp_path)

        self._run(91432, manager, monkeypatch, named_inputs=False)

        (request,) = manager.requests
        assert request.input_paths == {
            "main": ["/shared/91432/3/inputs/main_0.parquet", "/shared/91432/3/inputs/main_1.parquet"]
        }
        assert request.output_dir == "/shared/91432/3/outputs"
        written = [tmp_path / "91432" / "3" / "inputs" / f"main_{i}.parquet" for i in range(2)]
        assert [pl.read_parquet(path).columns for path in written] == [["a"], ["b"]]
