"""Kernel input ordering and the reserved positional ``"main"`` alias (no Docker)."""

from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.kernel.execution import _assert_safe_name, write_inputs_to_parquet
from flowfile_core.kernel.manager import KernelManager, ordered_input_files
from flowfile_core.kernel.models import ExecuteRequest


def _local_manager(shared_volume: Path) -> KernelManager:
    """A KernelManager in local-volume mode, without touching Docker."""
    mgr = KernelManager.__new__(KernelManager)
    mgr._shared_volume = str(shared_volume)
    mgr._catalog_tables_dir = "/__catalog_tables_unused__"
    mgr._kernel_volume = None
    return mgr


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

    def test_interactive_request_matches_flow_run_inputs(self, tmp_path: Path):
        """The designer's rebuilt request lists the same files, in the same order, as the flow run."""
        mgr = _local_manager(tmp_path)
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = tuple(FlowDataEngine(pl.LazyFrame({"position": [i]})) for i in range(11))
        names = [f"df_{i + 1}" for i in range(11)]

        written = write_inputs_to_parquet(tables, mgr, str(input_dir), 1, 2, input_names=names, local=True)
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

    def test_write_inputs_refuses_an_input_named_main(self, tmp_path: Path):
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = (FlowDataEngine(pl.LazyFrame({"a": [1]})), FlowDataEngine(pl.LazyFrame({"b": [2]})))

        with pytest.raises(ValueError, match="'main' is reserved"):
            write_inputs_to_parquet(
                tables, _local_manager(tmp_path), str(input_dir), 1, 2, input_names=["orders", "main"], local=True
            )

    def test_unnamed_inputs_still_use_main(self, tmp_path: Path):
        input_dir = _input_dir(tmp_path, 1, 2)
        tables = (FlowDataEngine(pl.LazyFrame({"a": [1]})), FlowDataEngine(pl.LazyFrame({"b": [2]})))

        result = write_inputs_to_parquet(tables, _local_manager(tmp_path), str(input_dir), 1, 2, local=True)

        assert result == {"main": ["/shared/1/2/inputs/main_0.parquet", "/shared/1/2/inputs/main_1.parquet"]}
