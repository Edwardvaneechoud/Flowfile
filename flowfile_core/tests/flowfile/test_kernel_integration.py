"""
Integration tests for the Docker-based kernel system.

These tests require Docker to be available.  The ``kernel_manager`` fixture
(session-scoped, defined in conftest.py) builds the flowfile-kernel image,
starts a container, and tears it down after all tests in this module finish.
"""

import asyncio
import os
from pathlib import Path
from typing import Literal

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult
from flowfile_core.schemas import input_schema, schemas


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_flowfile.py)
# ---------------------------------------------------------------------------


def _run(coro):
    """Run an async coroutine from sync test code."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _create_graph(
    flow_id: int = 1,
    execution_mode: Literal["Development", "Performance"] = "Development",
    execution_location: Literal["local", "remote"] | None = "remote",
) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="kernel_test_flow",
            path=".",
            execution_mode=execution_mode,
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _handle_run_info(run_info: RunInformation):
    if not run_info.success:
        errors = "errors:"
        for step in run_info.node_step_result:
            if not step.success:
                errors += f"\n  node_id:{step.node_id}, error: {step.error}"
        raise ValueError(f"Graph should run successfully:\n{errors}")


# ---------------------------------------------------------------------------
# Tests — kernel runtime (direct manager interaction)
# ---------------------------------------------------------------------------


class TestKernelRuntime:
    """Tests that exercise the kernel container directly via KernelManager."""

    def test_health_check(self, kernel_manager: tuple[KernelManager, str]):
        """Kernel container responds to health checks."""
        manager, kernel_id = kernel_manager
        info = _run(manager.get_kernel(kernel_id))
        assert info is not None
        assert info.state.value == "idle"

    def test_execute_print(self, kernel_manager: tuple[KernelManager, str]):
        """Simple print() produces stdout."""
        manager, kernel_id = kernel_manager
        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code='print("hello from kernel")',
                    input_paths={},
                    output_dir="/shared/test_print",
                ),
            )
        )
        assert result.success
        assert "hello from kernel" in result.stdout
        assert result.error is None

    def test_execute_syntax_error(self, kernel_manager: tuple[KernelManager, str]):
        """Syntax errors are captured, not raised."""
        manager, kernel_id = kernel_manager
        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=2,
                    code="def broken(",
                    input_paths={},
                    output_dir="/shared/test_syntax_err",
                ),
            )
        )
        assert not result.success
        assert result.error is not None

    def test_publish_and_list_artifacts(self, kernel_manager: tuple[KernelManager, str]):
        """publish_artifact stores an object; list_artifacts returns metadata."""
        manager, kernel_id = kernel_manager

        # Clear any leftover artifacts from previous tests
        _run(manager.clear_artifacts(kernel_id))

        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=3,
                    code='flowfile.publish_artifact("my_dict", {"a": 1, "b": 2})',
                    input_paths={},
                    output_dir="/shared/test_artifact",
                ),
            )
        )
        assert result.success
        assert "my_dict" in result.artifacts_published

    def test_read_and_write_parquet(self, kernel_manager: tuple[KernelManager, str]):
        """Kernel can read input parquet and write output parquet."""
        manager, kernel_id = kernel_manager
        shared = manager.shared_volume_path

        # Prepare input parquet
        input_dir = os.path.join(shared, "test_rw", "inputs")
        output_dir = os.path.join(shared, "test_rw", "outputs")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        df_in = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        df_in.write_parquet(os.path.join(input_dir, "main.parquet"))

        code = """
import polars as pl
df = flowfile.read_input()
df = df.with_columns((pl.col("x") * pl.col("y")).alias("product"))
flowfile.publish_output(df)
"""

        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=4,
                    code=code,
                    input_paths={"main": "/shared/test_rw/inputs/main.parquet"},
                    output_dir="/shared/test_rw/outputs",
                ),
            )
        )
        assert result.success, f"Kernel execution failed: {result.error}"
        assert len(result.output_paths) > 0

        # Verify output
        out_path = os.path.join(output_dir, "main.parquet")
        assert os.path.exists(out_path), f"Expected output parquet at {out_path}"
        df_out = pl.read_parquet(out_path)
        assert "product" in df_out.columns
        assert df_out["product"].to_list() == [10, 40, 90]

    def test_multiple_inputs(self, kernel_manager: tuple[KernelManager, str]):
        """Kernel can read multiple named inputs."""
        manager, kernel_id = kernel_manager
        shared = manager.shared_volume_path

        input_dir = os.path.join(shared, "test_multi", "inputs")
        output_dir = os.path.join(shared, "test_multi", "outputs")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).write_parquet(
            os.path.join(input_dir, "left.parquet")
        )
        pl.DataFrame({"id": [1, 2], "score": [90, 80]}).write_parquet(
            os.path.join(input_dir, "right.parquet")
        )

        code = """
inputs = flowfile.read_inputs()
left = inputs["left"].collect()
right = inputs["right"].collect()
merged = left.join(right, on="id")
flowfile.publish_output(merged)
"""
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=5,
                    code=code,
                    input_paths={
                        "left": "/shared/test_multi/inputs/left.parquet",
                        "right": "/shared/test_multi/inputs/right.parquet",
                    },
                    output_dir="/shared/test_multi/outputs",
                ),
            )
        )
        assert result.success, f"Kernel execution failed: {result.error}"

        df_out = pl.read_parquet(os.path.join(output_dir, "main.parquet"))
        assert set(df_out.columns) == {"id", "name", "score"}
        assert len(df_out) == 2

    def test_stderr_captured(self, kernel_manager: tuple[KernelManager, str]):
        """Writes to stderr are captured."""
        manager, kernel_id = kernel_manager
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=6,
                    code='import sys; sys.stderr.write("warn\\n")',
                    input_paths={},
                    output_dir="/shared/test_stderr",
                ),
            )
        )
        assert result.success
        assert "warn" in result.stderr

    def test_execution_time_tracked(self, kernel_manager: tuple[KernelManager, str]):
        """execution_time_ms is populated."""
        manager, kernel_id = kernel_manager
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=7,
                    code="x = sum(range(100000))",
                    input_paths={},
                    output_dir="/shared/test_timing",
                ),
            )
        )
        assert result.success
        assert result.execution_time_ms > 0


# ---------------------------------------------------------------------------
# Tests — python_script node in FlowGraph
# ---------------------------------------------------------------------------


class TestPythonScriptNode:
    """
    Tests that wire up the python_script node type inside a FlowGraph and
    run the graph end-to-end against a real kernel container.
    """

    def test_python_script_passthrough(self, kernel_manager: tuple[KernelManager, str]):
        """
        python_script node reads input, passes it through, and writes output.
        """
        manager, kernel_id = kernel_manager
        # Patch the singleton so flow_graph picks up *this* manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            # Node 1: manual input
            data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2: python_script
            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
df = flowfile.read_input()
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=2,
                    depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code,
                        kernel_id=kernel_id,
                    ),
                )
            )

            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            result = graph.get_node(2).get_resulting_data()
            assert result is not None
            df = result.data_frame
            if hasattr(df, "collect"):
                df = df.collect()
            assert len(df) == 2
            assert set(df.columns) >= {"name", "age"}

        finally:
            _kernel_mod._manager = _prev

    def test_python_script_transform(self, kernel_manager: tuple[KernelManager, str]):
        """
        python_script node transforms data (adds a column).
        """
        manager, kernel_id = kernel_manager

        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}, {"val": 2}, {"val": 3}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
import polars as pl
df = flowfile.read_input().collect()
df = df.with_columns((pl.col("val") * 10).alias("val_x10"))
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=2,
                    depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code,
                        kernel_id=kernel_id,
                    ),
                )
            )

            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            result = graph.get_node(2).get_resulting_data()
            assert result is not None
            df = result.data_frame
            if hasattr(df, "collect"):
                df = df.collect()
            assert "val_x10" in df.columns
            assert df["val_x10"].to_list() == [10, 20, 30]

        finally:
            _kernel_mod._manager = _prev

    def test_python_script_no_kernel_raises(self):
        """
        If no kernel_id is set, the node should raise at execution time.
        """
        graph = _create_graph()

        data = [{"a": 1}]
        node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
        graph.add_node_promise(node_promise)
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=1,
                node_id=1,
                raw_data_format=input_schema.RawData.from_pylist(data),
            )
        )

        node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
        graph.add_node_promise(node_promise_2)

        graph.add_python_script(
            input_schema.NodePythonScript(
                flow_id=1,
                node_id=2,
                depending_on_id=1,
                python_script_input=input_schema.PythonScriptInput(
                    code='print("hi")',
                    kernel_id=None,  # intentionally no kernel
                ),
            )
        )

        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_info = graph.run_graph()
        # Should fail because no kernel is selected
        assert not run_info.success
