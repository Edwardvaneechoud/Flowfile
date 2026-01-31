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

pytestmark = pytest.mark.kernel


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
                    input_paths={"main": ["/shared/test_rw/inputs/main.parquet"]},
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
                        "left": ["/shared/test_multi/inputs/left.parquet"],
                        "right": ["/shared/test_multi/inputs/right.parquet"],
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


# ---------------------------------------------------------------------------
# Tests — ArtifactContext integration (requires real kernel container)
# ---------------------------------------------------------------------------


class TestArtifactContextIntegration:
    """Integration tests verifying ArtifactContext works with real kernel execution."""

    def test_published_artifacts_recorded_in_context(self, kernel_manager: tuple[KernelManager, str]):
        """After execution, published artifacts appear in artifact_context."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
df = flowfile.read_input()
flowfile.publish_artifact("my_model", {"accuracy": 0.95})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            published = graph.artifact_context.get_published_by_node(2)
            assert len(published) >= 1
            names = [r.name for r in published]
            assert "my_model" in names
        finally:
            _kernel_mod._manager = _prev

    def test_available_artifacts_computed_before_execution(self, kernel_manager: tuple[KernelManager, str]):
        """Downstream nodes have correct available artifacts."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2: publishes artifact
            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)
            code_publish = """
df = flowfile.read_input()
flowfile.publish_artifact("trained_model", {"type": "RF"})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_publish, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            # Node 3: reads artifact (downstream of node 2)
            node_promise_3 = input_schema.NodePromise(flow_id=1, node_id=3, node_type="python_script")
            graph.add_node_promise(node_promise_3)
            code_consume = """
df = flowfile.read_input()
model = flowfile.read_artifact("trained_model")
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=3, depending_on_id=2,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_consume, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            # Node 3 should have "trained_model" available
            available = graph.artifact_context.get_available_for_node(3)
            assert "trained_model" in available

        finally:
            _kernel_mod._manager = _prev

    def test_artifacts_cleared_between_runs(self, kernel_manager: tuple[KernelManager, str]):
        """Running flow twice doesn't leak artifacts from first run."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
df = flowfile.read_input()
flowfile.publish_artifact("run_artifact", [1, 2, 3])
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            # First run
            run_info = graph.run_graph()
            _handle_run_info(run_info)
            assert len(graph.artifact_context.get_published_by_node(2)) >= 1

            # Second run — context should be cleared at start then repopulated
            run_info2 = graph.run_graph()
            _handle_run_info(run_info2)

            # Should still have the artifact from this run, but no leftover state
            published = graph.artifact_context.get_published_by_node(2)
            names = [r.name for r in published]
            assert "run_artifact" in names
            # Verify it's exactly one entry (not duplicated from first run)
            assert names.count("run_artifact") == 1

        finally:
            _kernel_mod._manager = _prev

    def test_multiple_artifacts_from_single_node(self, kernel_manager: tuple[KernelManager, str]):
        """Node publishing multiple artifacts records all of them."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
df = flowfile.read_input()
flowfile.publish_artifact("model", {"type": "classifier"})
flowfile.publish_artifact("encoder", {"type": "label_encoder"})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            published = graph.artifact_context.get_published_by_node(2)
            names = {r.name for r in published}
            assert "model" in names
            assert "encoder" in names

        finally:
            _kernel_mod._manager = _prev

    def test_artifact_context_to_dict_after_run(self, kernel_manager: tuple[KernelManager, str]):
        """to_dict() returns valid structure after flow execution."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)

            code = """
df = flowfile.read_input()
flowfile.publish_artifact("ctx_model", {"version": 1})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            d = graph.artifact_context.to_dict()
            assert "nodes" in d
            assert "kernels" in d
            # Should have at least node 2 in nodes
            assert "2" in d["nodes"]
            # Kernel should be tracked
            assert kernel_id in d["kernels"]

        finally:
            _kernel_mod._manager = _prev

    def test_train_model_and_apply(self, kernel_manager: tuple[KernelManager, str]):
        """Train a numpy linear-regression model in node 2, apply it in node 3."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            # Node 1: input data with features and target
            data = [
                {"x1": 1.0, "x2": 2.0, "y": 5.0},
                {"x1": 2.0, "x2": 3.0, "y": 8.0},
                {"x1": 3.0, "x2": 4.0, "y": 11.0},
                {"x1": 4.0, "x2": 5.0, "y": 14.0},
            ]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2: train model (least-squares fit) and publish as artifact
            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)
            train_code = """
import numpy as np
import polars as pl

df = flowfile.read_input().collect()
X = np.column_stack([df["x1"].to_numpy(), df["x2"].to_numpy(), np.ones(len(df))])
y_vals = df["y"].to_numpy()
coeffs = np.linalg.lstsq(X, y_vals, rcond=None)[0]
flowfile.publish_artifact("linear_model", {"coefficients": coeffs.tolist()})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=train_code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            # Node 3: load model and apply predictions
            node_promise_3 = input_schema.NodePromise(flow_id=1, node_id=3, node_type="python_script")
            graph.add_node_promise(node_promise_3)
            apply_code = """
import numpy as np
import polars as pl

df = flowfile.read_input().collect()
model = flowfile.read_artifact("linear_model")
coeffs = np.array(model["coefficients"])
X = np.column_stack([df["x1"].to_numpy(), df["x2"].to_numpy(), np.ones(len(df))])
predictions = X @ coeffs
result = df.with_columns(pl.Series("predicted_y", predictions))
flowfile.publish_output(result)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=3, depending_on_id=2,
                    python_script_input=input_schema.PythonScriptInput(
                        code=apply_code, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            # Verify model was published and tracked
            published = graph.artifact_context.get_published_by_node(2)
            assert any(r.name == "linear_model" for r in published)

            # Verify node 3 had the model available
            available = graph.artifact_context.get_available_for_node(3)
            assert "linear_model" in available

            # Verify predictions were produced
            node_3 = graph.get_node(3)
            result_df = node_3.get_resulting_data().data_frame.collect()
            assert "predicted_y" in result_df.columns
            # The predictions should be close to the actual y values
            preds = result_df["predicted_y"].to_list()
            actuals = result_df["y"].to_list()
            for pred, actual in zip(preds, actuals):
                assert abs(pred - actual) < 0.01, f"Prediction {pred} too far from {actual}"

        finally:
            _kernel_mod._manager = _prev

    def test_publish_delete_republish_access(self, kernel_manager: tuple[KernelManager, str]):
        """
        Flow: node_a publishes model -> node_b uses & deletes model ->
              node_c publishes new model -> node_d accesses new model.
        """
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            # Node 1: input data
            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2 (node_a): publish artifact_model v1
            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)
            code_a = """
df = flowfile.read_input()
flowfile.publish_artifact("artifact_model", {"version": 1, "weights": [0.5]})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_a, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            # Node 3 (node_b): read artifact_model, use it, then delete it
            node_promise_3 = input_schema.NodePromise(flow_id=1, node_id=3, node_type="python_script")
            graph.add_node_promise(node_promise_3)
            code_b = """
df = flowfile.read_input()
model = flowfile.read_artifact("artifact_model")
assert model["version"] == 1, f"Expected v1, got {model}"
flowfile.delete_artifact("artifact_model")
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=3, depending_on_id=2,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_b, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

            # Node 4 (node_c): publish new artifact_model v2
            node_promise_4 = input_schema.NodePromise(flow_id=1, node_id=4, node_type="python_script")
            graph.add_node_promise(node_promise_4)
            code_c = """
df = flowfile.read_input()
flowfile.publish_artifact("artifact_model", {"version": 2, "weights": [0.9]})
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=4, depending_on_id=3,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_c, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 4))

            # Node 5 (node_d): read artifact_model — should get v2
            node_promise_5 = input_schema.NodePromise(flow_id=1, node_id=5, node_type="python_script")
            graph.add_node_promise(node_promise_5)
            code_d = """
df = flowfile.read_input()
model = flowfile.read_artifact("artifact_model")
assert model["version"] == 2, f"Expected v2, got {model}"
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=5, depending_on_id=4,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_d, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(4, 5))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            # Verify artifact context tracks the flow correctly
            # Node 4 re-published artifact_model
            published_4 = graph.artifact_context.get_published_by_node(4)
            assert any(r.name == "artifact_model" for r in published_4)

            # Node 5 should see artifact_model as available (from node 4)
            available_5 = graph.artifact_context.get_available_for_node(5)
            assert "artifact_model" in available_5
            assert available_5["artifact_model"].source_node_id == 4

        finally:
            _kernel_mod._manager = _prev

    def test_duplicate_publish_fails(self, kernel_manager: tuple[KernelManager, str]):
        """Publishing an artifact with the same name without deleting first should fail."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            data = [{"val": 1}]
            node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            graph.add_node_promise(node_promise)
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1, node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2: publishes artifact
            node_promise_2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            graph.add_node_promise(node_promise_2)
            code_publish = """
df = flowfile.read_input()
flowfile.publish_artifact("model", "v1")
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=2, depending_on_id=1,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_publish, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            # Node 3: tries to publish same name without deleting — should fail
            node_promise_3 = input_schema.NodePromise(flow_id=1, node_id=3, node_type="python_script")
            graph.add_node_promise(node_promise_3)
            code_dup = """
df = flowfile.read_input()
flowfile.publish_artifact("model", "v2")
flowfile.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1, node_id=3, depending_on_id=2,
                    python_script_input=input_schema.PythonScriptInput(
                        code=code_dup, kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

            run_info = graph.run_graph()

            # Node 3 should have failed
            node_3_result = next(
                r for r in run_info.node_step_result if r.node_id == 3
            )
            assert node_3_result.success is False
            assert "already exists" in node_3_result.error

        finally:
            _kernel_mod._manager = _prev
