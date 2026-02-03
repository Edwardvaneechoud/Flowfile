"""
Kernel integration tests for the global artifacts feature.

These tests verify that publish_global, get_global, list_global_artifacts,
and delete_global_artifact work correctly when executed from within a kernel
container against the live Core API.

Requires:
- Docker available (for kernel container)
- flowfile_worker running (provides Core API endpoints)
"""

import asyncio
import os
from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult
from flowfile_core.schemas import input_schema, schemas

pytestmark = pytest.mark.kernel


# ---------------------------------------------------------------------------
# Helpers
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
    execution_mode: str = "Development",
    execution_location: str | None = "remote",
) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="global_artifacts_test_flow",
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
# Tests — Global Artifacts via direct kernel execution
# ---------------------------------------------------------------------------


class TestGlobalArtifactsKernelRuntime:
    """Tests that exercise global artifact functions directly via KernelManager."""

    def test_publish_global_basic(self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts):
        """publish_global stores an object to persistent storage."""
        manager, kernel_id = kernel_manager

        code = '''
artifact_id = flowfile.publish_global("kernel_test_model", {"accuracy": 0.95, "type": "classifier"})
print(f"Published artifact with ID: {artifact_id}")
'''
        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_publish_global",
                ),
            )
        )
        assert result.success, f"Execution failed: {result.error}"
        assert "Published artifact with ID:" in result.stdout

    def test_publish_and_get_global_roundtrip(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global then get_global retrieves the same data."""
        manager, kernel_id = kernel_manager

        # Publish an artifact
        publish_code = '''
data = {"model_type": "random_forest", "n_estimators": 100, "accuracy": 0.92}
artifact_id = flowfile.publish_global("rf_model_test", data)
print(f"artifact_id={artifact_id}")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=publish_code,
                    input_paths={},
                    output_dir="/shared/test_roundtrip_publish",
                ),
            )
        )
        assert result.success, f"Publish failed: {result.error}"

        # Retrieve it
        get_code = '''
retrieved = flowfile.get_global("rf_model_test")
assert retrieved["model_type"] == "random_forest", f"Got {retrieved}"
assert retrieved["n_estimators"] == 100
assert retrieved["accuracy"] == 0.92
print("Roundtrip successful!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=2,
                    code=get_code,
                    input_paths={},
                    output_dir="/shared/test_roundtrip_get",
                ),
            )
        )
        assert result.success, f"Get failed: {result.error}"
        assert "Roundtrip successful!" in result.stdout

    def test_publish_global_with_metadata(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global includes description and tags."""
        manager, kernel_id = kernel_manager

        code = '''
artifact_id = flowfile.publish_global(
    "tagged_model",
    {"weights": [1.0, 2.0, 3.0]},
    description="A test model with weights",
    tags=["ml", "test", "v1"],
)
print(f"Published with tags, id={artifact_id}")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_metadata",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"

    def test_list_global_artifacts(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """list_global_artifacts returns published artifacts."""
        manager, kernel_id = kernel_manager

        # Publish two artifacts
        setup_code = '''
flowfile.publish_global("list_test_a", {"value": 1})
flowfile.publish_global("list_test_b", {"value": 2})
print("Published two artifacts")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=setup_code,
                    input_paths={},
                    output_dir="/shared/test_list_setup",
                ),
            )
        )
        assert result.success, f"Setup failed: {result.error}"

        # List artifacts
        list_code = '''
artifacts = flowfile.list_global_artifacts()
names = [a["name"] for a in artifacts]
assert "list_test_a" in names, f"list_test_a not found in {names}"
assert "list_test_b" in names, f"list_test_b not found in {names}"
print(f"Found {len(artifacts)} artifacts")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=2,
                    code=list_code,
                    input_paths={},
                    output_dir="/shared/test_list",
                ),
            )
        )
        assert result.success, f"List failed: {result.error}"

    def test_delete_global_artifact(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """delete_global_artifact removes an artifact."""
        manager, kernel_id = kernel_manager

        # Publish then delete
        code = '''
# Publish
flowfile.publish_global("to_delete", {"temp": True})

# Verify it exists
obj = flowfile.get_global("to_delete")
assert obj["temp"] is True

# Delete
flowfile.delete_global_artifact("to_delete")

# Verify it's gone
try:
    flowfile.get_global("to_delete")
    assert False, "Should have raised KeyError"
except KeyError:
    print("Correctly deleted artifact")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_delete",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Correctly deleted artifact" in result.stdout

    def test_get_nonexistent_raises_key_error(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """get_global raises KeyError for nonexistent artifact."""
        manager, kernel_id = kernel_manager

        code = '''
try:
    flowfile.get_global("definitely_does_not_exist_12345")
    print("ERROR: Should have raised KeyError")
except KeyError as e:
    print(f"Correctly raised KeyError: {e}")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_keyerror",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Correctly raised KeyError" in result.stdout

    def test_versioning_on_republish(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """Publishing to same name creates a new version."""
        manager, kernel_id = kernel_manager

        code = '''
# Publish v1
id1 = flowfile.publish_global("versioned_model", {"version": 1})

# Publish v2 (same name)
id2 = flowfile.publish_global("versioned_model", {"version": 2})

# Should be different artifact IDs (different versions)
assert id2 != id1, f"Expected different IDs, got {id1} and {id2}"

# Get latest (should be v2)
latest = flowfile.get_global("versioned_model")
assert latest["version"] == 2, f"Expected version 2, got {latest}"

# Get specific version
v1 = flowfile.get_global("versioned_model", version=1)
assert v1["version"] == 1, f"Expected version 1, got {v1}"

print("Versioning works correctly!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_versioning",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Versioning works correctly!" in result.stdout


# ---------------------------------------------------------------------------
# Tests — Global Artifacts via FlowGraph python_script nodes
# ---------------------------------------------------------------------------


class TestGlobalArtifactsFlowGraph:
    """Tests that wire up global artifact calls inside FlowGraph python_script nodes."""

    def test_publish_global_in_flow(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """python_script node can publish a global artifact."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            graph = _create_graph()

            # Node 1: input data
            data = [{"x": 1, "y": 2}]
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            )
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data),
                )
            )

            # Node 2: publish global artifact
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            )
            code = '''
df = flowfile.read_input()
# Publish a global artifact (persists beyond flow run)
flowfile.publish_global("flow_published_model", {"trained_on": "flow_data"})
flowfile.publish_output(df)
'''
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=2,
                    depending_on_ids=[1],
                    python_script_input=input_schema.PythonScriptInput(
                        code=code,
                        kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(
                graph, input_schema.NodeConnection.create_from_simple_input(1, 2)
            )

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            # Verify the global artifact was published by retrieving it
            verify_code = '''
model = flowfile.get_global("flow_published_model")
assert model["trained_on"] == "flow_data"
print("Flow-published global artifact verified!")
'''
            result = _run(
                manager.execute(
                    kernel_id,
                    ExecuteRequest(
                        node_id=100,
                        code=verify_code,
                        input_paths={},
                        output_dir="/shared/verify_flow_publish",
                    ),
                )
            )
            assert result.success, f"Verification failed: {result.error}"

        finally:
            _kernel_mod._manager = _prev

    def test_use_global_artifact_across_flows(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """Global artifacts persist across separate flow runs."""
        manager, kernel_id = kernel_manager
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        try:
            # Flow 1: Publish a global artifact
            graph1 = _create_graph(flow_id=1)

            data1 = [{"val": 100}]
            graph1.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            )
            graph1.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data1),
                )
            )

            graph1.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            )
            publish_code = '''
df = flowfile.read_input()
# Publish global artifact in Flow 1
flowfile.publish_global("cross_flow_artifact", {"source": "flow_1", "value": 42})
flowfile.publish_output(df)
'''
            graph1.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=2,
                    depending_on_ids=[1],
                    python_script_input=input_schema.PythonScriptInput(
                        code=publish_code,
                        kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(
                graph1, input_schema.NodeConnection.create_from_simple_input(1, 2)
            )

            run_info1 = graph1.run_graph()
            _handle_run_info(run_info1)

            # Flow 2: Use the global artifact from Flow 1
            graph2 = _create_graph(flow_id=2)

            data2 = [{"other": "data"}]
            graph2.add_node_promise(
                input_schema.NodePromise(flow_id=2, node_id=1, node_type="manual_input")
            )
            graph2.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=2,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist(data2),
                )
            )

            graph2.add_node_promise(
                input_schema.NodePromise(flow_id=2, node_id=2, node_type="python_script")
            )
            consume_code = '''
import polars as pl

df = flowfile.read_input().collect()
# Read global artifact from Flow 1
artifact = flowfile.get_global("cross_flow_artifact")
assert artifact["source"] == "flow_1", f"Expected flow_1, got {artifact}"
assert artifact["value"] == 42

# Add artifact value to output
result = df.with_columns(pl.lit(artifact["value"]).alias("from_global"))
flowfile.publish_output(result)
'''
            graph2.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=2,
                    node_id=2,
                    depending_on_ids=[1],
                    python_script_input=input_schema.PythonScriptInput(
                        code=consume_code,
                        kernel_id=kernel_id,
                    ),
                )
            )
            add_connection(
                graph2, input_schema.NodeConnection.create_from_simple_input(1, 2)
            )

            run_info2 = graph2.run_graph()
            _handle_run_info(run_info2)

            # Verify the result includes the global artifact value
            result = graph2.get_node(2).get_resulting_data()
            df = result.data_frame
            if hasattr(df, "collect"):
                df = df.collect()
            assert "from_global" in df.columns
            assert df["from_global"][0] == 42

        finally:
            _kernel_mod._manager = _prev


# ---------------------------------------------------------------------------
# Tests — Complex Object Types
# ---------------------------------------------------------------------------


class TestGlobalArtifactsComplexTypes:
    """Tests for publishing various Python object types as global artifacts."""

    def test_publish_numpy_array(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global handles numpy arrays via joblib serialization."""
        manager, kernel_id = kernel_manager

        code = '''
import numpy as np

# Publish a numpy array
arr = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
artifact_id = flowfile.publish_global("numpy_matrix", arr)
print(f"Published numpy array, id={artifact_id}")

# Retrieve and verify
retrieved = flowfile.get_global("numpy_matrix")
assert np.array_equal(retrieved, arr), f"Arrays don't match: {retrieved}"
print("Numpy array roundtrip successful!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_numpy",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Numpy array roundtrip successful!" in result.stdout

    def test_publish_polars_dataframe(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global handles Polars DataFrames via parquet serialization."""
        manager, kernel_id = kernel_manager

        code = '''
import polars as pl

# Publish a Polars DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85.5, 92.0, 78.3],
})
artifact_id = flowfile.publish_global("polars_df", df)
print(f"Published Polars DataFrame, id={artifact_id}")

# Retrieve and verify
retrieved = flowfile.get_global("polars_df")
assert retrieved.equals(df), f"DataFrames don't match"
assert list(retrieved.columns) == ["id", "name", "score"]
print("Polars DataFrame roundtrip successful!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_polars_df",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Polars DataFrame roundtrip successful!" in result.stdout

    def test_publish_nested_dict(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global handles complex nested dictionaries."""
        manager, kernel_id = kernel_manager

        code = '''
# Publish a complex nested structure
config = {
    "model": {
        "type": "neural_network",
        "layers": [64, 128, 64],
        "activation": "relu",
    },
    "training": {
        "epochs": 100,
        "batch_size": 32,
        "optimizer": {"name": "adam", "lr": 0.001},
    },
    "data": {
        "features": ["x1", "x2", "x3"],
        "target": "y",
    },
}
artifact_id = flowfile.publish_global("model_config", config)
print(f"Published nested config, id={artifact_id}")

# Retrieve and verify
retrieved = flowfile.get_global("model_config")
assert retrieved["model"]["layers"] == [64, 128, 64]
assert retrieved["training"]["optimizer"]["lr"] == 0.001
print("Nested dict roundtrip successful!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_nested",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Nested dict roundtrip successful!" in result.stdout

    def test_publish_custom_class(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """publish_global handles custom class instances via pickle."""
        manager, kernel_id = kernel_manager

        code = '''
class ModelWrapper:
    def __init__(self, name, weights):
        self.name = name
        self.weights = weights

    def predict(self, x):
        return sum(w * xi for w, xi in zip(self.weights, x))

# Publish custom object
model = ModelWrapper("linear", [1.0, 2.0, 3.0])
artifact_id = flowfile.publish_global("custom_model", model)
print(f"Published custom object, id={artifact_id}")

# Retrieve and verify
retrieved = flowfile.get_global("custom_model")
assert retrieved.name == "linear"
assert retrieved.weights == [1.0, 2.0, 3.0]
assert retrieved.predict([1, 1, 1]) == 6.0
print("Custom class roundtrip successful!")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_custom_class",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Custom class roundtrip successful!" in result.stdout


# ---------------------------------------------------------------------------
# Tests — Error Handling
# ---------------------------------------------------------------------------


class TestGlobalArtifactsErrorHandling:
    """Tests for error handling in global artifact operations."""

    def test_delete_nonexistent_raises_key_error(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """delete_global_artifact raises KeyError for nonexistent artifact."""
        manager, kernel_id = kernel_manager

        code = '''
try:
    flowfile.delete_global_artifact("nonexistent_artifact_xyz")
    print("ERROR: Should have raised KeyError")
except KeyError as e:
    print(f"Correctly raised KeyError: {e}")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_delete_error",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Correctly raised KeyError" in result.stdout

    def test_get_specific_version_not_found(
        self, kernel_manager: tuple[KernelManager, str], cleanup_global_artifacts
    ):
        """get_global raises KeyError when specific version doesn't exist."""
        manager, kernel_id = kernel_manager

        code = '''
# Publish version 1
flowfile.publish_global("versioned_test", {"v": 1})

# Try to get version 999 (doesn't exist)
try:
    flowfile.get_global("versioned_test", version=999)
    print("ERROR: Should have raised KeyError")
except KeyError as e:
    print(f"Correctly raised KeyError for missing version: {e}")
'''
        result = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_version_error",
                ),
            )
        )
        assert result.success, f"Failed: {result.error}"
        assert "Correctly raised KeyError" in result.stdout
