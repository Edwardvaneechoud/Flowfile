"""
Integration tests for python_script node code generation.

These tests verify that the FlowGraphToPolarsConverter correctly handles
python_script nodes by building FlowGraphs with python_script nodes
and checking the generated code.
"""

import polars as pl
import pytest

from flowfile_core.flowfile.code_generator.code_generator import (
    FlowGraphToPolarsConverter,
    UnsupportedNodeError,
    export_flow_to_polars,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    """Create basic flow settings for tests."""
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow",
    )


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    """Create a basic flow graph for testing."""
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def verify_code_executes(code: str):
    """Execute generated code and verify no exceptions are raised."""
    exec_globals = {}
    try:
        exec(code, exec_globals)
        _ = exec_globals["run_etl_pipeline"]()
    except Exception as e:
        raise AssertionError(f"Code execution failed:\n{e}\n\nGenerated code:\n{code}")


def get_result_from_generated_code(code: str):
    """Execute generated code and return the result."""
    exec_globals = {}
    exec(code, exec_globals)
    return exec_globals["run_etl_pipeline"]()


def add_manual_input_node(flow: FlowGraph, node_id: int = 1) -> None:
    """Add a manual input node with sample data."""
    raw_data = input_schema.RawData(
        columns=[
            input_schema.MinimalFieldInfo(name="id", data_type="Int64"),
            input_schema.MinimalFieldInfo(name="name", data_type="String"),
            input_schema.MinimalFieldInfo(name="value", data_type="Float64"),
        ],
        data=[[1, 2, 3], ["Alice", "Bob", "Charlie"], [10.0, 20.0, 30.0]],
    )
    settings = input_schema.NodeManualInput(
        flow_id=1,
        node_id=node_id,
        raw_data_format=raw_data,
    )
    flow.add_manual_input(settings)


def add_python_script_node(
    flow: FlowGraph,
    node_id: int,
    code: str,
    depending_on_ids: list[int] | None = None,
    kernel_id: str | None = None,
) -> None:
    """Add a python_script node to the flow."""
    settings = input_schema.NodePythonScript(
        flow_id=1,
        node_id=node_id,
        depending_on_ids=depending_on_ids or [],
        python_script_input=input_schema.PythonScriptInput(
            code=code,
            kernel_id=kernel_id,
        ),
    )
    flow.add_python_script(settings)


def connect_nodes(flow: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    """Connect two nodes."""
    connection = input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type)
    add_connection(flow, connection)


# ---------------------------------------------------------------------------
# Basic python_script code generation tests
# ---------------------------------------------------------------------------


class TestSimplePythonScriptGeneration:
    """Test basic python_script node code generation."""

    def test_simple_passthrough(self):
        """Python script that reads input and publishes it unchanged."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "df = flowfile.read_input()\n"
            "flowfile.publish_output(df)\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "flowfile" not in generated
        assert "def _node_2" in generated
        assert "input_df" in generated
        assert "_node_2(" in generated

        verify_code_executes(generated)

    def test_transform_with_collect(self):
        """Python script that collects, transforms, and publishes output."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "import polars as pl\n"
            "df = flowfile.read_input().collect()\n"
            "result = df.with_columns(pl.col('value') * 2)\n"
            "flowfile.publish_output(result)\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "flowfile" not in generated
        assert "input_df.collect()" in generated
        assert "def _node_2" in generated

        verify_code_executes(generated)

    def test_empty_code_passthrough(self):
        """Empty python_script code should pass through."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        add_python_script_node(flow, node_id=2, code="", depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)
        verify_code_executes(generated)

    def test_no_output_passthrough(self):
        """Script without publish_output should pass through input."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = "df = flowfile.read_input().collect()\nx = len(df)\n"
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "return input_df" in generated
        verify_code_executes(generated)

    def test_passthrough_output_pattern(self):
        """publish_output(read_input()) should generate return input_df."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "df = flowfile.read_input().collect()\n"
            "x = len(df)\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "return input_df" in generated
        verify_code_executes(generated)


# ---------------------------------------------------------------------------
# Artifact tests
# ---------------------------------------------------------------------------


class TestArtifactCodeGeneration:
    """Test artifact publish/consume code generation."""

    def test_artifact_publish(self):
        """publish_artifact becomes _artifacts assignment."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "df = flowfile.read_input().collect()\n"
            "model = {'trained': True}\n"
            'flowfile.publish_artifact("my_model", model)\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "_artifacts" in generated
        assert "my_model" in generated
        assert "flowfile" not in generated
        # Should have _artifacts = {} at top level
        assert "_artifacts = {}" in generated

        verify_code_executes(generated)

    def test_artifact_chain(self):
        """Artifacts flow correctly between two python_script nodes."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        # Producer node
        producer_code = (
            "info = {'count': 42}\n"
            'flowfile.publish_artifact("info", info)\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=producer_code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        # Consumer node
        consumer_code = (
            'info = flowfile.read_artifact("info")\n'
            "df = flowfile.read_input().collect()\n"
            "flowfile.publish_output(df)\n"
        )
        add_python_script_node(flow, node_id=3, code=consumer_code, depending_on_ids=[2])
        connect_nodes(flow, 2, 3)

        generated = export_flow_to_polars(flow)

        assert "_artifacts" in generated
        assert "info" in generated
        assert "flowfile" not in generated

        verify_code_executes(generated)

    def test_artifact_delete(self):
        """delete_artifact becomes del _artifacts[...]."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "obj = {'x': 1}\n"
            'flowfile.publish_artifact("temp", obj)\n'
            'flowfile.delete_artifact("temp")\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "del _artifacts" in generated
        assert "temp" in generated
        verify_code_executes(generated)

    def test_unconsumed_artifact_error(self):
        """Consuming an artifact not published upstream should fail."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            'model = flowfile.read_artifact("missing_model")\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        with pytest.raises(UnsupportedNodeError, match="missing_model"):
            export_flow_to_polars(flow)


# ---------------------------------------------------------------------------
# Logging tests
# ---------------------------------------------------------------------------


class TestLoggingCodeGeneration:
    """Test that flowfile.log becomes print."""

    def test_log_becomes_print(self):
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            'flowfile.log("processing data")\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "print" in generated
        assert "flowfile" not in generated
        verify_code_executes(generated)


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Test error cases in python_script code generation."""

    def test_dynamic_artifact_name(self):
        """Dynamic artifact names should produce UnsupportedNodeError."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "name = 'model'\n"
            "flowfile.read_artifact(name)\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        with pytest.raises(UnsupportedNodeError, match="string literals"):
            export_flow_to_polars(flow)

    def test_syntax_error_in_code(self):
        """Syntax errors should produce UnsupportedNodeError."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = "def foo(:\n"
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        with pytest.raises(UnsupportedNodeError, match="Syntax error"):
            export_flow_to_polars(flow)

    def test_unsupported_display_call(self):
        """flowfile.display should produce UnsupportedNodeError."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "flowfile.display('hello')\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        with pytest.raises(UnsupportedNodeError, match="Unsupported flowfile API"):
            export_flow_to_polars(flow)


# ---------------------------------------------------------------------------
# Import handling tests
# ---------------------------------------------------------------------------


class TestImportHandling:
    """Test that imports from python_script nodes are handled correctly."""

    def test_user_imports_added(self):
        """User imports should appear in generated code."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "import json\n"
            "df = flowfile.read_input().collect()\n"
            "data = json.dumps({'count': len(df)})\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "import json" in generated
        assert "flowfile" not in generated

    def test_flowfile_import_excluded(self):
        """import flowfile should not appear in generated code."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "import flowfile\n"
            "import json\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        # "import flowfile" should not be present; "import json" should be
        lines = generated.split("\n")
        assert not any(line.strip() == "import flowfile" for line in lines)
        assert "import json" in generated


# ---------------------------------------------------------------------------
# Mixed node type tests
# ---------------------------------------------------------------------------


class TestMixedNodeTypes:
    """Test flows mixing python_script with other node types."""

    def test_manual_input_then_python_script(self):
        """Manual input → python_script → output."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "import polars as pl\n"
            "df = flowfile.read_input().collect()\n"
            "result = df.with_columns(pl.lit('new').alias('new_col'))\n"
            "flowfile.publish_output(result)\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)
        verify_code_executes(generated)

        result = get_result_from_generated_code(generated)
        if hasattr(result, "collect"):
            result = result.collect()
        assert "new_col" in result.columns

    def test_python_script_then_filter(self):
        """python_script → filter node."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        # Add filter node
        filter_settings = input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="value",
                    operator=transform_schema.FilterOperator.GREATER_THAN,
                    value="15",
                ),
            ),
        )
        flow.add_filter(filter_settings)
        connect_nodes(flow, 2, 3)

        generated = export_flow_to_polars(flow)
        verify_code_executes(generated)

    def test_multiple_python_script_nodes(self):
        """Chain of multiple python_script nodes."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code1 = (
            "import polars as pl\n"
            "df = flowfile.read_input().collect()\n"
            "result = df.with_columns(pl.col('value').alias('doubled'))\n"
            "flowfile.publish_output(result)\n"
        )
        add_python_script_node(flow, node_id=2, code=code1, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        code2 = (
            "df = flowfile.read_input().collect()\n"
            "flowfile.publish_output(df)\n"
        )
        add_python_script_node(flow, node_id=3, code=code2, depending_on_ids=[2])
        connect_nodes(flow, 2, 3)

        generated = export_flow_to_polars(flow)

        assert "def _node_2" in generated
        assert "def _node_3" in generated
        verify_code_executes(generated)


# ---------------------------------------------------------------------------
# Artifacts store initialization test
# ---------------------------------------------------------------------------


class TestArtifactStoreInitialization:
    """Test that _artifacts = {} is emitted properly."""

    def test_artifacts_dict_emitted_for_python_script(self):
        """_artifacts = {} should appear when python_script nodes exist."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = "flowfile.publish_output(flowfile.read_input())\n"
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)
        assert "_artifacts = {}" in generated

    def test_no_artifacts_dict_without_python_script(self):
        """_artifacts should NOT appear when no python_script nodes exist."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        # Add a filter node (not python_script)
        filter_settings = input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="value",
                    operator=transform_schema.FilterOperator.GREATER_THAN,
                    value="15",
                ),
            ),
        )
        flow.add_filter(filter_settings)
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)
        assert "_artifacts" not in generated


# ---------------------------------------------------------------------------
# Full pipeline test matching the spec's appendix example
# ---------------------------------------------------------------------------


class TestFullPipelineExample:
    """Test the complete example from the specification."""

    def test_train_predict_pipeline(self):
        """Simulate train → predict pipeline with artifacts."""
        flow = create_basic_flow()

        # Node 1: Manual input with training data
        raw_data = input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="f1", data_type="Float64"),
                input_schema.MinimalFieldInfo(name="f2", data_type="Float64"),
                input_schema.MinimalFieldInfo(name="target", data_type="Int64"),
            ],
            data=[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0], [0, 1, 0, 1]],
        )
        settings = input_schema.NodeManualInput(
            flow_id=1, node_id=1, raw_data_format=raw_data
        )
        flow.add_manual_input(settings)

        # Node 2: Train model
        train_code = (
            "import polars as pl\n"
            "df = flowfile.read_input().collect()\n"
            "model = {'trained': True, 'n_features': 2}\n"
            'flowfile.publish_artifact("model", model)\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=train_code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        # Node 3: Use model (consume artifact)
        predict_code = (
            "import polars as pl\n"
            'model = flowfile.read_artifact("model")\n'
            "df = flowfile.read_input().collect()\n"
            "result = df.with_columns(pl.lit(model['n_features']).alias('n_features'))\n"
            "flowfile.publish_output(result)\n"
        )
        add_python_script_node(flow, node_id=3, code=predict_code, depending_on_ids=[2])
        connect_nodes(flow, 2, 3)

        generated = export_flow_to_polars(flow)

        # Verify structure
        assert "_artifacts = {}" in generated
        assert "def _node_2" in generated
        assert "def _node_3" in generated
        assert "_artifacts" in generated
        assert "model" in generated
        assert "flowfile" not in generated

        # Verify execution
        verify_code_executes(generated)

        result = get_result_from_generated_code(generated)
        if hasattr(result, "collect"):
            result = result.collect()
        assert "n_features" in result.columns

    def test_list_artifacts_usage(self):
        """Test that list_artifacts becomes _artifacts reference."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = (
            "obj = {'x': 1}\n"
            'flowfile.publish_artifact("item", obj)\n'
            "arts = flowfile.list_artifacts()\n"
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "flowfile" not in generated
        verify_code_executes(generated)

    def test_node_comment_header(self):
        """Generated code should include node header comments."""
        flow = create_basic_flow()
        add_manual_input_node(flow, node_id=1)

        code = "flowfile.publish_output(flowfile.read_input())\n"
        add_python_script_node(flow, node_id=2, code=code, depending_on_ids=[1])
        connect_nodes(flow, 1, 2)

        generated = export_flow_to_polars(flow)

        assert "# --- Node 2: python_script ---" in generated
