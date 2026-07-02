"""Integration tests: flow parameters are substituted correctly at execution time.

These tests verify the full pipeline:
  define parameter → reference ${name} in node settings → run_graph() → correct output
"""

import os
import tempfile

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.schemas import FlowParameter


# Helpers


def make_graph(flow_id: int = 99, execution_location: str = "local") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="param_test",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def run_and_assert_ok(graph: FlowGraph) -> None:
    result = graph.run_graph()
    assert result is not None, "run_graph returned None"
    if not result.success:
        errors = "\n".join(
            f"  node {r.node_id}: {r.error}"
            for r in result.node_step_result
            if not r.success
        )
        raise AssertionError(f"Flow failed:\n{errors}")


# polars_code node


def test_polars_code_parameter_substituted(execution_location):
    """${col_name} inside polars code is replaced with the parameter value."""
    graph = make_graph(101, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="col_name", default_value="city")]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"name": "Alice", "city": "Amsterdam"}]
            ),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${col_name}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result_df = graph.get_node(2).get_resulting_data()
    assert result_df is not None
    assert list(result_df.columns) == ["city"]


def test_polars_code_original_code_preserved_after_run(execution_location):
    """After run_graph, the ${...} reference must still be in setting_input (not resolved)."""
    graph = make_graph(102, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="col", default_value="name")]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Bob"}]),
        )
    )

    original_code = "output_df = input_df.select(pl.col('${col}'))"
    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=original_code),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    stored_code = graph.get_node(2).setting_input.polars_code_input.polars_code
    assert stored_code == original_code, (
        f"Original ${'{col}'} reference should be preserved after run, got: {stored_code!r}"
    )


def test_polars_code_filter_with_numeric_parameter(execution_location):
    """A numeric parameter replaces ${threshold} so a filter expression evaluates correctly."""
    graph = make_graph(103, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="threshold", default_value="30")]

    rows = [{"age": 25}, {"age": 35}, {"age": 40}]
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(rows),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('age') > ${threshold})"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    ages = result.to_dict()["age"]
    assert all(a > 30 for a in ages), f"Expected only ages > 30, got {ages}"
    assert len(ages) == 2


def test_polars_code_multiple_parameters(execution_location):
    """Two parameters in the same code string are both resolved."""
    graph = make_graph(104, execution_location=execution_location)
    graph.flow_settings.parameters = [
        FlowParameter(name="src_col", default_value="name"),
        FlowParameter(name="new_col", default_value="upper_name"),
    ]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "alice"}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=(
                    "output_df = input_df.with_columns("
                    "pl.col('${src_col}').str.to_uppercase().alias('${new_col}'))"
                )
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    assert "upper_name" in result.columns
    assert result.to_dict()["upper_name"][0] == "ALICE"


def test_no_parameters_flow_unchanged(execution_location):
    """A flow with no parameters executes exactly as before (no regression)."""
    graph = make_graph(105, execution_location=execution_location)
    assert graph.flow_settings.parameters == []

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"x": 1}, {"x": 2}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.with_columns(pl.col('x') * 2)"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    run_and_assert_ok(graph)

    result = graph.get_node(2).get_resulting_data()
    assert result.to_dict()["x"] == [2, 4]


def test_unresolved_parameter_fails_node(execution_location):
    """A ${missing} reference that has no matching parameter causes the node to fail."""
    graph = make_graph(106, execution_location=execution_location)
    graph.flow_settings.parameters = []

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"x": 1}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${undefined_param}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = graph.run_graph()
    node_result = next(r for r in result.node_step_result if r.node_id == 2)
    assert not node_result.success, "Node with unresolved parameter should fail"
    assert "undefined_param" in (node_result.error or "")


# Issue 1: node retains example_data_generator / has_completed_last_run after run


def test_node_retains_example_data_after_run_with_parameter(execution_location):
    """After run_graph with a parameter, example_data_generator and
    has_completed_last_run must still be set (not cleared by a spurious reset
    caused by the _hash mismatch introduced by parameter substitution)."""
    graph = make_graph(201, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="col_name", default_value="city")]
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Alice", "city": "Amsterdam"}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${col_name}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    node = graph.get_node(2)
    assert node.schema[0].name == "city"
    run_and_assert_ok(graph)

    assert node.node_stats.has_completed_last_run, (
        "has_completed_last_run must be True after a successful run with parameters"
    )
    assert node.results.example_data_generator is not None, (
        "example_data_generator must be preserved after run_graph with parameters"
    )


# Issue 2: lazy schema prediction with parameters


def test_predicted_schema_uses_parameters():
    """get_predicted_schema() must apply flow parameters so that ${col_name}
    is resolved before calling the node function, allowing schema prediction
    to succeed and return real column names."""
    graph = make_graph(202)
    graph.flow_settings.parameters = [FlowParameter(name="col_name", default_value="city")]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Alice", "city": "Amsterdam"}]),
        )
    )

    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.select(pl.col('${col_name}'))"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    node = graph.get_node(2)
    schema = node.get_predicted_schema(force=True)

    assert schema is not None, "get_predicted_schema() must return a schema"
    col_names = [c.name for c in schema]
    assert col_names == ["city"], (
        f"Predicted schema should resolve ${'{col_name}'} → 'city', got: {col_names}"
    )


def test_predicted_schema_preserves_original_code_after_lazy_eval():
    """After get_predicted_schema(), the original ${...} reference must still
    be present in setting_input (parameters must be restored after lazy eval)."""
    graph = make_graph(203)
    graph.flow_settings.parameters = [FlowParameter(name="col", default_value="name")]

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Bob"}]),
        )
    )

    original_code = "output_df = input_df.select(pl.col('${col}'))"
    promise2 = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
    graph.add_node_promise(promise2)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=original_code),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    node = graph.get_node(2)
    node.get_predicted_schema(force=True)

    stored_code = node.setting_input.polars_code_input.polars_code
    assert stored_code == original_code, (
        f"Original ${{col}} reference must be preserved after lazy eval, got: {stored_code!r}"
    )


# Issue 3: read node predicted schema with parameterized file path


def test_read_node_predicted_schema_with_parameterized_path():
    """A read node whose file path uses ${file_name} should predict the correct
    schema (column names and types) WITHOUT running the graph, by resolving the
    parameter during lazy schema prediction."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("city,population\n")
        f.write("Amsterdam,900000\n")
        f.write("Rotterdam,650000\n")
        csv_path = f.name

    try:
        graph = make_graph(301)
        graph.flow_settings.parameters = [FlowParameter(name="file_name", default_value=csv_path)]
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read")
        graph.add_node_promise(promise)
        graph.add_read(
            input_schema.NodeRead(
                flow_id=graph.flow_id,
                node_id=1,
                received_file=input_schema.ReceivedTable(
                    name="${file_name}",
                    path="${file_name}",
                    file_type="csv",
                    table_settings=input_schema.InputCsvTable(),
                ),
            )
        )

        node = graph.get_node(1)
        schema = node.get_predicted_schema()

        assert schema is not None, "get_predicted_schema() must return a schema for a parameterized read node"
        col_names = [c.name for c in schema]
        assert "city" in col_names, f"Expected 'city' in predicted schema, got: {col_names}"
        assert "population" in col_names, f"Expected 'population' in predicted schema, got: {col_names}"
    finally:
        os.unlink(csv_path)


def test_read_node_predicted_schema_preserves_parameter_ref():
    """After get_predicted_schema(), the original ${file_name} reference must
    still be present in the read node's setting_input (not the resolved path)."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("x,y\n1,2\n")
        csv_path = f.name

    try:
        graph = make_graph(302)
        graph.flow_settings.parameters = [FlowParameter(name="file_name", default_value=csv_path)]

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read")
        graph.add_node_promise(promise)
        graph.add_read(
            input_schema.NodeRead(
                flow_id=graph.flow_id,
                node_id=1,
                received_file=input_schema.ReceivedTable(
                    name="${file_name}",
                    path="${file_name}",
                    file_type="csv",
                    table_settings=input_schema.InputCsvTable(),
                ),
            )
        )

        node = graph.get_node(1)
        node.get_predicted_schema(force=True)

        stored_path = node.setting_input.received_file.path
        assert stored_path == "${file_name}", (
            f"Original ${{file_name}} reference must be preserved after schema prediction, got: {stored_path!r}"
        )
    finally:
        os.unlink(csv_path)


def test_read_node_run_with_parameterized_path(execution_location):
    """A read node with ${file_name} in the path should successfully read the
    file after parameter resolution during run_graph()."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("name,age\nAlice,30\nBob,25\n")
        csv_path = f.name

    try:
        graph = make_graph(303, execution_location=execution_location)
        graph.flow_settings.parameters = [FlowParameter(name="file_name", default_value=csv_path)]

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read")
        graph.add_node_promise(promise)
        graph.add_read(
            input_schema.NodeRead(
                flow_id=graph.flow_id,
                node_id=1,
                received_file=input_schema.ReceivedTable(
                    name="${file_name}",
                    path="${file_name}",
                    file_type="csv",
                    table_settings=input_schema.InputCsvTable(),
                ),
            )
        )

        run_and_assert_ok(graph)

        result = graph.get_node(1).get_resulting_data()
        assert result is not None
        assert "name" in result.columns
        assert "age" in result.columns
        assert result.to_dict()["name"] == ["Alice", "Bob"]
    finally:
        os.unlink(csv_path)


# Issue 4: parameter change invalidates predicted schema


def test_parameter_change_invalidates_predicted_schema():
    """When a flow parameter value changes via the flow_settings setter,
    nodes that reference ${...} must have their predicted schema invalidated
    so the next get_predicted_schema() returns the schema for the new file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("city,population\n")
        f.write("Amsterdam,900000\n")
        csv_a = f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("product,price\n")
        f.write("Widget,9.99\n")
        csv_b = f.name

    try:
        graph = make_graph(304)
        graph.flow_settings = schemas.FlowSettings(
            flow_id=graph.flow_id,
            name="param_test",
            path=".",
            execution_mode="Development",
            execution_location="local",
            parameters=[FlowParameter(name="file_name", default_value=csv_a)],
        )

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read")
        graph.add_node_promise(promise)
        graph.add_read(
            input_schema.NodeRead(
                flow_id=graph.flow_id,
                node_id=1,
                received_file=input_schema.ReceivedTable(
                    name="${file_name}",
                    path="${file_name}",
                    file_type="csv",
                    table_settings=input_schema.InputCsvTable(),
                ),
            )
        )

        node = graph.get_node(1)
        schema_a = node.get_predicted_schema(force=True)
        assert schema_a is not None
        cols_a = [c.name for c in schema_a]
        assert "city" in cols_a, f"Expected 'city' in schema from file A, got: {cols_a}"
        assert "population" in cols_a, f"Expected 'population' in schema from file A, got: {cols_a}"

        graph.flow_settings = schemas.FlowSettings(
            flow_id=graph.flow_id,
            name="param_test",
            path=".",
            execution_mode="Development",
            execution_location="local",
            parameters=[FlowParameter(name="file_name", default_value=csv_b)],
        )

        schema_b = node.get_predicted_schema(force=True)
        assert schema_b is not None
        cols_b = [c.name for c in schema_b]
        assert "product" in cols_b, f"Expected 'product' in schema from file B, got: {cols_b}"
        assert "price" in cols_b, f"Expected 'price' in schema from file B, got: {cols_b}"

        assert node.setting_input.received_file.path == "${file_name}", (
            "Parameter reference must be preserved after invalidation"
        )
    finally:
        os.unlink(csv_a)
        os.unlink(csv_b)


# formula / filter nodes: type-correct expression-literal parameter rendering


def _manual_input(graph, node_id, rows):
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    )
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(rows),
        )
    )


def _formula(graph, node_id, depending_on_id, out_name, function, data_type="Auto"):
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="formula")
    )
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=out_name, data_type=data_type),
                function=function,
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))


def _advanced_filter(graph, node_id, depending_on_id, expr):
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="filter")
    )
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter=expr),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))


def _column(graph, node_id, col):
    return graph.get_node(node_id).get_resulting_data().to_dict()[col]


def test_formula_string_param_embedded(execution_location):
    """A string param embedded in a formula renders as a quoted literal (no manual quoting)."""
    graph = make_graph(201, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="prefix", default_value="v2_", type="string")]
    _manual_input(graph, 1, [{"name": "Alice"}, {"name": "Bob"}])
    _formula(graph, 2, 1, "renamed", "${prefix} + [name]")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "renamed") == ["v2_Alice", "v2_Bob"]


def test_formula_string_param_whole_field(execution_location):
    """A whole-field ${p} string param yields a constant string column (must not crash)."""
    graph = make_graph(202, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="greeting", default_value="hello", type="string")]
    _manual_input(graph, 1, [{"name": "a"}, {"name": "b"}])
    _formula(graph, 2, 1, "g", "${greeting}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "g") == ["hello", "hello"]


def test_formula_int_param_embedded(execution_location):
    graph = make_graph(203, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="bump", default_value="5", type="integer")]
    _manual_input(graph, 1, [{"val": 10}, {"val": 20}])
    _formula(graph, 2, 1, "bumped", "[val] + ${bump}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "bumped") == [15, 25]


def test_formula_int_param_whole_field_no_crash(execution_location):
    """Regression: a formula whose entire value is ${int_param} previously crashed
    (raw int stuffed into the expression parser). It must resolve to a literal."""
    graph = make_graph(204, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="bump", default_value="5", type="integer")]
    _manual_input(graph, 1, [{"val": 10}, {"val": 20}])
    _formula(graph, 2, 1, "five", "${bump}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "five") == [5, 5]


def test_formula_bool_param_whole_field(execution_location):
    graph = make_graph(205, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="flag", default_value="true", type="boolean")]
    _manual_input(graph, 1, [{"name": "a"}, {"name": "b"}])
    _formula(graph, 2, 1, "f", "${flag}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "f") == [True, True]


def test_formula_string_escaping_roundtrip(execution_location):
    """String param values with a backslash / newline round-trip through the literal renderer."""
    graph = make_graph(206, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="p", default_value="a\\b\nc", type="string")]
    _manual_input(graph, 1, [{"name": "x"}])
    _formula(graph, 2, 1, "s", "${p}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "s") == ["a\\b\nc"]


def test_filter_advanced_string_param(execution_location):
    graph = make_graph(207, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="target", default_value="NYC", type="string")]
    _manual_input(graph, 1, [{"city": "NYC"}, {"city": "LA"}, {"city": "NYC"}])
    _advanced_filter(graph, 2, 1, "[city] == ${target}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "city") == ["NYC", "NYC"]


def test_filter_advanced_numeric_param(execution_location):
    graph = make_graph(208, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="threshold", default_value="25", type="integer")]
    _manual_input(graph, 1, [{"age": 10}, {"age": 30}, {"age": 40}])
    _advanced_filter(graph, 2, 1, "[age] > ${threshold}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "age") == [30, 40]


def test_filter_advanced_preserves_original_after_run(execution_location):
    graph = make_graph(209, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="threshold", default_value="25", type="integer")]
    _manual_input(graph, 1, [{"age": 10}, {"age": 30}])
    _advanced_filter(graph, 2, 1, "[age] > ${threshold}")
    run_and_assert_ok(graph)
    stored = graph.get_node(2).setting_input.filter_input.advanced_filter
    assert stored == "[age] > ${threshold}", f"reference must be preserved after run, got {stored!r}"


def test_formula_param_type_change_rerun(execution_location):
    """The literal is inferred from the runtime value, so changing a param's TYPE
    between runs yields a correctly-typed result for the same ${ref}."""
    graph = make_graph(210, execution_location=execution_location)
    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="5", type="string")]
    _manual_input(graph, 1, [{"name": "a"}])
    _formula(graph, 2, 1, "c", "${x}")
    run_and_assert_ok(graph)
    assert _column(graph, 2, "c") == ["5"]  # string param -> quoted literal -> string column

    graph.flow_settings.parameters = [FlowParameter(name="x", default_value="5", type="integer")]
    graph.reset()
    run_and_assert_ok(graph)
    assert _column(graph, 2, "c") == [5]  # integer param -> bare literal -> int column
