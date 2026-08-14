"""Tests for the plain-Python (teaching) export.

Two things are load-bearing here and are asserted on every fixture flow:

1. the output never reaches for Polars — that is the whole promise of the mode,
   and inheritance from the Polars converter makes it easy to break silently;
2. the output actually runs and produces the same rows the engine does.
"""

import polars as pl
import pytest

from flowfile_core.flowfile.code_generator.plain_python import (
    PLAIN_PYTHON_NODE_TYPES,
    explain_node_in_plain_python,
    export_flow_to_plain_python,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="plain_python_flow", path=".", execution_mode="Performance")
    )
    return handler.get_flow(flow_id)


def add_rows(flow: FlowGraph, rows: list[dict], node_id: int = 1) -> FlowGraph:
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(rows)
        )
    )
    return flow


SALES = [
    {"product": "A", "region": "North", "revenue": 100.0, "qty": 2},
    {"product": "B", "region": "North", "revenue": 200.0, "qty": 4},
    {"product": "A", "region": "South", "revenue": 150.0, "qty": 3},
    {"product": "C", "region": "South", "revenue": 50.0, "qty": 1},
    {"product": "B", "region": "East", "revenue": 300.0, "qty": 6},
]


def run_plain(code: str) -> list[dict]:
    """Execute a generated script in a namespace with no dataframe library in it."""
    namespace: dict = {}
    exec(compile(code, "<plain_python_export>", "exec"), namespace)  # noqa: S102
    return namespace["run_etl_pipeline"]()


def engine_rows(flow: FlowGraph, node_id: int) -> list[dict]:
    return flow.get_node(node_id).get_resulting_data().data_frame.lazy().collect().to_dicts()


def assert_no_polars(code: str) -> None:
    """The guard that makes inheriting from the Polars converter safe."""
    assert "import polars" not in code, f"plain-Python export imported polars:\n{code}"
    assert "pl." not in code, f"plain-Python export emitted a Polars expression:\n{code}"
    assert "import flowfile" not in code, f"plain-Python export imported flowfile:\n{code}"


def assert_same_rows(actual: list[dict], expected: list[dict]) -> None:
    def key(row: dict):
        return tuple(sorted((k, str(v)) for k, v in row.items()))

    assert sorted(actual, key=key) == sorted(expected, key=key)


def test_supported_types_are_derived_from_real_emitters():
    """The allowlist must come from the emitters, so it can never run ahead of them."""
    from flowfile_core.flowfile.code_generator.plain_python_handlers import PlainPythonHandlersMixin

    for node_type in PLAIN_PYTHON_NODE_TYPES:
        assert hasattr(PlainPythonHandlersMixin, f"_handle_{node_type}")
    assert {"filter", "select", "sort", "unique", "group_by", "join", "manual_input", "record_count"} <= (
        PLAIN_PYTHON_NODE_TYPES
    )


def test_manual_input_only():
    flow = add_rows(create_graph(), SALES)
    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert_same_rows(run_plain(code), SALES)


def test_filter_basic_round_trip():
    flow = add_rows(create_graph(), SALES)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="revenue", operator="greater_than", value="100"),
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "for row in" in code
    assert_same_rows(run_plain(code), engine_rows(flow, 2))


def test_group_by_uses_accumulator_dict_and_matches_engine():
    flow = add_rows(create_graph(), SALES)
    flow.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl(old_name="product", agg="groupby"),
                    transform_schema.AggColl(old_name="revenue", agg="sum", new_name="total_revenue"),
                    transform_schema.AggColl(old_name="qty", agg="max", new_name="max_qty"),
                ]
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "groups = {}" in code, "group_by should teach the accumulator-dict pattern"
    assert_same_rows(run_plain(code), engine_rows(flow, 2))


def test_unique_uses_a_seen_set():
    flow = add_rows(create_graph(), SALES)
    flow.add_unique(
        input_schema.NodeUnique(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            unique_input=transform_schema.UniqueInput(columns=["product"], strategy="first"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "seen = set()" in code
    assert_same_rows(run_plain(code), engine_rows(flow, 2))


def test_sort_round_trip():
    flow = add_rows(create_graph(), SALES)
    flow.add_sort(
        input_schema.NodeSort(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="revenue", how="desc")],
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "sorted(" in code
    assert run_plain(code) == engine_rows(flow, 2), "sort order must match exactly, not just the row set"


def test_select_rename_and_drop():
    flow = add_rows(create_graph(), SALES)
    flow.add_select(
        input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            keep_missing=False,
            select_input=[
                transform_schema.SelectInput(old_name="product", new_name="sku", keep=True, position=0),
                transform_schema.SelectInput(old_name="revenue", keep=True, position=1),
                transform_schema.SelectInput(old_name="region", keep=False),
                transform_schema.SelectInput(old_name="qty", keep=False),
            ],
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert_same_rows(run_plain(code), engine_rows(flow, 2))


def test_record_count():
    flow = add_rows(create_graph(), SALES)
    flow.add_record_count(
        input_schema.NodeRecordCount(flow_id=flow.flow_id, node_id=2, depending_on_id=1)
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert run_plain(code) == [{"number_of_records": 5}]


def test_csv_read_makes_type_coercion_explicit(tmp_path):
    csv_path = tmp_path / "sales.csv"
    pl.DataFrame(SALES).write_csv(csv_path)

    flow = create_graph()
    flow.add_read(
        input_schema.NodeRead(
            flow_id=flow.flow_id,
            node_id=1,
            received_file=input_schema.ReceivedTable(
                name="sales.csv",
                path=str(csv_path),
                abs_file_path=str(csv_path),
                file_type="csv",
                table_settings=input_schema.InputCsvTable(delimiter=",", has_headers=True, encoding="utf-8"),
            ),
        )
    )
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "csv.DictReader" in code
    assert "float(row[" in code or "int(row[" in code, "numeric columns should be coerced explicitly"
    assert_same_rows(run_plain(code), engine_rows(flow, 1))


def test_join_builds_an_index_and_matches_engine():
    flow = create_graph()
    add_rows(flow, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], node_id=1)
    add_rows(flow, [{"id": 1, "city": "NYC"}, {"id": 2, "city": "LA"}], node_id=2)
    flow.add_join(
        input_schema.NodeJoin(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            auto_generate_selection=True,
            verify_integrity=False,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="id", right_col="id")],
                left_select=transform_schema.JoinInputs(
                    renames=[
                        transform_schema.SelectInput(old_name="id", keep=True, join_key=True),
                        transform_schema.SelectInput(old_name="name", keep=True),
                    ]
                ),
                right_select=transform_schema.JoinInputs(
                    renames=[
                        transform_schema.SelectInput(old_name="id", keep=False, join_key=True),
                        transform_schema.SelectInput(old_name="city", keep=True),
                    ]
                ),
                how="inner",
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 3, "main"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))
    flow.run_graph()

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "right_index" in code, "join should teach hash indexing, not a nested loop"
    assert_same_rows(run_plain(code), engine_rows(flow, 3))


def test_formula_node_becomes_an_exercise_stub_without_failing_the_export():
    """A DSL node must not cost the user the rest of the script."""
    flow = add_rows(create_graph(), SALES)
    flow.add_formula(
        input_schema.NodeFormula(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="with_vat", data_type="Double"),
                function="[revenue] * 1.21",
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    code = export_flow_to_plain_python(flow)
    assert_no_polars(code)
    assert "NotImplementedError" in code
    assert "[revenue] * 1.21" in code, "the stub should quote the rule the reader has to implement"
    assert "over to you" in code

    # It is valid Python and stops at the gap instead of returning wrong rows.
    compile(code, "<stub>", "exec")
    with pytest.raises(NotImplementedError):
        run_plain(code)


def test_explain_node_returns_prose_and_snippet_for_actual_settings():
    flow = add_rows(create_graph(), SALES)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="region", operator="equals", value="North"),
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    explanation = explain_node_in_plain_python(flow, 2)
    assert explanation["node_type"] == "filter"
    assert explanation["supported"] is True
    assert "keeps the rows that pass a test" in explanation["explanation"].lower()
    assert "North" in explanation["code"], "the snippet should reflect the configured value"
    assert "region" in explanation["code"]


def test_explain_node_is_honest_about_unsupported_types():
    flow = add_rows(create_graph(), SALES)
    flow.add_formula(
        input_schema.NodeFormula(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="x", data_type="Double"), function="[revenue] * 2"
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    explanation = explain_node_in_plain_python(flow, 2)
    assert explanation["supported"] is False
