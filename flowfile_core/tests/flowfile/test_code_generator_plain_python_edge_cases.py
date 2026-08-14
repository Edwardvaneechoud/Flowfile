"""Edge-case parity tests for the plain-Python (teaching) export.

Every test here pins a divergence found by adversarially reviewing the first
implementation. They are all the same shape: build a flow, run it, exec the
generated script, and assert the rows match — because "it runs and produces the
same rows" is the only thing that makes a teaching artifact trustworthy.

The recurring themes are nulls (Polars sorts them first, never joins on them,
and aggregates them away to null) and types (the engine casts; text from a CSV
does not cast itself).
"""

import polars as pl
import pytest

from flowfile_core.flowfile.code_generator.plain_python import export_flow_to_plain_python
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema

_flow_counter = iter(range(1000, 100000))


def create_graph() -> FlowGraph:
    handler = FlowfileHandler()
    flow_id = next(_flow_counter)
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="pp_edge", path=".", execution_mode="Performance")
    )
    return handler.get_flow(flow_id)


def add_typed_input(flow: FlowGraph, columns: list[tuple[str, str]], data: list[list], node_id: int = 1) -> FlowGraph:
    """Manual input with explicitly declared dtypes (from_pylist stringifies mixed columns)."""
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name=n, data_type=t) for n, t in columns],
                data=data,
            ),
        )
    )
    return flow


def run_plain(code: str) -> list[dict]:
    namespace: dict = {}
    exec(compile(code, "<plain_python_export>", "exec"), namespace)  # noqa: S102
    return namespace["run_etl_pipeline"]()


def engine_rows(flow: FlowGraph, node_id: int) -> list[dict]:
    return flow.get_node(node_id).get_resulting_data().data_frame.lazy().collect().to_dicts()


def assert_matches_engine(flow: FlowGraph, node_id: int, ordered: bool = False) -> str:
    """The load-bearing assertion: generated script rows == engine rows."""
    code = export_flow_to_plain_python(flow)
    assert "import polars" not in code and "pl." not in code, code
    actual, expected = run_plain(code), engine_rows(flow, node_id)
    if ordered:
        assert actual == expected, f"\nscript={actual}\nengine={expected}\n\n{code}"
    else:
        def key(row: dict):
            return tuple(sorted((k, str(v)) for k, v in row.items()))

        assert sorted(actual, key=key) == sorted(expected, key=key), f"\nscript={actual}\nengine={expected}\n\n{code}"
    return code


def connect(flow: FlowGraph, src: int, tgt: int, kind: str = "main") -> None:
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(src, tgt, kind))


# --------------------------------------------------------------------- sorting


@pytest.mark.parametrize("descending", [False, True])
def test_sort_puts_nulls_first_in_both_directions(descending):
    """Polars sorts nulls first regardless of direction; Python can't compare None at all."""
    flow = add_typed_input(create_graph(), [("a", "Int64")], [[3, None, 1]])
    flow.add_sort(
        input_schema.NodeSort(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="a", how="desc" if descending else "asc")],
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2, ordered=True)


def test_sort_multi_key_mixed_directions_with_nulls():
    flow = add_typed_input(
        create_graph(), [("a", "Int64"), ("b", "Int64")], [[1, 1, None, 2], [2, None, 5, 1]]
    )
    flow.add_sort(
        input_schema.NodeSort(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            sort_input=[
                transform_schema.SortByInput(column="a", how="asc"),
                transform_schema.SortByInput(column="b", how="desc"),
            ],
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2, ordered=True)


# ---------------------------------------------------------------- aggregations


def test_aggregations_on_singleton_and_all_null_groups():
    """Polars returns null for min/max/mean/median/std/var on an empty group; sum returns 0."""
    flow = add_typed_input(
        create_graph(), [("g", "String"), ("v", "Int64")],
        [["x", "y", "y"], [None, 5, 7]],
    )
    flow.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(agg_cols=[
                transform_schema.AggColl(old_name="g", agg="groupby"),
                transform_schema.AggColl(old_name="v", agg="sum", new_name="s"),
                transform_schema.AggColl(old_name="v", agg="min", new_name="mn"),
                transform_schema.AggColl(old_name="v", agg="max", new_name="mx"),
                transform_schema.AggColl(old_name="v", agg="mean", new_name="av"),
                transform_schema.AggColl(old_name="v", agg="median", new_name="md"),
                transform_schema.AggColl(old_name="v", agg="std", new_name="sd"),
                transform_schema.AggColl(old_name="v", agg="var", new_name="vr"),
                transform_schema.AggColl(old_name="v", agg="count", new_name="ct"),
                transform_schema.AggColl(old_name="v", agg="n_unique", new_name="nu"),
            ]),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_aggregated_columns_that_sanitize_alike_get_distinct_variables():
    """"my col" and "my-col" both sanitize to my_col; one must not shadow the other."""
    flow = add_typed_input(
        create_graph(),
        [("g", "String"), ("my col", "Int64"), ("my-col", "Int64")],
        [["a", "a"], [1, 2], [10, 20]],
    )
    flow.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(agg_cols=[
                transform_schema.AggColl(old_name="g", agg="groupby"),
                transform_schema.AggColl(old_name="my col", agg="sum", new_name="s1"),
                transform_schema.AggColl(old_name="my-col", agg="sum", new_name="s2"),
            ]),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_group_by_without_aggregations_uses_the_renamed_column():
    flow = add_typed_input(create_graph(), [("g", "String")], [["x", "x", "y"]])
    flow.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(agg_cols=[
                transform_schema.AggColl(old_name="g", agg="groupby", new_name="grp"),
            ]),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    code = assert_matches_engine(flow, 2)
    assert '"grp"' in code


# ----------------------------------------------------------------------- joins


def _join_flow(how: str, left: list[dict], right: list[dict], left_cols, right_cols) -> FlowGraph:
    flow = create_graph()
    add_typed_input(flow, left_cols, [[r[c] for r in left] for c, _ in left_cols], node_id=1)
    add_typed_input(flow, right_cols, [[r[c] for r in right] for c, _ in right_cols], node_id=2)
    flow.add_join(
        input_schema.NodeJoin(
            flow_id=flow.flow_id, node_id=3, depending_on_ids=[1, 2],
            auto_generate_selection=True, verify_integrity=False,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="id", right_col="id")],
                left_select=transform_schema.JoinInputs(
                    renames=[transform_schema.SelectInput(old_name=c, keep=True) for c, _ in left_cols]
                ),
                right_select=transform_schema.JoinInputs(
                    renames=[transform_schema.SelectInput(old_name=c, keep=True) for c, _ in right_cols]
                ),
                how=how,
            ),
        )
    )
    connect(flow, 1, 3, "main")
    connect(flow, 2, 3, "right")
    return flow


@pytest.mark.parametrize("how", ["inner", "left"])
def test_join_with_colliding_column_names_keeps_both_sides(how):
    """The engine suffixes the right side with _right; duplicate dict keys would eat the left value."""
    flow = _join_flow(
        how,
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        [{"id": 1, "name": "Acme"}],
        [("id", "Int64"), ("name", "String")],
        [("id", "Int64"), ("name", "String")],
    )
    flow.run_graph()
    code = assert_matches_engine(flow, 3)
    assert "name_right" in code, "the right-hand collision must be renamed, not overwrite the left"


@pytest.mark.parametrize("how", ["inner", "left", "semi", "anti"])
def test_join_never_matches_a_null_key(how):
    flow = _join_flow(
        how,
        [{"id": 1, "name": "Alice"}, {"id": None, "name": "Nobody"}],
        [{"id": 1, "city": "NYC"}, {"id": None, "city": "Nowhere"}],
        [("id", "Int64"), ("name", "String")],
        [("id", "Int64"), ("city", "String")],
    )
    flow.run_graph()
    assert_matches_engine(flow, 3)


def test_right_join_becomes_an_honest_stub():
    flow = _join_flow(
        "right",
        [{"id": 1, "name": "Alice"}],
        [{"id": 1, "city": "NYC"}],
        [("id", "Int64"), ("name", "String")],
        [("id", "Int64"), ("city", "String")],
    )
    code = export_flow_to_plain_python(flow)
    assert "NotImplementedError" in code
    assert "swapped" in code


# ------------------------------------------------------------------------ CSV


def test_csv_blank_cells_become_none_not_empty_string(tmp_path):
    path = tmp_path / "n.csv"
    path.write_text("name,amount\nalice,10\nbob,\n")
    flow = create_graph()
    flow.add_read(
        input_schema.NodeRead(
            flow_id=flow.flow_id, node_id=1,
            received_file=input_schema.ReceivedTable(
                name="n.csv", path=str(path), abs_file_path=str(path), file_type="csv",
                table_settings=input_schema.InputCsvTable(delimiter=",", has_headers=True, encoding="utf-8"),
            ),
        )
    )
    flow.run_graph()
    assert_matches_engine(flow, 1)


def test_csv_filter_over_a_column_with_a_blank_cell(tmp_path):
    """The classic: a blank cell left as "" makes the null guard pass and then TypeErrors."""
    path = tmp_path / "n.csv"
    path.write_text("name,revenue\na,100.0\nb,\nc,300.0\n")
    flow = create_graph()
    flow.add_read(
        input_schema.NodeRead(
            flow_id=flow.flow_id, node_id=1,
            received_file=input_schema.ReceivedTable(
                name="n.csv", path=str(path), abs_file_path=str(path), file_type="csv",
                table_settings=input_schema.InputCsvTable(delimiter=",", has_headers=True, encoding="utf-8"),
            ),
        )
    )
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="revenue", operator="greater_than", value="100"),
            ),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_csv_boolean_and_date_columns_are_typed(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("name,flag,when\nalice,true,2024-01-01\nbob,false,2024-02-02\n")
    flow = create_graph()
    flow.add_read(
        input_schema.NodeRead(
            flow_id=flow.flow_id, node_id=1,
            received_file=input_schema.ReceivedTable(
                name="t.csv", path=str(path), abs_file_path=str(path), file_type="csv",
                table_settings=input_schema.InputCsvTable(delimiter=",", has_headers=True, encoding="utf-8"),
            ),
        )
    )
    flow.run_graph()
    assert_matches_engine(flow, 1)


# --------------------------------------------------------------------- filters


def test_contains_is_a_regex_like_the_engine():
    flow = add_typed_input(create_graph(), [("s", "String")], [["a.c", "abc", "xyz"]])
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="s", operator="contains", value="a.c"),
            ),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_between_without_an_upper_bound_stubs_instead_of_narrowing():
    """The flow itself fails here; silently returning a plausible subset would be worse."""
    flow = add_typed_input(create_graph(), [("a", "Int64")], [[1, 2, 3, 4]])
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="a", operator="between", value="2", value2=None),
            ),
        )
    )
    connect(flow, 1, 2)
    code = export_flow_to_plain_python(flow)
    assert "NotImplementedError" in code


# ------------------------------------------------------------- select & typing


def test_select_applies_casts_even_without_data_type_change():
    flow = add_typed_input(create_graph(), [("a", "Int64"), ("b", "Int64")], [[1], [0]])
    flow.add_select(
        input_schema.NodeSelect(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1, keep_missing=False,
            select_input=[
                transform_schema.SelectInput(old_name="a", data_type="Boolean", data_type_change=True, keep=True),
                transform_schema.SelectInput(old_name="b", data_type="String", data_type_change=False, keep=True),
            ],
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_manual_input_honours_declared_column_types():
    """Stored values can be text while the declared dtype is Int64; the engine casts."""
    flow = add_typed_input(create_graph(), [("a", "Int64"), ("b", "Float64")], [[["1", "2"][i] for i in (0, 1)], ["1.5", "2.5"]])
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="a", operator="greater_than", value="1"),
            ),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_temporal_literals_import_their_module():
    """repr(date(...)) is `datetime.date(...)`, which needs an import to run."""
    import datetime as dt

    flow = create_graph()
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id, node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="d", data_type="Date")],
                data=[[dt.date(2024, 1, 1)]],
            ),
        )
    )
    code = export_flow_to_plain_python(flow)
    assert "import datetime" in code
    run_plain(code)  # must not NameError


# ----------------------------------------------------------------------- union


def test_union_with_a_single_input_iterates_tables_not_rows():
    flow = add_typed_input(create_graph(), [("a", "Int64")], [[1, 2]])
    flow.add_union(
        input_schema.NodeUnion(
            flow_id=flow.flow_id, node_id=2, depending_on_ids=[1],
            union_input=transform_schema.UnionInput(mode="relaxed"),
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2)


def test_union_keeps_columns_from_an_input_contributing_no_rows():
    flow = create_graph()
    add_typed_input(flow, [("a", "Int64"), ("b", "Int64")], [[], []], node_id=1)
    add_typed_input(flow, [("b", "Int64"), ("c", "Int64")], [[5], [6]], node_id=2)
    flow.add_union(
        input_schema.NodeUnion(
            flow_id=flow.flow_id, node_id=3, depending_on_ids=[1, 2],
            union_input=transform_schema.UnionInput(mode="relaxed"),
        )
    )
    connect(flow, 1, 3)
    connect(flow, 2, 3)
    flow.run_graph()
    assert_matches_engine(flow, 3)


# ----------------------------------------------------------- script mechanics


def test_pinned_node_reference_does_not_collide_with_loop_variables():
    """A node literally named `row` must not be shadowed by `for row in ...`."""
    flow = add_typed_input(create_graph(), [("product", "String")], [["b", "a", "c"]])
    flow.get_node(1).setting_input.node_reference = "row"
    flow.add_sort(
        input_schema.NodeSort(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="product", how="asc")],
        )
    )
    connect(flow, 1, 2)
    flow.run_graph()
    assert_matches_engine(flow, 2, ordered=True)


def test_explore_data_passes_through_instead_of_stubbing():
    """A canvas-only preview node must not abort an otherwise runnable script."""
    flow = add_typed_input(create_graph(), [("a", "Int64")], [[1, 2]])
    flow.add_explore_data(
        input_schema.NodeExploreData(flow_id=flow.flow_id, node_id=2, depending_on_id=1)
    )
    connect(flow, 1, 2)
    code = export_flow_to_plain_python(flow)
    assert "NotImplementedError" not in code
    assert run_plain(code) == [{"a": 1}, {"a": 2}]


def test_empty_flow_produces_a_runnable_script():
    flow = create_graph()
    code = export_flow_to_plain_python(flow)
    run_plain(code)  # must not TypeError on `for row in None`
    assert "pipeline_output or []" in code


def test_left_join_teaching_comment_is_indented_with_its_block():
    """The artifact exists to be read; a comment at column 0 inside a block is a defect."""
    flow = _join_flow(
        "left",
        [{"id": 1, "name": "Alice"}],
        [{"id": 1, "city": "NYC"}],
        [("id", "Int64"), ("name", "String")],
        [("id", "Int64"), ("city", "String")],
    )
    code = export_flow_to_plain_python(flow)
    for line in code.splitlines():
        if line.strip().startswith("#") and "unmatched left row" in line:
            assert line.startswith("        #"), f"comment not indented with its block: {line!r}"
            break
    else:
        pytest.fail("left-join teaching comment missing")


def test_generated_scripts_stay_free_of_dataframe_imports():
    """The whole promise of the mode, asserted over a flow touching many emitters."""
    flow = add_typed_input(
        create_graph(), [("g", "String"), ("v", "Int64")], [["a", "b", "a"], [1, 2, 3]]
    )
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id, node_id=2, depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="v", operator="greater_than", value="0"),
            ),
        )
    )
    flow.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=flow.flow_id, node_id=3, depending_on_id=2,
            groupby_input=transform_schema.GroupByInput(agg_cols=[
                transform_schema.AggColl(old_name="g", agg="groupby"),
                transform_schema.AggColl(old_name="v", agg="sum", new_name="total"),
            ]),
        )
    )
    flow.add_sort(
        input_schema.NodeSort(
            flow_id=flow.flow_id, node_id=4, depending_on_id=3,
            sort_input=[transform_schema.SortByInput(column="total", how="desc")],
        )
    )
    connect(flow, 1, 2)
    connect(flow, 2, 3)
    connect(flow, 3, 4)
    flow.run_graph()
    code = assert_matches_engine(flow, 4, ordered=True)
    for banned in ("import polars", "pl.", "import flowfile", "polars_expr_transformer"):
        assert banned not in code
