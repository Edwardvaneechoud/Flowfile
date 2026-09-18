"""Tests for the conservative static settings validation (missing-column warnings)."""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.flowfile.settings_validation import validate_flow_settings
from flowfile_core.schemas import input_schema, schemas, transform_schema
from tests.flowfile.conftest import (
    add_test_catalog_writer as _add_catalog_writer,
)
from tests.flowfile.conftest import (
    catalog_cleanup as _catalog_cleanup,
)
from tests.flowfile.conftest import (
    create_test_namespace as _create_namespace,
)


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="validation_flow", path=".",
                             execution_mode="Development", execution_location="local")
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id,
                                     raw_data_format=input_schema.RawData.from_pylist(data)))


def add_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type))


def add_select(graph: FlowGraph, node_id: int, renames: list[tuple[str, str]], keep_missing: bool = True) -> None:
    select_input = [transform_schema.SelectInput(old_name=o, new_name=n) for o, n in renames]
    graph.add_select(input_schema.NodeSelect(flow_id=graph.flow_id, node_id=node_id,
                                             select_input=select_input, keep_missing=keep_missing))


def add_group_by(graph: FlowGraph, node_id: int, agg_cols: list[tuple[str, str]]) -> None:
    groupby_input = transform_schema.GroupByInput(
        [transform_schema.AggColl(col, agg) for col, agg in agg_cols])
    graph.add_group_by(input_schema.NodeGroupBy(flow_id=graph.flow_id, node_id=node_id,
                                                groupby_input=groupby_input))


def issues_by_node(result) -> dict:
    return {n.node_id: n.issues for n in result.nodes}


BASE_DATA = [{"a": 1, "b": 2.5}, {"a": 2, "b": 3.5}]


def test_happy_path_no_issues():
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "group_by", 2)
    connect(graph, 1, 2)
    add_group_by(graph, 2, [("a", "groupby"), ("b", "sum")])

    result = validate_flow_settings(graph)
    assert result.enabled is True
    assert result.nodes == []


def broken_group_by_graph() -> FlowGraph:
    """input(a, b) -> select(a renamed to a2) -> group_by still referencing 'a'."""
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    add_select(graph, 2, [("a", "a2"), ("b", "b")])
    add_promise(graph, "group_by", 3)
    connect(graph, 2, 3)
    add_group_by(graph, 3, [("a", "groupby"), ("b", "sum")])
    return graph


def test_upstream_rename_flags_downstream_group_by():
    graph = broken_group_by_graph()
    result = validate_flow_settings(graph)
    issues = issues_by_node(result)
    assert set(issues) == {3}
    (issue,) = issues[3]
    assert issue.input_handle == "main"
    assert issue.missing_columns == ["a"]
    assert "a" in issue.message


def test_fixing_rename_clears_issue():
    graph = broken_group_by_graph()
    assert issues_by_node(validate_flow_settings(graph))
    add_select(graph, 2, [("a", "a"), ("b", "b")])
    assert validate_flow_settings(graph).nodes == []


@pytest.mark.parametrize("bad_side", ["left", "right"])
def test_join_key_missing_is_attributed_to_correct_handle(bad_side):
    graph = create_graph()
    add_manual_input(graph, [{"id": 1, "name": "x"}], node_id=1)
    add_manual_input(graph, [{"id": 1, "value": 2}], node_id=2)
    add_promise(graph, "join", 3)
    connect(graph, 1, 3, "main")
    connect(graph, 2, 3, "right")
    left_col = "nope" if bad_side == "left" else "id"
    right_col = "nope" if bad_side == "right" else "id"
    join_input = {"join_mapping": [{"left_col": left_col, "right_col": right_col}],
                  "left_select": {"renames": []}, "right_select": {"renames": []}, "how": "inner"}
    graph.add_join(input_schema.NodeJoin(flow_id=graph.flow_id, node_id=3, join_input=join_input,
                                         auto_generate_selection=True, verify_integrity=False))

    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {3}
    (issue,) = issues[3]
    assert issue.input_handle == bad_side
    assert issue.missing_columns == ["nope"]


def filter_graph(filter_input: transform_schema.FilterInput) -> FlowGraph:
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "filter", 2)
    connect(graph, 1, 2)
    graph.add_filter(input_schema.NodeFilter(flow_id=graph.flow_id, node_id=2, filter_input=filter_input))
    return graph


def test_filter_basic_missing_column_warns():
    graph = filter_graph(transform_schema.FilterInput(
        mode="basic", basic_filter=transform_schema.BasicFilter(field="gone", value="1")))
    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    assert issues[2][0].missing_columns == ["gone"]


def test_filter_advanced_expression_missing_column_warns():
    graph = filter_graph(transform_schema.FilterInput(mode="advanced", advanced_filter="[gone] > 1"))
    issues = issues_by_node(validate_flow_settings(graph))
    assert issues[2][0].missing_columns == ["gone"]


def test_filter_advanced_expression_valid_stays_silent():
    graph = filter_graph(transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"))
    assert validate_flow_settings(graph).nodes == []


def test_filter_advanced_expression_must_be_boolean():
    graph = filter_graph(transform_schema.FilterInput(mode="advanced", advanced_filter="[a]"))
    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    (issue,) = issues[2]
    assert issue.kind == "invalid_expression"
    assert issue.missing_columns == []
    assert issue.message.startswith("Invalid filter expression: ")
    assert "filter predicate must be of type" in issue.message


def test_filter_advanced_expression_unparseable_is_reported():
    graph = filter_graph(transform_schema.FilterInput(mode="advanced", advanced_filter="((( [a]"))
    issues = issues_by_node(validate_flow_settings(graph))
    assert issues[2][0].kind == "invalid_expression"


def test_basic_filter_is_never_expression_checked():
    graph = filter_graph(transform_schema.FilterInput(
        mode="basic", basic_filter=transform_schema.BasicFilter(field="a", value="1")))
    assert validate_flow_settings(graph).nodes == []


def formula_graph(expression: str) -> FlowGraph:
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "formula", 2)
    connect(graph, 1, 2)
    graph.add_formula(input_schema.NodeFormula(
        flow_id=graph.flow_id, node_id=2,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="out"), function=expression)))
    return graph


def test_formula_missing_column_warns():
    issues = issues_by_node(validate_flow_settings(formula_graph("[gone] * 2 + [a]")))
    assert set(issues) == {2}
    assert issues[2][0].missing_columns == ["gone"]


def test_formula_conditional_branches_are_checked():
    issues = issues_by_node(validate_flow_settings(
        formula_graph("if [a] > 1 then [gone] else [b] endif")))
    assert issues[2][0].missing_columns == ["gone"]


def test_formula_conservative_cases_stay_silent():
    assert validate_flow_settings(formula_graph("[a] + [b]")).nodes == []
    # brackets inside a string literal are not column references
    assert validate_flow_settings(formula_graph('"literal [gone] text"')).nodes == []


def test_formula_type_error_is_reported():
    issues = issues_by_node(validate_flow_settings(formula_graph('[a] + "x"')))
    assert set(issues) == {2}
    (issue,) = issues[2]
    assert issue.kind == "invalid_expression"
    assert issue.input_handle == "main"
    assert issue.missing_columns == []
    assert issue.message == (
        "Invalid formula: arithmetic on dtypes i64 and str is not allowed "
        "(lhs: column 'a', rhs: expression `\"x\"`); try an explicit cast first"
    )


@pytest.mark.parametrize("expression", ["((( [a]", "sum([a])"])
def test_formula_unparseable_is_reported(expression):
    issues = issues_by_node(validate_flow_settings(formula_graph(expression)))
    assert issues[2][0].kind == "invalid_expression"
    assert "\n" not in issues[2][0].message


def test_missing_column_wins_over_expression_issue():
    """A missing column must not also be re-reported as an invalid expression."""
    issues = issues_by_node(validate_flow_settings(formula_graph('[gone] + "x"')))
    (issue,) = issues[2]
    assert issue.kind == "missing_columns"
    assert issue.missing_columns == ["gone"]


def multi_formula_graph(rows: list[tuple[str, str, str | None]]) -> FlowGraph:
    """1 manual_input(a, b) -> 2 formula with the given (name, expression, data_type) entries."""
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "formula", 2)
    connect(graph, 1, 2)
    graph.add_formula(input_schema.NodeFormula(
        flow_id=graph.flow_id, node_id=2,
        functions=[
            transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=name, data_type=data_type or "Auto"),
                function=expression)
            for name, expression, data_type in rows]))
    return graph


def test_multi_entry_reference_to_an_earlier_output_stays_silent():
    graph = multi_formula_graph([("x", "[a] + 1", None), ("y", "[x] + 1", None)])
    assert validate_flow_settings(graph).nodes == []
    assert graph.run_graph().success


def test_multi_entry_forward_reference_warns_on_the_referencing_entry():
    graph = multi_formula_graph([("x", "[y] + 1", None), ("y", "[a] + 1", None)])
    issues = issues_by_node(validate_flow_settings(graph))
    # the column phase names the missing input column; the chain phase stays quiet about it
    assert [i.kind for i in issues[2]] == ["missing_columns"]
    assert issues[2][0].missing_columns == ["y"]


def test_multi_entry_message_names_the_entry():
    graph = multi_formula_graph([("x", "[a] + 1", None), ("y", '[x] + "z"', None)])
    (issue,) = issues_by_node(validate_flow_settings(graph))[2]
    assert issue.kind == "invalid_expression"
    assert issue.message.startswith('Formula 2 ("y"): ')


def test_single_entry_message_keeps_the_node_level_label():
    (issue,) = issues_by_node(validate_flow_settings(formula_graph('[a] + "x"')))[2]
    assert issue.message.startswith("Invalid formula: ")


def test_multi_entry_broken_entry_does_not_cascade():
    """A type error on entry 1 must not make entry 2's reference to its output an error too."""
    graph = multi_formula_graph([("x", '[a] + "z"', None), ("y", '[x] + "!"', None)])
    issues = issues_by_node(validate_flow_settings(graph))[2]
    assert [i.message.split(":")[0] for i in issues] == ['Formula 1 ("x")']


def test_multi_entry_reports_every_failing_entry():
    graph = multi_formula_graph([("x", '[a] + "z"', None), ("y", "((( [a]", None)])
    issues = issues_by_node(validate_flow_settings(graph))[2]
    assert [i.message.split(":")[0] for i in issues] == ['Formula 1 ("x")', 'Formula 2 ("y")']


def test_blank_entry_is_never_reported():
    graph = multi_formula_graph([("x", "   ", None), ("y", "[a] + 1", None)])
    assert validate_flow_settings(graph).nodes == []


def test_duplicate_output_names_warn_without_blocking_the_run():
    graph = multi_formula_graph([("x", "[a] + 1", None), ("x", "[a] + 2", None)])
    (issue,) = issues_by_node(validate_flow_settings(graph))[2]
    assert issue.kind == "duplicate_output"
    assert "formula 1" in issue.message
    assert graph.run_graph().success


def test_blank_output_name_is_a_config_issue():
    graph = multi_formula_graph([("a", "[a] + 1", None), ("", "[a] + 2", None)])
    (issue,) = issues_by_node(validate_flow_settings(graph))[2]
    assert issue.message == "Formula 2: output column name is empty"


def test_formula_with_defined_parameter_is_resolved_then_checked():
    graph = formula_graph("[a] + ${bump}")
    graph.flow_settings.parameters = [FlowParameter(name="bump", type="integer", default_value="2")]
    assert validate_flow_settings(graph).nodes == []

    graph = formula_graph("[a] + ${label}")
    graph.flow_settings.parameters = [FlowParameter(name="label", type="string", default_value="x")]
    issues = issues_by_node(validate_flow_settings(graph))
    assert issues[2][0].kind == "invalid_expression"


def test_formula_with_undefined_parameter_stays_silent():
    assert validate_flow_settings(formula_graph("[a] + ${nowhere}")).nodes == []


def test_blocked_prediction_upstream_suppresses_expression_issue():
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    add_select(graph, 2, [("a", "a"), ("b", "b")])
    add_promise(graph, "formula", 3)
    connect(graph, 2, 3)
    graph.add_formula(input_schema.NodeFormula(
        flow_id=graph.flow_id, node_id=3,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="out"), function='[a] + "x"')))
    assert issues_by_node(validate_flow_settings(graph))

    select_node = graph.get_node(2)
    select_node._executes_on_kernel = True
    select_node.reset()
    assert validate_flow_settings(graph).nodes == []


def test_blocked_prediction_upstream_suppresses_warning():
    graph = broken_group_by_graph()
    assert issues_by_node(validate_flow_settings(graph))

    select_node = graph.get_node(2)
    select_node._executes_on_kernel = True
    select_node.reset()
    assert validate_flow_settings(graph).nodes == []


def test_toggle_off_skips_analysis():
    graph = broken_group_by_graph()
    graph.flow_settings.validate_settings = False
    result = validate_flow_settings(graph)
    assert result.enabled is False
    assert result.nodes == []


def test_unpivot_dtype_mode_skips_but_column_mode_warns():
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "unpivot", 2)
    connect(graph, 1, 2)
    graph.add_unpivot(input_schema.NodeUnpivot(
        flow_id=graph.flow_id, node_id=2,
        unpivot_input=transform_schema.UnpivotInput(value_columns=["gone"],
                                                    data_type_selector_mode="data_type",
                                                    data_type_selector="numeric")))
    assert validate_flow_settings(graph).nodes == []

    graph.add_unpivot(input_schema.NodeUnpivot(
        flow_id=graph.flow_id, node_id=2,
        unpivot_input=transform_schema.UnpivotInput(value_columns=["gone"],
                                                    data_type_selector_mode="column")))
    issues = issues_by_node(validate_flow_settings(graph))
    assert issues[2][0].missing_columns == ["gone"]


def test_unconnected_node_no_issues():
    graph = create_graph()
    add_promise(graph, "group_by", 1)
    add_group_by(graph, 1, [("a", "groupby")])
    assert validate_flow_settings(graph).nodes == []


def test_select_never_warns_it_skips_missing_columns():
    """A select drops an unavailable column from its selection and still runs."""
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    select_input = [
        transform_schema.SelectInput(old_name="a"),
        transform_schema.SelectInput(old_name="gone_keep"),
        transform_schema.SelectInput(old_name="gone_drop", keep=False),
    ]
    graph.add_select(input_schema.NodeSelect(flow_id=graph.flow_id, node_id=2,
                                             select_input=select_input, keep_missing=True))
    assert validate_flow_settings(graph).nodes == []
    # data_type is unset here, which used to crash on the missing column
    assert graph.run_graph().success


ORACLE_DATA = [{"a": 1, "b": 2.5, "s": "x,y"}, {"a": 2, "b": 3.5, "s": "z"}]


def _oracle_graph(node_type: str, target: str, drop_target: bool) -> FlowGraph:
    """input(a, b, s) -> select (optionally renaming `target` away) -> the node under test."""
    graph = create_graph()
    add_manual_input(graph, ORACLE_DATA, node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    add_select(graph, 2, [(c, f"{c}_gone" if drop_target and c == target else c)
                          for c in ("a", "b", "s")])
    add_promise(graph, node_type, 3)
    connect(graph, 2, 3)
    return graph


def _tolerance_cases():
    """(node_type, add_method, target column, settings referencing that column)."""
    return [
        ("select", "add_select", "a", lambda fid: input_schema.NodeSelect(
            flow_id=fid, node_id=3, keep_missing=True,
            select_input=[transform_schema.SelectInput(old_name="a", data_type="Int64")])),
        ("dynamic_rename", "add_dynamic_rename", "a", lambda fid: input_schema.NodeDynamicRename(
            flow_id=fid, node_id=3,
            dynamic_rename_input=transform_schema.DynamicRenameInput(
                rename_mode="prefix", prefix="x_", selection_mode="list", selected_columns=["a"]))),
        ("group_by", "add_group_by", "a", lambda fid: input_schema.NodeGroupBy(
            flow_id=fid, node_id=3,
            groupby_input=transform_schema.GroupByInput([transform_schema.AggColl("a", "sum")]))),
        ("sort", "add_sort", "a", lambda fid: input_schema.NodeSort(
            flow_id=fid, node_id=3, sort_input=[transform_schema.SortByInput(column="a")])),
        ("unique", "add_unique", "a", lambda fid: input_schema.NodeUnique(
            flow_id=fid, node_id=3, unique_input=transform_schema.UniqueInput(columns=["a"]))),
        ("record_id", "add_record_id", "a", lambda fid: input_schema.NodeRecordId(
            flow_id=fid, node_id=3,
            record_id_input=transform_schema.RecordIdInput(group_by=True, group_by_columns=["a"]))),
        ("text_to_rows", "add_text_to_rows", "s", lambda fid: input_schema.NodeTextToRows(
            flow_id=fid, node_id=3,
            text_to_rows_input=transform_schema.TextToRowsInput(column_to_split="s"))),
        ("unpivot", "add_unpivot", "a", lambda fid: input_schema.NodeUnpivot(
            flow_id=fid, node_id=3,
            unpivot_input=transform_schema.UnpivotInput(value_columns=["a"],
                                                        data_type_selector_mode="column"))),
        ("filter", "add_filter", "a", lambda fid: input_schema.NodeFilter(
            flow_id=fid, node_id=3,
            filter_input=transform_schema.FilterInput(
                mode="basic", basic_filter=transform_schema.BasicFilter(field="a", value="1")))),
        ("formula", "add_formula", "a", lambda fid: input_schema.NodeFormula(
            flow_id=fid, node_id=3,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="out"), function="[a] + 1"))),
        # Entry 2 reads entry 1's output; dropping `a` breaks entry 1, so the node fails.
        ("formula", "add_formula", "a", lambda fid: input_schema.NodeFormula(
            flow_id=fid, node_id=3,
            functions=[
                transform_schema.FunctionInput(
                    field=transform_schema.FieldInput(name="out", data_type="Int64"), function="[a] + 1"),
                transform_schema.FunctionInput(
                    field=transform_schema.FieldInput(name="out2"), function="[out] * 2"),
            ])),
    ]


def _node_produces_data(graph: FlowGraph, node_id: int) -> bool:
    """Whether the node runs *and* its output can be materialized.

    A leaf node's LazyFrame is never collected by the run itself, so `run_graph().success`
    alone reports True for a node that fails the moment anyone looks at its data.
    """
    if not graph.run_graph().success:
        return False
    try:
        graph.get_node(node_id).get_resulting_data().collect()
    except Exception:
        return False
    return True


@pytest.mark.parametrize("node_type,add_method,target,settings_factory", _tolerance_cases(),
                         ids=[c[0] for c in _tolerance_cases()])
def test_warning_matches_runtime_behaviour(node_type, add_method, target, settings_factory):
    """The registry's entry criterion, made executable: warn if and only if the node fails.

    Nodes that skip missing columns (select, dynamic_rename) must stay silent; nodes that
    raise must warn. Any new extractor for a forgiving node type gets caught here.
    """
    control = _oracle_graph(node_type, target, drop_target=False)
    getattr(control, add_method)(settings_factory(control.flow_id))
    assert _node_produces_data(control, 3), f"{node_type}: control config is broken on its own"
    assert 3 not in issues_by_node(validate_flow_settings(control))

    graph = _oracle_graph(node_type, target, drop_target=True)
    getattr(graph, add_method)(settings_factory(graph.flow_id))
    warned = 3 in issues_by_node(validate_flow_settings(graph))
    ran_ok = _node_produces_data(graph, 3)
    assert warned == (not ran_ok), (
        f"{node_type}: validator warned={warned} but the node produced data={ran_ok}"
    )


def test_join_key_warning_matches_runtime_behaviour():
    """Join *keys* do break the node — unlike the join's select lists, which are reconciled."""
    graph = create_graph()
    add_manual_input(graph, [{"id": 1, "name": "x"}], node_id=1)
    add_manual_input(graph, [{"id": 1, "value": 2}], node_id=2)
    add_promise(graph, "select", 3)
    connect(graph, 1, 3)
    add_select(graph, 3, [("id", "id2"), ("name", "name")])
    add_promise(graph, "join", 4)
    connect(graph, 3, 4, "main")
    connect(graph, 2, 4, "right")
    join_input = {"join_mapping": [{"left_col": "id", "right_col": "id"}],
                  "left_select": {"renames": []}, "right_select": {"renames": []}, "how": "inner"}
    graph.add_join(input_schema.NodeJoin(flow_id=graph.flow_id, node_id=4, join_input=join_input,
                                         auto_generate_selection=True, verify_integrity=False))

    warned = 4 in issues_by_node(validate_flow_settings(graph))
    ran_ok = _node_produces_data(graph, 4)
    assert warned is True
    assert warned == (not ran_ok)


def _single_input_node_cases():
    return [
        ("sort", "add_sort",
         input_schema.NodeSort(flow_id=1, node_id=2, sort_input=[transform_schema.SortByInput(column="zzz")])),
        ("unique", "add_unique",
         input_schema.NodeUnique(flow_id=1, node_id=2,
                                 unique_input=transform_schema.UniqueInput(columns=["zzz"]))),
        ("text_to_rows", "add_text_to_rows",
         input_schema.NodeTextToRows(flow_id=1, node_id=2,
                                     text_to_rows_input=transform_schema.TextToRowsInput(column_to_split="zzz"))),
        ("record_id", "add_record_id",
         input_schema.NodeRecordId(flow_id=1, node_id=2,
                                   record_id_input=transform_schema.RecordIdInput(group_by=True,
                                                                                  group_by_columns=["zzz"]))),
    ]


@pytest.mark.parametrize("node_type,add_method,settings", _single_input_node_cases(),
                         ids=[c[0] for c in _single_input_node_cases()])
def test_single_input_extractors_flag_missing_column(node_type, add_method, settings):
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, node_type, 2)
    connect(graph, 1, 2)
    getattr(graph, add_method)(settings)

    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    assert issues[2][0].missing_columns == ["zzz"]


def test_record_id_without_grouping_ignores_stale_columns():
    graph = create_graph()
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "record_id", 2)
    connect(graph, 1, 2)
    graph.add_record_id(input_schema.NodeRecordId(
        flow_id=graph.flow_id, node_id=2,
        record_id_input=transform_schema.RecordIdInput(group_by=False, group_by_columns=["zzz"])))
    assert validate_flow_settings(graph).nodes == []


def _get_test_client():
    from fastapi.testclient import TestClient

    from flowfile_core import main

    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def test_endpoint_unknown_flow_returns_404():
    client = _get_test_client()
    response = client.get("/flow/settings_validation", params={"flow_id": 999999})
    assert response.status_code == 404


def test_endpoint_response_shape():
    from flowfile_core import flow_file_handler

    client = _get_test_client()
    flow_id = 9871
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="sv_endpoint", path="."))
    graph = flow_file_handler.get_flow(flow_id)
    add_manual_input(graph, BASE_DATA, node_id=1)
    add_promise(graph, "group_by", 2)
    connect(graph, 1, 2)
    add_group_by(graph, 2, [("gone", "groupby")])
    try:
        response = client.get("/flow/settings_validation", params={"flow_id": flow_id})
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is True
        assert data["nodes"] == [{
            "node_id": 2,
            "issues": [{
                "input_handle": "main",
                "missing_columns": ["gone"],
                "message": "Column(s) not available from the main input: gone",
                "kind": "missing_columns",
            }],
        }]

        graph.flow_settings.validate_settings = False
        response = client.get("/flow/settings_validation", params={"flow_id": flow_id})
        assert response.json() == {"enabled": False, "nodes": []}
    finally:
        flow_file_handler.delete_flow(flow_id)


# SCD2 catalog writer: the extractor's entry criterion, made executable.


def _scd2_writer_graph(namespace_id: int, table_name: str, drop_target: bool, **scd2_kwargs) -> FlowGraph:
    """input(id, val) -> select (optionally renaming `id` away) -> scd2 catalog writer."""
    graph = create_graph()
    add_manual_input(graph, [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}], node_id=1)
    add_promise(graph, "select", 2)
    connect(graph, 1, 2)
    add_select(graph, 2, [("id", "id_gone" if drop_target else "id"), ("val", "val")])
    _add_catalog_writer(
        graph,
        node_id=3,
        depending_on_id=2,
        table_name=table_name,
        namespace_id=namespace_id,
        write_mode="scd2",
        merge_keys=["id"],
        partition_by=scd2_kwargs.pop("partition_by", None),
        scd2=input_schema.Scd2Settings(**scd2_kwargs),
    )
    return graph


@pytest.fixture
def scd2_namespace():
    _catalog_cleanup()
    yield _create_namespace()
    _catalog_cleanup()


def test_scd2_writer_warning_matches_runtime_behaviour(scd2_namespace):
    """A missing business key makes the SCD2 write fail, so it must warn — and the control
    configuration must run clean, proving the warning is attributable."""
    control = _scd2_writer_graph(scd2_namespace, "scd2_control", drop_target=False)
    assert 3 not in issues_by_node(validate_flow_settings(control))
    assert control.run_graph().success

    broken = _scd2_writer_graph(scd2_namespace, "scd2_missing_key", drop_target=True)
    assert 3 in issues_by_node(validate_flow_settings(broken))
    assert not broken.run_graph().success


def test_scd2_named_compare_column_warns_and_fails(scd2_namespace):
    """An explicitly named compare column that is not in the input is a hard error at run
    time (unlike an empty compare set, which resolves to every non-key column)."""
    graph = _scd2_writer_graph(scd2_namespace, "scd2_bad_compare", drop_target=False, compare_columns=["gone"])
    issues = issues_by_node(validate_flow_settings(graph))
    assert issues[3][0].missing_columns == ["gone"]
    assert not graph.run_graph().success


def test_scd2_generated_partition_column_does_not_warn(scd2_namespace):
    """is_current is a legal SCD2 partition column but is generated by the write, not an
    input column — the extractor must not report it as missing."""
    graph = _scd2_writer_graph(scd2_namespace, "scd2_partitioned", drop_target=False, partition_by=["is_current"])
    assert validate_flow_settings(graph).nodes == []
    assert graph.run_graph().success


TEXT_DATE_DATA = [{"d": "2005-01-10", "n": 1}, {"d": "2015-03-14", "n": 2}]


def text_date_graph(node_type: str, add_settings) -> FlowGraph:
    graph = create_graph()
    add_manual_input(graph, TEXT_DATE_DATA, node_id=1)
    add_promise(graph, node_type, 2)
    connect(graph, 1, 2)
    add_settings(graph)
    return graph


def formula_over_text_date(expression: str) -> FlowGraph:
    return text_date_graph("formula", lambda g: g.add_formula(input_schema.NodeFormula(
        flow_id=g.flow_id, node_id=2,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="out"), function=expression))))


def filter_over_text_date(expression: str) -> FlowGraph:
    return text_date_graph("filter", lambda g: g.add_filter(input_schema.NodeFilter(
        flow_id=g.flow_id, node_id=2,
        filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter=expression))))


def add_parquet_output(graph: FlowGraph, directory, node_id: int = 3) -> None:
    """A sink, so the expression is actually collected: a leaf node is never materialized."""
    add_promise(graph, "output", node_id)
    connect(graph, node_id - 1, node_id)
    graph.add_output(input_schema.NodeOutput(
        flow_id=graph.flow_id, node_id=node_id,
        output_settings=input_schema.OutputSettings(
            name="validated.parquet", directory=str(directory), file_type="parquet",
            write_mode="overwrite", table_settings=input_schema.OutputParquetTable())))


def test_formula_date_function_on_text_warns_and_fails(tmp_path):
    graph = formula_over_text_date('format_date([d], "%A, %d %B, %Y")')
    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    (issue,) = issues[2]
    assert issue.kind == "invalid_expression"
    assert issue.input_handle == "main"
    assert issue.message == (
        "Invalid formula: format_date needs a Date or Datetime column; 'd' is text "
        '— wrap it in to_date([d], "%Y-%m-%d")'
    )
    add_parquet_output(graph, tmp_path)
    assert not graph.run_graph().success


def test_formula_date_function_on_a_parsed_date_stays_silent_and_runs(tmp_path):
    graph = formula_over_text_date('format_date(to_date([d], "%Y-%m-%d"), "%A, %d %B, %Y")')
    assert validate_flow_settings(graph).nodes == []
    add_parquet_output(graph, tmp_path)
    assert graph.run_graph().success


def test_filter_date_function_on_text_warns_and_fails(tmp_path):
    graph = filter_over_text_date("year([d]) > 2010")
    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    (issue,) = issues[2]
    assert issue.kind == "invalid_expression"
    assert issue.message.startswith("Invalid filter expression: year needs a Date or Datetime column")
    add_parquet_output(graph, tmp_path)
    assert not graph.run_graph().success


def test_filter_date_function_on_a_parsed_date_stays_silent_and_runs(tmp_path):
    graph = filter_over_text_date('year(to_date([d], "%Y-%m-%d")) > 2010')
    assert validate_flow_settings(graph).nodes == []
    add_parquet_output(graph, tmp_path)
    assert graph.run_graph().success


def _node_result(info, node_id):
    return next((r for r in info.node_step_result if r.node_id == node_id), None)


def test_leaf_formula_dtype_error_fails_the_run_and_blames_the_formula():
    # No sink: the run must still fail, and the formula node owns the error (not a green leaf).
    graph = formula_over_text_date('format_date([d], "%Y")')
    info = graph.run_graph()
    assert info.success is False
    node2 = _node_result(info, 2)
    assert node2.success is False
    assert node2.skipped is False
    assert "format_date needs a Date or Datetime column" in node2.error


def test_leaf_formula_valid_dtype_runs_green():
    graph = formula_over_text_date('format_date(to_date([d], "%Y-%m-%d"), "%Y")')
    assert graph.run_graph().success


def test_formula_dtype_error_blames_the_formula_not_the_downstream_sink(tmp_path):
    graph = formula_over_text_date('format_date([d], "%Y")')
    add_parquet_output(graph, tmp_path)
    info = graph.run_graph()
    assert info.success is False
    node2 = _node_result(info, 2)
    assert node2.success is False
    assert "format_date needs a Date or Datetime column" in node2.error
    # the sink is skipped, never blamed for the formula's error
    node3 = _node_result(info, 3)
    assert node3 is None or node3.success is not True


def test_leaf_filter_dtype_error_fails_the_run_and_blames_the_filter():
    graph = filter_over_text_date("year([d]) > 2010")
    info = graph.run_graph()
    assert info.success is False
    node2 = _node_result(info, 2)
    assert node2.success is False
    assert "year needs a Date or Datetime column" in node2.error


def test_leaf_filter_valid_dtype_runs_green():
    graph = filter_over_text_date('year(to_date([d], "%Y-%m-%d")) > 2010')
    assert graph.run_graph().success


def _formula_input_graph() -> FlowGraph:
    graph = create_graph()
    add_manual_input(graph, TEXT_DATE_DATA, node_id=1)
    add_promise(graph, "formula", 2)
    connect(graph, 1, 2)
    return graph


def test_add_formula_returns_the_dtype_error():
    graph = _formula_input_graph()
    valid, msg = graph.add_formula(input_schema.NodeFormula(
        flow_id=graph.flow_id, node_id=2,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="out"), function='format_date([d], "%Y")')))
    assert valid is False
    assert msg == (
        "format_date needs a Date or Datetime column; 'd' is text "
        '— wrap it in to_date([d], "%Y-%m-%d")'
    )


def test_add_formula_valid_expression_returns_true():
    graph = _formula_input_graph()
    valid, msg = graph.add_formula(input_schema.NodeFormula(
        flow_id=graph.flow_id, node_id=2,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="out"),
            function='format_date(to_date([d], "%Y-%m-%d"), "%Y")')))
    assert (valid, msg) == (True, "")


def test_add_filter_returns_the_dtype_error():
    graph = create_graph()
    add_manual_input(graph, TEXT_DATE_DATA, node_id=1)
    add_promise(graph, "filter", 2)
    connect(graph, 1, 2)
    valid, msg = graph.add_filter(input_schema.NodeFilter(
        flow_id=graph.flow_id, node_id=2,
        filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="year([d]) > 2010")))
    assert valid is False
    assert "year needs a Date or Datetime column" in msg
