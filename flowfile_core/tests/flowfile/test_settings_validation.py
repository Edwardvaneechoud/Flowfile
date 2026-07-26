"""Tests for the conservative static settings validation (missing-column warnings)."""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.flowfile.settings_validation import validate_flow_settings
from flowfile_core.schemas import input_schema, schemas, transform_schema


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
        "Invalid formula: arithmetic on string and numeric not allowed, try an explicit cast first"
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


def test_select_only_kept_entries_warn():
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
    issues = issues_by_node(validate_flow_settings(graph))
    assert set(issues) == {2}
    assert issues[2][0].missing_columns == ["gone_keep"]


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
