"""Tests for the Alteryx -> Flowfile tool mappers and the convert orchestrator."""

from pathlib import Path

import pytest
import yaml
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx import ConversionResult, YxmdParseError, convert_yxmd
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import schemas

FIXTURE_DIR = Path(__file__).parent / "fixtures"
ENVELOPE_KEYS = schemas.FlowfileNode._setting_input_exclude

FIXTURE_OUTPUT_DIR = "C:\\Temp\\alteryx_out"

JOIN_WITH_DISTINCT_KEYS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput">
        <Position x="10" y="10" />
      </GuiSettings>
      <Properties><Configuration>
        <Fields><Field name="left_key" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput">
        <Position x="10" y="110" />
      </GuiSettings>
      <Properties><Configuration>
        <Fields><Field name="right_key" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join">
        <Position x="110" y="60" />
      </GuiSettings>
      <Properties><Configuration>
        <JoinInfo connection="Left"><Field field="left_key" /></JoinInfo>
        <JoinInfo connection="Right"><Field field="right_key" /></JoinInfo>
        <JoinByRecordPos value="False" />
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="Left" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Right" /></Connection>
    <Connection><Origin ToolID="3" Connection="Right" /><Destination ToolID="4" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""

NO_POSITIONS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo locale="0"><Field field="a" order="Desc" /></SortInfo>
      </Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo locale="0"><Field field="a" order="Ascending" /></SortInfo>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def read_fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def convert(name: str) -> ConversionResult:
    return convert_yxmd(read_fixture(name), source_name=name)


def dumped_nodes(result: ConversionResult) -> dict[int, dict]:
    return {node["id"]: node for node in result.flow_data.model_dump(mode="json")["nodes"]}


def node_of_type(nodes: dict[int, dict], node_type: str) -> list[dict]:
    return [node for node in nodes.values() if node["type"] == node_type]


def write_flow(result: ConversionResult, target: Path) -> Path:
    target.write_text(
        yaml.dump(
            result.flow_data.model_dump(mode="json"), default_flow_style=False, sort_keys=False, allow_unicode=True
        ),
        encoding="utf-8",
    )
    return target


@pytest.fixture()
def all_supported() -> ConversionResult:
    return convert("all_supported.yxmd")


@pytest.fixture()
def formulas() -> ConversionResult:
    return convert("formulas.yxmd")


@pytest.fixture()
def containers() -> ConversionResult:
    return convert("containers.yxmd")


@pytest.fixture()
def unsupported() -> ConversionResult:
    return convert("unsupported.yxmd")


# ---------------------------------------------------------------------------
# Parse failures propagate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ["zero_tools.yxmd", "invalid.xml"])
def test_unusable_workflows_raise_parse_error(fixture: str):
    with pytest.raises(YxmdParseError):
        convert(fixture)


def test_workflow_name_falls_back_to_the_source_filename(formulas: ConversionResult):
    assert formulas.flow_data.flowfile_name == "formulas"


def test_workflow_name_comes_from_meta_info(all_supported: ConversionResult):
    assert all_supported.flow_data.flowfile_name == "All Supported Tools"


# ---------------------------------------------------------------------------
# Envelope / serialization contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture", ["all_supported.yxmd", "formulas.yxmd", "containers.yxmd", "unsupported.yxmd"]
)
def test_setting_input_never_carries_envelope_keys(fixture: str):
    for node in dumped_nodes(convert(fixture)).values():
        assert node["setting_input"] is not None
        assert ENVELOPE_KEYS.isdisjoint(node["setting_input"]), node


def test_flow_settings_are_development_local_and_not_auto_saved(all_supported: ConversionResult):
    settings = all_supported.flow_data.flowfile_settings
    assert settings.execution_mode == "Development"
    assert settings.execution_location == "local"
    assert settings.auto_save is False


def test_outputs_and_output_handles_stay_parallel(all_supported: ConversionResult):
    for node in dumped_nodes(all_supported).values():
        assert len(node["outputs"]) == len(node["output_handles"]), node


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------


def test_positions_are_normalised_and_scaled(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    # Tool 1 sits at the workflow minimum (54, 54); tool 2 is 96px to its right.
    assert (nodes[1]["x_position"], nodes[1]["y_position"]) == (60, 100)
    assert (nodes[2]["x_position"], nodes[2]["y_position"]) == (60 + round(96 * 3.0), 100)


def test_chained_formula_nodes_are_offset_from_each_other(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    first, second = nodes[4], nodes[5]
    assert second["x_position"] > first["x_position"]
    assert second["y_position"] > first["y_position"]


def test_workflow_without_positions_gets_a_synthetic_layered_layout():
    nodes = dumped_nodes(convert_yxmd(NO_POSITIONS, source_name="no_positions.yxmd"))
    source = nodes[1]
    downstream = [nodes[2], nodes[3]]
    assert (source["x_position"], source["y_position"]) == (60, 100)
    assert all(node["x_position"] == 360 for node in downstream)
    assert sorted(node["y_position"] for node in downstream) == [100, 300]


# ---------------------------------------------------------------------------
# Per-mapper assertions
# ---------------------------------------------------------------------------


def test_text_input_becomes_columnar_raw_data(all_supported: ConversionResult):
    raw = dumped_nodes(all_supported)[1]["setting_input"]["raw_data_format"]
    assert [column["name"] for column in raw["columns"]] == ["id", "name", "region", "amount", "tags", "notes"]
    # Columnar: data[col_idx][row_idx], never row-major.
    assert raw["data"][0] == [1, 2, 3, 4]
    assert raw["data"][1] == ["alice", "bob", "carol", "dan"]
    # The empty <c /> cell becomes a null rather than shifting the row.
    assert raw["data"][5] == ["vip", "-", None, "check"]


def test_text_input_types_are_inferred_from_the_values(all_supported: ConversionResult):
    raw = dumped_nodes(all_supported)[1]["setting_input"]["raw_data_format"]
    types = {column["name"]: column["data_type"] for column in raw["columns"]}
    assert types["id"] == "Int64"
    assert types["amount"] == "Int64"
    assert types["name"] == "String"


def test_select_maps_renames_drops_and_unknown_checkbox(all_supported: ConversionResult):
    settings = dumped_nodes(all_supported)[2]["setting_input"]
    assert settings["keep_missing"] is False
    by_name = {item["old_name"]: item for item in settings["select_input"]}
    assert by_name["name"]["new_name"] == "customer_name"
    assert by_name["notes"]["keep"] is False
    assert "keep" not in by_name["id"]


def test_select_reports_dropped_type_changes(all_supported: ConversionResult):
    row = next(row for row in all_supported.report.rows if row.alteryx_tool_id == 2)
    assert row.status == "partial"
    assert any("amount" in message for message in row.messages)


def test_select_keeps_unknown_columns_when_alteryx_does(containers: ConversionResult):
    settings = dumped_nodes(containers)[2]["setting_input"]
    assert settings["keep_missing"] is True


def test_filter_splits_when_the_false_anchor_is_wired(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    filter_node = nodes[3]
    assert filter_node["setting_input"]["split_mode"] is True
    assert filter_node["setting_input"]["filter_input"]["mode"] == "advanced"
    assert filter_node["setting_input"]["filter_input"]["advanced_filter"] == "[amount] > 100"
    handles = dict(zip(filter_node["outputs"], filter_node["output_handles"], strict=True))
    union_id = node_of_type(nodes, "union")[0]["id"]
    formula_id = min(node["id"] for node in node_of_type(nodes, "formula"))
    assert handles[formula_id] == "output-0"
    assert handles[union_id] == "output-1"


def test_filter_does_not_split_when_only_the_true_anchor_is_wired(containers: ConversionResult):
    assert dumped_nodes(containers)[3]["setting_input"]["split_mode"] is False


def test_formula_tool_becomes_one_node_per_assignment(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    first, second = nodes[4], nodes[5]
    assert first["setting_input"]["function"]["field"]["name"] == "total"
    assert first["setting_input"]["function"]["function"] == "[amount] * 1.21"
    assert second["setting_input"]["function"]["function"] == "uppercase([customer_name])"
    assert first["input_ids"] == [3]
    assert second["input_ids"] == [first["id"]]
    assert second["outputs"] == [6]


def test_untranslatable_formula_on_a_new_column_keeps_a_typed_null_stub(formulas: ConversionResult):
    node = next(
        node
        for node in dumped_nodes(formulas).values()
        if node["type"] == "formula" and node["setting_input"]["function"]["field"]["name"] == "name_flag"
    )
    body = node["setting_input"]["function"]["function"]
    assert body.startswith("// Alteryx formula could not be converted automatically:")
    assert "// Original: REGEX_Match([name], \"^a.*\")" in body
    assert body.splitlines()[-1] == "nullif(0, 0)"
    assert node["setting_input"]["function"]["field"]["data_type"] == "Boolean"
    assert node["description"].startswith("⚠")
    simple_function_to_expr(body)


def test_untranslatable_formula_on_an_existing_column_keeps_an_identity_stub(formulas: ConversionResult):
    node = next(
        node
        for node in dumped_nodes(formulas).values()
        if node["type"] == "formula" and node["setting_input"]["function"]["field"]["name"] == "amount"
    )
    body = node["setting_input"]["function"]["function"]
    assert "// Original: ToNumber(REGEX_Replace(ToString([amount])" in body
    assert body.splitlines()[-1] == "[amount]"
    simple_function_to_expr(body)


def test_sort_reads_the_long_form_order_values(all_supported: ConversionResult):
    assert dumped_nodes(all_supported)[6]["setting_input"]["sort_input"] == [
        {"column": "amount", "how": "desc"},
        {"column": "id", "how": "asc"},
    ]


def test_summarize_becomes_one_flat_agg_list(all_supported: ConversionResult):
    agg_cols = dumped_nodes(all_supported)[7]["setting_input"]["groupby_input"]["agg_cols"]
    assert [(agg["old_name"], agg["agg"], agg["new_name"]) for agg in agg_cols] == [
        ("region", "groupby", "region"),
        ("tags", "groupby", "tags"),
        ("total", "sum", "Sum_total"),
    ]


def test_sample_first_n(all_supported: ConversionResult):
    settings = dumped_nodes(all_supported)[8]["setting_input"]
    assert settings["sample_method"] == "first"
    assert settings["sample_size"] == 2


def test_unique_keeps_the_first_row_explicitly(all_supported: ConversionResult):
    assert dumped_nodes(all_supported)[9]["setting_input"]["unique_input"] == {
        "columns": ["region"],
        "strategy": "first",
    }


def test_unique_dupes_anchor_gets_a_passthrough_placeholder(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    unique_node = nodes[9]
    placeholder = nodes[10]
    assert placeholder["type"] == "polars_code"
    # Same upstream as the unique node, so the D branch still receives data.
    assert placeholder["input_ids"] == unique_node["input_ids"]
    assert placeholder["setting_input"]["polars_code_input"]["polars_code"].endswith("output_df = input_df")
    row = next(row for row in all_supported.report.rows if row.alteryx_tool_id == 8)
    assert row.status == "partial"
    assert row.flowfile_node_ids == [9, 10]


def test_text_to_columns_splits_to_rows(all_supported: ConversionResult):
    assert dumped_nodes(all_supported)[11]["setting_input"]["text_to_rows_input"] == {
        "column_to_split": "tags",
        "output_column_name": None,
        "split_by_fixed_value": True,
        "split_fixed_value": ",",
        "split_by_column": None,
    }


def test_file_output_splits_the_windows_path(all_supported: ConversionResult):
    settings = dumped_nodes(all_supported)[12]["setting_input"]["output_settings"]
    assert settings["name"] == "result.csv"
    assert settings["directory"] == FIXTURE_OUTPUT_DIR
    assert settings["file_type"] == "csv"
    assert settings["write_mode"] == "overwrite"


def test_union_maps_by_name_to_relaxed(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    union_node = node_of_type(nodes, "union")[0]
    assert union_node["setting_input"]["union_input"]["mode"] == "relaxed"
    # Every inbound wire lands on the main input.
    assert len(union_node["input_ids"]) == 5
    assert union_node["right_input_id"] is None


# ---------------------------------------------------------------------------
# Join fan-out
# ---------------------------------------------------------------------------


def test_join_fans_out_to_inner_and_two_anti_joins(all_supported: ConversionResult):
    nodes = dumped_nodes(all_supported)
    joins = sorted(node_of_type(nodes, "join"), key=lambda node: node["id"])
    assert [node["setting_input"]["join_input"]["how"] for node in joins] == ["inner", "anti", "anti"]

    inner, anti_left, anti_right = joins
    sort_id, right_source_id = 6, 13
    assert inner["input_ids"] == [sort_id] and inner["right_input_id"] == right_source_id
    assert anti_left["input_ids"] == [sort_id] and anti_left["right_input_id"] == right_source_id
    # The unmatched-right output is the same anti join with the sides swapped.
    assert anti_right["input_ids"] == [right_source_id] and anti_right["right_input_id"] == sort_id

    union_id = node_of_type(nodes, "union")[0]["id"]
    assert all(node["outputs"] == [union_id] for node in joins)


def test_anti_right_join_swaps_the_key_mapping():
    nodes = dumped_nodes(convert_yxmd(JOIN_WITH_DISTINCT_KEYS, source_name="join.yxmd"))
    anti_right = node_of_type(nodes, "join")[0]
    assert anti_right["setting_input"]["join_input"]["how"] == "anti"
    assert anti_right["setting_input"]["join_input"]["join_mapping"] == [
        {"left_col": "right_key", "right_col": "left_key"}
    ]
    # Only the R anchor is wired, so no inner join node is emitted.
    assert len(node_of_type(nodes, "join")) == 1


def test_edges_to_unknown_tools_are_dropped_without_failing():
    result = convert_yxmd(JOIN_WITH_DISTINCT_KEYS, source_name="join.yxmd")
    anti_right = node_of_type(dumped_nodes(result), "join")[0]
    assert anti_right["outputs"] == []


# ---------------------------------------------------------------------------
# Placeholders
# ---------------------------------------------------------------------------


def test_unsupported_tool_becomes_a_documented_passthrough(unsupported: ConversionResult):
    nodes = dumped_nodes(unsupported)
    placeholder = nodes[2]
    code = placeholder["setting_input"]["polars_code_input"]["polars_code"]
    assert placeholder["type"] == "polars_code"
    assert code.startswith("# Alteryx tool 'Transpose' (ToolID 2) could not be converted automatically.")
    assert code.endswith("output_df = input_df")
    assert "Transpose" in placeholder["description"]
    assert placeholder["description"].startswith("⚠")


def test_macro_placeholder_is_labelled_by_its_filename(unsupported: ConversionResult):
    row = next(row for row in unsupported.report.rows if row.alteryx_tool_id == 3)
    assert row.alteryx_tool == "Something.yxmc"
    assert row.status == "placeholder"


def test_placeholders_preserve_the_graph_shape(unsupported: ConversionResult):
    nodes = dumped_nodes(unsupported)
    assert nodes[1]["outputs"] == [2]
    assert nodes[2]["outputs"] == [3]
    assert nodes[3]["outputs"] == [4]
    assert nodes[4]["type"] == "output"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        (
            "all_supported.yxmd",
            {"total": 13, "converted": 10, "partial": 3, "commented": 0, "placeholder": 0, "skipped": 0},
        ),
        ("formulas.yxmd", {"total": 2, "converted": 1, "partial": 0, "commented": 1, "placeholder": 0, "skipped": 0}),
        ("containers.yxmd", {"total": 5, "converted": 4, "partial": 0, "commented": 0, "placeholder": 0, "skipped": 1}),
        ("unsupported.yxmd", {"total": 4, "converted": 2, "partial": 0, "commented": 0, "placeholder": 2, "skipped": 0}),
    ],
)
def test_report_counts(fixture: str, expected: dict):
    report = convert(fixture).report
    assert report.total_tools == expected["total"]
    assert len(report.rows) == expected["total"]
    for status in ("converted", "partial", "commented", "placeholder", "skipped"):
        assert getattr(report, status) == expected[status], status


def test_text_box_is_reported_as_skipped(containers: ConversionResult):
    row = next(row for row in containers.report.rows if row.status == "skipped")
    assert row.alteryx_tool_id == 4
    assert row.flowfile_node_ids == []


def test_commented_formula_rows_name_the_field_and_reason(formulas: ConversionResult):
    row = next(row for row in formulas.report.rows if row.alteryx_tool_id == 2)
    assert row.status == "commented"
    assert row.flowfile_node_ids == [2, 3, 4, 5]
    assert any("name_flag" in message for message in row.messages)


# ---------------------------------------------------------------------------
# End-to-end: YAML -> open_flow
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fixture", "expected_types", "expected_edges"),
    [
        (
            "containers.yxmd",
            ["manual_input", "select", "filter", "output"],
            [(1, 2), (2, 3), (3, 4)],
        ),
        (
            "unsupported.yxmd",
            ["manual_input", "polars_code", "polars_code", "output"],
            [(1, 2), (2, 3), (3, 4)],
        ),
        (
            "formulas.yxmd",
            ["manual_input", "formula", "formula", "formula", "formula"],
            [(1, 2), (2, 3), (3, 4), (4, 5)],
        ),
    ],
)
def test_converted_flow_opens_with_the_expected_shape(
    tmp_path: Path, fixture: str, expected_types: list[str], expected_edges: list[tuple[int, int]]
):
    result = convert(fixture)
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert [node.node_type for node in sorted(flow.nodes, key=lambda node: node.node_id)] == expected_types
    assert sorted(flow.node_connections) == expected_edges


def test_all_supported_opens_with_every_edge(tmp_path: Path, all_supported: ConversionResult):
    flow = open_flow(write_flow(all_supported, tmp_path / "flow.yaml"))
    assert sorted(node.node_id for node in flow.nodes) == list(range(1, 18))
    assert sorted(flow.node_connections) == [
        (1, 2),
        (2, 3),
        (3, 4),
        (3, 17),
        (4, 5),
        (5, 6),
        (6, 7),
        (6, 14),
        (6, 15),
        (6, 16),
        (7, 8),
        (8, 9),
        (8, 10),
        (9, 11),
        (10, 17),
        (11, 12),
        (13, 14),
        (13, 15),
        (13, 16),
        (14, 17),
        (15, 17),
        (16, 17),
    ]


def test_descriptions_survive_the_round_trip(tmp_path: Path, all_supported: ConversionResult):
    flow = open_flow(write_flow(all_supported, tmp_path / "flow.yaml"))
    assert flow.get_node(3).setting_input.description == "Keep high-value orders"
    assert flow.get_node(10).setting_input.description.startswith("⚠")


# ---------------------------------------------------------------------------
# End-to-end: hermetic runs
# ---------------------------------------------------------------------------


def test_formula_flow_runs(tmp_path: Path, formulas: ConversionResult):
    """The all-TextInput fixture must actually execute, comments and stubs included."""
    flow = open_flow(write_flow(formulas, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, run_info.node_step_result

    data = flow.get_node(5).get_resulting_data().data_frame.collect()
    assert data["total"].to_list() == [300, 160, 440]
    assert data["name"].to_list() == ["ALICE", "BOB", "CAROL"]
    # The untranslated new column lands as a typed null, the untranslated existing one is untouched.
    assert data["name_flag"].to_list() == [None, None, None]
    assert data["amount"].to_list() == [150, 80, 220]


def test_all_supported_flow_runs_and_writes_its_output(tmp_path: Path):
    raw = (FIXTURE_DIR / "all_supported.yxmd").read_text(encoding="utf-8")
    result = convert_yxmd(
        raw.replace(FIXTURE_OUTPUT_DIR, str(tmp_path)).encode("utf-8"), source_name="all_supported.yxmd"
    )
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]
    assert (tmp_path / "result.csv").exists()
    assert flow.get_node(17).get_resulting_data().data_frame.collect().height > 0
