"""Tests for the Alteryx -> Flowfile tool mappers and the convert orchestrator."""

from pathlib import Path

import polars as pl
import pytest
import yaml
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx import ConversionResult, YxmdParseError, convert_yxmd
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
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


def select_of_type(alteryx_type: str) -> bytes:
    """A one-tool workflow whose Select retypes a single field."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="value" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration>
        <SelectFields><SelectField field="value" selected="True" type="{alteryx_type}" /></SelectFields>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def regex_tokenize(expression: str, num_fields: str = "3", split_to_rows: str = "False") -> bytes:
    """A two-tool workflow whose RegEx tool tokenizes the 'codes' column."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="codes" /></Fields>
        <Data><r><c>AB-12 CD-34</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.RegEx.RegEx" />
      <Properties><Configuration>
        <Field>codes</Field>
        <RegExExpression value="{expression}" />
        <Method>ParseSimple</Method>
        <ParseSimple>
          <SplitToRows value="{split_to_rows}" />
          <RootName>token</RootName>
          <NumFields value="{num_fields}" />
          <ErrorHandling>Warn</ErrorHandling>
        </ParseSimple>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def csv_input(header_row: str) -> bytes:
    """A one-tool workflow reading a CSV, with the HeaderRow option written as given."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties>
        <Configuration>
          <File FileFormat="0">in.csv</File>
          <FormatSpecificOptions>
            <Delimeter>,</Delimeter>
            {header_row}
          </FormatSpecificOptions>
        </Configuration>
        <MetaInfo connection="Output">
          <RecordInfo>
            <Field name="Field_1" type="V_String" />
            <Field name="Field_2" type="V_String" />
          </RecordInfo>
        </MetaInfo>
      </Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
""".encode()


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




@pytest.mark.parametrize("fixture", ["zero_tools.yxmd", "invalid.xml"])
def test_unusable_workflows_raise_parse_error(fixture: str):
    with pytest.raises(YxmdParseError):
        convert(fixture)


def test_workflow_name_falls_back_to_the_source_filename(formulas: ConversionResult):
    assert formulas.flow_data.flowfile_name == "formulas"


def test_workflow_name_comes_from_meta_info(all_supported: ConversionResult):
    assert all_supported.flow_data.flowfile_name == "All Supported Tools"




@pytest.mark.parametrize("fixture", ["all_supported.yxmd", "formulas.yxmd", "containers.yxmd", "unsupported.yxmd"])
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


def test_select_converts_alteryx_data_types(all_supported: ConversionResult):
    row = next(row for row in all_supported.report.rows if row.alteryx_tool_id == 2)
    assert row.status == "converted"
    by_name = {item["old_name"]: item for item in dumped_nodes(all_supported)[2]["setting_input"]["select_input"]}
    assert by_name["amount"]["data_type"] == "Float64"
    # A field Alteryx does not retype must not gain a cast.
    assert "data_type" not in by_name["region"]
    settings = next(node for node in all_supported.flow_data.nodes if node.id == 2).setting_input
    by_old = {item.old_name: item for item in settings.select_input}
    assert by_old["amount"].data_type_change is True
    assert by_old["region"].data_type_change is False


@pytest.mark.parametrize(
    ("alteryx_type", "expected"),
    [
        ("Bool", "Boolean"),
        ("Byte", "Int16"),
        ("Int16", "Int16"),
        ("Int32", "Int32"),
        ("Int64", "Int64"),
        ("Float", "Float32"),
        ("Double", "Float64"),
        ("FixedDecimal", "Float64"),
        ("V_String", "String"),
        ("V_WString", "String"),
        ("Date", "Date"),
        ("DateTime", "Datetime"),
        ("Time", "Time"),
    ],
)
def test_alteryx_widths_map_onto_matching_polars_types(alteryx_type: str, expected: str):
    result = convert_yxmd(select_of_type(alteryx_type), source_name="types.yxmd")
    item = result.flow_data.model_dump(mode="json")["nodes"][1]["setting_input"]["select_input"][0]
    assert item["data_type"] == expected
    assert cast_str_to_polars_type(expected) is not None


def test_unknown_alteryx_type_is_reported_and_left_alone():
    result = convert_yxmd(select_of_type("SpatialObj"), source_name="types.yxmd")
    item = result.flow_data.model_dump(mode="json")["nodes"][1]["setting_input"]["select_input"][0]
    assert "data_type" not in item
    row = next(row for row in result.report.rows if row.alteryx_tool == "AlteryxSelect")
    assert row.status == "partial"
    assert any("SpatialObj" in message for message in row.messages)


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
    assert '// Original: REGEX_Match([name], "^a.*")' in body
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




def test_unsupported_tool_becomes_a_documented_passthrough(unsupported: ConversionResult):
    nodes = dumped_nodes(unsupported)
    placeholder = nodes[2]
    code = placeholder["setting_input"]["polars_code_input"]["polars_code"]
    assert placeholder["type"] == "polars_code"
    assert code.startswith("# Alteryx tool 'DateTime' (ToolID 2) could not be converted automatically.")
    assert code.endswith("output_df = input_df")
    assert "DateTime" in placeholder["description"]
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




@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        (
            "all_supported.yxmd",
            {"total": 13, "converted": 11, "partial": 2, "commented": 0, "placeholder": 0, "skipped": 0},
        ),
        ("formulas.yxmd", {"total": 2, "converted": 1, "partial": 0, "commented": 1, "placeholder": 0, "skipped": 0}),
        ("containers.yxmd", {"total": 5, "converted": 4, "partial": 0, "commented": 0, "placeholder": 0, "skipped": 1}),
        (
            "unsupported.yxmd",
            {"total": 4, "converted": 2, "partial": 0, "commented": 0, "placeholder": 2, "skipped": 0},
        ),
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




@pytest.fixture()
def dynamic_rename() -> ConversionResult:
    return convert("dynamic_rename.yxmd")


@pytest.fixture()
def price_paid() -> ConversionResult:
    return convert("price_paid.yxmd")


@pytest.fixture()
def simple_filter() -> ConversionResult:
    return convert("simple_filter.yxmd")


@pytest.fixture()
def regex_and_multifield() -> ConversionResult:
    return convert("regex_and_multifield.yxmd")


def report_row(result: ConversionResult, tool_id: int):
    return next(row for row in result.report.rows if row.alteryx_tool_id == tool_id)


def test_rename_formula_mode_binds_the_column_name(dynamic_rename: ConversionResult):
    row = report_row(dynamic_rename, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "dynamic_rename"
    settings = dumped_nodes(dynamic_rename)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert settings["rename_mode"] == "formula"
    assert settings["formula"] == "uppercase([column_name])"
    assert settings["selection_mode"] == "all"


def test_rename_first_row_mode_maps_natively(dynamic_rename: ConversionResult):
    row = report_row(dynamic_rename, 3)
    assert row.status == "converted"
    settings = dumped_nodes(dynamic_rename)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert settings["rename_mode"] == "first_row"


def test_rename_prefix_and_suffix_become_two_chained_nodes(dynamic_rename: ConversionResult):
    row = report_row(dynamic_rename, 4)
    assert row.status == "converted"
    assert len(row.flowfile_node_ids) == 2
    nodes = dumped_nodes(dynamic_rename)
    first, second = (nodes[node_id]["setting_input"]["dynamic_rename_input"] for node_id in row.flowfile_node_ids)
    assert (first["rename_mode"], first["prefix"]) == ("prefix", "pre_")
    assert (second["rename_mode"], second["suffix"]) == ("suffix", "_post")
    assert nodes[row.flowfile_node_ids[0]]["outputs"] == [row.flowfile_node_ids[1]]


def test_unsupported_rename_mode_keeps_the_original_configuration(dynamic_rename: ConversionResult):
    row = report_row(dynamic_rename, 5)
    assert row.status == "placeholder"
    code = dumped_nodes(dynamic_rename)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "RemovePrefixSuffix" in code
    assert "<RenameMode>" in code


def test_rename_from_right_input_rows_resolves_to_a_select(price_paid: ConversionResult):
    row = report_row(price_paid, 4)
    assert row.status == "partial"
    assert row.flowfile_node_type == "select"
    renames = dumped_nodes(price_paid)[row.flowfile_node_ids[0]]["setting_input"]["select_input"]
    assert renames[0] == {"old_name": "Field_1", "new_name": "Transaction unique identifier"}
    assert len(renames) == 16


def test_the_name_source_input_is_not_wired_and_is_not_reported_as_a_dropped_edge(price_paid: ConversionResult):
    text_input = next(node for node in price_paid.flow_data.nodes if node.type == "manual_input")
    assert text_input.outputs == []
    assert not any("dropped" in message for row in price_paid.report.rows for message in row.messages)




def test_headerless_read_renames_polars_columns_to_the_alteryx_names(price_paid: ConversionResult):
    row = report_row(price_paid, 1)
    assert row.status == "converted"
    assert len(row.flowfile_node_ids) == 2
    assert any("column_1" in message for message in row.messages)
    nodes = dumped_nodes(price_paid)
    read_id, rename_id = row.flowfile_node_ids
    assert nodes[read_id]["type"] == "read"
    assert nodes[read_id]["outputs"] == [rename_id]
    renames = nodes[rename_id]["setting_input"]["select_input"]
    assert renames[0] == {"old_name": "column_1", "new_name": "Field_1"}
    assert len(renames) == 16


def read_settings(header_row: str) -> tuple:
    result = convert_yxmd(csv_input(header_row), source_name="csv_input.yxmd")
    row = report_row(result, 1)
    assert row.status == "converted", row.messages
    nodes = dumped_nodes(result)
    return row, nodes[row.flowfile_node_ids[0]]["setting_input"]["received_file"]["table_settings"]


@pytest.mark.parametrize("header_row", ["<HeaderRow>True</HeaderRow>", '<HeaderRow value="True" />'])
def test_headered_csv_input_is_read_with_headers_in_both_xml_shapes(header_row: str):
    """Input Data writes the flag as element text; the attribute shape has to keep working too."""
    row, table_settings = read_settings(header_row)
    assert table_settings["has_headers"] is True
    assert len(row.flowfile_node_ids) == 1
    assert not any("column_1" in message for message in row.messages)


@pytest.mark.parametrize("header_row", ["<HeaderRow>False</HeaderRow>", '<HeaderRow value="False" />'])
def test_headerless_csv_input_still_gets_the_positional_rename_in_both_xml_shapes(header_row: str):
    row, table_settings = read_settings(header_row)
    assert table_settings["has_headers"] is False
    assert len(row.flowfile_node_ids) == 2
    assert any("column_1" in message for message in row.messages)


def test_an_unwritten_header_option_leaves_the_reader_default_alone():
    _, table_settings = read_settings("")
    assert table_settings["has_headers"] is True




@pytest.mark.parametrize(
    ("tool_id", "expected"),
    [
        (2, '[grade] = "A"'),
        (3, "[score] >= 15"),
        (4, 'not((is_empty([grade]) or [grade] = ""))'),
    ],
)
def test_simple_filters_are_rebuilt_into_expressions(simple_filter: ConversionResult, tool_id: int, expected: str):
    row = report_row(simple_filter, tool_id)
    assert row.status == "converted", row.messages
    settings = dumped_nodes(simple_filter)[row.flowfile_node_ids[0]]["setting_input"]
    assert settings["filter_input"]["advanced_filter"] == expected


def test_unsupported_simple_filter_operator_keeps_the_original_configuration(simple_filter: ConversionResult):
    row = report_row(simple_filter, 5)
    assert row.status == "placeholder"
    assert any("IsBetween" in message for message in row.messages)
    code = dumped_nodes(simple_filter)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "<Operator>IsBetween</Operator>" in code




def test_multi_field_formula_becomes_one_formula_per_selected_field(regex_and_multifield: ConversionResult):
    row = report_row(regex_and_multifield, 3)
    assert row.status == "converted"
    assert len(row.flowfile_node_ids) == 2
    nodes = dumped_nodes(regex_and_multifield)
    functions = [nodes[node_id]["setting_input"]["function"] for node_id in row.flowfile_node_ids]
    assert [function["field"]["name"] for function in functions] == ["flag_a", "flag_b"]
    assert functions[0]["function"] == '[flag_a] = "Y"'
    assert functions[1]["function"] == '[flag_b] = "Y"'


def test_regex_parse_becomes_runnable_polars_code(regex_and_multifield: ConversionResult):
    row = report_row(regex_and_multifield, 2)
    assert row.status == "partial"
    assert row.flowfile_node_type == "polars_code"
    code = dumped_nodes(regex_and_multifield)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"][
        "polars_code"
    ]
    assert "_pattern = '([A-Z]{2})-([0-9]{2})'" in code
    frame = polars_code_parser.get_executable(code, num_inputs=1)(pl.DataFrame({"code": ["AB-12", "nope"]}))
    assert frame.to_dicts() == [
        {"code": "AB-12", "letters": "AB", "digits": "12"},
        {"code": "nope", "letters": None, "digits": None},
    ]


def test_regex_lookahead_is_rejected_instead_of_generating_failing_code(regex_and_multifield: ConversionResult):
    row = report_row(regex_and_multifield, 4)
    assert row.status == "placeholder"
    assert any("lookahead" in message for message in row.messages)


def tokenize_code(source: bytes) -> str:
    result = convert_yxmd(source, source_name="tokenize.yxmd")
    row = report_row(result, 2)
    assert row.status == "partial", row.messages
    return dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]


def test_regex_tokenize_spreads_every_match_across_the_output_columns():
    """Tokenize is not capture-group extraction: each match of the expression is one column."""
    code = tokenize_code(regex_tokenize("[A-Z]{2}-[0-9]{2}"))
    assert "str.extract_all(_pattern)" in code
    assert "str.extract(_pattern, 1)" not in code
    frame = polars_code_parser.get_executable(code, num_inputs=1)(pl.DataFrame({"codes": ["AB-12 CD-34", "nope"]}))
    assert frame.to_dicts() == [
        {"codes": "AB-12 CD-34", "token1": "AB-12", "token2": "CD-34", "token3": None},
        {"codes": "nope", "token1": None, "token2": None, "token3": None},
    ]


def test_regex_tokenize_returns_the_marked_group_of_each_match():
    code = tokenize_code(regex_tokenize("([A-Z]{2})-[0-9]{2}", num_fields="2"))
    frame = polars_code_parser.get_executable(code, num_inputs=1)(pl.DataFrame({"codes": ["AB-12 CD-34", "nope"]}))
    assert frame.to_dicts() == [
        {"codes": "AB-12 CD-34", "token1": "AB", "token2": "CD"},
        {"codes": "nope", "token1": None, "token2": None},
    ]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (regex_tokenize("([A-Z]{2})-([0-9]{2})"), "more than one group"),
        (regex_tokenize("[A-Z]{2}", split_to_rows="True"), "split to rows"),
    ],
)
def test_untranslatable_tokenize_configurations_stay_placeholders(source: bytes, expected: str):
    row = report_row(convert_yxmd(source, source_name="tokenize.yxmd"), 2)
    assert row.status == "placeholder"
    assert any(expected in message for message in row.messages)


def test_every_generated_polars_code_node_is_executable(price_paid: ConversionResult):
    for node in price_paid.flow_data.nodes:
        if node.type != "polars_code":
            continue
        polars_code_parser.get_executable(
            node.setting_input.polars_code_input.polars_code, num_inputs=max(1, len(node.input_ids))
        )




def test_placeholders_embed_the_original_configuration_and_annotation(price_paid: ConversionResult):
    row = report_row(price_paid, 8)
    assert row.status == "placeholder"
    code = dumped_nodes(price_paid)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "# Alteryx annotation: 2016PPData.yxdb" in code
    assert "2016PPData.yxdb</File>" in code
    assert code.splitlines()[-1] == "output_df = input_df"


def test_year_function_now_converts(price_paid: ConversionResult):
    row = report_row(price_paid, 6)
    assert row.status == "converted"
    settings = dumped_nodes(price_paid)[row.flowfile_node_ids[0]]["setting_input"]
    assert settings["filter_input"]["advanced_filter"] == "year([Date of Transfer]) = 2016"


def test_price_paid_workflow_converts_without_placeholders_beyond_the_yxdb_writer(price_paid: ConversionResult):
    placeholders = [row for row in price_paid.report.rows if row.status == "placeholder"]
    assert [row.alteryx_tool for row in placeholders] == ["DbFileOutput"]


PRICE_PAID_ROWS = [
    (
        "{A1}",
        "250000",
        "2016-05-04 00:00",
        "SW1A 1AA",
        "F",
        "Y",
        "L",
        "10",
        "",
        "DOWNING ST",
        "",
        "LONDON",
        "WESTMINSTER",
        "GREATER LONDON",
        "A",
        "A",
    ),
    (
        "{A2}",
        "180000",
        "2015-07-19 00:00",
        "M1 1AE",
        "T",
        "N",
        "F",
        "12",
        "",
        "HIGH ST",
        "",
        "MANCHESTER",
        "MANCHESTER",
        "GREATER MANCHESTER",
        "A",
        "A",
    ),
    (
        "{A3}",
        "999000",
        "2016-11-30 00:00",
        "B1 2JQ",
        "D",
        "N",
        "F",
        "5",
        "",
        "BROAD ST",
        "",
        "BIRMINGHAM",
        "BIRMINGHAM",
        "WEST MIDLANDS",
        "B",
        "A",
    ),
    (
        "{A4}",
        "310000",
        "2016-02-01 00:00",
        "LS1 4AP",
        "S",
        "Y",
        "L",
        "7",
        "A",
        "PARK ROW",
        "",
        "LEEDS",
        "LEEDS",
        "WEST YORKSHIRE",
        "A",
        "A",
    ),
]


def test_price_paid_workflow_runs_and_reproduces_the_alteryx_result(tmp_path: Path, price_paid: ConversionResult):
    """The real-world fixture must run end to end and select the rows Alteryx would select."""
    source = tmp_path / "pp-complete.csv"
    source.write_text("\n".join(",".join(f'"{cell}"' for cell in row) for row in PRICE_PAID_ROWS), encoding="utf-8")
    for node in price_paid.flow_data.nodes:
        if node.type == "read":
            received = node.setting_input.received_file
            received.path = received.abs_file_path = str(source)
            received.directory, received.name = str(tmp_path), source.name

    flow = open_flow(write_flow(price_paid, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]

    terminal = next(node for node in price_paid.flow_data.nodes if node.type == "polars_code" and not node.outputs)
    frame = flow.get_node(terminal.id).get_resulting_data().data_frame.collect()
    # Year([Date of Transfer]) = 2016 AND [PPDCategory Type] = "A"
    assert frame["Transaction unique identifier"].to_list() == ["{A1}", "{A4}"]
    assert frame.schema["Price"] == pl.Int32
    assert frame.schema["Date of Transfer"] == pl.Date
    assert frame.schema["NewBuild"] == pl.Boolean
    assert frame["PostCodeArea"].to_list() == ["SW", "LS"]
    assert "Record Status - monthly file only" not in frame.columns




@pytest.fixture()
def extra_tools() -> ConversionResult:
    return convert("extra_tools.yxmd")


def tool_after_text_input(plugin: str, config: str) -> bytes:
    """A two-tool workflow: a Text Input feeding one tool with the given configuration."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="value" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="{plugin}" />
      <Properties><Configuration>{config}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def macro_after_text_input(macro: str, config: str) -> bytes:
    """A two-tool workflow: a Text Input feeding one macro tool with the given configuration."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="value" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings />
      <Properties><Configuration>{config}</Configuration></Properties>
      <EngineSettings Macro="{macro}" />
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input2" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def cleanse_config(overrides: dict[str, str | None] | None = None) -> str:
    """The Cleanse macro's question values with factory defaults; None removes an entry."""
    values = {
        "Check Box (135)": "False",
        "Check Box (136)": "False",
        "List Box (11)": '"value"',
        "Check Box (84)": "True",
        "Check Box (117)": "True",
        "Check Box (15)": "True",
        "Check Box (109)": "False",
        "Check Box (122)": "False",
        "Check Box (53)": "False",
        "Check Box (58)": "False",
        "Check Box (70)": "False",
        "Check Box (77)": "False",
        "Drop Down (81)": "upper",
    }
    values.update(overrides or {})
    return "".join(f'<Value name="{name}">{text}</Value>' for name, text in values.items() if text is not None)


def test_record_id_maps_to_a_record_id_node(extra_tools: ConversionResult):
    row = report_row(extra_tools, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "record_id"
    settings = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]["setting_input"]["record_id_input"]
    assert settings["output_column_name"] == "RowNr"
    assert settings["offset"] == 1


def test_running_total_maps_to_window_functions(extra_tools: ConversionResult):
    row = report_row(extra_tools, 3)
    assert row.status == "converted"
    assert row.flowfile_node_type == "window_functions"
    window = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]["setting_input"]["window_input"]
    assert window["partition_by"] == ["region"]
    assert [(w["column"], w["function"], w["new_column_name"]) for w in window["window_functions"]] == [
        ("sales", "cum_sum", "RunTot_sales")
    ]


def test_transpose_becomes_unpivot_plus_rename(extra_tools: ConversionResult):
    row = report_row(extra_tools, 4)
    assert row.status == "converted"
    assert row.flowfile_node_type == "unpivot"
    unpivot, rename = (dumped_nodes(extra_tools)[node_id] for node_id in row.flowfile_node_ids)
    assert unpivot["setting_input"]["unpivot_input"]["index_columns"] == ["region", "product"]
    assert unpivot["setting_input"]["unpivot_input"]["value_columns"] == ["q1", "q2"]
    renames = {s["old_name"]: s["new_name"] for s in rename["setting_input"]["select_input"]}
    assert renames == {"variable": "Name", "value": "Value"}
    assert rename["input_ids"] == [unpivot["id"]]


def test_cross_tab_maps_to_pivot(extra_tools: ConversionResult):
    row = report_row(extra_tools, 5)
    assert row.status == "partial"
    assert any("underscores" in message for message in row.messages)
    pivot = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]["setting_input"]["pivot_input"]
    assert pivot["index_columns"] == ["region"]
    assert pivot["pivot_column"] == "product"
    assert pivot["value_col"] == "sales"
    assert pivot["aggregations"] == ["sum"]


def test_append_fields_maps_to_cross_join(extra_tools: ConversionResult):
    row = report_row(extra_tools, 7)
    assert row.status == "converted"
    node = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]
    assert node["type"] == "cross_join"
    assert node["input_ids"] == [1]
    assert node["right_input_id"] == 7


def test_data_cleansing_maps_the_cleanse_macro(extra_tools: ConversionResult):
    row = report_row(extra_tools, 8)
    assert row.status == "converted"
    assert row.flowfile_node_type == "data_cleansing"
    cleansing = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert cleansing["selection_mode"] == "list"
    assert cleansing["selected_columns"] == ["region", "product"]
    assert cleansing["case_mode"] == "uppercase"
    enabled = {flag for flag in cleansing if cleansing[flag] is True}
    assert enabled == {"replace_nulls_with_blank", "replace_nulls_with_zero", "trim_whitespace"}


def test_data_cleansing_ignores_the_case_dropdown_when_case_is_disabled():
    config = cleanse_config({"Check Box (77)": "False", "Drop Down (81)": "upper"})
    result = convert_yxmd(macro_after_text_input("Cleanse.yxmc", config), source_name="inline.yxmd")
    row = report_row(result, 2)
    assert row.status == "converted"
    cleansing = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert cleansing["case_mode"] == "none"


def test_data_cleansing_with_an_empty_field_list_cleanses_no_columns():
    result = convert_yxmd(
        macro_after_text_input("Cleanse.yxmc", cleanse_config({"List Box (11)": ""})), source_name="inline.yxmd"
    )
    row = report_row(result, 2)
    assert row.status == "converted"
    cleansing = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert cleansing["selection_mode"] == "list"
    assert cleansing["selected_columns"] == []


def test_data_cleansing_matches_the_macro_by_basename():
    result = convert_yxmd(
        macro_after_text_input("Macros\\Cleanse.yxmc", cleanse_config()), source_name="inline.yxmd"
    )
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "data_cleansing"


@pytest.mark.parametrize(
    ("case_id", "overrides"),
    [
        ("missing-question", {"Check Box (15)": None}),
        ("unrecognized-question", {"Check Box (999)": "True"}),
        ("unknown-case-mode", {"Check Box (77)": "True", "Drop Down (81)": "sentence"}),
        ("unquoted-field-list", {"List Box (11)": "value"}),
        ("dynamic-unknown-fields", {"List Box (11)": '"value","*Unknown"'}),
    ],
)
def test_data_cleansing_fails_closed_to_a_placeholder(case_id: str, overrides: dict):
    document = macro_after_text_input("Cleanse.yxmc", cleanse_config(overrides))
    row = report_row(convert_yxmd(document, source_name="inline.yxmd"), 2)
    assert row.status == "placeholder", case_id
    assert row.flowfile_node_type == "polars_code"
    assert row.messages


@pytest.mark.parametrize(
    ("case_id", "plugin", "config"),
    [
        (
            "record-id-string-type",
            "AlteryxBasePluginsGui.RecordID.RecordID",
            "<FieldName>RecordID</FieldName><StartValue>1</StartValue><FieldType>String</FieldType>",
        ),
        (
            "transpose-unknown-selected",
            "AlteryxBasePluginsGui.Transpose.Transpose",
            '<KeyFields /><DataFields><Field field="*Unknown" selected="True" /></DataFields>',
        ),
        (
            "cross-tab-unmapped-method",
            "AlteryxBasePluginsGui.CrossTab.CrossTab",
            '<GroupFields /><HeaderField field="value" /><DataField field="value" />'
            "<Methods><Method method=\"CountNonNull\" /></Methods>",
        ),
        (
            "running-total-no-fields",
            "AlteryxSpatialPluginsGui.RunningTotal.RunningTotal",
            "<GroupByFields /><RunningTotalFields />",
        ),
    ],
)
def test_new_tools_fail_closed_to_placeholders(case_id: str, plugin: str, config: str):
    result = convert_yxmd(tool_after_text_input(plugin, config), source_name="inline.yxmd")
    row = report_row(result, 2)
    assert row.status == "placeholder", case_id
    assert row.flowfile_node_type == "polars_code"


def test_extra_tools_flow_runs(tmp_path: Path, extra_tools: ConversionResult):
    flow = open_flow(write_flow(extra_tools, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]

    record_id = flow.get_node(2).get_resulting_data().data_frame.collect()
    assert record_id["RowNr"].to_list() == [1, 2, 3]

    running = flow.get_node(3).get_resulting_data().data_frame.collect()
    assert running["RunTot_sales"].to_list() == [10, 30, 30]

    transposed = flow.get_node(5).get_resulting_data().data_frame.collect()
    assert set(transposed.columns) == {"region", "product", "Name", "Value"}
    assert transposed.height == 6

    pivoted = flow.get_node(6).get_resulting_data().data_frame.collect().sort("region")
    assert pivoted["apples"].to_list() == [10, 30]
    assert pivoted["pears"].to_list() == [20, 0]

    appended = flow.get_node(8).get_resulting_data().data_frame.collect()
    assert appended.height == 3
    assert appended["tax"].to_list() == [0.2, 0.2, 0.2]

    cleansed_id = report_row(extra_tools, 8).flowfile_node_ids[0]
    cleansed = flow.get_node(cleansed_id).get_resulting_data().data_frame.collect()
    assert cleansed["region"].to_list() == ["NORTH", "NORTH", "SOUTH"]
    assert cleansed["product"].to_list() == ["APPLES", "PEARS", "APPLES"]
