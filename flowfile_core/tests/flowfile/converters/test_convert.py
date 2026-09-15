"""Tests for the Alteryx -> Flowfile tool mappers and the convert orchestrator."""

import ast
from pathlib import Path

import polars as pl
import pytest
import yaml
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx import ConversionResult, YxmdParseError, convert_yxmd
from flowfile_core.flowfile.converters.alteryx.convert import emit_tools
from flowfile_core.flowfile.converters.alteryx.scope import BUCKETS
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import parse_yxmd
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import schemas

FIXTURE_DIR = Path(__file__).parent / "fixtures"
ENVELOPE_KEYS = schemas.FlowfileNode._setting_input_exclude

# Alteryx's own sample workflows are proprietary, so none of them is committed here. A test that
# needs a real one reads it from the private corpus beside the repo and skips where that is absent.
LEARNING = Path(__file__).resolve().parents[5]
CORPUS_MULTI_FIELD_FORMULA = LEARNING / "alteryx_nodes" / "02 Preparation" / "Multi-Field_Formula.yxmd"
needs_corpus = pytest.mark.skipif(
    not CORPUS_MULTI_FIELD_FORMULA.exists(),
    reason=f"Alteryx corpus workflow not present ({CORPUS_MULTI_FIELD_FORMULA})",
)

# Every .yxmd in the folder converts except the ones named here, so a new fixture joins the
# corpus-wide guards by existing rather than by being remembered.
UNCONVERTIBLE_FIXTURES = frozenset({"empty_canvas.yxmd"})
CONVERTIBLE_FIXTURES = sorted(
    path.name for path in FIXTURE_DIR.glob("*.yxmd") if path.name not in UNCONVERTIBLE_FIXTURES
)

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


TEXT_BOXES = b"""<?xml version="1.0"?>
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
      <GuiSettings Plugin="AlteryxGuiToolkit.TextBox.TextBox">
        <Position x="54" y="54" width="804" height="72" />
      </GuiSettings>
      <Properties><Configuration>
        <Text />
        <FillColor r="13" g="35" b="69" />
      </Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxGuiToolkit.TextBox.TextBox" />
      <Properties><Configuration>
        <Text><![CDATA[ First line
  second line  ]]></Text>
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


DROPPED_CONNECTION = b"""<?xml version="1.0"?>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties><Configuration>
        <File FileFormat="0">in.csv</File>
        <FormatSpecificOptions><Delimeter>,</Delimeter></FormatSpecificOptions>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


LLM_WITH_CREDENTIALS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="LLMOverride_1_0" />
      <Properties><Configuration>
        <inputUrl>https://ayx-sandbox.bender.rocks/aims/</inputUrl>
        <validCredentials>true</validCredentials>
        <Secrets />
        <Connection DcmType="ConnectionId">7a9a50b6-a4bd-4841-b4c9-3bfd051752d2</Connection>
        <authServerDetails>
          <client_id>aa7d8c1a-8001-49d5-86a0-f6a5322f5d46</client_id>
          <token_endpoint>/as/token</token_endpoint>
          <url>https://pingauth-sandbox.alteryxcloud.com</url>
        </authServerDetails>
        <llmConnectionId>01JB01XTJVFQCTCMMS9X3F3HX1</llmConnectionId>
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""

ODBC_INPUT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties><Configuration>
        <Passwords><Password>Zm9vYmFy</Password></Passwords>
        <File>odbc:DRIVER={ODBC Driver 18};SERVER=corp.database.windows.net;UID=reports;PWD=hunter2|||orders</File>
        <FormatSpecificOptions />
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


YXDB_INPUT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties>
        <Configuration>
          <Passwords />
          <File OutputFileName="" FileFormat="19" SearchSubDirs="False"
                RecordLimit="">..\\..\\..\\data\\OneToolData\\CustomerFile1.yxdb</File>
          <FormatSpecificOptions />
        </Configuration>
        <MetaInfo connection="Output">
          <RecordInfo>
            <Field name="CustomerID" type="Int32" />
            <Field name="Spend" type="Double" />
          </RecordInfo>
        </MetaInfo>
      </Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo locale="0"><Field field="Spend" order="Desc" /></SortInfo>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def db_file_output(file_element: str, extra: str = "") -> bytes:
    """A one-tool workflow whose Output Data tool writes the given File element."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Region" /></Fields>
        <Data><r><c>North</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput" />
      <Properties><Configuration>
        {file_element}
        <Passwords />
        <FormatSpecificOptions />
        {extra}
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


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


@pytest.fixture()
def out_of_scope() -> ConversionResult:
    return convert("out_of_scope.yxmd")


@pytest.mark.parametrize("fixture", [*sorted(UNCONVERTIBLE_FIXTURES), "invalid.xml"])
def test_unusable_workflows_raise_parse_error(fixture: str):
    with pytest.raises(YxmdParseError):
        convert(fixture)


def test_the_fixture_guards_cover_every_workflow_in_the_folder():
    assert UNCONVERTIBLE_FIXTURES <= {path.name for path in FIXTURE_DIR.glob("*.yxmd")}
    assert len(CONVERTIBLE_FIXTURES) >= 16


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


# (tool id, status, flowfile node type, a fragment the row's message or code must contain).
# (tool id, status, node type, a fragment the row's message or emitted settings must contain).
TEXT_TO_COLUMNS_CASES = [
    (812, "converted", "polars_code", "Blend1, Blend2, Blend3"),
    (813, "converted", "polars_code", "Portion1, Portion2, Portion3"),
    (814, "placeholder", "polars_code", "Flags=8"),
    (815, "converted", "text_to_rows", "'split_fixed_value': '\\t'"),
    (816, "converted", "polars_code", "to rows on any of"),
    (817, "placeholder", "polars_code", "Flags=24"),
    (818, "converted", "polars_code", "Portion31, Portion32"),
]


@pytest.fixture()
def text_to_columns() -> ConversionResult:
    return convert("text_to_columns.yxmd")


@pytest.mark.parametrize(("tool_id", "status", "node_type", "fragment"), TEXT_TO_COLUMNS_CASES)
def test_text_to_columns_reads_numfields_as_the_mode(
    text_to_columns: ConversionResult, tool_id: int, status: str, node_type: str, fragment: str
):
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == tool_id)
    assert (row.status, row.flowfile_node_type) == (status, node_type)
    node = dumped_nodes(text_to_columns)[row.flowfile_node_ids[0]]
    haystack = " ".join([*row.messages, str(node["setting_input"])])
    assert fragment in haystack


def test_text_to_columns_flags_message_does_not_claim_which_bit_is_which(text_to_columns: ConversionResult):
    """Which bit means what is an open Designer question, so the refusal must not assert one."""
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == 814)
    message = row.messages[0].lower()
    assert "not supported" in message
    assert not any(claim in message for claim in ("quote", "bracket", "empty field"))


def test_text_to_columns_keeps_the_split_columns_in_place(text_to_columns: ConversionResult):
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == 812)
    code = dumped_nodes(text_to_columns)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    frame = pl.LazyFrame({"BatchNo": [7], "Blend": ["oat;barley;rye"], "Mill": ["Vetle"]})
    result = polars_code_parser.get_executable(code, 1)(frame).collect()
    assert result.columns == ["BatchNo", "Blend1", "Blend2", "Blend3", "Mill"]
    assert result.row(0) == (7, "oat", "barley", "rye", "Vetle")


def test_text_to_columns_escapes_name_one_character_each(text_to_columns: ConversionResult):
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == 816)
    code = dumped_nodes(text_to_columns)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    result = polars_code_parser.get_executable(code, 1)(pl.LazyFrame({"Portion3": ["a~b c{d}e"]})).collect()
    assert result["Portion3"].to_list() == ["a", "b", "c", "d", "e"]


def test_text_to_columns_falls_back_to_the_field_name_when_the_root_name_is_empty(
    text_to_columns: ConversionResult,
):
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == 818)
    assert "Portion31, Portion32, Portion33, Portion34" in row.messages[0]


TEXT_TO_ROWS_WITH_ROOT_NAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextToColumns.TextToColumns" />
      <Properties><Configuration>
        <Field>Blend</Field>
        <RootName>Leftover</RootName>
        <Delimeters value="%s" />
        <NumFields value="1" />
        <Flags value="0" />
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


@pytest.mark.parametrize(("delimiters", "node_type"), [(b";", "text_to_rows"), (b";~", "polars_code")])
def test_split_to_rows_keeps_the_source_column_name_in_both_branches(delimiters: bytes, node_type: str):
    """Alteryx's own help says split-to-rows leaves the output columns as the input columns.

    The RootName differs from the field on purpose: a branch that applied it would name the
    output 'Leftover' and fail here.
    """
    result = convert_yxmd(TEXT_TO_ROWS_WITH_ROOT_NAME % delimiters, source_name="rows.yxmd")
    row = result.report.rows[0]
    assert (row.status, row.flowfile_node_type) == ("converted", node_type)
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]
    if node_type == "text_to_rows":
        # None makes the engine write the split values back into the column it split.
        assert settings["text_to_rows_input"]["column_to_split"] == "Blend"
        assert settings["text_to_rows_input"]["output_column_name"] is None
    else:
        code_line = settings["polars_code_input"]["polars_code"].splitlines()[-1]
        assert code_line.endswith(".explode('Blend')") and "Leftover" not in code_line
    assert any("'Leftover'" in message and "not used" in message for message in row.messages)


def test_split_to_rows_reports_a_root_name_it_did_not_apply(text_to_columns: ConversionResult):
    # Tool 816 carries RootName "Leftover"; rows mode never renames, so it must be reported.
    row = next(row for row in text_to_columns.report.rows if row.alteryx_tool_id == 816)
    assert any("Leftover" in message and "not used" in message for message in row.messages)


TEXT_TO_COLUMNS_DROP_EXTRA = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextToColumns.TextToColumns" />
      <Properties><Configuration>
        <Field>Address</Field>
        <ErrorHandling>Drop</ErrorHandling>
        <RootName>Address</RootName>
        <Delimeters value="," />
        <NumFields value="2" />
        <Flags value="0" />
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


def test_text_to_columns_refuses_extra_character_handling_it_cannot_express():
    result = convert_yxmd(TEXT_TO_COLUMNS_DROP_EXTRA, source_name="drop_extra.yxmd")
    row = result.report.rows[0]
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "'Drop'" in row.messages[0]


# The three `|||` suffixes the corpus carries (01 In Out/Input_Data.yxmd tools 80, 78 and 137).
EXCEL_INPUT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties><Configuration><Passwords />
        <File RecordLimit="" SearchSubDirs="False" FileFormat="0">C:\\data\\Book.xlsx|||%s</File>
      </Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


@pytest.mark.parametrize(
    ("suffix", "status", "sheet"),
    [
        (b"`Output$`", "converted", "Output"),
        (b"`Sheet 1$`", "converted", "Sheet 1"),
        (b"`NamedRangeExample`", "placeholder", None),
        (b"&lt;List of Sheet Names&gt;", "placeholder", None),
    ],
)
def test_excel_sheet_is_read_without_its_backticks_and_a_named_range_is_refused(
    suffix: bytes, status: str, sheet: str | None
):
    result = convert_yxmd(EXCEL_INPUT % suffix, source_name="excel.yxmd")
    row = result.report.rows[0]
    assert row.status == status
    if sheet is None:
        return
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["received_file"]
    assert settings["table_settings"]["sheet_name"] == sheet


EXCEL_OUTPUT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput" />
      <Properties><Configuration><Passwords />
        <File MaxRecords="" FileFormat="0">C:\\out\\Book.xlsx|||%s</File>
      </Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


@pytest.mark.parametrize(
    ("suffix", "status", "sheet"),
    [
        (b"`Results$`", "converted", "Results"),
        (b"Results$", "converted", "Results"),
        (b"`NamedRangeExample`", "partial", None),
        (b"&lt;List of Sheet Names&gt;", "partial", None),
    ],
)
def test_excel_output_reads_the_sheet_suffix_the_same_way_the_input_does(suffix: bytes, status: str, sheet: str | None):
    result = convert_yxmd(EXCEL_OUTPUT % suffix, source_name="excel_out.yxmd")
    row = result.report.rows[0]
    assert row.status == status
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["output_settings"]
    # The raw `|||` suffix must never reach the sheet name, backticks and trailing `$` included.
    assert settings["table_settings"]["sheet_name"] == (sheet or "Sheet1")
    if sheet is None:
        assert "default sheet" in row.messages[0]


def test_the_named_range_message_names_the_reader_and_the_writer():
    reader = convert_yxmd(EXCEL_INPUT % b"`NamedRangeExample`", source_name="excel.yxmd").report.rows[0]
    writer = convert_yxmd(EXCEL_OUTPUT % b"`NamedRangeExample`", source_name="excel_out.yxmd").report.rows[0]
    for row in (reader, writer):
        assert "Excel reader or writer only addresses whole sheets" in row.messages[0], row.messages


def test_file_output_splits_the_windows_path(all_supported: ConversionResult):
    settings = dumped_nodes(all_supported)[12]["setting_input"]["output_settings"]
    assert settings["name"] == "result.csv"
    assert settings["directory"] == FIXTURE_OUTPUT_DIR
    assert settings["file_type"] == "csv"
    assert settings["write_mode"] == "overwrite"


MULTI_INPUT_OUT_OF_ORDER = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.Union.Union" />
      <Properties><Configuration><Mode>ByName</Mode></Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="a" /></Fields>
        <Data><r><c>2</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="4"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="a" /></Fields>
        <Data><r><c>3</c></r></Data></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection name="#3"><Origin ToolID="4" Connection="Output" />
      <Destination ToolID="1" Connection="Input" /></Connection>
    <Connection name="#1"><Origin ToolID="2" Connection="Output" />
      <Destination ToolID="1" Connection="Input" /></Connection>
    <Connection name="#2"><Origin ToolID="3" Connection="Output" />
      <Destination ToolID="1" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_multi_input_wires_follow_the_numbers_alteryx_wrote_not_document_order():
    result = convert_yxmd(MULTI_INPUT_OUT_OF_ORDER, source_name="union.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    union = dumped_nodes(result)[rows[1].flowfile_node_ids[0]]
    # #1, #2, #3 are tools 2, 3, 4 — document order would have put tool 4 first.
    assert union["input_ids"] == [rows[tool_id].flowfile_node_ids[0] for tool_id in (2, 3, 4)]


MIXED_NUMBERED_WIRES = MULTI_INPUT_OUT_OF_ORDER.replace(
    b'<Connection name="#1"><Origin ToolID="2"', b'<Connection><Origin ToolID="2"'
)
DUPLICATE_NUMBERED_WIRES = MULTI_INPUT_OUT_OF_ORDER.replace(
    b'<Connection name="#1"><Origin ToolID="2"', b'<Connection name="#2"><Origin ToolID="2"'
)


def test_partly_numbered_wires_keep_document_order():
    """Alteryx numbers all of an anchor's wires or none; a partial set states nothing."""
    result = convert_yxmd(MIXED_NUMBERED_WIRES, source_name="union.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    union = dumped_nodes(result)[rows[1].flowfile_node_ids[0]]
    assert union["input_ids"] == [rows[tool_id].flowfile_node_ids[0] for tool_id in (4, 2, 3)]


def test_duplicate_wire_numbers_keep_document_order():
    result = convert_yxmd(DUPLICATE_NUMBERED_WIRES, source_name="union.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    union = dumped_nodes(result)[rows[1].flowfile_node_ids[0]]
    assert union["input_ids"] == [rows[tool_id].flowfile_node_ids[0] for tool_id in (4, 2, 3)]


def test_unnumbered_wires_keep_document_order():
    result = convert_yxmd(MULTI_INPUT_OUT_OF_ORDER.replace(b' name="#', b' data-name="#'), source_name="union.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    union = dumped_nodes(result)[rows[1].flowfile_node_ids[0]]
    assert union["input_ids"] == [rows[tool_id].flowfile_node_ids[0] for tool_id in (4, 2, 3)]


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


@pytest.fixture()
def join_select() -> ConversionResult:
    return convert("join_select.yxmd")


def join_selects(result: ConversionResult, tool_id: int) -> tuple[list[tuple], list[tuple]]:
    row = next(row for row in result.report.rows if row.alteryx_tool_id == tool_id)
    node = next(node for node in result.flow_data.nodes if node.id == row.flowfile_node_ids[0])
    join_input = node.setting_input.join_input
    return tuple(
        [(entry.old_name, entry.new_name, entry.keep) for entry in side.renames]
        for side in (join_input.left_select, join_input.right_select)
    )


def test_join_select_keeps_alteryx_own_output_names(join_select: ConversionResult):
    left, right = join_selects(join_select, 741)
    assert left == [("CrateId", "CrateId", True), ("Grower", "Grower", True), ("Variety", "Variety", True)]
    # Right_CrateId is what Alteryx calls the column, so it is what downstream tools reference.
    assert right == [("CrateId", "Right_CrateId", True), ("PickedOn", "PickedOn", True)]


def test_a_kept_right_join_key_is_partial_because_it_cannot_be_placed(join_select: ConversionResult):
    """Alteryx puts it where the right input starts; Flowfile's join can only append it."""
    row = next(row for row in join_select.report.rows if row.alteryx_tool_id == 741)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "Right_CrateId" in row.messages[0] and "column order" in row.messages[0]


def test_a_join_without_a_kept_right_key_stays_converted(join_select: ConversionResult):
    row = next(row for row in join_select.report.rows if row.alteryx_tool_id == 742)
    assert (row.status, row.messages) == ("converted", [])


def test_an_l_anchor_consumer_is_not_told_the_inner_joins_projection():
    """Tool 744's L output is an anti join carrying the left input through, not the projection."""
    ctx, rows = emit_tools(parse_yxmd(read_fixture("join_select.yxmd")))
    assert (rows[745].status, rows[745].flowfile_node_type) == ("converted", "sort")
    # What the join records for itself is the projection a J-anchor consumer would be handed.
    assert ctx.tool_columns[744] == ["CrateId", "Grower", "Variety", "Right_CrateId", "PickedOn"]
    # The Sort wired to the L anchor is handed the left input's own columns instead.
    assert ctx.anchor_columns[(744, "Left")] == ["CrateId", "Grower", "Variety"]
    assert ctx.tool_columns[745] == ["CrateId", "Grower", "Variety"]


JOIN_SELECT_ALL_ANCHORS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="key" /><Field name="b" /></Fields>
        <Data><r><c>1</c><c>x</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="key" /><Field name="d" /></Fields>
        <Data><r><c>1</c><c>y</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
      <Properties><Configuration joinByRecordPos="False">
        <JoinInfo connection="Left"><Field field="key" /></JoinInfo>
        <JoinInfo connection="Right"><Field field="key" /></JoinInfo>
        <SelectConfiguration><Configuration outputConnection="Join"><SelectFields>
          <SelectField field="Right_key" selected="False" rename="Right_key" />
          <SelectField field="d" selected="True" rename="Detail" />
          <SelectField field="*Unknown" selected="True" />
        </SelectFields></Configuration></SelectConfiguration>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="Left" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Right" /></Connection>
    <Connection><Origin ToolID="3" Connection="Join" />
      <Destination ToolID="4" Connection="Input" /></Connection>
    <Connection><Origin ToolID="3" Connection="Left" />
      <Destination ToolID="5" Connection="Input" /></Connection>
    <Connection><Origin ToolID="3" Connection="Right" />
      <Destination ToolID="6" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_the_anti_joins_carry_no_select_config():
    """Alteryx's field selection shapes the J output only; L and R pass one input through whole."""
    joins = node_of_type(dumped_nodes(convert_yxmd(JOIN_SELECT_ALL_ANCHORS, source_name="join.yxmd")), "join")
    inner, anti_left, anti_right = (node["setting_input"]["join_input"] for node in joins)
    assert (inner["how"], anti_left["how"], anti_right["how"]) == ("inner", "anti", "anti")
    # The selection reached the inner join, so its absence below is not an absence of selection.
    assert [(entry["new_name"], entry.get("keep", True)) for entry in inner["right_select"]["select"]] == [
        ("Right_key", False),
        ("Detail", True),
    ]
    for anti in (anti_left, anti_right):
        assert (anti["left_select"]["select"], anti["right_select"]["select"]) == ([], [])


RENAME_FROM_JOIN_R_ANCHOR = JOIN_SELECT_ALL_ANCHORS.replace(
    b"  </Nodes>",
    b"""    <Node ToolID="4"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="x" /><Field name="y" /></Fields>
        <Data><r><c>1</c><c>2</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="5"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>RightInputMetadata</RenameMode>
        <Fields><Field name="x" selected="True" /><Field name="y" selected="True" /></Fields>
      </Configuration></Properties></Node>
  </Nodes>""",
).replace(
    b"""    <Connection><Origin ToolID="3" Connection="Join" />
      <Destination ToolID="4" Connection="Input" /></Connection>
    <Connection><Origin ToolID="3" Connection="Left" />
      <Destination ToolID="5" Connection="Input" /></Connection>
    <Connection><Origin ToolID="3" Connection="Right" />
      <Destination ToolID="6" Connection="Input" /></Connection>
""",
    b"""    <Connection><Origin ToolID="4" Connection="Output" />
      <Destination ToolID="5" Connection="Targets" /></Connection>
    <Connection><Origin ToolID="3" Connection="Right" />
      <Destination ToolID="5" Connection="Source" /></Connection>
""",
)


def test_a_dynamic_rename_fed_by_a_joins_r_anchor_sees_the_right_inputs_columns():
    """The R anchor passes the right input (key, d) through; the inner projection would say (key, b, Detail)."""
    result = convert_yxmd(RENAME_FROM_JOIN_R_ANCHOR, source_name="rename.yxmd")
    row = report_row(result, 5)
    assert row.flowfile_node_type == "select", row.messages
    renames = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["select_input"]
    assert [(entry["old_name"], entry["new_name"]) for entry in renames] == [("x", "key"), ("y", "d")]


def test_join_select_drops_the_fields_alteryx_deselected(join_select: ConversionResult):
    _left, right = join_selects(join_select, 742)  # noqa: F841 - only the right side is asserted
    assert right == [
        ("Grower", "Right_Grower", False),
        ("Variety", "Right_Variety", False),
        ("Packhouse", "Packhouse", True),
    ]


def test_join_by_record_position_is_read_from_the_attribute_alteryx_writes(join_select: ConversionResult):
    row = next(row for row in join_select.report.rows if row.alteryx_tool_id == 743)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "record position" in row.messages[0]


def test_join_select_is_read_when_the_source_tool_is_later_in_the_document(join_select: ConversionResult):
    # Every Text Input sits after the joins, so the columns come from what they declare.
    for tool_id in (741, 742):
        row = next(row for row in join_select.report.rows if row.alteryx_tool_id == tool_id)
        assert not any("could not tell which input" in message for message in row.messages)


JOIN_SELECT_UNKNOWN_COLUMNS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
      <Properties><Configuration joinByRecordPos="False">
        <JoinInfo connection="Left"><Field field="id" /></JoinInfo>
        <JoinInfo connection="Right"><Field field="id" /></JoinInfo>
        <SelectConfiguration><Configuration outputConnection="Join"><SelectFields>
          <SelectField field="Right_id" selected="False" rename="Right_id" />
          <SelectField field="*Unknown" selected="True" />
        </SelectFields></Configuration></SelectConfiguration>
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


def test_join_select_is_refused_when_the_input_columns_are_unknown():
    row = convert_yxmd(JOIN_SELECT_UNKNOWN_COLUMNS, source_name="join.yxmd").report.rows[0]
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "could not tell which input" in row.messages[0]


APPEND_FIELDS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.AppendFields.AppendFields" />
      <Properties><Configuration>
        <CartesianMode>%s</CartesianMode>
        <SelectConfiguration><Configuration outputConnection="Output"><SelectFields>
          <SelectField field="*Unknown" selected="True" />
        </SelectFields></Configuration></SelectConfiguration>
      </Configuration></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


@pytest.mark.parametrize(
    ("mode", "status"), [(b"Allow", "converted"), (b"Warn", "converted"), (b"Error", "partial")]
)  # a missing <CartesianMode> is covered separately: the file states nothing, so it fails closed
def test_append_fields_keeps_everything_and_warns_only_on_the_cartesian_guard(mode: bytes, status: str):
    row = convert_yxmd(APPEND_FIELDS % mode, source_name="append.yxmd").report.rows[0]
    assert row.status == status
    assert row.flowfile_node_type == "cross_join"
    # "*Unknown selected=True" alone is Flowfile's own default, so it is not a reason to downgrade.
    assert not any("field selection" in message for message in row.messages)


def test_append_fields_without_a_cartesian_mode_fails_closed():
    without = APPEND_FIELDS % b"Allow"
    without = without.replace(b"        <CartesianMode>Allow</CartesianMode>\n", b"")
    row = convert_yxmd(without, source_name="append.yxmd").report.rows[0]
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "does not say what to do" in row.messages[0]


def test_an_unrecognised_cartesian_mode_fails_closed_like_a_missing_one():
    row = convert_yxmd(APPEND_FIELDS % b"Stop", source_name="append.yxmd").report.rows[0]
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "'Stop'" in row.messages[0] and "Allow, Warn, Error" in row.messages[0]


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
            {
                "total": 13,
                "annotations": 0,
                "converted": 11,
                "partial": 2,
                "commented": 0,
                "placeholder": 0,
                "skipped": 0,
            },
        ),
        (
            "formulas.yxmd",
            {
                "total": 2,
                "annotations": 0,
                "converted": 1,
                "partial": 0,
                "commented": 1,
                "placeholder": 0,
                "skipped": 0,
            },
        ),
        (
            "containers.yxmd",
            {
                "total": 4,
                "annotations": 1,
                "converted": 4,
                "partial": 0,
                "commented": 0,
                "placeholder": 0,
                "skipped": 0,
            },
        ),
        (
            "unsupported.yxmd",
            {
                "total": 4,
                "annotations": 0,
                "converted": 2,
                "partial": 0,
                "commented": 0,
                "placeholder": 2,
                "skipped": 0,
            },
        ),
        (
            "out_of_scope.yxmd",
            {
                "total": 8,
                "annotations": 0,
                "converted": 1,
                "partial": 0,
                "commented": 0,
                "placeholder": 1,
                "out_of_scope": 4,
                "no_op": 2,
                "skipped": 0,
            },
        ),
    ],
)
def test_report_counts(fixture: str, expected: dict):
    report = convert(fixture).report
    assert report.total_tools == expected["total"]
    assert report.total_annotations == expected["annotations"]
    assert len(report.rows) == expected["total"] + expected["annotations"]
    for status in ("converted", "partial", "commented", "placeholder", "out_of_scope", "no_op", "skipped"):
        assert getattr(report, status) == expected.get(status, 0), status


def test_a_comment_never_counts_as_a_converted_tool(containers: ConversionResult):
    report = containers.report
    comment_row = next(row for row in report.rows if row.flowfile_node_type == "comment")
    assert comment_row.entity == "annotation"
    assert comment_row.status == "converted"
    # The comment is converted, but the coverage numbers only know about the four tools.
    assert report.coverage.tools == 4
    assert report.coverage.mapped == 4
    assert report.coverage.mapped_percent == 100


SCOPE_EXPECTATIONS = {
    2: ("out_of_scope", "scope:spatial"),
    3: ("out_of_scope", "scope:reporting"),
    4: ("out_of_scope", "scope:computer_vision"),
    5: ("no_op", "no_op"),
    6: ("no_op", "no_op"),
    7: ("placeholder", "unmapped_tool"),
    8: ("out_of_scope", "scope:genai"),
}


@pytest.mark.parametrize(("tool_id", "expected"), sorted(SCOPE_EXPECTATIONS.items()))
def test_scope_decides_the_status_and_reason(out_of_scope: ConversionResult, tool_id: int, expected: tuple[str, str]):
    row = next(row for row in out_of_scope.report.rows if row.alteryx_tool_id == tool_id)
    assert (row.status, row.reason) == expected


def test_a_vendor_plugin_and_a_user_macro_are_still_placed_by_their_census_name(out_of_scope: ConversionResult):
    """`tool_key` collapses both to a fixed key, so only `census_name` can carry scope."""
    rows = {row.alteryx_tool_id: row for row in out_of_scope.report.rows}
    assert (rows[4].alteryx_tool_key, rows[4].census_name) == ("custom_plugin", "ImageInput_1_0")
    assert (rows[8].alteryx_tool_key, rows[8].census_name) == ("user_macro", "Precision_Match.yxmc")
    assert rows[8].alteryx_tool == "Precision Match\\Precision_Match.yxmc"


def test_only_a_tool_alteryx_ships_is_requestable(out_of_scope: ConversionResult):
    """The dialog gates its "Request node" button on this, so it must not name someone's own macro."""
    rows = {row.alteryx_tool_id: row for row in out_of_scope.report.rows}
    assert rows[7].requestable is True
    assert rows[4].requestable is False
    assert rows[8].requestable is False


def test_an_out_of_scope_node_says_why_instead_of_promising_a_rebuild(out_of_scope: ConversionResult):
    row = next(row for row in out_of_scope.report.rows if row.alteryx_tool_id == 2)
    sentence = BUCKETS["spatial"]
    assert row.messages == [sentence]

    node = dumped_nodes(out_of_scope)[row.flowfile_node_ids[0]]
    assert node["description"] == sentence
    assert not node["description"].startswith("⚠")
    code = node["setting_input"]["polars_code_input"]["polars_code"]
    assert code.startswith(f"# Alteryx tool 'Buffer' (ToolID 2): {sentence}")
    assert "could not be converted automatically" not in code
    assert "rebuild the logic here" not in code


def test_a_no_op_tool_emits_no_node_and_hands_its_input_on(out_of_scope: ConversionResult):
    """Tool 5 is a Message between the Image Input and the DateTime; it costs no node at all."""
    rows = {row.alteryx_tool_id: row for row in out_of_scope.report.rows}
    message = rows[5]
    assert (message.status, message.reason) == ("no_op", "no_op")
    assert (message.flowfile_node_ids, message.flowfile_node_type) == ([], None)
    assert message.messages == [BUCKETS["no_op"]]
    # The DateTime behind it is wired to what fed the Message, and no wire was reported dropped.
    nodes = dumped_nodes(out_of_scope)
    assert nodes[rows[7].flowfile_node_ids[0]]["input_ids"] == rows[4].flowfile_node_ids
    assert not any("was dropped" in text for row in out_of_scope.report.rows for text in row.messages)
    assert rows[4].status == "out_of_scope"


def test_an_out_of_scope_tool_still_passes_its_data_through(out_of_scope: ConversionResult):
    """Six nodes for eight tools: the Message and the Test emit none, the chain stays connected."""
    nodes = dumped_nodes(out_of_scope)
    assert [nodes[node_id]["outputs"] for node_id in sorted(nodes)] == [[2], [3], [4], [5], [6], []]
    assert [nodes[node_id]["input_ids"] for node_id in sorted(nodes)] == [[], [1], [2], [3], [4], [5]]


@pytest.fixture()
def no_op_passthrough() -> ConversionResult:
    return convert("no_op_passthrough.yxmd")


@pytest.fixture()
def detour() -> ConversionResult:
    return convert("detour.yxmd")


def test_every_output_anchor_of_a_no_op_is_wired_to_its_source(no_op_passthrough: ConversionResult):
    """The Block Until Done's three anchors, one of them behind a Message, all carry the Text Input."""
    rows = {row.alteryx_tool_id: row for row in no_op_passthrough.report.rows}
    assert (rows[2].status, rows[2].flowfile_node_ids) == ("no_op", [])
    assert (rows[6].status, rows[6].flowfile_node_ids) == ("no_op", [])
    nodes = dumped_nodes(no_op_passthrough)
    source = rows[1].flowfile_node_ids
    for tool_id in (3, 4, 5):
        assert nodes[rows[tool_id].flowfile_node_ids[0]]["input_ids"] == source, tool_id
    assert nodes[source[0]]["outputs"] == [4, 5, 2, 3]
    assert not any("was dropped" in text for row in no_op_passthrough.report.rows for text in row.messages)


def test_a_consumer_behind_a_no_op_still_knows_its_columns(no_op_passthrough: ConversionResult):
    """Column knowledge resolves through the no-ops, and does so before they are mapped.

    Tools 5 and 7 sit behind the Message but are written before it, so the Filter's record of
    its input columns is what proves the resolution does not depend on document order.
    """
    ctx, rows = emit_tools(parse_yxmd(read_fixture("no_op_passthrough.yxmd")))
    assert ctx.input_columns(5) == ["CrateId", "Grower"]
    assert ctx.tool_columns[7] == ["CrateId", "Grower"]
    assert 2 not in ctx.tool_columns and 6 not in ctx.tool_columns


def test_a_detour_wires_only_the_anchor_its_configuration_makes_live(detour: ConversionResult):
    rows = {row.alteryx_tool_id: row for row in detour.report.rows}
    nodes = dumped_nodes(detour)
    source = rows[1].flowfile_node_ids
    for detour_id, live_consumer, dead_consumer, live, dead in ((2, 3, 4, "Right", "Left"), (7, 8, 9, "Left", "Right")):
        assert (rows[detour_id].status, rows[detour_id].flowfile_node_ids) == ("no_op", [])
        assert nodes[rows[live_consumer].flowfile_node_ids[0]]["input_ids"] == source
        assert nodes[rows[dead_consumer].flowfile_node_ids[0]]["input_ids"] == []
        assert (rows[dead_consumer].status, rows[dead_consumer].reason) == ("partial", "dropped_connection")
        sentence = (
            f"This Alteryx Detour is configured to send its records out of the '{live}' anchor, so nothing "
            f"leaves the '{dead}' anchor; the connections from it were not wired."
        )
        assert rows[dead_consumer].messages == [sentence]
        assert sentence in rows[detour_id].messages


def test_a_detour_end_takes_the_side_its_detour_made_live(detour: ConversionResult):
    """Two hops: the Detour End resolves through the Detour to the Text Input that fed it."""
    rows = {row.alteryx_tool_id: row for row in detour.report.rows}
    nodes = dumped_nodes(detour)
    for end_id, consumer_id, detour_id, live in ((5, 6, 2, "Right"), (10, 11, 7, "Left")):
        assert (rows[end_id].status, rows[end_id].flowfile_node_ids) == ("no_op", [])
        assert rows[end_id].messages[1] == (
            f"The records arrive from the '{live}' anchor of the Alteryx Detour "
            f"(ToolID {detour_id}), which is the side its configuration makes live."
        )
        assert nodes[rows[consumer_id].flowfile_node_ids[0]]["input_ids"] == rows[1].flowfile_node_ids


DETOUR_END_WITHOUT_A_DETOUR = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /></Fields>
        <Data><r><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.DetourEnd.DetourEnd" />
      <Properties><Configuration /></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="%s" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="%s" /></Connection>
  </Connections>
</AlteryxDocument>
"""


@pytest.mark.parametrize("anchors", [(b"Left", b"Right"), (b"Left", b"Left")])
def test_a_detour_end_that_no_detour_feeds_fails_closed(anchors: tuple[bytes, bytes]):
    """No live Detour upstream means no side to take, so a real node stands in and runs.

    The second case is the one that would otherwise pick a side silently: both wires arrive on
    one anchor, so the wire that carries the records cannot be told from the one that does not.
    """
    result = convert_yxmd(DETOUR_END_WITHOUT_A_DETOUR % anchors, source_name="detour_end.yxmd")
    row = report_row(result, 3)
    assert (row.status, row.reason, row.flowfile_node_type) == ("placeholder", "mapper_refused", "polars_code")
    node = dumped_nodes(result)[row.flowfile_node_ids[0]]
    body = node["setting_input"]["polars_code_input"]["polars_code"]
    ast.parse(body)
    # Two sources, so the body reads the first of two inputs — and both are really wired to it.
    assert body.endswith("output_df = input_df_1")
    assert node["input_ids"] == [1, 2]


def _arity_workflow(tools: str, connections: str) -> bytes:
    """One Text Input plus whatever else the shape needs, as bytes ready to convert."""
    return (
        """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="601"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /><Field name="Grower" /></Fields>
        <Data><r><c>1</c><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>"""
        + tools
        + """
  </Nodes>
  <Connections>"""
        + connections
        + """
  </Connections>
</AlteryxDocument>
"""
    ).encode("utf-8")


_DETOUR_RIGHT = """
    <Node ToolID="602"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="True" /></Configuration></Properties></Node>"""
_DETOUR_END = """
    <Node ToolID="605"><GuiSettings Plugin="AlteryxBasePluginsGui.DetourEnd.DetourEnd" />
      <Properties><Configuration /></Properties></Node>"""

# A live side that carries a Select is no longer a wire *from* a Detour, so the end refuses; the
# dead Left wire is a source the refusal counts but wiring never lays.
DETOUR_END_BEHIND_A_SELECT = _arity_workflow(
    _DETOUR_RIGHT
    + """
    <Node ToolID="603"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration>
        <SelectFields><SelectField field="CrateId" selected="True" /></SelectFields>
      </Configuration></Properties></Node>"""
    + _DETOUR_END,
    """
    <Connection><Origin ToolID="601" Connection="Output" />
      <Destination ToolID="602" Connection="Input" /></Connection>
    <Connection><Origin ToolID="602" Connection="Right" />
      <Destination ToolID="603" Connection="Input" /></Connection>
    <Connection><Origin ToolID="603" Connection="Output" />
      <Destination ToolID="605" Connection="Right" /></Connection>
    <Connection><Origin ToolID="602" Connection="Left" />
      <Destination ToolID="605" Connection="Left" /></Connection>""",
)

# Two anchors of ONE Flowfile node: a Filter is one node with a True and a False handle.
DETOUR_END_BEHIND_ONE_FILTER = _arity_workflow(
    """
    <Node ToolID="604"><GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter" />
      <Properties><Configuration>
        <Expression>[CrateId] &gt; 0</Expression>
      </Configuration></Properties></Node>"""
    + _DETOUR_END,
    """
    <Connection><Origin ToolID="601" Connection="Output" />
      <Destination ToolID="604" Connection="Input" /></Connection>
    <Connection><Origin ToolID="604" Connection="True" />
      <Destination ToolID="605" Connection="Left" /></Connection>
    <Connection><Origin ToolID="604" Connection="False" />
      <Destination ToolID="605" Connection="Right" /></Connection>""",
)

# The same shape through an unmapped tool, whose every anchor is registered on its one placeholder.
DETOUR_END_BEHIND_ONE_PLACEHOLDER = _arity_workflow(
    """
    <Node ToolID="606"><GuiSettings Plugin="AlteryxBasePluginsGui.Tile.Tile" />
      <Properties><Configuration><Method>EqualRecords</Method></Configuration></Properties></Node>"""
    + _DETOUR_END,
    """
    <Connection><Origin ToolID="601" Connection="Output" />
      <Destination ToolID="606" Connection="Input" /></Connection>
    <Connection><Origin ToolID="606" Connection="Output" />
      <Destination ToolID="605" Connection="Left" /></Connection>
    <Connection><Origin ToolID="606" Connection="Output2" />
      <Destination ToolID="605" Connection="Right" /></Connection>""",
)

# The negative control: map_join registers Left and Right on two DIFFERENT nodes, so these really
# are two inputs and the body has to keep saying so.
DETOUR_END_BEHIND_A_JOIN = _arity_workflow(
    """
    <Node ToolID="607"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /><Field name="Packhouse" /></Fields>
        <Data><r><c>1</c><c>Shed 2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="608"><GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
      <Properties><Configuration joinByRecordPos="False">
        <JoinInfo connection="Left"><Field field="CrateId" /></JoinInfo>
        <JoinInfo connection="Right"><Field field="CrateId" /></JoinInfo>
      </Configuration></Properties></Node>"""
    + _DETOUR_END,
    """
    <Connection><Origin ToolID="601" Connection="Output" />
      <Destination ToolID="608" Connection="Left" /></Connection>
    <Connection><Origin ToolID="607" Connection="Output" />
      <Destination ToolID="608" Connection="Right" /></Connection>
    <Connection><Origin ToolID="608" Connection="Left" />
      <Destination ToolID="605" Connection="Left" /></Connection>
    <Connection><Origin ToolID="608" Connection="Right" />
      <Destination ToolID="605" Connection="Right" /></Connection>""",
)

# Both wires leave the side the Detour makes dead, so the node is wired to nothing at all.
DETOUR_END_BEHIND_A_DEAD_SIDE = _arity_workflow(
    _DETOUR_RIGHT + _DETOUR_END,
    """
    <Connection><Origin ToolID="601" Connection="Output" />
      <Destination ToolID="602" Connection="Input" /></Connection>
    <Connection><Origin ToolID="602" Connection="Left" />
      <Destination ToolID="605" Connection="Left" /></Connection>
    <Connection><Origin ToolID="602" Connection="Left" />
      <Destination ToolID="605" Connection="Right" /></Connection>""",
)


@pytest.mark.parametrize(
    ("workflow", "edges", "tail"),
    [
        (DETOUR_END_BEHIND_A_SELECT, 1, "output_df = input_df"),
        (DETOUR_END_BEHIND_ONE_FILTER, 1, "output_df = input_df"),
        (DETOUR_END_BEHIND_ONE_PLACEHOLDER, 1, "output_df = input_df"),
        (DETOUR_END_BEHIND_A_JOIN, 2, "output_df = input_df_1"),
        (DETOUR_END_BEHIND_A_DEAD_SIDE, 0, "output_df = pl.DataFrame()"),
    ],
    ids=["select_on_the_live_side", "one_filter", "one_placeholder", "a_join_really_is_two", "dead_side_only"],
)
def test_a_placeholder_body_is_written_for_the_edges_it_receives(
    tmp_path: Path, workflow: bytes, edges: int, tail: str
):
    """The engine binds input_df for one input and input_df_1..N for more, so a body written for a
    wire count wiring then collapses names something that was never bound."""
    result = convert_yxmd(workflow, source_name="arity.yxmd")
    row = report_row(result, 605)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused"), row.messages
    node = dumped_nodes(result)[row.flowfile_node_ids[0]]
    assert len(node["input_ids"]) == edges
    assert node["is_start_node"] is (edges == 0)
    body = node["setting_input"]["polars_code_input"]["polars_code"]
    ast.parse(body)
    assert body.endswith(tail)
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]


def _two_sources(destination: str, dest_anchors: tuple[str, str], extra: str = "", wires: str = "") -> bytes:
    """Two Text Inputs into one tool that emits no node, plus whatever the shape needs behind it."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="711"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="712"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /></Fields>
        <Data><r><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="713"><GuiSettings Plugin="{destination}" />
      <Properties><Configuration /></Properties></Node>{extra}
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="711" Connection="Output" />
      <Destination ToolID="713" Connection="{dest_anchors[0]}" /></Connection>
    <Connection><Origin ToolID="712" Connection="Output" />
      <Destination ToolID="713" Connection="{dest_anchors[1]}" /></Connection>{wires}
  </Connections>
</AlteryxDocument>
""".encode()


# Alteryx unions every wire arriving on one input anchor; a Message carries one of them onward.
TWO_WIRES_INTO_ONE_MESSAGE = _two_sources(
    "AlteryxBasePluginsGui.Message.Message",
    ("Input", "Input"),
    extra="""
    <Node ToolID="714"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration>
        <SelectFields><SelectField field="CrateId" selected="True" /></SelectFields>
      </Configuration></Properties></Node>""",
    wires="""
    <Connection><Origin ToolID="713" Connection="Output" />
      <Destination ToolID="714" Connection="Input" /></Connection>""",
)

# A Detour End expects the dead side of its own Detour, not a stream from somewhere else.
A_STRANGER_ON_A_DETOUR_END = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="721"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="722"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="True" /></Configuration></Properties></Node>
    <Node ToolID="723"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /></Fields>
        <Data><r><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="724"><GuiSettings Plugin="AlteryxBasePluginsGui.DetourEnd.DetourEnd" />
      <Properties><Configuration /></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="721" Connection="Output" />
      <Destination ToolID="722" Connection="Input" /></Connection>
    <Connection><Origin ToolID="722" Connection="Right" />
      <Destination ToolID="724" Connection="Right" /></Connection>
    <Connection><Origin ToolID="723" Connection="Output" />
      <Destination ToolID="724" Connection="Left" /></Connection>
  </Connections>
</AlteryxDocument>
"""

# Expect Equal is a sink: Alteryx gives it no output anchor, so both wires end there on purpose.
EXPECT_EQUAL_COMPARES_TWO_INPUTS = _two_sources("AlteryxBasePluginsGui.ExpectEqual.ExpectEqual", ("Expected", "Actual"))


def dropped_messages(result: ConversionResult) -> list[tuple[int, str]]:
    return [
        (row.alteryx_tool_id, message)
        for row in result.report.rows
        for message in row.messages
        if "was not carried over" in message or "was dropped" in message
    ]


def test_a_no_op_reports_the_second_wire_onto_one_anchor(tmp_path: Path):
    """Alteryx unions two wires onto a Message's input; Flowfile has one edge, so one is lost."""
    result = convert_yxmd(TWO_WIRES_INTO_ONE_MESSAGE, source_name="two_wires.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = (
        "The Alteryx 'Message' (ToolID 713) has no effect on the data, so no node was imported for it "
        "and only the stream it passes on was kept; the connection from ToolID 712 ('Output') into its "
        "'Input' anchor was not carried over."
    )
    assert dropped_messages(result) == [(712, sentence), (713, sentence)]
    # The wire it did carry is the one the consumer gets, and the source that lost one says so.
    assert (rows[712].status, rows[712].reason) == ("partial", "dropped_connection")
    assert (rows[711].status, rows[713].status) == ("converted", "no_op")
    nodes = dumped_nodes(result)
    assert nodes[rows[714].flowfile_node_ids[0]]["input_ids"] == rows[711].flowfile_node_ids
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_detour_end_reports_a_wire_that_is_not_its_detour():
    """The dead side of its own Detour is expected here; a third stream is a wire the flow lost."""
    result = convert_yxmd(A_STRANGER_ON_A_DETOUR_END, source_name="stranger.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[724].status == "no_op"
    messages = dropped_messages(result)
    assert [tool_id for tool_id, _ in messages] == [723, 724]
    assert "ToolID 723 ('Output') into its 'Left' anchor was not carried over" in messages[0][1]
    assert (rows[723].status, rows[723].reason) == ("partial", "dropped_connection")


def test_a_sink_consumes_every_wire_it_is_given(tmp_path: Path):
    """Expect Equal has no output anchor at all, so neither input is a wire that went missing."""
    result = convert_yxmd(EXPECT_EQUAL_COMPARES_TWO_INPUTS, source_name="expect_equal.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[713].status == "no_op"
    assert (rows[711].status, rows[712].status) == ("converted", "converted")
    assert dropped_messages(result) == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


# The three shapes below all put a no-op between a tool and the thing it needs to read, which is
# the only way to tell a resolved origin from a raw one.
SORT_THEN_MESSAGE_THEN_RUNNING_TOTAL = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="731"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /><Field name="Crates" /></Fields>
        <Data><r><c>Okonjo</c><c>3</c></r><r><c>Salgado</c><c>5</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="732"><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo><Field field="Grower" order="Ascending" /></SortInfo>
      </Configuration></Properties></Node>
    <Node ToolID="733"><GuiSettings Plugin="AlteryxBasePluginsGui.Message.Message" />
      <Properties><Configuration /></Properties></Node>
    <Node ToolID="734"><GuiSettings Plugin="AlteryxBasePluginsGui.RunningTotal.RunningTotal" />
      <Properties><Configuration>
        <RunningTotalFields><Field field="Crates" /></RunningTotalFields>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="731" Connection="Output" />
      <Destination ToolID="732" Connection="Input" /></Connection>
    <Connection><Origin ToolID="732" Connection="Output" />
      <Destination ToolID="733" Connection="Input" /></Connection>
    <Connection><Origin ToolID="733" Connection="Output" />
      <Destination ToolID="734" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""

TEXT_INPUT_THEN_MESSAGE_THEN_RENAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="741"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /><Field name="Grower" /></Fields>
        <Data><r><c>1</c><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="742"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="NewName" /></Fields>
        <Data><r><c>Crate</c></r><r><c>Picker</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="743"><GuiSettings Plugin="AlteryxBasePluginsGui.Message.Message" />
      <Properties><Configuration /></Properties></Node>
    <Node ToolID="744"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>RightInputRows</RenameMode>
        <Fields><Field name="CrateId" /><Field name="Grower" /><Field name="*Unknown" /></Fields>
        <NamesFromRows><InputMode>Positional</InputMode><NewName>NewName</NewName></NamesFromRows>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="741" Connection="Output" />
      <Destination ToolID="744" Connection="Input" /></Connection>
    <Connection><Origin ToolID="742" Connection="Output" />
      <Destination ToolID="743" Connection="Input" /></Connection>
    <Connection><Origin ToolID="743" Connection="Output" />
      <Destination ToolID="744" Connection="Right" /></Connection>
  </Connections>
</AlteryxDocument>
"""

JOIN_BEHIND_A_MESSAGE = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="751"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /><Field name="Grower" /></Fields>
        <Data><r><c>1</c><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="752"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /><Field name="Packhouse" /></Fields>
        <Data><r><c>1</c><c>Shed 2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="753"><GuiSettings Plugin="AlteryxBasePluginsGui.Message.Message" />
      <Properties><Configuration /></Properties></Node>
    <Node ToolID="754"><GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
      <Properties><Configuration joinByRecordPos="False">
        <JoinInfo connection="Left"><Field field="CrateId" /></JoinInfo>
        <JoinInfo connection="Right"><Field field="CrateId" /></JoinInfo>
        <SelectConfiguration><Configuration outputConnection="Join">
          <SelectFields>
            <SelectField field="Right_CrateId" selected="True" rename="Right_CrateId" />
            <SelectField field="*Unknown" selected="True" />
          </SelectFields>
        </Configuration></SelectConfiguration>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="751" Connection="Output" />
      <Destination ToolID="754" Connection="Left" /></Connection>
    <Connection><Origin ToolID="752" Connection="Output" />
      <Destination ToolID="753" Connection="Input" /></Connection>
    <Connection><Origin ToolID="753" Connection="Output" />
      <Destination ToolID="754" Connection="Right" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_running_total_behind_a_no_op_still_sees_the_sort():
    """`_feeds_in_stated_order` reads the tool the wire resolves to, not the Message in front of it."""
    result = convert_yxmd(SORT_THEN_MESSAGE_THEN_RUNNING_TOTAL, source_name="running_total.yxmd")
    row = report_row(result, 734)
    assert (row.status, row.reason) == ("converted", "converted"), row.messages
    assert not any("order the rows arrive" in message for message in row.messages)


def test_a_dynamic_rename_behind_a_no_op_reads_the_text_input_rows():
    """`resolved_source` walks past the Message to the Text Input whose rows hold the names."""
    result = convert_yxmd(TEXT_INPUT_THEN_MESSAGE_THEN_RENAME, source_name="rename.yxmd")
    row = report_row(result, 744)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "select")
    assert "the rows of 'TextInput' (ToolID 742)" in row.messages[0]
    renames = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["select_input"]
    assert [(entry["old_name"], entry["new_name"]) for entry in renames] == [
        ("CrateId", "Crate"),
        ("Grower", "Picker"),
    ]


def test_a_join_behind_a_no_op_still_applies_its_field_selection():
    """`_anchor_columns` resolves through the Message, so the Right_ prefix resolves to a real column."""
    result = convert_yxmd(JOIN_BEHIND_A_MESSAGE, source_name="join_behind_message.yxmd")
    row = report_row(result, 754)
    assert not any("which input each selected field comes from" in message for message in row.messages), row.messages
    join = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["join_input"]
    right = join["right_select"]["select"]
    assert [(entry["old_name"], entry.get("new_name", entry["old_name"])) for entry in right] == [
        ("CrateId", "Right_CrateId"),
        ("Packhouse", "Packhouse"),
    ]


WIRE_OUT_OF_A_TEST_TOOL = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.Test.Test" />
      <Properties><Configuration><Tests /></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration>
        <SelectFields><SelectField field="CrateId" selected="True" /></SelectFields>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_wire_leaving_a_test_tool_is_reported_on_both_rows():
    """A Test is a sink in Alteryx, so a wire out of one is a wire Flowfile cannot reconnect."""
    result = convert_yxmd(WIRE_OUT_OF_A_TEST_TOOL, source_name="test_tool.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[2].status == "no_op"
    assert (rows[3].status, rows[3].reason) == ("partial", "dropped_connection")
    for tool_id in (2, 3):
        assert any("was dropped" in message for message in rows[tool_id].messages), tool_id
    assert dumped_nodes(result)[rows[3].flowfile_node_ids[0]]["input_ids"] == []


@pytest.fixture()
def explorer_box() -> ConversionResult:
    return convert("explorer_box.yxmd")


def test_an_explorer_box_becomes_a_canvas_comment_not_a_tool(explorer_box: ConversionResult):
    """It documents the canvas, so it is an annotation and leaves both coverage percentages."""
    row = report_row(explorer_box, 2)
    assert (row.entity, row.status, row.reason) == ("annotation", "converted", "annotation")
    assert (row.flowfile_node_type, row.flowfile_node_ids) == ("comment", [])
    assert [comment.text for comment in explorer_box.flow_data.comments if comment.id == 1] == [
        "Alteryx Explorer Box: https://example.com/crate-handbook.htm"
    ]
    # Only the Text Input and the two wired boxes are tools; the annotations are counted apart.
    assert (explorer_box.report.total_tools, explorer_box.report.total_annotations) == (3, 5)
    assert [node["type"] for node in dumped_nodes(explorer_box).values()] == [
        "manual_input",
        "polars_code",
        "polars_code",
    ]


def test_an_explorer_box_keeps_a_windows_path_exactly_as_written(explorer_box: ConversionResult):
    """The path only resolves on the machine the workflow came from; rewriting it would be a guess."""
    row = report_row(explorer_box, 3)
    assert (row.status, row.reason) == ("converted", "annotation")
    texts = [comment.text for comment in explorer_box.flow_data.comments]
    assert "Alteryx Explorer Box: D:\\Orchard\\Handbooks\\picking.html" in texts


def test_an_explorer_box_without_an_address_is_skipped(explorer_box: ConversionResult):
    row = report_row(explorer_box, 4)
    assert (row.entity, row.status, row.reason) == ("annotation", "skipped", "annotation")
    assert row.flowfile_node_type is None
    assert not any("names no address" in comment.text for comment in explorer_box.flow_data.comments)


def test_an_explorer_box_address_carrying_a_token_is_not_copied(explorer_box: ConversionResult):
    row = report_row(explorer_box, 5)
    assert (row.entity, row.status, row.reason) == ("annotation", "partial", "annotation")
    assert any("carries credentials or query parameters" in message for message in row.messages)
    assert not any("okonjo-secret" in comment.text for comment in explorer_box.flow_data.comments)
    assert any("was not copied out of the workflow" in comment.text for comment in explorer_box.flow_data.comments)


def test_a_wired_explorer_box_stays_a_node(explorer_box: ConversionResult):
    """A comment cannot carry a wire, so a box that has one keeps a pass-through node instead."""
    row = report_row(explorer_box, 6)
    assert (row.entity, row.status, row.reason) == ("tool", "placeholder", "mapper_refused")
    assert (
        dumped_nodes(explorer_box)[row.flowfile_node_ids[0]]["input_ids"]
        == report_row(explorer_box, 1).flowfile_node_ids
    )


def test_a_wired_explorer_box_does_not_copy_a_credential_address_into_its_body(explorer_box: ConversionResult):
    """The wired branch dumps the configuration, so it has to refuse what the comment refuses."""
    row = report_row(explorer_box, 8)
    assert (row.entity, row.status, row.reason) == ("tool", "placeholder", "mapper_refused")
    body = dumped_nodes(explorer_box)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "salgado-secret" not in body
    assert "<URL>[redacted by Flowfile]</URL>" in body
    assert "Credential values were not copied out of the workflow: URL." in body
    # The box without a token still shows its address, so the screen is the address, not the tag.
    plain = report_row(explorer_box, 6)
    plain_body = dumped_nodes(explorer_box)[plain.flowfile_node_ids[0]]["setting_input"]["polars_code_input"][
        "polars_code"
    ]
    assert "<URL>https://example.com/wired.htm</URL>" in plain_body


def test_comment_ids_are_unique_across_explorer_boxes_and_text_boxes(explorer_box: ConversionResult):
    """`restore_comments` keys comments by id, so a collision would silently drop one."""
    comments = explorer_box.flow_data.comments
    assert len(comments) == 4
    assert len({comment.id for comment in comments}) == len(comments)
    assert comments[-1].text == "Picking rules live in the handbook above."


@pytest.fixture()
def map_input() -> ConversionResult:
    return convert("map_input.yxmd")


def map_input_raw(result: ConversionResult, tool_id: int) -> dict:
    row = report_row(result, tool_id)
    assert row.flowfile_node_type == "manual_input", row.messages
    return dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["raw_data_format"]


def test_map_input_becomes_a_manual_input_of_all_string_columns(map_input: ConversionResult):
    """Alteryx declares no types here, so inferring one would retype a label like 00123."""
    raw = map_input_raw(map_input, 3)
    assert [(column["name"], column["data_type"]) for column in raw["columns"]] == [
        ("PlotCode", "String"),
        ("Grower", "String"),
    ]
    # Columnar, padded where the row ran short.
    assert raw["data"] == [["00123", "00124"], ["Okonjo", None]]
    row = report_row(map_input, 3)
    assert (row.status, row.reason) == ("converted", "converted")


def test_map_input_keeps_a_drawn_shape_byte_for_byte(map_input: ConversionResult):
    """The shapes are found by what they hold, not by Alteryx's default column name."""
    raw = map_input_raw(map_input, 1)
    assert [column["name"] for column in raw["columns"]] == ["Marker", "Shape"]
    assert raw["data"][1] == ['{"type":"point","coordinates":[4.895168,52.370216]}']
    row = report_row(map_input, 1)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "no spatial type" in row.messages[0]


def test_map_input_says_what_it_did_not_import(map_input: ConversionResult):
    row = report_row(map_input, 2)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("says it holds 3 rows but the workflow stores 2" in message for message in row.messages)
    assert any("backdrop, not an input" in message for message in row.messages)
    assert map_input_raw(map_input, 2)["data"][0] == ["Weigh bridge", "Cold store"]


def test_map_input_in_select_mode_fails_closed(map_input: ConversionResult):
    """Its output is whatever a user clicks on a map while the workflow runs."""
    row = report_row(map_input, 4)
    assert (row.status, row.reason, row.flowfile_node_type) == ("placeholder", "option_unsupported", "polars_code")
    assert "pick features on a map" in row.messages[0]


@pytest.fixture()
def make_group() -> ConversionResult:
    return convert("make_group.yxmd")


def test_make_group_becomes_a_solver_and_a_reshape(make_group: ConversionResult):
    """Two nodes because the shapes differ: the solver labels rows, Alteryx returns Key/Group pairs."""
    row = report_row(make_group, 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "row_order_unknown", "graph_solver")
    nodes = dumped_nodes(make_group)
    solver_id, reshape_id = row.flowfile_node_ids
    assert [nodes[node_id]["type"] for node_id in row.flowfile_node_ids] == ["graph_solver", "polars_code"]
    solver = nodes[solver_id]["setting_input"]["graph_solver_input"]
    assert (solver["col_from"], solver["col_to"]) == ("Grower", "Buyer")
    assert solver["output_column_name"] == "__make_group_component"
    assert nodes[reshape_id]["input_ids"] == [solver_id]
    assert "first key it meets in arrival order" in row.messages[0]


def test_the_make_group_flow_runs_and_names_the_alteryx_columns(tmp_path: Path, make_group: ConversionResult):
    """Alteryx's own downstream tools read these two columns by name, so the names are the contract."""
    flow = open_flow(write_flow(make_group, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]
    groups = flow.get_node(report_row(make_group, 2).flowfile_node_ids[1]).get_resulting_data()
    frame = groups.data_frame.collect()
    assert frame.columns == ["Key", "Group"]
    assert dict(zip(frame["Key"].to_list(), frame["Group"].to_list(), strict=True)) == {
        "Okonjo": "Okonjo",
        "Salgado": "Okonjo",
        "Vetle": "Okonjo",
        "Petrova": "Nakamura",
        "Nakamura": "Nakamura",
        "Ibarra": "Ibarra",
    }
    # The Select behind it keeps both columns, so a consumer really is handed Key and Group.
    downstream = flow.get_node(report_row(make_group, 3).flowfile_node_ids[0]).get_resulting_data()
    assert downstream.data_frame.collect().columns == ["Key", "Group"]


def test_make_group_without_both_keys_fails_closed(make_group: ConversionResult):
    row = report_row(make_group, 4)
    assert (row.status, row.reason, row.flowfile_node_type) == ("placeholder", "mapper_refused", "polars_code")
    assert "does not name both of its key fields" in row.messages[0]


def test_make_group_names_an_option_it_did_not_read(make_group: ConversionResult):
    row = report_row(make_group, 5)
    assert (row.status, row.reason) == ("partial", "row_order_unknown")
    assert "CaseSensitive" in row.messages[1]


def test_make_group_refuses_to_group_null_keys_when_it_runs(make_group: ConversionResult):
    """polars_grouper joins every null-keyed row into one component, which is a wrong group."""
    row = report_row(make_group, 2)
    code = dumped_nodes(make_group)[row.flowfile_node_ids[1]]["setting_input"]["polars_code_input"]["polars_code"]
    executable = polars_code_parser.get_executable(code, 1)
    solved = pl.LazyFrame({"Grower": ["Okonjo", None], "Buyer": [None, "Vetle"], "__make_group_component": [0, 0]})
    with pytest.raises(AssertionError, match="a key is null"):
        executable(solved).collect()


def _make_group(first: str, second: str, columns: str, rows: str) -> bytes:
    """A Text Input feeding one Make Group, with whatever key pair the shape needs."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="761"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields>{columns}</Fields>
        <Data>{rows}</Data>
      </Configuration></Properties></Node>
    <Node ToolID="762"><GuiSettings Plugin="AlteryxBasePluginsGui.MakeGroup.MakeGroup" />
      <Properties><Configuration>
        <Key1st>{first}</Key1st>
        <Key2nd>{second}</Key2nd>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="761" Connection="Output" />
      <Destination ToolID="762" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


ONE_COLUMN = '<Field name="Grower" />'
ONE_COLUMN_ROWS = "<r><c>Okonjo</c></r><r><c>Salgado</c></r>"


def test_make_group_with_one_column_on_both_keys_emits_one_key(tmp_path: Path):
    """Alteryx allows the same field on both sides; selecting it twice is a duplicate-name error."""
    result = convert_yxmd(_make_group("Grower", "Grower", ONE_COLUMN, ONE_COLUMN_ROWS), source_name="mg.yxmd")
    row = report_row(result, 762)
    assert row.status == "partial", row.messages
    code = dumped_nodes(result)[row.flowfile_node_ids[1]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "_keys = ['Grower']" in code
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]


def test_make_group_avoids_an_upstream_column_of_its_own_helper_name():
    """The component column is private; shadowing a real one would drop the user's data."""
    columns = '<Field name="Grower" /><Field name="Buyer" /><Field name="__make_group_component" />'
    rows = "<r><c>Okonjo</c><c>Salgado</c><c>x</c></r>"
    result = convert_yxmd(_make_group("Grower", "Buyer", columns, rows), source_name="mg.yxmd")
    row = report_row(result, 762)
    solver = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["graph_solver_input"]
    assert solver["output_column_name"] == "__make_group_component_1"


def test_make_group_refuses_a_key_carrying_a_backslash():
    """A backslash cannot be written into the generated code as a literal, so the mapper stops."""
    result = convert_yxmd(_make_group("C:\\", "Buyer", ONE_COLUMN, ONE_COLUMN_ROWS), source_name="mg.yxmd")
    row = report_row(result, 762)
    assert (row.status, row.reason, row.flowfile_node_type) == ("placeholder", "option_unsupported", "polars_code")


MAKE_GROUP_FEEDING_A_MULTI_FIELD_FORMULA = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="771"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /><Field name="Buyer" /></Fields>
        <Data><r><c>Okonjo</c><c>Salgado</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="772"><GuiSettings Plugin="AlteryxBasePluginsGui.MakeGroup.MakeGroup" />
      <Properties><Configuration>
        <Key1st>Grower</Key1st>
        <Key2nd>Buyer</Key2nd>
      </Configuration></Properties></Node>
    <Node ToolID="773"><GuiSettings Plugin="AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula" />
      <Properties><Configuration>
        <FieldType>Text</FieldType>
        <Fields><Field name="Key" /><Field name="Group" /><Field name="*Unknown" /></Fields>
        <CopyOutput value="False" />
        <Expression>[_CurrentFieldName_] + "=" + [_CurrentField_]</Expression>
        <ChangeFieldType value="False" />
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="771" Connection="Output" />
      <Destination ToolID="772" Connection="Input" /></Connection>
    <Connection><Origin ToolID="772" Connection="Output" />
      <Destination ToolID="773" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_consumer_of_make_group_is_told_the_alteryx_column_names():
    """[_CurrentFieldName_] over *Unknown converts only when the upstream columns are known."""
    result = convert_yxmd(MAKE_GROUP_FEEDING_A_MULTI_FIELD_FORMULA, source_name="mg_mff.yxmd")
    row = report_row(result, 773)
    assert (row.status, row.reason) == ("converted", "converted"), row.messages
    assert not any("Flowfile cannot see" in message for message in row.messages)


@pytest.fixture()
def field_info() -> ConversionResult:
    return convert("field_info.yxmd")


def field_info_body(result: ConversionResult, tool_id: int) -> str:
    row = report_row(result, tool_id)
    return dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]


FIELD_INFO_FRAME = pl.LazyFrame(
    {"CrateId": [1, 2], "Grower": ["Okonjo", "Salgado"], "PickedOn": [None, None]},
    schema={"CrateId": pl.Int32, "Grower": pl.String, "PickedOn": pl.Datetime("us")},
)


def test_field_info_emits_the_input_schema_as_rows(field_info: ConversionResult):
    row = report_row(field_info, 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "polars_code")
    result = polars_code_parser.get_executable(field_info_body(field_info, 2), 1)(FIELD_INFO_FRAME).collect()
    assert result.columns == ["Name", "Type"]
    assert result.to_dicts() == [
        {"Name": "CrateId", "Type": "Int32"},
        {"Name": "Grower", "Type": "String"},
        {"Name": "PickedOn", "Type": "Datetime(time_unit='us', time_zone=None)"},
    ]


def test_field_info_names_the_columns_alteryx_has_and_polars_does_not(field_info: ConversionResult):
    message = report_row(field_info, 2).messages[0]
    for column in ("Size", "Scale", "Source", "Description"):
        assert column in message, column
    assert "Polars type name" in message


def test_field_info_reads_the_schema_without_reading_a_row(field_info: ConversionResult):
    """An empty frame still has a schema, which is the whole output of this tool."""
    executable = polars_code_parser.get_executable(field_info_body(field_info, 2), 1)
    empty = executable(FIELD_INFO_FRAME.filter(pl.col("CrateId") < 0)).collect()
    assert empty["Name"].to_list() == ["CrateId", "Grower", "PickedOn"]


def test_field_info_is_wired_at_both_ends_and_names_its_two_columns(field_info: ConversionResult):
    """One consumer pins all three registrations: the output anchor, the input anchor and the columns.

    The Multi-Field Formula behind it binds [_CurrentFieldName_] over `*Unknown`, which converts
    only when the upstream column names are known — so its status is the record of `tool_columns`.
    """
    rows = {row.alteryx_tool_id: row for row in field_info.report.rows}
    consumer = rows[6]
    # register_all_outputs: the consumer is wired to the Field Info node, not to what fed it.
    assert dumped_nodes(field_info)[consumer.flowfile_node_ids[0]]["input_ids"] == rows[2].flowfile_node_ids
    # register_all_inputs: the Text Input's wire reached a node, so nothing was reported dropped.
    assert (rows[1].status, rows[1].reason) == ("converted", "converted")
    assert not any("dropped" in message for row in field_info.report.rows for message in row.messages)
    # tool_columns: the consumer could only convert because it was told Name and Type.
    assert (consumer.status, consumer.reason) == ("converted", "converted"), consumer.messages
    assert "String columns" in consumer.messages[0]


@pytest.mark.parametrize(("tool_id", "inputs"), [(3, 0), (5, 2)])
def test_field_info_fails_closed_unless_exactly_one_input_arrives(
    field_info: ConversionResult, tool_id: int, inputs: int
):
    row = report_row(field_info, tool_id)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert f"has {inputs} inputs" in row.messages[0]
    body = field_info_body(field_info, tool_id)
    assert "collect_schema" not in body
    assert body.endswith("output_df = pl.DataFrame()" if inputs == 0 else "output_df = input_df_1")


FIELD_INFO_WITH_OPTIONS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.FieldInfo.FieldInfo" />
      <Properties><Configuration><OutputFields>Name</OutputFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_field_info_configuration_flowfile_cannot_read_fails_closed():
    result = convert_yxmd(FIELD_INFO_WITH_OPTIONS, source_name="field_info.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "OutputFields" in row.messages[0]
    assert field_info_body(result, 2).endswith("output_df = input_df")


API_OUTPUT_WITH_OPTIONS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.APIOutput.APIOutput" />
      <Properties><Configuration><ResponseFormat>xml</ResponseFormat></Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


def test_api_output_becomes_the_api_response_node():
    result = convert("api_output.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("converted", "converted", "api_response")
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]
    assert (settings["orientation"], settings["max_rows"]) == ("records", None)


def test_api_output_configuration_flowfile_does_not_read_is_named():
    row = convert_yxmd(API_OUTPUT_WITH_OPTIONS, source_name="api.yxmd").report.rows[0]
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "api_response")
    assert "ResponseFormat" in row.messages[0]


def test_the_api_response_flow_runs_and_returns_its_input(tmp_path: Path):
    result = convert("api_output.yxmd")
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]
    response = flow.get_node(report_row(result, 2).flowfile_node_ids[0]).get_resulting_data().data_frame.collect()
    assert response.columns == ["CrateId", "Grower"]
    assert response["Grower"].to_list() == ["Okonjo", "Salgado"]


def test_resolve_output_returns_an_unaliased_anchor_and_refuses_a_cycle():
    from flowfile_core.flowfile.converters.alteryx.mappers import EmitContext

    ctx = EmitContext()
    assert ctx.resolve_output(7, "Output") == (7, "Output")
    ctx.alias_output(7, "Output", 8, "Output")
    assert ctx.resolve_output(7, "Output") == (8, "Output")
    ctx.alias_output(8, "Output", 7, "Output")
    assert ctx.resolve_output(7, "Output") is None


def test_out_of_scope_tools_leave_the_in_scope_denominator(out_of_scope: ConversionResult):
    coverage = out_of_scope.report.coverage
    # 8 tools, 6 of them settled non-goals: only TextInput and DateTime are Flowfile's to convert.
    assert (coverage.tools, coverage.in_scope, coverage.mapped) == (8, 2, 1)
    assert (coverage.mapped_percent, coverage.in_scope_percent) == (12, 50)
    assert "1 of 8 Alteryx tools reached a Flowfile node (12% of all tools)" in coverage.definition
    assert "of the 2 tools Flowfile aims to convert, 50%" in coverage.definition
    assert "1 of the mapped tools need no manual work" in coverage.definition


def test_the_definition_sentence_reads_as_english_on_a_one_tool_canvas():
    from flowfile_core.flowfile.converters.alteryx.report import ToolReportRow, build_coverage

    row = ToolReportRow(alteryx_tool_id=1, alteryx_tool="Filter", status="converted", reason="converted")
    definition = build_coverage([row]).definition
    assert "1 of 1 Alteryx tool reached a Flowfile node" in definition
    assert "of the 1 tool Flowfile aims to convert, 100%" in definition


FAILURE_REASONS = frozenset(
    {
        "translator_refused",
        "option_unsupported",
        "connection_string",
        "dropped_connection",
        "file_format",
        "row_order_unknown",
        "unmapped_tool",
        "mapper_refused",
        "no_op",
        *(f"scope:{bucket}" for bucket in BUCKETS if bucket != "no_op"),
    }
)


def generated_bodies(result: ConversionResult) -> list[str]:
    """Every `polars_code` body the conversion emitted."""
    return [
        node["setting_input"]["polars_code_input"]["polars_code"]
        for node in node_of_type(dumped_nodes(result), "polars_code")
    ]


@pytest.mark.parametrize("fixture", CONVERTIBLE_FIXTURES)
def test_every_generated_body_is_parseable_python(fixture: str):
    """An XML string that reached a `#` line unsanitised would show up here as a SyntaxError."""
    for body in generated_bodies(convert(fixture)):
        ast.parse(body)


INJECTED_MARKER = "INJECTED_MARKER"


def test_a_line_break_in_xml_cannot_escape_a_generated_comment():
    """The marker may appear in a comment or inside a quoted value, but never as code.

    Had a line break escaped its `#` line, `INJECTED_MARKER = 4` would parse as an assignment,
    so looking for it among the tree's names is what distinguishes injected code from data.
    """
    bodies = generated_bodies(convert("injected_comments.yxmd"))
    assert bodies, "the fixture must emit generated code for this to prove anything"
    for body in bodies:
        names = {node.id for node in ast.walk(ast.parse(body)) if isinstance(node, ast.Name)}
        assert not any(INJECTED_MARKER in name for name in names), f"marker became code:\n{body}"
    # The marker really does reach the generated text, so the assertion above is not vacuous.
    assert any(INJECTED_MARKER in body for body in bodies)


def test_the_injected_fixture_still_dispatches_to_the_real_mappers():
    rows = {row.alteryx_tool_id: row for row in convert("injected_comments.yxmd").report.rows}
    assert rows[604].status == "converted" and rows[604].alteryx_tool == "TextToColumns"
    assert rows[603].status == "converted" and rows[603].alteryx_tool == "RecordID"


BACKSLASH_NAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextToColumns.TextToColumns" />
      <Properties><Configuration>
        <Field>a\\</Field>
        <Delimeters value="-\\s" />
        <NumFields value="1" />
        <Flags value="0" />
      </Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


def test_a_name_ending_in_a_backslash_is_refused_instead_of_generated():
    # repr() doubles the trailing backslash, which desynchronises the code node's comment
    # stripper and truncates the line into a SyntaxError when the flow runs.
    row = convert_yxmd(BACKSLASH_NAME, source_name="backslash.yxmd").report.rows[0]
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "ends with a backslash" in row.messages[0]


def test_a_backslash_that_is_not_trailing_still_converts():
    row = convert_yxmd(
        BACKSLASH_NAME.replace(b"<Field>a\\</Field>", b"<Field>a\\b</Field>"), source_name="backslash.yxmd"
    ).report.rows[0]
    assert row.status == "converted"


REGEX_REPLACE = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.RegEx.RegEx" />
      <Properties><Configuration>
        <Field>path</Field>
        <RegExExpression value="^/" />
        <Method>Replace</Method>
        <Replace expression="%s"><CopyUnmatched value="True" /></Replace>
      </Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


def test_a_regex_replacement_ending_in_a_backslash_is_refused():
    # The replacement is repr'd into the generated code like the pattern, so it needs the same screen.
    row = convert_yxmd(REGEX_REPLACE % b"C:\\", source_name="regex.yxmd").report.rows[0]
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "the replacement 'C:\\'" in row.messages[0] and "ends with a backslash" in row.messages[0]


def test_a_regex_replacement_with_an_inner_backslash_still_generates_code():
    # RegEx rows are always partial (the "verify the output" caveat); the point is that code was emitted.
    row = convert_yxmd(REGEX_REPLACE % b"C:\\x", source_name="regex.yxmd").report.rows[0]
    assert (row.status, row.flowfile_node_type) == ("partial", "polars_code")
    assert not any("ends with a backslash" in message for message in row.messages)


@pytest.mark.parametrize("fixture", CONVERTIBLE_FIXTURES)
def test_every_row_carries_a_reason_that_matches_its_status(fixture: str):
    """A reason is only worth having if it cannot contradict the status beside it."""
    for row in convert(fixture).report.rows:
        assert row.reason, row.alteryx_tool
        if row.entity == "annotation":
            assert row.reason == "annotation", row.alteryx_tool
        elif row.status == "converted":
            assert row.reason in ("converted", "viewer"), (row.alteryx_tool, row.reason)
        else:
            assert row.reason in FAILURE_REASONS, (row.alteryx_tool, row.status, row.reason)


def test_coverage_reports_both_percentages_and_its_own_definition():
    coverage = convert("unsupported.yxmd").report.coverage
    assert (coverage.tools, coverage.mapped, coverage.converted) == (4, 2, 2)
    assert (coverage.mapped_percent, coverage.converted_percent) == (50, 50)
    assert "2 of 4 Alteryx tools" in coverage.definition
    assert "50%" in coverage.definition


def test_a_dropped_connection_downgrades_both_ends_to_partial():
    result = convert_yxmd(DROPPED_CONNECTION, source_name="inline.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[1].status == "partial"
    assert rows[2].status == "partial"
    for row in (rows[1], rows[2]):
        assert any("was dropped" in message for message in row.messages)
    # Still mapped — the tools converted, the wire did not.
    assert result.report.coverage.mapped == 2
    assert result.report.coverage.converted == 0


def test_a_comment_only_workflow_imports_as_a_flow_with_no_nodes():
    result = convert("zero_tools.yxmd")
    assert result.flow_data.nodes == []
    assert [comment.text for comment in result.flow_data.comments] == ["Workflow still to be built."]
    report = result.report
    assert (report.total_tools, report.total_annotations) == (0, 1)
    assert report.coverage.tools == 0
    assert report.coverage.mapped_percent == 0


def test_text_box_becomes_a_canvas_comment(containers: ConversionResult):
    row = next(row for row in containers.report.rows if row.alteryx_tool_id == 4)
    assert row.status == "converted"
    assert row.flowfile_node_type == "comment"
    assert row.flowfile_node_ids == []

    comments = containers.flow_data.comments
    assert [comment.text for comment in comments] == ["Remember to raise the threshold before the quarterly run."]
    comment = comments[0]
    # Same origin and scale as the tools (the fixture's top-left tool sits at 54/54).
    assert (comment.x_position, comment.y_position) == (round((450 - 54) * 3.0) + 60, round((78 - 54) * 3.0) + 100)
    assert (comment.width, comment.height) == (120 * 3.0, 60 * 3.0)


def test_comments_survive_the_yaml_round_trip(containers: ConversionResult, tmp_path: Path):
    flow = open_flow(write_flow(containers, tmp_path / "flow.yaml"))
    assert [comment.text for comment in flow._comments.values()] == [
        "Remember to raise the threshold before the quarterly run."
    ]


def test_empty_text_box_is_skipped_not_imported_blank():
    result = convert_yxmd(TEXT_BOXES, source_name="inline.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[2].status == "skipped"
    assert rows[3].status == "converted"
    assert [comment.text for comment in result.flow_data.comments] == ["First line\nsecond line"]
    assert (result.report.total_tools, result.report.total_annotations) == (1, 2)
    assert (result.report.converted, result.report.skipped) == (1, 0)
    # A text box without a size gets a readable minimum instead of a zero box.
    assert (result.flow_data.comments[0].width, result.flow_data.comments[0].height) == (120, 40)


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


@pytest.fixture()
def multi_field_formula() -> ConversionResult:
    """Alteryx's own Multi-Field Formula sample, read from the private corpus and never committed."""
    return convert_yxmd(CORPUS_MULTI_FIELD_FORMULA.read_bytes(), source_name="Multi-Field_Formula.yxmd")


@pytest.fixture()
def multi_field_formula_runs() -> ConversionResult:
    return convert("multi_field_formula_runs.yxmd")


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


def multi_field_settings(result: ConversionResult, tool_id: int) -> dict:
    row = report_row(result, tool_id)
    assert row.flowfile_node_type == "multi_field_formula", row.messages
    assert len(row.flowfile_node_ids) == 1
    return dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["multi_field_formula_input"]


def test_multi_field_formula_becomes_one_node_over_the_selected_fields(regex_and_multifield: ConversionResult):
    """A partially selected picker keeps the placeholder expression on a single node."""
    row = report_row(regex_and_multifield, 3)
    assert row.status == "converted"
    assert len(row.flowfile_node_ids) == 1
    settings = multi_field_settings(regex_and_multifield, 3)
    assert settings["formula"] == '[_CurrentField_] = "Y"'
    assert settings["selection_mode"] == "list"
    assert settings["selected_columns"] == ["flag_a", "flag_b"]
    assert settings["output_mode"] == "replace"
    assert settings["output_data_type"] == "Auto"


@needs_corpus
def test_multi_field_replace_over_a_data_type_keeps_the_placeholder(multi_field_formula: ConversionResult):
    """Tool 36: every text field, in place, no type change."""
    row = report_row(multi_field_formula, 36)
    assert row.status == "converted"
    assert row.messages[0] == "Mapped onto one multi-field formula node over String columns."
    settings = multi_field_settings(multi_field_formula, 36)
    assert settings["formula"] == "uppercase([_CurrentField_])"
    assert settings["selection_mode"] == "data_type"
    assert settings["selected_data_type"] == "String"
    assert settings["selected_columns"] == []
    assert settings["output_mode"] == "replace"
    assert (settings["output_prefix"], settings["output_suffix"]) == ("", "")
    assert settings["output_data_type"] == "Auto"


@needs_corpus
def test_multi_field_copy_output_becomes_a_prefixed_new_column_mode(multi_field_formula: ConversionResult):
    """Tool 37: the same selection, written to New_-prefixed copies."""
    row = report_row(multi_field_formula, 37)
    assert row.status == "converted"
    settings = multi_field_settings(multi_field_formula, 37)
    assert settings["output_mode"] == "new"
    assert settings["output_prefix"] == "New_"
    assert settings["output_suffix"] == ""
    assert settings["selection_mode"] == "data_type"
    assert settings["selected_data_type"] == "String"


@needs_corpus
def test_multi_field_suffix_keeps_its_leading_space_and_the_trailing_space_column(
    multi_field_formula: ConversionResult,
):
    """Tool 38: an explicit 12-column list, a ' % Total' suffix and a FixedDecimal output type."""
    row = report_row(multi_field_formula, 38)
    assert row.status == "converted"
    settings = multi_field_settings(multi_field_formula, 38)
    assert settings["selection_mode"] == "list"
    assert settings["selected_columns"] == [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August ",
        "September",
        "October",
        "November",
        "December",
    ]
    # The translator normalises operator spacing; the raw Alteryx text is `[_CurrentField_]/[Total ]*100`.
    assert settings["formula"] == "[_CurrentField_] / [Total ] * 100"
    assert settings["output_mode"] == "new"
    assert settings["output_suffix"] == " % Total"
    assert settings["output_prefix"] == ""
    assert settings["output_data_type"] == "Float64"
    assert any("unknown at design time" in message for message in row.messages)
    assert any("FixedDecimal" in message for message in row.messages)


@needs_corpus
def test_every_multi_field_formula_tool_in_the_reference_workflow_converts(multi_field_formula: ConversionResult):
    rows = [row for row in multi_field_formula.report.rows if row.alteryx_tool == "MultiFieldFormula"]
    assert [row.alteryx_tool_id for row in rows] == [36, 37, 38]
    assert {row.status for row in rows} == {"converted"}
    assert {row.flowfile_node_type for row in rows} == {"multi_field_formula"}


def test_untranslatable_multi_field_expression_is_commented_onto_an_identity_stub(
    multi_field_formula_runs: ConversionResult,
):
    row = report_row(multi_field_formula_runs, 5)
    assert row.status == "commented"
    assert any("REGEX_Match" in message and "kept as a comment" in message for message in row.messages)
    nodes = dumped_nodes(multi_field_formula_runs)
    node = nodes[row.flowfile_node_ids[0]]
    assert node["description"].startswith("⚠")
    formula = node["setting_input"]["multi_field_formula_input"]["formula"]
    assert formula.startswith("// Alteryx formula could not be converted automatically:")
    assert '// Original: REGEX_Match([_CurrentField_], "^A.*")' in formula
    assert formula.endswith("[_CurrentField_]")
    # The stub has to stay a parseable formula, otherwise the node would not run.
    simple_function_to_expr(formula)


def test_imported_multi_field_formula_flow_runs(tmp_path: Path, multi_field_formula_runs: ConversionResult):
    """The hand-written TextInput fixture must execute: replace, prefix, suffix+cast and the stub."""
    flow = open_flow(write_flow(multi_field_formula_runs, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]

    data = flow.get_node(5).get_resulting_data().data_frame.collect()
    assert data.columns == ["name", "city", "jan", "Total ", "New_name", "New_city", "jan % Total"]
    # Tool 2 overwrote in place; tool 5's comment stub left the same column untouched.
    assert data["name"].to_list() == ["ANN", "BOB"]
    assert data["city"].to_list() == ["ROME", "OSLO"]
    # [_CurrentFieldName_] binds the column name as a literal.
    assert data["New_name"].to_list() == ["name=ANN", "name=BOB"]
    assert data["New_city"].to_list() == ["city=ROME", "city=OSLO"]
    # The FixedDecimal output type lands as Float64 and the ' % Total' suffix keeps its space.
    assert data["jan % Total"].to_list() == [25.0, 75.0]
    assert data.schema["jan % Total"] == pl.Float64


MULTI_FIELD_QUOTED_NAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula" />
      <Properties><Configuration>
        <Fields>
          <Field name="plain" selected="True" />
          <Field name="say &quot;hi&quot;" selected="%s" />
        </Fields>
        <FieldType>String</FieldType>
        <CopyOutput value="False" />
        <Expression>[_CurrentFieldName_] + "=" + [_CurrentField_]</Expression>
      </Configuration></Properties></Node>
  </Nodes>
</AlteryxDocument>
"""


def test_multi_field_formula_refuses_a_quoted_field_name_at_import():
    row = convert_yxmd(MULTI_FIELD_QUOTED_NAME % b"True", source_name="mff.yxmd").report.rows[0]
    # Without this the node imports cleanly and bind_multi_field_formula raises when the flow runs.
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert "double quote" in row.messages[0]


MULTI_FIELD_UNKNOWN_UPSTREAM = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="plain" /><Field name="say &quot;hi&quot;" /></Fields>
        <Data><r><c>1</c><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula" />
      <Properties><Configuration>
        <Fields><Field name="plain" selected="True" /><Field name="*Unknown" selected="True" /></Fields>
        <FieldType>String</FieldType>
        <CopyOutput value="False" />
        <Expression>[_CurrentFieldName_] + "=" + [_CurrentField_]</Expression>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="%s" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_multi_field_formula_checks_upstream_columns_when_unknown_is_selected():
    """*Unknown means the node also runs over columns the tool's own field list never named."""
    row = report_row(convert_yxmd(MULTI_FIELD_UNKNOWN_UPSTREAM % b"1", source_name="mff.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert "double quote" in row.messages[0]


def test_multi_field_formula_admits_when_the_upstream_columns_are_unknown():
    # Origin 9 does not exist, so nothing upstream is known and the check cannot be made.
    row = report_row(convert_yxmd(MULTI_FIELD_UNKNOWN_UPSTREAM % b"9", source_name="mff.yxmd"), 2)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("Flowfile cannot see" in message for message in row.messages)


# Tool 9 is a Sort with no input, so what leaves it is unknown without dropping any wire.
MULTI_FIELD_DESELECTED_FIELD = MULTI_FIELD_UNKNOWN_UPSTREAM.replace(
    b'<Fields><Field name="plain" selected="True" /><Field name="*Unknown" selected="True" /></Fields>',
    b'<Fields><Field name="plain" selected="True" /><Field name="other" selected="False" />'
    b'<Field name="*Unknown" selected="True" /></Fields>',
).replace(
    b"  </Nodes>",
    b"""    <Node ToolID="9"><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration><SortInfo locale="0"><Field field="plain" order="Asc" /></SortInfo>
      </Configuration></Properties></Node>
  </Nodes>""",
)


@pytest.mark.parametrize("origin", [b"1", b"9"])
def test_a_deselected_field_puts_the_node_in_list_mode_so_only_the_list_is_checked(origin: bytes):
    """A deselected field means the node lists its columns; *Unknown then widens nothing it touches.

    Origin 1 carries a double-quoted column the list never names; origin 9's columns are unknown.
    Data-type mode refused the first and downgraded the second; list mode has no reason to do either.
    """
    result = convert_yxmd(MULTI_FIELD_DESELECTED_FIELD % origin, source_name="mff.yxmd")
    row = report_row(result, 2)
    assert row.status == "converted", row.messages
    assert not any("double quote" in message for message in row.messages)
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["multi_field_formula_input"]
    assert (settings["selection_mode"], settings["selected_columns"]) == ("list", ["plain"])
    assert not any("Flowfile cannot see" in message for message in row.messages)


def test_multi_field_formula_ignores_a_quoted_field_it_does_not_touch():
    row = convert_yxmd(MULTI_FIELD_QUOTED_NAME % b"False", source_name="mff.yxmd").report.rows[0]
    assert row.status == "converted"


def test_multi_field_formula_runs_fixture_report_counts(multi_field_formula_runs: ConversionResult):
    report = multi_field_formula_runs.report
    assert report.total_tools == 8
    assert (report.converted, report.commented) == (4, 3)
    assert (report.partial, report.placeholder, report.skipped) == (1, 0, 0)


MULTI_FIELD_PLUGIN = "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula"
ALL_FIELDS = '<Field name="name" /><Field name="city" /><Field name="qty" /><Field name="*Unknown" />'


def multi_field_after_text_input(config: str) -> bytes:
    """A Text Input (two text columns and a numeric one) feeding one Multi-Field Formula tool."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="name" /><Field name="city" /><Field name="qty" type="Int32" /></Fields>
        <Data><r><c>ann</c><c>rome</c><c>2</c></r><r><c>bob</c><c>oslo</c><c>4</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="{MULTI_FIELD_PLUGIN}" />
      <Properties><Configuration>{config}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def multi_field_inline(config: str) -> ConversionResult:
    return convert_yxmd(multi_field_after_text_input(config), source_name="inline.yxmd")


def test_all_types_picker_becomes_all_columns_mode():
    result = multi_field_inline(
        f"<FieldType>All</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" /><Expression>ToString([_CurrentField_])</Expression>'
    )
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.messages == ["Mapped onto one multi-field formula node over all columns."]
    assert multi_field_settings(result, 2)["selection_mode"] == "all"


def test_unmapped_field_type_picker_falls_back_to_an_explicit_list():
    """An unrecognised FieldType token must not guess a group; it lists what it can see."""
    result = multi_field_inline(
        f"<FieldType>Spatial</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" /><Expression>ToString([_CurrentField_])</Expression>'
    )
    row = report_row(result, 2)
    settings = multi_field_settings(result, 2)
    assert settings["selection_mode"] == "list"
    assert settings["selected_columns"] == ["name", "city", "qty"]
    assert any("unknown at design time" in message for message in row.messages)


def test_legacy_output_suffix_tag_keeps_its_leading_space():
    """`OutputSuffix` is read raw like `NewFieldAddOn`; a stripped suffix would rename the column."""
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="True" /><OutputSuffix> % Total</OutputSuffix>'
        "<Expression>ToString([_CurrentField_])</Expression>"
    )
    settings = multi_field_settings(result, 2)
    assert settings["output_mode"] == "new"
    assert settings["output_suffix"] == " % Total"
    assert settings["output_prefix"] == ""


def test_add_on_without_a_position_defaults_to_a_prefix():
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="True" /><NewFieldAddOn>New_</NewFieldAddOn>'
        "<Expression>ToString([_CurrentField_])</Expression>"
    )
    settings = multi_field_settings(result, 2)
    assert (settings["output_prefix"], settings["output_suffix"]) == ("New_", "")


def test_add_on_position_is_matched_case_insensitively():
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="True" /><NewFieldAddOn>_new</NewFieldAddOn>'
        "<NewFieldAddOnPos>SUFFIX</NewFieldAddOnPos>"
        "<Expression>ToString([_CurrentField_])</Expression>"
    )
    settings = multi_field_settings(result, 2)
    assert (settings["output_prefix"], settings["output_suffix"]) == ("", "_new")


def test_copy_output_without_an_add_on_becomes_a_placeholder():
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="True" /><NewFieldAddOn />'
        "<Expression>ToString([_CurrentField_])</Expression>"
    )
    row = report_row(result, 2)
    assert row.status == "placeholder"
    assert any("names are not recorded in the workflow" in message for message in row.messages)


def test_mapped_output_type_is_applied_without_a_caveat():
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" /><Expression>ToString([_CurrentField_])</Expression>'
        '<ChangeFieldType value="True" /><OutputFieldType type="Int32" />'
    )
    row = report_row(result, 2)
    assert multi_field_settings(result, 2)["output_data_type"] == "Int32"
    assert row.messages == ["Mapped onto one multi-field formula node over String columns."]


def test_unmappable_output_type_keeps_auto_and_says_so():
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" /><Expression>ToString([_CurrentField_])</Expression>'
        '<ChangeFieldType value="True" /><OutputFieldType type="SpatialObj" />'
    )
    row = report_row(result, 2)
    assert multi_field_settings(result, 2)["output_data_type"] == "Auto"
    assert row.messages[1] == (
        "Alteryx output type 'SpatialObj' has no Flowfile equivalent; the type the expression produces is kept."
    )


def test_a_deliberate_retype_to_text_is_honoured():
    """Only a Text selection echoing its own type is treated as a no-op; a Numeric one is a real cast."""
    result = multi_field_inline(
        f"<FieldType>Numeric</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" /><Expression>ToString([_CurrentField_])</Expression>'
        '<ChangeFieldType value="True" /><OutputFieldType type="V_String" size="254" />'
    )
    row = report_row(result, 2)
    assert multi_field_settings(result, 2)["output_data_type"] == "String"
    assert not any("keeps the type the expression produces" in message for message in row.messages)


def test_declared_string_output_type_is_ignored(price_paid: ConversionResult):
    """price_paid tool 7 is the echo case: a Text selection whose OutputFieldType repeats V_String."""
    settings = multi_field_settings(price_paid, 7)
    assert settings["output_data_type"] == "Auto"
    assert settings["selected_columns"] == ["Old/New"]
    assert settings["formula"] == '[_CurrentField_] = "Y"'
    assert report_row(price_paid, 7).messages[1] == (
        "Alteryx writes the result into a 'V_String' field; Flowfile keeps the type the expression produces."
    )


def test_current_field_type_placeholder_downgrades_the_row_to_partial():
    """The placeholder converts verbatim but yields Polars type names, so the row carries a caveat."""
    result = multi_field_inline(
        f"<FieldType>Text</FieldType><Fields>{ALL_FIELDS}</Fields>"
        '<CopyOutput value="False" />'
        '<Expression>IIF([_CurrentFieldType_] = "V_WString", "text", "other")</Expression>'
    )
    row = report_row(result, 2)
    assert row.status == "partial"
    assert row.messages[1] == (
        "Alteryx's [_CurrentFieldType_] yields Alteryx type names (V_WString, Double, Bool…); "
        "Flowfile's yields Polars names (String, Float64, Boolean…) — review comparisons against type literals."
    )


def test_commented_stub_drops_the_declared_output_cast(tmp_path: Path, multi_field_formula_runs: ConversionResult):
    """Tool 8: an Int32 cast on an identity stub over text would only fail at collect time."""
    row = report_row(multi_field_formula_runs, 8)
    assert row.status == "commented"
    assert multi_field_settings(multi_field_formula_runs, 8)["output_data_type"] == "Auto"
    assert "Alteryx output type 'Int32' is not applied until the expression is rebuilt." in row.messages

    flow = open_flow(write_flow(multi_field_formula_runs, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]
    data = flow.get_node(8).get_resulting_data().data_frame.collect()
    assert data["city"].to_list() == ["ROME", "OSLO"]


def test_current_field_type_and_record_id_tools_run(tmp_path: Path, multi_field_formula_runs: ConversionResult):
    """Tool 6 keeps `[_CurrentFieldType_]`; tool 7's rejected `[_RecordID_]` becomes an identity stub."""
    assert report_row(multi_field_formula_runs, 6).status == "partial"
    record_id_row = report_row(multi_field_formula_runs, 7)
    assert record_id_row.status == "commented"
    stub = multi_field_settings(multi_field_formula_runs, 7)["formula"]
    assert "_RecordID_" in stub
    assert stub.endswith("[_CurrentField_]")

    flow = open_flow(write_flow(multi_field_formula_runs, tmp_path / "flow.yaml"))
    assert flow.run_graph().success
    data = flow.get_node(8).get_resulting_data().data_frame.collect()
    assert data["type_name"].to_list() == ["String", "String"]
    assert data["name"].to_list() == ["ANN", "BOB"]


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


def test_placeholders_embed_the_original_configuration_and_annotation(unsupported: ConversionResult):
    row = report_row(unsupported, 2)
    assert row.status == "placeholder"
    code = dumped_nodes(unsupported)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "# Alteryx annotation: Parse the date column" in code
    assert "<OutputFieldName>DateTime_Out</OutputFieldName>" in code
    assert code.splitlines()[-1] == "output_df = input_df"


def test_year_function_now_converts(price_paid: ConversionResult):
    row = report_row(price_paid, 6)
    assert row.status == "converted"
    settings = dumped_nodes(price_paid)[row.flowfile_node_ids[0]]["setting_input"]
    assert settings["filter_input"]["advanced_filter"] == "year([Date of Transfer]) = 2016"


def test_price_paid_workflow_converts_without_any_placeholder(price_paid: ConversionResult):
    assert [row.alteryx_tool for row in price_paid.report.rows if row.status == "placeholder"] == []
    writer = report_row(price_paid, 8)
    assert (writer.status, writer.flowfile_node_type) == ("partial", "output")


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

    writer = next(node for node in price_paid.flow_data.nodes if node.type == "output")
    writer.setting_input.output_settings.directory = str(tmp_path)

    flow = open_flow(write_flow(price_paid, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step for step in run_info.node_step_result if not step.success]

    # The Alteryx workflow ends in a .yxdb writer; the converted flow really writes its Parquet twin.
    written = tmp_path / "2016PPData.parquet"
    assert written.exists()
    frame = pl.read_parquet(written)
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


# Configurations copied from the corpus: 02 Preparation/Record_ID.yxmd tools 124, 125 and 138.
RECORD_ID_VARIANTS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="Region" /><Field name="Spend" /></Fields>
        <Data><r><c>east</c><c>1</c></r><r><c>east</c><c>2</c></r><r><c>west</c><c>3</c></r>
        </Data></Configuration></Properties></Node>
    <Node ToolID="124"><GuiSettings Plugin="AlteryxBasePluginsGui.RecordID.RecordID" />
      <Properties><Configuration><FieldName>Record ID</FieldName><StartValue>1</StartValue>
        <FieldType>Int64</FieldType><FieldSize>6</FieldSize><Position>0</Position>
      </Configuration></Properties></Node>
    <Node ToolID="125"><GuiSettings Plugin="AlteryxBasePluginsGui.RecordID.RecordID" />
      <Properties><Configuration><FieldName>Record ID Last Column</FieldName><StartValue>-100</StartValue>
        <FieldType>Int32</FieldType><FieldSize>6</FieldSize><Position>1</Position>
      </Configuration></Properties></Node>
    <Node ToolID="138"><GuiSettings Plugin="AlteryxBasePluginsGui.RecordID.RecordID" />
      <Properties><Configuration><FieldName>Grouped Record ID</FieldName><StartValue>1</StartValue>
        <FieldType>Int32</FieldType><FieldSize>6</FieldSize><Position>1</Position>
        <GroupFields orderChanged="False"><Field name="Region" /></GroupFields>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="124" Connection="Input" /></Connection>
    <Connection><Origin ToolID="124" Connection="Output" /><Destination ToolID="125" Connection="Input" /></Connection>
    <Connection><Origin ToolID="125" Connection="Output" /><Destination ToolID="138" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


@pytest.fixture()
def record_id_variants() -> ConversionResult:
    return convert_yxmd(RECORD_ID_VARIANTS, source_name="record_id.yxmd")


@pytest.mark.parametrize(("tool_id", "node_type"), [(124, "record_id"), (125, "polars_code"), (138, "polars_code")])
def test_record_id_uses_generated_code_only_where_the_node_cannot_express_it(
    record_id_variants: ConversionResult, tool_id: int, node_type: str
):
    row = report_row(record_id_variants, tool_id)
    assert (row.status, row.flowfile_node_type) == ("converted", node_type)


def test_record_id_runs_with_a_negative_start_and_a_group(tmp_path: Path, record_id_variants: ConversionResult):
    flow = open_flow(write_flow(record_id_variants, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, run_info
    last = report_row(record_id_variants, 138).flowfile_node_ids[0]
    frame = flow.get_node(last).get_resulting_data().data_frame.collect()
    assert frame["Record ID"].to_list() == [1, 2, 3]
    # Alteryx counts from the configured start value, which Polars' row index cannot go below.
    assert frame["Record ID Last Column"].to_list() == [-100, -99, -98]
    assert frame["Grouped Record ID"].to_list() == [1, 2, 1]
    # Position=1 puts the new column last; Position=0 puts it first.
    assert frame.columns == ["Record ID", "Region", "Spend", "Record ID Last Column", "Grouped Record ID"]


def test_running_total_maps_to_window_functions(extra_tools: ConversionResult):
    row = report_row(extra_tools, 3)
    # A running total accumulates in row order, and nothing upstream of this tool states one.
    assert (row.status, row.reason) == ("partial", "row_order_unknown")
    assert "order the rows arrive" in row.messages[0]
    assert row.flowfile_node_type == "window_functions"
    window = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]["setting_input"]["window_input"]
    assert window["partition_by"] == ["region"]
    assert [(w["column"], w["function"], w["new_column_name"]) for w in window["window_functions"]] == [
        ("sales", "cum_sum", "RunTot_sales")
    ]


RUNNING_TOTAL_AFTER_SORT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo locale="0"><Field field="spend" order="Asc" /></SortInfo>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.RunningTotal.RunningTotal" />
      <Properties><Configuration>
        <GroupByFields><Field field="city" /></GroupByFields>
        <RunningTotalFields><Field field="spend" /></RunningTotalFields>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_running_total_stays_converted_when_a_sort_states_the_order():
    result = convert_yxmd(RUNNING_TOTAL_AFTER_SORT, source_name="running_total.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.messages) == ("converted", [])


RUNNING_TOTAL_SECOND_STREAM = RUNNING_TOTAL_AFTER_SORT.replace(
    b"  </Nodes>",
    b"""    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="city" /><Field name="spend" /></Fields>
        <Data><r><c>oslo</c><c>2</c></r></Data></Configuration></Properties>
    </Node>
  </Nodes>""",
).replace(
    b"  </Connections>",
    b"""    <Connection name="#2"><Origin ToolID="3" Connection="Output" />
      <Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>""",
)

RUNNING_TOTAL_AFTER_REFUSED_SORT = RUNNING_TOTAL_AFTER_SORT.replace(
    b'<SortInfo locale="0"><Field field="spend" order="Asc" /></SortInfo>', b'<SortInfo locale="0" />'
)


def test_a_second_unsorted_stream_keeps_the_running_total_partial():
    """One unsorted input is enough to make the accumulation order unstated again."""
    result = convert_yxmd(RUNNING_TOTAL_SECOND_STREAM, source_name="running_total.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("partial", "row_order_unknown")


def test_a_sort_the_importer_refused_does_not_state_an_order():
    result = convert_yxmd(RUNNING_TOTAL_AFTER_REFUSED_SORT, source_name="running_total.yxmd")
    assert report_row(result, 1).status != "converted", "the Sort must be refused for this to prove anything"
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("partial", "row_order_unknown")


# The same two tools with the Sort written after the Running Total it feeds.
RUNNING_TOTAL_SORT_LATER_IN_DOCUMENT = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.RunningTotal.RunningTotal" />
      <Properties><Configuration>
        <GroupByFields><Field field="city" /></GroupByFields>
        <RunningTotalFields><Field field="spend" /></RunningTotalFields>
      </Configuration></Properties>
    </Node>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration>
        <SortInfo locale="0"><Field field="spend" order="Asc" /></SortInfo>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_sort_mapped_later_in_the_document_still_states_the_order():
    """Mapping runs in document order; here the Sort is reached after the Running Total it feeds."""
    result = convert_yxmd(RUNNING_TOTAL_SORT_LATER_IN_DOCUMENT, source_name="running_total.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.messages) == ("converted", [])


def test_a_refused_sort_later_in_the_document_states_no_order():
    refused = RUNNING_TOTAL_SORT_LATER_IN_DOCUMENT.replace(
        b'<SortInfo locale="0"><Field field="spend" order="Asc" /></SortInfo>', b'<SortInfo locale="0" />'
    )
    result = convert_yxmd(refused, source_name="running_total.yxmd")
    assert report_row(result, 1).status != "converted", "the Sort must be refused for this to prove anything"
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("partial", "row_order_unknown")


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


def test_cross_tab_maps_to_pivot_plus_alteryx_rename(extra_tools: ConversionResult):
    row = report_row(extra_tools, 5)
    assert row.status == "converted"
    assert row.messages == []
    pivot, rename = (dumped_nodes(extra_tools)[node_id] for node_id in row.flowfile_node_ids)
    assert pivot["setting_input"]["pivot_input"] == {
        "index_columns": ["region"],
        "pivot_column": "product",
        "value_col": "sales",
        "aggregations": ["sum"],
    }
    assert rename["type"] == "polars_code"
    assert rename["input_ids"] == [pivot["id"]]
    code = rename["setting_input"]["polars_code_input"]["polars_code"]
    assert "_suffixes = [('', '')]" in code, "a lone Sum keeps the bare header"
    assert "output_df = input_df.rename(_renames)" in code


def cross_tab_after_text_input(fields: list[str], rows: list[tuple], config: str) -> bytes:
    """A Text Input with several typed-by-inference columns feeding one Cross Tab; None is an empty cell."""
    field_xml = "".join(f'<Field name="{name}" />' for name in fields)
    row_xml = "".join(
        "<r>" + "".join("<c />" if cell is None else f"<c>{cell}</c>" for cell in row) + "</r>" for row in rows
    )
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields>{field_xml}</Fields><Data>{row_xml}</Data></Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.CrossTab.CrossTab" />
      <Properties><Configuration>{config}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def run_cross_tab(document: bytes, tmp_path: Path) -> tuple[ConversionResult, pl.DataFrame | None, list]:
    """Convert, run, and return the Cross Tab's output frame (None when the run failed) plus the failures."""
    result = convert_yxmd(document, source_name="inline.yxmd")
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    failures = [step for step in run_info.node_step_result if not step.success]
    if failures:
        return result, None, failures
    last_node = report_row(result, 2).flowfile_node_ids[-1]
    return result, flow.get_node(last_node).get_resulting_data().data_frame.collect(), []


PRESIDENTS = ["presidentNo", "VicePresidentNo", "Pres_or_VP", "Name"]
PRESIDENT_ROWS = [
    (1, None, "President", "George Washington"),
    (1, 1, "Vice President", "John Adams"),
    (3, None, "President", "Thomas Jefferson"),
    (3, 1, "Vice President", "Aaron Burr"),
    (3, 2, "Vice President", "George Clinton"),
]

# Cross_Tab.yxmd tool 141: Concat with the separator Alteryx writes as ``,\s`` (comma + space).
CROSS_TAB_141 = (
    '<GroupFields><Field field="presidentNo" /></GroupFields><HeaderField field="Pres_or_VP" />'
    '<DataField field="Name" /><Methods><Method method="Concat" /><Separator>,\\s</Separator>'
    '<FieldSize value="2048" /></Methods>'
)
# Cross_Tab.yxmd tool 143: the header field is null for every president.
CROSS_TAB_143 = (
    '<GroupFields><Field field="presidentNo" /></GroupFields><HeaderField field="VicePresidentNo" />'
    '<DataField field="Name" /><Methods><Method method="Concat" /><Separator>,\\s</Separator>'
    '<FieldSize value="2048" /></Methods>'
)
# Cross_Tab.yxmd tool 214 and Transpose.yxmd tool 202: two methods on one data field.
CROSS_TAB_214 = (
    '<GroupFields><Field field="Suggested Age Range" /></GroupFields><HeaderField field="Category" />'
    '<DataField field="Value" /><Methods><Method method="Sum" /><Method method="Avg" /></Methods>'
)


def test_cross_tab_concat_joins_with_the_unescaped_separator(tmp_path: Path):
    result, frame, failures = run_cross_tab(
        cross_tab_after_text_input(PRESIDENTS, PRESIDENT_ROWS, CROSS_TAB_141), tmp_path
    )
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "polars_code"
    assert row.messages == [
        "Alteryx truncates the concatenated values at 2048 characters (FieldSize); "
        "Flowfile strings are unbounded, so the full values are kept."
    ]
    assert failures == []
    assert frame.columns == ["presidentNo", "Concat_President", "Concat_Vice_President"]
    assert frame.sort("presidentNo")["Concat_Vice_President"].to_list() == ["John Adams", "Aaron Burr, George Clinton"]


def test_a_cross_tab_concat_separator_ending_in_a_backslash_is_refused():
    # Alteryx writes a literal backslash as ``\\``; unescaped and repr'd it would sit before the closing quote.
    config = CROSS_TAB_141.replace("<Separator>,\\s</Separator>", "<Separator>\\\\</Separator>")
    assert config != CROSS_TAB_141
    row = report_row(
        convert_yxmd(cross_tab_after_text_input(PRESIDENTS, PRESIDENT_ROWS, config), source_name="c.yxmd"), 2
    )
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert "the Concat separator" in row.messages[0] and "ends with a backslash" in row.messages[0]


def test_cross_tab_null_header_gets_its_own_column(tmp_path: Path):
    _, frame, failures = run_cross_tab(cross_tab_after_text_input(PRESIDENTS, PRESIDENT_ROWS, CROSS_TAB_143), tmp_path)
    assert failures == []
    assert frame.columns == ["presidentNo", "Concat_1", "Concat_2", "Concat__Null_"]
    assert frame.sort("presidentNo")["Concat__Null_"].to_list() == ["George Washington", "Thomas Jefferson"]


def test_cross_tab_two_methods_prefix_the_alteryx_method_name(tmp_path: Path):
    rows = [
        ("Kids", "Fun and Games", 10),
        ("Kids", "Fun and Games", 30),
        ("Kids", "Educational", 5),
        ("Teens", "Educational", 7),
    ]
    result, frame, failures = run_cross_tab(
        cross_tab_after_text_input(["Suggested Age Range", "Category", "Value"], rows, CROSS_TAB_214), tmp_path
    )
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "pivot"
    assert failures == []
    assert set(frame.columns) == {
        "Suggested Age Range",
        "Sum_Educational",
        "Avg_Educational",
        "Sum_Fun_and_Games",
        "Avg_Fun_and_Games",
    }
    kids = frame.filter(pl.col("Suggested Age Range") == "Kids")
    assert kids["Sum_Fun_and_Games"].to_list() == [40]
    assert kids["Avg_Fun_and_Games"].to_list() == [20.0]


def test_cross_tab_sanitises_headers_like_alteryx(tmp_path: Path):
    rows = [("n", "New York", 1), ("n", "2019-Q1", 2), ("n", "a.b/c", 3)]
    config = (
        '<GroupFields><Field field="region" /></GroupFields><HeaderField field="city" />'
        '<DataField field="sales" /><Methods><Method method="Sum" /></Methods>'
    )
    _, frame, failures = run_cross_tab(cross_tab_after_text_input(["region", "city", "sales"], rows, config), tmp_path)
    assert failures == []
    assert frame.columns == ["region", "2019_Q1", "New_York", "a_b_c"]


def test_cross_tab_header_collision_stops_the_flow_naming_both_values(tmp_path: Path):
    rows = [("n", "A-B", 1), ("n", "A B", 2)]
    config = (
        '<GroupFields><Field field="region" /></GroupFields><HeaderField field="city" />'
        '<DataField field="sales" /><Methods><Method method="Sum" /></Methods>'
    )
    _, frame, failures = run_cross_tab(cross_tab_after_text_input(["region", "city", "sales"], rows, config), tmp_path)
    assert frame is None
    assert len(failures) == 1
    assert "'A B' and 'A-B' both become column 'A_B'" in failures[0].error


@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("Count", ["region", "Count_x"]),
        ("Avg", ["region", "Avg_x"]),
        ("Sum", ["region", "x"]),
        ("First", ["region", "x"]),
    ],
)
def test_cross_tab_prefixes_a_lone_method_unless_it_is_sum_first_or_last(tmp_path: Path, method: str, expected: list):
    """Alteryx's Cross Tab docs: the method is prefixed unless Sum, First or Last is the only method."""
    rows = [("n", "x", 1), ("n", "x", 2)]
    config = (
        '<GroupFields><Field field="region" /></GroupFields><HeaderField field="city" />'
        f'<DataField field="sales" /><Methods><Method method="{method}" /></Methods>'
    )
    _, frame, failures = run_cross_tab(cross_tab_after_text_input(["region", "city", "sales"], rows, config), tmp_path)
    assert failures == []
    assert frame.columns == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(",\\s", ", "), ("\\t", "\t"), ("\\n", "\n"), (",", ","), ("\\\\s", "\\s"), ("|", "|")],
)
def test_cross_tab_separator_unescaping(raw: str, expected: str):
    from flowfile_core.flowfile.converters.alteryx.mappers import _unescape_separator

    assert _unescape_separator(raw) == expected


def test_append_fields_maps_to_cross_join(extra_tools: ConversionResult):
    row = report_row(extra_tools, 7)
    assert row.status == "converted"
    node = dumped_nodes(extra_tools)[row.flowfile_node_ids[0]]
    assert node["type"] == "cross_join"
    assert node["input_ids"] == [1]
    assert node["right_input_id"] == report_row(extra_tools, 6).flowfile_node_ids[0]


def test_count_records_maps_the_macro_onto_record_count(tmp_path: Path):
    result = convert_yxmd(macro_after_text_input("CountRecords.yxmc", ""), source_name="inline.yxmd")
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "record_count"
    nodes = dumped_nodes(result)
    count_id, rename_id = row.flowfile_node_ids
    assert nodes[count_id]["type"] == "record_count"
    assert nodes[rename_id]["type"] == "select"
    assert nodes[count_id]["outputs"] == [rename_id]

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, run_info
    frame = flow.get_node(rename_id).get_resulting_data().data_frame.collect()
    assert frame.to_dict(as_series=False) == {"Count": [1]}
    assert frame.schema["Count"] == pl.Int64


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


def test_data_cleansing_converts_a_macro_build_without_the_null_checkboxes():
    """The installed Cleanse.yxmc writes 11 values; the two null-removal ids are absent."""
    config = cleanse_config({"Check Box (135)": None, "Check Box (136)": None})
    result = convert_yxmd(macro_after_text_input("Cleanse.yxmc", config), source_name="inline.yxmd")
    row = report_row(result, 2)
    assert row.status == "converted"
    assert row.flowfile_node_type == "data_cleansing"
    cleansing = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert cleansing["remove_null_rows"] is False
    assert cleansing["remove_null_columns"] is False
    assert cleansing["replace_nulls_with_blank"] is True


def test_data_cleansing_still_reads_the_null_checkboxes_when_the_build_writes_them():
    config = cleanse_config({"Check Box (135)": "True", "Check Box (136)": "True"})
    result = convert_yxmd(macro_after_text_input("Cleanse.yxmc", config), source_name="inline.yxmd")
    cleansing = dumped_nodes(result)[report_row(result, 2).flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert (cleansing["remove_null_rows"], cleansing["remove_null_columns"]) == (True, True)


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
    result = convert_yxmd(macro_after_text_input("Macros\\Cleanse.yxmc", cleanse_config()), source_name="inline.yxmd")
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
            '<Methods><Method method="CountNonNull" /></Methods>',
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

    pivoted_id = report_row(extra_tools, 5).flowfile_node_ids[-1]
    pivoted = flow.get_node(pivoted_id).get_resulting_data().data_frame.collect().sort("region")
    assert pivoted["apples"].to_list() == [10, 30]
    assert pivoted["pears"].to_list() == [20, 0]

    appended_id = report_row(extra_tools, 7).flowfile_node_ids[0]
    appended = flow.get_node(appended_id).get_resulting_data().data_frame.collect()
    assert appended.height == 3
    assert appended["tax"].to_list() == [0.2, 0.2, 0.2]

    cleansed_id = report_row(extra_tools, 8).flowfile_node_ids[0]
    cleansed = flow.get_node(cleansed_id).get_resulting_data().data_frame.collect()
    assert cleansed["region"].to_list() == ["NORTH", "NORTH", "SOUTH"]
    assert cleansed["product"].to_list() == ["APPLES", "PEARS", "APPLES"]


def placeholder_code(result: ConversionResult, node_id: int = 1) -> str:
    return dumped_nodes(result)[node_id]["setting_input"]["polars_code_input"]["polars_code"]


def test_credentials_never_reach_the_saved_flow():
    result = convert_yxmd(LLM_WITH_CREDENTIALS, source_name="llm.yxmd")
    code = placeholder_code(result)
    for secret in (
        "7a9a50b6-a4bd-4841-b4c9-3bfd051752d2",
        "aa7d8c1a-8001-49d5-86a0-f6a5322f5d46",
        "01JB01XTJVFQCTCMMS9X3F3HX1",
    ):
        assert secret not in code
    assert "[redacted by Flowfile]" in code
    # The shape of the configuration still survives, so the node can be rebuilt by hand.
    assert "authServerDetails" in code
    assert "https://ayx-sandbox.bender.rocks/aims/" in code
    assert "Credential values were not copied out of the workflow:" in code
    assert "llmConnectionId" in code


def test_a_password_bearing_connection_string_is_blanked_whole():
    result = convert_yxmd(ODBC_INPUT, source_name="odbc.yxmd")
    code = placeholder_code(result)
    assert "hunter2" not in code
    assert "Zm9vYmFy" not in code
    assert "corp.database.windows.net" not in code
    assert "[redacted by Flowfile]" in code


def test_an_ordinary_configuration_is_copied_untouched(unsupported: ConversionResult):
    code = placeholder_code(unsupported, node_id=2)
    assert "[redacted by Flowfile]" not in code
    assert "Credential values were not copied" not in code


def test_a_yxdb_input_reads_the_parquet_sibling():
    result = convert_yxmd(YXDB_INPUT, source_name="yxdb.yxmd")
    row = next(row for row in result.report.rows if row.alteryx_tool_id == 1)
    assert row.status == "partial"
    assert row.flowfile_node_type == "read"

    received = dumped_nodes(result)[1]["setting_input"]["received_file"]
    assert received["file_type"] == "parquet"
    assert received["name"] == "CustomerFile1.parquet"
    assert received["path"] == "..\\..\\..\\data\\OneToolData\\CustomerFile1.parquet"
    assert received["directory"] == "..\\..\\..\\data\\OneToolData"


def test_a_yxdb_input_names_the_command_that_creates_the_file():
    row = next(row for row in convert_yxmd(YXDB_INPUT, source_name="yxdb.yxmd").report.rows if row.alteryx_tool_id == 1)
    assert any("does not open .yxdb" in message for message in row.messages)
    command = next(message for message in row.messages if "flowfile convert yxdb" in message)
    assert '"..\\..\\..\\data\\OneToolData\\CustomerFile1.yxdb"' in command


def test_a_yxdb_input_keeps_its_cached_columns_for_downstream_tools():
    result = convert_yxmd(YXDB_INPUT, source_name="yxdb.yxmd")
    # The Sort resolved its field, which only the cached schema of the .yxdb reader can supply.
    sort_row = next(row for row in result.report.rows if row.alteryx_tool_id == 2)
    assert sort_row.status == "converted"
    nodes = dumped_nodes(result)
    assert nodes[2]["setting_input"]["sort_input"][0]["column"] == "Spend"
    assert nodes[1]["outputs"] == [2]


def output_row_and_settings(data: bytes) -> tuple:
    result = convert_yxmd(data, source_name="output.yxmd")
    row = next(row for row in result.report.rows if row.alteryx_tool_id == 2)
    node = dumped_nodes(result).get(row.flowfile_node_ids[0])
    return row, node["setting_input"]


def test_a_yxdb_output_writes_the_parquet_sibling():
    row, settings = output_row_and_settings(
        db_file_output('<File FileFormat="19" MaxRecords="">C:\\out\\orders.yxdb</File>')
    )
    assert row.status == "partial"
    assert settings["output_settings"]["file_type"] == "parquet"
    assert settings["output_settings"]["name"] == "orders.parquet"
    assert settings["output_settings"]["directory"] == "C:\\out"
    assert any("does not write .yxdb" in message for message in row.messages)


def test_a_windows_variable_in_the_target_path_is_not_pretended_to_be_a_folder():
    row, settings = output_row_and_settings(
        db_file_output('<File FileFormat="0" MaxRecords="">%temp%OutputToolExample_Simple.csv</File>')
    )
    assert row.status == "partial"
    assert settings["output_settings"]["name"] == "OutputToolExample_Simple.csv"
    assert settings["output_settings"]["directory"] == ""
    assert any("%temp%" in message for message in row.messages)


def test_a_multi_file_output_is_refused_instead_of_written_as_one_file():
    row, settings = output_row_and_settings(
        db_file_output(
            '<File FileFormat="19" MaxRecords="">%temp%OutputToolExample_RegionGrouped_.yxdb</File>',
            extra=(
                '<MultiFile value="True" /><MultiFileType>Suffix</MultiFileType><MultiFileField>Region</MultiFileField>'
            ),
        )
    )
    assert row.status == "placeholder"
    assert any("one file per value of 'Region'" in message for message in row.messages)


def test_an_ordinary_csv_output_still_converts_without_caveats():
    row, settings = output_row_and_settings(
        db_file_output('<File FileFormat="0" MaxRecords="">C:\\out\\orders.csv</File>')
    )
    assert row.status == "converted"
    assert row.messages == []
    assert settings["output_settings"]["name"] == "orders.csv"


MULTI_FIELD_COPY_FIELDS = (
    '<Fields orderChanged="False">'
    '<Field name="value" /><Field name="*Unknown" /><Field name="Total " selected="False" />'
    "</Fields>"
)


def multi_field_copy_workflow(affix_elements: str) -> bytes:
    """The Multi-Field Formula as Alteryx writes it when it copies to new fields."""
    return tool_after_text_input(
        "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula",
        "<FieldType>Numeric</FieldType>"
        + MULTI_FIELD_COPY_FIELDS
        + affix_elements
        + '<CopyOutput value="True" />'
        + "<Expression>[_CurrentField_]/[Total ]*100</Expression>"
        + '<ChangeFieldType value="False" />',
    )


def inline_multi_field_settings(document: bytes):
    result = convert_yxmd(document, source_name="inline.yxmd")
    row = report_row(result, 2)
    assert row.flowfile_node_type == "multi_field_formula", row.messages
    return row, dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["multi_field_formula_input"]


@pytest.mark.parametrize(
    ("case_id", "affix_elements"),
    [
        ("no-affix-at-all", ""),
        ("position-without-an-affix", "<NewFieldAddOnPos>Prefix</NewFieldAddOnPos>"),
    ],
)
def test_multi_field_formula_fails_closed_when_the_new_names_are_unknown(case_id: str, affix_elements: str):
    row = report_row(convert_yxmd(multi_field_copy_workflow(affix_elements), source_name="inline.yxmd"), 2)
    assert row.status == "placeholder", case_id
    assert any("not recorded in the workflow" in message for message in row.messages)


@pytest.mark.parametrize(
    ("case_id", "affix_elements"),
    [
        ("affix-without-a-position", "<NewFieldAddOn>New_</NewFieldAddOn>"),
        ("unknown-position", "<NewFieldAddOn>New_</NewFieldAddOn><NewFieldAddOnPos>Around</NewFieldAddOnPos>"),
    ],
)
def test_multi_field_formula_treats_a_non_suffix_position_as_a_prefix(case_id: str, affix_elements: str):
    """Only ``Suffix`` appends; anything else takes Alteryx's own default, which is a prefix."""
    row, settings = inline_multi_field_settings(multi_field_copy_workflow(affix_elements))
    assert row.status == "converted", case_id
    assert (settings["output_prefix"], settings["output_suffix"]) == ("New_", ""), case_id


def test_multi_field_formula_without_copy_output_still_writes_in_place():
    """No affix is recorded, but nothing is copied either, so the missing names are not a problem."""
    document = tool_after_text_input(
        "AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula",
        "<FieldType>Text</FieldType>"
        + MULTI_FIELD_COPY_FIELDS
        + '<CopyOutput value="False" />'
        + "<Expression>Uppercase([_CurrentField_])</Expression>"
        + '<ChangeFieldType value="False" />',
    )
    row, settings = inline_multi_field_settings(document)
    assert row.status == "converted"
    assert settings["output_mode"] == "replace"
    assert (settings["output_prefix"], settings["output_suffix"]) == ("", "")


def dsn_workflow(plugin: str, dsn: str, annotation: str = "") -> bytes:
    """A one-tool reader/writer whose File element is an ODBC connection string."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="{plugin}" />
      <Properties>
        <Configuration>
          <File>{dsn}</File>
          <FormatSpecificOptions />
        </Configuration>
        <Annotation DisplayMode="0">
          <DefaultAnnotationText>{annotation}</DefaultAnnotationText>
        </Annotation>
      </Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
""".encode()


# Every one of these ends in a dot-segment, which is what made the first redaction guard miss:
# `_extension` sliced the string before the guard ever saw it.
DSN_CASES = [
    (
        "azure-dotted-host",
        "odbc:DRIVER={ODBC Driver 18};SERVER=corp.database.windows.net;UID=r;PWD=hunter2",
        ("hunter2", "corp.database.windows.net"),
    ),
    ("password-containing-a-dot", "odbc:SERVER=db01;UID=x;PWD=hun.ter2", ("hun.ter2", "ter2")),
    (
        "dsn-whose-tail-looks-like-a-yxdb",
        "odbc:SERVER=corp.database.windows.net;UID=r;PWD=hunter2;DATABASE=archive.yxdb",
        ("hunter2", "corp.database.windows.net"),
    ),
    ("uppercase-password-keyword", "odbc:SERVER=host.corp.local;UID=r;Password=hunter2", ("hunter2",)),
]


@pytest.mark.parametrize(
    "plugin",
    ["AlteryxBasePluginsGui.DbFileInput.DbFileInput", "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput"],
)
@pytest.mark.parametrize(("case_id", "dsn", "secrets"), DSN_CASES)
def test_a_connection_string_never_reaches_the_report_or_the_saved_flow(
    plugin: str, case_id: str, dsn: str, secrets: tuple[str, ...]
):
    result = convert_yxmd(dsn_workflow(plugin, dsn), source_name="dsn.yxmd")
    row = result.report.rows[0]
    # Every message the user sees, plus everything written into the flow file itself.
    written = yaml.dump(result.flow_data.model_dump(mode="json"), allow_unicode=True)
    haystack = " ".join(row.messages) + " " + written
    for secret in secrets:
        assert secret not in haystack, f"{case_id} ({plugin.rsplit('.', 1)[-1]}) leaked {secret!r}"
    assert "[redacted by Flowfile]" in haystack, case_id


DOTTED_DSN = "odbc:DRIVER={ODBC Driver 18};SERVER=corp.database.windows.net;UID=r;PWD=hunter2"

CONNECTION_SOURCES = [
    # The File value is refused as a connection, but the annotation repeats it — the row still says so.
    ("dsn-repeated-in-the-annotation", "C:\\data\\orders.csv", DOTTED_DSN),
    ("ftp-with-inline-credentials", "ftp://admin:x@host/data.csv", ""),
    ("https-with-a-token-parameter", "https://host/export.csv?access_token=x", ""),
    ("azure-storage-key-pairs", "AccountName=a;AccountKey=x;EndpointSuffix=core.windows.net", ""),
    ("dsn-without-any-password", "odbc:DRIVER={SQL Server};SERVER=corp;Trusted_Connection=yes", ""),
]


@pytest.mark.parametrize(
    "plugin",
    ["AlteryxBasePluginsGui.DbFileInput.DbFileInput", "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput"],
)
@pytest.mark.parametrize(("case_id", "file_value", "annotation"), CONNECTION_SOURCES)
def test_a_connection_source_is_named_on_the_row_and_never_left_green(
    plugin: str, case_id: str, file_value: str, annotation: str
):
    result = convert_yxmd(dsn_workflow(plugin, file_value, annotation), source_name="dsn.yxmd")
    row = result.report.rows[0]
    assert any("remove any credentials before sharing" in message for message in row.messages), case_id
    assert row.status != "converted", case_id


@pytest.mark.parametrize(
    "plugin",
    ["AlteryxBasePluginsGui.DbFileInput.DbFileInput", "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput"],
)
@pytest.mark.parametrize(
    "file_value",
    [
        "C:\\data\\customers.csv",
        "..\\..\\..\\data\\OneToolData\\CustomerFile1.csv",
        ".\\2016PPData.csv",
        "%temp%OutputToolExample_Simple.csv",
        "\\\\server\\share\\file.csv",
        "C:\\Users\\me\\My Documents\\report v1.2.csv",
    ],
)
def test_a_plain_path_is_never_called_a_connection_string(plugin: str, file_value: str):
    row = convert_yxmd(dsn_workflow(plugin, file_value, file_value), source_name="plain.yxmd").report.rows[0]
    assert not any("remove any credentials" in message for message in row.messages)
    # A `%temp%` writer is partial for its own reason (the Windows variable), not for this one.
    is_temp_writer = file_value.startswith("%") and "Output" in plugin
    assert row.status == ("partial" if is_temp_writer else "converted"), row.messages


def test_an_unreadable_format_is_described_never_echoed():
    """The message must not become a second way to print the connection string."""
    row = convert_yxmd(
        dsn_workflow(
            "AlteryxBasePluginsGui.DbFileInput.DbFileInput",
            "odbc:DRIVER={SQL Server};SERVER=corp;Trusted_Connection=yes",
        ),
        source_name="dsn.yxmd",
    ).report.rows[0]
    unsupported = next(message for message in row.messages if "does not support" in message)
    assert "an unrecognised format" in unsupported
    assert "Trusted_Connection" not in unsupported
    assert "corp" not in unsupported


def test_an_ordinary_unsupported_format_is_still_quoted_by_name():
    row = convert_yxmd(
        dsn_workflow("AlteryxBasePluginsGui.DbFileInput.DbFileInput", "C:\\data\\customers.mdb"),
        source_name="mdb.yxmd",
    ).report.rows[0]
    assert any("reads 'mdb', which Flowfile does not support" in message for message in row.messages)


def test_the_redaction_notice_only_appears_when_something_was_blanked(unsupported: ConversionResult):
    """A tool with nothing to hide must not be told its credentials were removed."""
    for row in unsupported.report.rows:
        if row.status != "placeholder":
            continue
        code = dumped_nodes(unsupported)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
        assert "Credential values were not copied" not in code


def test_the_redaction_notice_names_each_blanked_element_once():
    result = convert_yxmd(LLM_WITH_CREDENTIALS, source_name="llm.yxmd")
    notice = next(line for line in placeholder_code(result).splitlines() if "Credential values were not copied" in line)
    names = notice.split(":", 1)[1].strip().rstrip(".").split(", ")
    assert names == sorted(set(names)), notice
    assert "llmConnectionId" in names
