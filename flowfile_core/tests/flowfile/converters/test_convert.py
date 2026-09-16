"""Tests for the Alteryx -> Flowfile tool mappers and the convert orchestrator."""

import ast
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest
import yaml
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx import (
    ConversionResult,
    YxmdParseError,
    convert_yxmd,
    mappers,
)
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
    assert '// Original: REGEX_Match([name], "^(?=a).*")' in body
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
    assert code.startswith("# Alteryx tool 'XMLParse' (ToolID 2) could not be converted automatically.")
    assert code.endswith("output_df = input_df")
    assert "XMLParse" in placeholder["description"]
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
                # The Sample is `partial`: every mode picks rows by position, and nothing
                # upstream of this one states an order. The Formula is `partial` too: it
                # declares 'total' Double over '[amount]', and a Select and a Filter stand between
                # it and the Text Input that would settle that column's type.
                "converted": 9,
                "partial": 4,
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
    """Tool 5 is a Message between the Image Input and the XMLParse; it costs no node at all."""
    rows = {row.alteryx_tool_id: row for row in out_of_scope.report.rows}
    message = rows[5]
    assert (message.status, message.reason) == ("no_op", "no_op")
    assert (message.flowfile_node_ids, message.flowfile_node_type) == ([], None)
    assert message.messages == [BUCKETS["no_op"]]
    # The XMLParse behind it is wired to what fed the Message, and no wire was reported dropped.
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

# Two anchors of ONE Flowfile node, but two different output handles: a Filter's True and False
# are two streams, so they stay two edges and the body reads two inputs.
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
        (DETOUR_END_BEHIND_ONE_FILTER, 2, "output_df = input_df_1"),
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

# Two Detours' live anchors onto one Detour End's 'Right': the live side is identifiable but the
# stream on it is not, which is the reader rule the Detour End already applied.
TWO_LIVE_WIRES_ON_ONE_DETOUR_END = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="731"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="732"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="733"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="True" /></Configuration></Properties></Node>
    <Node ToolID="734"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="True" /></Configuration></Properties></Node>
    <Node ToolID="735"><GuiSettings Plugin="AlteryxBasePluginsGui.DetourEnd.DetourEnd" />
      <Properties><Configuration /></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="731" Connection="Output" />
      <Destination ToolID="733" Connection="Input" /></Connection>
    <Connection><Origin ToolID="732" Connection="Output" />
      <Destination ToolID="734" Connection="Input" /></Connection>
    <Connection><Origin ToolID="733" Connection="Right" />
      <Destination ToolID="735" Connection="Right" /></Connection>
    <Connection><Origin ToolID="734" Connection="Right" />
      <Destination ToolID="735" Connection="Right" /></Connection>
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


# --- pre-flight: the wiring shapes a Flowfile edge cannot hold ---

# One Select has one input port; Alteryx would union both Text Inputs onto it.
TWO_SOURCES_ON_ONE_SELECT = _two_sources(
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect",
    ("Input", "Input"),
)

# A Unique with a Dupes branch registers BOTH its nodes on the same input anchor, so the refusal
# has to hold for every target a wire fans out to, not just the first.
TWO_SOURCES_ON_A_UNIQUE_WITH_DUPES = _two_sources(
    "AlteryxBasePluginsGui.Unique.Unique",
    ("Input", "Input"),
    extra="""
    <Node ToolID="715"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration>
        <SelectFields><SelectField field="CrateId" selected="True" /></SelectFields>
      </Configuration></Properties></Node>""",
    wires="""
    <Connection><Origin ToolID="713" Connection="Dupes" />
      <Destination ToolID="715" Connection="Input" /></Connection>""",
)

# Both halves of one Filter into one Union: two streams, two handles, two edges.
FILTER_BOTH_ANCHORS_INTO_ONE_UNION = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="741"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r><r><c>-2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="742"><GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter" />
      <Properties><Configuration>
        <Expression>[CrateId] &gt; 0</Expression>
      </Configuration></Properties></Node>
    <Node ToolID="743"><GuiSettings Plugin="AlteryxBasePluginsGui.Union.Union" />
      <Properties><Configuration><Mode>ByName</Mode></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="741" Connection="Output" />
      <Destination ToolID="742" Connection="Input" /></Connection>
    <Connection><Origin ToolID="742" Connection="True" />
      <Destination ToolID="743" Connection="Input" /></Connection>
    <Connection><Origin ToolID="742" Connection="False" />
      <Destination ToolID="743" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""

# A wire on the name anchor of three renames that take their names from their own configuration.
A_NAME_WIRE_ON_A_RENAME_THAT_READS_NONE = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="760"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r><r><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="761"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Crates</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="762"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>Formula</RenameMode>
        <Fields><Field name="a" /><Field name="*Unknown" /></Fields>
        <Expression>Uppercase([_CurrentField_])</Expression>
      </Configuration></Properties></Node>
    <Node ToolID="770"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r><r><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="771"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Crates</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="772"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>FirstRow</RenameMode>
        <Fields><Field name="a" /><Field name="*Unknown" /></Fields>
      </Configuration></Properties></Node>
    <Node ToolID="780"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r><r><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="781"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Crates</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="782"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>AddPrefixSuffix</RenameMode>
        <Fields><Field name="a" /><Field name="*Unknown" /></Fields>
        <AddPrefixSuffix><Prefix>pre_</Prefix><Suffix>_post</Suffix></AddPrefixSuffix>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="760" Connection="Output" />
      <Destination ToolID="762" Connection="Input" /></Connection>
    <Connection><Origin ToolID="761" Connection="Output" />
      <Destination ToolID="762" Connection="Right" /></Connection>
    <Connection><Origin ToolID="770" Connection="Output" />
      <Destination ToolID="772" Connection="Input" /></Connection>
    <Connection><Origin ToolID="771" Connection="Output" />
      <Destination ToolID="772" Connection="Right" /></Connection>
    <Connection><Origin ToolID="780" Connection="Output" />
      <Destination ToolID="782" Connection="Input" /></Connection>
    <Connection><Origin ToolID="781" Connection="Output" />
      <Destination ToolID="782" Connection="Right" /></Connection>
  </Connections>
</AlteryxDocument>
"""


# Two Text Inputs on a Dynamic Rename's name anchor: the mapper reads one of them.
TWO_NAME_SOURCES_ON_A_DYNAMIC_RENAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="751"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Field_1" /></Fields>
        <Data><r><c>3</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="752"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Crates</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="753"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Pallets</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="754"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>RightInputRows</RenameMode>
        <Fields><Field name="Field_1" /><Field name="*Unknown" /></Fields>
        <NamesFromRows><InputMode>Positional</InputMode><NewName>Name</NewName></NamesFromRows>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="751" Connection="Output" />
      <Destination ToolID="754" Connection="Input" /></Connection>
    <Connection><Origin ToolID="752" Connection="Output" />
      <Destination ToolID="754" Connection="Right" /></Connection>
    <Connection><Origin ToolID="753" Connection="Output" />
      <Destination ToolID="754" Connection="Right" /></Connection>
  </Connections>
</AlteryxDocument>
"""


# The same rename with one name stream: the mapper reads it and the anchor stays suppressed.
ONE_NAME_SOURCE_ON_A_DYNAMIC_RENAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="751"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Field_1" /></Fields>
        <Data><r><c>3</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="752"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Name" /></Fields>
        <Data><r><c>Crates</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="754"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>RightInputRows</RenameMode>
        <Fields><Field name="Field_1" /><Field name="*Unknown" /></Fields>
        <NamesFromRows><InputMode>Positional</InputMode><NewName>Name</NewName></NamesFromRows>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="751" Connection="Output" />
      <Destination ToolID="754" Connection="Input" /></Connection>
    <Connection><Origin ToolID="752" Connection="Output" />
      <Destination ToolID="754" Connection="Right" /></Connection>
  </Connections>
</AlteryxDocument>
"""

# A tool that is not an Explorer Box: its <URL> is configuration a reader needs, not canvas décor.
AN_ORDINARY_TOOL_WITH_A_URL = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="761"><GuiSettings Plugin="AlteryxBasePluginsGui.Tile.Tile" />
      <Properties><Configuration>
        <Method>EqualRecords</Method>
        <URL>https://orchard.example.com/feed?token=okonjo-secret</URL>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections />
</AlteryxDocument>
"""


def full_input_messages(result: ConversionResult) -> list[tuple[int, str]]:
    return [
        (row.alteryx_tool_id, message)
        for row in result.report.rows
        for message in row.messages
        if "was not wired" in message or "was not read" in message
    ]


def test_a_second_source_on_a_single_input_node_is_dropped_and_named(tmp_path: Path):
    """Alteryx unions both wires onto the Select; a union here would invent rows, so one is dropped."""
    result = convert_yxmd(TWO_SOURCES_ON_ONE_SELECT, source_name="two_sources.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = (
        "The Flowfile 'select' node for ToolID 713 ('AlteryxSelect') takes 1 input stream and already has "
        "its input from ToolID 711 (on 'Input'); the connection from ToolID 712 ('Output') into its 'Input' "
        "anchor was not wired. Alteryx unions the streams arriving on one anchor — add a Union node upstream "
        "and wire that in if that is what this workflow meant."
    )
    assert full_input_messages(result) == [(712, sentence), (713, sentence)]
    assert (rows[712].status, rows[712].reason) == ("partial", "dropped_connection")
    assert (rows[713].status, rows[713].reason) == ("partial", "dropped_connection")
    assert rows[711].status == "converted"
    nodes = dumped_nodes(result)
    assert nodes[rows[713].flowfile_node_ids[0]]["input_ids"] == rows[711].flowfile_node_ids
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_the_dupes_branch_of_a_two_source_unique_refuses_the_same_wire(tmp_path: Path):
    """One Alteryx wire reaches two Flowfile nodes here; the second source is refused at both."""
    result = convert_yxmd(TWO_SOURCES_ON_A_UNIQUE_WITH_DUPES, source_name="two_sources_unique.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    unique_id, dupes_id = rows[713].flowfile_node_ids
    nodes = dumped_nodes(result)
    # Both nodes stand for one Alteryx tool, so the refused wire has to be refused at both.
    assert nodes[unique_id]["input_ids"] == rows[711].flowfile_node_ids
    assert nodes[dupes_id]["input_ids"] == rows[711].flowfile_node_ids
    messages = full_input_messages(result)
    assert [tool_id for tool_id, _ in messages] == [712, 713]
    assert "'unique' node for ToolID 713" in messages[0][1]
    assert "from ToolID 711 (on 'Input'); the connection from ToolID 712 ('Output')" in messages[0][1]
    assert (rows[712].status, rows[712].reason) == ("partial", "dropped_connection")
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def _join_wires(wires: str) -> bytes:
    """Three Text Inputs that share a join key, into one Alteryx Join; *wires* says which anchors."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
<Nodes>
<Node ToolID="711"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
<Properties><Configuration>
<Fields><Field name="CrateId" /><Field name="Grower" /></Fields>
<Data><r><c>1</c><c>Okonjo</c></r></Data>
</Configuration></Properties></Node>
<Node ToolID="712"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
<Properties><Configuration>
<Fields><Field name="CrateId" /><Field name="Variety" /></Fields>
<Data><r><c>1</c><c>Gala</c></r></Data>
</Configuration></Properties></Node>
<Node ToolID="714"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
<Properties><Configuration>
<Fields><Field name="CrateId" /><Field name="Packhouse" /></Fields>
<Data><r><c>1</c><c>Shed 2</c></r></Data>
</Configuration></Properties></Node>
<Node ToolID="713"><GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join" />
<Properties><Configuration joinByRecordPos="False">
<JoinInfo connection="Left"><Field field="CrateId" /></JoinInfo>
<JoinInfo connection="Right"><Field field="CrateId" /></JoinInfo>
</Configuration></Properties></Node>
</Nodes>
<Connections>{wires}
</Connections>
</AlteryxDocument>
""".encode()


def _wire_xml(origin: int, anchor: str) -> str:
    return (
        f'\n<Connection><Origin ToolID="{origin}" Connection="Output" />'
        f'<Destination ToolID="713" Connection="{anchor}" /></Connection>'
    )


# Two wires on Left used to fill both ports, so the Join's real Right wire looked like the spare.
TWO_LEFT_WIRES_AND_A_RIGHT_ON_ONE_JOIN = _join_wires(
    _wire_xml(711, "Left") + _wire_xml(712, "Left") + _wire_xml(714, "Right")
)
# The right-hand slot holds one stream; a second write replaced the first without a word.
TWO_RIGHT_WIRES_ON_ONE_JOIN = _join_wires(_wire_xml(711, "Left") + _wire_xml(712, "Right") + _wire_xml(714, "Right"))
# Two Left wires and nothing on Right: the shape that silently gave a Join two main ports.
TWO_LEFT_WIRES_AND_NO_RIGHT_ON_ONE_JOIN = _join_wires(_wire_xml(711, "Left") + _wire_xml(712, "Left"))


def test_both_halves_of_a_filter_reach_one_union_as_two_edges(tmp_path: Path):
    """The output handle is part of an edge's identity; without it document order picks a winner.

    Two wires on one anchor of an N-ary target is not a full slot: arity is asked per slot.
    """
    result = convert_yxmd(FILTER_BOTH_ANCHORS_INTO_ONE_UNION, source_name="filter_union.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    nodes = dumped_nodes(result)
    filter_id = rows[742].flowfile_node_ids[0]
    union_id = rows[743].flowfile_node_ids[0]
    assert nodes[filter_id]["outputs"] == [union_id, union_id]
    assert nodes[filter_id]["output_handles"] == ["output-0", "output-1"]
    assert nodes[union_id]["input_ids"] == [filter_id, filter_id]
    assert nodes[union_id]["right_input_id"] is None
    assert full_input_messages(result) == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]


# A Join's right-hand wire lives in ``right_input_id``, so the two slots are counted — and
# described — apart: the main slot holds one stream once the right-hand slot is taken out of the
# palette's two ports, and the right-hand slot holds exactly one.
JOIN_FULL_SENTENCE = (
    "The Flowfile 'join' node for ToolID 713 ('Join') {room} already has its input "
    "from {taken}; the connection from ToolID {dropped} ('Output') into its '{anchor}' anchor was "
    "not wired. Alteryx unions the streams arriving on one anchor — add a Union node upstream and "
    "wire that in if that is what this workflow meant."
)
JOIN_MAIN_ROOM = "takes 1 input stream and"
JOIN_RIGHT_ROOM = "holds one stream in its right-hand input slot and"


def test_a_second_left_wire_is_dropped_and_the_right_wire_keeps_its_slot(tmp_path: Path):
    """Two wires on Left used to fill both ports, so the Join's real Right wire was the one reported."""
    result = convert_yxmd(TWO_LEFT_WIRES_AND_A_RIGHT_ON_ONE_JOIN, source_name="two_left.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = JOIN_FULL_SENTENCE.format(
        room=JOIN_MAIN_ROOM, taken="ToolID 711 (on 'Left')", dropped=712, anchor="Left"
    )
    assert full_input_messages(result) == [(712, sentence), (713, sentence)]
    assert (rows[712].status, rows[712].reason) == ("partial", "dropped_connection")
    assert (rows[713].status, rows[713].reason) == ("partial", "dropped_connection")
    assert (rows[711].status, rows[714].status) == ("converted", "converted")
    nodes = dumped_nodes(result)
    join = nodes[rows[713].flowfile_node_ids[0]]
    assert join["input_ids"] == rows[711].flowfile_node_ids
    assert join["right_input_id"] == rows[714].flowfile_node_ids[0]
    assert nodes[rows[712].flowfile_node_ids[0]]["outputs"] == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_second_right_wire_is_dropped_instead_of_overwriting_the_first(tmp_path: Path):
    """Writing ``right_input_id`` twice replaced the first wire in silence."""
    result = convert_yxmd(TWO_RIGHT_WIRES_ON_ONE_JOIN, source_name="two_right.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = JOIN_FULL_SENTENCE.format(
        room=JOIN_RIGHT_ROOM, taken="ToolID 712 (on 'Right')", dropped=714, anchor="Right"
    )
    assert full_input_messages(result) == [(714, sentence), (713, sentence)]
    assert (rows[714].status, rows[714].reason) == ("partial", "dropped_connection")
    assert (rows[713].status, rows[713].reason) == ("partial", "dropped_connection")
    nodes = dumped_nodes(result)
    join = nodes[rows[713].flowfile_node_ids[0]]
    assert join["right_input_id"] == rows[712].flowfile_node_ids[0]
    assert join["input_ids"] == rows[711].flowfile_node_ids
    assert nodes[rows[714].flowfile_node_ids[0]]["outputs"] == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_two_left_wires_alone_do_not_fill_the_right_hand_slot(tmp_path: Path):
    """The right slot stays empty rather than being filled from the anchor next to it."""
    result = convert_yxmd(TWO_LEFT_WIRES_AND_NO_RIGHT_ON_ONE_JOIN, source_name="two_left_only.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert (rows[713].status, rows[713].reason) == ("partial", "dropped_connection")
    join = dumped_nodes(result)[rows[713].flowfile_node_ids[0]]
    assert join["input_ids"] == rows[711].flowfile_node_ids
    assert join["right_input_id"] is None
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_second_name_source_on_a_dynamic_rename_refuses_instead_of_taking_the_first(tmp_path: Path):
    """Reading the first of two streams on an anchor was a guess, not an answer.

    Alteryx unions both name streams; the rename used to take whichever the document wrote first and
    report the other as unread, so the columns it produced were decided by line numbering. It now
    refuses and names both origins in file order.
    """
    result = convert_yxmd(TWO_NAME_SOURCES_ON_A_DYNAMIC_RENAME, source_name="two_name_sources.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert (rows[754].status, rows[754].reason) == ("placeholder", "mapper_refused")
    assert rows[754].messages == [
        "The Alteryx Dynamic Rename was not converted because several connections arrive on its "
        "field-name anchor, so the names it should use are not one tool's.",
        "The Alteryx 'DynamicRename' (ToolID 754) reads its 'Right' anchor at import time, and 2 "
        "connections arrive there — from ToolID 752 ('Output'), ToolID 753 ('Output'). Alteryx unions "
        "those streams, so what "
        "reaches this tool is not any one of them; the import answered 'unknown' rather than taking "
        "the first.",
    ]
    # No name stream is silently dropped now: the placeholder receives all three wires.
    assert len(dumped_nodes(result)[rows[754].flowfile_node_ids[0]]["input_ids"]) == 3
    assert full_input_messages(result) == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_one_name_source_on_a_dynamic_rename_still_reads_the_anchor(tmp_path: Path):
    """The refusal is about the second stream only — a single wire still resolves at import time."""
    result = convert_yxmd(ONE_NAME_SOURCE_ON_A_DYNAMIC_RENAME, source_name="one_name_source.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[754].status == "partial"
    renames = dumped_nodes(result)[rows[754].flowfile_node_ids[0]]["setting_input"]["select_input"]
    assert renames == [{"old_name": "Field_1", "new_name": "Crates"}]
    assert dumped_nodes(result)[rows[752].flowfile_node_ids[0]]["outputs"] == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


# --- pre-flight: the main slot is one budget per Alteryx anchor, not one per node ---

# Four anchor names all register `main` on one `dynamic_rename` node, whose template holds one
# stream. No corpus `.yxmd` writes this shape; the 11 interface files that do it with an Action or
# Question control wire beside the data are outside the census, so it is built by hand here.
TWO_TARGET_ANCHORS_ON_ONE_DYNAMIC_RENAME = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="791"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="792"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Grower" /></Fields>
        <Data><r><c>Okonjo</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="793"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>Formula</RenameMode>
        <Fields orderChanged="False"><Field name="*Unknown" /></Fields>
        <Expression>Uppercase([_CurrentField_])</Expression>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="791" Connection="Output" />
      <Destination ToolID="793" Connection="Targets" /></Connection>
    <Connection><Origin ToolID="792" Connection="Output" />
      <Destination ToolID="793" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_second_target_anchor_on_a_one_port_node_is_refused_without_union_advice(tmp_path: Path):
    """Two anchors shared one budget, so the message named a capacity and a fix that were both wrong.

    The port still goes to the anchor document order reaches first — one port cannot hold two
    streams — but the sentence now says what the node really takes, which anchor holds it, and that
    a Union is the wrong instrument for two *different* anchors.
    """
    result = convert_yxmd(TWO_TARGET_ANCHORS_ON_ONE_DYNAMIC_RENAME, source_name="two_anchors.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = (
        "The Flowfile 'dynamic_rename' node for ToolID 793 ('DynamicRename') takes 1 input stream in "
        "total, shared between the 2 Alteryx anchors wired into it, and already has its input from "
        "ToolID 791 (on 'Targets'); the connection from ToolID 792 ('Output') into its 'Input' anchor "
        "was not wired. This wire arrived on a different anchor from the one holding the port, so a "
        "Union would merge two streams the workflow never put together; reconnect by hand whichever "
        "one this node really needs."
    )
    assert full_input_messages(result) == [(792, sentence), (793, sentence)]
    assert (rows[792].status, rows[792].reason) == ("partial", "dropped_connection")
    assert (rows[793].status, rows[793].reason) == ("partial", "dropped_connection")
    nodes = dumped_nodes(result)
    assert nodes[rows[793].flowfile_node_ids[0]]["input_ids"] == rows[791].flowfile_node_ids
    assert nodes[rows[792].flowfile_node_ids[0]]["outputs"] == []
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def _crowded_placeholder(busy_wires: int) -> bytes:
    """One unmapped 10-input tool fed `busy_wires` times on 'Input' and once on 'Data'."""
    sources = "".join(
        f"""
    <Node ToolID="{800 + i}"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>{i}</c></r></Data>
      </Configuration></Properties></Node>"""
        for i in range(busy_wires + 1)
    )
    busy = "".join(
        f"""
    <Connection><Origin ToolID="{800 + i}" Connection="Output" />
      <Destination ToolID="899" Connection="Input" /></Connection>"""
        for i in range(busy_wires)
    )
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>{sources}
    <Node ToolID="899"><GuiSettings Plugin="AlteryxSpatialPluginsGui.Summarize.Summarize" />
      <Properties><Configuration><SummarizeFields>
        <SummarizeField field="Shape" action="ConvexHull" rename="Hull" />
      </SummarizeFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>{busy}
    <Connection><Origin ToolID="{800 + busy_wires}" Connection="Output" />
      <Destination ToolID="899" Connection="Data" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def test_a_busy_anchor_does_not_take_the_port_the_anchor_beside_it_never_got(tmp_path: Path):
    """Every wired anchor is served one port before any anchor is served a second.

    Ten wires on 'Input' used to fill all ten ports of the placeholder, so the single wire on
    'Data' — a different Alteryx stream — was the one reported dropped. The tenth wire on the busy
    anchor is the honest loser: it is the one Alteryx itself would union.
    """
    result = convert_yxmd(_crowded_placeholder(10), source_name="crowded.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    node = dumped_nodes(result)[rows[899].flowfile_node_ids[0]]
    assert len(node["input_ids"]) == 10
    # The 'Data' wire is in, and the tenth 'Input' wire is the one refused.
    assert rows[810].flowfile_node_ids[0] in node["input_ids"]
    assert rows[809].flowfile_node_ids[0] not in node["input_ids"]
    dropped = [tool_id for tool_id, _ in full_input_messages(result)]
    assert dropped == [809, 899]
    sentence = dict(full_input_messages(result))[809]
    assert "takes 10 input streams in total, shared between the 2 Alteryx anchors" in sentence
    # Its own anchor is the one that ran out, so a Union upstream really is the answer here.
    assert "add a Union node upstream" in sentence
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_second_anchor_costs_nothing_while_ports_remain(tmp_path: Path):
    """The reservation must not refuse a wire that fits: nine plus one is ten, and all ten land."""
    result = convert_yxmd(_crowded_placeholder(9), source_name="roomy.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert len(dumped_nodes(result)[rows[899].flowfile_node_ids[0]]["input_ids"]) == 10
    assert full_input_messages(result) == []


def _placeholder_fed_by_a_wire_that_leaves_from_nowhere(data_origin: str) -> bytes:
    """Ten live wires on 'Input' and one on 'Data' that can never become an edge.

    `data_origin` is the `<Origin>` element for the 'Data' wire: a Detour's dead side, or a ToolID
    that is not in `<Nodes>` at all.
    """
    sources = "".join(
        f"""
    <Node ToolID="{820 + i}"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>{i}</c></r></Data>
      </Configuration></Properties></Node>"""
        for i in range(10)
    )
    busy = "".join(
        f"""
    <Connection><Origin ToolID="{820 + i}" Connection="Output" />
      <Destination ToolID="898" Connection="Input" /></Connection>"""
        for i in range(10)
    )
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>{sources}
    <Node ToolID="830"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="CrateId" /></Fields>
        <Data><r><c>99</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="831"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="True" /></Configuration></Properties></Node>
    <Node ToolID="898"><GuiSettings Plugin="AlteryxSpatialPluginsGui.Summarize.Summarize" />
      <Properties><Configuration><SummarizeFields>
        <SummarizeField field="Shape" action="ConvexHull" rename="Hull" />
      </SummarizeFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>{busy}
    <Connection><Origin ToolID="830" Connection="Output" />
      <Destination ToolID="831" Connection="Input" /></Connection>
    <Connection>{data_origin}
      <Destination ToolID="898" Connection="Data" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


@pytest.mark.parametrize(
    ("data_origin", "probe"),
    [
        ('<Origin ToolID="831" Connection="Left" />', "a Detour's dead side"),
        ('<Origin ToolID="999" Connection="Output" />', "a ToolID absent from <Nodes>"),
    ],
    ids=["detour_dead_side", "absent_origin_tool"],
)
def test_a_port_is_not_held_for_an_anchor_whose_wire_can_never_become_an_edge(
    tmp_path: Path, data_origin: str, probe: str
):
    """The reservation is for anchors that lay edges, and these two anchors never will.

    Both wires are dropped in `_wire` — one because the Detour declared its left side empty, one
    because its origin is not a tool — so holding the tenth port for the 'Data' anchor holds it for
    a stream that never comes, and the tenth wire on 'Input' is refused for nothing.
    """
    result = convert_yxmd(_placeholder_fed_by_a_wire_that_leaves_from_nowhere(data_origin), source_name="probe.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    node = dumped_nodes(result)[rows[898].flowfile_node_ids[0]]
    assert len(node["input_ids"]) == 10, probe
    assert all(rows[820 + i].flowfile_node_ids[0] in node["input_ids"] for i in range(10)), probe
    # The dead wire is still reported — dropped, not silently merged — and nothing else is.
    assert full_input_messages(result) == []
    assert any(
        "nothing leaves the 'Left' anchor" in message or "was dropped; reconnect it by hand" in message
        for message in rows[898].messages
    ), probe
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_held_port_says_which_anchor_it_is_held_for(tmp_path: Path):
    """Nine wires on 'Input' and one on 'Data' leaves one port free, and the tenth is still refused.

    The refusal is right — the free port belongs to the anchor that has none — but the sentence used
    to read as arithmetic gone wrong: 'takes 10 ... already has its input from 9 ToolIDs', with no
    word about the port being held.
    """
    result = convert_yxmd(_crowded_placeholder(10), source_name="crowded.yxmd")
    sentence = dict(full_input_messages(result))[809]
    assert "Its 1 remaining port is held for the 1 Alteryx anchor wired into it that hold none yet" in sentence


def test_the_two_join_anchors_of_all_supported_still_fill_their_budget(tmp_path: Path):
    """The per-anchor share must not cost the shape that legitimately uses two anchors on one node."""
    result = convert_yxmd(read_fixture("all_supported.yxmd"), source_name="all_supported.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    join = dumped_nodes(result)[rows[12].flowfile_node_ids[0]]
    assert join["input_ids"] and join["right_input_id"] is not None
    assert full_input_messages(result) == []


UNREAD_ANCHOR_SENTENCE = (
    "The Alteryx 'DynamicRename' (ToolID {tool}) does not read its 'Right' anchor in this "
    "configuration, so no stream was taken from it; the connection from ToolID {source} ('Output') "
    "into that anchor was not carried over."
)


@pytest.fixture()
def rename_reading_no_anchor() -> ConversionResult:
    return convert_yxmd(A_NAME_WIRE_ON_A_RENAME_THAT_READS_NONE, source_name="unread_anchor.yxmd")


@pytest.mark.parametrize(
    ("mode", "source", "tool"),
    [("Formula", 761, 762), ("FirstRow", 771, 772), ("AddPrefixSuffix", 781, 782)],
    ids=["formula", "first_row", "add_prefix_suffix"],
)
def test_a_rename_that_reads_no_anchor_says_so(
    rename_reading_no_anchor: ConversionResult, mode: str, source: int, tool: int
):
    """These modes take their names from their own configuration, so both clauses of the old
    sentence — that the anchor was read, and that this wire is a further stream — were false."""
    result = rename_reading_no_anchor
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = UNREAD_ANCHOR_SENTENCE.format(tool=tool, source=source)
    assert (tool, sentence) in dropped_messages(result)
    assert (source, sentence) in dropped_messages(result)
    assert (rows[tool].status, rows[tool].reason) == ("partial", "dropped_connection")
    assert (rows[source].status, rows[source].reason) == ("partial", "dropped_connection")
    assert rows[tool].messages.count(sentence) == 1
    assert dumped_nodes(result)[rows[source].flowfile_node_ids[0]]["outputs"] == []


def test_no_rename_that_reads_no_anchor_claims_it_read_one(tmp_path: Path, rename_reading_no_anchor: ConversionResult):
    """The old wording must be gone entirely, not merely joined by the new one."""
    result = rename_reading_no_anchor
    assert full_input_messages(result) == []
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert len(rows[782].flowfile_node_ids) == 2
    nodes = dumped_nodes(result)
    assert nodes[rows[782].flowfile_node_ids[0]]["outputs"] == [rows[782].flowfile_node_ids[1]]
    for data, tool in ((760, 762), (770, 772), (780, 782)):
        assert nodes[rows[tool].flowfile_node_ids[0]]["input_ids"] == rows[data].flowfile_node_ids
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_the_right_input_rename_reports_nothing_when_it_really_read_the_anchor():
    """The "does not read this anchor" wording must not reach the mode that does read it.

    One stream on 'Right' is read, so there is no unread stream and no dropped wire to report —
    which is what makes the two wordings around it (the unread-anchor modes above, and the refusal
    when several streams arrive) provably about their own case and not about this one.
    """
    result = convert_yxmd(ONE_NAME_SOURCE_ON_A_DYNAMIC_RENAME, source_name="one_name_source.yxmd")
    assert dropped_messages(result) == []
    assert full_input_messages(result) == []


@pytest.mark.parametrize(
    ("pattern", "is_backreference"),
    [
        (r"(\w)\1", True),
        (r"(\w)\\\1", True),
        (r"(\w)\9", True),
        (r"\\1", False),
        (r"\\\\1", False),
        (r"\w{2}\\02 Learn_one_tool_at_a_time\\22 Developer\\", False),
        (r"\0", False),
        (r"\d{2}", False),
    ],
    ids=[
        "plain",
        "behind_an_escaped_backslash",
        "group_nine",
        "escaped_backslash_then_literal",
        "two_escaped_backslashes",
        "windows_path",
        "group_zero_is_not_one",
        "escape_that_is_not_a_digit",
    ],
)
def test_a_backreference_is_a_digit_behind_an_odd_number_of_backslashes(pattern: str, is_backreference: bool):
    """`\\[1-9]` read the second half of an escaped backslash as the start of a backreference.

    A Windows path written inside a regex — which is what `RunCommand.yxmd` tool 15 holds — was
    therefore refused for a construct it does not contain. That tool is still refused, because its
    pattern ends in a backslash, but for the reason that is true of it.
    """
    from flowfile_core.flowfile.converters.alteryx.mappers import _REGEX_BACKREF_RE

    assert bool(_REGEX_BACKREF_RE.search(pattern)) is is_backreference


# The same Excel read written both ways round. Alteryx spells the Excel header option
# `FirstRowData` with the sense inverted, so `True` means "no header row".
EXCEL_HEADER_BOTH_WAYS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="7841"><GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties>
        <Configuration>
          <File OutputFileName="" RecordLimit="" SearchSubDirs="False" FileFormat="25">crates.xlsx|||`Sheet1$`</File>
          <FormatSpecificOptions>
            <FirstRowData>True</FirstRowData>
            <ImportLine>1</ImportLine>
          </FormatSpecificOptions>
        </Configuration>
        <MetaInfo connection="Output"><RecordInfo>
          <Field name="Field_1" type="V_String" size="32" />
          <Field name="Field_2" type="V_String" size="32" />
        </RecordInfo></MetaInfo>
      </Properties></Node>
    <Node ToolID="7701"><GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties>
        <Configuration>
          <File OutputFileName="" RecordLimit="" SearchSubDirs="False" FileFormat="25">crates.xlsx|||`Sheet1$`</File>
          <FormatSpecificOptions>
            <FirstRowData>False</FirstRowData>
            <ImportLine>1</ImportLine>
          </FormatSpecificOptions>
        </Configuration>
        <MetaInfo connection="Output"><RecordInfo>
          <Field name="Field_1" type="V_String" size="32" />
          <Field name="Field_2" type="V_String" size="32" />
        </RecordInfo></MetaInfo>
      </Properties></Node>
  </Nodes>
  <Connections />
</AlteryxDocument>
"""


def test_a_headerless_excel_read_renames_the_positional_columns():
    """`FirstRowData=True` is the Excel spelling of "no header row", and it never used to be read.

    Only the CSV/JSON spelling (`HeaderRow`) was, so the positional-rename branch — the one that
    stops every downstream reference to Alteryx's Field_1..N silently missing Polars' column_1..N —
    was unreachable for Excel however the tool was configured.
    """
    result = convert_yxmd(EXCEL_HEADER_BOTH_WAYS, source_name="excel_header.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    nodes = dumped_nodes(result)

    headerless = rows[7841]  # FirstRowData=True -> the first row is data
    assert len(headerless.flowfile_node_ids) == 2
    read, rename = (nodes[node_id] for node_id in headerless.flowfile_node_ids)
    assert read["setting_input"]["received_file"]["table_settings"]["has_headers"] is False
    assert [(entry["old_name"], entry["new_name"]) for entry in rename["setting_input"]["select_input"]] == [
        ("column_1", "Field_1"),
        ("column_2", "Field_2"),
    ]
    assert any("read without headers" in message for message in headerless.messages)

    with_header = rows[7701]  # FirstRowData=False -> the first row names the columns
    assert len(with_header.flowfile_node_ids) == 1
    assert nodes[with_header.flowfile_node_ids[0]]["setting_input"]["received_file"]["table_settings"]["has_headers"]


# Two Text Inputs onto one RegEx tool's 'Input'. A RegEx becomes a 10-port `polars_code`, so both
# wires really land — which is what makes "the columns arriving here" a question about a union.
TWO_STREAMS_INTO_ONE_ANCHOR_A_MAPPER_READS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="771"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="code" /></Fields>
        <Data><r><c>AB-12</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="772"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="code" /><Field name="letters" /></Fields>
        <Data><r><c>ZZ-99</c><c>ZZ</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="773"><GuiSettings Plugin="AlteryxBasePluginsGui.RegEx.RegEx" />
      <Properties><Configuration>
        <Field>code</Field>
        <RegExExpression value="([A-Z]{2})-([0-9]{2})" />
        <CaseInsensitve value="False" />
        <Method>ParseComplex</Method>
        <ParseComplex>
          <Field field="letters" type="String" size="2" />
          <Field field="digits" type="String" size="2" />
        </ParseComplex>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="771" Connection="Output" />
      <Destination ToolID="773" Connection="Input" /></Connection>
    <Connection><Origin ToolID="772" Connection="Output" />
      <Destination ToolID="773" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""

MULTI_STREAM_READ_SENTENCE = (
    "The Alteryx '{tool}' (ToolID {tool_id}) reads its '{anchor}' anchor at import time, and {n} "
    "connections arrive there — from {sources}. Alteryx unions those streams, so what reaches this tool "
    "is not any one of them; the import answered 'unknown' rather than taking the first."
)


def test_a_mapper_reading_an_anchor_two_streams_arrive_on_is_told_both_origins():
    """`input_columns` took the first wire; on a 10-port node both wires really are laid.

    The RegEx mapper asks which columns arrive so it can refuse a parse name that already exists —
    and one of the two streams here brings a 'letters' column while the other does not. Answering
    from whichever wire the document wrote first is a statement about line numbering, so the row now
    names both origins in file order and the answer is unknown.
    """
    result = convert_yxmd(TWO_STREAMS_INTO_ONE_ANCHOR_A_MAPPER_READS, source_name="two_streams.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    sentence = MULTI_STREAM_READ_SENTENCE.format(
        tool="RegEx", tool_id=773, anchor="Input", n=2, sources="ToolID 771 ('Output'), ToolID 772 ('Output')"
    )
    assert sentence in rows[773].messages
    # Both wires reach the node, which is exactly why the first one is not the answer.
    assert len(dumped_nodes(result)[rows[773].flowfile_node_ids[0]]["input_ids"]) == 2
    assert full_input_messages(result) == []


def test_a_no_op_still_carries_the_first_of_two_streams_rather_than_none(tmp_path: Path):
    """The read rule stops at the relay: a no-op that carried nothing would lose data the flow has.

    `emit_passthrough` is deciding an edge, not answering a question about the data, and wiring
    already tells both ends about every wire it did not carry — so carrying one beats carrying none,
    the way the arity rule gives a contested port to one anchor rather than to neither.
    """
    result = convert_yxmd(TWO_WIRES_INTO_ONE_MESSAGE, source_name="two_wires_message.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert rows[713].status == "no_op"
    assert dumped_nodes(result)[rows[714].flowfile_node_ids[0]]["input_ids"] == rows[711].flowfile_node_ids
    assert [tool_id for tool_id, _ in dropped_messages(result)] == [712, 713]
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_detour_end_refuses_two_live_streams_on_one_anchor(tmp_path: Path):
    """The Detour End's live side already answers only for one wire; this pins that it still does.

    It is the one reader that got this rule right from the start: two live streams on one anchor mean
    the live one cannot be told from the other, so it passes its input through and says why instead
    of resolving the tie by document order.
    """
    result = convert_yxmd(TWO_LIVE_WIRES_ON_ONE_DETOUR_END, source_name="two_live.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    assert (rows[735].status, rows[735].reason) == ("placeholder", "mapper_refused")
    assert rows[735].messages == [
        "2 of the 2 inputs of this Alteryx Detour End come from the live anchor of a Detour, so which "
        "stream continues cannot be read from the workflow; this node passes its input through."
    ]
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success


def test_a_detour_end_is_never_told_about_the_dead_side_of_its_own_detour(detour: ConversionResult):
    """The dead wire is expected here and carries nothing; reporting it would be noise, not news."""
    rows = {row.alteryx_tool_id: row for row in detour.report.rows}
    for end_id, detour_id, live in ((5, 2, "Right"), (10, 7, "Left")):
        assert (rows[end_id].status, rows[end_id].reason) == ("no_op", "no_op")
        assert rows[end_id].messages == [
            "This tool has no effect on the data (messages, tests, ordering hints); its input is wired "
            "straight to what it fed.",
            f"The records arrive from the '{live}' anchor of the Alteryx Detour "
            f"(ToolID {detour_id}), which is the side its configuration makes live.",
        ]


def test_a_url_outside_an_explorer_box_stays_in_its_configuration_dump():
    """Screening every <URL> would strip the address an unmapped tool needs to be rebuilt."""
    result = convert_yxmd(AN_ORDINARY_TOOL_WITH_A_URL, source_name="url.yxmd")
    row = report_row(result, 761)
    body = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "https://orchard.example.com/feed?token=okonjo-secret" in body
    assert "Credential values were not copied" not in body


def test_make_group_and_field_info_publish_the_columns_alteryx_gives_them():
    """Downstream mappers read `tool_columns`, which the finished flow no longer shows."""
    ctx, _ = emit_tools(parse_yxmd(read_fixture("make_group.yxmd")))
    assert ctx.tool_columns[2] == ["Key", "Group"]
    ctx, _ = emit_tools(parse_yxmd(read_fixture("field_info.yxmd")))
    assert ctx.tool_columns[2] == ["Name", "Type"]


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
    # 8 tools, 6 of them settled non-goals: only TextInput and XMLParse are Flowfile's to convert.
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
    assert '// Original: REGEX_Match([_CurrentField_], "^(?=A).*")' in formula
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
    assert "# Alteryx annotation: Parse the payload column" in code
    assert "<OutputFieldName>XMLParse_Out</OutputFieldName>" in code
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
            "transpose-no-data-fields",
            "AlteryxBasePluginsGui.Transpose.Transpose",
            '<KeyFields /><DataFields><Field field="value" selected="False" /></DataFields>',
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


# --- Random Records ---

RANDOM_RECORDS_DEFAULTS = {
    "Number": "False",
    "NNumber": "1000",
    "Percent": "False",
    "NPercent": "10",
    "Deterministic": "False",
    "Seed": "17",
}


def random_records_config(overrides: dict[str, str | None]) -> str:
    values = {**RANDOM_RECORDS_DEFAULTS, **overrides}
    return "".join(f'<Value name="{name}">{text}</Value>' for name, text in values.items() if text is not None)


@pytest.fixture()
def random_records() -> ConversionResult:
    return convert("random_records.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "expected"),
    [
        (2, {"sample_method": "random", "sample_size": 3, "fraction": 10.0, "seed": None}),
        (3, {"sample_method": "random_fraction", "sample_size": 1000, "fraction": 50.0, "seed": None}),
        (4, {"sample_method": "random_fraction", "sample_size": 1000, "fraction": 25.0, "seed": 458676342}),
    ],
    ids=["a_record_count", "a_percentage", "a_percentage_with_a_seed"],
)
def test_random_records_maps_the_macro_onto_the_sample_node(
    random_records: ConversionResult, tool_id: int, expected: dict
):
    """`fraction` on the node is a percentage — the engine divides by 100, so 50 stays 50."""
    row = report_row(random_records, tool_id)
    assert (row.status, row.reason, row.flowfile_node_type) == ("converted", "converted", "sample")
    settings = dumped_nodes(random_records)[row.flowfile_node_ids[0]]["setting_input"]
    assert {key: settings[key] for key in expected} == expected


def test_a_random_records_without_a_seed_says_it_draws_again_every_run(random_records: ConversionResult):
    sentence = "This Alteryx Random Records tool is not deterministic, so it draws different rows on every run."
    assert sentence in report_row(random_records, 2).messages
    assert sentence not in report_row(random_records, 4).messages


@pytest.mark.parametrize(
    ("case_id", "overrides", "reason", "fragment"),
    [
        ("neither", {}, "mapper_refused", "neither a record count nor a percentage"),
        (
            "both",
            {"Number": "True", "Percent": "True"},
            "mapper_refused",
            "both a record count and a percentage",
        ),
        (
            "unreadable_count",
            {"Number": "True", "NNumber": "a few"},
            "mapper_refused",
            "record count could not be read",
        ),
        (
            "unreadable_percentage",
            {"Percent": "True", "NPercent": "most"},
            "mapper_refused",
            "percentage could not be read",
        ),
        ("percentage_above_100", {"Percent": "True", "NPercent": "250"}, "mapper_refused", "is not between 0 and 100"),
        ("negative_percentage", {"Percent": "True", "NPercent": "-5"}, "mapper_refused", "is not between 0 and 100"),
        ("zero_percentage", {"Percent": "True", "NPercent": "0"}, "mapper_refused", "is not between 0 and 100"),
        (
            "unreadable_seed",
            {"Number": "True", "Deterministic": "True", "Seed": ""},
            "mapper_refused",
            "its seed could not be read",
        ),
        (
            "a_negative_seed",
            {"Number": "True", "Deterministic": "True", "Seed": "-1"},
            "mapper_refused",
            "its seed could not be read",
        ),
        (
            "an_infinite_seed",
            {"Number": "True", "Deterministic": "True", "Seed": "1e400"},
            "mapper_refused",
            "its seed could not be read",
        ),
        (
            "a_decimal_seed",
            {"Number": "True", "Deterministic": "True", "Seed": "7.5"},
            "mapper_refused",
            "its seed could not be read",
        ),
        ("a_negative_count", {"Number": "True", "NNumber": "-5"}, "mapper_refused", "record count could not be read"),
        ("a_zero_count", {"Number": "True", "NNumber": "0"}, "mapper_refused", "record count could not be read"),
        ("a_decimal_count", {"Number": "True", "NNumber": "2.7"}, "mapper_refused", "record count could not be read"),
        (
            "an_infinite_count",
            {"Number": "True", "NNumber": "1e400"},
            "mapper_refused",
            "record count could not be read",
        ),
        (
            "an_option_flowfile_does_not_read",
            {"Number": "True", "WithReplacement": "True"},
            "option_unsupported",
            "settings Flowfile does not read: WithReplacement",
        ),
    ],
)
def test_random_records_fails_closed(case_id: str, overrides: dict, reason: str, fragment: str):
    """A count that is not a number, a percentage outside 0-100 or an option added by a later
    macro build are all read and refused, never rounded into something that runs."""
    document = macro_after_text_input("RandomRecords.yxmc", random_records_config(overrides))
    row = report_row(convert_yxmd(document, source_name="random_records.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", reason)
    assert any(fragment in message for message in row.messages), row.messages


def test_the_random_records_flow_runs(tmp_path: Path, random_records: ConversionResult):
    """The seeded tool keeps exactly a quarter of four rows; the rest only have to execute."""
    flow = open_flow(write_flow(random_records, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    seeded = report_row(random_records, 4).flowfile_node_ids[0]
    frame = flow.get_node(seeded).get_resulting_data().data_frame.collect()
    assert frame.columns == ["CrateId", "Grower"]
    assert frame.height == 1


# --- Data Cleanse Pro ---


CLEANSE_PRO_PLUGIN = "AlteryxBasePluginsGui.DataCleansePro.DataCleansePro"

# The same tool behind an unmapped one, whose output columns nothing can name.
CLEANSE_PRO_BEHIND_A_PLACEHOLDER = b"""<?xml version="1.0"?>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.XMLParse.XMLParse" />
      <Properties><Configuration /></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DataCleansePro.DataCleansePro" />
      <Properties><Configuration>%s</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


CLEANSE_PRO_DEFAULTS = {
    "RemoveNullRows": "False",
    "RemoveNullColumns": "False",
    "RemoveTabsLineBreaksAndDuplicates": "False",
    "RemoveLeadingAndTrailingWhitespace": "False",
    "RemoveAllWhitespaces": "False",
    "RemoveHTMLTags": "False",
    "RemoveInvisibleCharacters": "False",
    "RemoveLetters": "False",
    "RemoveNumbers": "False",
    "RemovePunctuation": "False",
    "Letters": "",
    "Numbers": "",
    "Punctuations": "",
    "Exceptions": "",
    "Checkbox_ReplaceStringColumns": "False",
    "Checkbox_ReplaceNumericColumns": "False",
    "radioButton_ReplaceNullwithBlanks": "True",
    "radioButton_ReplaceBlankswithNulls": "False",
    "radioButton_ReplaceNullwithZero": "True",
    "radioButton_ReplaceZerowithNulls": "False",
    "ReplaceWithBlanks": "False",
    "ReplaceWithZero": "False",
    "CheckBox_ModifyCase": "False",
    "ModifyCase": "none",
}

CLEANSE_PRO_FIELDS = (
    '<Fields><Field value="value" selected="True" /><Field value="*Unknown" selected="False" /></Fields>'
)


def cleanse_pro_config(overrides: dict[str, str] | None = None, fields: str = CLEANSE_PRO_FIELDS) -> str:
    values = {**CLEANSE_PRO_DEFAULTS, **(overrides or {})}
    return "".join(f'<{name} value="{text}" />' for name, text in values.items()) + fields


@pytest.fixture()
def cleanse_pro() -> ConversionResult:
    return convert("data_cleanse_pro.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "expected"),
    [
        (
            2,
            {
                "selection_mode": "list",
                "selected_columns": ["Grower"],
                "trim_whitespace": True,
                "remove_numbers": True,
            },
        ),
        (3, {"selection_mode": "all", "selected_columns": [], "normalize_whitespace": True}),
        (4, {"selection_mode": "list", "selected_columns": ["Crates"], "replace_nulls_with_zero": True}),
        (
            5,
            {
                "selection_mode": "list",
                "selected_columns": ["Grower", "Packhouse"],
                "case_mode": "uppercase",
                "remove_punctuation": True,
            },
        ),
    ],
    ids=["a_named_list", "everything_via_unknown", "numeric_nulls_to_zero", "uppercase"],
)
def test_data_cleanse_pro_maps_onto_the_data_cleansing_node(
    cleanse_pro: ConversionResult, tool_id: int, expected: dict
):
    """Every rule that is off has to stay off: the defaults on the node are not all False."""
    row = report_row(cleanse_pro, tool_id)
    assert (row.status, row.reason, row.flowfile_node_type) == ("converted", "converted", "data_cleansing")
    settings = dumped_nodes(cleanse_pro)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert settings == {
        "remove_null_rows": False,
        "remove_null_columns": False,
        "selection_mode": "list",
        "selected_columns": [],
        "replace_nulls_with_blank": False,
        "replace_nulls_with_zero": False,
        "trim_whitespace": False,
        "normalize_whitespace": False,
        "remove_all_whitespace": False,
        "remove_letters": False,
        "remove_numbers": False,
        "remove_punctuation": False,
        "case_mode": "none",
        **expected,
    }


def test_data_cleanse_pro_reads_the_field_names_from_the_value_attribute():
    """Data Cleanse Pro writes @value where every other tool writes @name; @name selects nothing."""
    fields = '<Fields><Field name="value" selected="True" /><Field value="*Unknown" selected="False" /></Fields>'
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config(fields=fields))
    settings = dumped_nodes(convert_yxmd(document, source_name="pro.yxmd"))[2]["setting_input"]["cleansing_input"]
    assert settings["selected_columns"] == []
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config())
    settings = dumped_nodes(convert_yxmd(document, source_name="pro.yxmd"))[2]["setting_input"]["cleansing_input"]
    assert settings["selected_columns"] == ["value"]


@pytest.mark.parametrize(
    ("case_id", "overrides", "expected"),
    [
        ("both_off", {}, False),
        (
            "checkbox_only",
            {"Checkbox_ReplaceStringColumns": "True", "radioButton_ReplaceNullwithBlanks": "False"},
            False,
        ),
        ("radio_only_is_stale_state", {"radioButton_ReplaceNullwithBlanks": "True"}, False),
        (
            "both_on",
            {
                "Checkbox_ReplaceStringColumns": "True",
                "radioButton_ReplaceNullwithBlanks": "True",
                "ReplaceWithBlanks": "True",
            },
            True,
        ),
    ],
)
def test_a_cleanse_pro_radio_button_only_counts_under_a_ticked_checkbox(case_id: str, overrides: dict, expected: bool):
    """Alteryx keeps the radio's last position when the box is unticked, so the box decides too."""
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config(overrides))
    settings = dumped_nodes(convert_yxmd(document, source_name="pro.yxmd"))[2]["setting_input"]["cleansing_input"]
    assert settings["replace_nulls_with_blank"] is expected


def test_a_cleanse_pro_that_contradicts_its_own_resolved_setting_is_refused():
    """ReplaceWithBlanks is what Alteryx resolved the pair into; disagreeing means this is misread."""
    overrides = {"Checkbox_ReplaceStringColumns": "True", "radioButton_ReplaceNullwithBlanks": "True"}
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config(overrides))
    row = report_row(convert_yxmd(document, source_name="pro.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert any("its own <ReplaceWithBlanks> says the opposite" in message for message in row.messages), row.messages


@pytest.mark.parametrize(
    ("case_id", "overrides", "fragment"),
    [
        ("html_tags", {"RemoveHTMLTags": "True"}, "<RemoveHTMLTags> (removing HTML tags)"),
        (
            "invisible_characters",
            {"RemoveInvisibleCharacters": "True"},
            "<RemoveInvisibleCharacters> (removing invisible characters)",
        ),
        (
            "blanks_to_nulls",
            {"Checkbox_ReplaceStringColumns": "True", "radioButton_ReplaceBlankswithNulls": "True"},
            "<radioButton_ReplaceBlankswithNulls> (replacing blanks with nulls)",
        ),
        (
            "zeroes_to_nulls",
            {"Checkbox_ReplaceNumericColumns": "True", "radioButton_ReplaceZerowithNulls": "True"},
            "<radioButton_ReplaceZerowithNulls> (replacing zeroes with nulls)",
        ),
        ("letters", {"Letters": "aeiou"}, "overrides the <Letters> character set"),
        ("exceptions", {"Exceptions": "-"}, "overrides the <Exceptions> character set"),
        ("unknown_case", {"CheckBox_ModifyCase": "True", "ModifyCase": "sentence"}, "case mode 'sentence'"),
    ],
)
def test_data_cleanse_pro_fails_closed(case_id: str, overrides: dict, fragment: str):
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config(overrides))
    row = report_row(convert_yxmd(document, source_name="pro.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any(fragment in message for message in row.messages), row.messages


def test_a_cleanse_pro_option_flowfile_does_not_read_fails_closed():
    """A later build adding an option must not convert as though the option were not there."""
    config = cleanse_pro_config() + '<RemoveEmoji value="True" />'
    row = report_row(convert_yxmd(tool_after_text_input(CLEANSE_PRO_PLUGIN, config), source_name="pro.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any("settings Flowfile does not read: RemoveEmoji" in message for message in row.messages), row.messages


def test_a_cleanse_pro_selecting_all_but_one_column_freezes_the_list_and_says_so():
    """`*Unknown` on with a named field off is "everything except"; a named list cannot grow."""
    fields = '<Fields><Field value="value" selected="False" /><Field value="*Unknown" selected="True" /></Fields>'
    document = tool_after_text_input(CLEANSE_PRO_PLUGIN, cleanse_pro_config(fields=fields))
    result = convert_yxmd(document, source_name="pro.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("cleanses every column except value" in message for message in row.messages), row.messages
    settings = dumped_nodes(result)[row.flowfile_node_ids[0]]["setting_input"]["cleansing_input"]
    assert (settings["selection_mode"], settings["selected_columns"]) == ("list", [])


def test_a_cleanse_pro_selecting_all_but_one_is_refused_when_the_columns_are_unknown():
    """Behind an unmapped tool there is no column list to subtract from, so nothing is guessed."""
    fields = '<Fields><Field value="value" selected="False" /><Field value="*Unknown" selected="True" /></Fields>'
    document = CLEANSE_PRO_BEHIND_A_PLACEHOLDER % cleanse_pro_config(fields=fields).encode()
    row = report_row(convert_yxmd(document, source_name="pro.yxmd"), 3)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert any("not known at import time" in message for message in row.messages), row.messages


def test_the_cleanse_pro_flow_runs_and_applies_each_rule(tmp_path: Path, cleanse_pro: ConversionResult):
    flow = open_flow(write_flow(cleanse_pro, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frames = {
        tool_id: flow.get_node(report_row(cleanse_pro, tool_id).flowfile_node_ids[0])
        .get_resulting_data()
        .data_frame.collect()
        for tool_id in (2, 3, 4, 5)
    }
    assert frames[2]["Grower"].to_list() == ["Okonjo", "salgado", "Brandt"]
    assert frames[3]["Packhouse"].to_list() == ["Shed 2", "Shed 1", None]
    assert frames[4]["Crates"].to_list() == [7, 0, 4]
    assert frames[5]["Grower"].to_list() == [" OKONJO ", "SALGADO", "BRANDT"]


# --- Transpose *Unknown ---

TRANSPOSE_UNKNOWN_CONFIG = """
        <ErrorWarn>Ignore</ErrorWarn>
        <KeyFields><Field field="%s" /></KeyFields>
        <DataFields>
          <Field field="%s" selected="False" />
          <Field field="*Unknown" selected="True" />
        </DataFields>"""


@pytest.fixture()
def transpose_unknown() -> ConversionResult:
    return convert("transpose_unknown.yxmd")


def test_transpose_unknown_becomes_a_selector_over_everything_else(transpose_unknown: ConversionResult):
    """`*Unknown` is resolved at run time by Alteryx, so the code says what to leave out, not in."""
    row = report_row(transpose_unknown, 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("converted", "converted", "polars_code")
    body = dumped_nodes(transpose_unknown)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"][
        "polars_code"
    ]
    ast.parse(body)
    assert body.endswith(
        "output_df = input_df.unpivot(\n"
        "    on=cs.exclude(['Ref', 'Bucket']),\n"
        "    index=['Ref'],\n"
        "    variable_name='Name',\n"
        "    value_name='Value',\n"
        ")"
    )


def test_a_transpose_set_to_warn_on_mixed_types_says_so(transpose_unknown: ConversionResult):
    """Alteryx warns about data fields that do not share a type; polars widens or fails instead."""
    sentence = (
        "This Alteryx Transpose is set to warn when its data fields do not share one type; Flowfile widens "
        "them to a common type instead, and fails when there is none."
    )
    assert sentence in report_row(transpose_unknown, 2).messages
    assert sentence not in report_row(transpose_unknown, 3).messages
    # Tool 4 is the STATIC path set to Warn: both paths return the same `_transpose_messages`.
    static_warn = report_row(transpose_unknown, 4)
    assert (static_warn.status, static_warn.flowfile_node_type) == ("converted", "unpivot")
    assert static_warn.messages == [sentence]


def test_the_static_transpose_path_is_untouched_by_the_unknown_branch(transpose_unknown: ConversionResult):
    """A listed selection still becomes the native unpivot plus the Alteryx rename, as before."""
    row = report_row(transpose_unknown, 3)
    assert (row.status, row.flowfile_node_type) == ("converted", "unpivot")
    nodes = dumped_nodes(transpose_unknown)
    unpivot_id, rename_id = row.flowfile_node_ids
    assert nodes[unpivot_id]["setting_input"]["unpivot_input"] == {
        "index_columns": ["Ref"],
        "value_columns": ["Q1", "Q2"],
        "data_type_selector": None,
        "data_type_selector_mode": "column",
    }
    assert [item["new_name"] for item in nodes[rename_id]["setting_input"]["select_input"]] == ["Name", "Value"]


def test_the_transpose_unknown_flow_runs_and_keeps_only_the_data_columns(
    tmp_path: Path, transpose_unknown: ConversionResult
):
    """Ref is a key and Bucket is deselected, so only Q1 and Q2 become Name/Value rows."""
    flow = open_flow(write_flow(transpose_unknown, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    node_id = report_row(transpose_unknown, 2).flowfile_node_ids[0]
    frame = flow.get_node(node_id).get_resulting_data().data_frame.collect().sort("Ref", "Name")
    assert frame.columns == ["Ref", "Name", "Value"]
    assert frame.rows() == [("C-1", "Q1", 11), ("C-1", "Q2", 12), ("C-2", "Q1", 21), ("C-2", "Q2", 22)]


@pytest.mark.parametrize(
    ("case_id", "key", "deselected"),
    [("a_key_field", "value\\\\", "other"), ("a_deselected_field", "value", "other\\\\")],
)
def test_a_transpose_name_ending_in_a_backslash_is_refused(case_id: str, key: str, deselected: str):
    """The name lands in a generated body, where a trailing backslash breaks the comment stripper."""
    config = TRANSPOSE_UNKNOWN_CONFIG % (key, deselected)
    document = tool_after_text_input("AlteryxBasePluginsGui.Transpose.Transpose", config)
    row = report_row(convert_yxmd(document, source_name="transpose.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any("ends with a backslash" in message for message in row.messages), row.messages


# --- Date Time ---

DATETIME_PLUGIN = "AlteryxBasePluginsGui.DateTime.DateTime"


def datetime_config(alteryx_format: str, *, is_from: str = "False", **overrides: str) -> str:
    values = {
        "IsFrom": f'<IsFrom value="{is_from}" />',
        "InputFieldName": "<InputFieldName>value</InputFieldName>",
        "Language": "<Language>English</Language>",
        "Format": f"<Format>{alteryx_format}</Format>",
        "OutputFieldName": "<OutputFieldName>parsed</OutputFieldName>",
    }
    values.update(overrides)
    return "".join(values.values())


@pytest.fixture()
def datetime_tokens() -> ConversionResult:
    return convert("datetime_tokens.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "column", "expression"),
    [
        (2, "Shipped on", 'to_date([Shipped text], "%m-%d/%Y")'),
        (3, "Gate moment", 'to_datetime([Gate time], "%H:%M:%S")'),
        (4, "Picked label", 'format_date([Picked on], "%B %d, %Y")'),
        (5, "Weighed label", 'format_date([Weighed at], "%A, %d %B, %Y")'),
    ],
    ids=["parse_a_date", "parse_a_time", "format_a_date", "format_a_datetime"],
)
def test_date_time_becomes_one_formula_per_tool(
    datetime_tokens: ConversionResult, tool_id: int, column: str, expression: str
):
    """`IsFrom` is the whole mode: true writes a string out of a date, false reads one in."""
    row = report_row(datetime_tokens, tool_id)
    assert (row.flowfile_node_type, len(row.flowfile_node_ids)) == ("formula", 1)
    settings = dumped_nodes(datetime_tokens)[row.flowfile_node_ids[0]]["setting_input"]["function"]
    assert settings["field"]["name"] == column
    assert settings["function"] == expression


def test_a_time_only_parse_is_partial_because_flowfile_has_no_time_column(datetime_tokens: ConversionResult):
    """`to_datetime` puts a bare time on 0001-01-01, which is not what Alteryx calls a Time."""
    row = report_row(datetime_tokens, 3)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("becomes a Datetime holding that time on 0001-01-01" in message for message in row.messages)
    assert report_row(datetime_tokens, 2).status == "converted"


@pytest.mark.parametrize("tool_id", [4, 5])
def test_formatting_a_date_is_partial_because_alteryx_keeps_dates_as_text(
    datetime_tokens: ConversionResult, tool_id: int
):
    """`format_date` compiles to `.dt.to_string()`, which raises on the String an Alteryx Text
    Input becomes — three of the six corpus instances do exactly that, so this is not theoretical."""
    row = report_row(datetime_tokens, tool_id)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("has to be one by the time this node runs" in message for message in row.messages), row.messages
    assert report_row(datetime_tokens, 2).status == "converted"


def test_the_datetime_flow_runs_and_reproduces_the_alteryx_values(tmp_path: Path, datetime_tokens: ConversionResult):
    """Both directions on real values: the format tools read declared Date/DateTime columns."""
    flow = open_flow(write_flow(datetime_tokens, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frames = {
        tool_id: flow.get_node(report_row(datetime_tokens, tool_id).flowfile_node_ids[0])
        .get_resulting_data()
        .data_frame.collect()
        for tool_id in (2, 3, 4, 5)
    }
    assert frames[2]["Shipped on"].to_list() == [date(2024, 7, 19), date(2024, 11, 2)]
    assert frames[3]["Gate moment"].to_list() == [datetime(1, 1, 1, 8, 15), datetime(1, 1, 1, 16, 40, 30)]
    assert frames[4]["Picked label"].to_list() == ["March 05, 2024", "November 21, 2024"]
    assert frames[5]["Weighed label"].to_list() == ["Tuesday, 05 March, 2024", "Thursday, 21 November, 2024"]


@pytest.mark.parametrize(
    ("alteryx_format", "strftime"),
    [
        ("yyyy-MM-dd", '"%Y-%m-%d"'),
        ("dd/MM/yyyy HH:mm:ss", '"%d/%m/%Y %H:%M:%S"'),
        ("hh:mm tt", '"%I:%M %p"'),
        ("dy Mon dd yyyy", '"%a %b %d %Y"'),
        ("day, Month dd, yyyy", '"%A, %B %d, %Y"'),
        ("yyyy-MM-dd 100%", '"%Y-%m-%d 100%%"'),
    ],
)
def test_the_alteryx_token_table_becomes_strftime(alteryx_format: str, strftime: str):
    """A run of letters is one token or none, so `Month` is never read as `MM` plus a literal."""
    document = tool_after_text_input(DATETIME_PLUGIN, datetime_config(alteryx_format))
    row = report_row(convert_yxmd(document, source_name="dt.yxmd"), 2)
    assert row.flowfile_node_type == "formula", row.messages
    function = dumped_nodes(convert_yxmd(document, source_name="dt.yxmd"))[2]["setting_input"]["function"]["function"]
    assert function.endswith(f"{strftime})")


@pytest.mark.parametrize(
    ("case_id", "config", "fragment"),
    [
        ("two_digit_year", datetime_config("MM/dd/yy"), "'yy' is not a date token"),
        ("unpadded_day", datetime_config("M/d/yyyy"), "'M' is not a date token"),
        ("unpadded_hour", datetime_config("H:mm"), "'H' is not a date token"),
        ("an_unknown_token", datetime_config("yyyy-MM-ddTzz"), "'ddTzz' is not a date token"),
        ("a_compounded_word", datetime_config("Monday"), "'Monday' is not a date token"),
        ("a_repeated_token", datetime_config("dddd"), "'dddd' is not a date token"),
        ("tokens_with_no_separator", datetime_config("yyyyMMdd"), "'yyyyMMdd' is not a date token"),
        ("no_date_part_at_all", datetime_config("--/--"), "it names no date or time part at all"),
        ("an_empty_format", datetime_config(""), "the format is empty"),
        (
            "another_language",
            datetime_config("yyyy-MM-dd", Language="<Language>French</Language>"),
            "reads or writes French month and day names",
        ),
        ("no_input_field", datetime_config("yyyy", InputFieldName="<InputFieldName />"), "names no input field"),
        ("no_output_field", datetime_config("yyyy", OutputFieldName="<OutputFieldName />"), "names no output field"),
        ("no_direction", datetime_config("yyyy", IsFrom="<IsFrom />"), "whether it reads a string or writes one"),
    ],
)
def test_date_time_fails_closed(case_id: str, config: str, fragment: str):
    """An unread token would format the wrong thing silently, so anything unverified is refused.

    The three separator-free cases are the point: `Monday` used to become `%b%A` and `dddd` `%d%d`,
    both `converted` and both wrong; `yyyyMMdd` happened to be right, by luck rather than by rule.
    """
    row = report_row(convert_yxmd(tool_after_text_input(DATETIME_PLUGIN, config), source_name="dt.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any(fragment in message for message in row.messages), row.messages


# --- Rank ---

RANK_PLUGIN = "AlteryxBasePluginsGui.Rank.Rank"


def rank_config(modes: str, sort_info: str = '<Field field="value" order="Descending" />', groups: str = "") -> str:
    return (
        f"<RankingModes>{modes}</RankingModes>"
        f'<SortInfo locale="0">{sort_info}</SortInfo>'
        f"<GroupFields>{groups}</GroupFields>"
    )


@pytest.fixture()
def rank_modes() -> ConversionResult:
    return convert("rank_modes.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "status", "reason", "expression"),
    [
        (2, "converted", "converted", "pl.col('Units').rank(method='dense', descending=True).alias('Rank')"),
        (3, "converted", "converted", "pl.col('Units').rank(method='average', descending=True).alias('Rank')"),
        (4, "partial", "row_order_unknown", "pl.col('Units').rank(method='ordinal', descending=True).alias('Rank')"),
        (
            5,
            "partial",
            "row_order_unknown",
            "pl.col('Units').rank(method='ordinal', descending=True).over(['Team']).alias('Rank')",
        ),
        (9, "converted", "converted", "pl.col('Units').rank(method='ordinal', descending=True).alias('Rank')"),
    ],
    ids=["dense", "fractional", "ordinal", "ordinal_grouped", "ordinal_behind_a_sort"],
)
def test_rank_becomes_a_rank_expression_per_mode(
    rank_modes: ConversionResult, tool_id: int, status: str, reason: str, expression: str
):
    """Ordinal breaks ties by arrival order, so it is only `converted` when a Sort states one."""
    row = report_row(rank_modes, tool_id)
    assert (row.status, row.reason, row.flowfile_node_type) == (status, reason, "polars_code")
    body = dumped_nodes(rank_modes)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    ast.parse(body)
    assert body.endswith(f"output_df = input_df.with_columns(\n    {expression}\n)")


def test_rank_names_its_column_and_says_it_does_not_reorder(rank_modes: ConversionResult):
    """Nothing in the corpus pins the name, so the report is where a reader learns it."""
    row = report_row(rank_modes, 2)
    assert row.messages == [
        "The rank is added as a column called 'Rank' and the rows keep the order they arrived in; "
        "sort on 'Rank' downstream if the rows themselves have to be in rank order.",
        "A row whose 'Units' is null gets a null 'Rank': Flowfile ranks only the rows that have a "
        "value, so a null takes no rank number and is not ranked last.",
    ]
    assert report_row(rank_modes, 4).messages[-1] == (
        "Alteryx's Ordinal rank breaks ties by the order the rows arrive in, which this workflow does not "
        "state; sort the rows upstream if the order matters."
    )


@pytest.mark.parametrize("tool_id", [6, 7])
def test_standard_and_competition_stay_placeholders_until_the_tie_rule_is_confirmed(
    rank_modes: ConversionResult, tool_id: int
):
    """1,2,2,4 and 1,3,3,4 are one polars argument apart and the wrong one is silently wrong data."""
    row = report_row(rank_modes, tool_id)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any("guessing between 1,2,2,4 and 1,3,3,4" in message for message in row.messages), row.messages


def test_the_rank_flow_runs_and_reproduces_the_documented_tie_behaviour(tmp_path: Path, rank_modes: ConversionResult):
    """Five rows with one tie and one null, so every mode's tie rule shows up in the numbers.

    Nothing re-sorts the frames. The ranks are read in the order the rows leave each node, which is
    the only way the Sort in front of tool 9 can be seen at all: it breaks the 20/20 tie the other
    way round from arrival order, so tool 9's ranks are no longer a copy of tool 4's.
    """
    flow = open_flow(write_flow(rank_modes, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    ranked = {}
    for tool_id in (2, 3, 4, 5, 9):
        frame = (
            flow.get_node(report_row(rank_modes, tool_id).flowfile_node_ids[0])
            .get_resulting_data()
            .data_frame.collect()
        )
        assert frame.columns == ["Crate", "Units", "Team", "Rank"]
        ranked[tool_id] = frame.select(["Crate", "Rank"]).rows()
    assert ranked[2] == [("C-1", 3), ("C-2", 2), ("C-3", 2), ("C-4", 1), ("C-5", None)]
    assert ranked[3] == [("C-1", 4.0), ("C-2", 2.5), ("C-3", 2.5), ("C-4", 1.0), ("C-5", None)]
    assert ranked[4] == [("C-1", 4), ("C-2", 2), ("C-3", 3), ("C-4", 1), ("C-5", None)]
    assert ranked[5] == [("C-1", 2), ("C-2", 1), ("C-3", 2), ("C-4", 1), ("C-5", None)]
    # Behind the Sort the tie resolves the other way round, and the null leads a descending sort.
    assert ranked[9] == [("C-5", None), ("C-4", 1), ("C-3", 2), ("C-2", 3), ("C-1", 4)]


RANK_BEHIND_A_PLACEHOLDER = b"""<?xml version="1.0"?>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.XMLParse.XMLParse" />
      <Properties><Configuration /></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Rank.Rank" />
      <Properties><Configuration>%s</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_rank_does_not_overwrite_an_incoming_rank_column(tmp_path: Path, rank_modes: ConversionResult):
    """Tool 11's input already carries a 'Rank', so the mapper names its own column around it."""
    row = report_row(rank_modes, 11)
    assert (row.status, row.reason) == ("converted", "converted")
    flow = open_flow(write_flow(rank_modes, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(row.flowfile_node_ids[0]).get_resulting_data().data_frame.collect().sort("Crate")
    assert frame.columns == ["Crate", "Rank", "Units", "Rank_1"]
    assert frame["Rank"].to_list() == ["gold", "silver", "bronze"]
    assert frame["Rank_1"].to_list() == [3, 1, 2]
    assert row.messages == [
        "The rank is added as a column called 'Rank_1', because the columns reaching this tool already "
        "carry a 'Rank' that Flowfile does not overwrite, and the rows keep the order they arrived in; "
        "sort on 'Rank_1' downstream if the rows themselves have to be in rank order.",
        "A row whose 'Units' is null gets a null 'Rank_1': Flowfile ranks only the rows that have a "
        "value, so a null takes no rank number and is not ranked last.",
    ]


def test_rank_publishes_the_resolved_column_once():
    """`['Crate', 'Rank', 'Units', 'Rank']` is what a downstream Cleanse Pro would freeze into its list."""
    ctx, _ = emit_tools(parse_yxmd(read_fixture("rank_modes.yxmd")))
    assert ctx.tool_columns[2] == ["Crate", "Units", "Team", "Rank"]
    assert ctx.tool_columns[11] == ["Crate", "Rank", "Units", "Rank_1"]


def test_rank_says_it_could_not_check_for_a_rank_column_when_the_schema_is_unknown():
    """Behind a placeholder there is no column list to test against, so the caveat is the honest answer."""
    document = (
        RANK_BEHIND_A_PLACEHOLDER
        % rank_config('<Mode value="Dense" />', '<Field field="value" order="Descending" />').encode()
    )
    row = report_row(convert_yxmd(document, source_name="rank.yxmd"), 3)
    assert (row.status, row.reason) == ("converted", "converted")
    assert row.messages == [
        "The rank is added as a column called 'Rank' and the rows keep the order they arrived in; "
        "sort on 'Rank' downstream if the rows themselves have to be in rank order.",
        "A row whose 'value' is null gets a null 'Rank': Flowfile ranks only the rows that have a "
        "value, so a null takes no rank number and is not ranked last.",
        "The columns reaching this tool are not known at import time, so whether one of them is already "
        "called 'Rank' could not be checked; if one is, this node replaces it.",
    ]


@pytest.mark.parametrize(
    ("case_id", "config", "status_reason", "fragment"),
    [
        (
            "two_modes",
            rank_config('<Mode value="Dense" /><Mode value="Ordinal" />'),
            ("placeholder", "option_unsupported"),
            "names 2 ranking modes",
        ),
        ("no_mode", rank_config(""), ("placeholder", "option_unsupported"), "names 0 ranking modes"),
        (
            "an_unknown_mode",
            rank_config('<Mode value="Percentile" />'),
            ("placeholder", "option_unsupported"),
            "ranking mode 'Percentile' has no Flowfile equivalent",
        ),
        (
            "no_sort_field",
            rank_config('<Mode value="Dense" />', sort_info=""),
            ("placeholder", "mapper_refused"),
            "names no field to rank by",
        ),
        (
            "two_sort_fields",
            rank_config(
                '<Mode value="Dense" />',
                sort_info='<Field field="value" order="Desc" /><Field field="other" order="Asc" />',
            ),
            ("placeholder", "option_unsupported"),
            "ranks by 2 fields at once",
        ),
        (
            "a_field_ending_in_a_backslash",
            rank_config('<Mode value="Dense" />', sort_info='<Field field="value\\\\" order="Desc" />'),
            ("placeholder", "option_unsupported"),
            "ends with a backslash",
        ),
    ],
)
def test_rank_fails_closed(case_id: str, config: str, status_reason: tuple[str, str], fragment: str):
    row = report_row(convert_yxmd(tool_after_text_input(RANK_PLUGIN, config), source_name="rank.yxmd"), 2)
    assert (row.status, row.reason) == status_reason
    assert any(fragment in message for message in row.messages), row.messages


# --- Sample mode table ---

SAMPLE_PLUGIN = "AlteryxBasePluginsGui.Sample.Sample"


def sample_config(mode: str, n: str = "2", groups: str = "") -> str:
    return f"<Mode>{mode}</Mode><N>{n}</N><GroupFields>{groups}</GroupFields>"


@pytest.fixture()
def sample_modes() -> ConversionResult:
    return convert("sample_modes.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "node_type", "tail"),
    [
        (2, "sample", None),
        (3, "polars_code", "output_df = input_df.tail(2)"),
        (4, "polars_code", "output_df = input_df.slice(2)"),
        (5, "polars_code", "output_df = input_df.gather_every(3)"),
        (
            6,
            "polars_code",
            "_position = pl.int_range(pl.len()).over(['Lane'])\noutput_df = input_df.filter(_position < 2)",
        ),
        (8, "polars_code", "output_df = input_df.tail(2)"),
    ],
    ids=["first", "last", "skip", "one_in_n", "first_grouped", "last_behind_a_sort"],
)
def test_the_sample_mode_table(sample_modes: ConversionResult, tool_id: int, node_type: str, tail: str | None):
    """Only the ungrouped "first N" is a node Flowfile already has; the rest are generated."""
    row = report_row(sample_modes, tool_id)
    assert row.flowfile_node_type == node_type
    settings = dumped_nodes(sample_modes)[row.flowfile_node_ids[0]]["setting_input"]
    if tail is None:
        assert (settings["sample_method"], settings["sample_size"]) == ("first", 2)
        return
    body = settings["polars_code_input"]["polars_code"]
    ast.parse(body)
    assert body.endswith(tail)


SAMPLE_GROUP_SORT_SENTENCE = (
    "Alteryx's classic engine also sorts a grouped Sample's output by the grouping column, which this "
    "node does not; the rows keep the order they arrived in, as they do under Alteryx's AMP engine, so "
    "sort on the grouping column downstream if that order matters."
)


@pytest.mark.parametrize("tool_id", [2, 3, 4, 5, 6])
def test_every_sample_mode_is_partial_until_the_order_is_stated(sample_modes: ConversionResult, tool_id: int):
    """Every mode in the table picks rows by position — including the plain "first N", which this
    wave demotes from converted for exactly that reason."""
    row = report_row(sample_modes, tool_id)
    assert (row.status, row.reason) == ("partial", "row_order_unknown")
    order = (
        "Which rows this keeps depends on the order they arrive in, which this workflow does not state; "
        "sort the rows upstream if the order matters."
    )
    grouped = [SAMPLE_GROUP_SORT_SENTENCE] if tool_id == 6 else []
    assert row.messages == [order, *grouped]


def test_only_a_grouped_sample_says_the_classic_engine_sorts_its_output(sample_modes: ConversionResult):
    """`Sample.yxmd`'s own comment box at tool 96 states both halves; an ungrouped Sample has neither."""
    assert SAMPLE_GROUP_SORT_SENTENCE in report_row(sample_modes, 6).messages
    for tool_id in (2, 3, 4, 5, 8):
        assert SAMPLE_GROUP_SORT_SENTENCE not in report_row(sample_modes, tool_id).messages


def test_a_sample_with_no_mode_is_refused_rather_than_defaulted_to_first():
    """A missing `<Mode>` used to mean First, which silently keeps the wrong rows."""
    for config in ("<N>2</N>", "<Mode></Mode><N>2</N>"):
        row = report_row(convert_yxmd(tool_after_text_input(SAMPLE_PLUGIN, config), source_name="s.yxmd"), 2)
        assert (row.status, row.reason) == ("placeholder", "mapper_refused")
        assert any("Sample mode could not be read" in message for message in row.messages), row.messages


def test_a_sample_behind_a_sort_is_converted_without_a_caveat(sample_modes: ConversionResult):
    row = report_row(sample_modes, 8)
    assert (row.status, row.reason, row.messages) == ("converted", "converted", [])


def test_the_sample_flow_runs_and_keeps_the_right_rows(tmp_path: Path, sample_modes: ConversionResult):
    """Seven rows in two lanes, so every mode's slice is distinguishable."""
    flow = open_flow(write_flow(sample_modes, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    kept = {
        tool_id: flow.get_node(report_row(sample_modes, tool_id).flowfile_node_ids[0])
        .get_resulting_data()
        .data_frame.collect()["Crates"]
        .to_list()
        for tool_id in (2, 3, 4, 5, 6, 8)
    }
    assert kept[2] == [1, 2]
    assert kept[3] == [6, 7]
    assert kept[4] == [3, 4, 5, 6, 7]
    assert kept[5] == [1, 4, 7]
    assert kept[6] == [1, 2, 4, 5]
    assert kept[8] == [2, 1]


@pytest.mark.parametrize(
    ("case_id", "config", "reason", "fragment"),
    [
        (
            "random",
            sample_config("Random", n="10"),
            "option_unsupported",
            "a 1-in-N chance per row rather than a sample of a fixed size",
        ),
        (
            "n_percent",
            sample_config("NPercent", n="10"),
            "option_unsupported",
            "the first N% of the rows rather than a random share of them",
        ),
        (
            "an_unknown_mode",
            sample_config("Stratified"),
            "option_unsupported",
            "mode 'Stratified' has no Flowfile equivalent",
        ),
        ("an_unreadable_count", sample_config("First", n="a few"), "mapper_refused", "count could not be read"),
        ("a_zero_count", sample_config("First", n="0"), "mapper_refused", "0 is not a positive number"),
        ("a_negative_count", sample_config("First", n="-5"), "mapper_refused", "count could not be read"),
        ("a_decimal_count", sample_config("First", n="2.7"), "mapper_refused", "count could not be read"),
        ("an_infinite_count", sample_config("First", n="1e400"), "mapper_refused", "count could not be read"),
        (
            "a_group_field_ending_in_a_backslash",
            sample_config("First", groups='<Field name="lane\\\\" />'),
            "option_unsupported",
            "ends with a backslash",
        ),
    ],
)
def test_sample_fails_closed(case_id: str, config: str, reason: str, fragment: str):
    """Random and NPercent are held: neither is the fixed-size random sample the plan assumed."""
    row = report_row(convert_yxmd(tool_after_text_input(SAMPLE_PLUGIN, config), source_name="sample.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", reason)
    assert any(fragment in message for message in row.messages), row.messages


@pytest.mark.parametrize(
    ("case_id", "num_fields"),
    [("negative", "-2"), ("decimal", "2.7"), ("infinite", "1e400"), ("a_word", "several")],
)
def test_the_regex_output_field_count_is_read_by_the_same_reader(case_id: str, num_fields: str):
    """The fourth site of the one integer reader: a count that is not a whole number is refused."""
    result = convert_yxmd(regex_tokenize("([A-Z]{2})", num_fields=num_fields), source_name="regex.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any("output field count could not be read" in message for message in row.messages), row.messages


@pytest.mark.parametrize(
    ("text", "minimum", "expected"),
    [
        ("3", 0, 3),
        ("+3", 0, 3),
        ("  7 ", 0, 7),
        ("0", 0, 0),
        ("0", 1, None),
        ("-5", 0, None),
        ("2.7", 0, None),
        ("1e400", 0, None),
        ("inf", 0, None),
        ("nan", 0, None),
        ("", 0, None),
        ("+-3", 0, None),
        ("\u00b2", 0, None),
    ],
)
def test_the_whole_number_reader_refuses_everything_that_is_not_one(text: str, minimum: int, expected):
    """`int(float(...))` accepted three of these and raised OverflowError on a fourth."""
    assert mappers._whole_number(text, minimum=minimum) == expected


def test_an_infinite_count_does_not_escape_the_conversion():
    """`1e400` used to raise OverflowError past `convert_yxmd`, so the upload 500ed with no event."""
    document = tool_after_text_input(SAMPLE_PLUGIN, sample_config("First", n="1e400"))
    assert report_row(convert_yxmd(document, source_name="sample.yxmd"), 2).status == "placeholder"
    huge = b'<?xml version="1.0"?><AlteryxDocument><Nodes><Node ToolID="1e400" /></Nodes></AlteryxDocument>'
    with pytest.raises(YxmdParseError):
        convert_yxmd(huge, source_name="huge.yxmd")


def test_the_sample_group_fields_are_read_by_tag_not_by_position():
    """`<GroupFields>` sits after `<N>` in some workflows and before it in others."""
    config = '<GroupFields><Field name="value" /></GroupFields><N>2</N><Mode>Last</Mode>'
    result = convert_yxmd(tool_after_text_input(SAMPLE_PLUGIN, config), source_name="sample.yxmd")
    body = dumped_nodes(result)[2]["setting_input"]["polars_code_input"]["polars_code"]
    assert "_position = pl.int_range(pl.len()).over(['value'])" in body
    assert body.endswith("output_df = input_df.filter(_position >= _size - 2)")


# --- Select Records ---


@pytest.fixture()
def select_records() -> ConversionResult:
    return convert("select_records.yxmd")


@pytest.mark.parametrize(
    ("tool_id", "status", "predicate"),
    [
        (2, "partial", "[__select_records_row] <= 5"),
        (3, "partial", "([__select_records_row] >= 3 and [__select_records_row] <= 7)"),
        (4, "partial", "[__select_records_row] = 12"),
        (5, "partial", "[__select_records_row] >= 9"),
        (7, "converted", "([__select_records_row] >= 3 and [__select_records_row] <= 7)"),
    ],
    ids=["to_n", "n_to_m", "one_row", "n_and_up", "behind_a_sort"],
)
def test_select_records_becomes_record_id_filter_and_select(
    select_records: ConversionResult, tool_id: int, status: str, predicate: str
):
    """Alteryx filters on a position, which a Flowfile filter cannot ask about until it is a column."""
    row = report_row(select_records, tool_id)
    assert row.status == status
    assert len(row.flowfile_node_ids) == 3
    nodes = dumped_nodes(select_records)
    record_id, row_filter, drop = (nodes[node_id] for node_id in row.flowfile_node_ids)
    assert [record_id["type"], row_filter["type"], drop["type"]] == ["record_id", "filter", "select"]
    assert record_id["setting_input"]["record_id_input"] == {
        "output_column_name": "__select_records_row",
        "offset": 1,
        "group_by": False,
        "group_by_columns": [],
    }
    assert row_filter["setting_input"]["filter_input"]["advanced_filter"] == predicate
    assert drop["setting_input"]["keep_missing"] is True
    assert [(item["old_name"], item["keep"]) for item in drop["setting_input"]["select_input"]] == [
        ("__select_records_row", False)
    ]
    assert record_id["outputs"] == [row_filter["id"]] and row_filter["outputs"] == [drop["id"]]


def test_a_select_records_range_of_several_tokens_becomes_one_or(select_records: ConversionResult):
    """Whitespace and commas both separate; each token becomes one clause of a single OR."""
    document = macro_after_text_input("SelectRecords.yxmc", '<Value name="Ranges">-5\n3-7, 12\n9+</Value>')
    result = convert_yxmd(document, source_name="select_records.yxmd")
    row = report_row(result, 2)
    assert dumped_nodes(result)[row.flowfile_node_ids[1]]["setting_input"]["filter_input"]["advanced_filter"] == (
        "[__select_records_row] <= 5 or ([__select_records_row] >= 3 and [__select_records_row] <= 7)"
        " or [__select_records_row] = 12 or [__select_records_row] >= 9"
    )


SELECT_RECORDS_OVER_ITS_OWN_HELPER = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="__select_records_row" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings />
      <Properties><Configuration><Value name="Ranges">1</Value></Configuration></Properties>
      <EngineSettings Macro="SelectRecords.yxmc" />
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_the_select_records_helper_column_does_not_shadow_one_of_the_users():
    """`_unique_column` is what stops the helper overwriting a column the workflow already has."""
    result = convert_yxmd(SELECT_RECORDS_OVER_ITS_OWN_HELPER, source_name="select_records.yxmd")
    settings = dumped_nodes(result)[report_row(result, 2).flowfile_node_ids[0]]["setting_input"]
    assert settings["record_id_input"]["output_column_name"] == "__select_records_row_1"
    drop = dumped_nodes(result)[report_row(result, 2).flowfile_node_ids[-1]]["setting_input"]
    assert [item["old_name"] for item in drop["select_input"]] == ["__select_records_row_1"]


def test_the_select_records_flow_runs_and_keeps_exactly_those_rows(tmp_path: Path, select_records: ConversionResult):
    """Fourteen rows, so every range form lands somewhere different — and the helper column is gone."""
    flow = open_flow(write_flow(select_records, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    kept = {}
    for tool_id in (2, 3, 4, 5, 7):
        frame = (
            flow.get_node(report_row(select_records, tool_id).flowfile_node_ids[-1])
            .get_resulting_data()
            .data_frame.collect()
        )
        assert frame.columns == ["Crate", "Crates"]
        kept[tool_id] = frame["Crates"].to_list()
    assert kept[2] == [1, 2, 3, 4, 5]
    assert kept[3] == [3, 4, 5, 6, 7]
    assert kept[4] == [12]
    assert kept[5] == [9, 10, 11, 12, 13, 14]
    assert kept[7] == [12, 11, 10, 9, 8]


SELECT_RECORDS_BEHIND_A_PLACEHOLDER = b"""<?xml version="1.0"?>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.XMLParse.XMLParse" />
      <Properties><Configuration /></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="" />
      <EngineSettings Macro="SelectRecords.yxmc" />
      <Properties><Configuration><Value name="Ranges">1-3</Value></Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_select_records_names_its_helper_column_when_the_schema_is_unknown():
    """`_unique_column` cannot avoid a collision it cannot see, so the report names what to rename."""
    row = report_row(convert_yxmd(SELECT_RECORDS_BEHIND_A_PLACEHOLDER, source_name="sr.yxmd"), 3)
    assert row.status == "partial"
    assert row.messages[1] == (
        "The columns reaching this tool are not known at import time, so that name was not checked "
        "against them; rename a column of your own called '__select_records_row' before this node."
    )


def test_select_records_says_nothing_extra_when_the_schema_is_known(select_records: ConversionResult):
    """A known schema is checked, so there is nothing to warn about and the message stays one sentence."""
    assert all("was not checked against them" not in message for message in report_row(select_records, 2).messages)


@pytest.mark.parametrize(
    ("case_id", "ranges", "fragment"),
    [
        ("empty", "", "it names no rows at all"),
        ("backwards", "7-3", "'7-3' ends before it starts"),
        ("zero", "0-5", "numbers rows from below 1"),
        ("negative_looking", "1--5", "is not one of the forms N, N-M, N+ or -N"),
        ("a_word", "first ten", "'first' is not one of the forms"),
        ("a_plus_in_front", "+9", "'+9' is not one of the forms"),
        ("a_decimal", "1.5", "'1.5' is not one of the forms"),
    ],
)
def test_select_records_fails_closed(case_id: str, ranges: str, fragment: str):
    """A token this mapper cannot read is rows it would silently keep or drop."""
    document = macro_after_text_input("SelectRecords.yxmc", f'<Value name="Ranges">{ranges}</Value>')
    row = report_row(convert_yxmd(document, source_name="select_records.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert any(fragment in message for message in row.messages), row.messages


# --- Summarize exotic actions ---

SUMMARIZE_PLUGIN = "AlteryxSpatialPluginsGui.Summarize.Summarize"


def summarize_config(*fields: tuple[str, str]) -> str:
    return (
        "<SummarizeFields>"
        + "".join(
            f'<SummarizeField field="{column}" action="{action}" rename="{action}_{column}" />'
            for column, action in fields
        )
        + "</SummarizeFields>"
    )


SUMMARIZE_BEHIND_A_PLACEHOLDER = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="value" /></Fields>
        <Data><r><c>a</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.XMLParse.XMLParse" />
      <Properties><Configuration /></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxSpatialPluginsGui.Summarize.Summarize" />
      <Properties><Configuration>%s</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


@pytest.fixture()
def summarize_exotic() -> ConversionResult:
    return convert("summarize_exotic.yxmd")


def test_variance_reaches_the_native_group_by_node(summarize_exotic: ConversionResult):
    """`AggColl.agg` is `getattr(pl, ...)`, so the table entry has to be polars' name for it."""
    row = report_row(summarize_exotic, 2)
    assert (row.status, row.flowfile_node_type) == ("converted", "group_by")
    aggs = dumped_nodes(summarize_exotic)[row.flowfile_node_ids[0]]["setting_input"]["groupby_input"]["agg_cols"]
    assert [(item["old_name"], item["agg"], item["new_name"]) for item in aggs] == [
        ("Region", "groupby", "Region"),
        ("Spend", "var", "Variance_Spend"),
        ("Spend", "std", "StdDev_Spend"),
    ]


@pytest.mark.parametrize(
    ("tool_id", "status", "reason", "body"),
    [
        (
            3,
            "partial",
            "option_unsupported",
            "output_df = input_df.group_by([]).agg(\n"
            "    pl.col('Spend').mode().sort().first().alias('Mode_Spend'),\n"
            "    pl.col('Name').sort_by(pl.col('Name').str.len_chars(), descending=True, nulls_last=True)"
            ".first().alias('Longest_Name'),\n"
            "    (pl.col('Name').is_not_null() & (pl.col('Name') != \"\")).sum().alias('CountNonBlank_Name'),\n"
            "    (pl.col('Name').is_null() | (pl.col('Name') == \"\")).sum().alias('CountBlank_Name'),\n"
            ")",
        ),
        (
            4,
            "converted",
            "converted",
            "output_df = input_df.group_by([pl.col('Region').alias('Region')]).agg(\n"
            "    pl.col('Name').sort_by(pl.col('Name').str.len_chars(), descending=True, nulls_last=True)"
            ".first().alias('Longest_Name'),\n"
            "    (pl.col('Name').is_null() | (pl.col('Name') == \"\")).sum().alias('CountBlank_Name'),\n"
            ")",
        ),
    ],
    ids=["ungrouped_with_a_mode", "grouped_without_one"],
)
def test_an_exotic_action_turns_the_whole_tool_into_one_generated_group_by(
    summarize_exotic: ConversionResult, tool_id: int, status: str, reason: str, body: str
):
    """Splitting the tool would change what is grouped, so every aggregation moves together."""
    row = report_row(summarize_exotic, tool_id)
    assert (row.status, row.reason, row.flowfile_node_type) == (status, reason, "polars_code")
    code = dumped_nodes(summarize_exotic)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    ast.parse(code)
    assert code.endswith(body)


def test_a_mode_is_partial_because_its_tie_rule_is_unverified(summarize_exotic: ConversionResult):
    """`.sort().first()` is not Alteryx's rule, but it is at least the same answer every run."""
    assert report_row(summarize_exotic, 3).messages == [
        "Alteryx's Mode picks one value when several are equally common and its rule for that is not "
        "verified; the generated code takes the lowest of them, so the result is at least the same on "
        "every run."
    ]
    assert report_row(summarize_exotic, 4).messages == []


def test_first_and_last_read_an_order_the_workflow_does_not_state(summarize_exotic: ConversionResult):
    """The order rule applies to Rank Ordinal and to every Sample mode; these are the same
    question asked of an aggregation, and `pl.first`/`pl.last` answer it from arrival order."""
    row = report_row(summarize_exotic, 8)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "row_order_unknown", "group_by")
    assert row.messages == [
        "Alteryx's First and Last pick a value by the order the rows arrive in, which this workflow "
        "does not state; sort the rows upstream if the order matters."
    ]


def test_a_first_behind_a_sort_converts(summarize_exotic: ConversionResult):
    """A Sort states the order, which is exactly what the message asks the workflow to do."""
    row = report_row(summarize_exotic, 10)
    assert (row.status, row.reason, row.messages) == ("converted", "converted", [])


def test_a_first_in_a_generated_group_by_is_order_dependent_too():
    """Both exits of `map_summarize` answer the same question; only one of them used to."""
    config = summarize_config(("value", "First"), ("value", "Longest"))
    document = tool_after_text_input(SUMMARIZE_PLUGIN, config)
    row = report_row(convert_yxmd(document, source_name="s.yxmd"), 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "row_order_unknown", "polars_code")
    assert any(m.startswith("Alteryx's First and Last pick a value by the order") for m in row.messages)
    assert any(m.startswith("These Alteryx Summarize aggregations read the characters") for m in row.messages)


def test_the_spatial_summarize_actions_stay_a_placeholder(summarize_exotic: ConversionResult):
    """Scope is keyed by tool name, so moving one action out of the denominator would be new policy."""
    row = report_row(summarize_exotic, 5)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert any("SpatialObjCombine on Shape" in message for message in row.messages), row.messages


@pytest.mark.parametrize(
    ("action", "label"),
    [
        ("Longest", "Longest"),
        ("Shortest", "Shortest"),
        ("CountNonBlank", "Count Non Blank"),
        ("CountBlank", "Count Blank"),
    ],
)
def test_a_string_aggregation_on_a_column_flowfile_typed_numeric_is_partial(action: str, label: str):
    """The helper's one cell is `1`, so `value` is Int64 and `.str.len_chars()` raises at collect."""
    document = tool_after_text_input(SUMMARIZE_PLUGIN, summarize_config(("value", action)))
    row = report_row(convert_yxmd(document, source_name="s.yxmd"), 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "polars_code")
    assert any(f"{label} on 'value' (Int64)" in message for message in row.messages), row.messages


def test_a_string_aggregation_on_an_unknown_column_type_is_partial():
    """Nothing settles the type behind a placeholder, and an unprovable String is not a String."""
    document = SUMMARIZE_BEHIND_A_PLACEHOLDER % summarize_config(("value", "Longest")).encode()
    row = report_row(convert_yxmd(document, source_name="s.yxmd"), 3)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("Longest on 'value' (unknown here)" in message for message in row.messages), row.messages


def two_text_inputs_into_one_summarize(wires: str) -> bytes:
    """Two Text Inputs on one Summarize `Input` anchor: tool 1 types `Code` String, tool 2 Int64."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Code" type="V_String" /></Fields>
        <Data><r><c>abc</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Code" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="{SUMMARIZE_PLUGIN}" />
      <Properties><Configuration>{summarize_config(("Code", "Longest"))}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>{wires}</Connections>
</AlteryxDocument>
""".encode()


STRING_SOURCE_FIRST = (
    '<Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>'
    '<Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>'
)
NUMERIC_SOURCE_FIRST = (
    '<Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>'
    '<Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>'
)


@pytest.mark.parametrize(
    ("wires", "origins"),
    [
        (STRING_SOURCE_FIRST, "ToolID 1 ('Output'), ToolID 2 ('Output')"),
        (NUMERIC_SOURCE_FIRST, "ToolID 2 ('Output'), ToolID 1 ('Output')"),
    ],
    ids=["string_source_first", "numeric_source_first"],
)
def test_two_wires_on_one_summarize_anchor_settle_no_type(wires: str, origins: str):
    """`source_connection` answered from the first wire, so document order decided the type.

    Alteryx unions the streams arriving on one anchor: `Code` is a String on one of these Text
    Inputs and an Int64 on the other, so the union settles nothing and neither wire may answer for
    it. The old rule read the String source as the whole truth whenever the file happened to write
    that wire first, and said `converted` with no message at all. Both wires are named, in the order
    the file writes them, so the reader can see which two streams the answer is missing.

    This sentence is now generalised to every reader, so the Summarize no longer writes its own.
    """
    row = report_row(convert_yxmd(two_text_inputs_into_one_summarize(wires), source_name="s.yxmd"), 3)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("Longest on 'Code' (unknown here)" in message for message in row.messages), row.messages
    assert row.messages[-1] == MULTI_STREAM_READ_SENTENCE.format(
        tool="Summarize", tool_id=3, anchor="Input", n=2, sources=origins
    )


def text_input_with_two_columns_named_code() -> bytes:
    """One Text Input declaring `Code` twice — Int64 first, String second — into a Summarize."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="Code" /><Field name="Code" type="V_String" /></Fields>
        <Data><r><c>1</c><c>abc</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="{SUMMARIZE_PLUGIN}" />
      <Properties><Configuration>{summarize_config(("Code", "Longest"))}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def test_a_column_name_a_text_input_uses_twice_settles_no_type():
    """`dict(zip(names, types))` let the second `Code` own the name, so the String one answered.

    Which of the two a Summarize means is not a question the XML answers, and the flow cannot be
    built either way — `manual_input` raises `DuplicateError` on the repeated name — so the guard
    says unknown rather than picking the column that happens to come last.
    """
    row = report_row(convert_yxmd(text_input_with_two_columns_named_code(), source_name="s.yxmd"), 2)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("Longest on 'Code' (unknown here)" in message for message in row.messages), row.messages


def test_the_duplicate_named_text_input_really_cannot_be_built(tmp_path: Path):
    """The reason the guard has no answer to give, executed rather than asserted.

    The flow does not reach a run: `manual_input` builds its frame while the graph is being opened,
    so the repeated name stops it there. Whichever `Code` the Summarize meant is moot.
    """
    result = convert_yxmd(text_input_with_two_columns_named_code(), source_name="s.yxmd")
    path = write_flow(result, tmp_path / "flow.yaml")
    with pytest.raises(pl.exceptions.DuplicateError, match="'Code' has more than one occurrence"):
        open_flow(path)


XML_PARSE_PLUGIN = "AlteryxBasePluginsGui.XMLParse.XMLParse"
SORT_PLUGIN = "AlteryxBasePluginsGui.Sort.Sort"
FILE_INPUT_PLUGIN = "AlteryxBasePluginsGui.DbFileInput.DbFileInput"
SORT_ON_CODE = '<SortInfo locale="0"><Field field="Code" order="Ascending" /></SortInfo>'
# Alteryx's own cached output schema, which every tool carries and which says what *Alteryx* ran.
CACHED_STRING_CODE = (
    '<MetaInfo connection="Output"><RecordInfo><Field name="Code" size="10" type="V_String" /></RecordInfo></MetaInfo>'
)


def summarize_behind_one_tool(fields: str, plugin: str, config: str, cache: str = "") -> bytes:
    """A Text Input -> one tool that may carry Alteryx's cached schema -> a Summarize on Longest."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        {fields}
        <Data><r><c>01</c></r><r><c>2</c></r><r><c>003</c></r></Data>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="{plugin}" />
      <Properties>{cache}<Configuration>{config}</Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="{SUMMARIZE_PLUGIN}" />
      <Properties><Configuration>{summarize_config(("Code", "Longest"))}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" /><Destination ToolID="3" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


def summarize_behind_file_input(path: str) -> bytes:
    """An Input Data tool whose cached schema calls `Code` a string, feeding a Summarize on Longest."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="{FILE_INPUT_PLUGIN}" />
      <Properties>{CACHED_STRING_CODE}<Configuration>
        <File OutputFileName="" FileFormat="19" SearchSubDirs="False" RecordLimit="">{path}</File>
      </Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="{SUMMARIZE_PLUGIN}" />
      <Properties><Configuration>{summarize_config(("Code", "Longest"))}</Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" /><Destination ToolID="2" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


@pytest.mark.parametrize(
    "extension",
    ["parquet", "ndjson", "ipc", "feather", "arrow", "avro"],
    ids=lambda extension: f"reads_a_{extension}",
)
def test_a_self_describing_file_format_has_no_header_row_to_miss(extension: str):
    """`has_headers` lives only on the text formats' settings, and reading it 500ed the upload.

    `_READ_FILE_TYPES` has always advertised these six, but the headerless branch asked every
    settings object for a `has_headers` that `InputParquetTable`, `InputIpcTable`, `InputNdjsonTable`
    and `InputAvroTable` do not have, so an Input Data tool pointed at one raised `AttributeError`
    out of `convert_yxmd`. No corpus workflow reads one, which is why it stayed hidden.
    """
    row = report_row(convert_yxmd(summarize_behind_file_input(f"data\\customers.{extension}"), source_name="s.yxmd"), 1)
    assert (row.status, row.flowfile_node_type) == ("converted", "read")
    assert row.messages == []


@pytest.mark.parametrize(
    ("case_id", "fields", "plugin", "config", "cache", "fragment", "collects"),
    [
        # The cached-schema defect, one tool upstream of the test that pinned it: the cache rides on a tool
        # Flowfile did not convert, so the frame reaching the Summarize is the Text Input's Int64.
        # A placeholder could have changed the column, so the walk stops there and says unknown.
        (
            "a_placeholder_carrying_the_cache",
            '<Fields><Field name="Code" /></Fields>',
            XML_PARSE_PLUGIN,
            "",
            CACHED_STRING_CODE,
            "Longest on 'Code' (unknown here)",
            None,
        ),
        # A Sort changes no column, so the type lookup walks through it to the Text Input — which reads
        # `01 / 2 / 003` back as Int64. The cache still says V_String and is still not believed;
        # the guard now names the type it really found instead of calling it unknown.
        (
            "a_converted_pass_through_carrying_the_cache",
            '<Fields><Field name="Code" /></Fields>',
            SORT_PLUGIN,
            SORT_ON_CODE,
            CACHED_STRING_CODE,
            "Longest on 'Code' (Int64)",
            None,
        ),
        # What the one-hop rule used to cost, now recovered: `Code` really is a String on both
        # sides, and walking past the Sort reaches the Text Input that declares it.
        (
            "a_string_declared_text_input_behind_a_sort",
            '<Fields><Field name="Code" type="V_String" /></Fields>',
            SORT_PLUGIN,
            SORT_ON_CODE,
            "",
            None,
            [("003",)],
        ),
    ],
)
def test_a_cached_alteryx_record_info_is_not_a_flowfile_type(
    tmp_path: Path,
    case_id: str,
    fields: str,
    plugin: str,
    config: str,
    cache: str,
    fragment: str | None,
    collects: list | None,
):
    """The old reading asked the tool feeding the Summarize what type it emits and believed the answer.

    Every Alteryx tool carries a `<RecordInfo>` cache of the schema *Alteryx* last ran, so the
    answer survived a tool Flowfile refused to convert and contradicted Flowfile's own reading of
    the Text Input feeding it. The cache is still never an answer: both cached cases carry a
    V_String that says nothing about the Int64 Flowfile will really produce, and both still refuse.

    What changed since is how far the question travels. A tool that provably changes no column
    is walked through, so the third case — a String the workflow really does declare, one Sort away
    — is green instead of being the stated price of stopping at the first hop.
    """
    document = summarize_behind_one_tool(fields, plugin, config, cache)
    result = convert_yxmd(document, source_name="s.yxmd")
    row = report_row(result, 3)
    if fragment is None:
        assert (row.status, row.reason, row.flowfile_node_type) == ("converted", "converted", "polars_code")
        assert row.messages == []
    else:
        assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "polars_code")
        assert any(fragment in message for message in row.messages), row.messages

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    flow.run_graph()
    frame = flow.get_node(row.flowfile_node_ids[0]).get_resulting_data().data_frame
    if collects is None:
        with pytest.raises(pl.exceptions.SchemaError, match="expected .*String.*, got .*i64"):
            frame.collect()
    else:
        assert frame.collect().rows() == collects


def summarize_behind_file_input_typed(path: str, alteryx_type: str) -> bytes:
    """`summarize_behind_file_input` with the cached type under the reader's control."""
    return summarize_behind_file_input(path).replace(
        b'<Field name="Code" size="10" type="V_String" />',
        f'<Field name="Code" size="10" type="{alteryx_type}" />'.encode(),
    )


@pytest.mark.parametrize(
    ("alteryx_type", "answers"),
    [("V_String", True), ("String", True), ("WString", True), ("Int32", False), ("Double", False), ("Blob", False)],
    ids=lambda value: f"cached_{value}",
)
def test_a_typed_read_answers_only_for_its_string_columns(alteryx_type: str, answers: bool):
    """All four Alteryx string types reach Polars as `String`; the numeric names do not survive.

    `yxdb.py` widens `Byte` and `Int16` to `Int32` and turns `FixedDecimal` into a `Decimal`, so a
    cached numeric name is not what the Parquet sibling holds. Only the String half is an answer,
    and a column outside it reads "unknown here" rather than a type nobody checked.
    """
    document = summarize_behind_file_input_typed("data\\customers.parquet", alteryx_type)
    row = report_row(convert_yxmd(document, source_name="s.yxmd"), 2)
    if answers:
        assert (row.status, row.reason, row.messages) == ("converted", "converted", [])
    else:
        assert (row.status, row.reason) == ("partial", "option_unsupported")
        assert any("Longest on 'Code' (unknown here)" in message for message in row.messages), row.messages


def test_the_one_all_clear_the_guard_gives_is_executed(tmp_path: Path):
    """The rule's only "yes" answer, run rather than asserted: a Parquet String really is a String.

    The three cached cases prove the demotions prevent a `SchemaError`. This is the other direction —
    the flow the guard leaves `converted` collects, so the all-clear is not simply a quieter warning.
    """
    source = tmp_path / "customers.parquet"
    pl.DataFrame({"Code": ["01", "2", "003"]}).write_parquet(source)
    result = convert_yxmd(summarize_behind_file_input(str(source)), source_name="s.yxmd")
    row = report_row(result, 2)
    assert (row.status, row.reason, row.messages) == ("converted", "converted", [])

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    flow.run_graph()
    frame = flow.get_node(row.flowfile_node_ids[0]).get_resulting_data().data_frame
    assert frame.collect().rows() == [("003",)]


@pytest.mark.parametrize(
    ("case_id", "path", "status", "reason", "messages"),
    [
        # The node reads the Parquet *sibling*, which is a different file from the one Alteryx
        # cached and one this process cannot see; a `.yxdb` therefore settles nothing.
        ("a_yxdb_read_through_its_parquet_sibling", "data\\customers.yxdb", "partial", "option_unsupported", None),
        ("a_parquet_read", "data\\customers.parquet", "converted", "converted", []),
        ("a_csv_the_reader_types_by_inference", "data\\customers.csv", "partial", "option_unsupported", None),
        ("an_excel_sheet_the_reader_types_by_inference", "data\\customers.xlsx", "partial", "option_unsupported", None),
        # A value the emitter has already labelled `connection_string` cannot also be a file whose
        # types are settled, however its tail is spelled.
        (
            "an_odbc_connection_string_ending_in_parquet",
            "odbc:Driver={Foo};DBQ=\\\\srv\\share\\db.parquet",
            "partial",
            "option_unsupported",
            None,
        ),
        ("a_url_ending_in_parquet", "https://example.com/data.parquet", "partial", "option_unsupported", None),
    ],
)
def test_an_input_data_tool_answers_for_a_file_that_states_its_own_types(
    case_id: str, path: str, status: str, reason: str, messages: list[str] | None
):
    """The one cached schema Flowfile may read, because the reader reproduces it rather than guessing.

    A Parquet carries every column's type and is the same file Alteryx read to fill the cache, so an
    Alteryx string column arrives as a Polars String. A CSV or a worksheet is typed by inference at
    read time, so the same cached `V_String` proves nothing about the frame Flowfile will build —
    and a `.yxdb` is a third case: the node reads a Parquet sibling Alteryx never saw.
    """
    row = report_row(convert_yxmd(summarize_behind_file_input(path), source_name="s.yxmd"), 2)
    assert (row.status, row.reason) == (status, reason)
    if messages is None:
        assert any("Longest on 'Code' (unknown here)" in message for message in row.messages), row.messages
    else:
        assert row.messages == messages


def test_a_yxdb_read_and_the_tool_behind_it_make_the_same_claim():
    """The read row says it cannot prove the Parquet sibling exists; the guard used to disagree.

    `_map_yxdb_input` returns `partial`/`file_format` because nothing in the importer can see the
    file `flowfile convert yxdb` is asked to write — a different file from the `.yxdb` Alteryx
    cached, at a path this process is never even told the directory of. Reading that cache as
    Flowfile's own type certified the Summarize behind that same node `converted` with no message:
    two rows of one report making opposite claims about one file.
    """
    result = convert_yxmd(summarize_behind_file_input("data\\customers.yxdb"), source_name="s.yxmd")
    read_row, summarize_row = report_row(result, 1), report_row(result, 2)
    assert (read_row.status, read_row.reason) == ("partial", "file_format")
    assert (summarize_row.status, summarize_row.reason) == ("partial", "option_unsupported")
    assert any(
        "Longest on 'Code' (unknown here)" in message for message in summarize_row.messages
    ), summarize_row.messages


def test_a_numeric_looking_string_column_is_typed_int64_and_the_body_raises_at_collect(
    tmp_path: Path, summarize_exotic: ConversionResult
):
    """`01 / 2 / 003` is entered as text and read back as Int64, so the message is literally true."""
    row = report_row(summarize_exotic, 7)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert row.messages == [
        "These Alteryx Summarize aggregations read the characters of a column Flowfile does not know "
        "to be a String: Longest on 'Code' (Int64), Shortest on 'Code' (Int64). The column has to be "
        "a String by the time this node runs, or the flow fails when it is read — give it that type "
        "upstream (a Select that changes it) if Alteryx stored it as text."
    ]
    flow = open_flow(write_flow(summarize_exotic, tmp_path / "flow.yaml"))
    flow.run_graph()
    with pytest.raises(pl.exceptions.SchemaError, match="expected .*String.*, got .*i64"):
        flow.get_node(row.flowfile_node_ids[0]).get_resulting_data().data_frame.collect()


def test_the_exotic_summarize_flow_runs_on_blanks_nulls_and_a_tie(tmp_path: Path, summarize_exotic: ConversionResult):
    """Six rows: one null name, one all-spaces name that is *not* blank, and a tie on the modal spend."""
    flow = open_flow(write_flow(summarize_exotic, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frames = {
        tool_id: flow.get_node(report_row(summarize_exotic, tool_id).flowfile_node_ids[0])
        .get_resulting_data()
        .data_frame.collect()
        for tool_id in (2, 3, 4)
    }
    assert frames[2].sort("Region").rows() == [("N", 0.0, 0.0), ("S", 6.0, 2.449489742783178)]
    assert frames[3].rows() == [(1, "cecilia", 5, 1)]
    assert frames[4].sort("Region").rows() == [("N", "anna", 0), ("S", "cecilia", 1)]


def test_the_empty_string_half_of_blank_is_executed_not_just_printed(
    tmp_path: Path, summarize_exotic: ConversionResult
):
    """Blank is null *or* the empty string, and until now only the null half ever ran.

    No Text Input cell can carry an empty string: Designer writes exactly one empty form, `<c />`,
    and `<c></c>` and an empty CDATA parse to the same `text is None` — none of the three carries
    character data for ElementTree to hand back, so the fixture's `<c />` row is a null and nothing
    else.
    An empty string therefore has to be *computed*, and tool 11 computes one the way an analyst
    would: `Trim([Name])` over the three-space row. Both halves of both counts then fire, on one
    value each — `== ""` for the trimmed row, `is_null()` for the `<c />` row.

    Tool 12 is `partial` because a Formula states no output type Flowfile can read, which is the
    declared-type rule working, not a defect: the flow runs and the counts are right.
    """
    row = report_row(summarize_exotic, 12)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "polars_code")
    code = dumped_nodes(summarize_exotic)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"]["polars_code"]
    assert "(pl.col('Name').is_not_null() & (pl.col('Name') != \"\")).sum()" in code
    assert "(pl.col('Name').is_null() | (pl.col('Name') == \"\")).sum()" in code

    flow = open_flow(write_flow(summarize_exotic, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    counts = flow.get_node(row.flowfile_node_ids[0]).get_resulting_data().data_frame.collect()
    assert counts.rows() == [(4, 2)]


def test_a_generated_blank_count_does_not_trim_because_is_empty_does_not(summarize_exotic: ConversionResult):
    """One workflow must not answer "is this blank?" twice; the other side is pinned by the Filter tests."""
    for tool_id in (3, 4):
        row = report_row(summarize_exotic, tool_id)
        code = dumped_nodes(summarize_exotic)[row.flowfile_node_ids[0]]["setting_input"]["polars_code_input"][
            "polars_code"
        ]
        assert "strip_chars" not in code


@pytest.mark.parametrize(
    ("case_id", "config", "reason", "fragment"),
    [
        (
            "an_action_flowfile_does_not_know",
            summarize_config(("value", "Percentile")),
            "option_unsupported",
            "Unsupported Alteryx Summarize actions: Percentile on value",
        ),
        ("nothing_configured", "<SummarizeFields />", "mapper_refused", "has no aggregations configured"),
    ],
)
def test_summarize_fails_closed(case_id: str, config: str, reason: str, fragment: str):
    row = report_row(convert_yxmd(tool_after_text_input(SUMMARIZE_PLUGIN, config), source_name="s.yxmd"), 2)
    assert (row.status, row.reason) == ("placeholder", reason)
    assert any(fragment in message for message in row.messages), row.messages


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


# --- Pearson Correlation ---


@pytest.fixture()
def pearson() -> ConversionResult:
    return convert_yxmd(read_fixture("pearson_correlation.yxmd"), source_name="pearson_correlation.yxmd")


@pytest.fixture()
def pearson_flow(tmp_path: Path, pearson: ConversionResult):
    flow = open_flow(write_flow(pearson, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    return flow


def _grid(flow, result: ConversionResult, tool_id: int):
    return flow.get_node(report_row(result, tool_id).flowfile_node_ids[0]).get_resulting_data().data_frame.collect()


def test_a_correlation_is_a_square_grid_with_ones_down_its_diagonal(pearson_flow, pearson: ConversionResult):
    """Box 41 of `Pearson_Correlation.yxmd` describes exactly this: n rows, n columns, diagonal 1."""
    frame = _grid(pearson_flow, pearson, 210)
    assert frame.columns == ["Variable", "depth_m", "crates", "kilos"]
    assert frame["Variable"].to_list() == ["depth_m", "crates", "kilos"]
    assert frame.height == 3
    for index, name in enumerate(["depth_m", "crates", "kilos"]):
        assert frame[name][index] == pytest.approx(1.0)
    # crates is exactly 2 x depth_m here, so the off-diagonal is a perfect correlation too.
    assert frame["crates"][0] == pytest.approx(1.0)


def test_a_correlation_row_says_the_two_things_flowfile_chose(pearson: ConversionResult):
    """The grid's shape is Alteryx's; the name column and the row order are the import's.

    The null rule rides beside them — not a layout choice, but the third thing about this
    grid that the workflow does not state and a Designer run would settle.
    """
    row = report_row(pearson, 210)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert row.messages[0] == (
        "Two things about this grid are Flowfile's choice, not Alteryx's, and are worth checking against "
        "a Designer run: the name of the leading variable column ('Variable' here — Alteryx's own name for "
        "it is not recorded in the workflow), and the order of the rows, which follows the field list."
    )
    assert len(row.messages) == 2
    assert row.messages[1].startswith("A null changes the answer")


def test_the_covariance_option_replaces_the_correlation_rather_than_joining_it(pearson_flow, pearson: ConversionResult):
    """Boxes 22 and 42 put covariance and correlation as alternatives, so the diagonal is the variance."""
    frame = _grid(pearson_flow, pearson, 220)
    assert frame.columns == ["Variable", "depth_m", "crates", "kilos"]
    # depth_m is 1..5: a sample variance of 2.5, which is what the diagonal must hold here.
    assert frame["depth_m"][0] == pytest.approx(2.5)
    assert frame["crates"][0] == pytest.approx(5.0)
    row = report_row(pearson, 220)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("diagonal is each variable's variance, not 1" in message for message in row.messages)


def test_unknown_selected_freezes_to_the_numeric_columns_reaching_the_tool(pearson_flow, pearson: ConversionResult):
    """Alteryx's field list offers only numeric columns, so `*Unknown` cannot pull a string one in.

    Every one of the 15 corpus instances shows this directly: all four numeric columns of their input
    are listed, selected or not, and neither of its two string columns appears at all.
    """
    frame = _grid(pearson_flow, pearson, 230)
    assert frame.columns == ["Variable", "depth_m", "crates", "kilos"]
    assert "orchard" not in frame.columns


def test_unknown_does_not_pull_back_a_listed_field_the_user_unticked(pearson_flow, pearson: ConversionResult):
    """`*Unknown` stands for the columns the field list does not name, so a deselected one stays out.

    Tool 270 lists all four columns, unticks 'crates' and 'orchard', and ticks `*Unknown`. The
    exclusion used to be read off the *ticked* set, so 'crates' was correlated anyway — a numeric column
    the workflow says to leave out, silently back in the grid.
    """
    node_id = report_row(pearson, 270).flowfile_node_ids[0]
    code = dumped_nodes(pearson)[node_id]["setting_input"]["polars_code_input"]["polars_code"]
    assert "_fields = ['depth_m', 'kilos']" in code
    frame = _grid(pearson_flow, pearson, 270)
    assert frame.columns == ["Variable", "depth_m", "kilos"]
    assert frame["Variable"].to_list() == ["depth_m", "kilos"]


def test_a_string_column_selected_by_name_is_refused_rather_than_correlated(pearson: ConversionResult):
    """A named field is the user's own instruction, so a type Flowfile can state is a refusal."""
    row = report_row(pearson, 240)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Pearson Correlation was not converted because the selected field 'orchard' is a "
        "String column, which has no correlation to compute."
    ]


@pytest.mark.parametrize(
    ("tool_id", "because"),
    [
        (250, "1 field(s) are selected and a correlation compares at least two"),
        (260, "a field is selected more than once, and a grid cannot have two rows with one name"),
    ],
    ids=["one_field", "one_field_twice"],
)
def test_a_grid_that_cannot_be_built_is_refused(pearson: ConversionResult, tool_id: int, because: str):
    row = report_row(pearson, tool_id)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [f"The Alteryx Pearson Correlation was not converted because {because}."]


# --- Spearman Rank Correlation ---

CORPUS_SPEARMAN = LEARNING / "alteryx_nodes" / "11 Data Investigation" / "Spearman_Correlation.yxmd"
needs_spearman_corpus = pytest.mark.skipif(
    not CORPUS_SPEARMAN.exists(), reason=f"Alteryx corpus workflow not present ({CORPUS_SPEARMAN})"
)


@pytest.fixture()
def spearman() -> ConversionResult:
    return convert_yxmd(read_fixture("spearman_correlation.yxmd"), source_name="spearman_correlation.yxmd")


@pytest.fixture()
def spearman_flow(tmp_path: Path, spearman: ConversionResult):
    flow = open_flow(write_flow(spearman, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    return flow


def test_a_monotone_pair_ranks_as_a_perfect_correlation(spearman_flow, spearman: ConversionResult):
    """depth_m doubles kilos each row: monotone but not linear, which is the tool's whole point."""
    frame = _grid(spearman_flow, spearman, 310)
    assert frame.columns == ["Spearman"]
    assert frame.height == 1
    assert frame["Spearman"][0] == pytest.approx(1.0)


def test_tied_values_rank_by_their_average(spearman_flow, spearman: ConversionResult):
    """Polars' rule is the textbook one; Alteryx's is not in the workflow, which the row says."""
    frame = _grid(spearman_flow, spearman, 320)
    # ties_a ranks 1, 2.5, 2.5, 4, 5, 6 against ties_b's 1.5, 1.5, 3, 4.5, 4.5, 6 — and what the node
    # produces is exactly the Pearson correlation of those, which is the definition being claimed.
    ranks = pl.DataFrame({"a": [1.0, 2.5, 2.5, 4.0, 5.0, 6.0], "b": [1.5, 1.5, 3.0, 4.5, 4.5, 6.0]})
    assert frame["Spearman"][0] == pytest.approx(ranks.select(pl.corr("a", "b")).item())
    assert any("average rank" in message for message in report_row(spearman, 320).messages)


def test_group_by_gives_one_coefficient_per_group(spearman_flow, spearman: ConversionResult):
    frame = _grid(spearman_flow, spearman, 330)
    assert frame.columns == ["orchard", "Spearman"]
    assert dict(zip(frame["orchard"].to_list(), frame["Spearman"].to_list(), strict=True)) == {
        "Okonjo": pytest.approx(1.0),
        "Salgado": pytest.approx(1.0),
    }
    assert "one per value of 'orchard'" in report_row(spearman, 330).messages[0]


def test_the_grouping_field_is_not_read_while_its_switch_is_off(spearman_flow, spearman: ConversionResult):
    """12 of the 13 corpus instances leave a stale name in that box, so reading it would invent a group.

    Tool 340 names a column that does not exist at all; the flow runs because the switch above it is
    off, which is what proves the field was never looked at.
    """
    assert _grid(spearman_flow, spearman, 340).columns == ["Spearman"]


@pytest.mark.parametrize(
    ("tool_id", "because"),
    [
        (350, "both variables are the column 'depth_m', which correlates with itself by definition"),
        (360, "the macro's field selection names no column for Variable2"),
        (
            370,
            "the grouping column 'depth_m' is also one of the two variables, so every group would hold a "
            "single value of it",
        ),
        (380, "the variable 'orchard' is a String column, which has no rank correlation to compute"),
    ],
    ids=["same_field_twice", "missing_variable", "group_is_a_variable", "string_variable"],
)
def test_a_rank_correlation_that_cannot_be_computed_is_refused(spearman: ConversionResult, tool_id: int, because: str):
    row = report_row(spearman, tool_id)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [f"The Alteryx Spearman Correlation was not converted because {because}."]


@needs_spearman_corpus
def test_the_coefficient_matches_the_number_alteryx_prints_in_its_own_sample(tmp_path: Path):
    """`Spearman_Correlation.yxmd`'s comment box 39 states the answer Designer gives: .774136.

    That is the one part of this tool the workflow does verify, and it verifies it exactly — which is
    why these rows are `partial` for their *layout* and not for their arithmetic. Box 47 states the
    grouped result in words, and the signs agree too.
    """
    result = convert_yxmd(CORPUS_SPEARMAN.read_bytes(), source_name="Spearman_Correlation.yxmd")
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]

    ungrouped = _grid(flow, result, 2)
    assert ungrouped["Spearman"][0] == pytest.approx(0.774136, abs=5e-7)

    grouped = {row["Industry"]: row["Spearman"] for row in _grid(flow, result, 46).to_dicts()}
    assert grouped["Communication"] > 0
    assert grouped["Manufacturing"] < 0 and grouped["Software"] < 0


# --- Field Summary Report and Basic Data Profile ---


@pytest.fixture()
def profile() -> ConversionResult:
    return convert_yxmd(read_fixture("field_profile.yxmd"), source_name="field_profile.yxmd")


@pytest.fixture()
def profile_flow(tmp_path: Path, profile: ConversionResult):
    flow = open_flow(write_flow(profile, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    return flow


def test_a_field_summary_gives_one_row_per_column_with_its_nulls_and_cardinality(profile_flow, profile):
    """Box 15 of `Field_Summary.yxmd` names the two columns it calls useful: missing, and unique."""
    frame = _grid(profile_flow, profile, 410)
    assert frame.columns == ["Name", "Type", "PercentMissing", "UniqueValues"]
    by_name = {row["Name"]: row for row in frame.to_dicts()}
    assert set(by_name) == {"depth_m", "orchard", "orchard outline"}
    # depth_m holds one empty cell of four, and three distinct values counting the null.
    assert by_name["depth_m"]["PercentMissing"] == pytest.approx(25.0)
    assert by_name["depth_m"]["UniqueValues"] == 4
    assert by_name["orchard"]["PercentMissing"] == pytest.approx(0.0)
    assert by_name["orchard"]["UniqueValues"] == 2


def test_a_field_summary_profiles_only_the_selected_columns(profile_flow, profile):
    """A name with a space in it is ordinary here: the list is comma-joined, not space-joined."""
    frame = _grid(profile_flow, profile, 420)
    assert frame["Name"].to_list() == ["depth_m"]
    assert not any("every column that reaches it" in message for message in report_row(profile, 420).messages)


def test_an_all_selected_field_list_profiles_whatever_arrives_instead_of_naming_it(profile_flow, profile):
    """Naming an all-selected list would be a claim about what arrives, and the importer has none.

    A corpus workflow proves it: its list names a spatial column, while the Parquet that
    `flowfile convert yxdb` writes for the same `.yxdb` calls that spatial column
    that name with a `__spatial` suffix — so selecting the cached names by hand made it fail.
    """
    frame = _grid(profile_flow, profile, 410)
    assert set(frame["Name"].to_list()) == {"depth_m", "orchard", "orchard outline"}
    code = dumped_nodes(profile)[report_row(profile, 410).flowfile_node_ids[0]]["setting_input"]["polars_code_input"][
        "polars_code"
    ]
    assert "_source = input_df\n" in code and "input_df.select(" not in code
    assert any("every column that reaches it" in message for message in report_row(profile, 410).messages)


def test_the_rendered_report_anchors_drop_their_wires_instead_of_answering_with_the_table(profile):
    """The 'Reports' anchor produces a rendered report Flowfile has no node for.

    Letting it fall back to the tool's default output would hand the Browse the profile table under
    a different question's name — a wire that looks connected and is answering something else.
    """
    rows = {row.alteryx_tool_id: row for row in profile.report.rows}
    sentence = (
        "The Alteryx Field Summary Report's 'Reports' anchor produces a rendered report, which Flowfile "
        "has no node for; only the data anchor was imported, so the connections from 'Reports' were not "
        "wired."
    )
    assert sentence in rows[410].messages
    assert (rows[460].status, rows[460].reason) == ("partial", "dropped_connection")
    assert sentence in rows[460].messages
    assert dumped_nodes(profile)[rows[460].flowfile_node_ids[0]]["input_ids"] == []


def test_sampling_is_reported_rather_than_silently_ignored(profile):
    """A profile of a random tenth is a different answer from a profile of everything."""
    row = report_row(profile, 430)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("a random 10% of the rows" in message for message in row.messages)
    assert any("different answer, not a slower one" in message for message in row.messages)


def test_a_sample_that_is_both_a_count_and_a_percentage_is_refused(profile):
    """The exclusive pair is read together, the same way Random Records reads it."""
    row = report_row(profile, 440)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Field Summary Report was not converted because 'Sample Data' is on and the tool "
        "selects both a record count and a percentage, so the sample size cannot be read."
    ]


def test_a_field_summary_with_nothing_selected_is_refused(profile):
    row = report_row(profile, 450)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == ["The Alteryx Field Summary Report was not converted because no field is selected."]


def test_a_basic_data_profile_adds_each_column_extremes(profile_flow, profile):
    frame = _grid(profile_flow, profile, 470)
    assert frame.columns == ["Name", "Type", "PercentMissing", "UniqueValues", "Min", "Max"]
    by_name = {row["Name"]: row for row in frame.to_dicts()}
    assert (by_name["depth_m"]["Min"], by_name["depth_m"]["Max"]) == ("1", "4")
    assert (by_name["orchard"]["Min"], by_name["orchard"]["Max"]) == ("Okonjo", "Salgado")


def test_a_basic_data_profile_says_which_alteryx_limits_it_does_not_apply(profile):
    row = report_row(profile, 470)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert any("stops counting distinct values at 10000" in message for message in row.messages)
    assert any("'IsMetric'" in message for message in row.messages)


# --- Filter REGEX_Match, the Period operators and the DateType guard ---


@pytest.fixture()
def filter_regex_period() -> ConversionResult:
    return convert_yxmd(read_fixture("filter_regex_period.yxmd"), source_name="filter_regex_period.yxmd")


def _filter_expression(result: ConversionResult, tool_id: int) -> str:
    node = dumped_nodes(result)[report_row(result, tool_id).flowfile_node_ids[0]]
    return node["setting_input"]["filter_input"]["advanced_filter"]


@pytest.mark.parametrize(
    ("tool_id", "expected"),
    [
        (510, 'contains([orchard], "(?i)^(?:.*ondo)$")'),
        (515, 'contains([orchard], "(?i)^(?:.*ONDO)$")'),
        (520, 'contains([orchard], "(?i)^(?:.*ONDO)$")'),
        (525, 'contains([orchard], "^(?:.*ONDO)$")'),
        (550, "[picked] >= today() and [picked] <= add_days(today(), 2)"),
        (560, "[picked] <= today() and [picked] >= add_weeks(today(), -1)"),
        (570, "[picked] <= add_days(today(), 1)"),
        (580, '[picked_on] <= "2017-12-29"'),
        (630, "[picked] >= add_days(today(), 1) and [picked] <= add_days(add_days(today(), 1), 3)"),
        (640, "[picked] <= add_days(today(), -1) and [picked] >= add_days(add_days(today(), -1), -1)"),
        (650, 'starts_with(lowercase([orchard]), "ok")'),
    ],
    ids=[
        "regex_default_is_case_insensitive",
        "regex_default_matches_the_other_case",
        "regex_case_insensitive_explicitly",
        "regex_case_sensitive_explicitly",
        "period_after",
        "period_before",
        "tomorrow",
        "fixed",
        "period_anchored_on_tomorrow",
        "period_anchored_on_yesterday",
        "string_operator_on_a_fixed_date",
    ],
)
def test_the_filter_expression_each_option_really_stands_for(filter_regex_period, tool_id: int, expected: str):
    """REGEX_Match anchors, the Period operators are windows, and a dynamic date is not <Operand>.

    Box 150 of `Filter.yxmd` states the window: "Rows that are within a period of 2 days from Today's
    date are True", and box 135 names the dynamic dates — Today, Tomorrow, Yesterday, or a Fixed one.

    Alteryx documents `REGEX_Match(String, pattern, icase)` with "By default icase=1 (meaning ignore
    case)", so the flag is on unless the third argument is a literal false — which is the reading
    the spec had backwards.
    """
    row = report_row(filter_regex_period, tool_id)
    assert (row.status, row.reason) == ("converted", "converted")
    assert _filter_expression(filter_regex_period, tool_id) == expected


def test_a_relative_cut_off_says_it_is_evaluated_when_the_flow_runs(filter_regex_period):
    """The same flow keeps a different set of rows tomorrow, which a green badge does not say."""
    assert any("evaluated when the flow runs" in message for message in report_row(filter_regex_period, 550).messages)
    assert report_row(filter_regex_period, 580).messages == []


def test_the_period_window_may_be_anchored_on_tomorrow_or_yesterday_and_the_row_says_so(filter_regex_period):
    """The spec said the period operators anchor on today only; the code accepts all three.

    They are kept, because `_filter_anchor_date` is the same reader the comparison operators use and
    Alteryx's own box 135 names the same three dates for both — but that is now pinned here and
    stated in the row's message rather than left as an unclaimed extra.
    """
    for tool_id in (630, 640):
        row = report_row(filter_regex_period, tool_id)
        assert (row.status, row.reason) == ("converted", "converted")
        assert any("Today, Tomorrow and Yesterday" in message for message in row.messages)
        assert any("period operators anchor their window on the same three" in message for message in row.messages)


def test_a_string_operator_with_a_fixed_date_still_reads_its_operand(filter_regex_period):
    """The guard is on the dynamic date, not on the operator: tool 650 is `StartsWith` + `fixed`."""
    row = report_row(filter_regex_period, 650)
    assert (row.status, row.reason) == ("converted", "converted")
    assert row.messages == []


@pytest.mark.parametrize(
    ("tool_id", "because"),
    [
        (530, "the REGEX_Match() pattern uses lookahead, which Polars' regex engine does not support"),
        (
            540,
            "REGEX_Match() can only be converted when the pattern is a literal string, because a pattern "
            "built at run time cannot be checked for constructs Polars' regex engine lacks",
        ),
        (590, "the Alteryx simple filter period unit 'fortnights' is not supported"),
        (
            600,
            "the Alteryx simple filter's period operator is anchored on a fixed date, which Flowfile reads "
            "as a literal rather than as the window the tool draws around it",
        ),
        (610, "the Alteryx simple filter compares against 'next tuesday', which Flowfile cannot express"),
        (
            620,
            "the Alteryx simple filter applies 'StartsWith' to the dynamic date 'tomorrow', and Alteryx "
            "offers a dynamic date only on a comparison, so what this tool asks cannot be read from the "
            "workflow",
        ),
    ],
    ids=[
        "lookahead",
        "pattern_from_a_column",
        "unknown_period_unit",
        "fixed_anchor",
        "unknown_date_type",
        "string_operator_with_a_dynamic_date",
    ],
)
def test_a_filter_option_flowfile_cannot_express_is_refused(filter_regex_period, tool_id: int, because: str):
    row = report_row(filter_regex_period, tool_id)
    assert (row.status, row.reason) == ("placeholder", "translator_refused")
    assert row.messages[0] == f"The Alteryx filter expression could not be converted: {because}."


def test_a_simple_mode_filter_without_an_expression_does_not_print_an_empty_one(filter_regex_period):
    """A simple-mode tool need not carry <Expression> at all, and 590 does not."""
    assert report_row(filter_regex_period, 590).messages[1] == (
        "The tool carries no expression of its own; it is configured in the simple-mode editor."
    )


def test_the_filter_flow_runs_and_the_regex_keeps_the_right_rows(tmp_path: Path, filter_regex_period):
    flow = open_flow(write_flow(filter_regex_period, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    # '.*ondo' anchored matches 'Redondo' and nothing else; case-insensitively it still does.
    assert _grid(flow, filter_regex_period, 510)["orchard"].to_list() == ["Redondo"]
    assert _grid(flow, filter_regex_period, 520)["orchard"].to_list() == ["Redondo"]
    # '.*ONDO' with no third argument is the pair that tells the two readings apart: Alteryx's
    # default ignores case and keeps 'Redondo', and only an explicit 0 makes it keep nothing.
    assert _grid(flow, filter_regex_period, 515)["orchard"].to_list() == ["Redondo"]
    assert _grid(flow, filter_regex_period, 525)["orchard"].to_list() == []
    # A window of the next two days keeps neither the 2017 row nor the year-2400 one.
    assert _grid(flow, filter_regex_period, 550).height == 0


# A chain whose second assignment refers to the column the first one wrote, unbracketed.
BARE_IDENTIFIERS_IN_A_FORMULA_CHAIN = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="901"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /></Fields>
        <Data><r><c>4</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="902"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="doubled" type="Double" size="8" expression="x * 2" />
        <FormulaField field="squared" type="Double" size="8" expression="POW(doubled, 2)" />
        <FormulaField field="missing" type="Double" size="8" expression="ABS(nowhere)" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="901" Connection="Output" />
      <Destination ToolID="902" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_bare_reference_to_a_column_the_chain_just_wrote_resolves(tmp_path: Path):
    """`known` grows by each assignment's target, so the second formula can name the first's column.

    The third assignment names nothing at all, and keeps the refusal — which is what makes the first
    two a lookup rather than a rule that every bare word is a column.

    Both resolved references are wrapped by the floating rule, and their literals floated with
    them: the tool declares every target Double, 'x' is a Text Input column and 'doubled' is
    this chain's own Double target.
    """
    result = convert_yxmd(BARE_IDENTIFIERS_IN_A_FORMULA_CHAIN, source_name="bare.yxmd")
    row = report_row(result, 902)
    assert (row.status, row.reason) == ("commented", "translator_refused")
    nodes = dumped_nodes(result)
    bodies = [nodes[node_id]["setting_input"]["function"]["function"] for node_id in row.flowfile_node_ids]
    assert bodies[0] == "to_number([x]) * 2.0"
    assert bodies[1] == "power(to_number([doubled]), 2.0)"
    assert bodies[2].startswith("// Alteryx formula could not be converted automatically:")
    assert "unbracketed field reference 'nowhere'" in bodies[2]
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(row.flowfile_node_ids[1]).get_resulting_data().data_frame.collect()
    assert frame["squared"].to_list() == [64.0]


# A hashed column and an encoded one, neither of which any corpus tool uses.
CAVEAT_SENTENCE = "exact on ASCII input and different above it; check a non-ASCII value against Designer"


@pytest.fixture
def caveated_md5(monkeypatch: pytest.MonkeyPatch) -> None:
    """No shipped FunctionSpec carries a caveat, so the demotion is pinned on a patched one."""
    from flowfile_core.flowfile.converters.alteryx import expression

    monkeypatch.setitem(
        expression.FUNCTION_MAP, "md5_utf8", expression.FunctionSpec("MD5_UTF8", "md5", 1, 1, caveat=CAVEAT_SENTENCE)
    )


HASHING_AND_ENCODING_IN_A_FORMULA = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="911"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="grower" /></Fields>
        <Data><r><c>abc</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="912"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="digest" type="V_String" size="32" expression="MD5_UTF8([grower])" />
        <FormulaField field="packed" type="V_String" size="64" expression="Base64Encode([grower])" />
      </FormulaFields></Configuration></Properties></Node>
    <Node ToolID="913"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="shouty" type="V_String" size="32" expression="Uppercase([grower])" />
      </FormulaFields></Configuration></Properties></Node>
    <Node ToolID="914"><GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter" />
      <Properties><Configuration>
        <Expression>MD5_UTF8([grower]) = "x"</Expression>
        <Mode>Custom</Mode>
      </Configuration></Properties></Node>
    <Node ToolID="915"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration>
        <RenameMode>Formula</RenameMode>
        <Expression>MD5_UTF8([_CurrentField_])</Expression>
        <Fields><Field name="grower" /></Fields>
      </Configuration></Properties></Node>
    <Node ToolID="916"><GuiSettings Plugin="AlteryxBasePluginsGui.MultiFieldFormula.MultiFieldFormula" />
      <Properties><Configuration>
        <FieldType>Text</FieldType>
        <Fields><Field name="grower" /></Fields>
        <CopyOutput value="False" />
        <ChangeFieldType value="False" />
        <Expression>MD5_UTF8([_CurrentField_])</Expression>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="911" Connection="Output" />
      <Destination ToolID="912" Connection="Input" /></Connection>
    <Connection><Origin ToolID="912" Connection="Output" />
      <Destination ToolID="913" Connection="Input" /></Connection>
    <Connection><Origin ToolID="911" Connection="Output" />
      <Destination ToolID="914" Connection="Input" /></Connection>
    <Connection><Origin ToolID="911" Connection="Output" />
      <Destination ToolID="915" Connection="Input" /></Connection>
    <Connection><Origin ToolID="911" Connection="Output" />
      <Destination ToolID="916" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_caveated_function_costs_the_row_its_green_badge(tmp_path: Path, caveated_md5: None):
    """A mapping that is exact on some inputs and different on others is not `converted`.

    No shipped FunctionSpec carries a caveat today (MD5_UTF8 is exact, the other MD5 variants are
    refused), so the demotion is pinned on a caveat patched onto MD5_UTF8 for the test.
    """
    result = convert_yxmd(HASHING_AND_ENCODING_IN_A_FORMULA, source_name="hashing.yxmd")
    hashed = report_row(result, 912)
    assert (hashed.status, hashed.reason) == ("partial", "option_unsupported")
    assert any(CAVEAT_SENTENCE in message for message in hashed.messages)

    # The formula beside it promises nothing extra, so it keeps its green badge.
    plain = report_row(result, 913)
    assert (plain.status, plain.reason) == ("converted", "converted")
    assert plain.messages == []

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(hashed.flowfile_node_ids[-1]).get_resulting_data().data_frame.collect()
    # 'abc' is ASCII, so this is the digest Alteryx would print too.
    assert frame["digest"].to_list() == ["900150983cd24fb0d6963f7d28e17f72"]
    assert frame["packed"].to_list() == ["YWJj"]


# --- a declared floating output computes in floating point ---

FLOAT_DECLARED_OVER_AN_INTEGER_COLUMN = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="920"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /><Field name="label" /></Fields>
        <Data><r><c>500</c><c>big</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="921"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="wraps" type="Double" size="8" expression="-20*POW([x], 7)" />
        <FormulaField field="counted" type="Int32" size="4" expression="[x] + 1" />
        <FormulaField field="named" type="Double" size="8"
          expression="IF [label] = &quot;big&quot; THEN 1 ELSE 0 ENDIF" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="920" Connection="Output" />
      <Destination ToolID="921" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_double_assignment_computes_in_float64_and_does_not_wrap(tmp_path: Path):
    """-20 * 500^7 is -1.5625e20, which Int64 cannot hold; Alteryx declares the field Double.

    The repair is on the *operands* — the column reference and the integer literals — not on the
    finished value: casting the result would arrive after the wrap, and
    `(-20 * pl.col('x').pow(7)).cast(pl.Float64)` was measured returning the wrapped integer rather
    than the number Designer prints.
    """
    result = convert_yxmd(FLOAT_DECLARED_OVER_AN_INTEGER_COLUMN, source_name="floats.yxmd")
    row = report_row(result, 921)
    assert (row.status, row.reason) == ("converted", "converted")
    nodes = dumped_nodes(result)
    bodies = [nodes[node_id]["setting_input"]["function"]["function"] for node_id in row.flowfile_node_ids]
    assert bodies[0] == "-20.0 * power(to_number([x]), 7.0)"
    # An integer-declared target is left alone: Alteryx computes that one in an integer type too.
    assert bodies[1] == "[x] + 1"
    # A Double-declared target reading a text column is left alone as well — to_number() is a strict
    # cast, so wrapping 'label' would stop the flow instead of computing anything.
    assert bodies[2] == 'if [label] = "big" then 1 else 0 endif'

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(row.flowfile_node_ids[-1]).get_resulting_data().data_frame.collect()
    assert frame["wraps"].to_list() == [-1.5625e20]
    assert frame["wraps"].to_list() != [-8676047410323587072]


FLOAT_DECLARED_OVER_AN_UNTYPED_COLUMN = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="930"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /></Fields>
        <Data><r><c>500</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="931"><GuiSettings Plugin="AlteryxBasePluginsGui.XMLParse.XMLParse" />
      <Properties><Configuration /></Properties></Node>
    <Node ToolID="932"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="wraps" type="Double" size="8" expression="-20*POW([x], 7)" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="930" Connection="Output" />
      <Destination ToolID="931" Connection="Input" /></Connection>
    <Connection><Origin ToolID="931" Connection="Output" />
      <Destination ToolID="932" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_declared_float_over_a_column_of_unknown_type_loses_its_green_badge():
    """An unconverted tool between the Text Input and the Formula could have changed 'x' entirely.

    The type lookup's walk crosses only tools it can prove leave the column alone; a placeholder is the
    opposite of that, so the walk stops here and the answer stays unknown. Fail closed rather than
    cast on a guess — to_number() on a text column stops the flow, so the row says what it could
    not settle and keeps the arithmetic it can defend.
    """
    result = convert_yxmd(FLOAT_DECLARED_OVER_AN_UNTYPED_COLUMN, source_name="untyped.yxmd")
    row = report_row(result, 932)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert row.messages == [
        "Alteryx declares 'wraps' as Double and evaluates the whole expression in floating point, "
        "but the type of 'x' is not settled by this workflow. Flowfile computes in the type the "
        "input carries, which differs from Alteryx wherever an integer column overflows; check "
        "this column against Designer before trusting it."
    ]
    nodes = dumped_nodes(result)
    # The literals still float — that half needs no promise about the column — but 'x' itself is
    # left uncast, which is the half the row is demoted for.
    assert nodes[row.flowfile_node_ids[0]]["setting_input"]["function"]["function"] == "-20.0 * power([x], 7.0)"


# --- the integer literals compute in the declared floating type too ---

FLOAT_DECLARED_OVER_INTEGER_LITERALS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="940"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /><Field name="code" /></Fields>
        <Data><r><c>3</c><c>abcdef</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="941"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="literal_only" type="Double" size="8" expression="POW(2, 70)" />
        <FormulaField field="wraps_first" type="Double" size="8" expression="[x]*POW(2,70)" />
        <FormulaField field="adds_big" type="Double" size="8" expression="[x]+POW(60,6)" />
        <FormulaField field="counted" type="Int32" size="4" expression="POW(2, 10)" />
        <FormulaField field="sliced" type="Double" size="8" expression="Length(Substring([code], 0, 5)) * 2" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="940" Connection="Output" />
      <Destination ToolID="941" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_declared_float_computes_its_integer_literals_in_float_too(tmp_path: Path):
    """A cast on the column references cannot reach these: two of them read no column at all.

    `POW(2, 70)` wraps to 0 in Int32 where Alteryx prints 1.18e21, and in `[x]+POW(60,6)` the
    literal subexpression has already wrapped before the column is ever added. Rendering the
    integer literals as floats is what makes the arithmetic floating.
    """
    result = convert_yxmd(FLOAT_DECLARED_OVER_INTEGER_LITERALS, source_name="literals.yxmd")
    row = report_row(result, 941)
    assert (row.status, row.reason) == ("converted", "converted")
    # Every operand's type is settled here, so the row owes no floating-point caveat.
    assert row.messages == ["5 Alteryx assignments became 5 chained Flowfile nodes."]
    nodes = dumped_nodes(result)
    bodies = [nodes[node_id]["setting_input"]["function"]["function"] for node_id in row.flowfile_node_ids]
    assert bodies[0] == "power(2.0, 70.0)"
    assert bodies[1] == "to_number([x]) * power(2.0, 70.0)"
    assert bodies[2] == "to_number([x]) + power(60.0, 6.0)"
    # An integer-declared target keeps integer literals: Alteryx computes that one in Int32 too.
    assert bodies[3] == "power(2, 10)"
    # Only arithmetic operands float. A positional argument means an integer and stays one, or
    # `substring()` would be handed 0.0 and 5.0.
    assert bodies[4] == "length(substring([code], 0, 5)) * 2.0"

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(row.flowfile_node_ids[-1]).get_resulting_data().data_frame.collect()
    assert frame["literal_only"].to_list() == [1.1805916207174113e21]
    assert frame["wraps_first"].to_list() == [3.541774862152234e21]
    assert frame["adds_big"].to_list() == [46656000003.0]
    assert frame["counted"].to_list() == [1024]
    assert frame["sliced"].to_list() == [10.0]


# --- the type lookup walks through tools that change no column ---

FLOAT_DECLARED_TWO_HOPS_UPSTREAM = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="950"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /><Field name="tag" /></Fields>
        <Data><r><c>500</c><c>Y</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="951"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration><SelectFields>
        <SelectField field="x" selected="True" />
        <SelectField field="tag" selected="True" />
        <SelectField field="*Unknown" selected="True" />
      </SelectFields></Configuration></Properties></Node>
    <Node ToolID="952"><GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter" />
      <Properties><Configuration>
        <Mode>Simple</Mode>
        <Simple><Operator>=</Operator><Field>tag</Field>
          <Operands><DateType>fixed</DateType><Operand>Y</Operand></Operands></Simple>
      </Configuration></Properties></Node>
    <Node ToolID="953"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="wraps" type="Double" size="8" expression="-20*POW([x], 7)" />
      </FormulaFields></Configuration></Properties></Node>
    <Node ToolID="954"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="halved" type="Double" size="8" expression="[x]/2" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="950" Connection="Output" />
      <Destination ToolID="951" Connection="Input" /></Connection>
    <Connection><Origin ToolID="951" Connection="Output" />
      <Destination ToolID="952" Connection="Input" /></Connection>
    <Connection><Origin ToolID="952" Connection="True" />
      <Destination ToolID="953" Connection="Input" /></Connection>
    <Connection><Origin ToolID="952" Connection="False" />
      <Destination ToolID="954" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_the_type_lookup_walks_through_tools_that_change_no_column(tmp_path: Path):
    """A Select that renames nothing and a Filter that drops rows both leave 'x' exactly as it was.

    This is `ControlContainer.yxmd` tools 60 and 61 in miniature — both of a Filter's anchors, since
    both carry every column through. The lookup used to stop at the first hop and tell these
    rows that 'x' was "not settled by this workflow", which the Text Input two hops up settles.
    """
    result = convert_yxmd(FLOAT_DECLARED_TWO_HOPS_UPSTREAM, source_name="hops.yxmd")
    nodes = dumped_nodes(result)
    for tool_id, body in ((953, "-20.0 * power(to_number([x]), 7.0)"), (954, "to_number([x]) / 2.0")):
        row = report_row(result, tool_id)
        assert (row.status, row.reason) == ("converted", "converted"), tool_id
        assert row.messages == [], tool_id
        assert nodes[row.flowfile_node_ids[0]]["setting_input"]["function"]["function"] == body, tool_id

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(report_row(result, 953).flowfile_node_ids[-1]).get_resulting_data().data_frame.collect()
    assert frame["wraps"].to_list() == [-1.5625e20]


A_RENAME_BETWEEN_THE_SOURCE_AND_THE_FORMULA = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="960"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="x" /></Fields>
        <Data><r><c>500</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="961"><GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect" />
      <Properties><Configuration><SelectFields>
        <SelectField field="x" selected="True" rename="y" />
        <SelectField field="*Unknown" selected="True" />
      </SelectFields></Configuration></Properties></Node>
    <Node ToolID="962"><GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula" />
      <Properties><Configuration><FormulaFields>
        <FormulaField field="wraps" type="Double" size="8" expression="-20*POW([y], 7)" />
      </FormulaFields></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="960" Connection="Output" />
      <Destination ToolID="961" Connection="Input" /></Connection>
    <Connection><Origin ToolID="961" Connection="Output" />
      <Destination ToolID="962" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_rename_between_the_source_and_the_formula_stops_the_walk():
    """'y' downstream is 'x' upstream, and the walk is a name lookup — so it stops and says so.

    The walk only crosses a tool it can prove changed nothing about this column; a Select that
    renames it is exactly the tool it must not cross.
    """
    result = convert_yxmd(A_RENAME_BETWEEN_THE_SOURCE_AND_THE_FORMULA, source_name="renamed.yxmd")
    row = report_row(result, 962)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert row.messages == [
        "Alteryx declares 'wraps' as Double and evaluates the whole expression in floating point, "
        "but the type of 'y' is not settled by this workflow. Flowfile computes in the type the "
        "input carries, which differs from Alteryx wherever an integer column overflows; check "
        "this column against Designer before trusting it."
    ]


# --- a named group is not lookbehind, in the RegEx tool either ---


def test_a_named_group_in_the_regex_tool_is_not_refused_as_lookbehind():
    """`(?<` was screened as a substring, so `(?<a>...)` was refused with a sentence about lookbehind.

    Polars accepts both spellings of a named group; only `(?P<a>...)` used to survive the screen.
    The `<` is written `&lt;` because a raw one cannot appear in an XML attribute value.
    """
    accepted = convert_yxmd(
        regex_tokenize("(?&lt;a>[A-Z]{2})-(?&lt;b>[0-9]{2})", num_fields="2"), source_name="named.yxmd"
    )
    row = report_row(accepted, 2)
    assert row.status == "partial", row.messages
    assert not any("lookbehind" in message for message in row.messages), row.messages

    refused = convert_yxmd(regex_tokenize("(?&lt;=A)B", num_fields="1"), source_name="behind.yxmd")
    row = report_row(refused, 2)
    assert row.status == "placeholder"
    assert row.messages == [
        "The Alteryx RegEx tool could not be converted: the Alteryx regular expression uses "
        "lookbehind, which Polars' regex engine does not support."
    ]


# --- a wire onto an anchor the macro does not have is dropped, not quietly rerouted ---


def test_a_wire_onto_an_anchor_the_field_summary_macro_does_not_have_is_dropped():
    """`register_all_inputs` registered whatever anchor a wire arrived on, so a misaddressed wire
    landed on the profile's own input and nothing said so. Only `Field Input` is registered now."""
    document = read_fixture("field_profile.yxmd").replace(
        b'<Destination ToolID="420" Connection="Field Input" />',
        b'<Destination ToolID="420" Connection="Nonsense Anchor" />',
    )
    result = convert_yxmd(document, source_name="field_profile.yxmd")
    dropped = "A connection from ToolID 400 (Output) to ToolID 420 (Nonsense Anchor) was dropped; reconnect it by hand."
    # Both ends of the wire say so: the Text Input it left and the macro it never reached.
    assert dropped in report_row(result, 400).messages
    assert dropped in report_row(result, 420).messages
    assert (report_row(result, 400).status, report_row(result, 400).reason) == ("partial", "dropped_connection")

    # The wire the corpus really draws is untouched.
    untouched = convert_yxmd(read_fixture("field_profile.yxmd"), source_name="field_profile.yxmd")
    assert not any("was dropped" in message for message in report_row(untouched, 420).messages)


# --- what a fixed-date filter does on a real Date column ---


def test_a_fixed_date_filter_on_a_real_date_column_raises_when_the_flow_runs(tmp_path: Path):
    """Documents today's behaviour, which §6 decision 6 is about and will change.

    Tool 580 filters `picked_on`, the string the Text Input holds, so its `<= "2017-12-29"` is a
    string-to-string comparison that cannot raise. Pointed at `picked` — the Date the Formula at 505
    parses — the same emitted expression compares a date with a string literal and Polars refuses.

    The row is still `converted` with no message: nothing in `<DateType>fixed</DateType>` tells the
    importer which of the two columns it is looking at, which is exactly decision 6. When that
    decision lands, this test changes with it.
    """
    document = read_fixture("filter_regex_period.yxmd").replace(b"<Field>picked_on</Field>", b"<Field>picked</Field>")
    result = convert_yxmd(document, source_name="filter_regex_period.yxmd")
    row = report_row(result, 580)
    assert (row.status, row.reason) == ("converted", "converted")
    assert row.messages == []
    assert _filter_expression(result, 580) == '[picked] <= "2017-12-29"'

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert not run_info.success
    errors = [step.error for step in run_info.node_step_result if not step.success]
    assert any("cannot compare 'date/datetime/time' to a string value" in (error or "") for error in errors), errors


CORPUS_PEARSON = LEARNING / "alteryx_nodes" / "11 Data Investigation" / "Pearson_Correlation.yxmd"
needs_pearson_corpus = pytest.mark.skipif(
    not CORPUS_PEARSON.exists(), reason=f"Alteryx corpus workflow not present ({CORPUS_PEARSON})"
)


@needs_pearson_corpus
def test_the_piecewise_correlation_matches_the_number_alteryx_prints_in_its_own_sample(tmp_path: Path):
    """`Pearson_Correlation.yxmd`'s comment box 65 states Designer's answer for tool 63: -.761336.

    This once printed -0.0969, because `-20*POW(x, 7)` wrapped in Int64 over x = 500, 750
    and 1000. Box 65 also states the Spearman coefficient as -1, and that one is *not* pinned here:
    the column ties (27 four times, 26 six times) and average-rank Spearman gives -0.98294, so the
    box is describing the function rather than reporting what the tool returned.
    """
    result = convert_yxmd(CORPUS_PEARSON.read_bytes(), source_name="Pearson_Correlation.yxmd")
    assert report_row(result, 47).status == "converted"
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    for node_id in [node.node_id for node in flow.nodes if node.node_type == "explore_data"]:
        flow.delete_node(node_id)
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]

    grid = _grid(flow, result, 63)
    assert grid["Variable"].to_list() == ["x", "Piecewise_function"]
    assert grid["Piecewise_function"][0] == pytest.approx(-0.761336, abs=5e-7)


# --- what a null does, and what the covariance divides by ---


def _correlation_with_a_null(plugin_and_config: str) -> bytes:
    """Three numeric columns, one null in 'a', feeding one data-investigation tool."""
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="700"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /><Field name="b" /><Field name="c" /></Fields>
        <Data>
          <r><c>1</c><c>2</c><c>5</c></r>
          <r><c>2</c><c>4</c><c>3</c></r>
          <r><c>3</c><c>6</c><c>9</c></r>
          <r><c>4</c><c>8</c><c>1</c></r>
          <r><c></c><c>10</c><c>7</c></r>
        </Data>
      </Configuration></Properties></Node>
    {plugin_and_config}
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="700" Connection="Output" />
      <Destination ToolID="710" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
""".encode()


_PEARSON_710 = """<Node ToolID="710">
      <GuiSettings Plugin="AlteryxBasePluginsGui.PearsonCorrelation.PearsonCorrelation" />
      <Properties><Configuration>
        <Fields orderChanged="False">
          <Field name="a" /><Field name="b" /><Field name="c" />
          <Field name="*Unknown" selected="False" />
        </Fields>
        <Covariance value="{covariance}" />
      </Configuration></Properties></Node>"""


def test_one_null_makes_a_whole_correlation_row_and_column_nan(tmp_path: Path):
    """`DataFrame.corr` is numpy's corrcoef: the null does not drop a pair, it poisons a variable.

    'a' holds one null out of five, so 'a' correlates with nothing — not even with itself, whose
    diagonal is NaN rather than 1. That is why the generated comment no longer promises a 1.
    """
    result = convert_yxmd(_correlation_with_a_null(_PEARSON_710.format(covariance="False")), source_name="n.yxmd")
    row = report_row(result, 710)
    assert any("makes that whole variable's row and column NaN" in message for message in row.messages)
    node_id = row.flowfile_node_ids[0]
    assert (
        "the diagonal is 1 unless a variable holds nulls or overflows"
        in (dumped_nodes(result)[node_id]["setting_input"]["polars_code_input"]["polars_code"])
    )

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success
    frame = _grid(flow, result, 710)
    assert all(value != value for value in frame["a"].to_list())  # NaN, the whole column
    assert frame["b"][0] != frame["b"][0]  # and the whole row
    # b and c hold no nulls, so they correlate with each other and with themselves as usual.
    assert frame["b"][1] == pytest.approx(1.0)
    assert frame["c"][1] == frame["b"][2]


def test_the_covariance_branch_drops_the_pair_instead_and_divides_by_n_minus_one(tmp_path: Path):
    """One XML flag away from the test above, and the null rule is the other one.

    b is 2..10 with no nulls, so cov(b, b) is its sample variance: 10 with the n-1 divisor and 8
    with n. a has a null, and cov(a, b) over the four surviving pairs is 3.3333, not null.
    """
    result = convert_yxmd(_correlation_with_a_null(_PEARSON_710.format(covariance="True")), source_name="c.yxmd")
    row = report_row(result, 710)
    assert any("the divisor is n-1, the sample covariance" in message for message in row.messages)
    assert any("drops that pair of rows rather than the whole column" in message for message in row.messages)

    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success
    frame = _grid(flow, result, 710)
    assert frame["b"][1] == pytest.approx(10.0)  # n-1; the population variance would be 8.0
    # cov(a, a) is a's own variance over the four rows that survive: 1.6667 with n-1, 1.25 with n.
    assert frame["a"][0] == pytest.approx(1.6666667, abs=1e-6)
    # cov(b, a) is computed on those same four pairs, not on b's five rows and not as null.
    assert frame["a"][1] == pytest.approx(
        pl.DataFrame({"a": [1.0, 2, 3, 4], "b": [2.0, 4, 6, 8]}).select(pl.cov("a", "b")).item()
    )


SPEARMAN_WITH_A_NULL = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="700"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="a" /><Field name="b" /></Fields>
        <Data>
          <r><c>1</c><c>2</c></r>
          <r><c>2</c><c>4</c></r>
          <r><c>3</c><c>6</c></r>
          <r><c></c><c>1</c></r>
        </Data>
      </Configuration></Properties></Node>
    <Node ToolID="720">
      <GuiSettings />
      <Properties><Configuration>
        <Value name="Input.Field Selection">Variable1=a
Variable2=b</Value>
        <Value name="Enable Group By">False</Value>
        <Value name="Select Field to Group By">a</Value>
      </Configuration></Properties>
      <EngineSettings Macro="SpearmanCorrCoeff.yxmc" />
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="700" Connection="Output" />
      <Destination ToolID="720" Connection="Field Selection" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_null_drops_the_pair_in_the_rank_correlation(tmp_path: Path):
    """The third null rule in the wave, and the third different one — hence the sentence.

    The fourth row would break the monotone run if it counted; dropping the pair leaves 1.0.
    """
    result = convert_yxmd(SPEARMAN_WITH_A_NULL, source_name="s.yxmd")
    row = report_row(result, 720)
    assert any("drops that pair of rows rather than the whole column" in message for message in row.messages)
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    assert flow.run_graph().success
    assert _grid(flow, result, 720)["Spearman"][0] == pytest.approx(1.0)


# --- the cheap pins ---


def test_every_caveat_call_site_costs_its_row_the_green_badge(tmp_path: Path, caveated_md5: None):
    """`_caveated` was pinned at the Formula site only; three other callers used it untested.

    A custom-mode Filter, a Dynamic Rename formula and a Multi-Field Formula all run an Alteryx
    expression through the same translator, so all four have to demote the same way — deleting the
    call at any of the other three left the suite green.
    """
    result = convert_yxmd(HASHING_AND_ENCODING_IN_A_FORMULA, source_name="hashing.yxmd")
    for tool_id in (914, 915, 916):
        row = report_row(result, tool_id)
        assert (row.status, row.reason) == ("partial", "option_unsupported"), tool_id
        assert any(CAVEAT_SENTENCE in message for message in row.messages), tool_id


DETOUR_END_WITH_TWO_WIRES_ON_ONE_ANCHOR = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="940"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="crate" /></Fields>
        <Data><r><c>1</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="941"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="crate" /></Fields>
        <Data><r><c>2</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="942"><GuiSettings Plugin="AlteryxBasePluginsGui.Detour.Detour" />
      <Properties><Configuration><DetourRight value="False" /></Configuration></Properties></Node>
    <Node ToolID="943"><GuiSettings Plugin="AlteryxBasePluginsGui.DetourEnd.DetourEnd" />
      <Properties><Configuration /></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="940" Connection="Output" />
      <Destination ToolID="942" Connection="Input" /></Connection>
    <Connection><Origin ToolID="942" Connection="Left" />
      <Destination ToolID="943" Connection="Input" /></Connection>
    <Connection><Origin ToolID="941" Connection="Output" />
      <Destination ToolID="943" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_detour_end_whose_live_wire_shares_an_anchor_refuses_to_pick_one():
    """One live Detour wire and one ordinary wire on the same anchor is not a resolvable shape.

    The live side is known, but both streams arrive on 'Input', so the node cannot be told which of
    them to carry: Alteryx would union them and passing one through would be a guess. This branch of
    `_detour_end_wire` had no test at all — deleting it left the suite green.
    """
    result = convert_yxmd(DETOUR_END_WITH_TWO_WIRES_ON_ONE_ANCHOR, source_name="detour.yxmd")
    row = report_row(result, 943)
    assert any("arrive on its 'Input' anchor" in message for message in row.messages), row.messages
    assert any("passes its input through" in message for message in row.messages)


EXCEL_READ_WITH_BOTH_HEADER_OPTIONS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="950"><GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties><Configuration>
        <Passwords />
        <File OutputFileName="" RecordLimit="" SearchSubDirs="False" FileFormat="25">crates.xlsx|||`Sheet1$`</File>
        <FormatSpecificOptions>
          <FirstRowData>{first_row_data}</FirstRowData>
          <HeaderRow>{header_row}</HeaderRow>
        </FormatSpecificOptions>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections />
</AlteryxDocument>
"""


@pytest.mark.parametrize(
    ("first_row_data", "header_row", "expects_sentence"),
    [("True", "True", True), ("False", "True", True), ("True", None, False)],
    ids=["both_and_they_disagree", "both_and_they_agree", "only_first_row_data"],
)
def test_an_excel_read_carrying_both_header_settings_says_which_one_it_read(
    first_row_data: str, header_row: str | None, expects_sentence: bool
):
    """Only FirstRowData is applied for Excel, and a workflow carrying both said nothing about it."""
    raw = EXCEL_READ_WITH_BOTH_HEADER_OPTIONS.decode().format(
        first_row_data=first_row_data, header_row=header_row or "True"
    )
    if header_row is None:
        raw = raw.replace("          <HeaderRow>True</HeaderRow>\n", "")
    row = report_row(convert_yxmd(raw.encode(), source_name="excel.yxmd"), 950)
    said = [message for message in row.messages if "both of Alteryx's header settings" in message]
    assert bool(said) is expects_sentence, row.messages
    if expects_sentence:
        assert "only FirstRowData was applied" in said[0]


def test_a_named_column_that_does_not_arrive_is_refused_rather_than_called_untyped(
    pearson: ConversionResult, spearman: ConversionResult
):
    """ "The type of 'x' is not known at import time" is a sentence about a column that arrives.

    Both fixtures name a column their Text Input does not hold. The old sentence read as a hedge —
    and was plainly false against a Text Input, the one source that does say what it holds — while
    the flow died at run time on the missing column. The refusal names what does arrive instead.
    """
    pearson_row = report_row(pearson, 280)
    assert (pearson_row.status, pearson_row.reason) == ("placeholder", "mapper_refused")
    assert pearson_row.messages == [
        "The Alteryx Pearson Correlation was not converted because the column(s) 'harvested' do not "
        "reach this tool; what arrives is 'depth_m', 'crates', 'kilos', 'orchard'."
    ]

    spearman_row = report_row(spearman, 390)
    assert (spearman_row.status, spearman_row.reason) == ("placeholder", "mapper_refused")
    assert "the column(s) 'harvested' do not reach this tool" in spearman_row.messages[0]
    assert "'ties_a', 'ties_b'" in spearman_row.messages[0]


def test_a_sample_that_is_neither_a_count_nor_a_percentage_is_refused(profile: ConversionResult):
    """The exclusive pair can be false on both sides, and that branch had no test at all."""
    row = report_row(profile, 480)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Field Summary Report was not converted because 'Sample Data' is on and the tool "
        "selects neither a record count nor a percentage, so the sample size cannot be read."
    ]


def test_the_profile_says_what_percent_missing_counts(profile: ConversionResult):
    """An empty string is not a null, and whether Alteryx calls one missing is stated nowhere."""
    row = report_row(profile, 410)
    assert any("'PercentMissing' counts nulls and nothing else" in message for message in row.messages)
    assert any("stated nowhere in the workflow" in message for message in row.messages)


THREE_WIRES_OUT_OF_ORDER = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.JoinMultiple.JoinMultiple" />
      <Properties><Configuration><JoinByRecPos value="True" /></Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="wire" /></Fields>
        <Data><r><c>one</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="wire" /></Fields>
        <Data><r><c>two</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="4"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="wire" /></Fields>
        <Data><r><c>three</c></r></Data></Configuration></Properties></Node>
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


def test_a_multi_input_body_binds_input_df_n_to_the_nth_entry_of_input_ids(tmp_path: Path):
    """`input_df_2` is the second wire Alteryx numbered, not the second one the document writes.

    `test_multi_input_wires_follow_the_numbers_alteryx_wrote_not_document_order` pins the list;
    this pins that the engine hands a three-input body the same list in the same order, which is
    what every N-input mapper (JoinMultiple, Find Replace, Union) is about to be built on.
    """
    result = convert_yxmd(THREE_WIRES_OUT_OF_ORDER, source_name="join_multiple.yxmd")
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    placeholder_id = rows[1].flowfile_node_ids[0]
    assert dumped_nodes(result)[placeholder_id]["input_ids"] == [rows[tool].flowfile_node_ids[0] for tool in (2, 3, 4)]

    # The passthrough body reads only `input_df_1`; this one names all three.
    for node in result.flow_data.nodes:
        if node.id == placeholder_id:
            node.setting_input.polars_code_input.polars_code = (
                "output_df = pl.concat([input_df_1, input_df_2, input_df_3])"
            )
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []
    frame = flow.get_node(placeholder_id).get_resulting_data().data_frame.collect()
    assert frame["wire"].to_list() == ["one", "two", "three"]


SORTED_TARGETS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="Crate" /></Fields>
        <Data><r><c>b</c></r><r><c>a</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration><SortInfo>
        <Field field="Crate" order="Ascending" /></SortInfo></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration><RenameMode>FirstRow</RenameMode>
        <Fields><Field name="Crate" /><Field name="*Unknown" /></Fields>
        <Expression /></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" />
      <Destination ToolID="2" Connection="Input" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" />
      <Destination ToolID="3" Connection="%s" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_the_order_rule_can_be_asked_about_an_anchor_other_than_input():
    """A `Targets`-fed tool used to answer False for every upstream, sorted or not."""
    ctx, _ = emit_tools(parse_yxmd(SORTED_TARGETS % b"Targets"))
    assert mappers._feeds_in_stated_order(ctx, 3, anchor="Targets") is True
    # The default anchor carries no wire at all here, and no wires is not a stated order.
    assert mappers._feeds_in_stated_order(ctx, 3) is False


def test_an_unsorted_targets_stream_still_states_no_order():
    document = SORTED_TARGETS % b"Targets"
    unsorted = document.replace(b'<Field field="Crate" order="Ascending" />', b"")
    ctx, _ = emit_tools(parse_yxmd(unsorted))
    assert mappers._feeds_in_stated_order(ctx, 3, anchor="Targets") is False


def test_the_data_anchors_never_overlap_the_side_input_anchors():
    """`sole_source_connection` takes the first *connected* anchor of the tuple it is given.

    A name in both tuples would let a Dynamic Rename or a Find Replace read its own data stream as
    the lookup table whenever the real `Source` anchor is unwired, which is a wrong answer rather
    than a refusal.
    """
    assert not set(mappers.TARGETS_ANCHORS) & set(mappers.DYNAMIC_RENAME_SOURCE_ANCHORS)


@pytest.fixture()
def rename_affix() -> ConversionResult:
    return convert("dynamic_rename_affix.yxmd")


def test_add_reads_the_type_and_text_alteryx_really_writes(rename_affix: ConversionResult):
    """`<RenameMode>Add</RenameMode>` with `<AddPrefixSuffix><Type>/<Text>`, the corpus's own shape.

    The mode itself used to fall through to "has no Flowfile equivalent", and behind that gate
    the reading looked for `<Prefix>`/`<Suffix>` elements no corpus workflow writes.
    """
    row = report_row(rename_affix, 2)
    assert (row.status, row.reason, row.messages) == ("converted", "converted", [])
    settings = dumped_nodes(rename_affix)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert (settings["rename_mode"], settings["prefix"]) == ("prefix", "Total_")
    assert settings["selected_columns"] == ["Units"]


def test_an_added_suffix_keeps_the_space_alteryx_wrote(rename_affix: ConversionResult):
    """`<Text> (raw)</Text>` — the leading space is part of the name, as in Multi-Field Formula."""
    row = report_row(rename_affix, 3)
    assert row.status == "converted"
    settings = dumped_nodes(rename_affix)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert (settings["rename_mode"], settings["suffix"]) == ("suffix", " (raw)")


def test_remove_becomes_a_formula_that_strips_only_the_names_carrying_the_affix(rename_affix: ConversionResult):
    """Flowfile has prefix and suffix rename modes but no remove, so the rule becomes a formula."""
    row = report_row(rename_affix, 4)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    settings = dumped_nodes(rename_affix)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert settings["rename_mode"] == "formula"
    assert settings["formula"] == (
        'if ends_with([column_name], "Name") then left([column_name], length([column_name]) - 4) '
        "else [column_name] endif"
    )


def test_removing_a_prefix_cuts_from_the_other_end(rename_affix: ConversionResult):
    row = report_row(rename_affix, 5)
    settings = dumped_nodes(rename_affix)[row.flowfile_node_ids[0]]["setting_input"]["dynamic_rename_input"]
    assert settings["formula"] == (
        'if starts_with([column_name], "Total_") then right([column_name], length([column_name]) - 6) '
        "else [column_name] endif"
    )


def test_remove_says_what_it_could_not_verify(rename_affix: ConversionResult):
    """Two unverified points and, on tool 4 only, the on-error setting."""
    assert report_row(rename_affix, 4).messages == [
        "Alteryx removes the suffix here; Flowfile strips it from a column that carries it and leaves "
        "every other column's name alone. What Designer does to a column without the suffix is stated "
        "nowhere in this workflow.",
        "A column whose whole name is 'Name' would be renamed to an empty string, which this workflow "
        "does not say Alteryx allows.",
        "Alteryx's own on-error warning for this rename is not reproduced, and this workflow does not "
        "state which condition it guards; check the renamed columns.",
    ]
    # Tool 5 writes no <OnError>, so it carries only the two sentences the rename itself owes.
    assert len(report_row(rename_affix, 5).messages) == 2


def test_a_mapped_table_pairs_the_names_by_name_not_by_position(rename_affix: ConversionResult):
    """The mode exists because the table's order and the stream's order disagree (corpus box 28)."""
    row = report_row(rename_affix, 7)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "select")
    renames = dumped_nodes(rename_affix)[row.flowfile_node_ids[0]]["setting_input"]["select_input"]
    assert [(entry["old_name"], entry["new_name"]) for entry in renames] == [
        ("CrateName (raw)", "Crate"),
        ("Units", "Boxes"),
    ]
    assert row.messages[0].startswith("The new column names were read from the rows of 'TextInput' (ToolID 6) matched")


def test_a_mapped_table_names_both_the_columns_it_missed_and_the_ones_it_does_not_touch(
    rename_affix: ConversionResult,
):
    """'Team' maps to itself and is not a rename; 'Weight' has no row; 'Ghost' names no column."""
    messages = report_row(rename_affix, 7).messages
    assert "The mapping table names no new name for 'Weight'; those columns keep their names." in messages
    assert "The mapping table also names 'Ghost', which this tool does not rename." in messages


def test_every_affix_mode_runs(rename_affix: ConversionResult, tmp_path: Path):
    """Add prefix, add suffix, remove suffix, remove prefix and the mapped table, in one chain."""
    flow = open_flow(write_flow(rename_affix, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []
    last = report_row(rename_affix, 7).flowfile_node_ids[-1]
    frame = flow.get_node(last).get_resulting_data().data_frame.collect()
    # Four renames in a row leave the arriving columns unknown, so the Select names only what it
    # renames and Flowfile emits those first — which is the caveat the row carries.
    assert frame.columns == ["Crate", "Boxes", "Team", "Weight"]
    assert frame["Boxes"].to_list() == [4, 7]
    messages = report_row(rename_affix, 7).messages
    assert any("check the column order against Alteryx's" in message for message in messages)


MAPPED_WITHOUT_THE_NAMED_COLUMNS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="a" /></Fields>
        <Data><r><c>1</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration><Fields><Field name="New" /></Fields>
        <Data><r><c>b</c></r></Data></Configuration></Properties></Node>
    <Node ToolID="3"><GuiSettings Plugin="AlteryxBasePluginsGui.DynamicRename.DynamicRename" />
      <Properties><Configuration><RenameMode>RightInputRows</RenameMode>
        <Fields><Field name="a" /><Field name="*Unknown" /></Fields><Expression />
        <NamesFromRows><InputMode>Mapped</InputMode><OldName>Old</OldName><NewName>New</NewName>
        </NamesFromRows></Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" Connection="Output" />
      <Destination ToolID="3" Connection="Targets" /></Connection>
    <Connection><Origin ToolID="2" Connection="Output" />
      <Destination ToolID="3" Connection="Source" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_a_mapped_table_missing_the_old_name_column_is_refused_not_read_from_the_first_one():
    """Positional falls back to the first column; Mapped names two, so a fallback would pair wrongly."""
    row = report_row(convert_yxmd(MAPPED_WITHOUT_THE_NAMED_COLUMNS, source_name="rename.yxmd"), 3)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert "the column(s) 'Old' of 'TextInput' (ToolID 2)" in row.messages[0]


def test_an_affix_type_that_is_neither_a_prefix_nor_a_suffix_is_refused():
    document = MAPPED_WITHOUT_THE_NAMED_COLUMNS.replace(
        b"<RenameMode>RightInputRows</RenameMode>", b"<RenameMode>Add</RenameMode>"
    ).replace(
        b"<NamesFromRows><InputMode>Mapped</InputMode><OldName>Old</OldName><NewName>New</NewName>\n        "
        b"</NamesFromRows>",
        b"<AddPrefixSuffix><Type>Infix</Type><Text>x</Text></AddPrefixSuffix>",
    )
    row = report_row(convert_yxmd(document, source_name="rename.yxmd"), 3)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert row.messages == ["Alteryx Dynamic Rename affix type 'infix' is neither a prefix nor a suffix."]


def test_an_on_error_setting_that_is_not_warn_is_refused():
    """'Warn' says the run carries on; nothing in the corpus states what any other value does."""
    document = MAPPED_WITHOUT_THE_NAMED_COLUMNS.replace(
        b"<RenameMode>RightInputRows</RenameMode>", b"<RenameMode>Remove</RenameMode>"
    ).replace(
        b"<NamesFromRows><InputMode>Mapped</InputMode><OldName>Old</OldName><NewName>New</NewName>\n        "
        b"</NamesFromRows>",
        b"<RemovePrefixSuffix><Type>Prefix</Type><Text>x</Text><OnError>Error</OnError></RemovePrefixSuffix>",
    )
    row = report_row(convert_yxmd(document, source_name="rename.yxmd"), 3)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert row.messages == [
        "Alteryx Dynamic Rename on-error setting 'Error' has no Flowfile equivalent; only 'Warn', "
        "which lets the run carry on, can be reproduced."
    ]


def test_an_affix_holding_a_double_quote_is_refused_at_import_not_at_run_time():
    document = MAPPED_WITHOUT_THE_NAMED_COLUMNS.replace(
        b"<RenameMode>RightInputRows</RenameMode>", b"<RenameMode>Remove</RenameMode>"
    ).replace(
        b"<NamesFromRows><InputMode>Mapped</InputMode><OldName>Old</OldName><NewName>New</NewName>\n        "
        b"</NamesFromRows>",
        b"<RemovePrefixSuffix><Type>Suffix</Type><Text>a&quot;b</Text></RemovePrefixSuffix>",
    )
    row = report_row(convert_yxmd(document, source_name="rename.yxmd"), 3)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert "contains a double quote" in row.messages[0]


@pytest.fixture()
def date_time_now() -> ConversionResult:
    return convert("date_time_now.yxmd")


def test_date_time_now_is_a_start_node_holding_one_formatted_row(date_time_now: ConversionResult, tmp_path: Path):
    """`DateTimeNow.yxmd`'s comment box 94: "A single row is returned with the date time data"."""
    row = report_row(date_time_now, 1)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "polars_code")
    node = dumped_nodes(date_time_now)[row.flowfile_node_ids[0]]
    assert node["input_ids"] == []
    assert node["setting_input"]["polars_code_input"]["polars_code"].endswith(
        "output_df = pl.select(pl.lit(datetime.datetime.now()).dt.strftime('%Y-%m-%d')" ".alias('DateTimeNow')).lazy()"
    )

    flow = open_flow(write_flow(date_time_now, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []
    frame = flow.get_node(report_row(date_time_now, 2).flowfile_node_ids[0]).get_resulting_data().data_frame.collect()
    assert frame.columns == ["RunDate"]
    assert frame.height == 1
    assert frame["RunDate"][0] == date.today().strftime("%Y-%m-%d")


def test_date_time_now_says_the_column_name_is_flowfiles_and_the_value_is_the_run_time(
    date_time_now: ConversionResult,
):
    assert report_row(date_time_now, 1).messages == [
        "Alteryx does not record what it calls this tool's one column, so Flowfile names it "
        "'DateTimeNow'; rename it if the rest of the flow expects another name.",
        "The value is the moment the flow runs, formatted as 'yyyy-MM-dd' (strftime '%Y-%m-%d'), "
        "so it changes from run to run.",
    ]


def test_date_time_now_formats_a_time_through_the_same_token_table(date_time_now: ConversionResult):
    code = dumped_nodes(date_time_now)[report_row(date_time_now, 3).flowfile_node_ids[0]]["setting_input"]
    assert "strftime('%H:%M:%S')" in code["polars_code_input"]["polars_code"]


def test_date_time_now_refuses_another_language(date_time_now: ConversionResult):
    row = report_row(date_time_now, 4)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert row.messages == ["This Alteryx Date Time Now tool writes French month and day names, which Flowfile cannot."]


def test_date_time_now_refuses_a_token_the_date_time_tool_would_refuse_too(date_time_now: ConversionResult):
    """One token table for both tools, so a run of letters that is not exactly one token fails closed."""
    row = report_row(date_time_now, 5)
    assert (row.status, row.reason) == ("placeholder", "option_unsupported")
    assert row.messages == [
        "The Alteryx date format 'yyyy-MM-ddTHH:mm' was not converted because "
        "'ddTHH' is not a date token Flowfile converts."
    ]


@pytest.fixture()
def imputation() -> ConversionResult:
    return convert("imputation.yxmd")


def imputation_code(result: ConversionResult, tool_id: int) -> str:
    node = dumped_nodes(result)[report_row(result, tool_id).flowfile_node_ids[0]]
    return node["setting_input"]["polars_code_input"]["polars_code"]


def test_imputation_replaces_nulls_with_the_user_value_in_place(imputation: ConversionResult):
    row = report_row(imputation, 2)
    assert (row.status, row.reason, row.messages) == ("converted", "converted", [])
    code = imputation_code(imputation, 2)
    assert "_fields = ['Units', 'Weight']" in code
    assert "[pl.col(_f).fill_null(pl.lit(0)).alias(_imputed[_f]) for _f in _fields]" in code


def test_the_user_value_is_written_as_the_number_alteryx_means_not_as_the_widgets_decimals(
    imputation: ConversionResult,
):
    """`0.00000` is how the updown widget spells zero; `fill_null(0.0)` would widen an Int64 column."""
    assert "fill_null(pl.lit(0))" in imputation_code(imputation, 2)


def test_imputation_puts_the_values_in_alteryxs_own_separate_field(imputation: ConversionResult):
    """Box 174 names the suffix, so it is not Flowfile's invention — only the position is."""
    assert "_imputed = {'Units': 'Units_ImputedValue', 'Weight': 'Weight_ImputedValue'}" in imputation_code(
        imputation, 3
    )
    assert report_row(imputation, 3).messages == [
        "The imputed values land in 'Units_ImputedValue', 'Weight_ImputedValue' and the original column(s) "
        "are left alone, as Alteryx's own comment box describes; Flowfile appends them after the columns "
        "that arrive."
    ]


def test_the_indicator_is_one_where_the_value_was_imputed(imputation: ConversionResult):
    """Box 177 states the 1/0 meaning; the indicator reads the column before it is replaced."""
    code = imputation_code(imputation, 4)
    assert "[pl.col(_f).is_null().cast(pl.Int64).alias(_indicator[_f]) for _f in _fields]" in code
    assert "'Units_Indicator', 'Weight_Indicator'" in report_row(imputation, 4).messages[0]


def test_imputation_by_mean_names_the_type_it_widens(imputation: ConversionResult):
    row = report_row(imputation, 5)
    assert (row.status, row.reason) == ("converted", "converted")
    assert "fill_null(pl.col(_f).mean())" in imputation_code(imputation, 5)
    assert row.messages == [
        "'Units', 'Weight' arrive as whole numbers and the replacement is the mean, so Flowfile's result "
        "is a Float64 column; what Alteryx does to the column's type here is not stated in this workflow."
    ]


def test_imputation_by_mode_follows_the_summarize_precedent(imputation: ConversionResult):
    """Summarize (tool 111): sort the tied values, take the first, report it as partial."""
    row = report_row(imputation, 6)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "fill_null(pl.col(_f).drop_nulls().mode().sort().first())" in imputation_code(imputation, 6)
    assert row.messages == [mappers.IMPUTATION_MODE_MESSAGE]


def test_the_mode_leaves_the_null_it_is_replacing_out_of_the_count(imputation: ConversionResult):
    """`mode()` counts a null like any other value, and the most frequent value here *is* the null."""
    code = imputation_code(imputation, 6)
    executable = polars_code_parser.get_executable(code, 1)
    frame = pl.LazyFrame({"Units": [4, None, None, 4, 7], "Weight": [1, 1, 2, None, None]})
    assert executable(frame).collect()["Units"].to_list() == [4, 4, 4, 4, 7]


def test_imputation_refuses_a_mean_over_a_column_whose_known_type_is_not_numeric(imputation: ConversionResult):
    row = report_row(imputation, 7)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Imputation was not converted because the field 'Crate' is a String column, "
        "which has no median to compute."
    ]


def test_two_replace_with_radios_on_at_once_is_refused_not_resolved(imputation: ConversionResult):
    row = report_row(imputation, 8)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Imputation was not converted because 2 of the radio buttons 'radio Mean', "
        "'radio Median', 'radio Mode', 'radio User Specified Replace With Value' are on, so the "
        "option cannot be read."
    ]


def test_replacing_a_chosen_value_with_a_statistic_says_which_rows_the_statistic_counted(
    imputation: ConversionResult,
):
    row = report_row(imputation, 9)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    code = imputation_code(imputation, 9)
    assert "pl.when((pl.col(_f) == 4)).then(pl.col(_f).median()).otherwise(pl.col(_f))" in code
    assert "(pl.col(_f) == 4).fill_null(False).cast(pl.Int64)" in code
    assert mappers.IMPUTATION_FROM_VALUE_STATISTIC_MESSAGE.format(value=4) in row.messages


def test_an_indicator_name_alteryx_already_uses_is_numbered_and_reported(imputation: ConversionResult):
    """'Units_Indicator' arrives from the Text Input, so Alteryx's own name is not available."""
    row = report_row(imputation, 10)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert "_indicator = {'Units': 'Units_Indicator_1'}" in imputation_code(imputation, 10)
    assert row.messages[-1] == (
        "Alteryx's own name(s) for 'Units_Indicator_1' were already taken by a column arriving here, "
        "so Flowfile numbered them."
    )


def test_every_imputation_branch_runs(imputation: ConversionResult, tmp_path: Path):
    flow = open_flow(write_flow(imputation, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []

    replaced = flow.get_node(report_row(imputation, 2).flowfile_node_ids[0]).get_resulting_data()
    # Row 2's Units is empty in the Text Input and is the value the tool imputes.
    assert replaced.data_frame.collect()["Units"].to_list() == [4, 0, 8, 4]

    averaged = flow.get_node(report_row(imputation, 5).flowfile_node_ids[0]).get_resulting_data()
    frame = averaged.data_frame.collect()
    assert frame["Units"].to_list() == [4.0, pytest.approx(16 / 3), 8.0, 4.0]

    flagged = flow.get_node(report_row(imputation, 4).flowfile_node_ids[0]).get_resulting_data()
    assert flagged.data_frame.collect()["Units_Indicator"].to_list() == [0, 1, 0, 0]


@pytest.fixture()
def weighted_average() -> ConversionResult:
    return convert("weighted_average.yxmd")


def weighted_average_code(result: ConversionResult, tool_id: int) -> str:
    node = dumped_nodes(result)[report_row(result, tool_id).flowfile_node_ids[0]]
    return node["setting_input"]["polars_code_input"]["polars_code"]


def test_an_ungrouped_weighted_average_is_one_row(weighted_average: ConversionResult):
    row = report_row(weighted_average, 2)
    assert (row.status, row.reason) == ("converted", "converted")
    assert "output_df = input_df.select(" in weighted_average_code(weighted_average, 2)
    assert (
        "((pl.col('UnitCost') * pl.col('Units')).sum() / pl.col('Units').sum()).alias('WeightedAverage')"
        in weighted_average_code(weighted_average, 2)
    )
    assert row.messages == [mappers.WEIGHTED_AVG_ZERO_MESSAGE]


def test_a_grouped_weighted_average_is_one_row_per_group(weighted_average: ConversionResult):
    assert "output_df = input_df.group_by(['Team']).agg(" in weighted_average_code(weighted_average, 3)


def test_weighting_a_column_by_itself_is_refused(weighted_average: ConversionResult):
    row = report_row(weighted_average, 4)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Weighted Average was not converted because the value and the weight are both "
        "'Units', which weights every row by itself."
    ]


def test_weighted_average_refuses_a_column_whose_known_type_is_not_numeric(weighted_average: ConversionResult):
    """Box 94: "Both columns must be numeric data to be available for selection"."""
    row = report_row(weighted_average, 5)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Weighted Average was not converted because 'Crate' is a String column, and "
        "Alteryx's own comment box says the value and the weight must both be numeric."
    ]


def test_an_output_name_that_is_already_a_group_field_is_renamed_and_reported(weighted_average: ConversionResult):
    row = report_row(weighted_average, 6)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    assert ".alias('Team_1')" in weighted_average_code(weighted_average, 6)
    assert row.messages[-1] == (
        "The output name Alteryx wrote was already a group field here, so Flowfile called the column 'Team_1'."
    )


def test_a_group_field_the_stream_does_not_carry_is_refused_not_split(weighted_average: ConversionResult):
    """One `GroupFields` value is one column name; no corpus workflow states how several are spelled."""
    row = report_row(weighted_average, 7)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert "the column(s) 'Depot' do not reach this tool" in row.messages[0]


def test_the_weighted_average_is_the_weighted_average(weighted_average: ConversionResult, tmp_path: Path):
    flow = open_flow(write_flow(weighted_average, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []

    ungrouped = flow.get_node(report_row(weighted_average, 2).flowfile_node_ids[0]).get_resulting_data()
    frame = ungrouped.data_frame.collect()
    # (1*10 + 3*20 + 2*5 + 2*15) / (1 + 3 + 2 + 2) = 110 / 8
    assert frame.to_dicts() == [{"WeightedAverage": 110 / 8}]

    grouped = flow.get_node(report_row(weighted_average, 3).flowfile_node_ids[0]).get_resulting_data()
    by_team = {row["Team"]: row["WeightedAverage"] for row in grouped.data_frame.collect().to_dicts()}
    assert by_team == {"reds": (1 * 10 + 3 * 20) / 4, "blues": (2 * 5 + 2 * 15) / 4}


@pytest.fixture()
def create_samples() -> ConversionResult:
    return convert("create_samples.yxmd")


def test_create_samples_becomes_the_native_random_split(create_samples: ConversionResult):
    """Box 94: the two percentages are named and the rest of the rows are the holdout."""
    row = report_row(create_samples, 2)
    assert (row.status, row.reason, row.flowfile_node_type) == ("partial", "option_unsupported", "random_split")
    settings = dumped_nodes(create_samples)[row.flowfile_node_ids[0]]["setting_input"]
    assert settings["splits"] == [
        {"name": "Estimation", "percentage": 70.0},
        {"name": "Validation", "percentage": 20.0},
        {"name": "Holdout", "percentage": 10.0},
    ]
    assert settings["seed"] == 42
    assert row.messages == [mappers.CREATE_SAMPLES_MEMBERSHIP_MESSAGE]


def test_each_alteryx_anchor_leaves_from_its_own_split(create_samples: ConversionResult):
    """The node's outputs are its splits in order, so `Holdout` must not land on the first handle."""
    nodes = dumped_nodes(create_samples)
    split = nodes[report_row(create_samples, 2).flowfile_node_ids[0]]
    consumers = {handle: node_id for node_id, handle in zip(split["outputs"], split["output_handles"], strict=True)}
    assert consumers == {
        "output-0": report_row(create_samples, 3).flowfile_node_ids[0],
        "output-1": report_row(create_samples, 4).flowfile_node_ids[0],
        "output-2": report_row(create_samples, 5).flowfile_node_ids[0],
    }


def test_a_holdout_of_nothing_is_declared_empty_rather_than_emitted(create_samples: ConversionResult):
    """`NodeRandomSplit` rejects a split of 0, and a wire on that anchor is a dropped connection."""
    row = report_row(create_samples, 6)
    settings = dumped_nodes(create_samples)[row.flowfile_node_ids[0]]["setting_input"]
    assert [split["name"] for split in settings["splits"]] == ["Estimation", "Validation"]
    message = mappers.CREATE_SAMPLES_EMPTY_ANCHOR_MESSAGE.format(anchor="Holdout")
    assert message in row.messages
    consumer = report_row(create_samples, 7)
    assert (consumer.status, consumer.reason) == ("partial", "dropped_connection")
    assert message in consumer.messages


def test_samples_asking_for_more_rows_than_there_are_is_refused(create_samples: ConversionResult):
    row = report_row(create_samples, 8)
    assert (row.status, row.reason) == ("placeholder", "mapper_refused")
    assert row.messages == [
        "The Alteryx Create Samples was not converted because its samples ask for 70% + 40% of the rows, "
        "which is more than there are."
    ]


def test_an_unreadable_percentage_and_two_zeroes_are_both_refused(create_samples: ConversionResult):
    assert report_row(create_samples, 9).messages == [
        "The Alteryx Create Samples was not converted because the estimation pct it writes is not a number."
    ]
    assert report_row(create_samples, 10).messages == [
        "The Alteryx Create Samples was not converted because both sample percentages are zero, "
        "so every sample is empty."
    ]


def test_the_three_samples_hold_every_row_once(create_samples: ConversionResult, tmp_path: Path):
    flow = open_flow(write_flow(create_samples, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []
    frames = [
        flow.get_node(report_row(create_samples, tool).flowfile_node_ids[0]).get_resulting_data().data_frame.collect()
        for tool in (3, 4, 5)
    ]
    assert [frame.height for frame in frames] == [7, 2, 1]
    assert sorted(crate for frame in frames for crate in frame["Crate"].to_list()) == sorted(
        f"r{index}" for index in range(1, 11)
    )


@pytest.fixture()
def generate_rows() -> ConversionResult:
    return convert("generate_rows.yxmd")


def generate_rows_nodes(result: ConversionResult, tool_id: int) -> list[dict]:
    nodes = dumped_nodes(result)
    return [nodes[node_id] for node_id in report_row(result, tool_id).flowfile_node_ids]


def test_a_generate_rows_with_no_input_seeds_its_own_row(generate_rows: ConversionResult):
    """`Generate_Rows.yxmd` tool 151 is wired to nothing, so the chain starts from a one-row frame."""
    row = report_row(generate_rows, 2)
    assert (row.status, row.reason) == ("partial", "option_unsupported")
    seed, start, end, ranges = generate_rows_nodes(generate_rows, 2)
    assert seed["input_ids"] == []
    assert "pl.lit(1).alias('_alteryx_generate_seed')" in seed["setting_input"]["polars_code_input"]["polars_code"]
    assert [node["setting_input"]["function"]["function"] for node in (start, end)] == ["1", "5"]
    assert "pl.int_ranges(" in ranges["setting_input"]["polars_code_input"]["polars_code"]


def test_the_generated_field_is_bracketed_before_it_reaches_the_translator(generate_rows: ConversionResult):
    """`RowCount <= 10` is unbracketed and `RowCount` is an Alteryx function, so it is refused bare."""
    assert mappers._bracket_field("RowCount <= 10", "RowCount") == "[RowCount] <= 10"
    # Not inside a string, not inside an existing bracket, and not when it is the function call.
    assert mappers._bracket_field("'RowCount' + RowCount", "RowCount") == "'RowCount' + [RowCount]"
    assert mappers._bracket_field("[RowCount] + RowCountX", "RowCount") == "[RowCount] + RowCountX"
    assert mappers._bracket_field("RowCount()", "RowCount") == "RowCount()"


def test_a_bound_reading_an_input_column_generates_a_different_number_of_rows_per_row(
    generate_rows: ConversionResult,
):
    start, end, ranges = generate_rows_nodes(generate_rows, 3)
    assert end["setting_input"]["function"]["function"] == "[Seats]"
    code = ranges["setting_input"]["polars_code_input"]["polars_code"]
    assert ".explode('Seat')" in code
    assert "pl.col('_alteryx_generate_end').cast(pl.Int64) + 1" in code


def test_an_exclusive_condition_does_not_add_a_step_of_room(generate_rows: ConversionResult):
    code = generate_rows_nodes(generate_rows, 5)[-1]["setting_input"]["polars_code_input"]["polars_code"]
    assert "pl.int_ranges(pl.col('_alteryx_generate_start').cast(pl.Int64), " in code
    assert "pl.col('_alteryx_generate_end').cast(pl.Int64), 3)" in code


def test_a_date_range_chooses_its_cast_from_the_frame_not_from_a_guess(generate_rows: ConversionResult):
    """Alteryx stores a date as text; whether Flowfile's column is text or a Date is the schema's answer."""
    code = generate_rows_nodes(generate_rows, 4)[-1]["setting_input"]["polars_code_input"]["polars_code"]
    assert "_start.str.strptime(pl.Date) if _schema['_alteryx_generate_start'] == pl.String else _start" in code
    assert "pl.date_ranges(_start, _end, '1d', closed='both')" in code


def test_generate_rows_refuses_every_shape_that_is_not_a_range(generate_rows: ConversionResult):
    refusals = {tool: report_row(generate_rows, tool) for tool in (6, 7, 8, 9, 10)}
    assert {tool: (row.status, row.reason) for tool, row in refusals.items()} == {
        tool: ("placeholder", "mapper_refused") for tool in (6, 7, 8, 9, 10)
    }
    assert refusals[6].messages == [
        "The Alteryx Generate Rows was not converted because its condition '[Back] >= 1' is not 'Back' "
        "compared with '<' or '<=' to a bound."
    ]
    assert refusals[7].messages == [
        "The Alteryx Generate Rows was not converted because its loop expression \"If [Crate]='a' Then "
        "[Odd] + 1 Else [Odd] + 2 EndIf\" is not 'Odd' plus a fixed number."
    ]
    assert refusals[8].messages == [
        "The Alteryx Generate Rows was not converted because it caps the run at 3 records, and this "
        "workflow does not state what Alteryx counts towards that cap."
    ]
    # Box 170 of Generate_Rows.yxmd: `[date]+1` "would not have worked" on a date column.
    assert refusals[9].messages == [
        "The Alteryx Generate Rows was not converted because its loop expression '[Wrong] + 1' is not a "
        "DateTimeAdd of a fixed amount to 'Wrong'."
    ]
    assert refusals[10].messages == [
        "The Alteryx Generate Rows was not converted because the column 'Crate' it creates already arrives here."
    ]


def test_generate_rows_says_it_read_the_record_count_as_no_cap(generate_rows: ConversionResult):
    assert report_row(generate_rows, 2).messages[0] == (
        "Alteryx's record-count cap is not written here, which this workflow does not say the meaning of; "
        "Flowfile reads it as no cap and generates every row the condition allows."
    )


def test_the_generated_rows_are_the_range(generate_rows: ConversionResult, tmp_path: Path):
    flow = open_flow(write_flow(generate_rows, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert [step.node_id for step in run_info.node_step_result if not step.success] == []

    def frame_of(tool_id: int) -> pl.DataFrame:
        return (
            flow.get_node(report_row(generate_rows, tool_id).flowfile_node_ids[-1])
            .get_resulting_data()
            .data_frame.collect()
        )

    standalone = frame_of(2)
    assert standalone.columns == ["RowCount"]
    assert standalone["RowCount"].to_list() == [1, 2, 3, 4, 5]
    assert standalone["RowCount"].dtype == pl.Int32

    per_row = frame_of(3)
    assert per_row["Crate"].to_list() == ["a", "a", "b", "b", "b"]
    assert per_row["Seat"].to_list() == [1, 2, 1, 2, 3]

    dates = frame_of(4)
    assert dates["Day"].to_list() == [
        date(2015, 2, 1),
        date(2015, 2, 2),
        date(2015, 2, 3),
        date(2015, 3, 1),
        date(2015, 3, 2),
    ]

    assert frame_of(5)["Step"].to_list() == [0, 3, 6, 9, 0, 3, 6, 9]


GENERATE_ROWS_WITH_AN_EMPTY_RANGE = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2021.4">
  <Nodes>
    <Node ToolID="960"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" />
      <Properties><Configuration>
        <Fields><Field name="table" /><Field name="Seats" /></Fields>
        <Data><r><c>window</c><c>2</c></r><r><c>bar</c><c>0</c></r></Data>
      </Configuration></Properties></Node>
    <Node ToolID="961"><GuiSettings Plugin="AlteryxBasePluginsGui.GenerateRows.GenerateRows" />
      <Properties><Configuration>
        <UpdateField value="False" />
        <UpdateField_Name />
        <CreateField_Name>Seat</CreateField_Name>
        <CreateField_Type>Int32</CreateField_Type>
        <CreateField_Size>4</CreateField_Size>
        <Expression_Init>1</Expression_Init>
        <Expression_Cond>[Seat] &lt;= [Seats]</Expression_Cond>
        <Expression_Loop>[Seat] + 1</Expression_Loop>
      </Configuration></Properties></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="960" Connection="Output" /><Destination ToolID="961" Connection="Input" /></Connection>
  </Connections>
</AlteryxDocument>
"""


def test_an_input_row_whose_loop_never_runs_is_dropped_not_kept_as_a_null_row(tmp_path: Path):
    """Alteryx emits nothing for a row whose condition is false from the start (the corpus box says
    the customer with zero authorized users is absent from the output); an exploded empty list
    would keep it as one null row."""
    result = convert_yxmd(GENERATE_ROWS_WITH_AN_EMPTY_RANGE, source_name="empty_range.yxmd")
    row = report_row(result, 961)
    assert row.status != "placeholder"
    flow = open_flow(write_flow(result, tmp_path / "flow.yaml"))
    run_info = flow.run_graph()
    assert run_info.success, [step.error for step in run_info.node_step_result if not step.success]
    frame = flow.get_node(row.flowfile_node_ids[-1]).get_resulting_data().data_frame.collect()
    assert frame["table"].to_list() == ["window", "window"]
    assert frame["Seat"].to_list() == [1, 2]
