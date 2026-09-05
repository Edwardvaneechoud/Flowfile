from pathlib import Path

import pytest

from flowfile_core.flowfile.converters.alteryx.yxmd_parser import (
    AlteryxWorkflow,
    YxmdParseError,
    parse_yxmd,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"

MINIMAL_WITHOUT_ANCHORS = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput"><Position x="10" y="20" /></GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
    </Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" /><Destination ToolID="2" /></Connection>
  </Connections>
</AlteryxDocument>
"""

WRONG_ROOT = b"""<?xml version="1.0"?>
<AlteryxMacro yxmdVer="2023.1"><Nodes /></AlteryxMacro>
"""


def read_fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


@pytest.fixture()
def all_supported() -> AlteryxWorkflow:
    return parse_yxmd(read_fixture("all_supported.yxmd"))


@pytest.fixture()
def containers() -> AlteryxWorkflow:
    return parse_yxmd(read_fixture("containers.yxmd"))


def test_workflow_name_from_meta_info(all_supported: AlteryxWorkflow):
    assert all_supported.name == "All Supported Tools"


def test_workflow_name_is_none_when_meta_info_has_no_name():
    workflow = parse_yxmd(read_fixture("formulas.yxmd"))
    assert workflow.name is None


def test_tools_are_returned_in_document_order(all_supported: AlteryxWorkflow):
    assert [tool.tool_id for tool in all_supported.tools] == list(range(1, 14))


def test_tool_name_is_last_dotted_segment_of_plugin(all_supported: AlteryxWorkflow):
    tool_names = {tool.tool_id: tool.tool_name for tool in all_supported.tools}
    assert tool_names == {
        1: "TextInput",
        2: "AlteryxSelect",
        3: "Filter",
        4: "Formula",
        5: "Sort",
        6: "Summarize",
        7: "Sample",
        8: "Unique",
        9: "TextToColumns",
        10: "DbFileOutput",
        11: "TextInput",
        12: "Join",
        13: "Union",
    }


def test_plugin_is_kept_verbatim(all_supported: AlteryxWorkflow):
    tools = {tool.tool_id: tool for tool in all_supported.tools}
    assert tools[3].plugin == "AlteryxBasePluginsGui.Filter.Filter"
    assert tools[12].plugin == "AlteryxBasePluginsGui.Join.Join"


def test_positions_are_parsed(all_supported: AlteryxWorkflow):
    tools = {tool.tool_id: tool for tool in all_supported.tools}
    assert (tools[1].x, tools[1].y) == (54, 54)
    assert (tools[10].x, tools[10].y) == (918, 54)
    assert (tools[13].x, tools[13].y) == (726, 330)


def test_missing_position_yields_none():
    workflow = parse_yxmd(MINIMAL_WITHOUT_ANCHORS)
    tools = {tool.tool_id: tool for tool in workflow.tools}
    assert (tools[1].x, tools[1].y) == (10, 20)
    assert (tools[2].x, tools[2].y) == (None, None)


def test_annotation_text_is_read(all_supported: AlteryxWorkflow):
    tools = {tool.tool_id: tool for tool in all_supported.tools}
    assert tools[3].annotation == "Keep high-value orders"
    assert tools[4].annotation == "Add VAT total and normalise the customer name"
    assert tools[11].annotation == "Customer tiers"


def test_default_annotation_text_is_ignored(all_supported: AlteryxWorkflow):
    tools = {tool.tool_id: tool for tool in all_supported.tools}
    assert tools[5].annotation == ""
    assert tools[7].annotation == ""


def test_configuration_element_is_exposed(all_supported: AlteryxWorkflow):
    tools = {tool.tool_id: tool for tool in all_supported.tools}
    assert tools[3].configuration.findtext("Expression") == "[amount] > 100"
    assert [f.get("name") for f in tools[1].configuration.findall("Fields/Field")] == [
        "id",
        "name",
        "region",
        "amount",
        "tags",
        "notes",
    ]
    assert tools[6].configuration.find("SummarizeFields/SummarizeField").get("action") == "GroupBy"


def test_all_supported_has_no_text_boxes(all_supported: AlteryxWorkflow):
    assert all_supported.text_boxes == []


def test_connections_are_parsed_with_anchors(all_supported: AlteryxWorkflow):
    wires = {
        (c.origin_tool_id, c.origin_anchor, c.dest_tool_id, c.dest_anchor) for c in all_supported.connections
    }
    assert len(all_supported.connections) == 16
    expected = {
        (1, "Output", 2, "Input"),
        (2, "Output", 3, "Input"),
        (3, "True", 4, "Input"),
        (3, "False", 13, "Input"),
        (4, "Output", 5, "Input"),
        (5, "Output", 6, "Input"),
        (5, "Output", 12, "Left"),
        (11, "Output", 12, "Right"),
        (6, "Output", 7, "Input"),
        (7, "Output", 8, "Input"),
        (8, "Unique", 9, "Input"),
        (8, "Dupes", 13, "Input"),
        (9, "Output", 10, "Input"),
        (12, "Join", 13, "Input"),
        (12, "Left", 13, "Input"),
        (12, "Right", 13, "Input"),
    }
    assert wires == expected


def test_missing_connection_attribute_falls_back_to_default_anchors():
    workflow = parse_yxmd(MINIMAL_WITHOUT_ANCHORS)
    connection = workflow.connections[0]
    assert (connection.origin_anchor, connection.dest_anchor) == ("Output", "Input")


def test_containers_are_flattened_recursively(containers: AlteryxWorkflow):
    assert [tool.tool_id for tool in containers.tools] == [1, 2, 3, 5]
    assert [tool.tool_name for tool in containers.tools] == [
        "TextInput",
        "AlteryxSelect",
        "Filter",
        "DbFileOutput",
    ]


def test_container_nodes_are_not_emitted_as_tools(containers: AlteryxWorkflow):
    plugins = [tool.plugin for tool in containers.tools + containers.text_boxes]
    assert not any(plugin.endswith(".ToolContainer") for plugin in plugins)


def test_nested_tool_keeps_its_configuration_and_annotation(containers: AlteryxWorkflow):
    nested_filter = next(tool for tool in containers.tools if tool.tool_id == 3)
    assert nested_filter.configuration.findtext("Expression") == "[amount] > 100"
    assert nested_filter.annotation == "Nested two containers deep"
    assert (nested_filter.x, nested_filter.y) == (318, 78)


def test_text_boxes_are_kept_apart_from_tools_with_their_size(containers: AlteryxWorkflow):
    assert [tool.tool_id for tool in containers.text_boxes] == [4]
    text_box = containers.text_boxes[0]
    assert text_box.tool_name == "TextBox"
    assert text_box.plugin == "AlteryxGuiToolkit.TextBox.TextBox"
    assert (text_box.x, text_box.y, text_box.width, text_box.height) == (450, 78, 120, 60)
    assert all(tool.width is None for tool in containers.tools)


def test_macro_node_has_macro_path_and_empty_tool_name():
    workflow = parse_yxmd(read_fixture("unsupported.yxmd"))
    tools = {tool.tool_id: tool for tool in workflow.tools}
    assert tools[2].tool_name == "DateTime"
    assert tools[3].tool_name == ""
    assert tools[3].plugin == "Something.yxmc"
    assert tools[3].annotation == "Team standard cleanup macro"


def test_invalid_xml_raises():
    with pytest.raises(YxmdParseError, match="not valid XML"):
        parse_yxmd(read_fixture("invalid.xml"))


def test_empty_payload_raises():
    with pytest.raises(YxmdParseError):
        parse_yxmd(b"")


def test_wrong_root_element_raises():
    with pytest.raises(YxmdParseError, match="AlteryxDocument"):
        parse_yxmd(WRONG_ROOT)


def test_workflow_without_tools_raises():
    with pytest.raises(YxmdParseError, match="no Alteryx tools"):
        parse_yxmd(read_fixture("zero_tools.yxmd"))
