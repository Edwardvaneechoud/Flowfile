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


CACHED_SCHEMA = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
      <Properties>
        <Configuration />
        <MetaInfo connection="Output"><RecordInfo>
          <Field name="FirstName" type="String" size="25" />
          <Field name="Visits" type="Byte" />
          <Field name="Untyped" />
        </RecordInfo></MetaInfo>
      </Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" />
      <Properties><Configuration /></Properties>
    </Node>
  </Nodes>
</AlteryxDocument>
"""


def test_output_field_types_are_read_beside_the_names():
    """A field with no `type` keeps its name but contributes no type, so a reader cannot assume one."""
    tools = {tool.tool_id: tool for tool in parse_yxmd(CACHED_SCHEMA).tools}
    assert tools[1].output_fields == ["FirstName", "Visits", "Untyped"]
    assert tools[1].output_field_types == {"FirstName": "String", "Visits": "Byte"}
    assert (tools[2].output_fields, tools[2].output_field_types) == ([], {})


def test_all_supported_has_no_text_boxes(all_supported: AlteryxWorkflow):
    assert all_supported.text_boxes == []


def test_connections_are_parsed_with_anchors(all_supported: AlteryxWorkflow):
    wires = {(c.origin_tool_id, c.origin_anchor, c.dest_tool_id, c.dest_anchor) for c in all_supported.connections}
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
    assert connection.name == ""


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


def test_connection_names_are_kept_in_document_order_by_the_parser():
    # The parser reads the file; putting the wires in Alteryx's order is the converter's job.
    workflow = parse_yxmd(MULTI_INPUT_OUT_OF_ORDER)
    assert [(c.name, c.origin_tool_id) for c in workflow.connections] == [("#3", 4), ("#1", 2), ("#2", 3)]


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
    assert tools[2].tool_name == "XMLParse"
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


def test_workflow_without_tools_or_comments_raises():
    with pytest.raises(YxmdParseError, match="nothing to convert"):
        parse_yxmd(read_fixture("empty_canvas.yxmd"))


def test_comment_only_workflow_parses_with_no_tools():
    workflow = parse_yxmd(read_fixture("zero_tools.yxmd"))
    assert workflow.tools == []
    assert len(workflow.text_boxes) == 1
    assert workflow.text_boxes[0].configuration.findtext("Text") == "Workflow still to be built."


def one_node(tool_id: str, position: str = '<Position x="10" y="20" />') -> bytes:
    return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="{tool_id}">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput">{position}</GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>
""".encode()


@pytest.mark.parametrize("tool_id", ["2.9", "1e400", "-5", "0x10", "1_0", "١٢٣", "", " ", "two"])
def test_a_tool_id_that_is_not_a_whole_number_is_refused_by_name(tool_id: str):
    """`int(float(...))` read `ToolID="2.9"` as tool 2 — a different tool, and usually a real one.

    A ToolID is an identity, so rounding one is not a small error: the node lands on top of another
    tool's id, and every wire naming it follows. The value is quoted back rather than guessed at.
    """
    with pytest.raises(YxmdParseError, match="ToolID is not a whole number"):
        parse_yxmd(one_node(tool_id))


@pytest.mark.parametrize(
    ("end", "wire"),
    [
        ("origin", '<Origin ToolID="2.9" /><Destination ToolID="2" />'),
        ("destination", '<Origin ToolID="1" /><Destination ToolID="2.9" />'),
    ],
)
def test_a_connection_tool_id_that_is_not_a_whole_number_is_refused(end: str, wire: str):
    """A wire is an identity too: the truncated id pointed the wire at whatever tool 2 happened to be."""
    document = MINIMAL_WITHOUT_ANCHORS.replace(b'<Origin ToolID="1" /><Destination ToolID="2" />', wire.encode())
    with pytest.raises(YxmdParseError, match=f"{end} ToolID is not a whole number"):
        parse_yxmd(document)


@pytest.mark.parametrize(("tool_id", "expected"), [("7", 7), (" 7 ", 7), ("007", 7), ("+7", 7)])
def test_the_whole_number_spellings_a_tool_id_may_use(tool_id: str, expected: int):
    workflow = parse_yxmd(one_node(tool_id))
    assert [tool.tool_id for tool in workflow.tools] == [expected]


@pytest.mark.parametrize(
    ("position", "expected"),
    [
        ('<Position x="10" y="20" />', (10, 20)),
        ('<Position x="10.6" y="20.4" />', (10, 20)),
        ('<Position x="-30" y="-40" />', (-30, -40)),
        ('<Position x="1e400" y="nan" />', (None, None)),
        ('<Position x="left" y="up" />', (None, None)),
    ],
    ids=["whole", "decimal", "negative", "unrepresentable", "not_a_number"],
)
def test_a_canvas_coordinate_is_tolerant_where_a_tool_id_is_not(position: str, expected: tuple):
    """A pixel has an obvious nearest answer and a ToolID does not, so only one of them refuses.

    No corpus or fixture workflow writes a non-integer or negative position — this is about what a
    wrong answer costs, not about what Designer writes. A sign survives because `convert.py` shifts
    the canvas by the smallest x and y it sees.
    """
    workflow = parse_yxmd(one_node("1", position))
    assert (workflow.tools[0].x, workflow.tools[0].y) == expected


def test_a_node_without_a_tool_id_is_refused_and_named_by_its_position():
    """Absent is the same claim as unreadable: the parser cannot say which tool this is.

    Dropping the node took every wire naming it with it, silently. No canvas node in the corpus
    lacks a ToolID — the three id-less `<Node>` elements in each of `07 Reporting/Report_Header.yxmd`
    and `Report_Footer.yxmd` are `LayoutFields/Layout` configuration inside a tool's `<Properties>`,
    which `_collect_tools` never visits — so there is nothing real to tolerate.
    """
    document = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" /></Node>
    <Node><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" /></Node>
  </Nodes>
</AlteryxDocument>
"""
    with pytest.raises(YxmdParseError, match=r"ToolID is absent \(the <Node> at document position 1\)"):
        parse_yxmd(document)


def test_the_document_position_counts_every_node_the_parser_visits():
    """Containers and their children are numbered too, so the index names one element exactly.

    632 of the 3610 nodes the parser visits across the corpus live inside `<ChildNodes>`, and every
    one of them carries a ToolID — which is what makes the refusal above measured, not assumed.
    """
    document = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" /></Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxGuiToolkit.ToolContainer.ToolContainer" />
      <ChildNodes>
        <Node><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" /></Node>
      </ChildNodes>
    </Node>
  </Nodes>
</AlteryxDocument>
"""
    with pytest.raises(YxmdParseError, match=r"ToolID is absent \(the <Node> at document position 2\)"):
        parse_yxmd(document)


@pytest.mark.parametrize(
    "container",
    [
        '<Node><GuiSettings Plugin="AlteryxGuiToolkit.ToolContainer.ToolContainer" /></Node>',
        '<Node><GuiSettings Plugin="AlteryxGuiToolkit.ToolContainer.ToolContainer" /><ChildNodes /></Node>',
    ],
    ids=["by_plugin", "by_child_nodes"],
)
def test_a_tool_container_without_a_tool_id_is_refused_like_a_tool(container: str):
    """W6 pre-flight: `_collect_tools` skipped a container before `_tool_id` could look at it.

    An id-less container was therefore dropped in silence while an id-less canvas tool beside it was
    refused — two answers to one question. All 120 containers in the corpus carry a ToolID, so this
    tolerates nothing real either way.
    """
    document = f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" /></Node>
    {container}
  </Nodes>
</AlteryxDocument>
""".encode()
    with pytest.raises(YxmdParseError, match=r"container's ToolID is absent \(the <Node> at document position 1\)"):
        parse_yxmd(document)


@pytest.mark.parametrize(
    ("end", "wire"),
    [
        ("origin", '<Origin /><Destination ToolID="2" />'),
        ("destination", '<Origin ToolID="1" /><Destination />'),
    ],
)
def test_a_connection_end_without_a_tool_id_is_refused(end: str, wire: str):
    """A wire whose end has no name was dropped whole; an id nobody wrote is still an id nobody reads."""
    document = MINIMAL_WITHOUT_ANCHORS.replace(b'<Origin ToolID="1" /><Destination ToolID="2" />', wire.encode())
    with pytest.raises(YxmdParseError, match=f"{end} ToolID is absent"):
        parse_yxmd(document)


def test_a_connection_end_refusal_names_which_connection_it_was():
    """The message said which end was absent but not which wire, so a file with many said nothing.

    A node's refusal has carried its document position since W5.12; a wire's did not, and a
    `<Connection>` has no other name to be called by.
    """
    document = b"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1"><GuiSettings Plugin="AlteryxBasePluginsGui.TextInput.TextInput" /></Node>
    <Node ToolID="2"><GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort" /></Node>
  </Nodes>
  <Connections>
    <Connection><Origin ToolID="1" /><Destination ToolID="2" /></Connection>
    <Connection><Origin ToolID="1" /><Destination /></Connection>
  </Connections>
</AlteryxDocument>
"""
    with pytest.raises(
        YxmdParseError,
        match=r"destination ToolID is absent \(the <Connection> at document position 1\)",
    ):
        parse_yxmd(document)
