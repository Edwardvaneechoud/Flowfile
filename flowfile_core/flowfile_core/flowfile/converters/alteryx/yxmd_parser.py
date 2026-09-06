"""Parser for Alteryx `.yxmd` workflow XML.

Stdlib ElementTree only: the payload is untrusted user upload, and tool
configurations are handed to the mappers as raw elements.
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

CONTAINER_SUFFIX = ".ToolContainer"
TEXT_BOX_SUFFIX = ".TextBox"
DEFAULT_ORIGIN_ANCHOR = "Output"
DEFAULT_DESTINATION_ANCHOR = "Input"


class YxmdParseError(ValueError):
    """Raised when the uploaded bytes are not a usable Alteryx workflow."""


@dataclass
class AlteryxTool:
    """One Alteryx tool, flattened out of any containers it lived in."""

    tool_id: int
    plugin: str
    tool_name: str
    x: int | None = None
    y: int | None = None
    # Only text boxes carry a size; tools are drawn at a fixed footprint.
    width: int | None = None
    height: int | None = None
    configuration: ET.Element | None = None
    annotation: str = ""
    default_annotation: str = ""
    # Alteryx's cached output schema for the tool; the only column names a reader can offer.
    output_fields: list[str] = field(default_factory=list)


@dataclass
class AlteryxConnection:
    """One wire between two tools, keyed by the anchor names on both ends."""

    origin_tool_id: int
    origin_anchor: str
    dest_tool_id: int
    dest_anchor: str


@dataclass
class AlteryxWorkflow:
    """Parsed workflow: convertible tools, wires, and the canvas text boxes (Comment tools)."""

    name: str | None
    tools: list[AlteryxTool]
    connections: list[AlteryxConnection]
    text_boxes: list[AlteryxTool] = field(default_factory=list)


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _read_annotation(node: ET.Element, tag: str) -> str:
    element = node.find(f"Properties/Annotation/{tag}")
    if element is None or element.text is None:
        return ""
    return element.text.strip()


def _read_output_fields(node: ET.Element) -> list[str]:
    holder = node.find("Properties/MetaInfo[@connection='Output']/RecordInfo")
    if holder is None:
        holder = node.find("Properties/MetaInfo/RecordInfo")
    if holder is None:
        return []
    return [name for element in holder.findall("Field") if (name := element.get("name"))]


def _build_tool(node: ET.Element, gui_settings: ET.Element | None, plugin: str) -> AlteryxTool | None:
    tool_id = _parse_int(node.get("ToolID"))
    if tool_id is None:
        return None
    if plugin:
        tool_name = plugin.rsplit(".", 1)[-1]
    else:
        # Macro nodes carry no Plugin; the macro path is the only identity they have.
        engine_settings = node.find("EngineSettings")
        plugin = (engine_settings.get("Macro") or "") if engine_settings is not None else ""
        tool_name = ""
    position = gui_settings.find("Position") if gui_settings is not None else None
    return AlteryxTool(
        tool_id=tool_id,
        plugin=plugin,
        tool_name=tool_name,
        x=_parse_int(position.get("x")) if position is not None else None,
        y=_parse_int(position.get("y")) if position is not None else None,
        width=_parse_int(position.get("width")) if position is not None else None,
        height=_parse_int(position.get("height")) if position is not None else None,
        configuration=node.find("Properties/Configuration"),
        annotation=_read_annotation(node, "AnnotationText"),
        default_annotation=_read_annotation(node, "DefaultAnnotationText"),
        output_fields=_read_output_fields(node),
    )


def _collect_tools(parent: ET.Element, tools: list[AlteryxTool], text_boxes: list[AlteryxTool]) -> None:
    for node in parent.findall("Node"):
        gui_settings = node.find("GuiSettings")
        plugin = gui_settings.get("Plugin", "") if gui_settings is not None else ""
        child_nodes = node.find("ChildNodes")
        if child_nodes is not None or plugin.endswith(CONTAINER_SUFFIX):
            if child_nodes is not None:
                _collect_tools(child_nodes, tools, text_boxes)
            continue
        tool = _build_tool(node, gui_settings, plugin)
        if tool is None:
            continue
        if plugin.endswith(TEXT_BOX_SUFFIX):
            text_boxes.append(tool)
        else:
            tools.append(tool)


def _parse_connections(root: ET.Element) -> list[AlteryxConnection]:
    connections: list[AlteryxConnection] = []
    holder = root.find("Connections")
    if holder is None:
        return connections
    for connection in holder.findall("Connection"):
        origin = connection.find("Origin")
        destination = connection.find("Destination")
        if origin is None or destination is None:
            continue
        origin_id = _parse_int(origin.get("ToolID"))
        dest_id = _parse_int(destination.get("ToolID"))
        if origin_id is None or dest_id is None:
            continue
        connections.append(
            AlteryxConnection(
                origin_tool_id=origin_id,
                origin_anchor=origin.get("Connection") or DEFAULT_ORIGIN_ANCHOR,
                dest_tool_id=dest_id,
                dest_anchor=destination.get("Connection") or DEFAULT_DESTINATION_ANCHOR,
            )
        )
    return connections


def _workflow_name(root: ET.Element) -> str | None:
    element = root.find("Properties/MetaInfo/Name")
    if element is None or element.text is None:
        return None
    return element.text.strip() or None


def parse_yxmd(data: bytes) -> AlteryxWorkflow:
    """Parse Alteryx workflow bytes into tools and connections, flattening containers."""
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise YxmdParseError(f"The file is not valid XML: {exc}") from exc
    if root.tag != "AlteryxDocument":
        raise YxmdParseError(f"Root element is <{root.tag}>, expected <AlteryxDocument>; this is not a .yxmd workflow.")

    tools: list[AlteryxTool] = []
    text_boxes: list[AlteryxTool] = []
    nodes_element = root.find("Nodes")
    if nodes_element is not None:
        _collect_tools(nodes_element, tools, text_boxes)
    if not tools:
        raise YxmdParseError("The workflow contains no Alteryx tools to convert.")

    return AlteryxWorkflow(
        name=_workflow_name(root),
        tools=tools,
        connections=_parse_connections(root),
        text_boxes=text_boxes,
    )
