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
    # The same cached schema's Alteryx type per column, for the mappers that generate typed code.
    output_field_types: dict[str, str] = field(default_factory=dict)


@dataclass
class AlteryxConnection:
    """One wire between two tools, keyed by the anchor names on both ends.

    ``name`` is Alteryx's own label for the wire, written as ``#1``/``#2``/``#3`` on the wires
    into a multi-input tool. It is the order those inputs are consumed in, which document order
    is not, so it is the only record of a Union's or a multi-input macro's input order.
    """

    origin_tool_id: int
    origin_anchor: str
    dest_tool_id: int
    dest_anchor: str
    name: str = ""


@dataclass
class AlteryxWorkflow:
    """Parsed workflow: convertible tools, wires, and the canvas text boxes (Comment tools)."""

    name: str | None
    tools: list[AlteryxTool]
    connections: list[AlteryxConnection]
    text_boxes: list[AlteryxTool] = field(default_factory=list)


def _parse_coordinate(value: str | None) -> int | None:
    """A canvas coordinate, truncated toward zero; a decimal here is a pixel, not an identity.

    No corpus or fixture workflow writes a non-integer position, so the float is tolerance rather
    than need — but a coordinate that is off by less than a pixel has an obvious answer, where a
    ToolID that is off by one names a different tool. A sign is kept: `convert.py` shifts the canvas
    by the smallest x and y it sees, so a tool left of the origin is meaningful.
    """
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, OverflowError):
        # `1e400` is a float infinity, and an unhandled OverflowError here 500s the whole upload.
        return None


def _tool_id(value: str | None, where: str, at: str = "") -> int:
    """A ToolID as a whole number, or a refusal; ``at`` locates the element in document order.

    A ToolID is an identity, so there is no such thing as a close answer: ``int(float("2.9"))`` is
    ``2``, which is a different tool and usually an existing one, so the node or the wire lands
    silently somewhere else. No float is built, which is also why ``1e400`` cannot raise
    ``OverflowError`` out of the parse. Ascii digits, optionally behind a ``+``; a minus sign and
    every other spelling are refused, because Alteryx numbers tools from one and anything else is a
    claim about identity this parser will not guess at.

    An absent attribute is refused on the same ground, at a node, at a tool container and at both
    ends of a wire: an identity the parser cannot read is the same claim however it fails to read
    it, and dropping the node silently took every wire naming it along with it. A container used to
    be the exception — ``_collect_tools`` skipped it before this ran — so an id-less one vanished
    while an id-less canvas tool was refused.

    Every one of the 3610 ``<Node>`` elements this parser visits across the 137 corpus files carries
    a ToolID: the 632 inside ``<ChildNodes>`` and the 120 containers included, so a node without one
    is not a shape Designer writes. The six ``<Node>`` elements in the corpus that have none (three
    in each of `07 Reporting/Report_Header.yxmd` and `Report_Footer.yxmd`) are inside a tool's own
    ``Properties/Configuration/LayoutFields/Layout``, which ``_collect_tools`` never descends into.
    """
    if value is None:
        raise YxmdParseError(f"{where} is absent{at}.")
    stripped = value.strip()
    digits = stripped[1:] if stripped[:1] == "+" else stripped
    if not (digits.isascii() and digits.isdigit()):
        raise YxmdParseError(f"{where} is not a whole number{at}: {value!r}.")
    return int(digits)


def _read_annotation(node: ET.Element, tag: str) -> str:
    element = node.find(f"Properties/Annotation/{tag}")
    if element is None or element.text is None:
        return ""
    return element.text.strip()


def _record_info(node: ET.Element) -> ET.Element | None:
    holder = node.find("Properties/MetaInfo[@connection='Output']/RecordInfo")
    return holder if holder is not None else node.find("Properties/MetaInfo/RecordInfo")


def _read_output_fields(node: ET.Element) -> list[str]:
    holder = _record_info(node)
    if holder is None:
        return []
    return [name for element in holder.findall("Field") if (name := element.get("name"))]


def _read_output_field_types(node: ET.Element) -> dict[str, str]:
    """The cached schema's Alteryx type per column; a field without a ``type`` is simply absent."""
    holder = _record_info(node)
    if holder is None:
        return {}
    return {
        name: alteryx_type
        for element in holder.findall("Field")
        if (name := element.get("name")) and (alteryx_type := element.get("type"))
    }


def _build_tool(node: ET.Element, gui_settings: ET.Element | None, plugin: str, at: str) -> AlteryxTool:
    tool_id = _tool_id(node.get("ToolID"), "A tool's ToolID", at)
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
        x=_parse_coordinate(position.get("x")) if position is not None else None,
        y=_parse_coordinate(position.get("y")) if position is not None else None,
        width=_parse_coordinate(position.get("width")) if position is not None else None,
        height=_parse_coordinate(position.get("height")) if position is not None else None,
        configuration=node.find("Properties/Configuration"),
        annotation=_read_annotation(node, "AnnotationText"),
        default_annotation=_read_annotation(node, "DefaultAnnotationText"),
        output_fields=_read_output_fields(node),
        output_field_types=_read_output_field_types(node),
    )


def _collect_tools(
    parent: ET.Element, tools: list[AlteryxTool], text_boxes: list[AlteryxTool], position: int = 0
) -> int:
    """Flatten every ``<Node>`` under ``parent``, returning the document position reached.

    ``position`` counts the ``<Node>`` elements this parser visits, containers included, in the
    order the document writes them — the only name a node whose ToolID cannot be read still has.
    """
    for node in parent.findall("Node"):
        at = f" (the <Node> at document position {position})"
        position += 1
        gui_settings = node.find("GuiSettings")
        plugin = gui_settings.get("Plugin", "") if gui_settings is not None else ""
        child_nodes = node.find("ChildNodes")
        if child_nodes is not None or plugin.endswith(CONTAINER_SUFFIX):
            # A container carries no tool of its own but is still a node a wire can name, and it is
            # refused for an unreadable ToolID on the same ground as a canvas tool: dropping it
            # silently is how the id-less case became invisible everywhere except a canvas tool.
            _tool_id(node.get("ToolID"), "A tool container's ToolID", at)
            if child_nodes is not None:
                position = _collect_tools(child_nodes, tools, text_boxes, position)
            continue
        tool = _build_tool(node, gui_settings, plugin, at)
        if plugin.endswith(TEXT_BOX_SUFFIX):
            text_boxes.append(tool)
        else:
            tools.append(tool)
    return position


def _parse_connections(root: ET.Element) -> list[AlteryxConnection]:
    connections: list[AlteryxConnection] = []
    holder = root.find("Connections")
    if holder is None:
        return connections
    for position, connection in enumerate(holder.findall("Connection")):
        at = f" (the <Connection> at document position {position})"
        origin = connection.find("Origin")
        destination = connection.find("Destination")
        if origin is None or destination is None:
            continue
        origin_id = _tool_id(origin.get("ToolID"), "A connection's origin ToolID", at)
        dest_id = _tool_id(destination.get("ToolID"), "A connection's destination ToolID", at)
        connections.append(
            AlteryxConnection(
                origin_tool_id=origin_id,
                origin_anchor=origin.get("Connection") or DEFAULT_ORIGIN_ANCHOR,
                dest_tool_id=dest_id,
                dest_anchor=destination.get("Connection") or DEFAULT_DESTINATION_ANCHOR,
                name=connection.get("name") or "",
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
    if not tools and not text_boxes:
        raise YxmdParseError("The workflow contains nothing to convert: no Alteryx tools and no comments.")

    return AlteryxWorkflow(
        name=_workflow_name(root),
        tools=tools,
        connections=_parse_connections(root),
        text_boxes=text_boxes,
    )
