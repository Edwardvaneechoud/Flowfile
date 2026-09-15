"""Per-tool mappers turning parsed Alteryx tools into Flowfile nodes.

Every mapper emits one or more :class:`schemas.FlowfileNode` objects through the shared
``EmitContext`` and returns the report row describing what happened. Anchors are registered
in the context so the generic wiring pass in ``convert.py`` can translate Alteryx wires into
Flowfile edges without knowing anything about individual tools.
"""

from __future__ import annotations

import copy
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field

from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx.expression import TranslationOutcome, try_translate
from flowfile_core.flowfile.converters.alteryx.report import ToolEntity, ToolReportRow, ToolStatus
from flowfile_core.flowfile.converters.alteryx.scope import ScopeVerdict, census_tool_name, classify
from flowfile_core.flowfile.converters.alteryx.tool_identity import is_official, tool_key
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import AlteryxConnection, AlteryxTool
from flowfile_core.schemas import input_schema, schemas, transform_schema

MAIN = "main"
RIGHT = "right"
PASS_HANDLE = "output-0"
FAIL_HANDLE = "output-1"
DEFAULT_INPUT_ANCHOR = "Input"
DEFAULT_OUTPUT_ANCHOR = "Output"
WARNING_PREFIX = "⚠ "

CONFIG_COMMENT_MAX_LINES = 80
CONFIG_COMMENT_LINE_LIMIT = 300

# Alteryx canvas units are a third of Flowfile's, and a comment has a floor Flowfile can show.
POS_SCALE = 3.0
COMMENT_MIN_WIDTH = 120
COMMENT_MIN_HEIGHT = 40

FORMULA_STEP_DX = 150
FORMULA_STEP_DY = 110
ANTI_DX = 180
ANTI_DY = 160

# Byte widens to Int16, not UInt8, because Int16 is a type the select node's UI offers.
_ALTERYX_TYPE_MAP: dict[str, str] = {
    "bool": "Boolean",
    "byte": "Int16",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "fixeddecimal": "Float64",
    "float": "Float32",
    "double": "Float64",
    "string": "String",
    "v_string": "String",
    "wstring": "String",
    "v_wstring": "String",
    "date": "Date",
    "datetime": "Datetime",
    "time": "Time",
}

_SUMMARIZE_ACTIONS: dict[str, str] = {
    "groupby": "groupby",
    "sum": "sum",
    "min": "min",
    "max": "max",
    "avg": "mean",
    "mean": "mean",
    "median": "median",
    "count": "count",
    "countdistinct": "n_unique",
    "concat": "concat",
    "concatenate": "concat",
    "stddev": "std",
    "first": "first",
    "last": "last",
}

_READ_FILE_TYPES: dict[str, str] = {
    "csv": "csv",
    "txt": "csv",
    "tsv": "csv",
    "json": "json",
    "ndjson": "ndjson",
    "parquet": "parquet",
    "xlsx": "excel",
    "xlsm": "excel",
    "xls": "excel",
    "ipc": "ipc",
    "feather": "ipc",
    "arrow": "ipc",
    "avro": "avro",
}

_WRITE_FILE_TYPES: dict[str, str] = {
    "csv": "csv",
    "txt": "csv",
    "tsv": "csv",
    "ndjson": "ndjson",
    "parquet": "parquet",
    "xlsx": "excel",
    "xlsm": "excel",
    "ipc": "ipc",
    "feather": "ipc",
    "arrow": "ipc",
    "avro": "avro",
}

_OUTPUT_TABLE_SETTINGS: dict[str, type] = {
    "csv": input_schema.OutputCsvTable,
    "parquet": input_schema.OutputParquetTable,
    "excel": input_schema.OutputExcelTable,
    "ipc": input_schema.OutputIpcTable,
    "ndjson": input_schema.OutputNdjsonTable,
    "avro": input_schema.OutputAvroTable,
}


@dataclass
class _PlaceholderBody:
    """What a passthrough body was written from, so it can be written again for the real edges.

    ``notes`` is a copy so that a re-render reproduces the body the node was first given: a mapper
    is free to keep appending to the list it passed, and the rewrite must not pick those up.
    """

    tool: AlteryxTool
    notes: list[str]
    header: str | None
    emitted_inputs: int


@dataclass
class EmitContext:
    """Shared state every mapper writes into while emitting nodes."""

    flow_id: int = 1
    positions: dict[int, tuple[int, int]] = field(default_factory=dict)
    inbound: dict[int, list[AlteryxConnection]] = field(default_factory=dict)
    outbound: dict[int, list[AlteryxConnection]] = field(default_factory=dict)
    nodes: list[schemas.FlowfileNode] = field(default_factory=list)
    # (alteryx tool id, anchor) -> (flowfile node id, output handle)
    output_map: dict[tuple[int, str], tuple[int, str]] = field(default_factory=dict)
    # (alteryx tool id, anchor) -> [(flowfile node id, "main"|"right")]
    input_map: dict[tuple[int, str], list[tuple[int, str]]] = field(default_factory=dict)
    # (tool id, output anchor) -> (tool id, output anchor) that really produces it, for a tool
    # that emits no node. Pairs, never node ids, so the per-anchor handles in output_map hold.
    output_aliases: dict[tuple[int, str], tuple[int, str]] = field(default_factory=dict)
    # (tool id, output anchor) -> why that anchor carries no data, for the consumer's message
    inactive_outputs: dict[tuple[int, str], str] = field(default_factory=dict)
    # best-effort column tracker: tool id -> columns leaving that tool (None = unknown)
    tool_columns: dict[int, list[str] | None] = field(default_factory=dict)
    # (tool id, output anchor) -> columns leaving *that anchor*, when they are not the tool's own.
    # A Join's L and R anchors carry one input through untouched, not the join's projection.
    anchor_columns: dict[tuple[int, str], list[str] | None] = field(default_factory=dict)
    # every parsed tool by id, so a mapper can read the tool feeding one of its anchors
    tools: dict[int, AlteryxTool] = field(default_factory=dict)
    # (dest tool id, dest anchor) pairs a mapper resolved at convert time and does not want wired
    suppressed_inputs: set[tuple[int, str]] = field(default_factory=set)
    # flowfile node id -> the passthrough body it was given, rewritten once wiring has settled.
    # Keyed by node, not by tool: one tool can emit several placeholders, each with its own arity.
    placeholder_bodies: dict[int, _PlaceholderBody] = field(default_factory=dict)
    # the wires a tool that emits no node really hands onward, by the identity of the connection
    # object. Not by value: two wires can agree on both ends and both anchors, and the parser's
    # dataclass compares by value. `AlteryxWorkflow.connections` holds every object for the whole
    # conversion, so an id cannot be recycled underneath this.
    consumed_connections: set[int] = field(default_factory=set)
    # canvas comments a mapper made; `convert_yxmd` adds them to the ones the text boxes made
    comments: list[schemas.FlowfileComment] = field(default_factory=list)
    next_node_id: int = 0
    next_comment_id: int = 0

    def new_node_id(self) -> int:
        self.next_node_id += 1
        return self.next_node_id

    def new_comment_id(self) -> int:
        """The next comment id. Comments are keyed by id on restore, so a clash drops one silently."""
        self.next_comment_id += 1
        return self.next_comment_id

    def position(self, tool: AlteryxTool, dx: int = 0, dy: int = 0) -> tuple[int, int]:
        x, y = self.positions.get(tool.tool_id, (60, 100))
        return x + dx, y + dy

    def add_node(
        self,
        tool: AlteryxTool,
        node_type: str,
        settings,
        *,
        dx: int = 0,
        dy: int = 0,
        description: str = "",
        is_start_node: bool = False,
    ) -> int:
        x, y = self.position(tool, dx, dy)
        self.nodes.append(
            schemas.FlowfileNode(
                id=settings.node_id,
                type=node_type,
                is_start_node=is_start_node,
                description=description,
                x_position=x,
                y_position=y,
                input_ids=[],
                outputs=[],
                output_handles=[],
                setting_input=settings,
            )
        )
        return settings.node_id

    def register_output(self, tool_id: int, anchor: str, node_id: int, handle: str = PASS_HANDLE) -> None:
        self.output_map[(tool_id, anchor)] = (node_id, handle)

    def register_input(self, tool_id: int, anchor: str, node_id: int, kind: str = MAIN) -> None:
        self.input_map.setdefault((tool_id, anchor), []).append((node_id, kind))

    def register_all_outputs(self, tool_id: int, node_id: int) -> None:
        """Point every anchor the workflow actually uses at one node (placeholders)."""
        self.register_output(tool_id, DEFAULT_OUTPUT_ANCHOR, node_id)
        for connection in self.outbound.get(tool_id, []):
            self.register_output(tool_id, connection.origin_anchor, node_id)

    def register_all_inputs(self, tool_id: int, node_id: int, kind: str = MAIN) -> None:
        self.register_input(tool_id, DEFAULT_INPUT_ANCHOR, node_id, kind)
        for connection in self.inbound.get(tool_id, []):
            if connection.dest_anchor != DEFAULT_INPUT_ANCHOR:
                self.register_input(tool_id, connection.dest_anchor, node_id, kind)

    def input_count(self, tool_id: int) -> int:
        return len(self.inbound.get(tool_id, []))

    def alias_output(self, tool_id: int, anchor: str, source_tool_id: int, source_anchor: str) -> None:
        """Point one output anchor at the anchor that really produces its data; no node is emitted."""
        self.output_aliases[(tool_id, anchor)] = (source_tool_id, source_anchor)

    def resolve_output(self, tool_id: int, anchor: str) -> tuple[int, str] | None:
        """The (tool, anchor) whose Flowfile node really produces the data on this anchor.

        A tool that does nothing to the data emits no node and aliases its output anchors onto
        whatever fed it, so its consumers have to be pointed one hop further back — or several,
        for a chain of them. The pair comes back unchanged when nothing aliases it, and an anchor
        that has emitted a node ends the walk. ``None`` means the aliases form a cycle, which no
        Alteryx canvas produces but a hand-written file could.
        """
        seen: set[tuple[int, str]] = set()
        key = (tool_id, anchor)
        while key not in self.output_map:
            if key in seen:
                return None
            seen.add(key)
            if key in self.output_aliases:
                key = self.output_aliases[key]
                continue
            # Mapping runs in document order, so the no-op feeding this anchor may not have
            # registered its alias yet; its configuration says the same thing that alias will.
            connection = _no_op_source(self, *key)
            if connection is None:
                return key
            key = (connection.origin_tool_id, connection.origin_anchor)
        return key

    def resolved_source(self, tool_id: int, anchors: tuple[str, ...]) -> tuple[int, str] | None:
        """The (tool, anchor) really producing the data on the first of ``anchors`` that is wired."""
        connection = self.source_connection(tool_id, anchors)
        if connection is None:
            return None
        return self.resolve_output(connection.origin_tool_id, connection.origin_anchor)

    def has_outgoing(self, tool_id: int, anchor: str) -> bool:
        return any(connection.origin_anchor == anchor for connection in self.outbound.get(tool_id, []))

    def input_columns(self, tool_id: int, anchor: str = DEFAULT_INPUT_ANCHOR) -> list[str] | None:
        """Columns arriving on one anchor, when they are confidently known."""
        for connection in self.inbound.get(tool_id, []):
            if connection.dest_anchor == anchor:
                key = self.resolve_output(connection.origin_tool_id, connection.origin_anchor)
                if key is None:
                    return None
                if key in self.anchor_columns:
                    return self.anchor_columns[key]
                return self.tool_columns.get(key[0])
        return None

    def source_connection(self, tool_id: int, anchors: tuple[str, ...]) -> AlteryxConnection | None:
        """The wire into the first of ``anchors`` that is actually connected."""
        for anchor in anchors:
            for connection in self.inbound.get(tool_id, []):
                if connection.dest_anchor == anchor:
                    return connection
        return None

    def suppress_input(self, tool_id: int, anchors: tuple[str, ...]) -> None:
        """Mark anchors this mapper resolved at convert time so wiring skips them silently."""
        self.suppressed_inputs.update((tool_id, anchor) for anchor in anchors)

    def consume(self, *connections: AlteryxConnection) -> None:
        """Record wires a tool with no node carries onward, so wiring does not report them dropped."""
        self.consumed_connections.update(id(connection) for connection in connections)

    def carries(self, connection: AlteryxConnection) -> bool:
        return id(connection) in self.consumed_connections


ToolMapper = Callable[[AlteryxTool, EmitContext], ToolReportRow]


def tool_label(tool: AlteryxTool) -> str:
    """Human-readable identity: the tool name, or the macro filename for macros."""
    return tool.tool_name or tool.plugin or "Unknown"


def comment_text(text_box: AlteryxTool) -> str:
    """The text of an Alteryx Comment tool, with line structure kept and edges trimmed."""
    raw = _text(text_box.configuration, "Text")
    return "\n".join(line.strip() for line in raw.splitlines()).strip()


def _row(
    tool: AlteryxTool,
    status: ToolStatus,
    node_ids: list[int],
    node_type: str | None,
    messages: list[str] | None = None,
    *,
    reason: str,
    entity: ToolEntity = "tool",
) -> ToolReportRow:
    """One report row. ``reason`` is required: every status has a cause worth naming.

    *entity* is ``annotation`` for a tool that documents the canvas rather than touching data,
    which keeps it out of both coverage percentages the way a Comment tool already is.
    """
    key = tool_key(tool.plugin)
    return ToolReportRow(
        alteryx_tool_id=tool.tool_id,
        alteryx_tool=tool_label(tool),
        census_name=census_tool_name(tool),
        entity=entity,
        alteryx_tool_key=key,
        flowfile_node_ids=node_ids,
        flowfile_node_type=node_type,
        status=status,
        reason=reason,
        requestable=is_official(key),
        messages=messages or [],
    )


def _config(tool: AlteryxTool) -> ET.Element:
    return tool.configuration if tool.configuration is not None else ET.Element("Configuration")


def _text(element: ET.Element | None, path: str, default: str = "") -> str:
    if element is None:
        return default
    found = element.find(path)
    if found is None or found.text is None:
        return default
    return found.text.strip()


def _raw_text(element: ET.Element | None, path: str, default: str = "") -> str:
    """The element's text exactly as written; unlike `_text` it keeps significant whitespace."""
    if element is None:
        return default
    found = element.find(path)
    if found is None or found.text is None:
        return default
    return found.text


def _is_true(value: str | None) -> bool:
    return (value or "").strip().lower() == "true"


def _attribute(element: ET.Element | None, path: str, name: str, default: str = "") -> str:
    if element is None:
        return default
    found = element.find(path)
    if found is None:
        return default
    return (found.get(name) or default).strip()


def _flag(element: ET.Element | None, path: str) -> bool | None:
    """Read an Alteryx boolean written either as a ``value`` attribute or as element text.

    Alteryx serializes the same option both ways depending on the tool, so both shapes have to
    be accepted. The attribute wins when present because ``<X value="True" />`` is self-closing
    and therefore carries no text to contradict it. ``None`` means the option was not written at
    all (or was written empty), which leaves the reader's own default in place.
    """
    if element is None:
        return None
    found = element.find(path)
    if found is None:
        return None
    raw = found.get("value")
    if raw is None:
        raw = found.text
    return _is_true(raw) if (raw or "").strip() else None


def _description(tool: AlteryxTool, warning: str = "") -> str:
    parts = [part for part in (warning, tool.annotation) if part]
    return " — ".join(parts)


_COMMENT_BREAK_RE = re.compile(r"[\r\n\x0b\x0c\x1c-\x1e\x85  ]")


def _backslash_refusal(values: dict[str, str | list[str]]) -> str | None:
    """Why a name or value cannot be embedded in generated code, or None when all of them can.

    The code node strips comments before running, and its scanner reads any quote preceded by a
    backslash as escaped. ``repr()`` doubles a trailing backslash, so a value *ending* in one puts
    a backslash immediately before the closing quote: the scanner never leaves the string, and the
    rest of that line is misread — a later quote toggles wrongly and the line truncates into a
    SyntaxError when the flow runs. A backslash anywhere else is harmless, which is why this is not
    a "contains" check: regex patterns are full of ``\\d`` and ``\\s`` and convert correctly.
    """
    offenders = [
        f"{label} '{_one_line(value)}'"
        for label, item in values.items()
        for value in ([item] if isinstance(item, str) else item)
        if value.endswith("\\")
    ]
    if not offenders:
        return None
    return (
        f"Flowfile could not generate code for this tool: {', '.join(offenders)} ends with a backslash, "
        "which the code node's comment stripper misreads."
    )


def _comment_safe(value: str) -> str:
    """Neutralise anything that could end a generated ``#`` comment line.

    Alteryx text reaches the mapper already unescaped, so a ``&#10;`` in a Plugin attribute, an
    annotation or a field name is a real newline by the time it is interpolated; a line break
    inside a comment ends the comment and the rest of the attribute becomes code that runs with
    the flow. This is the one sanitiser for text that lands in a comment. Text that lands as a
    *value* is ``repr()``-quoted instead, which escapes every one of these characters.
    """
    return _COMMENT_BREAK_RE.sub(" ", value)


def _one_line(value: str, limit: int = 200) -> str:
    """`_comment_safe` plus collapsing runs of whitespace and truncating to *limit*."""
    collapsed = " ".join(_comment_safe(value).split())
    return collapsed[: limit - 3] + "..." if len(collapsed) > limit else collapsed


def _split_path(raw: str) -> tuple[str, str]:
    """Split a (possibly Windows) path into directory and filename without touching os.path."""
    cleaned = raw.strip()
    index = max(cleaned.rfind("\\"), cleaned.rfind("/"))
    if index < 0:
        return "", cleaned
    return cleaned[:index], cleaned[index + 1 :]


_WINDOWS_ABSOLUTE_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")


def _is_foreign_absolute_path(path: str) -> bool:
    """A drive-letter or UNC path, which only resolves on the machine the workflow came from."""
    return bool(_WINDOWS_ABSOLUTE_RE.match(path.strip()))


def _extension(name: str) -> str:
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def _map_alteryx_type(alteryx_type: str | None) -> str | None:
    if not alteryx_type:
        return None
    return _ALTERYX_TYPE_MAP.get(alteryx_type.strip().lower())


_SECRET_NAME_RE = re.compile(r"password|secret|token|credential|client_?id|api_?key|connectionid|dcm", re.IGNORECASE)
_CONNECTION_STRING_RE = re.compile(r"(?:^|[;:\s])(?:pwd|password)\s*=", re.IGNORECASE)
REDACTED = "[redacted by Flowfile]"
# The Explorer Box's address tag, the one value screened by shape rather than by name.
HTML_BOX = "HtmlBox"
ADDRESS_TAG = "URL"


_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]+://|^[A-Za-z][A-Za-z0-9+.\-]{2,}:(?![\\/])")
_KEY_VALUE_PAIRS_RE = re.compile(r";\s*[A-Za-z_][A-Za-z0-9_ .\-]*\s*=")
_USER_PASSWORD_RE = re.compile(r"[^\s:@/\\]+:[^\s:@/\\]*@")
_QUERY_PARAMETER_RE = re.compile(r"\?[A-Za-z0-9_.\-]+=")

CONNECTION_NOTICE = (
    "This node carries the original Alteryx connection string or URL; remove any credentials before sharing the flow."
)


def _looks_like_connection(value: str) -> bool:
    """Whether a value reads as a connection string or URL rather than a plain file path.

    A drive letter is deliberately not a scheme (``C:\\data`` is a path, ``odbc:`` is not), and a
    single ``key=value`` is not enough — Alteryx writes those in ordinary paths.
    """
    text = value.strip()
    if not text:
        return False
    return bool(
        _SCHEME_RE.search(text)
        or _KEY_VALUE_PAIRS_RE.search(text)
        or _USER_PASSWORD_RE.search(text)
        or _QUERY_PARAMETER_RE.search(text)
    )


def _flag_connection_source(tool: AlteryxTool, path: str, row: ToolReportRow) -> ToolReportRow:
    """Say so when the tool's path or annotation still carries a connection string or URL.

    The secret is already in the user's own ``.yxmd``, so this is about telling them what the
    converted flow now contains, not about hiding it. A converted node drops to ``partial``
    because sharing it needs a human decision first; a placeholder keeps its status.
    """
    annotations = f"{tool.annotation}\n{tool.default_annotation}"
    if not (_looks_like_connection(path) or _looks_like_connection(annotations)):
        return row
    if CONNECTION_NOTICE not in row.messages:
        row.messages.append(CONNECTION_NOTICE)
    if row.status == "converted":
        row.status, row.reason = "partial", "connection_string"
    return row


def _carries_credentials(value: str) -> bool:
    """Whether a ``<File>`` value is really a connection string carrying a password.

    Such a value is not a path at all, so it must never be split into a directory and a name and
    stored on a node — the saved flow file would carry the credentials, whatever the tail of the
    string looks like.
    """
    return bool(_CONNECTION_STRING_RE.search(value))


def _safe_path(value: str) -> str:
    """A path safe to repeat in a report message; a connection string with a password is not."""
    return REDACTED if _carries_credentials(value) else value


_SIMPLE_EXTENSION_RE = re.compile(r"^[A-Za-z0-9]{1,8}$")
UNRECOGNISED_FORMAT = "an unrecognised format"


def _safe_extension(filename: str) -> str:
    """How to name a file format in a message, quoted, without ever echoing the value.

    ``_extension`` hands back whatever follows the last dot, which for a connection string is a
    slice of the connection string. Only something that actually looks like an extension is
    repeated; anything else is described, not quoted.
    """
    extension = _extension(filename)
    return f"'{extension}'" if _SIMPLE_EXTENSION_RE.match(extension) else UNRECOGNISED_FORMAT


def _redact_secrets(element: ET.Element, *, screen_address: bool = False) -> list[str]:
    """Blank credential-shaped values in a config tree, returning the names that were blanked.

    The placeholder comment is written into the saved flow file, so whatever Alteryx stored in
    the tool configuration — DCM connection ids, OAuth client ids, ODBC passwords — would
    otherwise land on disk in plain text. Matching is by name and fails closed: every attribute
    of a credential-shaped element goes too, and any element whose text looks like a connection
    string carrying a password is blanked whole even when its own name says nothing.

    *screen_address* adds the Explorer Box's rule: its ``<URL>`` is screened by the same test
    that decides whether the address may go on the canvas, so a wired box does not copy into a
    placeholder body what the comment refused. It is off for every other tool, whose ``<URL>``
    is the configuration a reader needs to rebuild the node rather than a canvas decoration.
    """
    redacted: list[str] = []
    for node in element.iter():
        is_secret = bool(_SECRET_NAME_RE.search(node.tag)) or "DcmType" in node.attrib
        for name in list(node.attrib):
            if (is_secret or _SECRET_NAME_RE.search(name)) and node.attrib[name]:
                node.attrib[name] = REDACTED
                redacted.append(f"{node.tag}@{name}")
        text = (node.text or "").strip()
        is_secret_address = screen_address and node.tag == ADDRESS_TAG and _address_carries_credentials(text)
        if text and (is_secret or is_secret_address or _CONNECTION_STRING_RE.search(text)):
            node.text = REDACTED
            redacted.append(node.tag)
    return sorted(dict.fromkeys(redacted))


def _config_xml_lines(tool: AlteryxTool) -> tuple[list[str], list[str]]:
    """The tool's ``<Configuration>`` pretty-printed into lines, plus the names redacted out."""
    if tool.configuration is None:
        return [], []
    element = copy.deepcopy(tool.configuration)
    redacted = _redact_secrets(element, screen_address=census_tool_name(tool) == HTML_BOX)
    ET.indent(element, space="  ")
    try:
        rendered = ET.tostring(element, encoding="unicode")
    except (TypeError, ValueError):
        return [], []
    lines = [_comment_safe(line.rstrip())[:CONFIG_COMMENT_LINE_LIMIT] for line in rendered.splitlines() if line.strip()]
    if len(lines) > CONFIG_COMMENT_MAX_LINES:
        dropped = len(lines) - CONFIG_COMMENT_MAX_LINES
        lines = [*lines[:CONFIG_COMMENT_MAX_LINES], f"... ({dropped} more lines; see the original .yxmd)"]
    return lines, redacted


def _original_config_lines(tool: AlteryxTool) -> list[str]:
    """Everything the .yxmd said about this tool, so it can be rebuilt without the original file."""
    lines: list[str] = []
    annotation = tool.default_annotation or tool.annotation
    if annotation:
        lines.extend(f"Alteryx annotation: {_one_line(part)}" for part in annotation.splitlines() if part.strip())
    config, redacted = _config_xml_lines(tool)
    if config:
        lines.append(f"Original Alteryx configuration ({_one_line(tool.plugin or tool_label(tool))}):")
        if redacted:
            names = _one_line(", ".join(redacted))
            lines.append(f"Credential values were not copied out of the workflow: {names}.")
        lines.extend(config)
    return lines


def _placeholder_code(tool: AlteryxTool, num_inputs: int, notes: list[str], header: str | None = None) -> str:
    """The passthrough body. *header* replaces the two-line "could not be converted" preamble."""
    identity = f"# Alteryx tool '{_one_line(tool_label(tool))}' (ToolID {tool.tool_id})"
    lines = (
        [f"{identity}: {header}"]
        if header
        else [
            f"{identity} could not be converted automatically.",
            "# This node passes its input through unchanged; rebuild the logic here.",
        ]
    )
    lines.extend(f"# {_one_line(note)}" for note in notes)
    lines.extend(f"# {line}" for line in _original_config_lines(tool))
    if num_inputs == 0:
        lines.append("output_df = pl.DataFrame()")
    elif num_inputs == 1:
        lines.append("output_df = input_df")
    else:
        lines.append("output_df = input_df_1")
    return "\n".join(lines)


def emit_placeholder(
    tool: AlteryxTool,
    ctx: EmitContext,
    messages: list[str],
    *,
    dx: int = 0,
    dy: int = 0,
    register_anchors: bool = True,
    wording: str | None = None,
    code_wording: str | None = None,
) -> int:
    """Emit the polars_code passthrough that keeps the graph shape intact.

    *wording* and *code_wording* carry a sentence that replaces the default "needs manual
    conversion" text on the node description and in the generated code. A node standing in for
    a tool Flowfile will never convert is not a defect, so the description also loses its ⚠.

    The Alteryx wire count is only the first guess at how many inputs the body should read:
    wiring can hand the node fewer edges than the tool has wires. :func:`rewrite_placeholder_bodies`
    corrects it afterwards, which is why the arguments it needs are recorded here.
    """
    inputs = ctx.input_count(tool.tool_id)
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_placeholder_code(tool, inputs, messages, code_wording)
        ),
    )
    warning = wording or (
        f"{WARNING_PREFIX}Needs manual conversion: Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id})"
    )
    node_id = ctx.add_node(
        tool,
        "polars_code",
        settings,
        dx=dx,
        dy=dy,
        description=_description(tool, warning),
        is_start_node=inputs == 0,
    )
    ctx.placeholder_bodies[node_id] = _PlaceholderBody(tool, list(messages), code_wording, inputs)
    if register_anchors:
        ctx.register_all_outputs(tool.tool_id, node_id)
        ctx.register_all_inputs(tool.tool_id, node_id)
    return node_id


def _received_edges(node: schemas.FlowfileNode) -> int:
    """How many streams the engine will really hand this node once the flow is loaded."""
    return len(node.input_ids or []) + (node.left_input_id is not None) + (node.right_input_id is not None)


def rewrite_placeholder_bodies(ctx: EmitContext) -> None:
    """Write every passthrough body for the edges its node actually received.

    A body is rendered when the node is emitted, from the number of Alteryx wires arriving at the
    tool. Wiring then collapses every wire resolving to one Flowfile node into a single edge and
    drops the wires whose source anchor carries nothing, so a body written for two inputs can land
    on a node the engine hands a single ``input_df`` — and the node fails at run time on a name
    that was never bound. Reading the finished node instead is order-independent: by the time this
    runs, every wire the workflow states has been laid or accounted for.
    """
    nodes = {node.id: node for node in ctx.nodes}
    for node_id, body in ctx.placeholder_bodies.items():
        node = nodes.get(node_id)
        if node is None:
            continue
        received = _received_edges(node)
        if received == body.emitted_inputs:
            continue
        node.setting_input.polars_code_input.polars_code = _placeholder_code(
            body.tool, received, body.notes, body.header
        )
        node.is_start_node = received == 0


def emit_passthrough(tool: AlteryxTool, ctx: EmitContext, anchor_map: dict[str, str]) -> None:
    """Emit no node: hand each output anchor the wire that arrives on its input anchor.

    A tool that does nothing to the data should not cost a node in the imported flow, so its
    consumers are wired straight to whatever fed it. Only the wires it really hands on are marked
    consumed — Alteryx unions every wire arriving on one input anchor and this carries the first,
    so anything else it received is a wire the imported flow does not have and wiring says so on
    both rows. A caller that swallows more than it passes on (a sink, a Detour End's dead side)
    declares that itself. ``tool_columns`` is deliberately left unwritten: the column readers
    resolve through the alias to the real source, and an entry here would answer "unknown" for a
    tool that changes nothing.
    """
    for output_anchor, input_anchor in anchor_map.items():
        connection = ctx.source_connection(tool.tool_id, (input_anchor,))
        if connection is not None:
            ctx.alias_output(tool.tool_id, output_anchor, connection.origin_tool_id, connection.origin_anchor)
            ctx.consume(connection)


def uncarried_wire_message(tool: AlteryxTool, connection: AlteryxConnection, *, carried: bool) -> str:
    """Why a wire into a tool that emitted no node never reached the tool's consumers.

    *carried* says whether the tool handed any stream onward at all: a no-op fed only on an anchor
    it does not read keeps nothing, and telling the user one stream survived would be false.
    """
    fate = (
        "only the stream it passes on was kept"
        if carried
        else "it carried no stream onward at all, so this branch ends here"
    )
    return (
        f"The Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id}) has no effect on the data, so no node was "
        f"imported for it and {fate}; the connection from ToolID {connection.origin_tool_id} "
        f"('{connection.origin_anchor}') into its '{connection.dest_anchor}' anchor was not carried over."
    )


def map_unsupported(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Fallback mapper for every tool without a dedicated implementation.

    The only place scope is consulted: a tool Flowfile has decided not to convert says so in
    its own words instead of promising a manual rebuild that is never coming. Everything else
    is an honest placeholder, which is also where an unlisted tool lands.
    """
    verdict = classify(census_tool_name(tool))
    if verdict is None:
        message = (
            f"Alteryx tool '{tool_label(tool)}' has no Flowfile equivalent; a passthrough placeholder was inserted."
        )
        node_id = emit_placeholder(tool, ctx, [])
        ctx.tool_columns[tool.tool_id] = None
        return _row(tool, "placeholder", [node_id], "polars_code", [message], reason="unmapped_tool")

    node_id = emit_placeholder(tool, ctx, [], wording=verdict.sentence, code_wording=verdict.sentence)
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, verdict.status, [node_id], "polars_code", [verdict.sentence], reason=verdict.reason)


def _placeholder_row(tool: AlteryxTool, ctx: EmitContext, messages: list[str], reason: str) -> ToolReportRow:
    """A tool whose configuration the mapper read and would not guess at; never scope-aware."""
    node_id = emit_placeholder(tool, ctx, messages)
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, "placeholder", [node_id], "polars_code", messages, reason=reason)


# Which input anchor feeds which output anchor, for the tools that only pass records on.
# Test and ExpectEqual are sinks: Alteryx gives them no output anchor at all.
NO_OP_ANCHORS: dict[str, dict[str, str]] = {
    "Message": {DEFAULT_OUTPUT_ANCHOR: DEFAULT_INPUT_ANCHOR},
    "Throttle": {DEFAULT_OUTPUT_ANCHOR: DEFAULT_INPUT_ANCHOR},
    "BlockUntilDone": {
        DEFAULT_OUTPUT_ANCHOR: DEFAULT_INPUT_ANCHOR,
        "Output2": DEFAULT_INPUT_ANCHOR,
        "Output3": DEFAULT_INPUT_ANCHOR,
    },
    "Test": {},
    "ExpectEqual": {},
}

DETOUR = "Detour"
DETOUR_END = "DetourEnd"
DETOUR_LEFT = "Left"
DETOUR_RIGHT = "Right"


def _no_op_verdict(tool: AlteryxTool) -> ScopeVerdict | None:
    """The scope verdict when this tool really has no effect on the data, else ``None``."""
    verdict = classify(census_tool_name(tool))
    return verdict if verdict is not None and verdict.status == "no_op" else None


def _detour_active_anchor(tool: AlteryxTool) -> str:
    """The Detour anchor the records leave by; Alteryx detours left unless told otherwise."""
    return DETOUR_RIGHT if _flag(_config(tool), "DetourRight") else DETOUR_LEFT


def _is_detour_wire(ctx: EmitContext, connection: AlteryxConnection) -> bool:
    """Whether this wire leaves a Detour directly, by either of its sides."""
    source = ctx.tools.get(connection.origin_tool_id)
    return source is not None and census_tool_name(source) == DETOUR


def _is_live_detour_wire(ctx: EmitContext, connection: AlteryxConnection) -> bool:
    if not _is_detour_wire(ctx, connection):
        return False
    return connection.origin_anchor == _detour_active_anchor(ctx.tools[connection.origin_tool_id])


def _detour_end_wire(ctx: EmitContext, tool: AlteryxTool) -> tuple[AlteryxConnection | None, str]:
    """The one wire a Detour End takes its records from, or why the mapper will not guess.

    Which side is live is a property of the upstream Detour's configuration, not of the order
    the tools happened to be mapped in, so this reads the tools rather than what was emitted.
    """
    inbound = ctx.inbound.get(tool.tool_id, [])
    live = [connection for connection in inbound if _is_live_detour_wire(ctx, connection)]
    if len(live) != 1:
        return None, (
            f"{len(live)} of the {len(inbound)} inputs of this Alteryx Detour End come from the live anchor of a "
            "Detour, so which stream continues cannot be read from the workflow; this node passes its input through."
        )
    if sum(1 for connection in inbound if connection.dest_anchor == live[0].dest_anchor) > 1:
        return None, (
            f"Several inputs of this Alteryx Detour End arrive on its '{live[0].dest_anchor}' anchor, so the live "
            "stream cannot be told from the others; this node passes its input through."
        )
    return live[0], ""


def _no_op_source(ctx: EmitContext, tool_id: int, anchor: str) -> AlteryxConnection | None:
    """The wire a tool with no effect hands to the consumers of one of its output anchors.

    One reading of the tool's own configuration, shared by the mappers below and by the anchor
    resolution, so what a consumer is told never depends on which of the two ran first.
    """
    tool = ctx.tools.get(tool_id)
    if tool is None or _no_op_verdict(tool) is None:
        return None
    name = census_tool_name(tool)
    if name == DETOUR:
        if anchor != _detour_active_anchor(tool):
            return None
        return ctx.source_connection(tool_id, (DEFAULT_INPUT_ANCHOR,))
    if name == DETOUR_END:
        return _detour_end_wire(ctx, tool)[0] if anchor == DEFAULT_OUTPUT_ANCHOR else None
    input_anchor = NO_OP_ANCHORS.get(name, {}).get(anchor)
    return ctx.source_connection(tool_id, (input_anchor,)) if input_anchor else None


def map_no_op(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """A tool with no effect on the data: no node at all, its input handed to its consumers."""
    verdict = _no_op_verdict(tool)
    if verdict is None:
        return map_unsupported(tool, ctx)
    anchors = NO_OP_ANCHORS.get(census_tool_name(tool), {})
    emit_passthrough(tool, ctx, anchors)
    if not anchors:
        # A sink has no output anchor at all, so every wire into it ends there on purpose.
        ctx.consume(*ctx.inbound.get(tool.tool_id, []))
    return _row(tool, verdict.status, [], None, [verdict.sentence], reason=verdict.reason)


def map_detour(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Detour sends every record down one of two anchors; the other carries nothing."""
    verdict = _no_op_verdict(tool)
    if verdict is None:
        return map_unsupported(tool, ctx)
    active = _detour_active_anchor(tool)
    inactive = DETOUR_LEFT if active == DETOUR_RIGHT else DETOUR_RIGHT
    emit_passthrough(tool, ctx, {active: DEFAULT_INPUT_ANCHOR})
    messages = [verdict.sentence]
    if ctx.has_outgoing(tool.tool_id, inactive):
        message = (
            f"This Alteryx Detour is configured to send its records out of the '{active}' anchor, so nothing "
            f"leaves the '{inactive}' anchor; the connections from it were not wired."
        )
        ctx.inactive_outputs[(tool.tool_id, inactive)] = message
        messages.append(message)
    return _row(tool, verdict.status, [], None, messages, reason=verdict.reason)


def map_detour_end(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Detour End rejoins the two paths of a Detour; only the live one carries records."""
    verdict = _no_op_verdict(tool)
    if verdict is None:
        return map_unsupported(tool, ctx)
    connection, refusal = _detour_end_wire(ctx, tool)
    if connection is None:
        return _placeholder_row(tool, ctx, [refusal], reason="mapper_refused")
    emit_passthrough(tool, ctx, {DEFAULT_OUTPUT_ANCHOR: connection.dest_anchor})
    # The dead side of the same Detour is expected to arrive here and expected to carry nothing.
    ctx.consume(*(wire for wire in ctx.inbound.get(tool.tool_id, []) if _is_detour_wire(ctx, wire)))
    message = (
        f"The records arrive from the '{connection.origin_anchor}' anchor of the Alteryx Detour "
        f"(ToolID {connection.origin_tool_id}), which is the side its configuration makes live."
    )
    return _row(tool, verdict.status, [], None, [verdict.sentence, message], reason=verdict.reason)


def comment_bounds(tool: AlteryxTool, ctx: EmitContext) -> tuple[int, int, int, int]:
    """The canvas box for a comment made from an Alteryx box tool: placed by the layout, size scaled."""
    x, y = ctx.position(tool)
    return (
        x,
        y,
        max(round((tool.width or 0) * POS_SCALE), COMMENT_MIN_WIDTH),
        max(round((tool.height or 0) * POS_SCALE), COMMENT_MIN_HEIGHT),
    )


EXPLORER_BOX_PREFIX = "Alteryx Explorer Box: "
EXPLORER_BOX_REDACTED = "an address that looks like it carries credentials, which was not copied out of the workflow"


def _address_carries_credentials(url: str) -> bool:
    """Whether an Explorer Box address carries a secret, which must not be copied onto the canvas.

    Deliberately not ``_looks_like_connection``: every address here has a scheme or is a path,
    so that test would refuse the documentation links this tool mostly points at. Only userinfo,
    a password key-value or query parameters (where tokens travel) are treated as secrets.
    ``_redact_secrets`` applies it to any ``<URL>`` it dumps, so a wired Explorer Box — which
    becomes a placeholder rather than a comment — does not copy in what the comment refused.
    """
    return bool(_carries_credentials(url) or _USER_PASSWORD_RE.search(url) or _QUERY_PARAMETER_RE.search(url))


def map_html_box(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Explorer Box shows a web page on the canvas; Flowfile keeps the address as a comment.

    It is documentation, not a transformation — it has no anchors and no output schema — so it
    becomes an annotation like a Comment tool rather than a node, and never enters the coverage
    percentages. ``HideAddressBar`` is read and ignored: Flowfile has no browser pane to hide a
    bar in. The address is kept exactly as written, including a Windows path that only resolves
    on the machine the workflow came from; resolving it would be a guess about someone's disk.
    """
    if ctx.input_count(tool.tool_id) or ctx.outbound.get(tool.tool_id):
        return _placeholder_row(
            tool,
            ctx,
            [
                "This Alteryx Explorer Box is wired into the flow, which a canvas comment cannot be; "
                "it was left as a pass-through node instead."
            ],
            reason="mapper_refused",
        )
    url = _text(_config(tool), "URL")
    if not url:
        return _row(
            tool,
            "skipped",
            [],
            None,
            ["This Alteryx Explorer Box names no address, so there was nothing to keep."],
            reason="annotation",
            entity="annotation",
        )

    status: ToolStatus = "converted"
    messages = ["Imported as a canvas comment holding the address; Flowfile does not show the page itself."]
    if _address_carries_credentials(url):
        status = "partial"
        text = f"{EXPLORER_BOX_PREFIX}{EXPLORER_BOX_REDACTED}."
        messages.append(
            "The address was not copied into the comment because it carries credentials or query parameters; "
            "it is still in the original .yxmd."
        )
    else:
        text = f"{EXPLORER_BOX_PREFIX}{url}"
    x, y, width, height = comment_bounds(tool, ctx)
    ctx.comments.append(
        schemas.FlowfileComment(
            id=ctx.new_comment_id(), text=text, x_position=x, y_position=y, width=width, height=height
        )
    )
    return _row(tool, status, [], "comment", messages, reason="annotation", entity="annotation")


def _parse_number(value: str) -> tuple[bool, bool]:
    """Return (is_int, is_float) for a raw text cell."""
    text = value.strip()
    if not text:
        return False, False
    try:
        int(text)
        return True, True
    except ValueError:
        pass
    try:
        float(text)
        return False, True
    except ValueError:
        return False, False


def _column_values(raw: list[str | None], declared: str | None) -> tuple[str, list]:
    """Type a Text Input column, preferring the declared Alteryx type over inference."""
    if declared in ("String", "Date", "Datetime", "Time", "Boolean"):
        return declared, [value for value in raw]
    filled = [value for value in raw if value is not None and value.strip() != ""]
    if filled and all(_parse_number(value)[0] for value in filled):
        return "Int64", [int(value.strip()) if value and value.strip() else None for value in raw]
    if filled and all(_parse_number(value)[1] for value in filled):
        return "Float64", [float(value.strip()) if value and value.strip() else None for value in raw]
    return "String", [value for value in raw]


def map_text_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    fields = config.findall("Fields/Field")
    names = [field_element.get("name") or f"column_{index}" for index, field_element in enumerate(fields)]
    declared = [_map_alteryx_type(field_element.get("type")) for field_element in fields]
    rows: list[list[str | None]] = []
    for row_element in config.findall("Data/r"):
        cells = [cell.text for cell in row_element.findall("c")]
        cells = cells[: len(names)] + [None] * max(0, len(names) - len(cells))
        rows.append(cells)

    columns: list[input_schema.MinimalFieldInfo] = []
    data: list[list] = []
    inferred: list[str] = []
    for index, name in enumerate(names):
        raw = [row[index] for row in rows]
        data_type, values = _column_values(raw, declared[index])
        if declared[index] is None and data_type != "String":
            inferred.append(name)
        columns.append(input_schema.MinimalFieldInfo(name=name, data_type=data_type))
        data.append(values)

    settings = input_schema.NodeManualInput(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        raw_data_format=input_schema.RawData(columns=columns, data=data),
    )
    node_id = ctx.add_node(tool, "manual_input", settings, description=_description(tool), is_start_node=True)
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = names
    messages = []
    if inferred:
        messages.append(
            "Column types were inferred from the entered values (Alteryx stores Text Input cells as text): "
            + ", ".join(inferred)
        )
    return _row(tool, "converted", [node_id], "manual_input", messages, reason="converted")


def map_select(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    keep_missing = True
    select_input: list[transform_schema.SelectInput] = []
    unmapped_types: list[str] = []
    for element in config.findall("SelectFields/SelectField"):
        name = element.get("field") or ""
        selected = _is_true(element.get("selected"))
        if name == "*Unknown":
            keep_missing = selected
            continue
        if not name:
            continue
        alteryx_type = element.get("type")
        data_type = _map_alteryx_type(alteryx_type) if selected else None
        if selected and alteryx_type and data_type is None:
            unmapped_types.append(f"{name} ({alteryx_type})")
        select_input.append(
            transform_schema.SelectInput(
                old_name=name,
                new_name=element.get("rename") or name,
                keep=selected,
                data_type=data_type,
                data_type_change=data_type is not None,
            )
        )

    settings = input_schema.NodeSelect(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        keep_missing=keep_missing,
        select_input=select_input,
    )
    node_id = ctx.add_node(tool, "select", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [item.new_name for item in select_input if item.keep] or None

    messages = []
    status: ToolStatus = "converted"
    reason = "converted"
    if unmapped_types:
        status, reason = "partial", "option_unsupported"
        messages.append(
            "These Alteryx data types have no Flowfile equivalent and were left unchanged: " + ", ".join(unmapped_types)
        )
    return _row(tool, status, [node_id], "select", messages, reason=reason)


# Rebuilding the Alteryx expression from the simple-mode triple reuses the fail-closed translator.
_SIMPLE_FILTER_TEMPLATES: dict[str, tuple[str, int]] = {
    "=": ("{field} = {operand}", 1),
    "==": ("{field} = {operand}", 1),
    "!=": ("{field} != {operand}", 1),
    "<>": ("{field} != {operand}", 1),
    ">": ("{field} > {operand}", 1),
    ">=": ("{field} >= {operand}", 1),
    "<": ("{field} < {operand}", 1),
    "<=": ("{field} <= {operand}", 1),
    "isnull": ("IsNull({field})", 0),
    "isnotnull": ("!IsNull({field})", 0),
    "isempty": ("IsEmpty({field})", 0),
    "isnotempty": ("!IsEmpty({field})", 0),
    "contains": ("Contains({field}, {operand})", 1),
    "doesnotcontain": ("!Contains({field}, {operand})", 1),
    "!contains": ("!Contains({field}, {operand})", 1),
    "startswith": ("StartsWith({field}, {operand})", 1),
    "doesnotstartwith": ("!StartsWith({field}, {operand})", 1),
    "endswith": ("EndsWith({field}, {operand})", 1),
    "doesnotendwith": ("!EndsWith({field}, {operand})", 1),
}


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _simple_filter_expression(config: ET.Element) -> tuple[str, str | None]:
    """Rebuild the Alteryx expression a simple-mode filter stands for, or explain why we cannot."""
    simple = config.find("Simple")
    if simple is None:
        return "", "the Alteryx filter has no simple-mode configuration"
    field_name = _text(simple, "Field")
    if not field_name:
        return "", "the Alteryx simple filter names no field"
    if "[" in field_name or "]" in field_name:
        return "", f"the Alteryx simple filter field {field_name!r} cannot be written as a field reference"
    raw_operator = _text(simple, "Operator")
    template, arity = _SIMPLE_FILTER_TEMPLATES.get(raw_operator.strip().lower(), (None, 0))
    if template is None:
        return "", f"the Alteryx simple filter operator {raw_operator or '(empty)'!r} is not supported"
    operands = [(element.text or "").strip() for element in simple.findall("Operands/Operand")]
    if arity and not operands:
        return "", f"the Alteryx simple filter operator {raw_operator!r} has no operand"
    operand = ""
    if arity:
        operand = operands[0]
        if not _looks_numeric(operand):
            if '"' in operand:
                return "", "the Alteryx simple filter operand contains a double quote and cannot be converted safely"
            operand = f'"{operand}"'
    return template.format(field=f"[{field_name}]", operand=operand), None


def map_filter(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    mode = _text(config, "Mode")
    rebuilt_from_simple = mode.lower() == "simple" or (
        config.find("Simple") is not None and not _text(config, "Expression")
    )
    simple_reason = None
    if rebuilt_from_simple:
        expression, simple_reason = _simple_filter_expression(config)
    else:
        expression = _text(config, "Expression")
    outcome = TranslationOutcome(None, simple_reason) if simple_reason else try_translate(expression)
    split_mode = ctx.has_outgoing(tool.tool_id, "False")

    if outcome.translated is None:
        messages = [
            f"The Alteryx filter expression could not be converted: {outcome.reason}.",
            f"Original expression: {_one_line(expression)}",
            "Both Alteryx branches now receive unfiltered data until this node is rebuilt.",
        ]
        return _placeholder_row(tool, ctx, messages, reason="translator_refused")

    settings = input_schema.NodeFilter(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter=outcome.translated),
        split_mode=split_mode,
    )
    node_id = ctx.add_node(tool, "filter", settings, description=_description(tool))
    ctx.register_output(tool.tool_id, DEFAULT_OUTPUT_ANCHOR, node_id, PASS_HANDLE)
    ctx.register_output(tool.tool_id, "True", node_id, PASS_HANDLE)
    if split_mode:
        ctx.register_output(tool.tool_id, "False", node_id, FAIL_HANDLE)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "filter", [], reason="converted")


def _commented_formula_body(expression: str, reason: str, stub: str) -> str:
    return (
        f"// Alteryx formula could not be converted automatically: {_one_line(reason)}.\n"
        f"// Original: {_one_line(expression)}\n"
        f"{stub}"
    )


@dataclass
class _Assignment:
    """One Alteryx expression writing one output column."""

    target: str
    expression: str
    data_type: str | None = None


def _formula_assignments(tool: AlteryxTool) -> list[_Assignment]:
    return [
        _Assignment(
            target=element.get("field") or f"formula_{index + 1}",
            expression=element.get("expression") or "",
            data_type=_map_alteryx_type(element.get("type")),
        )
        for index, element in enumerate(_config(tool).findall("FormulaFields/FormulaField"))
    ]


def _emit_formula_chain(tool: AlteryxTool, ctx: EmitContext, assignments: list[_Assignment]) -> ToolReportRow:
    """Emit one Flowfile formula node per Alteryx assignment, chained in configuration order."""
    known = ctx.input_columns(tool.tool_id)
    node_ids: list[int] = []
    messages: list[str] = []
    commented = False
    placeholder = False
    previous_id: int | None = None

    for index, assignment in enumerate(assignments):
        target, expression = assignment.target, assignment.expression
        outcome = try_translate(expression)
        dx, dy = index * FORMULA_STEP_DX, index * FORMULA_STEP_DY

        if outcome.translated is not None:
            settings = input_schema.NodeFormula(
                flow_id=ctx.flow_id,
                node_id=ctx.new_node_id(),
                function=transform_schema.FunctionInput(
                    field=transform_schema.FieldInput(name=target, data_type=transform_schema.AUTO_DATA_TYPE),
                    function=outcome.translated,
                ),
            )
            node_id = ctx.add_node(tool, "formula", settings, dx=dx, dy=dy, description=_description(tool))
        else:
            is_new_column = known is not None and target not in known
            stub = "nullif(0, 0)" if is_new_column else f"[{target}]"
            body = _commented_formula_body(expression, outcome.reason or "no reason recorded", stub)
            stub_type = (
                (assignment.data_type or transform_schema.AUTO_DATA_TYPE)
                if is_new_column
                else (transform_schema.AUTO_DATA_TYPE)
            )
            try:
                simple_function_to_expr(body)
            except Exception:  # the comment body itself is unusable; degrade to a code placeholder
                placeholder = True
                messages.append(f"'{target}': {outcome.reason}. Original expression preserved in a placeholder node.")
                node_id = emit_placeholder(
                    tool,
                    ctx,
                    [f"{target} = {expression}", str(outcome.reason)],
                    dx=dx,
                    dy=dy,
                    register_anchors=False,
                )
                node_ids.append(node_id)
                if previous_id is not None:
                    _link(ctx, previous_id, node_id)
                previous_id = node_id
                continue
            commented = True
            messages.append(f"'{target}': {outcome.reason}. The original expression is kept as a comment.")
            settings = input_schema.NodeFormula(
                flow_id=ctx.flow_id,
                node_id=ctx.new_node_id(),
                function=transform_schema.FunctionInput(
                    field=transform_schema.FieldInput(name=target, data_type=stub_type),
                    function=body,
                ),
            )
            warning = f"{WARNING_PREFIX}Alteryx formula for '{target}' needs manual conversion"
            node_id = ctx.add_node(tool, "formula", settings, dx=dx, dy=dy, description=_description(tool, warning))

        node_ids.append(node_id)
        if previous_id is not None:
            _link(ctx, previous_id, node_id)
        previous_id = node_id
        if known is not None and target not in known:
            known = [*known, target]

    ctx.register_input(tool.tool_id, DEFAULT_INPUT_ANCHOR, node_ids[0], MAIN)
    for connection in ctx.inbound.get(tool.tool_id, []):
        if connection.dest_anchor != DEFAULT_INPUT_ANCHOR:
            ctx.register_input(tool.tool_id, connection.dest_anchor, node_ids[0], MAIN)
    ctx.register_all_outputs(tool.tool_id, node_ids[-1])
    ctx.tool_columns[tool.tool_id] = known

    status: ToolStatus = "placeholder" if placeholder else ("commented" if commented else "converted")
    reason = "converted" if status == "converted" else "translator_refused"
    if len(node_ids) > 1:
        messages.insert(0, f"{len(node_ids)} Alteryx assignments became {len(node_ids)} chained Flowfile nodes.")
    return _row(tool, status, node_ids, "formula", messages, reason=reason)


def map_formula(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    assignments = _formula_assignments(tool)
    if not assignments:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Formula tool has no expressions configured."], reason="mapper_refused"
        )
    return _emit_formula_chain(tool, ctx, assignments)


def _link(ctx: EmitContext, from_id: int, to_id: int, handle: str = PASS_HANDLE) -> None:
    """Connect two emitted nodes directly (used for 1:N expansions)."""
    nodes = {node.id: node for node in ctx.nodes}
    source, target = nodes[from_id], nodes[to_id]
    source.outputs.append(to_id)
    source.output_handles.append(handle)
    target.input_ids.append(from_id)


DYNAMIC_RENAME_SOURCE_ANCHORS = ("Source", "Right", "R")
DYNAMIC_RENAME_TARGET_ANCHORS = ("Targets", "Input", "Left", "T")


def _normalise_mode(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _field_selection(config: ET.Element) -> tuple[list[str], list[str], bool]:
    """``<Fields>`` as (every named field, the selected ones, whether ``*Unknown`` is selected)."""
    names: list[str] = []
    selected: list[str] = []
    unknown_selected = False
    for element in config.findall("Fields/Field"):
        name = element.get("name") or ""
        raw = element.get("selected")
        is_selected = raw is None or _is_true(raw)
        if name == "*Unknown":
            unknown_selected = is_selected
            continue
        if not name:
            continue
        names.append(name)
        if is_selected:
            selected.append(name)
    return names, selected, unknown_selected


def _selection_settings(names: list[str], selected: list[str], unknown_selected: bool) -> dict:
    if unknown_selected and selected == names:
        return {"selection_mode": "all", "selected_columns": []}
    return {"selection_mode": "list", "selected_columns": selected}


def _register_rename_anchors(ctx: EmitContext, tool_id: int, first_id: int, last_id: int | None = None) -> None:
    """Wire only the data anchor; the field-name anchor is resolved at import time, not wired."""
    ctx.register_input(tool_id, DEFAULT_INPUT_ANCHOR, first_id, MAIN)
    unwired: list[str] = []
    for connection in ctx.inbound.get(tool_id, []):
        if connection.dest_anchor in DYNAMIC_RENAME_TARGET_ANCHORS:
            ctx.register_input(tool_id, connection.dest_anchor, first_id, MAIN)
        else:
            unwired.append(connection.dest_anchor)
    ctx.suppress_input(tool_id, tuple(unwired))
    ctx.register_all_outputs(tool_id, first_id if last_id is None else last_id)


def _text_input_values(tool: AlteryxTool, column: str) -> list[str] | None:
    """The rows of one Text Input column, when the tool feeding the names is a Text Input."""
    if tool.tool_name != "TextInput":
        return None
    config = _config(tool)
    names = [element.get("name") or "" for element in config.findall("Fields/Field")]
    if not names:
        return None
    index = names.index(column) if column in names else 0
    values: list[str] = []
    for row in config.findall("Data/r"):
        cells = row.findall("c")
        if index < len(cells) and cells[index].text:
            values.append(cells[index].text.strip())
        else:
            values.append("")
    return values


def _emit_dynamic_rename(
    tool: AlteryxTool,
    ctx: EmitContext,
    rename_input: transform_schema.DynamicRenameInput,
    messages: list[str],
) -> ToolReportRow:
    settings = input_schema.NodeDynamicRename(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), dynamic_rename_input=rename_input
    )
    node_id = ctx.add_node(tool, "dynamic_rename", settings, description=_description(tool))
    _register_rename_anchors(ctx, tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, "converted", [node_id], "dynamic_rename", messages, reason="converted")


def _static_rename_to_select(
    tool: AlteryxTool, ctx: EmitContext, targets: list[str], new_names: list[str], origin: str
) -> ToolReportRow:
    """Turn a rename whose new names are already known at import time into a plain select."""
    pairs = list(zip(targets, new_names, strict=False))
    select_input = [
        transform_schema.SelectInput(old_name=old, new_name=new, keep=True) for old, new in pairs if new and old != new
    ]
    if not select_input:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Dynamic Rename resolved to no column renames."], reason="mapper_refused"
        )
    settings = input_schema.NodeSelect(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), keep_missing=True, select_input=select_input
    )
    node_id = ctx.add_node(tool, "select", settings, description=_description(tool))
    _register_rename_anchors(ctx, tool.tool_id, node_id)
    rename_map = {old: new for old, new in pairs if new}
    ctx.tool_columns[tool.tool_id] = [rename_map.get(name, name) for name in targets]
    messages = [
        f"The new column names were read from {origin} at import time and became a Select node "
        f"renaming {len(select_input)} column(s).",
        "The Alteryx field-name input is no longer connected; the node that supplied it is kept unwired "
        "so you can see where the names came from.",
    ]
    if len(new_names) < len(targets):
        messages.append(
            f"Only {len(new_names)} name(s) were available for {len(targets)} column(s); the rest keep their names."
        )
    return _row(tool, "partial", [node_id], "select", messages, reason="option_unsupported")


def _rename_from_right_input(
    tool: AlteryxTool, ctx: EmitContext, config: ET.Element, targets: list[str], mode: str
) -> ToolReportRow:
    connection = ctx.source_connection(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    resolved = ctx.resolved_source(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    source = ctx.tools.get(resolved[0]) if resolved is not None else None
    if connection is None or source is None:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Dynamic Rename field-name input is not connected."], reason="mapper_refused"
        )
    if mode == "rightinputmetadata":
        # The wire may come from one anchor of a multi-output tool (a Join's L or R passes one
        # input through), so the lookup is per anchor, the same one the join consumers use.
        new_names = _anchor_columns(ctx, tool.tool_id, connection.dest_anchor)
        if not new_names:
            return _placeholder_row(
                tool,
                ctx,
                [
                    "The Alteryx Dynamic Rename takes its names from the right input's column names, "
                    f"which are not known at import time for '{tool_label(source)}' (ToolID {source.tool_id})."
                ],
                reason="mapper_refused",
            )
        return _static_rename_to_select(
            tool, ctx, targets, new_names, f"the columns of '{tool_label(source)}' (ToolID {source.tool_id})"
        )

    names_from_rows = config.find("NamesFromRows")
    input_mode = _text(names_from_rows, "InputMode") if names_from_rows is not None else ""
    if input_mode and input_mode.strip().lower() != "positional":
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Dynamic Rename input mode '{input_mode}' has no Flowfile equivalent."],
            reason="option_unsupported",
        )
    column = _text(names_from_rows, "NewName") if names_from_rows is not None else ""
    new_names = _text_input_values(source, column)
    if not new_names:
        return _placeholder_row(
            tool,
            ctx,
            [
                "The Alteryx Dynamic Rename takes its names from the rows of "
                f"'{tool_label(source)}' (ToolID {source.tool_id}), which cannot be read at import time; "
                "rebuild this as a Select node once you know the names."
            ],
            reason="mapper_refused",
        )
    return _static_rename_to_select(
        tool, ctx, targets, new_names, f"the rows of '{tool_label(source)}' (ToolID {source.tool_id})"
    )


def map_dynamic_rename(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    raw_mode = _text(config, "RenameMode")
    mode = _normalise_mode(raw_mode)
    names, selected, unknown_selected = _field_selection(config)
    selection = _selection_settings(names, selected, unknown_selected)

    if mode in ("firstrow", "takefieldnamesfromfirstrowofdata"):
        return _emit_dynamic_rename(
            tool,
            ctx,
            transform_schema.DynamicRenameInput(rename_mode="first_row", **selection),
            ["The first row of data is promoted to column headers and dropped, as in Alteryx."],
        )

    if mode == "formula":
        expression = _text(config, "Expression")
        outcome = try_translate(_CURRENT_FIELD_RE.sub("[column_name]", expression))
        if outcome.translated is None:
            return _placeholder_row(
                tool,
                ctx,
                [
                    f"The Alteryx Dynamic Rename formula could not be converted: {outcome.reason}.",
                    f"Original expression: {_one_line(expression)}",
                ],
                reason="translator_refused",
            )
        return _emit_dynamic_rename(
            tool,
            ctx,
            transform_schema.DynamicRenameInput(rename_mode="formula", formula=outcome.translated, **selection),
            [f"The Alteryx rename formula became the Flowfile formula {outcome.translated!r}."],
        )

    if mode in ("addprefixsuffix", "addprefix", "addsuffix", "prefix", "suffix", "prefixsuffix"):
        prefix = _text(config, ".//Prefix")
        suffix = _text(config, ".//Suffix")
        if not prefix and not suffix:
            return _placeholder_row(
                tool, ctx, ["The Alteryx Dynamic Rename has no prefix or suffix configured."], reason="mapper_refused"
            )
        return _emit_prefix_suffix_rename(tool, ctx, prefix, suffix, selection)

    if mode in ("rightinputrows", "rightinputmetadata"):
        return _rename_from_right_input(tool, ctx, config, selected or names, mode)

    return _placeholder_row(
        tool,
        ctx,
        [f"Alteryx Dynamic Rename mode '{raw_mode or '(empty)'}' has no Flowfile equivalent."],
        reason="option_unsupported",
    )


def _emit_prefix_suffix_rename(
    tool: AlteryxTool, ctx: EmitContext, prefix: str, suffix: str, selection: dict
) -> ToolReportRow:
    """Alteryx applies prefix and suffix in one tool; Flowfile needs one node per rule."""
    rules = [("prefix", prefix), ("suffix", suffix)]
    node_ids: list[int] = []
    previous_id: int | None = None
    for index, (rename_mode, value) in enumerate([rule for rule in rules if rule[1]]):
        settings = input_schema.NodeDynamicRename(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            dynamic_rename_input=transform_schema.DynamicRenameInput(
                rename_mode=rename_mode, **{rename_mode: value}, **selection
            ),
        )
        node_id = ctx.add_node(
            tool, "dynamic_rename", settings, dx=index * FORMULA_STEP_DX, description=_description(tool)
        )
        node_ids.append(node_id)
        if previous_id is not None:
            _link(ctx, previous_id, node_id)
        previous_id = node_id

    _register_rename_anchors(ctx, tool.tool_id, node_ids[0], node_ids[-1])
    ctx.tool_columns[tool.tool_id] = None
    messages = []
    if len(node_ids) > 1:
        messages.append("Alteryx applies the prefix and the suffix in one tool; Flowfile needs one node for each.")
    return _row(tool, "converted", node_ids, "dynamic_rename", messages, reason="converted")


_CURRENT_FIELD_RE = re.compile(r"\[_CurrentField_\]", re.IGNORECASE)

# Alteryx's FieldType picker, onto Flowfile's readable data-type groups.
_MULTI_FIELD_TYPE_GROUPS: dict[str, str] = {
    "numeric": "Numeric",
    "text": "String",
    "string": "String",
    "bool": "Boolean",
    "boolean": "Boolean",
    "datetime": "Date",
    "date": "Date",
    "time": "Date",
}
_MULTI_FIELD_ALL_TYPES = frozenset({"all", "alltypes", "alltypesof", "any"})
_MULTI_FIELD_UNKNOWN_MESSAGE = (
    "Alteryx would also apply the expression to fields unknown at design time; "
    "Flowfile applies it to the listed columns only."
)
_CURRENT_FIELD_NAME_RE = re.compile(r"\[_CurrentFieldName_\]", re.IGNORECASE)
_CURRENT_FIELD_TYPE_RE = re.compile(r"\[_CurrentFieldType_\]", re.IGNORECASE)
_MULTI_FIELD_TYPE_NAME_MESSAGE = (
    "Alteryx's [_CurrentFieldType_] yields Alteryx type names (V_WString, Double, Bool…); "
    "Flowfile's yields Polars names (String, Float64, Boolean…) — review comparisons against type literals."
)


def _multi_field_selection(
    names: list[str], selected: list[str], unknown_selected: bool, field_type: str
) -> tuple[dict, list[str]]:
    """Map one tool's field picker onto the node's selection settings, plus any caveat messages.

    Alteryx picks fields by data type and then lets the user deselect individual ones, so a
    selection that still covers every named field *and* ``*Unknown`` is the tool's "every field
    of this type" state and becomes the node's data-type (or all-columns) mode. Anything
    narrower becomes an explicit list, which cannot follow fields Alteryx would only have
    discovered at run time — the caveat message says so. An unrecognised ``FieldType`` token
    fails closed to the list rather than guessing a group.
    """
    if unknown_selected and selected == names:
        group = _MULTI_FIELD_TYPE_GROUPS.get(field_type)
        if group is not None:
            return {"selection_mode": "data_type", "selected_data_type": group}, []
        if field_type in _MULTI_FIELD_ALL_TYPES:
            return {"selection_mode": "all"}, []
    return (
        {"selection_mode": "list", "selected_columns": selected},
        [_MULTI_FIELD_UNKNOWN_MESSAGE] if unknown_selected else [],
    )


def _multi_field_add_on(config: ET.Element) -> tuple[str, str]:
    """The prefix/suffix new columns get, read raw because the add-on's leading spaces matter.

    Newer workflows write one ``NewFieldAddOn`` plus a ``NewFieldAddOnPos`` side; older ones
    wrote separate ``OutputPrefix``/``OutputSuffix`` tags.
    """
    found = config.find("NewFieldAddOn")
    if found is None:
        return _raw_text(config, "OutputPrefix"), _raw_text(config, "OutputSuffix")
    add_on = found.text or ""
    return ("", add_on) if _text(config, "NewFieldAddOnPos").lower() == "suffix" else (add_on, "")


def _multi_field_output_type(config: ET.Element) -> tuple[str | None, list[str]]:
    """The Flowfile type the results are cast to, with a message for whatever the mapping loses.

    A declared string output is deliberately not honoured *when the tool's own ``FieldType`` picker
    is already text*. Alteryx pre-fills ``OutputFieldType`` with the selection's own type, so a
    text-mode tool echoes ``V_String`` without meaning "stringify"; casting to text there is a no-op
    when the expression already returns text and an irreversible loss otherwise (Polars cannot cast
    text back to Boolean, so a downstream Select that retypes the column fails). Keeping the produced
    type is always recoverable — a later node can cast to text — so that echo follows the sibling
    Formula mapper and stays ``Auto``. A Numeric/Date/Bool selection declaring ``V_String`` is not an
    echo but a deliberate retype, and is honoured like any other cast.
    """
    if not _is_true(_attribute(config, "ChangeFieldType", "value")):
        return None, []
    declared = _attribute(config, "OutputFieldType", "type")
    if not declared:
        return None, []
    mapped = _map_alteryx_type(declared)
    if mapped is None:
        return None, [
            f"Alteryx output type '{declared}' has no Flowfile equivalent; the type the expression produces is kept."
        ]
    if mapped == "String" and _text(config, "FieldType").lower() in ("text", "string"):
        return None, [
            f"Alteryx writes the result into a '{declared}' field; Flowfile keeps the type the expression produces."
        ]
    if declared.lower() == "fixeddecimal":
        return mapped, ["Alteryx FixedDecimal(size, scale) is stored as Float64; size and scale are not kept."]
    return mapped, []


def _multi_field_scope(selection: dict) -> str:
    if selection["selection_mode"] == "all":
        return "all columns"
    if selection["selection_mode"] == "data_type":
        return f"{selection['selected_data_type']} columns"
    return f"{len(selection['selected_columns'])} column(s)"


def _multi_field_output_columns(known: list[str], selection: dict, prefix: str, suffix: str) -> list[str] | None:
    """Columns leaving the node in new-column mode, or ``None`` when the targets need the schema."""
    if selection["selection_mode"] == "all":
        targets = list(known)
    elif selection["selection_mode"] == "list":
        targets = [name for name in selection["selected_columns"] if name in known]
    else:
        return None
    return [*known, *(f"{prefix}{name}{suffix}" for name in targets)]


def map_multi_field_formula(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    expression = _text(config, "Expression")
    if not expression:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Multi-Field Formula tool has no expression configured."], reason="mapper_refused"
        )
    names, selected, unknown_selected = _field_selection(config)
    if not selected:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Multi-Field Formula tool has no fields selected."], reason="mapper_refused"
        )
    selection, messages = _multi_field_selection(names, selected, unknown_selected, _text(config, "FieldType").lower())
    # [_CurrentFieldName_] binds to a double-quoted literal, which a name holding a double quote
    # has no form for; without this the node imports cleanly and raises when the flow runs.
    names_unchecked = False
    if _CURRENT_FIELD_NAME_RE.search(expression):
        # In data-type or all-columns mode the node also touches columns the tool's own field list
        # never named, so the upstream schema has to be checked too — or the gap has to be
        # admitted. A deselected field puts the node in list mode, where only the list matters.
        in_scope = list(selected)
        if selection["selection_mode"] != "list":
            upstream = ctx.input_columns(tool.tool_id)
            if upstream is None:
                names_unchecked = True
            else:
                in_scope.extend(upstream)
        unquotable = [name for name in dict.fromkeys(in_scope) if '"' in name]
        if unquotable:
            return _placeholder_row(
                tool,
                ctx,
                [
                    f"'{_one_line(expression)}' uses [_CurrentFieldName_], and the field "
                    f"{', '.join(repr(name) for name in unquotable)} contains a double quote, which a "
                    "Flowfile formula cannot write as a string literal."
                ],
                reason="mapper_refused",
            )

    copy_output = _is_true(_attribute(config, "CopyOutput", "value"))
    prefix, suffix = _multi_field_add_on(config)
    if copy_output and not prefix and not suffix:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Multi-Field Formula writes to copied fields whose names are not recorded in the workflow."],
            reason="mapper_refused",
        )

    output_data_type, type_messages = _multi_field_output_type(config)

    outcome = try_translate(expression, allowed_specials=frozenset(transform_schema.MULTI_FIELD_PLACEHOLDERS))
    status: ToolStatus = "converted"
    reason = "converted"
    description = _description(tool)
    formula = outcome.translated
    if formula is None:
        refusal = outcome.reason or "no reason recorded"
        formula = _commented_formula_body(expression, refusal, f"[{transform_schema.MULTI_FIELD_CURRENT_FIELD}]")
        try:
            simple_function_to_expr(formula)
        except Exception:  # the comment body itself is unusable; degrade to a code placeholder
            return _placeholder_row(
                tool,
                ctx,
                [f"'{_one_line(expression)}': {refusal}. Original expression preserved in a placeholder node."],
                reason="translator_refused",
            )
        status, reason = "commented", "translator_refused"
        description = _description(tool, f"{WARNING_PREFIX}Alteryx multi-field formula needs manual conversion")
        messages.append(f"'{_one_line(expression)}': {refusal}. The original expression is kept as a comment.")
        # The stub echoes the input column, so a declared cast would break the identity pass-through.
        if output_data_type is not None:
            messages.append(
                f"Alteryx output type '{_attribute(config, 'OutputFieldType', 'type')}' "
                "is not applied until the expression is rebuilt."
            )
            output_data_type = None
    else:
        messages.extend(type_messages)
        if _CURRENT_FIELD_TYPE_RE.search(expression):
            status, reason = "partial", "option_unsupported"
            messages.append(_MULTI_FIELD_TYPE_NAME_MESSAGE)
        if names_unchecked:
            status, reason = "partial", "option_unsupported"
            messages.append(
                "This formula binds [_CurrentFieldName_] to every column it runs over, including ones "
                "Flowfile cannot see from the workflow; a column name containing a double quote cannot "
                "be written as a literal and will stop the flow when it runs."
            )

    settings = input_schema.NodeMultiFieldFormula(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        multi_field_formula_input=transform_schema.MultiFieldFormulaInput(
            formula=formula,
            output_mode="new" if copy_output else "replace",
            output_prefix=prefix,
            output_suffix=suffix,
            output_data_type=output_data_type or transform_schema.AUTO_DATA_TYPE,
            **selection,
        ),
    )
    node_id = ctx.add_node(tool, "multi_field_formula", settings, description=description)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.register_all_outputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    ctx.tool_columns[tool.tool_id] = (
        _multi_field_output_columns(known, selection, prefix, suffix) if copy_output and known is not None else known
    )
    messages.insert(0, f"Mapped onto one multi-field formula node over {_multi_field_scope(selection)}.")
    return _row(tool, status, [node_id], "multi_field_formula", messages, reason=reason)


_REGEX_UNSUPPORTED = (("(?=", "lookahead"), ("(?!", "negative lookahead"), ("(?<", "lookbehind"))
_REGEX_BACKREF_RE = re.compile(r"\\[1-9]")
_DUNDER_RE = re.compile(r"__\w+__")
_REPLACEMENT_GROUP_RE = re.compile(r"\$(\d+)")


def _regex_pattern(config: ET.Element) -> tuple[str, str | None]:
    """The tool's regex, rejected when it uses constructs the Rust regex engine has no support for."""
    pattern = _attribute(config, "RegExExpression", "value")
    if not pattern:
        return "", "the Alteryx RegEx tool has no expression configured"
    for token, label in _REGEX_UNSUPPORTED:
        if token in pattern:
            return "", f"the Alteryx regular expression uses {label}, which Polars' regex engine does not support"
    if _REGEX_BACKREF_RE.search(pattern):
        return "", "the Alteryx regular expression uses a backreference, which Polars' regex engine does not support"
    if _DUNDER_RE.search(pattern):
        return "", "the Alteryx regular expression contains a dunder pattern, which the Polars code node rejects"
    if _is_true(_attribute(config, "CaseInsensitve", "value")):
        pattern = f"(?i){pattern}"
    return pattern, None


def _regex_output_names(config: ET.Element, method: str, column: str) -> tuple[list[str], str | None]:
    if method == "parsecomplex":
        names = [element.get("field") or "" for element in config.findall("ParseComplex/Field")]
        if not all(names):
            return [], "the Alteryx RegEx tool has unnamed output fields"
        return names, None
    if _is_true(_attribute(config, "ParseSimple/SplitToRows", "value")):
        return [], "Alteryx RegEx 'split to rows' parsing has no verified Polars translation"
    try:
        count = int(float(_attribute(config, "ParseSimple/NumFields", "value") or "0"))
    except ValueError:
        return [], "the Alteryx RegEx output field count could not be read"
    if count < 1:
        return [], "the Alteryx RegEx tool parses into no output fields"
    root = _text(config, "ParseSimple/RootName") or column
    return [f"{root}{index + 1}" for index in range(count)], None


def _count_capture_groups(pattern: str) -> int:
    """Count marked groups: unescaped ``(`` outside a character class that does not open ``(?...)``."""
    count = 0
    escaped = False
    in_class = False
    for index, char in enumerate(pattern):
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif in_class:
            in_class = char != "]"
        elif char == "[":
            in_class = True
        elif char == "(" and not pattern.startswith("(?", index):
            count += 1
    return count


def _tokenize_code(config: ET.Element, column: str) -> tuple[str, str | None]:
    """Generate the Polars body for Alteryx's Tokenize (``ParseSimple``) method.

    Tokenize is not capture-group extraction: the expression describes the tokens themselves and
    every match becomes one output column. A single marked group narrows what each match
    contributes, so the matches are re-matched to pull that group out; several marked groups have
    no one-token-per-match meaning we can reproduce, so they are rejected instead of guessed at.
    """
    names, reason = _regex_output_names(config, "parsesimple", column)
    if reason is not None:
        return "", reason
    groups = _count_capture_groups(_attribute(config, "RegExExpression", "value"))
    if groups > 1:
        return "", "the Alteryx RegEx tokenize expression marks more than one group"
    tokens = f"pl.col({column!r}).str.extract_all(_pattern)"
    if groups == 1:
        tokens = f"{tokens}.list.eval(pl.element().str.extract(_pattern, 1))"
    picks = ",\n".join(
        f"    _tokens.list.get({index}, null_on_oob=True).alias({name!r})" for index, name in enumerate(names)
    )
    return f"_tokens = {tokens}\noutput_df = input_df.with_columns(\n{picks},\n)", None


def _regex_code(config: ET.Element, method: str, column: str) -> tuple[str, str | None]:
    """Generate the Polars body for one RegEx method, or explain why it cannot be generated.

    The pattern itself reaches the generated code through the ``_pattern`` variable, so it is
    not an argument here.
    """
    source = f"pl.col({column!r})"
    if method == "parsecomplex":
        names, reason = _regex_output_names(config, method, column)
        if reason is not None:
            return "", reason
        extracts = ",\n".join(
            f"    {source}.str.extract(_pattern, {index + 1}).alias({name!r})" for index, name in enumerate(names)
        )
        return f"output_df = input_df.with_columns(\n{extracts},\n)", None
    if method == "parsesimple":
        return _tokenize_code(config, column)
    if method == "match":
        target = _text(config, "Match/Field")
        if not target:
            return "", "the Alteryx RegEx match output field has no name"
        return f"output_df = input_df.with_columns({source}.str.contains(_pattern).alias({target!r}))", None
    if method == "replace":
        replacement = _REPLACEMENT_GROUP_RE.sub(r"${\1}", _attribute(config, "Replace", "expression"))
        if _DUNDER_RE.search(replacement):
            return "", "the Alteryx RegEx replacement contains a dunder pattern, which the Polars code node rejects"
        replaced = f"{source}.str.replace_all(_pattern, {replacement!r})"
        if _is_true(_attribute(config, "Replace/CopyUnmatched", "value")):
            body = replaced
        else:
            body = f"pl.when({source}.str.contains(_pattern)).then({replaced}).otherwise(None)"
        return f"output_df = input_df.with_columns({body}.alias({column!r}))", None
    return "", f"Alteryx RegEx method '{method}' has no verified Polars translation"


def map_regex(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    column = _text(config, "Field")
    method = _normalise_mode(_text(config, "Method"))
    pattern, reason = _regex_pattern(config)
    if not column:
        reason = "the Alteryx RegEx tool names no input field"
    if reason is None:
        code, reason = _regex_code(config, method, column)
    if reason is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx RegEx tool could not be converted: {reason}."], reason="option_unsupported"
        )
    screened: dict[str, str | list[str]] = {
        "the field": column,
        "the pattern": pattern,
        "an output column": _regex_added_columns(config, method, column),
    }
    if method == "replace":
        screened["the replacement"] = _attribute(config, "Replace", "expression")
    refusal = _backslash_refusal(screened)
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")

    header = [
        f"# Alteryx RegEx (ToolID {tool.tool_id}) translated to Polars; check the result against Alteryx.",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"_pattern = {pattern!r}",
    ]
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code="\n".join([*header, code])),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)

    known = ctx.input_columns(tool.tool_id)
    added = _regex_added_columns(config, method, column)
    ctx.tool_columns[tool.tool_id] = [*known, *[name for name in added if name not in known]] if known else None
    messages = [
        "The Alteryx RegEx tool became generated Polars code; Alteryx and Polars regex dialects differ, "
        "so verify the output before relying on it."
    ]
    if method == "parsesimple":
        messages.append(
            f"Tokenize splits every match of the expression in '{column}' across {len(added)} columns "
            f"({', '.join(added)}); '{column}' itself is kept."
        )
    return _row(tool, "partial", [node_id], "polars_code", messages, reason="option_unsupported")


def _regex_added_columns(config: ET.Element, method: str, column: str) -> list[str]:
    if method in ("parsecomplex", "parsesimple"):
        return _regex_output_names(config, method, column)[0]
    if method == "match":
        return [name for name in [_text(config, "Match/Field")] if name]
    return []


def _sort_fields(config: ET.Element) -> list[transform_schema.SortByInput]:
    """The Sort tool's keys; an empty list is the one thing that stops it becoming a sort node."""
    return [
        transform_schema.SortByInput(
            column=element.get("field") or "",
            how="desc" if (element.get("order") or "").lower().startswith("desc") else "asc",
        )
        for element in config.findall("SortInfo/Field")
        if element.get("field")
    ]


def map_sort(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    sort_input = _sort_fields(_config(tool))
    if not sort_input:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Sort tool has no sort fields configured."], reason="mapper_refused"
        )
    settings = input_schema.NodeSort(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), sort_input=sort_input)
    node_id = ctx.add_node(tool, "sort", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "sort", [], reason="converted")


def map_summarize(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    agg_cols: list[transform_schema.AggColl] = []
    unmapped: list[str] = []
    for element in config.findall("SummarizeFields/SummarizeField"):
        name = element.get("field") or ""
        action = (element.get("action") or "").strip()
        mapped = _SUMMARIZE_ACTIONS.get(action.lower())
        if not name:
            continue
        if mapped is None:
            unmapped.append(f"{action or '(empty)'} on {name}")
            continue
        agg_cols.append(transform_schema.AggColl(name, mapped, element.get("rename") or None))
    if unmapped:
        return _placeholder_row(
            tool,
            ctx,
            ["Unsupported Alteryx Summarize actions: " + ", ".join(unmapped)],
            reason="option_unsupported",
        )
    if not agg_cols:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Summarize tool has no aggregations configured."], reason="mapper_refused"
        )

    settings = input_schema.NodeGroupBy(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        groupby_input=transform_schema.GroupByInput(agg_cols=agg_cols),
    )
    node_id = ctx.add_node(tool, "group_by", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [agg.new_name for agg in agg_cols]
    return _row(tool, "converted", [node_id], "group_by", [], reason="converted")


def map_sample(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    mode = _text(config, "Mode") or "First"
    group_fields = config.find("GroupFields")
    if mode.lower() != "first":
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Sample mode '{mode}' has no Flowfile equivalent; only 'First N' is converted."],
            reason="option_unsupported",
        )
    if group_fields is not None and len(list(group_fields)) > 0:
        return _placeholder_row(
            tool, ctx, ["Grouped Alteryx sampling has no Flowfile equivalent."], reason="option_unsupported"
        )
    try:
        size = int(float(_text(config, "N") or "1"))
    except ValueError:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Sample record count could not be read."], reason="mapper_refused"
        )

    settings = input_schema.NodeSample(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), sample_method="first", sample_size=size
    )
    node_id = ctx.add_node(tool, "sample", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "sample", [], reason="converted")


def map_unique(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    columns = [element.get("field") for element in config.findall("UniqueFields/Field") if element.get("field")]
    settings = input_schema.NodeUnique(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        unique_input=transform_schema.UniqueInput(columns=columns or None, strategy="first"),
    )
    node_id = ctx.add_node(tool, "unique", settings, description=_description(tool))
    ctx.register_output(tool.tool_id, DEFAULT_OUTPUT_ANCHOR, node_id)
    ctx.register_output(tool.tool_id, "Unique", node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)

    node_ids = [node_id]
    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if ctx.has_outgoing(tool.tool_id, "Dupes"):
        status, reason = "partial", "option_unsupported"
        messages.append(
            "The Alteryx duplicates (D) output has no Flowfile equivalent; "
            "a passthrough placeholder now feeds that branch with the unfiltered input."
        )
        dupes_id = emit_placeholder(
            tool,
            ctx,
            ["This branch should contain the duplicate rows dropped by the Unique node."],
            dy=ANTI_DY,
            register_anchors=False,
        )
        ctx.register_output(tool.tool_id, "Dupes", dupes_id)
        ctx.register_all_inputs(tool.tool_id, dupes_id)
        node_ids.append(dupes_id)
    return _row(tool, status, node_ids, "unique", messages, reason=reason)


# The Text To Columns help, quoted verbatim in the corpus workflow's own comments, documents
# these three escapes as single characters — `\s` is a space, not a whitespace class.
_T2C_DELIMITER_ESCAPES = {"s": " ", "t": "\t", "n": "\n", "\\": "\\"}
_T2C_EXTRA_IN_LAST = "last"


def _text_to_columns_delimiters(raw: str) -> tuple[list[str], str | None]:
    """The distinct characters Alteryx's delimiter box means, or why it could not be read.

    Every character in the box is a delimiter in its own right, so the box is a character set
    rather than a separator string; ``\\s``, ``\\t`` and ``\\n`` name one character each.
    """
    characters: list[str] = []
    index = 0
    while index < len(raw):
        character = raw[index]
        if character != "\\":
            characters.append(character)
            index += 1
            continue
        escape = raw[index + 1 : index + 2]
        if escape not in _T2C_DELIMITER_ESCAPES:
            return [], f"the delimiter escape '\\{escape}' is not one Alteryx documents"
        characters.append(_T2C_DELIMITER_ESCAPES[escape])
        index += 2
    distinct = list(dict.fromkeys(characters))
    return distinct, None if distinct else "the tool has no delimiter configured"


def _t2c_collapse_expression(column: str, characters: list[str]) -> tuple[str, str]:
    """A Polars expression reducing every delimiter to the first one, plus that separator.

    Polars splits on a literal, not a character set, so the other delimiters are rewritten to
    the first one. Replacing a delimiter with a delimiter cannot merge two fields, because every
    occurrence of either character was already a split point.
    """
    separator = characters[0]
    expression = f"pl.col({column!r})"
    for other in characters[1:]:
        expression += f".str.replace_all({other!r}, {separator!r}, literal=True)"
    return expression, separator


def _text_to_columns_rows_code(tool: AlteryxTool, column: str, characters: list[str]) -> str:
    """Split to rows on a set of delimiters; the split column keeps its name and position."""
    expression, separator = _t2c_collapse_expression(column, characters)
    return "\n".join(
        [
            f"# Alteryx Text To Columns (ToolID {tool.tool_id}): split {column!r} to rows on any of "
            f"{''.join(characters)!r}.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            f"output_df = input_df.with_columns({expression}.str.split({separator!r})).explode({column!r})",
        ]
    )


def _text_to_columns_columns_code(tool: AlteryxTool, column: str, characters: list[str], names: list[str]) -> str:
    """Split to columns: ``splitn`` leaves extra text in the last field, as ErrorHandling='Last' asks.

    The new columns take the split column's place, which is where Alteryx puts them; the source
    column itself does not survive, so the generated select rebuilds the order at run time.
    """
    expression, separator = _t2c_collapse_expression(column, characters)
    return "\n".join(
        [
            f"# Alteryx Text To Columns (ToolID {tool.tool_id}): split {column!r} into {len(names)} columns on any of "
            f"{''.join(characters)!r}; extra text stays in the last column.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            f"_names = {names!r}",
            "_order = []",
            "for _column in input_df.collect_schema().names():",
            f"    if _column == {column!r}:",
            "        _order.extend(_names)",
            "    else:",
            "        _order.append(_column)",
            f"_split = {expression}.str.splitn({separator!r}, {len(names)})"
            ".struct.rename_fields(_names).alias('_alteryx_split')",
            f"output_df = input_df.with_columns(_split).drop({column!r}).unnest('_alteryx_split').select(_order)",
        ]
    )


def map_text_to_columns(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    column = _text(config, "Field")
    if not column:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Text To Columns tool has no field to split."], reason="mapper_refused"
        )

    flags = _attribute(config, "Flags", "value", "0")
    if flags not in ("", "0"):
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Text To Columns tool uses advanced options (Flags={flags}) that are not supported."],
            reason="option_unsupported",
        )

    delimiters_element = config.find("Delimeters")
    raw_delimiters = (delimiters_element.get("value") if delimiters_element is not None else "") or ""
    characters, refusal = _text_to_columns_delimiters(raw_delimiters)
    if refusal is not None:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Text To Columns delimiters could not be read: {refusal}."],
            reason="option_unsupported",
        )

    # <NumFields> is the mode: 1 means split to rows, anything larger is that many columns.
    raw_count = _attribute(config, "NumFields", "value")
    try:
        count = int(raw_count)
    except ValueError:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Text To Columns number of columns could not be read ('{raw_count}')."],
            reason="mapper_refused",
        )
    if count < 1:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Text To Columns number of columns is {count}, which is not a split."],
            reason="mapper_refused",
        )

    if count == 1:
        return _emit_text_to_rows(tool, ctx, column, characters, _text(config, "RootName"))
    return _emit_text_to_columns(tool, ctx, config, column, characters, count)


def _emit_text_to_rows(
    tool: AlteryxTool, ctx: EmitContext, column: str, characters: list[str], root_name: str
) -> ToolReportRow:
    """Split to rows: the native node for one delimiter, generated code for a set of them.

    The split column keeps its own name whichever branch runs. Alteryx's help, quoted in its own
    Text To Columns example, says split-to-rows leaves "the output columns the same as the input
    columns", and none of the rows-mode configurations carries a ``<RootName>`` at all — it is a
    split-to-columns setting. One that is present and different is reported, never applied.
    """
    messages: list[str] = []
    if root_name and root_name != column:
        messages.append(
            f"Alteryx splits '{column}' into rows under its own name; the tool's output root name "
            f"'{_one_line(root_name)}' applies to split-to-columns only and was not used."
        )
    if len(characters) > 1:
        refusal = _backslash_refusal({"the field": column, "a delimiter": characters})
        if refusal is not None:
            return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
        settings = input_schema.NodePolarsCode(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=_text_to_columns_rows_code(tool, column, characters)
            ),
        )
        node_type = "polars_code"
    else:
        settings = input_schema.NodeTextToRows(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            text_to_rows_input=transform_schema.TextToRowsInput(
                column_to_split=column,
                output_column_name=None,
                split_by_fixed_value=True,
                split_fixed_value=characters[0],
            ),
        )
        node_type = "text_to_rows"
    node_id = ctx.add_node(tool, node_type, settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], node_type, messages, reason="converted")


def _emit_text_to_columns(
    tool: AlteryxTool, ctx: EmitContext, config: ET.Element, column: str, characters: list[str], count: int
) -> ToolReportRow:
    """Split to columns: ``<RootName>1..N``, replacing the split column as Alteryx does."""
    error_handling = _text(config, "ErrorHandling")
    if error_handling.lower() != _T2C_EXTRA_IN_LAST:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"Alteryx Text To Columns handles extra characters as '{error_handling or 'not set'}'; "
                "only 'Last' (leave the extra text in the last column) is converted."
            ],
            reason="option_unsupported",
        )

    root_name = _text(config, "RootName") or column
    names = [f"{root_name}{index}" for index in range(1, count + 1)]
    refusal = _backslash_refusal({"the field": column, "a delimiter": characters, "an output column": names})
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_text_to_columns_columns_code(tool, column, characters, names)
        ),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    if known is None:
        ctx.tool_columns[tool.tool_id] = None
    else:
        ctx.tool_columns[tool.tool_id] = [
            name for existing in known for name in (names if existing == column else [existing])
        ]
    message = f"'{column}' became {count} columns ({', '.join(names)}) and does not survive, as in Alteryx."
    return _row(tool, "converted", [node_id], "polars_code", [message], reason="converted")


def map_union(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    mode = _text(_config(tool), "Mode") or "ByName"
    settings = input_schema.NodeUnion(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        union_input=transform_schema.UnionInput(mode="relaxed"),
    )
    node_id = ctx.add_node(tool, "union", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = None

    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if mode.lower() != "byname":
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"Alteryx union mode '{mode}' was converted to Flowfile's name-based union; verify the column order."
        )
    return _row(tool, status, [node_id], "union", messages, reason=reason)


_JOIN_UNKNOWN_FIELD = "*Unknown"
_JOIN_SIDE_PREFIXES = (("left", "Left_"), ("right", "Right_"))


def _declared_output_columns(tool: AlteryxTool) -> list[str] | None:
    """The columns a tool states without being mapped: Alteryx's cached schema, else a Text Input's own fields."""
    if tool.output_fields:
        return list(tool.output_fields)
    if tool.tool_name == "TextInput":
        names = [element.get("name") for element in _config(tool).findall("Fields/Field")]
        return [name for name in names if name] or None
    return None


def _anchor_columns(ctx: EmitContext, tool_id: int, anchor: str) -> list[str] | None:
    """Columns arriving on one anchor, including from a tool that has not been mapped yet.

    Mapping runs in document order, which is not topological, so a join can be reached before
    the tool feeding it. A tool that *has* been mapped is authoritative even when it answers
    "unknown"; only an unmapped one falls back to the schema it declares.
    """
    for connection in ctx.inbound.get(tool_id, []):
        if connection.dest_anchor != anchor:
            continue
        key = ctx.resolve_output(connection.origin_tool_id, connection.origin_anchor)
        if key is None:
            return None
        if key in ctx.anchor_columns:
            return ctx.anchor_columns[key]
        if key[0] in ctx.tool_columns:
            return ctx.tool_columns[key[0]]
        source = ctx.tools.get(key[0])
        return _declared_output_columns(source) if source is not None else None
    return None


def _join_by_record_position(config: ET.Element) -> bool:
    """Alteryx writes this as an attribute on ``<Configuration>``; the element form is a fixture shape."""
    return _is_true(config.get("joinByRecordPos")) or _is_true(_attribute(config, "JoinByRecordPos", "value"))


def _resolve_join_field(field: str, left_columns: list[str], right_columns: list[str]) -> tuple[str, str] | None:
    """``(side, column)`` for one Alteryx join SelectField, or None when it names no known column.

    Alteryx prefixes a field with ``Left_``/``Right_`` only when the bare name also exists on the
    other side, so a column that really is called ``Right_id`` has to win over the prefix reading.
    A name on both sides unprefixed is the left one, which is why left is tried first.
    """
    for side, columns in (("left", left_columns), ("right", right_columns)):
        if field in columns:
            return side, field
    for side, prefix in _JOIN_SIDE_PREFIXES:
        columns = left_columns if side == "left" else right_columns
        if field.startswith(prefix) and field[len(prefix) :] in columns:
            return side, field[len(prefix) :]
    return None


def _join_select_config(
    config: ET.Element, left_columns: list[str] | None, right_columns: list[str] | None, anchor: str = "Join"
) -> tuple[list[transform_schema.SelectInput], list[transform_schema.SelectInput], str | None]:
    """Alteryx's join field selection as Flowfile select lists, or a reason it was not converted.

    The lists are emitted in full and in upstream order because Flowfile appends columns the
    selection does not mention, which would otherwise reorder the output. Alteryx's own output
    name — the one downstream tools reference — is kept verbatim, prefix included.
    """
    selection = config.find(f"SelectConfiguration/Configuration[@outputConnection='{anchor}']")
    if selection is None:
        return [], [], None

    entries: list[tuple[str, str, bool]] = []
    for element in selection.findall("SelectFields/SelectField"):
        field = element.get("field") or ""
        selected = _is_true(element.get("selected"))
        if field == _JOIN_UNKNOWN_FIELD:
            if not selected:
                return [], [], "the selection drops every field it does not list, which Flowfile cannot express"
            continue
        entries.append((field, element.get("rename") or field, selected))
    if not entries:
        # Nothing is renamed or dropped, which is already what a Flowfile join does.
        return [], [], None
    if left_columns is None or right_columns is None:
        return [], [], "Flowfile could not tell which input each selected field comes from"

    chosen: dict[tuple[str, str], tuple[str, bool]] = {}
    for field, new_name, selected in entries:
        resolved = _resolve_join_field(field, left_columns, right_columns)
        if resolved is None:
            return [], [], f"the selected field '{field}' does not match a column on either input"
        chosen[resolved] = (new_name, selected)

    selects: dict[str, list[transform_schema.SelectInput]] = {"left": [], "right": []}
    for side, columns in (("left", left_columns), ("right", right_columns)):
        for column in columns:
            new_name, keep = chosen.get((side, column), (column, True))
            selects[side].append(transform_schema.SelectInput(old_name=column, new_name=new_name, keep=keep))
    return selects["left"], selects["right"], None


def _join_settings(
    ctx: EmitContext,
    mapping: list[tuple[str, str]],
    how: str,
    swap: bool,
    left_select: list[transform_schema.SelectInput] | None = None,
    right_select: list[transform_schema.SelectInput] | None = None,
) -> input_schema.NodeJoin:
    join_mapping = [
        transform_schema.JoinMap(left_col=right, right_col=left)
        if swap
        else transform_schema.JoinMap(left_col=left, right_col=right)
        for left, right in mapping
    ]
    return input_schema.NodeJoin(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        join_input=transform_schema.JoinInput(
            join_mapping=join_mapping,
            left_select=transform_schema.JoinInputs(renames=left_select or []),
            right_select=transform_schema.JoinInputs(renames=right_select or []),
            how=how,
        ),
    )


def map_join(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    left_fields = [
        element.get("field") for element in config.findall("JoinInfo[@connection='Left']/Field") if element.get("field")
    ]
    right_fields = [
        element.get("field")
        for element in config.findall("JoinInfo[@connection='Right']/Field")
        if element.get("field")
    ]
    if _join_by_record_position(config):
        return _placeholder_row(
            tool, ctx, ["Alteryx 'join by record position' has no Flowfile equivalent."], reason="option_unsupported"
        )
    if not left_fields or len(left_fields) != len(right_fields):
        return _placeholder_row(
            tool, ctx, ["The Alteryx join keys could not be read as matching pairs."], reason="mapper_refused"
        )

    mapping = list(zip(left_fields, right_fields, strict=True))
    wants_join = ctx.has_outgoing(tool.tool_id, "Join")
    wants_left = ctx.has_outgoing(tool.tool_id, "Left")
    wants_right = ctx.has_outgoing(tool.tool_id, "Right")
    if not (wants_join or wants_left or wants_right):
        wants_join = True

    left_columns = _anchor_columns(ctx, tool.tool_id, "Left")
    right_columns = _anchor_columns(ctx, tool.tool_id, "Right")
    left_select, right_select, select_refusal = _join_select_config(config, left_columns, right_columns)

    node_ids: list[int] = []
    messages: list[str] = []
    if wants_join:
        inner = _join_settings(ctx, mapping, "inner", swap=False, left_select=left_select, right_select=right_select)
        inner_id = ctx.add_node(tool, "join", inner, description=_description(tool))
        ctx.register_output(tool.tool_id, DEFAULT_OUTPUT_ANCHOR, inner_id)
        ctx.register_output(tool.tool_id, "Join", inner_id)
        ctx.register_input(tool.tool_id, DEFAULT_INPUT_ANCHOR, inner_id, MAIN)
        ctx.register_input(tool.tool_id, "Left", inner_id, MAIN)
        ctx.register_input(tool.tool_id, "Right", inner_id, RIGHT)
        node_ids.append(inner_id)
    if wants_left:
        anti_left = _join_settings(ctx, mapping, "anti", swap=False)
        anti_left_id = ctx.add_node(tool, "join", anti_left, dx=ANTI_DX, dy=-ANTI_DY, description=_description(tool))
        ctx.register_output(tool.tool_id, "Left", anti_left_id)
        ctx.register_input(tool.tool_id, "Left", anti_left_id, MAIN)
        ctx.register_input(tool.tool_id, "Right", anti_left_id, RIGHT)
        node_ids.append(anti_left_id)
        messages.append("The unmatched-left (L) output became an anti join.")
    if wants_right:
        anti_right = _join_settings(ctx, mapping, "anti", swap=True)
        anti_right_id = ctx.add_node(tool, "join", anti_right, dx=ANTI_DX, dy=ANTI_DY, description=_description(tool))
        ctx.register_output(tool.tool_id, "Right", anti_right_id)
        ctx.register_input(tool.tool_id, "Right", anti_right_id, MAIN)
        ctx.register_input(tool.tool_id, "Left", anti_right_id, RIGHT)
        node_ids.append(anti_right_id)
        messages.append("The unmatched-right (R) output became an anti join with the inputs swapped.")

    ctx.tool_columns[tool.tool_id] = _join_output_columns(left_select, right_select) if left_select else None
    # The L and R anchors are anti joins: each passes one input through untouched, so a consumer
    # wired to them must not be told the inner join's projection.
    if wants_left:
        ctx.anchor_columns[(tool.tool_id, "Left")] = left_columns
    if wants_right:
        ctx.anchor_columns[(tool.tool_id, "Right")] = right_columns

    status: ToolStatus = "converted"
    reason = "converted"
    if select_refusal is not None:
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"The Alteryx join's field selection and renames were not converted because {select_refusal}; "
            "Flowfile keeps every column from both inputs."
        )
    else:
        misplaced = _join_kept_right_keys(mapping, right_select)
        if misplaced:
            status, reason = "partial", "option_unsupported"
            messages.append(
                f"Alteryx puts the kept join key(s) {', '.join(misplaced)} where the right input's columns "
                "start; Flowfile's join appends them after the other columns, so the values match but the "
                "column order does not."
            )
    if messages and status == "converted":
        status, reason = "partial", "option_unsupported"
    return _row(tool, status, node_ids, "join", messages, reason=reason)


def _join_kept_right_keys(
    mapping: list[tuple[str, str]], right_select: list[transform_schema.SelectInput]
) -> list[str]:
    """Right-side join keys the selection keeps, which Flowfile's join can only place last."""
    keys = {right for _left, right in mapping}
    return [select.new_name for select in right_select if select.keep and select.old_name in keys]


def _join_output_columns(
    left_select: list[transform_schema.SelectInput], right_select: list[transform_schema.SelectInput]
) -> list[str]:
    return [select.new_name for select in [*left_select, *right_select] if select.keep]


def _file_element_path(config: ET.Element) -> tuple[str, str]:
    """Return (path, table) for an Alteryx File element, splitting the ``|||`` suffix off unchanged.

    The suffix is not always a worksheet, so it is handed on verbatim for ``_excel_sheet`` to read.
    """
    element = config.find("File")
    raw = (element.text or "").strip() if element is not None and element.text else ""
    if "|||" in raw:
        path, _, table = raw.partition("|||")
        return path.strip(), table.strip()
    return raw, ""


EXCEL_SHEET_SUFFIX = "$"
EXCEL_SHEET_LIST = "<List of Sheet Names>"


def _excel_sheet(table: str) -> tuple[str, str | None]:
    """The worksheet an Alteryx Excel path names, or why Flowfile cannot read what it points at.

    Alteryx quotes the name in backticks and marks a worksheet with the trailing ``$`` the Excel
    drivers use; without that marker the name is a named range, which Flowfile cannot address.
    """
    if table == EXCEL_SHEET_LIST:
        return "", "it reads the workbook's list of sheet names rather than the data in a sheet"
    name = table.strip("`")
    if not name:
        return "", None
    if not name.endswith(EXCEL_SHEET_SUFFIX):
        return "", (
            f"'{name}' is an Excel named range, and Flowfile's Excel reader or writer only addresses whole sheets"
        )
    return name[: -len(EXCEL_SHEET_SUFFIX)], None


YXDB_EXTENSION = "yxdb"
YXDB_CONVERT_COMMAND = "flowfile convert yxdb"


def _parquet_sibling(path: str, filename: str) -> tuple[str, str]:
    """The ``<stem>.parquet`` path beside a ``.yxdb``, keeping the workflow's own separators."""
    parquet_name = f"{filename[: -len(YXDB_EXTENSION) - 1]}.parquet"
    return path[: len(path) - len(filename)] + parquet_name, parquet_name


def _map_yxdb_input(tool: AlteryxTool, ctx: EmitContext, path: str, directory: str, filename: str) -> ToolReportRow:
    """Read the Parquet sibling that ``flowfile convert yxdb`` writes next to a ``.yxdb``.

    The importer never parses ``.yxdb`` itself — an upload cannot reach the user's data, so
    conversion is a CLI step on the machine that holds it. The node is pointed at the file that
    step produces and stays ``partial`` because nothing here can prove the file exists yet.
    """
    parquet_path, parquet_name = _parquet_sibling(path, filename)
    received = input_schema.ReceivedTable.create_from_path(parquet_path, file_type="parquet")
    received.name = parquet_name
    received.directory = directory or None
    if _is_foreign_absolute_path(parquet_path):
        received.abs_file_path = parquet_path

    settings = input_schema.NodeRead(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), received_file=received)
    warning = f"{WARNING_PREFIX}Reads the Parquet copy of '{filename}'"
    node_id = ctx.add_node(tool, "read", settings, description=_description(tool, warning), is_start_node=True)
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = tool.output_fields or None
    safe_source = _safe_path(path)
    messages = [
        f"Alteryx read '{safe_source}'. Flowfile does not open .yxdb files, so this node reads "
        f"'{parquet_name}' beside it instead.",
        f'Create that file once on the machine holding the data: {YXDB_CONVERT_COMMAND} "{safe_source}"',
    ]
    return _row(tool, "partial", [node_id], "read", messages, reason="file_format")


def map_file_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    path, _sheet = _file_element_path(_config(tool))
    return _flag_connection_source(tool, path, _emit_file_input(tool, ctx))


def _emit_file_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    path, table = _file_element_path(config)
    if not path or _carries_credentials(path):
        return _placeholder_row(
            tool,
            ctx,
            ["This Alteryx Input Data tool reads from a database or connection, not a file."],
            reason="connection_string",
        )
    directory, filename = _split_path(path)
    if _extension(filename) == YXDB_EXTENSION:
        return _map_yxdb_input(tool, ctx, path, directory, filename)
    file_type = _READ_FILE_TYPES.get(_extension(filename))
    if file_type is None:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Input Data tool reads {_safe_extension(filename)}, which Flowfile does not support."],
            reason="file_format",
        )

    sheet = ""
    if file_type == "excel" and table:
        sheet, refusal = _excel_sheet(table)
        if refusal is not None:
            return _placeholder_row(
                tool,
                ctx,
                [f"This Alteryx Input Data tool was not converted because {refusal}."],
                reason="option_unsupported",
            )

    received = input_schema.ReceivedTable.create_from_path(path, file_type=file_type)
    received.name = filename
    received.directory = directory or None
    if _is_foreign_absolute_path(path):
        # Resolving a foreign path against this machine's cwd would invent a path that points nowhere.
        received.abs_file_path = path
    if sheet:
        received.table_settings.sheet_name = sheet
    if file_type in ("csv", "json"):
        delimiter = _text(config, "FormatSpecificOptions/Delimeter")
        if len(delimiter) == 1:
            received.table_settings.delimiter = delimiter
        has_headers = _flag(config, "FormatSpecificOptions/HeaderRow")
        if has_headers is not None:
            received.table_settings.has_headers = has_headers

    settings = input_schema.NodeRead(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), received_file=received)
    node_id = ctx.add_node(tool, "read", settings, description=_description(tool), is_start_node=True)
    node_ids = [node_id]
    messages: list[str] = []
    if _is_foreign_absolute_path(path):
        messages.append(
            f"The workflow reads from '{_safe_path(path)}'; repoint this node at your own copy of the file."
        )

    headerless = received.table_settings.has_headers is False
    if headerless and tool.output_fields:
        # Without this rename, references to Alteryx's Field_N names silently miss Polars' column_N.
        rename_id = _emit_positional_header_rename(tool, ctx, tool.output_fields)
        node_ids.append(rename_id)
        _link(ctx, node_id, rename_id)
        node_id = rename_id
        messages.append(
            "The file is read without headers, so a Select node renames Polars' "
            f"column_1..column_{len(tool.output_fields)} to the Alteryx names "
            f"({tool.output_fields[0]}, ...)."
        )
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = tool.output_fields or None
    return _row(tool, "converted", node_ids, "read", messages, reason="converted")


def _emit_positional_header_rename(tool: AlteryxTool, ctx: EmitContext, names: list[str]) -> int:
    select_input = [
        transform_schema.SelectInput(old_name=f"column_{index + 1}", new_name=name, keep=True)
        for index, name in enumerate(names)
    ]
    settings = input_schema.NodeSelect(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), keep_missing=True, select_input=select_input
    )
    return ctx.add_node(tool, "select", settings, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)


_ENVIRONMENT_PREFIX_RE = re.compile(r"^%([^%\\/]+)%")


def _strip_environment_prefix(path: str) -> tuple[str, str]:
    """Split a leading Windows environment variable (``%temp%out.yxdb``) off a target path."""
    match = _ENVIRONMENT_PREFIX_RE.match(path)
    if match is None:
        return path, ""
    return path[match.end() :].lstrip("\\/"), match.group(1)


def map_file_output(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    path, _sheet = _file_element_path(_config(tool))
    return _flag_connection_source(tool, path, _emit_file_output(tool, ctx))


def _emit_file_output(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    original_path, table = _file_element_path(config)
    if not original_path or _carries_credentials(original_path):
        return _placeholder_row(
            tool,
            ctx,
            ["This Alteryx Output Data tool writes to a database or connection, not a file."],
            reason="connection_string",
        )
    if _is_true(_attribute(config, "MultiFile", "value")):
        split_field = _text(config, "MultiFileField")
        target = f"one file per value of '{split_field}'" if split_field else "one file per group"
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Output Data tool writes {target}; a Flowfile output node writes a single file."],
            reason="option_unsupported",
        )

    path, environment_variable = _strip_environment_prefix(original_path)
    directory, filename = _split_path(path)
    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if _extension(filename) == YXDB_EXTENSION:
        filename = f"{filename[: -len(YXDB_EXTENSION) - 1]}.parquet"
        status, reason = "partial", "file_format"
        messages.append(
            f"Alteryx wrote '{_safe_path(original_path)}'. Flowfile does not write .yxdb, so this node writes "
            f"'{filename}' instead; anything downstream in Alteryx has to read the Parquet file."
        )
    if environment_variable:
        status = "partial"
        # the format change is the bigger cause, so it keeps the slug when both apply
        reason = "option_unsupported" if reason == "converted" else reason
        messages.append(
            f"The Alteryx path started with the Windows variable '%{environment_variable}%'; "
            "set the target folder on this node before running."
        )
    file_type = _WRITE_FILE_TYPES.get(_extension(filename))
    if file_type is None:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Output Data tool writes {_safe_extension(filename)}, which Flowfile does not support."],
            reason="file_format",
        )

    table_settings = _OUTPUT_TABLE_SETTINGS[file_type]()
    if file_type == "csv":
        delimiter = _text(config, "FormatSpecificOptions/Delimeter")
        if len(delimiter) == 1:
            table_settings.delimiter = delimiter
    if file_type == "excel" and table:
        # The `|||` suffix is only sometimes a worksheet; writing it raw put backticks and the
        # driver's trailing `$` into the sheet name.
        sheet, refusal = _excel_sheet(table)
        if refusal is not None:
            status = "partial"
            reason = "option_unsupported" if reason == "converted" else reason
            messages.append(
                f"The Alteryx Output Data tool writes to '{_one_line(table)}', but {refusal}; "
                "this node writes the workbook's default sheet instead."
            )
        elif sheet:
            table_settings.sheet_name = sheet

    settings = input_schema.NodeOutput(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        output_settings=input_schema.OutputSettings(
            name=filename,
            directory=directory,
            file_type=file_type,
            write_mode="overwrite",
            table_settings=table_settings,
        ),
    )
    node_id = ctx.add_node(tool, "output", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, status, [node_id], "output", messages, reason=reason)


def map_browse(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    settings = input_schema.NodeExploreData(flow_id=ctx.flow_id, node_id=ctx.new_node_id())
    node_id = ctx.add_node(tool, "explore_data", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "explore_data", [], reason="viewer")


MAP_INPUT_DRAW_MODE = "draw"
MAP_INPUT_SELECT_MODE = "select"
# Alteryx writes a drawn shape as its own lowercase literal, which is not GeoJSON.
_MAP_INPUT_SPATIAL_RE = re.compile(r'^\s*\{\s*"type"\s*:\s*"\w+"\s*,\s*"coordinates"\s*:', re.IGNORECASE)
MAP_INPUT_SPATIAL_MESSAGE = (
    "The drawn shapes are kept exactly as Alteryx wrote them, as text: Flowfile has no spatial type, "
    "and every spatial tool that would read them is out of scope."
)


def _map_input_rows(config: ET.Element, width: int) -> list[list[str | None]]:
    """The drawn rows, cell text verbatim, padded and truncated to the declared field count."""
    rows: list[list[str | None]] = []
    for row_element in config.findall("Data/r"):
        cells: list[str | None] = [cell.text for cell in row_element.findall("c")]
        rows.append(cells[:width] + [None] * max(0, width - len(cells)))
    return rows


def map_map_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Map Input holds shapes someone drew on a map; Flowfile keeps them as typed-in rows.

    Every column is String, whatever it holds: the tool declares no types, and the shapes are
    Alteryx's own ``{"type":"point","coordinates":[...]}`` literal, which only survives a
    round-trip if it is copied byte for byte. Inferring types here would turn a label like
    ``00123`` into a number, which is the one thing a hand-typed input must not do.
    """
    config = _config(tool)
    mode = _normalise_mode(_text(config, "Mode"))
    if mode != MAP_INPUT_DRAW_MODE:
        message = (
            "This Alteryx Map Input asks the user to pick features on a map while the workflow runs, "
            "which a Flowfile flow cannot do."
            if mode == MAP_INPUT_SELECT_MODE
            else f"Alteryx Map Input mode {_text(config, 'Mode') or '(empty)'!r} has no Flowfile equivalent."
        )
        return _placeholder_row(tool, ctx, [message], reason="option_unsupported")

    fields = config.findall("Fields/Field")
    if not fields:
        return _placeholder_row(
            tool,
            ctx,
            ["This Alteryx Map Input declares no fields, so there is nothing to read."],
            reason="mapper_refused",
        )
    names = [element.get("name") or f"column_{index}" for index, element in enumerate(fields)]
    rows = _map_input_rows(config, len(names))

    settings = input_schema.NodeManualInput(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name=name, data_type="String") for name in names],
            data=[[row[index] for row in rows] for index in range(len(names))],
        ),
    )
    node_id = ctx.add_node(tool, "manual_input", settings, description=_description(tool), is_start_node=True)
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = names

    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if any(_MAP_INPUT_SPATIAL_RE.match(value or "") for row in rows for value in row):
        status, reason = "partial", "option_unsupported"
        messages.append(MAP_INPUT_SPATIAL_MESSAGE)
    declared = _attribute(config, "NumRows", "value")
    if declared.isdigit() and int(declared) != len(rows):
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"The Alteryx Map Input says it holds {declared} rows but the workflow stores {len(rows)}; "
            "the stored rows were kept."
        )
    if _text(config, "ReferenceFile"):
        messages.append(
            "The reference file this map was drawn over is a backdrop, not an input, so it was not imported."
        )
    return _row(tool, status, [node_id], "manual_input", messages, reason=reason)


MAKE_GROUP_COLUMNS = ["Key", "Group"]
MAKE_GROUP_COMPONENT = "__make_group_component"
_MAKE_GROUP_KEYS = ("Key1st", "Key2nd")


def _unique_column(name: str, taken: list[str] | None) -> str:
    """A column name nothing upstream already uses, so a private helper column cannot shadow data."""
    if not taken:
        return name
    candidate = name
    suffix = 1
    while candidate in taken:
        candidate, suffix = f"{name}_{suffix}", suffix + 1
    return candidate


def _make_group_code(tool: AlteryxTool, keys: list[str], component: str) -> str:
    """Turn the solved graph back into Alteryx's shape: one row per key, plus the group it is in."""
    message = (
        f"Alteryx Make Group (ToolID {tool.tool_id}): a key is null, and the group solver joins every "
        "null-keyed row into a single group; remove or fill the nulls before this node."
    )
    return "\n".join(
        [
            f"# Alteryx Make Group (ToolID {tool.tool_id}): one row per key, with the group it belongs to.",
            "# Alteryx names the group after the first key it meets; this names it after the lowest key.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            f"_keys = {keys!r}",
            f"_pairs = input_df.select([pl.col(_key).cast(pl.String) for _key in _keys] + [pl.col({component!r})])",
            "assert not _pairs.select(",
            "    pl.any_horizontal([pl.col(_key).is_null().any() for _key in _keys])",
            f").collect().item(), {message!r}",
            "output_df = (",
            f"    _pairs.unpivot(on=_keys, index=[{component!r}], value_name='Key')",
            "    .unique(subset=['Key'], maintain_order=True)",
            f"    .with_columns(pl.col('Key').min().over({component!r}).alias('Group'))",
            "    .select(['Key', 'Group'])",
            ")",
        ]
    )


def map_make_group(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Make Group finds the connected components of a pair of key columns.

    Two nodes, because the shapes differ: Flowfile's ``graph_solver`` appends a component id to
    every input row, while Alteryx returns exactly two columns — ``Key`` and ``Group``, one row
    per distinct key — so generated code reshapes the solved frame back into that.
    """
    config = _config(tool)
    keys = [_text(config, key) for key in _MAKE_GROUP_KEYS]
    if not all(keys):
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Make Group tool does not name both of its key fields."],
            reason="mapper_refused",
        )
    refusal = _backslash_refusal({"a Make Group key": keys})
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")

    known = ctx.input_columns(tool.tool_id)
    component = _unique_column(MAKE_GROUP_COMPONENT, known)
    solver = input_schema.NodeGraphSolver(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        graph_solver_input=transform_schema.GraphSolverInput(
            col_from=keys[0], col_to=keys[1], output_column_name=component
        ),
    )
    solver_id = ctx.add_node(tool, "graph_solver", solver, description=_description(tool))
    reshape = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_make_group_code(tool, list(dict.fromkeys(keys)), component)
        ),
    )
    reshape_id = ctx.add_node(tool, "polars_code", reshape, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    _link(ctx, solver_id, reshape_id)
    ctx.register_all_inputs(tool.tool_id, solver_id)
    ctx.register_all_outputs(tool.tool_id, reshape_id)
    ctx.tool_columns[tool.tool_id] = list(MAKE_GROUP_COLUMNS)

    messages = [
        "Alteryx names each group after the first key it meets in arrival order; the generated code names "
        "it after the lowest key in the group, which is the same name whenever the lowest key arrives first."
    ]
    unknown = sorted({child.tag for child in config} - set(_MAKE_GROUP_KEYS))
    if unknown:
        messages.append(f"This Alteryx Make Group carries configuration Flowfile does not read: {', '.join(unknown)}.")
    return _row(tool, "partial", [solver_id, reshape_id], "graph_solver", messages, reason="row_order_unknown")


FIELD_INFO_COLUMNS = ["Name", "Type"]
_FIELD_INFO_MISSING = ("Size", "Scale", "Source", "Description")


def _field_info_code(tool: AlteryxTool) -> str:
    """The input's schema as two String columns, read without touching a single row."""
    return "\n".join(
        [
            f"# Alteryx Field Info (ToolID {tool.tool_id}): the input's schema as rows, one per column.",
            "# Type is the Polars type name; Alteryx's Size, Scale, Source and Description have no equivalent.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            "_schema = input_df.collect_schema()",
            "output_df = pl.LazyFrame(",
            '    {"Name": list(_schema.names()), "Type": [str(_type) for _type in _schema.dtypes()]},',
            '    schema={"Name": pl.String, "Type": pl.String},',
            ")",
        ]
    )


def map_field_info(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Field Info describes the incoming schema; Flowfile reads it off the frame.

    Alteryx emits six columns per field (Name, Type, Size, Scale, Source, Description). Polars
    keeps only a name and a dtype — size and scale live inside the dtype, and a column has no
    provenance or description to report — so the node emits the two it can and says so.
    """
    unknown = sorted({child.tag for child in _config(tool)})
    if unknown:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Field Info carries configuration Flowfile cannot read: {', '.join(unknown)}."],
            reason="option_unsupported",
        )
    inputs = ctx.input_count(tool.tool_id)
    if inputs != 1:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"This Alteryx Field Info has {inputs} inputs; it describes the schema of exactly one, "
                "so there is nothing to describe."
            ],
            reason="mapper_refused",
        )
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=_field_info_code(tool)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = list(FIELD_INFO_COLUMNS)
    message = (
        "Alteryx also reports " + ", ".join(_FIELD_INFO_MISSING) + " per field, which Polars does not keep; "
        "Type is the Polars type name (Int64, Datetime(time_unit='us', time_zone=None)), not the Alteryx one."
    )
    return _row(tool, "partial", [node_id], "polars_code", [message], reason="option_unsupported")


def map_api_output(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's API Output hands its records back to whatever called the workflow.

    Alteryx writes one engine message per record for an SDK callback to collect; Flowfile marks
    the node's input as the body of the API response when the flow is published as an endpoint.
    The same records, delivered differently — and the tool has nothing to configure, so the node
    defaults (every row, one object per row) are a reading rather than a guess.
    """
    unknown = sorted({child.tag for child in _config(tool)})
    settings = input_schema.NodeApiResponse(flow_id=ctx.flow_id, node_id=ctx.new_node_id())
    node_id = ctx.add_node(tool, "api_response", settings, description=_description(tool))
    # A sink: Alteryx gives the tool no output anchor, so a wire out of one is reported dropped.
    ctx.register_all_inputs(tool.tool_id, node_id)
    if unknown:
        return _row(
            tool,
            "partial",
            [node_id],
            "api_response",
            [
                "This Alteryx API Output carries configuration Flowfile does not read, so the response "
                f"shape may differ: {', '.join(unknown)}."
            ],
            reason="option_unsupported",
        )
    return _row(tool, "converted", [node_id], "api_response", [], reason="converted")


_RECORD_ID_LAST_POSITION = "1"


def _record_id_code(tool: AlteryxTool, name: str, start: int, group_fields: list[str], last: bool) -> str:
    """Alteryx's Record ID as generated code, for what Flowfile's row index cannot express.

    Polars numbers rows from zero upwards, so a start value below zero has to be counted and then
    shifted; and the node always puts its column first, which is not where Alteryx's Position=1 asks
    for it.
    """
    counter = "pl.int_range(pl.len(), dtype=pl.Int64)"
    if group_fields:
        counter += f".over({group_fields!r})"
    lines = [
        f"# Alteryx Record ID (ToolID {tool.tool_id}): {name!r} starting at {start}"
        + (f", restarting for each {_one_line(', '.join(group_fields))}" if group_fields else "")
        + (", added as the last column." if last else ", added as the first column."),
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"output_df = input_df.with_columns(({counter} + {start}).alias({name!r}))",
    ]
    if not last:
        lines.append(f"output_df = output_df.select([pl.col({name!r}), pl.exclude({name!r})])")
    return "\n".join(lines)


def map_record_id(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    name = _text(config, "FieldName") or "RecordID"
    field_type = _text(config, "FieldType")
    if field_type and _map_alteryx_type(field_type) not in ("Int16", "Int32", "Int64"):
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Record ID type '{field_type}' is not an integer; Flowfile record IDs are integers."],
            reason="option_unsupported",
        )
    try:
        offset = int(_text(config, "StartValue") or "1")
    except ValueError:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Record ID start value could not be read."], reason="mapper_refused"
        )
    group_fields = [element.get("name") for element in config.findall("GroupFields/Field") if element.get("name")]
    last = _text(config, "Position") == _RECORD_ID_LAST_POSITION

    if offset < 0 or last:
        refusal = _backslash_refusal({"the record id field": name, "a group field": group_fields})
        if refusal is not None:
            return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
        settings = input_schema.NodePolarsCode(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=_record_id_code(tool, name, offset, group_fields, last)
            ),
        )
        node_type = "polars_code"
    else:
        settings = input_schema.NodeRecordId(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            record_id_input=transform_schema.RecordIdInput(
                output_column_name=name,
                offset=offset,
                group_by=bool(group_fields),
                group_by_columns=group_fields,
            ),
        )
        node_type = "record_id"
    node_id = ctx.add_node(tool, node_type, settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    if known is None:
        ctx.tool_columns[tool.tool_id] = None
    else:
        ctx.tool_columns[tool.tool_id] = [*known, name] if last else [name, *known]
    return _row(tool, "converted", [node_id], node_type, [], reason="converted")


def map_transpose(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    key_fields = [element.get("field") for element in config.findall("KeyFields/Field") if element.get("field")]
    selected = [
        element.get("field")
        for element in config.findall("DataFields/Field")
        if element.get("field") and _is_true(element.get("selected"))
    ]
    if not selected:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Transpose tool selects no data fields."], reason="mapper_refused"
        )
    if "*Unknown" in selected:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Transpose selects '*Unknown' data fields, so the column set is not static."],
            reason="option_unsupported",
        )

    settings = input_schema.NodeUnpivot(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        unpivot_input=transform_schema.UnpivotInput(index_columns=key_fields, value_columns=selected),
    )
    unpivot_id = ctx.add_node(tool, "unpivot", settings, description=_description(tool))
    # Polars unpivot names its outputs variable/value; Alteryx Transpose names them Name/Value.
    rename = input_schema.NodeSelect(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        keep_missing=True,
        select_input=[
            transform_schema.SelectInput(old_name="variable", new_name="Name"),
            transform_schema.SelectInput(old_name="value", new_name="Value"),
        ],
    )
    rename_id = ctx.add_node(tool, "select", rename, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    _link(ctx, unpivot_id, rename_id)
    ctx.register_all_inputs(tool.tool_id, unpivot_id)
    ctx.register_all_outputs(tool.tool_id, rename_id)
    ctx.tool_columns[tool.tool_id] = [*key_fields, "Name", "Value"]
    return _row(tool, "converted", [unpivot_id, rename_id], "unpivot", [], reason="converted")


CROSS_TAB_SAFE_CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
CROSS_TAB_NULL_HEADER = "_Null_"
# Alteryx prefixes "<Method>_" to every new column unless one of these is the only method
# (Cross Tab tool docs, "Method for Aggregating Values"). Not yet verified in Designer.
CROSS_TAB_PREFIX_FREE_METHODS = frozenset({"sum", "first", "last"})
_SEPARATOR_ESCAPES = {"\\s": " ", "\\t": "\t", "\\n": "\n", "\\r": "\r", "\\\\": "\\"}
_SEPARATOR_ESCAPE_RE = re.compile(r"\\[stnr\\]")


def _cross_tab_prefixes(methods: list[str]) -> list[str]:
    """Alteryx's per-method column prefix in the XML's own spelling (``Sum_``, ``Avg_``, ``Concat_``)."""
    if len(methods) == 1 and methods[0].lower() in CROSS_TAB_PREFIX_FREE_METHODS:
        return [""]
    return [f"{method}_" for method in methods]


def _unescape_separator(raw: str) -> str:
    """Alteryx writes the Concat separator with ``\\s`` for a space (``,\\s`` = comma + space)."""
    return _SEPARATOR_ESCAPE_RE.sub(lambda match: _SEPARATOR_ESCAPES[match.group(0)], raw)


_CROSS_TAB_NAMING_HELPER = [
    f"_safe = {CROSS_TAB_SAFE_CHARACTERS!r}",
    "def _alteryx_names(_pairs, _prefix):",
    "    _renames = {}",
    "    _origins = {}",
    "    for _column, _header in _pairs:",
    "        _target = _prefix + ''.join(_ch if _ch in _safe else '_' for _ch in _header)",
    "        assert _target not in _origins, (",
    '            f"Alteryx Cross Tab header values {_origins[_target]!r} and {_header!r} both become "',
    '            f"column {_target!r}; rename one of them before the Cross Tab."',
    "        )",
    "        _origins[_target] = _header",
    "        _renames[_column] = _target",
    "    return _renames",
]


def _cross_tab_rename_code(tool: AlteryxTool, index_columns: list[str], methods: list[str], aggs: list[str]) -> str:
    """The polars_code that renames a native pivot's output columns the way Alteryx names them.

    The pivot node names a new column ``<header>`` for one aggregation and ``<header>_<agg>``
    for several (see ``flow_data_engine.do_pivot``); if that format changes, the suffix table
    generated here must change with it. Header values are data, so the sanitising and the
    collision check can only run when the flow runs.
    """
    prefixes = _cross_tab_prefixes(methods)
    suffixes = [("_" + agg if len(aggs) > 1 else "", prefix) for agg, prefix in zip(aggs, prefixes, strict=True)]
    lines = [
        f"# Alteryx Cross Tab (ToolID {tool.tool_id}): name the pivot's new columns the way Alteryx does.",
        "# Every character outside [0-9A-Za-z_] in a header value becomes '_', and with several methods",
        "# the method is prefixed ('Sum_New_York'). Two header values that sanitise to the same name stop",
        "# the flow here instead of one silently overwriting the other.",
        f"_index = {index_columns!r}",
        f"_suffixes = {suffixes!r}",
        *_CROSS_TAB_NAMING_HELPER,
        "_pairs = {}",
        "for _column in input_df.collect_schema().names():",
        "    if _column in _index:",
        "        continue",
        "    for _suffix, _prefix in _suffixes:",
        "        if _column.endswith(_suffix):",
        "            _pairs.setdefault(_prefix, []).append((_column, _column[: len(_column) - len(_suffix)]))",
        "            break",
        "_renames = {}",
        "for _prefix, _columns in _pairs.items():",
        "    _renames.update(_alteryx_names(_columns, _prefix))",
        "output_df = input_df.rename(_renames)",
    ]
    return "\n".join(lines)


def _cross_tab_pivot_code(
    tool: AlteryxTool,
    index_columns: list[str],
    pivot_column: str,
    value_col: str,
    methods: list[str],
    aggs: list[str],
    separator: str,
) -> str:
    """The whole Cross Tab as polars_code, used when a method is Concat.

    The native pivot's concat aggregation has no separator option, and adding one would touch
    every layer that generates code for group_by; a generated pivot keeps the change inside the
    importer. A null header value becomes the column ``_Null_``.
    """
    aggregates = []
    for prefix, agg in zip(_cross_tab_prefixes(methods), aggs, strict=True):
        if agg == "concat":
            aggregates.append(f"({prefix!r}, pl.element().cast(pl.String).str.join({separator!r}))")
        else:
            aggregates.append(f"({prefix!r}, pl.element().{agg}())")
    index = index_columns or ["_row"]
    header = [
        f"# Alteryx Cross Tab (ToolID {tool.tool_id}) as a Polars pivot; the new columns are named the way",
        "# Alteryx names them (non-alphanumeric characters become '_', a null header becomes '_Null_').",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"_index = {index!r}",
        *_CROSS_TAB_NAMING_HELPER,
        "# Polars pivots eagerly; the header is a string with nulls given their own bucket.",
        "_frame = input_df.collect().with_columns("
        f"pl.col({pivot_column!r}).cast(pl.String).fill_null({CROSS_TAB_NULL_HEADER!r}))",
    ]
    if not index_columns:
        header.append("_frame = _frame.with_columns(pl.lit(1).alias('_row'))")
    body = [
        "_wide = None",
        f"for _prefix, _aggregate in [{', '.join(aggregates)}]:",
        f"    _part = _frame.pivot(on={pivot_column!r}, index=_index, values={value_col!r}, "
        "aggregate_function=_aggregate, maintain_order=True, sort_columns=True)",
        "    _part = _part.rename(_alteryx_names([(_c, _c) for _c in _part.columns if _c not in _index], _prefix))",
        "    _wide = _part if _wide is None else _wide.join(_part, on=_index, how='left')",
    ]
    if not index_columns:
        body.append("_wide = _wide.drop('_row')")
    body.append("output_df = _wide.lazy()")
    return "\n".join([*header, *body])


def map_cross_tab(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    index_columns = [element.get("field") for element in config.findall("GroupFields/Field") if element.get("field")]
    pivot_column = _attribute(config, "HeaderField", "field")
    value_col = _attribute(config, "DataField", "field")
    raw_methods = ((element.get("method") or "").strip() for element in config.findall("Methods/Method"))
    methods = [method for method in raw_methods if method]
    if not pivot_column or not value_col:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Cross Tab header or data field could not be read."], reason="mapper_refused"
        )
    if not methods:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Cross Tab tool has no aggregation methods configured."], reason="mapper_refused"
        )
    unmapped = [method for method in methods if _SUMMARIZE_ACTIONS.get(method.lower()) in (None, "groupby")]
    if unmapped:
        return _placeholder_row(
            tool, ctx, ["Unsupported Alteryx Cross Tab methods: " + ", ".join(unmapped)], reason="option_unsupported"
        )
    aggs = [_SUMMARIZE_ACTIONS[method.lower()] for method in methods]
    refusal = _backslash_refusal(
        {"the header field": pivot_column, "the data field": value_col, "a group field": index_columns}
    )
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    ctx.tool_columns[tool.tool_id] = None

    if "concat" in aggs:
        separator = _unescape_separator(_raw_text(config, "Methods/Separator", ","))
        refusal = _backslash_refusal({"the Concat separator": separator})
        if refusal is not None:
            return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
        code = _cross_tab_pivot_code(tool, index_columns, pivot_column, value_col, methods, aggs, separator)
        settings = input_schema.NodePolarsCode(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
        node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
        ctx.register_all_outputs(tool.tool_id, node_id)
        ctx.register_all_inputs(tool.tool_id, node_id)
        messages = []
        field_size = _attribute(config, "Methods/FieldSize", "value")
        if field_size:
            messages.append(
                f"Alteryx truncates the concatenated values at {field_size} characters (FieldSize); "
                "Flowfile strings are unbounded, so the full values are kept."
            )
        return _row(tool, "converted", [node_id], "polars_code", messages, reason="converted")

    settings = input_schema.NodePivot(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        pivot_input=transform_schema.PivotInput(
            index_columns=index_columns, pivot_column=pivot_column, value_col=value_col, aggregations=aggs
        ),
    )
    pivot_id = ctx.add_node(tool, "pivot", settings, description=_description(tool))
    rename = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_cross_tab_rename_code(tool, index_columns, methods, aggs)
        ),
    )
    rename_id = ctx.add_node(tool, "polars_code", rename, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    _link(ctx, pivot_id, rename_id)
    ctx.register_all_inputs(tool.tool_id, pivot_id)
    ctx.register_all_outputs(tool.tool_id, rename_id)
    return _row(tool, "converted", [pivot_id, rename_id], "pivot", [], reason="converted")


# Alteryx errors or warns when one Target record gains more than this many appended records.
_APPEND_CARTESIAN_GUARD = 16
_APPEND_CARTESIAN_MODES = frozenset({"allow", "warn", "error"})


def map_append_fields(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    # "Targets" is the left input and "Source" the right, matching the anchors registered below.
    left_columns = _anchor_columns(ctx, tool.tool_id, "Targets")
    right_columns = _anchor_columns(ctx, tool.tool_id, "Source")
    left_select, right_select, select_refusal = _join_select_config(config, left_columns, right_columns, "Output")

    settings = input_schema.NodeCrossJoin(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        cross_join_input=transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=left_select),
            right_select=transform_schema.JoinInputs(renames=right_select),
        ),
    )
    node_id = ctx.add_node(tool, "cross_join", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_input(tool.tool_id, DEFAULT_INPUT_ANCHOR, node_id, MAIN)
    ctx.register_input(tool.tool_id, "Targets", node_id, MAIN)
    ctx.register_input(tool.tool_id, "Source", node_id, RIGHT)
    ctx.tool_columns[tool.tool_id] = _join_output_columns(left_select, right_select) if left_select else None

    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if select_refusal is not None:
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"The Alteryx Append Fields field selection was not converted because {select_refusal}; "
            "Flowfile keeps every column from both inputs."
        )
    # Alteryx's "Warn" still appends everything, so only "Error" changes what the workflow produces.
    cartesian_mode = _text(config, "CartesianMode")
    if not cartesian_mode:
        status, reason = "partial", "option_unsupported"
        messages.append(
            "This Alteryx Append Fields tool does not say what to do when a Target record gains more than "
            f"{_APPEND_CARTESIAN_GUARD} appended records; every workflow in the corpus states it, so which "
            "setting Alteryx would fall back to is not something the file shows. If it is the erroring one, "
            "Flowfile's cross join produces the full result where Alteryx would stop."
        )
    elif cartesian_mode.lower() == "error":
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"Alteryx stops with an error when a Target record gains more than {_APPEND_CARTESIAN_GUARD} "
            "appended records; Flowfile's cross join has no such guard and produces the full result instead."
        )
    elif cartesian_mode.lower() not in _APPEND_CARTESIAN_MODES:
        status, reason = "partial", "option_unsupported"
        messages.append(
            f"This Alteryx Append Fields tool sets CartesianMode to '{_one_line(cartesian_mode)}', which is not "
            "one of the settings Flowfile knows (Allow, Warn, Error); if it is an erroring one, Flowfile's cross "
            "join produces the full result where Alteryx would stop."
        )
    return _row(tool, status, [node_id], "cross_join", messages, reason=reason)


def map_running_total(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    group_fields = [element.get("field") for element in config.findall("GroupByFields/Field") if element.get("field")]
    total_fields = list(
        dict.fromkeys(
            element.get("field") for element in config.findall("RunningTotalFields/Field") if element.get("field")
        )
    )
    if not total_fields:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Running Total tool has no fields to total."], reason="mapper_refused"
        )

    settings = input_schema.NodeWindowFunctions(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        window_input=transform_schema.WindowFunctionsInput(
            partition_by=group_fields,
            window_functions=[
                transform_schema.WindowFunctionInput(column=name, function="cum_sum", new_column_name=f"RunTot_{name}")
                for name in total_fields
            ],
        ),
    )
    node_id = ctx.add_node(tool, "window_functions", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    added = [f"RunTot_{name}" for name in total_fields]
    ctx.tool_columns[tool.tool_id] = [*known, *added] if known is not None else None

    # A running total accumulates in row order, and the tool's configuration never states one.
    if _feeds_in_stated_order(ctx, tool.tool_id):
        return _row(tool, "converted", [node_id], "window_functions", [], reason="converted")
    return _row(
        tool,
        "partial",
        [node_id],
        "window_functions",
        [
            "The running total accumulates in the order the rows arrive, which this workflow does not "
            "state; sort the rows upstream if the order matters."
        ],
        reason="row_order_unknown",
    )


_ORDER_STATING_TOOLS = frozenset({"Sort"})


def _emitted_sort(ctx: EmitContext, source: AlteryxTool) -> bool:
    """True only when this tool becomes a Flowfile sort node.

    A Sort the mapper refused is a passthrough placeholder, which states no order at all, so the
    tool's name on its own is not enough. Mapping runs in document order, so a Sort that has
    registered an output is judged by the node it emitted; one not yet mapped is judged by the
    only thing that can make ``map_sort`` refuse it, an empty key list.
    """
    if source.tool_name not in _ORDER_STATING_TOOLS:
        return False
    origin = ctx.output_map.get((source.tool_id, DEFAULT_OUTPUT_ANCHOR))
    if origin is not None:
        return any(node.id == origin[0] and node.type == "sort" for node in ctx.nodes)
    return bool(_sort_fields(_config(source)))


def _feeds_in_stated_order(ctx: EmitContext, tool_id: int) -> bool:
    """Whether *every* stream arriving on the input anchor was put in a stated order.

    One unsorted stream is enough to make an order-dependent result order-dependent again, so
    this is an all-of check over the anchor's wires, not a look at whichever one comes first.
    """
    origins = [
        ctx.resolve_output(connection.origin_tool_id, connection.origin_anchor)
        for connection in ctx.inbound.get(tool_id, [])
        if connection.dest_anchor == DEFAULT_INPUT_ANCHOR
    ]
    sources = [ctx.tools.get(origin[0]) if origin is not None else None for origin in origins]
    return bool(sources) and all(source is not None and _emitted_sort(ctx, source) for source in sources)


_CLEANSE_CHECKBOXES = {
    "Check Box (135)": "remove_null_rows",
    "Check Box (136)": "remove_null_columns",
    "Check Box (84)": "replace_nulls_with_blank",
    "Check Box (117)": "replace_nulls_with_zero",
    "Check Box (15)": "trim_whitespace",
    "Check Box (109)": "normalize_whitespace",
    "Check Box (122)": "remove_all_whitespace",
    "Check Box (53)": "remove_letters",
    "Check Box (58)": "remove_numbers",
    "Check Box (70)": "remove_punctuation",
}
# The shipped macro build omits these two; an option Alteryx never wrote was never ticked.
_CLEANSE_OPTIONAL_CHECKBOXES = frozenset({"Check Box (135)", "Check Box (136)"})
_CLEANSE_FIELD_LIST = "List Box (11)"
_CLEANSE_CASE_ENABLED = "Check Box (77)"
_CLEANSE_CASE_MODE = "Drop Down (81)"
_CLEANSE_CASE_MODES = {"upper": "uppercase", "lower": "lowercase", "title": "titlecase"}


def _parse_cleanse_fields(raw: str) -> list[str] | None:
    """Parse the Cleanse field list box: comma-separated double-quoted names, or empty for none."""
    cleaned = raw.strip()
    if not cleaned:
        return []
    if not re.fullmatch(r'"[^"]*"(?:,"[^"]*")*', cleaned):
        return None
    return re.findall(r'"([^"]*)"', cleaned)


def map_data_cleansing(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Maps the Data Cleansing macro (Cleanse.yxmc) onto the native data_cleansing node.

    The macro's configuration is a flat list of question values whose names are the
    widget ids baked into the shipped Cleanse.yxmc. Those ids have been stable across
    Alteryx releases for years but are not a contract, so a missing or unrecognized
    name fails closed to a placeholder instead of guessing by position. The two
    null-row/null-column checkboxes are the exception: builds of the macro exist that
    never write them, and whether that means the widget is absent or merely unticked,
    both readings mean the option is off — so those two default to False. The Flowfile
    node was built for parity with this tool (same frame-wide null-row/column rules,
    character classes, whitespace precedence and dtype scoping), so recognized
    options translate one-to-one.
    """
    config = _config(tool)
    values = {value.get("name", ""): (value.text or "").strip() for value in config.findall("Value")}
    expected = {*_CLEANSE_CHECKBOXES, _CLEANSE_FIELD_LIST, _CLEANSE_CASE_ENABLED, _CLEANSE_CASE_MODE}
    missing = sorted(expected - set(values) - _CLEANSE_OPTIONAL_CHECKBOXES)
    if missing:
        return _placeholder_row(
            tool,
            ctx,
            ["The Data Cleansing configuration is missing expected settings: " + ", ".join(missing) + "."],
            reason="mapper_refused",
        )
    unrecognized = sorted(set(values) - expected)
    if unrecognized:
        return _placeholder_row(
            tool,
            ctx,
            ["The Data Cleansing configuration has unrecognized settings: " + ", ".join(unrecognized) + "."],
            reason="option_unsupported",
        )
    fields = _parse_cleanse_fields(values[_CLEANSE_FIELD_LIST])
    if fields is None:
        return _placeholder_row(
            tool, ctx, ["The Data Cleansing field list could not be read."], reason="mapper_refused"
        )
    if "*Unknown" in fields:
        return _placeholder_row(
            tool,
            ctx,
            ["The Data Cleansing tool cleanses dynamic or unknown fields, which Flowfile cannot express."],
            reason="option_unsupported",
        )
    case_mode = "none"
    if _is_true(values[_CLEANSE_CASE_ENABLED]):
        case_mode = _CLEANSE_CASE_MODES.get(values[_CLEANSE_CASE_MODE].lower())
        if case_mode is None:
            return _placeholder_row(
                tool,
                ctx,
                [f"The Data Cleansing case mode '{values[_CLEANSE_CASE_MODE]}' is not recognized."],
                reason="option_unsupported",
            )

    settings = input_schema.NodeDataCleansing(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        cleansing_input=transform_schema.DataCleansingInput(
            selection_mode="list",
            selected_columns=fields,
            case_mode=case_mode,
            **{target: _is_true(values.get(name)) for name, target in _CLEANSE_CHECKBOXES.items()},
        ),
    )
    node_id = ctx.add_node(tool, "data_cleansing", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    ctx.tool_columns[tool.tool_id] = None if settings.cleansing_input.remove_null_columns else known
    return _row(tool, "converted", [node_id], "data_cleansing", [], reason="converted")


def map_count_records(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Maps the Count Records macro (CountRecords.yxmc) onto the native record_count node.

    Both return exactly one row, also for an empty input. Flowfile names its column
    ``number_of_records`` where Alteryx names it ``Count`` (an Int64), so a select
    renames and casts it to keep downstream references working.
    """
    settings = input_schema.NodeRecordCount(flow_id=ctx.flow_id, node_id=ctx.new_node_id())
    count_id = ctx.add_node(tool, "record_count", settings, description=_description(tool))
    rename = input_schema.NodeSelect(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        keep_missing=True,
        select_input=[
            transform_schema.SelectInput(
                old_name="number_of_records", new_name="Count", data_type="Int64", data_type_change=True
            )
        ],
    )
    rename_id = ctx.add_node(tool, "select", rename, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    _link(ctx, count_id, rename_id)
    ctx.register_all_inputs(tool.tool_id, count_id)
    ctx.register_all_outputs(tool.tool_id, rename_id)
    ctx.tool_columns[tool.tool_id] = ["Count"]
    return _row(tool, "converted", [count_id, rename_id], "record_count", [], reason="converted")


TOOL_MAPPERS: dict[str, ToolMapper] = {
    "TextInput": map_text_input,
    "AlteryxSelect": map_select,
    "Filter": map_filter,
    "Formula": map_formula,
    "Sort": map_sort,
    "Summarize": map_summarize,
    "Sample": map_sample,
    "Unique": map_unique,
    "TextToColumns": map_text_to_columns,
    "Union": map_union,
    "Join": map_join,
    "DynamicRename": map_dynamic_rename,
    "MultiFieldFormula": map_multi_field_formula,
    "RegEx": map_regex,
    "RecordID": map_record_id,
    "Transpose": map_transpose,
    "CrossTab": map_cross_tab,
    "AppendFields": map_append_fields,
    "RunningTotal": map_running_total,
    "DbFileInput": map_file_input,
    "DbFileOutput": map_file_output,
    "BrowseV2": map_browse,
    "Browse": map_browse,
    "Message": map_no_op,
    "Throttle": map_no_op,
    "BlockUntilDone": map_no_op,
    "Test": map_no_op,
    "ExpectEqual": map_no_op,
    "Detour": map_detour,
    "DetourEnd": map_detour_end,
    HTML_BOX: map_html_box,
    "APIOutput": map_api_output,
    "FieldInfo": map_field_info,
    "MakeGroup": map_make_group,
    "MapInput": map_map_input,
}


MACRO_MAPPERS: dict[str, ToolMapper] = {
    "cleanse.yxmc": map_data_cleansing,
    "countrecords.yxmc": map_count_records,
}


def get_mapper(tool: AlteryxTool) -> ToolMapper:
    """The mapper for a tool, falling back to the placeholder mapper.

    Macro tools carry no plugin name (tool_name is empty), so they dispatch on the
    macro filename instead — matched case-insensitively on the basename because the
    shipped macros resolve against Alteryx's RuntimeData\\Macros directory while
    user copies may carry a full path.
    """
    if not tool.tool_name and tool.plugin:
        macro_file = tool.plugin.replace("\\", "/").rsplit("/", 1)[-1].lower()
        return MACRO_MAPPERS.get(macro_file, map_unsupported)
    return TOOL_MAPPERS.get(tool.tool_name, map_unsupported)
