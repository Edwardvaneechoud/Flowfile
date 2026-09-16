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
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from polars_expr_transformer import simple_function_to_expr

from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.flowfile.converters.alteryx.expression import (
    DATETIME_ADD_CALLS,
    DUNDER_RE,
    TranslationOutcome,
    regex_rejection,
    try_translate,
    unsupported_construct,
)
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
    # `AggColl.agg` is `getattr(pl, ...)`, so the value has to be the polars name, not Alteryx's.
    "variance": "var",
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
    # the wires a mapper read at import time on an anchor it suppressed, by connection identity.
    # A suppressed anchor carries no edge, so wiring has to tell the one wire that was really read
    # from the ones that were not; without this every extra stream on that anchor vanishes silently.
    resolved_inputs: set[int] = field(default_factory=set)
    # tool id -> anchor -> the origin tool ids of every stream on it, for an anchor a mapper read
    # while more than one wire arrived there. The reader answers unknown; this is what it tells the
    # user it could not choose between.
    multi_stream_reads: dict[int, dict[str, list[tuple[int, str]]]] = field(default_factory=dict)
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
        """The (tool, anchor) really producing the data on the first of ``anchors`` that is wired.

        ``None`` when that anchor carries more than one wire: there is then no single producer to
        name, and the first one is a fact about the file's line numbering.
        """
        connection = self.sole_source_connection(tool_id, anchors)
        if connection is None:
            return None
        return self.resolve_output(connection.origin_tool_id, connection.origin_anchor)

    def has_outgoing(self, tool_id: int, anchor: str) -> bool:
        return any(connection.origin_anchor == anchor for connection in self.outbound.get(tool_id, []))

    def input_columns(self, tool_id: int, anchor: str = DEFAULT_INPUT_ANCHOR) -> list[str] | None:
        """Columns arriving on one anchor, when they are confidently known.

        A second wire on the anchor makes them unknown rather than the first wire's. Alteryx unions
        every stream arriving on one anchor, so the columns there are the union's — which this
        importer never builds — and whichever wire the document happens to write first says nothing
        about the frame the node will be handed. :meth:`note_multi_stream_read` records the question
        so the tool's row can say which streams made it unanswerable.
        """
        wires = self.anchor_wires(tool_id, (anchor,))
        if len(wires) != 1:
            self.note_multi_stream_read(tool_id, anchor, wires)
            return None
        key = self.resolve_output(wires[0].origin_tool_id, wires[0].origin_anchor)
        if key is None:
            return None
        if key in self.anchor_columns:
            return self.anchor_columns[key]
        return self.tool_columns.get(key[0])

    def anchor_wires(self, tool_id: int, anchors: tuple[str, ...]) -> list[AlteryxConnection]:
        """Every wire into the first of ``anchors`` that is connected, in document order."""
        for anchor in anchors:
            wires = [connection for connection in self.inbound.get(tool_id, []) if connection.dest_anchor == anchor]
            if wires:
                return wires
        return []

    def source_connection(self, tool_id: int, anchors: tuple[str, ...]) -> AlteryxConnection | None:
        """The first wire into the first of ``anchors`` that is actually connected.

        For the callers that *relay* a stream rather than read it — a no-op aliasing its output onto
        whatever fed it. Carrying the first is better than carrying none, and wiring already reports
        every wire that was not carried on both of its rows. A caller asking a question *about* the
        data wants :meth:`sole_source_connection` instead.
        """
        wires = self.anchor_wires(tool_id, anchors)
        return wires[0] if wires else None

    def sole_source_connection(self, tool_id: int, anchors: tuple[str, ...]) -> AlteryxConnection | None:
        """The wire into the first connected anchor, only when it is the only wire there.

        ``None`` for an anchor carrying several streams, and the question is recorded so the row can
        name them: a mapper that reads an anchor is asking what data arrives, and Alteryx's answer
        there is the union of every wire on it, not the first one in the file.
        """
        wires = self.anchor_wires(tool_id, anchors)
        if len(wires) == 1:
            return wires[0]
        if wires:
            self.note_multi_stream_read(tool_id, wires[0].dest_anchor, wires)
        return None

    def note_multi_stream_read(self, tool_id: int, anchor: str, wires: list[AlteryxConnection]) -> None:
        """Record that a mapper asked about an anchor carrying several streams and was told nothing."""
        if len(wires) > 1:
            self.multi_stream_reads.setdefault(tool_id, {})[anchor] = [
                (connection.origin_tool_id, connection.origin_anchor) for connection in wires
            ]

    def suppress_input(self, tool_id: int, anchors: tuple[str, ...]) -> None:
        """Mark anchors this mapper resolved at convert time so wiring lays no edge for them."""
        self.suppressed_inputs.update((tool_id, anchor) for anchor in anchors)

    def mark_resolved(self, *connections: AlteryxConnection | None) -> None:
        """Record the wires a mapper really read on a suppressed anchor, so wiring stays quiet about them."""
        self.resolved_inputs.update(id(connection) for connection in connections if connection is not None)

    def was_resolved(self, connection: AlteryxConnection) -> bool:
        return id(connection) in self.resolved_inputs

    def anchor_was_read(self, tool_id: int, anchor: str) -> bool:
        """Whether the mapper took anything at all off this anchor, on any wire arriving on it.

        A mapper suppresses an anchor because it will lay no edge for it, which is not the same as
        having read it: a rename taking its names from a formula suppresses the name anchor and
        reads none of it. The per-wire answer cannot tell those apart, so the message asks the anchor.
        """
        return any(
            self.was_resolved(connection)
            for connection in self.inbound.get(tool_id, [])
            if connection.dest_anchor == anchor
        )

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


def _macro_values(config: ET.Element) -> dict[str, str]:
    """A macro's ``<Value name="...">`` questions as a dict; a macro writes every answer this way."""
    return {value.get("name", ""): (value.text or "").strip() for value in config.findall("Value")}


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


def _whole_number(text: str, *, minimum: int = 0) -> int | None:
    """An Alteryx integer setting, or ``None`` when the file does not state a usable one.

    Alteryx writes these as free text, so a workflow can carry ``2.7``, ``-5`` or ``1e400`` where a
    count belongs. Reading them with ``int(float(...))`` truncated the decimal, passed the negative
    into a node that then kept no rows, and let the infinity raise ``OverflowError`` out of the whole
    conversion — no float is built here, so that last one cannot arise at all. ``minimum`` is the
    smallest value the calling site can use: zero where zero means something of its own, one where
    the setting is a count.
    """
    stripped = text.strip()
    digits = stripped[1:] if stripped[:1] in ("+", "-") else stripped
    if not digits.isdecimal():
        return None
    value = int(stripped)
    return value if value >= minimum else None


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


EXCEL_HEADER_OPTIONS_MESSAGE = (
    "This Excel read carries both of Alteryx's header settings — FirstRowData={first_row_data} and "
    "HeaderRow={header_row} — and only FirstRowData was applied, because that is the one Alteryx's "
    "own Excel options use. If the two disagree, the imported node follows FirstRowData; check the "
    "first row of the sheet against a Designer run."
)


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


_NODE_TEMPLATES = get_all_standard_nodes()[1]


def main_capacity(node: schemas.FlowfileNode, *, right_port: bool = False) -> int:
    """How many streams this node's main slot holds in total, once a right-hand slot is taken out."""
    template = _NODE_TEMPLATES.get(node.type)
    return 0 if template is None else template.input - (1 if right_port else 0)


def accepts_another_input(
    node: schemas.FlowfileNode,
    kind: str = MAIN,
    *,
    right_port: bool = False,
    anchor_wired: int = 0,
    starving_anchors: int = 0,
) -> bool:
    """Whether the palette lets this node take one more stream in the slot ``kind`` names.

    Alteryx unions every wire arriving on one input anchor; a Flowfile node has a fixed number of
    input ports, so two Alteryx sources on the single ``Input`` of a Select is a shape the imported
    flow cannot hold. A port is not interchangeable with the port beside it: the right-hand wire of
    a two-input node sits in ``right_input_id``, so a second wire onto that slot overwrites the
    first instead of filling a free port, while two wires onto the left slot fill both ports and
    leave the real right-hand wire nothing to take. Asking per slot is what keeps the wire the
    workflow states on the anchor the workflow put it on. ``right_port`` says the mappers gave this
    node a right-hand slot, which the main slot therefore does not get to use.

    Inside the main slot the same question is asked per *anchor*, for the same reason. Several
    distinct Alteryx anchors can register ``main`` on one node — an interface wizard's ``Action``
    control anchor beside the ``Input`` carrying the data, a Dynamic Rename wired on two of its four
    target anchors — and one budget for the whole node hands every port to whichever anchor document
    order reached first, so a second wire on one anchor can take the port the anchor beside it never
    got. Every wired anchor is therefore served one port before any anchor is served a second:
    ``anchor_wired`` is what this connection's own anchor already holds and ``starving_anchors`` how
    many *other* wired anchors on this node hold nothing yet. When there are fewer ports than
    anchors somebody still has to lose, and that is document order — a port cannot be split — but
    the loser is then told so in its own words.

    A node type the palette does not know is not limited here — every type the mappers emit is in
    the registry, so that is a guard against a future one going missing, not a licence to guess.
    """
    if node.type not in _NODE_TEMPLATES:
        return True
    if kind == RIGHT:
        return node.right_input_id is None
    free = main_capacity(node, right_port=right_port) - len(node.input_ids or [])
    return free > 0 if anchor_wired == 0 else free > starving_anchors


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


def unread_input_message(tool: AlteryxTool, connection: AlteryxConnection, *, read: bool) -> str:
    """Why a stream on an anchor the mapper suppressed was not read.

    ``read`` says whether the mapper took anything off this anchor at all. One that did not — a
    rename whose names come from a formula, from the first row, or from a prefix — suppresses the
    anchor without reading it, and calling this wire a further stream on an anchor one stream was
    read from would be false twice over.
    """
    if read:
        return (
            f"The Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id}) read its '{connection.dest_anchor}' anchor "
            f"at import time and takes one stream from it; the connection from ToolID {connection.origin_tool_id} "
            f"('{connection.origin_anchor}') is a further stream on that anchor and was not read."
        )
    return (
        f"The Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id}) does not read its "
        f"'{connection.dest_anchor}' anchor in this configuration, so no stream was taken from it; the "
        f"connection from ToolID {connection.origin_tool_id} ('{connection.origin_anchor}') into that "
        "anchor was not carried over."
    )


def multi_stream_read_message(tool: AlteryxTool, anchor: str, origins: list[tuple[int, str]]) -> str:
    """Why a mapper could not answer a question about an anchor several streams arrive on.

    Each origin is named with the anchor it left by, because a tool can feed one anchor twice from
    two of its own outputs — a Filter's True and False into one Union — and "ToolID 1, ToolID 1"
    names the same tool twice while saying nothing about which streams those are.
    """
    sources = ", ".join(f"ToolID {origin} ({anchor_name!r})" for origin, anchor_name in origins)
    return (
        f"The Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id}) reads its '{anchor}' anchor at import "
        f"time, and {len(origins)} connections arrive there — from {sources}. Alteryx unions those streams, "
        "so what reaches this tool is not any one of them; the import answered 'unknown' rather than "
        "taking the first."
    )


def full_input_message(
    tool: AlteryxTool,
    connection: AlteryxConnection,
    node_type: str,
    sources: list[tuple[int, str]],
    *,
    slot: str,
    capacity: int,
    anchor_count: int,
    own_anchor_full: bool,
    starving_anchors: int = 0,
) -> str:
    """Why a wire was dropped: the Flowfile node it targets has no input port left for it.

    Alteryx unions every wire arriving on one input anchor. Emitting a union here would change the
    data in a way the workflow never asked for, so the wire is dropped and both ends are told.

    Two different refusals wear this message, and the advice is only true of one of them.
    ``own_anchor_full`` is the Alteryx shape: more streams on a single anchor than the slot has
    ports, which a Union upstream really does express. When the ports went to a *different* anchor
    instead, a Union would merge streams the workflow never put together — an interface wizard's
    control wire into the data, say — so that half of the message is left unsaid.

    What is counted is the slot the wire resolved to, not the node: ``capacity`` is what the main
    slot holds once a right-hand slot is taken out of it, never the palette's raw port count,
    ``sources`` names the wires already in *that* slot with the anchor each arrived on, and
    ``anchor_count`` counts only the anchors that really laid an edge.

    A node can be refused a wire while ports are still free, because every wired anchor is served
    one port before any anchor is served a second. ``starving_anchors`` is how many anchors those
    free ports are being held for, and saying so is the difference between a sentence that reads as
    arithmetic gone wrong — "takes 10 ... already has its input from 9 ToolIDs" — and one that names
    the rule it followed.
    """
    taken = ", ".join(f"ToolID {source} (on '{anchor}')" for source, anchor in sources) or "none"
    if slot == RIGHT:
        room = "holds one stream in its right-hand input slot and"
    elif anchor_count > 1:
        room = (
            f"takes {capacity} input stream{'' if capacity == 1 else 's'} in total, shared between the "
            f"{anchor_count} Alteryx anchors wired into it, and"
        )
    else:
        room = f"takes {capacity} input stream{'' if capacity == 1 else 's'} and"
    advice = (
        " Alteryx unions the streams arriving on one anchor — add a Union node upstream and wire that in "
        "if that is what this workflow meant."
        if own_anchor_full
        else " This wire arrived on a different anchor from the one holding the port, so a Union would merge "
        "two streams the workflow never put together; reconnect by hand whichever one this node really needs."
    )
    free = capacity - len(sources)
    held = (
        f" Its {free} remaining port{'' if free == 1 else 's'} {'is' if free == 1 else 'are'} held for the "
        f"{starving_anchors} Alteryx anchor{'' if starving_anchors == 1 else 's'} wired into it that hold "
        "none yet, because every wired anchor is served one port before any anchor is served a second."
        if slot != RIGHT and free > 0 and starving_anchors > 0
        else ""
    )
    return (
        f"The Flowfile '{node_type}' node for ToolID {connection.dest_tool_id} "
        f"('{tool_label(tool)}') {room} already has its input from {taken}; the connection from "
        f"ToolID {connection.origin_tool_id} ('{connection.origin_anchor}') into its "
        f"'{connection.dest_anchor}' anchor was not wired.{held}{advice}"
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
    ``_redact_secrets`` applies it to the ``<URL>`` of an Explorer Box and of nothing else, so a
    wired one — which becomes a placeholder rather than a comment — does not copy into its
    configuration dump what the comment refused. Every other tool's ``<URL>`` is configuration a
    reader needs to rebuild the node, and is screened by name and by connection string like the
    rest of its settings.
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


def _text_input_columns(tool: AlteryxTool) -> tuple[list[str], list[str], list[list], list[str]]:
    """A Text Input as Flowfile builds it: column names, types, values, and which types were inferred.

    One authority, because a mapper that types the column and a consumer that asks what type it has
    must not answer differently.
    """
    config = _config(tool)
    fields = config.findall("Fields/Field")
    names = [field_element.get("name") or f"column_{index}" for index, field_element in enumerate(fields)]
    declared = [_map_alteryx_type(field_element.get("type")) for field_element in fields]
    rows: list[list[str | None]] = []
    for row_element in config.findall("Data/r"):
        cells = [cell.text for cell in row_element.findall("c")]
        rows.append(cells[: len(names)] + [None] * max(0, len(names) - len(cells)))

    types: list[str] = []
    data: list[list] = []
    inferred: list[str] = []
    for index, name in enumerate(names):
        data_type, values = _column_values([row[index] for row in rows], declared[index])
        if declared[index] is None and data_type != "String":
            inferred.append(name)
        types.append(data_type)
        data.append(values)
    return names, types, data, inferred


def map_text_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    names, types, data, inferred = _text_input_columns(tool)
    columns = [input_schema.MinimalFieldInfo(name=name, data_type=types[index]) for index, name in enumerate(names)]
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


# The dynamic dates Alteryx offers beside a fixed one, as the expression each stands for. Written in
# the Alteryx dialect so they go through the same fail-closed translator as everything else.
# `Filter.yxmd`'s comment box 135 names exactly this set: "Today, Tomorrow, Yesterday; you can also
# filter by selecting a Fixed Date".
_FILTER_RELATIVE_DATES: dict[str, str] = {
    "today": "DateTimeToday()",
    "tomorrow": 'DateTimeAdd(DateTimeToday(), 1, "days")',
    "yesterday": 'DateTimeAdd(DateTimeToday(), -1, "days")',
}
_FILTER_FIXED_DATE = "fixed"

# Alteryx's "Start date and periods after" / "End date and periods before" are *ranges*, not
# one-sided comparisons: `Filter.yxmd`'s comment box 150 reads "Rows that are within a period of
# 2 days from Today's date are True". Each entry is (the bound the anchor date itself is, the sign
# of the period).
_FILTER_PERIOD_OPERATORS: dict[str, tuple[str, int]] = {
    "periodafter": (">=", 1),
    "periodbefore": ("<=", -1),
}
_FILTER_PERIOD_UNITS = frozenset({"days", "weeks", "months", "years"})
# A dynamic date is a thing to compare against, so only the comparison operators may carry one. The
# same table also holds six string operators, and `StartsWith` against `today()` is a text
# comparison with a date on one side: it builds, reports `converted`, and answers a question nobody
# asked. Alteryx's own editor offers the dynamic dates only here.
_FILTER_COMPARISON_OPERATORS = frozenset({"<", "<=", "=", "==", "!=", "<>", ">", ">="})
FILTER_RELATIVE_DATE_MESSAGE = (
    "This filter's cut-off is relative to the day the flow runs — Alteryx offers Today, Tomorrow and "
    "Yesterday, and the period operators anchor their window on the same three — so it is evaluated "
    "when the flow runs, not when it was imported: the same flow keeps a different set of rows "
    "tomorrow. That is what the Alteryx tool does too — its own <Operand> still holds whatever fixed "
    "date the tool last had, and that stale literal is deliberately not read. Alteryx offers these "
    "operators only on a date column; if the column reaching this node is text, the comparison "
    "raises when the flow runs."
)


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _filter_anchor_date(simple: ET.Element) -> tuple[str | None, str | None]:
    """The date a simple-mode operator compares against, when it is a dynamic one rather than fixed.

    ``(None, None)`` means the filter is on a fixed date and its ``<Operand>`` is the answer. The
    two are read apart because ``<Operand>`` holds a *stale* literal whenever the date is dynamic:
    `Filter.yxmd` tool 139 is `<= tomorrow` and still carries `<Operand>2017-12-29</Operand>` from
    whenever the tool last had a fixed date, so rebuilding from it reported `converted` for a filter
    that had silently frozen to a day in the past.
    """
    date_type = _text(simple, "Operands/DateType").strip().lower()
    if not date_type or date_type == _FILTER_FIXED_DATE:
        return None, None
    relative = _FILTER_RELATIVE_DATES.get(date_type)
    if relative is None:
        return None, f"the Alteryx simple filter compares against {date_type!r}, which Flowfile cannot express"
    return relative, None


def _filter_period_expression(simple: ET.Element, field: str, operator: str) -> tuple[str, str | None]:
    """A "start date and periods after" window as the pair of comparisons it really is."""
    bound, sign = _FILTER_PERIOD_OPERATORS[operator]
    anchor, refusal = _filter_anchor_date(simple)
    if refusal is not None:
        return "", refusal
    if anchor is None:
        return "", (
            "the Alteryx simple filter's period operator is anchored on a fixed date, which Flowfile "
            "reads as a literal rather than as the window the tool draws around it"
        )
    unit = _text(simple, "Operands/PeriodType").strip().lower()
    if unit not in _FILTER_PERIOD_UNITS:
        return "", f"the Alteryx simple filter period unit {unit or '(empty)'!r} is not supported"
    count = _whole_number(_text(simple, "Operands/PeriodCount"), minimum=1)
    if count is None:
        return "", "the Alteryx simple filter period count could not be read"
    far = f'DateTimeAdd({anchor}, {sign * count}, "{unit}")'
    near, far_bound = (bound, "<=") if sign > 0 else (bound, ">=")
    return f"{field} {near} {anchor} AND {field} {far_bound} {far}", None


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
    field = f"[{field_name}]"
    raw_operator = _text(simple, "Operator")
    operator = raw_operator.strip().lower()
    if operator in _FILTER_PERIOD_OPERATORS:
        return _filter_period_expression(simple, field, operator)
    template, arity = _SIMPLE_FILTER_TEMPLATES.get(operator, (None, 0))
    if template is None:
        return "", f"the Alteryx simple filter operator {raw_operator or '(empty)'!r} is not supported"
    operand = ""
    if arity:
        if operator in _FILTER_COMPARISON_OPERATORS:
            relative, refusal = _filter_anchor_date(simple)
            if refusal is not None:
                return "", refusal
            if relative is not None:
                # A dynamic date never uses <Operand>; that element is whatever the tool held last.
                return template.format(field=field, operand=relative), None
        else:
            date_type = _text(simple, "Operands/DateType").strip().lower()
            if date_type and date_type != _FILTER_FIXED_DATE:
                return "", (
                    f"the Alteryx simple filter applies {raw_operator!r} to the dynamic date {date_type!r}, and "
                    "Alteryx offers a dynamic date only on a comparison, so what this tool asks cannot be read "
                    "from the workflow"
                )
        operands = [(element.text or "").strip() for element in simple.findall("Operands/Operand")]
        if not operands:
            return "", f"the Alteryx simple filter operator {raw_operator!r} has no operand"
        operand = operands[0]
        if not _looks_numeric(operand):
            if '"' in operand:
                return "", "the Alteryx simple filter operand contains a double quote and cannot be converted safely"
            operand = f'"{operand}"'
    return template.format(field=field, operand=operand), None


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
        # A simple-mode tool need not carry an <Expression> at all — `Filter.yxmd` tool 152 has none —
        # so the line that echoes it is only printed when there is something to echo.
        original = _text(config, "Expression") or expression
        messages = [
            f"The Alteryx filter expression could not be converted: {outcome.reason}.",
            *(
                [f"Original expression: {_one_line(original)}"]
                if original.strip()
                else ["The tool carries no expression of its own; it is configured in the simple-mode editor."]
            ),
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
    messages = [FILTER_RELATIVE_DATE_MESSAGE] if "today()" in outcome.translated else []
    status, reason = _caveated("converted", "converted", messages, outcome)
    return _row(tool, status, [node_id], "filter", messages, reason=reason)


def _caveated(
    status: ToolStatus, reason: str, messages: list[str], *outcomes: TranslationOutcome
) -> tuple[ToolStatus, str]:
    """Put a translation's caveats on the row and stop calling it ``converted``.

    A caveat is a function whose Flowfile equivalent agrees with Alteryx on some inputs and not on
    others (a hash whose byte encoding differs above ASCII, say). Dropping
    the sentence would leave a green badge on a column that quietly differs, which is the one outcome
    this importer is built not to produce. A row that is already worse than converted keeps its own
    status: a caveat cannot improve anything.
    """
    for outcome in outcomes:
        for caveat in outcome.caveats:
            if caveat not in messages:
                messages.append(caveat)
        if outcome.caveats and status == "converted":
            status, reason = "partial", "option_unsupported"
    return status, reason


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
    alteryx_type: str | None = None


def _formula_assignments(tool: AlteryxTool) -> list[_Assignment]:
    return [
        _Assignment(
            target=element.get("field") or f"formula_{index + 1}",
            expression=element.get("expression") or "",
            data_type=_map_alteryx_type(element.get("type")),
            alteryx_type=element.get("type"),
        )
        for index, element in enumerate(_config(tool).findall("FormulaFields/FormulaField"))
    ]


FLOAT_TYPES = frozenset({"Float32", "Float64"})
INTEGER_TYPES = frozenset({"Int16", "Int32", "Int64"})
NUMERIC_TYPES = FLOAT_TYPES | INTEGER_TYPES


def _float_operands(
    ctx: EmitContext, tool_id: int, fields: frozenset[str], chain_types: dict[str, str | None]
) -> tuple[frozenset[str], list[str]]:
    """Split the columns an expression reads into the ones to cast to Float64 and the unknown ones.

    Alteryx evaluates the whole expression in the type it declares for the output, so a Double
    assignment over integer columns still computes in floating point; Polars computes in the
    operands' own type and wraps. The repair is applied to the *operands* — this cast on each
    numeric column reference, and the float rendering of integer literals the caller asks
    ``try_translate`` for beside it — because a cast on the finished value arrives after the wrap:
    ``(-20 * pl.col('x').pow(7)).cast(Float64)`` was measured returning the wrapped integer, not
    −1.5625e20. Neither half covers the other: a literal-only ``POW(2, 70)`` names no column to
    cast, and ``[a] * [b]`` over two integer columns contains no literal to float.

    A column of unknown type cannot be cast: ``to_number`` is a strict cast, so naming a text column
    here stops the flow at run time. The unknown ones come back for the caller to say so on its row.
    A known non-numeric column is neither cast nor reported — Alteryx's declared output type says
    nothing about what a date or a string operand does on the way there.
    """
    to_cast: set[str] = set()
    unknown: list[str] = []
    for name in sorted(fields):
        data_type = chain_types[name] if name in chain_types else _input_column_type(ctx, tool_id, name)
        if data_type is None:
            unknown.append(name)
        elif data_type in NUMERIC_TYPES:
            to_cast.add(name)
    return frozenset(to_cast), unknown


def _float_operand_message(target: str, declared: str | None, unknown: list[str]) -> str:
    columns = ", ".join(f"'{name}'" for name in unknown)
    return (
        f"Alteryx declares '{target}' as {declared or 'a floating type'} and evaluates the whole "
        f"expression in floating point, but the type of {columns} is not settled by this workflow. "
        "Flowfile computes in the type the input carries, which differs from Alteryx wherever an "
        "integer column overflows; check this column against Designer before trusting it."
    )


def _emit_formula_chain(tool: AlteryxTool, ctx: EmitContext, assignments: list[_Assignment]) -> ToolReportRow:
    """Emit one Flowfile formula node per Alteryx assignment, chained in configuration order."""
    known = ctx.input_columns(tool.tool_id)
    node_ids: list[int] = []
    messages: list[str] = []
    commented = False
    placeholder = False
    caveated = False
    unsettled_floats = False
    previous_id: int | None = None
    # The declared Flowfile type of each target this chain has already written, which is what a
    # later assignment referencing it knows about its type. A target written twice keeps the last.
    chain_types: dict[str, str | None] = {}

    for index, assignment in enumerate(assignments):
        target, expression = assignment.target, assignment.expression
        # Alteryx accepts an unbracketed field name, and `known` is the only place that says which
        # names are columns here — the tool's own input plus every target the chain has written so far.
        outcome = try_translate(expression, known_columns=frozenset(known or ()))
        dx, dy = index * FORMULA_STEP_DX, index * FORMULA_STEP_DY

        if outcome.translated is not None and assignment.data_type in FLOAT_TYPES:
            to_cast, unknown = _float_operands(ctx, tool.tool_id, outcome.fields, chain_types)
            recast = try_translate(
                expression,
                known_columns=frozenset(known or ()),
                float_fields=to_cast,
                float_literals=True,
            )
            # No fallback for a recast that fails: every FUNCTION_MAP entry was tried at every
            # arity and every column/literal split, and nothing parses plain and then fails once
            # its columns are cast and its literals floated. A branch nothing can reach is a
            # branch nothing tests.
            if recast.translated is not None:
                outcome = recast
            if unknown:
                unsettled_floats = True
                messages.append(_float_operand_message(target, assignment.alteryx_type, unknown))

        if outcome.translated is not None:
            caveated = caveated or bool(outcome.caveats)
            for caveat in outcome.caveats:
                if caveat not in messages:
                    messages.append(caveat)
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
        chain_types[target] = assignment.data_type
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
    if (caveated or unsettled_floats) and status == "converted":
        status, reason = "partial", "option_unsupported"
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


# Alteryx date tokens onto strftime. A whole run of letters has to be exactly one of these, so the
# order carries no meaning and a mapping is the honest shape. Only the padded forms are here: an
# unpadded `d`, `M` or `H` and the two-digit `yy` fall through to the refusal below, `yy` because
# its century is a guess and the rest because chrono has no code for them.
_DATETIME_TOKENS: dict[str, str] = {
    "Month": "%B",
    "yyyy": "%Y",
    "Mon": "%b",
    "day": "%A",
    "dy": "%a",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "hh": "%I",
    "mm": "%M",
    "ss": "%S",
    "tt": "%p",
}
# strftime codes that place the value on a calendar; a parse with none of them has only a time.
_DATETIME_DATE_CODES = frozenset("YymdbBaAj")
DATETIME_LANGUAGE = "English"


def _datetime_strftime(alteryx_format: str) -> tuple[str, str | None]:
    """Alteryx's date-format tokens as a strftime string, or why one of them was not converted.

    A whole run of letters has to be exactly one token. Alteryx's token list is longer than the part
    verified here, so stitching two together — reading `Monday` as `Mon` + `day`, or `dddd` as `dd`
    twice — would silently format the wrong thing. Whether Designer accepts a separator-free run of
    several real tokens is unverified and no corpus workflow writes one, so `yyyyMMdd` is refused
    with the rest rather than converted on a guess.
    """
    if not alteryx_format.strip():
        return "", "the format is empty"
    rendered: list[str] = []
    index = 0
    while index < len(alteryx_format):
        character = alteryx_format[index]
        if character.isascii() and character.isalpha():
            end = index
            while end < len(alteryx_format) and alteryx_format[end].isascii() and alteryx_format[end].isalpha():
                end += 1
            run = alteryx_format[index:end]
            code = _DATETIME_TOKENS.get(run)
            if code is None:
                return "", f"'{run}' is not a date token Flowfile converts"
            rendered.append(code)
            index = end
            continue
        rendered.append("%%" if character == "%" else character)
        index += 1
    result = "".join(rendered)
    if "%" not in result.replace("%%", ""):
        return "", "it names no date or time part at all"
    return result, None


def map_date_time(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Date Time tool parses a string into a date or formats a date into a string.

    ``IsFrom`` is the whole mode: true converts the field *to* a string in the given format, false
    converts *from* one. The tool's "Custom" radio button is a label on the format box, not a third
    mode — the token string is in ``<Format>`` either way. Both directions become the Alteryx
    formula the translator already knows, so the format string is validated once, by the same
    whitelist every ``DateTimeParse`` in a Formula tool goes through.
    """
    config = _config(tool)
    language = _text(config, "Language") or DATETIME_LANGUAGE
    if language != DATETIME_LANGUAGE:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Date Time tool reads or writes {language} month and day names, which Flowfile cannot."],
            reason="option_unsupported",
        )
    source = _text(config, "InputFieldName")
    target = _text(config, "OutputFieldName")
    if not source or not target:
        missing = "input" if not source else "output"
        return _placeholder_row(
            tool, ctx, [f"This Alteryx Date Time tool names no {missing} field."], reason="option_unsupported"
        )
    to_string = _flag(config, "IsFrom")
    if to_string is None:
        return _placeholder_row(
            tool,
            ctx,
            ["This Alteryx Date Time tool does not say whether it reads a string or writes one."],
            reason="option_unsupported",
        )
    alteryx_format = _raw_text(config, "Format").strip()
    strftime, refusal = _datetime_strftime(alteryx_format)
    if refusal is not None:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx date format '{_one_line(alteryx_format)}' was not converted because {refusal}."],
            reason="option_unsupported",
        )

    function = "DateTimeFormat" if to_string else "DateTimeParse"
    expression = f'{function}([{source}], "{strftime}")'
    row = _emit_formula_chain(tool, ctx, [_Assignment(target=target, expression=expression)])
    if row.status != "converted":
        return row
    if to_string:
        # Measured on the corpus: every one of these reads a Text Input column, which Alteryx stores
        # as text and Flowfile therefore types as String, and the generated `format_date` raises on it.
        row.status, row.reason = "partial", "option_unsupported"
        row.messages.append(
            f"Alteryx keeps a date as text and formats it from there; Flowfile's formula needs a real Date "
            f"or Datetime column, so '{_one_line(source)}' has to be one by the time this node runs — parse "
            "it first (a Date Time tool the other way round, or a Select that changes the type) if it is text."
        )
    elif not set(strftime.replace("%%", "")) & _DATETIME_DATE_CODES:
        row.status, row.reason = "partial", "option_unsupported"
        row.messages.append(
            f"Alteryx reads '{_one_line(alteryx_format)}' into a Time; Flowfile has no Time column, so "
            f"'{_one_line(target)}' becomes a Datetime holding that time on 0001-01-01."
        )
    return row


DATE_TIME_NOW_COLUMN = "DateTimeNow"


def map_date_time_now(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Date Time Now: a start node whose one row holds the time the workflow ran.

    The tool is implemented as a supporting macro (`DTNEngine.yxmc`) but Alteryx writes its plugin
    name on the node, so it dispatches like any other tool. Its output is a string, not a date —
    comment box 79 of `DateTimeNow.yxmd` says the tool "inputs the current date and time … in the
    format you choose", and the format goes through the same token table the Date Time tool uses,
    so an unmapped token refuses here as it does there.
    """
    config = _config(tool)
    values = _macro_values(config)
    language = values.get("Language") or DATETIME_LANGUAGE
    if language != DATETIME_LANGUAGE:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Date Time Now tool writes {language} month and day names, which Flowfile cannot."],
            reason="option_unsupported",
        )
    alteryx_format = values.get("OutputFormat", "")
    strftime, refusal = _datetime_strftime(alteryx_format)
    if refusal is not None:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx date format '{_one_line(alteryx_format)}' was not converted because {refusal}."],
            reason="option_unsupported",
        )
    # Nothing downstream can collide with it: the tool reads no input, so the frame is this column.
    column = _unique_column(DATE_TIME_NOW_COLUMN, [])
    code = "\n".join(
        [
            f"# Alteryx Date Time Now (ToolID {tool.tool_id}): one row holding the time the flow runs.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            f"output_df = pl.select(pl.lit(datetime.datetime.now()).dt.strftime({strftime!r})"
            f".alias({column!r})).lazy()",
        ]
    )
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [column]
    return _row(
        tool,
        "partial",
        [node_id],
        "polars_code",
        [
            f"Alteryx does not record what it calls this tool's one column, so Flowfile names it "
            f"'{column}'; rename it if the rest of the flow expects another name.",
            f"The value is the moment the flow runs, formatted as '{_one_line(alteryx_format)}' "
            f"(strftime {strftime!r}), so it changes from run to run.",
        ],
        reason="option_unsupported",
    )


def _link(ctx: EmitContext, from_id: int, to_id: int, handle: str = PASS_HANDLE) -> None:
    """Connect two emitted nodes directly (used for 1:N expansions)."""
    nodes = {node.id: node for node in ctx.nodes}
    source, target = nodes[from_id], nodes[to_id]
    source.outputs.append(to_id)
    source.output_handles.append(handle)
    target.input_ids.append(from_id)


DYNAMIC_RENAME_SOURCE_ANCHORS = ("Source", "Right", "R")
# The data side of a side-input tool, shared by Dynamic Rename and Find Replace: both wire the
# stream they transform onto `Targets` and the lookup table onto `Source`. It is deliberately
# disjoint from DYNAMIC_RENAME_SOURCE_ANCHORS, which `sole_source_connection` consumes
# first-match-wins — a `Targets` inside that tuple would make the data stream answer for the
# lookup table on every tool whose Source anchor is not wired.
TARGETS_ANCHORS = ("Targets", "Input", "Left", "T")


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


def _register_rename_anchors(
    ctx: EmitContext,
    tool_id: int,
    first_id: int,
    last_id: int | None = None,
    *,
    read: AlteryxConnection | None = None,
) -> None:
    """Wire only the data anchor; the field-name anchor is resolved at import time, not wired.

    Suppression is per anchor, because an anchor is what carries an edge, but *read* is the one
    wire the mapper really took its names from. Every other wire onto a suppressed anchor is a
    stream the imported flow does not have, and wiring reports each of them rather than letting
    them disappear into the first.
    """
    ctx.register_input(tool_id, DEFAULT_INPUT_ANCHOR, first_id, MAIN)
    unwired: list[str] = []
    for connection in ctx.inbound.get(tool_id, []):
        if connection.dest_anchor in TARGETS_ANCHORS:
            ctx.register_input(tool_id, connection.dest_anchor, first_id, MAIN)
        else:
            unwired.append(connection.dest_anchor)
    ctx.suppress_input(tool_id, tuple(unwired))
    ctx.mark_resolved(read)
    ctx.register_all_outputs(tool_id, first_id if last_id is None else last_id)


def _text_input_values(tool: AlteryxTool, column: str, *, strict: bool = False) -> list[str] | None:
    """The rows of one Text Input column, when the tool feeding the names is a Text Input.

    ``strict`` is for a caller that names the column it wants: the Mapped rename reads *two* of
    them, so falling back to the first column would silently pair a name with itself.
    """
    if tool.tool_name != "TextInput":
        return None
    config = _config(tool)
    names = [element.get("name") or "" for element in config.findall("Fields/Field")]
    if not names or (strict and column not in names):
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
    tool: AlteryxTool,
    ctx: EmitContext,
    targets: list[str],
    pairs: list[tuple[str, str]],
    origin: str,
    read: AlteryxConnection,
    extra_messages: list[str] | None = None,
) -> ToolReportRow:
    """Turn a rename whose new names are already known at import time into a plain select.

    *pairs* is (current name, new name); the Positional branch zips the two lists in order and the
    Mapped branch matches them by the name the side table states, which is the whole difference
    between the two modes.
    """
    rename_map = {old: new for old, new in pairs if new and old != new}
    if not rename_map:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Dynamic Rename resolved to no column renames."], reason="mapper_refused"
        )
    # A select emits its listed columns first and the rest after, so listing only the renamed ones
    # would move them to the front — a rename is not allowed to change the layout. Listing every
    # column in the order it really arrives keeps it. Where that order is not known, the tool's
    # cached field list is not evidence of it, so the shipped shape stands and the row says so.
    arriving = _targets_columns(ctx, tool.tool_id)
    order = arriving if arriving is not None else [name for name in targets if name in rename_map]
    select_input = [
        transform_schema.SelectInput(old_name=name, new_name=rename_map.get(name, name), keep=True) for name in order
    ]
    settings = input_schema.NodeSelect(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), keep_missing=True, select_input=select_input
    )
    node_id = ctx.add_node(tool, "select", settings, description=_description(tool))
    _register_rename_anchors(ctx, tool.tool_id, node_id, read=read)
    ctx.tool_columns[tool.tool_id] = [rename_map.get(name, name) for name in order]
    messages = [
        f"The new column names were read from {origin} at import time and became a Select node "
        f"renaming {len(rename_map)} column(s).",
        "The Alteryx field-name input is no longer connected; the node that supplied it is kept unwired "
        "so you can see where the names came from.",
    ]
    if arriving is None and len(order) != len(targets):
        messages.append(
            "The columns arriving here are not known at import time, so the Select node names only the "
            "renamed ones and Flowfile emits them before the columns it leaves alone; check the column "
            "order against Alteryx's."
        )
    messages.extend(extra_messages or [])
    return _row(tool, "partial", [node_id], "select", messages, reason="option_unsupported")


def _targets_columns(ctx: EmitContext, tool_id: int) -> list[str] | None:
    """The columns arriving on the data anchor of a side-input tool, when they are known."""
    for anchor in TARGETS_ANCHORS:
        if ctx.anchor_wires(tool_id, (anchor,)):
            return ctx.input_columns(tool_id, anchor)
    return None


def _rename_from_right_input(
    tool: AlteryxTool, ctx: EmitContext, config: ET.Element, targets: list[str], mode: str
) -> ToolReportRow:
    wires = ctx.anchor_wires(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    connection = ctx.sole_source_connection(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    resolved = ctx.resolved_source(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    source = ctx.tools.get(resolved[0]) if resolved is not None else None
    if connection is None or source is None:
        # Several streams on the name anchor is a different fault from none, and the row is told
        # which one it is: `_report_multi_stream_reads` names them, this says what it cost.
        reason = (
            "several connections arrive on its field-name anchor, so the names it should use are not one tool's"
            if len(wires) > 1
            else "the field-name input is not connected"
        )
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Dynamic Rename was not converted because {reason}."], reason="mapper_refused"
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
        origin = f"the columns of '{tool_label(source)}' (ToolID {source.tool_id})"
        pairs = list(zip(targets, new_names, strict=False))
        extra = (
            [f"Only {len(new_names)} name(s) were available for {len(targets)} column(s); the rest keep their names."]
            if len(new_names) < len(targets)
            else []
        )
        return _static_rename_to_select(tool, ctx, targets, pairs, origin, connection, extra)

    names_from_rows = config.find("NamesFromRows")
    raw_input_mode = _text(names_from_rows, "InputMode") if names_from_rows is not None else ""
    input_mode = _normalise_mode(raw_input_mode)
    origin = f"the rows of '{tool_label(source)}' (ToolID {source.tool_id})"
    if input_mode == "mapped":
        return _mapped_rename_to_select(tool, ctx, names_from_rows, targets, source, origin, connection)
    if input_mode and input_mode != "positional":
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Dynamic Rename input mode '{raw_input_mode}' has no Flowfile equivalent."],
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
    extra = (
        [f"Only {len(new_names)} name(s) were available for {len(targets)} column(s); the rest keep their names."]
        if len(new_names) < len(targets)
        else []
    )
    return _static_rename_to_select(
        tool, ctx, targets, list(zip(targets, new_names, strict=False)), origin, connection, extra
    )


def _mapped_rename_to_select(
    tool: AlteryxTool,
    ctx: EmitContext,
    names_from_rows: ET.Element | None,
    targets: list[str],
    source: AlteryxTool,
    origin: str,
    read: AlteryxConnection,
) -> ToolReportRow:
    """`InputMode=Mapped`: the side table names both the old and the new column, so order is free.

    Positional pairs the two lists by position, which is exactly what this mode exists to avoid —
    `Dynamic_Rename.yxmd`'s own comment box 28 says the table is mapped "to account for the fact
    that Field12 and Field11 are out of order".
    """
    old_column = _text(names_from_rows, "OldName")
    new_column = _text(names_from_rows, "NewName")
    old_values = _text_input_values(source, old_column, strict=True) if old_column else None
    new_values = _text_input_values(source, new_column, strict=True) if new_column else None
    if old_values is None or new_values is None:
        missing = [name for name, values in ((old_column, old_values), (new_column, new_values)) if values is None]
        return _placeholder_row(
            tool,
            ctx,
            [
                "The Alteryx Dynamic Rename maps old names to new ones through the column(s) "
                f"{', '.join(repr(name) for name in missing) or '(unnamed)'} of "
                f"'{tool_label(source)}' (ToolID {source.tool_id}), which cannot be read at import time; "
                "rebuild this as a Select node once you know the names."
            ],
            reason="mapper_refused",
        )
    mapping = {old: new for old, new in zip(old_values, new_values, strict=False) if old and new}
    pairs = [(target, mapping[target]) for target in targets if target in mapping]
    if not pairs:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"The Alteryx Dynamic Rename's mapping table names no column this tool renames; its "
                f"{old_column!r} values are {_one_line(', '.join(sorted(mapping))) or '(empty)'}."
            ],
            reason="mapper_refused",
        )
    extra = []
    unmapped = [target for target in targets if target not in mapping]
    if unmapped:
        extra.append(
            f"The mapping table names no new name for {', '.join(repr(name) for name in unmapped)}; "
            "those columns keep their names."
        )
    unused = [old for old in mapping if old not in targets]
    if unused:
        extra.append(
            f"The mapping table also names {', '.join(repr(name) for name in unused)}, which this tool "
            "does not rename."
        )
    return _static_rename_to_select(tool, ctx, targets, pairs, f"{origin} matched by name", read, extra)


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
        rename_messages = [f"The Alteryx rename formula became the Flowfile formula {outcome.translated!r}."]
        status, reason = _caveated("converted", "converted", rename_messages, outcome)
        row = _emit_dynamic_rename(
            tool,
            ctx,
            transform_schema.DynamicRenameInput(rename_mode="formula", formula=outcome.translated, **selection),
            rename_messages,
        )
        row.status, row.reason = status, reason
        return row

    if mode in ("add", "addprefixsuffix", "addprefix", "addsuffix", "prefix", "suffix", "prefixsuffix"):
        return _add_affix_rename(tool, ctx, config, selection)

    if mode in ("remove", "removeprefixsuffix", "removeprefix", "removesuffix"):
        return _remove_affix_rename(tool, ctx, config, selection)

    if mode in ("rightinputrows", "rightinputmetadata"):
        return _rename_from_right_input(tool, ctx, config, selected or names, mode)

    return _placeholder_row(
        tool,
        ctx,
        [f"Alteryx Dynamic Rename mode '{raw_mode or '(empty)'}' has no Flowfile equivalent."],
        reason="option_unsupported",
    )


_AFFIX_TYPES = ("prefix", "suffix")


def _affix_settings(config: ET.Element, element_name: str) -> tuple[str, str, str] | None:
    """``<AddPrefixSuffix>``/``<RemovePrefixSuffix>`` as (type, text, on-error), or None when absent.

    Alteryx writes the affix as a `<Type>`/`<Text>` pair inside one element, not as the `<Prefix>`
    and `<Suffix>` elements this mapper used to look for. The text is read raw:
    a leading or trailing space in a prefix is part of the name Alteryx builds. None means the
    element says neither, which is what sends the Add branch to the older reading rather than
    refusing a shape that used to convert.
    """
    element = config.find(element_name)
    if element is None or (element.find("Type") is None and element.find("Text") is None):
        return None
    return _text(element, "Type").lower(), _raw_text(element, "Text"), _text(element, "OnError")


def _affix_refusal(affix_type: str, text: str, on_error: str) -> str | None:
    """Why this affix cannot become a rename, or None."""
    if affix_type not in _AFFIX_TYPES:
        named = f"'{_one_line(affix_type)}'" if affix_type else "(empty)"
        return f"Alteryx Dynamic Rename affix type {named} is neither a prefix nor a suffix."
    if not text:
        return "The Alteryx Dynamic Rename has no prefix or suffix configured."
    if on_error and on_error.lower() != "warn":
        # 'Warn' says what it does to the run: it carries on. No comment box in the corpus states
        # what any other value does, and guessing would decide whether the flow stops.
        return (
            f"Alteryx Dynamic Rename on-error setting '{_one_line(on_error)}' has no Flowfile equivalent; "
            "only 'Warn', which lets the run carry on, can be reproduced."
        )
    return None


def _add_affix_rename(tool: AlteryxTool, ctx: EmitContext, config: ET.Element, selection: dict) -> ToolReportRow:
    settings = _affix_settings(config, "AddPrefixSuffix")
    if settings is None:
        # An Alteryx build that writes the affix as its own element still converts.
        prefix, suffix = _text(config, ".//Prefix"), _text(config, ".//Suffix")
        if not prefix and not suffix:
            return _placeholder_row(
                tool, ctx, ["The Alteryx Dynamic Rename has no prefix or suffix configured."], reason="mapper_refused"
            )
        return _emit_prefix_suffix_rename(tool, ctx, prefix, suffix, selection)

    affix_type, text, on_error = settings
    refusal = _affix_refusal(affix_type, text, on_error)
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    prefix = text if affix_type == "prefix" else ""
    suffix = text if affix_type == "suffix" else ""
    row = _emit_prefix_suffix_rename(tool, ctx, prefix, suffix, selection)
    if on_error:
        row.messages.append(_AFFIX_ON_ERROR_MESSAGE)
        row.status, row.reason = "partial", "option_unsupported"
    return row


_AFFIX_ON_ERROR_MESSAGE = (
    "Alteryx's own on-error warning for this rename is not reproduced, and this workflow does not "
    "state which condition it guards; check the renamed columns."
)
_REMOVE_AFFIX_MESSAGES = (
    "Alteryx removes the {affix} here; Flowfile strips it from a column that carries it and leaves "
    "every other column's name alone. What Designer does to a column without the {affix} is stated "
    "nowhere in this workflow.",
    "A column whose whole name is {text!r} would be renamed to an empty string, which this workflow "
    "does not say Alteryx allows.",
)


def _remove_affix_rename(tool: AlteryxTool, ctx: EmitContext, config: ET.Element, selection: dict) -> ToolReportRow:
    """Alteryx removes a prefix or suffix; Flowfile has no such rename mode, so it becomes a formula."""
    settings = _affix_settings(config, "RemovePrefixSuffix")
    if settings is None:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Dynamic Rename has no prefix or suffix configured."], reason="mapper_refused"
        )
    affix_type, text, on_error = settings
    refusal = _affix_refusal(affix_type, text, on_error)
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    backslash = _backslash_refusal({f"the Dynamic Rename {affix_type}": text})
    if backslash is not None:
        return _placeholder_row(tool, ctx, [backslash], reason="mapper_refused")
    if '"' in text:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"The Alteryx Dynamic Rename removes {text!r}, which contains a double quote and so has "
                "no form as a Flowfile formula string literal."
            ],
            reason="mapper_refused",
        )
    test, keep = ("starts_with", "right") if affix_type == "prefix" else ("ends_with", "left")
    formula = (
        f'if {test}([column_name], "{text}") '
        f"then {keep}([column_name], length([column_name]) - {len(text)}) "
        "else [column_name] endif"
    )
    messages = [message.format(affix=affix_type, text=text) for message in _REMOVE_AFFIX_MESSAGES]
    if on_error:
        messages.append(_AFFIX_ON_ERROR_MESSAGE)
    row = _emit_dynamic_rename(
        tool,
        ctx,
        transform_schema.DynamicRenameInput(rename_mode="formula", formula=formula, **selection),
        messages,
    )
    row.status, row.reason = "partial", "option_unsupported"
    return row


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
        status, reason = _caveated(status, reason, messages, outcome)
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


# A backreference is a digit behind an *odd* number of backslashes. An even number is one or more
# escaped backslashes and the digit is a literal, which is what a Windows path written in a regex
# looks like: `RunCommand.yxmd` tool 15's `\w{2}\\02 Learn_one_tool_at_a_time\\22 Developer\\` was
# read as `\2` and refused for a backreference it does not contain.
_REGEX_BACKREF_RE = re.compile(r"(?<!\\)(?:\\\\)*\\[1-9]")
_REPLACEMENT_GROUP_RE = re.compile(r"\$(\d+)")


def _regex_pattern(config: ET.Element) -> tuple[str, str | None]:
    """The tool's regex, rejected when it uses constructs the Rust regex engine has no support for.

    The engine decides, and it decides first: a name for the construct is only wording put on a
    refusal it already made. Screening on the names instead refused `(?<n>a)b` — a named group
    Polars accepts — as "lookbehind", while the `(?P<n>a)b` spelling of the same group converted.

    The backreference and dunder screens stay ahead of it because neither is the engine's own
    answer: a dunder is the polars_code node's rule, and the engine's complaint about `\\1` names
    an escape rather than the backreference the reader wrote.
    """
    pattern = _attribute(config, "RegExExpression", "value")
    if not pattern:
        return "", "the Alteryx RegEx tool has no expression configured"
    if _REGEX_BACKREF_RE.search(pattern):
        return "", "the Alteryx regular expression uses a backreference, which Polars' regex engine does not support"
    if DUNDER_RE.search(pattern):
        return "", "the Alteryx regular expression contains a dunder pattern, which the Polars code node rejects"
    if _is_true(_attribute(config, "CaseInsensitve", "value")):
        pattern = f"(?i){pattern}"
    rejection = regex_rejection(pattern)
    if rejection is not None:
        label = unsupported_construct(pattern)
        if label is not None:
            return "", f"the Alteryx regular expression uses {label}, which Polars' regex engine does not support"
        return "", f"Polars' regex engine rejected the Alteryx regular expression: {rejection}"
    return pattern, None


def _regex_output_names(config: ET.Element, method: str, column: str) -> tuple[list[str], str | None]:
    if method == "parsecomplex":
        names = [element.get("field") or "" for element in config.findall("ParseComplex/Field")]
        if not all(names):
            return [], "the Alteryx RegEx tool has unnamed output fields"
        return names, None
    if _is_true(_attribute(config, "ParseSimple/SplitToRows", "value")):
        return [], "Alteryx RegEx 'split to rows' parsing has no verified Polars translation"
    count = _whole_number(_attribute(config, "ParseSimple/NumFields", "value") or "0")
    if count is None:
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
        if DUNDER_RE.search(replacement):
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


STRING_TYPE = "String"
# The file format whose Flowfile read takes each column's type from the file rather than inferring
# it, and which Alteryx itself read to fill the cache being quoted back. A CSV or an Excel sheet is
# read by inference, so a cached `String` on one records what Alteryx's own run produced, not a type
# Flowfile will produce. Arrow/IPC and Avro state their types too, but no corpus or fixture workflow
# reads one, so they are left out rather than trusted on the reasoning alone.
_TYPED_READ_EXTENSIONS = frozenset({"parquet"})


def _reads_typed_file(tool: AlteryxTool) -> bool:
    """Whether the tool's ``<File>`` names a file format that states its own column types.

    A connection string or URL is refused even when its tail looks like a file name, because the
    importer has already labelled that same tool `connection_string` — a value the emitter calls a
    connection cannot also be a file whose types are settled.

    A ``.yxdb`` is refused too, although ``flowfile convert yxdb`` hands every Alteryx string type
    back as a Polars ``String``. The cache being read is Alteryx's record of reading the ``.yxdb``,
    while the node Flowfile emits reads the *Parquet sibling* beside it — a different file, which
    ``_map_yxdb_input`` already says it cannot prove exists, and which nothing here can prove came
    from that command rather than from a differently typed Parquet at the same path. There is no
    sibling check to ask: ``convert_yxmd`` is handed uploaded bytes and never the directory the
    workflow lives in, so the only honest answer about a file this process cannot see is unknown.
    """
    path, _table = _file_element_path(_config(tool))
    if not path or _carries_credentials(path) or _looks_like_connection(path):
        return False
    _directory, filename = _split_path(path)
    return _extension(filename) in _TYPED_READ_EXTENSIONS


def _declared_column_types(tool: AlteryxTool | None) -> dict[str, str]:
    """The Flowfile type of each column a tool emits, as far as the tool's own XML settles it.

    Read from the tool's configuration rather than from what its mapper published, because mapping
    runs in document order and a source is often reached after the tool consuming it.

    Only the columns whose type the XML really settles are in the answer; an absent column means
    unknown, which the caller says rather than guesses at.

    Alteryx's cached ``<RecordInfo>`` is *Alteryx's* type, not Flowfile's, so it is not an answer.
    It rides on tools Flowfile turned into placeholders, and on a Text Input it says ``V_String``
    for the column ``_text_input_columns`` reads back as ``Int64``. Two tools do state a type
    Flowfile will really produce: a Text Input, which the mapper types from this same helper and
    which answers for every column it has; and an Input Data tool reading the one file format that
    both carries its own types and is the file Alteryx cached (``_reads_typed_file``), which answers
    **only for its string columns** — all four Alteryx string types reach Polars as ``String``, while
    a cached numeric name says nothing about the width Polars will give it.

    A Text Input column name entered twice answers for neither: ``dict(zip(...))`` let the second
    one own the type, which is a guess about which column a consumer meant. The flow cannot be built
    either way — ``manual_input`` raises ``DuplicateError`` on the repeated name — so there is no
    right answer to pick, only a quieter wrong one.
    """
    if tool is None:
        return {}
    if tool.tool_name == "TextInput":
        names, types, _, _ = _text_input_columns(tool)
        occurrences = Counter(names)
        return {name: data_type for name, data_type in zip(names, types, strict=True) if occurrences[name] == 1}
    if tool.tool_name == "DbFileInput" and _reads_typed_file(tool):
        return {
            name: STRING_TYPE
            for name, alteryx_type in tool.output_field_types.items()
            if _map_alteryx_type(alteryx_type) == STRING_TYPE
        }
    return {}


def _default_anchor_wires(
    ctx: EmitContext, tool_id: int, anchor: str = DEFAULT_INPUT_ANCHOR
) -> list[AlteryxConnection]:
    """Every wire arriving on one of the tool's input anchors, in document order."""
    return [connection for connection in ctx.inbound.get(tool_id, []) if connection.dest_anchor == anchor]


# Tools that hand every column to every output anchor with the name and the type it arrived with:
# they choose rows, never columns. A Filter's True and False anchors both qualify, as do a Unique's
# Unique and Duplicate ones. Nothing that can rename, cast or drop a column belongs here — a
# Summarize rebuilds the frame, a Join renames its collisions, a Transpose replaces the columns.
_ROW_ONLY_TOOLS = frozenset({"Filter", "Sort", "Sample", "Unique"})
# A budget rather than a rule about workflows: `seen` already ends the aliased cycle a hand-written
# file could contain, and no real chain of pass-throughs is twenty tools long.
_TYPE_WALK_HOPS = 20


def _passes_column_through(tool: AlteryxTool, column: str) -> bool:
    """Whether this tool hands ``column`` on with the name and the type it received.

    Only tools this can be *proved* for, from the tool's own XML — being unable to prove it is the
    same answer as changing it. A Record ID adds one column and touches no other, so it answers for
    everything but the one it writes. A Select has to be read: it can rename, cast and deselect, and
    it passes this column through only when it does none of the three to it.
    """
    if tool.tool_name in _ROW_ONLY_TOOLS:
        return True
    if tool.tool_name == "RecordID":
        return column != (_text(_config(tool), "FieldName") or "RecordID")
    if tool.tool_name != "AlteryxSelect":
        return False
    entries = {element.get("field"): element for element in _config(tool).findall("SelectFields/SelectField")}
    element = entries.get(column)
    if element is None:
        # The column travels through `*Unknown`, which has to be selected — and must not be the
        # name another field was renamed to, or the column upstream is a different one.
        unknown = entries.get("*Unknown")
        if unknown is None or not _is_true(unknown.get("selected")):
            return False
        return not any(other.get("rename") == column for other in entries.values())
    if not _is_true(element.get("selected")):
        return False
    return element.get("rename") in (None, "", column) and not element.get("type")


def _input_column_type(ctx: EmitContext, tool_id: int, column: str, anchor: str = DEFAULT_INPUT_ANCHOR) -> str | None:
    """The Flowfile type of one column arriving on an anchor; ``None`` means unknown.

    The walk goes back through every tool that provably passes the column through untouched
    (``_passes_column_through``) until it reaches one that types the column or one that could have
    changed it. Stopping at the first hop instead was measured telling `ControlContainer.yxmd`'s
    tools 60 and 61 that `Field 2` "is not settled by this workflow", when the Text Input two hops
    up declares it Int64 and the Filter between them changes no column at all.

    A second wire on the anchor makes the answer unknown rather than the first wire's. Alteryx
    unions the streams arriving on one anchor, so a column's type there is settled by all of them or
    by none; ``source_connection`` hands back whichever document order reaches first, which is a
    statement about the file's line numbering and not about the frame this node will be given.
    """
    seen: set[tuple[int, str]] = set()
    for hop in range(_TYPE_WALK_HOPS):
        wires = _default_anchor_wires(ctx, tool_id, anchor)
        if len(wires) != 1:
            # Only the tool being mapped is asking; a hop of this walk's own is not its business.
            if hop == 0:
                ctx.note_multi_stream_read(tool_id, anchor, wires)
            return None
        key = ctx.resolve_output(wires[0].origin_tool_id, wires[0].origin_anchor)
        if key is None or key in seen:
            return None
        seen.add(key)
        tool = ctx.tools.get(key[0])
        if tool is None:
            return None
        declared = _declared_column_types(tool).get(column)
        if declared is not None:
            return declared
        if not _passes_column_through(tool, column):
            return None
        tool_id, anchor = key[0], DEFAULT_INPUT_ANCHOR
    return None


# Alteryx actions with no `pl.<name>`, as the expression a generated group_by uses instead.
# `{c}` is the quoted column name. Longest and Shortest sort nulls last in both directions so a
# missing value never wins either end: a null is not a string of length zero. Blank is null or the
# empty string, the rule `expression.py`'s IsEmpty emits, so a string of spaces is not blank.
_SUMMARIZE_GENERATED: dict[str, str] = {
    "mode": "pl.col({c}).mode().sort().first()",
    "longest": "pl.col({c}).sort_by(pl.col({c}).str.len_chars(), descending=True, nulls_last=True).first()",
    "shortest": "pl.col({c}).sort_by(pl.col({c}).str.len_chars(), nulls_last=True).first()",
    "countnonblank": '(pl.col({c}).is_not_null() & (pl.col({c}) != "")).sum()',
    "countblank": '(pl.col({c}).is_null() | (pl.col({c}) == "")).sum()',
}
SUMMARIZE_MODE_ACTION = "mode"
SUMMARIZE_MODE_MESSAGE = (
    "Alteryx's Mode picks one value when several are equally common and its rule for that is not "
    "verified; the generated code takes the lowest of them, so the result is at least the same on "
    "every run."
)
SUMMARIZE_GROUP_BY = "groupby"
# First and Last name a value by its position, so both read an arrival order the tool never states.
_SUMMARIZE_ORDER_ACTIONS = frozenset({"first", "last"})
SUMMARIZE_ORDER_MESSAGE = (
    "Alteryx's First and Last pick a value by the order the rows arrive in, which this workflow does "
    "not state; sort the rows upstream if the order matters."
)
# The generated actions that read a column's characters, as Alteryx spells them in its own dialog.
SUMMARIZE_STRING_ACTIONS: dict[str, str] = {
    "longest": "Longest",
    "shortest": "Shortest",
    "countnonblank": "Count Non Blank",
    "countblank": "Count Blank",
}
SUMMARIZE_STRING_TYPE_MESSAGE = (
    "These Alteryx Summarize aggregations read the characters of a column Flowfile does not know to be a String: "
)
SUMMARIZE_STRING_TYPE_ADVICE = (
    ". The column has to be a String by the time this node runs, or the flow fails when it is read "
    "— give it that type upstream (a Select that changes it) if Alteryx stored it as text."
)


@dataclass
class _Summarization:
    """One ``<SummarizeField>``: the column, the action as Flowfile spells it, and the output name."""

    column: str
    action: str
    output: str


def _summarize_expression(item: _Summarization) -> str:
    """The aggregation as generated Polars, matching what ``AggColl.agg_func`` would have built."""
    quoted = repr(item.column)
    if item.action in _SUMMARIZE_GENERATED:
        return _SUMMARIZE_GENERATED[item.action].format(c=quoted)
    if item.action == "concat":
        return f"pl.col({quoted}).cast(pl.Utf8).str.concat(delimiter=',')"
    return f"pl.{item.action}({quoted})"


def _summarize_code(tool: AlteryxTool, keys: list[_Summarization], aggregations: list[_Summarization]) -> str:
    """The whole Summarize as one generated group_by; an empty key list groups the whole frame."""
    group_exprs = ", ".join(f"pl.col({item.column!r}).alias({item.output!r})" for item in keys)
    lines = [
        f"# Alteryx Summarize (ToolID {tool.tool_id}): "
        + (f"grouped by {_one_line(', '.join(item.column for item in keys))}" if keys else "over every row")
        + ", with aggregations Flowfile's group-by node cannot name.",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"output_df = input_df.group_by([{group_exprs}]).agg(",
        *(f"    {_summarize_expression(item)}.alias({item.output!r})," for item in aggregations),
        ")",
    ]
    return "\n".join(lines)


def _summarizations(config: ET.Element) -> tuple[list[_Summarization], list[str]]:
    """Every ``<SummarizeField>`` as a ``_Summarization``, plus the actions Flowfile does not know."""
    items: list[_Summarization] = []
    unmapped: list[str] = []
    for element in config.findall("SummarizeFields/SummarizeField"):
        column = element.get("field") or ""
        raw = (element.get("action") or "").strip()
        if not column:
            continue
        action = _SUMMARIZE_ACTIONS.get(raw.lower()) or (raw.lower() if raw.lower() in _SUMMARIZE_GENERATED else None)
        if action is None:
            unmapped.append(f"{raw or '(empty)'} on {column}")
            continue
        items.append(_Summarization(column, action, element.get("rename") or None or f"{raw}_{column}"))
    return items, unmapped


def map_summarize(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Summarize is a group-by; five of its actions have no name in Flowfile's node.

    Mode, Longest, Shortest, Count Non Blank and Count Blank are ordinary Polars expressions but not
    ones ``AggColl`` can name, so a tool using any of them becomes a single generated group_by with
    *all* of its aggregations in it — splitting the tool in two would change what is grouped.
    """
    config = _config(tool)
    items, unmapped = _summarizations(config)
    if unmapped:
        return _placeholder_row(
            tool,
            ctx,
            ["Unsupported Alteryx Summarize actions: " + ", ".join(unmapped)],
            reason="option_unsupported",
        )
    keys = [item for item in items if item.action == SUMMARIZE_GROUP_BY]
    aggregations = [item for item in items if item.action != SUMMARIZE_GROUP_BY]
    if not items:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Summarize tool has no aggregations configured."], reason="mapper_refused"
        )

    generated = [item for item in aggregations if item.action in _SUMMARIZE_GENERATED]
    order_dependent = any(item.action in _SUMMARIZE_ORDER_ACTIONS for item in aggregations)
    order_messages = (
        [SUMMARIZE_ORDER_MESSAGE] if order_dependent and not _feeds_in_stated_order(ctx, tool.tool_id) else []
    )
    if not generated:
        settings = input_schema.NodeGroupBy(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[transform_schema.AggColl(item.column, item.action, item.output) for item in items]
            ),
        )
        node_id = ctx.add_node(tool, "group_by", settings, description=_description(tool))
        ctx.register_all_outputs(tool.tool_id, node_id)
        ctx.register_all_inputs(tool.tool_id, node_id)
        ctx.tool_columns[tool.tool_id] = [item.output for item in items]
        if order_messages:
            return _row(tool, "partial", [node_id], "group_by", order_messages, reason="row_order_unknown")
        return _row(tool, "converted", [node_id], "group_by", [], reason="converted")

    refusal = _backslash_refusal(
        {
            "a Summarize field": [item.column for item in items],
            "a Summarize output name": [item.output for item in items],
        }
    )
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")

    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=_summarize_code(tool, keys, aggregations)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [item.output for item in [*keys, *aggregations]]

    messages = [SUMMARIZE_MODE_MESSAGE] if any(item.action == SUMMARIZE_MODE_ACTION for item in generated) else []
    messages += order_messages
    untyped = []
    for item in generated:
        if item.action not in SUMMARIZE_STRING_ACTIONS:
            continue
        found = _input_column_type(ctx, tool.tool_id, item.column)
        if found != "String":
            untyped.append(
                f"{SUMMARIZE_STRING_ACTIONS[item.action]} on '{_one_line(item.column)}' ({found or 'unknown here'})"
            )
    if untyped:
        # Why a type reads "unknown here" when several streams arrive is the general rule's to say:
        # `_report_multi_stream_reads` appends that sentence to this row, naming both origins.
        messages.append(SUMMARIZE_STRING_TYPE_MESSAGE + ", ".join(untyped) + SUMMARIZE_STRING_TYPE_ADVICE)
    if messages:
        # An order-dependent answer is silently wrong where a type error is loud, so it names the row.
        reason = "row_order_unknown" if order_messages else "option_unsupported"
        return _row(tool, "partial", [node_id], "polars_code", messages, reason=reason)
    return _row(tool, "converted", [node_id], "polars_code", [], reason="converted")


SAMPLE_ORDER_MESSAGE = (
    "Which rows this keeps depends on the order they arrive in, which this workflow does not state; "
    "sort the rows upstream if the order matters."
)
# `Sample.yxmd`'s comment box at tool 96, beside the grouped Sample at tool 99, states both halves.
SAMPLE_GROUP_SORT_MESSAGE = (
    "Alteryx's classic engine also sorts a grouped Sample's output by the grouping column, which this "
    "node does not; the rows keep the order they arrived in, as they do under Alteryx's AMP engine, so "
    "sort on the grouping column downstream if that order matters."
)
# Alteryx Sample modes that pick a fixed slice, as (whole-frame method, per-group row predicate).
# The group form is a filter on the row's position inside its group, because polars has no
# "first N of each group" verb that keeps the frame lazy.
# First has no whole-frame form: an ungrouped First is the native sample node, never generated code.
_SAMPLE_MODES: dict[str, tuple[str | None, str]] = {
    "first": (None, "_position < {n}"),
    "last": ("input_df.tail({n})", "_position >= _size - {n}"),
    "skip": ("input_df.slice({n})", "_position >= {n}"),
    "sample": ("input_df.gather_every({n})", "_position % {n} == 0"),
}
# Blocked until their meaning is settled: `Random` is a per-row 1-in-N draw rather than a
# fixed-size sample, and `NPercent` is Alteryx's "first N%", a deterministic head — so neither is
# the `NodeSample` random branch.
_SAMPLE_BLOCKED_MODES = {
    "random": "a 1-in-N chance per row rather than a sample of a fixed size",
    "npercent": "the first N% of the rows rather than a random share of them",
}


def _sample_code(tool: AlteryxTool, mode: str, size: int, groups: list[str]) -> str:
    """The Alteryx slice as generated code; grouped forms filter on the row's place in its group."""
    whole, predicate = _SAMPLE_MODES[mode]
    header = (
        f"# Alteryx Sample (ToolID {tool.tool_id}): mode '{mode}', N={size}"
        + (f", restarting for each {_one_line(', '.join(groups))}" if groups else "")
        + "."
    )
    lines = [header, *(f"# {line}" for line in _original_config_lines(tool))]
    if not groups:
        lines.append(f"output_df = {whole.format(n=size)}")
        return "\n".join(lines)
    lines.append(f"_position = pl.int_range(pl.len()).over({groups!r})")
    if "_size" in predicate:
        lines.append(f"_size = pl.len().over({groups!r})")
    lines.append(f"output_df = input_df.filter({predicate.format(n=size)})")
    return "\n".join(lines)


def map_sample(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Sample keeps a slice of the rows, optionally restarting for each group.

    Every mode in the table picks rows by position, so every one of them depends on the order the
    rows arrive in — including the plain "first N" this mapper used to convert without a caveat.
    The ungrouped "first N" is the one shape the native sample node already expresses; the rest
    become generated code rather than a node Flowfile does not have. A mode Alteryx did not write is
    not "First": defaulting it would keep the wrong rows in silence, so it is refused the way a
    record count that cannot be read is.
    """
    config = _config(tool)
    raw_mode = _text(config, "Mode")
    if not raw_mode:
        return _placeholder_row(tool, ctx, ["The Alteryx Sample mode could not be read."], reason="mapper_refused")
    mode = _normalise_mode(raw_mode)
    if mode in _SAMPLE_BLOCKED_MODES:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"Alteryx Sample mode '{raw_mode}' is not converted: it keeps "
                f"{_SAMPLE_BLOCKED_MODES[mode]}, which is not verified against Designer."
            ],
            reason="option_unsupported",
        )
    if mode not in _SAMPLE_MODES:
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Sample mode '{raw_mode}' has no Flowfile equivalent."],
            reason="option_unsupported",
        )
    size = _whole_number(_text(config, "N"))
    if size is None:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Sample record count could not be read."], reason="mapper_refused"
        )
    if size < 1:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Sample record count {size} is not a positive number."], reason="mapper_refused"
        )
    groups = [element.get("name") for element in config.findall("GroupFields/Field") if element.get("name")]
    refusal = _backslash_refusal({"a Sample group field": groups})
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")

    if mode == "first" and not groups:
        settings = input_schema.NodeSample(
            flow_id=ctx.flow_id, node_id=ctx.new_node_id(), sample_method="first", sample_size=size
        )
        node_type = "sample"
    else:
        settings = input_schema.NodePolarsCode(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=_sample_code(tool, mode, size, groups)),
        )
        node_type = "polars_code"
    node_id = ctx.add_node(tool, node_type, settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)

    grouped = [SAMPLE_GROUP_SORT_MESSAGE] if groups else []
    if _feeds_in_stated_order(ctx, tool.tool_id):
        return _row(tool, "converted", [node_id], node_type, grouped, reason="converted")
    return _row(tool, "partial", [node_id], node_type, [SAMPLE_ORDER_MESSAGE, *grouped], reason="row_order_unknown")


_RANDOM_RECORDS_KEYS = ("Number", "NNumber", "Percent", "NPercent", "Deterministic", "Seed")


def _random_records_seed(values: dict[str, str]) -> tuple[int | None, str | None]:
    """The seed, or why it could not be read. ``None`` is Alteryx's own "different every run".

    A negative seed is refused rather than passed on: Polars seeds a shuffle with an unsigned
    integer, so it would raise only once the flow was finally executed.
    """
    if not _is_true(values.get("Deterministic")):
        return None, None
    seed = _whole_number(values.get("Seed") or "")
    if seed is None:
        return None, "The Alteryx Random Records tool is set to be deterministic but its seed could not be read."
    return seed, None


def map_random_records(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Maps the Random Records macro (RandomRecords.yxmc) onto the native sample node.

    The macro asks for a count or a percentage, never both, so the two checkboxes are read as the
    exclusive pair they are and any other combination is refused rather than resolved by
    precedence. ``NPercent`` is handed over as written: ``fraction`` on the node is a percentage
    (its validator allows 0 to 100) and the engine is what divides by a hundred.

    Both tools keep the rows in their original order and both return the whole frame when asked
    for more rows than there are, so there is no order to guess and nothing to caveat.
    """
    values = _macro_values(_config(tool))
    unrecognized = sorted(set(values) - set(_RANDOM_RECORDS_KEYS))
    if unrecognized:
        return _placeholder_row(
            tool,
            ctx,
            ["The Random Records configuration has settings Flowfile does not read: " + ", ".join(unrecognized) + "."],
            reason="option_unsupported",
        )
    by_count, by_percent = _is_true(values.get("Number")), _is_true(values.get("Percent"))
    if by_count == by_percent:
        chosen = "both a record count and a percentage" if by_count else "neither a record count nor a percentage"
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Random Records tool selects {chosen}, so how much to sample cannot be read."],
            reason="mapper_refused",
        )
    seed, refusal = _random_records_seed(values)
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="mapper_refused")

    if by_count:
        size = _whole_number(values.get("NNumber") or "", minimum=1)
        if size is None:
            return _placeholder_row(
                tool, ctx, ["The Alteryx Random Records record count could not be read."], reason="mapper_refused"
            )
        settings = input_schema.NodeSample(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            sample_method="random",
            sample_size=size,
            seed=seed,
        )
    else:
        try:
            percent = float(values.get("NPercent") or "")
        except ValueError:
            return _placeholder_row(
                tool, ctx, ["The Alteryx Random Records percentage could not be read."], reason="mapper_refused"
            )
        if not 0 < percent <= 100:
            return _placeholder_row(
                tool,
                ctx,
                [f"The Alteryx Random Records percentage {percent:g} is not between 0 and 100."],
                reason="mapper_refused",
            )
        settings = input_schema.NodeSample(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            sample_method="random_fraction",
            fraction=percent,
            seed=seed,
        )

    node_id = ctx.add_node(tool, "sample", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    messages = (
        []
        if seed is not None
        else ["This Alteryx Random Records tool is not deterministic, so it draws different rows on every run."]
    )
    return _row(tool, "converted", [node_id], "sample", messages, reason="converted")


RANK_COLUMN = "Rank"
# Alteryx's ranking modes onto polars' `rank` methods, with whether the arrival order decides them.
_RANK_METHODS: dict[str, tuple[str, bool]] = {
    "dense": ("dense", False),
    "fractional": ("average", False),
    "ordinal": ("ordinal", True),
}
# Blocked until their tie shape is settled; see `map_rank`.
_RANK_BLOCKED_MODES = frozenset({"standard", "competition"})
RANK_ORDER_MESSAGE = (
    "Alteryx's Ordinal rank breaks ties by the order the rows arrive in, which this workflow does not "
    "state; sort the rows upstream if the order matters."
)
RANK_COLLISION_UNKNOWN_MESSAGE = (
    "The columns reaching this tool are not known at import time, so whether one of them is already called "
    f"'{RANK_COLUMN}' could not be checked; if one is, this node replaces it."
)


def _rank_code(tool: AlteryxTool, field: str, method: str, descending: bool, groups: list[str], column: str) -> str:
    """The rank as a new column, with the rows left exactly where they were."""
    over = f".over({groups!r})" if groups else ""
    return "\n".join(
        [
            f"# Alteryx Rank (ToolID {tool.tool_id}): {_one_line(field)}, "
            f"{'descending' if descending else 'ascending'}"
            + (f", within each {_one_line(', '.join(groups))}" if groups else "")
            + ".",
            *(f"# {line}" for line in _original_config_lines(tool)),
            "output_df = input_df.with_columns(",
            f"    pl.col({field!r}).rank(method={method!r}, descending={descending}){over}.alias({column!r})",
            ")",
        ]
    )


def map_rank(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Rank numbers the rows by one field, optionally restarting for each group.

    Not `window_functions`: that node has no descending flag, and half of Alteryx's rank is the
    direction. The rank is added as a column and nothing is reordered, which is why the message
    names the column — a workflow that wants the rows in rank order has to sort on it.

    A null in the ranked field is not a value, so polars leaves its rank null rather than ranking it
    last (measured on polars 1.43.2); the message says so, because SQL users expect the opposite.

    Standard and Competition are blocked, and not because the two agree. `02 Preparation/Rank.yxmd`'s
    own comment boxes call Standard "Equal items share the lowest possible rank" (1,2,2,4) and
    Modified Competition "The next item receives the following rank, regardless of the number of
    ties" (1,3,3,4) — two different answers, one polars argument apart. Which polars method each one
    is remains Edward's call under decision 3; until it is ruled, guessing is a silent data change.
    """
    config = _config(tool)
    modes = [element.get("value") or "" for element in config.findall("RankingModes/Mode")]
    if len(modes) != 1:
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Rank tool names {len(modes)} ranking modes; Flowfile can express exactly one."],
            reason="option_unsupported",
        )
    mode = _normalise_mode(modes[0])
    if mode in _RANK_BLOCKED_MODES:
        return _placeholder_row(
            tool,
            ctx,
            [
                f"Alteryx ranking mode '{modes[0]}' is not converted: how it numbers ties is not verified "
                "against Designer, and guessing between 1,2,2,4 and 1,3,3,4 would be a silent data change."
            ],
            reason="option_unsupported",
        )
    if mode not in _RANK_METHODS:
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx ranking mode '{modes[0] or '(empty)'}' has no Flowfile equivalent."],
            reason="option_unsupported",
        )
    method, order_dependent = _RANK_METHODS[mode]

    fields = config.findall("SortInfo/Field")
    if len(fields) != 1:
        reason = "names no field to rank by" if not fields else f"ranks by {len(fields)} fields at once"
        return _placeholder_row(
            tool,
            ctx,
            [f"This Alteryx Rank tool {reason}, which Flowfile's rank cannot express."],
            reason="mapper_refused" if not fields else "option_unsupported",
        )
    field = fields[0].get("field") or ""
    if not field:
        return _placeholder_row(
            tool, ctx, ["This Alteryx Rank tool names no field to rank by."], reason="mapper_refused"
        )
    descending = (fields[0].get("order") or "").lower().startswith("desc")
    groups = [element.get("name") for element in config.findall("GroupFields/Field") if element.get("name")]
    refusal = _backslash_refusal({"the Rank field": field, "a Rank group field": groups})
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")

    known = ctx.input_columns(tool.tool_id)
    column = _unique_column(RANK_COLUMN, known)
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_rank_code(tool, field, method, descending, groups, column)
        ),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    # `_unique_column` answers with a name `known` does not hold, so the published list cannot repeat it.
    ctx.tool_columns[tool.tool_id] = [*known, column] if known is not None else None

    taken = (
        f", because the columns reaching this tool already carry a '{RANK_COLUMN}' that Flowfile does not overwrite,"
        if column != RANK_COLUMN
        else ""
    )
    messages = [
        f"The rank is added as a column called '{column}'{taken} and the rows keep the order they arrived in; "
        f"sort on '{column}' downstream if the rows themselves have to be in rank order.",
        f"A row whose '{_one_line(field)}' is null gets a null '{column}': Flowfile ranks only the rows that "
        f"have a value, so a null takes no rank number and is not ranked last.",
    ]
    if known is None:
        messages.append(RANK_COLLISION_UNKNOWN_MESSAGE)
    if order_dependent and not _feeds_in_stated_order(ctx, tool.tool_id):
        messages.append(RANK_ORDER_MESSAGE)
        return _row(tool, "partial", [node_id], "polars_code", messages, reason="row_order_unknown")
    return _row(tool, "converted", [node_id], "polars_code", messages, reason="converted")


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

    excel_header_conflict: str | None = None
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
    elif file_type == "excel":
        # Alteryx states the same option for Excel under its own name and with the sense inverted:
        # `FirstRowData=True` means the first row *is* data, so there is no header to read.
        first_row_is_data = _flag(config, "FormatSpecificOptions/FirstRowData")
        if first_row_is_data is not None:
            received.table_settings.has_headers = not first_row_is_data
        if first_row_is_data is not None and _flag(config, "FormatSpecificOptions/HeaderRow") is not None:
            excel_header_conflict = EXCEL_HEADER_OPTIONS_MESSAGE.format(
                first_row_data=first_row_is_data, header_row=_flag(config, "FormatSpecificOptions/HeaderRow")
            )

    settings = input_schema.NodeRead(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), received_file=received)
    node_id = ctx.add_node(tool, "read", settings, description=_description(tool), is_start_node=True)
    node_ids = [node_id]
    messages: list[str] = []
    if _is_foreign_absolute_path(path):
        messages.append(
            f"The workflow reads from '{_safe_path(path)}'; repoint this node at your own copy of the file."
        )
    if excel_header_conflict is not None:
        messages.append(excel_header_conflict)

    # Only the text formats have a header row to miss: Parquet, Arrow/IPC, NDJSON and Avro name their
    # own columns, and their settings object carries no `has_headers` to read at all.
    headerless = received.file_type in ("csv", "json", "excel") and received.table_settings.has_headers is False
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


TRANSPOSE_UNKNOWN_FIELD = "*Unknown"
TRANSPOSE_NAME_COLUMN = "Name"
TRANSPOSE_VALUE_COLUMN = "Value"
_TRANSPOSE_WARN = "warn"
_TRANSPOSE_MIXED_TYPES = (
    "This Alteryx Transpose is set to warn when its data fields do not share one type; Flowfile widens "
    "them to a common type instead, and fails when there is none."
)


def _transpose_unknown_code(tool: AlteryxTool, keys: list[str], deselected: list[str]) -> str:
    """Unpivot everything that is neither a key nor explicitly deselected, whatever else arrives.

    Alteryx resolves ``*Unknown`` against the schema at run time, so the column set is not a list
    the importer can write down; a selector that names what to *leave out* says the same thing and
    keeps working when a column is added upstream.
    """
    excluded = [*keys, *(name for name in deselected if name not in keys)]
    return "\n".join(
        [
            f"# Alteryx Transpose (ToolID {tool.tool_id}): every column except "
            f"{_one_line(', '.join(excluded)) or '(none)'} becomes a "
            f"{TRANSPOSE_NAME_COLUMN}/{TRANSPOSE_VALUE_COLUMN} pair.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            "output_df = input_df.unpivot(",
            f"    on=cs.exclude({excluded!r}),",
            f"    index={keys!r},",
            f"    variable_name={TRANSPOSE_NAME_COLUMN!r},",
            f"    value_name={TRANSPOSE_VALUE_COLUMN!r},",
            ")",
        ]
    )


def _transpose_messages(config: ET.Element) -> list[str]:
    """Alteryx's data-field type-conflict setting, read and reported but never acted on."""
    return [_TRANSPOSE_MIXED_TYPES] if _text(config, "ErrorWarn").lower() == _TRANSPOSE_WARN else []


def _emit_transpose_unknown(tool: AlteryxTool, ctx: EmitContext, config: ET.Element, keys: list[str]) -> ToolReportRow:
    deselected = [
        name
        for element in config.findall("DataFields/Field")
        if (name := element.get("field")) and name != TRANSPOSE_UNKNOWN_FIELD and not _is_true(element.get("selected"))
    ]
    refusal = _backslash_refusal({"a Transpose key field": keys, "a deselected Transpose data field": deselected})
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=_transpose_unknown_code(tool, keys, deselected)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [*keys, TRANSPOSE_NAME_COLUMN, TRANSPOSE_VALUE_COLUMN]
    return _row(tool, "converted", [node_id], "polars_code", _transpose_messages(config), reason="converted")


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
    if TRANSPOSE_UNKNOWN_FIELD in selected:
        return _emit_transpose_unknown(tool, ctx, config, key_fields)

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
            transform_schema.SelectInput(old_name="variable", new_name=TRANSPOSE_NAME_COLUMN),
            transform_schema.SelectInput(old_name="value", new_name=TRANSPOSE_VALUE_COLUMN),
        ],
    )
    rename_id = ctx.add_node(tool, "select", rename, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    _link(ctx, unpivot_id, rename_id)
    ctx.register_all_inputs(tool.tool_id, unpivot_id)
    ctx.register_all_outputs(tool.tool_id, rename_id)
    ctx.tool_columns[tool.tool_id] = [*key_fields, TRANSPOSE_NAME_COLUMN, TRANSPOSE_VALUE_COLUMN]
    return _row(tool, "converted", [unpivot_id, rename_id], "unpivot", _transpose_messages(config), reason="converted")


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


def _feeds_in_stated_order(ctx: EmitContext, tool_id: int, anchor: str = DEFAULT_INPUT_ANCHOR) -> bool:
    """Whether *every* stream arriving on *anchor* was put in a stated order.

    One unsorted stream is enough to make an order-dependent result order-dependent again, so
    this is an all-of check over the anchor's wires, not a look at whichever one comes first.
    A tool whose data arrives somewhere other than ``Input`` — a Dynamic Rename's or a Find
    Replace's ``Targets`` — answered False for every upstream until the anchor became an
    argument, because the filter below matched no wire at all and an empty ``sources`` is False.
    """
    origins = [
        ctx.resolve_output(connection.origin_tool_id, connection.origin_anchor)
        for connection in ctx.inbound.get(tool_id, [])
        if connection.dest_anchor == anchor
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


def _emit_cleansing_node(
    tool: AlteryxTool,
    ctx: EmitContext,
    cleansing_input: transform_schema.DataCleansingInput,
    messages: list[str] | None = None,
    *,
    status: ToolStatus = "converted",
    reason: str = "converted",
) -> ToolReportRow:
    """Emit the data_cleansing node, shared by the Cleanse macro and the Data Cleanse Pro tool.

    The two read completely different configurations — widget ids in one, self-describing elements
    in the other — and agree from here on, so everything downstream of the reading lives once.
    Removing empty columns changes the schema in a way that depends on the data, which the importer
    never sees, so the column tracker has to admit it no longer knows what leaves this tool.
    """
    settings = input_schema.NodeDataCleansing(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), cleansing_input=cleansing_input
    )
    node_id = ctx.add_node(tool, "data_cleansing", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    ctx.tool_columns[tool.tool_id] = None if cleansing_input.remove_null_columns else known
    return _row(tool, status, [node_id], "data_cleansing", messages or [], reason=reason)


def _parse_quoted_field_list(raw: str) -> list[str] | None:
    """Parse a macro list box: comma-separated double-quoted names, or empty for none.

    Shared by the Cleanse and Imputation macros, which write their field lists identically.
    """
    cleaned = raw.strip()
    if not cleaned:
        return []
    if not re.fullmatch(r'"[^"]*"(?:,"[^"]*")*', cleaned):
        return None
    return re.findall(r'"([^"]*)"', cleaned)


# Imputation_v3.yxmc's widget names, spelled exactly as the shipped macro writes them.
_IMPUTATION_FIELDS = "listbox Select Incoming Fields"
_IMPUTATION_FROM_NULL = "radio Null Value"
_IMPUTATION_FROM_VALUE = "radio User Specified Replace From Value"
_IMPUTATION_FROM_NUMBER = "updown User Replace Value"
_IMPUTATION_WITH_VALUE = "radio User Specified Replace With Value"
_IMPUTATION_WITH_NUMBER = "updown User Replace With Value"
_IMPUTATION_INDICATOR = "checkbox Impute Indicator"
_IMPUTATION_SEPARATE = "checkbox Imputed Values Separate Field"
# Each statistic and the Polars expression that computes it over the column being imputed.
_IMPUTATION_STATISTICS = {
    "radio Mean": "pl.col(_f).mean()",
    "radio Median": "pl.col(_f).median()",
    # The Summarize precedent (tool 111): Polars returns an arbitrary mode on a tie, so the
    # generated code sorts and takes the first to be at least deterministic. `drop_nulls` first
    # because a null is one of the values `mode()` counts, and the null is what is being replaced.
    "radio Mode": "pl.col(_f).drop_nulls().mode().sort().first()",
}
_IMPUTATION_EXPECTED = frozenset(
    {
        _IMPUTATION_FIELDS,
        _IMPUTATION_FROM_NULL,
        _IMPUTATION_FROM_VALUE,
        _IMPUTATION_FROM_NUMBER,
        _IMPUTATION_WITH_VALUE,
        _IMPUTATION_WITH_NUMBER,
        _IMPUTATION_INDICATOR,
        _IMPUTATION_SEPARATE,
        *_IMPUTATION_STATISTICS,
    }
)
# Alteryx's own names for the two optional columns, stated in `Imputation.yxmd`'s boxes 174 and 177.
IMPUTATION_IMPUTED_SUFFIX = "_ImputedValue"
IMPUTATION_INDICATOR_SUFFIX = "_Indicator"
IMPUTATION_MODE_MESSAGE = (
    "Polars returns an arbitrary value when two values tie for most frequent, so the generated code "
    "sorts the tied values and takes the first; Alteryx's own tie rule is not stated in this workflow."
)
IMPUTATION_FROM_VALUE_STATISTIC_MESSAGE = (
    "The statistic is computed over every row, the rows holding {value} included. Whether Alteryx "
    "leaves those rows out of it is stated nowhere in this workflow, and the two readings give "
    "different numbers."
)
IMPUTATION_WIDENS_MESSAGE = (
    "{columns} arrive as whole numbers and the replacement is the {statistic}, so Flowfile's result is "
    "a Float64 column; what Alteryx does to the column's type here is not stated in this workflow."
)
IMPUTATION_UNKNOWN_TYPE_MESSAGE = (
    "The type of {columns} is not known at import time (only a Text Input or a typed file read states "
    "one Flowfile will really produce), so a non-numeric column here would raise when the flow runs "
    "instead of being refused now."
)


def _imputation_number(raw: str) -> str | None:
    """An Alteryx `updown` value as a Python literal, or None when it is not a number.

    A whole number is written as an integer so that filling an integer column keeps its type;
    `0.00000` is how the widget spells zero, not a statement that the column becomes a Double.
    """
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return repr(int(value)) if value.is_integer() else repr(value)


def _exclusive_radio(values: dict[str, str], names: list[str]) -> tuple[str, str | None]:
    """The one radio button that is on, or why the group cannot be read.

    Alteryx writes every button of a group, so several on or none on is a configuration this
    importer has no reading for rather than one to resolve by precedence.
    """
    on = [name for name in names if _is_true(values.get(name))]
    if len(on) == 1:
        return on[0], None
    quoted = ", ".join(repr(name) for name in names)
    state = "none of" if not on else f"{len(on)} of"
    return "", f"{state} the radio buttons {quoted} are on, so the option cannot be read"


def map_imputation(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Imputation_v3.yxmc: replace nulls (or one chosen value) with a statistic or a fixed number.

    The macro asks two exclusive questions — what to replace and what to replace it with — and each
    radio button sits beside the value it enables, so a stale number can be left in the box: corpus
    tool 180 has Mean on and `0.00000` still in the replace-with box. The radio is therefore read
    first and the number only under the radio that uses it.
    """
    config = _config(tool)
    values = _macro_values(config)
    missing = sorted(_IMPUTATION_EXPECTED - set(values))
    if missing:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Imputation configuration is missing expected settings: " + ", ".join(missing) + "."],
            reason="mapper_refused",
        )
    unrecognized = sorted(set(values) - _IMPUTATION_EXPECTED)
    if unrecognized:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Imputation configuration has unrecognized settings: " + ", ".join(unrecognized) + "."],
            reason="option_unsupported",
        )

    fields = _parse_quoted_field_list(values[_IMPUTATION_FIELDS])
    refusal: str | None = None
    if fields is None:
        refusal = "its field list could not be read"
    elif not fields:
        refusal = "it selects no field to impute"
    elif "*Unknown" in fields:
        refusal = "it imputes dynamic or unknown fields, which Flowfile cannot express"
    elif len(fields) != len(set(fields)):
        refusal = "a field is selected more than once"
    if refusal is None:
        from_radio, refusal = _exclusive_radio(values, [_IMPUTATION_FROM_NULL, _IMPUTATION_FROM_VALUE])
    if refusal is None:
        with_radio, refusal = _exclusive_radio(values, [*_IMPUTATION_STATISTICS, _IMPUTATION_WITH_VALUE])
    if refusal is None:
        refusal = _backslash_refusal({"an Imputation field": fields})
        if refusal is not None:
            return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    if refusal is None:
        known = ctx.input_columns(tool.tool_id)
        refusal = _missing_column_refusal(known, fields)
    if refusal is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Imputation was not converted because {refusal}."], reason="mapper_refused"
        )

    from_value = _imputation_number(values[_IMPUTATION_FROM_NUMBER]) if from_radio == _IMPUTATION_FROM_VALUE else None
    if from_radio == _IMPUTATION_FROM_VALUE and from_value is None:
        return _placeholder_row(
            tool,
            ctx,
            [
                "The Alteryx Imputation replaces a chosen value, and "
                f"'{_one_line(values[_IMPUTATION_FROM_NUMBER])}' is not a number Flowfile can read."
            ],
            reason="mapper_refused",
        )
    with_value = _imputation_number(values[_IMPUTATION_WITH_NUMBER]) if with_radio == _IMPUTATION_WITH_VALUE else None
    if with_radio == _IMPUTATION_WITH_VALUE and with_value is None:
        return _placeholder_row(
            tool,
            ctx,
            [
                "The Alteryx Imputation replaces with a fixed value, and "
                f"'{_one_line(values[_IMPUTATION_WITH_NUMBER])}' is not a number Flowfile can read."
            ],
            reason="mapper_refused",
        )

    statistic = with_radio in _IMPUTATION_STATISTICS
    if with_radio in ("radio Mean", "radio Median"):
        for column in fields:
            declared = _input_column_type(ctx, tool.tool_id, column)
            if declared is not None and declared not in _NUMERIC_TYPES:
                return _placeholder_row(
                    tool,
                    ctx,
                    [
                        f"The Alteryx Imputation was not converted because the field {column!r} is a "
                        f"{declared} column, which has no {with_radio.split()[-1].lower()} to compute."
                    ],
                    reason="mapper_refused",
                )

    separate = _is_true(values[_IMPUTATION_SEPARATE])
    indicator = _is_true(values[_IMPUTATION_INDICATOR])
    taken = list(known) if known is not None else list(fields)
    imputed_names: dict[str, str] = {}
    indicator_names: dict[str, str] = {}
    renamed: list[str] = []
    for column in fields:
        if separate:
            name = _unique_column(f"{column}{IMPUTATION_IMPUTED_SUFFIX}", taken)
            imputed_names[column] = name
            taken.append(name)
            if name != f"{column}{IMPUTATION_IMPUTED_SUFFIX}":
                renamed.append(name)
        else:
            imputed_names[column] = column
        if indicator:
            name = _unique_column(f"{column}{IMPUTATION_INDICATOR_SUFFIX}", taken)
            indicator_names[column] = name
            taken.append(name)
            if name != f"{column}{IMPUTATION_INDICATOR_SUFFIX}":
                renamed.append(name)

    replacement = _IMPUTATION_STATISTICS[with_radio] if statistic else f"pl.lit({with_value})"
    if from_radio == _IMPUTATION_FROM_NULL:
        matched = "pl.col(_f).is_null()"
        flagged, value_expression = matched, f"pl.col(_f).fill_null({replacement})"
    else:
        matched = f"(pl.col(_f) == {from_value})"
        # A null compares to null, and box 177 gives the indicator only 1 and 0.
        flagged = f"{matched}.fill_null(False)"
        value_expression = f"pl.when({matched}).then({replacement}).otherwise(pl.col(_f))"

    lines = [
        f"# Alteryx Imputation (ToolID {tool.tool_id}): replace "
        + ("nulls" if from_radio == _IMPUTATION_FROM_NULL else f"the value {from_value}")
        + " in the listed columns.",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"_fields = {fields!r}",
        f"_imputed = {imputed_names!r}",
    ]
    projections = [f"[{value_expression}.alias(_imputed[_f]) for _f in _fields]"]
    if indicator:
        lines.append(f"_indicator = {indicator_names!r}")
        projections.append(f"[{flagged}.cast(pl.Int64).alias(_indicator[_f]) for _f in _fields]")
    body = "\n    + ".join(projections)
    lines.append(f"output_df = input_df.with_columns(\n    {body}\n)")

    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code="\n".join(lines)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    added = [name for name in taken if known is None or name not in known]
    ctx.tool_columns[tool.tool_id] = [*known, *added] if known is not None else None

    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if separate:
        messages.append(
            f"The imputed values land in {', '.join(repr(imputed_names[f]) for f in fields)} and the original "
            "column(s) are left alone, as Alteryx's own comment box describes; Flowfile appends them after "
            "the columns that arrive."
        )
    if indicator:
        messages.append(
            f"The column(s) {', '.join(repr(indicator_names[f]) for f in fields)} hold 1 where the value was "
            "imputed and 0 where it was not, the meaning Alteryx's own comment box states; Flowfile appends "
            "them last."
        )
    if renamed:
        messages.append(
            f"Alteryx's own name(s) for {', '.join(repr(name) for name in renamed)} were already taken by a "
            "column arriving here, so Flowfile numbered them."
        )
        status, reason = "partial", "option_unsupported"
    if with_radio == "radio Mode":
        messages.append(IMPUTATION_MODE_MESSAGE)
        status, reason = "partial", "option_unsupported"
    if statistic and from_radio == _IMPUTATION_FROM_VALUE:
        messages.append(IMPUTATION_FROM_VALUE_STATISTIC_MESSAGE.format(value=from_value))
        status, reason = "partial", "option_unsupported"
    if with_radio in ("radio Mean", "radio Median"):
        whole = [column for column in fields if (_input_column_type(ctx, tool.tool_id, column) or "").startswith("Int")]
        if whole:
            messages.append(
                IMPUTATION_WIDENS_MESSAGE.format(
                    columns=", ".join(repr(name) for name in whole), statistic=with_radio.split()[-1].lower()
                )
            )
    untyped = [column for column in fields if _input_column_type(ctx, tool.tool_id, column) is None]
    if untyped:
        messages.append(IMPUTATION_UNKNOWN_TYPE_MESSAGE.format(columns=_one_line(", ".join(repr(c) for c in untyped))))
        status, reason = "partial", "option_unsupported"
    return _row(tool, status, [node_id], "polars_code", messages, reason=reason)


_WEIGHTED_AVG_VALUE = "Value"
_WEIGHTED_AVG_WEIGHT = "Weight"
_WEIGHTED_AVG_OUTPUT = "OutputFieldName"
_WEIGHTED_AVG_GROUPS = "GroupFields"
_WEIGHTED_AVG_EXPECTED = frozenset(
    {_WEIGHTED_AVG_VALUE, _WEIGHTED_AVG_WEIGHT, _WEIGHTED_AVG_OUTPUT, _WEIGHTED_AVG_GROUPS}
)
WEIGHTED_AVG_COLUMN = "WeightedAverage"
WEIGHTED_AVG_ZERO_MESSAGE = (
    "When the weights of a group add up to zero there is nothing to divide by; Flowfile's answer is "
    "NaN, and Alteryx's is stated nowhere in this workflow."
)
WEIGHTED_AVG_UNKNOWN_TYPE_MESSAGE = (
    "The type of {columns} is not known at import time (only a Text Input or a typed file read states "
    "one Flowfile will really produce), so a non-numeric column here would raise when the flow runs "
    "instead of being refused now — and Alteryx's own comment box 94 says both columns must be numeric."
)


def map_weighted_average(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """WeightedAvg.yxmc: sum(value x weight) / sum(weight), over the whole stream or per group.

    The macro names one `GroupFields` value and box 80 says a user may pick several; no corpus
    instance writes more than one and nothing here states how Alteryx separates them, so the value
    is read as one column name and a name the stream does not hold is refused rather than split on
    a guessed separator.
    """
    config = _config(tool)
    values = _macro_values(config)
    missing = sorted(_WEIGHTED_AVG_EXPECTED - set(values))
    if missing:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Weighted Average configuration is missing expected settings: " + ", ".join(missing) + "."],
            reason="mapper_refused",
        )
    unrecognized = sorted(set(values) - _WEIGHTED_AVG_EXPECTED)
    if unrecognized:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Weighted Average configuration has unrecognized settings: " + ", ".join(unrecognized) + "."],
            reason="option_unsupported",
        )

    value, weight = values[_WEIGHTED_AVG_VALUE], values[_WEIGHTED_AVG_WEIGHT]
    groups = [values[_WEIGHTED_AVG_GROUPS]] if values[_WEIGHTED_AVG_GROUPS] else []
    known = ctx.input_columns(tool.tool_id)
    refusal: str | None = None
    if not value or not weight:
        refusal = f"it names no {'value' if not value else 'weight'} column"
    elif value == weight:
        refusal = f"the value and the weight are both {value!r}, which weights every row by itself"
    if refusal is None:
        refusal = _missing_column_refusal(known, [value, weight, *groups])
    if refusal is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Weighted Average was not converted because {refusal}."], reason="mapper_refused"
        )
    backslash = _backslash_refusal({"a Weighted Average column": [value, weight, *groups]})
    if backslash is not None:
        return _placeholder_row(tool, ctx, [backslash], reason="option_unsupported")
    for column in (value, weight):
        declared = _input_column_type(ctx, tool.tool_id, column)
        if declared is not None and declared not in _NUMERIC_TYPES:
            return _placeholder_row(
                tool,
                ctx,
                [
                    f"The Alteryx Weighted Average was not converted because {column!r} is a {declared} column, "
                    "and Alteryx's own comment box says the value and the weight must both be numeric."
                ],
                reason="mapper_refused",
            )

    output = _unique_column(values[_WEIGHTED_AVG_OUTPUT] or WEIGHTED_AVG_COLUMN, groups)
    average = f"((pl.col({value!r}) * pl.col({weight!r})).sum() / pl.col({weight!r}).sum()).alias({output!r})"
    lines = [
        f"# Alteryx Weighted Average (ToolID {tool.tool_id}): sum(value x weight) / sum(weight)"
        + (f", one row per {', '.join(repr(name) for name in groups)}." if groups else " over every row."),
        *(f"# {line}" for line in _original_config_lines(tool)),
    ]
    if groups:
        lines.append(f"output_df = input_df.group_by({groups!r}).agg(\n    {average}\n)")
    else:
        lines.append(f"output_df = input_df.select(\n    {average}\n)")

    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code="\n".join(lines)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [*groups, output]

    messages = [WEIGHTED_AVG_ZERO_MESSAGE]
    status: ToolStatus = "converted"
    reason = "converted"
    if output != (values[_WEIGHTED_AVG_OUTPUT] or WEIGHTED_AVG_COLUMN):
        messages.append(
            f"The output name Alteryx wrote was already a group field here, so Flowfile called the "
            f"column {output!r}."
        )
        status, reason = "partial", "option_unsupported"
    untyped = [column for column in (value, weight) if _input_column_type(ctx, tool.tool_id, column) is None]
    if untyped:
        messages.append(WEIGHTED_AVG_UNKNOWN_TYPE_MESSAGE.format(columns=", ".join(repr(name) for name in untyped)))
        status, reason = "partial", "option_unsupported"
    return _row(tool, status, [node_id], "polars_code", messages, reason=reason)


# Create_Samples.yxmc's three output anchors, in the order its two percentages describe them.
CREATE_SAMPLES_ANCHORS = ("Estimation", "Validation", "Holdout")
_CREATE_SAMPLES_ESTIMATION = "estimation pct"
_CREATE_SAMPLES_VALIDATION = "validation pct"
_CREATE_SAMPLES_SEED = "rand seed"
_CREATE_SAMPLES_EXPECTED = frozenset({_CREATE_SAMPLES_ESTIMATION, _CREATE_SAMPLES_VALIDATION, _CREATE_SAMPLES_SEED})
CREATE_SAMPLES_MEMBERSHIP_MESSAGE = (
    "Flowfile and Alteryx shuffle with different generators, so the same seed puts different rows in "
    "each sample; only the sizes agree."
)
CREATE_SAMPLES_EMPTY_ANCHOR_MESSAGE = (
    "The two percentages add up to 100, so Alteryx's {anchor} sample is empty; Flowfile declares that "
    "anchor empty rather than emitting a split with no rows."
)


def _create_samples_percentage(raw: str) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value


def map_create_samples(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Create_Samples.yxmc splits rows into an estimation, a validation and a holdout sample.

    Comment box 94 of `CreateSamples.yxmd` states the arithmetic: the two percentages are named and
    "if the combined total is less than 100%, the remaining rows are returned in the holdout sample".
    `NodeRandomSplit` refuses a split of zero and requires the splits to sum to 100, so a holdout of
    nothing is left out of the node and the anchor is declared empty instead.
    """
    config = _config(tool)
    values = _macro_values(config)
    missing = sorted(_CREATE_SAMPLES_EXPECTED - set(values))
    if missing:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Create Samples configuration is missing expected settings: " + ", ".join(missing) + "."],
            reason="mapper_refused",
        )
    unrecognized = sorted(set(values) - _CREATE_SAMPLES_EXPECTED)
    if unrecognized:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Create Samples configuration has unrecognized settings: " + ", ".join(unrecognized) + "."],
            reason="option_unsupported",
        )
    estimation = _create_samples_percentage(values[_CREATE_SAMPLES_ESTIMATION])
    validation = _create_samples_percentage(values[_CREATE_SAMPLES_VALIDATION])
    refusal: str | None = None
    if estimation is None or validation is None:
        unreadable = _CREATE_SAMPLES_ESTIMATION if estimation is None else _CREATE_SAMPLES_VALIDATION
        refusal = f"the {unreadable} it writes is not a number"
    elif estimation < 0 or validation < 0:
        refusal = "a sample percentage is negative"
    elif estimation + validation > 100:
        refusal = f"its samples ask for {estimation:g}% + {validation:g}% of the rows, which is more than there are"
    elif estimation + validation == 0:
        refusal = "both sample percentages are zero, so every sample is empty"
    if refusal is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Create Samples was not converted because {refusal}."], reason="mapper_refused"
        )

    seed = _whole_number(values[_CREATE_SAMPLES_SEED]) if values[_CREATE_SAMPLES_SEED] else None
    shares = dict(zip(CREATE_SAMPLES_ANCHORS, [estimation, validation, 100.0 - estimation - validation], strict=True))
    splits = [
        input_schema.RandomSplitGroup(name=anchor, percentage=share) for anchor, share in shares.items() if share > 0
    ]
    settings = input_schema.NodeRandomSplit(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), splits=splits, seed=seed)
    node_id = ctx.add_node(tool, "random_split", settings, description=_description(tool))
    ctx.register_all_inputs(tool.tool_id, node_id)
    # The node's outputs are its splits in order, so the anchor a wire leaves from picks the handle.
    # Only the three anchors the macro names are registered: the tool has no unnamed output, so a
    # wire on any other anchor is reported dropped rather than handed one of the three samples.
    for index, split in enumerate(splits):
        ctx.register_output(tool.tool_id, split.name, node_id, f"output-{index}")
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)

    messages = [CREATE_SAMPLES_MEMBERSHIP_MESSAGE]
    for anchor, share in shares.items():
        if share > 0:
            continue
        message = CREATE_SAMPLES_EMPTY_ANCHOR_MESSAGE.format(anchor=anchor)
        ctx.inactive_outputs[(tool.tool_id, anchor)] = message
        if ctx.has_outgoing(tool.tool_id, anchor):
            messages.append(message)
    return _row(tool, "partial", [node_id], "random_split", messages, reason="option_unsupported")


GENERATE_ROWS_START_COLUMN = "_alteryx_generate_start"
GENERATE_ROWS_END_COLUMN = "_alteryx_generate_end"
GENERATE_ROWS_SEED_COLUMN = "_alteryx_generate_seed"
# Alteryx's DateTimeAdd units onto Polars interval strings; the same seven the translator maps.
_GENERATE_ROWS_INTERVALS = {
    "year": "y",
    "month": "mo",
    "week": "w",
    "day": "d",
    "hour": "h",
    "minute": "m",
    "second": "s",
}
_GENERATE_ROWS_INTEGER_TYPES = frozenset({"Int16", "Int32", "Int64"})
# The Flowfile date-arithmetic calls DateTimeAdd becomes; each one needs a real Date, not a text date.
_GENERATE_ROWS_DATE_CALLS = DATETIME_ADD_CALLS
_GENERATE_ROWS_DATE_TYPES = {"Date": "date_ranges", "Datetime": "datetime_ranges"}
_GENERATE_ROWS_STEP_RE = re.compile(r"^\[(?P<field>.+?)\]\s*\+\s*(?P<amount>\d+)$")
_GENERATE_ROWS_DATE_STEP_RE = re.compile(
    r"^DateTimeAdd\(\s*\[(?P<field>.+?)\]\s*,\s*(?P<amount>\d+)\s*,\s*(?P<quote>[\"'])(?P<unit>[A-Za-z]+)(?P=quote)\s*\)$",
    re.IGNORECASE,
)
_GENERATE_ROWS_COND_RE = re.compile(r"^\[(?P<field>.+?)\]\s*(?P<operator><=|<)\s*(?P<bound>.+)$", re.DOTALL)
GENERATE_ROWS_RECORD_COUNT_MESSAGE = (
    "Alteryx's record-count cap is {written} here, which this workflow does not say the meaning of; "
    "Flowfile reads it as no cap and generates every row the condition allows."
)
GENERATE_ROWS_TEXT_DATE_MESSAGE = (
    "Alteryx keeps a date as text and its date functions accept text; Flowfile's need a real Date "
    "column, so {expressions} raises at run time unless what it reads is already one."
)


def _bracket_field(expression: str, field: str) -> str:
    """Write every bare mention of *field* as `[field]`, leaving strings and brackets alone.

    Alteryx's own expression builder writes the generated column unbracketed — `RowCount <= 10` —
    and the translator refuses a bare identifier that names a function, which `RowCount` does. The
    caller knows this identifier is its own new column, so binding it here is a fact rather than a
    guess, the shape `map_dynamic_rename` uses for `[_CurrentField_]`.
    """
    out: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char in "\"'":
            end = expression.find(char, index + 1)
            end = len(expression) if end == -1 else end + 1
            out.append(expression[index:end])
            index = end
            continue
        if char == "[":
            end = expression.find("]", index + 1)
            end = len(expression) if end == -1 else end + 1
            out.append(expression[index:end])
            index = end
            continue
        if char.isalpha() or char == "_":
            end = index
            while end < len(expression) and (expression[end].isalnum() or expression[end] == "_"):
                end += 1
            word = expression[index:end]
            after = expression[end:].lstrip()
            out.append(f"[{field}]" if word == field and not after.startswith("(") else word)
            index = end
            continue
        out.append(char)
        index += 1
    return "".join(out)


def _generate_rows_step(loop: str, field: str, flowfile_type: str) -> tuple[str, str | None]:
    """The loop's constant step as a Polars argument, or why the loop is not a range.

    An Alteryx loop expression is a recurrence, and only the ones that add a fixed amount to the
    generated column are a range. The declared type picks which shape is allowed: box 170 of
    `Generate_Rows.yxmd` says `[date]+1` "would not have worked" on a date, and `DateTimeAdd` on an
    integer column is the same mismatch the other way round.
    """
    if flowfile_type in _GENERATE_ROWS_INTEGER_TYPES:
        match = _GENERATE_ROWS_STEP_RE.match(loop)
        if match is None or match.group("field") != field:
            return "", f"its loop expression {_one_line(loop)!r} is not {field!r} plus a fixed number"
        return match.group("amount"), None
    if flowfile_type in _GENERATE_ROWS_DATE_TYPES:
        match = _GENERATE_ROWS_DATE_STEP_RE.match(loop)
        if match is None or match.group("field") != field:
            return "", (f"its loop expression {_one_line(loop)!r} is not a DateTimeAdd of a fixed amount to {field!r}")
        interval = _GENERATE_ROWS_INTERVALS.get(match.group("unit").strip().lower().rstrip("s"))
        if interval is None:
            return "", f"the DateTimeAdd unit {match.group('unit')!r} in its loop expression has no Polars interval"
        return f"{match.group('amount')}{interval}", None
    return "", f"the column it creates is declared {flowfile_type}, which Flowfile cannot generate a range of"


def _generate_rows_code(
    tool: AlteryxTool,
    field: str,
    flowfile_type: str,
    start: str,
    end: str,
    step: str,
    inclusive: bool,
    drop: list[str],
    seeded: bool,
) -> str:
    """The range body: one list per input row, exploded into rows."""
    lines = [
        f"# Alteryx Generate Rows (ToolID {tool.tool_id}): {field!r} runs from the initialisation "
        f"expression to the condition's bound, stepping by {step}.",
        *(f"# {line}" for line in _original_config_lines(tool)),
    ]
    if flowfile_type in _GENERATE_ROWS_INTEGER_TYPES:
        # int_ranges stops before its end, so an inclusive `<=` needs one more step of room.
        stop = f"pl.col({end!r}).cast(pl.Int64) + {step}" if inclusive else f"pl.col({end!r}).cast(pl.Int64)"
        rows = f"pl.int_ranges(pl.col({start!r}).cast(pl.Int64), {stop}, {step})"
        cast = f".with_columns(pl.col({field!r}).cast(pl.{flowfile_type}))"
    else:
        # The bounds may arrive as Alteryx's own text dates or as real ones, and only the frame says
        # which, so the cast is chosen from the schema rather than assumed here.
        lines.extend(
            [
                "_schema = input_df.collect_schema()",
                f"_start = pl.col({start!r})",
                f"_end = pl.col({end!r})",
                f"_start = _start.str.strptime(pl.{flowfile_type}) if _schema[{start!r}] == pl.String else _start",
                f"_end = _end.str.strptime(pl.{flowfile_type}) if _schema[{end!r}] == pl.String else _end",
            ]
        )
        closed = "both" if inclusive else "left"
        rows = f"pl.{_GENERATE_ROWS_DATE_TYPES[flowfile_type]}(_start, _end, {step!r}, closed={closed!r})"
        cast = ""
    source = f"pl.select(pl.lit(1).alias({GENERATE_ROWS_SEED_COLUMN!r})).lazy()" if seeded else "input_df"
    lines.append(
        f"output_df = (\n    {source}\n    .with_columns({rows}.alias({field!r}))\n"
        f"    .filter(pl.col({field!r}).list.len() > 0)\n"
        f"    .explode({field!r})\n    .drop({drop!r})\n){cast}"
    )
    return "\n".join(lines)


def map_generate_rows(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Generate Rows loops a new column from an initialisation value while a condition holds.

    Only the shape that is a range converts: a loop that adds a fixed amount and a condition that
    compares the generated column with a bound. Anything else is a recurrence whose next value
    depends on the data — corpus tool 164 steps by `[category]` — and a range cannot express it.
    """
    config = _config(tool)
    field = _text(config, "CreateField_Name")
    known = ctx.input_columns(tool.tool_id)
    refusal: str | None = None
    if _flag(config, "UpdateField"):
        refusal = "it loops an existing column rather than creating one, which Flowfile cannot reproduce"
    elif not field:
        refusal = "it names no column to create"
    elif known is not None and field in known:
        refusal = f"the column {field!r} it creates already arrives here"
    flowfile_type = _map_alteryx_type(_text(config, "CreateField_Type"))
    if refusal is None and flowfile_type is None:
        refusal = f"the Alteryx type '{_text(config, 'CreateField_Type') or '(empty)'}' has no Flowfile equivalent"
    if refusal is None:
        refusal = _backslash_refusal({"the Generate Rows column": field})
    record_count = _attribute(config, "RecordCount", "value")
    if refusal is None and record_count.strip() not in ("", "0"):
        refusal = (
            f"it caps the run at {_one_line(record_count)} records, and this workflow does not state "
            "what Alteryx counts towards that cap"
        )
    if refusal is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Generate Rows was not converted because {refusal}."], reason="mapper_refused"
        )

    step, refusal = _generate_rows_step(_bracket_field(_text(config, "Expression_Loop"), field), field, flowfile_type)
    condition = _bracket_field(_text(config, "Expression_Cond"), field)
    if refusal is None:
        match = _GENERATE_ROWS_COND_RE.match(condition.strip())
        if match is None or match.group("field") != field:
            refusal = f"its condition {_one_line(condition)!r} is not {field!r} compared with '<' or '<=' to a bound"
        elif f"[{field}]" in match.group("bound"):
            refusal = f"its condition's bound reads {field!r}, the column the tool is still generating"
    if refusal is not None:
        return _placeholder_row(
            tool, ctx, [f"The Alteryx Generate Rows was not converted because {refusal}."], reason="mapper_refused"
        )

    inclusive = match.group("operator") == "<="
    columns = frozenset(known or ())
    initial = try_translate(_bracket_field(_text(config, "Expression_Init"), field), known_columns=columns)
    bound = try_translate(match.group("bound").strip(), known_columns=columns)
    for label, outcome, expression in (
        ("initialisation", initial, _text(config, "Expression_Init")),
        ("condition's bound", bound, match.group("bound").strip()),
    ):
        if outcome.translated is None:
            return _placeholder_row(
                tool,
                ctx,
                [
                    f"The Alteryx Generate Rows {label} expression could not be converted: {outcome.reason}.",
                    f"Original expression: {_one_line(expression)}",
                ],
                reason="translator_refused",
            )

    taken = [*(known or []), field]
    start_column = _unique_column(GENERATE_ROWS_START_COLUMN, taken)
    end_column = _unique_column(GENERATE_ROWS_END_COLUMN, [*taken, start_column])
    seeded = ctx.input_count(tool.tool_id) == 0
    drop = [start_column, end_column, *([GENERATE_ROWS_SEED_COLUMN] if seeded else [])]

    node_ids: list[int] = []
    previous: int | None = None
    if seeded:
        seed = input_schema.NodePolarsCode(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=f"output_df = pl.select(pl.lit(1).alias({GENERATE_ROWS_SEED_COLUMN!r})).lazy()"
            ),
        )
        previous = ctx.add_node(tool, "polars_code", seed, description=_description(tool))
        node_ids.append(previous)
    for index, (column, formula) in enumerate(((start_column, initial.translated), (end_column, bound.translated))):
        settings = input_schema.NodeFormula(
            flow_id=ctx.flow_id,
            node_id=ctx.new_node_id(),
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=column, data_type=transform_schema.AUTO_DATA_TYPE),
                function=formula,
            ),
        )
        node_id = ctx.add_node(
            tool, "formula", settings, dx=(index + 1) * FORMULA_STEP_DX, description=_description(tool)
        )
        node_ids.append(node_id)
        if previous is not None:
            _link(ctx, previous, node_id)
        previous = node_id

    code = _generate_rows_code(
        tool, field, flowfile_type, start_column, end_column, step, inclusive, drop, seeded=False
    )
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
    )
    last_id = ctx.add_node(tool, "polars_code", settings, dx=3 * FORMULA_STEP_DX, description=_description(tool))
    node_ids.append(last_id)
    _link(ctx, previous, last_id)
    if not seeded:
        ctx.register_all_inputs(tool.tool_id, node_ids[0])
    ctx.register_all_outputs(tool.tool_id, last_id)
    ctx.tool_columns[tool.tool_id] = [*(known or []), field]

    messages = [
        GENERATE_ROWS_RECORD_COUNT_MESSAGE.format(written="0" if record_count.strip() == "0" else "not written")
    ]
    if not seeded:
        messages.append(
            f"Alteryx runs this loop for every row that arrives and repeats that row's data on each "
            f"generated row; the generated {field!r} is exploded out of one list per input row, and a row "
            "whose loop never satisfies the condition is dropped, as Alteryx drops it."
        )
    text_dates = [
        label
        for label, outcome in (("initialisation", initial), ("condition's bound", bound))
        if flowfile_type in _GENERATE_ROWS_DATE_TYPES
        and any(f"{call}(" in outcome.translated for call in _GENERATE_ROWS_DATE_CALLS)
    ]
    if text_dates:
        messages.append(
            GENERATE_ROWS_TEXT_DATE_MESSAGE.format(expressions=" and ".join(f"the {label}" for label in text_dates))
        )
    return _row(tool, "partial", node_ids, "polars_code", messages, reason="option_unsupported")


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
    values = _macro_values(config)
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
    fields = _parse_quoted_field_list(values[_CLEANSE_FIELD_LIST])
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

    cleansing_input = transform_schema.DataCleansingInput(
        selection_mode="list",
        selected_columns=fields,
        case_mode=case_mode,
        **{target: _is_true(values.get(name)) for name, target in _CLEANSE_CHECKBOXES.items()},
    )
    return _emit_cleansing_node(tool, ctx, cleansing_input)


# Data Cleanse Pro writes self-describing elements instead of the macro's widget ids, and its own
# casing is inconsistent: `Checkbox_Replace*` has a lower-case b, `CheckBox_ModifyCase` a capital
# one. Every tag is spelled here exactly as Alteryx writes it.
_CLEANSE_PRO_FLAGS = {
    "RemoveNullRows": "remove_null_rows",
    "RemoveNullColumns": "remove_null_columns",
    "RemoveLeadingAndTrailingWhitespace": "trim_whitespace",
    "RemoveTabsLineBreaksAndDuplicates": "normalize_whitespace",
    "RemoveAllWhitespaces": "remove_all_whitespace",
    "RemoveLetters": "remove_letters",
    "RemoveNumbers": "remove_numbers",
    "RemovePunctuation": "remove_punctuation",
}
# Checkbox -> (the radio button it gates, the element Alteryx resolves the pair into, the setting).
_CLEANSE_PRO_REPLACEMENTS = (
    (
        "Checkbox_ReplaceStringColumns",
        "radioButton_ReplaceNullwithBlanks",
        "ReplaceWithBlanks",
        "replace_nulls_with_blank",
    ),
    ("Checkbox_ReplaceNumericColumns", "radioButton_ReplaceNullwithZero", "ReplaceWithZero", "replace_nulls_with_zero"),
)
# The inverse replacements, which Flowfile has no setting for; only refused when really switched on.
_CLEANSE_PRO_INVERSE = (
    ("Checkbox_ReplaceStringColumns", "radioButton_ReplaceBlankswithNulls", "replacing blanks with nulls"),
    ("Checkbox_ReplaceNumericColumns", "radioButton_ReplaceZerowithNulls", "replacing zeroes with nulls"),
)
_CLEANSE_PRO_UNSUPPORTED = {
    "RemoveHTMLTags": "removing HTML tags",
    "RemoveInvisibleCharacters": "removing invisible characters",
}
# Per-tool character overrides; an empty value is the default set the Flowfile node already applies.
_CLEANSE_PRO_CHARACTER_SETS = ("Letters", "Numbers", "Punctuations", "Exceptions")
_CLEANSE_PRO_CASE = ("CheckBox_ModifyCase", "ModifyCase")
_CLEANSE_PRO_TAGS = frozenset(
    {
        *_CLEANSE_PRO_FLAGS,
        *_CLEANSE_PRO_UNSUPPORTED,
        *_CLEANSE_PRO_CHARACTER_SETS,
        *_CLEANSE_PRO_CASE,
        *(name for pair in _CLEANSE_PRO_REPLACEMENTS for name in pair[:3]),
        *(pair[1] for pair in _CLEANSE_PRO_INVERSE),
        "Fields",
    }
)
CLEANSE_PRO_UNKNOWN_FIELD = "*Unknown"


def _cleanse_pro_selection(config: ET.Element) -> tuple[list[str], list[str], bool]:
    """``Fields`` as (every named field, the selected ones, whether ``*Unknown`` is selected).

    Data Cleanse Pro names its fields in ``@value``, not the ``@name`` every other tool uses, so
    this cannot be ``_field_selection``: reading the wrong attribute would silently select nothing.
    """
    names: list[str] = []
    selected: list[str] = []
    unknown_selected = False
    for element in config.findall("Fields/Field"):
        name = element.get("value") or ""
        is_selected = _is_true(element.get("selected"))
        if name == CLEANSE_PRO_UNKNOWN_FIELD:
            unknown_selected = is_selected
            continue
        if not name:
            continue
        names.append(name)
        if is_selected:
            selected.append(name)
    return names, selected, unknown_selected


def _cleanse_pro_replacements(config: ET.Element) -> tuple[dict[str, bool], str | None]:
    """The two null-replacement settings, or why the configuration contradicts itself.

    Alteryx keeps the radio button's last position when its checkbox is unticked (corpus tool 132
    still says "replace blanks with nulls" under a box nobody ticked), so neither element decides
    on its own. ``ReplaceWithBlanks``/``ReplaceWithZero`` are what Alteryx resolved the pair into,
    so they are the arbiter, and a disagreement means this reading of the tool is wrong.
    """
    settings: dict[str, bool] = {}
    for checkbox, radio, resolved, target in _CLEANSE_PRO_REPLACEMENTS:
        enabled = _flag(config, checkbox) is True and _flag(config, radio) is True
        if _flag(config, resolved) is not enabled:
            return {}, (
                f"This Alteryx Data Cleanse Pro says <{checkbox}> and <{radio}> together mean "
                f"{'on' if enabled else 'off'}, but its own <{resolved}> says the opposite; "
                "the tool was not converted rather than guessed at."
            )
        settings[target] = enabled
    return settings, None


def _cleanse_pro_refusal(config: ET.Element) -> str | None:
    """The first option Flowfile cannot express, in the tool's own words, or None."""
    unrecognized = sorted({child.tag for child in config} - _CLEANSE_PRO_TAGS)
    if unrecognized:
        return "This Alteryx Data Cleanse Pro carries settings Flowfile does not read: " + ", ".join(unrecognized) + "."
    for tag, description in _CLEANSE_PRO_UNSUPPORTED.items():
        if _flag(config, tag):
            return f"Alteryx Data Cleanse Pro option <{tag}> ({description}) has no Flowfile equivalent."
    for checkbox, radio, description in _CLEANSE_PRO_INVERSE:
        if _flag(config, checkbox) and _flag(config, radio):
            return f"Alteryx Data Cleanse Pro option <{radio}> ({description}) has no Flowfile equivalent."
    for tag in _CLEANSE_PRO_CHARACTER_SETS:
        if _attribute(config, tag, "value"):
            return (
                f"This Alteryx Data Cleanse Pro overrides the <{tag}> character set, which the Flowfile "
                "data cleansing node does not take."
            )
    return None


def map_data_cleanse_pro(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Maps Alteryx's Data Cleanse Pro tool onto the native data_cleansing node.

    The newer sibling of the Cleanse macro: the same rules, written as named elements rather than
    widget ids, plus per-tool character sets and HTML/invisible-character stripping that Flowfile
    has no equivalent for and refuses instead of dropping quietly.
    """
    config = _config(tool)
    refusal = _cleanse_pro_refusal(config)
    if refusal is not None:
        return _placeholder_row(tool, ctx, [refusal], reason="option_unsupported")
    replacements, contradiction = _cleanse_pro_replacements(config)
    if contradiction is not None:
        return _placeholder_row(tool, ctx, [contradiction], reason="mapper_refused")

    case_mode = "none"
    if _flag(config, "CheckBox_ModifyCase"):
        raw_case = _attribute(config, "ModifyCase", "value") or _text(config, "ModifyCase")
        case_mode = "none" if raw_case.lower() == "none" else _CLEANSE_CASE_MODES.get(raw_case.lower())
        if case_mode is None:
            return _placeholder_row(
                tool,
                ctx,
                [f"The Alteryx Data Cleanse Pro case mode '{raw_case or '(empty)'}' is not recognized."],
                reason="option_unsupported",
            )

    names, selected, unknown_selected = _cleanse_pro_selection(config)
    deselected = [name for name in names if name not in selected]
    messages: list[str] = []
    status: ToolStatus = "converted"
    reason = "converted"
    if not unknown_selected:
        selection = {"selection_mode": "list", "selected_columns": selected}
    elif not deselected:
        selection = {"selection_mode": "all", "selected_columns": []}
    else:
        # Every column except a named few: the set is only nameable if the schema is known here.
        known = ctx.input_columns(tool.tool_id)
        if known is None:
            return _placeholder_row(
                tool,
                ctx,
                [
                    "This Alteryx Data Cleanse Pro cleanses every column except "
                    + ", ".join(deselected)
                    + ", and the columns reaching it are not known at import time."
                ],
                reason="mapper_refused",
            )
        selection = {"selection_mode": "list", "selected_columns": [c for c in known if c not in deselected]}
        status, reason = "partial", "option_unsupported"
        messages.append(
            "This Alteryx Data Cleanse Pro cleanses every column except "
            + ", ".join(deselected)
            + "; Flowfile cleanses a named list, so the list was frozen to the columns reaching this tool "
            "at import time and a column added upstream later will not be cleansed."
        )

    cleansing_input = transform_schema.DataCleansingInput(
        case_mode=case_mode,
        **selection,
        **replacements,
        **{target: bool(_flag(config, tag)) for tag, target in _CLEANSE_PRO_FLAGS.items()},
    )
    return _emit_cleansing_node(tool, ctx, cleansing_input, messages, status=status, reason=reason)


SELECT_RECORDS_ROW = "__select_records_row"
_SELECT_RECORDS_RANGE_RE = re.compile(r"^(?:(\d+)\s*-\s*(\d+)|(\d+)\s*\+|-\s*(\d+)|(\d+))$")


def _select_records_ranges(raw: str) -> tuple[list[tuple[int, int | None]], str | None]:
    """The macro's range box as 1-based inclusive (start, end) pairs; ``None`` end means "to the end".

    Four forms and nothing else: ``N``, ``N-M``, ``N+`` and ``-N``. Anything the box holds that is
    not one of them is refused rather than skipped, because a token this mapper does not understand
    is rows the imported flow would silently keep or drop.
    """
    tokens = [token for token in re.split(r"[,\s]+", raw.strip()) if token]
    if not tokens:
        return [], "it names no rows at all"
    ranges: list[tuple[int, int | None]] = []
    for token in tokens:
        match = _SELECT_RECORDS_RANGE_RE.match(token)
        if match is None:
            return [], f"'{_one_line(token)}' is not one of the forms N, N-M, N+ or -N"
        start_end, end_of, from_start, to_end, single = match.groups()
        if start_end is not None:
            start, end = int(start_end), int(end_of)
        elif from_start is not None:
            start, end = int(from_start), None
        elif to_end is not None:
            start, end = 1, int(to_end)
        else:
            start = end = int(single)
        if start < 1 or (end is not None and end < 1):
            return [], f"'{_one_line(token)}' numbers rows from below 1, and Alteryx numbers them from 1"
        if end is not None and end < start:
            return [], f"'{_one_line(token)}' ends before it starts"
        ranges.append((start, end))
    return ranges, None


def _select_records_predicate(column: str, ranges: list[tuple[int, int | None]]) -> str:
    """The ranges as one expression in the Flowfile formula dialect: an OR over the ranges."""
    clauses = []
    for start, end in ranges:
        if end is None:
            clauses.append(f"[{column}] >= {start}")
        elif end == start:
            clauses.append(f"[{column}] = {start}")
        elif start == 1:
            clauses.append(f"[{column}] <= {end}")
        else:
            clauses.append(f"([{column}] >= {start} and [{column}] <= {end})")
    return " or ".join(clauses)


def map_select_records(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Maps the Select Records macro (SelectRecords.yxmc) onto record_id + filter + select.

    Alteryx picks rows by their position in the stream, which is not something a Flowfile filter can
    ask about, so the position is made into a column first and dropped again afterwards. The helper
    column is named against the incoming schema so it cannot shadow a column of the user's own; when
    that schema is not known at import time the name cannot be checked, so the report names it.
    """
    raw = _macro_values(_config(tool)).get("Ranges", "")
    ranges, refusal = _select_records_ranges(raw)
    if refusal is not None:
        return _placeholder_row(
            tool,
            ctx,
            [f"The Alteryx Select Records range '{_one_line(raw)}' was not converted because {refusal}."],
            reason="mapper_refused",
        )
    known = ctx.input_columns(tool.tool_id)
    column = _unique_column(SELECT_RECORDS_ROW, known)

    record_id = input_schema.NodeRecordId(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        record_id_input=transform_schema.RecordIdInput(output_column_name=column, offset=1),
    )
    record_id_node = ctx.add_node(tool, "record_id", record_id, description=_description(tool))
    row_filter = input_schema.NodeFilter(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        filter_input=transform_schema.FilterInput(
            mode="advanced", advanced_filter=_select_records_predicate(column, ranges)
        ),
    )
    filter_node = ctx.add_node(tool, "filter", row_filter, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)
    drop = input_schema.NodeSelect(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        keep_missing=True,
        select_input=[transform_schema.SelectInput(old_name=column, keep=False)],
    )
    drop_node = ctx.add_node(tool, "select", drop, dx=2 * FORMULA_STEP_DX, dy=2 * FORMULA_STEP_DY)
    _link(ctx, record_id_node, filter_node)
    _link(ctx, filter_node, drop_node)
    ctx.register_all_inputs(tool.tool_id, record_id_node)
    ctx.register_all_outputs(tool.tool_id, drop_node)
    ctx.tool_columns[tool.tool_id] = known

    node_ids = [record_id_node, filter_node, drop_node]
    messages = [
        f"Alteryx picks these rows by their position, so the position becomes a temporary column "
        f"('{column}') that the last of the three nodes drops again."
    ]
    if known is None:
        messages.append(
            f"The columns reaching this tool are not known at import time, so that name was not checked "
            f"against them; rename a column of your own called '{column}' before this node."
        )
    if _feeds_in_stated_order(ctx, tool.tool_id):
        return _row(tool, "converted", node_ids, "filter", messages, reason="converted")
    messages.append(
        "Which rows those positions are depends on the order the rows arrive in, which this workflow "
        "does not state; sort the rows upstream if the order matters."
    )
    return _row(tool, "partial", node_ids, "filter", messages, reason="row_order_unknown")


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


CORRELATION_VARIABLE_COLUMN = "Variable"
_NUMERIC_TYPES = frozenset({"Int16", "Int32", "Int64", "Float32", "Float64"})
CORRELATION_LAYOUT_MESSAGE = (
    "Two things about this grid are Flowfile's choice, not Alteryx's, and are worth checking against "
    "a Designer run: the name of the leading variable column ({column!r} here — Alteryx's own name for "
    "it is not recorded in the workflow), and the order of the rows, which follows the field list."
)
# What a null does is a third thing worth checking, and it is not a layout choice: one XML flag
# inside this one tool swaps the rule, because the correlation branch is numpy's corrcoef and the
# covariance branch is a pairwise reduction. Alteryx's rule is stated in no workflow here, so the
# honest move is to name Flowfile's rather than to pick one and stay quiet about it.
CORRELATION_NULL_MESSAGE = (
    "A null changes the answer, and it changes it differently in the two branches of this one tool. "
    "The correlation grid is computed the way numpy's corrcoef computes it, so a single null anywhere "
    "in a variable makes that whole variable's row and column NaN — its own diagonal included — while "
    "the Covariance option drops the affected pair of rows instead. Alteryx's own null rule is stated "
    "nowhere in this workflow, so check any column holding nulls against a Designer run."
)
COVARIANCE_MESSAGE = (
    "Alteryx's Covariance option replaces the correlation with the covariance rather than adding it "
    "(Pearson_Correlation.yxmd's own comment boxes 22 and 42 put the two as alternatives), so the grid "
    "holds covariances and its diagonal is each variable's variance, not 1. Every corpus instance using "
    "the option compares exactly two variables, so the n x n covariance grid is the correlation tool's "
    "layout carried over; verify it before trusting a wider one. Two more things the workflow does not "
    "state: the divisor is n-1, the sample covariance (Polars' pl.cov defaults to ddof=1), and a null "
    "in either variable drops that pair of rows rather than the whole column."
)
CORRELATION_UNKNOWN_TYPE_MESSAGE = (
    "The type of {columns} is not known at import time (only a Text Input or a typed file read states "
    "one Flowfile will really produce), so a non-numeric column here would raise when the flow runs "
    "instead of being refused now."
)


def _missing_column_refusal(known: list[str] | None, columns: list[str]) -> str | None:
    """Why a named column cannot be read here: the input is known and does not hold it.

    "The type of 'x' is not known at import time" is a sentence about a column that *arrives* and
    whose width Flowfile cannot state. A column that does not arrive at all is a different fact, and
    saying the first about the second reads as a hedge while the flow dies at run time on a missing
    column — which is what happens today whenever the upstream is a Text Input, the one source that
    does say what it holds.
    """
    if known is None:
        return None
    missing = [column for column in columns if column not in known]
    if not missing:
        return None
    return (
        f"the column(s) {', '.join(repr(column) for column in missing)} do not reach this tool; what "
        f"arrives is {', '.join(repr(column) for column in known) or '(nothing)'}"
    )


def _correlation_fields(tool: AlteryxTool, ctx: EmitContext, config: ET.Element) -> tuple[list[str], str | None]:
    """The variables the grid compares, or why the tool will not be converted.

    ``*Unknown`` means "every other numeric column": Designer's field list offers only numeric
    columns, which the corpus shows directly — every one of the 15 instances lists the four numeric
    columns of its input and neither of the two string ones, whether selected or not. So it is
    frozen to the numeric columns arriving at import time, the shape DataCleansePro's
    tool 133 already has, and a column whose type Flowfile cannot state makes that freeze impossible.

    ``*Unknown`` stands for the columns the field list does not *name*, so the exclusion is on
    ``names`` and not on ``selected``: a listed field the user unticked was excluded on purpose, and
    reading it back out of ``*Unknown`` would correlate a variable the workflow says to leave out.
    """
    names, selected, unknown_selected = _field_selection(config)
    known = ctx.input_columns(tool.tool_id)
    if unknown_selected:
        if known is None:
            return [], (
                "'*Unknown' is selected and the columns reaching this tool are not known at import time, "
                "so the tool's field list cannot be resolved"
            )
        for column in known:
            if column not in names:
                if _input_column_type(ctx, tool.tool_id, column) is None:
                    return [], (
                        f"'*Unknown' is selected and the type of the column {column!r} reaching this tool is "
                        "not known at import time, so whether Alteryx would offer it as a variable cannot be read"
                    )
                if _input_column_type(ctx, tool.tool_id, column) in _NUMERIC_TYPES:
                    selected.append(column)
    if len(selected) != len(set(selected)):
        return [], "a field is selected more than once, and a grid cannot have two rows with one name"
    if len(selected) < 2:
        return [], f"{len(selected)} field(s) are selected and a correlation compares at least two"
    absent = _missing_column_refusal(known, selected)
    if absent is not None:
        return [], absent
    for column in selected:
        declared = _input_column_type(ctx, tool.tool_id, column)
        if declared is not None and declared not in _NUMERIC_TYPES:
            return [], (f"the selected field {column!r} is a {declared} column, which has no correlation to compute")
    return selected, None


def _correlation_code(tool: AlteryxTool, fields: list[str], label: str, covariance: bool) -> str:
    """The n x n grid Alteryx's own comment boxes describe: one row per variable, diagonal 1."""
    header = (
        "the covariance of every pair of variables, one row per variable"
        if covariance
        else (
            "the correlation of every pair of variables, one row per variable; the diagonal is 1 unless "
            "a variable holds nulls or overflows"
        )
    )
    lines = [
        f"# Alteryx Pearson Correlation (ToolID {tool.tool_id}): {header}.",
        f"# The leading {label!r} column names the row's variable; Alteryx's own name for it is not in the file.",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"_fields = {fields!r}",
        "_df = input_df.select(_fields).collect()",
    ]
    if covariance:
        lines.extend(
            [
                "output_df = pl.DataFrame(",
                f"    [pl.Series({label!r}, _fields)]",
                "    + [pl.Series(_b, [_df.select(pl.cov(_a, _b)).item() for _a in _fields]) for _b in _fields]",
                ").lazy()",
            ]
        )
    else:
        lines.append(f"output_df = _df.corr(label={label!r}).lazy()")
    return "\n".join(lines)


def map_pearson_correlation(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Pearson Correlation compares every selected variable with every other.

    The output is an n x n grid with a leading column naming each row's variable, which is what
    `Pearson_Correlation.yxmd`'s comment boxes 41 and 22 describe. Polars' ``DataFrame.corr`` builds
    exactly that grid and will even write the leading column, but the *name* of that column is
    Flowfile's invention — Alteryx does not record it in the workflow — so the row says so and stays
    ``partial``, the same treatment the Rank column's name gets.
    """
    config = _config(tool)
    fields, refusal = _correlation_fields(tool, ctx, config)
    if refusal is not None:
        message = f"The Alteryx Pearson Correlation was not converted because {refusal}."
        return _placeholder_row(tool, ctx, [message], reason="mapper_refused")
    backslash = _backslash_refusal({"a correlation field": fields})
    if backslash is not None:
        return _placeholder_row(tool, ctx, [backslash], reason="option_unsupported")

    covariance = _is_true(_attribute(config, "Covariance", "value"))
    label = _unique_column(CORRELATION_VARIABLE_COLUMN, fields)
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_correlation_code(tool, fields, label, covariance)
        ),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [label, *fields]

    messages = [CORRELATION_LAYOUT_MESSAGE.format(column=label), CORRELATION_NULL_MESSAGE]
    if covariance:
        messages.append(COVARIANCE_MESSAGE)
    untyped = [column for column in fields if _input_column_type(ctx, tool.tool_id, column) is None]
    if untyped:
        messages.append(CORRELATION_UNKNOWN_TYPE_MESSAGE.format(columns=_one_line(", ".join(repr(c) for c in untyped))))
    return _row(tool, "partial", [node_id], "polars_code", messages, reason="option_unsupported")


SPEARMAN_ANCHOR = "Field Selection"
SPEARMAN_COLUMN = "Spearman"
_SPEARMAN_VARIABLES = ("Variable1", "Variable2")
SPEARMAN_LAYOUT_MESSAGE = (
    "The shape of this output is Flowfile's choice, not Alteryx's: one row holding the coefficient in "
    "a column named {column!r}{grouped}. The macro does not record what it calls its own output "
    "columns, so check them against a Designer run before anything downstream reads them by name. A "
    "null in either variable drops that pair of rows rather than the whole column — the opposite of "
    "the Pearson grid, where one null makes a whole row and column NaN — and the macro states no null "
    "rule of its own either."
)
SPEARMAN_TIES_MESSAGE = (
    "Polars ranks tied values by their average rank, which is the textbook Spearman definition; "
    "Alteryx's own tie rule is not stated in the workflow, so a column with repeated values may not "
    "give the same coefficient in both tools."
)


def _spearman_variables(values: dict[str, str]) -> tuple[list[str], str | None]:
    """The two columns the macro compares, read from its newline-separated field-selection answer."""
    assignments: dict[str, str] = {}
    for line in values.get(f"Input.{SPEARMAN_ANCHOR}", "").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            assignments[key.strip()] = value.strip()
    missing = [name for name in _SPEARMAN_VARIABLES if not assignments.get(name)]
    if missing:
        return [], f"the macro's field selection names no column for {' and '.join(missing)}"
    variables = [assignments[name] for name in _SPEARMAN_VARIABLES]
    if variables[0] == variables[1]:
        return [], f"both variables are the column {variables[0]!r}, which correlates with itself by definition"
    return variables, None


def _spearman_group_field(values: dict[str, str], variables: list[str]) -> tuple[str | None, str | None]:
    """The grouping column, only when the switch above it is on.

    12 of the 13 corpus instances leave ``Enable Group By`` off while ``Select Field to Group By``
    still holds a name — sometimes a real column, sometimes one from a different workflow — so the
    switch is read first and the field is not looked at at all when it is off.
    """
    if not _is_true(values.get("Enable Group By")):
        return None, None
    field = values.get("Select Field to Group By", "").strip()
    if not field:
        return None, "'Enable Group By' is on and no grouping column is named"
    if field in variables:
        return None, (
            f"the grouping column {field!r} is also one of the two variables, so every group would hold "
            "a single value of it"
        )
    return field, None


def _spearman_code(tool: AlteryxTool, variables: list[str], group: str | None, column: str) -> str:
    """``pl.corr(..., method='spearman')``, per group when the macro's Group By switch is on."""
    first, second = variables
    correlation = f"pl.corr(pl.col({first!r}), pl.col({second!r}), method='spearman').alias({column!r})"
    header = (
        f"# Alteryx Spearman Rank Correlation (ToolID {tool.tool_id}): {first!r} against {second!r}"
        f"{f', per {group!r}' if group else ''}."
    )
    body = (
        f"output_df = input_df.group_by(pl.col({group!r}), maintain_order=True).agg({correlation})"
        if group
        else f"output_df = input_df.select({correlation})"
    )
    return "\n".join(
        [
            header,
            "# Polars ranks ties by their average rank; Alteryx's own tie rule is not in the workflow.",
            *(f"# {line}" for line in _original_config_lines(tool)),
            body,
        ]
    )


def map_spearman_correlation(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """The Spearman macro correlates two columns by rank, optionally within groups.

    Its input arrives on a ``Field Selection`` anchor rather than ``Input`` on all 13 corpus
    instances, and its questions are the macro's flat ``<Value name>`` list, so both the columns and
    the grouping switch are read from there. The coefficient itself is exact — a rank correlation has
    one definition — but the shape Alteryx wraps it in is not recorded anywhere in the workflow, so
    the row stays ``partial`` and says which parts of the output are the import's invention.
    """
    values = _macro_values(_config(tool))
    variables, refusal = _spearman_variables(values)
    if refusal is None:
        group, refusal = _spearman_group_field(values, variables)
    if refusal is not None:
        message = f"The Alteryx Spearman Correlation was not converted because {refusal}."
        return _placeholder_row(tool, ctx, [message], reason="mapper_refused")

    columns = [*variables, *([group] if group else [])]
    backslash = _backslash_refusal({"a Spearman column": columns})
    if backslash is not None:
        return _placeholder_row(tool, ctx, [backslash], reason="option_unsupported")
    known = ctx.input_columns(tool.tool_id, SPEARMAN_ANCHOR)
    absent = _missing_column_refusal(known, columns)
    if absent is not None:
        message = f"The Alteryx Spearman Correlation was not converted because {absent}."
        return _placeholder_row(tool, ctx, [message], reason="mapper_refused")
    for column in variables:
        declared = _input_column_type(ctx, tool.tool_id, column, SPEARMAN_ANCHOR)
        if declared is not None and declared not in _NUMERIC_TYPES:
            message = (
                f"The Alteryx Spearman Correlation was not converted because the variable {column!r} is a "
                f"{declared} column, which has no rank correlation to compute."
            )
            return _placeholder_row(tool, ctx, [message], reason="mapper_refused")

    label = _unique_column(SPEARMAN_COLUMN, known)
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=_spearman_code(tool, variables, group, label)),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [*([group] if group else []), label]

    messages = [
        SPEARMAN_LAYOUT_MESSAGE.format(
            column=label, grouped=f", one per value of {group!r}" if group else " and nothing else"
        ),
        SPEARMAN_TIES_MESSAGE,
    ]
    untyped = [column for column in variables if _input_column_type(ctx, tool.tool_id, column, SPEARMAN_ANCHOR) is None]
    if untyped:
        messages.append(CORRELATION_UNKNOWN_TYPE_MESSAGE.format(columns=", ".join(repr(c) for c in untyped)))
    return _row(tool, "partial", [node_id], "polars_code", messages, reason="option_unsupported")


# The one input anchor the corpus establishes by name: all four Field Summary instances are wired
# on `Field Input`. Registering it alone, rather than `register_all_inputs`, is what makes a wire
# onto any other anchor a dropped connection instead of a silent extra stream into the profile.
FIELD_SUMMARY_INPUT_ANCHOR = "Field Input"
FIELD_SUMMARY_REPORT_ANCHORS = ("Reports", "Interactive")
PROFILE_COLUMNS = ["Name", "Type", "PercentMissing", "UniqueValues"]
_FIELD_SUMMARY_KEYS = ("Select Fields", "Sample Data", "Number", "NNumber", "Percent", "NPercent")
PROFILE_COLUMN_SET_MESSAGE = (
    "Alteryx's own comment boxes name only two of this table's columns — Percent Missing and Unique "
    "Values — so the column set and their names here ({columns}) are Flowfile's, not Alteryx's. "
    "Check them against a Designer run before anything downstream reads them by name. "
    "'PercentMissing' counts nulls and nothing else: an empty string is a value here, and whether "
    "Alteryx counts one as missing is stated nowhere in the workflow."
)
FIELD_SUMMARY_REPORT_MESSAGE = (
    "The Alteryx Field Summary Report's '{anchor}' anchor produces a rendered report, which Flowfile "
    "has no node for; only the data anchor was imported, so the connections from '{anchor}' were not "
    "wired."
)
FIELD_SUMMARY_ALL_FIELDS_MESSAGE = (
    "Every field in the tool's list is selected, so the node profiles every column that reaches it "
    "rather than the names the list holds. Those names are Alteryx's cached schema, which is not "
    "always what arrives: a spatial column read from a `.yxdb` is renamed on the way into Flowfile."
)
FIELD_SUMMARY_SAMPLE_MESSAGE = (
    "'Sample Data' is on, so Alteryx profiles {size} rather than the whole input — and its own comment "
    "box 12 says the sample is different on every run. The imported node profiles every row, which is "
    "a different answer, not a slower one."
)
BASIC_PROFILE_LIMIT_MESSAGE = (
    "Alteryx stops counting distinct values at {unique_count} and stops keeping a value once it is "
    "{unique_size} characters wide; the generated code applies no such limit, so a very wide or very "
    "high-cardinality column costs more here and its unique count is exact where Alteryx's is capped."
)
BASIC_PROFILE_METRIC_MESSAGE = (
    "The tool's 'IsMetric' setting only changes the units of its spatial measurements, which are out "
    "of scope for this importer, so it was read and ignored."
)


def _profile_code(tool: AlteryxTool, label: str, fields: list[str] | None, extremes: bool) -> str:
    """One row per column: its type, the percentage of nulls, and how many distinct values it holds."""
    selection = f"input_df.select({fields!r})" if fields is not None else "input_df"
    columns = [*PROFILE_COLUMNS, *(["Min", "Max"] if extremes else [])]
    lines = [
        f"# Alteryx {label} (ToolID {tool.tool_id}): one row per column of the input.",
        f"# Columns: {', '.join(columns)}. Alteryx's own names for them are not recorded in the workflow.",
        *(f"# {line}" for line in _original_config_lines(tool)),
        f"_source = {selection}",
        "_schema = _source.collect_schema()",
        "_names = list(_schema.names())",
        "_stats = _source.select(",
        "    [pl.col(_name).null_count().alias(f'_null_{_index}') for _index, _name in enumerate(_names)]",
        "    + [pl.col(_name).n_unique().alias(f'_uniq_{_index}') for _index, _name in enumerate(_names)]",
    ]
    if extremes:
        lines.append(
            "    + [pl.col(_name).min().cast(pl.String).alias(f'_min_{_index}') for _index, _name in enumerate(_names)]"
        )
        lines.append(
            "    + [pl.col(_name).max().cast(pl.String).alias(f'_max_{_index}') for _index, _name in enumerate(_names)]"
        )
    lines.extend(
        [
            "    + [pl.len().alias('_rows')]",
            ").collect().row(0, named=True)",
            "_rows = _stats['_rows']",
            "_profile = {",
            "    'Name': _names,",
            "    'Type': [str(_type) for _type in _schema.dtypes()],",
            "    'PercentMissing': [",
            "        (100.0 * _stats[f'_null_{_index}'] / _rows) if _rows else None for _index in range(len(_names))",
            "    ],",
            "    'UniqueValues': [_stats[f'_uniq_{_index}'] for _index in range(len(_names))],",
        ]
    )
    if extremes:
        lines.append("    'Min': [_stats[f'_min_{_index}'] for _index in range(len(_names))],")
        lines.append("    'Max': [_stats[f'_max_{_index}'] for _index in range(len(_names))],")
    lines.extend(
        [
            "}",
            "output_df = pl.LazyFrame(",
            "    _profile,",
            "    schema={",
            "        'Name': pl.String,",
            "        'Type': pl.String,",
            "        'PercentMissing': pl.Float64,",
            "        'UniqueValues': pl.UInt32,",
            *(["        'Min': pl.String,", "        'Max': pl.String,"] if extremes else []),
            "    },",
            ")",
        ]
    )
    return "\n".join(lines)


def _field_summary_fields(values: dict[str, str]) -> tuple[list[str] | None, str | None]:
    """The macro's comma-joined ``Name=True,Name=False`` list; the names themselves may hold spaces.

    ``None`` means "every column", which is what a list with nothing deselected asks for — and it is
    emitted as *no* selection rather than as the names, the shape Transpose's ``*Unknown`` takes.
    Naming them would be a claim about what arrives, and the importer does not know that: Alteryx's
    cached schema names a spatial column plainly, where the Parquet that `flowfile convert yxdb`
    wrote for the same `.yxdb` gives it a ``__spatial`` suffix, because a spatial column is renamed
    on the way in.
    Profiling whatever reaches the node is also what Alteryx itself does with an all-selected list.
    """
    raw = values.get("Select Fields", "")
    selected: list[str] = []
    listed = 0
    for entry in raw.split(","):
        name, separator, flag = entry.rpartition("=")
        if not separator or not name:
            return None, f"its field list entry {entry.strip()!r} is not a 'Name=True' pair"
        listed += 1
        if _is_true(flag):
            selected.append(name)
    if not selected:
        return None, "no field is selected"
    if len(selected) != len(set(selected)):
        return None, "a field is selected more than once"
    return (None if len(selected) == listed else selected), None


def _field_summary_sample(values: dict[str, str]) -> tuple[str | None, str | None]:
    """How much Alteryx would profile, or why the exclusive count/percentage pair cannot be read."""
    if not _is_true(values.get("Sample Data")):
        return None, None
    by_count, by_percent = _is_true(values.get("Number")), _is_true(values.get("Percent"))
    if by_count == by_percent:
        chosen = "both a record count and a percentage" if by_count else "neither a record count nor a percentage"
        return None, f"'Sample Data' is on and the tool selects {chosen}, so the sample size cannot be read"
    if by_count:
        size = _whole_number(values.get("NNumber") or "", minimum=1)
        if size is None:
            return None, "the sample record count could not be read"
        return f"a random {size} rows", None
    percent = _whole_number(values.get("NPercent") or "", minimum=1)
    if percent is None:
        return None, "the sample percentage could not be read"
    return f"a random {percent}% of the rows", None


def map_field_summary_report(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """The Field Summary Report macro profiles each selected column of its input.

    Its data anchor's XML name is not established anywhere available: the workflow's comment boxes
    call the three outputs "O", "R" and "I", but the two wires the corpus really draws name their
    anchors ``Reports`` and ``Interactive``, so the labels are not the anchor names and "O" cannot be
    assumed to be one. The profile is therefore registered on the tool's *default* output, which is
    what any anchor the mappers do not name falls back to — while the two rendered-report anchors are
    declared empty by name, so their consumers are told the wire was dropped instead of being handed
    the profile table under a different question's name.

    The input side is the opposite: `Field Input` is a name the corpus does establish, so only that
    anchor is registered. `register_all_inputs` would register whatever a wire happened to arrive
    on, which laid a wire addressed to a nonexistent anchor onto the profile's own input and said
    nothing about it; an unknown anchor now reaches no node and is reported on both rows.
    """
    values = _macro_values(_config(tool))
    unrecognized = sorted(set(values) - set(_FIELD_SUMMARY_KEYS))
    if unrecognized:
        message = "The Field Summary Report configuration has settings Flowfile does not read: " + ", ".join(
            unrecognized
        )
        return _placeholder_row(tool, ctx, [f"{message}."], reason="option_unsupported")
    fields, refusal = _field_summary_fields(values)
    if refusal is None:
        sample, refusal = _field_summary_sample(values)
    if refusal is not None:
        message = f"The Alteryx Field Summary Report was not converted because {refusal}."
        return _placeholder_row(tool, ctx, [message], reason="mapper_refused")
    backslash = _backslash_refusal({"a Field Summary field": fields or []})
    if backslash is not None:
        return _placeholder_row(tool, ctx, [backslash], reason="option_unsupported")

    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_profile_code(tool, "Field Summary Report", fields, extremes=False)
        ),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_output(tool.tool_id, DEFAULT_OUTPUT_ANCHOR, node_id)
    ctx.register_input(tool.tool_id, FIELD_SUMMARY_INPUT_ANCHOR, node_id)
    ctx.tool_columns[tool.tool_id] = list(PROFILE_COLUMNS)

    messages = [PROFILE_COLUMN_SET_MESSAGE.format(columns=", ".join(PROFILE_COLUMNS))]
    if fields is None:
        messages.append(FIELD_SUMMARY_ALL_FIELDS_MESSAGE)
    for anchor in FIELD_SUMMARY_REPORT_ANCHORS:
        message = FIELD_SUMMARY_REPORT_MESSAGE.format(anchor=anchor)
        ctx.inactive_outputs[(tool.tool_id, anchor)] = message
        if ctx.has_outgoing(tool.tool_id, anchor):
            messages.append(message)
    if sample is not None:
        messages.append(FIELD_SUMMARY_SAMPLE_MESSAGE.format(size=sample))
    return _row(tool, "partial", [node_id], "polars_code", messages, reason="option_unsupported")


def map_basic_data_profile(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Alteryx's Basic Data Profile is the same per-column summary, plus each column's extremes."""
    config = _config(tool)
    unique_count = _whole_number(_text(config, "Limit_UniqueCount"), minimum=1)
    unique_size = _whole_number(_text(config, "Limit_UniqueValuesSize"), minimum=1)
    if unique_count is None or unique_size is None:
        message = (
            "The Alteryx Basic Data Profile was not converted because its distinct-value limits could not be read."
        )
        return _placeholder_row(tool, ctx, [message], reason="mapper_refused")

    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=_profile_code(tool, "Basic Data Profile", None, extremes=True)
        ),
    )
    node_id = ctx.add_node(tool, "polars_code", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    columns = [*PROFILE_COLUMNS, "Min", "Max"]
    ctx.tool_columns[tool.tool_id] = columns

    messages = [
        PROFILE_COLUMN_SET_MESSAGE.format(columns=", ".join(columns)),
        BASIC_PROFILE_LIMIT_MESSAGE.format(unique_count=unique_count, unique_size=unique_size),
    ]
    if config.find("IsMetric") is not None:
        messages.append(BASIC_PROFILE_METRIC_MESSAGE)
    return _row(tool, "partial", [node_id], "polars_code", messages, reason="option_unsupported")


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
    "DataCleansePro": map_data_cleanse_pro,
    "DateTime": map_date_time,
    "DateTimeNow": map_date_time_now,
    "GenerateRows": map_generate_rows,
    "Rank": map_rank,
    "PearsonCorrelation": map_pearson_correlation,
    "BasicDataProfile": map_basic_data_profile,
}


MACRO_MAPPERS: dict[str, ToolMapper] = {
    "cleanse.yxmc": map_data_cleansing,
    "countrecords.yxmc": map_count_records,
    "randomrecords.yxmc": map_random_records,
    "selectrecords.yxmc": map_select_records,
    "spearmancorrcoeff.yxmc": map_spearman_correlation,
    "field_summary_report.yxmc": map_field_summary_report,
    "imputation_v3.yxmc": map_imputation,
    "weightedavg.yxmc": map_weighted_average,
    "create_samples.yxmc": map_create_samples,
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
