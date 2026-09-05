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
from flowfile_core.flowfile.converters.alteryx.report import ToolReportRow, ToolStatus
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
    # best-effort column tracker: tool id -> columns leaving that tool (None = unknown)
    tool_columns: dict[int, list[str] | None] = field(default_factory=dict)
    # every parsed tool by id, so a mapper can read the tool feeding one of its anchors
    tools: dict[int, AlteryxTool] = field(default_factory=dict)
    # (dest tool id, dest anchor) pairs a mapper resolved at convert time and does not want wired
    suppressed_inputs: set[tuple[int, str]] = field(default_factory=set)
    next_node_id: int = 0

    def new_node_id(self) -> int:
        self.next_node_id += 1
        return self.next_node_id

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

    def has_outgoing(self, tool_id: int, anchor: str) -> bool:
        return any(connection.origin_anchor == anchor for connection in self.outbound.get(tool_id, []))

    def input_columns(self, tool_id: int, anchor: str = DEFAULT_INPUT_ANCHOR) -> list[str] | None:
        """Columns arriving on one anchor, when they are confidently known."""
        for connection in self.inbound.get(tool_id, []):
            if connection.dest_anchor == anchor:
                return self.tool_columns.get(connection.origin_tool_id)
        return None

    def source_tool(self, tool_id: int, anchors: tuple[str, ...]) -> AlteryxTool | None:
        """The tool feeding the first of ``anchors`` that is actually wired."""
        for anchor in anchors:
            for connection in self.inbound.get(tool_id, []):
                if connection.dest_anchor == anchor:
                    return self.tools.get(connection.origin_tool_id)
        return None

    def suppress_input(self, tool_id: int, anchors: tuple[str, ...]) -> None:
        """Mark anchors this mapper resolved at convert time so wiring skips them silently."""
        self.suppressed_inputs.update((tool_id, anchor) for anchor in anchors)


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
) -> ToolReportRow:
    return ToolReportRow(
        alteryx_tool_id=tool.tool_id,
        alteryx_tool=tool_label(tool),
        flowfile_node_ids=node_ids,
        flowfile_node_type=node_type,
        status=status,
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


def _one_line(value: str, limit: int = 200) -> str:
    collapsed = " ".join(value.split())
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


def _config_xml_lines(tool: AlteryxTool) -> list[str]:
    """The tool's ``<Configuration>`` element pretty-printed into lines."""
    if tool.configuration is None:
        return []
    element = copy.deepcopy(tool.configuration)
    ET.indent(element, space="  ")
    try:
        rendered = ET.tostring(element, encoding="unicode")
    except (TypeError, ValueError):
        return []
    lines = [line.rstrip()[:CONFIG_COMMENT_LINE_LIMIT] for line in rendered.splitlines() if line.strip()]
    if len(lines) > CONFIG_COMMENT_MAX_LINES:
        dropped = len(lines) - CONFIG_COMMENT_MAX_LINES
        lines = [*lines[:CONFIG_COMMENT_MAX_LINES], f"... ({dropped} more lines; see the original .yxmd)"]
    return lines


def _original_config_lines(tool: AlteryxTool) -> list[str]:
    """Everything the .yxmd said about this tool, so it can be rebuilt without the original file."""
    lines: list[str] = []
    annotation = tool.default_annotation or tool.annotation
    if annotation:
        lines.extend(f"Alteryx annotation: {part.strip()}" for part in annotation.splitlines() if part.strip())
    config = _config_xml_lines(tool)
    if config:
        lines.append(f"Original Alteryx configuration ({tool.plugin or tool_label(tool)}):")
        lines.extend(config)
    return lines


def _placeholder_code(tool: AlteryxTool, num_inputs: int, notes: list[str]) -> str:
    lines = [
        f"# Alteryx tool '{tool_label(tool)}' (ToolID {tool.tool_id}) could not be converted automatically.",
        "# This node passes its input through unchanged; rebuild the logic here.",
    ]
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
    num_inputs: int | None = None,
    dx: int = 0,
    dy: int = 0,
    register_anchors: bool = True,
) -> int:
    """Emit the polars_code passthrough that keeps the graph shape intact."""
    inputs = ctx.input_count(tool.tool_id) if num_inputs is None else num_inputs
    settings = input_schema.NodePolarsCode(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=_placeholder_code(tool, inputs, messages)),
    )
    warning = f"{WARNING_PREFIX}Needs manual conversion: Alteryx '{tool_label(tool)}' (ToolID {tool.tool_id})"
    node_id = ctx.add_node(
        tool,
        "polars_code",
        settings,
        dx=dx,
        dy=dy,
        description=_description(tool, warning),
        is_start_node=inputs == 0,
    )
    if register_anchors:
        ctx.register_all_outputs(tool.tool_id, node_id)
        ctx.register_all_inputs(tool.tool_id, node_id)
    return node_id


def map_unsupported(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    """Fallback mapper for every tool without a dedicated implementation."""
    message = f"Alteryx tool '{tool_label(tool)}' has no Flowfile equivalent; a passthrough placeholder was inserted."
    node_id = emit_placeholder(tool, ctx, [])
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, "placeholder", [node_id], "polars_code", [message])


def _placeholder_row(tool: AlteryxTool, ctx: EmitContext, messages: list[str]) -> ToolReportRow:
    node_id = emit_placeholder(tool, ctx, messages)
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, "placeholder", [node_id], "polars_code", messages)


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
    return _row(tool, "converted", [node_id], "manual_input", messages)


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
    if unmapped_types:
        status = "partial"
        messages.append(
            "These Alteryx data types have no Flowfile equivalent and were left unchanged: " + ", ".join(unmapped_types)
        )
    return _row(tool, status, [node_id], "select", messages)


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
        return _placeholder_row(tool, ctx, messages)

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
    return _row(tool, "converted", [node_id], "filter", [])


def _commented_formula_body(expression: str, reason: str, stub: str) -> str:
    return (
        f"// Alteryx formula could not be converted automatically: {reason}.\n"
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
                    num_inputs=1,
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
    if len(node_ids) > 1:
        messages.insert(0, f"{len(node_ids)} Alteryx assignments became {len(node_ids)} chained Flowfile nodes.")
    return _row(tool, status, node_ids, "formula", messages)


def map_formula(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    assignments = _formula_assignments(tool)
    if not assignments:
        return _placeholder_row(tool, ctx, ["The Alteryx Formula tool has no expressions configured."])
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
    return _row(tool, "converted", [node_id], "dynamic_rename", messages)


def _static_rename_to_select(
    tool: AlteryxTool, ctx: EmitContext, targets: list[str], new_names: list[str], origin: str
) -> ToolReportRow:
    """Turn a rename whose new names are already known at import time into a plain select."""
    pairs = list(zip(targets, new_names, strict=False))
    select_input = [
        transform_schema.SelectInput(old_name=old, new_name=new, keep=True) for old, new in pairs if new and old != new
    ]
    if not select_input:
        return _placeholder_row(tool, ctx, ["The Alteryx Dynamic Rename resolved to no column renames."])
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
    return _row(tool, "partial", [node_id], "select", messages)


def _rename_from_right_input(
    tool: AlteryxTool, ctx: EmitContext, config: ET.Element, targets: list[str], mode: str
) -> ToolReportRow:
    source = ctx.source_tool(tool.tool_id, DYNAMIC_RENAME_SOURCE_ANCHORS)
    if source is None:
        return _placeholder_row(tool, ctx, ["The Alteryx Dynamic Rename field-name input is not connected."])
    if mode == "rightinputmetadata":
        new_names = ctx.tool_columns.get(source.tool_id)
        if not new_names:
            return _placeholder_row(
                tool,
                ctx,
                [
                    "The Alteryx Dynamic Rename takes its names from the right input's column names, "
                    f"which are not known at import time for '{tool_label(source)}' (ToolID {source.tool_id})."
                ],
            )
        return _static_rename_to_select(
            tool, ctx, targets, new_names, f"the columns of '{tool_label(source)}' (ToolID {source.tool_id})"
        )

    names_from_rows = config.find("NamesFromRows")
    input_mode = _text(names_from_rows, "InputMode") if names_from_rows is not None else ""
    if input_mode and input_mode.strip().lower() != "positional":
        return _placeholder_row(
            tool, ctx, [f"Alteryx Dynamic Rename input mode '{input_mode}' has no Flowfile equivalent."]
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
            return _placeholder_row(tool, ctx, ["The Alteryx Dynamic Rename has no prefix or suffix configured."])
        return _emit_prefix_suffix_rename(tool, ctx, prefix, suffix, selection)

    if mode in ("rightinputrows", "rightinputmetadata"):
        return _rename_from_right_input(tool, ctx, config, selected or names, mode)

    return _placeholder_row(
        tool, ctx, [f"Alteryx Dynamic Rename mode '{raw_mode or '(empty)'}' has no Flowfile equivalent."]
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
    return _row(tool, "converted", node_ids, "dynamic_rename", messages)


_CURRENT_FIELD_RE = re.compile(r"\[_CurrentField_\]", re.IGNORECASE)
_CURRENT_FIELD_NAME_RE = re.compile(r"\[_CurrentFieldName_\]", re.IGNORECASE)
_SPECIAL_FIELD_RE = re.compile(r"\[_[A-Za-z]\w*_\]")


def _substitute_current_field(expression: str, name: str) -> str:
    """Bind Alteryx's [_CurrentField_] / [_CurrentFieldName_] to one field.

    The replacements go through callables so a field name containing a backslash is inserted
    literally instead of being read as a ``re.sub`` escape.
    """
    bound = _CURRENT_FIELD_RE.sub(lambda _match: f"[{name}]", expression)
    return _CURRENT_FIELD_NAME_RE.sub(lambda _match: f'"{name}"', bound)


def map_multi_field_formula(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    expression = _text(config, "Expression")
    if not expression:
        return _placeholder_row(tool, ctx, ["The Alteryx Multi-Field Formula tool has no expression configured."])
    _, selected, _unknown = _field_selection(config)
    if not selected:
        return _placeholder_row(tool, ctx, ["The Alteryx Multi-Field Formula tool has no fields selected."])

    copy_output = _is_true(_attribute(config, "CopyOutput", "value"))
    prefix, suffix = _text(config, "OutputPrefix"), _text(config, "OutputSuffix")
    if copy_output and not prefix and not suffix:
        return _placeholder_row(
            tool,
            ctx,
            ["The Alteryx Multi-Field Formula writes to copied fields whose names are not recorded in the workflow."],
        )
    declared_type = ""
    if _is_true(_attribute(config, "ChangeFieldType", "value")):
        declared_type = _attribute(config, "OutputFieldType", "type")
    data_type = _map_alteryx_type(declared_type)

    assignments: list[_Assignment] = []
    for name in selected:
        if '"' in name:
            return _placeholder_row(
                tool, ctx, [f"The Alteryx Multi-Field Formula field {name!r} cannot be written as a field reference."]
            )
        body = _substitute_current_field(expression, name)
        if _SPECIAL_FIELD_RE.search(body):
            return _placeholder_row(
                tool,
                ctx,
                [
                    "The Alteryx Multi-Field Formula uses a special field reference with no Flowfile equivalent.",
                    f"Original expression: {_one_line(expression)}",
                ],
            )
        assignments.append(_Assignment(f"{prefix}{name}{suffix}" if copy_output else name, body, data_type))

    row = _emit_formula_chain(tool, ctx, assignments)
    row.messages.insert(
        0,
        f"The Alteryx Multi-Field Formula was expanded into one Flowfile formula per field: {', '.join(selected)}.",
    )
    if declared_type:
        row.messages.append(
            f"Alteryx stored the result as '{declared_type}'; Flowfile keeps the type the expression produces. "
            "Add a Select node if you need the Alteryx type."
        )
    return row


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
        return _placeholder_row(tool, ctx, [f"The Alteryx RegEx tool could not be converted: {reason}."])

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
    return _row(tool, "partial", [node_id], "polars_code", messages)


def _regex_added_columns(config: ET.Element, method: str, column: str) -> list[str]:
    if method in ("parsecomplex", "parsesimple"):
        return _regex_output_names(config, method, column)[0]
    if method == "match":
        return [name for name in [_text(config, "Match/Field")] if name]
    return []


def map_sort(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    sort_input = [
        transform_schema.SortByInput(
            column=element.get("field") or "",
            how="desc" if (element.get("order") or "").lower().startswith("desc") else "asc",
        )
        for element in config.findall("SortInfo/Field")
        if element.get("field")
    ]
    if not sort_input:
        return _placeholder_row(tool, ctx, ["The Alteryx Sort tool has no sort fields configured."])
    settings = input_schema.NodeSort(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), sort_input=sort_input)
    node_id = ctx.add_node(tool, "sort", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "sort", [])


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
        )
    if not agg_cols:
        return _placeholder_row(tool, ctx, ["The Alteryx Summarize tool has no aggregations configured."])

    settings = input_schema.NodeGroupBy(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        groupby_input=transform_schema.GroupByInput(agg_cols=agg_cols),
    )
    node_id = ctx.add_node(tool, "group_by", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = [agg.new_name for agg in agg_cols]
    return _row(tool, "converted", [node_id], "group_by", [])


def map_sample(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    mode = _text(config, "Mode") or "First"
    group_fields = config.find("GroupFields")
    if mode.lower() != "first":
        return _placeholder_row(
            tool, ctx, [f"Alteryx Sample mode '{mode}' has no Flowfile equivalent; only 'First N' is converted."]
        )
    if group_fields is not None and len(list(group_fields)) > 0:
        return _placeholder_row(tool, ctx, ["Grouped Alteryx sampling has no Flowfile equivalent."])
    try:
        size = int(float(_text(config, "N") or "1"))
    except ValueError:
        return _placeholder_row(tool, ctx, ["The Alteryx Sample record count could not be read."])

    settings = input_schema.NodeSample(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), sample_method="first", sample_size=size
    )
    node_id = ctx.add_node(tool, "sample", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "sample", [])


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
    if ctx.has_outgoing(tool.tool_id, "Dupes"):
        status = "partial"
        messages.append(
            "The Alteryx duplicates (D) output has no Flowfile equivalent; "
            "a passthrough placeholder now feeds that branch with the unfiltered input."
        )
        dupes_id = emit_placeholder(
            tool,
            ctx,
            ["This branch should contain the duplicate rows dropped by the Unique node."],
            num_inputs=1,
            dy=ANTI_DY,
            register_anchors=False,
        )
        ctx.register_output(tool.tool_id, "Dupes", dupes_id)
        ctx.register_all_inputs(tool.tool_id, dupes_id)
        node_ids.append(dupes_id)
    return _row(tool, status, node_ids, "unique", messages)


def map_text_to_columns(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    column = _text(config, "Field")
    split_type = _text(config, "SplitType") or "SplitToRows"
    delimiters_element = config.find("Delimeters")
    delimiter = (delimiters_element.get("value") if delimiters_element is not None else "") or ""
    root_name = _text(config, "RootName")

    if split_type.lower() != "splittorows":
        return _placeholder_row(
            tool,
            ctx,
            [f"Alteryx Text To Columns split type '{split_type}' has no Flowfile equivalent (only split to rows)."],
        )
    if not column or len(delimiter) != 1:
        return _placeholder_row(
            tool,
            ctx,
            ["Only a single-character Alteryx delimiter can be converted; this tool uses " f"'{delimiter}'."],
        )

    settings = input_schema.NodeTextToRows(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        text_to_rows_input=transform_schema.TextToRowsInput(
            column_to_split=column,
            output_column_name=root_name if root_name and root_name != column else None,
            split_by_fixed_value=True,
            split_fixed_value=delimiter,
        ),
    )
    node_id = ctx.add_node(tool, "text_to_rows", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "text_to_rows", [])


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
    if mode.lower() != "byname":
        status = "partial"
        messages.append(
            f"Alteryx union mode '{mode}' was converted to Flowfile's name-based union; verify the column order."
        )
    return _row(tool, status, [node_id], "union", messages)


def _join_settings(ctx: EmitContext, mapping: list[tuple[str, str]], how: str, swap: bool) -> input_schema.NodeJoin:
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
            left_select=transform_schema.JoinInputs(renames=[]),
            right_select=transform_schema.JoinInputs(renames=[]),
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
    by_record_position = config.find("JoinByRecordPos")
    if by_record_position is not None and _is_true(by_record_position.get("value")):
        return _placeholder_row(tool, ctx, ["Alteryx 'join by record position' has no Flowfile equivalent."])
    if not left_fields or len(left_fields) != len(right_fields):
        return _placeholder_row(tool, ctx, ["The Alteryx join keys could not be read as matching pairs."])

    mapping = list(zip(left_fields, right_fields, strict=True))
    wants_join = ctx.has_outgoing(tool.tool_id, "Join")
    wants_left = ctx.has_outgoing(tool.tool_id, "Left")
    wants_right = ctx.has_outgoing(tool.tool_id, "Right")
    if not (wants_join or wants_left or wants_right):
        wants_join = True

    node_ids: list[int] = []
    messages: list[str] = []
    if wants_join:
        inner = _join_settings(ctx, mapping, "inner", swap=False)
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

    ctx.tool_columns[tool.tool_id] = None
    status: ToolStatus = "converted"
    if config.find("SelectConfiguration") is not None:
        status = "partial"
        messages.append(
            "The Alteryx join's field selection and renames were not converted; "
            "Flowfile keeps every column from both inputs."
        )
    elif messages:
        status = "partial"
    return _row(tool, status, node_ids, "join", messages)


def _file_element_path(config: ET.Element) -> tuple[str, str]:
    """Return (path, sheet) for an Alteryx File element, splitting the ``|||sheet$`` suffix."""
    element = config.find("File")
    raw = (element.text or "").strip() if element is not None and element.text else ""
    if "|||" in raw:
        path, _, sheet = raw.partition("|||")
        return path.strip(), sheet.strip().rstrip("$")
    return raw, ""


def map_file_input(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    path, sheet = _file_element_path(config)
    if not path:
        return _placeholder_row(
            tool, ctx, ["This Alteryx Input Data tool reads from a database or connection, not a file."]
        )
    directory, filename = _split_path(path)
    file_type = _READ_FILE_TYPES.get(_extension(filename))
    if file_type is None:
        return _placeholder_row(
            tool, ctx, [f"Alteryx input file format '{_extension(filename) or filename}' is not supported."]
        )

    received = input_schema.ReceivedTable.create_from_path(path, file_type=file_type)
    received.name = filename
    received.directory = directory or None
    if _is_foreign_absolute_path(path):
        # Resolving a foreign path against this machine's cwd would invent a path that points nowhere.
        received.abs_file_path = path
    if file_type == "excel" and sheet:
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
        messages.append(f"The workflow reads from '{path}'; repoint this node at your own copy of the file.")

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
    return _row(tool, "converted", node_ids, "read", messages)


def _emit_positional_header_rename(tool: AlteryxTool, ctx: EmitContext, names: list[str]) -> int:
    select_input = [
        transform_schema.SelectInput(old_name=f"column_{index + 1}", new_name=name, keep=True)
        for index, name in enumerate(names)
    ]
    settings = input_schema.NodeSelect(
        flow_id=ctx.flow_id, node_id=ctx.new_node_id(), keep_missing=True, select_input=select_input
    )
    return ctx.add_node(tool, "select", settings, dx=FORMULA_STEP_DX, dy=FORMULA_STEP_DY)


def map_file_output(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    path, sheet = _file_element_path(config)
    if not path:
        return _placeholder_row(
            tool, ctx, ["This Alteryx Output Data tool writes to a database or connection, not a file."]
        )
    directory, filename = _split_path(path)
    file_type = _WRITE_FILE_TYPES.get(_extension(filename))
    if file_type is None:
        return _placeholder_row(
            tool, ctx, [f"Alteryx output file format '{_extension(filename) or filename}' is not supported."]
        )

    table_settings = _OUTPUT_TABLE_SETTINGS[file_type]()
    if file_type == "csv":
        delimiter = _text(config, "FormatSpecificOptions/Delimeter")
        if len(delimiter) == 1:
            table_settings.delimiter = delimiter
    if file_type == "excel" and sheet:
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
    return _row(tool, "converted", [node_id], "output", [])


def map_browse(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    settings = input_schema.NodeExploreData(flow_id=ctx.flow_id, node_id=ctx.new_node_id())
    node_id = ctx.add_node(tool, "explore_data", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = ctx.input_columns(tool.tool_id)
    return _row(tool, "converted", [node_id], "explore_data", [])


def map_record_id(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    name = _text(config, "FieldName") or "RecordID"
    field_type = _text(config, "FieldType")
    if field_type and _map_alteryx_type(field_type) not in ("Int16", "Int32", "Int64"):
        return _placeholder_row(
            tool, ctx, [f"Alteryx Record ID type '{field_type}' is not an integer; Flowfile record IDs are integers."]
        )
    try:
        offset = int(_text(config, "StartValue") or "1")
    except ValueError:
        return _placeholder_row(tool, ctx, ["The Alteryx Record ID start value could not be read."])

    settings = input_schema.NodeRecordId(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        record_id_input=transform_schema.RecordIdInput(output_column_name=name, offset=offset),
    )
    node_id = ctx.add_node(tool, "record_id", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    ctx.tool_columns[tool.tool_id] = [name, *known] if known is not None else None
    return _row(tool, "converted", [node_id], "record_id", [])


def map_transpose(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    key_fields = [element.get("field") for element in config.findall("KeyFields/Field") if element.get("field")]
    selected = [
        element.get("field")
        for element in config.findall("DataFields/Field")
        if element.get("field") and _is_true(element.get("selected"))
    ]
    if not selected:
        return _placeholder_row(tool, ctx, ["The Alteryx Transpose tool selects no data fields."])
    if "*Unknown" in selected:
        return _placeholder_row(
            tool, ctx, ["The Alteryx Transpose selects '*Unknown' data fields, so the column set is not static."]
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
    return _row(tool, "converted", [unpivot_id, rename_id], "unpivot", [])


def map_cross_tab(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    index_columns = [element.get("field") for element in config.findall("GroupFields/Field") if element.get("field")]
    pivot_column = _attribute(config, "HeaderField", "field")
    value_col = _attribute(config, "DataField", "field")
    raw_methods = ((element.get("method") or "").strip() for element in config.findall("Methods/Method"))
    methods = [method for method in raw_methods if method]
    if not pivot_column or not value_col:
        return _placeholder_row(tool, ctx, ["The Alteryx Cross Tab header or data field could not be read."])
    if not methods:
        return _placeholder_row(tool, ctx, ["The Alteryx Cross Tab tool has no aggregation methods configured."])
    unmapped = [method for method in methods if _SUMMARIZE_ACTIONS.get(method.lower()) in (None, "groupby")]
    if unmapped:
        return _placeholder_row(tool, ctx, ["Unsupported Alteryx Cross Tab methods: " + ", ".join(unmapped)])

    settings = input_schema.NodePivot(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        pivot_input=transform_schema.PivotInput(
            index_columns=index_columns,
            pivot_column=pivot_column,
            value_col=value_col,
            aggregations=[_SUMMARIZE_ACTIONS[method.lower()] for method in methods],
        ),
    )
    node_id = ctx.add_node(tool, "pivot", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = None
    return _row(
        tool,
        "partial",
        [node_id],
        "pivot",
        [
            "Alteryx replaces non-alphanumeric characters in the new Cross Tab column names with underscores; "
            "Flowfile keeps the raw values, so downstream references may need updating."
        ],
    )


def map_append_fields(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    settings = input_schema.NodeCrossJoin(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        cross_join_input=transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[]),
            right_select=transform_schema.JoinInputs(renames=[]),
        ),
    )
    node_id = ctx.add_node(tool, "cross_join", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_input(tool.tool_id, DEFAULT_INPUT_ANCHOR, node_id, MAIN)
    ctx.register_input(tool.tool_id, "Targets", node_id, MAIN)
    ctx.register_input(tool.tool_id, "Source", node_id, RIGHT)
    ctx.tool_columns[tool.tool_id] = None

    messages: list[str] = []
    status: ToolStatus = "converted"
    if _config(tool).find("SelectConfiguration") is not None:
        status = "partial"
        messages.append(
            "The Alteryx Append Fields field selection was not converted; "
            "Flowfile keeps every column from both inputs."
        )
    return _row(tool, status, [node_id], "cross_join", messages)


def map_running_total(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    group_fields = [element.get("field") for element in config.findall("GroupByFields/Field") if element.get("field")]
    total_fields = list(
        dict.fromkeys(
            element.get("field") for element in config.findall("RunningTotalFields/Field") if element.get("field")
        )
    )
    if not total_fields:
        return _placeholder_row(tool, ctx, ["The Alteryx Running Total tool has no fields to total."])

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
    return _row(tool, "converted", [node_id], "window_functions", [])


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
    name fails closed to a placeholder instead of guessing by position. The Flowfile
    node was built for parity with this tool (same frame-wide null-row/column rules,
    character classes, whitespace precedence and dtype scoping), so recognized
    options translate one-to-one.
    """
    config = _config(tool)
    values = {value.get("name", ""): (value.text or "").strip() for value in config.findall("Value")}
    expected = {*_CLEANSE_CHECKBOXES, _CLEANSE_FIELD_LIST, _CLEANSE_CASE_ENABLED, _CLEANSE_CASE_MODE}
    missing = sorted(expected - set(values))
    if missing:
        return _placeholder_row(
            tool, ctx, ["The Data Cleansing configuration is missing expected settings: " + ", ".join(missing) + "."]
        )
    unrecognized = sorted(set(values) - expected)
    if unrecognized:
        return _placeholder_row(
            tool, ctx, ["The Data Cleansing configuration has unrecognized settings: " + ", ".join(unrecognized) + "."]
        )
    fields = _parse_cleanse_fields(values[_CLEANSE_FIELD_LIST])
    if fields is None:
        return _placeholder_row(tool, ctx, ["The Data Cleansing field list could not be read."])
    if "*Unknown" in fields:
        return _placeholder_row(
            tool, ctx, ["The Data Cleansing tool cleanses dynamic or unknown fields, which Flowfile cannot express."]
        )
    case_mode = "none"
    if _is_true(values[_CLEANSE_CASE_ENABLED]):
        case_mode = _CLEANSE_CASE_MODES.get(values[_CLEANSE_CASE_MODE].lower())
        if case_mode is None:
            return _placeholder_row(
                tool, ctx, [f"The Data Cleansing case mode '{values[_CLEANSE_CASE_MODE]}' is not recognized."]
            )

    settings = input_schema.NodeDataCleansing(
        flow_id=ctx.flow_id,
        node_id=ctx.new_node_id(),
        cleansing_input=transform_schema.DataCleansingInput(
            selection_mode="list",
            selected_columns=fields,
            case_mode=case_mode,
            **{target: _is_true(values[name]) for name, target in _CLEANSE_CHECKBOXES.items()},
        ),
    )
    node_id = ctx.add_node(tool, "data_cleansing", settings, description=_description(tool))
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.register_all_inputs(tool.tool_id, node_id)
    known = ctx.input_columns(tool.tool_id)
    ctx.tool_columns[tool.tool_id] = None if settings.cleansing_input.remove_null_columns else known
    return _row(tool, "converted", [node_id], "data_cleansing", [])


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
    return _row(tool, "converted", [count_id, rename_id], "record_count", [])


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
