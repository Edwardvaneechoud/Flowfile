"""Per-tool mappers turning parsed Alteryx tools into Flowfile nodes.

Every mapper emits one or more :class:`schemas.FlowfileNode` objects through the shared
``EmitContext`` and returns the report row describing what happened. Anchors are registered
in the context so the generic wiring pass in ``convert.py`` can translate Alteryx wires into
Flowfile edges without knowing anything about individual tools.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field

from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx.expression import try_translate
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

FORMULA_STEP_DX = 150
FORMULA_STEP_DY = 110
ANTI_DX = 180
ANTI_DY = 160

_ALTERYX_TYPE_MAP: dict[str, str] = {
    "bool": "Boolean",
    "byte": "Int64",
    "int16": "Int64",
    "int32": "Int64",
    "int64": "Int64",
    "fixeddecimal": "Float64",
    "float": "Float64",
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


# ---------------------------------------------------------------------------
# Emit context
# ---------------------------------------------------------------------------


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


ToolMapper = Callable[[AlteryxTool, EmitContext], ToolReportRow]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def tool_label(tool: AlteryxTool) -> str:
    """Human-readable identity: the tool name, or the macro filename for macros."""
    return tool.tool_name or tool.plugin or "Unknown"


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


def _extension(name: str) -> str:
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def _map_alteryx_type(alteryx_type: str | None) -> str | None:
    if not alteryx_type:
        return None
    return _ALTERYX_TYPE_MAP.get(alteryx_type.strip().lower())


# ---------------------------------------------------------------------------
# Placeholder
# ---------------------------------------------------------------------------


def _placeholder_code(tool: AlteryxTool, num_inputs: int, notes: list[str]) -> str:
    lines = [
        f"# Alteryx tool '{tool_label(tool)}' (ToolID {tool.tool_id}) could not be converted automatically.",
        "# This node passes its input through unchanged; rebuild the logic here.",
    ]
    lines.extend(f"# {_one_line(note)}" for note in notes)
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


# ---------------------------------------------------------------------------
# Text Input
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Select
# ---------------------------------------------------------------------------


def map_select(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    keep_missing = True
    select_input: list[transform_schema.SelectInput] = []
    retyped: list[str] = []
    for element in config.findall("SelectFields/SelectField"):
        name = element.get("field") or ""
        selected = _is_true(element.get("selected"))
        if name == "*Unknown":
            keep_missing = selected
            continue
        if not name:
            continue
        if element.get("type"):
            retyped.append(name)
        select_input.append(
            transform_schema.SelectInput(
                old_name=name,
                new_name=element.get("rename") or name,
                keep=selected,
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
    if retyped:
        status = "partial"
        messages.append(
            "Data type changes were not converted; set them on a downstream Formula or Select node: "
            + ", ".join(retyped)
        )
    return _row(tool, status, [node_id], "select", messages)


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


def map_filter(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    expression = _text(config, "Expression")
    outcome = try_translate(expression)
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


# ---------------------------------------------------------------------------
# Formula
# ---------------------------------------------------------------------------


def _commented_formula_body(expression: str, reason: str, stub: str) -> str:
    return (
        f"// Alteryx formula could not be converted automatically: {reason}.\n"
        f"// Original: {_one_line(expression)}\n"
        f"{stub}"
    )


def map_formula(tool: AlteryxTool, ctx: EmitContext) -> ToolReportRow:
    config = _config(tool)
    formula_fields = config.findall("FormulaFields/FormulaField")
    if not formula_fields:
        return _placeholder_row(tool, ctx, ["The Alteryx Formula tool has no expressions configured."])

    known = ctx.input_columns(tool.tool_id)
    node_ids: list[int] = []
    messages: list[str] = []
    commented = False
    placeholder = False
    previous_id: int | None = None

    for index, element in enumerate(formula_fields):
        expression = element.get("expression") or ""
        target = element.get("field") or f"formula_{index + 1}"
        data_type = _map_alteryx_type(element.get("type"))
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
                (data_type or transform_schema.AUTO_DATA_TYPE) if is_new_column else (transform_schema.AUTO_DATA_TYPE)
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


def _link(ctx: EmitContext, from_id: int, to_id: int, handle: str = PASS_HANDLE) -> None:
    """Connect two emitted nodes directly (used for 1:N expansions)."""
    nodes = {node.id: node for node in ctx.nodes}
    source, target = nodes[from_id], nodes[to_id]
    source.outputs.append(to_id)
    source.output_handles.append(handle)
    target.input_ids.append(from_id)


# ---------------------------------------------------------------------------
# Sort / Summarize / Sample / Unique / TextToColumns / Union
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# File input / output / browse
# ---------------------------------------------------------------------------


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
    if file_type == "excel" and sheet:
        received.table_settings.sheet_name = sheet
    if file_type in ("csv", "json"):
        delimiter = _text(config, "FormatSpecificOptions/Delimeter")
        if len(delimiter) == 1:
            received.table_settings.delimiter = delimiter
        header_element = config.find("FormatSpecificOptions/HeaderRow")
        if header_element is not None:
            received.table_settings.has_headers = _is_true(header_element.get("value"))

    settings = input_schema.NodeRead(flow_id=ctx.flow_id, node_id=ctx.new_node_id(), received_file=received)
    node_id = ctx.add_node(tool, "read", settings, description=_description(tool), is_start_node=True)
    ctx.register_all_outputs(tool.tool_id, node_id)
    ctx.tool_columns[tool.tool_id] = None
    return _row(tool, "converted", [node_id], "read", [])


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
    "DbFileInput": map_file_input,
    "DbFileOutput": map_file_output,
    "BrowseV2": map_browse,
    "Browse": map_browse,
}


def get_mapper(tool: AlteryxTool) -> ToolMapper:
    """The mapper for a tool, falling back to the placeholder mapper."""
    return TOOL_MAPPERS.get(tool.tool_name, map_unsupported)
