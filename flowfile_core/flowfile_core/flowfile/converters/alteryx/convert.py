"""Orchestrates the Alteryx ``.yxmd`` -> Flowfile conversion.

Parses the workflow, maps every tool in document order, wires the emitted nodes from the
Alteryx connection list and assembles a validated :class:`schemas.FlowfileData` plus the
report describing what a user still has to finish by hand.
"""

from __future__ import annotations

from pathlib import Path

from flowfile_core.flowfile.converters.alteryx.mappers import (
    DEFAULT_INPUT_ANCHOR,
    DEFAULT_OUTPUT_ANCHOR,
    RIGHT,
    EmitContext,
    get_mapper,
    tool_label,
)
from flowfile_core.flowfile.converters.alteryx.report import ConversionReport, ConversionResult, ToolReportRow
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import AlteryxWorkflow, parse_yxmd
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas import schemas
from shared._version import get_version

POS_SCALE = 3.0
X_OFFSET = 60
Y_OFFSET = 100
SYNTHETIC_X_STEP = 300
SYNTHETIC_Y_STEP = 200

__all__ = ["convert_yxmd"]


def _synthetic_positions(workflow: AlteryxWorkflow) -> dict[int, tuple[int, int]]:
    """Layered left-to-right layout for workflows whose tools carry no canvas coordinates."""
    depth = {tool.tool_id: 0 for tool in workflow.tools}
    incoming: dict[int, list[int]] = {tool.tool_id: [] for tool in workflow.tools}
    for connection in workflow.connections:
        if connection.dest_tool_id in incoming and connection.origin_tool_id in depth:
            incoming[connection.dest_tool_id].append(connection.origin_tool_id)
    for _ in range(len(workflow.tools)):
        changed = False
        for tool in workflow.tools:
            for source_id in incoming[tool.tool_id]:
                if depth[source_id] + 1 > depth[tool.tool_id]:
                    depth[tool.tool_id] = depth[source_id] + 1
                    changed = True
        if not changed:
            break
    rows: dict[int, int] = {}
    positions: dict[int, tuple[int, int]] = {}
    for tool in workflow.tools:
        level = depth[tool.tool_id]
        row = rows.get(level, 0)
        rows[level] = row + 1
        positions[tool.tool_id] = (X_OFFSET + level * SYNTHETIC_X_STEP, Y_OFFSET + row * SYNTHETIC_Y_STEP)
    return positions


def _compute_positions(workflow: AlteryxWorkflow) -> dict[int, tuple[int, int]]:
    positioned = [tool for tool in workflow.tools if tool.x is not None and tool.y is not None]
    if not positioned:
        return _synthetic_positions(workflow)
    min_x = min(tool.x for tool in positioned)
    min_y = min(tool.y for tool in positioned)
    positions: dict[int, tuple[int, int]] = {}
    unpositioned = []
    for tool in workflow.tools:
        if tool.x is None or tool.y is None:
            unpositioned.append(tool)
            continue
        positions[tool.tool_id] = (
            round((tool.x - min_x) * POS_SCALE) + X_OFFSET,
            round((tool.y - min_y) * POS_SCALE) + Y_OFFSET,
        )
    base_y = max(y for _, y in positions.values()) + SYNTHETIC_Y_STEP
    for index, tool in enumerate(unpositioned):
        positions[tool.tool_id] = (X_OFFSET + index * SYNTHETIC_X_STEP, base_y)
    return positions


def _build_context(workflow: AlteryxWorkflow) -> EmitContext:
    ctx = EmitContext(positions=_compute_positions(workflow))
    known_tool_ids = {tool.tool_id for tool in workflow.tools}
    for connection in workflow.connections:
        if connection.origin_tool_id in known_tool_ids:
            ctx.outbound.setdefault(connection.origin_tool_id, []).append(connection)
        if connection.dest_tool_id in known_tool_ids:
            ctx.inbound.setdefault(connection.dest_tool_id, []).append(connection)
    return ctx


def _wire(ctx: EmitContext, workflow: AlteryxWorkflow, rows: dict[int, ToolReportRow]) -> None:
    """Translate Alteryx wires into Flowfile edges via the anchor registries."""
    nodes = {node.id: node for node in ctx.nodes}
    seen: set[tuple[int, int, str]] = set()
    for connection in workflow.connections:
        origin = ctx.output_map.get((connection.origin_tool_id, connection.origin_anchor)) or ctx.output_map.get(
            (connection.origin_tool_id, DEFAULT_OUTPUT_ANCHOR)
        )
        targets = ctx.input_map.get((connection.dest_tool_id, connection.dest_anchor)) or ctx.input_map.get(
            (connection.dest_tool_id, DEFAULT_INPUT_ANCHOR)
        )
        if origin is None or not targets:
            missing_tool_id = connection.origin_tool_id if origin is None else connection.dest_tool_id
            row = rows.get(missing_tool_id)
            message = (
                f"A connection from ToolID {connection.origin_tool_id} ({connection.origin_anchor}) to "
                f"ToolID {connection.dest_tool_id} ({connection.dest_anchor}) was dropped; reconnect it by hand."
            )
            if row is not None and message not in row.messages:
                row.messages.append(message)
            continue
        source_id, handle = origin
        for target_id, kind in targets:
            key = (source_id, target_id, kind)
            if key in seen:
                continue
            seen.add(key)
            if kind == RIGHT:
                nodes[target_id].right_input_id = source_id
            else:
                nodes[target_id].input_ids.append(source_id)
            nodes[source_id].outputs.append(target_id)
            nodes[source_id].output_handles.append(handle)


def _build_report(workflow: AlteryxWorkflow, name: str, rows: list[ToolReportRow]) -> ConversionReport:
    report = ConversionReport(
        workflow_name=name,
        total_tools=len(workflow.tools) + len(workflow.skipped_tools),
        rows=rows,
    )
    for row in rows:
        setattr(report, row.status, getattr(report, row.status) + 1)
    return report


def convert_yxmd(data: bytes, *, source_name: str) -> ConversionResult:
    """Convert Alteryx workflow bytes into a Flowfile flow plus a conversion report.

    Raises:
        YxmdParseError: when the bytes are not a usable Alteryx workflow.
    """
    workflow = parse_yxmd(data)
    flow_name = workflow.name or Path(source_name).stem or "Imported Alteryx workflow"
    ctx = _build_context(workflow)

    rows: list[ToolReportRow] = []
    rows_by_tool: dict[int, ToolReportRow] = {}
    for tool in workflow.tools:
        row = get_mapper(tool)(tool, ctx)
        rows.append(row)
        rows_by_tool[tool.tool_id] = row
    for tool in workflow.skipped_tools:
        rows.append(
            ToolReportRow(
                alteryx_tool_id=tool.tool_id,
                alteryx_tool=tool_label(tool),
                status="skipped",
                messages=["Alteryx canvas comments are not imported."],
            )
        )

    _wire(ctx, workflow, rows_by_tool)

    flow_data = schemas.FlowfileData(
        flowfile_version=get_version(),
        flowfile_id=create_unique_id(),
        flowfile_name=flow_name,
        flowfile_settings=schemas.FlowfileSettings(
            description=f"Imported from the Alteryx workflow '{source_name}'.",
            execution_mode="Development",
            execution_location="local",
            auto_save=False,
        ),
        nodes=ctx.nodes,
    )
    # Fail here rather than at open time if a mapper ever emits an unserializable payload.
    schemas.FlowfileData.model_validate(flow_data.model_dump(mode="json"))
    return ConversionResult(flow_data=flow_data, report=_build_report(workflow, flow_name, rows))
