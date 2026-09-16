"""Orchestrates the Alteryx ``.yxmd`` -> Flowfile conversion.

Parses the workflow, maps every tool in document order, wires the emitted nodes from the
Alteryx connection list and assembles a validated :class:`schemas.FlowfileData` plus the
report describing what a user still has to finish by hand.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TextIO

import yaml

from flowfile_core.flowfile.converters.alteryx.mappers import (
    DEFAULT_INPUT_ANCHOR,
    DEFAULT_OUTPUT_ANCHOR,
    MAIN,
    POS_SCALE,
    RIGHT,
    EmitContext,
    accepts_another_input,
    comment_bounds,
    comment_text,
    full_input_message,
    get_mapper,
    main_capacity,
    multi_stream_read_message,
    rewrite_placeholder_bodies,
    tool_label,
    uncarried_wire_message,
    unread_input_message,
)
from flowfile_core.flowfile.converters.alteryx.report import (
    ConversionReport,
    ConversionResult,
    ToolReportRow,
    build_coverage,
)
from flowfile_core.flowfile.converters.alteryx.scope import census_tool_name
from flowfile_core.flowfile.converters.alteryx.tool_identity import tool_key
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import AlteryxConnection, AlteryxWorkflow, parse_yxmd
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas import schemas
from shared._version import get_version

X_OFFSET = 60
Y_OFFSET = 100
SYNTHETIC_X_STEP = 300
SYNTHETIC_Y_STEP = 200

__all__ = ["build_report", "convert_yxmd", "dump_flow_yaml", "emit_tools"]


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
    """Canvas positions for tools and text boxes, sharing one origin so comments stay beside their tools."""
    everything = [*workflow.tools, *workflow.text_boxes]
    positioned = [tool for tool in everything if tool.x is not None and tool.y is not None]
    if not positioned:
        return _synthetic_positions(workflow)
    min_x = min(tool.x for tool in positioned)
    min_y = min(tool.y for tool in positioned)
    positions: dict[int, tuple[int, int]] = {}
    unpositioned = []
    for tool in everything:
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


_CONNECTION_ORDINAL_RE = re.compile(r"^#(\d+)$")


def _connection_ordinal(connection: AlteryxConnection) -> int | None:
    match = _CONNECTION_ORDINAL_RE.match(connection.name)
    return int(match.group(1)) if match else None


def _order_connections(connections: list[AlteryxConnection]) -> list[AlteryxConnection]:
    """Reorder the wires into each anchor the way Alteryx numbered them.

    Only wires sharing one destination anchor are permuted, and only when every one of them
    carries a distinct ``#N``; everything else keeps document order, which is all the file says.
    """
    groups: dict[tuple[int, str], list[int]] = {}
    for index, connection in enumerate(connections):
        groups.setdefault((connection.dest_tool_id, connection.dest_anchor), []).append(index)

    ordered = list(connections)
    for positions in groups.values():
        if len(positions) < 2:
            continue
        ordinals = [_connection_ordinal(connections[position]) for position in positions]
        if None in ordinals or len(set(ordinals)) != len(ordinals):
            continue
        by_ordinal = [position for _, position in sorted(zip(ordinals, positions, strict=True))]
        for slot, position in zip(positions, by_ordinal, strict=True):
            ordered[slot] = connections[position]
    return ordered


def _build_context(workflow: AlteryxWorkflow) -> EmitContext:
    ctx = EmitContext(
        positions=_compute_positions(workflow),
        tools={tool.tool_id: tool for tool in workflow.tools},
    )
    known_tool_ids = set(ctx.tools)
    for connection in workflow.connections:
        if connection.origin_tool_id in known_tool_ids:
            ctx.outbound.setdefault(connection.origin_tool_id, []).append(connection)
        if connection.dest_tool_id in known_tool_ids:
            ctx.inbound.setdefault(connection.dest_tool_id, []).append(connection)
    return ctx


def _report_dropped_connection(row: ToolReportRow | None, message: str) -> None:
    """A tool that lost a wire is not fully converted, whichever end of the wire it sat on."""
    if row is None:
        return
    if message not in row.messages:
        row.messages.append(message)
    if row.status == "converted":
        row.status, row.reason = "partial", "dropped_connection"


def _report_both_ends(rows: dict[int, ToolReportRow], connection: AlteryxConnection, message: str) -> None:
    for tool_id in (connection.origin_tool_id, connection.dest_tool_id):
        _report_dropped_connection(rows.get(tool_id), message)


def _resolved_origin(ctx: EmitContext, connection: AlteryxConnection) -> tuple[int, str] | None:
    """The Flowfile (node, output handle) a wire leaves from, or ``None`` when it leaves from nowhere.

    An anchor a mapper did not register falls back to the tool's default output — but not one it
    declared empty. A Field Summary's rendered-report anchors carry data in Alteryx that Flowfile
    cannot produce, and the fallback would hand their consumers the profile table instead: a wire
    that looks connected and is answering a different question.
    """
    source = ctx.resolve_output(connection.origin_tool_id, connection.origin_anchor)
    if source is None:
        return None
    origin = ctx.output_map.get(source)
    if origin is None and source not in ctx.inactive_outputs:
        origin = ctx.output_map.get((source[0], DEFAULT_OUTPUT_ANCHOR))
    return origin


def _main_anchor_targets(ctx: EmitContext, workflow: AlteryxWorkflow) -> dict[int, set[tuple[int, str]]]:
    """Which Alteryx anchors really put a wire on each node's main slot.

    The share :func:`accepts_another_input` gives an anchor is computed from this, so an anchor
    that lays no edge must not appear: ``register_all_inputs`` registers the default ``Input`` on
    every node whether or not anything arrives there, and counting those would shrink the share of
    the anchors that are actually wired. Wires a no-op carries onward and anchors a mapper resolved
    at import time are left out for the same reason — neither becomes an edge.

    So is an anchor whose every wire leaves from nowhere: a Detour's dead side, an output the tool
    declared empty, an ``<Origin>`` naming a ToolID that is not in ``<Nodes>``. Those wires reach
    :func:`_wire` and are reported dropped, so reserving a port for the anchor they arrive on holds
    it for a stream that never comes — and refuses the last port to the anchor that could use it.
    The resolution is the same one :func:`_wire` performs, which is what keeps the two in step.
    """
    anchors: dict[int, set[tuple[int, str]]] = {}
    for connection in workflow.connections:
        if ctx.carries(connection):
            continue
        key = (connection.dest_tool_id, connection.dest_anchor)
        if key in ctx.suppressed_inputs:
            continue
        if _resolved_origin(ctx, connection) is None:
            continue
        targets = ctx.input_map.get(key) or ctx.input_map.get((connection.dest_tool_id, DEFAULT_INPUT_ANCHOR))
        for node_id, kind in targets or []:
            if kind != RIGHT:
                anchors.setdefault(node_id, set()).add(key)
    return anchors


def _starving_anchors(
    main_anchors: dict[int, set[tuple[int, str]]],
    anchor_wired: dict[tuple[int, str, int, str], int],
    node_id: int,
    anchor_key: tuple[int, str],
) -> int:
    """How many *other* Alteryx anchors on this node's main slot still hold no port at all."""
    return sum(
        1
        for key in main_anchors.get(node_id, ())
        if key != anchor_key and not anchor_wired.get((node_id, MAIN, *key), 0)
    )


def _wire(ctx: EmitContext, workflow: AlteryxWorkflow, rows: dict[int, ToolReportRow]) -> None:
    """Translate Alteryx wires into Flowfile edges via the anchor registries.

    Two Alteryx shapes have no Flowfile edge to become, and both are reported rather than merged:
    a wire onto an anchor a mapper already read at import time, and a wire onto a node whose input
    ports are full. Alteryx unions everything arriving on one anchor; emitting that union here
    would change the data, while a message on both rows only changes what the user is told.

    An edge is identified by its output handle as well as its ends, so a Filter's True and False
    anchors into one target stay two streams rather than one wire seen twice. The targets of a
    single wire are deduplicated too, because ``register_all_inputs`` names one node once per
    anchor. A refusal covers the whole connection: one Alteryx tool can stand on several Flowfile
    nodes — a Unique and its Dupes branch — and they have to agree about what reached them.
    Fullness is asked of the slot the wire resolves to, not of the node: a second wire onto a
    Join's right-hand anchor would overwrite the first silently, and two wires onto its left anchor
    would fill both ports and leave the real right-hand wire reported as the dropped one. The main
    slot is asked per Alteryx anchor for the same reason — several anchors can register ``main`` on
    one node, and one budget for all of them hands every port to whichever anchor document order
    reached first.
    """
    nodes = {node.id: node for node in ctx.nodes}
    # nodes a mapper gave a right-hand slot, so their main slot is one port smaller than the template
    right_ports = {node_id for pairs in ctx.input_map.values() for node_id, kind in pairs if kind == RIGHT}
    main_anchors = _main_anchor_targets(ctx, workflow)
    # (flowfile node id, slot) -> the (Alteryx tool, anchor) pairs already in that slot, to name them
    wired_from: dict[tuple[int, str], list[tuple[int, str]]] = {}
    # (flowfile node id, slot, dest tool, dest anchor) -> ports that anchor already holds in that slot
    anchor_wired: dict[tuple[int, str, int, str], int] = {}
    seen: set[tuple[int, str, int, str]] = set()
    for connection in workflow.connections:
        if ctx.carries(connection):
            continue
        if (connection.dest_tool_id, connection.dest_anchor) in ctx.suppressed_inputs:
            if not ctx.was_resolved(connection):
                message = unread_input_message(
                    ctx.tools[connection.dest_tool_id],
                    connection,
                    read=ctx.anchor_was_read(connection.dest_tool_id, connection.dest_anchor),
                )
                _report_both_ends(rows, connection, message)
            continue
        origin = _resolved_origin(ctx, connection)
        targets = ctx.input_map.get((connection.dest_tool_id, connection.dest_anchor)) or ctx.input_map.get(
            (connection.dest_tool_id, DEFAULT_INPUT_ANCHOR)
        )
        if origin is None or not targets:
            message = (
                f"A connection from ToolID {connection.origin_tool_id} ({connection.origin_anchor}) to "
                f"ToolID {connection.dest_tool_id} ({connection.dest_anchor}) was dropped; reconnect it by hand."
            )
            destination = rows.get(connection.dest_tool_id)
            if origin is None:
                # An anchor a tool deliberately leaves empty (a Detour's unused side) says so itself.
                message = ctx.inactive_outputs.get((connection.origin_tool_id, connection.origin_anchor), message)
            elif destination is not None and destination.status == "no_op":
                # There is no node to reconnect to; say which wire the tool did not hand on instead.
                carried = any(ctx.carries(wire) for wire in ctx.inbound.get(connection.dest_tool_id, []))
                message = uncarried_wire_message(ctx.tools[connection.dest_tool_id], connection, carried=carried)
            _report_both_ends(rows, connection, message)
            continue
        source_id, handle = origin
        # Distinct against the edges already laid and against each other.
        fresh: list[tuple[int, str]] = []
        for pair in targets:
            if (source_id, handle, *pair) not in seen and pair not in fresh:
                fresh.append(pair)
        anchor_key = (connection.dest_tool_id, connection.dest_anchor)
        full = [
            (target_id, kind)
            for target_id, kind in fresh
            if not accepts_another_input(
                nodes[target_id],
                kind,
                right_port=target_id in right_ports,
                anchor_wired=anchor_wired.get((target_id, kind, *anchor_key), 0),
                starving_anchors=_starving_anchors(main_anchors, anchor_wired, target_id, anchor_key),
            )
        ]
        if full:
            for target_id, kind in full:
                held = wired_from.get((target_id, kind), [])
                if kind == RIGHT:
                    capacity, anchor_count = 1, 1
                else:
                    capacity = main_capacity(nodes[target_id], right_port=target_id in right_ports)
                    anchor_count = len(main_anchors.get(target_id, ())) or 1
                _report_both_ends(
                    rows,
                    connection,
                    full_input_message(
                        ctx.tools[connection.dest_tool_id],
                        connection,
                        nodes[target_id].type,
                        held,
                        slot=kind,
                        capacity=capacity,
                        anchor_count=anchor_count,
                        starving_anchors=_starving_anchors(main_anchors, anchor_wired, target_id, anchor_key),
                        # A Union upstream only answers the Alteryx shape: more streams on this one
                        # anchor than the slot holds. When another anchor holds the ports, merging
                        # them is precisely what the workflow did not ask for.
                        own_anchor_full=bool(held) and all(anchor == connection.dest_anchor for _, anchor in held),
                    ),
                )
            continue
        for target_id, kind in fresh:
            seen.add((source_id, handle, target_id, kind))
            target = nodes[target_id]
            if kind == RIGHT:
                target.right_input_id = source_id
            else:
                target.input_ids.append(source_id)
            held_key = (target_id, kind, *anchor_key)
            anchor_wired[held_key] = anchor_wired.get(held_key, 0) + 1
            wired_from.setdefault((target_id, kind), []).append((connection.origin_tool_id, connection.dest_anchor))
            nodes[source_id].outputs.append(target_id)
            nodes[source_id].output_handles.append(handle)


def _emit_comments(
    workflow: AlteryxWorkflow, ctx: EmitContext
) -> tuple[list[schemas.FlowfileComment], list[ToolReportRow]]:
    """Turn Alteryx text boxes into canvas comments; colour, font and shape are dropped.

    Alteryx also uses empty text boxes as decorative background bands, which carry no
    information without their fill colour, so those are skipped rather than imported blank.
    """
    comments: list[schemas.FlowfileComment] = []
    rows: list[ToolReportRow] = []
    for box in workflow.text_boxes:
        text = comment_text(box)
        if not text:
            rows.append(
                ToolReportRow(
                    alteryx_tool_id=box.tool_id,
                    alteryx_tool=tool_label(box),
                    census_name=census_tool_name(box),
                    entity="annotation",
                    alteryx_tool_key=tool_key(box.plugin),
                    status="skipped",
                    reason="annotation",
                    messages=["An empty Alteryx comment (a decorative box) was not imported."],
                )
            )
            continue
        x, y, width, height = comment_bounds(box, ctx)
        comments.append(
            schemas.FlowfileComment(
                id=ctx.new_comment_id(), text=text, x_position=x, y_position=y, width=width, height=height
            )
        )
        rows.append(
            ToolReportRow(
                alteryx_tool_id=box.tool_id,
                alteryx_tool=tool_label(box),
                census_name=census_tool_name(box),
                entity="annotation",
                alteryx_tool_key=tool_key(box.plugin),
                flowfile_node_type="comment",
                status="converted",
                reason="annotation",
                messages=["Imported as a canvas comment; colour, font and shape are not kept."],
            )
        )
    return comments, rows


def build_report(name: str, rows: list[ToolReportRow]) -> ConversionReport:
    """Count tools and annotations apart, so a wall of comments cannot flatter the coverage.

    Every status must have a counter of the same name on :class:`ConversionReport`, otherwise
    the ``setattr`` below raises while converting instead of losing the count silently.
    """
    tool_rows = [row for row in rows if row.entity == "tool"]
    report = ConversionReport(
        workflow_name=name,
        total_tools=len(tool_rows),
        total_annotations=len(rows) - len(tool_rows),
        coverage=build_coverage(tool_rows),
        rows=rows,
    )
    for row in tool_rows:
        setattr(report, row.status, getattr(report, row.status) + 1)
    return report


def dump_flow_yaml(flow_data: schemas.FlowfileData, handle: TextIO) -> None:
    """Write a converted flow as the YAML `flow_file_handler.import_flow` reads back."""
    yaml.dump(
        flow_data.model_dump(mode="json"),
        handle,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


def _report_multi_stream_reads(ctx: EmitContext, rows: dict[int, ToolReportRow]) -> None:
    """Tell each tool which streams made the anchor it read unanswerable.

    The status is left alone on purpose. What the several streams made unknown is what arrives at
    the tool, and the mapper has already fallen back — refusing, or freezing to the columns it can
    see — so the row's own status is whatever that fallback earned. A tool that converted correctly
    while merely passing an unknown column set downstream is not partial for a fact about its input;
    the tool that cannot work without those columns is where the refusal lands, as it already does.
    """
    for tool_id, anchors in ctx.multi_stream_reads.items():
        row = rows.get(tool_id)
        tool = ctx.tools.get(tool_id)
        if row is None or tool is None:
            continue
        for anchor, origins in anchors.items():
            message = multi_stream_read_message(tool, anchor, origins)
            if message not in row.messages:
                row.messages.append(message)


def emit_tools(workflow: AlteryxWorkflow) -> tuple[EmitContext, dict[int, ToolReportRow]]:
    """Run every tool mapper in document order and hand back the shared state they wrote.

    The context is what a downstream mapper is told about its inputs (``tool_columns``,
    ``anchor_columns``), which the finished flow no longer shows, so tests pin it here.
    """
    workflow.connections = _order_connections(workflow.connections)
    ctx = _build_context(workflow)
    rows_by_tool = {tool.tool_id: get_mapper(tool)(tool, ctx) for tool in workflow.tools}
    _report_multi_stream_reads(ctx, rows_by_tool)
    return ctx, rows_by_tool


def convert_yxmd(data: bytes, *, source_name: str) -> ConversionResult:
    """Convert Alteryx workflow bytes into a Flowfile flow plus a conversion report.

    Raises:
        YxmdParseError: when the bytes are not a usable Alteryx workflow.
    """
    workflow = parse_yxmd(data)
    flow_name = workflow.name or Path(source_name).stem or "Imported Alteryx workflow"
    ctx, rows_by_tool = emit_tools(workflow)
    rows: list[ToolReportRow] = list(rows_by_tool.values())
    comments, comment_rows = _emit_comments(workflow, ctx)
    rows.extend(comment_rows)

    _wire(ctx, workflow, rows_by_tool)
    rewrite_placeholder_bodies(ctx)

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
        comments=[*ctx.comments, *comments],
    )
    # Fail here rather than at open time if a mapper ever emits an unserializable payload.
    schemas.FlowfileData.model_validate(flow_data.model_dump(mode="json"))
    return ConversionResult(flow_data=flow_data, report=build_report(flow_name, rows))


_build_report = build_report
