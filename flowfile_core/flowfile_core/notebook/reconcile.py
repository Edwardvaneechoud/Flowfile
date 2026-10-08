"""Reconcile a relabelled clean-run payload with the live canvas into editor operations (pure, no FlowGraph).

Both sides are save-format payloads (``FlowGraph.get_flowfile_data().model_dump(mode="json")``) whose node
ids already agree (the session's were relabelled onto provenance ids). :func:`reconcile` emits the
``EditorOperation`` list ``POST /editor/apply_operations/`` runs as one transaction:

* a cell is *pinned* when it is known to the canvas (its provenance names live nodes, or it is ``node-<id>``
  of a live node) and not in ``changed_cell_ids``. A pinned cell's existing nodes keep their settings,
  description and type whatever the clean run rebuilt, and new nodes a pinned cell created (a lossy rebuild)
  are not added; a pinned node fed by such a node keeps its live inputs;
* a node of a changed or new cell is re-sent (``update_settings``, or ``update_user_defined_settings`` for a
  custom node) only when its settings differ under :func:`compare.settings_equal`, its user description
  differs, or its ``node_reference`` changes; an unchanged node never gets an op, since ``update_node``
  swaps its function and resets its cache even for an equal hash;
* new ids become ``add_node`` at the free slot nearest their inputs or outputs (``util/layout/placement.py``:
  live nodes never move and a deleted node's slot is free), then their inputs are connected, then their settings
  are sent; a type change under the same id is ``delete_node`` + ``add_node`` at the live position + every edge
  re-connected;
* edges are diffed per target handle: ``input-0`` as an ordered list (a union's or multi-input Polars
  code's order matters and ``connect`` appends, so a changed order re-adds every input), ``input-1`` /
  ``input-2`` as single slots and keyed inputs as sets, each with its source handle (``output-0..9``);
* a live node absent from the session is deleted when its cell was deleted or changed, else kept;
* ``node_reference`` comes from the captured names with a generated label counted as ``None`` and is made
  unique here (the settings route does not check it);
* parameters compare as ``FlowParameter`` models; a change is one ``set_flow_parameters`` op, first, so
  ``${name}`` references in the settings sent after it resolve.
"""

from __future__ import annotations

import heapq
from collections import Counter, defaultdict
from typing import Any

from pydantic import BaseModel, Field

from flowfile_core.flowfile.util.layout.placement import placer_from_payload
from flowfile_core.notebook.compare import parameters_equal, settings_equal
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

MAIN, RIGHT, LEFT = "input-0", "input-1", "input-2"
DEFAULT_HANDLE = "output-0"


class ReconcilePlan(BaseModel):
    """The ops of one push, the review warnings, the live nodes it deletes and whether parameters change."""

    operations: list[schemas.EditorOperation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    deletions: list[int] = Field(default_factory=list)
    parameter_changes: bool = False


Edge = tuple[int, str, int, str]


def _label(node_type: str, node_id: int) -> str:
    from flowfile_core.flowfile.code_generator.code_generator import node_label

    return node_label(node_type, node_id)


def _reference(names: dict[int, str] | None, node: dict) -> str | None:
    """A node's reference with a generated label (``filtered_12``) counted as ``None``."""
    ref = (names or {}).get(node["id"], node.get("node_reference"))
    if not ref or ref == _label(node["type"], node["id"]):
        return None
    return ref


def _is_user_defined(node: dict) -> bool:
    settings = node.get("setting_input")
    return isinstance(settings, dict) and bool(settings.get("is_user_defined"))


def _source_handles(nodes: dict[int, dict]) -> dict[tuple[int, int], list[str]]:
    """``(source, target) -> [handles]`` in the source's ``outputs`` order."""
    handles: dict[tuple[int, int], list[str]] = defaultdict(list)
    for node in nodes.values():
        outputs = node.get("outputs") or []
        names = node.get("output_handles") or []
        for index, target in enumerate(outputs):
            handles[(node["id"], target)].append(names[index] if index < len(names) else DEFAULT_HANDLE)
    return handles


def incoming_edges(node: dict, nodes: dict[int, dict]) -> dict[str, list[tuple[int, str]]]:
    """``target handle -> [(source, source handle)]`` for one node: main in order, right, left, keyed.

    Static source handles come from the source's ``outputs``/``output_handles`` (consumed in order, so a
    source feeding two slots through two handles keeps both); keyed edges carry their own.
    """
    handles = _source_handles(nodes)
    used: Counter = Counter()

    def handle(source: int) -> str:
        options = handles.get((source, node["id"])) or [DEFAULT_HANDLE]
        index = used[source]
        used[source] += 1
        return options[index] if index < len(options) else options[0]

    edges: dict[str, list[tuple[int, str]]] = {}
    main = [(source, handle(source)) for source in node.get("input_ids") or []]
    if main:
        edges[MAIN] = main
    for slot, field in ((LEFT, "left_input_id"), (RIGHT, "right_input_id")):
        if node.get(field) is not None:
            edges[slot] = [(node[field], handle(node[field]))]
    for conn in node.get("input_connections") or []:
        edges.setdefault(conn["input_handle"], []).append(
            (conn["from_id"], conn.get("source_handle") or DEFAULT_HANDLE)
        )
    return edges


def _topological(nodes: dict[int, dict]) -> list[int]:
    """Kahn order over the payload's edges with a min-heap on id; ids left in a cycle follow in id order."""
    parents = {
        nid: {s for edges in incoming_edges(n, nodes).values() for s, _ in edges if s in nodes}
        for nid, n in nodes.items()
    }
    children: dict[int, set[int]] = defaultdict(set)
    for nid, sources in parents.items():
        for source in sources:
            children[source].add(nid)
    degree = {nid: len(sources) for nid, sources in parents.items()}
    heap = [nid for nid, d in degree.items() if d == 0]
    heapq.heapify(heap)
    order: list[int] = []
    while heap:
        nid = heapq.heappop(heap)
        order.append(nid)
        for child in sorted(children[nid]):
            degree[child] -= 1
            if degree[child] == 0:
                heapq.heappush(heap, child)
    return order + sorted(set(nodes) - set(order))


def user_description(node: dict) -> str:
    """The description a user typed, ``""`` when the node shows its auto-generated one.

    JSON dumps drop ``description_is_auto_generated``, so a description equal to the settings' default
    description counts as auto (the same guess a file load makes).
    """
    text = node.get("description") or ""
    if node.get("description_is_auto_generated") is not None:
        return "" if node["description_is_auto_generated"] else text
    if not text or not isinstance(node.get("setting_input"), dict):
        return text
    try:
        model = get_settings_class_for_node_type(node["type"], node["setting_input"])
        data = {**node["setting_input"], "flow_id": 0, "node_id": node["id"], "description": ""}
        if model is not input_schema.UserDefinedNode:
            data.pop("is_user_defined", None)
        default = model.model_validate(data).get_default_description()
    except Exception:
        return text
    return "" if text == default else text


def settings_body(node: dict, flow_id: int, *, description: str, node_reference: str | None) -> dict[str, Any]:
    """The ``update_settings`` body for a payload node: its ``setting_input`` plus the fields a dump excludes.

    Mirrors the file loader: ``depending_on_id(s)`` from the node's inputs, ``is_user_defined`` kept for
    custom nodes and an output node's ``table_settings`` filled. Position and group are server-owned.
    """
    body = dict(node["setting_input"])
    body.update(
        flow_id=flow_id,
        node_id=node["id"],
        pos_x=float(node.get("x_position") or 0),
        pos_y=float(node.get("y_position") or 0),
        description=description,
        node_reference=node_reference,
        is_setup=True,
    )
    inputs = list(node.get("input_ids") or [])
    extra = [node[f] for f in ("left_input_id", "right_input_id") if node.get(f)]
    model = get_settings_class_for_node_type(node["type"], node["setting_input"])
    if _is_user_defined(node) or model is input_schema.UserDefinedNode:
        body["is_user_defined"] = True
        body["depending_on_ids"] = inputs + extra
    elif model is not None:
        if "depending_on_id" in model.model_fields:
            body["depending_on_id"] = inputs[0] if inputs else -1
        if "depending_on_ids" in model.model_fields:
            body["depending_on_ids"] = inputs + extra
        output_settings = body.get("output_settings")
        if node["type"] == "output" and isinstance(output_settings, dict) and output_settings.get("file_type"):
            output_settings.setdefault("table_settings", {"file_type": output_settings["file_type"]})
    return body


def _connection(edge: Edge) -> input_schema.NodeConnection:
    source, source_handle, target, target_handle = edge
    return input_schema.NodeConnection.create_from_simple_input(source, target, target_handle, source_handle)


def _diff_main(live: list[tuple[int, str]], session: list[tuple[int, str]]) -> tuple[list, list]:
    """Main inputs as ordered lists: ``(remove, add)``; a changed order removes and re-adds every input."""
    if live == session:
        return [], []
    remaining, removed = Counter(session), []
    kept = []
    for entry in live:
        if remaining[entry] > 0:
            remaining[entry] -= 1
            kept.append(entry)
        else:
            removed.append(entry)
    added = list(session)
    for entry in kept:
        added.remove(entry)
    if kept + added == session:
        return removed, added
    return list(live), list(session)


def reconcile(
    live: dict,
    session: dict,
    changed_cell_ids: list[str] | set[str],
    node_ids_by_cell: dict[str, list[int]],
    live_names: dict[int, str] | None = None,
    session_names: dict[int, str] | None = None,
    live_parameters: list[Any] | None = None,
    session_parameters: list[Any] | None = None,
    *,
    live_cells: dict[int, str] | None = None,
) -> ReconcilePlan:
    """The ops that turn the ``live`` payload into the ``session`` one under the pinning rules above.

    ``node_ids_by_cell`` is the clean run's ``{cell_id: [node ids]}`` (every cell of the push is a key);
    ``live_cells`` maps a live node to the cell whose provenance lists it (default ``node-<id>``). Parameters
    default to each payload's ``flowfile_settings.parameters``.
    """
    flow_id = live.get("flowfile_id", 0)
    live_nodes = {n["id"]: n for n in live.get("nodes") or []}
    session_nodes = {n["id"]: n for n in session.get("nodes") or []}
    changed = set(changed_cell_ids)
    present_cells = set(node_ids_by_cell)
    live_cells = {int(k): v for k, v in (live_cells or {}).items()}
    live_cell = {nid: live_cells.get(nid, f"node-{nid}") for nid in live_nodes}
    known_cells = set(live_cell.values())
    session_cell = {nid: cell for cell, ids in node_ids_by_cell.items() for nid in ids}
    plan = ReconcilePlan()

    def pinned(node_id: int) -> bool:
        cell = session_cell.get(node_id) or live_cell.get(node_id)
        return cell is not None and cell in known_cells and cell not in changed

    skipped = {nid for nid in session_nodes if nid not in live_nodes and pinned(nid)}
    type_changed = {
        nid
        for nid, node in session_nodes.items()
        if nid in live_nodes and live_nodes[nid]["type"] != node["type"] and not pinned(nid)
    }
    deleted = sorted(
        nid
        for nid in live_nodes
        if nid not in session_nodes and (live_cell[nid] not in present_cells or live_cell[nid] in changed)
    )
    kept_absent = {nid for nid in live_nodes if nid not in session_nodes and nid not in deleted}
    added = {nid for nid in session_nodes if nid not in live_nodes and nid not in skipped}
    gone = set(deleted) | type_changed
    final = (set(session_nodes) - skipped) | kept_absent

    live_params = live_parameters if live_parameters is not None else live["flowfile_settings"]["parameters"]
    session_params = (
        session_parameters if session_parameters is not None else session["flowfile_settings"]["parameters"]
    )
    if not parameters_equal(live_params, session_params):
        plan.parameter_changes = True
        plan.operations.append(schemas.SetFlowParametersOperation(op="set_flow_parameters", parameters=session_params))
        plan.warnings.append("Changes the flow parameters; undo does not restore them.")

    references = _final_references(
        live_nodes, session_nodes, live_names, session_names, kept_absent, skipped, pinned, plan.warnings
    )

    order = _topological(session_nodes)
    removals: list[Edge] = []
    connects: dict[int, list[Edge]] = defaultdict(list)
    for target in order:
        if target in skipped:
            continue
        session_in = incoming_edges(session_nodes[target], session_nodes)
        if any(source in skipped for edges in session_in.values() for source, _ in edges):
            if pinned(target):
                continue
            plan.warnings.append(f"Node {target} reads from a node its cell did not rebuild; that input is dropped.")
        live_in = {} if target in added or target in type_changed else incoming_edges(live_nodes[target], live_nodes)
        for handle in sorted(set(session_in) | set(live_in)):
            old = [(s, h) for s, h in live_in.get(handle, []) if s not in gone and s not in kept_absent]
            new = [(s, h) for s, h in session_in.get(handle, []) if s not in skipped]
            if handle == MAIN:
                remove, add = _diff_main(old, new)
            else:
                remove = [e for e in old if e not in new]
                add = [e for e in new if e not in old]
            removals.extend((s, h, target, handle) for s, h in remove)
            connects[target].extend((s, h, target, handle) for s, h in add)

    ops = plan.operations
    ops.extend(schemas.DeleteConnectionOperation(op="delete_connection", connection=_connection(e)) for e in removals)
    for nid in deleted:
        ops.append(schemas.DeleteNodeOperation(op="delete_node", node_id=nid))
        plan.deletions.append(nid)
        plan.warnings.append(f"Deletes node {nid} ({live_nodes[nid]['type']}).")
    for nid in sorted(type_changed):
        ops.append(schemas.DeleteNodeOperation(op="delete_node", node_id=nid))
        plan.warnings.append(
            f"Node {nid} changes type from {live_nodes[nid]['type']} to {session_nodes[nid]['type']}; "
            "its edges are re-connected."
        )

    placer = placer_from_payload(live)
    for nid in deleted:
        placer.release(nid)
    targets: dict[int, list[int]] = defaultdict(list)
    for target, edges in connects.items():
        for source, *_ in edges:
            targets[source].append(target)
    new_ids = [nid for nid in order if nid in added]
    placed = placer.place_all(new_ids, {nid: [s for s, *_ in connects[nid]] for nid in new_ids}, targets)
    for nid in order:
        if nid in skipped:
            continue
        node = session_nodes[nid]
        if nid in added or nid in type_changed:
            pos = placer.position(nid) if nid in type_changed else placed[nid]
            ops.append(
                schemas.AddNodeOperation(op="add_node", node_id=nid, node_type=node["type"], pos_x=pos[0], pos_y=pos[1])
            )
        ops.extend(schemas.ConnectOperation(op="connect", connection=_connection(e)) for e in connects[nid])
        is_new = nid in added or nid in type_changed
        op = _settings_op(nid, live_nodes.get(nid), node, flow_id, references, live_names, pinned(nid), is_new)
        if op is not None:
            ops.append(op)
    for nid in sorted(kept_absent):
        ref = references.get(nid)
        node = live_nodes[nid]
        if ref != _reference(live_names, node) and isinstance(node.get("setting_input"), dict):
            ops.append(
                _op_for(node, settings_body(node, flow_id, description=user_description(node), node_reference=ref))
            )
    plan.warnings.extend(_kernel_key_warnings(live_nodes, session_nodes, live_names, references, final))
    return plan


def _final_references(
    live_nodes, session_nodes, live_names, session_names, kept_absent, skipped, pinned, warnings
) -> dict[int, str | None]:
    """Every surviving node's reference, unique: a changed cell's name beats a pinned one, which beats a kept one."""
    candidates: list[tuple[int, int, str]] = []
    for nid, node in session_nodes.items():
        if nid in skipped:
            continue
        ref = _reference(session_names, node)
        if ref:
            candidates.append((1 if pinned(nid) else 0, nid, ref))
    for nid in kept_absent:
        ref = _reference(live_names, live_nodes[nid])
        if ref:
            candidates.append((2, nid, ref))
    references: dict[int, str | None] = {nid: None for nid in session_nodes if nid not in skipped}
    references.update({nid: None for nid in kept_absent})
    taken: dict[str, int] = {}
    for _, nid, ref in sorted(candidates):
        if ref in taken:
            warnings.append(f"Name {ref!r} is already node {taken[ref]}'s; node {nid} loses it.")
            continue
        taken[ref] = nid
        references[nid] = ref
    return references


def _op_for(node: dict, body: dict) -> schemas.EditorOperation:
    if _is_user_defined(node):
        return schemas.UpdateUserDefinedSettingsOperation(
            op="update_user_defined_settings", node_type=node["type"], settings=body
        )
    return schemas.UpdateSettingsOperation(op="update_settings", node_type=node["type"], settings=body)


def _settings_op(
    nid, live_node, node, flow_id, references, live_names, is_pinned, is_new
) -> schemas.EditorOperation | None:
    """The settings op for one surviving session node, or ``None`` when nothing about it changes."""
    ref = references.get(nid)
    if not isinstance(node.get("setting_input"), dict):
        return None
    if is_new:
        return _op_for(node, settings_body(node, flow_id, description=user_description(node), node_reference=ref))
    live_ref = _reference(live_names, live_node)
    if is_pinned:
        if ref == live_ref or not isinstance(live_node.get("setting_input"), dict):
            return None
        body = settings_body(live_node, flow_id, description=user_description(live_node), node_reference=ref)
        return _op_for(live_node, body)
    same_settings = settings_equal(live_node.get("setting_input"), node["setting_input"], node["type"])
    description = user_description(node)
    same_meta = description == user_description(live_node) and ref == live_ref
    if same_settings and same_meta:
        return None
    source = node if not same_settings or not isinstance(live_node.get("setting_input"), dict) else live_node
    return _op_for(source, settings_body(source, flow_id, description=description, node_reference=ref))


def _kernel_key_warnings(live_nodes, session_nodes, live_names, references, final) -> list[str]:
    """A Python Script reads an unnamed input as ``df_<id>`` and a named one by its reference; warn when a key moves."""
    warnings = []
    for nid, node in session_nodes.items():
        if node["type"] != "python_script" or nid not in live_nodes or nid not in final:
            continue

        def keys(payload_nodes, refs, nid=nid):
            edges = incoming_edges(payload_nodes[nid], payload_nodes)
            return {refs(s) or f"df_{s}" for handle in edges.values() for s, _ in handle}

        before = keys(live_nodes, lambda s: _reference(live_names, live_nodes[s]) if s in live_nodes else None)
        after = keys(session_nodes, lambda s: references.get(s))
        if before != after:
            warnings.append(
                f"Python Script node {nid} reads its inputs as {sorted(after)} instead of {sorted(before)}; "
                "update its read_input calls."
            )
    return warnings
