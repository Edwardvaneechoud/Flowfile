"""Edits on a saved flow document as a person makes them in the editor, through the canonical form so both ends of
every edge stay in agreement, as a save writes them."""

import copy

from shared.flow_merge.canonical import canonical, emit
from shared.flow_merge.edges import MAIN

from .fixtures import REPO_ROOT, load

SALES = REPO_ROOT / "docs/assets/flows/sales_dashboard.yaml"


def sales() -> dict:
    """A real save: read -> unique -> formula 3, which feeds three branches (join 6, pivot 10, formula 12)."""
    return load(SALES)


def step(document: dict, step_id: int) -> dict:
    return next(n for n in document["nodes"] if n["id"] == step_id)


def settings(document: dict, step_id: int) -> dict:
    return step(document, step_id)["setting_input"]


def add_step(document: dict, step_id: int, step_type: str, inputs: dict | None = None, **fields) -> dict:
    """A new step at ``step_id`` fed by ``inputs`` (handle -> source ids)."""
    flow, _ = canonical(document)
    flow.steps[step_id] = {
        "id": step_id,
        "type": step_type,
        "is_start_node": False,
        "description": fields.pop("description", ""),
        "node_reference": fields.pop("node_reference", None),
        "x_position": fields.pop("x_position", 2000),
        "y_position": fields.pop("y_position", 600),
        "group_id": fields.pop("group_id", None),
        "left_input_id": None,
        "right_input_id": None,
        "input_ids": None,
        "outputs": [],
        "output_handles": [],
        "input_connections": None,
        "setting_input": fields.pop("setting_input", {"cache_results": False, "step": step_id}),
    }
    flow.inputs[step_id] = {h: tuple((s, "output-0") for s in sources) for h, sources in (inputs or {}).items()}
    return emit(flow)


def remove_step(document: dict, step_id: int) -> dict:
    flow, _ = canonical(document)
    del flow.steps[step_id]
    flow.inputs.pop(step_id, None)
    for slots in flow.inputs.values():
        for handle in list(slots):
            slots[handle] = tuple(e for e in slots[handle] if e[0] != step_id)
            if not slots[handle]:
                del slots[handle]
    return emit(flow)


def set_inputs(document: dict, step_id: int, handle: str = MAIN, sources: tuple = ()) -> dict:
    flow, _ = canonical(document)
    slots = flow.inputs.setdefault(step_id, {})
    if sources:
        slots[handle] = tuple((s, "output-0") for s in sources)
    else:
        slots.pop(handle, None)
    return emit(flow)


def copied(document: dict) -> dict:
    return copy.deepcopy(document)
