"""One reading of a saved flow document, so a diff and a merge see each edge once.

A document stores each edge on both ends (:mod:`shared.flow_merge.edges`). :func:`canonical` reads it from the
target, the side a load wires, and :func:`emit` writes both ends back the way a save does: a static target's
``input_ids`` (``None`` when it has no main input), ``right_input_id`` and ``left_input_id``; a keyed target's
``input_connections``; every source's ``outputs`` and ``output_handles`` sorted by (target, handle). Everything
else in the document is kept as it was, key order included, so a document a save wrote comes back unchanged.

Where the two ends disagree the reading follows the loader (``io_flowfile.populate_graph_from_flow_information``):
an edge from a step that is not in the flow is dropped, and an edge only its source lists becomes the main input of
a target that has no input at all, else is dropped. Each such repair is reported as an :class:`Issue`.
"""

from __future__ import annotations

import copy
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field

from shared.flow_merge.edges import LEFT, MAIN, RIGHT, incoming_edges, source_handles

Edge = tuple[int, str]
EDGE_FIELDS = ("left_input_id", "right_input_id", "input_ids", "outputs", "output_handles", "input_connections")
STATIC_SLOTS = (MAIN, RIGHT, LEFT)


@dataclass(frozen=True)
class Issue:
    """Something a reading or a merge noticed: ``code`` is stable, ``message`` a sentence for a person."""

    code: str
    message: str
    ids: tuple[int, ...] = ()


@dataclass
class CanonicalFlow:
    """A flow document read once.

    ``steps`` are the document's node dicts in document order, edge fields included but not authoritative: the
    edges are ``inputs`` (target -> input handle -> edges in slot order), and ``keyed`` the targets whose edges are
    stored as ``input_connections``. ``order`` is the document's top-level keys, ``head`` the values of those that are
    not nodes, groups, comments or flow settings, ``settings`` the flow settings with ``parameters`` read out by name.
    """

    order: list[str]
    head: dict
    settings: dict | None
    parameters: dict[str, dict]
    steps: dict[int, dict]
    inputs: dict[int, dict[str, tuple[Edge, ...]]]
    keyed: set[int] = field(default_factory=set)
    groups: dict[int, dict] = field(default_factory=dict)
    comments: dict[int, dict] = field(default_factory=dict)

    def readers_of(self, step_id: int) -> list[int]:
        """The steps that read from ``step_id``, in id order."""
        return sorted(
            t for t, slots in self.inputs.items() if any(s == step_id for es in slots.values() for s, _ in es)
        )


def _is_id(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _by_id(items: list | None, what: str, issues: list[Issue]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for item in items or []:
        item_id = item.get("id") if isinstance(item, dict) else None
        if not _is_id(item_id):
            issues.append(Issue(f"bad_{what}", f"A {what} without a whole-number id is left out."))
        elif item_id in out:
            issues.append(
                Issue(f"duplicate_{what}", f"Two {what}s have id {item_id}; the second is left out.", (item_id,))
            )
        else:
            out[item_id] = item
    return out


def canonical(document: Mapping) -> tuple[CanonicalFlow, list[Issue]]:
    """Read ``document`` (a saved flow as plain data) once; the input is not changed."""
    doc = copy.deepcopy(dict(document))
    issues: list[Issue] = []
    steps = _by_id(doc.get("nodes"), "step", issues)
    keyed = {sid for sid, node in steps.items() if node.get("input_connections")}
    inputs: dict[int, dict[str, tuple[Edge, ...]]] = {}
    for sid, node in steps.items():
        slots: dict[str, tuple[Edge, ...]] = {}
        for handle, edges in incoming_edges(node, steps).items():
            kept = []
            for source, source_handle in edges:
                if source in steps:
                    kept.append((source, source_handle))
                else:
                    issues.append(
                        Issue(
                            "dangling_edge",
                            f"Step {sid} reads from step {source}, which is not in the flow.",
                            (sid, source),
                        )
                    )
            if kept:
                slots[handle] = tuple(kept)
        if slots:
            inputs[sid] = slots
    handles = source_handles(steps)
    for sid, node in steps.items():
        for target in dict.fromkeys(node.get("outputs") or []):
            if target not in steps or any(s == sid for es in inputs.get(target, {}).values() for s, _ in es):
                continue
            if target not in inputs and target not in keyed:
                inputs[target] = {MAIN: ((sid, (handles.get((sid, target)) or ["output-0"])[0]),)}
                issues.append(
                    Issue(
                        "source_only_edge",
                        f"Step {sid} lists step {target} as an output that does not list it as an input; "
                        "it is read as the main input, as a load does.",
                        (sid, target),
                    )
                )
            else:
                issues.append(
                    Issue(
                        "source_only_edge",
                        f"Step {sid} lists step {target} as an output that does not list it as an input; "
                        "the edge is dropped, as a load does.",
                        (sid, target),
                    )
                )
    settings = doc.get("flowfile_settings")
    parameters: dict[str, dict] = {}
    if isinstance(settings, dict):
        for parameter in settings.get("parameters") or []:
            name = parameter.get("name") if isinstance(parameter, dict) else None
            if name is not None and name not in parameters:
                parameters[name] = parameter
    flow = CanonicalFlow(
        order=list(doc),
        head={k: v for k, v in doc.items() if k not in ("nodes", "groups", "comments", "flowfile_settings")},
        settings=settings if isinstance(settings, dict) else None,
        parameters=parameters,
        steps=steps,
        inputs=inputs,
        keyed=keyed,
        groups=_by_id(doc.get("groups"), "group", issues),
        comments=_by_id(doc.get("comments"), "comment", issues),
    )
    return flow, issues


def _outputs(flow: CanonicalFlow) -> dict[int, list[Edge]]:
    out: dict[int, list[Edge]] = defaultdict(list)
    for target, slots in flow.inputs.items():
        for edges in slots.values():
            for source, handle in edges:
                out[source].append((target, handle))
    for edges in out.values():
        edges.sort()
    return out


def _same_edge_value(key: str, written: object, value: object) -> bool:
    """Spellings a load reads the same: no main input as ``None`` or ``[]``, handles left out as all ``output-0``."""
    if (written is _ABSENT or written in (None, [])) and value in (None, []):
        return True
    return key == "output_handles" and written is _ABSENT and all(handle == "output-0" for handle in value or [])


_ABSENT = object()


def _set(node: dict, key: str, value: object) -> None:
    """Write an edge field the way a save would, keeping a spelling the file already had that means the same."""
    if not _same_edge_value(key, node.get(key, _ABSENT), value):
        node[key] = value


def _emit_step(node: dict, slots: dict[str, tuple[Edge, ...]], keyed: bool, outputs: list[Edge]) -> dict:
    node = copy.deepcopy(node)
    if keyed or any(handle not in STATIC_SLOTS for handle in slots):
        connections = [
            {"from_id": source, "input_handle": handle, "source_handle": source_handle}
            for handle, edges in slots.items()
            for source, source_handle in edges
        ]
        _set(node, "left_input_id", None)
        _set(node, "right_input_id", None)
        _set(node, "input_ids", None)
        _set(node, "input_connections", connections or None)
    else:
        _set(node, "left_input_id", slots[LEFT][0][0] if LEFT in slots else None)
        _set(node, "right_input_id", slots[RIGHT][0][0] if RIGHT in slots else None)
        _set(node, "input_ids", [source for source, _ in slots.get(MAIN, ())] or None)
        _set(node, "input_connections", None)
    written = node.get("outputs") or []
    written_handles = node.get("output_handles") or []
    as_written = sorted(
        (target, written_handles[i] if i < len(written_handles) else "output-0") for i, target in enumerate(written)
    )
    if as_written != outputs:
        _set(node, "outputs", [target for target, _ in outputs])
        _set(node, "output_handles", [handle for _, handle in outputs])
    return node


def emit(flow: CanonicalFlow) -> dict:
    """The document ``flow`` reads as, both ends of every edge written from ``inputs``."""
    outputs = _outputs(flow)
    nodes = [
        _emit_step(node, flow.inputs.get(sid, {}), sid in flow.keyed, outputs.get(sid, []))
        for sid, node in flow.steps.items()
    ]
    order = list(flow.order)
    for key in ("nodes", "groups", "comments"):
        if key not in order and (key == "nodes" or getattr(flow, key)):
            order.append(key)
    doc: dict = {}
    for key in order:
        if key == "nodes":
            doc[key] = nodes
        elif key == "groups":
            doc[key] = [copy.deepcopy(group) for group in flow.groups.values()]
        elif key == "comments":
            doc[key] = [copy.deepcopy(comment) for comment in flow.comments.values()]
        elif key == "flowfile_settings":
            settings = copy.deepcopy(flow.settings)
            if isinstance(settings, dict) and ("parameters" in settings or flow.parameters):
                settings["parameters"] = [copy.deepcopy(p) for p in flow.parameters.values()]
            doc[key] = settings
        else:
            doc[key] = copy.deepcopy(flow.head[key])
    return doc
