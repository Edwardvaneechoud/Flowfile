"""A flow with open merge conflicts, written as a document no Flowfile opens as a flow.

Text conflict markers in a flow file would leave a person with YAML to edit by hand and a file that does not parse;
an extra key would be dropped by the next save without anyone noticing. So a conflicted flow is a wrapper::

    flowfile_conflict: 1
    flow_uuid: ...          # a project file's head keys stay on top, so the project still knows the flow
    sides: {base: ..., ours: ..., theirs: ...}
    merged: {...}           # the flow as it stands: ours plus everything that merged without a conflict
    conflicts: [...]        # each part both sides changed, as it is on the base and on each side
    warnings: [...]
    id_map: {...}

``FlowfileData`` has no ``nodes`` at its top, so every loader refuses it, older Flowfile releases included.
:func:`resolve` takes a side per conflict and, once none is left, gives back the plain flow.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, Literal

from shared.flow_merge.canonical import Issue, canonical, emit
from shared.flow_merge.merge import Conflict, IdMap, MergeResult

FORMAT = 1
MARKER = "flowfile_conflict"
HEAD_KEYS = ("flow_uuid", "catalog_name", "namespace")

Choice = Literal["ours", "theirs"]


class UnresolvedMergeError(ValueError):
    """A flow file that still holds merge conflicts was opened, run or applied."""


def is_conflicted(data: Any) -> bool:
    return isinstance(data, dict) and MARKER in data


def refuse_conflicted(data: Any, where: str) -> None:
    """Raise :class:`UnresolvedMergeError` naming ``where`` when ``data`` is a conflicted flow."""
    if is_conflicted(data):
        count = len(data.get("conflicts") or [])
        raise UnresolvedMergeError(
            f"{where} holds {count} merge conflict{'s' if count != 1 else ''} that are not settled yet. "
            "Settle them in the editor, or with `flowfile-merge resolve`, before it can be opened or run."
        )


def _conflict_dict(conflict: Conflict) -> dict:
    return {
        "key": conflict.key,
        "kind": conflict.kind,
        "label": conflict.label,
        "message": conflict.message,
        "related": list(conflict.related),
        "base": conflict.base,
        "ours": conflict.ours,
        "theirs": conflict.theirs,
    }


def conflict_from_dict(data: Mapping) -> Conflict:
    return Conflict(
        data["key"],
        data["kind"],
        data.get("label", data["key"]),
        data.get("message", ""),
        tuple(data.get("related") or ()),
        data.get("base"),
        data.get("ours"),
        data.get("theirs"),
    )


def conflict_document(result: MergeResult, sides: Mapping[str, str] | None = None) -> dict:
    """``result`` as a file: the plain merged flow when it is clean, else the wrapper."""
    if result.clean:
        return copy.deepcopy(result.document)
    head = {k: copy.deepcopy(result.document[k]) for k in HEAD_KEYS if k in result.document}
    return {
        MARKER: FORMAT,
        **head,
        "sides": dict(sides or {"base": "base", "ours": "ours", "theirs": "theirs"}),
        "merged": copy.deepcopy(result.document),
        "conflicts": [_conflict_dict(c) for c in result.conflicts],
        "warnings": [{"code": w.code, "message": w.message, "ids": list(w.ids)} for w in result.warnings],
        "id_map": {
            "nodes": dict(result.id_map.nodes),
            "groups": dict(result.id_map.groups),
            "comments": dict(result.id_map.comments),
        },
    }


def open_conflicts(document: Mapping) -> list[Conflict]:
    """The conflicts a wrapper still holds, ``[]`` for a plain flow."""
    return [conflict_from_dict(c) for c in document.get("conflicts") or []] if is_conflicted(document) else []


def _edges(slots: Mapping | None) -> dict[str, tuple]:
    return {handle: tuple((int(s), str(h)) for s, h in edges) for handle, edges in (slots or {}).items()}


def _take_step(flow, step_id: int, variant: Mapping | None) -> None:
    node = (variant or {}).get("step")
    if node is None:
        flow.steps.pop(step_id, None)
        flow.inputs.pop(step_id, None)
        for slots in flow.inputs.values():
            for handle in list(slots):
                slots[handle] = tuple(e for e in slots[handle] if e[0] != step_id)
                if not slots[handle]:
                    del slots[handle]
        return
    flow.steps[step_id] = copy.deepcopy(node)
    flow.inputs[step_id] = _edges(variant.get("inputs"))
    for target, slots in (variant.get("readers") or {}).items():
        target = int(target)
        if target in flow.steps:
            flow.inputs.setdefault(target, {}).update(_edges(slots))
    if variant.get("step", {}).get("input_connections"):
        flow.keyed.add(step_id)


def resolve(conflicted: Mapping, choices: Mapping[str, Choice]) -> dict:
    """``conflicted`` with each conflict ``choices`` names (``step:7`` -> ``theirs``) settled on that side.

    Gives back the plain flow once no conflict is left, else the wrapper with the rest. A conflict not named stays
    open; a key the wrapper does not hold is a ``KeyError``.
    """
    if not is_conflicted(conflicted):
        raise ValueError("Not a conflicted flow: there is nothing to resolve.")
    held = {c["key"]: c for c in conflicted.get("conflicts") or []}
    unknown = sorted(set(choices) - set(held))
    if unknown:
        raise KeyError(f"No open conflict {', '.join(unknown)}; open: {', '.join(held) or 'none'}.")
    if conflicted["merged"].get("nodes") is None and "schedules" in conflicted["merged"]:
        return _resolve_records(conflicted, held, choices)
    flow, _ = canonical(conflicted["merged"])
    for key, side in choices.items():
        if side not in ("ours", "theirs"):
            raise ValueError(f"Take 'ours' or 'theirs' for {key}, not {side!r}.")
        conflict = held[key]
        kind, _, name = key.partition(":")
        if kind == "step":
            _take_step(flow, int(name), conflict.get(side))
        elif kind == "parameter":
            value = conflict.get(side)
            if value is None:
                flow.parameters.pop(name, None)
            else:
                flow.parameters[name] = copy.deepcopy(value)
    present = set(flow.steps)
    for slots in flow.inputs.values():
        for handle in list(slots):
            slots[handle] = tuple(e for e in slots[handle] if e[0] in present)
            if not slots[handle]:
                del slots[handle]
    merged = emit(flow)
    left = [c for k, c in held.items() if k not in choices]
    if not left:
        return merged
    return {
        **{k: v for k, v in conflicted.items() if k not in ("merged", "conflicts")},
        "merged": merged,
        "conflicts": left,
    }


def _resolve_records(conflicted: Mapping, held: dict, choices: Mapping[str, Choice]) -> dict:
    from shared.flow_merge.records import take_schedule

    merged = copy.deepcopy(conflicted["merged"])
    for key, side in choices.items():
        if side not in ("ours", "theirs"):
            raise ValueError(f"Take 'ours' or 'theirs' for {key}, not {side!r}.")
        take_schedule(merged, key.partition(":")[2], held[key].get(side))
    left = [c for k, c in held.items() if k not in choices]
    if not left:
        return merged
    return {
        **{k: v for k, v in conflicted.items() if k not in ("merged", "conflicts")},
        "merged": merged,
        "conflicts": left,
    }


def as_result(conflicted: Mapping) -> MergeResult:
    """A wrapper read back as a :class:`MergeResult` (its ``changes`` left empty)."""
    from shared.flow_merge.diff import FlowDiff

    id_map = conflicted.get("id_map") or {}
    return MergeResult(
        document=copy.deepcopy(conflicted["merged"]),
        changes=FlowDiff(),
        conflicts=tuple(open_conflicts(conflicted)),
        warnings=tuple(
            Issue(w["code"], w["message"], tuple(w.get("ids") or ())) for w in conflicted.get("warnings") or []
        ),
        id_map=IdMap(
            {int(k): v for k, v in (id_map.get("nodes") or {}).items()},
            {int(k): v for k, v in (id_map.get("groups") or {}).items()},
            {int(k): v for k, v in (id_map.get("comments") or {}).items()},
        ),
    )
