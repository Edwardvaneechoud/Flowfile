"""The three-way merge of a flow, step by step.

``merge(base, ours, theirs)`` combines what ``ours`` and ``theirs`` each changed since ``base``. ``ours`` is the side
merged into (production, or the branch being merged into) and keeps its ids: a step that exists on ``ours`` never
changes id, since a step's id names its incremental-read cursor and its Kafka consumer group outside the file. A step
``theirs`` added under an id ``ours`` also added gets the next free id, and every field that names it is rewritten.

Per step, its type and settings are one unit: changed on one side, the change is taken; changed differently on both, it
is a conflict. Its inputs merge per input slot, its description, reference and position per field. A step removed on
one side is removed, unless the other side changed it or connected a new reader to it: then it is a conflict.

The result is always a flow that loads: ``ours`` plus everything that merged without a conflict. Each conflict holds the
step as it is on the base and on each side (with its inputs, and the input slots of the steps that read it), so a person
can take one side per step afterwards (:func:`shared.flow_merge.conflict.resolve`).
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from shared.flow_merge.canonical import CanonicalFlow, Edge, Issue, canonical, emit
from shared.flow_merge.diff import FLOW_NOISE, SETTINGS_NOISE, FlowDiff, diff, step_label, step_settings
from shared.flow_merge.edges import MAIN
from shared.flow_merge.placement import Placer, node_box
from shared.flow_merge.relabel import relabel, relabel_comments, relabel_groups

UNION_TYPES = frozenset({"union"})
LAYOUT = ("x_position", "y_position", "group_id")
_ABSENT: Any = object()

Side = Literal["ours", "theirs"]


@dataclass(frozen=True)
class Conflict:
    """A part of the flow both sides changed in ways that cannot both hold.

    ``key`` names it (``step:7``, ``parameter:region``); ``base``, ``ours`` and ``theirs`` are that part on each side,
    ``None`` where it does not exist. For a step each is ``{"step", "inputs", "readers"}``: the node, its input slots,
    and the input slots of the steps that read from it on that side.
    """

    key: str
    kind: str
    label: str
    message: str
    related: tuple[int, ...] = ()
    base: dict | None = None
    ours: dict | None = None
    theirs: dict | None = None


@dataclass(frozen=True)
class IdMap:
    """The ids ``theirs`` used that the merge renumbered: old id -> new id."""

    nodes: dict[int, int] = field(default_factory=dict)
    groups: dict[int, int] = field(default_factory=dict)
    comments: dict[int, int] = field(default_factory=dict)


@dataclass(frozen=True)
class MergeResult:
    document: dict
    changes: FlowDiff
    conflicts: tuple[Conflict, ...] = ()
    warnings: tuple[Issue, ...] = ()
    id_map: IdMap = field(default_factory=IdMap)

    @property
    def clean(self) -> bool:
        return not self.conflicts


def _content(node: Mapping | None) -> Any:
    """What a step does: its type and settings, without what a save caches."""
    return None if node is None else (node.get("type"), step_settings(node))


def _added_only(base: Any, side: Any) -> bool:
    """``side`` is ``base`` with keys added and none changed or removed: a re-save by a newer Flowfile."""
    if isinstance(base, dict) and isinstance(side, dict):
        return all(key in side and _added_only(value, side[key]) for key, value in base.items())
    return base == side


def _with_added(target: Any, extra: Any) -> Any:
    """``target`` with the keys ``extra`` has and it lacks, at any depth."""
    if isinstance(target, dict) and isinstance(extra, dict):
        out = dict(target)
        for key, value in extra.items():
            out[key] = copy.deepcopy(value) if key not in target else _with_added(target[key], value)
        return out
    return target


def _slots_json(slots: Mapping[str, tuple[Edge, ...]] | None) -> dict:
    return {handle: [list(edge) for edge in edges] for handle, edges in (slots or {}).items()}


def _variant(flow: CanonicalFlow, step_id: int) -> dict | None:
    node = flow.steps.get(step_id)
    readers = {
        target: _slots_json({h: es for h, es in slots.items() if any(s == step_id for s, _ in es)})
        for target, slots in flow.inputs.items()
        if target != step_id and any(s == step_id for es in slots.values() for s, _ in es)
    }
    if node is None and not readers:
        return None
    return {"step": copy.deepcopy(node), "inputs": _slots_json(flow.inputs.get(step_id)), "readers": readers}


def _three_way(base: Any, ours: Any, theirs: Any) -> tuple[Any, bool]:
    """The merged value and whether both sides changed it differently (``ours`` is then kept)."""
    if ours == base:
        return theirs, False
    if theirs == base or theirs == ours:
        return ours, False
    return ours, True


def _merge_fields(
    base: Mapping | None, ours: Mapping, theirs: Mapping, *, skip: frozenset = frozenset()
) -> tuple[dict, list[str]]:
    """A dict merged per key, ``ours`` winning where both changed a key differently; those keys are returned."""
    base = base or {}
    out: dict = {}
    contested: list[str] = []
    for key in [*ours, *(k for k in theirs if k not in ours)]:
        if key in skip:
            if key in ours:
                out[key] = copy.deepcopy(ours[key])
            continue
        value, both = _three_way(base.get(key, _ABSENT), ours.get(key, _ABSENT), theirs.get(key, _ABSENT))
        if both:
            contested.append(key)
        if value is not _ABSENT:
            out[key] = copy.deepcopy(value)
    return out, contested


def _renumber(
    base: Mapping[int, Any],
    ours: Mapping[int, Any],
    theirs: Mapping[int, Any],
    same: Callable[[int], bool],
    taken: set[int],
) -> dict[int, int]:
    """New ids for what ``theirs`` added under an id ``ours`` also added, unless both added the same thing."""
    clashes = sorted(i for i in theirs if i not in base and i in ours and not same(i))
    next_id = max(taken, default=0) + 1
    mapping = {}
    for old in clashes:
        mapping[old] = next_id
        next_id += 1
    return mapping


class _Merge:
    def __init__(self, base: Mapping, ours: Mapping, theirs: Mapping) -> None:
        self.warnings: list[Issue] = []
        self.conflicts: list[Conflict] = []
        self.B, issues_b = canonical(base)
        self.O, issues_o = canonical(ours)
        raw_t, issues_t = canonical(theirs)
        for side, issues in (("base", issues_b), ("ours", issues_o), ("theirs", issues_t)):
            self.warnings += [Issue(i.code, f"{side}: {i.message}", i.ids) for i in issues]
        self.id_map = self._renumber_theirs(raw_t)
        self.T, _ = canonical(
            relabel_comments(
                relabel_groups(relabel(emit(raw_t), self.id_map.nodes), self.id_map.groups), self.id_map.comments
            )
        )
        self.deleted_by = {
            "ours": set(self.B.steps) - set(self.O.steps),
            "theirs": set(self.B.steps) - set(self.T.steps),
        }
        self.nodes: dict[int, dict] = {}
        self.inputs: dict[int, dict[str, tuple[Edge, ...]]] = {}
        self.conflicted: set[int] = set()
        self.from_theirs: set[int] = set()

    def _renumber_theirs(self, raw_t: CanonicalFlow) -> IdMap:
        taken = set(self.B.steps) | set(self.O.steps) | set(raw_t.steps)
        nodes = _renumber(
            self.B.steps,
            self.O.steps,
            raw_t.steps,
            lambda i: _content(self.O.steps[i]) == _content(raw_t.steps[i])
            and self.O.inputs.get(i) == raw_t.inputs.get(i),
            taken,
        )
        group_taken = set(self.B.groups) | set(self.O.groups) | set(raw_t.groups)
        groups = _renumber(
            self.B.groups, self.O.groups, raw_t.groups, lambda i: self.O.groups[i] == raw_t.groups[i], group_taken
        )
        comment_taken = set(self.B.comments) | set(self.O.comments) | set(raw_t.comments)
        comments = _renumber(
            self.B.comments,
            self.O.comments,
            raw_t.comments,
            lambda i: self.O.comments[i] == raw_t.comments[i],
            comment_taken,
        )
        return IdMap(nodes, groups, comments)

    def _side(self, side: Side) -> CanonicalFlow:
        return self.O if side == "ours" else self.T

    def _label(self, step_id: int) -> str:
        node = self.O.steps.get(step_id) or self.T.steps.get(step_id) or self.B.steps.get(step_id) or {}
        return step_label(step_id, node)

    def _conflict(self, step_id: int, kind: str, message: str, related: tuple[int, ...] = ()) -> None:
        self.conflicted.add(step_id)
        self.conflicts.append(
            Conflict(
                f"step:{step_id}",
                kind,
                self._label(step_id),
                message,
                related,
                _variant(self.B, step_id),
                _variant(self.O, step_id),
                _variant(self.T, step_id),
            )
        )

    def _slot_changed(self, side: Side, step_id: int, handle: str) -> bool:
        """Whether ``side`` changed the slot on purpose: an edge it lost only because it removed the source is not."""
        removed = self.deleted_by[side]
        base = tuple(e for e in self.B.inputs.get(step_id, {}).get(handle, ()) if e[0] not in removed)
        return self._side(side).inputs.get(step_id, {}).get(handle, ()) != base

    def _inputs_changed(self, side: Side, step_id: int) -> bool:
        handles = set(self.B.inputs.get(step_id, {})) | set(self._side(side).inputs.get(step_id, {}))
        return any(self._slot_changed(side, step_id, h) for h in handles)

    def _settings_change(self, step_id: int, side: Side) -> Literal["none", "added", "changed"]:
        base, node = self.B.steps[step_id], self._side(side).steps[step_id]
        if _content(node) == _content(base):
            return "none"
        if base.get("type") == node.get("type") and _added_only(step_settings(base), step_settings(node)):
            return "added"
        return "changed"

    def steps(self) -> None:
        ids = list(dict.fromkeys([*self.O.steps, *self.T.steps, *self.B.steps]))
        for sid in ids:
            b, o, t = self.B.steps.get(sid), self.O.steps.get(sid), self.T.steps.get(sid)
            if b is None:
                if o is not None:
                    self.nodes[sid] = copy.deepcopy(o)
                else:
                    self.nodes[sid] = copy.deepcopy(t)
                    self.from_theirs.add(sid)
            elif o is not None and t is not None:
                self._both_kept(sid)
            elif o is not None:
                if self._settings_change(sid, "ours") != "none" or self._inputs_changed("ours", sid):
                    self.nodes[sid] = copy.deepcopy(o)
                    self._conflict(
                        sid, "delete_vs_change", f"{self._label(sid)} was removed on theirs and changed on ours."
                    )
            elif t is not None:
                if self._settings_change(sid, "theirs") != "none" or self._inputs_changed("theirs", sid):
                    self._conflict(
                        sid, "delete_vs_change", f"{self._label(sid)} was removed on ours and changed on theirs."
                    )

    def _both_kept(self, sid: int) -> None:
        b, o, t = self.B.steps[sid], self.O.steps[sid], self.T.steps[sid]
        ours, theirs = self._settings_change(sid, "ours"), self._settings_change(sid, "theirs")
        if ours == "changed" and theirs == "changed" and _content(o) != _content(t):
            chosen = copy.deepcopy(o)
            self._conflict(sid, "both_changed", f"Both sides changed the settings of {self._label(sid)}.")
        elif theirs == "changed" and ours != "changed":
            chosen = copy.deepcopy(t)
            if ours == "added":
                chosen["setting_input"] = _with_added(chosen.get("setting_input"), o.get("setting_input"))
        else:
            chosen = copy.deepcopy(o)
            if theirs == "added":
                chosen["setting_input"] = _with_added(chosen.get("setting_input"), t.get("setting_input"))
        if "added" in (ours, theirs):
            self.warnings.append(
                Issue(
                    "mixed_versions",
                    f"{self._label(sid)}: one side only added settings a newer Flowfile writes.",
                    (sid,),
                )
            )
        if chosen.get("type") != b.get("type"):
            self.warnings.append(
                Issue(
                    "id_reused",
                    f"Step {sid} is now a {chosen.get('type')}, no longer a {b.get('type')}: what is kept for a step "
                    "outside the flow (its incremental-read position, its Kafka offsets) goes to the new step.",
                    (sid,),
                )
            )
        for key in LAYOUT:
            value, _ = _three_way(b.get(key), o.get(key), t.get(key))
            chosen[key] = value
        reference, both = _three_way(b.get("node_reference"), o.get("node_reference"), t.get("node_reference"))
        chosen["node_reference"] = reference
        if both:
            self.warnings.append(Issue("reference_kept", f"{self._label(sid)}: both renamed it; ours is kept.", (sid,)))
        chosen["description"] = self._description(sid, ours, theirs)
        self.nodes[sid] = chosen

    def _description(self, sid: int, ours: str, theirs: str) -> Any:
        """Typed beats generated: a side that changed the description without the settings typed it."""
        b, o, t = (self.B.steps[sid].get("description"), self.O.steps[sid].get("description"),
                   self.T.steps[sid].get("description"))  # fmt: skip
        value, both = _three_way(b, o, t)
        if not both:
            return value
        typed_o, typed_t = ours == "none", theirs == "none"
        if typed_t and not typed_o:
            return t
        if typed_o and typed_t:
            self.warnings.append(
                Issue("description_kept", f"{self._label(sid)}: both typed a description; ours is kept.", (sid,))
            )
        return o

    def inputs_of_steps(self) -> None:
        for sid in self.nodes:
            if sid in self.conflicted:
                self.inputs[sid] = dict(
                    self.O.inputs.get(sid, {}) if sid in self.O.steps else self.T.inputs.get(sid, {})
                )
                continue
            if sid not in self.B.steps:
                self.inputs[sid] = dict((self.T if sid in self.from_theirs else self.O).inputs.get(sid, {}))
                continue
            self._merge_slots(sid)

    def _merge_slots(self, sid: int) -> None:
        base, ours, theirs = self.B.inputs.get(sid, {}), self.O.inputs.get(sid, {}), self.T.inputs.get(sid, {})
        node = self.nodes[sid]
        slots: dict[str, tuple[Edge, ...]] = {}
        contested: list[str] = []
        for handle in dict.fromkeys([*base, *ours, *theirs]):
            o_slot, t_slot = ours.get(handle, ()), theirs.get(handle, ())
            o_changed, t_changed = self._slot_changed("ours", sid, handle), self._slot_changed("theirs", sid, handle)
            if t_changed and not o_changed:
                merged = t_slot
            elif not t_changed or o_slot == t_slot:
                merged = o_slot
            elif handle == MAIN and node.get("type") in UNION_TYPES:
                merged = self._union_slot(base.get(handle, ()), o_slot, t_slot)
                self.warnings.append(
                    Issue("union_inputs", f"{self._label(sid)}: the inputs both sides gave it are all kept.", (sid,))
                )
            else:
                contested.append(handle)
                merged = o_slot
            if merged:
                slots[handle] = merged
        if contested:
            self.inputs[sid] = dict(ours)
            self._conflict(
                sid,
                "inputs_both_changed",
                f"Both sides changed what feeds {self._label(sid)} ({', '.join(contested)}).",
            )
            return
        positional = node.get("type") not in UNION_TYPES and any(
            len(s.get(MAIN, ())) >= 2 for s in (base, ours, theirs)
        )
        for changed_inputs, changed_settings in (("ours", "theirs"), ("theirs", "ours")):
            if (
                positional
                and self._slot_changed(changed_inputs, sid, MAIN)
                and self._settings_change(sid, changed_settings) == "changed"
            ):
                self.inputs[sid] = dict(ours)
                self._conflict(
                    sid,
                    "inputs_vs_settings",
                    f"{self._label(sid)} reads its inputs by position: {changed_inputs} changed them and "
                    f"{changed_settings} changed its settings.",
                )
                return
        self.inputs[sid] = slots

    @staticmethod
    def _union_slot(base: tuple, ours: tuple, theirs: tuple) -> tuple:
        removed = [e for e in base if e not in ours or e not in theirs]
        kept = [e for e in base if e not in removed]
        added = [e for e in ours if e not in base] + [e for e in theirs if e not in base and e not in ours]
        return tuple(kept + added)

    def deleted_but_connected(self) -> None:
        """A step one side removed and the other side newly reads from cannot simply go."""
        for side, other in (("ours", "theirs"), ("theirs", "ours")):
            for sid in sorted(self.deleted_by[side] - self.deleted_by[other] - self.conflicted):
                flow = self._side(other)
                readers = tuple(
                    t
                    for t, slots in flow.inputs.items()
                    if any(s == sid for es in slots.values() for s, _ in es)
                    and any(self._slot_changed(other, t, h) for h in slots)
                )
                if readers:
                    if side == "theirs":
                        self.nodes[sid] = copy.deepcopy(self.O.steps[sid])
                        self.inputs[sid] = dict(self.O.inputs.get(sid, {}))
                    self._conflict(
                        sid,
                        "deleted_but_connected",
                        f"{self._label(sid)} was removed on {side}, and {other} connects "
                        f"step{'s' if len(readers) > 1 else ''} {', '.join(map(str, readers))} to it.",
                        readers,
                    )

    def finish_steps(self) -> None:
        present = set(self.nodes)
        for sid in list(self.inputs):
            if sid not in present:
                del self.inputs[sid]
                continue
            slots = {h: tuple(e for e in es if e[0] in present) for h, es in self.inputs[sid].items()}
            self.inputs[sid] = {h: es for h, es in slots.items() if es}
        self._place_new_steps()
        self._unique_references()
        self._code_names()

    def _place_new_steps(self) -> None:
        for sid in sorted(self.from_theirs):
            node = self.nodes[sid]
            others = {
                i: (n.get("x_position") or 0, n.get("y_position") or 0) for i, n in self.nodes.items() if i != sid
            }
            box = node_box(node.get("x_position") or 0, node.get("y_position") or 0)
            if any(box.overlaps(node_box(x, y)) for x, y in others.values()):
                x, y = Placer(others).free_slot(node.get("x_position") or 0, node.get("y_position") or 0)
                node["x_position"], node["y_position"] = x, y

    def _unique_references(self) -> None:
        seen: dict[str, int] = {}
        for sid in sorted(self.nodes, key=lambda i: (i in self.from_theirs, i)):
            ref = self.nodes[sid].get("node_reference")
            if not ref:
                continue
            if ref in seen:
                n = 2
                while f"{ref}_{n}" in seen:
                    n += 1
                self.nodes[sid]["node_reference"] = f"{ref}_{n}"
                self.warnings.append(
                    Issue(
                        "reference_renamed",
                        f"Step {sid} and step {seen[ref]} were both called {ref!r}; step {sid} is now {ref}_{n}.",
                        (sid, seen[ref]),
                    )
                )
                ref = f"{ref}_{n}"
            seen[ref] = sid

    def _code_names(self) -> None:
        """Code in a step names an input without a reference ``df_<id>``; a renumbered input changes that name."""
        for old, new in self.id_map.nodes.items():
            if self.nodes.get(new, {}).get("node_reference"):
                continue
            pattern = re.compile(rf"\bdf_{old}\b")
            for target, slots in self.inputs.items():
                if any(s == new for es in slots.values() for s, _ in es):
                    text = json.dumps(self.nodes[target].get("setting_input"), default=str)
                    if pattern.search(text):
                        self.warnings.append(
                            Issue(
                                "code_names_renumbered_step",
                                f"Step {target}'s code names df_{old}, but that input is now step {new}: "
                                f"write df_{new}, or give step {new} a reference.",
                                (target, new),
                            )
                        )

    def groups(self) -> dict[int, dict]:
        out: dict[int, dict] = {}
        for gid in dict.fromkeys([*self.O.groups, *self.T.groups, *self.B.groups]):
            b, o, t = self.B.groups.get(gid), self.O.groups.get(gid), self.T.groups.get(gid)
            if o is not None and t is not None:
                out[gid], _ = _merge_fields(b, o, t)
            elif b is None:
                out[gid] = copy.deepcopy(o if o is not None else t)
            elif (o if o is not None else t) != b and (o is not None or t is not None):
                out[gid] = copy.deepcopy(o if o is not None else t)
                self.warnings.append(
                    Issue(
                        "group_kept",
                        f"Group {gid} was removed on one side and changed on the other; it is kept.",
                        (gid,),
                    )
                )
        for group in out.values():
            if group.get("parent_group_id") is not None and group["parent_group_id"] not in out:
                group["parent_group_id"] = None
        for gid in out:
            seen, current = {gid}, out[gid].get("parent_group_id")
            while current is not None:
                if current in seen:
                    out[gid]["parent_group_id"] = None
                    self.warnings.append(
                        Issue("group_cycle", f"Group {gid} was nested in itself; it is now at the top.", (gid,))
                    )
                    break
                seen.add(current)
                current = out[current].get("parent_group_id")
        for node in self.nodes.values():
            if node.get("group_id") is not None and node["group_id"] not in out:
                node["group_id"] = None
        return out

    def comments(self) -> dict[int, dict]:
        out: dict[int, dict] = {}
        extra: list[dict] = []
        for cid in dict.fromkeys([*self.O.comments, *self.T.comments, *self.B.comments]):
            b, o, t = self.B.comments.get(cid), self.O.comments.get(cid), self.T.comments.get(cid)
            if o is not None and t is not None:
                merged, contested = _merge_fields(b, o, t)
                out[cid] = merged
                if "text" in contested:
                    copy_ = copy.deepcopy(t)
                    copy_["x_position"] = (copy_.get("x_position") or 0) + 30
                    copy_["y_position"] = (copy_.get("y_position") or 0) + 30
                    extra.append(copy_)
                    self.warnings.append(
                        Issue(
                            "comment_both_edited", f"Both sides edited comment {cid}; theirs is kept beside it.", (cid,)
                        )
                    )
            elif b is None:
                out[cid] = copy.deepcopy(o if o is not None else t)
            elif (o if o is not None else t) not in (None, b):
                out[cid] = copy.deepcopy(o if o is not None else t)
        next_id = max([*out, *self.B.comments, *self.T.comments], default=0) + 1
        for comment in extra:
            comment["id"] = next_id
            out[next_id] = comment
            next_id += 1
        return out

    def parameters(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for name in dict.fromkeys([*self.O.parameters, *self.T.parameters, *self.B.parameters]):
            b, o, t = self.B.parameters.get(name), self.O.parameters.get(name), self.T.parameters.get(name)
            value, both = _three_way(b, o, t)
            if both:
                self.conflicts.append(
                    Conflict(
                        f"parameter:{name}",
                        "parameter_both_changed",
                        f"parameter {name}",
                        f"Both sides changed the flow parameter {name}.",
                        base=copy.deepcopy(b),
                        ours=copy.deepcopy(o),
                        theirs=copy.deepcopy(t),
                    )
                )
            if value is not None:
                out[name] = copy.deepcopy(value)
        return out

    def settings(self) -> dict | None:
        if self.O.settings is None and self.T.settings is None:
            return None
        base = {k: v for k, v in (self.B.settings or {}).items() if k != "parameters"}
        ours = {k: v for k, v in (self.O.settings or {}).items() if k != "parameters"}
        theirs = {k: v for k, v in (self.T.settings or {}).items() if k != "parameters"}
        merged, contested = _merge_fields(base, ours, theirs, skip=SETTINGS_NOISE)
        for key in contested:
            self.warnings.append(Issue("setting_kept", f"Both sides changed the flow setting {key}; ours is kept."))
        if "parameters" in (self.O.settings or {}):
            merged = {k: merged.get(k) for k in self.O.settings if k == "parameters" or k in merged} | merged
        return merged

    def head(self) -> dict:
        merged, contested = _merge_fields(self.B.head, self.O.head, self.T.head, skip=FLOW_NOISE)
        for key in contested:
            self.warnings.append(Issue("flow_field_kept", f"Both sides changed the flow's {key}; ours is kept."))
        return merged

    def run(self) -> tuple[dict, IdMap]:
        self.steps()
        self.inputs_of_steps()
        self.deleted_but_connected()
        self.finish_steps()
        groups = self.groups()
        flow = CanonicalFlow(
            order=list(self.O.order) + [k for k in self.T.order if k not in self.O.order],
            head=self.head(),
            settings=self.settings(),
            parameters=self.parameters(),
            steps={
                sid: self.nodes[sid]
                for sid in [*self.O.steps, *(s for s in self.nodes if s not in self.O.steps)]
                if sid in self.nodes
            },  # fmt: skip
            inputs=self.inputs,
            keyed={s for s in self.nodes if s in self.O.keyed or s in self.T.keyed},
            groups=groups,
            comments=self.comments(),
        )
        return emit(flow), self.id_map


def merge(base: Mapping, ours: Mapping, theirs: Mapping, *, keep_ids: Side = "ours") -> MergeResult:
    """``ours`` with what ``theirs`` changed since ``base``; ``keep_ids="theirs"`` keeps the ids of ``theirs`` instead.

    Keeping the ids of a side also makes it the side that wins a tie of layout, a description both typed or a
    reference both changed, and the side whose step stays in the document while a conflict is open.
    """
    if keep_ids == "theirs":
        job = _Merge(base, theirs, ours)
        document, id_map = job.run()
        conflicts = tuple(
            Conflict(c.key, c.kind, c.label, _swap_words(c.message), c.related, c.base, c.theirs, c.ours)
            for c in job.conflicts
        )
        warnings = tuple(Issue(w.code, _swap_words(w.message), w.ids) for w in job.warnings)
    else:
        job = _Merge(base, ours, theirs)
        document, id_map = job.run()
        conflicts, warnings = tuple(job.conflicts), tuple(job.warnings)
    return MergeResult(document, diff(ours, document), conflicts, warnings, id_map)


def _swap_words(text: str) -> str:
    return re.sub(r"\b(ours|theirs)\b", lambda m: "theirs" if m.group(1) == "ours" else "ours", text)
