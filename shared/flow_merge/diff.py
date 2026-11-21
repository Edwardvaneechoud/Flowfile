"""What changed between two versions of a flow, step by step, with layout kept apart.

A change is semantic when it changes what the flow does: a step added or removed, its type, settings, inputs or
reference, a typed description, a flow parameter, a flow setting, a group's name or a comment's text. Layout is where
things sit: a step's position or group, a group's or comment's box. Noise is what a save rewrites without anyone
changing anything (the version that wrote the file, its id, the registration it came from, ``is_start_node``, both
ends of an edge, a schema a source cached) and is left out of both.

A file written by an older Flowfile leaves out keys a newer one writes with their default value. Between two files of
different Flowfile versions, a key present on one side only is reported under ``format``, not as a change.

A step's description is the text its canvas card shows: what someone typed, else a text generated from its settings.
A file cannot tell the two apart, so a description is reported as changed only when the settings did not change.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from shared.flow_merge.canonical import CanonicalFlow, canonical

FLOW_NOISE = frozenset({"flowfile_version", "flowfile_id"})
SETTINGS_NOISE = frozenset({"source_registration_id", "hub_origin", "execution_location", "path", "modified_on"})
LAYOUT_FIELDS = ("x_position", "y_position", "group_id")
CACHED_SCHEMA_TYPES = frozenset({"rest_api_reader", "database_reader", "kafka_source"})
GROUP_LAYOUT = frozenset({"x_position", "y_position", "width", "height", "collapsed"})
COMMENT_LAYOUT = frozenset({"x_position", "y_position", "width", "height"})

Kind = Literal["added", "removed", "changed"]
Item = Literal["step", "group", "comment", "parameter", "setting", "flow"]


class _Missing:
    def __repr__(self) -> str:
        return "<absent>"


MISSING: Any = _Missing()


@dataclass(frozen=True)
class FieldChange:
    """One value that differs; ``MISSING`` on a side where the key is not there."""

    path: str
    before: Any
    after: Any


@dataclass(frozen=True)
class Change:
    kind: Kind
    item: Item
    id: int | str
    label: str
    aspects: tuple[str, ...] = ()
    fields: tuple[FieldChange, ...] = ()


@dataclass(frozen=True)
class FlowDiff:
    changes: tuple[Change, ...] = ()
    layout: tuple[Change, ...] = ()
    format: tuple[Change, ...] = ()
    issues: tuple = field(default=())

    @property
    def empty(self) -> bool:
        return not self.changes


def _drop_machine_paths(value: Any) -> None:
    """``abs_file_path`` is the file's path resolved on the machine that loaded the flow."""
    if isinstance(value, dict):
        value.pop("abs_file_path", None)
        for item in value.values():
            _drop_machine_paths(item)
    elif isinstance(value, list):
        for item in value:
            _drop_machine_paths(item)


def step_settings(node: Mapping) -> Any:
    """A step's ``setting_input`` without what a run, a load or a save fills in by itself."""
    settings = copy.deepcopy(node.get("setting_input"))
    if isinstance(settings, dict) and node.get("type") in CACHED_SCHEMA_TYPES:
        settings.pop("fields", None)
    _drop_machine_paths(settings)
    return settings


def _split_format(fields: list[FieldChange], versions_differ: bool) -> tuple[list[FieldChange], list[FieldChange]]:
    """``(changes, format)``: a key on one side only is format when the two files come from different versions."""
    if not versions_differ:
        return fields, []
    added = [f for f in fields if f.before is MISSING or f.after is MISSING]
    return [f for f in fields if f not in added], added


def step_label(step_id: int, node: Mapping) -> str:
    """``step 6 join "left join on product_line"``: the id, the type and what the canvas card says."""
    label = f"step {step_id} {node.get('type')}"
    if node.get("node_reference"):
        label += f" ({node['node_reference']})"
    if node.get("description"):
        label += f' "{node["description"]}"'
    return label


def field_changes(before: Any, after: Any, path: str = "") -> list[FieldChange]:
    """The values that differ, down to the deepest dict key or list index both sides still share a shape for."""
    if before == after:
        return []
    if isinstance(before, dict) and isinstance(after, dict):
        out: list[FieldChange] = []
        for key in [*before, *(k for k in after if k not in before)]:
            out += field_changes(before.get(key, MISSING), after.get(key, MISSING), f"{path}.{key}" if path else key)
        return out
    if (
        isinstance(before, list)
        and isinstance(after, list)
        and len(before) == len(after)
        and all(isinstance(x, dict) for x in before + after)
    ):
        out = []
        for index, (a, b) in enumerate(zip(before, after, strict=True)):
            out += field_changes(a, b, f"{path}[{index}]")
        return out
    return [FieldChange(path or "value", before, after)]


def _inputs(flow: CanonicalFlow, step_id: int) -> dict[str, list[list]]:
    return {handle: [list(edge) for edge in edges] for handle, edges in flow.inputs.get(step_id, {}).items()}


def _steps(
    a: CanonicalFlow, b: CanonicalFlow, versions_differ: bool
) -> tuple[list[Change], list[Change], list[Change]]:
    changes: list[Change] = []
    layout: list[Change] = []
    format_: list[Change] = []
    for sid in sorted(a.steps.keys() | b.steps.keys()):
        old, new = a.steps.get(sid), b.steps.get(sid)
        if old is not None and new is not None and old.get("type") != new.get("type"):
            changes.append(Change("removed", "step", sid, step_label(sid, old), ("type",)))
            changes.append(Change("added", "step", sid, step_label(sid, new), ("type",)))
            continue
        if old is None:
            changes.append(Change("added", "step", sid, step_label(sid, new)))
            continue
        if new is None:
            changes.append(Change("removed", "step", sid, step_label(sid, old)))
            continue
        aspects: list[str] = []
        fields: list[FieldChange] = []
        settings, defaults = _split_format(
            field_changes(step_settings(old), step_settings(new), "settings"), versions_differ
        )
        if defaults:
            format_.append(Change("changed", "step", sid, step_label(sid, new), ("settings",), tuple(defaults)))
        if settings:
            aspects.append("settings")
            fields += settings
        inputs = field_changes(_inputs(a, sid), _inputs(b, sid), "inputs")
        if inputs:
            aspects.append("inputs")
            fields += inputs
        if old.get("node_reference") != new.get("node_reference"):
            aspects.append("reference")
            fields.append(FieldChange("node_reference", old.get("node_reference"), new.get("node_reference")))
        if not settings and (old.get("description") or "") != (new.get("description") or ""):
            aspects.append("description")
            fields.append(FieldChange("description", old.get("description"), new.get("description")))
        if aspects:
            changes.append(Change("changed", "step", sid, step_label(sid, new), tuple(aspects), tuple(fields)))
        moved = [FieldChange(f, old.get(f), new.get(f)) for f in LAYOUT_FIELDS if old.get(f) != new.get(f)]
        if moved:
            aspects = tuple(dict.fromkeys("group" if f.path == "group_id" else "position" for f in moved))
            layout.append(Change("changed", "step", sid, step_label(sid, new), aspects, tuple(moved)))
    return changes, layout, format_


def _boxes(
    item: Item, a: dict[int, dict], b: dict[int, dict], layout_keys: frozenset, name: str
) -> tuple[list[Change], list[Change]]:
    changes: list[Change] = []
    layout: list[Change] = []
    for key in sorted(a.keys() | b.keys()):
        old, new = a.get(key), b.get(key)
        label = f"{item} {key}" + (f' "{(new or old).get(name)}"' if (new or old).get(name) else "")
        if old is None or new is None:
            changes.append(Change("added" if old is None else "removed", item, key, label))
            continue
        semantic = field_changes(
            {k: v for k, v in old.items() if k not in layout_keys},
            {k: v for k, v in new.items() if k not in layout_keys},
        )
        boxes = field_changes(
            {k: v for k, v in old.items() if k in layout_keys}, {k: v for k, v in new.items() if k in layout_keys}
        )
        if semantic:
            changes.append(Change("changed", item, key, label, tuple(f.path for f in semantic), tuple(semantic)))
        if boxes:
            layout.append(Change("changed", item, key, label, ("position",), tuple(boxes)))
    return changes, layout


def _flow(a: CanonicalFlow, b: CanonicalFlow, versions_differ: bool) -> tuple[list[Change], list[Change]]:
    changes: list[Change] = []
    format_: list[Change] = []
    for key in [*a.head, *(k for k in b.head if k not in a.head)]:
        if key in FLOW_NOISE:
            continue
        old, new = a.head.get(key, MISSING), b.head.get(key, MISSING)
        if old != new:
            changes.append(Change("changed", "flow", key, f"flow {key}", (key,), (FieldChange(key, old, new),)))
    old_settings = {k: v for k, v in (a.settings or {}).items() if k not in SETTINGS_NOISE and k != "parameters"}
    new_settings = {k: v for k, v in (b.settings or {}).items() if k not in SETTINGS_NOISE and k != "parameters"}
    settings, defaults = _split_format(field_changes(old_settings, new_settings), versions_differ)
    for change in settings:
        changes.append(
            Change("changed", "setting", change.path, f"flow setting {change.path}", (change.path,), (change,))
        )
    for change in defaults:
        format_.append(
            Change("changed", "setting", change.path, f"flow setting {change.path}", (change.path,), (change,))
        )
    for name in [*a.parameters, *(n for n in b.parameters if n not in a.parameters)]:
        old, new = a.parameters.get(name), b.parameters.get(name)
        if old == new:
            continue
        label = f"parameter {name}"
        if old is None or new is None:
            changes.append(Change("added" if old is None else "removed", "parameter", name, label))
        else:
            fields = field_changes(old, new)
            changes.append(Change("changed", "parameter", name, label, tuple(f.path for f in fields), tuple(fields)))
    return changes, format_


def diff_flows(a: CanonicalFlow, b: CanonicalFlow) -> FlowDiff:
    """What turns flow ``a`` into flow ``b``."""
    versions_differ = a.head.get("flowfile_version") != b.head.get("flowfile_version")
    flow, flow_format = _flow(a, b, versions_differ)
    steps, step_layout, step_format = _steps(a, b, versions_differ)
    groups, group_layout = _boxes("group", a.groups, b.groups, GROUP_LAYOUT, "name")
    comments, comment_layout = _boxes("comment", a.comments, b.comments, COMMENT_LAYOUT, "text")
    return FlowDiff(
        changes=tuple(flow + steps + groups + comments),
        layout=tuple(step_layout + group_layout + comment_layout),
        format=tuple(flow_format + step_format),
    )


def diff(a: Mapping, b: Mapping) -> FlowDiff:
    """What turns document ``a`` into document ``b``, each read once (:func:`shared.flow_merge.canonical.canonical`)."""
    flow_a, issues_a = canonical(a)
    flow_b, issues_b = canonical(b)
    result = diff_flows(flow_a, flow_b)
    return FlowDiff(result.changes, result.layout, result.format, tuple(issues_a + issues_b))
