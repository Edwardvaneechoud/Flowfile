"""Flow documents and their differences as text a person reviews: plain, Markdown or JSON.

:func:`show` is one flow as git's ``textconv`` of a ``*.flow.yaml`` file: its steps in id order with their inputs and
settings, nothing of layout or noise, so a host's line diff of two versions reads as a diff of steps.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from typing import Any, Literal

from shared.flow_merge.canonical import canonical
from shared.flow_merge.diff import MISSING, Change, FieldChange, FlowDiff, step_settings
from shared.flow_merge.yaml_io import dump

Format = Literal["text", "md", "json"]
_MAX_VALUE = 80


def _value(value: Any) -> str:
    if value is MISSING:
        return "absent"
    text = json.dumps(value, ensure_ascii=False, default=str) if not isinstance(value, str) else repr(value)
    return text if len(text) <= _MAX_VALUE else text[: _MAX_VALUE - 1] + "…"


def _field(change: FieldChange) -> str:
    if change.before is MISSING:
        return f"{change.path} set to {_value(change.after)}"
    if change.after is MISSING:
        return f"{change.path} removed (was {_value(change.before)})"
    return f"{change.path}: {_value(change.before)} → {_value(change.after)}"


_NOUNS = (("step", "step"), ("parameter", "parameter"), ("setting", "flow setting"), ("flow", "flow field"))


def summary(result: FlowDiff) -> str:
    """``3 steps (2 changed, 1 added); 1 parameter (1 changed)``, or ``no change``."""
    parts = []
    for item, noun in _NOUNS:
        counts = Counter(c.kind for c in result.changes if c.item == item)
        if counts:
            total = sum(counts.values())
            detail = ", ".join(f"{counts[k]} {k}" for k in ("changed", "added", "removed") if counts[k])
            parts.append(f"{total} {noun}{'s' if total != 1 else ''} ({detail})")
    others = sum(1 for c in result.changes if c.item in ("group", "comment"))
    if others:
        parts.append(f"{others} group or comment change{'s' if others != 1 else ''}")
    return "; ".join(parts) or "no change"


def _lines(change: Change) -> list[str]:
    head = f"{change.kind} {change.label}"
    if change.kind != "changed" or not change.fields:
        return [head]
    return [head, *(f"  {_field(f)}" for f in change.fields)]


def render_diff(result: FlowDiff, fmt: Format = "text", *, name: str | None = None, layout: bool = False) -> str:
    """``result`` for a review: the summary, then each change with the values that differ."""
    if fmt == "json":
        return json.dumps(to_json(result, name=name), indent=2, ensure_ascii=False, default=str)
    title = f"{name}: {summary(result)}" if name else summary(result)
    moved = len(result.layout)
    moved_text = f"{moved} layout change{'s' if moved != 1 else ''}"
    if fmt == "md":
        lines = [f"**{title}**" if name else title, ""]
        sections = [(None, result.changes)] + ([(f"Layout ({moved_text})", result.layout)] if layout and moved else [])
        for heading, changes in sections:
            if heading:
                lines += ["", f"_{heading}_", ""]
            for change in changes:
                first, *rest = _lines(change)
                lines.append(f"- {first}")
                lines += [f"  - `{line.strip()}`" for line in rest]
        if moved and not layout:
            lines.append(f"- _{moved_text} not shown_")
        return "\n".join(lines).rstrip() + "\n"
    lines = [title]
    for change in result.changes:
        lines += [f"  {line}" for line in _lines(change)]
    if moved and layout:
        lines.append(f"  layout ({moved_text}):")
        for change in result.layout:
            lines += [f"    {line}" for line in _lines(change)]
    elif moved:
        lines.append(f"  ({moved_text} not shown)")
    return "\n".join(lines) + "\n"


def side_lines(base: dict | None, side: dict | None) -> list[str]:
    """What one side of a step conflict did to the step: its changed settings and inputs, one line each."""
    from shared.flow_merge.diff import field_changes

    base_step = (base or {}).get("step")
    step = (side or {}).get("step")
    if step is None:
        return ["removed the step"]
    if base_step is None:
        return ["added the step"]
    fields = field_changes(step_settings(base_step), step_settings(step), "settings")
    fields += field_changes((base or {}).get("inputs") or {}, (side or {}).get("inputs") or {}, "inputs")
    return [_field(f) for f in fields] or ["no change to its settings or inputs"]


def review_items(result: FlowDiff) -> list[dict]:
    """The changes of ``result`` for a list on a page: ``{kind, label, details}``, details one line per value."""
    return [{"kind": c.kind, "label": c.label, "details": [_field(f) for f in c.fields]} for c in result.changes]


def _field_json(change: FieldChange) -> dict:
    return {
        "path": change.path,
        "before": None if change.before is MISSING else change.before,
        "after": None if change.after is MISSING else change.after,
        "before_absent": change.before is MISSING,
        "after_absent": change.after is MISSING,
    }


def _change_json(change: Change) -> dict:
    return {
        "kind": change.kind,
        "item": change.item,
        "id": change.id,
        "label": change.label,
        "aspects": list(change.aspects),
        "fields": [_field_json(f) for f in change.fields],
    }


def to_json(result: FlowDiff, *, name: str | None = None) -> dict:
    return {
        "name": name,
        "summary": summary(result),
        "changes": [_change_json(c) for c in result.changes],
        "layout": [_change_json(c) for c in result.layout],
        "format": [_change_json(c) for c in result.format],
        "issues": [{"code": i.code, "message": i.message, "ids": list(i.ids)} for i in result.issues],
    }


def show(document: Mapping) -> str:
    """One flow as review text: name, parameters, then each step with its inputs and settings."""
    flow, _ = canonical(document)
    lines = [f"flow {flow.head.get('flowfile_name') or flow.head.get('catalog_name') or ''}".rstrip()]
    for name, parameter in flow.parameters.items():
        lines.append(f"parameter {name} = {_value(parameter.get('default_value'))}")
    for sid in sorted(flow.steps):
        node = flow.steps[sid]
        lines.append("")
        title = f"step {sid} {node.get('type')}"
        if node.get("node_reference"):
            title += f" ({node['node_reference']})"
        lines.append(title)
        if node.get("description"):
            lines.append(f"  description: {node['description']}")
        for handle, edges in flow.inputs.get(sid, {}).items():
            sources = ", ".join(f"{s}" + ("" if h == "output-0" else f".{h}") for s, h in edges)
            lines.append(f"  {handle} <- {sources}")
        settings = step_settings(node)
        if settings not in (None, {}):
            body = dump(settings).rstrip("\n").split("\n")
            lines += ["  settings:", *(f"    {line}" for line in body)]
    for gid, group in sorted(flow.groups.items()):
        members = [str(s) for s, n in sorted(flow.steps.items()) if n.get("group_id") == gid]
        lines += ["", f"group {gid} {group.get('name') or ''}".rstrip(), f"  steps: {', '.join(members) or 'none'}"]
    for cid, comment in sorted(flow.comments.items()):
        lines += ["", f"comment {cid}", *(f"  {line}" for line in str(comment.get("text") or "").split("\n"))]
    return "\n".join(lines) + "\n"
