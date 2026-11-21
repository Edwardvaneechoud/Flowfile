"""The three-way merge of a schedule file: flat records matched by id, merged field by field.

A project writes a flow's schedules as one file (``schedules/<flow>.yaml``)::

    flow_uuid: ...
    schedules:
    - {id: <schedule uuid>, name, enabled, schedule_type, interval_seconds, cron_expression, cron_timezone, ...}

Two changes to different fields of one schedule combine (one side moves it to 03:00, the other pauses it). The same
field changed differently, a schedule removed on one side and changed on the other, or one id added differently on
both, is a conflict on that schedule, kept with each side's entry. A schedule without an id (a file written by hand)
is matched on what it is: its type, interval, cron expression, time zone, name and backend.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from shared.flow_merge.canonical import Issue
from shared.flow_merge.diff import FlowDiff
from shared.flow_merge.merge import Conflict, IdMap, MergeResult

_SIGNATURE = ("schedule_type", "interval_seconds", "cron_expression", "cron_timezone", "name", "backend")
_ABSENT: Any = object()


def is_records(document: Any) -> bool:
    """A schedule file: a ``schedules`` list and no ``nodes``."""
    return isinstance(document, dict) and "schedules" in document and "nodes" not in document


def _key(entry: Mapping) -> str:
    return str(entry["id"]) if entry.get("id") else "sig:" + "|".join(str(entry.get(f)) for f in _SIGNATURE)


def _by_key(document: Mapping | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for entry in (document or {}).get("schedules") or []:
        if isinstance(entry, dict):
            out.setdefault(_key(entry), entry)
    return out


def _label(entry: Mapping) -> str:
    when = entry.get("cron_expression") or (
        f"every {entry.get('interval_seconds')} s" if entry.get("interval_seconds") else ""
    )
    name = entry.get("name") or entry.get("id") or "schedule"
    return f"schedule {name}" + (f" ({when})" if when else "")


def merge_records(base: Mapping | None, ours: Mapping, theirs: Mapping) -> MergeResult:
    base_entries, our_entries, their_entries = _by_key(base), _by_key(ours), _by_key(theirs)
    conflicts: list[Conflict] = []
    warnings: list[Issue] = []
    merged: list[dict] = []
    for key in dict.fromkeys([*our_entries, *their_entries, *base_entries]):
        b, o, t = base_entries.get(key), our_entries.get(key), their_entries.get(key)
        label = _label(o or t or b)
        if o is not None and t is not None:
            entry, contested = {}, []
            for field in [*o, *(f for f in t if f not in o)]:
                bv = (b or {}).get(field, _ABSENT)
                ov, tv = o.get(field, _ABSENT), t.get(field, _ABSENT)
                if ov == bv:
                    value = tv
                elif tv == bv or tv == ov:
                    value = ov
                else:
                    value = ov
                    contested.append(field)
                if value is not _ABSENT:
                    entry[field] = copy.deepcopy(value)
            if contested:
                conflicts.append(
                    Conflict(
                        f"schedule:{key}",
                        "both_changed",
                        label,
                        f"Both sides changed {', '.join(contested)} of {label}.",
                        base=copy.deepcopy(b),
                        ours=copy.deepcopy(o),
                        theirs=copy.deepcopy(t),
                    )
                )
            merged.append(entry)
        elif b is None:
            merged.append(copy.deepcopy(o if o is not None else t))
        elif o is None and t is None:
            continue
        else:
            kept = o if o is not None else t
            if kept != b:
                gone = "theirs" if o is not None else "ours"
                conflicts.append(
                    Conflict(
                        f"schedule:{key}",
                        "delete_vs_change",
                        label,
                        f"{label} was removed on {gone} and changed on the other side.",
                        base=copy.deepcopy(b),
                        ours=copy.deepcopy(o),
                        theirs=copy.deepcopy(t),
                    )
                )
                if o is not None:
                    merged.append(copy.deepcopy(o))
    document: dict = {}
    for field in [*ours, *(f for f in theirs if f not in ours)]:
        if field == "schedules":
            document[field] = merged
            continue
        bv, ov, tv = (base or {}).get(field, _ABSENT), ours.get(field, _ABSENT), theirs.get(field, _ABSENT)
        value = tv if ov == bv else ov
        if ov != bv and tv not in (bv, ov):
            warnings.append(Issue("field_kept", f"Both sides changed {field}; ours is kept."))
        if value is not _ABSENT:
            document[field] = copy.deepcopy(value)
    document.setdefault("schedules", merged)
    return MergeResult(document, FlowDiff(), tuple(conflicts), tuple(warnings), IdMap())


def take_schedule(merged: dict, key: str, entry: Mapping | None) -> None:
    """Settle the conflict on schedule ``key`` with ``entry`` (``None`` removes it), in place."""
    schedules = merged.setdefault("schedules", [])
    index = next((i for i, e in enumerate(schedules) if _key(e) == key), None)
    if entry is None:
        if index is not None:
            del schedules[index]
    elif index is None:
        schedules.append(copy.deepcopy(dict(entry)))
    else:
        schedules[index] = copy.deepcopy(dict(entry))
