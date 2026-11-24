"""A pull whose branch moved on both sides: the project's files merged in process, nothing applied until settled.

Each managed file either side changed since the merge base is merged on its own: a flow step by step
(``shared.flow_merge.merge``), a schedule file field by field (``merge_records``), any other file only when one side
changed it. Two files at one path whose ``flow_uuid`` differs are two different objects that took one name (each side
made a flow of that name), never merged: the person keeps one of them whole. The remote's copy keeps its ids
(``keep_ids="theirs"``): it is what others already have, so only this copy's new steps are renumbered. What could not
be merged waits in a state file under ``.git`` (never in the working tree, which still holds this branch's last
version) until the person settles every item; the caller then commits the merge with both parents and applies it as
one import. Files outside the managed set are not merged here (``git_ops.unmanaged_changes``).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from shared.flow_merge.conflict import conflict_document, is_conflicted, resolve
from shared.flow_merge.diff import MISSING, field_changes
from shared.flow_merge.merge import merge
from shared.flow_merge.records import is_records, merge_records
from shared.flow_merge.render import _field, _value, side_lines
from shared.flow_merge.yaml_io import dump, load

STATE_NAME = "flowfile-merge.json"


@dataclass
class TreeMerge:
    """What merging two trees gives: ``changes`` to make on top of ours (``None`` deletes the file), the flows
    still holding step conflicts (``flows``: path to the conflict wrapper), the files only a person can choose
    between (``files``: path to the two texts) and the merges' warnings."""

    changes: dict[str, str | None] = field(default_factory=dict)
    flows: dict[str, dict] = field(default_factory=dict)
    files: dict[str, dict] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.flows and not self.files


def _document(text: str | None):
    if text is None:
        return None
    data = load(text)
    return data if isinstance(data, dict) else None


def _merge_documents(path: str, base: str | None, ours: str, theirs: str, sides: dict, out: TreeMerge) -> None:
    try:
        b, o, t = _document(base), _document(ours), _document(theirs)
    except ValueError:
        o = t = None
    if o is None or t is None or is_conflicted(o) or is_conflicted(t) or _other_object(o, t):
        out.files[path] = {"ours": ours, "theirs": theirs}
        return
    shown = None
    if is_records(o) or is_records(t):
        result = merge_records(b, o, t)
    elif "nodes" in o and "nodes" in t:
        result = merge(b or {"nodes": []}, o, t, keep_ids="theirs")
        # keep_ids="theirs" leaves the remote's step in the document, which is what the editor's canvas shows.
        shown = "theirs"
    else:
        out.files[path] = {"ours": ours, "theirs": theirs}
        return
    out.warnings += [f"{path}: {w.message}" for w in result.warnings]
    if result.clean:
        out.changes[path] = dump(result.document, "project")
    else:
        out.flows[path] = conflict_document(result, sides, shown=shown)


def _other_object(ours: dict, theirs: dict) -> bool:
    """Both sides name the object they hold and the names differ: two flows (or their schedules) that took one path."""
    o, t = ours.get("flow_uuid"), theirs.get("flow_uuid")
    return bool(o) and bool(t) and o != t


def merge_trees(base: dict[str, str], ours: dict[str, str], theirs: dict[str, str], sides: dict) -> TreeMerge:
    """Merge ``theirs`` into ``ours`` against ``base``, each ``{path: text}`` of managed files."""
    out = TreeMerge()
    for path in sorted({*base, *ours, *theirs}):
        b, o, t = base.get(path), ours.get(path), theirs.get(path)
        if o == t or t == b:
            continue
        if o == b:
            out.changes[path] = t
        elif o is None or t is None:
            out.files[path] = {"ours": o, "theirs": t}
        else:
            _merge_documents(path, b, o, t, sides, out)
    return out


def state_path(root: Path) -> Path:
    return Path(root) / ".git" / STATE_NAME


def load_state(root: Path) -> dict | None:
    path = state_path(root)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(root: Path, state: dict) -> None:
    path = state_path(root)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".flowfile-merge-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, path)


def clear_state(root: Path) -> None:
    state_path(root).unlink(missing_ok=True)


def new_state(
    branch: str, ours: str, theirs: str, merged: TreeMerge, sides: dict, unmanaged: dict | None = None
) -> dict:
    """The waiting merge as the state file holds it; ``unmanaged`` is what the merge commit takes from theirs among the
    files outside the managed set (``git_ops.unmanaged_changes``), kept so a later conclusion commits it too."""
    return {
        "branch": branch,
        "ours": ours,
        "theirs": theirs,
        "sides": sides,
        "changes": merged.changes,
        "flows": merged.flows,
        "files": merged.files,
        "warnings": merged.warnings,
        "unmanaged": unmanaged or {},
    }


def open_items(state: dict) -> list[dict]:
    """Every item left to settle: a flow's step conflicts one row each, a whole file one row."""
    sides = state.get("sides") or {}
    rows = []
    for path, wrapper in sorted((state.get("flows") or {}).items()):
        for conflict in wrapper.get("conflicts") or []:
            kind, _, name = conflict["key"].partition(":")
            step = kind == "step"
            rows.append(
                {
                    "path": path,
                    "key": conflict["key"],
                    "kind": conflict["kind"],
                    "label": conflict.get("label") or conflict["key"],
                    "message": conflict.get("message") or "",
                    "step_id": int(name) if step else None,
                    "ours_label": sides.get("ours", "ours"),
                    "theirs_label": sides.get("theirs", "theirs"),
                    "ours": _side(kind, conflict.get("base"), conflict.get("ours"), conflict.get("theirs")),
                    "theirs": _side(kind, conflict.get("base"), conflict.get("theirs"), conflict.get("ours")),
                }
            )
    for path, texts in sorted((state.get("files") or {}).items()):
        rows.append(
            {
                "path": path,
                "key": None,
                "kind": "file",
                "label": path,
                "message": _file_message(texts),
                "step_id": None,
                "ours_label": sides.get("ours", "ours"),
                "theirs_label": sides.get("theirs", "theirs"),
                "ours": _file_side(texts["ours"]),
                "theirs": _file_side(texts["theirs"]),
            }
        )
    return rows


def _parsed(text: str | None) -> dict | None:
    try:
        return _document(text)
    except ValueError:
        return None


def _file_side(text: str | None) -> list[str]:
    if text is None:
        return ["removed the file"]
    document = _parsed(text) or {}
    if document.get("flow_uuid"):
        name = document.get("catalog_name") or document.get("flowfile_name") or "the flow"
        return [f"holds {name} ({str(document['flow_uuid'])[:8]})"]
    return ["changed the file"]


def _side(kind: str, base: dict | None, side: dict | None, other: dict | None) -> list[str]:
    """What one side did to the conflicted item, one line per value: a step's settings and inputs, or the fields of a
    schedule or parameter entry. An entry both sides added is told by the values that differ from the other side's."""
    if kind == "step":
        return side_lines(base, side)
    noun = kind or "entry"
    if side is None:
        return [f"removed the {noun}"]
    if base is None:
        added = [c for c in field_changes(other or {}, side) if c.after is not MISSING]
        return [f"added the {noun}", *(f"{c.path} = {_value(c.after)}" for c in added)]
    return [_field(c) for c in field_changes(base, side)] or [f"no change to the {noun}"]


def _file_message(texts: dict) -> str:
    if texts["ours"] is None:
        return "This copy removed the file; the remote changed it."
    if texts["theirs"] is None:
        return "The remote removed the file; this copy changed it."
    ours, theirs = _parsed(texts["ours"]), _parsed(texts["theirs"])
    if ours is not None and theirs is not None and _other_object(ours, theirs):
        return "This copy and the remote each hold a different flow under this name; keep one of them whole."
    return "Both changed the file; keep one of them whole."


def settle(state: dict, path: str, take: str, key: str | None = None) -> dict:
    """``state`` with one item settled: step ``key`` of flow ``path``, or the whole file ``path``. A flow with no
    conflict left becomes a plain change. Unknown items are a ``KeyError``."""
    if take not in ("ours", "theirs"):
        raise ValueError(f"take must be 'ours' or 'theirs', got {take!r}")
    flows, files = state.get("flows") or {}, state.get("files") or {}
    if key is not None:
        wrapper = flows[path]
        settled = resolve(wrapper, {key: take})
        if is_conflicted(settled):
            flows[path] = settled
        else:
            del flows[path]
            state["changes"][path] = dump(settled, "project")
    else:
        texts = files.pop(path)
        state["changes"][path] = texts[take]
    return state


def settle_flow_document(state: dict, path: str, document: dict, wrapper: dict | None) -> dict:
    """``state`` after the editor changed flow ``path``: ``document`` is what the canvas holds, ``wrapper`` the
    conflicts it still holds (``None`` once settled)."""
    flows = state.get("flows") or {}
    if path not in flows:
        raise KeyError(path)
    if wrapper:
        flows[path] = {**wrapper, "merged": document}
    else:
        del flows[path]
        state["changes"][path] = dump(document, "project")
    return state


def is_settled(state: dict) -> bool:
    return not state.get("flows") and not state.get("files")
