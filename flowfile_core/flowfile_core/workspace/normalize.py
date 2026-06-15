"""Determinism rules: canonical YAML + flow normalization.

The load-bearing requirement of the whole workspace feature: *unchanged DB state
must produce a byte-identical export*. Every non-deterministic source has a rule
here.

| source                         | rule                                            |
|--------------------------------|-------------------------------------------------|
| volatile ``flowfile_id``       | normalized to a constant; identity is flow_uuid |
| canvas x/y, group geometry     | split into ``<flow>.layout.yaml``               |
| absolute data/flow paths       | tokenized to ``${user_data}`` / ``${storage}``  |
| dict key & list order          | single canonical dumper (sorted keys)           |
| timestamps / runtime columns   | never serialized by the exporters               |

The canonical dumper sorts keys so the export is stable regardless of the key
order in the source dict (e.g. a hand-edited or older-version flow file).
"""

from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
from typing import Any

import yaml

# Constant written in place of the runtime ``flowfile_id``. The stable identity
# of a flow is its ``flow_uuid`` (carried alongside in the project file); the
# integer id is an in-memory/runtime handle that is reassigned on apply.
NORMALIZED_FLOW_ID = 0

# setting_input keys whose string values are filesystem paths we tokenize for
# portability. Only absolute paths under a known root are rewritten; anything
# else is left verbatim (still deterministic on a given machine).
_PATH_KEYS = frozenset(
    {"abs_file_path", "path", "directory", "file_path", "flow_path", "save_location"}
)


class _CanonicalDumper(yaml.SafeDumper):
    """SafeDumper subclass so we can tune representers without global state."""


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.Node:
    # Use block style for multi-line strings so they diff cleanly.
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_CanonicalDumper.add_representer(str, _str_representer)


def canonical_yaml_dump(data: Any) -> str:
    """Serialize to canonical YAML: sorted keys, block style, trailing newline."""
    text = yaml.dump(
        data,
        Dumper=_CanonicalDumper,
        sort_keys=True,
        default_flow_style=False,
        allow_unicode=True,
        width=4096,
        indent=2,
    )
    if not text.endswith("\n"):
        text += "\n"
    return text


def canonical_yaml_load(text: str) -> Any:
    return yaml.safe_load(text)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class PathTokenizer:
    """Bidirectional rewriter between absolute paths and ``${token}`` paths.

    Roots are matched longest-first so the most specific token wins when roots
    nest (e.g. ``flows_directory`` under ``user_data_directory``).
    """

    def __init__(self, roots: list[tuple[str, str]]) -> None:
        # roots: list of (token, absolute_path)
        cleaned = [(token, str(Path(p))) for token, p in roots if p]
        self._roots = sorted(cleaned, key=lambda kv: len(kv[1]), reverse=True)

    def to_token(self, value: str) -> str:
        for token, root in self._roots:
            r = root.rstrip("/\\")
            if value == r:
                return token
            if value.startswith(r + os.sep) or value.startswith(r + "/"):
                rest = value[len(r) :].lstrip("/\\").replace(os.sep, "/")
                return f"{token}/{rest}"
        return value

    def from_token(self, value: str) -> str:
        for token, root in self._roots:
            if value == token:
                return str(Path(root))
            if value.startswith(token + "/"):
                rest = value[len(token) + 1 :]
                return str(Path(root).joinpath(*rest.split("/")))
        return value


def _walk_paths(obj: Any, fn) -> Any:
    """Recursively apply ``fn`` to string values stored under path-like keys."""
    if isinstance(obj, dict):
        out = {}
        for key, val in obj.items():
            if key in _PATH_KEYS and isinstance(val, str) and val:
                out[key] = fn(val)
            else:
                out[key] = _walk_paths(val, fn)
        return out
    if isinstance(obj, list):
        return [_walk_paths(item, fn) for item in obj]
    return obj


# Group geometry fields that are cosmetic (canvas) and belong in the layout file.
_GROUP_GEOMETRY_KEYS = ("x_position", "y_position", "width", "height", "collapsed")
_GROUP_GEOMETRY_RESET = {
    "x_position": 0.0,
    "y_position": 0.0,
    "width": 0.0,
    "height": 0.0,
    "collapsed": False,
}


def normalize_flow(
    flow_data: dict, flow_uuid: str, tokenizer: PathTokenizer
) -> tuple[dict, dict]:
    """Split a ``FlowfileData`` dump into a deterministic flow doc + layout doc.

    Returns ``(flow_doc, layout_doc)``:

    * ``flow_doc`` is the logic file -- ``flow_uuid`` injected, ``flowfile_id``
      normalized, positions/geometry stripped, data paths tokenized.
    * ``layout_doc`` holds the cosmetic canvas coordinates keyed by node/group id.
    """
    data = copy.deepcopy(flow_data)
    data["flowfile_id"] = NORMALIZED_FLOW_ID

    layout_nodes: dict[int, dict[str, int]] = {}
    for node in data.get("nodes", []) or []:
        nid = node.get("id")
        layout_nodes[nid] = {
            "x_position": int(node.get("x_position") or 0),
            "y_position": int(node.get("y_position") or 0),
        }
        node["x_position"] = 0
        node["y_position"] = 0

    layout_groups: dict[int, dict[str, Any]] = {}
    for group in data.get("groups", []) or []:
        gid = group.get("id")
        layout_groups[gid] = {k: group.get(k) for k in _GROUP_GEOMETRY_KEYS}
        group.update(_GROUP_GEOMETRY_RESET)

    data = _walk_paths(data, tokenizer.to_token)

    flow_doc = {"flow_uuid": flow_uuid, **data}
    layout_doc = {"flow_uuid": flow_uuid, "nodes": layout_nodes, "groups": layout_groups}
    return flow_doc, layout_doc


def denormalize_flow(
    flow_doc: dict,
    layout_doc: dict | None,
    tokenizer: PathTokenizer,
    flow_id: int,
) -> dict:
    """Reverse :func:`normalize_flow` into a runtime ``FlowfileData`` dict.

    ``flow_uuid`` is removed (it is not a ``FlowfileData`` field), paths are
    re-absolutized for the local machine, the runtime ``flowfile_id`` is set, and
    canvas coordinates are merged back from the layout doc.
    """
    data = copy.deepcopy(flow_doc)
    data.pop("flow_uuid", None)
    data = _walk_paths(data, tokenizer.from_token)
    data["flowfile_id"] = flow_id

    if layout_doc:
        lnodes = layout_doc.get("nodes") or {}
        for node in data.get("nodes", []) or []:
            nid = node.get("id")
            pos = lnodes.get(nid)
            if pos is None:
                pos = lnodes.get(str(nid))
            if pos:
                node["x_position"] = pos.get("x_position", 0)
                node["y_position"] = pos.get("y_position", 0)
        lgroups = layout_doc.get("groups") or {}
        for group in data.get("groups", []) or []:
            gid = group.get("id")
            geo = lgroups.get(gid)
            if geo is None:
                geo = lgroups.get(str(gid))
            if geo:
                for key in _GROUP_GEOMETRY_KEYS:
                    if key in geo and geo[key] is not None:
                        group[key] = geo[key]
    return data


def default_path_tokenizer() -> PathTokenizer:
    """Tokenizer seeded from the runtime storage roots.

    Imported lazily so ``normalize`` stays import-light for pure unit tests.
    """
    from shared.storage_config import storage

    roots = [
        ("${flows}", str(storage.flows_directory)),
        ("${outputs}", str(storage.outputs_directory)),
        ("${uploads}", str(storage.uploads_directory)),
        ("${user_data}", str(storage.user_data_directory)),
        ("${storage}", str(storage.base_directory)),
        ("${home}", str(Path.home())),
    ]
    return PathTokenizer(roots)
