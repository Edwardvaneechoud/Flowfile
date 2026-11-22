"""Rewrite the ids a saved flow document uses for its steps, groups and comments (pure, no FlowGraph).

The canvas notebook's push relabels a clean run onto provenance ids (:func:`provenance_mapping`, :func:`relabel`); a
merge renumbers the steps, groups and comments one side added under an id the other side also used.
"""

from __future__ import annotations

import copy
from collections import defaultdict, deque
from typing import Any

_NODE_ID_LIST_FIELDS = ("input_ids", "outputs")
_NODE_ID_FIELDS = ("id", "left_input_id", "right_input_id")
_SETTING_ID_KEYS = frozenset({"node_id", "depending_on_id", "upstream_node_id", "upstream_train_node_id"})
_SETTING_ID_LIST_KEYS = frozenset({"depending_on_ids"})


def _relabel_settings(value: Any, mapping: dict[int, int]) -> Any:
    """Rewrite node-id keys anywhere inside a node's ``setting_input`` dict, in place."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _SETTING_ID_KEYS and isinstance(item, int):
                value[key] = mapping.get(item, item)
            elif key in _SETTING_ID_LIST_KEYS and isinstance(item, list):
                value[key] = [mapping.get(i, i) for i in item]
            else:
                _relabel_settings(item, mapping)
    elif isinstance(value, list):
        for item in value:
            _relabel_settings(item, mapping)
    return value


def relabel(flowfile_data: dict, mapping: dict[int, int]) -> dict:
    """Return a deep copy of a save-format payload with every node id rewritten via ``mapping``.

    Rewrites each node's ``id``, ``left_input_id``, ``right_input_id``, ``input_ids``,
    ``outputs``, ``input_connections[].from_id`` and the node-id keys of ``setting_input``
    (``node_id``, ``depending_on_id(s)``, ``upstream_node_id``, ``upstream_train_node_id``).
    ``output_handles`` is parallel to ``outputs`` and holds handle names, so it is kept as is.
    Ids absent from ``mapping`` are unchanged; the mapping must be injective.
    """
    data = copy.deepcopy(flowfile_data)
    for node in data.get("nodes") or []:
        for field in _NODE_ID_FIELDS:
            if node.get(field) is not None:
                node[field] = mapping.get(node[field], node[field])
        for field in _NODE_ID_LIST_FIELDS:
            if node.get(field):
                node[field] = [mapping.get(i, i) for i in node[field]]
        for conn in node.get("input_connections") or []:
            conn["from_id"] = mapping.get(conn["from_id"], conn["from_id"])
        if isinstance(node.get("setting_input"), dict):
            _relabel_settings(node["setting_input"], mapping)
    return data


def relabel_groups(flowfile_data: dict, mapping: dict[int, int]) -> dict:
    """Return a deep copy with every group id rewritten via ``mapping``.

    A group is named by its own ``id``, by a child group's ``parent_group_id`` and by a node's ``group_id``;
    a group has no member list. Ids absent from ``mapping`` are unchanged; the mapping must be injective.
    """
    data = copy.deepcopy(flowfile_data)
    for group in data.get("groups") or []:
        for field in ("id", "parent_group_id"):
            if group.get(field) is not None:
                group[field] = mapping.get(group[field], group[field])
    for node in data.get("nodes") or []:
        if node.get("group_id") is not None:
            node["group_id"] = mapping.get(node["group_id"], node["group_id"])
    return data


def relabel_comments(flowfile_data: dict, mapping: dict[int, int]) -> dict:
    """Return a deep copy with every comment id rewritten via ``mapping``; nothing else names a comment."""
    data = copy.deepcopy(flowfile_data)
    for comment in data.get("comments") or []:
        if comment.get("id") is not None:
            comment["id"] = mapping.get(comment["id"], comment["id"])
    return data


def provenance_mapping(
    created: list[tuple[str, str, int]],
    provenance: dict[str, list[tuple[str, int]]],
    ceiling: int,
) -> dict[int, int]:
    """Map every newly created node id onto its canvas id, or a fresh id above ``ceiling``.

    ``created`` is ``(cell_id, node_type, new_id)`` in creation order; ``provenance`` is
    ``cell_id -> [(node_type, canvas_id)]`` in the order the cell renders them. Within a
    cell, the k-th created node of a type takes the k-th canvas id of that type. Unmatched
    nodes get increasing ids above ``ceiling`` (and above every provenance id, so a fresh
    id can never collide with a matched one).
    """
    queues: dict[tuple[str, str], deque[int]] = defaultdict(deque)
    for cell_id, entries in provenance.items():
        for node_type, canvas_id in entries:
            queues[(cell_id, node_type)].append(canvas_id)
    next_id = max([ceiling, *(cid for entries in provenance.values() for _, cid in entries)]) + 1
    mapping: dict[int, int] = {}
    for cell_id, node_type, new_id in created:
        queue = queues.get((cell_id, node_type))
        if queue:
            mapping[new_id] = queue.popleft()
        else:
            mapping[new_id] = next_id
            next_id += 1
    return mapping
