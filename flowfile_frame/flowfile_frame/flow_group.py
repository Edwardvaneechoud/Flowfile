"""``ff.FlowGroup``: a visual group on the canvas, built from Python.

A group is a labelled box around nodes, organisational only: it never changes execution or results.
``FlowGroup(...)`` builds nothing; the group is placed on a graph the first time a node joins it
(``frame.add_to_group(group)``, ``native_node.add_to_group(group)``), and a nested group places its
parent first. A group belongs to one graph; when frames are merged onto a combined graph (a join,
a concat, a native node over frames of two graphs) every group of the merged graphs follows
(:func:`rebind_groups`), so a group built before a join still names the same box after it.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, get_args

from flowfile_core.schemas.schemas import GroupColor

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

_COLORS: tuple[str, ...] = get_args(GroupColor)
_BOUND: weakref.WeakKeyDictionary[FlowGraph, weakref.WeakSet[FlowGroup]] = weakref.WeakKeyDictionary()
"""Every group bound to a graph, so a merge can move them onto the combined graph."""


class FlowGroup:
    """A visual group of canvas nodes: a named, optionally coloured box, optionally nested in another group.

    ``name`` is the box's label (the designer's default is ``"Group"``), ``color`` one of the designer's
    tints (``slate``, ``blue``, ``green``, ``amber``, ``rose``, ``violet``, ``cyan``; ``None`` is the default
    tint) and ``parent_group`` the group this one nests inside. Nodes join through
    ``frame.add_to_group(group)``, which returns the frame, so it chains.

    Example:
        >>> cleaning = ff.FlowGroup("Cleaning", color="blue")
        >>> prep = ff.FlowGroup("Prep", parent_group=cleaning)
        >>> df = ff.read_csv("orders.csv").add_to_group(cleaning)
        >>> df = df.filter(ff.col("amount") > 0).add_to_group(prep)
    """

    def __init__(self, name: str = "Group", *, color: GroupColor | None = None, parent_group: FlowGroup | None = None):
        from flowfile_frame.native import NativeNodeError

        if not isinstance(name, str) or not name.strip():
            raise NativeNodeError("FlowGroup needs a non-empty name")
        if color is not None and color not in _COLORS:
            raise NativeNodeError(f"FlowGroup color must be one of {list(_COLORS)}, got {color!r}")
        if parent_group is not None and not isinstance(parent_group, FlowGroup):
            raise NativeNodeError(f"parent_group must be a FlowGroup, got {type(parent_group).__name__}")
        self._name = name
        self._color: GroupColor | None = color
        self._parent = parent_group
        self._graph: FlowGraph | None = None
        self._id: int | None = None

    @property
    def name(self) -> str:
        """The box's label."""
        return self._name

    @property
    def color(self) -> GroupColor | None:
        """The box's tint, ``None`` for the designer's default."""
        return self._color

    @property
    def parent_group(self) -> FlowGroup | None:
        """The group this one nests inside, if any."""
        return self._parent

    @property
    def flow_graph(self) -> FlowGraph | None:
        """The graph the group is placed on; ``None`` until a node joins it."""
        return self._graph

    @property
    def id(self) -> int | None:
        """The group's id on its graph; ``None`` until a node joins it."""
        return self._id

    @property
    def node_ids(self) -> list[int]:
        """The ids of the nodes directly in this group (not those of nested groups)."""
        if self._graph is None or self._id is None:
            return []
        return list(self._graph._member_node_ids(self._id))

    def __repr__(self) -> str:
        placed = f", id={self._id}" if self._id is not None else ""
        parent = f", parent_group={self._parent!r}" if self._parent is not None else ""
        return f"FlowGroup({self._name!r}{placed}{parent})"

    def _bind(self, graph: FlowGraph) -> int:
        """Place the group (and its parents) on ``graph`` if not placed yet; return its id there."""
        from flowfile_frame.native import NativeNodeError

        if self._graph is not None:
            if self._graph is not graph:
                raise NativeNodeError(
                    f"{self!r} belongs to another graph; a group holds nodes of one graph (join or concat "
                    "the frames first, then add the result to the group)"
                )
            return self._id
        parent_id = self._parent._bind(graph) if self._parent is not None else None
        group = graph.create_group(self._name, [], color=self._color, parent_group_id=parent_id)
        self._graph, self._id = graph, group.id
        _BOUND.setdefault(graph, weakref.WeakSet()).add(self)
        return group.id

    def _add_node(self, graph: FlowGraph, node_id: int) -> None:
        graph.add_nodes_to_group(self._bind(graph), [node_id])


def rebind_groups(old_graphs, combined: FlowGraph, group_id_mapping: dict[tuple[int, int], int]) -> None:
    """Move every group bound to one of ``old_graphs`` onto ``combined`` under its new id."""
    for graph in old_graphs:
        for group in list(_BOUND.get(graph) or ()):
            new_id = group_id_mapping.get((graph.flow_id, group._id))
            if new_id is None:
                continue
            group._graph, group._id = combined, new_id
            _BOUND.setdefault(combined, weakref.WeakSet()).add(group)
        _BOUND.pop(graph, None)
