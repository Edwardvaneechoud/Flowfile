"""Collision-free positions for nodes added to a canvas that already holds nodes, groups and comments.

Used when nodes are created from code (the canvas notebook's push) rather than dropped by hand.
Existing nodes never move: a new node takes the free slot nearest to where its neighbours want it.
"""

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

X_SPACING = 250
Y_SPACING = 100
FALLBACK = (50, 50)
NODE_WIDTH, NODE_HEIGHT = 180, 80
MAX_ROWS = 50

Point = tuple[float, float]


@dataclass(frozen=True, slots=True)
class Box:
    x: float
    y: float
    width: float
    height: float

    @property
    def bottom(self) -> float:
        return self.y + self.height

    def overlaps(self, other: "Box") -> bool:
        """True when the interiors intersect; boxes that only touch do not overlap."""
        return (
            self.x < other.x + other.width
            and other.x < self.x + self.width
            and self.y < other.bottom
            and other.y < self.bottom
        )


def node_box(x: float, y: float) -> Box:
    """The space a node takes up: a nominal size, since the backend never sees rendered nodes."""
    return Box(x, y, NODE_WIDTH, NODE_HEIGHT)


def _snap(value: float) -> int:
    return math.floor(value + 0.5)


def _row_offsets() -> Iterable[int]:
    yield 0
    for row in range(1, MAX_ROWS + 1):
        yield row
        yield -row


class Placer:
    """Hands out positions for new nodes, one at a time or as a batch, without covering anything.

    ``positions`` are the nodes already on the canvas and ``obstacles`` the group and comment boxes.
    A placed node is recorded, so later placements anchor on it and avoid it. Nodes with no
    positioned neighbour start a band below the content that was on the canvas when the placer
    was made.
    """

    def __init__(self, positions: Mapping[int, Point] | None = None, obstacles: Iterable[Box] = ()) -> None:
        self._positions: dict[int, Point] = {
            node_id: (float(x), float(y)) for node_id, (x, y) in (positions or {}).items()
        }
        self._obstacles = list(obstacles)
        self._band = self._band_origin()

    def position(self, node_id: int) -> Point | None:
        return self._positions.get(node_id)

    def reserve(self, node_id: int, x: float, y: float) -> None:
        """Records a node whose position is already decided."""
        self._positions[node_id] = (float(x), float(y))

    def release(self, node_id: int) -> None:
        """Frees the space of a node that leaves the canvas."""
        self._positions.pop(node_id, None)

    def place(self, node_id: int, inputs: Sequence[int] = (), outputs: Sequence[int] = ()) -> tuple[int, int]:
        """The free slot nearest to the node's preferred spot, recorded for the placements after it.

        Preferred is one column right of its right-most positioned input at their median height,
        else one column left of its left-most positioned output, else the band below the content.
        From there the same column is searched row by row, nearest first and below before above.
        """
        self._positions.pop(node_id, None)
        x, y = self._preferred(inputs, outputs)
        spot = self._nearest_free(_snap(x), _snap(y))
        self._positions[node_id] = (float(spot[0]), float(spot[1]))
        return spot

    def place_all(
        self,
        order: Sequence[int],
        inputs: Mapping[int, Sequence[int]],
        outputs: Mapping[int, Sequence[int]],
    ) -> dict[int, tuple[int, int]]:
        """Places a batch given in topological ``order``.

        Each pass places every node with a positioned input or output, so a new source feeding a
        node that sits next to the existing canvas lands beside it rather than in the band. When a
        pass places nothing, the first remaining node starts a new subgraph in the band.
        """
        placed: dict[int, tuple[int, int]] = {}
        pending = list(order)
        while pending:
            anchored = [
                node_id
                for node_id in pending
                if self._known(inputs.get(node_id, ())) or self._known(outputs.get(node_id, ()))
            ]
            for node_id in anchored or pending[:1]:
                placed[node_id] = self.place(node_id, inputs.get(node_id, ()), outputs.get(node_id, ()))
            pending = [node_id for node_id in pending if node_id not in placed]
        return placed

    def _known(self, node_ids: Iterable[int]) -> list[Point]:
        return [self._positions[node_id] for node_id in dict.fromkeys(node_ids) if node_id in self._positions]

    def _preferred(self, inputs: Sequence[int], outputs: Sequence[int]) -> Point:
        sources = self._known(inputs)
        if sources:
            return max(x for x, _ in sources) + X_SPACING, statistics.median(y for _, y in sources)
        targets = self._known(outputs)
        if targets:
            return min(x for x, _ in targets) - X_SPACING, statistics.median(y for _, y in targets)
        return self._band

    def _boxes(self) -> list[Box]:
        return [node_box(x, y) for x, y in self._positions.values()] + self._obstacles

    def _band_origin(self) -> Point:
        boxes = self._boxes()
        if not boxes:
            return FALLBACK
        return min(box.x for box in boxes), max(box.bottom for box in boxes) + Y_SPACING

    def _nearest_free(self, x: int, y: int) -> tuple[int, int]:
        boxes = self._boxes()
        for row in _row_offsets():
            candidate_y = y + row * Y_SPACING
            if not any(node_box(x, candidate_y).overlaps(box) for box in boxes):
                return x, candidate_y
        return x, _snap(max(box.bottom for box in boxes) + Y_SPACING)


def placer_from_payload(flowfile_data: Mapping) -> Placer:
    """A placer over a save-format dump (``FlowGraph.get_flowfile_data().model_dump(mode="json")``)."""
    positions = {
        node["id"]: (node.get("x_position") or 0, node.get("y_position") or 0)
        for node in flowfile_data.get("nodes") or []
    }
    obstacles = [
        Box(item.get("x_position") or 0, item.get("y_position") or 0, item.get("width") or 0, item.get("height") or 0)
        for key in ("groups", "comments")
        for item in flowfile_data.get(key) or []
    ]
    return Placer(positions, obstacles)
