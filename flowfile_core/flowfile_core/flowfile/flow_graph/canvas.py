"""Canvas organisation of a FlowGraph: node groups, comments, positions and automatic layout.
Organisational only; the executor never reads any of it.
"""

from time import time
from typing import TYPE_CHECKING

from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout
from flowfile_core.flowfile.util.layout.placement import NODE_HEIGHT, NODE_WIDTH
from flowfile_core.schemas import schemas
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)

if TYPE_CHECKING:
    pass


class CanvasMixin(GraphMixinBase):
    def _next_group_id(self) -> int:
        """Allocate a monotonically increasing group id (never reuses a freed id this session)."""
        self._group_id_seq = max([self._group_id_seq, *self._groups]) + 1
        return self._group_id_seq

    def _member_node_ids(self, group_id: int) -> list[int]:
        """Derive a group's members by scanning node group_id (single source of truth)."""
        return [node.node_id for node in self.nodes if getattr(node.setting_input, "group_id", None) == group_id]

    def _set_node_group(self, node_id: int, group_id: int | None) -> None:
        self._note_graph_write()
        node = self.get_node(node_id)
        if node is not None and node.setting_input is not None and hasattr(node.setting_input, "group_id"):
            node.setting_input.group_id = group_id

    def _child_group_ids(self, group_id: int) -> list[int]:
        """Sub-groups whose immediate parent is this group."""
        return [gid for gid, g in self._groups.items() if g.parent_group_id == group_id]

    def _group_depth(self, group_id: int) -> int:
        """Nesting depth (0 = top-level); cycle-safe."""
        depth, seen, current = 0, set(), self._groups.get(group_id)
        while current is not None and current.parent_group_id is not None and current.id not in seen:
            seen.add(current.id)
            depth += 1
            current = self._groups.get(current.parent_group_id)
        return depth

    def _is_ancestor_group(self, ancestor_id: int, group_id: int) -> bool:
        """True if ancestor_id equals group_id or one of its ancestors (cycle-safe)."""
        seen, current = set(), self._groups.get(group_id)
        while current is not None and current.id not in seen:
            if current.id == ancestor_id:
                return True
            seen.add(current.id)
            current = self._groups.get(current.parent_group_id) if current.parent_group_id is not None else None
        return False

    def _recompute_group_bounds(self, group_id: int | None = None) -> None:
        """Refit one or all group boxes around their member nodes and child groups.

        Uses nominal node dimensions since the backend doesn't know rendered sizes;
        the frontend refines bounds on first user interaction. Groups with no members
        keep their current bounds. When refitting all groups, deepest first so a parent
        unions already-fitted child-group boxes.
        """
        node_width, node_height, padding, header = float(NODE_WIDTH), float(NODE_HEIGHT), 40.0, 36.0
        if group_id is not None:
            target_ids = [group_id]
        else:
            target_ids = sorted(self._groups, key=self._group_depth, reverse=True)
        for gid in target_ids:
            group = self._groups.get(gid)
            if group is None:
                continue
            boxes: list[tuple[float, float, float, float]] = []  # (x, y, w, h)
            for nid in self._member_node_ids(gid):
                node = self.get_node(nid)
                if node is not None and node.setting_input is not None:
                    nx = float(node.setting_input.pos_x or 0)
                    ny = float(node.setting_input.pos_y or 0)
                    boxes.append((nx, ny, node_width, node_height))
            for cid in self._child_group_ids(gid):
                child = self._groups.get(cid)
                if child is not None:
                    boxes.append((child.x_position, child.y_position, child.width, child.height))
            if not boxes:
                continue
            min_x = min(b[0] for b in boxes) - padding
            min_y = min(b[1] for b in boxes) - padding - header
            max_x = max(b[0] + b[2] for b in boxes) + padding
            max_y = max(b[1] + b[3] for b in boxes) + padding
            group.x_position = min_x
            group.y_position = min_y
            group.width = max_x - min_x
            group.height = max_y - min_y

    def create_group(
        self,
        name: str,
        node_ids: list[int],
        *,
        color: schemas.GroupColor | None = None,
        bounds: schemas.GroupBounds | None = None,
        parent_group_id: int | None = None,
        child_group_ids: list[int] | None = None,
    ) -> schemas.GroupInformation:
        """Create a visual group. Organizational only.

        Members are the given nodes (group_id) and child groups (their parent_group_id).
        The new group itself nests under parent_group_id. Bounds are computed when not supplied.
        """

        def _do() -> schemas.GroupInformation:
            group_id = self._next_group_id()
            group = schemas.GroupInformation(id=group_id, name=name, color=color, parent_group_id=parent_group_id)
            if bounds is not None:
                group.x_position, group.y_position, group.width, group.height = bounds
            self._groups[group_id] = group
            for node_id in node_ids:
                self._set_node_group(node_id, group_id)
            for cid in child_group_ids or []:
                child = self._groups.get(cid)
                if child is not None and not self._is_ancestor_group(cid, group_id):
                    child.parent_group_id = group_id
            if bounds is None:
                self._recompute_group_bounds(group_id)
            return group

        return self._execute_with_history(_do, HistoryActionType.CREATE_GROUP, f"Create group '{name}'")

    def update_group(
        self,
        group_id: int,
        *,
        name: str | None = None,
        color: schemas.GroupColor | None = None,
        bounds: schemas.GroupBounds | None = None,
        collapsed: bool | None = None,
    ) -> schemas.GroupInformation:
        """Rename / recolor / move / resize / collapse a group box."""
        group = self._groups.get(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} does not exist")

        def _do() -> schemas.GroupInformation:
            if name is not None:
                group.name = name
            if color is not None:
                group.color = color
            if bounds is not None:
                group.x_position, group.y_position, group.width, group.height = bounds
            if collapsed is not None:
                group.collapsed = collapsed
            return group

        return self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP, f"Update group '{group.name}'")

    def delete_group(self, group_id: int) -> None:
        """Remove a group box (ungroup). Members and sub-groups lift up one level."""
        group = self._groups.get(group_id)
        if group is None:
            return
        new_parent = group.parent_group_id

        def _do() -> None:
            for node_id in self._member_node_ids(group_id):
                self._set_node_group(node_id, new_parent)
            for cid in self._child_group_ids(group_id):
                child = self._groups.get(cid)
                if child is not None:
                    child.parent_group_id = new_parent
            self._groups.pop(group_id, None)

        self._execute_with_history(_do, HistoryActionType.DELETE_GROUP, f"Delete group '{group.name}'")

    def add_nodes_to_group(self, group_id: int, node_ids: list[int]) -> schemas.GroupInformation:
        """Add nodes to an existing group and refit its bounds."""
        group = self._groups.get(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} does not exist")

        def _do() -> schemas.GroupInformation:
            for node_id in node_ids:
                self._set_node_group(node_id, group_id)
            self._recompute_group_bounds(group_id)
            return group

        return self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP_MEMBERSHIP, "Add nodes to group")

    def remove_nodes_from_group(self, node_ids: list[int]) -> None:
        """Remove nodes from whatever group they belong to; prune groups left empty."""

        def _do() -> None:
            affected: set[int] = set()
            for node_id in node_ids:
                node = self.get_node(node_id)
                current = getattr(node.setting_input, "group_id", None) if node is not None else None
                if current is not None:
                    affected.add(current)
                    self._set_node_group(node_id, None)
            for gid in affected:
                if not self._member_node_ids(gid) and not self._child_group_ids(gid):
                    self._groups.pop(gid, None)
                else:
                    self._recompute_group_bounds(gid)

        self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP_MEMBERSHIP, "Remove nodes from group")

    def assign_node_to_named_group(
        self, node_id: int, name: str, *, color: schemas.GroupColor | None = None
    ) -> schemas.GroupInformation:
        """Assign a node to a group identified by name, creating it if absent (find-or-create)."""
        existing = next((group for group in self._groups.values() if group.name == name), None)
        if existing is not None:
            return self.add_nodes_to_group(existing.id, [node_id])
        return self.create_group(name, [node_id], color=color)

    def set_node_positions(self, updates: list[schemas.NodePositionUpdate]) -> None:
        """Persist dragged node positions (absolute canvas coordinates) onto setting_input.

        Plain mutator: the caller (update_layout route) captures history once for the
        whole drag-end batch so node moves and group-bounds changes share one snapshot.
        """
        self._note_graph_write()
        for update in updates:
            node = self.get_node(update.node_id)
            if node is not None and node.setting_input is not None and hasattr(node.setting_input, "pos_x"):
                node.setting_input.pos_x = update.pos_x
                node.setting_input.pos_y = update.pos_y

    def set_group_bounds(self, updates: list[schemas.GroupBoundsUpdate]) -> None:
        """Persist group box bounds (used together with set_node_positions on drag/resize)."""
        self._note_graph_write()
        for update in updates:
            group = self._groups.get(update.group_id)
            if group is not None:
                group.x_position = update.x_position
                group.y_position = update.y_position
                group.width = update.width
                group.height = update.height

    def restore_groups(self, groups: list[schemas.GroupInformation]) -> None:
        """Replace the runtime group registry (used by open_flow and restore_from_snapshot)."""
        self._groups = {group.id: group for group in groups}
        self._group_id_seq = max(self._groups, default=0)  # next id resumes above the highest restored
        for group in self._groups.values():
            if group.width <= 0 or group.height <= 0:
                self._recompute_group_bounds(group.id)

    def _next_comment_id(self) -> int:
        self._comment_id_seq = max([self._comment_id_seq, *self._comments]) + 1
        return self._comment_id_seq

    def create_comment(
        self,
        text: str,
        x_position: float,
        y_position: float,
        *,
        width: float | None = None,
        height: float | None = None,
    ) -> schemas.CommentInformation:
        """Create a canvas comment at the given absolute position."""

        def _do() -> schemas.CommentInformation:
            comment = schemas.CommentInformation(
                id=self._next_comment_id(), text=text, x_position=x_position, y_position=y_position
            )
            if width is not None:
                comment.width = width
            if height is not None:
                comment.height = height
            self._comments[comment.id] = comment
            return comment

        return self._execute_with_history(_do, HistoryActionType.CREATE_COMMENT, "Add comment")

    def update_comment(
        self,
        comment_id: int,
        *,
        text: str | None = None,
        bounds: schemas.CommentBounds | None = None,
    ) -> schemas.CommentInformation:
        """Edit the text and/or move or resize a canvas comment."""
        comment = self._comments.get(comment_id)
        if comment is None:
            raise ValueError(f"Comment {comment_id} does not exist")

        def _do() -> schemas.CommentInformation:
            if text is not None:
                comment.text = text
            if bounds is not None:
                comment.x_position, comment.y_position, comment.width, comment.height = bounds
            return comment

        return self._execute_with_history(_do, HistoryActionType.UPDATE_COMMENT, "Update comment")

    def delete_comment(self, comment_id: int) -> None:
        """Remove a canvas comment."""
        if comment_id not in self._comments:
            return
        self._execute_with_history(
            lambda: self._comments.pop(comment_id, None), HistoryActionType.DELETE_COMMENT, "Delete comment"
        )

    def set_comment_bounds(self, updates: list[schemas.CommentBoundsUpdate]) -> None:
        """Persist comment bounds (used together with set_node_positions on drag/resize)."""
        self._note_graph_write()
        for update in updates:
            comment = self._comments.get(update.comment_id)
            if comment is not None:
                comment.x_position = update.x_position
                comment.y_position = update.y_position
                comment.width = update.width
                comment.height = update.height

    def restore_comments(self, comments: list[schemas.CommentInformation]) -> None:
        """Replace the runtime comment registry (used by open_flow and restore_from_snapshot)."""
        self._comments = {comment.id: comment for comment in comments}
        self._comment_id_seq = max(self._comments, default=0)

    def _serialized_comments(self) -> list[schemas.FlowfileComment]:
        return [schemas.FlowfileComment(**comment.model_dump()) for comment in self._comments.values()]

    def apply_layout(self, y_spacing: int = 150, x_spacing: int = 200, initial_y: int = 100):
        """Calculates and applies a layered layout to all nodes in the graph.

        This updates their x and y positions for UI rendering.

        Args:
            y_spacing: The minimum vertical spacing between two nodes in a layer.
            x_spacing: The horizontal spacing between layers.
            initial_y: The y-position of the topmost node.
        """
        self.flow_logger.info("Applying layered layout...")
        self._note_graph_write()
        start_time = time()
        try:
            new_positions = calculate_layered_layout(
                self, y_spacing=y_spacing, x_spacing=x_spacing, initial_y=initial_y
            )

            if not new_positions:
                self.flow_logger.warning("Layout calculation returned no positions.")
                return

            updated_count = 0
            for node_id, (pos_x, pos_y) in new_positions.items():
                node = self.get_node(node_id)
                if node and hasattr(node, "setting_input"):
                    setting = node.setting_input
                    if hasattr(setting, "pos_x") and hasattr(setting, "pos_y"):
                        setting.pos_x = pos_x
                        setting.pos_y = pos_y
                        updated_count += 1
                    else:
                        self.flow_logger.warning(
                            f"Node {node_id} setting_input ({type(setting)}) lacks pos_x/pos_y attributes."
                        )
                elif node:
                    self.flow_logger.warning(f"Node {node_id} lacks setting_input attribute.")
                # else: node removed between calculation and apply; skip it

            # Reflowed node positions invalidate group boxes — refit them.
            self._recompute_group_bounds()

            end_time = time()
            self.flow_logger.info(
                f"Layout applied to {updated_count}/{len(self.nodes)} nodes in {end_time - start_time:.2f} seconds."
            )

        except Exception as e:
            self.flow_logger.error(f"Layout failed, keeping current positions: {e}")
