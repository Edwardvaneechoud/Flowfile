"""
History tracking schemas for undo/redo functionality.

This module defines the Pydantic models used for tracking flow graph history,
enabling users to undo and redo actions on the graph.
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class HistoryActionType(str, Enum):
    """Enumeration of action types that can be tracked in history."""

    ADD_NODE = "add_node"
    DELETE_NODE = "delete_node"
    MOVE_NODE = "move_node"
    ADD_CONNECTION = "add_connection"
    DELETE_CONNECTION = "delete_connection"
    UPDATE_SETTINGS = "update_settings"
    COPY_NODE = "copy_node"
    PASTE_NODES = "paste_nodes"
    APPLY_LAYOUT = "apply_layout"
    BATCH = "batch"


class HistoryConfig(BaseModel):
    """Configuration for history tracking behavior."""

    enabled: bool = Field(default=True, description="Whether history tracking is enabled")
    max_stack_size: int = Field(default=50, description="Maximum number of history entries to keep")
    use_diff_storage: bool = Field(
        default=True, description="Use diff-based storage for memory efficiency"
    )


class NodeDiff(BaseModel):
    """Represents changes to a single node."""

    node_id: int
    action: str  # 'add', 'remove', 'modify'
    old_data: Optional[dict[str, Any]] = None
    new_data: Optional[dict[str, Any]] = None


class ConnectionDiff(BaseModel):
    """Represents changes to a connection."""

    from_node_id: int
    to_node_id: int
    connection_type: str
    action: str  # 'add', 'remove'


class SettingsDiff(BaseModel):
    """Represents changes to flow settings."""

    old_settings: Optional[dict[str, Any]] = None
    new_settings: Optional[dict[str, Any]] = None


class FlowDiff(BaseModel):
    """Represents the diff between two flow states.

    This is used for memory-efficient storage of history entries.
    Instead of storing full snapshots, we store only the changes.
    """

    node_changes: list[NodeDiff] = Field(default_factory=list)
    connection_changes: list[ConnectionDiff] = Field(default_factory=list)
    settings_changes: Optional[SettingsDiff] = None
    start_node_changes: Optional[dict[str, list[int]]] = None  # 'added', 'removed'


class HistoryEntry(BaseModel):
    """A single entry in the history stack.

    For memory efficiency, we can store either a full snapshot or a diff.
    The first entry is always a full snapshot, subsequent entries can be diffs.
    """

    action_type: HistoryActionType
    description: str
    timestamp: float
    node_id: Optional[int] = None

    # Either snapshot_data (full state) or diff_data (changes only) should be set
    snapshot_data: Optional[dict[str, Any]] = Field(
        default=None, description="Full flow state (JSON serializable)"
    )
    diff_data: Optional[FlowDiff] = Field(
        default=None, description="Diff from previous state for memory efficiency"
    )

    # Flag to indicate if this entry stores a full snapshot
    is_full_snapshot: bool = Field(default=True)


class HistoryState(BaseModel):
    """Current state of the history system, used for API responses."""

    can_undo: bool = Field(default=False, description="Whether undo is available")
    can_redo: bool = Field(default=False, description="Whether redo is available")
    undo_description: Optional[str] = Field(default=None, description="Description of next undo action")
    redo_description: Optional[str] = Field(default=None, description="Description of next redo action")
    undo_count: int = Field(default=0, description="Number of actions available to undo")
    redo_count: int = Field(default=0, description="Number of actions available to redo")


class UndoRedoResult(BaseModel):
    """Result of an undo or redo operation."""

    success: bool = Field(description="Whether the operation succeeded")
    action_description: Optional[str] = Field(default=None, description="Description of the action performed")
    error_message: Optional[str] = Field(default=None, description="Error message if operation failed")
    new_state: Optional[HistoryState] = Field(default=None, description="Updated history state after operation")


class ActionResponse(BaseModel):
    """Response for actions that modify the flow graph.

    Includes the history state so the frontend doesn't need to poll for it.
    """

    success: bool = Field(default=True, description="Whether the action succeeded")
    message: Optional[str] = Field(default=None, description="Optional message")
    history_state: Optional[HistoryState] = Field(default=None, description="Current history state after action")
