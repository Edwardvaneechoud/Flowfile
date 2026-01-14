"""
Pydantic models for the undo/redo history management system.

This module defines the data structures used to track and manage
the history of flow graph modifications, enabling undo and redo operations.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from flowfile_core.schemas.schemas import FlowfileData


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
    BATCH = "batch"


class HistoryEntry(BaseModel):
    """Represents a single entry in the undo/redo history stack.

    Attributes:
        snapshot: The complete flow state at the time of the action.
        action_type: The type of action that was performed.
        description: A human-readable description of the action.
        timestamp: Unix timestamp when the action occurred.
        node_id: Optional node ID if the action involves a specific node.
    """

    snapshot: FlowfileData
    action_type: HistoryActionType
    description: str
    timestamp: float
    node_id: Optional[int] = None


class HistoryState(BaseModel):
    """Represents the current state of the history manager.

    Attributes:
        can_undo: Whether there are actions that can be undone.
        can_redo: Whether there are actions that can be redone.
        undo_description: Description of the next action to be undone.
        redo_description: Description of the next action to be redone.
        undo_count: Number of actions in the undo stack.
        redo_count: Number of actions in the redo stack.
    """

    can_undo: bool = False
    can_redo: bool = False
    undo_description: Optional[str] = None
    redo_description: Optional[str] = None
    undo_count: int = 0
    redo_count: int = 0


class UndoRedoResult(BaseModel):
    """Result of an undo or redo operation.

    Attributes:
        success: Whether the operation was successful.
        action_description: Description of the action that was undone/redone.
        error_message: Error message if the operation failed.
    """

    success: bool
    action_description: Optional[str] = None
    error_message: Optional[str] = None


class HistoryConfig(BaseModel):
    """Configuration for the history manager.

    Attributes:
        max_stack_size: Maximum number of entries in each stack.
        enabled: Whether history tracking is enabled.
    """

    max_stack_size: int = Field(default=50, ge=1, le=200)
    enabled: bool = True
