"""
Schema definitions for the undo/redo history system.

This module defines the Pydantic models for tracking flow graph history,
enabling users to undo and redo changes to their flow graphs.
"""

from enum import Enum
from typing import Optional, TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
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
    APPLY_LAYOUT = "apply_layout"
    BATCH = "batch"


class HistoryConfig(BaseModel):
    """Configuration for the history system."""

    enabled: bool = Field(default=True, description="Whether history tracking is enabled")
    max_stack_size: int = Field(default=50, description="Maximum number of history entries to keep")


class HistoryEntry(BaseModel):
    """A single entry in the history stack.

    Stores a complete snapshot of the flow state along with metadata
    about the action that created this entry.
    """

    # Note: snapshot is stored as a dict to avoid circular import issues
    # It will be validated as FlowfileData when used
    snapshot: dict = Field(..., description="Full flow state snapshot as dict")
    action_type: HistoryActionType = Field(..., description="Type of action that created this entry")
    description: str = Field(..., description="Human-readable description of the action")
    timestamp: float = Field(..., description="Unix timestamp when the entry was created")
    node_id: Optional[int] = Field(default=None, description="ID of the affected node, if applicable")

    class Config:
        arbitrary_types_allowed = True


class HistoryState(BaseModel):
    """Current state of the history system.

    Provides information about what undo/redo operations are available.
    """

    can_undo: bool = Field(default=False, description="Whether undo is available")
    can_redo: bool = Field(default=False, description="Whether redo is available")
    undo_description: Optional[str] = Field(
        default=None, description="Description of the action that would be undone"
    )
    redo_description: Optional[str] = Field(
        default=None, description="Description of the action that would be redone"
    )
    undo_count: int = Field(default=0, description="Number of available undo steps")
    redo_count: int = Field(default=0, description="Number of available redo steps")


class UndoRedoResult(BaseModel):
    """Result of an undo or redo operation."""

    success: bool = Field(..., description="Whether the operation succeeded")
    action_description: Optional[str] = Field(
        default=None, description="Description of the action that was undone/redone"
    )
    error_message: Optional[str] = Field(
        default=None, description="Error message if the operation failed"
    )
