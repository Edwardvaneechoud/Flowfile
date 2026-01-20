"""
Schema definitions for the undo/redo history system.

This module defines the Pydantic models for tracking flow graph history,
enabling users to undo and redo changes to their flow graphs.
"""

import pickle
import zlib
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
    use_compression: bool = Field(default=True, description="Whether to compress snapshots")
    compression_level: int = Field(default=6, ge=1, le=9, description="Compression level (1-9)")


class CompressedSnapshot:
    """Efficiently stores a compressed flow state snapshot.

    Uses zlib compression to reduce memory usage by 60-80%.
    This is not a Pydantic model to avoid serialization overhead.
    """

    __slots__ = ('_compressed_data', '_hash')

    def __init__(self, snapshot_dict: dict, compression_level: int = 6):
        """Create a compressed snapshot from a dictionary.

        Args:
            snapshot_dict: The flow state dictionary to compress.
            compression_level: Compression level 1-9 (higher = smaller but slower).
        """
        # Pickle and compress the snapshot
        pickled = pickle.dumps(snapshot_dict, protocol=pickle.HIGHEST_PROTOCOL)
        self._compressed_data = zlib.compress(pickled, level=compression_level)

        # Pre-compute hash for fast comparison
        self._hash = self._compute_hash(snapshot_dict)

    @staticmethod
    def _compute_hash(snapshot_dict: dict) -> int:
        """Compute a fast structural hash of the snapshot."""
        nodes = snapshot_dict.get("nodes", [])

        # Build tuple of node signatures for hashing
        node_signatures = []
        for n in sorted(nodes, key=lambda x: x.get("id", 0)):
            sig = (
                n.get("id"),
                n.get("type"),
                tuple(n.get("input_ids") or []),
                n.get("left_input_id"),
                n.get("right_input_id"),
                tuple(n.get("outputs") or []),
                n.get("x_position"),
                n.get("y_position"),
                # Include a hash of setting_input for change detection
                hash(str(n.get("setting_input"))) if n.get("setting_input") else None,
            )
            node_signatures.append(sig)

        settings = snapshot_dict.get("flowfile_settings", {})
        settings_tuple = tuple(sorted(settings.items())) if isinstance(settings, dict) else hash(str(settings))

        return hash((
            snapshot_dict.get("flowfile_id"),
            settings_tuple,
            tuple(node_signatures),
        ))

    def decompress(self) -> dict:
        """Decompress and return the original snapshot dictionary."""
        pickled = zlib.decompress(self._compressed_data)
        return pickle.loads(pickled)

    @property
    def hash(self) -> int:
        """Get the pre-computed hash for fast comparison."""
        return self._hash

    @property
    def compressed_size(self) -> int:
        """Get the size of the compressed data in bytes."""
        return len(self._compressed_data)

    def __eq__(self, other: "CompressedSnapshot") -> bool:
        """Fast equality check using pre-computed hashes."""
        if not isinstance(other, CompressedSnapshot):
            return False
        return self._hash == other._hash


class HistoryEntry:
    """A single entry in the history stack.

    Stores a compressed snapshot of the flow state along with metadata
    about the action that created this entry.

    Uses __slots__ for memory efficiency.
    """

    __slots__ = ('_snapshot', 'action_type', 'description', 'timestamp', 'node_id')

    def __init__(
        self,
        snapshot: CompressedSnapshot,
        action_type: HistoryActionType,
        description: str,
        timestamp: float,
        node_id: Optional[int] = None,
    ):
        self._snapshot = snapshot
        self.action_type = action_type
        self.description = description
        self.timestamp = timestamp
        self.node_id = node_id

    @classmethod
    def from_dict(
        cls,
        snapshot_dict: dict,
        action_type: HistoryActionType,
        description: str,
        timestamp: float,
        node_id: Optional[int] = None,
        compression_level: int = 6,
    ) -> "HistoryEntry":
        """Create a HistoryEntry from a snapshot dictionary.

        Args:
            snapshot_dict: The flow state dictionary.
            action_type: The type of action.
            description: Human-readable description.
            timestamp: Unix timestamp.
            node_id: Optional affected node ID.
            compression_level: Compression level 1-9.
        """
        compressed = CompressedSnapshot(snapshot_dict, compression_level)
        return cls(compressed, action_type, description, timestamp, node_id)

    def get_snapshot(self) -> dict:
        """Decompress and return the snapshot dictionary."""
        return self._snapshot.decompress()

    @property
    def snapshot_hash(self) -> int:
        """Get the hash of the snapshot for comparison."""
        return self._snapshot.hash

    @property
    def compressed_size(self) -> int:
        """Get the compressed size in bytes."""
        return self._snapshot.compressed_size


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


class OperationResponse(BaseModel):
    """Standard response for operations that modify the flow graph.

    Includes the current history state so the frontend can update its UI.
    """

    success: bool = Field(default=True, description="Whether the operation succeeded")
    message: Optional[str] = Field(default=None, description="Optional message")
    history: HistoryState = Field(..., description="Current history state after the operation")
