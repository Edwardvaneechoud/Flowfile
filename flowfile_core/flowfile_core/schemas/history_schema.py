"""
Schema definitions for the undo/redo history system.

This module defines the Pydantic models for tracking flow graph history,
enabling users to undo and redo changes to their flow graphs.
"""

import hashlib
import json
import pickle
import zlib
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, SecretBytes, SecretStr

# Undo/redo covers the graph only; flow-level settings and identity are never part of a step.
IN_SCOPE_KEYS = ("nodes", "groups", "comments")
_PERSISTED_SETTINGS_EXCLUDE = ("source_registration_id",)


def _canonical(value: Any) -> Any:
    """Normalize a snapshot value into JSON-safe, order-stable primitives for hashing."""
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_canonical(item) for item in value]
    if value is None or (isinstance(value, str | int | float | bool) and not isinstance(value, Enum)):
        return value
    return _canonical(_json_default(value))


def _json_default(value: Any) -> Any:
    """Encode what ``json`` cannot natively (the C encoder calls this only for such leaves)."""
    if isinstance(value, BaseModel):
        return value.model_dump()
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, SecretBytes):
        return value.get_secret_value().hex()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, set | frozenset):
        return sorted(value, key=repr)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _digest(value: Any) -> int:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)
    except TypeError:
        # Non-string (or mixed-type) dict keys: normalize in Python first.
        encoded = json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"), default=str)
    return int.from_bytes(hashlib.blake2b(encoded.encode("ascii"), digest_size=16).digest(), "big")


def _in_scope(snapshot_dict: dict) -> dict:
    return {
        key: sorted(snapshot_dict.get(key) or [], key=lambda item: (item.get("id") is None, item.get("id") or 0))
        for key in IN_SCOPE_KEYS
    }


def in_scope_hash(snapshot_dict: dict) -> int:
    """Hash of everything undo/redo restores: every field of every node, group and comment."""
    return _digest(_in_scope(snapshot_dict))


def persisted_hash(snapshot_dict: dict, graph_hash: int | None = None) -> int:
    """Hash of what a save writes, minus machine-local fields; the dirty flag compares these.

    ``graph_hash`` (the snapshot's :func:`in_scope_hash`) is reused when the caller already has it.
    """
    settings = {
        key: value
        for key, value in (snapshot_dict.get("flowfile_settings") or {}).items()
        if key not in _PERSISTED_SETTINGS_EXCLUDE
    }
    if graph_hash is None:
        graph_hash = in_scope_hash(snapshot_dict)
    return _digest([graph_hash, settings])


class HistorySnapshot:
    """One capture of a flow, hashed once.

    ``pickled`` is a detached copy (later in-place edits of live settings cannot leak into
    it) that doubles as the undo-entry payload; it is None when only the hashes are needed.
    """

    __slots__ = ("graph_hash", "persisted_hash", "pickled")

    def __init__(self, snapshot_dict: dict, keep_payload: bool = True):
        self.pickled = pickle.dumps(snapshot_dict, protocol=pickle.HIGHEST_PROTOCOL) if keep_payload else None
        self.graph_hash = in_scope_hash(snapshot_dict)
        self.persisted_hash = persisted_hash(snapshot_dict, self.graph_hash)

    def load(self) -> dict:
        """A fresh copy of the captured snapshot dict."""
        return pickle.loads(self.pickled)


class HistoryActionType(str, Enum):
    """Enumeration of action types that can be tracked in history."""

    ADD_NODE = "add_node"
    DELETE_NODE = "delete_node"
    MOVE_NODE = "move_node"
    MOVE_NODES = "move_nodes"
    ADD_CONNECTION = "add_connection"
    DELETE_CONNECTION = "delete_connection"
    UPDATE_SETTINGS = "update_settings"
    COPY_NODE = "copy_node"
    PASTE_NODES = "paste_nodes"
    APPLY_LAYOUT = "apply_layout"
    CREATE_GROUP = "create_group"
    UPDATE_GROUP = "update_group"
    DELETE_GROUP = "delete_group"
    UPDATE_GROUP_MEMBERSHIP = "update_group_membership"
    CREATE_COMMENT = "create_comment"
    UPDATE_COMMENT = "update_comment"
    DELETE_COMMENT = "delete_comment"
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

    __slots__ = ("_compressed_data", "_hash")

    def __init__(self, snapshot_dict: dict, compression_level: int = 6):
        """Create a compressed snapshot from a dictionary.

        Args:
            snapshot_dict: The flow state dictionary to compress.
            compression_level: Compression level 1-9 (higher = smaller but slower).
        """
        pickled = pickle.dumps(snapshot_dict, protocol=pickle.HIGHEST_PROTOCOL)
        self._compressed_data = zlib.compress(pickled, level=compression_level)

        self._hash = self._compute_hash(snapshot_dict)

    @classmethod
    def from_pickled(cls, pickled: bytes, snapshot_hash: int, compression_level: int = 6) -> "CompressedSnapshot":
        """Build from an already pickled snapshot and its known hash (no second pickle or hash pass)."""
        snapshot = cls.__new__(cls)
        snapshot._compressed_data = zlib.compress(pickled, level=compression_level)
        snapshot._hash = snapshot_hash
        return snapshot

    @staticmethod
    def _compute_hash(snapshot_dict: dict) -> int:
        """Hash of the in-scope snapshot (see :func:`in_scope_hash`)."""
        return in_scope_hash(snapshot_dict)

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

    __slots__ = ("_snapshot", "action_type", "description", "timestamp", "node_id")

    def __init__(
        self,
        snapshot: CompressedSnapshot,
        action_type: HistoryActionType,
        description: str,
        timestamp: float,
        node_id: int | None = None,
    ):
        self._snapshot = snapshot
        self.action_type = action_type
        self.description = description
        self.timestamp = timestamp
        self.node_id = node_id

    @classmethod
    def from_snapshot(
        cls,
        snapshot: HistorySnapshot,
        action_type: HistoryActionType,
        description: str,
        timestamp: float,
        node_id: int | None = None,
        compression_level: int = 6,
    ) -> "HistoryEntry":
        """Create a HistoryEntry from a :class:`HistorySnapshot`, reusing its pickle and hash."""
        compressed = CompressedSnapshot.from_pickled(snapshot.pickled, snapshot.graph_hash, compression_level)
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
    undo_description: str | None = Field(default=None, description="Description of the action that would be undone")
    redo_description: str | None = Field(default=None, description="Description of the action that would be redone")
    undo_count: int = Field(default=0, description="Number of available undo steps")
    redo_count: int = Field(default=0, description="Number of available redo steps")
    flow_id: int | None = Field(default=None, description="The flow this history belongs to")


class UndoRedoResult(BaseModel):
    """Result of an undo or redo operation."""

    success: bool = Field(..., description="Whether the operation succeeded")
    action_description: str | None = Field(default=None, description="Description of the action that was undone/redone")
    error_message: str | None = Field(default=None, description="Error message if the operation failed")
    history: HistoryState | None = Field(default=None, description="History state after the operation")


class OperationResponse(BaseModel):
    """Standard response for operations that modify the flow graph.

    Includes the current history state so the frontend can update its UI.
    """

    success: bool = Field(default=True, description="Whether the operation succeeded")
    message: str | None = Field(default=None, description="Optional message")
    history: HistoryState = Field(..., description="Current history state after the operation")
