"""
Node execution state - separable from node definition.
Can be persisted to database/cache for stateless operation.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn


@dataclass
class SourceFileInfo:
    """
    Tracks source file state for cache invalidation.
    Used by read nodes to detect when source files change.
    """
    path: str
    mtime: float
    size: int

    @classmethod
    def from_path(cls, path: str) -> SourceFileInfo | None:
        """Create from file path, returns None if file doesn't exist."""
        try:
            stat = os.stat(path)
            return cls(path=path, mtime=stat.st_mtime, size=stat.st_size)
        except OSError:
            return None

    def has_changed(self) -> bool:
        """Check if file has changed since this info was recorded."""
        try:
            stat = os.stat(self.path)
            return stat.st_mtime != self.mtime or stat.st_size != self.size
        except OSError:
            return True  # File missing = changed

    def to_dict(self) -> dict:
        """Serialize for external storage."""
        return {"path": self.path, "mtime": self.mtime, "size": self.size}

    @classmethod
    def from_dict(cls, data: dict) -> SourceFileInfo:
        """Deserialize from external storage."""
        return cls(path=data["path"], mtime=data["mtime"], size=data["size"])


@dataclass
class NodeExecutionState:
    """
    All mutable state for a node's execution.

    This can be:
    - Stored in memory (current behavior)
    - Persisted to database (stateless workers)
    - Stored in Redis/cache (distributed execution)

    Kept separate from FlowNode to enable stateless execution patterns.
    """
    # Execution tracking
    has_run_with_current_setup: bool = False
    has_completed_last_run: bool = False
    is_canceled: bool = False
    error: str | None = None

    # Results (not serialized - too large)
    resulting_data: "FlowDataEngine | None" = field(default=None, repr=False)
    example_data_path: str | None = None
    example_data_generator: Callable[[], pa.Table] | None = field(default=None, repr=False)
    warnings: str | None = None

    # Schema
    result_schema: "list[FlowfileColumn] | None" = field(default=None, repr=False)
    predicted_schema: "list[FlowfileColumn] | None" = field(default=None, repr=False)

    # Source tracking (for read nodes)
    source_file_info: SourceFileInfo | None = None

    # Hash for cache lookup
    execution_hash: str | None = None

    def reset(self) -> None:
        """Reset to clean state for re-execution."""
        self.has_run_with_current_setup = False
        self.has_completed_last_run = False
        self.is_canceled = False
        self.error = None
        self.resulting_data = None
        self.example_data_path = None
        self.example_data_generator = None
        self.warnings = None
        self.result_schema = None
        self.predicted_schema = None
        # Note: source_file_info intentionally NOT reset - needed for change detection
        self.execution_hash = None

    def reset_results_only(self) -> None:
        """Reset just the results, keep tracking state."""
        self.error = None
        self.resulting_data = None
        self.example_data_path = None
        self.example_data_generator = None
        self.warnings = None

    def mark_successful(self) -> None:
        """Mark execution as successful."""
        self.has_run_with_current_setup = True
        self.has_completed_last_run = True
        self.error = None

    def mark_failed(self, error: str) -> None:
        """Mark execution as failed."""
        self.has_run_with_current_setup = False
        self.has_completed_last_run = False
        self.error = error

    def to_dict(self) -> dict:
        """
        Serialize for external storage (stateless mode).
        Note: resulting_data and generators are not serialized.
        """
        return {
            "has_run_with_current_setup": self.has_run_with_current_setup,
            "has_completed_last_run": self.has_completed_last_run,
            "is_canceled": self.is_canceled,
            "error": self.error,
            "example_data_path": self.example_data_path,
            "warnings": self.warnings,
            "execution_hash": self.execution_hash,
            "source_file_info": self.source_file_info.to_dict() if self.source_file_info else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> NodeExecutionState:
        """Deserialize from external storage."""
        state = cls(
            has_run_with_current_setup=data.get("has_run_with_current_setup", False),
            has_completed_last_run=data.get("has_completed_last_run", False),
            is_canceled=data.get("is_canceled", False),
            error=data.get("error"),
            example_data_path=data.get("example_data_path"),
            warnings=data.get("warnings"),
            execution_hash=data.get("execution_hash"),
        )
        if data.get("source_file_info"):
            state.source_file_info = SourceFileInfo.from_dict(data["source_file_info"])
        return state
