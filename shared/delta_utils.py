"""Shared Delta Lake / catalog utility helpers.

Small, dependency-light functions used by both ``flowfile_core`` and
``flowfile_worker``.  Keeping them here avoids cross-package duplication.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------


def make_json_safe(val: object) -> object:
    """Coerce *val* to a JSON-native Python type."""
    if val is None or isinstance(val, bool | int | float | str):
        return val
    return str(val)


# ---------------------------------------------------------------------------
# Delta timestamp formatting
# ---------------------------------------------------------------------------


def format_delta_timestamp(ts: object) -> str | None:
    """Convert a raw delta-log timestamp to an ISO 8601 string.

    Handles ``None``, ``str``, ``datetime``, and epoch-millisecond
    ``int``/``float`` values.
    """
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, int | float):
        # Milliseconds since epoch
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return str(ts)


# ---------------------------------------------------------------------------
# Delta table size
# ---------------------------------------------------------------------------


def get_delta_size_bytes(path: str | Path) -> int:
    """Sum the sizes of active data files from the Delta transaction log.

    Uses the Delta log metadata rather than filesystem scanning, which
    correctly excludes tombstoned files from previous versions.

    Falls back to filesystem scanning if the delta log can't be read.
    """
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(str(path))
        add_actions = dt.get_add_actions(flatten=True)
        size_col = add_actions.column("size_bytes")
        return sum(v for v in size_col.to_pylist() if v is not None)
    except Exception:
        logger.warning("Failed to read size from delta log, falling back to filesystem scan", exc_info=True)
        return sum(f.stat().st_size for f in Path(path).rglob("*.parquet"))


# ---------------------------------------------------------------------------
# Catalog path validation
# ---------------------------------------------------------------------------


def validate_catalog_path(table_name: str, catalog_dir: Path) -> Path:
    """Validate that *table_name* is a simple name and resolve it under *catalog_dir*.

    Only a bare name is accepted (no path separators, no ``..``, no null
    bytes).  The full path is constructed from the trusted *catalog_dir*.

    Raises ``ValueError`` when the input is invalid.
    """
    if not table_name:
        raise ValueError("table_name must not be empty")
    if "\x00" in table_name:
        raise ValueError("table_name contains null bytes")
    if "/" in table_name or "\\" in table_name:
        raise ValueError("table_name must not contain path separators")
    if ".." in table_name:
        raise ValueError("table_name must not contain '..'")

    return catalog_dir.resolve() / table_name
