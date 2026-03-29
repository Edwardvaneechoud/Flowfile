"""Utility helpers for Delta Lake and legacy Parquet table detection and I/O.

Centralises all format-aware logic so that callers (service, flow_graph, etc.)
can remain format-agnostic.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def is_delta_table(path: str | Path) -> bool:
    """Return ``True`` if *path* is a directory containing ``_delta_log/``."""
    p = Path(path)
    return p.is_dir() and (p / "_delta_log").is_dir()


def is_legacy_parquet(path: str | Path) -> bool:
    """Return ``True`` if *path* is a single ``.parquet`` file."""
    p = Path(path)
    return p.is_file() and p.suffix.lower() == ".parquet"


def table_exists(path: str | Path) -> bool:
    """Return ``True`` if either a Delta directory or a Parquet file exists."""
    return is_delta_table(path) or is_legacy_parquet(path)


def get_delta_table_size_bytes(path: str | Path) -> int:
    """Sum the sizes of active data files from the Delta transaction log.

    Uses the Delta log metadata rather than filesystem scanning, which
    correctly excludes tombstoned files from previous versions.
    """

    try:
        dt = DeltaTable(str(path))
        add_actions = dt.get_add_actions(flatten=True)
        size_col = add_actions.column("size_bytes")
        return sum(v for v in size_col.to_pylist() if v is not None)
    except Exception:
        logger.warning("Failed to read size from delta log, falling back to filesystem scan", exc_info=True)
        return sum(f.stat().st_size for f in Path(path).rglob("*.parquet"))


def read_delta_preview(path: str, n_rows: int = 100) -> pa.Table:
    """Read the first N rows from a Delta table using PyArrow."""
    dt = DeltaTable(str(path))

    # Creates a lazy dataset reference, no data is loaded yet
    dataset = dt.to_pyarrow_dataset()

    return dataset.head(n_rows)


def delete_table_storage(path: str | Path) -> None:
    """Delete either a Delta directory or a single Parquet file."""
    p = Path(path)
    if p.is_dir():
        shutil.rmtree(p)
    elif p.is_file():
        p.unlink()
