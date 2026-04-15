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

from shared.delta_models import SourceTableVersion
from shared.delta_utils import get_delta_size_bytes

logger = logging.getLogger(__name__)


def check_source_versions_current(source_table_versions_json: str | None) -> bool:
    """Return True if all source delta tables are still at their recorded versions.

    Returns True when no versions are recorded (backward compat for existing virtual tables).
    Returns False if any source table has been updated, deleted, or is unreadable.
    """
    if not source_table_versions_json:
        return True
    try:
        import json

        raw = json.loads(source_table_versions_json)
        versions = [SourceTableVersion(**entry) for entry in raw]
    except (ValueError, KeyError, TypeError):
        logger.warning("Could not parse source_table_versions JSON, treating as stale")
        return False

    for sv in versions:
        try:
            current_version = DeltaTable(sv.file_path, without_files=True).version()
            if current_version != sv.version:
                logger.info(
                    "Source table %d at %s changed: expected version %d, current %d",
                    sv.table_id, sv.file_path, sv.version, current_version,
                )
                return False
        except Exception:
            logger.warning("Could not read delta version for source table %d at %s", sv.table_id, sv.file_path)
            return False
    return True


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

    Delegates to ``shared.delta_utils.get_delta_size_bytes``.
    """
    return get_delta_size_bytes(path)


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
