"""Centralised catalog-read primitives.

The worker has exactly two ways to open catalog data: ``open_catalog_table``
for materialised delta tables and ``open_virtual_result`` for IPC files
written by the flow-virtual table resolver. Anything else is a bug.

Imports polars at module top — only import this module from spawned
children or one-shot worker tasks, never from the FastAPI process.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from shared.delta_utils import validate_catalog_path
from shared.storage_config import storage


def _catalog_path(name: str) -> Path:
    return validate_catalog_path(name, storage.catalog_tables_directory)


def _virtual_results_path(name: str) -> Path:
    return validate_catalog_path(name, storage.catalog_virtual_results_directory)


def open_catalog_table(name: str) -> pl.LazyFrame:
    """Open a materialised catalog table by bare directory name."""
    return pl.scan_delta(str(_catalog_path(name)))


def open_virtual_result(name: str) -> pl.LazyFrame:
    """Open a flow-virtual IPC result by bare filename."""
    return pl.scan_ipc(str(_virtual_results_path(name)))
