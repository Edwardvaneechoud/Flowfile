"""Previews for physical and virtual tables, plus Delta history."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from flowfile_core.catalog.delta_utils import (
    is_delta_table,
    read_delta_preview,
    table_exists,
)
from flowfile_core.catalog.exceptions import TableNotFoundError
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.services.virtual_tables import VirtualTableService
from flowfile_core.catalog.text_utils import hash_source_versions, parse_delta_history
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_delta_history,
    trigger_delta_version_preview,
    trigger_resolve_virtual_table,
)
from flowfile_core.schemas.catalog_schema import (
    CatalogTablePreview,
    DeltaTableHistory,
)
from flowfile_core.utils.arrow_reader import read_top_n
from shared.delta_utils import validate_catalog_path
from shared.storage_config import storage

logger = logging.getLogger(__name__)


def _should_offload() -> bool:
    """Return True when heavy I/O should be delegated to the worker process."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    return OFFLOAD_TO_WORKER.value


class TablePreviewService:
    """Owns table preview fetching (physical, Delta-versioned, virtual) and Delta history."""

    def __init__(
        self,
        repo: CatalogRepository,
        tables: TableService,
        virtual_tables: VirtualTableService,
    ) -> None:
        self.repo = repo
        self._tables = tables
        self._virtual_tables = virtual_tables

    def get_table_preview(
        self,
        table_id: int,
        limit: int,
        version: int | None = None,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Read the first N rows from a catalog table."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        if getattr(table, "table_type", "physical") == "virtual":
            if getattr(table, "sql_query", None):
                return self._get_query_virtual_table_preview(table, limit, user_id)
            return self._get_virtual_table_preview(table, limit, user_id)

        return self._get_physical_table_preview(table, limit, version)

    def _format_virtual_preview(
        self,
        lazy_frame: pl.LazyFrame,
        table: CatalogTable,
        limit: int,
    ) -> CatalogTablePreview:
        """Materialise *lazy_frame* on the worker, read top-N rows back as PyArrow.

        Honours the core-never-collects rule — the plan is shipped to the
        worker, written as IPC, then read back via ``read_top_n``.
        """
        versions_hash = hash_source_versions(table.source_table_versions)
        result = trigger_resolve_virtual_table(table.id, lazy_frame.serialize(), versions_hash)
        ipc_path = validate_catalog_path(result["ipc_path"], storage.catalog_virtual_results_directory)
        pa_table = read_top_n(str(ipc_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=result.get("row_count"))

    def _get_query_virtual_table_preview(
        self,
        table: CatalogTable,
        limit: int,
        user_id: int | None,
    ) -> CatalogTablePreview:
        """Resolve a query-based virtual table and return a preview."""
        lazy_frame = resolve_or_log(
            lambda: self._virtual_tables.resolve_query_virtual_table(table.id, user_id=user_id),
            kind="query virtual table for preview",
            identifier=table.id,
        )
        if lazy_frame is None:
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)
        return self._format_virtual_preview(lazy_frame, table, limit)

    def _get_virtual_table_preview(
        self,
        table: CatalogTable,
        limit: int,
        user_id: int | None,
    ) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview of the collected result."""
        lazy_frame = resolve_or_log(
            lambda: self._virtual_tables.resolve_virtual_flow_table(table.id, user_id=user_id),
            kind="virtual flow table for preview",
            identifier=table.id,
        )
        if lazy_frame is None:
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)
        return self._format_virtual_preview(lazy_frame, table, limit)

    def resolve_virtual_flow_table_preview(
        self,
        table_id: int,
        limit: int,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview (worker-backed)."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        lazy_frame = self._virtual_tables.resolve_virtual_flow_table(table_id, user_id=user_id)
        return self._format_virtual_preview(lazy_frame, table, limit)

    def _get_physical_table_preview(
        self,
        table: CatalogTable,
        limit: int,
        version: int | None,
    ) -> CatalogTablePreview:
        """Read a preview from a physical (Delta or Parquet) catalog table."""
        if not table.file_path:
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        data_path = Path(table.file_path)
        if not table_exists(data_path):
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        if version is not None and is_delta_table(data_path):
            return self._get_delta_version_preview(data_path, version, limit)

        if is_delta_table(data_path):
            pa_table = read_delta_preview(str(data_path), n_rows=limit)
        else:
            pa_table = read_top_n(str(data_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=table.row_count)

    def _get_delta_version_preview(self, data_path: Path, version: int, limit: int) -> CatalogTablePreview:
        """Read a Delta table preview at a specific version via the worker (or locally)."""
        table_path = str(data_path)
        if _should_offload():
            try:
                return trigger_delta_version_preview(data_path.name, version, limit)
            except (RuntimeError, OSError):
                logger.warning(
                    "Worker delta version preview failed, falling back to local", exc_info=True
                )

        delta_table = DeltaTable(table_path, version=version)
        dataset = delta_table.to_pyarrow_dataset()
        pa_table = dataset.head(limit)
        return format_pyarrow_preview(pa_table)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        """Return the version history for a Delta catalog table."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        if not table.file_path:
            return DeltaTableHistory(current_version=0, history=[])

        data_path = Path(table.file_path)
        if not is_delta_table(data_path):
            return DeltaTableHistory(current_version=0, history=[])

        table_path = str(data_path)
        if _should_offload():
            try:
                return trigger_delta_history(data_path.name, limit)
            except (RuntimeError, OSError):
                logger.warning("Worker delta history read failed, falling back to local", exc_info=True)

        delta_table = DeltaTable(table_path, without_files=True)
        raw_history = delta_table.history(limit)
        current_version = delta_table.version()
        history = parse_delta_history(raw_history)
        return DeltaTableHistory(current_version=current_version, history=history)
