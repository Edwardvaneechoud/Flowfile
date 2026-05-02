"""Previews for physical tables and Delta history.

Virtual tables don't have a preview path: the catalog UI shows a
"no data preview available" placeholder for them and never calls these
endpoints, so ``get_table_preview`` returns an empty preview when handed
a virtual table id.
"""

from __future__ import annotations

import logging
from pathlib import Path

from deltalake import DeltaTable

from flowfile_core.catalog.delta_utils import (
    is_delta_table,
    read_delta_preview,
    table_exists,
)
from flowfile_core.catalog.exceptions import TableNotFoundError
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.text_utils import parse_delta_history
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_delta_history,
    trigger_delta_version_preview,
)
from flowfile_core.schemas.catalog_schema import (
    CatalogTablePreview,
    DeltaTableHistory,
)
from flowfile_core.utils.arrow_reader import read_top_n

logger = logging.getLogger(__name__)


def _should_offload() -> bool:
    """Return True when heavy I/O should be delegated to the worker process."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    return OFFLOAD_TO_WORKER.value


class TablePreviewService:
    """Owns physical/Delta-versioned table preview fetching and Delta history."""

    def __init__(
        self,
        repo: CatalogRepository,
        tables: TableService,
    ) -> None:
        self.repo = repo
        self._tables = tables

    def get_table_preview(
        self,
        table_id: int,
        limit: int,
        version: int | None = None,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Read the first N rows from a catalog table.

        Virtual tables fall through to ``_get_physical_table_preview`` and
        return an empty preview (no ``file_path``).
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        return self._get_physical_table_preview(table, limit, version)

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
            except (RuntimeError, OSError, ValueError, KeyError):
                logger.warning("Worker delta version preview failed, falling back to local", exc_info=True)

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
            except (RuntimeError, OSError, ValueError, KeyError):
                logger.warning("Worker delta history read failed, falling back to local", exc_info=True)

        delta_table = DeltaTable(table_path, without_files=True)
        raw_history = delta_table.history(limit)
        current_version = delta_table.version()
        history = parse_delta_history(raw_history)
        return DeltaTableHistory(current_version=current_version, history=history)
