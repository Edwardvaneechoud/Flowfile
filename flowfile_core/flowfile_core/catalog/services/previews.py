"""Previews for physical and virtual tables, plus Delta history."""

from __future__ import annotations

import io
import logging
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError
from requests.exceptions import RequestException

from flowfile_core.catalog.compute import uses_worker
from flowfile_core.catalog.delta_utils import (
    is_delta_table,
    read_delta_preview,
    table_exists,
)
from flowfile_core.catalog.exceptions import TableNotFoundError, TableVersionUnavailableError
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import format_pyarrow_preview
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.tables import TableService, _is_managed_table_path
from flowfile_core.catalog.services.virtual_tables import VirtualTableService
from flowfile_core.catalog.storage_backend import (
    CatalogStorageTarget,
    _catalog_table_dir_name,
    _is_cloud_uri,
    resolve_for_namespace,
)
from flowfile_core.catalog.text_utils import parse_delta_history
from flowfile_core.configs.settings import FLOWFILE_COMPUTE_PROTOCOL
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.bounded import count_rows, head_rows
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_delta_history,
    trigger_delta_preview,
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

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ResultRef

logger = logging.getLogger(__name__)


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
        # Post-resolution read: resolution may have re-stamped source_table_versions,
        # and *table* was fetched before it ran.
        versions_hash = self._virtual_tables.fresh_versions_hash(table.id)
        worker_url = registry.worker_url()
        try:
            result = trigger_resolve_virtual_table(
                table.id, lazy_frame.serialize(), versions_hash, worker_url=worker_url
            )
        except RequestException as exc:
            raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {exc}") from exc
        ipc_path = validate_catalog_path(result["ipc_path"], storage.catalog_virtual_results_directory)
        pa_table = read_top_n(str(ipc_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=result.get("row_count"))

    @staticmethod
    def _read_virtual_preview(lazy_frame: pl.LazyFrame, ref: ResultRef | None, limit: int) -> CatalogTablePreview:
        """The first *limit* rows of a virtual table and its row count, read through a backend; no file is opened.

        With *ref*, the producer run's output reference, the backend that holds it samples it, and ``total_rows`` is
        its row count, counted there when the reference carries none. Without one, *lazy_frame* is read through
        ``head_rows`` and ``count_rows`` on the current context's backend.
        """
        if ref is not None:
            backend = registry.backend(ref.backend_id)
            with pa.ipc.open_stream(io.BytesIO(backend.sample(ref, limit))) as reader:
                pa_table = reader.read_all()
            total_rows = ref.row_count if ref.row_count is not None else backend.count(ref)
        else:
            backend = current_context().backend
            pa_table = head_rows(backend, lazy_frame, limit)
            total_rows = count_rows(backend, lazy_frame)
        return format_pyarrow_preview(pa_table, total_rows=total_rows)

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
        if FLOWFILE_COMPUTE_PROTOCOL:
            return self._read_virtual_preview(lazy_frame, None, limit)
        return self._format_virtual_preview(lazy_frame, table, limit)

    def _get_virtual_table_preview(
        self,
        table: CatalogTable,
        limit: int,
        user_id: int | None,
    ) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview of the collected result.

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, the rows are read through a backend (``_read_virtual_preview``) while
        the producer run's reference, when there is one, is still held.
        """
        if FLOWFILE_COMPUTE_PROTOCOL:
            with ExitStack() as stack:
                held = resolve_or_log(
                    lambda: stack.enter_context(
                        self._virtual_tables.held_virtual_flow_table(table.id, user_id=user_id)
                    ),
                    kind="virtual flow table for preview",
                    identifier=table.id,
                )
                if held is None:
                    return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)
                return self._read_virtual_preview(*held, limit)
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
        """Resolve a virtual flow table and return a preview (worker-backed).

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, the rows are read through a backend (``_read_virtual_preview``) while
        the producer run's reference, when there is one, is still held.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        if FLOWFILE_COMPUTE_PROTOCOL:
            with self._virtual_tables.held_virtual_flow_table(table_id, user_id=user_id) as held:
                return self._read_virtual_preview(*held, limit)
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

        if _is_cloud_uri(table.file_path):
            target = resolve_for_namespace(table.namespace_id)
            if version is not None:
                return self._get_delta_version_preview(table.file_path, version, limit, target=target)
            return self._get_delta_preview(table.file_path, limit, total_rows=table.row_count, target=target)

        data_path = Path(table.file_path)
        if not table_exists(data_path):
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        if version is not None and is_delta_table(data_path):
            return self._get_delta_version_preview(str(data_path), version, limit)

        if is_delta_table(data_path):
            pa_table = read_delta_preview(str(data_path), n_rows=limit)
        else:
            pa_table = read_top_n(str(data_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=table.row_count)

    def _get_delta_version_preview(
        self, table_path: str, version: int, limit: int, target: CatalogStorageTarget | None = None
    ) -> CatalogTablePreview:
        """Read a Delta table preview at a specific version, on the worker or in this process.

        The worker reads an object-storage or managed table when catalog I/O goes to it (``uses_worker``); a worker it
        cannot reach is ``ComputeUnavailable``, never a read here instead. An external local table, whose bare name the
        worker's routes cannot resolve, is read here, as its latest preview is.
        """
        storage_payload = target.to_worker_payload() if target else None
        storage_options = (target.storage_options or None) if target else None
        if uses_worker() and (target is not None or _is_managed_table_path(table_path)):
            worker_url = registry.worker_url()
            try:
                return trigger_delta_version_preview(
                    _catalog_table_dir_name(table_path),
                    version,
                    limit,
                    storage=storage_payload,
                    worker_url=worker_url,
                )
            except RequestException as exc:
                raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {exc}") from exc

        try:
            delta_table = DeltaTable(table_path, version=version, storage_options=storage_options)
            pa_table = delta_table.to_pyarrow_dataset().head(limit)
        except (FileNotFoundError, DeltaError) as exc:
            logger.info("Delta version %s unavailable for %s: %s", version, table_path, exc)
            raise TableVersionUnavailableError(version=version) from exc
        return format_pyarrow_preview(pa_table)

    def _get_delta_preview(
        self,
        table_path: str,
        limit: int,
        total_rows: int | None = None,
        target: CatalogStorageTarget | None = None,
    ) -> CatalogTablePreview:
        """Read a Delta table preview (latest version) on the worker when catalog I/O goes to it, else in this process.

        A worker it cannot reach is ``ComputeUnavailable``, never a read here instead. ``total_rows`` stays
        authoritative when known (a page-bounded read never sees the whole table).
        """
        storage_payload = target.to_worker_payload() if target else None
        storage_options = (target.storage_options or None) if target else None
        if uses_worker():
            worker_url = registry.worker_url()
            try:
                preview = trigger_delta_preview(
                    _catalog_table_dir_name(table_path),
                    limit,
                    storage=storage_payload,
                    worker_url=worker_url,
                )
            except RequestException as exc:
                raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {exc}") from exc
            return preview.model_copy(update={"total_rows": total_rows}) if total_rows is not None else preview

        pa_table = pl.scan_delta(table_path, storage_options=storage_options).head(limit).collect().to_arrow()
        return format_pyarrow_preview(pa_table, total_rows=total_rows)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        """Return the version history for a Delta catalog table.

        Read as ``_get_delta_version_preview`` reads: on the worker for an object-storage or managed table when catalog
        I/O goes to it, a worker it cannot reach ``ComputeUnavailable``; an external local table in this process.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        if not table.file_path:
            return DeltaTableHistory(current_version=0, history=[])

        is_cloud = _is_cloud_uri(table.file_path)
        if not is_cloud and not is_delta_table(Path(table.file_path)):
            return DeltaTableHistory(current_version=0, history=[])

        target = resolve_for_namespace(table.namespace_id) if is_cloud else None
        table_path = table.file_path
        storage_payload = target.to_worker_payload() if target else None
        storage_options = (target.storage_options or None) if target else None
        if uses_worker() and (is_cloud or _is_managed_table_path(table_path)):
            worker_url = registry.worker_url()
            try:
                return trigger_delta_history(
                    _catalog_table_dir_name(table_path),
                    limit,
                    storage=storage_payload,
                    worker_url=worker_url,
                )
            except RequestException as exc:
                raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {exc}") from exc

        delta_table = DeltaTable(table_path, without_files=True, storage_options=storage_options)
        raw_history = delta_table.history(limit)
        current_version = delta_table.version()
        history = parse_delta_history(raw_history)
        return DeltaTableHistory(current_version=current_version, history=history)
