"""Catalog tables: registration, lookup, lifecycle, favourites, enrichment."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm.attributes import flag_modified

from flowfile_core.catalog.delta_utils import (
    delete_table_storage,
    get_delta_table_size_bytes,
    is_delta_table,
    is_legacy_parquet,
    table_exists,
)
from flowfile_core.catalog.exceptions import (
    AmbiguousTableError,
    NamespaceNotFoundError,
    TableExistsError,
    TableFavoriteNotFoundError,
    TableNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.schedules import ScheduleService
from flowfile_core.catalog.validators import format_full_name, validate_table_registration
from flowfile_core.database.models import CatalogNamespace, CatalogTable, TableFavorite
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_optimize_catalog_table,
    trigger_read_table_metadata,
    trigger_vacuum_catalog_table,
)
from flowfile_core.schemas.catalog_schema import (
    CatalogTableOut,
    ColumnSchema,
    FlowSummary,
    OptimizeTableResponse,
    VacuumTableResponse,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)


def _is_managed_table_path(file_path: str) -> bool:
    """True when the storage path lives under Flowfile's managed catalog dir.

    External/user-registered paths (registered by pointing at an existing file
    outside the managed dir) return False and must never be deleted.
    """
    try:
        Path(file_path).resolve().relative_to(storage.catalog_tables_directory.resolve())
        return True
    except (ValueError, OSError):
        return False


def _should_offload() -> bool:
    """Return True when heavy I/O should be delegated to the worker process."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    return OFFLOAD_TO_WORKER.value


@dataclass(frozen=True)
class CatalogMaterializationResult:
    table_path: str
    schema: list[dict[str, str]]
    row_count: int
    column_count: int
    size_bytes: int
    storage_format: str = "delta"


class TableService:
    """Owns catalog table CRUD, lookup, registration, favourites and enrichment."""

    def __init__(
        self,
        repo: CatalogRepository,
        namespaces: NamespaceService,
        flows: FlowRegistrationService,
        schedules: ScheduleService,
    ) -> None:
        self.repo = repo
        self._namespaces = namespaces
        self._flows = flows
        self._schedules = schedules

    # ---- Validation + resolution ----------------------------------------- #

    def validate_table_registration(self, name: str, namespace_id: int | None) -> None:
        """Check that the namespace exists and the table name is unique."""
        validate_table_registration(
            name,
            namespace_id,
            namespace_exists=lambda nid: self.repo.get_namespace(nid) is not None,
            table_by_name_exists=lambda n, nid: self.repo.get_table_by_name(n, nid) is not None,
        )

    def resolve_table(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
    ) -> CatalogTable:
        """Resolve a ``"ns.table"`` or bare ``"table"`` reference to a single CatalogTable."""
        if not reference:
            raise TableNotFoundError(name=reference)

        if "." in reference:
            ns_name, _, table_name = reference.partition(".")
            if not ns_name or not table_name:
                raise TableNotFoundError(name=reference)
            return self._resolve_qualified(ns_name, table_name, strict=strict)

        return self._resolve_bare(reference, default_namespace_id, strict=strict)

    def _all_namespaces_named(self, ns_name: str) -> list[CatalogNamespace]:
        roots = self.repo.list_root_namespaces()
        out: list[CatalogNamespace] = list(roots)
        for root in roots:
            out.extend(self.repo.list_child_namespaces(root.id))
        return [namespace for namespace in out if namespace.name == ns_name]

    def _resolve_qualified(self, ns_name: str, table_name: str, *, strict: bool) -> CatalogTable:
        candidates_ns = self._all_namespaces_named(ns_name)
        if not candidates_ns:
            raise NamespaceNotFoundError(name=ns_name)

        tables: list[CatalogTable] = []
        for namespace in candidates_ns:
            t = self.repo.get_table_by_name(table_name, namespace.id)
            if t is not None:
                tables.append(t)
        if not tables:
            raise TableNotFoundError(name=f"{ns_name}.{table_name}")
        if len(tables) == 1:
            return tables[0]
        return self._disambiguate(f"{ns_name}.{table_name}", tables, strict=strict)

    def _resolve_bare(self, name: str, default_namespace_id: int | None, *, strict: bool) -> CatalogTable:
        if default_namespace_id is not None:
            t = self.repo.get_table_by_name(name, default_namespace_id)
            if t is None:
                raise TableNotFoundError(name=name)
            return t
        matches = self.repo.list_tables_by_name(name)
        if not matches:
            raise TableNotFoundError(name=name)
        if len(matches) == 1:
            return matches[0]
        return self._disambiguate(name, matches, strict=strict)

    def _disambiguate(self, reference: str, matches: list[CatalogTable], *, strict: bool) -> CatalogTable:
        candidates = [
            {
                "id": t.id,
                "name": t.name,
                "namespace_id": t.namespace_id,
                "namespace_name": self._namespaces.resolve_namespace_name(t.namespace_id),
            }
            for t in matches
        ]
        if strict:
            raise AmbiguousTableError(name=reference, candidates=candidates)
        picked_candidate, *other_candidates = candidates
        alternatives = ", ".join(
            f"{format_full_name(c['namespace_name'], c['name'])} (id={c['id']})" for c in other_candidates
        )
        logger.warning(
            "Ambiguous table reference '%s' resolved to id=%s (%s). Other candidates: %s",
            reference,
            picked_candidate["id"],
            format_full_name(picked_candidate["namespace_name"], picked_candidate["name"]),
            alternatives,
        )
        return matches[0]

    # ---- Metadata helpers ------------------------------------------------ #

    @staticmethod
    def _read_table_metadata(table_path: str, storage_format: str) -> tuple[list[dict[str, str]], int, int, int]:
        """Read schema, row_count, column_count, size_bytes from a table."""
        if _should_offload():
            try:
                data = trigger_read_table_metadata(Path(table_path).name)
                schema_list = [{"name": c["name"], "dtype": c["dtype"]} for c in data["column_schema"]]
                return schema_list, data["row_count"], data["column_count"], data["size_bytes"]
            except (RuntimeError, OSError, ValueError, KeyError):
                logger.warning("Worker metadata read failed, falling back to local read", exc_info=True)

        path = Path(table_path)

        if storage_format == "delta" or (storage_format is None and is_delta_table(path)):
            from deltalake import DeltaTable

            delta_table = DeltaTable(str(path))
            pa_schema = delta_table.schema().to_arrow()
            schema_list = [{"name": field.name, "dtype": str(field.type)} for field in pa_schema]
            row_count = delta_table.to_pyarrow_dataset().count_rows()
            size_bytes = get_delta_table_size_bytes(path)
        else:
            from pyarrow import dataset as ds

            dataset = ds.dataset(str(path), format="parquet")
            schema_list = [{"name": field.name, "dtype": str(field.type)} for field in dataset.schema]
            row_count = dataset.count_rows()
            size_bytes = path.stat().st_size

        return schema_list, row_count, len(schema_list), size_bytes

    @staticmethod
    def _parse_schema_columns(table: CatalogTable) -> list[ColumnSchema]:
        """Parse the JSON-encoded column schema from a catalog table."""
        if not table.schema_json:
            return []
        try:
            raw = json.loads(table.schema_json)
            return [ColumnSchema(name=c["name"], dtype=c["dtype"]) for c in raw]
        except (json.JSONDecodeError, KeyError, TypeError):
            return []

    @staticmethod
    def _parse_partition_columns(table: CatalogTable) -> list[str] | None:
        """Parse the JSON-encoded partition columns from a catalog table."""
        raw = getattr(table, "partition_columns", None)
        if not raw:
            return None
        try:
            return list(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            return None

    def _resolve_flow_name(self, registration_id: int | None) -> str | None:
        """Look up a flow registration name by id, returning None when absent."""
        if not registration_id:
            return None
        reg = self.repo.get_flow(registration_id)
        return reg.name if reg else None

    def _check_file_exists(self, table: CatalogTable) -> bool:
        """Determine whether the backing data for a table is available."""
        is_virtual = getattr(table, "table_type", "physical") == "virtual"
        if is_virtual:
            return True
        file_exists = table_exists(table.file_path) if table.file_path else False
        if not file_exists:
            logger.warning(
                "Catalog table %s (id=%d) references missing file: %s",
                table.name,
                table.id,
                table.file_path,
            )
        return file_exists

    @staticmethod
    def _compute_laziness_blockers(producer_file_path: str | None) -> list[str] | None:
        """Compute laziness blockers for a virtual table from its producer flow.

        The two ``resolve_or_log`` sites cover the broad set of errors that
        can come out of opening a flow file or running the laziness pass —
        polars/io errors, missing files, malformed YAML.
        """
        if not producer_file_path:
            return None

        from flowfile_core.flowfile.handler import open_flow

        flow_graph = resolve_or_log(
            lambda: open_flow(Path(producer_file_path)),
            kind="producer flow for laziness inspection",
            identifier=producer_file_path,
        )
        if flow_graph is None:
            return None

        result = resolve_or_log(
            lambda: flow_graph.check_flow_laziness(),
            kind="laziness inspection on producer flow",
            identifier=producer_file_path,
        )
        if result is None:
            return None
        _, reasons = result
        return reasons

    # ---- DTO conversion + bulk enrichment -------------------------------- #

    def table_to_out(
        self,
        table: CatalogTable,
        user_id: int | None = None,
        compute_laziness: bool = False,
    ) -> CatalogTableOut:
        columns = self._parse_schema_columns(table)
        source_registration_name = self._resolve_flow_name(table.source_registration_id)
        producer_registration_name = self._resolve_flow_name(table.producer_registration_id)

        readers = self.repo.list_readers_for_table(table.id)
        read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

        is_favorite = False
        if user_id is not None:
            is_favorite = self.repo.get_table_favorite(user_id, table.id) is not None

        file_exists_flag = self._check_file_exists(table)

        is_virtual = getattr(table, "table_type", "physical") == "virtual"
        laziness_blockers: list[str] | None = None
        if compute_laziness and is_virtual and table.producer_registration_id:
            producer = self.repo.get_flow(table.producer_registration_id)
            laziness_blockers = self._compute_laziness_blockers(producer.flow_path if producer else None)

        namespace_name = self._namespaces.resolve_namespace_name(table.namespace_id)
        full_table_name = format_full_name(namespace_name, table.name)

        return CatalogTableOut(
            id=table.id,
            name=table.name,
            namespace_id=table.namespace_id,
            namespace_name=namespace_name,
            full_table_name=full_table_name,
            description=table.description,
            owner_id=table.owner_id,
            file_exists=file_exists_flag,
            is_favorite=is_favorite,
            schema_columns=columns,
            row_count=table.row_count,
            column_count=table.column_count,
            size_bytes=table.size_bytes,
            file_path=table.file_path,
            source_registration_id=table.source_registration_id,
            source_registration_name=source_registration_name,
            source_run_id=table.source_run_id,
            read_by_flows=read_by_flows,
            table_type=getattr(table, "table_type", "physical"),
            producer_registration_id=table.producer_registration_id,
            producer_registration_name=producer_registration_name,
            is_optimized=getattr(table, "is_optimized", None),
            laziness_blockers=laziness_blockers,
            sql_query=getattr(table, "sql_query", None),
            polars_plan=getattr(table, "polars_plan", None),
            source_table_versions=getattr(table, "source_table_versions", None),
            partition_columns=self._parse_partition_columns(table),
            created_at=table.created_at,
            updated_at=table.updated_at,
        )

    def bulk_enrich_tables(self, tables: list[CatalogTable], user_id: int) -> list[CatalogTableOut]:
        """Enrich multiple tables with favorite status in bulk to avoid N+1 queries."""
        if not tables:
            return []

        table_ids = [t.id for t in tables]
        favorite_ids = self.repo.bulk_get_favorite_table_ids(user_id, table_ids)

        ns_name_cache: dict[int, str | None] = {}
        for t in tables:
            if t.namespace_id is not None and t.namespace_id not in ns_name_cache:
                ns_name_cache[t.namespace_id] = self._namespaces.resolve_namespace_name(t.namespace_id)

        result: list[CatalogTableOut] = []
        for table in tables:
            columns = self._parse_schema_columns(table)
            source_registration_name = self._resolve_flow_name(table.source_registration_id)
            producer_registration_name = self._resolve_flow_name(table.producer_registration_id)

            readers = self.repo.list_readers_for_table(table.id)
            read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

            file_exists_flag = self._check_file_exists(table)

            namespace_name = ns_name_cache.get(table.namespace_id) if table.namespace_id is not None else None
            full_table_name = format_full_name(namespace_name, table.name)

            result.append(
                CatalogTableOut(
                    id=table.id,
                    name=table.name,
                    namespace_id=table.namespace_id,
                    namespace_name=namespace_name,
                    full_table_name=full_table_name,
                    description=table.description,
                    owner_id=table.owner_id,
                    file_exists=file_exists_flag,
                    is_favorite=table.id in favorite_ids,
                    schema_columns=columns,
                    row_count=table.row_count,
                    column_count=table.column_count,
                    size_bytes=table.size_bytes,
                    file_path=table.file_path,
                    source_registration_id=table.source_registration_id,
                    source_registration_name=source_registration_name,
                    source_run_id=table.source_run_id,
                    read_by_flows=read_by_flows,
                    table_type=getattr(table, "table_type", "physical"),
                    producer_registration_id=table.producer_registration_id,
                    producer_registration_name=producer_registration_name,
                    is_optimized=getattr(table, "is_optimized", None),
                    sql_query=getattr(table, "sql_query", None),
                    polars_plan=getattr(table, "polars_plan", None),
                    source_table_versions=getattr(table, "source_table_versions", None),
                    partition_columns=self._parse_partition_columns(table),
                    created_at=table.created_at,
                    updated_at=table.updated_at,
                )
            )
        return result

    # ---- Materialisation + registration ---------------------------------- #

    def _materialize_table_with_worker(
        self,
        source_file_path: str,
        table_name: str | None = None,
    ) -> CatalogMaterializationResult:
        # Lazy module lookup so monkeypatches on ``catalog.service.trigger_catalog_materialize``
        # — used by tests — flow through.
        from flowfile_core.catalog import service as _service_module

        response = _service_module.trigger_catalog_materialize(
            source_file_path=source_file_path,
            table_name=table_name,
        )
        if response.ok:
            data = response.json()
            schema = [
                {"name": col["name"], "dtype": col["dtype"]}
                for col in data.get("column_schema", [])
                if "name" in col and "dtype" in col
            ]
            return CatalogMaterializationResult(
                table_path=data["table_path"],
                schema=schema,
                row_count=data["row_count"],
                column_count=data["column_count"],
                size_bytes=data["size_bytes"],
                storage_format="delta",
            )

        detail = None
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            detail = None

        if response.status_code == 422:
            if isinstance(detail, dict) and detail.get("error_type") == "unsupported_file_type":
                raise ValueError(detail.get("message", "Unsupported file type"))
            raise ValueError(detail.get("message", "Unsupported file type") if isinstance(detail, dict) else "")

        if isinstance(detail, dict):
            message = detail.get("message", response.text)
        else:
            message = response.text
        raise RuntimeError(f"Worker catalog materialization failed: {message}")

    def register_table(
        self,
        name: str,
        file_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
    ) -> CatalogTableOut:
        """Register a new table by materializing it as a Delta table."""
        self.validate_table_registration(name, namespace_id)

        materialized = self._materialize_table_with_worker(
            source_file_path=file_path,
            table_name=name,
        )

        return self._create_table_record_from_metadata(
            name=name,
            table_path=materialized.table_path,
            schema=materialized.schema,
            row_count=materialized.row_count,
            column_count=materialized.column_count,
            size_bytes=materialized.size_bytes,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format=materialized.storage_format,
        )

    def register_table_from_data(
        self,
        name: str,
        table_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
        storage_format: str = "delta",
        schema: list[dict[str, str]] | None = None,
        row_count: int | None = None,
        column_count: int | None = None,
        size_bytes: int | None = None,
        partition_columns: list[str] | None = None,
    ) -> CatalogTableOut:
        """Register an already-materialized table (Delta or Parquet) in the catalog."""
        self.validate_table_registration(name, namespace_id)

        if schema is not None and row_count is not None and size_bytes is not None:
            schema_list = schema
            if column_count is None:
                column_count = len(schema_list)
        else:
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(table_path, storage_format)

        return self._create_table_record_from_metadata(
            name=name,
            table_path=table_path,
            schema=schema_list,
            row_count=row_count,
            column_count=column_count,
            size_bytes=size_bytes,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format=storage_format,
            partition_columns=partition_columns,
        )

    def register_table_from_parquet(
        self,
        name: str,
        parquet_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
    ) -> CatalogTableOut:
        """Backward-compatible alias for ``register_table_from_data``."""
        return self.register_table_from_data(
            name=name,
            table_path=parquet_path,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format="parquet",
        )

    def overwrite_table_data(
        self,
        table_id: int,
        table_path: str | None = None,
        parquet_path: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
        description: str | None = None,
        storage_format: str | None = None,
        schema: list[dict[str, str]] | None = None,
        row_count: int | None = None,
        column_count: int | None = None,
        size_bytes: int | None = None,
        partition_columns: list[str] | None = None,
    ) -> CatalogTableOut:
        """Replace the data of an existing catalog table **in-place**."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        resolved_path_str = table_path or parquet_path
        dest_path = Path(resolved_path_str)

        if storage_format is None:
            storage_format = "delta" if is_delta_table(dest_path) else "parquet"

        if schema is not None and row_count is not None and size_bytes is not None:
            schema_list = schema
            if column_count is None:
                column_count = len(schema_list)
        else:
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(str(dest_path), storage_format)

        old_path = Path(table.file_path)
        if old_path != dest_path and old_path.exists():
            try:
                delete_table_storage(old_path)
            except OSError:
                logger.warning("Failed to delete old table storage %s", old_path, exc_info=True)

        table.file_path = str(dest_path)
        table.storage_format = storage_format
        table.schema_json = json.dumps(schema_list)
        table.row_count = row_count
        table.column_count = column_count
        table.size_bytes = size_bytes
        if partition_columns is not None:
            table.partition_columns = json.dumps(partition_columns) if partition_columns else None
        if source_registration_id is not None:
            table.source_registration_id = source_registration_id
        if source_run_id is not None:
            table.source_run_id = source_run_id
        if description is not None:
            table.description = description

        table = self.repo.update_table(table)

        self._schedules.safely_fire_table_trigger_schedules(table.id, table.updated_at)

        return self.table_to_out(table)

    def _create_table_record_from_metadata(
        self,
        name: str,
        table_path: str,
        schema: list[dict[str, str]],
        row_count: int,
        column_count: int,
        size_bytes: int,
        owner_id: int,
        namespace_id: int | None,
        description: str | None,
        source_registration_id: int | None,
        source_run_id: int | None,
        storage_format: str = "delta",
        partition_columns: list[str] | None = None,
    ) -> CatalogTableOut:
        table = CatalogTable(
            name=name,
            namespace_id=namespace_id,
            description=description,
            owner_id=owner_id,
            file_path=table_path,
            storage_format=storage_format,
            schema_json=json.dumps(schema),
            row_count=row_count,
            column_count=column_count,
            size_bytes=size_bytes,
            partition_columns=json.dumps(partition_columns) if partition_columns else None,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
        )
        table = self.repo.create_table(table)
        return self.table_to_out(table)

    # ---- Delta maintenance (optimize / vacuum) --------------------------- #

    def _require_delta_table_path(self, table: CatalogTable) -> Path:
        """Guard: ensure *table* is a physical on-disk Delta table, else raise ``ValueError``."""
        if getattr(table, "table_type", "physical") == "virtual" or not table.file_path:
            raise ValueError(f"Table '{table.name}' is virtual and has no Delta storage to maintain")
        path = Path(table.file_path)
        if table.storage_format != "delta" or is_legacy_parquet(path) or not is_delta_table(path):
            raise ValueError(f"Table '{table.name}' is not a Delta table and cannot be optimized or vacuumed")
        return path

    def _update_table_size(self, table: CatalogTable, size_bytes: int | None) -> None:
        """Persist a recomputed size after a maintenance op."""
        if size_bytes is None:
            return
        table.size_bytes = size_bytes
        # Maintenance must not look like a data change: table-trigger schedules fire on
        # updated_at, so force the current value into the UPDATE to suppress onupdate.
        flag_modified(table, "updated_at")
        self.repo.update_table(table)

    def optimize_table(self, table_id: int, z_order_columns: list[str] | None = None) -> OptimizeTableResponse:
        """Compact (and optionally Z-order) a Delta catalog table, refreshing its size."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        data_path = self._require_delta_table_path(table)

        if z_order_columns:
            valid = {c.name for c in self._parse_schema_columns(table)}
            missing = [c for c in z_order_columns if c not in valid]
            if missing:
                raise ValueError(f"z_order_columns not in table schema: {missing}")

        if _should_offload():
            result = trigger_optimize_catalog_table(data_path.name, z_order_columns)
            metrics, size_bytes = result.get("metrics", {}), result.get("size_bytes")
        else:
            from shared.delta_utils import optimize_delta

            metrics = optimize_delta(str(data_path), z_order_columns=z_order_columns or None)
            size_bytes = get_delta_table_size_bytes(data_path)

        self._update_table_size(table, size_bytes)
        return OptimizeTableResponse(metrics=metrics, size_bytes=size_bytes)

    def vacuum_table(
        self,
        table_id: int,
        retention_hours: int = 168,
        dry_run: bool = True,
    ) -> VacuumTableResponse:
        """Vacuum tombstoned files from a Delta catalog table; refresh size on a real run."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        data_path = self._require_delta_table_path(table)

        if _should_offload():
            result = trigger_vacuum_catalog_table(data_path.name, retention_hours, dry_run)
            files, size_bytes = result.get("files_removed", []), result.get("size_bytes")
        else:
            from shared.delta_utils import vacuum_delta

            files = vacuum_delta(str(data_path), retention_hours=retention_hours, dry_run=dry_run)
            size_bytes = get_delta_table_size_bytes(data_path)

        if not dry_run:
            self._update_table_size(table, size_bytes)
        return VacuumTableResponse(
            dry_run=dry_run,
            files_removed=list(files),
            file_count=len(files),
            size_bytes=size_bytes,
        )

    # ---- Path resolution ------------------------------------------------- #

    def resolve_write_destination(
        self,
        table_name: str,
        namespace_id: int | None,
        write_mode: str,
        catalog_dir: Path,
    ) -> tuple[CatalogTable | None, Path, str]:
        """Resolve the destination path and Delta write mode for a catalog write."""
        existing = self.repo.get_table_by_name(table_name, namespace_id)

        if existing is not None:
            if write_mode == "error":
                raise TableExistsError(name=table_name, namespace_id=namespace_id)

            old_path = Path(existing.file_path)
            if is_delta_table(old_path):
                return existing, old_path, write_mode

            new_dir = old_path.parent / old_path.stem
            return existing, new_dir, write_mode

        dir_name = f"{table_name}_{uuid4().hex[:8]}"
        return None, catalog_dir / dir_name, write_mode

    def resolve_table_file_path(
        self,
        table_id: int | None = None,
        table_name: str | None = None,
        namespace_id: int | None = None,
    ) -> str | None:
        """Resolve a catalog table's file path by ID or by name + namespace."""
        if table_id is not None:
            table = self.repo.get_table(table_id)
            if table is not None:
                return table.file_path
        elif table_name:
            table = self.repo.get_table_by_name(table_name, namespace_id)
            if table is not None:
                return table.file_path
        return None

    # ---- CRUD ------------------------------------------------------------ #

    def get_table(self, table_id: int, user_id: int | None = None) -> CatalogTableOut:
        """Get a catalog table by ID."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        return self.table_to_out(table, user_id=user_id)

    def resolve_table_out(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
        user_id: int | None = None,
    ) -> tuple[CatalogTableOut, list[dict]]:
        """Resolve a reference and return its DTO plus ambiguity warnings."""
        warnings: list[dict] = []
        if not strict and "." not in reference and default_namespace_id is None:
            matches = self.repo.list_tables_by_name(reference)
            if len(matches) > 1:
                warnings = [
                    {
                        "id": t.id,
                        "name": t.name,
                        "namespace_id": t.namespace_id,
                        "namespace_name": self._namespaces.resolve_namespace_name(t.namespace_id),
                    }
                    for t in matches
                ]
        table = self.resolve_table(reference, default_namespace_id=default_namespace_id, strict=strict)
        return self.table_to_out(table, user_id=user_id), warnings

    def list_tables(self, namespace_id: int | None = None, user_id: int | None = None) -> list[CatalogTableOut]:
        """List tables, optionally filtered by namespace."""
        tables = self.repo.list_tables(namespace_id=namespace_id)
        if user_id is not None:
            return self.bulk_enrich_tables(tables, user_id)
        return [self.table_to_out(t) for t in tables]

    def update_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> CatalogTableOut:
        """Update a catalog table's metadata."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        if name is not None:
            table.name = name
        if description is not None:
            table.description = description
        if namespace_id is not None:
            table.namespace_id = namespace_id
        table = self.repo.update_table(table)
        return self.table_to_out(table)

    def delete_table(self, table_id: int, delete_file: bool = False) -> None:
        """Delete a catalog table; optionally delete its materialized storage.

        Storage is only removed when ``delete_file`` is set AND the path is
        Flowfile-managed (under the catalog tables dir) — external/user-owned
        files are never touched. Virtual tables have no file to delete.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        file_path = table.file_path
        self.repo.delete_table(table_id)

        if delete_file and file_path and _is_managed_table_path(file_path):
            try:
                storage_path = Path(file_path)
                if storage_path.exists():
                    delete_table_storage(storage_path)
            except OSError:
                logger.warning("Failed to delete materialized storage %s", file_path, exc_info=True)

    # ---- Favourites ------------------------------------------------------ #

    def add_table_favorite(self, user_id: int, table_id: int) -> TableFavorite:
        """Add a table to user's favourites (idempotent)."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        existing = self.repo.get_table_favorite(user_id, table_id)
        if existing is not None:
            return existing
        favorite = TableFavorite(user_id=user_id, table_id=table_id)
        return self.repo.add_table_favorite(favorite)

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        """Remove a table from user's favourites."""
        existing = self.repo.get_table_favorite(user_id, table_id)
        if existing is None:
            raise TableFavoriteNotFoundError(user_id=user_id, table_id=table_id)
        self.repo.remove_table_favorite(user_id, table_id)

    def list_table_favorites(self, user_id: int) -> list[CatalogTableOut]:
        """List all tables the user has favourited, enriched."""
        favorites = self.repo.list_table_favorites(user_id)
        tables: list[CatalogTable] = []
        for favorite in favorites:
            table = self.repo.get_table(favorite.table_id)
            if table is not None:
                tables.append(table)
        return self.bulk_enrich_tables(tables, user_id)
