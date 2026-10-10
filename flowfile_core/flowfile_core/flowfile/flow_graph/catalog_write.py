"""Catalog writer execution: SCD2 shaping, Delta writes, table registration and the write handlers."""

import datetime
import io as _io
import json
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import polars as pl

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import (
    delete_table_storage,
    is_delta_table,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import _is_cloud_uri, resolve_for_namespace
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalDfFetcher,
)
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.catalog_resolution import (
    _authorize_catalog_write,
    _effective_namespace_id,
    _resolve_catalog_sql_tables,
)
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.catalog_schema import TableWriteMetadata
from flowfile_core.schemas.delta_write import MERGE_MODES
from shared.delta_utils import (
    enable_change_data_feed,
    get_delta_partition_columns,
    get_delta_size_bytes,
    merge_into_delta,
    scd2_into_delta,
    scd2_parse_iso_utc,
)
from shared.delta_utils import write_delta as _write_delta

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


def _scd2_primitive_kwargs(scd2_config: dict, run_timestamp: str) -> dict:
    """Map a resolved SCD2 catalog config onto ``scd2_into_delta``'s keyword names.

    Both branches of a write are built from this one mapping — the local writer forwards it to the
    primitive verbatim, the worker branch re-spells only ``valid_from_iso`` as ``run_timestamp``
    (the worker's parameter name for the same instant) and adds the transport fields. So the local
    and remote branches of the same write cannot disagree about what was compared or how the
    generated columns are named.
    """
    return {
        "business_keys": scd2_config["business_keys"],
        "valid_from_iso": run_timestamp,
        "compare_columns": scd2_config["compare_columns"],
        "full_snapshot": scd2_config["full_snapshot"],
        "surrogate_key_column": scd2_config["surrogate_key_column"],
        "valid_from_column": scd2_config["valid_from_column"],
        "valid_to_column": scd2_config["valid_to_column"],
        "is_current_column": scd2_config["is_current_column"],
        # .get: persisted-shape configs never carry the creation-time knob.
        "partition_on_current": scd2_config.get("partition_on_current", True),
    }


def _scd2_row_filter(
    cfg: dict | None,
    view: str | None,
    as_of: str | None,
    *,
    node_logger: NodeLogger,
) -> pl.Expr | None:
    """Build the lazy row filter for an SCD2 history view, or ``None`` for an unfiltered scan.

    Column names come from *cfg* — the catalog table's own persisted SCD2 record — never from the
    reader node's settings. Per the product decision, ``None``/``"all"`` mean no implicit filter
    (every version, the reader default); ``"active"`` filters to current rows; ``"active_at"``
    applies a half-open validity interval. A view requested against a table with no SCD2 record is
    ignored with a warning rather than raised: retargeting a reader to a plain table, or predicting
    its schema, must never fail the flow.
    """
    if cfg is None:
        if view is not None:
            node_logger.warning(f"Ignoring scd2_view={view!r}: the target catalog table is not an SCD2 table")
        return None
    if view in (None, "all"):
        return None
    valid_from = cfg["valid_from_column"]
    valid_to = cfg["valid_to_column"]
    if view == "active":
        return pl.col(valid_to).is_null()
    # Half-open [valid_from, valid_to); "Z" normalized for 3.10's fromisoformat, naive instants are UTC.
    ts = pl.lit(datetime.datetime.fromisoformat(as_of.replace("Z", "+00:00"))).cast(pl.Datetime("us", time_zone="UTC"))
    return (pl.col(valid_from) <= ts) & (pl.col(valid_to).is_null() | (pl.col(valid_to) > ts))


_SCD2_SYSTEM_DTYPES = (pl.String, pl.Datetime("us", "UTC"), pl.Datetime("us", "UTC"), pl.Boolean)


def _scd2_system_column_dtypes(cfg) -> dict[str, pl.DataType]:
    """The four generated columns in canonical order, mapped to the dtypes ``_scd2_stamp`` writes.

    *cfg* is either a resolved SCD2 config dict or an ``Scd2Settings``, so the run-time output and
    the design-time schema callback derive the same names and dtypes from the same place.
    """
    if isinstance(cfg, dict):
        names = [cfg[f"{k}_column"] for k in ("surrogate_key", "valid_from", "valid_to", "is_current")]
    else:
        names = cfg.system_columns
    return dict(zip(names, _SCD2_SYSTEM_DTYPES, strict=True))


def _scd2_output_projection(output_dtypes: dict[str, pl.DataType], table_schema) -> list[pl.Expr]:
    """Project a table read onto the writer's declared output schema.

    A column the table does not carry becomes a typed NULL and a wider stored dtype is narrowed
    back to the input's, so the frame leaving the node matches the schema the canvas advertised
    before the run whatever the table's own history looks like.
    """
    exprs: list[pl.Expr] = []
    for name, dtype in output_dtypes.items():
        if name not in table_schema:
            exprs.append(pl.lit(None).cast(dtype).alias(name))
        elif table_schema[name] != dtype:
            exprs.append(pl.col(name).cast(dtype, strict=False))
        else:
            exprs.append(pl.col(name))
    return exprs


def _scd2_writer_output(
    df: FlowDataEngine,
    dest_path: str,
    scd2_config: dict,
    output_mode: str,
    version: int | None,
    run_timestamp: str,
    storage_options: dict[str, str] | None,
) -> FlowDataEngine:
    """The frame an SCD2 catalog writer emits downstream, read back at the version it settled on.

    All three modes yield the input's columns in input order followed by the four generated
    columns, so the node's advertised schema holds whichever mode is selected. Pinning the read to
    *version* is what makes a concurrent writer unable to change what this run passes on. The plan
    stays lazy — core never materialises it.
    """
    scan_kwargs = CloudStorageReader.get_secure_scan_kwargs(storage_options, None)
    if version is not None:
        scan_kwargs["version"] = version
    system_dtypes = _scd2_system_column_dtypes(scd2_config)
    input_dtypes = dict(df.data_frame.collect_schema())
    output_dtypes = {**input_dtypes, **system_dtypes}
    is_current = scd2_config["is_current_column"]
    table = pl.scan_delta(str(dest_path), **scan_kwargs)

    if output_mode == "input":
        keys = list(scd2_config["business_keys"])
        current = (
            table.filter(pl.col(is_current))
            .select(keys + list(system_dtypes))
            # Cast keys back to the input's dtype (Int32 stored as Int64) or the join matches nothing.
            .with_columns(pl.col(k).cast(input_dtypes[k], strict=False) for k in keys)
        )
        joined = df.data_frame.join(current, on=keys, how="left", maintain_order="left")
        return FlowDataEngine(joined.select(list(output_dtypes)))

    if output_mode == "changed":
        ts = pl.lit(scd2_parse_iso_utc(run_timestamp)).cast(pl.Datetime("us", "UTC"))
        valid_from = scd2_config["valid_from_column"]
        valid_to = scd2_config["valid_to_column"]
        table = table.filter((pl.col(valid_from) == ts) | (pl.col(valid_to) == ts))
    else:
        table = table.filter(pl.col(is_current))
    return FlowDataEngine(table.select(_scd2_output_projection(output_dtypes, table.collect_schema())))


class CatalogDeltaWrite(NamedTuple):
    """What one catalog Delta write produced.

    *meta* is ``None`` when the write was skipped (the catalog no-op protocol). *scd2_version* is
    the Delta version an SCD2 write settled on — committed, or classified against on a skip — and
    is ``None`` for every other write mode.
    """

    meta: TableWriteMetadata | None
    scd2_version: int | None = None


def _write_catalog_delta_local(
    df: FlowDataEngine,
    dest_path: str | Path,
    delta_mode: str,
    merge_keys: list[str] | None,
    partition_by: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
    scd2_kwargs: dict | None = None,
    enable_cdf: bool = False,
) -> CatalogDeltaWrite:
    """Write a Delta table in-process. ``meta`` is ``None`` when the write was skipped.

    *storage_options* (passed verbatim, ``None`` ⇒ local filesystem) routes the write to object
    storage so standalone CLI/scheduler runs — which have no worker to offload to — can still write
    cloud catalog tables. An empty dict means ambient credentials, so it must reach the delta calls
    as-is (the downstream helpers branch on ``is None``, not truthiness).

    *scd2_kwargs* carries the resolved SCD2 configuration (see ``_scd2_primitive_kwargs``) and is
    required when *delta_mode* is ``"scd2"``. *enable_cdf* turns change tracking on for a table
    this write creates; an existing one is handled by ``_ensure_catalog_cdc_enabled`` afterwards.
    """
    dest = str(dest_path)
    if delta_mode == "scd2":
        # The sanctioned local collect: SCD2 classification needs rows and a CLI run has no worker.
        result = scd2_into_delta(
            df.data_frame.collect(),
            dest,
            partition_by=partition_by,
            storage_options=storage_options,
            **(scd2_kwargs or {}),
        )
        if result.skipped:
            return CatalogDeltaWrite(None, result.version)
        # An SCD2 table's row count is its whole history, so the post-write scan is the only truth.
        after = pl.scan_delta(dest, **({} if storage_options is None else {"storage_options": storage_options}))
        after_schema = after.collect_schema()
        return CatalogDeltaWrite(
            {
                "schema": [{"name": n, "dtype": str(d)} for n, d in after_schema.items()],
                "row_count": result.rows_total,
                "column_count": len(after_schema),
                "size_bytes": get_delta_size_bytes(dest_path, storage_options=storage_options),
                "scd2_metrics": {
                    "rows_inserted": result.rows_inserted,
                    "rows_closed": result.rows_closed,
                    "rows_total": result.rows_total,
                    "rows_current": result.rows_current,
                    "created": result.created,
                },
            },
            result.version,
        )
    if delta_mode in ("upsert", "update", "delete"):
        wrote = merge_into_delta(
            df.data_frame.collect(),
            dest,
            merge_mode=delta_mode,
            merge_keys=merge_keys,
            partition_by=partition_by,
            storage_options=storage_options,
            enable_cdf=enable_cdf,
        )
    else:
        wrote = _write_delta(
            df.data_frame,
            dest,
            mode=delta_mode,
            partition_by=partition_by,
            storage_options=storage_options,
            enable_cdf=enable_cdf,
        )
    if not wrote:
        return CatalogDeltaWrite(None)
    return CatalogDeltaWrite(
        {
            "schema": [{"name": c.column_name, "dtype": c.data_type} for c in df.schema],
            "row_count": df.count(),
            "column_count": df.number_of_fields,
            "size_bytes": get_delta_size_bytes(dest_path, storage_options=storage_options),
        }
    )


def _write_catalog_delta_remote(
    flow_id: int,
    node: FlowNode,
    df: FlowDataEngine,
    op_type: str,
    op_kwargs: dict,
    table_label: str,
) -> CatalogDeltaWrite:
    """Write a Delta table via the worker service. ``meta`` is ``None`` when the write was skipped.

    *table_label* names the table in the error message, quoted as it should appear there.
    """
    fetcher = ExternalDfFetcher(
        flow_id=flow_id,
        node_id=node.node_id,
        lf=df.data_frame,
        wait_on_completion=False,
        operation_type=op_type,
        kwargs=op_kwargs,
    )
    node._fetch_cached_df = fetcher
    try:
        result = fetcher.get_result()
    except Exception as e:
        raise RuntimeError(f"Worker failed to write delta table {table_label}: {e}") from e
    scd2_version = result.get("version") if isinstance(result, dict) else None
    if isinstance(result, dict) and result.get("skipped"):
        return CatalogDeltaWrite(None, scd2_version)
    meta: TableWriteMetadata = {}
    if isinstance(result, dict):
        # Exactly these four keys reach the catalog service as **meta_kwargs (explicit keywords only).
        meta = {k: result.get(k) for k in ("schema", "row_count", "column_count", "size_bytes")}
        if result.get("scd2_metrics"):
            meta["scd2_metrics"] = result["scd2_metrics"]
    return CatalogDeltaWrite(meta, scd2_version)


def _delta_op(
    delta_mode: str,
    *,
    output_path: str,
    merge_keys: list[str],
    partition_by: list[str] | None,
    enable_cdf: bool,
) -> tuple[str, dict]:
    """The worker operation and kwargs for a non-SCD2 Delta write: ``merge_delta`` or ``write_delta``."""
    if delta_mode in MERGE_MODES:
        return "merge_delta", {
            "output_path": output_path,
            "merge_mode": delta_mode,
            "merge_keys": merge_keys,
            "partition_by": partition_by,
            "enable_cdf": enable_cdf,
        }
    return "write_delta", {
        "output_path": output_path,
        "mode": delta_mode,
        "partition_by": partition_by,
        "enable_cdf": enable_cdf,
    }


_SCD2_DRIFT_FIELDS = (
    "business_keys",
    "surrogate_key_column",
    "valid_from_column",
    "valid_to_column",
    "is_current_column",
)


def _resolve_scd2_config(
    settings: input_schema.CatalogWriteSettings,
    df: FlowDataEngine,
    existing,
) -> dict:
    """Resolve the SCD2 config for this write and refuse a target it cannot maintain.

    ``compare_columns`` is resolved here — in core, from the node's *schema*, never its data — so
    core, the worker and the persisted catalog record can never disagree about what was compared.
    An existing target must already be SCD2-tracked with the same business key and generated column
    names: converting a plain table in place would fabricate history that was never recorded, and
    renaming a key or a generated column mid-stream would silently orphan every prior version.
    """
    cfg = settings.scd2 or input_schema.Scd2Settings()
    system = set(cfg.system_columns)
    keys = list(settings.merge_keys)
    frame_cols = [c.column_name for c in df.schema]
    compare = list(cfg.compare_columns) or [c for c in frame_cols if c not in set(keys) and c not in system]
    if not compare:
        raise ValueError(
            "SCD2 write has no columns to compare: every input column is a business key. "
            "Add at least one non-key column, or narrow the business key."
        )
    missing = [c for c in (*keys, *compare) if c not in frame_cols]
    if missing:
        raise ValueError(f"SCD2 write references columns absent from the input: {missing}")

    resolved = {
        "business_keys": keys,
        "surrogate_key_column": cfg.surrogate_key_column,
        "valid_from_column": cfg.valid_from_column,
        "valid_to_column": cfg.valid_to_column,
        "is_current_column": cfg.is_current_column,
        "compare_columns": compare,
        "full_snapshot": cfg.full_snapshot,
        # Creation-time only — threaded to the primitive, filtered out of the persisted record.
        "partition_on_current": cfg.partition_on_current,
    }
    if existing is None:
        return resolved

    raw = getattr(existing, "scd2_config", None)
    if not raw:
        raise ValueError(
            f"Catalog table '{existing.name}' already exists and is not an SCD2 table. "
            f"Flowfile will not convert it in place — write to a new table name, or delete the "
            f"existing table first."
        )
    try:
        prior = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Catalog table '{existing.name}' has an unreadable SCD2 configuration; "
            f"delete the table and recreate it with an SCD2 write."
        ) from exc
    drifted = {k: (prior.get(k), resolved[k]) for k in _SCD2_DRIFT_FIELDS if prior.get(k) != resolved[k]}
    if drifted:
        raise ValueError(
            f"SCD2 settings do not match the existing table '{existing.name}': {drifted}. "
            f"Changing the business key or a generated column name would corrupt the history."
        )
    return resolved


def _reject_non_scd2_write_to_scd2_table(settings: input_schema.CatalogWriteSettings, existing) -> None:
    """Refuse an incremental write onto an SCD2-tracked table.

    ``append``/``upsert``/``update``/``delete`` would leave rows in the table that carry no
    surrogate key and no validity window, so every history read would silently be wrong. A plain
    ``overwrite`` is allowed: it replaces the data wholesale and clears the SCD2 flag.
    """
    if settings.write_mode not in ("append", "upsert", "update", "delete"):
        return
    if existing is None or not getattr(existing, "scd2_config", None):
        return
    raise ValueError(
        f"Catalog table '{existing.name}' is SCD2-tracked: a '{settings.write_mode}' write would "
        f"break its history. Use the 'scd2' write mode, or overwrite the table to rebuild it as a "
        f"normal table."
    )


def _register_catalog_table(
    existing,
    dest_path: str | Path,
    settings: input_schema.CatalogWriteSettings,
    source_registration_id: int | None,
    user_id: int,
    meta_kwargs: TableWriteMetadata,
    storage_options: dict[str, str] | None = None,
    is_cloud: bool = False,
    scd2_config: dict | None = None,
) -> None:
    """Register or update the catalog table entry, cleaning up orphaned storage on failure for new tables.

    For cloud targets *dest_path* is an object-storage URI; local-only probes are skipped
    and cloud orphans are not auto-cleaned.
    """
    if is_cloud:
        partition_columns = get_delta_partition_columns(dest_path, storage_options=storage_options)
    else:
        partition_columns = get_delta_partition_columns(dest_path) if is_delta_table(dest_path) else []
    if scd2_config is not None:
        # Creation-time knobs stay out of the persisted record; readers only need the shape.
        scd2_config = {k: v for k, v in scd2_config.items() if k != "partition_on_current"}
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            if existing is not None:
                svc.overwrite_table_data(
                    table_id=existing.id,
                    table_path=str(dest_path),
                    source_registration_id=source_registration_id,
                    description=settings.description,
                    storage_format="delta",
                    partition_columns=partition_columns,
                    scd2_config=scd2_config,
                    **meta_kwargs,
                )
            else:
                svc.register_table_from_data(
                    name=settings.table_name,
                    table_path=str(dest_path),
                    owner_id=user_id,
                    namespace_id=_effective_namespace_id(svc, settings),
                    description=settings.description,
                    source_registration_id=source_registration_id,
                    storage_format="delta",
                    partition_columns=partition_columns,
                    scd2_config=scd2_config,
                    **meta_kwargs,
                )
    except Exception:
        if existing is None and not is_cloud and Path(dest_path).exists():
            try:
                delete_table_storage(Path(dest_path))
            except OSError:
                logger.warning("Failed to clean up orphan table %s", dest_path, exc_info=True)
        raise

    old_path = getattr(existing, "file_path", None) if existing is not None else None
    if old_path and str(old_path) != str(dest_path) and not _is_cloud_uri(str(old_path)) and is_delta_table(old_path):
        # SCD2 rebuilt into a fresh directory; the superseded local dir is unreferenced (cloud objects stay).
        try:
            delete_table_storage(Path(old_path))
        except OSError:
            logger.warning("Failed to remove replaced table directory %s", old_path, exc_info=True)


def _ensure_catalog_cdc_enabled(
    table_name: str,
    namespace_id: int | None,
    dest_path: str,
    storage_options: dict[str, str] | None,
) -> None:
    """Mirror the Delta change-data-feed property onto the catalog row after a tracked write.

    A table this write created already carries the property (the writer passed ``enable_cdf``);
    an existing one gets it here, as its own commit. The recorded version is the cursor floor.
    Failures are logged, not raised: the data is already committed, and a failed enable surfaces
    as the reader's actionable "enable change tracking first" error.
    """
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.get_table_by_name(table_name, namespace_id)
            if table is None or table.cdc_enabled:
                return
            enabled_version = enable_change_data_feed(dest_path, storage_options=storage_options)
            repo.set_cdc_enabled(table.id, enabled_version)
    except Exception:
        logger.warning("Could not enable change tracking on catalog table %s", table_name, exc_info=True)


def _collect_source_table_versions(graph: "FlowGraph") -> str | None:
    """Collect delta versions of upstream catalog tables used by this flow.

    For each catalog_reader node that reads a physical delta table, records
    the current delta version. For optimized virtual table sources, includes
    their transitive source_table_versions.

    Complete-or-nothing: returns None as soon as any catalog source can't be
    fingerprinted (SQL reader, missing record, virtual source without its own
    versions, cloud/non-delta path, probe failure). A partial fingerprint would
    let version checks pass while the unfingerprinted source changed, so plan
    replay and the worker's IPC cache would serve stale data. None degrades to
    fall-back-to-flow-execution / always-rebuild, which is correct.

    A flow with no catalog sources at all returns "[]" — provably complete
    (the serialized plan embeds all its data), so plan replay stays valid.

    Returns a JSON string of SourceTableVersion entries ("[]" when none), or
    None when unfingerprintable.
    """
    from deltalake import DeltaTable as _DeltaTable

    from shared.delta_models import SourceTableVersion

    versions: list[SourceTableVersion] = []
    seen_table_ids: set[int] = set()

    table_ids: list[int] = []
    for node in graph.nodes:
        if node.node_type != "catalog_reader":
            continue
        setting = node.setting_input
        if getattr(setting, "sql_query", None):
            return None
        table_id = getattr(setting, "catalog_table_id", None)
        if not table_id or table_id in seen_table_ids:
            continue
        seen_table_ids.add(table_id)
        table_ids.append(table_id)

    # Single DB session for all lookups
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            for table_id in table_ids:
                table_record = repo.get_table(table_id)
                if table_record is None:
                    return None
                if table_record.table_type == "virtual":
                    # Include transitive versions from optimized virtual sources
                    if not table_record.source_table_versions:
                        return None
                    existing = json.loads(table_record.source_table_versions)
                    for entry in existing:
                        sv = SourceTableVersion(**entry)
                        if sv.table_id not in seen_table_ids:
                            seen_table_ids.add(sv.table_id)
                            versions.append(sv)
                elif table_record.file_path and is_delta_table(table_record.file_path):
                    try:
                        current_version = _DeltaTable(table_record.file_path, without_files=True).version()
                        versions.append(
                            SourceTableVersion(
                                table_id=table_id,
                                file_path=table_record.file_path,
                                version=current_version,
                            )
                        )
                    except Exception:
                        logger.warning("Could not read delta version for source table %d", table_id, exc_info=True)
                        return None
                else:
                    return None
    except Exception:
        logger.warning("Could not collect source table versions", exc_info=True)
        return None

    return json.dumps([v.model_dump() for v in versions])


def _virtual_sources_use_cloud(graph: "FlowGraph") -> bool:
    """Return ``True`` when any catalog source feeding this flow lives in object storage.

    Such a flow can't cache a serialized plan: it would freeze the source's decrypted cloud
    credentials. Detection is DB-only (no object-storage I/O).
    """
    physical_ids: list[int] = []
    seen: set[int] = set()
    for node in graph.nodes:
        if node.node_type != "catalog_reader":
            continue
        setting = node.setting_input
        if getattr(setting, "sql_query", None):
            try:
                resolved = _resolve_catalog_sql_tables(setting.node_id, getattr(setting, "user_id", None))
                if any(_is_cloud_uri(path) for path in resolved.table_paths.values()):
                    return True
            except Exception:
                logger.warning("Could not resolve catalog SQL sources; treating as cloud", exc_info=True)
                return True
            continue
        table_id = getattr(setting, "catalog_table_id", None)
        if table_id and table_id not in seen:
            seen.add(table_id)
            physical_ids.append(table_id)

    if not physical_ids:
        return False
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            for table_id in physical_ids:
                record = repo.get_table(table_id)
                if record is not None and record.file_path and _is_cloud_uri(record.file_path):
                    return True
    except Exception:
        logger.warning("Could not determine catalog cloud sources; treating as cloud", exc_info=True)
        return True
    return False


def _handle_virtual_table_write(
    graph: "FlowGraph",
    node_catalog_writer: input_schema.NodeCatalogWriter,
    df: FlowDataEngine,
) -> FlowDataEngine:
    """Handle virtual-mode catalog write: register a virtual table without materializing data."""
    settings = node_catalog_writer.catalog_write_settings
    reg_id = graph._flow_settings.source_registration_id
    if not reg_id:
        # Python-built flows have no registration: auto-register under "General > Python Editor".
        try:
            from flowfile_core.flowfile.catalog_helpers import register_python_editor_flow

            reg_id = register_python_editor_flow(
                graph,
                user_id=node_catalog_writer.user_id,
            )
        except Exception:
            import traceback

            graph.flow_logger.warning(f"Auto-registration for virtual catalog write failed:\n{traceback.format_exc()}")
            reg_id = None
    if not reg_id:
        raise ValueError(
            "Cannot create a virtual table: this flow is not linked to a catalog registration. "
            "Open the flow from the catalog, or register it first via "
            "flowfile_frame.register_flow_with_catalog(...)."
        )

    serialized_lf: bytes | None = None
    polars_plan: str | None = None
    source_table_versions: str | None = None
    changed_execution_mode = False

    writer_node = graph.get_node(node_catalog_writer.node_id)
    is_lazy, _reasons = writer_node.check_upstream_laziness()
    optimize = is_lazy and not _virtual_sources_use_cloud(graph)
    if optimize:
        if graph.execution_mode != "performance":
            graph.execution_mode = "performance"
            graph.reset()
            changed_execution_mode = True
            incoming_node = graph.get_node(node_catalog_writer.node_id).node_inputs.main_inputs[0]
            df = incoming_node.get_resulting_data()
        polars_plan = df.data_frame.explain()
        graph.flow_logger.info(f"creating a virtual table with: {polars_plan}")
        buf = _io.BytesIO()
        df.data_frame.serialize(buf)
        serialized_lf = buf.getvalue()
        source_table_versions = _collect_source_table_versions(graph)
    else:
        graph.flow_logger.info("creating a virtual table from workflow")

    schema_json = json.dumps([{"name": c.column_name, "dtype": c.data_type} for c in df.schema])

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        ns_id = _effective_namespace_id(svc, settings)
        existing = repo.get_table_by_name(settings.table_name, ns_id)
        _authorize_catalog_write(db, node_catalog_writer.user_id, existing=existing, namespace_id=ns_id)
        if existing is not None and getattr(existing, "table_type", "physical") != "virtual":
            raise ValueError(
                f"Cannot write virtual table '{settings.table_name}': a non-virtual "
                f"catalog table with that name already exists in this namespace."
            )
        if existing is not None:
            svc.update_virtual_flow_table(
                table_id=existing.id,
                name=settings.table_name or None,
                producer_registration_id=reg_id,
                description=settings.description,
                serialized_lazy_frame=serialized_lf,
                is_optimized=optimize,
                schema_json=schema_json,
                polars_plan=polars_plan,
                source_table_versions=source_table_versions,
            )
        else:
            svc.create_virtual_flow_table(
                name=settings.table_name,
                owner_id=node_catalog_writer.user_id or 1,
                producer_registration_id=reg_id,
                namespace_id=ns_id,
                description=settings.description,
                serialized_lazy_frame=serialized_lf,
                is_optimized=optimize,
                schema_json=schema_json,
                polars_plan=polars_plan,
                source_table_versions=source_table_versions,
            )

    if changed_execution_mode:
        graph.execution_mode = "Development"
    return df


def _handle_physical_table_write(
    graph: "FlowGraph",
    node_catalog_writer: input_schema.NodeCatalogWriter,
    df: FlowDataEngine,
) -> FlowDataEngine:
    """Handle physical-mode catalog write: materialize data as a Delta table and register it."""
    settings = node_catalog_writer.catalog_write_settings
    user_id = node_catalog_writer.user_id or 1

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        namespace_id = _effective_namespace_id(svc, settings)
        target = resolve_for_namespace(namespace_id, db=db)
        if not target.is_cloud:
            Path(target.base).mkdir(parents=True, exist_ok=True)
        existing, dest_path, delta_mode = svc.resolve_write_destination(
            table_name=settings.table_name,
            namespace_id=namespace_id,
            write_mode=settings.write_mode,
            target=target,
        )
        # Authorize before any dispatch: neither writer may get a destination the principal cannot write.
        _authorize_catalog_write(db, node_catalog_writer.user_id, existing=existing, namespace_id=namespace_id)
        # Both SCD2 gates read the catalog record, so they run inside the session and before dispatch.
        scd2_config = _resolve_scd2_config(settings, df, existing) if settings.write_mode == "scd2" else None
        _reject_non_scd2_write_to_scd2_table(settings, existing)

    # Forward-only: an existing table keeps its own location; the catalog config only steers new tables.
    dest_is_cloud = _is_cloud_uri(dest_path)
    if dest_is_cloud:
        if not target.is_cloud:
            raise ValueError(
                f"Catalog table '{settings.table_name}' lives in object storage ({dest_path}) but its "
                "catalog is not configured for object storage; set the catalog's storage_uri / "
                "storage_connection_name to write to it."
            )
        storage_payload = target.to_worker_payload()
        storage_options = target.storage_options
    else:
        storage_payload = None
        storage_options = None

    if settings.track_changes and existing is not None:
        # Enable before the write so this write's own commit lands above the cursor floor.
        _ensure_catalog_cdc_enabled(settings.table_name, namespace_id, dest_path, storage_options)

    # One instant for the whole write: the surrogate key depends on valid_from, so both branches agree.
    run_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    scd2_kwargs = _scd2_primitive_kwargs(scd2_config, run_timestamp) if delta_mode == "scd2" else None
    if delta_mode == "scd2":
        op_type = "scd2_delta"
        # The worker's scd2_delta takes the same fields; only the instant is named run_timestamp there.
        op_kwargs = {k: v for k, v in scd2_kwargs.items() if k != "valid_from_iso"}
        op_kwargs["run_timestamp"] = scd2_kwargs["valid_from_iso"]
        op_kwargs["output_path"] = dest_path
        op_kwargs["partition_by"] = settings.partition_by
    else:
        op_type, op_kwargs = _delta_op(
            delta_mode,
            output_path=dest_path,
            merge_keys=settings.merge_keys,
            partition_by=settings.partition_by,
            enable_cdf=settings.track_changes,
        )
    if storage_payload is not None:
        op_kwargs["storage_payload"] = storage_payload

    # Follows execution_location like every writer; a cloud destination only threads storage_options.
    if graph.flow_settings.execution_location != "local":
        written = root()._write_catalog_delta_remote(
            flow_id=graph.flow_id,
            node=graph.get_node(node_catalog_writer.node_id),
            df=df,
            op_type=op_type,
            op_kwargs=op_kwargs,
            table_label=f"'{settings.table_name}'",
        )
    else:
        written = _write_catalog_delta_local(
            df,
            dest_path,
            delta_mode,
            settings.merge_keys,
            settings.partition_by,
            storage_options=storage_options,
            scd2_kwargs=scd2_kwargs,
            enable_cdf=settings.track_changes,
        )
    meta_kwargs = written.meta

    def _node_output() -> FlowDataEngine:
        """Only an SCD2 write changes what leaves the node; every other mode passes its input on."""
        if delta_mode != "scd2":
            return df
        return _scd2_writer_output(
            df,
            dest_path,
            scd2_config,
            (settings.scd2 or input_schema.Scd2Settings()).output_mode,
            written.scd2_version,
            run_timestamp,
            storage_options,
        )

    if meta_kwargs is None:
        return _node_output()

    # Transport-only: the catalog service methods take explicit keyword arguments.
    scd2_metrics = meta_kwargs.pop("scd2_metrics", None)
    if scd2_metrics:
        graph.flow_logger.get_node_logger(node_catalog_writer.node_id).info(
            f"SCD2 write to '{settings.table_name}': +{scd2_metrics.get('rows_inserted')} inserted, "
            f"{scd2_metrics.get('rows_closed')} closed "
            f"({scd2_metrics.get('rows_current')} current of {scd2_metrics.get('rows_total')} total rows)"
        )

    _register_catalog_table(
        existing=existing,
        dest_path=dest_path,
        settings=settings,
        source_registration_id=graph._flow_settings.source_registration_id,
        user_id=user_id,
        meta_kwargs=meta_kwargs,
        storage_options=storage_options,
        is_cloud=dest_is_cloud,
        scd2_config=scd2_config,
    )
    if settings.track_changes:
        _ensure_catalog_cdc_enabled(settings.table_name, namespace_id, dest_path, storage_options)
    return _node_output()
