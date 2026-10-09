"""Source fingerprints that decide whether a catalog or cloud Delta reader must re-run."""

import hashlib
import json
import os
import re

from flowfile_core.catalog.delta_utils import (
    get_live_delta_version,
    is_delta_table,
)
from flowfile_core.catalog.storage_backend import _is_cloud_uri, resolve_for_namespace
from flowfile_core.flowfile.flow_graph.catalog_resolution import (
    _resolve_catalog_sql_tables,
    _resolve_catalog_table_info,
)
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.parameter_resolver import (
    resolve_parameters,
)
from flowfile_core.schemas import input_schema


def _probe_version_entry(path: str, storage_options: dict | None, version_cache: dict[str, int]) -> int:
    """Live Delta version for *path*, memoized per refresh pass."""
    if path not in version_cache:
        version_cache[path] = get_live_delta_version(path, storage_options=storage_options)
    return version_cache[path]


def _fingerprint_virtual_source(
    is_optimized: bool,
    serialized_lf: bytes | None,
    source_table_versions: str | None,
    version_cache: dict[str, int],
) -> tuple[list[dict], bool]:
    """Fingerprint entries for a virtual-table source, or force=True.

    An optimized virtual is identified by its stored plan (rotates when the
    producer re-writes) plus live versions of its recorded Delta sources
    (rotate on out-of-band writes). A non-optimized or fingerprint-less
    virtual re-runs its producer flow at resolution, so its output can change
    with nothing observable here — always invalidate, like run_flow nodes.
    """
    if not is_optimized or not serialized_lf or source_table_versions is None:
        return [], True
    entries: list[dict] = [{"plan": hashlib.sha256(serialized_lf).hexdigest()}]
    for entry in json.loads(source_table_versions):
        entries.append(
            {
                "table_id": entry["table_id"],
                "path": entry["file_path"],
                "version": _probe_version_entry(entry["file_path"], None, version_cache),
            }
        )
    return entries, False


def _cdc_since_param_state(settings, node: FlowNode) -> str | None:
    """The resolved ``since`` value of a since-version/since-time change reader, when a flow parameter sets it.

    *settings* is any object carrying the ``cdc_*`` fields (catalog reader node, cloud read settings). A
    literal is already part of the settings hash, so only a ``${param}`` ref has run state to fold in.
    """
    if settings.cdc_mode not in ("since_version", "since_timestamp"):
        return None
    raw = settings.cdc_from_version if settings.cdc_mode == "since_version" else settings.cdc_from_timestamp
    if not isinstance(raw, str) or "${" not in raw:
        return None
    params = node._params_getter() if node._params_getter else {}
    return resolve_parameters(raw, params)


def _delta_reader_fingerprint(path: str, version: int | None, cdc_state: int | str | None) -> str:
    """Fingerprint payload for a Delta-backed catalog reader.

    A change reader folds its run state in — the stored cursor, or the resolved ``since`` value
    when that comes from a flow parameter: head and that state are the values that decide what
    the next read returns, so a run that finds no new data re-executes once (empty frame) and
    then short-circuits until one of them moves.
    """
    payload: dict = {"path": path, "version": version}
    if cdc_state is not None:
        payload["cursor"] = cdc_state
    return json.dumps(payload, sort_keys=True)


def _catalog_reader_source_fingerprint(
    settings: "input_schema.NodeCatalogReader",
    version_cache: dict[str, int],
    opts_by_namespace: dict[int | None, dict | None],
    cdc_state: int | str | None = None,
) -> tuple[str | None, bool]:
    """Canonical freshness fingerprint for a catalog_reader node's sources.

    Returns ``(fingerprint_json, force)``. ``force=True`` means the node must
    be invalidated unconditionally (unfingerprintable source). Raises on probe
    failures — the caller treats that as force (fail-open re-run, so the real
    error surfaces instead of a silently-served stale snapshot).
    """
    entries: list[dict] = []

    if settings.sql_query:
        resolved = _resolve_catalog_sql_tables(settings.node_id, settings.user_id)

        def _referenced(name: str) -> bool:
            return re.search(rf"\b{re.escape(name)}\b", settings.sql_query, re.IGNORECASE) is not None

        physical = {n: p for n, p in resolved.table_paths.items() if _referenced(n)}
        virtual = {n: v for n, v in resolved.virtual_tables.items() if _referenced(n)}
        if not physical and not virtual:
            # Conservative: no name matched the query text — treat every
            # registered table as a potential source (over-fresh, never stale).
            physical, virtual = resolved.table_paths, resolved.virtual_tables

        for name, path in sorted(physical.items()):
            ns = resolved.table_namespaces.get(name)
            opts = None
            if _is_cloud_uri(path):
                if ns not in opts_by_namespace:
                    opts_by_namespace[ns] = resolve_for_namespace(ns).storage_options or None
                opts = opts_by_namespace[ns]
            entries.append({"name": name, "path": path, "version": _probe_version_entry(path, opts, version_cache)})
        for name, (is_optimized, serialized_lf, _vid, versions_json) in sorted(virtual.items()):
            sub_entries, force = _fingerprint_virtual_source(is_optimized, serialized_lf, versions_json, version_cache)
            if force:
                return None, True
            entries.append({"name": name, "virtual": sub_entries})
        return json.dumps(entries, sort_keys=True), False

    info = _resolve_catalog_table_info(settings)
    if not info.authorized:
        return None, True
    if info.table_type == "virtual":
        sub_entries, force = _fingerprint_virtual_source(
            info.is_optimized, info.serialized_lf, info.source_table_versions, version_cache
        )
        if force:
            return None, True
        return json.dumps({"table_id": info.table_id, "virtual": sub_entries}, sort_keys=True), False
    if not info.file_path:
        return None, True
    if _is_cloud_uri(info.file_path):
        ns = info.namespace_id
        if ns not in opts_by_namespace:
            opts_by_namespace[ns] = resolve_for_namespace(ns).storage_options or None
        version = _probe_version_entry(info.file_path, opts_by_namespace[ns], version_cache)
        return _delta_reader_fingerprint(info.file_path, version, cdc_state), False
    if is_delta_table(info.file_path):
        version = _probe_version_entry(info.file_path, None, version_cache)
        return _delta_reader_fingerprint(info.file_path, version, cdc_state), False
    # Legacy parquet: the SourceFileInfo idiom (mtime+size).
    stat = os.stat(info.file_path)
    return (
        json.dumps({"path": info.file_path, "mtime": stat.st_mtime, "size": stat.st_size}, sort_keys=True),
        False,
    )
