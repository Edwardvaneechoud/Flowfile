import time

import polars as pl

from .dtypes import build_empty_lf_from_schema
from .log import logger
from .nodes_aggregate import build_group_by, build_unpivot
from .nodes_combine import build_join
from .nodes_polars_code import build_polars_code_schema
from .nodes_transform import build_filter, build_head, build_select, build_sort, build_unique
from .state import _SOURCE_TYPES, _schema_lazyframes, _schema_schemas


def _schema_identity(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Pass-through builder for nodes whose output schema == input schema."""
    return input_lf


_SCHEMA_BUILDERS = {
    "filter": build_filter,
    "select": build_select,
    "group_by": build_group_by,
    "sort": build_sort,
    "unique": build_unique,
    "head": build_head,
    "unpivot": build_unpivot,
    "explore_data": _schema_identity,
    "output": _schema_identity,
    "external_output": _schema_identity,
    "write_to_catalog": _schema_identity,
}


def _build_schema_node(ntype: str, meta: dict) -> pl.LazyFrame | None:
    """Build a node's schema-only LazyFrame from its already-resolved inputs.
    Returns None when an input's schema isn't available yet."""
    input_ids = meta.get("input_ids") or []
    left = meta.get("left")
    right = meta.get("right")
    settings = meta.get("settings", {})

    if ntype == "join":
        left_id = left if left is not None else (input_ids[0] if input_ids else None)
        left_lf = _schema_lazyframes.get(left_id)
        right_lf = _schema_lazyframes.get(right)
        if left_lf is None or right_lf is None:
            return None
        return build_join(left_lf, right_lf, settings)

    if ntype in ("polars_code", "formula"):
        lfs = [_schema_lazyframes.get(i) for i in input_ids]
        if not lfs or any(x is None for x in lfs):
            return None
        return build_polars_code_schema(lfs, settings)

    in_id = left if left is not None else (input_ids[0] if input_ids else None)
    inp = _schema_lazyframes.get(in_id)
    if inp is None:
        return None
    builder = _SCHEMA_BUILDERS.get(ntype, _schema_identity)
    return builder(inp, settings)


def propagate_schemas(graph_json: dict, source_schemas: dict) -> dict[str, dict]:
    """Walk the DAG and resolve every node's output schema lazily (data-free).

    Mirrors flowfile_core: empty (0-row) LazyFrames chained through the node
    builders, reading collect_schema() at each hop. Never touches data.

    graph_json: {"order": [ids...],
                 "nodes": {"<id>": {type, input_ids, left, right, settings}}}
    source_schemas: {"<id>": [{name, data_type}, ...]} for source nodes.
    Returns {"<id>": {schema, schema_resolved, error}}.
    """
    _schema_lazyframes.clear()
    _schema_schemas.clear()
    results: dict[str, dict] = {}

    t0 = time.perf_counter()
    order = graph_json.get("order", [])
    nodes_meta = graph_json.get("nodes", {})

    for nid in order:
        key = str(nid)
        meta = nodes_meta.get(key)
        if not meta:
            continue
        ntype = meta.get("type")
        try:
            if key in source_schemas or ntype in _SOURCE_TYPES:
                sch = source_schemas.get(key)
                if not sch:
                    results[key] = {"schema": [], "schema_resolved": False, "error": "No data loaded"}
                    continue
                lf = build_empty_lf_from_schema(sch)
            elif ntype == "pivot":
                results[key] = {
                    "schema": [],
                    "schema_resolved": False,
                    "error": "Pivot output columns depend on the data; run the flow.",
                }
                continue
            else:
                lf = _build_schema_node(ntype, meta)
                if lf is None:
                    results[key] = {"schema": [], "schema_resolved": False, "error": "Upstream schema unavailable"}
                    continue

            _schema_lazyframes[nid] = lf
            schema = [{"name": n, "data_type": str(d)} for n, d in lf.collect_schema().items()]
            _schema_schemas[nid] = schema
            results[key] = {"schema": schema, "schema_resolved": True, "error": None}
        except Exception as e:
            results[key] = {"schema": [], "schema_resolved": False, "error": str(e)}

    unresolved = {k: r["error"] for k, r in results.items() if not r["schema_resolved"]}
    logger.info(
        "propagate_schemas: resolved=%d unresolved=%d (%.0fms)",
        len(results) - len(unresolved),
        len(unresolved),
        (time.perf_counter() - t0) * 1000,
    )
    for key, why in unresolved.items():
        logger.debug("propagate_schemas node=%s unresolved: %s", key, why)

    return results
