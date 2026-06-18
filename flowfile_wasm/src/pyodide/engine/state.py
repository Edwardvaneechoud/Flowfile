import gc
from collections import OrderedDict
from hashlib import md5

import polars as pl

from .log import logger

_lazyframes: dict[int, pl.LazyFrame] = {}


_preview_cache: OrderedDict[int, dict] = OrderedDict()


_PREVIEW_CACHE_MAX_SIZE = 20  # Max number of cached previews


_PREVIEW_CACHE_MAX_MEMORY_MB = 50  # Approximate max memory for preview cache


_plan_hashes: dict[int, str] = {}


_schemas: dict[int, list[dict[str, str]]] = {}


_schema_lazyframes: dict[int, pl.LazyFrame] = {}


_schema_schemas: dict[int, list[dict[str, str]]] = {}


# Binary node outputs (xlsx/parquet-IPC bytes) staged for a one-shot JS pull:
# Python bytes don't survive the toJs() bridge, so JS fetches them separately
# via take_output_binary + PyProxy.getBuffer.
_output_binaries: dict[int, bytes] = {}


_SOURCE_TYPES = {"read", "manual_input", "external_data", "read_from_catalog"}


def _get_plan_hash(lf: pl.LazyFrame) -> str:
    """Get a hash of the query plan to detect changes."""
    try:
        plan_str = str(lf.explain(optimized=True))
        return md5(plan_str.encode()).hexdigest()[:16]
    except Exception:
        return str(id(lf))


def store_lazyframe(node_id: int, lf: pl.LazyFrame):
    """Store a LazyFrame for a node. Invalidates preview cache if plan changed."""
    # Get schema from LazyFrame (doesn't require collection!)
    schema = lf.collect_schema()
    _schemas[node_id] = [{"name": name, "data_type": str(dtype)} for name, dtype in schema.items()]

    # Check if plan changed - if so, invalidate preview cache
    new_hash = _get_plan_hash(lf)
    old_hash = _plan_hashes.get(node_id)

    if old_hash != new_hash:
        if node_id in _preview_cache:
            logger.debug("store_lazyframe node=%s plan changed, preview cache invalidated", node_id)
        _preview_cache.pop(node_id, None)
        _plan_hashes[node_id] = new_hash

    _lazyframes[node_id] = lf


def get_lazyframe(node_id: int) -> pl.LazyFrame | None:
    """Get a LazyFrame for a node."""
    return _lazyframes.get(node_id)


def get_schema(node_id: int) -> list[dict[str, str]]:
    """Get schema for a node (available without collecting)."""
    return _schemas.get(node_id, [])


def has_cached_preview(node_id: int) -> bool:
    """Check if a node has a cached preview."""
    return node_id in _preview_cache


def get_cached_preview(node_id: int) -> dict | None:
    """Get cached preview data if available."""
    return _preview_cache.get(node_id)


def take_output_binary(node_id: int) -> bytes | None:
    """Pop and return a node's staged binary output (one-shot; frees the heap copy)."""
    return _output_binaries.pop(node_id, None)


def clear_node(node_id: int):
    """Clear all data for a node."""
    logger.debug("clear_node node=%s", node_id)
    _lazyframes.pop(node_id, None)
    _schemas.pop(node_id, None)
    _preview_cache.pop(node_id, None)
    _plan_hashes.pop(node_id, None)
    _schema_lazyframes.pop(node_id, None)
    _schema_schemas.pop(node_id, None)
    _output_binaries.pop(node_id, None)


def clear_all():
    """Clear all data."""
    logger.info("clear_all: dropping %d lazyframes, %d previews", len(_lazyframes), len(_preview_cache))
    _lazyframes.clear()
    _schemas.clear()
    _preview_cache.clear()
    _plan_hashes.clear()
    _schema_lazyframes.clear()
    _schema_schemas.clear()
    _output_binaries.clear()
    gc.collect()
