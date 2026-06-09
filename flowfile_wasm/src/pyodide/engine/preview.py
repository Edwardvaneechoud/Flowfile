import polars as pl

from .state import (
    _PREVIEW_CACHE_MAX_MEMORY_MB,
    _PREVIEW_CACHE_MAX_SIZE,
    _lazyframes,
    _plan_hashes,
    _preview_cache,
    _schemas,
    get_cached_preview,
    has_cached_preview,
)


def _estimate_preview_size_mb(preview_data: dict) -> float:
    """Estimate memory size of preview data in MB."""
    try:
        data = preview_data.get("data", [])
        if not data:
            return 0.001
        # Rough estimate: each cell ~50 bytes average
        num_cells = len(data) * len(data[0]) if data and len(data) > 0 else 0
        return (num_cells * 50) / (1024 * 1024)
    except Exception:
        return 0.1


def _evict_preview_cache_if_needed():
    """Evict oldest entries if cache exceeds limits."""
    # Evict by count
    while len(_preview_cache) > _PREVIEW_CACHE_MAX_SIZE:
        _preview_cache.popitem(last=False)

    # Evict by estimated memory
    total_mb = sum(_estimate_preview_size_mb(v) for v in _preview_cache.values())
    while total_mb > _PREVIEW_CACHE_MAX_MEMORY_MB and len(_preview_cache) > 1:
        _preview_cache.popitem(last=False)
        total_mb = sum(_estimate_preview_size_mb(v) for v in _preview_cache.values())


def materialize_preview(node_id: int, max_rows: int = 100) -> dict:
    """
    Materialize a preview for a node (on-demand).
    This is the expensive operation - only called when user clicks to view.
    Memory-optimized: collects once with row count included.
    """
    lf = _lazyframes.get(node_id)
    if lf is None:
        return {"error": f"No LazyFrame found for node #{node_id}"}

    try:
        # Optimized: Add row number to get total count in single collection
        # This avoids the double .collect() call
        lf_with_count = lf.with_row_index("__row_idx__")
        preview_df = lf_with_count.head(max_rows).collect()

        # Get total rows from a separate lightweight query (just count, no data)
        try:
            total_rows = lf.select(pl.len()).collect().item()
        except Exception:
            # Fallback: if we got max_rows, there's probably more
            total_rows = len(preview_df)

        # Remove the temporary index column before returning
        if "__row_idx__" in preview_df.columns:
            preview_df = preview_df.drop("__row_idx__")

        preview_data = {
            "columns": preview_df.columns,
            # Use native Polars rows() instead of numpy to reduce memory footprint
            "data": [list(row) for row in preview_df.rows()] if len(preview_df) > 0 else [],
            "total_rows": total_rows,
            "preview_rows": len(preview_df),
        }

        # Explicitly delete the DataFrame to free memory immediately
        del preview_df

        # Cache the preview with LRU eviction
        _preview_cache[node_id] = preview_data
        _preview_cache.move_to_end(node_id)  # Mark as recently used
        _evict_preview_cache_if_needed()

        return preview_data
    except Exception as e:
        return {"error": str(e)}


def fetch_preview(node_id: int, max_rows: int = 100, force_refresh: bool = False) -> dict:
    """
    Fetch preview data for a node. Called when user clicks to view.
    Uses LRU cache with eviction.
    """
    if not force_refresh and has_cached_preview(node_id):
        cached = get_cached_preview(node_id)
        # Mark as recently used for LRU
        if node_id in _preview_cache:
            _preview_cache.move_to_end(node_id)
        return {"success": True, "data": cached, "from_cache": True}

    preview_data = materialize_preview(node_id, max_rows)

    if "error" in preview_data:
        return {"success": False, "error": preview_data["error"]}

    return {"success": True, "data": preview_data, "from_cache": False}


def invalidate_downstream_previews(node_id: int, node_graph: dict[int, list[int]]):
    """
    Invalidate preview cache for a node and all its downstream dependents.
    """
    to_invalidate = [node_id]
    visited = set()

    while to_invalidate:
        current = to_invalidate.pop(0)
        if current in visited:
            continue
        visited.add(current)

        _preview_cache.pop(current, None)

        downstream = node_graph.get(current, [])
        to_invalidate.extend(downstream)


def get_memory_stats() -> dict:
    """Get memory statistics for debugging."""
    return {
        "lazyframes_count": len(_lazyframes),
        "preview_cache_count": len(_preview_cache),
        "schemas_count": len(_schemas),
        "plan_hashes_count": len(_plan_hashes),
        "preview_cache_size_estimate_mb": sum(_estimate_preview_size_mb(v) for v in _preview_cache.values()),
    }


def df_to_preview(df: pl.DataFrame, max_rows: int = 100) -> dict:
    """Convert dataframe to preview format"""
    preview_df = df.head(max_rows)
    return {
        "columns": df.columns,
        # Use native Polars rows() instead of numpy to reduce memory footprint
        "data": [list(row) for row in preview_df.rows()] if len(preview_df) > 0 else [],
        "total_rows": len(df),
    }
