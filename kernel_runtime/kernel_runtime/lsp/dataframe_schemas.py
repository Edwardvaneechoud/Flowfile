"""Column names and dtypes of the Polars frames in a live cell namespace.

Pure and data-free: the editor needs names and dtypes, never values. Nothing here
touches a user object's attributes — dispatch goes through ``type(obj)`` +
``issubclass`` (a ``__class__`` property can lie, ``isinstance`` would believe it),
eager schemas are read through the base-class descriptor so a subclass overriding
``schema`` is never invoked, and a ``LazyFrame`` gets no attribute access at all
(both ``.schema`` and ``.collect_schema()`` resolve the plan, which can do I/O).
"""

import polars as pl

_SKIP_NAMES = frozenset({"flowfile_ctx", "flowfile"})


def collect_dataframe_schemas(
    namespace: dict,
    *,
    max_frames: int = 100,
    max_columns: int = 2000,
) -> list[dict]:
    """Describe every Polars frame bound in ``namespace``, alphabetically, capped."""
    frames: list[dict] = []
    for name in sorted(k for k in namespace if isinstance(k, str)):
        if len(frames) >= max_frames:
            break
        if name.startswith("_") or name in _SKIP_NAMES:
            continue
        try:
            obj_type = type(namespace[name])
            if issubclass(obj_type, pl.LazyFrame):
                frames.append({"name": name, "kind": "LazyFrame", "state": "unresolved", "columns": []})
                continue
            if not issubclass(obj_type, pl.DataFrame):
                continue
            schema = pl.DataFrame.schema.__get__(namespace[name])
            columns = [{"name": col, "dtype": str(dtype)} for col, dtype in list(schema.items())[:max_columns]]
            frames.append(
                {
                    "name": name,
                    "kind": "DataFrame",
                    "state": "ready",
                    "columns": columns,
                    "truncated": len(schema) > max_columns,
                }
            )
        except Exception:  # noqa: BLE001 — one hostile object must not take the endpoint down
            continue
    return frames
