"""Column names and dtypes of the Polars frames in a live cell namespace.

Pure and data-free: the editor needs names and dtypes, never values. Nothing here
touches a user object's attributes — dispatch goes through ``type(obj)`` +
``issubclass`` (a ``__class__`` property can lie, ``isinstance`` would believe it),
and schemas are read through the base-class function/descriptor so a subclass
overriding ``schema`` / ``collect_schema`` is never invoked.

``collect_dataframe_schemas`` never touches a ``LazyFrame`` — both ``.schema``
and ``.collect_schema()`` resolve the plan — so it reports one ``unresolved``
with no attribute access at all. Resolving a plan is a separate, opt-in call
(``describe_lazy_frame``) and it is not free: ``collect_schema`` resolves the
plan without executing the query, but a scan may read file metadata or an
inference sample (a CSV's first rows), so it can do I/O, and an inferred dtype
can differ from the one a full ``collect()`` would produce.
"""

import polars as pl

_SKIP_NAMES = frozenset({"flowfile_ctx"})


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


def describe_lazy_frame(name: str, obj, *, max_columns: int = 2000) -> dict:
    """Resolve one lazy plan's schema, degrading to the ``unresolved`` entry on any failure.

    Opt-in and deliberately per frame: the caller decides which plans are worth
    resolving and under what deadline. The resolution goes through the base-class
    ``pl.LazyFrame.collect_schema`` so a subclass override is never invoked.
    """
    unresolved = {"name": name, "kind": "LazyFrame", "state": "unresolved", "columns": []}
    if not issubclass(type(obj), pl.LazyFrame):
        return unresolved
    try:
        schema = pl.LazyFrame.collect_schema(obj)
        columns = [{"name": col, "dtype": str(dtype)} for col, dtype in list(schema.items())[:max_columns]]
    except BaseException:  # noqa: BLE001 — a pyo3 panic subclasses BaseException, and this runs on a worker thread
        return unresolved
    return {
        "name": name,
        "kind": "LazyFrame",
        "state": "ready",
        "columns": columns,
        "truncated": len(schema) > max_columns,
    }
