"""Introspection of the kernel's public Python API for editor type hints.

The frontend code editor fetches this schema to provide signature-aware
completions and hover docs for ``flowfile_ctx`` (the injected context object),
the catalog reference chain, and the most common ``polars`` symbols.

The schema is derived by runtime introspection (``inspect``) so it stays in
sync with whatever version of ``flowfile_client`` / ``polars`` is baked into
the running kernel image, instead of a hand-maintained list.
"""

from __future__ import annotations

import inspect
from typing import Any

import polars as pl

from kernel_runtime import flowfile_client


def _summarize_doc(obj: Any) -> str:
    """Return the first paragraph of an object's docstring (trimmed)."""
    doc = inspect.getdoc(obj)
    if not doc:
        return ""
    paragraph: list[str] = []
    for line in doc.splitlines():
        if not line.strip():
            break
        paragraph.append(line.strip())
    return " ".join(paragraph)


def _signature_str(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except (TypeError, ValueError):
        return ""


def _return_type(obj: Any) -> str:
    try:
        ann = inspect.signature(obj).return_annotation
    except (TypeError, ValueError):
        return ""
    if ann is inspect.Signature.empty:
        return ""
    if isinstance(ann, str):
        return ann
    return getattr(ann, "__name__", str(ann))


def _kind(obj: Any) -> str:
    if inspect.isclass(obj):
        return "class"
    if isinstance(obj, property):
        return "property"
    if inspect.isfunction(obj) or inspect.ismethod(obj) or inspect.isbuiltin(obj):
        return "function"
    if callable(obj):
        return "function"
    return "variable"


def _entry(name: str, obj: Any, namespace: str) -> dict[str, str]:
    return {
        "name": name,
        "kind": _kind(obj),
        "namespace": namespace,
        "signature": _signature_str(obj) if callable(obj) and not isinstance(obj, property) else "",
        "return_type": _return_type(obj) if callable(obj) and not isinstance(obj, property) else "",
        "doc": _summarize_doc(obj),
    }


def _public_members(container: Any, namespace: str, *, only_callables: bool = True) -> list[dict[str, str]]:
    """Build entries for the public attributes of a module or class."""
    entries: list[dict[str, str]] = []
    for name in dir(container):
        if name.startswith("_"):
            continue
        try:
            obj = inspect.getattr_static(container, name) if inspect.isclass(container) else getattr(container, name)
        except (AttributeError, Exception):  # noqa: BLE001 - introspection must never crash
            continue
        if only_callables and not (callable(obj) or isinstance(obj, property)):
            continue
        try:
            entries.append(_entry(name, obj, namespace))
        except Exception:  # noqa: BLE001 - skip anything that resists introspection
            continue
    return entries


# Top-level polars symbols we surface (functions, readers, datatypes). Limiting
# the polars surface keeps the payload focused on what notebook authors reach
# for, while LazyFrame/DataFrame/Expr methods are introspected in full below.
_POLARS_TOPLEVEL = (
    "col", "lit", "when", "concat", "select", "sql_expr", "struct", "format",
    "DataFrame", "LazyFrame", "Series", "Expr",
    "read_csv", "read_parquet", "read_json", "read_ndjson", "read_database",
    "scan_csv", "scan_parquet", "scan_ndjson", "scan_delta",
    "from_dict", "from_dicts", "from_records", "from_pandas",
    "all", "any", "sum", "min", "max", "mean", "median", "count", "first", "last",
    "int_range", "date_range", "datetime_range",
)


def _polars_toplevel_entries() -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for name in _POLARS_TOPLEVEL:
        obj = getattr(pl, name, None)
        if obj is None:
            continue
        try:
            entries.append(_entry(name, obj, "pl"))
        except Exception:  # noqa: BLE001
            continue
    return entries


def build_api_schema() -> list[dict[str, str]]:
    """Introspect the kernel's public API into a flat list of symbol entries."""
    entries: list[dict[str, str]] = []

    # flowfile_ctx — the injected context module
    entries.extend(_public_members(flowfile_client, "flowfile_ctx"))

    # Catalog reference chain returned by flowfile_ctx helpers
    for cls_name in ("CatalogRef", "SchemaRef", "TableRef"):
        cls = getattr(flowfile_client, cls_name, None)
        if cls is not None:
            entries.extend(_public_members(cls, cls_name))

    # polars surface
    entries.extend(_polars_toplevel_entries())
    entries.extend(_public_members(pl.LazyFrame, "LazyFrame"))
    entries.extend(_public_members(pl.DataFrame, "DataFrame"))
    entries.extend(_public_members(pl.Expr, "Expr"))

    # De-duplicate on (namespace, name) while preserving first occurrence.
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for entry in entries:
        key = (entry["namespace"], entry["name"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(entry)
    return unique


_CACHED_SCHEMA: list[dict[str, str]] | None = None


def get_api_schema() -> list[dict[str, str]]:
    """Return the cached API schema, building it on first access."""
    global _CACHED_SCHEMA
    if _CACHED_SCHEMA is None:
        _CACHED_SCHEMA = build_api_schema()
    return _CACHED_SCHEMA
