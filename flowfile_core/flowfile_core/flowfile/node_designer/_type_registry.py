"""Re-export shim — implementation moved to shared/node_designer/_type_registry.py."""

from shared.node_designer._type_registry import (
    TypeMapping,
    TypeRegistry,
    _registry,
    check_column_type,
    get_polars_types,
    normalize_type_spec,
)

__all__ = [
    "TypeMapping",
    "TypeRegistry",
    "_registry",
    "check_column_type",
    "get_polars_types",
    "normalize_type_spec",
]
