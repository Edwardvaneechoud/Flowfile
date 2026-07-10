# _type_registry.py - Internal type system (not for public use)
"""
Internal type registry for mapping between different type representations.
This module should not be imported directly by users.
"""

from dataclasses import dataclass
from typing import Any

import polars as pl

from shared.node_designer.types import DataType, TypeGroup


@dataclass(frozen=True)
class TypeMapping:
    """Internal mapping between type representations."""

    data_type: DataType
    polars_type: type[pl.DataType]
    type_group: TypeGroup
    aliases: tuple[str, ...] = ()


class TypeRegistry:
    """
    Internal registry for type conversions and lookups.
    This class is not part of the public API.
    """

    def __init__(self):
        self._mappings: list[TypeMapping] = [
            # Numeric types
            TypeMapping(DataType.Int8, pl.Int8, TypeGroup.Numeric, ("i8",)),
            TypeMapping(DataType.Int16, pl.Int16, TypeGroup.Numeric, ("i16",)),
            TypeMapping(DataType.Int32, pl.Int32, TypeGroup.Numeric, ("i32", "int32")),
            TypeMapping(DataType.Int64, pl.Int64, TypeGroup.Numeric, ("i64", "int64", "int", "integer", "bigint")),
            TypeMapping(DataType.Int128, pl.Int128, TypeGroup.Numeric, ("i128", "int128")),
            TypeMapping(DataType.UInt8, pl.UInt8, TypeGroup.Numeric, ("u8",)),
            TypeMapping(DataType.UInt16, pl.UInt16, TypeGroup.Numeric, ("u16",)),
            TypeMapping(DataType.UInt32, pl.UInt32, TypeGroup.Numeric, ("u32", "uint32")),
            TypeMapping(DataType.UInt64, pl.UInt64, TypeGroup.Numeric, ("u64", "uint64")),
            TypeMapping(DataType.UInt128, pl.UInt128, TypeGroup.Numeric, ("u128", "uint128")),
            TypeMapping(DataType.Float16, pl.Float16, TypeGroup.Numeric, ("f16", "float16", "half")),
            TypeMapping(DataType.Float32, pl.Float32, TypeGroup.Numeric, ("f32", "float32")),
            TypeMapping(DataType.Float64, pl.Float64, TypeGroup.Numeric, ("f64", "float64", "float", "double")),
            TypeMapping(DataType.Decimal, pl.Decimal, TypeGroup.Numeric, ("decimal", "dec")),
            # String types
            TypeMapping(DataType.String, pl.String, TypeGroup.String, ("str", "string", "utf8", "varchar", "text")),
            TypeMapping(
                DataType.Categorical, pl.Categorical, TypeGroup.String, ("cat", "categorical", "enum", "factor")
            ),
            # Date types
            TypeMapping(DataType.Date, pl.Date, TypeGroup.Date, ("date",)),
            TypeMapping(DataType.Datetime, pl.Datetime, TypeGroup.Date, ("datetime", "timestamp")),
            TypeMapping(DataType.Time, pl.Time, TypeGroup.Date, ("time",)),
            TypeMapping(DataType.Duration, pl.Duration, TypeGroup.Date, ("duration", "timedelta")),
            # Other types
            TypeMapping(DataType.Boolean, pl.Boolean, TypeGroup.Boolean, ("bool", "boolean")),
            TypeMapping(DataType.Binary, pl.Binary, TypeGroup.Binary, ("binary", "bytes", "bytea")),
            TypeMapping(DataType.List, pl.List, TypeGroup.Complex, ("list", "array")),
            TypeMapping(DataType.Struct, pl.Struct, TypeGroup.Complex, ("struct", "object")),
            TypeMapping(DataType.Array, pl.Array, TypeGroup.Complex, ("fixed_array",)),
        ]

        self._build_indices()

    def _build_indices(self):
        """Build lookup indices for fast access."""
        self._by_data_type: dict[DataType, TypeMapping] = {}
        self._by_polars_type: dict[type[pl.DataType], TypeMapping] = {}
        self._by_alias: dict[str, TypeMapping] = {}
        self._by_group: dict[TypeGroup, list[TypeMapping]] = {g: [] for g in TypeGroup}

        for mapping in self._mappings:
            self._by_data_type[mapping.data_type] = mapping
            self._by_polars_type[mapping.polars_type] = mapping

            if mapping.type_group != TypeGroup.All:
                self._by_group[mapping.type_group].append(mapping)

            # Register all aliases (case-insensitive)
            for alias in mapping.aliases:
                self._by_alias[alias.lower()] = mapping

            # Register enum names as aliases
            self._by_alias[mapping.data_type.value.lower()] = mapping
            self._by_alias[mapping.polars_type.__name__.lower()] = mapping

            # Register "pl.TypeName" format
            self._by_alias[f"pl.{mapping.polars_type.__name__}".lower()] = mapping

    def normalize_preserving_groups(self, type_spec: Any) -> str | set[str]:
        """Map a TypeSpec to canonical tokens WITHOUT expanding groups.

        A group input yields its canonical group name (e.g. ``{"Numeric"}``); a
        specific input yields its canonical DataType value (e.g. ``{"Int64"}``).
        Returns the ``"ALL"`` sentinel or a set of canonical token strings. A bare
        string that names both a group and a specific type (``"String"``, ``"Date"``,
        ``"Boolean"``, ``"Binary"``) resolves to the group — matching the frontend's
        ``data_type_group`` semantics.
        """
        if type_spec == TypeGroup.All or type_spec == "ALL":
            return "ALL"
        if isinstance(type_spec, TypeGroup):
            return {type_spec.value}
        if isinstance(type_spec, DataType):
            return {type_spec.value}
        if isinstance(type_spec, type) and issubclass(type_spec, pl.DataType):
            mapping = self._by_polars_type.get(type_spec)
            if mapping:
                return {mapping.data_type.value}
        if isinstance(type_spec, pl.DataType):
            base_type = type_spec.base_type() if hasattr(type_spec, "base_type") else type(type_spec)
            mapping = self._by_polars_type.get(base_type)
            if mapping:
                return {mapping.data_type.value}
        if isinstance(type_spec, str):
            type_spec_lower = type_spec.lower()
            for group in TypeGroup:
                if group.lower() == type_spec_lower:
                    return "ALL" if group == TypeGroup.All else {group.value}
            try:
                return {DataType(type_spec).value}
            except (ValueError, KeyError):
                pass
            mapping = self._by_alias.get(type_spec_lower)
            if mapping:
                return {mapping.data_type.value}
        return set()

    def normalize_list_preserving_groups(self, type_specs: list[Any]) -> str | set[str]:
        """List form of ``normalize_preserving_groups``; any ``ALL`` collapses to ``"ALL"``."""
        result: set[str] = set()
        for spec in type_specs:
            item = self.normalize_preserving_groups(spec)
            if item == "ALL":
                return "ALL"
            result.update(item)
        return result


# Singleton instance
_registry = TypeRegistry()


def normalize_type_spec_preserving_groups(type_spec: Any) -> str | set[str]:
    """Normalize a type spec to canonical tokens, keeping group names un-expanded."""
    if isinstance(type_spec, list):
        return _registry.normalize_list_preserving_groups(type_spec)
    return _registry.normalize_preserving_groups(type_spec)
