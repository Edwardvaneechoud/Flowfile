# _type_registry.py - Internal type system (not for public use)
"""
Internal type registry for mapping between different type representations.
This module should not be imported directly by users.
"""

from dataclasses import dataclass
from typing import Type, List, Dict, Set, Any
import polars as pl

# Import public types
from flowfile_core.types import TypeGroup, DataType


@dataclass(frozen=True)
class TypeMapping:
    """Internal mapping between type representations."""
    data_type: DataType
    polars_type: Type[pl.DataType]
    type_group: TypeGroup
    aliases: tuple[str, ...] = ()


class TypeRegistry:
    """
    Internal registry for type conversions and lookups.
    This class is not part of the public API.
    """

    def __init__(self):
        self._mappings: List[TypeMapping] = [
            # Numeric types
            TypeMapping(DataType.Int8, pl.Int8, TypeGroup.Numeric, ("i8",)),
            TypeMapping(DataType.Int16, pl.Int16, TypeGroup.Numeric, ("i16",)),
            TypeMapping(DataType.Int32, pl.Int32, TypeGroup.Numeric, ("i32", "int32")),
            TypeMapping(DataType.Int64, pl.Int64, TypeGroup.Numeric,
                        ("i64", "int64", "int", "integer", "bigint")),
            TypeMapping(DataType.UInt8, pl.UInt8, TypeGroup.Numeric, ("u8",)),
            TypeMapping(DataType.UInt16, pl.UInt16, TypeGroup.Numeric, ("u16",)),
            TypeMapping(DataType.UInt32, pl.UInt32, TypeGroup.Numeric, ("u32", "uint32")),
            TypeMapping(DataType.UInt64, pl.UInt64, TypeGroup.Numeric, ("u64", "uint64")),
            TypeMapping(DataType.Float32, pl.Float32, TypeGroup.Numeric, ("f32", "float32")),
            TypeMapping(DataType.Float64, pl.Float64, TypeGroup.Numeric,
                        ("f64", "float64", "float", "double")),
            TypeMapping(DataType.Decimal, pl.Decimal, TypeGroup.Numeric,
                        ("decimal", "numeric", "dec")),

            # String types
            TypeMapping(DataType.String, pl.String, TypeGroup.String,
                        ("str", "string", "utf8", "varchar", "text")),
            TypeMapping(DataType.Categorical, pl.Categorical, TypeGroup.String,
                        ("cat", "categorical", "enum", "factor")),

            # Date types
            TypeMapping(DataType.Date, pl.Date, TypeGroup.Date, ("date",)),
            TypeMapping(DataType.Datetime, pl.Datetime, TypeGroup.Date,
                        ("datetime", "timestamp")),
            TypeMapping(DataType.Time, pl.Time, TypeGroup.Date, ("time",)),
            TypeMapping(DataType.Duration, pl.Duration, TypeGroup.Date,
                        ("duration", "timedelta")),

            # Other types
            TypeMapping(DataType.Boolean, pl.Boolean, TypeGroup.Boolean,
                        ("bool", "boolean")),
            TypeMapping(DataType.Binary, pl.Binary, TypeGroup.Binary,
                        ("binary", "bytes", "bytea")),
            TypeMapping(DataType.List, pl.List, TypeGroup.Complex, ("list", "array")),
            TypeMapping(DataType.Struct, pl.Struct, TypeGroup.Complex, ("struct", "object")),
            TypeMapping(DataType.Array, pl.Array, TypeGroup.Complex, ("fixed_array",)),
        ]

        self._build_indices()

    def _build_indices(self):
        """Build lookup indices for fast access."""
        self._by_data_type: Dict[DataType, TypeMapping] = {}
        self._by_polars_type: Dict[Type[pl.DataType], TypeMapping] = {}
        self._by_alias: Dict[str, TypeMapping] = {}
        self._by_group: Dict[TypeGroup, List[TypeMapping]] = {g: [] for g in TypeGroup}

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

    def normalize(self, type_spec: Any) -> Set[DataType]:
        """
        Normalize any type specification to a set of DataType enums.
        This is the main internal API for type resolution.
        """
        # Handle special case: All types
        if type_spec == TypeGroup.All or type_spec == "ALL":
            return set(self._by_data_type.keys())

        # Handle TypeGroup
        if isinstance(type_spec, TypeGroup):
            return {m.data_type for m in self._by_group.get(type_spec, [])}

        # Handle DataType
        if isinstance(type_spec, DataType):
            return {type_spec}

        # Handle Polars type class
        if isinstance(type_spec, type) and issubclass(type_spec, pl.DataType):
            mapping = self._by_polars_type.get(type_spec)
            if mapping:
                return {mapping.data_type}

        # Handle Polars type instance
        if isinstance(type_spec, pl.DataType):
            base_type = type_spec.base_type() if hasattr(type_spec, 'base_type') else type(type_spec)
            mapping = self._by_polars_type.get(base_type)
            if mapping:
                return {mapping.data_type}

        # Handle string aliases
        if isinstance(type_spec, str):
            type_spec_lower = type_spec.lower()

            # Try TypeGroup name
            try:
                group = TypeGroup(type_spec)
                return {m.data_type for m in self._by_group.get(group, [])}
            except (ValueError, KeyError):
                pass

            # Try DataType name
            try:
                dt = DataType(type_spec)
                return {dt}
            except (ValueError, KeyError):
                pass

            # Check aliases
            mapping = self._by_alias.get(type_spec_lower)
            if mapping:
                return {mapping.data_type}

        # Default to empty set if unrecognized
        return set()

    def normalize_list(self, type_specs: List[Any]) -> Set[DataType]:
        """Normalize a list of type specifications."""
        result = set()
        for spec in type_specs:
            result.update(self.normalize(spec))
        return result

    def get_polars_types(self, data_types: Set[DataType]) -> Set[Type[pl.DataType]]:
        """Convert a set of DataType enums to Polars types."""
        result = set()
        for dt in data_types:
            mapping = self._by_data_type.get(dt)
            if mapping:
                result.add(mapping.polars_type)
        return result

    def get_polars_type(self, data_type: DataType) -> Type[pl.DataType]:
        """Get the Polars type for a single DataType."""
        mapping = self._by_data_type.get(data_type)
        return mapping.polars_type if mapping else pl.String  # Default fallback


# Singleton instance
_registry = TypeRegistry()


# Internal API functions (not for public use)
def normalize_type_spec(type_spec: Any) -> Set[DataType]:
    """Internal function to normalize type specifications."""
    if isinstance(type_spec, list):
        return _registry.normalize_list(type_spec)
    return _registry.normalize(type_spec)


def get_polars_types(data_types: Set[DataType]) -> Set[Type[pl.DataType]]:
    """Internal function to get Polars types."""
    return _registry.get_polars_types(data_types)


def check_column_type(column_dtype: pl.DataType, accepted_types: Set[DataType]) -> bool:
    """Check if a column's dtype matches the accepted types."""
    normalized = _registry.normalize(column_dtype)
    return bool(normalized & accepted_types)
