"""The one definition of how an explode_hierarchy node calls polars-grouper.

The engine and the code generator both go through this module, so a flow and its exported
code cast the same columns and call the same function.

The plugin keeps parent/child ids as Int32/Int64/UInt32/UInt64/String and turns any other id
dtype into strings, but it panics on the small and 128-bit integers, Categorical, Enum and
Decimal, and fails on time-zone-aware datetimes. Casting up front (small integers to Int64,
everything else to String) gives what an Int64 or String input gives; where the plugin already
stringifies an id (floats, dates, naive datetimes, booleans, mixed dtypes) the result is unchanged.

The plugin also stringifies when parent and child have different dtypes, so two integer id columns
of different widths or signedness are first cast to their common integer type. When none exists
without loss (UInt64 with a signed column) each column is cast on its own, as above.
"""

from collections.abc import Callable

import polars as pl
from polars_grouper import hierarchy_levels, hierarchy_paths, hierarchy_totals

from flowfile_core.schemas.transform_schema import ExplodeHierarchyInput, HierarchyOutputDetail

HIERARCHY_OUTPUT_ALIAS = "hierarchy"

HIERARCHY_FUNCTIONS: dict[HierarchyOutputDetail, Callable[..., pl.Expr]] = {
    "totals": hierarchy_totals,
    "levels": hierarchy_levels,
    "paths": hierarchy_paths,
}

KEPT_NODE_ID_TYPES = frozenset({pl.String, pl.Int32, pl.Int64, pl.UInt32, pl.UInt64})
WIDENED_NODE_ID_TYPES = frozenset({pl.Int8, pl.Int16, pl.UInt8, pl.UInt16})
INTEGER_NODE_ID_BITS: dict[type[pl.DataType], tuple[bool, int]] = {
    pl.Int8: (True, 8),
    pl.Int16: (True, 16),
    pl.Int32: (True, 32),
    pl.Int64: (True, 64),
    pl.UInt8: (False, 8),
    pl.UInt16: (False, 16),
    pl.UInt32: (False, 32),
    pl.UInt64: (False, 64),
}


def hierarchy_function_name(detail: HierarchyOutputDetail) -> str:
    """The polars_grouper function name for an output detail, for generated imports and calls."""
    return HIERARCHY_FUNCTIONS[detail].__name__


def hierarchy_node_id_cast(dtype: pl.DataType | type[pl.DataType]) -> pl.DataType | None:
    """The dtype a parent/child column must be cast to before the plugin sees it, or None to pass it as is."""
    base = dtype.base_type()
    if base in KEPT_NODE_ID_TYPES:
        return None
    if base in WIDENED_NODE_ID_TYPES:
        return pl.Int64()
    return pl.String()


def _common_integer_node_id_type(
    parent: pl.DataType | type[pl.DataType], child: pl.DataType | type[pl.DataType]
) -> pl.DataType | None:
    """The integer dtype the plugin keeps that holds every value of both columns, or None if there is none."""
    kinds = [INTEGER_NODE_ID_BITS.get(parent.base_type()), INTEGER_NODE_ID_BITS.get(child.base_type())]
    if None in kinds:
        return None
    signed = kinds[0][0] or kinds[1][0]
    bits = max(width if is_signed == signed else 2 * width for is_signed, width in kinds)
    if bits > 64:
        return None
    if bits < 32:
        return pl.Int64()
    return getattr(pl, f"{'Int' if signed else 'UInt'}{bits}")()


def hierarchy_node_id_casts(
    parent: pl.DataType | type[pl.DataType] | None, child: pl.DataType | type[pl.DataType] | None
) -> tuple[pl.DataType | None, pl.DataType | None]:
    """The (parent, child) casts to apply before the plugin sees the ids; None for a missing column or no cast.

    Integer ids share a dtype whenever one holds both losslessly, so the output ids stay integers.
    """
    common = None if parent is None or child is None else _common_integer_node_id_type(parent, child)
    if common is not None:
        return tuple(None if dtype.base_type() == common.base_type() else common for dtype in (parent, child))
    return tuple(None if dtype is None else hierarchy_node_id_cast(dtype) for dtype in (parent, child))


def _node_id_exprs(parent: str, child: str, schema: pl.Schema) -> list[pl.Expr]:
    casts = hierarchy_node_id_casts(schema.get(parent), schema.get(child))
    return [
        pl.col(name) if cast is None else pl.col(name).cast(cast)
        for name, cast in zip((parent, child), casts, strict=True)
    ]


def explode_hierarchy_frame(lf: pl.LazyFrame, settings: ExplodeHierarchyInput) -> pl.LazyFrame:
    """Explode ``lf``'s parent -> child edges into the table ``settings.output_detail`` describes.

    Reads the input schema only; nothing is collected, so cycles and null quantities surface as
    a ``ComputeError`` when the result is collected. Empty or identical parent/child columns
    raise ``ValueError`` straight away.
    """
    settings.check_edge_columns()
    schema = lf.collect_schema()
    quantity = pl.col(settings.quantity_column).cast(pl.Float64) if settings.quantity_column else None
    function = HIERARCHY_FUNCTIONS[settings.output_detail]
    return lf.select(
        function(
            *_node_id_exprs(settings.parent_column, settings.child_column, schema),
            quantity,
            top_level_only=settings.top_level_only,
            include_self=settings.include_self,
            max_depth=settings.max_depth,
        ).alias(HIERARCHY_OUTPUT_ALIAS)
    ).unnest(HIERARCHY_OUTPUT_ALIAS)
