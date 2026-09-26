"""The one definition of how an explode_hierarchy node calls polars-grouper.

The engine and the code generator both go through this module, so a flow and its exported
code cast the same columns and call the same function.

The plugin keeps parent/child ids as Int32/Int64/UInt32/UInt64/String and turns any other id
dtype into strings, but it panics on the small and 128-bit integers, Categorical, Enum and
Decimal, and fails on time-zone-aware datetimes. Casting up front (small integers to Int64,
everything else to String) gives what an Int64 or String input gives; where the plugin already
stringifies an id (floats, dates, naive datetimes, booleans, mixed dtypes) the result is unchanged.
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


def _node_id_expr(name: str, schema: pl.Schema) -> pl.Expr:
    expr = pl.col(name)
    if name not in schema:
        return expr
    cast = hierarchy_node_id_cast(schema[name])
    return expr if cast is None else expr.cast(cast)


def explode_hierarchy_frame(lf: pl.LazyFrame, settings: ExplodeHierarchyInput) -> pl.LazyFrame:
    """Explode ``lf``'s parent -> child edges into the table ``settings.output_detail`` describes.

    Reads the input schema only; nothing is collected, so cycles and null quantities surface as
    a ``ComputeError`` when the result is collected.
    """
    schema = lf.collect_schema()
    quantity = pl.col(settings.quantity_column).cast(pl.Float64) if settings.quantity_column else None
    function = HIERARCHY_FUNCTIONS[settings.output_detail]
    return lf.select(
        function(
            _node_id_expr(settings.parent_column, schema),
            _node_id_expr(settings.child_column, schema),
            quantity,
            top_level_only=settings.top_level_only,
            include_self=settings.include_self,
            max_depth=settings.max_depth,
        ).alias(HIERARCHY_OUTPUT_ALIAS)
    ).unnest(HIERARCHY_OUTPUT_ALIAS)
