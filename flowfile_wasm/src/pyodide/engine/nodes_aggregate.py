import gc

import polars as pl

from .errors import format_error, format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe


def _build_agg_exprs(agg_defs: list[dict]) -> list:
    """Build aggregation expressions from agg column definitions."""
    exprs = []
    for a in agg_defs:
        col = pl.col(a["old_name"])
        agg = a.get("agg", "count")
        new_name = a.get("new_name", f"{a['old_name']}_{agg}")

        if agg == "sum":
            exprs.append(col.sum().alias(new_name))
        elif agg == "max":
            exprs.append(col.max().alias(new_name))
        elif agg == "min":
            exprs.append(col.min().alias(new_name))
        elif agg == "count":
            exprs.append(col.count().alias(new_name))
        elif agg == "mean":
            exprs.append(col.mean().alias(new_name))
        elif agg == "median":
            exprs.append(col.median().alias(new_name))
        elif agg == "first":
            exprs.append(col.first().alias(new_name))
        elif agg == "last":
            exprs.append(col.last().alias(new_name))
        elif agg == "n_unique":
            exprs.append(col.n_unique().alias(new_name))
        elif agg == "concat":
            exprs.append(col.cast(pl.Utf8).str.join(",").alias(new_name))
    return exprs


def build_group_by(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the grouped/aggregated LazyFrame (no store, no collect)."""
    groupby_input = settings.get("groupby_input", {})
    agg_cols = groupby_input.get("agg_cols", [])

    if not agg_cols:
        return input_lf

    group_cols = [pl.col(c["old_name"]).alias(c["new_name"]) for c in agg_cols if c.get("agg") == "groupby"]
    agg_defs = [c for c in agg_cols if c.get("agg") != "groupby"]
    exprs = _build_agg_exprs(agg_defs)

    if not group_cols:
        return input_lf.select(exprs) if exprs else input_lf
    if exprs:
        return input_lf.group_by(group_cols).agg(exprs)
    return input_lf.group_by(group_cols).agg(pl.count()).drop("count")


@log_node
def execute_group_by(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute group by node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Group By error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_group_by(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("group_by", node_id, e, input_lf)}


@log_node
def execute_pivot(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute pivot node - converts data from long to wide format
    Note: Pivot requires collecting data due to dynamic column creation.
    Memory-optimized: cleans up intermediate DataFrames."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Pivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    df = None
    grouped = None
    result = None

    try:
        # Pivot needs to collect to determine unique values for columns
        df = input_lf.collect()

        pivot_input = settings.get("pivot_input", {})
        index_columns = pivot_input.get("index_columns", [])
        pivot_column = pivot_input.get("pivot_column", "")
        value_col = pivot_input.get("value_col", "")
        aggregations = pivot_input.get("aggregations", ["sum"])

        if not pivot_column:
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Pivot error on node #{node_id}: No pivot column specified. Please select a column whose values will become new columns.",
            }
        if not value_col:
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Pivot error on node #{node_id}: No value column specified. Please select a column containing values to aggregate.",
            }
        if pivot_column not in df.columns:
            cols = list(df.columns)
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' not found. Available columns: {cols}",
            }
        if value_col not in df.columns:
            cols = list(df.columns)
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Pivot error on node #{node_id}: Value column '{value_col}' not found. Available columns: {cols}",
            }

        max_unique = 200
        unique_values = (
            df.select(pl.col(pivot_column).cast(pl.String))
            .unique()
            .sort(pivot_column)
            .limit(max_unique)
            .to_series()
            .to_list()
        )

        if len(unique_values) >= max_unique:
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Pivot error on node #{node_id}: Pivot column '{pivot_column}' has too many unique values (>={max_unique}). Please use a column with fewer unique values.",
            }

        group_cols = index_columns + [pivot_column] if index_columns else [pivot_column]

        agg_map = {
            "sum": lambda c: pl.col(c).sum(),
            "mean": lambda c: pl.col(c).mean(),
            "min": lambda c: pl.col(c).min(),
            "max": lambda c: pl.col(c).max(),
            "count": lambda c: pl.col(c).count(),
            "first": lambda c: pl.col(c).first(),
            "last": lambda c: pl.col(c).last(),
            "median": lambda c: pl.col(c).median(),
        }

        agg_exprs = []
        for agg in aggregations:
            if agg in agg_map:
                agg_exprs.append(agg_map[agg](value_col).alias(agg))
            else:
                agg_exprs.append(pl.col(value_col).sum().alias(agg))

        if not agg_exprs:
            agg_exprs = [pl.col(value_col).sum().alias("sum")]

        grouped = df.group_by(group_cols).agg(agg_exprs)

        # Free original df memory immediately after grouping
        del df
        df = None

        if index_columns:
            index_exprs = [pl.col(c) for c in index_columns]
        else:
            grouped = grouped.with_columns(pl.lit(1).alias("__temp_idx__"))
            index_columns = ["__temp_idx__"]
            index_exprs = [pl.col("__temp_idx__")]

        pivot_exprs = []
        for unique_val in unique_values:
            for agg in aggregations:
                col_name = f"{unique_val}_{agg}" if len(aggregations) > 1 else str(unique_val)
                pivot_exprs.append(pl.col(agg).filter(pl.col(pivot_column) == unique_val).first().alias(col_name))

        result = grouped.group_by(index_exprs).agg(pivot_exprs)

        # Free grouped memory
        del grouped
        grouped = None

        if "__temp_idx__" in result.columns:
            result = result.drop("__temp_idx__")

        result_lf = result.lazy()
        store_lazyframe(node_id, result_lf)

        # Free result memory (rebind, not del, so the except cleanup below stays bound)
        result = None
        gc.collect()

        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        # Clean up on error
        if df is not None:
            del df
        if grouped is not None:
            del grouped
        if result is not None:
            del result
        gc.collect()
        return {"success": False, "error": format_error("pivot", node_id, e)}


def build_unpivot(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the unpivoted (wide->long) LazyFrame (no store, no collect)."""
    unpivot_input = settings.get("unpivot_input", {})
    index_columns = unpivot_input.get("index_columns", [])
    value_columns = unpivot_input.get("value_columns", [])
    data_type_selector = unpivot_input.get("data_type_selector")
    selector_mode = unpivot_input.get("data_type_selector_mode", "column")

    if selector_mode == "data_type" and data_type_selector:
        import polars.selectors as cs

        selector_map = {
            "float": cs.float,
            "numeric": cs.numeric,
            "string": cs.string,
            "date": cs.temporal,
            "all": cs.all,
        }
        if data_type_selector in selector_map:
            on_selector = selector_map[data_type_selector]()
        else:
            on_selector = cs.all()
        return input_lf.unpivot(on=on_selector, index=index_columns if index_columns else None)
    elif value_columns:
        schema = input_lf.collect_schema()
        missing = [c for c in value_columns if c not in schema]
        if missing:
            raise ValueError(f"Columns not found: {missing}. Available columns: {list(schema.keys())}")
        return input_lf.unpivot(on=value_columns, index=index_columns if index_columns else None)
    return input_lf.unpivot(index=index_columns if index_columns else None)


@log_node
def execute_unpivot(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute unpivot node - converts data from wide to long format (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Unpivot error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_unpivot(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("unpivot", node_id, e, input_lf)}
