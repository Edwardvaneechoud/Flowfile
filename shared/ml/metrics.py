"""Model evaluation metrics for the Evaluate Model node.

Pure polars expressions so the Evaluate node can run in-process on
``flowfile_core`` without a worker offload — the computation is a single-pass
aggregation over two columns and stays well below the cost of the train/apply
hops.

Output is intentionally a long-form ``(metric: Utf8, value: Float64)``
LazyFrame: it survives switching between task types and adding new metrics
without changing the schema downstream nodes see.
"""

from __future__ import annotations

import polars as pl

REGRESSION_METRICS: tuple[str, ...] = ("mae", "mse", "rmse", "r2", "mape", "n")
SUPPORTED_TASK_TYPES: tuple[str, ...] = ("regression",)


def compute_metrics(
    lf: pl.LazyFrame,
    actual_column: str,
    predicted_column: str,
    task_type: str,
) -> pl.LazyFrame:
    """Return a long-form ``(metric, value)`` LazyFrame for *task_type*.

    Both columns are cast to Float64 and rows where either side is null are
    dropped before aggregation, so a single missing prediction does not poison
    every metric.
    """
    schema_names = lf.collect_schema().names()
    for col in (actual_column, predicted_column):
        if col not in schema_names:
            raise ValueError(
                f"Evaluate Model: column {col!r} not found in input. "
                f"Available columns: {schema_names!r}."
            )
    if actual_column == predicted_column:
        raise ValueError(
            "Evaluate Model: 'actual_column' and 'predicted_column' must be different."
        )
    if task_type == "regression":
        return _regression_metrics(lf, actual_column, predicted_column)
    raise ValueError(
        f"Evaluate Model: unsupported task_type {task_type!r}. "
        f"Supported: {SUPPORTED_TASK_TYPES!r}."
    )


def _regression_metrics(
    lf: pl.LazyFrame,
    actual: str,
    predicted: str,
) -> pl.LazyFrame:
    base = lf.drop_nulls(subset=[actual, predicted]).with_columns(
        pl.col(actual).cast(pl.Float64).alias("__a"),
        pl.col(predicted).cast(pl.Float64).alias("__p"),
    )
    a = pl.col("__a")
    p = pl.col("__p")
    err = p - a
    abs_err = err.abs()
    sq_err = err.pow(2)
    # MAPE divides by |actual| — undefined when actual==0. Mask those rows
    # to null so the mean ignores them; if every actual is 0 the result is
    # NaN, which is more honest than silently reporting 0%.
    pct_err = pl.when(a == 0).then(None).otherwise(abs_err / a.abs())

    wide = base.select(
        abs_err.mean().alias("mae"),
        sq_err.mean().alias("mse"),
        sq_err.mean().sqrt().alias("rmse"),
        # R² = 1 - SS_res / SS_tot. Undefined when the actuals are all equal
        # (SS_tot=0); polars yields NaN there which we leave as-is.
        (pl.lit(1.0) - sq_err.sum() / (a - a.mean()).pow(2).sum()).alias("r2"),
        (pct_err.mean() * pl.lit(100.0)).alias("mape"),
        pl.len().cast(pl.Float64).alias("n"),
    )
    return wide.unpivot(
        on=list(REGRESSION_METRICS),
        variable_name="metric",
        value_name="value",
    ).with_columns(pl.col("value").cast(pl.Float64))
