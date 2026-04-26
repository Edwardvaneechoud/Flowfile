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
CLASSIFICATION_METRICS: tuple[str, ...] = (
    "accuracy",
    "precision",
    "recall",
    "f1",
    "n_correct",
    "n_total",
)
SUPPORTED_TASK_TYPES: tuple[str, ...] = ("regression", "classification")


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
    if task_type == "classification":
        return _classification_metrics(lf, actual_column, predicted_column)
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


def _classification_metrics(
    lf: pl.LazyFrame,
    actual: str,
    predicted: str,
) -> pl.LazyFrame:
    """Macro-averaged classification metrics in long form.

    Both columns are cast to Utf8 so integer 0/1 labels and string labels share
    one grouping path. Macro-averaging treats each class equally regardless of
    support — fair on imbalanced data, which is the common classification case.

    Polars has no native confusion-matrix expression and the per-class loop is
    awkward in pure expressions, so we collect the two label vectors once and
    compute in Python. Aggregated label counts are tiny relative to the prior
    train/apply hops, so the collect cost is negligible.
    """
    base = (
        lf.drop_nulls(subset=[actual, predicted])
        .with_columns(
            pl.col(actual).cast(pl.Utf8).alias("__a"),
            pl.col(predicted).cast(pl.Utf8).alias("__p"),
        )
        .select("__a", "__p")
        .collect()
    )
    n_total = base.height
    if n_total == 0:
        return _empty_classification_metrics()

    correct = int((base["__a"] == base["__p"]).sum())
    accuracy = correct / n_total
    classes = sorted(set(base["__a"].to_list()) | set(base["__p"].to_list()))

    precisions: list[float] = []
    recalls: list[float] = []
    f1s: list[float] = []
    for cls in classes:
        a_eq = base["__a"] == cls
        p_eq = base["__p"] == cls
        tp = int((a_eq & p_eq).sum())
        fp = int((~a_eq & p_eq).sum())
        fn = int((a_eq & ~p_eq).sum())
        # Zero-denominator -> 0.0 (sklearn's zero_division=0). NaN would
        # propagate through the macro mean and silently destroy F1 whenever
        # any single class has no predicted positives.
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
        precisions.append(prec)
        recalls.append(rec)
        f1s.append(f1)

    n_classes = len(classes) or 1
    return pl.LazyFrame(
        {
            "metric": list(CLASSIFICATION_METRICS),
            "value": [
                float(accuracy),
                sum(precisions) / n_classes,
                sum(recalls) / n_classes,
                sum(f1s) / n_classes,
                float(correct),
                float(n_total),
            ],
        }
    ).with_columns(pl.col("value").cast(pl.Float64))


def _empty_classification_metrics() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "metric": list(CLASSIFICATION_METRICS),
            "value": [0.0] * len(CLASSIFICATION_METRICS),
        }
    ).with_columns(pl.col("value").cast(pl.Float64))
