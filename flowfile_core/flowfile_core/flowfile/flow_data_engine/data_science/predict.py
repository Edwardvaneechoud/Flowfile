"""Apply a fitted estimator to a Polars DataFrame."""

from __future__ import annotations

from typing import Any

import polars as pl


def apply_predict(
    df: pl.DataFrame,
    estimator: Any,
    feature_cols: list[str],
    prediction_col: str = "prediction",
) -> pl.DataFrame:
    """Append a prediction column produced by ``estimator.predict``."""
    if not feature_cols:
        raise ValueError("feature_cols must contain at least one column.")
    if not hasattr(estimator, "predict"):
        raise TypeError(f"Estimator {type(estimator).__name__} has no .predict method.")

    X = df.select(feature_cols).to_numpy()
    preds = estimator.predict(X)
    return df.with_columns(pl.Series(name=prediction_col, values=preds))
