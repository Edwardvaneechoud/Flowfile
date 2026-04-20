"""Apply a fitted artefact to a Polars LazyFrame — fully lazy."""

from __future__ import annotations

from typing import Any

import polars as pl


def apply_predict(
    lf: pl.LazyFrame | pl.DataFrame,
    artefact: dict[str, Any],
    feature_cols: list[str],
    prediction_col: str = "prediction",
) -> pl.LazyFrame:
    """Append a ``prediction_col`` computed from the artefact's coefficients.

    Works purely in the Polars expression layer — no ``.to_numpy()`` / no
    materialisation. Downstream nodes receive the unchanged lazy plan with
    one column appended.
    """
    if not feature_cols:
        raise ValueError("feature_cols must contain at least one column.")

    required_keys = {"coeffs", "bias", "feature_names"}
    missing = required_keys - set(artefact)
    if missing:
        raise ValueError(f"Artefact is missing keys: {sorted(missing)}")

    coeffs = artefact["coeffs"]
    bias = float(artefact["bias"])
    trained_features = list(artefact["feature_names"])

    if list(feature_cols) != trained_features:
        raise ValueError(
            f"feature_cols {list(feature_cols)} do not match the artefact's "
            f"trained features {trained_features}. The predict node must "
            "receive the same feature columns (in the same order) used at fit time."
        )

    lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf

    expr = pl.sum_horizontal(
        [pl.lit(c) * pl.col(f) for c, f in zip(coeffs, feature_cols, strict=True)]
    ) + pl.lit(bias)

    return lf.with_columns(expr.alias(prediction_col))
