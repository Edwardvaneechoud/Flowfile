"""Linear regression fit via polars-ds.

Phase 1 of the data-science pack ships a single estimator family.
The artefact is a JSON-serialisable dict that round-trips through the
Catalog; the predict side (:mod:`shared.data_science.predict`) reads it
and builds a pure Polars expression — so downstream flows stay lazy.

Scaling to other estimator families (ridge, lasso, logistic, …) is a
new node per family, not another branch in a dispatch dict: each family
has its own hyperparam shape and deserves its own typed settings form.
They can reuse ``predict.apply_predict`` when the coefficients+bias
shape matches.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

NullPolicy = Literal["raise", "skip", "zero", "one", "ignore"]
Solver = Literal["qr", "cholesky", "svd"]

# Schema of the preview DataFrame surfaced to the downstream graph. Used by
# the fit node's ``schema_callback`` so the flow knows the shape before run.
PREVIEW_SCHEMA: list[dict[str, str]] = [
    {"name": "feature", "data_type": "String"},
    {"name": "coefficient", "data_type": "Float64"},
]


def fit_linear_regression(
    df: pl.DataFrame | pl.LazyFrame,
    feature_cols: list[str],
    target_col: str,
    *,
    fit_intercept: bool = True,
    null_policy: NullPolicy = "skip",
    solver: Solver = "qr",
    prediction_col: str = "prediction",
) -> tuple[dict[str, Any], pl.DataFrame, list[dict[str, str]]]:
    """Fit OLS and return ``(artefact, preview, output_schema)``.

    Args:
        df: Training data — eager or lazy; polars-ds collects internally.
        feature_cols: Ordered list of predictor column names.
        target_col: Column to regress on. Must not be ``None``.
        fit_intercept: Whether to fit a bias term (intercept).
        null_policy: How polars-ds handles nulls in feature columns.
            ``"skip"`` drops affected rows; ``"zero"`` / ``"one"`` fill
            with the constant; ``"raise"`` surfaces them; or pass a
            numeric string to fill with that value. Rows whose target
            is null are always dropped.
        solver: Least-squares solver. ``"qr"`` is the polars-ds default
            and numerically stable; ``"svd"`` is slower but handles
            rank-deficient matrices.
        prediction_col: Name of the column the predict node will append.
            Recorded in ``output_schema`` so downstream nodes can
            resolve their schema lazily.

    Returns:
        artefact: JSON-serialisable dict with ``{kind, coeffs, bias,
            feature_names, fit_intercept, solver, null_policy}``.
        preview: Small Polars DataFrame shown to the user
            (coefficients + intercept row).
        output_schema: One-row list describing the predict column.
    """
    if not feature_cols:
        raise ValueError("feature_cols must contain at least one column.")
    if not target_col:
        raise ValueError("target_col is required for linear regression.")

    from polars_ds.linear_models import LR

    model = LR(has_bias=fit_intercept, solver=solver)
    model.fit_df(df, features=feature_cols, target=target_col, null_policy=null_policy)

    coeffs = [float(c) for c in model.coeffs().tolist()]
    bias = float(model.bias())

    artefact: dict[str, Any] = {
        "kind": "linear_regression",
        "coeffs": coeffs,
        "bias": bias,
        "feature_names": list(feature_cols),
        "fit_intercept": fit_intercept,
        "solver": solver,
        "null_policy": null_policy,
    }

    preview_rows: list[tuple[str, float]] = list(zip(feature_cols, coeffs, strict=True))
    if fit_intercept:
        preview_rows.append(("__intercept__", bias))
    preview = pl.DataFrame(
        {
            "feature": [r[0] for r in preview_rows],
            "coefficient": [r[1] for r in preview_rows],
        }
    )

    output_schema = [{"name": prediction_col, "data_type": "Float64"}]
    return artefact, preview, output_schema
