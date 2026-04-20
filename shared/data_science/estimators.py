"""Estimator dispatch for the Data Science Fit node.

Each entry in ``FIT_KINDS`` maps a ``kind`` string (the user-facing dropdown
value) to a function that fits an estimator on a Polars DataFrame and returns:

    (artefact_dict, preview_df, output_schema)

* ``artefact_dict`` is a JSON-serialisable ``dict`` capturing everything the
  predict side needs (coefficients, bias, feature names). Persisting as JSON
  instead of pickle makes artefacts portable across Python versions and
  human-inspectable.
* ``preview_df`` is a small Polars DataFrame surfaced to the flow's downstream
  graph so users can inspect coefficients/metrics without running predict.
* ``output_schema`` is a list of ``{"name": str, "data_type": str}`` dicts
  describing the columns this artefact will produce when applied via predict.
  Persisted alongside the artefact so consumer nodes can compute their
  schema lazily, before any data movement.

Phase 1 ships only ``linreg`` end-to-end via ``polars_ds.linear_models.LR``.
The other entries are stubs that raise ``NotImplementedError`` so they
appear in the dropdown today and become trivial follow-up additions.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

FitKind = Literal["linreg", "ridge", "lasso", "kmeans", "knn_cls", "knn_reg", "pca"]

# Schema of the preview DataFrame each fit emits — used by add-time
# schema_callback so that the downstream graph knows the shape before run.
PREVIEW_SCHEMAS: dict[str, list[dict[str, str]]] = {
    "linreg": [
        {"name": "feature", "data_type": "String"},
        {"name": "coefficient", "data_type": "Float64"},
    ],
}


def _fit_linreg(
    df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    hyperparams: dict[str, Any],
    prediction_col: str,
) -> tuple[dict[str, Any], pl.DataFrame, list[dict[str, str]]]:
    """Fit OLS via ``polars_ds.linear_models.LR`` and return a JSON artefact."""
    from polars_ds.linear_models import LR

    if target_col is None:
        raise ValueError("Linear regression requires a target column.")

    model = LR(has_bias=True, **hyperparams)
    model.fit_df(df, features=feature_cols, target=target_col)

    coeffs = [float(c) for c in model.coeffs().tolist()]
    bias = float(model.bias())

    artefact: dict[str, Any] = {
        "kind": "linreg",
        "coeffs": coeffs,
        "bias": bias,
        "feature_names": list(feature_cols),
    }

    preview_df = pl.DataFrame(
        {
            "feature": [*feature_cols, "__intercept__"],
            "coefficient": [*coeffs, bias],
        }
    )

    output_schema = [{"name": prediction_col, "data_type": "Float64"}]
    return artefact, preview_df, output_schema


def _not_implemented(kind: str):
    def _stub(*_args, **_kwargs):
        raise NotImplementedError(
            f"Estimator '{kind}' is registered but not yet wired. "
            "Open an issue or PR to add it; the surrounding plumbing is "
            "already in place — only the fit function is missing."
        )

    return _stub


FIT_KINDS = {
    "linreg": _fit_linreg,
    "ridge": _not_implemented("ridge"),
    "lasso": _not_implemented("lasso"),
    "kmeans": _not_implemented("kmeans"),
    "knn_cls": _not_implemented("knn_cls"),
    "knn_reg": _not_implemented("knn_reg"),
    "pca": _not_implemented("pca"),
}

SUPERVISED_KINDS = {"linreg", "ridge", "lasso", "knn_cls", "knn_reg"}


def fit_estimator(
    kind: str,
    df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str | None,
    hyperparams: dict[str, Any] | None = None,
    prediction_col: str = "prediction",
) -> tuple[dict[str, Any], pl.DataFrame, list[dict[str, str]]]:
    """Dispatch ``kind`` to the appropriate fit function with validation."""
    if kind not in FIT_KINDS:
        raise ValueError(f"Unknown estimator kind: {kind!r}. Known: {sorted(FIT_KINDS)}")
    if kind in SUPERVISED_KINDS and not target_col:
        raise ValueError(f"Estimator '{kind}' is supervised and requires target_col.")
    if not feature_cols:
        raise ValueError("feature_cols must contain at least one column.")
    return FIT_KINDS[kind](df, feature_cols, target_col, hyperparams or {}, prediction_col)
