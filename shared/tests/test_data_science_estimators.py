"""Unit tests for the linear-regression fit + predict pair."""

from __future__ import annotations

import json
import math

import polars as pl
import pytest

from shared.data_science.estimators import PREVIEW_SCHEMA, fit_linear_regression
from shared.data_science.predict import apply_predict


@pytest.fixture
def linreg_dataset() -> pl.DataFrame:
    # y = 2*x1 + 3*x2 with non-collinear features so the coefficients are
    # uniquely identifiable.
    return pl.DataFrame(
        {
            "x1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "x2": [3.0, 1.0, 4.0, 1.0, 5.0],
            "y": [11.0, 7.0, 18.0, 11.0, 25.0],
        }
    )


def test_fit_returns_json_artefact(linreg_dataset: pl.DataFrame) -> None:
    artefact, preview, output_schema = fit_linear_regression(
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
        prediction_col="prediction",
    )

    assert artefact["kind"] == "linear_regression"
    assert artefact["feature_names"] == ["x1", "x2"]
    assert artefact["fit_intercept"] is True
    assert artefact["solver"] == "qr"
    assert artefact["null_policy"] == "skip"

    assert math.isclose(artefact["coeffs"][0], 2.0, abs_tol=1e-6)
    assert math.isclose(artefact["coeffs"][1], 3.0, abs_tol=1e-6)
    assert math.isclose(artefact["bias"], 0.0, abs_tol=1e-6)

    # Preview matches the artefact so the UI preview is truthful.
    preview_coefs = dict(zip(preview["feature"].to_list(), preview["coefficient"].to_list(), strict=True))
    assert math.isclose(preview_coefs["x1"], 2.0, abs_tol=1e-6)
    assert math.isclose(preview_coefs["x2"], 3.0, abs_tol=1e-6)
    assert math.isclose(preview_coefs["__intercept__"], 0.0, abs_tol=1e-6)

    assert output_schema == [{"name": "prediction", "data_type": "Float64"}]


def test_artefact_round_trips_through_json(linreg_dataset: pl.DataFrame) -> None:
    artefact, _, _ = fit_linear_regression(
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )
    assert json.loads(json.dumps(artefact)) == artefact


def test_fit_without_intercept_omits_intercept_row(linreg_dataset: pl.DataFrame) -> None:
    artefact, preview, _ = fit_linear_regression(
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
        fit_intercept=False,
    )
    assert artefact["fit_intercept"] is False
    assert "__intercept__" not in preview["feature"].to_list()


def test_fit_accepts_alternate_solvers(linreg_dataset: pl.DataFrame) -> None:
    for solver in ("qr", "cholesky", "svd"):
        artefact, _, _ = fit_linear_regression(
            df=linreg_dataset,
            feature_cols=["x1", "x2"],
            target_col="y",
            solver=solver,
        )
        assert artefact["solver"] == solver
        # Coefficients should agree across solvers on well-conditioned data.
        assert math.isclose(artefact["coeffs"][0], 2.0, abs_tol=1e-6)
        assert math.isclose(artefact["coeffs"][1], 3.0, abs_tol=1e-6)


def test_fit_requires_feature_cols() -> None:
    with pytest.raises(ValueError, match="feature_cols"):
        fit_linear_regression(
            df=pl.DataFrame({"y": [1.0, 2.0]}),
            feature_cols=[],
            target_col="y",
        )


def test_fit_requires_target_col(linreg_dataset: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="target_col"):
        fit_linear_regression(
            df=linreg_dataset,
            feature_cols=["x1", "x2"],
            target_col="",
        )


def test_fit_accepts_lazyframe_input(linreg_dataset: pl.DataFrame) -> None:
    # polars-ds accepts both eager and lazy frames; confirm we pass through.
    artefact, _, _ = fit_linear_regression(
        df=linreg_dataset.lazy(),
        feature_cols=["x1", "x2"],
        target_col="y",
    )
    assert math.isclose(artefact["coeffs"][0], 2.0, abs_tol=1e-6)


def test_predict_is_lazy_and_round_trips(linreg_dataset: pl.DataFrame) -> None:
    artefact, _, _ = fit_linear_regression(
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )

    new_data = pl.LazyFrame({"x1": [6.0, 7.0], "x2": [2.0, 8.0], "note": ["a", "b"]})
    out = apply_predict(new_data, artefact, ["x1", "x2"], "prediction")

    assert isinstance(out, pl.LazyFrame), "predict must stay lazy"
    collected = out.collect()
    assert "prediction" in collected.columns
    # Non-feature columns pass through unchanged.
    assert collected["note"].to_list() == ["a", "b"]
    # y = 2*x1 + 3*x2 → 18 and 38
    assert math.isclose(collected["prediction"][0], 18.0, abs_tol=1e-6)
    assert math.isclose(collected["prediction"][1], 38.0, abs_tol=1e-6)


def test_predict_rejects_feature_col_mismatch(linreg_dataset: pl.DataFrame) -> None:
    artefact, _, _ = fit_linear_regression(
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )
    with pytest.raises(ValueError, match="do not match"):
        apply_predict(pl.LazyFrame({"x1": [1.0], "x2": [1.0]}), artefact, ["x2", "x1"], "prediction")


def test_predict_rejects_malformed_artefact() -> None:
    with pytest.raises(ValueError, match="missing keys"):
        apply_predict(pl.LazyFrame({"x1": [1.0]}), {"coeffs": [1.0]}, ["x1"], "prediction")


def test_preview_schema_is_stable() -> None:
    # The flow-graph schema callback reads this; if it ever drifts from the
    # DataFrame produced by fit, downstream nodes will mispredict their schema.
    assert PREVIEW_SCHEMA == [
        {"name": "feature", "data_type": "String"},
        {"name": "coefficient", "data_type": "Float64"},
    ]
