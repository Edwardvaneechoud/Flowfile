"""Unit tests for the data-science estimator + predict layer.

Exercises the library directly — no worker, no Catalog, no HTTP. Worker
round-trip is covered separately by integration tests.
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from shared.data_science.estimators import (
    FIT_KINDS,
    PREVIEW_SCHEMAS,
    fit_estimator,
)
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


def test_linreg_returns_json_artefact(linreg_dataset: pl.DataFrame) -> None:
    artefact, preview, output_schema = fit_estimator(
        kind="linreg",
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
        prediction_col="prediction",
    )

    assert artefact["kind"] == "linreg"
    assert artefact["feature_names"] == ["x1", "x2"]
    assert math.isclose(artefact["coeffs"][0], 2.0, abs_tol=1e-6)
    assert math.isclose(artefact["coeffs"][1], 3.0, abs_tol=1e-6)
    assert math.isclose(artefact["bias"], 0.0, abs_tol=1e-6)

    # Preview DataFrame rows match the artefact (so the UI preview is truthful).
    preview_coefs = dict(zip(preview["feature"].to_list(), preview["coefficient"].to_list(), strict=True))
    assert math.isclose(preview_coefs["x1"], 2.0, abs_tol=1e-6)
    assert math.isclose(preview_coefs["x2"], 3.0, abs_tol=1e-6)
    assert math.isclose(preview_coefs["__intercept__"], 0.0, abs_tol=1e-6)

    assert output_schema == [{"name": "prediction", "data_type": "Float64"}]


def test_linreg_artefact_is_json_serialisable(linreg_dataset: pl.DataFrame) -> None:
    import json

    artefact, _, _ = fit_estimator(
        kind="linreg",
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )
    # Round-tripping through JSON must preserve the artefact exactly.
    assert json.loads(json.dumps(artefact)) == artefact


def test_predict_is_lazy_and_round_trips(linreg_dataset: pl.DataFrame) -> None:
    artefact, _, _ = fit_estimator(
        kind="linreg",
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )

    new_data = pl.LazyFrame({"x1": [6.0, 7.0], "x2": [2.0, 8.0]})
    out = apply_predict(new_data, artefact, ["x1", "x2"], "prediction")

    assert isinstance(out, pl.LazyFrame), "predict must stay lazy"

    collected = out.collect()
    assert "prediction" in collected.columns
    # y = 2*x1 + 3*x2 → 18 and 38
    assert math.isclose(collected["prediction"][0], 18.0, abs_tol=1e-6)
    assert math.isclose(collected["prediction"][1], 38.0, abs_tol=1e-6)


def test_predict_rejects_feature_col_mismatch(linreg_dataset: pl.DataFrame) -> None:
    artefact, _, _ = fit_estimator(
        kind="linreg",
        df=linreg_dataset,
        feature_cols=["x1", "x2"],
        target_col="y",
    )
    with pytest.raises(ValueError, match="do not match"):
        apply_predict(pl.LazyFrame({"x1": [1.0], "x2": [1.0]}), artefact, ["x2", "x1"], "prediction")


def test_predict_rejects_malformed_artefact() -> None:
    with pytest.raises(ValueError, match="missing keys"):
        apply_predict(pl.LazyFrame({"x1": [1.0]}), {"coeffs": [1.0]}, ["x1"], "prediction")


def test_supervised_kinds_require_target_col(linreg_dataset: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="target_col"):
        fit_estimator(
            kind="linreg",
            df=linreg_dataset,
            feature_cols=["x1", "x2"],
            target_col=None,
        )


def test_unknown_kind_is_rejected(linreg_dataset: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="Unknown estimator kind"):
        fit_estimator(
            kind="not_a_real_kind",
            df=linreg_dataset,
            feature_cols=["x1"],
            target_col="y",
        )


def test_stub_kinds_raise_not_implemented(linreg_dataset: pl.DataFrame) -> None:
    # Phase 1 ships only linreg; the rest are wired stubs.
    with pytest.raises(NotImplementedError, match="ridge"):
        fit_estimator(
            kind="ridge",
            df=linreg_dataset,
            feature_cols=["x1", "x2"],
            target_col="y",
        )


def test_all_known_kinds_are_dispatched() -> None:
    # Catches typos when somebody adds a kind to the literal but forgets the dispatch entry.
    expected = {"linreg", "ridge", "lasso", "kmeans", "knn_cls", "knn_reg", "pca"}
    assert set(FIT_KINDS) == expected
    # Only linreg ships preview schema in Phase 1.
    assert "linreg" in PREVIEW_SCHEMAS
