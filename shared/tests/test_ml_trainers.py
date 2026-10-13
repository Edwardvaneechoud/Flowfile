"""Unit tests for :mod:`shared.ml.trainers` on the column shapes real data produces.

Derived flags (``is_not_null().cast(Int8)``), narrow integer reads, decimals and
missing values all reach the trainers in practice; the worker round-trip tests only
feed clean Float64 columns.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from shared.ml.trainers import TRAINER_REGISTRY, get_trainer

FEATURES = ["flag", "x"]
CLASSIFIERS = {"logistic_regression", "knn_classifier"}
NULL_PROPAGATING = sorted(set(TRAINER_REGISTRY) - {"knn_classifier"})


def _data(n: int = 60) -> pl.DataFrame:
    flag = [i % 2 for i in range(n)]
    x = [((i * 7) % 13) / 3.0 for i in range(n)]
    noise = [((i * 5) % 7 - 3) / 10.0 for i in range(n)]
    y_reg = [2.0 * f + 0.5 * v + e for f, v, e in zip(flag, x, noise, strict=True)]
    # Flipped labels every 9th row keep the classes non-separable so logistic coefficients stay finite.
    y_cls = [int((f + v / 4 + e > 1.2) != (i % 9 == 0)) for i, (f, v, e) in enumerate(zip(flag, x, noise, strict=True))]
    return pl.DataFrame({"flag": flag, "x": x, "y_reg": y_reg, "y_cls": y_cls})


def _target(model_type: str) -> str:
    return "y_cls" if model_type in CLASSIFIERS else "y_reg"


def _params(model_type: str) -> dict:
    return {"k": 3} if model_type == "knn_classifier" else {}


def _train(model_type: str, df: pl.DataFrame) -> dict:
    trainer = get_trainer(model_type)
    return json.loads(trainer.train(df.lazy(), _target(model_type), FEATURES, _params(model_type)))


def _predict(model_type: str, model: dict, df: pl.DataFrame) -> list:
    return get_trainer(model_type).apply(df.lazy(), model, "pred").collect()["pred"].to_list()


@pytest.mark.parametrize("model_type", sorted(TRAINER_REGISTRY))
@pytest.mark.parametrize(
    "dtype",
    [pl.Int8, pl.Int16, pl.UInt8, pl.Boolean, pl.Float32, pl.Decimal(10, 2)],
    ids=lambda d: str(d),
)
def test_narrow_and_decimal_features_train_like_float64(model_type, dtype):
    baseline_df = _data()
    narrow_df = baseline_df.with_columns(pl.col("flag").cast(dtype))

    baseline = _train(model_type, baseline_df)
    narrow = _train(model_type, narrow_df)

    if model_type == "knn_classifier":
        assert narrow["train_X"] == baseline["train_X"]
    else:
        assert narrow["coefficients"] == pytest.approx(baseline["coefficients"])
        assert narrow["intercept"] == pytest.approx(baseline["intercept"])
    assert _predict(model_type, narrow, narrow_df) == pytest.approx(_predict(model_type, baseline, baseline_df))


@pytest.mark.parametrize("model_type", NULL_PROPAGATING)
def test_rows_with_missing_features_are_skipped_in_training(model_type):
    df = _data()
    with_nulls = df.with_columns(
        pl.when(pl.int_range(pl.len()) % 5 == 0).then(None).otherwise(pl.col("x")).alias("x"),
        pl.col("flag").cast(pl.Int8),
    )

    model = _train(model_type, with_nulls)
    complete = _train(model_type, with_nulls.drop_nulls(FEATURES))

    assert model["coefficients"] == pytest.approx(complete["coefficients"])
    assert model["intercept"] == pytest.approx(complete["intercept"])


@pytest.mark.parametrize(
    "redundant",
    [pl.col("flag") + pl.col("x") + 1, pl.lit(1.0)],
    ids=["sum_of_other_features", "constant"],
)
def test_linear_regression_names_redundant_features(redundant):
    df = _data().with_columns(redundant.alias("total"))
    trainer = get_trainer("linear_regression")

    with pytest.raises(ValueError, match="constant, or a linear combination"):
        trainer.train(df.lazy(), "y_reg", [*FEATURES, "total"], {})


@pytest.mark.parametrize("model_type", ["ridge_regression", "lasso_regression", "logistic_regression"])
def test_regularised_and_logistic_models_fit_redundant_features(model_type):
    df = _data().with_columns((pl.col("flag") + pl.col("x") + 1).alias("total"))

    model = json.loads(get_trainer(model_type).train(df.lazy(), _target(model_type), [*FEATURES, "total"], {}))

    assert len(model["coefficients"]) == 3


@pytest.mark.parametrize("model_type", NULL_PROPAGATING)
def test_apply_leaves_rows_with_missing_features_unscored(model_type):
    model = _train(model_type, _data())
    new = pl.DataFrame(
        {"flag": [1, None, 0], "x": [2.0, 1.0, None]},
        schema={"flag": pl.Int8, "x": pl.Float64},
    )

    predictions = _predict(model_type, model, new)

    assert predictions[0] is not None
    assert predictions[1:] == [None, None]
