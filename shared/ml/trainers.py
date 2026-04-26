"""Trainer implementations for the ML node.

Each ``Trainer`` knows how to fit a model, serialise it, and apply it to new
data. v1 supports linear, ridge, and lasso regression — all three return
coefficient vectors via :func:`polars_ds.lin_reg`, so we serialise as JSON
and reconstruct predictions as a polars expression. Future trainers backed
by sklearn (random forest, gradient boosting, KNN) plug in via the same
protocol with ``serialization_format="joblib"``.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, Protocol

import polars as pl
from pydantic import BaseModel

from shared.ml.algorithms import (
    HyperparamsKNNClassifier,
    HyperparamsLasso,
    HyperparamsLinear,
    HyperparamsLogistic,
    HyperparamsRidge,
    MLAlgorithmSpec,
    MLParamSpec,
)


class Trainer(Protocol):
    """Strategy interface every supported algorithm implements."""

    model_type: str
    label: str
    task_type: str
    output_dtype: str
    serialization_format: str
    params_class: type[BaseModel]

    def spec(self) -> MLAlgorithmSpec: ...

    def train(
        self,
        lf: pl.LazyFrame,
        target: str,
        features: list[str],
        params: dict[str, Any],
    ) -> bytes: ...

    def apply(
        self,
        lf: pl.LazyFrame,
        model: dict[str, Any],
        output_column: str,
    ) -> pl.LazyFrame: ...


class _LinearFamilyTrainer:
    """Shared implementation for linear-coefficient trainers (linear/ridge/lasso).

    Subclasses set ``model_type``, ``label`` and override :meth:`_lin_reg_kwargs`
    to inject the regularisation hyperparameter. The fitted model is serialised
    to a small JSON document and applied as a sum-of-products polars expression.
    """

    task_type: ClassVar[str] = "regression"
    output_dtype: ClassVar[str] = "Float64"
    serialization_format: ClassVar[str] = "json"
    params_class: ClassVar[type[BaseModel]] = HyperparamsLinear

    model_type: ClassVar[str]
    label: ClassVar[str]
    description: ClassVar[str | None] = None
    extra_param_specs: ClassVar[tuple[MLParamSpec, ...]] = ()

    def _lin_reg_kwargs(self, params: BaseModel) -> dict[str, Any]:
        return {"add_bias": params.add_bias}

    def spec(self) -> MLAlgorithmSpec:
        params: list[MLParamSpec] = [
            MLParamSpec(
                name="add_bias",
                type="boolean",
                label="Include intercept",
                default=True,
                description="Fit a bias / intercept term in addition to the feature coefficients.",
            ),
            *self.extra_param_specs,
        ]
        return MLAlgorithmSpec(
            model_type=self.model_type,
            label=self.label,
            task_type=self.task_type,
            output_dtype=self.output_dtype,
            params=params,
            description=self.description,
        )

    def train(
        self,
        lf: pl.LazyFrame,
        target: str,
        features: list[str],
        params: dict[str, Any],
    ) -> bytes:
        import polars_ds as pds

        validated = self.params_class(**params)
        kwargs = self._lin_reg_kwargs(validated)
        coefs_frame = lf.select(pds.lin_reg(*features, target=target, **kwargs)).collect()
        if coefs_frame.height == 0:
            raise ValueError("Training data is empty; cannot fit a regression model.")
        coeffs = coefs_frame.row(0)[0]
        if validated.add_bias:
            *coefficients, intercept = coeffs
        else:
            coefficients = list(coeffs)
            intercept = 0.0

        if len(coefficients) != len(features):
            raise ValueError(
                f"Coefficient count ({len(coefficients)}) does not match feature count ({len(features)}). "
                "polars_ds.lin_reg returned an unexpected shape."
            )

        model = {
            "model_type": self.model_type,
            "task_type": self.task_type,
            "target": target,
            "features": list(features),
            "coefficients": list(coefficients),
            "intercept": float(intercept),
            "params": validated.model_dump(),
            "output_dtype": self.output_dtype,
        }
        return json.dumps(model).encode("utf-8")

    def apply(
        self,
        lf: pl.LazyFrame,
        model: dict[str, Any],
        output_column: str,
    ) -> pl.LazyFrame:
        features = model["features"]
        coefficients = model["coefficients"]
        intercept = float(model.get("intercept", 0.0))
        missing = [f for f in features if f not in lf.collect_schema().names()]
        if missing:
            raise ValueError(
                f"Apply Model: input is missing required feature column(s) {missing!r}. "
                f"Model was trained on {features!r}."
            )
        expr = pl.lit(intercept, dtype=pl.Float64)
        for feature_name, coef in zip(features, coefficients, strict=True):
            expr = expr + pl.col(feature_name).cast(pl.Float64) * pl.lit(float(coef))
        return lf.with_columns(expr.alias(output_column))


class LinearRegressionTrainer(_LinearFamilyTrainer):
    """Ordinary least-squares linear regression."""

    model_type = "linear_regression"
    label = "Linear Regression"
    description = "Ordinary least-squares regression."
    params_class = HyperparamsLinear


class RidgeRegressionTrainer(_LinearFamilyTrainer):
    """L2-penalised linear regression."""

    model_type = "ridge_regression"
    label = "Ridge Regression"
    description = "Linear regression with L2 (Ridge) regularisation."
    params_class = HyperparamsRidge
    extra_param_specs = (
        MLParamSpec(
            name="l2_reg",
            type="number",
            label="L2 penalty (alpha)",
            default=0.1,
            min=0.0,
            step=0.01,
            description="Strength of the L2 penalty term. Higher values shrink coefficients more.",
        ),
    )

    def _lin_reg_kwargs(self, params: BaseModel) -> dict[str, Any]:
        kwargs = super()._lin_reg_kwargs(params)
        kwargs["l2_reg"] = params.l2_reg
        return kwargs


class LassoRegressionTrainer(_LinearFamilyTrainer):
    """L1-penalised linear regression."""

    model_type = "lasso_regression"
    label = "Lasso Regression"
    description = "Linear regression with L1 (Lasso) regularisation; performs feature selection."
    params_class = HyperparamsLasso
    extra_param_specs = (
        MLParamSpec(
            name="l1_reg",
            type="number",
            label="L1 penalty (alpha)",
            default=0.1,
            min=0.0,
            step=0.01,
            description="Strength of the L1 penalty term. Higher values produce sparser solutions.",
        ),
        MLParamSpec(
            name="max_iter",
            type="integer",
            label="Max iterations",
            default=200,
            min=1,
            step=1,
            description="Maximum coordinate-descent iterations.",
        ),
    )

    def _lin_reg_kwargs(self, params: BaseModel) -> dict[str, Any]:
        kwargs = super()._lin_reg_kwargs(params)
        kwargs["l1_reg"] = params.l1_reg
        kwargs["max_iter"] = params.max_iter
        return kwargs


class LogisticRegressionTrainer:
    """Binary logistic regression backed by polars_ds.logistic_reg.

    Targets must be 0/1 integer labels; predictions are the argmax of the
    sigmoid (logit > 0 -> class 1) cast to Int64. Same JSON wire format as
    the regression family, so the worker dispatch path is unchanged.
    """

    model_type: ClassVar[str] = "logistic_regression"
    label: ClassVar[str] = "Logistic Regression"
    description: ClassVar[str | None] = (
        "Binary classification. Target column must contain 0/1 integer labels."
    )
    task_type: ClassVar[str] = "classification"
    output_dtype: ClassVar[str] = "Int64"
    serialization_format: ClassVar[str] = "json"
    params_class: ClassVar[type[BaseModel]] = HyperparamsLogistic

    extra_param_specs: ClassVar[tuple[MLParamSpec, ...]] = (
        MLParamSpec(
            name="l2_reg",
            type="number",
            label="L2 penalty",
            default=0.0,
            min=0.0,
            step=0.01,
            description="L2 (Ridge) regularisation strength. 0 means unregularised.",
        ),
        MLParamSpec(
            name="l1_reg",
            type="number",
            label="L1 penalty",
            default=0.0,
            min=0.0,
            step=0.01,
            description=(
                "L1 (Lasso) regularisation. Non-zero switches the solver to "
                "OWL-QN and yields sparser coefficients."
            ),
        ),
        MLParamSpec(
            name="max_iter",
            type="integer",
            label="Max iterations",
            default=200,
            min=1,
            step=1,
            description="Maximum L-BFGS / OWL-QN iterations.",
        ),
    )

    def spec(self) -> MLAlgorithmSpec:
        params: list[MLParamSpec] = [
            MLParamSpec(
                name="add_bias",
                type="boolean",
                label="Include intercept",
                default=True,
                description="Fit a bias / intercept term in addition to the feature coefficients.",
            ),
            *self.extra_param_specs,
        ]
        return MLAlgorithmSpec(
            model_type=self.model_type,
            label=self.label,
            task_type=self.task_type,
            output_dtype=self.output_dtype,
            params=params,
            description=self.description,
        )

    def train(
        self,
        lf: pl.LazyFrame,
        target: str,
        features: list[str],
        params: dict[str, Any],
    ) -> bytes:
        import polars_ds as pds

        validated = self.params_class(**params)
        coefs_frame = lf.select(
            pds.logistic_reg(
                *features,
                target=target,
                add_bias=validated.add_bias,
                l1_reg=validated.l1_reg,
                l2_reg=validated.l2_reg,
                max_iter=validated.max_iter,
            )
        ).collect()
        if coefs_frame.height == 0:
            raise ValueError(
                "Training data is empty; cannot fit a logistic regression model."
            )
        coeffs = coefs_frame.row(0)[0]
        if validated.add_bias:
            *coefficients, intercept = coeffs
        else:
            coefficients = list(coeffs)
            intercept = 0.0

        if len(coefficients) != len(features):
            raise ValueError(
                f"Coefficient count ({len(coefficients)}) does not match feature count ({len(features)}). "
                "polars_ds.logistic_reg returned an unexpected shape."
            )

        model = {
            "model_type": self.model_type,
            "task_type": self.task_type,
            "target": target,
            "features": list(features),
            "coefficients": list(coefficients),
            "intercept": float(intercept),
            "params": validated.model_dump(),
            "output_dtype": self.output_dtype,
        }
        return json.dumps(model).encode("utf-8")

    def apply(
        self,
        lf: pl.LazyFrame,
        model: dict[str, Any],
        output_column: str,
    ) -> pl.LazyFrame:
        features = model["features"]
        coefficients = model["coefficients"]
        intercept = float(model.get("intercept", 0.0))
        missing = [f for f in features if f not in lf.collect_schema().names()]
        if missing:
            raise ValueError(
                f"Apply Model: input is missing required feature column(s) {missing!r}. "
                f"Model was trained on {features!r}."
            )
        # Decision boundary: sigmoid(logit) > 0.5  <=>  logit > 0; skip the
        # exp/log roundtrip and threshold the linear combination directly.
        logit = pl.lit(intercept, dtype=pl.Float64)
        for feature_name, coef in zip(features, coefficients, strict=True):
            logit = logit + pl.col(feature_name).cast(pl.Float64) * pl.lit(float(coef))
        return lf.with_columns((logit > 0).cast(pl.Int64).alias(output_column))


class KNNClassifierTrainer:
    """Binary KNN classification backed by polars_ds.query_knn_ptwise.

    KNN is non-parametric: the "model" is the training feature matrix and
    label vector serialised inside our JSON envelope. At apply time we vstack
    the training rows on top of the query rows, run the kd-tree query with
    ``data_mask`` so only training rows are candidate neighbours, and take a
    majority vote over the k retrieved labels for each query row.

    Targets must be 0/1 integer labels. The serialised model size grows with
    the training set; suitable for demo-scale data, less so for million-row
    training sets.
    """

    model_type: ClassVar[str] = "knn_classifier"
    label: ClassVar[str] = "K-Nearest Neighbours (Classifier)"
    description: ClassVar[str | None] = (
        "Non-parametric binary classification. Stores training data in the "
        "model artifact and predicts via majority vote over k nearest neighbours."
    )
    task_type: ClassVar[str] = "classification"
    output_dtype: ClassVar[str] = "Int64"
    serialization_format: ClassVar[str] = "json"
    params_class: ClassVar[type[BaseModel]] = HyperparamsKNNClassifier

    def spec(self) -> MLAlgorithmSpec:
        params: list[MLParamSpec] = [
            MLParamSpec(
                name="k",
                type="integer",
                label="Neighbours (k)",
                default=5,
                min=1,
                step=1,
                description="Number of nearest neighbours to consult for the majority vote.",
            ),
            MLParamSpec(
                name="distance",
                type="select",
                label="Distance metric",
                default="sql2",
                options=["sql2", "l1", "l2", "inf"],
                description=(
                    "Distance function. 'sql2' is squared L2 (fastest), 'l1' is "
                    "Manhattan, 'l2' is Euclidean, 'inf' is Chebyshev."
                ),
            ),
        ]
        return MLAlgorithmSpec(
            model_type=self.model_type,
            label=self.label,
            task_type=self.task_type,
            output_dtype=self.output_dtype,
            params=params,
            description=self.description,
        )

    def train(
        self,
        lf: pl.LazyFrame,
        target: str,
        features: list[str],
        params: dict[str, Any],
    ) -> bytes:
        validated = self.params_class(**params)
        df = lf.select([*features, target]).drop_nulls().collect()
        if df.height == 0:
            raise ValueError("Training data is empty; cannot fit a KNN model.")

        distinct_y = sorted(set(df[target].to_list()))
        if not set(distinct_y).issubset({0, 1}):
            raise ValueError(
                "KNN classifier requires a binary 0/1 target column; "
                f"got distinct values {distinct_y!r}."
            )

        train_X = {f: df[f].cast(pl.Float64).to_list() for f in features}
        train_y = df[target].cast(pl.Int64).to_list()

        model = {
            "model_type": self.model_type,
            "task_type": self.task_type,
            "target": target,
            "features": list(features),
            "k": validated.k,
            "distance": validated.distance,
            "train_X": train_X,
            "train_y": train_y,
            "params": validated.model_dump(),
            "output_dtype": self.output_dtype,
        }
        return json.dumps(model).encode("utf-8")

    def apply(
        self,
        lf: pl.LazyFrame,
        model: dict[str, Any],
        output_column: str,
    ) -> pl.LazyFrame:
        """Apply the KNN model lazily.

        The whole pipeline stays in polars: train rows (carrying labels) are
        concatenated under the test rows, ``query_knn_ptwise`` produces a list
        of neighbour row-indices per test row, that list is exploded and
        self-joined back to the labels, and a group-by + majority vote yields
        the prediction. The result is left-joined onto the original input via a
        ``__test_pos`` row index so we never collect the user's data here —
        downstream nodes can keep streaming.

        Tie-break (e.g. k=2 with split labels) goes to class 1, matching the
        previous eager implementation.
        """
        import polars_ds as pds

        features = model["features"]
        train_X = model["train_X"]
        train_y = model["train_y"]
        k = int(model["k"])
        dist = model.get("distance", "sql2")

        missing = [f for f in features if f not in lf.collect_schema().names()]
        if missing:
            raise ValueError(
                f"Apply Model: input is missing required feature column(s) {missing!r}. "
                f"Model was trained on {features!r}."
            )

        # Train rows carry their label so the neighbour-label lookup is a
        # single join instead of a Python indexing loop.
        train_lf = pl.LazyFrame(
            {**{f: train_X[f] for f in features}, "__label": train_y},
            schema={**{f: pl.Float64 for f in features}, "__label": pl.Int64},
        ).with_columns(
            pl.lit(True).alias("__is_train"),
            pl.lit(None, dtype=pl.UInt32).alias("__test_pos"),
        )

        # Tag input rows with their position so we can re-attach predictions
        # to the original LazyFrame without collecting.
        test_lf = lf.with_row_index("__test_pos")
        test_features = test_lf.select(
            *[pl.col(f).cast(pl.Float64) for f in features],
            pl.lit(None, dtype=pl.Int64).alias("__label"),
            pl.lit(False).alias("__is_train"),
            pl.col("__test_pos"),
        )

        combined = pl.concat([train_lf, test_features], how="vertical").with_row_index(
            "__row_idx"
        )

        knn_lf = combined.with_columns(
            pds.query_knn_ptwise(
                *features,
                index="__row_idx",
                k=k,
                dist=dist,
                data_mask="__is_train",
            ).alias("__nb_idx")
        )

        label_lookup = combined.select(
            pl.col("__row_idx").alias("__nb_idx"),
            pl.col("__label").alias("__nb_label"),
        )

        # Explode neighbour indices, join labels, group-by test row, majority-vote.
        # Empty neighbour lists explode to a single null row -> count=0 -> class 0.
        voted = (
            knn_lf.filter(~pl.col("__is_train"))
            .select("__test_pos", "__nb_idx")
            .explode("__nb_idx")
            .join(label_lookup, on="__nb_idx", how="left")
            .group_by("__test_pos")
            .agg(
                pl.col("__nb_label").drop_nulls().sum().alias("__vote_sum"),
                pl.col("__nb_label").drop_nulls().count().alias("__vote_cnt"),
            )
            .with_columns(
                pl.when(pl.col("__vote_cnt") == 0)
                .then(pl.lit(0, dtype=pl.Int64))
                .otherwise(
                    (pl.col("__vote_sum") * 2 >= pl.col("__vote_cnt")).cast(pl.Int64)
                )
                .alias(output_column)
            )
            .select("__test_pos", output_column)
        )

        return (
            test_lf.join(voted, on="__test_pos", how="left")
            .with_columns(pl.col(output_column).fill_null(0).cast(pl.Int64))
            .drop("__test_pos")
        )


_TRAINERS: tuple[Trainer, ...] = (
    LinearRegressionTrainer(),
    RidgeRegressionTrainer(),
    LassoRegressionTrainer(),
    LogisticRegressionTrainer(),
    KNNClassifierTrainer(),
)

TRAINER_REGISTRY: dict[str, Trainer] = {t.model_type: t for t in _TRAINERS}


def get_trainer(model_type: str) -> Trainer:
    """Return the trainer for *model_type* or raise a clear error."""
    try:
        return TRAINER_REGISTRY[model_type]
    except KeyError as exc:
        supported = ", ".join(sorted(TRAINER_REGISTRY))
        raise ValueError(
            f"Unknown model_type {model_type!r}. Supported: {supported}."
        ) from exc
