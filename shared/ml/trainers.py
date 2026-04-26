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
    HyperparamsLasso,
    HyperparamsLinear,
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


_TRAINERS: tuple[Trainer, ...] = (
    LinearRegressionTrainer(),
    RidgeRegressionTrainer(),
    LassoRegressionTrainer(),
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
