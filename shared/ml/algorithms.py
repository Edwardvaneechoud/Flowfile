"""Pydantic specs for ML algorithms and hyperparameters.

The frontend reads these via ``GET /ml/algorithms`` to render the dynamic
hyperparameter form for the Train Model node, so a new algorithm only needs:

1. A new ``Hyperparams<Foo>`` Pydantic class declaring its tunable params,
2. A new ``Trainer`` registered in :mod:`shared.ml.trainers`, and
3. (Optional) extra ``MLParamSpec`` entries on the spec.

No new Vue component is required.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class HyperparamsLinear(BaseModel):
    """Hyperparameters for ordinary least-squares linear regression."""

    add_bias: bool = True


class HyperparamsRidge(BaseModel):
    """Hyperparameters for Ridge (L2-penalised) regression."""

    add_bias: bool = True
    l2_reg: float = Field(default=0.1, ge=0.0)


class HyperparamsLasso(BaseModel):
    """Hyperparameters for Lasso (L1-penalised) regression."""

    add_bias: bool = True
    l1_reg: float = Field(default=0.1, ge=0.0)
    max_iter: int = Field(default=200, ge=1)


class HyperparamsLogistic(BaseModel):
    """Hyperparameters for binary logistic regression (polars_ds L-BFGS / OWL-QN)."""

    add_bias: bool = True
    l2_reg: float = Field(default=0.0, ge=0.0)
    l1_reg: float = Field(default=0.0, ge=0.0)
    max_iter: int = Field(default=200, ge=1)


class HyperparamsKNNClassifier(BaseModel):
    """Hyperparameters for binary KNN classification (polars_ds kd-tree)."""

    k: int = Field(default=5, ge=1)
    distance: Literal["sql2", "l1", "l2", "inf"] = "sql2"


MLParamType = Literal["boolean", "number", "integer", "select"]


class MLParamSpec(BaseModel):
    """Description of a single hyperparameter for the dynamic UI."""

    name: str
    type: MLParamType
    label: str
    default: Any
    description: str | None = None
    min: float | None = None
    max: float | None = None
    step: float | None = None
    options: list[str] | None = None  # for "select" type


class MLAlgorithmSpec(BaseModel):
    """UI/registry description of one ML algorithm."""

    model_config = ConfigDict(protected_namespaces=())

    model_type: str
    label: str
    task_type: Literal["regression", "classification"]
    output_dtype: str  # Polars dtype string for the prediction column (e.g. "Float64")
    params: list[MLParamSpec] = Field(default_factory=list)
    description: str | None = None


def get_algorithm_specs() -> list[MLAlgorithmSpec]:
    """Build specs from the trainer registry.

    Imported lazily to avoid pulling polars-ds into modules that only need the
    schemas (e.g. early bootstrap or schema generation).
    """
    from shared.ml.trainers import TRAINER_REGISTRY

    return [trainer.spec() for trainer in TRAINER_REGISTRY.values()]
