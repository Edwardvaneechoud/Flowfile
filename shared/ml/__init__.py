"""Shared ML algorithm registry used by both flowfile_core and flowfile_worker.

Core uses this to render the algorithm picker / dynamic-params UI and to
validate hyperparameters. Worker uses this to dispatch training and apply
operations. The registry is the single source of truth for which algorithms
are supported and what hyperparameters they accept.
"""

from shared.ml.algorithms import (
    HyperparamsLasso,
    HyperparamsLinear,
    HyperparamsRidge,
    MLAlgorithmSpec,
    MLParamSpec,
    get_algorithm_specs,
)
from shared.ml.trainers import (
    TRAINER_REGISTRY,
    LassoRegressionTrainer,
    LinearRegressionTrainer,
    RidgeRegressionTrainer,
    Trainer,
    get_trainer,
)

__all__ = [
    "TRAINER_REGISTRY",
    "HyperparamsLasso",
    "HyperparamsLinear",
    "HyperparamsRidge",
    "LassoRegressionTrainer",
    "LinearRegressionTrainer",
    "MLAlgorithmSpec",
    "MLParamSpec",
    "RidgeRegressionTrainer",
    "Trainer",
    "get_algorithm_specs",
    "get_trainer",
]
