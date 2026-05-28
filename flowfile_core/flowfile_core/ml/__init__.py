"""ML metadata API.

Exposes :func:`shared.ml.algorithms.get_algorithm_specs` over HTTP so the
frontend can render the dynamic hyperparameter form for the Train Model node
without hard-coding algorithm-specific Vue components.
"""

from flowfile_core.ml.routes import router

__all__ = ["router"]
