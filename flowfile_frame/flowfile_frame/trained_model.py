"""The model a Train Model node fitted, as :meth:`FlowFrame.read_model` returns it."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from flowfile_core.flowfile.flow_graph.builders.ml import ml_flow_model_path

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

_SHARED_FIELDS = ("model_type", "task_type", "target", "features", "params", "output_dtype")


@dataclass(frozen=True, repr=False)
class TrainedModel:
    """A fitted model: the fields every model type has, plus what the fit produced in ``fitted``.

    ``fitted`` is the rest of the stored model as its trainer wrote it: ``coefficients`` and ``intercept``
    for a linear or logistic model, ``k``, ``distance`` and the training rows for KNN. ``coefficients``
    and ``intercept`` are also attributes (``None`` for a model without them), and ``to_dict()`` gives the
    whole stored model.
    """

    model_type: str
    task_type: str
    target: str
    features: list[str]
    params: dict[str, Any]
    output_dtype: str
    fitted: dict[str, Any]

    @property
    def coefficients(self) -> dict[str, float] | None:
        """Each feature's coefficient for a linear or logistic model, else ``None``."""
        coefficients = self.fitted.get("coefficients")
        return None if coefficients is None else dict(zip(self.features, coefficients, strict=True))

    @property
    def intercept(self) -> float | None:
        """The intercept of a linear or logistic model, else ``None``."""
        return self.fitted.get("intercept")

    def to_dict(self) -> dict[str, Any]:
        """The model as its trainer stored it."""
        return copy.deepcopy({**{name: getattr(self, name) for name in _SHARED_FIELDS}, **self.fitted})

    def __repr__(self) -> str:
        lines = [
            f"TrainedModel ({self.model_type}, {self.task_type})",
            f"  Target: {self.target}",
            f"  Features: {', '.join(self.features)}",
        ]
        coefficients = self.coefficients
        if coefficients is not None:
            lines.append(f"  Intercept: {self.intercept:.6g}")
            lines.append("  Coefficients: " + ", ".join(f"{name}={value:.6g}" for name, value in coefficients.items()))
        else:
            # Lists hold training rows (KNN), too long to print.
            lines += [f"  {name}: {value}" for name, value in self.fitted.items() if not isinstance(value, list | dict)]
        return "\n".join(lines)


def _from_stored(model: Mapping[str, Any]) -> TrainedModel:
    """A :class:`TrainedModel` from a stored model (the trainer's JSON)."""
    fitted = {name: value for name, value in model.items() if name not in _SHARED_FIELDS}
    return TrainedModel(**{name: model[name] for name in _SHARED_FIELDS}, fitted=fitted)


def _train_model_note(flow_graph: FlowGraph) -> str:
    """Where this flow's models are, for ``read_model`` called on a frame that is not a Train Model node's."""
    nodes = [
        f"node {node.node_id} ({settings.model_type} on {settings.target_column!r})"
        for node in flow_graph.nodes
        if node.node_type == "train_model" and (settings := getattr(node.setting_input, "train_input", None))
    ]
    if not nodes:
        return (
            "This flow has no Train Model node yet: fit one with .train_model(target=..., features=...) "
            "and call read_model() on the frame it returns"
        )
    return f"Train Model nodes in this flow: {', '.join(nodes)}; call read_model() on the frame train_model returned"


def _read_trained_model(flow_id: int, node_id: int) -> TrainedModel | None:
    """The model Train Model node ``node_id`` of flow ``flow_id`` last fitted, or ``None`` before it ran.

    The one place the frame reads where a fitted model is kept.
    """
    path = ml_flow_model_path(flow_id, node_id)
    return _from_stored(json.loads(path.read_text())) if path.exists() else None
